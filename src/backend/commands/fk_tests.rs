//! Integration tests for triggers + foreign-key enforcement (step 41).
//!
//! Each test stands up a real foundation `SharedState` over a tempdir + the full
//! per-task scope stack (the catalog-test harness, plus the after-trigger scope),
//! runs initdb, then drives real SQL through the parse -> analyze -> plan ->
//! (process_utility | ExecutorRun) pipeline. The assertions check the milestone
//! bar: FK violation on bad INSERT, success on good/NULL INSERT, the ON DELETE
//! actions, ADD CONSTRAINT validation, CREATE TRIGGER storage, and DROP FK
//! stopping enforcement.

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
    let dir = std::env::temp_dir().join(format!("pepperdb-fk-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 256,
        ..Default::default()
    })
}

/// Set up the full per-task scope stack (the catalog-test harness + the
/// after-trigger queue scope) and run the async body.
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
    crate::session::scope(
        sess,
        crate::backend::utils::resowner::resowner::scope(owner, body),
    )
    .await
}

/// Run initdb (seed the catalogs) in its own transaction and commit it, so the
/// seeded rows are durably visible to the per-statement autocommit transactions.
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

/// Open a read transaction + active snapshot for direct catalog inspection.
async fn begin_read_txn(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
    use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};
    StartTransactionCommand(shared).await;
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);
}

/// Close the read transaction opened by [`begin_read_txn`].
async fn end_read_txn(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::CommitTransactionCommand;
    use crate::backend::utils::time::snapmgr::PopActiveSnapshot;
    PopActiveSnapshot();
    CommitTransactionCommand(shared).await;
}

/// Run one SQL statement as its own autocommit transaction through the real
/// pipeline (mirrors `exec_simple_query`: start_xact -> push snapshot -> run -> pop
/// -> commit -> publish xid; on error: flush + abort). Returns Ok(rows) on success
/// or Err(message) if the command raised an ereport(ERROR). Each row is a Vec of
/// (i32, isnull); DDL/DML return an empty row list.
async fn run_sql(shared: &Arc<SharedState>, sql: &str) -> Result<Vec<Vec<(i32, bool)>>, String> {
    run_sql_count(shared, sql, 0).await
}

/// Like [`run_sql`] but drives the executor with a non-zero row-count fetch limit
/// (PG's portal `count`), so a RETURNING DML stops mid-run -- the path that
/// exercises the after-trigger drain at the ExecutorFinish boundary (Fix 1).
async fn run_sql_count(
    shared: &Arc<SharedState>,
    sql: &str,
    count: u64,
) -> Result<Vec<Vec<(i32, bool)>>, String> {
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
    let fut = async move { run_sql_inner(&shared2, &sql2, count).await };
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
            // Per-command recovery: flush the error state and abort the autocommit
            // transaction (rolls back this statement's writes only, since each
            // statement is its own transaction). AbortCurrentTransaction is async, so
            // it runs here after the catch (no future held across the catch).
            let msg = describe_panic(payload.as_ref());
            crate::utils::elog::flush_error_state();
            crate::backend::access::transam::xact::AbortCurrentTransaction(shared).await;
            Err(msg)
        }
    }
}

/// Render a caught panic payload (an ereport ErrorData or a string) to its message.
/// SQLSTATE distinction is not available (the errcodes are stubbed to a single
/// value in this milestone), so callers match on the message text.
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
    // Fall back to the message captured by the test panic hook (see run_sql). The
    // hook runs in the panicking thread (which may be a spawn_blocking worker), so
    // the capture is a process-global guarded by a Mutex.
    last_error().unwrap_or_else(|| "unknown error".to_string())
}

static LAST_ERROR_SLOT: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
static HOOK_INSTALLED: std::sync::Once = std::sync::Once::new();

/// Serialize the FK tests: the error-message capture rides a process-global slot
/// (the caught panic payload's concrete type is opaque across the async boundary,
/// so the panic hook stashes the message), which would race across the
/// parallel-by-default test threads. Each test holds this for its duration.
static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn clear_last_error() {
    *LAST_ERROR_SLOT.lock().unwrap() = None;
}

fn last_error() -> Option<String> {
    LAST_ERROR_SLOT.lock().unwrap().clone()
}

/// Install (once) a panic hook that records an ereport `ErrorData`'s message into
/// `LAST_ERROR_SLOT`, so `describe_panic` can recover it even when the caught
/// payload's concrete type is otherwise opaque across the async/thread boundary.
fn install_error_capture_hook() {
    HOOK_INSTALLED.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            if let Some(ed) = payload.downcast_ref::<crate::utils::elog::ErrorData>() {
                *LAST_ERROR_SLOT.lock().unwrap() = ed.message.clone();
                return; // suppress the default hook noise for caught ereports
            }
            prev(info);
        }));
    });
}

async fn run_sql_inner(shared: &Arc<SharedState>, sql: &str, count: u64) -> Vec<Vec<(i32, bool)>> {
    use crate::backend::parser::analyze::parse_analyze_fixedparams_async;
    use crate::backend::parser::parser::raw_parser;
    use crate::backend::optimizer::plan::planner::standard_planner;
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
    let receiver: Box<dyn crate::tcop::dest::DestReceiver> =
        Box::new(RowSink { sink: Arc::clone(&sink) });
    // execute_plan_into opens the range-table relations (under the right locks),
    // runs the plan, and tears down -- the same path the backend uses.
    crate::backend::tcop::postgres::execute_plan_into(shared, &plan, sql, None, receiver, count).await;
    let rows = sink.lock().unwrap().clone();
    drop(sink);
    rows
}

/// A DestReceiver collecting each row as a Vec of (i32, isnull).
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

// ===========================================================================
//  Tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn fk_insert_enforced_good_bad_null() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4 REFERENCES parent)").await.unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();

        // Good FK value -> ok.
        run_sql(&shared, "INSERT INTO child VALUES (1)").await.expect("matching FK ok");
        // Bad FK value -> FK violation.
        let err = run_sql(&shared, "INSERT INTO child VALUES (99)").await.unwrap_err();
        assert!(err.contains("foreign key"), "bad FK insert raises FK violation, got: {err}");
        // NULL FK value -> allowed (MATCH SIMPLE).
        run_sql(&shared, "INSERT INTO child VALUES (NULL)").await.expect("NULL FK ok");
    }))
    .await;
}

/// Fix 1: a RETURNING DML fetched with a row-count limit that stops the run before
/// the ModifyTable node reports done STILL fires the queued AFTER events (the RI FK
/// check). Before the fix, AfterTriggerEndQuery ran only when the ModifyTable node
/// returned None; a limited RETURNING fetch broke out of the run loop first, so the
/// FK violation was silently skipped. Now begin/end-query live at the
/// ExecutorStart/ExecutorFinish boundary, so the check fires regardless.
#[tokio::test(flavor = "multi_thread")]
async fn fk_returning_limited_fetch_still_enforced() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4 REFERENCES parent)").await.unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();
        run_sql(&shared, "INSERT INTO child VALUES (1)").await.unwrap();

        // UPDATE child to a non-existent parent key, fetched with a count limit of 1
        // (the run stops at the first RETURNING row before the ModifyTable node
        // reports done). The AFTER UPDATE RI check must still fire at the finish
        // boundary -> FK violation. Before the fix this was silently skipped.
        let err = run_sql_count(&shared, "UPDATE child SET pid = 99 RETURNING pid", 1)
            .await
            .unwrap_err();
        assert!(
            err.contains("foreign key"),
            "limited-fetch RETURNING update still raises the FK violation, got: {err}"
        );

        // Sanity: a valid limited-fetch RETURNING insert-then-update on a clean row
        // succeeds (the queue fires at finish, finds the key, and does not raise).
        run_sql(&shared, "INSERT INTO parent VALUES (2)").await.unwrap();
        run_sql(&shared, "INSERT INTO child VALUES (2)").await.unwrap();
        let rows = run_sql_count(&shared, "DELETE FROM child WHERE pid = 2 RETURNING pid", 1)
            .await
            .expect("valid limited-fetch RETURNING delete ok");
        assert_eq!(rows.len(), 1, "the one deleted row is returned");
        assert_eq!(rows[0][0].0, 2, "RETURNING projected the deleted pid");
    }))
    .await;
}

/// Fix 3: a genuinely non-coercible INSERT value raises a CATCHABLE
/// DATATYPE_MISMATCH ereport (PG transformAssignedExpr), not a panic. A boolean
/// literal has no implicit cast to an int4 column.
#[tokio::test(flavor = "multi_thread")]
async fn insert_non_coercible_value_raises_catchable_error() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();

        // `1 = 1` is a boolean expression; there is no implicit cast to int4.
        let err = run_sql(&shared, "INSERT INTO t VALUES (1 = 1)").await.unwrap_err();
        assert!(
            err.contains("is of type") && err.contains("but expression is of type"),
            "non-coercible INSERT value raises a catchable datatype-mismatch error, got: {err}"
        );

        // The error was catchable (the transaction recovered): a valid insert still works.
        run_sql(&shared, "INSERT INTO t VALUES (5)").await.expect("valid insert ok after caught error");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fk_delete_no_action_errors() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4 REFERENCES parent)").await.unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();
        run_sql(&shared, "INSERT INTO child VALUES (1)").await.unwrap();

        // Deleting the referenced parent row with a dependent child -> error.
        let err = run_sql(&shared, "DELETE FROM parent WHERE id = 1").await.unwrap_err();
        assert!(err.contains("foreign key"), "NO ACTION delete errors, got: {err}");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fk_delete_cascade_removes_child() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4 REFERENCES parent ON DELETE CASCADE)")
            .await
            .unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();
        run_sql(&shared, "INSERT INTO child VALUES (1)").await.unwrap();

        run_sql(&shared, "DELETE FROM parent WHERE id = 1").await.expect("cascade delete ok");
        let rows = run_sql(&shared, "SELECT pid FROM child").await.unwrap();
        assert!(rows.is_empty(), "ON DELETE CASCADE removed the child row, got {rows:?}");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fk_delete_set_null_nulls_child() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4 REFERENCES parent ON DELETE SET NULL)")
            .await
            .unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();
        run_sql(&shared, "INSERT INTO child VALUES (1)").await.unwrap();

        run_sql(&shared, "DELETE FROM parent WHERE id = 1").await.expect("set null delete ok");
        let rows = run_sql(&shared, "SELECT pid FROM child").await.unwrap();
        assert_eq!(rows.len(), 1, "the child row remains");
        assert!(rows[0][0].1, "ON DELETE SET NULL nulled child.pid, got {rows:?}");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_add_fk_validates_existing_data() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4)").await.unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();

        // Violating existing data -> ADD CONSTRAINT errors.
        run_sql(&shared, "INSERT INTO child VALUES (99)").await.unwrap();
        let err = run_sql(
            &shared,
            "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent",
        )
        .await
        .unwrap_err();
        assert!(err.contains("foreign key") || err.contains("violates"), "ADD FK on violating data errors, got: {err}");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_add_fk_on_clean_data_then_enforced() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4)").await.unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();
        run_sql(&shared, "INSERT INTO child VALUES (1)").await.unwrap();

        // Clean data -> ADD CONSTRAINT ok.
        run_sql(
            &shared,
            "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent",
        )
        .await
        .expect("ADD FK on clean data ok");

        // Now enforced: a bad insert errors.
        let err = run_sql(&shared, "INSERT INTO child VALUES (42)").await.unwrap_err();
        assert!(err.contains("foreign key"), "FK enforced after ADD, got: {err}");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_fk_stops_enforcement() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        run_sql(&shared, "CREATE TABLE child (pid int4)").await.unwrap();
        run_sql(&shared, "INSERT INTO parent VALUES (1)").await.unwrap();
        run_sql(
            &shared,
            "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent",
        )
        .await
        .unwrap();

        // Enforced before drop.
        run_sql(&shared, "INSERT INTO child VALUES (42)").await.unwrap_err();

        // Drop the FK -> enforcement stops.
        run_sql(&shared, "ALTER TABLE child DROP CONSTRAINT fk").await.expect("drop fk ok");
        run_sql(&shared, "INSERT INTO child VALUES (42)").await.expect("no enforcement after drop");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_trigger_stores_row_and_sets_relhastriggers() {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_class::{self as pc, RelationRelationId};
    use crate::catalog::pg_trigger::{self as pgt, TriggerRelationId};

    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE parent (id int4)").await.unwrap();
        // A FOREIGN KEY creates system triggers; assert pg_trigger has rows for the
        // child relation and relhastriggers is set.
        run_sql(&shared, "CREATE TABLE child (pid int4 REFERENCES parent)").await.unwrap();

        // Verify the catalog effects in a fresh read transaction.
        begin_read_txn(&shared).await;
        let child_oid =
            crate::backend::catalog::namespace::range_var_get_relid(&shared, None, "child")
                .await
                .expect("child resolves");

        // pg_trigger has the check trigger on child.
        let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
            &shared, TriggerRelationId, pgt::Anum_pg_trigger_tgrelid, child_oid,
        )
        .await;
        assert!(!rows.is_empty(), "child has a pg_trigger row (the RI check trigger)");
        for r in rows {
            crate::backend::access::common::heaptuple::heap_freetuple(r.tuple);
        }

        // relhastriggers is set on child's pg_class row.
        let crows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
            &shared, RelationRelationId, pc::Anum_pg_class_oid, child_oid,
        )
        .await;
        let mut has = false;
        for r in &crows {
            let p = GETSTRUCT(&r.tuple).cast::<pc::FormData_pg_class>();
            if unsafe { (*p).oid } == child_oid {
                has = unsafe { (*p).relhastriggers };
            }
        }
        for r in crows {
            crate::backend::access::common::heaptuple::heap_freetuple(r.tuple);
        }
        assert!(has, "relhastriggers set on child after FK creation");
        end_read_txn(&shared).await;
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn user_trigger_function_missing_language_clean_error() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        // A CREATE TRIGGER naming a non-existent function -> clean undefined-function
        // error (the function lookup fails before any PL execution is attempted).
        let err = run_sql(
            &shared,
            "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION no_such_fn()",
        )
        .await
        .unwrap_err();
        assert!(err.contains("does not exist"), "unknown trigger function errors cleanly, got: {err}");
    }))
    .await;
}
