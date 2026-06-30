//! Server Programming Interface (SPI) -- the server-side query path PL languages
//! and referential-integrity triggers call queries through. Translated from
//! backend/executor/spi.c (disposition: full leaf, the M9-reachable core
//! round-trip: connect -> execute("SELECT ...") -> read the result -> finish).
//!
//! `SPI_connect` pushes a connection frame onto the per-task SPI stack;
//! `SPI_execute` parses + plans + runs a query string, collecting the result rows
//! into the connection's tuptable (and setting `SPI_processed`); `SPI_finish` pops
//! the frame, restoring the caller's outer result. `SPI_prepare` builds a
//! `CachedPlanSource`; `SPI_execute_plan` runs it.
//!
//! Ownership (rules.md s10): the C `SPI_processed` / `SPI_tuptable` process
//! globals + the `_SPI_stack` become a per-task `tokio::task_local!` connection
//! stack (a backend is a tokio task). The result rows are kept DEFORMED (datum +
//! isnull vectors, with the result `TupleDesc`) rather than as raw `HeapTuple`
//! pointers, which keeps the per-task state `Send` and the result directly
//! readable by callers/tests.
//!
//! STAGED: SPI_execute_with_args (advanced), the SPI subtransaction machinery,
//! cursor advanced options, read-only/atomic snapshot subtleties, and the raw
//! `HeapTuple`-pointer `SPITupleTable` ABI (the deformed-row model supersedes it
//! for the in-process callers M9 reaches).

use std::cell::RefCell;
use std::sync::Arc;

use crate::access::tupdesc::TupleDesc;
use crate::nodes::params::ParamListInfoData;
use crate::shared_state::SharedState;
use crate::utils::elog::ERROR;
use crate::utils::plancache::CachedPlanSource;

/// One result row, deformed (datum + per-column NULL flags).
#[derive(Clone)]
pub struct SpiRow {
    pub values: Vec<crate::postgres::Datum>,
    pub isnull: Vec<bool>,
}

/// The result of an SPI query: the rows + their tuple descriptor.
pub struct SpiResult {
    pub tupdesc: Option<TupleDesc>,
    pub rows: Vec<SpiRow>,
}

/// One SPI connection frame (C `_SPI_connection`). Holds this level's result; the
/// outer frame's `SPI_processed`/tuptable are restored on finish.
struct SpiConnection {
    processed: u64,
    result: Option<SpiResult>,
}

tokio::task_local! {
    static SPI_STACK: RefCell<Vec<SpiConnection>>;
}

/// Establish the per-task SPI stack and run `fut`. A backend that may call SPI
/// (PL/pgSQL, RI triggers) wraps its work in this scope. For tests, wrap the body.
pub async fn spi_scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SPI_STACK.scope(RefCell::new(Vec::new()), fut).await
}

fn with_stack<R>(f: impl FnOnce(&mut Vec<SpiConnection>) -> R) -> Option<R> {
    SPI_STACK.try_with(|cell| f(&mut cell.borrow_mut())).ok()
}

/// PG `SPI_connect`: push a new SPI connection frame. The caller must call
/// `SPI_finish` to pop it. (The memory-context / subtransaction bookkeeping is
/// staged; the connection stack itself is faithful.)
pub fn spi_connect() {
    with_stack(|s| s.push(SpiConnection { processed: 0, result: None }))
        .unwrap_or_else(|| unreachable!("SPI_connect outside an SPI scope"));
}

/// PG `SPI_finish`: pop the current SPI connection frame, discarding its result.
/// (PG restores the outer `SPI_processed`/tuptable here; the per-task accessors
/// read the current top frame, so popping restores the outer view automatically.)
pub fn spi_finish() {
    with_stack(|s| {
        s.pop();
    })
    .unwrap_or_else(|| unreachable!("SPI_finish outside an SPI scope"));
}

/// PG `SPI_processed`: the number of rows the last `SPI_execute` produced (current
/// connection frame). 0 if none / no connection.
#[must_use]
pub fn spi_processed() -> u64 {
    with_stack(|s| s.last().map_or(0, |c| c.processed)).unwrap_or(0)
}

/// The current frame's result rows (the deformed-row analog of `SPI_tuptable`).
/// Empty if no result. Clones the rows out (the borrow is released).
#[must_use]
pub fn spi_tuptable_rows() -> Vec<SpiRow> {
    with_stack(|s| {
        s.last()
            .and_then(|c| c.result.as_ref())
            .map_or_else(Vec::new, |r| r.rows.clone())
    })
    .unwrap_or_default()
}

/// The current frame's result tuple descriptor, if any.
#[must_use]
pub fn spi_tuptable_desc() -> Option<TupleDesc> {
    with_stack(|s| s.last().and_then(|c| c.result.as_ref()).and_then(|r| r.tupdesc.clone())).flatten()
}

/// PG `SPI_execute` / `SPI_exec`: parse, plan, and execute `src`, collecting the
/// result rows into the current connection's tuptable. `tcount` caps the rows (0 =
/// all). Async (the executor reaches the buffer pool).
///
/// M9 reaches a single plannable SELECT (or DML). The read_only snapshot handling
/// is staged (runs under the active snapshot).
pub async fn spi_execute(
    shared: &Arc<SharedState>,
    src: &str,
    _read_only: bool,
    tcount: u64,
) {
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::backend::parser::analyze::parse_analyze_fixedparams_async;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    if with_stack(|s| s.is_empty()).unwrap_or(true) {
        crate::elog!(ERROR, "SPI_execute called without an SPI connection");
    }

    let mut parsetrees = crate::backend::parser::parser::raw_parser(src, RawParseMode::Default);
    if parsetrees.len() != 1 {
        unimplemented!("SPI_execute: multi-statement query strings deferred");
    }
    let Node::RawStmt(raw) = parsetrees.remove(0) else {
        unreachable!("raw_parser yields RawStmt nodes");
    };
    let raw: RawStmt = *raw;

    let analyzed = parse_analyze_fixedparams_async(shared, &raw, src, &[], 0).await;
    let mut query = if matches!(
        analyzed.commandType,
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
    ) {
        *analyzed
    } else {
        let mut rewritten = query_rewrite(*analyzed);
        rewritten.remove(0)
    };
    if query.commandType == CmdType::UTILITY {
        unimplemented!("SPI_execute: utility statement deferred");
    }
    let plan = standard_planner(&mut query, src, 0, None);

    run_plan_into_spi_result(shared, &plan, src, None, tcount).await;
}

/// PG `SPI_prepare`: parse + analyze `src` (with the given argument types) into a
/// `CachedPlanSource`, returned to the caller (the SPI plan handle). Async
/// (analysis may open relations).
pub async fn spi_prepare(
    shared: &Arc<SharedState>,
    src: &str,
    arg_types: &[crate::postgres_ext::Oid],
) -> Box<CachedPlanSource> {
    use crate::backend::parser::analyze::parse_analyze_varparams_async;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::backend::utils::cache::plancache::{CompleteCachedPlan, CreateCachedPlan};
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    let mut parsetrees = crate::backend::parser::parser::raw_parser(src, RawParseMode::Default);
    let Node::RawStmt(raw) = parsetrees.remove(0) else {
        unreachable!("raw_parser yields RawStmt nodes");
    };
    let raw: RawStmt = *raw;
    let inner = raw
        .stmt
        .clone()
        .unwrap_or_else(|| unreachable!("SPI_prepare: non-empty statement"));
    let command_tag = crate::backend::tcop::utility::create_command_tag(&inner);
    let mut src_plan = CreateCachedPlan(raw.clone(), src, command_tag);

    let mut resolved_types = arg_types.to_vec();
    let analyzed = parse_analyze_varparams_async(shared, &raw, src, &mut resolved_types).await;
    let query_list = if matches!(
        analyzed.commandType,
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
    ) {
        vec![*analyzed]
    } else {
        query_rewrite(*analyzed)
    };
    let num_params = i32::try_from(resolved_types.len()).unwrap_or(0);
    CompleteCachedPlan(&mut src_plan, query_list, &resolved_types, num_params, None, 0, true);
    src_plan
}

/// PG `SPI_execute_plan`: run a prepared SPI plan with bound parameters, collecting
/// the result into the current connection's tuptable. Async.
pub async fn spi_execute_plan(
    shared: &Arc<SharedState>,
    plan: &mut CachedPlanSource,
    params: Option<&ParamListInfoData>,
    _read_only: bool,
    tcount: u64,
) {
    if with_stack(|s| s.is_empty()).unwrap_or(true) {
        crate::elog!(ERROR, "SPI_execute_plan called without an SPI connection");
    }
    let cplan = crate::backend::utils::cache::plancache::GetCachedPlan(plan, params, None);
    let planned = cplan
        .stmt_list
        .first()
        .cloned()
        .unwrap_or_else(|| unreachable!("a complete SPI plan yields a PlannedStmt"));
    run_plan_into_spi_result(shared, &planned, &plan.query_string, params, tcount).await;
}

/// Run a planned statement into the current SPI connection's result (a deformed-
/// row tuptable), updating `SPI_processed`.
async fn run_plan_into_spi_result(
    shared: &Arc<SharedState>,
    plan: &crate::nodes::plannodes::PlannedStmt,
    query_string: &str,
    params: Option<&ParamListInfoData>,
    tcount: u64,
) {
    use crate::backend::access::transam::xact::{
        CommandCounterIncrement, GetCurrentCommandId, StartTransactionCommand,
    };
    use crate::backend::utils::sort::tuplestore::{
        tuplestore_advance, tuplestore_gettupleslot, tuplestore_rescan,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };

    // Ensure a transaction + active snapshot for the run. SPI normally inherits the
    // caller's transaction; a standalone call (PL/tests) opens one. Pushing a fresh
    // active snapshot (curcid = current command id) mirrors exec_simple_query.
    StartTransactionCommand(shared).await;
    CommandCounterIncrement();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        std::sync::Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);

    // Materialize into a store (reuses the shared cursor materialization).
    // `es_processed` is the executor's affected-row count: rows sent for a
    // SELECT/RETURNING, rows modified for a DML without RETURNING.
    let (mut store, tupdesc, es_processed) =
        crate::backend::tcop::pquery::run_plan_into_store(shared, plan, query_string, params).await;
    tuplestore_rescan(&mut store);

    PopActiveSnapshot();

    // Drain the store into deformed rows (honoring tcount; 0 = all).
    let mut rows: Vec<SpiRow> = Vec::new();
    let mut slot = crate::backend::executor::execTuples::make_single_tuple_table_slot(
        tupdesc.clone(),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );
    loop {
        if tcount != 0 && rows.len() as u64 >= tcount {
            break;
        }
        if !tuplestore_gettupleslot(&mut store, true, false, &mut slot) {
            break;
        }
        let n = slot.nvalid.max(0) as usize;
        rows.push(SpiRow {
            values: slot.values[..n].to_vec(),
            isnull: slot.isnull[..n].to_vec(),
        });
    }
    let _ = tuplestore_advance; // (kept available for skip-based variants)

    // PG `SPI_processed`: rows in the tuptable for SELECT/RETURNING, else the
    // ModifyTable affected-row count (a DML without RETURNING produces no result
    // rows, so use `es_processed`).
    let processed = if rows.is_empty() { es_processed } else { rows.len() as u64 };
    with_stack(|s| {
        if let Some(top) = s.last_mut() {
            top.processed = processed;
            top.result = Some(SpiResult { tupdesc, rows });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::tcop::postgres::bootstrap_then;
    use crate::postgres::DatumGetInt32;
    use crate::shared_state::SharedStateConfig;

    fn new_shared() -> Arc<SharedState> {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-spi-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    /// SPI_connect; SPI_execute("SELECT 1"); assert processed/tuptable; SPI_finish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spi_connect_execute_select_finish() {
        let shared = new_shared();
        bootstrap_then(shared, |shared| async move {
            spi_scope_async(async {
                spi_connect();
                spi_execute(&shared, "SELECT 1", true, 0).await;
                assert_eq!(spi_processed(), 1, "SELECT 1 produces one row");
                let rows = spi_tuptable_rows();
                assert_eq!(rows.len(), 1);
                assert_eq!(DatumGetInt32(rows[0].values[0]), 1, "the row value is 1");
                spi_finish();
            })
            .await;
        })
        .await;
    }

    /// Run a CREATE TABLE / index utility statement through the utility path, in the
    /// current transaction, so a later SPI DML sees the new relation.
    async fn create_via_utility(shared: &Arc<SharedState>, src: &str) {
        use crate::nodes::nodes::Node;
        use crate::nodes::parsenodes::RawStmt;
        use crate::parser::parser::RawParseMode;
        let mut parsetrees = crate::backend::parser::parser::raw_parser(src, RawParseMode::Default);
        let Node::RawStmt(raw) = parsetrees.remove(0) else { unreachable!() };
        let raw: RawStmt = *raw;
        let analyzed = crate::backend::parser::analyze::parse_analyze_fixedparams_async(
            shared, &raw, src, &[], 0,
        )
        .await;
        let plan = crate::backend::tcop::postgres::wrap_utility_stmt(&analyzed);
        let mut dest = crate::backend::tcop::dest::create_dest_receiver(
            crate::tcop::dest::CommandDest::DestNone,
        );
        crate::backend::tcop::utility::process_utility(
            shared,
            &plan,
            src,
            crate::tcop::utility::ProcessUtilityContext::Toplevel,
            dest.as_mut(),
            None,
        )
        .await;
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    }

    /// Run one SPI statement and commit it (SPI's StartTransactionCommand opens a
    /// fresh autocommit transaction per call in the M9 standalone path).
    async fn spi_exec_committed(shared: &Arc<SharedState>, src: &str) {
        use crate::backend::access::transam::xact::{
            CommitTransactionCommand, GetTopTransactionIdIfAny,
        };
        spi_execute(shared, src, false, 0).await;
        let committed = GetTopTransactionIdIfAny();
        CommitTransactionCommand(shared).await;
        crate::backend::tcop::postgres::publish_committed_xid(shared, committed);
    }

    /// SPI DML without RETURNING sets SPI_processed to the rows-modified count
    /// (es_processed), not 0. CREATE a table, then INSERT two rows and UPDATE them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spi_dml_sets_processed_to_affected_count() {
        use crate::backend::access::transam::xact::{
            CommitTransactionCommand, GetTopTransactionIdIfAny, StartTransactionCommand,
        };
        use crate::backend::utils::time::snapmgr::{
            GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
        };
        let shared = new_shared();
        bootstrap_then(shared, |shared| async move {
            // CREATE TABLE in its own autocommit transaction, then commit + publish so
            // the SPI calls (which open their own transactions) see the committed table.
            StartTransactionCommand(&shared).await;
            PushActiveSnapshot(GetTransactionSnapshot(&shared));
            create_via_utility(&shared, "CREATE TABLE spi_dml (a int)").await;
            PopActiveSnapshot();
            let committed = GetTopTransactionIdIfAny();
            CommitTransactionCommand(&shared).await;
            crate::backend::tcop::postgres::publish_committed_xid(&shared, committed);

            spi_scope_async(async {
                spi_connect();
                // INSERT one row without RETURNING -> SPI_processed == 1.
                spi_exec_committed(&shared, "INSERT INTO spi_dml VALUES (1)").await;
                assert_eq!(spi_processed(), 1, "INSERT of 1 row reports 1 modified");
                assert!(spi_tuptable_rows().is_empty(), "no RETURNING -> no result rows");
                spi_exec_committed(&shared, "INSERT INTO spi_dml VALUES (2)").await;
                assert_eq!(spi_processed(), 1, "second INSERT reports 1 modified");

                // UPDATE both rows without RETURNING -> SPI_processed == 2.
                spi_exec_committed(&shared, "UPDATE spi_dml SET a = a + 10").await;
                assert_eq!(spi_processed(), 2, "UPDATE of 2 rows reports 2 modified");
                assert!(spi_tuptable_rows().is_empty());

                spi_finish();
            })
            .await;
        })
        .await;
    }
}
