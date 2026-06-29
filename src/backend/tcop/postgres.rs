//! The POSTGRES backend command loop -- the "traffic cop". Translated from backend/tcop/postgres.c.
//!
//! This is the main module of the backend: once a connection's identity and
//! database are established, control enters the command loop, which reads a
//! message from the frontend, dispatches it by message type (simple query;
//! the extended-protocol Parse/Bind/Describe/Execute/Close; function call;
//! Sync/Flush; Terminate), and replies, announcing ReadyForQuery whenever the
//! session falls idle. The principal entry point is [`postgres_main`]; the other
//! exported routine is [`process_interrupts`], the deferred-interrupt service
//! routine invoked from `CHECK_FOR_INTERRUPTS` to act on signals (cancel, die,
//! the various idle/transaction timeouts) at the next safe point.
//!
//! In PostgreSQL the backend is a forked child whose lifetime error recovery is
//! anchored by two `sigsetjmp` points; PepperDB runs each backend as a tokio
//! task instead. There is no outer `setjmp`: an `elog(ERROR)` is a panic carrying
//! `ErrorData`, the per-command recovery point is a `catch_unwind` wrapping the
//! read-and-dispatch of one command, and a caught ERROR aborts the current
//! command and resumes the loop, while a FATAL (or any non-`ErrorData` bug panic)
//! is re-raised so it reaches the task boundary and ends the backend.
//!
//! [`process_interrupts`] is fully realized against the per-task signal slot
//! ([`ProcSignalSlot`]): it is synchronous, never awaits, and only reads and
//! clears atomic flags before panicking with the appropriate ERROR or FATAL.
//! With no slot in scope (an auxiliary task, or a test without a slot) it is a
//! no-op. The command loop's wire transport (the `pq_*` send/recv helpers) and
//! the parser, planner, and executor it dispatches into are not yet implemented;
//! the loop is faithful to the C control flow and runs up to the point where it
//! calls into one of those subsystems.

use crate::utils::rel::RelationData;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::net::TcpStream;

use crate::shared_state::SharedState;
use crate::backend::libpq::pqcomm::{self as pq, PqComm};
use crate::backend::storage::ipc::procsignal::{self, ProcSignalSlot};
use crate::libpq::libpq::PQ_LARGE_MESSAGE_LIMIT;
use crate::libpq::protocol::{
    PQMSG_BIND, PQMSG_CLOSE, PQMSG_COPY_DATA, PQMSG_COPY_DONE, PQMSG_COPY_FAIL, PQMSG_DESCRIBE,
    PQMSG_EXECUTE, PQMSG_FLUSH, PQMSG_FUNCTION_CALL, PQMSG_PARSE, PQMSG_QUERY, PQMSG_SYNC,
    PQMSG_TERMINATE,
};
use crate::miscadmin::{interrupts_can_be_processed, BackendType};
use crate::tcop::cmdtaglist::CommandTag;
use crate::tcop::dest::CommandDest;
use crate::utils::errcodes::{
    ERRCODE_ADMIN_SHUTDOWN, ERRCODE_CONNECTION_FAILURE, ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    ERRCODE_IDLE_SESSION_TIMEOUT, ERRCODE_QUERY_CANCELED,
};

/// Where the backend sends query results. PG `whereToSendOutput`; a normal client
/// backend always uses `DestRemote` (M1 has no standalone / replication mode).
const WHERE_TO_SEND_OUTPUT: CommandDest = CommandDest::DestRemote;

// ---------------------------------------------------------------------------
// PostgresMain -- the backend command loop.
// ---------------------------------------------------------------------------

/// PG `PostgresMain`. Installs the connection's pqcomm layer over `stream`,
/// announces readiness, then loops reading and dispatching client messages until
/// Terminate/EOF.
///
/// THE ASYNC BOUNDARY (rules.md s5): the command loop is `async` -- it awaits the
/// wire read (`pq_getmessage`) and the flush (`pq_flush`). The per-command
/// pipeline it dispatches (parse/analyze/rewrite/plan/Portal/ExecutorRun/printtup)
/// is SYNCHRONOUS and sits inside the per-command `catch_unwind` recovery point;
/// the receiver appends each message to the send buffer with the SYNC
/// `pq_putmessage_sync` (never `.await`), and the loop flushes afterward. No lock
/// guard is held across any `.await` (the read/flush only touch the async socket
/// mutex inside pqcomm).
///
/// The whole loop runs inside `pqcomm::scope` (publishes the per-task `PqComm`,
/// PG's `MyProcPort`), the full per-task database scope stack established by
/// [`init_postgres`] (relcache / catcache / catalog-index / exec-relations /
/// resowner / xact / snapmgr / combocid / WAL insertion -- the connect-to-database
/// phase), so the pipeline can open relations and the executor can scan/modify
/// them.
///
/// The default database OID a connecting backend attaches to. PG resolves the
/// database name to its pg_database OID in InitPostgres; M2 has a single seeded
/// database at this fixed OID (matching the boot initdb), so the name is recorded
/// on the Session for diagnostics but every backend attaches here.
pub const DEFAULT_DATABASE_OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid(90000);

pub async fn postgres_main(
    stream: TcpStream,
    shared: Arc<SharedState>,
    dbname: String,
    username: String,
) {
    let _ = &username;

    // PG runs InitPostgres (auth + connect-to-database) here; backend_startup
    // already ran the identity slice (backend_task_init) and published the
    // Session, so we flip to normal processing mode and record the attached db.
    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);
    if let Some(s) = crate::session::try_current() {
        s.set_database_id(DEFAULT_DATABASE_OID);
        s.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        if !dbname.is_empty() {
            s.set_database_name(Some(dbname));
        }
    }

    // Install the wire transport (PG `pq_init` over MyProcPort), then run the
    // command loop inside the connect-to-database scope stack.
    //
    // DEEP-STACK NOTE (rules.md s5): the nested scope futures plus the per-command
    // SYNC pipeline locals make this future very large in debug. Each scope layer
    // is individually `Box::pin`-ed inside `init_postgres` (heap, not stack), and
    // the backend task itself is spawned with an enlarged stack (backend_startup),
    // so the real backend does not stack-overflow.
    let comm = Arc::new(PqComm::new(stream));
    let loop_fut = Box::pin(init_postgres(shared.clone(), command_loop(shared)));
    pq::scope(comm, loop_fut).await;
}

/// PG `InitPostgres` (the connect-to-database phase). Establish the full per-task
/// scope stack a backend needs to read on-disk catalogs and run the executor, then
/// drive `inner` inside it. Models `catalog/tests.rs::in_scopes`. The executor's
/// open range-table relations are no longer a task-local registry: each query's
/// command frame (`run_plan_over_wire`) opens them into owned `Arc`s and passes a
/// borrow into the executor (relation-ownership-plan step 5).
///
/// Each nested scope future is `Box::pin`-ed so the combined future lives on the
/// heap rather than one giant stack frame (the stack is shallow at each layer).
async fn init_postgres<F>(shared: Arc<SharedState>, inner: F)
where
    F: std::future::Future<Output = ()>,
{
    use crate::backend::access::transam::xact::xact_scope;
    use crate::backend::access::transam::xloginsert::with_insertion;
    use crate::backend::catalog::indexing::scope_async as catalog_index_scope;
    use crate::backend::utils::cache::catcache::scope_async as catcache_scope;
    use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
    use crate::backend::utils::resowner::resowner;
    use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

    use crate::backend::storage::buffer::buf_init::with_private_refcount;

    let _ = &shared;
    let owner = resowner::ResourceOwner::create(None, "backend top-level");

    // RelationCacheInitializePhase3 (local-catalog half): nail this task's relcache
    // with the formrdesc catalogs (pg_class/pg_attribute/pg_proc/pg_type) so the
    // catalog caches can read the on-disk catalogs. The relcache is per-task
    // (task_local), so every backend re-nails inside its own relcache scope (PG runs
    // Phase3 in InitPostgres). Must be inside the relcache scope, before `inner`.
    let inner = async {
        crate::backend::utils::cache::relcache::relation_cache_initialize_phase3();
        inner.await;
    };

    // Establish this task's PrivateRefCount map (PG's per-backend pin cache). On the
    // multi-thread runtime a backend/bootstrap task migrates between workers across
    // `.await`s; the buffer pin bookkeeping must be task_local so a pin held across a
    // suspension point follows the task (a thread_local fallback would strand half
    // the pins on the origin worker -> the buffer-pin assertions trip).
    let inner = with_private_refcount(|| inner);

    let body = Box::pin(catalog_index_scope(Box::pin(inner)));
    let body = Box::pin(relcache_scope(body));
    let body = Box::pin(catcache_scope(body));
    let body = Box::pin(with_insertion(body));
    let body = Box::pin(combocid_scope(body));
    let body = Box::pin(snapmgr_scope(body));
    let body = Box::pin(xact_scope(body));
    resowner::scope(owner, body).await;
}

/// initdb at server boot (PG's `initdb` / bootstrap mode, run once before any
/// backend connects). Establish a boot session at the default database OID + the
/// connect-to-database scope stack, then run `bootstrap_catalogs` (step-15 initdb)
/// inside a committed transaction so pg_type etc. exist on disk (and the seeded
/// xids are committed in clog) before any backend's snapshot reads them.
///
/// The supervisor calls this exactly once at startup. Boxed/pinned for the deep
/// scope stack, like the backend path.
pub async fn bootstrap_cluster(shared: Arc<SharedState>) {
    let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
    sess.set_database_id(DEFAULT_DATABASE_OID);
    sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);

    let run = Box::pin(init_postgres(shared.clone(), Box::pin(do_initdb(shared.clone()))));
    crate::session::scope(sess, run).await;
}

/// Test-only: run `initdb` (warming the syscaches, like `bootstrap_cluster`) and
/// then `body`, all inside ONE backend session + relcache/catcache scope stack, so
/// the warm catcache (which is task-local) is live for `body`. Lets node-level
/// executor tests resolve catalog lookups (e.g. the AGGFNOID syscache) without the
/// full supervisor/wire harness. Boxed for the deep scope stack.
#[cfg(test)]
pub async fn bootstrap_then<F, Fut, T>(shared: Arc<SharedState>, body: F) -> T
where
    F: FnOnce(Arc<SharedState>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + 'static,
    T: Send + 'static,
{
    let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
    sess.set_database_id(DEFAULT_DATABASE_OID);
    sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);

    let out: Arc<std::sync::Mutex<Option<T>>> = Arc::new(std::sync::Mutex::new(None));
    let out2 = Arc::clone(&out);
    let shared2 = shared.clone();
    let inner = async move {
        do_initdb(shared.clone()).await;
        let r = body(shared).await;
        *out2.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(r);
    };
    let run = Box::pin(init_postgres(shared2, Box::pin(inner)));
    crate::session::scope(sess, run).await;
    let r = out.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
    r.unwrap_or_else(|| unreachable!("bootstrap_then body produced a result"))
}

/// The initdb body, run inside the boot session + connect-to-database scopes:
/// open a transaction, push the active snapshot the index build reads, seed the
/// catalogs, then commit so the rows are durable + committed for later backends.
async fn do_initdb(shared: Arc<SharedState>) {
    use crate::backend::access::transam::xact::{
        CommandCounterIncrement, CommitTransactionCommand, GetCurrentCommandId,
        GetCurrentTransactionIdIfAny, StartTransactionCommand,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, InvalidateCatalogSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };

    StartTransactionCommand(&shared).await;
    let mut snap = GetTransactionSnapshot(&shared);
    if let Some(s) = snap.as_mut() {
        Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);

    crate::backend::bootstrap::bootstrap::bootstrap_catalogs(&shared).await;

    CommandCounterIncrement();
    InvalidateCatalogSnapshot();
    PopActiveSnapshot();

    let committed = GetCurrentTransactionIdIfAny();
    CommitTransactionCommand(&shared).await;
    // Make the seeded rows visible to later backends' snapshots: advance the
    // shared latestCompletedXid past the bootstrap xid (see publish_committed_xid).
    publish_committed_xid(&shared, committed);
}

/// The backend message loop proper (PG `PostgresMain`'s `for (;;)`), run inside
/// the pqcomm + connect-to-database scopes. Async (awaits the wire read + flush,
/// and the per-command pipeline reaches the buffer pool / WAL).
async fn command_loop(shared: Arc<SharedState>) {
    use futures_util::FutureExt;
    // Send this backend's cancellation key to the frontend (BackendKeyData).
    send_backend_key_data().await;

    let mut send_ready_for_query = true;
    loop {
        // (1) If idle, tell the frontend we're ready (ReadyForQuery flushes).
        if send_ready_for_query {
            crate::backend::tcop::dest::ready_for_query(WHERE_TO_SEND_OUTPUT).await;
            send_ready_for_query = false;
        }

        // (2) A query-cancel arriving while we block on the read is suppressed:
        // ProcessInterrupts treats it as a no-op while DoingCommandRead.
        set_doing_command_read(true);

        // (3) Read one command from the wire (ASYNC -- the only blocking point).
        let read = read_command().await;

        // (5) Service interrupts that arrived while reading, before clearing
        // DoingCommandRead (an idle cancel is reset, not thrown).
        crate::miscadmin::check_for_interrupts();
        set_doing_command_read(false);

        // Terminate or EOF: the frontend is closing the socket. Normal exit.
        let Some((firstchar, body)) = read else {
            return;
        };

        // (7) Per-command recovery point (error.md s2.2, boundary 2 -- PG's
        // top-level sigsetjmp). The dispatch is now ASYNC (the pipeline reaches the
        // buffer pool / WAL); wrap the dispatch future in `FutureExt::catch_unwind`
        // so an ERROR (a panic carrying ErrorData) is recovered HERE, backend-local,
        // and the loop continues. FATAL / non-ErrorData bug-panics resume to the
        // task boundary. No lock guard is held across any `.await` (the scope_async
        // helpers manage borrow lifetimes; the dispatch borrows, copies, drops, then
        // awaits).
        let outcome = std::panic::AssertUnwindSafe(dispatch_command(&shared, firstchar, &body))
            .catch_unwind()
            .await;

        match outcome {
            Ok(CommandResult::Continue { ready }) => {
                send_ready_for_query |= ready;
                // Flush whatever the dispatch buffered (RowDescription/DataRow/
                // CommandComplete). If a ReadyForQuery follows it flushes too, but
                // flushing here keeps extended-protocol replies prompt.
                let _ = pq::pq_flush().await;
            }
            Ok(CommandResult::Done) => return,
            Err(payload) => match payload.downcast::<crate::utils::elog::ErrorData>() {
                Ok(edata) if edata.elevel < crate::utils::elog::FATAL => {
                    recover_from_error(&edata);
                    // Roll back the aborted command's transaction (autocommit): the
                    // recovery handler is sync, AbortCurrentTransaction is async, so
                    // drive it here after the catch (no future held across the catch).
                    crate::backend::access::transam::xact::AbortCurrentTransaction(&shared).await;
                    // Flush the buffered ErrorResponse, then announce idle.
                    let _ = pq::pq_flush().await;
                    send_ready_for_query = true;
                }
                Ok(edata) => std::panic::resume_unwind(edata), // FATAL
                Err(other) => std::panic::resume_unwind(other), // bug-panic
            },
        }
    }
}

/// What processing one client command resolved to.
enum CommandResult {
    /// Keep looping; `ready` requests a ReadyForQuery before the next read.
    Continue { ready: bool },
    /// The frontend closed (Terminate / EOF): exit the command loop.
    Done,
}

/// Dispatch one already-read client command by message type (step 7 of
/// `PostgresMain`). ASYNC: the simple-Query pipeline reaches the buffer pool / WAL.
/// The whole unit sits inside the per-command `catch_unwind` (the loop wraps the
/// returned future). An `elog(ERROR/FATAL)` raised here unwinds as a panic. The
/// simple-Query / DestRemote path is COMPLETE; the extended-protocol arms are grow
/// guards (rules.md s4).
async fn dispatch_command(shared: &Arc<SharedState>, firstchar: u8, body: &[u8]) -> CommandResult {
    match firstchar {
        PQMSG_QUERY => {
            // Body is the null-terminated query string.
            let query_string = cstr_body(body);
            exec_simple_query(shared, query_string).await;
            CommandResult::Continue { ready: true }
        }
        PQMSG_PARSE | PQMSG_BIND | PQMSG_EXECUTE | PQMSG_DESCRIBE | PQMSG_CLOSE => {
            unimplemented!("extended query protocol (Parse/Bind/Execute/Describe/Close) deferred")
        }
        PQMSG_FUNCTION_CALL => unimplemented!("fastpath function call deferred"),
        PQMSG_FLUSH => CommandResult::Continue { ready: false },
        PQMSG_SYNC => {
            finish_xact_command();
            CommandResult::Continue { ready: true }
        }
        // Terminate or EOF: the frontend is closing the socket. Normal exit.
        PQMSG_TERMINATE => CommandResult::Done,
        // COPY messages after a failed COPY: accept and ignore.
        PQMSG_COPY_DATA | PQMSG_COPY_DONE | PQMSG_COPY_FAIL => {
            CommandResult::Continue { ready: false }
        }
        other => {
            // PG: ereport(FATAL, ERRCODE_PROTOCOL_VIOLATION).
            crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("invalid frontend message type {other}"));
            });
            CommandResult::Continue { ready: false }
        }
    }
}

/// PG `exec_simple_query`: the real pipeline for a simple Query message.
///
/// StartTransactionCommand + push active snapshot -> raw_parser ->
/// parse_analyze_fixedparams_async + QueryRewrite -> standard_planner -> route by
/// command type:
///   - CMD_UTILITY (CREATE TABLE) -> ProcessUtility -> DefineRelation. Tag the
///     utility statement (CreateCommandTag, "CREATE TABLE").
///   - CMD_SELECT / CMD_INSERT -> open the plan's range-table relations, register
///     them in the per-task exec-relations registry, drive ExecutorStart/Run/End
///     against a DestRemote printtup receiver (SELECT yields RowDescription +
///     DataRow(s); INSERT yields a row count). Tags "SELECT <n>" / "INSERT 0 <n>".
///
/// Then EndCommand (CommandComplete) and CommitTransactionCommand (autocommit).
///
/// ASYNC: parse-analysis can open relations, ProcessUtility / the executor reach
/// the buffer pool + WAL. The receiver buffers each wire message synchronously via
/// `pq_putmessage_sync`; the command loop flushes after this returns (step 09).
///
/// STAGED (rules.md s4): empty query string -> NullCommand; multi-statement strings,
/// RETURNING, ON CONFLICT, extended protocol. An empty query is handled below.
async fn exec_simple_query(shared: &Arc<SharedState>, query_string: &str) {
    use crate::backend::parser::analyze::parse_analyze_fixedparams_async;
    use crate::backend::parser::parser::raw_parser;
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    let dest = WHERE_TO_SEND_OUTPUT;

    // start_xact_command(): open the per-statement transaction (autocommit) and push
    // the active snapshot the analyze/plan/executor read under (mirrors PG's
    // start_xact_command + the portal snapshot; here it bounds the whole statement).
    start_xact_command_async(shared).await;
    push_statement_snapshot(shared);

    // pg_parse_query: raw parse.
    let mut parsetrees = raw_parser(query_string, RawParseMode::Default);

    // Empty query string: tell the frontend and finish.
    if parsetrees.is_empty() {
        crate::backend::utils::time::snapmgr::PopActiveSnapshot();
        finish_xact_command_async(shared).await;
        crate::backend::tcop::dest::null_command(dest);
        return;
    }

    // M2 handles a single statement; multi-statement strings grow.
    if parsetrees.len() != 1 {
        unimplemented!("exec_simple_query: multi-statement query strings deferred");
    }
    let Node::RawStmt(raw) = parsetrees.remove(0) else {
        unreachable!("raw_parser yields RawStmt nodes");
    };
    let raw: RawStmt = *raw;

    crate::backend::tcop::dest::begin_command(CommandTag::Unknown, dest);

    // If we got a cancel signal in parsing, quit.
    crate::miscadmin::check_for_interrupts();

    // pg_analyze_and_rewrite_fixedparams: parse analysis (ASYNC -- opens relations
    // for SELECT/INSERT) + rewrite. The rewriter's INSERT/UPDATE/DELETE target-list
    // rewriting + rule firing is staged (rewriteHandler.rs s4); M2 has no rules or
    // views and `transform_insert_stmt` already builds a complete, attno-ordered
    // INSERT targetlist, so a plain INSERT is passed straight to the planner without
    // the rewrite pass. SELECT (and UTILITY) go through QueryRewrite as usual.
    let analyzed = parse_analyze_fixedparams_async(shared, &raw, query_string, &[], 0).await;
    let mut query = if analyzed.commandType == CmdType::INSERT {
        *analyzed
    } else {
        let mut rewritten = query_rewrite(*analyzed);
        if rewritten.len() != 1 {
            unimplemented!("exec_simple_query: query rewrite producing multiple queries deferred");
        }
        rewritten.remove(0)
    };

    // pg_plan_queries: plan. A utility Query (commandType == UTILITY) is NOT run
    // through the planner -- PG wraps its utilityStmt in a trivial CMD_UTILITY
    // PlannedStmt; only a plannable query reaches standard_planner.
    let plan = if query.commandType == CmdType::UTILITY {
        wrap_utility_stmt(&query)
    } else {
        standard_planner(&mut query, query_string, 0, None)
    };

    crate::miscadmin::check_for_interrupts();

    // The command tag for the completion message.
    let command_tag = command_tag_for(&plan);
    let mut qc = crate::tcop::cmdtag::QueryCompletion { command_tag, nprocessed: 0 };

    match plan.command_type {
        CmdType::UTILITY => {
            // PortalRunUtility -> ProcessUtility -> DefineRelation. The utility path
            // reaches the catalog/heap create (async).
            let mut receiver = crate::backend::tcop::dest::create_dest_receiver(dest);
            crate::backend::tcop::utility::process_utility(
                shared,
                &plan,
                query_string,
                crate::tcop::utility::ProcessUtilityContext::Toplevel,
                receiver.as_mut(),
                Some(&mut qc),
            )
            .await;
        }
        CmdType::SELECT | CmdType::INSERT => {
            run_plan_over_wire(shared, &plan, query_string, command_tag, dest, &mut qc).await;
        }
        other => unimplemented!("exec_simple_query: command type {other:?} deferred"),
    }

    crate::backend::utils::time::snapmgr::PopActiveSnapshot();

    // CommandComplete, then commit the autocommit transaction. Capture the xid the
    // statement may have assigned (a writing INSERT/CREATE) before committing, then
    // advance the shared latestCompletedXid so the NEXT statement's snapshot sees it.
    crate::backend::tcop::dest::end_command(&qc, dest, false);
    let committed = crate::backend::access::transam::xact::GetCurrentTransactionIdIfAny();
    finish_xact_command_async(shared).await;
    publish_committed_xid(shared, committed);
}

/// Advance the shared `latestCompletedXid` past a just-committed xid so later
/// transactions' snapshots treat it as completed (and its rows as visible).
///
/// PG does this inside `ProcArrayEndTransaction` under ProcArrayLock, keyed off the
/// committing backend's PGPROC. The single-process backend tasks here do not yet
/// register a PGPROC (InitProcess is deferred -- postinit.rs), so that path no-ops;
/// advancing the shared variable cache directly reproduces the visible effect that
/// makes committed work observable across the autocommit transaction boundary. A
/// read-only statement assigns no xid (`None`) and needs no advance.
fn publish_committed_xid(shared: &Arc<SharedState>, committed: Option<crate::c::TransactionId>) {
    use crate::access::transam::{
        full_transaction_id_from_u64, u64_from_full_transaction_id, xid_from_full_transaction_id,
    };
    let Some(xid) = committed.filter(|x| x.is_valid()) else { return };
    shared.variable_cache().with(|v| {
        // Same rule as MaintainLatestCompletedXid: bump to `xid` if it is newer,
        // lifting the 32-bit xid into the cache's current epoch (FullXidRelativeTo).
        let cur = v.latest_completed_xid;
        if !xid_from_full_transaction_id(cur).precedes(xid) {
            return;
        }
        let rel_xid = xid_from_full_transaction_id(cur);
        let delta = xid.0.wrapping_sub(rel_xid.0) as i32;
        v.latest_completed_xid = full_transaction_id_from_u64(
            u64_from_full_transaction_id(cur).wrapping_add(i64::from(delta) as u64),
        );
    });
}

/// Drive a planned SELECT / INSERT to completion over the wire: open the plan's
/// range-table relations, register them in the per-task exec-relations registry
/// (the es_relations equivalent), then run ExecutorStart/Run/Finish/End against a
/// DestRemote printtup receiver. Relations opened here are closed before returning.
async fn run_plan_over_wire(
    shared: &Arc<SharedState>,
    plan: &crate::nodes::plannodes::PlannedStmt,
    query_string: &str,
    command_tag: CommandTag,
    dest: CommandDest,
    qc: &mut crate::tcop::cmdtag::QueryCompletion,
) {
    use crate::access::sdir::ScanDirection;
    use crate::backend::executor::execMain::{
        standard_executor_end, standard_executor_finish, standard_executor_run,
        standard_executor_start,
    };

    // Open every RTE_RELATION in the range table (PG opens them before InitPlan,
    // under the right locks). `opened` OWNS the `Arc<RelationData>`s -- it is the
    // `'rel` ownership root, a stack binding that strictly encloses the executor run
    // below (relation-ownership-plan §1.2). The executor BORROWS from it.
    let opened = open_range_table_relations(shared, plan).await;

    // Build the borrowed range-table indexed by RT index (PG `es_relations`): slot
    // `rti - 1` is `Some(&*arc)` for an opened RELATION RTE, `None` otherwise.
    let max_rti = opened.iter().map(|(rti, _)| *rti).max().unwrap_or(0);
    let mut range_table_rels: Vec<Option<&crate::utils::rel::RelationData>> = vec![None; max_rti];
    for (rti, rel) in &opened {
        range_table_rels[*rti - 1] = Some(&**rel);
    }

    // Build the QueryDesc with the active snapshot + a DestRemote printtup receiver
    // (text format for every column in simple Query mode).
    let mut receiver = crate::backend::tcop::dest::create_dest_receiver(dest);
    if dest == CommandDest::DestRemote {
        crate::backend::access::common::printtup::set_remote_dest_receiver_params(
            receiver.as_mut(),
            &[],
        );
    }
    let snap = crate::backend::utils::time::snapmgr::GetActiveSnapshot();
    // The snapshot `Arc` is owned here (the command frame), so the scan can borrow
    // `&*snap` across its `.await`s.
    let snapshot_ref = snap.as_deref();
    let mut query_desc = make_query_desc(plan, query_string, snap.clone(), receiver);

    standard_executor_start(&mut query_desc, &range_table_rels, snapshot_ref, 0);
    standard_executor_run(Some(shared), &mut query_desc, ScanDirection::Forward, 0).await;
    standard_executor_finish(&mut query_desc);
    let processed = query_desc.estate.as_ref().map_or(0, |e| e.processed);
    qc.command_tag = command_tag;
    qc.nprocessed = processed;
    standard_executor_end(Some(shared), &mut query_desc);

    // Drop the borrows before closing the owners.
    drop(query_desc);
    drop(range_table_rels);

    // Close the relations we opened (drop the relcache refcount).
    for (_rti, rel) in opened {
        crate::backend::utils::cache::relcache::relation_close(rel);
    }
}

/// Open the open `Relation` for each RTE_RELATION in the plan's range table, keyed
/// by RT index (1-based). Warms the relcache (async build from pg_class/pg_attribute
/// for a user table), sets the physical address (the relcache build leaves
/// rd_locator zeroed -- PG's RelationInitPhysicalAddr fills it from
/// reltablespace/relfilenode), and takes a refcount via RelationIdGetRelation.
async fn open_range_table_relations(
    shared: &Arc<SharedState>,
    plan: &crate::nodes::plannodes::PlannedStmt,
) -> Vec<(usize, Arc<crate::utils::rel::RelationData>)> {
    use crate::nodes::nodes::Node;
    use crate::nodes::parsenodes::RTEKind;

    let mut opened = Vec::new();
    for (i, rte_node) in plan.rtable.iter().enumerate() {
        let Node::RangeTblEntry(rte) = rte_node else {
            continue; // non-RTE placeholder (e.g. the const RTE_RESULT for SELECT 1)
        };
        if rte.rtekind != RTEKind::RELATION || rte.relid.0 == 0 {
            continue;
        }
        let rti = i + 1;
        let relid = rte.relid;

        // Warm the relcache (async heap scan of pg_class/pg_attribute; it fills the
        // physical address at build time), then take an open handle (an Arc clone).
        crate::backend::utils::cache::relcache::relation_build_desc(shared, relid).await;
        let rel = crate::backend::utils::cache::relcache::relation_id_get_relation(relid)
            .unwrap_or_else(|| unreachable!("relation {relid:?} just built into the relcache"));
        opened.push((rti, rel));
    }
    opened
}

/// Build a QueryDesc for the wire path: carries the active snapshot + the DestRemote
/// printtup receiver. (PG `CreateQueryDesc`.)
#[allow(deprecated)]
fn make_query_desc<'rel>(
    plan: &crate::nodes::plannodes::PlannedStmt,
    query_string: &str,
    snapshot: crate::utils::snapshot::Snapshot,
    dest: Box<dyn crate::tcop::dest::DestReceiver>,
) -> crate::executor::execdesc::QueryDesc<'rel> {
    crate::executor::execdesc::QueryDesc {
        operation: plan.command_type,
        plannedstmt: Some(Box::new(plan.clone())),
        sourceText: query_string.to_string(),
        snapshot: Some(Box::new(snapshot)),
        crosscheck_snapshot: None,
        dest: Some(dest),
        params: None,
        queryEnv: None,
        instrument_options: crate::executor::instrument::InstrumentOption::empty(),
        tupDesc: None,
        estate: None,
        planstate: None,
        already_executed: false,
        totaltime: None,
    }
}

/// PG `pg_plan_query`'s utility shortcut: wrap a CMD_UTILITY `Query`'s utilityStmt
/// in a trivial `PlannedStmt` (no plan tree, no range table) that ProcessUtility
/// consumes. The planner is never invoked for a utility statement.
fn wrap_utility_stmt(query: &crate::nodes::parsenodes::Query) -> crate::nodes::plannodes::PlannedStmt {
    use crate::nodes::nodes::CmdType;
    let utility_stmt = query
        .utilityStmt
        .clone()
        .unwrap_or_else(|| unreachable!("a CMD_UTILITY Query carries its utilityStmt"));
    crate::nodes::plannodes::PlannedStmt {
        command_type: CmdType::UTILITY,
        query_id: query.queryId,
        plan_id: 0,
        has_returning: false,
        has_modifying_cte: false,
        can_set_tag: query.canSetTag,
        transient_plan: false,
        depends_on_role: false,
        parallel_mode_needed: false,
        jit_flags: 0,
        // A utility PlannedStmt carries no plan tree; ProcessUtility dispatches on
        // utility_stmt, never plan_tree. Reuse the utilityStmt node as the unread
        // plan_tree slot (the Node enum has no unit/placeholder variant).
        plan_tree: utility_stmt.clone(),
        part_prune_infos: Vec::new(),
        rtable: Vec::new(),
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
        utility_stmt: Some(utility_stmt),
        stmt_location: query.stmt_location,
        stmt_len: query.stmt_len,
    }
}

/// Derive the completion command tag from a planned statement. SELECT derives
/// directly; a CMD_UTILITY plan defers to `CreateCommandTag` over its carried
/// utilityStmt (e.g. a `CreateStmt` -> "CREATE TABLE"); other plannable command
/// types grow with their statements.
fn command_tag_for(plan: &crate::nodes::plannodes::PlannedStmt) -> CommandTag {
    match plan.command_type {
        crate::nodes::nodes::CmdType::SELECT => CommandTag::Select,
        crate::nodes::nodes::CmdType::INSERT => CommandTag::Insert,
        crate::nodes::nodes::CmdType::UTILITY => {
            let stmt = plan.utility_stmt.as_ref().unwrap_or_else(|| {
                unreachable!("a CMD_UTILITY plan carries its utilityStmt")
            });
            crate::backend::tcop::utility::create_command_tag(stmt)
        }
        other => unimplemented!("command_tag_for: {other:?} deferred"),
    }
}

/// PG `start_xact_command`: open the per-statement (autocommit) transaction. The
/// statement-timeout arm grows with the timeout subsystem.
async fn start_xact_command_async(shared: &Arc<SharedState>) {
    crate::backend::access::transam::xact::StartTransactionCommand(shared).await;
}

/// PG `finish_xact_command`: commit the per-statement (autocommit) transaction.
async fn finish_xact_command_async(shared: &Arc<SharedState>) {
    crate::backend::access::transam::xact::CommitTransactionCommand(shared).await;
}

/// Sync `finish_xact_command` for the Sync-message path (extended protocol, M1
/// near-no-op): nothing to commit when no statement transaction is open.
fn finish_xact_command() {
    // Sync between extended-protocol commands: the autocommit transaction is opened
    // and committed per simple-Query statement, so there is nothing to do here yet.
}

/// Push the active snapshot the statement runs under (PG's portal/transaction
/// snapshot). Take the transaction snapshot with curcid set to the current command
/// id so the statement sees its own prior commands within the transaction.
fn push_statement_snapshot(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::GetCurrentCommandId;
    use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        std::sync::Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);
}

/// Recover from a backend-local ERROR caught at the per-command recovery point
/// (error.md s2.2-2.3). PG's top-level sigsetjmp handler runs AbortCurrentTransaction,
/// reports the error to the client, and resets per-task error state before looping.
///
/// NOTE: AbortCurrentTransaction (xact.c) is `async` and cannot be driven from this
/// sync recovery handler (it sits inside the per-command `catch_unwind`, which must
/// not hold a future across the catch). Wiring the (sub)transaction rollback in here
/// is a follow-up; for now the rollback step is a clearly-marked TODO so the
/// structural recovery point lands and is correct staging.
fn recover_from_error(edata: &crate::utils::elog::ErrorData) {
    // TODO(xact): run AbortCurrentTransaction / AtAbort_* here once a sync entry
    // (or a drive-the-future shim) exists; it is async today (xact.rs).
    abort_current_transaction_stub();

    // Report the error to the client + server log. send_message_to_frontend is a
    // deferred pq stub; report_recovered_error walks the enabled destinations.
    crate::utils::elog::report_recovered_error(edata);

    // Reset per-task error state so the next command starts clean.
    crate::utils::elog::flush_error_state();
}

/// Placeholder for the (sub)transaction rollback step of ERROR recovery. The real
/// `AbortCurrentTransaction` is async (xact.rs); see `recover_from_error`.
fn abort_current_transaction_stub() {
    // TODO(xact): AbortCurrentTransaction(&shared).await -- needs a sync bridge.
}

// --- Wire helpers over the per-task PqComm (rules.md s5) --------------------

/// PG `pq_putmessage(BackendKeyData, ...)`. Sent once after startup. The cancel
/// key payload (pid + MyCancelKey) is appended by backend_startup; M1 sends an
/// empty BackendKeyData body (the cancel handshake is exercised separately).
async fn send_backend_key_data() {
    let _ = pq::pq_putmessage(crate::libpq::protocol::PQMSG_BACKEND_KEY_DATA, &[]).await;
}

/// PG `ReadCommand`/`SocketBackend`: read one message (type byte + body) from the
/// wire. Returns `Some((firstchar, body))`, or `None` on EOF / protocol loss.
/// Async (the only blocking point in the loop).
async fn read_command() -> Option<(u8, Vec<u8>)> {
    pq::pq_startmsgread();
    let firstchar = pq::pq_getbyte().await;
    if firstchar == pq::EOF {
        return None;
    }
    let mut body = Vec::new();
    if pq::pq_getmessage(&mut body, PQ_LARGE_MESSAGE_LIMIT).await.is_err() {
        return None;
    }
    Some((firstchar as u8, body))
}

/// Interpret a message body as the null-terminated query string (PG simple Query
/// body is a single C string). Strips the trailing NUL; lossy for non-UTF-8.
fn cstr_body(body: &[u8]) -> &str {
    let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    std::str::from_utf8(&body[..end]).unwrap_or("")
}

/// PG `DoingCommandRead` setter. Per-task state on the Session; a no-op with no
/// Session in scope (slotless test).
fn set_doing_command_read(v: bool) {
    if let Some(s) = crate::session::try_current() {
        s.set_doing_command_read(v);
    }
}

/// PG `DoingCommandRead` reader. False with no Session in scope.
fn doing_command_read() -> bool {
    crate::session::try_current().is_some_and(|s| s.doing_command_read())
}

// ---------------------------------------------------------------------------
// ProcessInterrupts -- the real deferred-interrupt service routine.
// ---------------------------------------------------------------------------

/// PG `ProcessInterrupts`. Reads/clears the current task's [`ProcSignalSlot`]
/// flags and acts: ProcDie -> FATAL terminate; QueryCancel (when processable) ->
/// ERROR cancel; the timeout/connection-lost flags -> FATAL. SYNC -- never
/// `.await`s; only atomic flag ops and panics. With no current slot (aux task or
/// a test without a slot scope) it is a no-op.
///
/// `elog(ERROR)`/`elog(FATAL)` are panics carrying `ErrorData` (TODO(panic)),
/// caught by the task-boundary `catch_unwind`.
pub fn process_interrupts() {
    let Some(slot) = procsignal::try_current() else {
        // No slot in scope: nothing to service (aux task / slotless test).
        return;
    };

    // OK to accept any interrupts now? (INTERRUPTS_CAN_BE_PROCESSED gating, but
    // QueryCancelHoldoffCount is handled per-flag below to match postgres.c.)
    let f = &slot.flags;
    if !interrupts_can_be_processed_for_die() {
        // InterruptHoldoffCount/CritSectionCount held off: process nothing.
        return;
    }
    f.interrupt_pending.store(false, Ordering::Release);

    if f.proc_die_pending.swap(false, Ordering::AcqRel) {
        f.query_cancel_pending.store(false, Ordering::Release); // ProcDie trumps QueryCancel
        crate::storage::proc::LockErrorCleanup(); // cancel any pending lock wait before dying
        proc_die_fatal();
    }

    if f.client_connection_lost.swap(false, Ordering::AcqRel) {
        f.query_cancel_pending.store(false, Ordering::Release); // lost connection trumps cancel
        crate::storage::proc::LockErrorCleanup();
        crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg("connection to client lost");
        });
    }

    // Query cancel is held off while reading a message from the client (we'd
    // lose FE/BE sync). Re-arm interrupt_pending so it fires once reading ends.
    if f.query_cancel_pending.load(Ordering::Acquire) && !query_cancel_processable() {
        f.interrupt_pending.store(true, Ordering::Release);
    } else if f.query_cancel_pending.swap(false, Ordering::AcqRel) {
        // PG inspects lock/statement-timeout indicators here (deferred); the base
        // case is a user cancel request. A cancel arriving while idle (reading a
        // command from the client) is a no-op: clear the flag, send no error.
        if !doing_command_read() {
            crate::storage::proc::LockErrorCleanup();
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_QUERY_CANCELED)
                    .errmsg("canceling statement due to user request");
            });
        }
    }

    // Recovery-conflict interrupts: deferred (HandleRecoveryConflictInterrupt).

    if f.idle_in_transaction_session_timeout_pending.swap(false, Ordering::AcqRel) {
        // TODO: gate on the IdleInTransactionSessionTimeout GUC (deferred).
        crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT)
                .errmsg("terminating connection due to idle-in-transaction timeout");
        });
    }

    if f.transaction_timeout_pending.swap(false, Ordering::AcqRel) {
        // TODO: gate on the TransactionTimeout GUC; ERRCODE_TRANSACTION_TIMEOUT
        // (not yet in errcodes) -> ADMIN_SHUTDOWN placeholder. TODO(errcode).
        crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_ADMIN_SHUTDOWN)
                .errmsg("terminating connection due to transaction timeout");
        });
    }

    if f.idle_session_timeout_pending.swap(false, Ordering::AcqRel) {
        // TODO: gate on the IdleSessionTimeout GUC (deferred).
        crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_IDLE_SESSION_TIMEOUT)
                .errmsg("terminating connection due to idle-session timeout");
        });
    }

    // Stats-update timeout, ProcSignalBarrier, parallel/log-memory-context
    // messages: deferred subsystems. Clear the cheap ones so they don't spin.
    f.proc_signal_barrier_pending.store(false, Ordering::Release);
    f.log_memory_context_pending.store(false, Ordering::Release);
    f.idle_stats_update_timeout_pending.store(false, Ordering::Release);
    f.check_client_connection_pending.store(false, Ordering::Release);
}

/// The die-interrupt gate: `InterruptHoldoffCount == 0 && CritSectionCount == 0`.
/// (postgres.c tests these two at the top; QueryCancelHoldoffCount is handled
/// per-flag for the cancel case.)
fn interrupts_can_be_processed_for_die() -> bool {
    crate::miscadmin::interrupt_holdoff_count() == 0 && crate::miscadmin::crit_section_count() == 0
}

/// Whether a query-cancel may be thrown now (not while reading a client
/// message): `QueryCancelHoldoffCount == 0`. Plus the general processable gate.
fn query_cancel_processable() -> bool {
    crate::miscadmin::query_cancel_holdoff_count() == 0 && interrupts_can_be_processed()
}

/// ProcDie -> FATAL terminate. Backend type tunes the message (autovacuum / bg
/// worker variants are deferred); the base case is a client backend.
fn proc_die_fatal() {
    let bt = crate::session::try_current().map(|s| s.backend_type());
    let msg = match bt {
        Some(BackendType::AUTOVAC_WORKER) => "terminating autovacuum process due to administrator command",
        _ => "terminating connection due to administrator command",
    };
    crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_ADMIN_SHUTDOWN).errmsg(msg);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::ipc::procsignal::ProcSignal;
    use crate::miscadmin::BackendType;
    use crate::session::Session;
    use crate::storage::latch::Latch;
    use crate::utils::elog::ErrorData;
    use std::sync::Arc;

    fn with_slot<F: FnOnce()>(setup: impl FnOnce(&ProcSignalSlot), body: F) {
        // Build a slot and publish it as the task-local for the duration of body.
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (_key, slot) = reg.register(424242, b"k", latch);
        setup(&slot);
        // procsignal::scope is async; for a sync test we use the task-local
        // scope synchronously via a tiny runtime.
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            // process_interrupts now runs LockErrorCleanup (PG postgres.c:3310/
            // 3382/3450), which reads the LOCAL_LOCKS task-local a real backend
            // always sets; mirror that here.
            crate::storage::lock::local_lock_scope(procsignal::scope(slot, async {
                body();
            }))
            .await;
        });
    }

    /// Like `with_slot` but also publishes a fresh Session (needed once holdoff
    /// counters live on the Session). Both the slot and the session are scoped to
    /// `body`.
    fn with_slot_and_session<F: FnOnce()>(setup: impl FnOnce(&ProcSignalSlot), body: F) {
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (_key, slot) = reg.register(424242, b"k", latch);
        setup(&slot);
        let session = Arc::new(Session::new(BackendType::BACKEND));
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            crate::session::scope(session, async {
                crate::storage::lock::local_lock_scope(procsignal::scope(slot, async {
                    body();
                }))
                .await;
            })
            .await;
        });
    }

    #[test]
    fn query_cancel_panics_with_canceled_errcode() {
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_slot(
                |s| {
                    s.flags.query_cancel_pending.store(true, Ordering::Release);
                    s.flags.interrupt_pending.store(true, Ordering::Release);
                },
                process_interrupts,
            );
        }));
        let payload = caught.expect_err("query cancel must panic");
        let edata = payload
            .downcast_ref::<ErrorData>()
            .expect("panic payload is ErrorData");
        assert_eq!(edata.sqlerrcode, ERRCODE_QUERY_CANCELED);
    }

    #[test]
    fn query_cancel_while_idle_is_noop() {
        // DoingCommandRead set: an idle cancel must clear the flag, not panic.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_slot_and_session(
                |s| {
                    s.flags.query_cancel_pending.store(true, Ordering::Release);
                    s.flags.interrupt_pending.store(true, Ordering::Release);
                },
                || {
                    crate::session::current().set_doing_command_read(true);
                    process_interrupts(); // idle -- cancel suppressed, no panic
                },
            );
        }));
        assert!(caught.is_ok(), "idle query cancel must not throw");
    }

    #[test]
    fn proc_die_panics_fatal() {
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_slot(
                |s| {
                    s.flags.proc_die_pending.store(true, Ordering::Release);
                    s.flags.interrupt_pending.store(true, Ordering::Release);
                },
                process_interrupts,
            );
        }));
        let payload = caught.expect_err("proc die must panic");
        let edata = payload
            .downcast_ref::<ErrorData>()
            .expect("panic payload is ErrorData");
        assert_eq!(edata.elevel, crate::utils::elog::FATAL);
        assert_eq!(edata.sqlerrcode, ERRCODE_ADMIN_SHUTDOWN);
    }

    #[test]
    fn held_off_does_not_process() {
        // Inside a session + slot scope: with InterruptHoldoffCount > 0,
        // ProcessInterrupts must not panic (held off). After resume, it panics.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_slot_and_session(
                |s| {
                    s.flags.query_cancel_pending.store(true, Ordering::Release);
                    s.flags.interrupt_pending.store(true, Ordering::Release);
                },
                || {
                    crate::miscadmin::hold_interrupts();
                    process_interrupts(); // held off -- no panic
                    crate::miscadmin::resume_interrupts();
                    process_interrupts(); // now panics (query cancel)
                },
            );
        }));
        let payload = caught.expect_err("must panic after resume");
        let edata = payload
            .downcast_ref::<ErrorData>()
            .expect("panic payload is ErrorData");
        assert_eq!(edata.sqlerrcode, ERRCODE_QUERY_CANCELED);
    }

    #[test]
    fn holdoff_does_not_leak_across_sessions() {
        // Proves the per-task fix: a hold_interrupts in session A must be
        // invisible to a different session B.
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            // Session A: raise the holdoff and confirm it took effect locally.
            let a = Arc::new(Session::new(BackendType::BACKEND));
            crate::session::scope(a, async {
                crate::miscadmin::hold_interrupts();
                assert_eq!(crate::miscadmin::interrupt_holdoff_count(), 1);
                assert!(!crate::miscadmin::interrupts_can_be_processed());
            })
            .await;

            // Session B: a fresh session sees no holdoff from A.
            let b = Arc::new(Session::new(BackendType::BACKEND));
            crate::session::scope(b, async {
                assert_eq!(crate::miscadmin::interrupt_holdoff_count(), 0);
                assert!(crate::miscadmin::interrupts_can_be_processed());
            })
            .await;
        });
    }

    #[test]
    fn no_session_counters_are_zero_and_no_panic() {
        // No current Session: counters read 0, can-be-processed is true, and
        // hold_interrupts is a no-op (no panic).
        assert!(crate::session::try_current().is_none());
        assert_eq!(crate::miscadmin::interrupt_holdoff_count(), 0);
        assert!(crate::miscadmin::interrupts_can_be_processed());
        crate::miscadmin::hold_interrupts();
    }

    #[test]
    fn no_current_slot_is_noop() {
        // Outside any slot scope, must be a no-op (no panic).
        process_interrupts();
    }

    // --- per-command recovery point (error.md s2.2-2.4) ---

    use crate::utils::elog::{errstart, ERROR as ELEVEL_ERROR, FATAL as ELEVEL_FATAL};

    /// Mirror of the command loop's catch-and-classify around one iteration:
    /// recover an ERROR (continue the session), resume a FATAL or bug-panic.
    /// Returns true if the iteration was recovered (loop would continue), false
    /// if it should propagate to the task boundary (after re-raising).
    fn drive_one(body: impl FnOnce() + std::panic::UnwindSafe) -> std::thread::Result<bool> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
                Ok(()) => true,
                Err(payload) => match payload.downcast::<ErrorData>() {
                    Ok(edata) if edata.elevel < ELEVEL_FATAL => {
                        recover_from_error(&edata);
                        true
                    }
                    Ok(edata) => std::panic::resume_unwind(edata),
                    Err(other) => std::panic::resume_unwind(other),
                },
            }
        }))
    }

    #[test]
    fn error_is_recovered_backend_local() {
        crate::utils::elog::flush_error_state();
        let recovered = drive_one(|| {
            if let Some(mut e) = errstart(ELEVEL_ERROR, None) {
                e.errmsg("recoverable boom");
                #[allow(deprecated)]
                crate::backend::utils::error::elog::errfinish(e, "postgres.rs", 1, "test");
            }
        });
        assert!(recovered.expect("ERROR must be recovered, not resumed"));
        crate::utils::elog::flush_error_state();
    }

    #[test]
    fn fatal_resumes_to_task_boundary() {
        crate::utils::elog::flush_error_state();
        let result = drive_one(|| {
            if let Some(mut e) = errstart(ELEVEL_FATAL, None) {
                e.errmsg("connection unusable");
                #[allow(deprecated)]
                crate::backend::utils::error::elog::errfinish(e, "postgres.rs", 1, "test");
            }
        });
        let payload = result.expect_err("FATAL must propagate past the recovery point");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, ELEVEL_FATAL);
        crate::utils::elog::flush_error_state();
    }

    #[test]
    fn bug_panic_resumes_to_task_boundary() {
        crate::utils::elog::flush_error_state();
        let result = drive_one(|| panic!("not an ErrorData"));
        let payload = result.expect_err("a non-ErrorData bug-panic must propagate");
        assert!(payload.downcast_ref::<ErrorData>().is_none());
        crate::utils::elog::flush_error_state();
    }
}

// --- end-to-end wire tests (the M2 milestone + the M1 SELECT 1 regression) -----
//
// These drive the REAL backend over a socket: a tempdir cluster is initdb'd at
// supervisor boot, a TCP client connects with a v3 startup packet, then sends the
// simple-Query messages and we assert the decoded backend->frontend bytes.
#[cfg(test)]
mod wire_tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream as ClientStream;

    use crate::backend::postmaster::auxprocess::aux_test_serial;
    use crate::backend::postmaster::postmaster::start_supervisor;
    use crate::backend::tcop::backend_startup::test_hook;
    use crate::shared_state::SharedStateConfig;

    /// One decoded backend->frontend message: type byte + body (length stripped).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Msg {
        ty: u8,
        body: Vec<u8>,
    }

    /// Split a raw byte stream into typed, length-prefixed messages.
    fn decode(mut bytes: &[u8]) -> Vec<Msg> {
        let mut out = Vec::new();
        while bytes.len() >= 5 {
            let ty = bytes[0];
            let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
            if bytes.len() < 1 + len {
                break;
            }
            let body = bytes[5..5 + (len - 4)].to_vec();
            out.push(Msg { ty, body });
            bytes = &bytes[1 + len..];
        }
        out
    }

    /// Frame a frontend message (type + int32 len-incl-self + body).
    fn framed(ty: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![ty];
        v.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        v.extend_from_slice(body);
        v
    }

    /// Frame a startup packet (int32 len-incl-self + body, no type byte).
    fn framed_startup(body: &[u8]) -> Vec<u8> {
        let total = (body.len() + 4) as u32;
        let mut out = total.to_be_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    fn loopback_port0() -> SocketAddr {
        (std::net::Ipv4Addr::LOCALHOST, 0).into()
    }

    /// A SharedStateConfig pointing at a fresh per-test tempdir cluster directory.
    fn tempdir_config() -> SharedStateConfig {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-wire-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(&dir);
        SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        }
    }

    /// Read framed messages from the socket until `done` is satisfied by the decoded
    /// set so far (or a timeout fires).
    async fn read_until(
        client: &mut ClientStream,
        buf: &mut Vec<u8>,
        done: impl Fn(&[Msg]) -> bool,
    ) -> Vec<Msg> {
        let mut chunk = [0u8; 8192];
        loop {
            if done(&decode(buf)) {
                return decode(buf);
            }
            let n = tokio::time::timeout(Duration::from_secs(10), client.read(&mut chunk))
                .await
                .expect("backend response timed out")
                .expect("socket read");
            assert!(n != 0, "backend closed the socket unexpectedly");
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Connect a client, send the v3 startup packet, and consume the backend's
    /// startup reply up to (and including) the first ReadyForQuery ('Z').
    async fn connect_and_startup(addr: SocketAddr) -> (ClientStream, Vec<u8>) {
        let mut client = ClientStream::connect(addr).await.expect("connect");
        // protocol 0x00030000 + "user\0postgres\0database\0postgres\0\0".
        let mut body = 0x0003_0000u32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0postgres\0database\0postgres\0\0");
        client.write_all(&framed_startup(&body)).await.expect("write startup");
        client.flush().await.expect("flush startup");

        // The backend sends BackendKeyData('K') then the idle ReadyForQuery('Z').
        let mut buf = Vec::new();
        read_until(&mut client, &mut buf, |m| m.iter().any(|x| x.ty == b'Z')).await;
        (client, buf)
    }

    /// Send one simple Query and read its reply up to the terminating
    /// ReadyForQuery; returns ONLY the messages produced by this query (the new
    /// bytes appended after `already_seen`).
    async fn simple_query(client: &mut ClientStream, buf: &mut Vec<u8>, sql: &str) -> Vec<Msg> {
        let before = decode(buf).len();
        let mut q = sql.as_bytes().to_vec();
        q.push(0);
        client
            .write_all(&framed(crate::libpq::protocol::PQMSG_QUERY, &q))
            .await
            .expect("write query");
        client.flush().await.expect("flush query");

        // Done when one more ReadyForQuery has arrived past the ones already seen.
        let target_z = decode(buf).iter().filter(|m| m.ty == b'Z').count() + 1;
        let all = read_until(client, buf, |m| {
            m.iter().filter(|x| x.ty == b'Z').count() >= target_z
        })
        .await;
        all[before..].to_vec()
    }

    /// THE MILESTONE: CREATE TABLE / INSERT / SELECT over the wire on an initdb'd
    /// tempdir cluster. Asserts the full message sequence per statement.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m2_create_insert_select_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        // 1) CREATE TABLE t (a int) -> CommandComplete "CREATE TABLE" + ReadyForQuery.
        let create = simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        let types: Vec<u8> = create.iter().map(|m| m.ty).collect();
        assert_eq!(types, vec![b'C', b'Z'], "CREATE TABLE: CommandComplete + ReadyForQuery");
        assert_eq!(create[0].body, b"CREATE TABLE\0");
        assert_eq!(create[1].body, b"I");

        // 2) INSERT INTO t VALUES (1) -> CommandComplete "INSERT 0 1" + ReadyForQuery.
        let insert = simple_query(&mut client, &mut buf, "INSERT INTO t VALUES (1)").await;
        let types: Vec<u8> = insert.iter().map(|m| m.ty).collect();
        assert_eq!(types, vec![b'C', b'Z'], "INSERT: CommandComplete + ReadyForQuery");
        assert_eq!(insert[0].body, b"INSERT 0 1\0");
        assert_eq!(insert[1].body, b"I");

        // 3) SELECT * FROM t -> RowDescription + DataRow + CommandComplete + RFQ.
        let select = simple_query(&mut client, &mut buf, "SELECT * FROM t").await;
        let types: Vec<u8> = select.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'C', b'Z'],
            "SELECT: RowDescription + DataRow + CommandComplete + ReadyForQuery"
        );

        // RowDescription: 1 field "a", type OID 23 (int4), attlen 4, typmod -1, fmt 0.
        let t = &select[0];
        let natts = u16::from_be_bytes([t.body[0], t.body[1]]);
        assert_eq!(natts, 1, "one field");
        // field name "a\0"
        assert_eq!(&t.body[2..4], b"a\0");
        // after name: resorigtbl(4) resorigcol(2) typoid(4) ...
        let typoid = u32::from_be_bytes([t.body[10], t.body[11], t.body[12], t.body[13]]);
        assert_eq!(typoid, 23, "int4 type OID");

        // DataRow: 1 column, text "1".
        let d = &select[1];
        let mut dr = Vec::new();
        dr.extend_from_slice(&1u16.to_be_bytes());
        dr.extend_from_slice(&1u32.to_be_bytes());
        dr.extend_from_slice(b"1");
        assert_eq!(d.body, dr, "DataRow text \"1\"");

        // CommandComplete "SELECT 1" + idle ReadyForQuery.
        assert_eq!(select[2].body, b"SELECT 1\0");
        assert_eq!(select[3].body, b"I");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Decode the single-column int4 text value of a DataRow message body. The
    /// body is `int16 ncols` then per column `int32 len` + `len` bytes of text.
    fn datarow_single_text(body: &[u8]) -> String {
        let ncols = u16::from_be_bytes([body[0], body[1]]);
        assert_eq!(ncols, 1, "one column DataRow");
        let len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
        assert!(len >= 0, "column is not NULL");
        let s = &body[6..6 + len as usize];
        String::from_utf8(s.to_vec()).expect("text value is utf8")
    }

    /// THE M3 MILESTONE: `SELECT a + 1 FROM t WHERE a > 0` over the wire. The
    /// projection computes a+1 per row via int4pl, and the WHERE qual filters rows
    /// via int4gt. Rows -2,-1,0,1,2,3 -> filter a>0 keeps 1,2,3 -> project a+1 ->
    /// 2,3,4.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m3_projection_and_qual_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let create = simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        assert_eq!(create[0].body, b"CREATE TABLE\0");

        for v in [-2, -1, 0, 1, 2, 3] {
            let ins = simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
            assert_eq!(ins[0].body, b"INSERT 0 1\0", "INSERT {v}");
        }

        // SELECT a + 1 FROM t WHERE a > 0 -> RowDescription + 3 DataRows + CC + RFQ.
        let sel = simple_query(&mut client, &mut buf, "SELECT a + 1 FROM t WHERE a > 0").await;
        let types: Vec<u8> = sel.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'D', b'D', b'C', b'Z'],
            "RowDescription + 3 DataRows + CommandComplete + ReadyForQuery"
        );

        // The qual a>0 keeps {1,2,3}; the projection a+1 yields {2,3,4}.
        let values: Vec<String> = sel
            .iter()
            .filter(|m| m.ty == b'D')
            .map(|m| datarow_single_text(&m.body))
            .collect();
        assert_eq!(values, vec!["2", "3", "4"], "a+1 over rows where a>0");

        assert_eq!(sel[4].body, b"SELECT 3\0", "three rows selected");
        assert_eq!(sel[5].body, b"I");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Decode every text column of a DataRow body (a NULL column -> the literal
    /// "NULL" sentinel so the assertion can name it).
    fn datarow_texts(body: &[u8]) -> Vec<String> {
        let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
        let mut out = Vec::with_capacity(ncols);
        let mut off = 2usize;
        for _ in 0..ncols {
            let len = i32::from_be_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]]);
            off += 4;
            if len < 0 {
                out.push("NULL".to_owned());
            } else {
                let n = len as usize;
                out.push(String::from_utf8(body[off..off + n].to_vec()).expect("utf8"));
                off += n;
            }
        }
        out
    }

    /// THE M4 MILESTONE: casts + CASE + COALESCE + NULLIF + GREATEST/LEAST over the
    /// wire. Over rows a in {-1, 0, 2}:
    ///   - `a::numeric` -> "-1","0","2" (int4_numeric via-func cast),
    ///   - `a::text`    -> "-1","0","2" (int4out CoerceViaIO),
    ///   - `CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END` -> "neg","neg","pos",
    ///   - `COALESCE(NULLIF(a,0), -9)` -> "-1","-9","2" (NULLIF nulls the 0, COALESCE
    ///     substitutes -9),
    ///   - `GREATEST(a,0)` / `LEAST(a,0)` -> (0,-1),(0,0),(2,0).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m4_casts_and_conditionals_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        for v in [-1, 0, 2] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
        }

        // a::numeric
        let sel = simple_query(&mut client, &mut buf, "SELECT a::numeric FROM t").await;
        let vals: Vec<String> = sel.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect();
        assert_eq!(vals, vec!["-1", "0", "2"], "a::numeric");

        // CAST(a AS float8) -- via i4tod; float8out renders integers with no fraction.
        let sel = simple_query(&mut client, &mut buf, "SELECT CAST(a AS float8) FROM t").await;
        let vals: Vec<String> = sel.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect();
        assert_eq!(vals, vec!["-1", "0", "2"], "CAST(a AS float8)");

        // a::text (int4out CoerceViaIO)
        let sel = simple_query(&mut client, &mut buf, "SELECT a::text FROM t").await;
        let vals: Vec<String> = sel.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect();
        assert_eq!(vals, vec!["-1", "0", "2"], "a::text");

        // CASE WHEN a > 0 THEN 1 ELSE 0 END
        let sel = simple_query(&mut client, &mut buf, "SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM t").await;
        let vals: Vec<String> = sel.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect();
        assert_eq!(vals, vec!["0", "0", "1"], "CASE WHEN a>0 THEN 1 ELSE 0");

        // COALESCE(NULLIF(a, 0), -9): NULLIF nulls the row where a=0; COALESCE then
        // substitutes -9. Rows -1,0,2 -> -1,-9,2.
        let sel = simple_query(&mut client, &mut buf, "SELECT COALESCE(NULLIF(a, 0), -9) FROM t").await;
        let vals: Vec<String> = sel.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect();
        assert_eq!(vals, vec!["-1", "-9", "2"], "COALESCE(NULLIF(a,0), -9)");

        // GREATEST(a, 0), LEAST(a, 0): two columns per row.
        let sel = simple_query(&mut client, &mut buf, "SELECT GREATEST(a, 0), LEAST(a, 0) FROM t").await;
        let rows: Vec<Vec<String>> = sel.iter().filter(|m| m.ty == b'D').map(|m| datarow_texts(&m.body)).collect();
        assert_eq!(
            rows,
            vec![
                vec!["0".to_owned(), "-1".to_owned()],
                vec!["0".to_owned(), "0".to_owned()],
                vec!["2".to_owned(), "0".to_owned()],
            ],
            "GREATEST(a,0), LEAST(a,0)"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// THE M5 MILESTONE: GROUP BY + aggregates + ORDER BY + DISTINCT + LIMIT over the
    /// wire. Over t(a int) with rows {3,1,2,1,3,1}:
    ///   - `SELECT count(*) FROM t`                 -> 6 (plain agg).
    ///   - `SELECT a, count(*) FROM t GROUP BY a ORDER BY a` -> (1,3),(2,1),(3,2).
    ///   - `SELECT a FROM t ORDER BY a` / `... DESC`.
    ///   - `SELECT DISTINCT a FROM t`               -> 1,2,3.
    ///   - `SELECT a FROM t ORDER BY a LIMIT 2`     -> 1,1; `LIMIT 2 OFFSET 1` -> 1,1.
    ///   - `SELECT a, count(*) FROM t GROUP BY a ORDER BY a LIMIT 5` (the headline).
    ///   - `SELECT sum(a), min(a), max(a) FROM t`   -> 10, 1, 3.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m5_grouping_and_ordering_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        for v in [3, 1, 2, 1, 3, 1] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
        }

        let texts = |msgs: &[Msg]| -> Vec<Vec<String>> {
            msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_texts(&m.body)).collect()
        };
        let single = |msgs: &[Msg]| -> Vec<String> {
            msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect()
        };

        // count(*) over the whole table (plain aggregation, no GROUP BY).
        let sel = simple_query(&mut client, &mut buf, "SELECT count(*) FROM t").await;
        assert_eq!(single(&sel), vec!["6"], "count(*) = 6");

        // The milestone shape (GROUP BY a ORDER BY a).
        let sel = simple_query(&mut client, &mut buf, "SELECT a, count(*) FROM t GROUP BY a ORDER BY a").await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["1".to_owned(), "3".to_owned()],
                vec!["2".to_owned(), "1".to_owned()],
                vec!["3".to_owned(), "2".to_owned()],
            ],
            "per-group counts ordered by a"
        );

        // ORDER BY a (ascending) and ORDER BY a DESC.
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t ORDER BY a").await;
        assert_eq!(single(&sel), vec!["1", "1", "1", "2", "3", "3"], "ORDER BY a");
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t ORDER BY a DESC").await;
        assert_eq!(single(&sel), vec!["3", "3", "2", "1", "1", "1"], "ORDER BY a DESC");

        // DISTINCT a.
        let sel = simple_query(&mut client, &mut buf, "SELECT DISTINCT a FROM t").await;
        assert_eq!(single(&sel), vec!["1", "2", "3"], "DISTINCT a");

        // LIMIT and LIMIT/OFFSET over the sorted rows {1,1,1,2,3,3}.
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t ORDER BY a LIMIT 2").await;
        assert_eq!(single(&sel), vec!["1", "1"], "ORDER BY a LIMIT 2");
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1").await;
        assert_eq!(single(&sel), vec!["1", "1"], "ORDER BY a LIMIT 2 OFFSET 1");

        // THE HEADLINE: GROUP BY a ORDER BY a LIMIT 5 -> the three groups (under 5).
        let sel = simple_query(&mut client, &mut buf, "SELECT a, count(*) FROM t GROUP BY a ORDER BY a LIMIT 5").await;
        let types: Vec<u8> = sel.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'D', b'D', b'C', b'Z'],
            "RowDescription + 3 group DataRows + CommandComplete + ReadyForQuery"
        );
        assert_eq!(
            texts(&sel),
            vec![
                vec!["1".to_owned(), "3".to_owned()],
                vec!["2".to_owned(), "1".to_owned()],
                vec!["3".to_owned(), "2".to_owned()],
            ],
            "SELECT a, count(*) FROM t GROUP BY a ORDER BY a LIMIT 5"
        );

        // sum / min / max over the whole table: 3+1+2+1+3+1 = 11... rows are
        // {3,1,2,1,3,1} so sum = 11, min = 1, max = 3.
        let sel = simple_query(&mut client, &mut buf, "SELECT sum(a), min(a), max(a) FROM t").await;
        assert_eq!(
            texts(&sel),
            vec![vec!["11".to_owned(), "1".to_owned(), "3".to_owned()]],
            "sum(a), min(a), max(a)"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M3 BoolExpr qual over the wire: `WHERE a > 0 AND a < 3` keeps {1,2} -- the
    /// AND short-circuit + per-clause int4 comparison. (Three-valued NULL logic is
    /// unit-tested in execExprInterp; the NULL literal is not yet parseable.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m3_boolexpr_and_qual_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        for v in [-1, 0, 1, 2, 3] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
        }

        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t WHERE a > 0 AND a < 3").await;
        let values: Vec<String> = sel
            .iter()
            .filter(|m| m.ty == b'D')
            .map(|m| datarow_single_text(&m.body))
            .collect();
        assert_eq!(values, vec!["1", "2"], "a where a>0 AND a<3");
        assert_eq!(sel.last().unwrap().body, b"I");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M1 REGRESSION: SELECT 1 over the wire still returns the const int4 row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m1_select_1_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let select = simple_query(&mut client, &mut buf, "SELECT 1").await;
        let types: Vec<u8> = select.iter().map(|m| m.ty).collect();
        assert_eq!(types, vec![b'T', b'D', b'C', b'Z'], "M1 SELECT 1 sequence");

        // RowDescription: field "?column?", type OID 23.
        let t = &select[0];
        assert_eq!(&t.body[0..2], &1u16.to_be_bytes(), "one field");
        assert_eq!(&t.body[2..11], b"?column?\0");

        // DataRow: text "1".
        let d = &select[1];
        let mut dr = Vec::new();
        dr.extend_from_slice(&1u16.to_be_bytes());
        dr.extend_from_slice(&1u32.to_be_bytes());
        dr.extend_from_slice(b"1");
        assert_eq!(d.body, dr);

        assert_eq!(select[2].body, b"SELECT 1\0");
        assert_eq!(select[3].body, b"I");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M1 REGRESSION: a const SELECT with a different value returns its text.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m1_select_42_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let select = simple_query(&mut client, &mut buf, "SELECT 42").await;
        let d = select.iter().find(|m| m.ty == b'D').expect("a DataRow");
        let mut dr = Vec::new();
        dr.extend_from_slice(&1u16.to_be_bytes());
        dr.extend_from_slice(&2u32.to_be_bytes()); // "42" is 2 bytes
        dr.extend_from_slice(b"42");
        assert_eq!(d.body, dr);
        let c = select.iter().find(|m| m.ty == b'C').expect("a CommandComplete");
        assert_eq!(c.body, b"SELECT 1\0"); // one row processed

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M1 REGRESSION: a two-column const SELECT returns a two-field RowDescription
    /// and a two-column DataRow.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m1_select_two_columns_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let select = simple_query(&mut client, &mut buf, "SELECT 1, 2").await;
        let t = select.iter().find(|m| m.ty == b'T').expect("a RowDescription");
        let natts = u16::from_be_bytes([t.body[0], t.body[1]]);
        assert_eq!(natts, 2);

        let d = select.iter().find(|m| m.ty == b'D').expect("a DataRow");
        let mut dr = Vec::new();
        dr.extend_from_slice(&2u16.to_be_bytes());
        dr.extend_from_slice(&1u32.to_be_bytes());
        dr.extend_from_slice(b"1");
        dr.extend_from_slice(&1u32.to_be_bytes());
        dr.extend_from_slice(b"2");
        assert_eq!(d.body, dr);

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }
}
