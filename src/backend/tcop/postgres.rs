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
pub const DEFAULT_DATABASE_OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(90000);

pub async fn postgres_main(
    stream: TcpStream,
    shared: Arc<SharedState>,
    dbname: String,
    username: String,
    proc_pid: i32,
    cancel_key: Vec<u8>,
) {
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
    let loop_fut = Box::pin(init_postgres(
        shared.clone(),
        command_loop(shared, proc_pid, cancel_key, username),
    ));
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

    // Per-backend named-portal table (portalmem.c PortalHashTable) and prepared-
    // statement table (prepare.c prepared_queries dynahash): both per-task, holding
    // non-Send plans/stores, so they bracket the command loop here.
    let inner = crate::backend::tcop::pquery::portal_scope_async(inner);
    let inner = crate::backend::commands::prepare::prepared_scope_async(inner);

    let body = Box::pin(catalog_index_scope(Box::pin(inner)));
    let body = Box::pin(relcache_scope(body));
    let body = Box::pin(catcache_scope(body));
    let body = Box::pin(with_insertion(body));
    let body = Box::pin(combocid_scope(body));
    let body = Box::pin(snapmgr_scope(body));
    // Per-backend after-trigger event queue (trigger.c's AfterTriggers state): so
    // AFTER ROW triggers queued during a statement fire at its end (RI checks).
    let body = Box::pin(crate::backend::commands::trigger::after_trigger_scope(body));
    // Per-backend GUC store (PG's process-wide GUC globals): so SET/SHOW persist for
    // the life of the connection. Boot defaults until a SET overrides them.
    let body = Box::pin(crate::backend::utils::misc::guc::guc_scope(body));
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
async fn command_loop(
    shared: Arc<SharedState>,
    proc_pid: i32,
    cancel_key: Vec<u8>,
    username: String,
) {
    use futures_util::FutureExt;
    // Complete the startup handshake: AuthenticationOk, ParameterStatus for the
    // reported GUCs, and this backend's BackendKeyData. Buffered here and flushed
    // by the first ReadyForQuery below (PG PostgresMain order).
    crate::backend::tcop::backend_startup::perform_authentication_and_report(
        proc_pid,
        &cancel_key,
        &username,
    )
    .await;

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
        PQMSG_PARSE => {
            exec_parse_message(shared, body).await;
            CommandResult::Continue { ready: false }
        }
        PQMSG_BIND => {
            exec_bind_message(shared, body).await;
            CommandResult::Continue { ready: false }
        }
        PQMSG_EXECUTE => {
            exec_execute_message(body);
            CommandResult::Continue { ready: false }
        }
        PQMSG_DESCRIBE => {
            // Body: subtype byte ('S' statement / 'P' portal) + name.
            let mut r = std::io::Cursor::new(body);
            let subtype = crate::backend::libpq::pqformat::pq_getmsgbyte(&mut r) as u8;
            let name = crate::backend::libpq::pqformat::pq_getmsgstring(&mut r).to_owned();
            match subtype {
                b'S' => exec_describe_statement_message(&name),
                b'P' => exec_describe_portal_message(&name),
                other => {
                    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                        e.errcode(crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION)
                            .errmsg(format!("invalid DESCRIBE message subtype {}", other as char));
                    });
                }
            }
            CommandResult::Continue { ready: false }
        }
        PQMSG_CLOSE => {
            exec_close_message(body);
            CommandResult::Continue { ready: false }
        }
        PQMSG_FUNCTION_CALL => unimplemented!("fastpath function call deferred"),
        PQMSG_FLUSH => CommandResult::Continue { ready: false },
        PQMSG_SYNC => {
            finish_xact_command_async(shared).await;
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

    // start_xact_command(): open the per-statement transaction (autocommit). Inside
    // an explicit transaction block this is a no-op (the block stays open); the
    // active snapshot is pushed below, after the aborted-block guard.
    start_xact_command_async(shared).await;

    // pg_parse_query: raw parse.
    let mut parsetrees = raw_parser(query_string, RawParseMode::Default);

    // Empty query string: tell the frontend and finish.
    if parsetrees.is_empty() {
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

    // If we are in an aborted transaction block, reject every command except the
    // ones that end the block (COMMIT/ROLLBACK/PREPARE/ROLLBACK TO). PG raises this
    // before analysis so the rejected statement is never planned/executed.
    if crate::backend::access::transam::xact::IsAbortedTransactionBlockState()
        && !is_transaction_exit_stmt(raw.stmt.as_ref())
    {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_IN_FAILED_SQL_TRANSACTION)
                .errmsg(
                    "current transaction is aborted, commands ignored until end of transaction block"
                        .to_string(),
                );
        });
    }

    // Push the active snapshot the analyze/plan/executor read under, with curcid set
    // to the current command id so the statement sees its own prior commands.
    push_statement_snapshot(shared);

    crate::backend::tcop::dest::begin_command(CommandTag::Unknown, dest);

    // If we got a cancel signal in parsing, quit.
    crate::miscadmin::check_for_interrupts();

    // pg_analyze_and_rewrite_fixedparams: parse analysis (ASYNC -- opens relations
    // for SELECT/INSERT/UPDATE/DELETE) + rewrite. The rewriter's data-modifying
    // target-list rewriting + rule firing is staged (rewriteHandler.rs s4); M2/M8 have
    // no rules or views and the transforms already build the final attno-ordered form
    // (INSERT tlist, UPDATE SET tlist + preptlist expansion, DELETE row identity), so a
    // plain data-modifying statement is passed straight to the planner without the
    // rewrite pass. SELECT (and UTILITY) go through QueryRewrite as usual.
    let analyzed = parse_analyze_fixedparams_async(shared, &raw, query_string, &[], 0).await;
    let mut query = if matches!(
        analyzed.commandType,
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
    ) {
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
        CmdType::SELECT | CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE => {
            run_plan_over_wire(shared, &plan, query_string, command_tag, dest, &mut qc).await;
        }
        other => unimplemented!("exec_simple_query: command type {other:?} deferred"),
    }

    crate::backend::utils::time::snapmgr::PopActiveSnapshot();

    // CommandComplete, then commit the autocommit transaction. Capture the xid the
    // transaction may have assigned (a writing INSERT/CREATE) before committing, then
    // advance the shared latestCompletedXid so the NEXT statement's snapshot sees it.
    //
    // Inside an explicit transaction block CommitTransactionCommand does NOT commit
    // (it just advances the command counter and stays in the block), so the xid must
    // NOT be published yet -- it would make uncommitted rows visible to other
    // snapshots. Only publish once the block has actually closed (block_state back to
    // Default after finish_xact_command); on COMMIT the just-assigned xid is still
    // live at capture time and is published then.
    crate::backend::tcop::dest::end_command(&qc, dest, false);
    // The TOP transaction id (subtransaction xids roll up to it at subcommit); a
    // statement run inside a block leaves the current frame on a subxact, so capture
    // the top frame, not the current one.
    let committed = crate::backend::access::transam::xact::GetTopTransactionIdIfAny();
    finish_xact_command_async(shared).await;
    if !crate::backend::access::transam::xact::IsTransactionOrTransactionBlock() {
        publish_committed_xid(shared, committed);
    }
}

// ---------------------------------------------------------------------------
// Extended query protocol (Parse / Bind / Describe / Execute / Close / Sync).
//
// Parse ('P') analyzes a query (with declared $n type OIDs) into a
// CachedPlanSource, stored in the per-backend prepared-statement table (unnamed
// statement = ""). Bind ('B') decodes the parameter values (text format), gets
// the cached plan, and MATERIALIZES the bound result into a named portal's store
// (mirroring the cursor path, so Execute/Describe/repeat-Execute work). Describe
// ('D') sends ParameterDescription + RowDescription (statement) or RowDescription
// (portal). Execute ('E') replays the portal's store as DataRows + CommandComplete
// (or PortalSuspended if a row limit cut it short). Close ('C') drops a statement
// or portal; Sync ('S') closes the implicit transaction + ReadyForQuery.
//
// STAGED: binary parameter/result formats (text is wired); the streaming
// (non-materialized) Execute with true PortalSuspended/resume across separate
// Execute messages beyond the materialized store; multi-statement Parse.
// ---------------------------------------------------------------------------

/// PG `exec_parse_message`: handle a Parse ('P') message. Body is the statement
/// name, the query string, and a count of parameter type OIDs. Analyzes the query
/// with those types (deducing unknowns) and stores a CachedPlanSource.
async fn exec_parse_message(shared: &Arc<SharedState>, body: &[u8]) {
    use crate::backend::libpq::pqformat::{pq_getmsgint, pq_getmsgstring};
    use crate::backend::parser::analyze::parse_analyze_varparams_async;
    use crate::backend::parser::parser::raw_parser;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::backend::utils::cache::plancache::{CompleteCachedPlan, CreateCachedPlan};
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    let mut r = std::io::Cursor::new(body);
    let stmt_name = pq_getmsgstring(&mut r).to_owned();
    let query_string = pq_getmsgstring(&mut r).to_owned();
    let num_param_types = pq_getmsgint(&mut r, 2) as usize;
    let mut param_types: Vec<crate::postgres_ext::Oid> = (0..num_param_types)
        .map(|_| crate::postgres_ext::Oid::new(pq_getmsgint(&mut r, 4)))
        .collect();

    // start_xact_command + raw parse. An empty query string yields an empty
    // CachedPlanSource (the Execute later sends EmptyQueryResponse). M9 reaches a
    // single statement.
    ensure_xact_started(shared).await;
    push_statement_snapshot(shared);

    let mut parsetrees = raw_parser(&query_string, RawParseMode::Default);
    let (command_tag, query_list) = if parsetrees.is_empty() {
        (CommandTag::Unknown, Vec::new())
    } else {
        if parsetrees.len() != 1 {
            unimplemented!("exec_parse_message: multi-statement Parse deferred");
        }
        let Node::RawStmt(raw) = parsetrees.remove(0) else {
            unreachable!("raw_parser yields RawStmt nodes");
        };
        let raw: RawStmt = *raw;
        let inner = raw
            .stmt
            .clone()
            .unwrap_or_else(|| unreachable!("a non-empty RawStmt carries its statement"));
        let command_tag = crate::backend::tcop::utility::create_command_tag(&inner);
        let analyzed =
            parse_analyze_varparams_async(shared, &raw, &query_string, &mut param_types).await;
        let qlist = if matches!(
            analyzed.commandType,
            CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
        ) {
            vec![*analyzed]
        } else {
            query_rewrite(*analyzed)
        };
        (command_tag, qlist)
    };

    crate::backend::utils::time::snapmgr::PopActiveSnapshot();

    // Build + complete the CachedPlanSource, then store it under the statement name.
    let raw_for_source = RawStmt { stmt: None, stmt_location: -1, stmt_len: 0 };
    let mut plansource = CreateCachedPlan(raw_for_source, &query_string, command_tag);
    let num_params = i32::try_from(param_types.len()).unwrap_or(0);
    CompleteCachedPlan(&mut plansource, query_list, &param_types, num_params, None, 0, true);
    crate::backend::commands::prepare::store_or_replace_prepared_statement(&stmt_name, plansource, false);

    // ParseComplete ('1').
    crate::backend::libpq::pqcomm::pq_putmessage_sync(
        crate::libpq::protocol::PQMSG_PARSE_COMPLETE,
        &[],
    );
}

/// PG `exec_bind_message`: handle a Bind ('B') message. Body: portal name,
/// statement name, parameter format codes, parameter values, result format codes.
/// Decodes the params (text format), gets the cached plan, materializes the bound
/// result into the named portal, and replies BindComplete ('2').
async fn exec_bind_message(shared: &Arc<SharedState>, body: &[u8]) {
    use crate::backend::libpq::pqformat::{pq_getmsgbytes, pq_getmsgint, pq_getmsgstring};
    use crate::nodes::params::{makeParamList, ParamFlags};

    let mut r = std::io::Cursor::new(body);
    let portal_name = pq_getmsgstring(&mut r).to_owned();
    let stmt_name = pq_getmsgstring(&mut r).to_owned();

    // Parameter format codes.
    let num_pformats = pq_getmsgint(&mut r, 2) as usize;
    let pformats: Vec<i16> =
        (0..num_pformats).map(|_| pq_getmsgint(&mut r, 2) as i16).collect();

    // Parameter values.
    let num_params = pq_getmsgint(&mut r, 2) as usize;
    let mut raw_params: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_params);
    for _ in 0..num_params {
        let len = pq_getmsgint(&mut r, 4) as i32;
        if len == -1 {
            raw_params.push(None);
        } else {
            raw_params.push(Some(pq_getmsgbytes(&mut r, len).to_vec()));
        }
    }
    // Result format codes (consumed; text-only output for M9).
    let result_format_count = pq_getmsgint(&mut r, 2) as usize;
    for _ in 0..result_format_count {
        let _ = pq_getmsgint(&mut r, 2);
    }

    ensure_xact_started(shared).await;
    push_statement_snapshot(shared);

    // Decode the params + clone the plan under the plansource borrow (sync); the
    // RefCell borrow must NOT be held across the async materialization below.
    let (query_string, plan, command_tag, param_li) =
        crate::backend::commands::prepare::with_plansource(&stmt_name, |src| {
            let param_types = src.param_types.clone();
            let mut param_li = makeParamList(i32::try_from(param_types.len()).unwrap_or(0));
            for (i, ptype) in param_types.iter().enumerate() {
                let fmt = param_format_code(&pformats, i, num_params);
                let prm = &mut param_li.params[i];
                prm.ptype = *ptype;
                prm.pflags = ParamFlags::CONST;
                match raw_params.get(i).and_then(Option::as_ref) {
                    None => {
                        prm.isnull = true;
                        prm.value = crate::postgres::Datum(0);
                    }
                    Some(bytes) if fmt == 0 => {
                        let text = String::from_utf8_lossy(bytes);
                        prm.value = type_input(*ptype, &text);
                        prm.isnull = false;
                    }
                    Some(_) => {
                        unimplemented!("Bind: binary parameter format (format code 1) deferred");
                    }
                }
            }
            let cplan =
                crate::backend::utils::cache::plancache::GetCachedPlan(src, Some(&param_li), None);
            let plan = cplan.stmt_list.first().cloned();
            (src.query_string.clone(), plan, src.commandTag, param_li)
        });

    // Materialize the bound result into the named portal (mirrors the cursor path)
    // so Execute / Describe / repeated Execute navigate the store.
    let (store, tupdesc, _processed) = match &plan {
        Some(plan) => {
            crate::backend::tcop::pquery::run_plan_into_store(
                shared,
                plan,
                &query_string,
                Some(&param_li),
            )
            .await
        }
        // An empty-query statement: an empty store + no descriptor.
        None => (
            crate::backend::utils::sort::tuplestore::tuplestore_begin_heap(true, false, 1024),
            None,
            0,
        ),
    };

    crate::backend::utils::time::snapmgr::PopActiveSnapshot();

    install_bound_portal(&portal_name, &query_string, command_tag, store, tupdesc);

    // BindComplete ('2').
    crate::backend::libpq::pqcomm::pq_putmessage_sync(
        crate::libpq::protocol::PQMSG_BIND_COMPLETE,
        &[],
    );
}

/// Install a freshly-materialized bound portal into the per-task portal table.
fn install_bound_portal(
    portal_name: &str,
    query_string: &str,
    command_tag: CommandTag,
    store: Box<crate::utils::tuplestore::Tuplestorestate>,
    tupdesc: Option<crate::access::tupdesc::TupleDesc>,
) {
    use crate::backend::tcop::pquery::{create_named_portal, with_named_portal};
    use crate::utils::portal::PortalStatus;
    // Bind replaces an existing portal of the same name (allow_dup, silent).
    create_named_portal(portal_name, true, true);
    with_named_portal(portal_name, |portal| {
        portal.source_text = query_string.to_string();
        portal.command_tag = command_tag;
        portal.tup_desc = tupdesc;
        portal.hold_store = Some(store);
        portal.status = PortalStatus::Ready;
        portal.at_start = true;
        portal.at_end = false;
        portal.portal_pos = 0;
    })
    .unwrap_or_else(|| unreachable!("bound portal just created"));
}

/// PG `exec_describe_statement_message`: handle Describe ('D') of a prepared
/// statement. Sends ParameterDescription ('t') then RowDescription ('T') or
/// NoData ('n').
fn exec_describe_statement_message(stmt_name: &str) {
    use crate::backend::libpq::pqcomm::pq_putmessage_sync;
    use crate::backend::libpq::pqformat::PqMsg;

    let (param_types, tupdesc) = crate::backend::commands::prepare::statement_describe(stmt_name);

    // ParameterDescription: int16 count, then one int32 type OID per parameter.
    let mut msg = PqMsg::default();
    msg.begin_message(crate::libpq::protocol::PQMSG_PARAMETER_DESCRIPTION);
    msg.send_int16(u16::try_from(param_types.len()).unwrap_or(0));
    for ptype in &param_types {
        msg.send_int32(ptype.get());
    }
    pq_putmessage_sync(msg.msgtype, &msg.data);

    send_row_description_or_nodata(tupdesc.as_ref());
}

/// PG `exec_describe_portal_message`: handle Describe ('D') of a portal. Sends
/// RowDescription ('T') or NoData ('n') for the portal's result.
fn exec_describe_portal_message(portal_name: &str) {
    let tupdesc = crate::backend::tcop::pquery::with_named_portal(portal_name, |p| p.tup_desc.clone());
    let Some(tupdesc) = tupdesc else {
        portal_not_found(portal_name);
    };
    send_row_description_or_nodata(tupdesc.as_ref());
}

/// Send a RowDescription ('T') for `tupdesc`, or NoData ('n') if the statement
/// returns no rows. Text format for every column (M9 wire mode).
fn send_row_description_or_nodata(tupdesc: Option<&crate::access::tupdesc::TupleDesc>) {
    match tupdesc {
        Some(td) => {
            crate::backend::access::common::printtup::send_row_description_message(td, &[]);
        }
        None => {
            crate::backend::libpq::pqcomm::pq_putmessage_sync(crate::libpq::protocol::PQMSG_NO_DATA, &[]);
        }
    }
}

/// PG `exec_execute_message`: handle Execute ('E'). Body: portal name + max row
/// count (0 = all). Replays the portal's materialized store as DataRows, then
/// sends CommandComplete (all rows fetched) or PortalSuspended (row limit hit).
fn exec_execute_message(body: &[u8]) {
    use crate::backend::libpq::pqformat::{pq_getmsgint, pq_getmsgstring};
    use crate::nodes::parsenodes::{FetchDirection, FETCH_ALL};

    let mut r = std::io::Cursor::new(body);
    let portal_name = pq_getmsgstring(&mut r).to_owned();
    let max_rows = i64::from(pq_getmsgint(&mut r, 4));
    let count = if max_rows == 0 { FETCH_ALL } else { max_rows };

    if !crate::backend::tcop::pquery::portal_exists(&portal_name) {
        portal_not_found(&portal_name);
    }

    // DestRemoteExecute: a printtup receiver that does NOT send a RowDescription
    // (Execute relies on a prior Describe for that, per the protocol).
    let mut receiver = crate::backend::tcop::dest::create_dest_receiver(CommandDest::DestRemoteExecute);
    crate::backend::access::common::printtup::set_remote_dest_receiver_params(receiver.as_mut(), &[]);

    let (fetched, completed, command_tag) = crate::backend::tcop::pquery::with_named_portal(
        &portal_name,
        |portal| {
            let n = crate::backend::tcop::pquery::portal_run_fetch(
                portal,
                FetchDirection::FORWARD,
                count,
                Some(receiver.as_mut()),
            );
            (n, portal.at_end, portal.command_tag)
        },
    )
    .unwrap_or_else(|| unreachable!("portal existence checked above"));

    if completed || count == FETCH_ALL {
        // CommandComplete with the tag + row count.
        let qc = crate::tcop::cmdtag::QueryCompletion { command_tag, nprocessed: fetched };
        crate::backend::tcop::dest::end_command(&qc, CommandDest::DestRemoteExecute, false);
    } else {
        // PortalSuspended: more rows remain, the row limit was hit.
        crate::backend::libpq::pqcomm::pq_putmessage_sync(
            crate::libpq::protocol::PQMSG_PORTAL_SUSPENDED,
            &[],
        );
    }
}

#[cold]
fn portal_not_found(name: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("portal \"{name}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `exec_close_message`: handle Close ('C'). Drops a statement ('S') or portal
/// ('P') and replies CloseComplete ('3').
fn exec_close_message(body: &[u8]) {
    use crate::backend::libpq::pqformat::{pq_getmsgbyte, pq_getmsgstring};
    let mut r = std::io::Cursor::new(body);
    let kind = pq_getmsgbyte(&mut r) as u8;
    let name = pq_getmsgstring(&mut r).to_owned();
    match kind {
        b'S' => crate::backend::commands::prepare::drop_prepared_statement(&name, false),
        b'P' => crate::backend::tcop::pquery::drop_named_portal(&name),
        other => {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("invalid CLOSE message subtype {}", other as char));
            });
        }
    }
    crate::backend::libpq::pqcomm::pq_putmessage_sync(crate::libpq::protocol::PQMSG_CLOSE_COMPLETE, &[]);
}

/// The format code for parameter `i` (PG's per-param vs single-code-for-all
/// convention): 0 codes -> all text; 1 code -> that code for all; else per-param.
fn param_format_code(pformats: &[i16], i: usize, _num_params: usize) -> i16 {
    match pformats.len() {
        0 => 0,
        1 => pformats[0],
        _ => pformats.get(i).copied().unwrap_or(0),
    }
}

/// Run a type's input function over `text` to produce its internal Datum (the
/// text-format parameter decode). PG calls the type's `typinput` via
/// `OidInputFunctionCall` after `getTypeInputInfo`.
fn type_input(type_oid: crate::postgres_ext::Oid, text: &str) -> crate::postgres::Datum {
    let (typinput, typioparam) =
        crate::backend::utils::cache::lsyscache::get_type_input_info(type_oid);
    crate::backend::utils::fmgr::fmgr::OidInputFunctionCall(typinput, text, typioparam, -1)
        .unwrap_or_else(|| unreachable!("text-format param input produced a value"))
}

/// PG `IsTransactionExitStmt`: a transaction-control statement that ends (or can
/// end) the current block -- COMMIT / PREPARE / ROLLBACK / ROLLBACK TO. These are
/// the only statements allowed to run while the block is in the aborted state.
fn is_transaction_exit_stmt(stmt: Option<&crate::nodes::nodes::Node>) -> bool {
    use crate::nodes::parsenodes::TransactionStmtKind as Kind;
    let Some(crate::nodes::nodes::Node::TransactionStmt(s)) = stmt else {
        return false;
    };
    matches!(s.kind, Kind::COMMIT | Kind::PREPARE | Kind::ROLLBACK | Kind::ROLLBACK_TO)
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
pub(crate) fn publish_committed_xid(
    shared: &Arc<SharedState>,
    committed: Option<crate::c::TransactionId>,
) {
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
    // Build a DestRemote printtup receiver (text format for every column in simple
    // Query mode), run the plan, and report the row count.
    let mut receiver = crate::backend::tcop::dest::create_dest_receiver(dest);
    if dest == CommandDest::DestRemote {
        crate::backend::access::common::printtup::set_remote_dest_receiver_params(
            receiver.as_mut(),
            &[],
        );
    }
    let processed = execute_plan_into(shared, plan, query_string, None, receiver, 0).await;
    qc.command_tag = command_tag;
    qc.nprocessed = processed;
}

/// Run a planned SELECT / INSERT / UPDATE / DELETE to completion (or `count` rows)
/// into `receiver`, returning the number of rows processed. Opens the plan's
/// range-table + index relations (the `'rel` ownership root is this frame), runs
/// ExecutorStart/Run/Finish/End against the active snapshot, then closes them.
///
/// This is the shared executor frame behind the simple-Query wire path, EXECUTE,
/// cursor materialization (portalcmds), and SPI. `bound_params` carries the $n
/// values for a parameterized plan (PG `queryDesc->params`).
pub(crate) async fn execute_plan_into(
    shared: &Arc<SharedState>,
    plan: &crate::nodes::plannodes::PlannedStmt,
    query_string: &str,
    bound_params: Option<&crate::nodes::params::ParamListInfoData>,
    receiver: Box<dyn crate::tcop::dest::DestReceiver>,
    count: u64,
) -> u64 {
    use crate::access::sdir::ScanDirection;
    use crate::backend::executor::execMain::{
        standard_executor_end, standard_executor_finish, standard_executor_run,
        standard_executor_start_indexed,
    };

    // Open every RTE_RELATION in the range table (PG opens them before InitPlan,
    // under the right locks). `opened` OWNS the `Arc<RelationData>`s -- it is the
    // `'rel` ownership root, a stack binding that strictly encloses the executor run
    // below (relation-ownership-plan §1.2). The executor BORROWS from it.
    let opened = open_range_table_relations(shared, plan).await;

    // Open the index relations the plan's index/bitmap scans reference (PG resolves
    // these via index_open in ExecInitIndexScan; the M6 wire path opens them up front
    // from the index registry so the executor borrows them off es_index_rels). The
    // owned `Arc`s live in this frame alongside `opened` (the `'rel` root).
    let opened_indexes = open_plan_index_relations(&plan.plan_tree);

    // Build the borrowed range-table indexed by RT index (PG `es_relations`): slot
    // `rti - 1` is `Some(&*arc)` for an opened RELATION RTE, `None` otherwise.
    let max_rti = opened.iter().map(|(rti, _)| *rti).max().unwrap_or(0);
    let mut range_table_rels: Vec<Option<&crate::utils::rel::RelationData>> = vec![None; max_rti];
    for (rti, rel) in &opened {
        range_table_rels[*rti - 1] = Some(&**rel);
    }

    let snap = crate::backend::utils::time::snapmgr::GetActiveSnapshot();
    // The snapshot `Arc` is owned here (the command frame), so the scan can borrow
    // `&*snap` across its `.await`s.
    let snapshot_ref = snap.as_deref();
    let mut query_desc = make_query_desc(plan, query_string, snap.clone(), receiver);
    // Thread the bound $n params (PG `queryDesc->params`); ExecutorStart copies them
    // onto the EState and CreateExprContext threads them to EEOP_PARAM_EXTERN.
    if let Some(params) = bound_params {
        query_desc.params = Some(Box::new(Box::new(params.clone())));
    }

    // Borrowed index-relation slice (PG es_index_rels): the executor's
    // ExecGetIndexRelation finds the open index by OID among these.
    let index_rels: Vec<Option<&crate::utils::rel::RelationData>> =
        opened_indexes.iter().map(|r| Some(&**r)).collect();

    standard_executor_start_indexed(
        &mut query_desc,
        &range_table_rels,
        &index_rels,
        snapshot_ref,
        0,
    );
    standard_executor_run(Some(shared), &mut query_desc, ScanDirection::Forward, count).await;
    standard_executor_finish(Some(shared), &mut query_desc).await;
    let processed = query_desc.estate.as_ref().map_or(0, |e| e.processed);
    standard_executor_end(Some(shared), &mut query_desc);

    // Drop the borrows before closing the owners.
    drop(query_desc);
    drop(range_table_rels);
    drop(index_rels);

    // Close the relations we opened (drop the relcache refcount).
    for (_rti, rel) in opened {
        crate::backend::utils::cache::relcache::relation_close(rel);
    }
    // The index Arcs are registry clones; dropping them just releases the refcount.
    drop(opened_indexes);

    processed
}

/// Open the index relations a plan's IndexScan / IndexOnlyScan / BitmapIndexScan
/// nodes reference, by collecting their `indexid`s and fetching each from the index
/// registry. Returns owned `Arc`s (registry clones). PG resolves these via
/// index_open in the executor; the M6 wire path opens them up front.
fn open_plan_index_relations(
    plan_tree: &crate::nodes::nodes::Node,
) -> Vec<Arc<crate::utils::rel::RelationData>> {
    use crate::backend::catalog::indexing::find_registered_index;
    let mut oids = Vec::new();
    collect_plan_index_oids(plan_tree, &mut oids);
    oids.into_iter()
        .filter_map(find_registered_index)
        .collect()
}

/// Walk a plan tree collecting the index OIDs of its index/bitmap scan nodes.
fn collect_plan_index_oids(node: &crate::nodes::nodes::Node, out: &mut Vec<crate::postgres_ext::Oid>) {
    use crate::nodes::nodes::Node;
    match node {
        Node::IndexScan(s) => out.push(s.indexid),
        Node::IndexOnlyScan(s) => out.push(s.indexid),
        Node::BitmapIndexScan(s) => out.push(s.indexid),
        _ => {}
    }
    for child in plan_children(node) {
        collect_plan_index_oids(child, out);
    }
}

/// The child plan nodes of a plan node (its lefttree/righttree), for the plan-tree
/// walk. Returns the concrete `Plan`-bearing children.
fn plan_children(node: &crate::nodes::nodes::Node) -> Vec<&crate::nodes::nodes::Node> {
    use crate::nodes::nodes::Node;
    let plan = match node {
        Node::Result(r) => &r.plan,
        Node::SeqScan(s) => &s.scan.plan,
        Node::IndexScan(s) => &s.scan.plan,
        Node::IndexOnlyScan(s) => &s.scan.plan,
        Node::BitmapHeapScan(s) => &s.scan.plan,
        Node::BitmapIndexScan(s) => &s.scan.plan,
        Node::Agg(a) => &a.plan,
        Node::Sort(s) => &s.plan,
        Node::Unique(u) => &u.plan,
        Node::Limit(l) => &l.plan,
        Node::ModifyTable(m) => &m.plan,
        _ => return Vec::new(),
    };
    let mut children = Vec::new();
    if let Some(lt) = plan.lefttree.as_ref() {
        children.push(lt);
    }
    if let Some(rt) = plan.righttree.as_ref() {
        children.push(rt);
    }
    children
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
        if rte.rtekind != RTEKind::RELATION || !rte.relid.is_valid() {
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
pub(crate) fn wrap_utility_stmt(
    query: &crate::nodes::parsenodes::Query,
) -> crate::nodes::plannodes::PlannedStmt {
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
        subplan_nodes: Vec::new(),
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
        crate::nodes::nodes::CmdType::UPDATE => CommandTag::Update,
        crate::nodes::nodes::CmdType::DELETE => CommandTag::Delete,
        crate::nodes::nodes::CmdType::MERGE => CommandTag::Merge,
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

/// PG `start_xact_command`'s idempotent guard for the extended protocol: start a
/// transaction only if one is not already open. Across a Parse/Bind/Describe/
/// Execute pipeline the transaction stays open until Sync commits it, so the
/// per-message handlers must not re-issue StartTransactionCommand from the
/// `Started` state (which the block-state machine rejects).
async fn ensure_xact_started(shared: &Arc<SharedState>) {
    if !crate::backend::access::transam::xact::IsTransactionState() {
        crate::backend::access::transam::xact::StartTransactionCommand(shared).await;
    }
}

/// PG `finish_xact_command`: commit the per-statement (autocommit) transaction.
async fn finish_xact_command_async(shared: &Arc<SharedState>) {
    crate::backend::access::transam::xact::CommitTransactionCommand(shared).await;
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

    /// Step 03: a failing query returns an ErrorResponse ('E') carrying a SQLSTATE
    /// and message, the ReadyForQuery reports idle ('I') after the autocommit
    /// rollback, and the SAME session stays usable -- a following SELECT 1 returns
    /// its row (the connection is not dropped).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m3_error_response_then_session_survives_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        // A reference to a nonexistent relation raises ERROR undefined_table (42P01).
        let err = simple_query(&mut client, &mut buf, "SELECT * FROM no_such_table").await;
        let types: Vec<u8> = err.iter().map(|m| m.ty).collect();
        assert_eq!(types, vec![b'E', b'Z'], "ErrorResponse + ReadyForQuery");

        // The 'E' body is a field list; find the M (primary message) and C (sqlstate)
        // fields, then the terminating zero.
        let e = &err[0];
        let fields = decode_error_fields(&e.body);
        assert_eq!(
            fields.iter().find(|(t, _)| *t == b'C').map(|(_, v)| v.as_str()),
            Some("42P01"),
            "SQLSTATE undefined_table"
        );
        assert_eq!(
            fields.iter().find(|(t, _)| *t == b'S').map(|(_, v)| v.as_str()),
            Some("ERROR"),
            "severity ERROR"
        );
        let msg = fields.iter().find(|(t, _)| *t == b'M').map(|(_, v)| v.as_str());
        assert!(
            msg.is_some_and(|m| m.contains("no_such_table")),
            "primary message names the missing relation: {msg:?}"
        );

        // The autocommit transaction was rolled back: idle status, not 'E'.
        assert_eq!(err[1].body, b"I", "ReadyForQuery idle after rollback");

        // THE POINT: the same session is still usable.
        let ok = simple_query(&mut client, &mut buf, "SELECT 1").await;
        let types: Vec<u8> = ok.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'C', b'Z'],
            "session survives: SELECT 1 returns a row"
        );
        assert_eq!(ok[2].body, b"SELECT 1\0");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Decode an ErrorResponse/NoticeResponse field list into (tag, value) pairs.
    fn decode_error_fields(body: &[u8]) -> Vec<(u8, String)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < body.len() && body[i] != 0 {
            let tag = body[i];
            i += 1;
            let start = i;
            while i < body.len() && body[i] != 0 {
                i += 1;
            }
            out.push((tag, String::from_utf8_lossy(&body[start..i]).into_owned()));
            i += 1; // skip the field NUL
        }
        out
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

    /// Parse a RowDescription body into the field names. Body: int16 nfields, then
    /// per field a NUL-terminated name + 18 bytes of metadata (tableoid/colno/
    /// typoid/typlen/typmod/format).
    fn rowdescription_field_names(body: &[u8]) -> Vec<String> {
        let nfields = u16::from_be_bytes([body[0], body[1]]) as usize;
        let mut out = Vec::with_capacity(nfields);
        let mut off = 2usize;
        for _ in 0..nfields {
            let end = body[off..].iter().position(|&b| b == 0).expect("field name NUL") + off;
            out.push(String::from_utf8(body[off..end].to_vec()).expect("utf8 name"));
            off = end + 1 + 18; // skip NUL + 18 metadata bytes
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

    /// THE M12 MILESTONE: window functions over the wire. Over t(g int, v int) with
    /// rows {(1,10),(1,20),(1,20),(2,5),(2,15)}:
    ///   - `row_number() OVER (ORDER BY v)`            -> 1..5 in v order.
    ///   - `rank()/dense_rank() OVER (ORDER BY v)`     -> peers share rank.
    ///   - `row_number() OVER (PARTITION BY g ORDER BY v)` -> restarts per g.
    ///   - `sum(v) OVER (PARTITION BY g)`              -> the partition total per row.
    ///   - `sum(v) OVER (ORDER BY v)`                  -> running total (default frame).
    ///   - `lag(v)/lead(v) OVER (ORDER BY v)`          -> neighbor value + NULL ends.
    ///   - a named `WINDOW w AS (...)` reused by two functions.
    #[allow(
        clippy::too_many_lines,
        reason = "end-to-end wire test exercising the full M12 window-function surface in one session"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m12_window_functions_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (g int, v int)").await;
        for (g, v) in [(1, 10), (1, 20), (1, 20), (2, 5), (2, 15)] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({g}, {v})")).await;
        }

        let texts = |msgs: &[Msg]| -> Vec<Vec<String>> {
            msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_texts(&m.body)).collect()
        };

        // row_number() OVER (ORDER BY v): 1..5 in ascending v order.
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT v, row_number() OVER (ORDER BY v) FROM t ORDER BY v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["5".to_owned(), "1".to_owned()],
                vec!["10".to_owned(), "2".to_owned()],
                vec!["15".to_owned(), "3".to_owned()],
                vec!["20".to_owned(), "4".to_owned()],
                vec!["20".to_owned(), "5".to_owned()],
            ],
            "row_number() OVER (ORDER BY v)"
        );

        // rank() and dense_rank() OVER (ORDER BY v): the two v=20 rows are peers.
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT v, rank() OVER (ORDER BY v), dense_rank() OVER (ORDER BY v) FROM t ORDER BY v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["5".to_owned(), "1".to_owned(), "1".to_owned()],
                vec!["10".to_owned(), "2".to_owned(), "2".to_owned()],
                vec!["15".to_owned(), "3".to_owned(), "3".to_owned()],
                vec!["20".to_owned(), "4".to_owned(), "4".to_owned()],
                vec!["20".to_owned(), "4".to_owned(), "4".to_owned()],
            ],
            "rank/dense_rank with peers"
        );

        // row_number() OVER (PARTITION BY g ORDER BY v): restarts at each partition.
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v) FROM t ORDER BY g, v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["1".to_owned(), "10".to_owned(), "1".to_owned()],
                vec!["1".to_owned(), "20".to_owned(), "2".to_owned()],
                vec!["1".to_owned(), "20".to_owned(), "3".to_owned()],
                vec!["2".to_owned(), "5".to_owned(), "1".to_owned()],
                vec!["2".to_owned(), "15".to_owned(), "2".to_owned()],
            ],
            "row_number() restarts per partition"
        );

        // sum(v) OVER (PARTITION BY g): the partition total on every row (g=1 -> 50,
        // g=2 -> 20).
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT g, v, sum(v) OVER (PARTITION BY g) FROM t ORDER BY g, v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["1".to_owned(), "10".to_owned(), "50".to_owned()],
                vec!["1".to_owned(), "20".to_owned(), "50".to_owned()],
                vec!["1".to_owned(), "20".to_owned(), "50".to_owned()],
                vec!["2".to_owned(), "5".to_owned(), "20".to_owned()],
                vec!["2".to_owned(), "15".to_owned(), "20".to_owned()],
            ],
            "sum(v) OVER (PARTITION BY g) partition total"
        );

        // sum(v) OVER (ORDER BY v): running total over the default RANGE frame
        // (UNBOUNDED PRECEDING .. CURRENT ROW). The two v=20 rows are peers, so both
        // include each other: 5,15,30,70,70.
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT v, sum(v) OVER (ORDER BY v) FROM t ORDER BY v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["5".to_owned(), "5".to_owned()],
                vec!["10".to_owned(), "15".to_owned()],
                vec!["15".to_owned(), "30".to_owned()],
                vec!["20".to_owned(), "70".to_owned()],
                vec!["20".to_owned(), "70".to_owned()],
            ],
            "sum(v) OVER (ORDER BY v) running total (RANGE peers)"
        );

        // lag(v) and lead(v) OVER (ORDER BY v): previous / next value, NULL at ends.
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT v, lag(v) OVER (ORDER BY v), lead(v) OVER (ORDER BY v) FROM t ORDER BY v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["5".to_owned(), "NULL".to_owned(), "10".to_owned()],
                vec!["10".to_owned(), "5".to_owned(), "15".to_owned()],
                vec!["15".to_owned(), "10".to_owned(), "20".to_owned()],
                vec!["20".to_owned(), "15".to_owned(), "20".to_owned()],
                vec!["20".to_owned(), "20".to_owned(), "NULL".to_owned()],
            ],
            "lag/lead with NULL at the partition ends"
        );

        // A named WINDOW reused by two functions.
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT v, row_number() OVER w, sum(v) OVER w FROM t WINDOW w AS (ORDER BY v) ORDER BY v",
        )
        .await;
        assert_eq!(
            texts(&sel),
            vec![
                vec!["5".to_owned(), "1".to_owned(), "5".to_owned()],
                vec!["10".to_owned(), "2".to_owned(), "15".to_owned()],
                vec!["15".to_owned(), "3".to_owned(), "30".to_owned()],
                vec!["20".to_owned(), "4".to_owned(), "70".to_owned()],
                vec!["20".to_owned(), "5".to_owned(), "70".to_owned()],
            ],
            "named WINDOW w reused"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// THE M7 MILESTONE: two-table INNER joins over the wire (the cost-chosen
    /// nestloop/hash/merge method). Over a(x int) {1,2,3,4} and b(y int, z int)
    /// {(2,20),(3,30),(3,31),(5,50)}:
    ///   - `SELECT a.x, b.z FROM a JOIN b ON a.x = b.y`  (explicit JOIN syntax)
    ///   - `SELECT a.x, b.z FROM a, b WHERE a.x = b.y`   (comma cross-join + WHERE)
    /// both return the matched rows {(2,20),(3,30),(3,31)}. A 3-table join chains.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m7_joins_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE a (x int)").await;
        simple_query(&mut client, &mut buf, "CREATE TABLE b (y int, z int)").await;
        for v in [1, 2, 3, 4] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO a VALUES ({v})")).await;
        }
        for (y, z) in [(2, 20), (3, 30), (3, 31), (5, 50)] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO b VALUES ({y}, {z})")).await;
        }

        let texts = |msgs: &[Msg]| -> Vec<Vec<String>> {
            let mut rows: Vec<Vec<String>> =
                msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_texts(&m.body)).collect();
            rows.sort();
            rows
        };
        let expected = vec![
            vec!["2".to_owned(), "20".to_owned()],
            vec!["3".to_owned(), "30".to_owned()],
            vec!["3".to_owned(), "31".to_owned()],
        ];

        // Explicit JOIN ... ON syntax.
        let sel = simple_query(&mut client, &mut buf, "SELECT a.x, b.z FROM a JOIN b ON a.x = b.y").await;
        let types: Vec<u8> = sel.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'D', b'D', b'C', b'Z'],
            "RowDescription + 3 join DataRows + CommandComplete + ReadyForQuery"
        );
        assert_eq!(texts(&sel), expected, "SELECT a.x, b.z FROM a JOIN b ON a.x = b.y");

        // Comma cross-join + WHERE equivalent (same result set).
        let sel = simple_query(&mut client, &mut buf, "SELECT a.x, b.z FROM a, b WHERE a.x = b.y").await;
        assert_eq!(texts(&sel), expected, "SELECT a.x, b.z FROM a, b WHERE a.x = b.y");

        // INNER JOIN spelling.
        let sel = simple_query(&mut client, &mut buf, "SELECT a.x, b.z FROM a INNER JOIN b ON a.x = b.y").await;
        assert_eq!(texts(&sel), expected, "INNER JOIN spelling");

        // A 3-table join: a JOIN b JOIN c on the shared key. c(w int) {3}.
        simple_query(&mut client, &mut buf, "CREATE TABLE c (w int)").await;
        simple_query(&mut client, &mut buf, "INSERT INTO c VALUES (3)").await;
        let sel = simple_query(
            &mut client,
            &mut buf,
            "SELECT a.x, b.z FROM a JOIN b ON a.x = b.y JOIN c ON b.y = c.w",
        )
        .await;
        // Only the b.y = 3 rows survive the c.w = 3 join: (3,30) and (3,31).
        assert_eq!(
            texts(&sel),
            vec![vec!["3".to_owned(), "30".to_owned()], vec!["3".to_owned(), "31".to_owned()]],
            "3-table join a JOIN b JOIN c"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// THE M8 MILESTONE: UPDATE / DELETE / RETURNING / FOR UPDATE over the wire.
    /// Over t(a int, b int) seeded with {(1,10),(2,20),(3,30),(5,50)}:
    ///   - `UPDATE t SET a = a + 1 WHERE b > 0`     -> 4 rows, every a bumped.
    ///   - `DELETE FROM t WHERE a = 5`              (after the bump, no a=5) -> 0 rows.
    ///   - `DELETE FROM t WHERE a = 6`              -> 1 row (the bumped (5,50)).
    ///   - `UPDATE t SET a = 1 RETURNING a, b`      -> the modified rows projected.
    ///   - `DELETE FROM t WHERE b = 10 RETURNING *` -> the deleted row projected.
    ///   - `SELECT a FROM t FOR UPDATE`             -> rows returned (rows locked).
    ///   - `UPDATE t SET a = 99 WHERE a = 12345`    -> 0 rows (no error).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m8_update_delete_returning_for_update_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int, b int)").await;
        for (a, b) in [(1, 10), (2, 20), (3, 30), (5, 50)] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({a}, {b})")).await;
        }

        let single = |msgs: &[Msg]| -> Vec<String> {
            msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect()
        };
        let texts = |msgs: &[Msg]| -> Vec<Vec<String>> {
            let mut rows: Vec<Vec<String>> =
                msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_texts(&m.body)).collect();
            rows.sort();
            rows
        };
        let tag = |msgs: &[Msg]| -> String {
            let c = msgs.iter().find(|m| m.ty == b'C').expect("CommandComplete");
            let end = c.body.iter().position(|&x| x == 0).unwrap_or(c.body.len());
            String::from_utf8_lossy(&c.body[..end]).into_owned()
        };

        // UPDATE ... SET a = a + 1 WHERE b > 0: all 4 rows bumped.
        let upd = simple_query(&mut client, &mut buf, "UPDATE t SET a = a + 1 WHERE b > 0").await;
        assert_eq!(tag(&upd), "UPDATE 4", "UPDATE affected-row count");
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t").await;
        assert_eq!(single(&sel), vec!["2", "3", "4", "6"], "new a values visible");

        // DELETE FROM t WHERE a = 5 -> 0 rows (a=5 was bumped to 6).
        let del0 = simple_query(&mut client, &mut buf, "DELETE FROM t WHERE a = 5").await;
        assert_eq!(tag(&del0), "DELETE 0", "DELETE matching zero rows -> 0, no error");

        // DELETE FROM t WHERE a = 6 -> 1 row (the bumped (5,50)).
        let del1 = simple_query(&mut client, &mut buf, "DELETE FROM t WHERE a = 6").await;
        assert_eq!(tag(&del1), "DELETE 1", "DELETE affected-row count");
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM t").await;
        assert_eq!(single(&sel), vec!["2", "3", "4"], "row gone after DELETE");

        // UPDATE ... RETURNING a, b: the modified rows are projected.
        let upd_ret =
            simple_query(&mut client, &mut buf, "UPDATE t SET a = 1 RETURNING a, b").await;
        let types: Vec<u8> = upd_ret.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'D', b'D', b'C', b'Z'],
            "RowDescription + 3 RETURNING DataRows + CommandComplete + ReadyForQuery"
        );
        // Remaining rows are {(2,10),(3,20),(4,30)} (b unchanged by the a bump), so
        // RETURNING a, b yields a=1 with b in {10,20,30}.
        assert_eq!(
            texts(&upd_ret),
            vec![
                vec!["1".to_owned(), "10".to_owned()],
                vec!["1".to_owned(), "20".to_owned()],
                vec!["1".to_owned(), "30".to_owned()],
            ],
            "UPDATE ... RETURNING a, b projects the new rows (a forced to 1)"
        );

        // DELETE ... RETURNING *: the deleted row (1,20) is projected.
        let del_ret =
            simple_query(&mut client, &mut buf, "DELETE FROM t WHERE b = 20 RETURNING *").await;
        assert_eq!(
            texts(&del_ret),
            vec![vec!["1".to_owned(), "20".to_owned()]],
            "DELETE ... RETURNING * projects the deleted row"
        );

        // SELECT ... FOR UPDATE: the remaining rows {(1,10),(1,30)} are returned
        // (and locked). a is 1 for both; check b.
        let sel_fu = simple_query(&mut client, &mut buf, "SELECT b FROM t FOR UPDATE").await;
        let mut fu = single(&sel_fu);
        fu.sort();
        assert_eq!(fu, vec!["10", "30"], "SELECT ... FOR UPDATE returns the locked rows");

        // UPDATE matching zero rows -> 0 affected, no error.
        let upd_none =
            simple_query(&mut client, &mut buf, "UPDATE t SET a = 99 WHERE a = 12345").await;
        assert_eq!(tag(&upd_none), "UPDATE 0", "UPDATE matching zero rows -> 0, no error");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M10 (step 38): ALTER TABLE / RENAME / DROP over the wire. Exercises the full
    /// DDL dispatch substrate end-to-end: ADD/DROP/RENAME COLUMN, RENAME TABLE,
    /// SET DEFAULT, DROP TABLE (+ IF EXISTS, + a dependent index dropped with it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m10_alter_drop_rename_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let tag = |msgs: &[Msg]| -> String {
            let c = msgs.iter().find(|m| m.ty == b'C').expect("CommandComplete");
            let end = c.body.iter().position(|&x| x == 0).unwrap_or(c.body.len());
            String::from_utf8_lossy(&c.body[..end]).into_owned()
        };
        let names = |msgs: &[Msg]| -> Vec<String> {
            let t = msgs.iter().find(|m| m.ty == b'T').expect("RowDescription");
            rowdescription_field_names(&t.body)
        };

        // CREATE TABLE t(a int).
        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        simple_query(&mut client, &mut buf, "INSERT INTO t VALUES (1)").await;

        // ALTER TABLE t ADD COLUMN b text -> SELECT * sees a, b.
        let add = simple_query(&mut client, &mut buf, "ALTER TABLE t ADD COLUMN b text").await;
        assert_eq!(tag(&add), "ALTER TABLE", "ADD COLUMN completion tag");
        let sel = simple_query(&mut client, &mut buf, "SELECT * FROM t").await;
        assert_eq!(names(&sel), vec!["a".to_owned(), "b".to_owned()], "SELECT * shows a, b");

        // ALTER TABLE t DROP COLUMN b -> SELECT * shows only a.
        simple_query(&mut client, &mut buf, "ALTER TABLE t DROP COLUMN b").await;
        let sel = simple_query(&mut client, &mut buf, "SELECT * FROM t").await;
        assert_eq!(names(&sel), vec!["a".to_owned()], "b gone from SELECT *");

        // ALTER TABLE t RENAME COLUMN a TO x.
        simple_query(&mut client, &mut buf, "ALTER TABLE t RENAME COLUMN a TO x").await;
        let sel = simple_query(&mut client, &mut buf, "SELECT * FROM t").await;
        assert_eq!(names(&sel), vec!["x".to_owned()], "a renamed to x");

        // ALTER TABLE t ALTER COLUMN x SET DEFAULT 5 (records the default; the
        // completion tag confirms the path).
        let sd = simple_query(&mut client, &mut buf, "ALTER TABLE t ALTER COLUMN x SET DEFAULT 5").await;
        assert_eq!(tag(&sd), "ALTER TABLE", "SET DEFAULT completion tag");

        // ALTER TABLE t RENAME TO t2 -> SELECT FROM t2 works, t errors.
        simple_query(&mut client, &mut buf, "ALTER TABLE t RENAME TO t2").await;
        let sel = simple_query(&mut client, &mut buf, "SELECT x FROM t2").await;
        assert!(sel.iter().any(|m| m.ty == b'T'), "SELECT from t2 works");
        // The old name no longer resolves: the query errors server-side (ErrorResponse
        // is not yet sent to the client at this milestone -- the response carries no
        // RowDescription, only ReadyForQuery).
        let err = simple_query(&mut client, &mut buf, "SELECT x FROM t").await;
        assert!(!err.iter().any(|m| m.ty == b'T'), "old name t no longer resolves");

        // CREATE INDEX, then DROP TABLE t2 drops the index too (dependency walk).
        simple_query(&mut client, &mut buf, "CREATE INDEX t2_x_idx ON t2 (x)").await;
        let drop_msgs = simple_query(&mut client, &mut buf, "DROP TABLE t2").await;
        assert_eq!(tag(&drop_msgs), "DROP TABLE", "DROP TABLE completion tag");
        // SELECT FROM t2 now errors (relation gone): no RowDescription in the reply.
        let gone = simple_query(&mut client, &mut buf, "SELECT * FROM t2").await;
        assert!(!gone.iter().any(|m| m.ty == b'T'), "t2 is gone after DROP");

        // DROP TABLE IF EXISTS on a missing table -> notice, no error (CommandComplete).
        let if_exists = simple_query(&mut client, &mut buf, "DROP TABLE IF EXISTS nosuch").await;
        assert_eq!(tag(&if_exists), "DROP TABLE", "IF EXISTS on missing is not an error");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Step 39 (M10 object DDL) over the wire: CREATE SCHEMA, schema-qualified
    /// CREATE TABLE + SELECT, DROP SCHEMA; ALTER COLUMN SET DEFAULT then an INSERT
    /// omitting the column lands the default; ADD CONSTRAINT CHECK; and a phase-B
    /// command (CREATE FUNCTION) routes to its not-yet-translated body without a
    /// parse error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m10_object_ddl_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let tag = |msgs: &[Msg]| -> String {
            let c = msgs.iter().find(|m| m.ty == b'C').expect("CommandComplete");
            let end = c.body.iter().position(|&x| x == 0).unwrap_or(c.body.len());
            String::from_utf8_lossy(&c.body[..end]).into_owned()
        };
        let single_vals = |msgs: &[Msg]| -> Vec<String> {
            msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect()
        };

        // CREATE SCHEMA s; schema-qualified table in it; SELECT resolves through it.
        let cs = simple_query(&mut client, &mut buf, "CREATE SCHEMA s").await;
        assert_eq!(tag(&cs), "CREATE SCHEMA", "CREATE SCHEMA completion tag");
        simple_query(&mut client, &mut buf, "CREATE TABLE s.t (a int)").await;
        simple_query(&mut client, &mut buf, "INSERT INTO s.t VALUES (7)").await;
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM s.t").await;
        assert_eq!(single_vals(&sel), vec!["7".to_owned()], "schema-qualified SELECT works");

        // SET DEFAULT then INSERT omitting the column -> the default lands.
        simple_query(&mut client, &mut buf, "CREATE TABLE d (a int, b int)").await;
        let sd = simple_query(&mut client, &mut buf, "ALTER TABLE d ALTER COLUMN b SET DEFAULT 5").await;
        assert_eq!(tag(&sd), "ALTER TABLE", "SET DEFAULT completion tag");
        simple_query(&mut client, &mut buf, "INSERT INTO d (a) VALUES (1)").await;
        let sel = simple_query(&mut client, &mut buf, "SELECT b FROM d").await;
        assert_eq!(single_vals(&sel), vec!["5".to_owned()], "omitted column gets its DEFAULT 5");

        // ADD CONSTRAINT CHECK: the completion tag confirms the pg_constraint store.
        let ac = simple_query(&mut client, &mut buf, "ALTER TABLE d ADD CONSTRAINT d_b_pos CHECK (b > 0)").await;
        assert_eq!(tag(&ac), "ALTER TABLE", "ADD CONSTRAINT completion tag");

        // CREATE SEQUENCE parses + routes (DefineSequence runs; the relation lands).
        let cseq = simple_query(&mut client, &mut buf, "CREATE SEQUENCE seq START WITH 1").await;
        assert_eq!(tag(&cseq), "CREATE SEQUENCE", "CREATE SEQUENCE completion tag");

        // DROP SCHEMA s (CASCADE drops s.t too).
        let ds = simple_query(&mut client, &mut buf, "DROP SCHEMA s CASCADE").await;
        assert_eq!(tag(&ds), "DROP SCHEMA", "DROP SCHEMA completion tag");
        let gone = simple_query(&mut client, &mut buf, "SELECT a FROM s.t").await;
        assert!(!gone.iter().any(|m| m.ty == b'T'), "s.t is gone after DROP SCHEMA");

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

    /// THE M6 MILESTONE: `CREATE INDEX i ON t(a)` then `SELECT * FROM t WHERE a = v`
    /// over the wire returns the right rows through the index. The table is populated
    /// to several heap pages so the cost-based planner prefers the index/bitmap path
    /// over a seqscan for the selective qual; the plan-choice assertion (IndexScan /
    /// BitmapHeapScan over SeqScan) is verified in the inline planner test
    /// `index_plan_chosen_over_seqscan`. Here we assert the rows are correct.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m6_create_index_and_index_scan_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let create = simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        assert_eq!(create[0].body, b"CREATE TABLE\0");

        // Populate t with 0..600 (a multi-page heap) so a selective WHERE qual makes
        // the index/bitmap path the cheapest plan. (Multi-row VALUES is staged, so
        // these are single-row INSERTs.)
        for v in 0..600 {
            let ins = simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
            assert_eq!(ins[0].body, b"INSERT 0 1\0", "INSERT {v}");
        }

        // CREATE INDEX i ON t (a) -> CommandComplete "CREATE INDEX" + ReadyForQuery.
        let ci = simple_query(&mut client, &mut buf, "CREATE INDEX i ON t (a)").await;
        let types: Vec<u8> = ci.iter().map(|m| m.ty).collect();
        assert_eq!(types, vec![b'C', b'Z'], "CREATE INDEX: CommandComplete + ReadyForQuery");
        assert_eq!(ci[0].body, b"CREATE INDEX\0");

        // SELECT * FROM t WHERE a = 20 -> RowDescription + one DataRow "20" + CC + RFQ,
        // served through the index.
        let sel = simple_query(&mut client, &mut buf, "SELECT * FROM t WHERE a = 20").await;
        let types: Vec<u8> = sel.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'T', b'D', b'C', b'Z'],
            "point lookup: RowDescription + one DataRow + CommandComplete + ReadyForQuery"
        );
        let values: Vec<String> = sel
            .iter()
            .filter(|m| m.ty == b'D')
            .map(|m| datarow_single_text(&m.body))
            .collect();
        assert_eq!(values, vec!["20"], "a = 20 returns the one matching row via the index");
        assert_eq!(sel[2].body, b"SELECT 1\0");

        // A non-matching point lookup returns no rows.
        let none = simple_query(&mut client, &mut buf, "SELECT * FROM t WHERE a = 1000").await;
        assert!(none.iter().all(|m| m.ty != b'D'), "a = 1000 matches no row");
        let cc = none.iter().find(|m| m.ty == b'C').expect("a CommandComplete");
        assert_eq!(cc.body, b"SELECT 0\0");

        // A selective range scan WHERE a > 595 -> {596, 597, 598, 599} (4 rows). The
        // planner may pick the plain IndexScan (index order) or the BitmapHeapScan
        // (physical order), so compare the result set, not the row order.
        let rng = simple_query(&mut client, &mut buf, "SELECT * FROM t WHERE a > 595").await;
        let mut rvals: Vec<i32> = rng
            .iter()
            .filter(|m| m.ty == b'D')
            .map(|m| datarow_single_text(&m.body).parse().expect("int4 text"))
            .collect();
        rvals.sort_unstable();
        assert_eq!(rvals, vec![596, 597, 598, 599], "a > 595 returns the four rows above 595");
        let cc = rng.iter().find(|m| m.ty == b'C').expect("a CommandComplete");
        assert_eq!(cc.body, b"SELECT 4\0");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// The CommandComplete tag string of a reply (without the trailing NUL).
    fn complete_tag(msgs: &[Msg]) -> String {
        let c = msgs.iter().find(|m| m.ty == b'C').expect("a CommandComplete");
        let end = c.body.iter().position(|&x| x == 0).unwrap_or(c.body.len());
        String::from_utf8_lossy(&c.body[..end]).into_owned()
    }

    /// The single-column text value of every DataRow in a reply.
    fn reply_single_col(msgs: &[Msg]) -> Vec<String> {
        msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_single_text(&m.body)).collect()
    }

    /// THE M9 MILESTONE: transaction control + SET/SHOW/RESET over the wire.
    ///   - BEGIN; INSERT; COMMIT;          -> the row persists.
    ///   - BEGIN; INSERT; ROLLBACK;        -> the row is gone.
    ///   - BEGIN; INSERT a; SAVEPOINT s; INSERT b; ROLLBACK TO s; COMMIT;
    ///       -> a persists, b is gone.
    ///   - BEGIN; <error>; SELECT; ROLLBACK;
    ///       -> the failed block reports RFQ status 'E' and the SELECT returns no
    ///          rows (rejected with "current transaction is aborted"); ROLLBACK
    ///          recovers.
    ///   - SET / SHOW / RESET round-trips; SET LOCAL reverts on ROLLBACK.
    ///   - COMMIT with no open block -> not an error (RFQ idle).
    ///
    /// NOTE: ErrorResponse / NoticeResponse are not yet sent to the client
    /// (send_message_to_frontend is a deferred stub, elog.rs), so the test reads the
    /// ReadyForQuery transaction-status byte ('I'/'T'/'E') and the observable
    /// behavior rather than 'E'/'N' messages.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m9_transaction_control_and_set_show_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;
        let c = &mut client;
        let b = &mut buf;

        simple_query(c, b, "CREATE TABLE t (a int)").await;

        // --- BEGIN/COMMIT: the row persists. -----------------------------------
        let begin = simple_query(c, b, "BEGIN").await;
        assert_eq!(complete_tag(&begin), "BEGIN", "BEGIN completion tag");
        assert_eq!(begin.iter().find(|m| m.ty == b'Z').unwrap().body, b"T", "RFQ status 'T' in xact");
        simple_query(c, b, "INSERT INTO t VALUES (1)").await;
        let commit = simple_query(c, b, "COMMIT").await;
        assert_eq!(complete_tag(&commit), "COMMIT", "COMMIT completion tag");
        assert_eq!(commit.iter().find(|m| m.ty == b'Z').unwrap().body, b"I", "RFQ idle after COMMIT");
        let sel = simple_query(c, b, "SELECT a FROM t").await;
        assert_eq!(reply_single_col(&sel), vec!["1"], "committed row persists");

        // --- BEGIN/ROLLBACK: the row is gone. ----------------------------------
        simple_query(c, b, "BEGIN").await;
        simple_query(c, b, "INSERT INTO t VALUES (2)").await;
        let rb = simple_query(c, b, "ROLLBACK").await;
        assert_eq!(complete_tag(&rb), "ROLLBACK", "ROLLBACK completion tag");
        let sel = simple_query(c, b, "SELECT a FROM t").await;
        assert_eq!(reply_single_col(&sel), vec!["1"], "rolled-back row is gone");

        // --- SAVEPOINT / ROLLBACK TO: a persists, b is gone. -------------------
        simple_query(c, b, "BEGIN").await;
        simple_query(c, b, "INSERT INTO t VALUES (10)").await; // a
        let sp = simple_query(c, b, "SAVEPOINT s").await;
        assert_eq!(complete_tag(&sp), "SAVEPOINT", "SAVEPOINT completion tag");
        simple_query(c, b, "INSERT INTO t VALUES (20)").await; // b
        let rbto = simple_query(c, b, "ROLLBACK TO s").await;
        assert_eq!(complete_tag(&rbto), "ROLLBACK", "ROLLBACK TO completion tag");
        simple_query(c, b, "COMMIT").await;
        let sel = simple_query(c, b, "SELECT a FROM t").await;
        let mut vals: Vec<i32> =
            reply_single_col(&sel).iter().map(|s| s.parse().unwrap()).collect();
        vals.sort_unstable();
        assert_eq!(vals, vec![1, 10], "savepoint: a (10) persists, b (20) rolled back");

        // --- Aborted block: an error poisons the block; non-exit stmts rejected. -
        simple_query(c, b, "BEGIN").await;
        // A statement that errors: insert into a non-existent table. The errored
        // command leaves the block in the failed state (RFQ status 'E').
        let err = simple_query(c, b, "INSERT INTO nosuchtable VALUES (1)").await;
        assert_eq!(
            err.iter().find(|m| m.ty == b'Z').unwrap().body,
            b"E",
            "errored statement -> RFQ status 'E' (failed transaction)"
        );
        // Now SELECT is rejected: no RowDescription / DataRow, still 'E'.
        let rej = simple_query(c, b, "SELECT a FROM t").await;
        assert!(!rej.iter().any(|m| m.ty == b'D'), "rejected SELECT returns no rows");
        assert!(!rej.iter().any(|m| m.ty == b'C'), "rejected SELECT has no CommandComplete");
        assert_eq!(
            rej.iter().find(|m| m.ty == b'Z').unwrap().body,
            b"E",
            "still in failed-transaction state after rejection"
        );
        // ROLLBACK recovers the session (RFQ back to idle).
        let rb = simple_query(c, b, "ROLLBACK").await;
        assert_eq!(complete_tag(&rb), "ROLLBACK", "ROLLBACK tag after a failed block");
        assert_eq!(rb.iter().find(|m| m.ty == b'Z').unwrap().body, b"I", "RFQ idle after ROLLBACK");
        let sel = simple_query(c, b, "SELECT a FROM t").await;
        let mut vals: Vec<i32> =
            reply_single_col(&sel).iter().map(|s| s.parse().unwrap()).collect();
        vals.sort_unstable();
        assert_eq!(vals, vec![1, 10], "session recovers after ROLLBACK; table unchanged");

        // --- SET / SHOW / RESET round-trip. ------------------------------------
        let set = simple_query(c, b, "SET application_name = 'pepper'").await;
        assert_eq!(complete_tag(&set), "SET", "SET completion tag");
        let show = simple_query(c, b, "SHOW application_name").await;
        assert!(show.iter().any(|m| m.ty == b'T'), "SHOW emits a RowDescription");
        assert_eq!(complete_tag(&show), "SHOW", "SHOW completion tag");
        assert_eq!(reply_single_col(&show), vec!["pepper"], "SHOW reflects the SET value");

        let reset = simple_query(c, b, "RESET application_name").await;
        assert_eq!(complete_tag(&reset), "RESET", "RESET completion tag");
        let show = simple_query(c, b, "SHOW application_name").await;
        assert_eq!(reply_single_col(&show), vec![""], "RESET restores the boot default (empty)");

        // --- SET LOCAL reverts on ROLLBACK. ------------------------------------
        simple_query(c, b, "SET application_name = 'outer'").await;
        simple_query(c, b, "BEGIN").await;
        simple_query(c, b, "SET LOCAL application_name = 'inner'").await;
        let show = simple_query(c, b, "SHOW application_name").await;
        assert_eq!(reply_single_col(&show), vec!["inner"], "SET LOCAL visible inside the block");
        simple_query(c, b, "ROLLBACK").await;
        let show = simple_query(c, b, "SHOW application_name").await;
        assert_eq!(reply_single_col(&show), vec!["outer"], "SET LOCAL reverted after ROLLBACK");

        // --- COMMIT with no open block -> a WARNING, not an error. -------------
        // PG emits a "there is no transaction in progress" WARNING (a NoticeResponse,
        // not yet wired to the client) and still completes successfully; the session
        // stays idle ('I') rather than entering the failed state.
        let commit = simple_query(c, b, "COMMIT").await;
        assert_eq!(complete_tag(&commit), "COMMIT", "stray COMMIT still tagged COMMIT");
        assert_eq!(
            commit.iter().find(|m| m.ty == b'Z').unwrap().body,
            b"I",
            "stray COMMIT leaves the session idle, not failed"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// THE M9 MILESTONE (prepared statements): PREPARE / EXECUTE / DEALLOCATE over
    /// the wire. `PREPARE p(int) AS SELECT $1 + 1` then `EXECUTE p(41)` -> 42,
    /// `EXECUTE p(7)` -> 8; `DEALLOCATE p` then `EXECUTE p(1)` -> the prepared
    /// statement no longer exists (failed-transaction status on the RFQ).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m9_prepare_execute_deallocate_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;
        let c = &mut client;
        let b = &mut buf;

        let prep = simple_query(c, b, "PREPARE p(int) AS SELECT $1 + 1").await;
        assert_eq!(complete_tag(&prep), "PREPARE", "PREPARE completion tag");

        // EXECUTE p(41) -> 42.
        let ex = simple_query(c, b, "EXECUTE p(41)").await;
        assert_eq!(reply_single_col(&ex), vec!["42"], "EXECUTE p(41) = 42");

        // EXECUTE p(7) -> 8 (re-execute the same prepared statement).
        let ex = simple_query(c, b, "EXECUTE p(7)").await;
        assert_eq!(reply_single_col(&ex), vec!["8"], "EXECUTE p(7) = 8");

        // DEALLOCATE p.
        let dealloc = simple_query(c, b, "DEALLOCATE p").await;
        assert_eq!(complete_tag(&dealloc), "DEALLOCATE", "DEALLOCATE completion tag");

        // EXECUTE p again -> error: the prepared statement no longer exists. The
        // errored statement leaves the (implicit) command in the failed state; no
        // DataRow / CommandComplete is produced for the rejected EXECUTE.
        let gone = simple_query(c, b, "EXECUTE p(1)").await;
        assert!(!gone.iter().any(|m| m.ty == b'D'), "deallocated EXECUTE returns no rows");
        assert!(!gone.iter().any(|m| m.ty == b'C'), "deallocated EXECUTE has no CommandComplete");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// THE M9 MILESTONE (cursors): DECLARE / FETCH / MOVE / CLOSE over the wire,
    /// inside a transaction block, over a multi-row table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m9_declare_fetch_close_cursor_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;
        let c = &mut client;
        let b = &mut buf;

        simple_query(c, b, "CREATE TABLE t (a int)").await;
        for v in [1, 2, 3, 4, 5] {
            simple_query(c, b, &format!("INSERT INTO t VALUES ({v})")).await;
        }

        // A cursor requires a transaction block.
        simple_query(c, b, "BEGIN").await;
        let decl = simple_query(c, b, "DECLARE cur CURSOR FOR SELECT a FROM t ORDER BY a").await;
        assert_eq!(complete_tag(&decl), "DECLARE CURSOR", "DECLARE CURSOR tag");

        // FETCH 2 -> the first two rows.
        let f2 = simple_query(c, b, "FETCH 2 FROM cur").await;
        assert_eq!(reply_single_col(&f2), vec!["1", "2"], "FETCH 2 -> first two rows");
        assert_eq!(complete_tag(&f2), "FETCH 2", "FETCH 2 row count");

        // FETCH ALL -> the remaining three rows.
        let fall = simple_query(c, b, "FETCH ALL FROM cur").await;
        assert_eq!(reply_single_col(&fall), vec!["3", "4", "5"], "FETCH ALL -> remainder");
        assert_eq!(complete_tag(&fall), "FETCH 3", "FETCH ALL row count");

        // FETCH past end -> zero rows.
        let fend = simple_query(c, b, "FETCH 2 FROM cur").await;
        assert!(!fend.iter().any(|m| m.ty == b'D'), "FETCH past end -> no rows");
        assert_eq!(complete_tag(&fend), "FETCH 0", "FETCH past end -> 0");

        // MOVE BACKWARD then FETCH re-reads (scrollable materialized store). After
        // FETCH ALL the cursor sits past row 5; MOVE BACKWARD 2 lands on row 4, and
        // FETCH FORWARD 1 returns the next row -> row 5 (PG cursor semantics).
        let mv = simple_query(c, b, "MOVE BACKWARD 2 FROM cur").await;
        assert_eq!(complete_tag(&mv), "MOVE 2", "MOVE BACKWARD 2");
        let refetch = simple_query(c, b, "FETCH 1 FROM cur").await;
        assert_eq!(reply_single_col(&refetch), vec!["5"], "FETCH FORWARD after MOVE BACKWARD 2 -> row 5");

        // CLOSE the cursor; a later FETCH errors (cursor does not exist) and the
        // block enters the failed state.
        let close = simple_query(c, b, "CLOSE cur").await;
        assert_eq!(complete_tag(&close), "CLOSE CURSOR", "CLOSE CURSOR tag");
        let after = simple_query(c, b, "FETCH 1 FROM cur").await;
        assert!(!after.iter().any(|m| m.ty == b'D'), "FETCH on a closed cursor -> no rows");
        assert!(!after.iter().any(|m| m.ty == b'C'), "FETCH on a closed cursor -> no CommandComplete");

        simple_query(c, b, "ROLLBACK").await;

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// THE M9 MILESTONE (extended protocol): a Parse / Bind / Describe / Execute /
    /// Sync sequence for a parameterized `SELECT $1 + 1` over the wire. Asserts the
    /// ParseComplete / BindComplete / RowDescription / DataRow(42) / CommandComplete
    /// / ReadyForQuery sequence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m9_extended_protocol_parse_bind_execute_over_the_wire() {
        use crate::libpq::protocol::{
            PQMSG_BIND, PQMSG_DESCRIBE, PQMSG_EXECUTE, PQMSG_PARSE, PQMSG_SYNC,
        };
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        // --- Parse ('P'): statement "" / query "SELECT $1 + 1" / 1 param type int4.
        let mut parse_body = Vec::new();
        parse_body.extend_from_slice(b"\0"); // unnamed statement
        parse_body.extend_from_slice(b"SELECT $1 + 1\0");
        parse_body.extend_from_slice(&1u16.to_be_bytes()); // 1 param type
        parse_body.extend_from_slice(&23u32.to_be_bytes()); // int4 OID

        // --- Bind ('B'): portal "" / statement "" / 0 pformats / 1 param "41" / 0 rformats.
        let mut bind_body = Vec::new();
        bind_body.extend_from_slice(b"\0"); // unnamed portal
        bind_body.extend_from_slice(b"\0"); // unnamed statement
        bind_body.extend_from_slice(&0u16.to_be_bytes()); // 0 param format codes (all text)
        bind_body.extend_from_slice(&1u16.to_be_bytes()); // 1 param value
        bind_body.extend_from_slice(&2u32.to_be_bytes()); // length 2
        bind_body.extend_from_slice(b"41");
        bind_body.extend_from_slice(&0u16.to_be_bytes()); // 0 result format codes

        // --- Describe ('D'): portal "".
        let mut describe_body = Vec::new();
        describe_body.push(b'P');
        describe_body.extend_from_slice(b"\0");

        // --- Execute ('E'): portal "" / max 0 (all rows).
        let mut execute_body = Vec::new();
        execute_body.extend_from_slice(b"\0");
        execute_body.extend_from_slice(&0u32.to_be_bytes());

        let mut msgs = Vec::new();
        msgs.extend_from_slice(&framed(PQMSG_PARSE, &parse_body));
        msgs.extend_from_slice(&framed(PQMSG_BIND, &bind_body));
        msgs.extend_from_slice(&framed(PQMSG_DESCRIBE, &describe_body));
        msgs.extend_from_slice(&framed(PQMSG_EXECUTE, &execute_body));
        msgs.extend_from_slice(&framed(PQMSG_SYNC, &[]));

        let target_z = decode(&buf).iter().filter(|m| m.ty == b'Z').count() + 1;
        client.write_all(&msgs).await.expect("write extended-protocol batch");
        client.flush().await.expect("flush");
        let before = decode(&buf).len();
        let all = read_until(&mut client, &mut buf, |m| {
            m.iter().filter(|x| x.ty == b'Z').count() >= target_z
        })
        .await;
        let reply = &all[before..];
        let types: Vec<u8> = reply.iter().map(|m| m.ty).collect();
        // ParseComplete '1', BindComplete '2', RowDescription 'T', DataRow 'D',
        // CommandComplete 'C', ReadyForQuery 'Z'.
        assert_eq!(
            types,
            vec![b'1', b'2', b'T', b'D', b'C', b'Z'],
            "Parse/Bind/Describe/Execute/Sync reply sequence"
        );
        let datarow = reply.iter().find(|m| m.ty == b'D').expect("a DataRow");
        assert_eq!(datarow_single_text(&datarow.body), "42", "SELECT $1 + 1 with $1 = 41 -> 42");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Step 39: CREATE SCHEMA inserts a pg_namespace row that a schema-qualified
    /// CREATE TABLE / SELECT can resolve; DROP SCHEMA removes it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m10_create_schema_table_drop_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let r = simple_query(&mut client, &mut buf, "CREATE SCHEMA s").await;
        assert_eq!(r[0].body, b"CREATE SCHEMA\0", "CREATE SCHEMA tag");

        // A table created in s resolves there; a schema-qualified SELECT reads it.
        let r = simple_query(&mut client, &mut buf, "CREATE TABLE s.t (a int)").await;
        assert_eq!(r[0].body, b"CREATE TABLE\0", "schema-qualified CREATE TABLE");
        let r = simple_query(&mut client, &mut buf, "INSERT INTO s.t VALUES (7)").await;
        assert_eq!(r[0].body, b"INSERT 0 1\0");
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM s.t").await;
        let d = sel.iter().find(|m| m.ty == b'D').expect("a DataRow");
        assert_eq!(datarow_single_text(&d.body), "7", "SELECT from schema-qualified table");

        // IF NOT EXISTS on the existing schema is a no-op (still CREATE SCHEMA tag).
        // PG emits a "schema already exists, skipping" NoticeResponse ('N') first,
        // so match the CommandComplete ('C') rather than the leading message.
        let r = simple_query(&mut client, &mut buf, "CREATE SCHEMA IF NOT EXISTS s").await;
        let cc = r.iter().find(|m| m.ty == b'C').expect("CommandComplete");
        assert_eq!(cc.body, b"CREATE SCHEMA\0", "IF NOT EXISTS no-op");
        assert!(r.iter().any(|m| m.ty == b'N'), "IF NOT EXISTS emits a NoticeResponse");

        // Drop the contained table, then the (now-empty) schema (pg_depend-driven
        // CASCADE to contained objects stages with dependency recording).
        let _ = simple_query(&mut client, &mut buf, "DROP TABLE s.t").await;
        let r = simple_query(&mut client, &mut buf, "DROP SCHEMA s").await;
        assert_eq!(r[0].body, b"DROP SCHEMA\0", "DROP SCHEMA tag");

        // After DROP, recreating the same schema name succeeds (the row is gone).
        let r = simple_query(&mut client, &mut buf, "CREATE SCHEMA s").await;
        assert_eq!(r[0].body, b"CREATE SCHEMA\0", "recreate after drop");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Step 39: ALTER TABLE ADD CONSTRAINT CHECK persists a pg_constraint row (now
    /// that pg_constraint is seeded on-disk). DROP CONSTRAINT finds + removes it,
    /// which only succeeds if the row was actually stored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m10_add_check_constraint_persists_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let r = simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        assert_eq!(r[0].body, b"CREATE TABLE\0");

        // ADD CONSTRAINT stores the pg_constraint row.
        let r = simple_query(&mut client, &mut buf, "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)").await;
        assert_eq!(r[0].body, b"ALTER TABLE\0", "ADD CONSTRAINT -> ALTER TABLE tag");

        // DROP CONSTRAINT scans pg_constraint by conrelid + matches conname; it only
        // succeeds (no "constraint does not exist" error) if the row persisted.
        let r = simple_query(&mut client, &mut buf, "ALTER TABLE t DROP CONSTRAINT c").await;
        let types: Vec<u8> = r.iter().map(|m| m.ty).collect();
        assert_eq!(types, vec![b'C', b'Z'], "DROP CONSTRAINT found the persisted row");
        assert_eq!(r[0].body, b"ALTER TABLE\0");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// Step 39 (completes step 38's deferred SET DEFAULT): after ALTER TABLE ... SET
    /// DEFAULT, an INSERT omitting that column fills it from pg_attrdef (seeded
    /// on-disk) via the planner's INSERT target-list expansion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m10_set_default_insert_fills_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let r = simple_query(&mut client, &mut buf, "CREATE TABLE t (a int, b int)").await;
        assert_eq!(r[0].body, b"CREATE TABLE\0");

        // Set a default on column a.
        let r = simple_query(&mut client, &mut buf, "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5").await;
        assert_eq!(r[0].body, b"ALTER TABLE\0", "SET DEFAULT -> ALTER TABLE tag");

        // INSERT naming only b -> a is filled with its default (5).
        let r = simple_query(&mut client, &mut buf, "INSERT INTO t (b) VALUES (9)").await;
        assert_eq!(r[0].body, b"INSERT 0 1\0");

        // SELECT a, b -> the default (5) landed in a; an omitted-no-default column is
        // NULL, but here b was provided (9).
        let sel = simple_query(&mut client, &mut buf, "SELECT a, b FROM t").await;
        let d = sel.iter().find(|m| m.ty == b'D').expect("a DataRow");
        let vals = datarow_texts(&d.body);
        assert_eq!(vals, vec!["5".to_string(), "9".to_string()], "a defaulted to 5, b = 9");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// All text values of every DataRow in a result message stream.
    fn datarow_values(msgs: &[Msg]) -> Vec<Vec<String>> {
        msgs.iter().filter(|m| m.ty == b'D').map(|m| datarow_texts(&m.body)).collect()
    }

    /// THE M11 MILESTONE: CREATE VIEW + SELECT through it. The view's query is stored
    /// as its ON SELECT _RETURN rule; `SELECT * FROM v` is rewritten (view RTE ->
    /// subquery RTE) then the subquery is pulled up into the host query, so the rows
    /// match the underlying SELECT. A further WHERE on top of the view composes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m11_create_view_and_select_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        let r = simple_query(&mut client, &mut buf, "CREATE TABLE t (a int, b int)").await;
        assert_eq!(r[0].body, b"CREATE TABLE\0");
        for (a, b) in [(-1, 10), (1, 20), (2, 30), (3, 40)] {
            let ins = simple_query(
                &mut client, &mut buf, &format!("INSERT INTO t VALUES ({a}, {b})"),
            )
            .await;
            assert_eq!(ins[0].body, b"INSERT 0 1\0");
        }

        // CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0 -> CREATE VIEW tag.
        let r = simple_query(
            &mut client, &mut buf, "CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0",
        )
        .await;
        assert_eq!(r[0].body, b"CREATE VIEW\0", "CREATE VIEW tag");

        // SELECT * FROM v -> the same rows as the underlying SELECT (a>0): {1,2,3}.
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM v").await;
        let vals: Vec<String> = datarow_values(&sel).into_iter().map(|mut r| r.remove(0)).collect();
        assert_eq!(vals, vec!["1", "2", "3"], "view rows = underlying SELECT where a>0");

        // SELECT a FROM v WHERE b > 25 -> the host WHERE composes with the view qual:
        // a>0 AND b>25 keeps (2,30),(3,40) -> a in {2,3}.
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM v WHERE b > 25").await;
        let vals: Vec<String> = datarow_values(&sel).into_iter().map(|mut r| r.remove(0)).collect();
        assert_eq!(vals, vec!["2", "3"], "host WHERE composes with the view's qual");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M11: a view over a two-table join expands + returns joined rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m11_view_over_join_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t1 (id int, x int)").await;
        simple_query(&mut client, &mut buf, "CREATE TABLE t2 (id int, y int)").await;
        for (id, x) in [(1, 100), (2, 200)] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t1 VALUES ({id}, {x})")).await;
        }
        for (id, y) in [(1, 11), (2, 22)] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t2 VALUES ({id}, {y})")).await;
        }

        let r = simple_query(
            &mut client,
            &mut buf,
            "CREATE VIEW jv AS SELECT t1.x, t2.y FROM t1, t2 WHERE t1.id = t2.id",
        )
        .await;
        assert_eq!(r[0].body, b"CREATE VIEW\0", "CREATE VIEW over a join");

        let sel = simple_query(&mut client, &mut buf, "SELECT x, y FROM jv").await;
        let rows = datarow_values(&sel);
        // The join on id matches (1<->1, 2<->2): (100,11),(200,22).
        assert!(rows.contains(&vec!["100".to_string(), "11".to_string()]), "row (100,11): {rows:?}");
        assert!(rows.contains(&vec!["200".to_string(), "22".to_string()]), "row (200,22): {rows:?}");
        assert_eq!(rows.len(), 2, "exactly the two joined rows: {rows:?}");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M11: a view defined over another view expands recursively.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m11_nested_view_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        for v in [1, 2, 3, 4, 5] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
        }
        // v1 keeps a > 1; v2 (on v1) keeps a < 5 -> the composition keeps {2,3,4}.
        simple_query(&mut client, &mut buf, "CREATE VIEW v1 AS SELECT a FROM t WHERE a > 1").await;
        let r = simple_query(&mut client, &mut buf, "CREATE VIEW v2 AS SELECT a FROM v1 WHERE a < 5").await;
        assert_eq!(r[0].body, b"CREATE VIEW\0", "CREATE VIEW over a view");

        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM v2").await;
        let vals: Vec<String> = datarow_values(&sel).into_iter().map(|mut r| r.remove(0)).collect();
        assert_eq!(vals, vec!["2", "3", "4"], "nested view: a>1 AND a<5");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// M11: CREATE OR REPLACE VIEW installs a new definition; DROP VIEW removes it
    /// (a subsequent SELECT errors).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m11_replace_and_drop_view_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;

        simple_query(&mut client, &mut buf, "CREATE TABLE t (a int)").await;
        for v in [1, 2, 3] {
            simple_query(&mut client, &mut buf, &format!("INSERT INTO t VALUES ({v})")).await;
        }
        simple_query(&mut client, &mut buf, "CREATE VIEW v AS SELECT a FROM t WHERE a > 1").await;
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM v").await;
        let vals: Vec<String> = datarow_values(&sel).into_iter().map(|mut r| r.remove(0)).collect();
        assert_eq!(vals, vec!["2", "3"], "original view: a>1");

        // CREATE OR REPLACE VIEW with a new qual (a < 3) -> the definition changes.
        let r = simple_query(&mut client, &mut buf, "CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE a < 3").await;
        assert_eq!(r[0].body, b"CREATE VIEW\0", "CREATE OR REPLACE VIEW tag");
        let sel = simple_query(&mut client, &mut buf, "SELECT a FROM v").await;
        let vals: Vec<String> = datarow_values(&sel).into_iter().map(|mut r| r.remove(0)).collect();
        assert_eq!(vals, vec!["1", "2"], "replaced view: a<3");

        // DROP VIEW -> gone; SELECT through it errors server-side. ErrorResponse
        // ('E') is not yet delivered to the client (send_message_to_frontend is a
        // deferred stub, elog.rs; see the m9 note), so the errored SELECT is observed
        // as producing neither a DataRow ('D') nor a successful CommandComplete ('C').
        let r = simple_query(&mut client, &mut buf, "DROP VIEW v").await;
        assert_eq!(r[0].body, b"DROP VIEW\0", "DROP VIEW tag");
        let r = simple_query(&mut client, &mut buf, "SELECT a FROM v").await;
        let tys: Vec<u8> = r.iter().map(|m| m.ty).collect();
        assert!(!r.iter().any(|m| m.ty == b'D'), "dropped-view SELECT -> no rows: {tys:?}");
        assert!(!r.iter().any(|m| m.ty == b'C'), "dropped-view SELECT -> no CommandComplete: {tys:?}");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    /// A unique server-file path under the OS temp dir for a COPY test.
    fn copy_tmpfile(name: &str) -> String {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("pepperdb-copy-{}-{}-{name}", std::process::id(), n))
            .to_string_lossy()
            .into_owned()
    }

    /// Multi-column rows of a SELECT reply, sorted for order-independent compare.
    fn sorted_rows(msgs: &[Msg]) -> Vec<Vec<String>> {
        let mut rows = datarow_values(msgs);
        rows.sort();
        rows
    }

    /// THE M13 (step 45) COPY PLUMBING: COPY TO/FROM a server file, text + CSV,
    /// with the common options, round-tripped over the wire.
    ///   - text round-trip (default tab delimiter, \N null) reproduces the rows;
    ///   - CSV + HEADER round-trip, incl. a field with a comma and a quote;
    ///   - DELIMITER '|' and a custom NULL marker honored both directions;
    ///   - column-list COPY FROM (subset) leaves the other column NULL;
    ///   - COPY (SELECT ...) TO exports the query result;
    ///   - BINARY format is rejected cleanly (staged).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m13_copy_to_from_file_roundtrip_over_the_wire() {
        let _aux = aux_test_serial().await;
        let _hook = test_hook::serial().await;

        let (sup, handle) = start_supervisor(loopback_port0(), tempdir_config()).await;
        let (mut client, mut buf) = connect_and_startup(sup.local_addr).await;
        let c = &mut client;
        let b = &mut buf;

        // Source table with an int + a text column whose values exercise CSV quoting.
        // Seed it via COPY FROM a file rather than INSERT of text literals (the
        // text-literal INSERT coercion path warms the type cache synchronously and is
        // a separate staged item; COPY FROM warms it asynchronously, so it is the
        // path under test here anyway).
        simple_query(c, b, "CREATE TABLE src (a int, b text)").await;
        let seed = copy_tmpfile("seed.txt");
        std::fs::write(&seed, "1\tplain\n2\thas,comma\n3\tq\"uote\n").expect("write seed file");
        let r = simple_query(c, b, &format!("COPY src FROM '{seed}'")).await;
        assert_eq!(complete_tag(&r), "COPY 3", "seed COPY FROM tag");
        let want = vec![
            vec!["1".to_string(), "plain".to_string()],
            vec!["2".to_string(), "has,comma".to_string()],
            vec!["3".to_string(), "q\"uote".to_string()],
        ];
        // Sanity: the seeded text rows read back correctly.
        let sel = simple_query(c, b, "SELECT a, b FROM src").await;
        assert_eq!(sorted_rows(&sel), want, "seed COPY FROM loaded the rows");

        // --- text round-trip --------------------------------------------------
        let txt = copy_tmpfile("t.txt");
        let r = simple_query(c, b, &format!("COPY src TO '{txt}'")).await;
        assert_eq!(complete_tag(&r), "COPY 3", "COPY TO (text) tag");
        simple_query(c, b, "CREATE TABLE dst_txt (a int, b text)").await;
        let r = simple_query(c, b, &format!("COPY dst_txt FROM '{txt}'")).await;
        assert_eq!(complete_tag(&r), "COPY 3", "COPY FROM (text) tag");
        let sel = simple_query(c, b, "SELECT a, b FROM dst_txt").await;
        assert_eq!(sorted_rows(&sel), want, "text round-trip reproduces the rows");

        // --- CSV + HEADER round-trip (quoted field with comma/quote) ----------
        let csv = copy_tmpfile("t.csv");
        let r = simple_query(c, b, &format!("COPY src TO '{csv}' (FORMAT csv, HEADER)")).await;
        assert_eq!(complete_tag(&r), "COPY 3", "COPY TO (csv) tag");
        // The file's first line is the header; the comma field must be quoted.
        let body = std::fs::read_to_string(&csv).expect("read csv export");
        assert!(body.starts_with("a,b\n"), "CSV export has a header line: {body:?}");
        assert!(body.contains("\"has,comma\""), "comma field is quoted: {body:?}");
        simple_query(c, b, "CREATE TABLE dst_csv (a int, b text)").await;
        let r =
            simple_query(c, b, &format!("COPY dst_csv FROM '{csv}' (FORMAT csv, HEADER)")).await;
        assert_eq!(complete_tag(&r), "COPY 3", "COPY FROM (csv) tag");
        let sel = simple_query(c, b, "SELECT a, b FROM dst_csv").await;
        assert_eq!(sorted_rows(&sel), want, "csv round-trip reproduces the rows");

        // --- DELIMITER '|' + custom NULL marker (round-trip through a file) ----
        // Seed `nul` with a NULL in column b via COPY FROM using the custom marker,
        // then export with the same options and re-import: the NULL survives.
        simple_query(c, b, "CREATE TABLE nul (a int, b text)").await;
        let pipe_in = copy_tmpfile("pipe_in.txt");
        std::fs::write(&pipe_in, "7|x\n8|NIL\n").expect("write pipe seed");
        let r = simple_query(
            c,
            b,
            &format!("COPY nul FROM '{pipe_in}' (DELIMITER '|', NULL 'NIL')"),
        )
        .await;
        assert_eq!(complete_tag(&r), "COPY 2", "DELIMITER/NULL FROM tag");
        let pipe = copy_tmpfile("pipe.txt");
        simple_query(c, b, &format!("COPY nul TO '{pipe}' (DELIMITER '|', NULL 'NIL')")).await;
        let body = std::fs::read_to_string(&pipe).expect("read pipe export");
        let mut lines: Vec<&str> = body.lines().collect();
        lines.sort_unstable();
        assert_eq!(lines, vec!["7|x", "8|NIL"], "DELIMITER '|' + NULL 'NIL' honored on TO");

        // --- column-list COPY FROM (subset) -> other column NULL --------------
        let only_a = copy_tmpfile("a.txt");
        simple_query(c, b, &format!("COPY src (a) TO '{only_a}'")).await;
        simple_query(c, b, "CREATE TABLE dst_a (a int, b text)").await;
        let r = simple_query(c, b, &format!("COPY dst_a (a) FROM '{only_a}'")).await;
        assert_eq!(complete_tag(&r), "COPY 3", "column-list COPY FROM tag");
        let sel = simple_query(c, b, "SELECT a, b FROM dst_a").await;
        let rows = sorted_rows(&sel);
        assert_eq!(rows.len(), 3, "three rows loaded");
        assert!(rows.iter().all(|r| r[1] == "NULL"), "unspecified column b is NULL: {rows:?}");

        // --- COPY (query) TO --------------------------------------------------
        let q = copy_tmpfile("q.txt");
        let r =
            simple_query(c, b, &format!("COPY (SELECT a FROM src WHERE a > 1) TO '{q}'")).await;
        assert_eq!(complete_tag(&r), "COPY 2", "COPY (query) TO tag");
        let body = std::fs::read_to_string(&q).expect("read query export");
        let mut lines: Vec<&str> = body.lines().collect();
        lines.sort_unstable();
        assert_eq!(lines, vec!["2", "3"], "COPY (query) TO exports the query result");

        // --- BINARY format is rejected cleanly (staged). The ErrorResponse is not
        // delivered to the client yet (send_message_to_frontend stub), so the errored
        // COPY is observed as producing no CommandComplete.
        let binf = copy_tmpfile("bin");
        let r = simple_query(c, b, &format!("COPY src TO '{binf}' (FORMAT binary)")).await;
        assert!(!r.iter().any(|m| m.ty == b'C'), "BINARY COPY has no CommandComplete (staged)");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }
}
