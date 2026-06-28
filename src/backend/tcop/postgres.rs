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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::net::TcpStream;

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
/// PG's `MyProcPort`) and `xact_scope` (a per-task transaction state so
/// `TransactionBlockStatusCode` reports 'I' idle for ReadyForQuery; M1's
/// start/finish_xact_command are near-no-ops over it).
pub async fn postgres_main(stream: TcpStream, dbname: String, username: String) {
    let _ = (&dbname, &username);

    // PG runs InitPostgres (auth + connect-to-database) here; that is the
    // deferred auth/catalog phase. backend_startup already ran the identity
    // slice (backend_task_init) and published the Session, so we only flip to
    // normal processing mode for the loop.
    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);

    // Install the wire transport (PG `pq_init` over MyProcPort) and the per-task
    // transaction state, then run the command loop inside both scopes. The loop
    // future is boxed: the SYNC pipeline's transient locals + the per-task
    // `XactState` make the combined future large, and `Box::pin` keeps it off the
    // caller's stack (clippy::large_futures) without changing behavior.
    let comm = Arc::new(PqComm::new(stream));
    let loop_fut = Box::pin(crate::backend::access::transam::xact::xact_scope(command_loop()));
    pq::scope(comm, loop_fut).await;
}

/// The backend message loop proper (PG `PostgresMain`'s `for (;;)`), run inside
/// the pqcomm + xact scopes. Async (awaits the wire read + flush).
async fn command_loop() {
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
        // top-level sigsetjmp). The SYNC dispatch builds all reply bytes into the
        // send buffer; wrap it in catch_unwind so an ERROR (a panic carrying
        // ErrorData) is recovered HERE, backend-local, and the loop continues.
        // FATAL / non-ErrorData bug-panics resume to the task boundary. No lock
        // guard or future is held across the catch.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_command(firstchar, &body)
        }));

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
/// `PostgresMain`). SYNC (no `.await`) so the whole unit sits inside the
/// per-command `catch_unwind`. An `elog(ERROR/FATAL)` raised here unwinds as a
/// panic. The simple-Query / DestRemote path is COMPLETE; the extended-protocol
/// arms are grow guards (rules.md s4).
fn dispatch_command(firstchar: u8, body: &[u8]) -> CommandResult {
    match firstchar {
        PQMSG_QUERY => {
            // Body is the null-terminated query string.
            let query_string = cstr_body(body);
            exec_simple_query(query_string);
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
/// raw_parser -> parse_analyze_fixedparams + QueryRewrite -> standard_planner ->
/// CreatePortal/PortalDefineQuery/PortalStart/PortalRun (driving a DestRemote
/// printtup receiver) -> EndCommand (CommandComplete). Everything here is
/// synchronous and buffers its wire output via `pq_putmessage_sync`; the command
/// loop flushes after this returns.
///
/// M1 scope: exactly one SELECT statement. An empty query string -> NullCommand
/// (EmptyQueryResponse). Multi-statement strings, utility/DML statements, and the
/// implicit-transaction-block handling grow with their subsystems (rules.md s4).
fn exec_simple_query(query_string: &str) {
    use crate::backend::parser::analyze::parse_analyze_fixedparams;
    use crate::backend::parser::parser::raw_parser;
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::nodes::nodes::Node;
    use crate::nodes::parsenodes::{RawStmt, FETCH_ALL};
    use crate::parser::parser::RawParseMode;

    let dest = WHERE_TO_SEND_OUTPUT;

    // start_xact_command(): near-no-op over the xact scope for M1 (the loop runs
    // inside xact_scope; full StartTransactionCommand grows with xact.rs wiring).
    start_xact_command();

    // pg_parse_query: raw parse. (tcopprot.c's pg_parse_query wrapper is deferred;
    // call the parser body directly, as the executor tests do.)
    let mut parsetrees = raw_parser(query_string, RawParseMode::Default);

    // Empty query string: tell the frontend and finish.
    if parsetrees.is_empty() {
        finish_xact_command();
        crate::backend::tcop::dest::null_command(dest);
        return;
    }

    // M1 handles a single statement; multi-statement strings grow.
    if parsetrees.len() != 1 {
        unimplemented!("exec_simple_query: multi-statement query strings deferred");
    }
    let Node::RawStmt(raw) = *parsetrees.remove(0) else {
        unreachable!("raw_parser yields RawStmt nodes");
    };
    let raw: RawStmt = *raw;

    crate::backend::tcop::dest::begin_command(CommandTag::Unknown, dest);

    // If we got a cancel signal in parsing, quit.
    crate::miscadmin::check_for_interrupts();

    // pg_analyze_and_rewrite_fixedparams: parse analysis + rewrite.
    let analyzed = parse_analyze_fixedparams(&raw, query_string, &[], 0, None);
    let mut rewritten = query_rewrite(analyzed);
    if rewritten.len() != 1 {
        unimplemented!("exec_simple_query: query rewrite producing multiple queries deferred");
    }

    // pg_plan_queries: plan.
    let mut query = rewritten.remove(0);
    let plan = standard_planner(&mut query, query_string, 0, None);

    crate::miscadmin::check_for_interrupts();

    // The command tag for the completion message. M1 reaches SELECT only;
    // CreateCommandTag (utility.c) is deferred, so derive it from the plan.
    let command_tag = command_tag_for(&plan);

    // CreatePortal / PortalDefineQuery / PortalStart.
    let mut portal = crate::backend::tcop::pquery::create_portal("");
    crate::backend::tcop::pquery::portal_define_query(
        &mut portal,
        query_string,
        command_tag,
        vec![plan],
    );
    // Select the wire format: text (0) for every column in simple Query mode.
    crate::backend::tcop::pquery::portal_set_result_format(&mut portal, &[]);
    crate::backend::tcop::pquery::portal_start(&mut portal);

    // Create the destination receiver and bind it to the portal (formats).
    let mut receiver = crate::backend::tcop::dest::create_dest_receiver(dest);
    if dest == CommandDest::DestRemote {
        crate::access::printtup::SetRemoteDestReceiverParams(receiver.as_mut(), portal.as_mut());
    }

    // Run the portal to completion (drives ExecutorRun -> printtup, which appends
    // RowDescription + DataRow(s) to the send buffer), then finish + drop.
    let mut qc = crate::tcop::cmdtag::QueryCompletion { command_tag, nprocessed: 0 };
    crate::backend::tcop::pquery::portal_run(&mut portal, FETCH_ALL, receiver, Some(&mut qc));

    if let Some(query_desc) = portal.query_desc.as_mut() {
        crate::executor::executor::ExecutorFinish(query_desc);
    }
    crate::backend::tcop::pquery::portal_drop(&mut portal);

    // Close the transaction statement (near-no-op for M1), then report completion.
    finish_xact_command();
    crate::backend::tcop::dest::end_command(&qc, dest, false);
}

/// Derive the completion command tag from a planned statement. M1 reaches SELECT;
/// the full `CreateCommandTag` (utility.c, raw-statement based) grows later.
fn command_tag_for(plan: &crate::nodes::plannodes::PlannedStmt) -> CommandTag {
    match plan.command_type {
        crate::nodes::nodes::CmdType::SELECT => CommandTag::Select,
        other => unimplemented!("command_tag_for: {other:?} (non-SELECT) deferred"),
    }
}

/// PG `start_xact_command`: ensure a transaction command is open. M1 runs the
/// whole loop inside `xact_scope`, so this is a near-no-op; the full
/// StartTransactionCommand wiring grows with xact.rs.
fn start_xact_command() {
    // TODO(xact): StartTransactionCommand(&shared) + statement-timeout arm.
}

/// PG `finish_xact_command`: close the transaction statement. Near-no-op for M1
/// (see `start_xact_command`); CommitTransactionCommand grows with xact.rs.
fn finish_xact_command() {
    // TODO(xact): CommitTransactionCommand(&shared).
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

// --- end-to-end M1 wire test (the SELECT 1 milestone) ----------------------
#[cfg(test)]
mod wire_tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::backend::libpq::pqcomm::{self as pq, PqComm};

    /// One decoded backend->frontend message: type byte + body (length stripped).
    #[derive(Debug, PartialEq, Eq)]
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

    /// Drive the backend command loop for a single simple Query, returning the
    /// decoded message sequence the client received.
    ///
    /// The backend runs as its own task over the server end of a duplex. The
    /// client writes the Query, then reads the whole response (the M1 reply is
    /// fully flushed before the backend blocks on the next read), then drops the
    /// duplex client end -- which makes the backend's next read see EOF, so it
    /// exits cleanly. We read a bounded amount with a timeout rather than to EOF,
    /// because `tokio::io::duplex` only signals EOF once the WHOLE peer end drops.
    async fn run_query(sql: &str) -> Vec<Msg> {
        let (server, mut client) = tokio::io::duplex(64 * 1024);

        let backend = tokio::spawn(async move {
            let comm = Arc::new(PqComm::new(server));
            let loop_fut =
                Box::pin(crate::backend::access::transam::xact::xact_scope(command_loop()));
            pq::scope(comm, loop_fut).await;
        });

        let mut q = sql.as_bytes().to_vec();
        q.push(0); // null-terminated query string
        client
            .write_all(&framed(crate::libpq::protocol::PQMSG_QUERY, &q))
            .await
            .unwrap();
        client.flush().await.unwrap();

        // Read the response until it ends with a ReadyForQuery ('Z') message --
        // the M1 reply terminates with the idle 'Z' that closes the query cycle.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.read(&mut chunk),
            )
            .await
            .expect("backend response timed out")
            .unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            // The cycle is done once we've seen the post-query ReadyForQuery: the
            // startup 'Z' plus the 'Z' that follows CommandComplete => two 'Z's.
            let msgs = decode(&buf);
            let zcount = msgs.iter().filter(|m| m.ty == b'Z').count();
            if zcount >= 2 && total_decoded_len(&msgs) == buf.len() {
                break;
            }
        }

        // Drop the client -> backend's next read sees EOF -> command loop exits.
        drop(client);
        backend.await.unwrap();
        decode(&buf)
    }

    /// Sum of the on-wire size of decoded messages (1 type byte + int32 len).
    fn total_decoded_len(msgs: &[Msg]) -> usize {
        msgs.iter().map(|m| 1 + 4 + m.body.len()).sum()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_1_produces_full_message_sequence() {
        let msgs = run_query("SELECT 1").await;

        // Sequence: BackendKeyData('K') | ReadyForQuery('Z','I') |
        //           RowDescription('T') | DataRow('D') | CommandComplete('C') |
        //           ReadyForQuery('Z','I')
        let types: Vec<u8> = msgs.iter().map(|m| m.ty).collect();
        assert_eq!(
            types,
            vec![b'K', b'Z', b'T', b'D', b'C', b'Z'],
            "M1 message sequence"
        );

        // RowDescription: 1 field "?column?", type OID 23, attlen 4, typmod -1, fmt 0.
        let t = &msgs[2];
        assert_eq!(t.ty, b'T');
        let mut td = Vec::new();
        td.extend_from_slice(&1u16.to_be_bytes()); // natts
        td.extend_from_slice(b"?column?\0");
        td.extend_from_slice(&0u32.to_be_bytes()); // resorigtbl
        td.extend_from_slice(&0u16.to_be_bytes()); // resorigcol
        td.extend_from_slice(&23u32.to_be_bytes()); // INT4OID
        td.extend_from_slice(&4u16.to_be_bytes()); // attlen
        td.extend_from_slice(&(-1i32 as u32).to_be_bytes()); // typmod
        td.extend_from_slice(&0u16.to_be_bytes()); // format
        assert_eq!(t.body, td);

        // DataRow: 1 column, text "1".
        let d = &msgs[3];
        assert_eq!(d.ty, b'D');
        let mut dr = Vec::new();
        dr.extend_from_slice(&1u16.to_be_bytes()); // 1 column
        dr.extend_from_slice(&1u32.to_be_bytes()); // length 1
        dr.extend_from_slice(b"1");
        assert_eq!(d.body, dr);

        // CommandComplete: "SELECT 1\0".
        assert_eq!(msgs[4].ty, b'C');
        assert_eq!(msgs[4].body, b"SELECT 1\0");

        // ReadyForQuery: 'I' idle.
        assert_eq!(msgs[5].ty, b'Z');
        assert_eq!(msgs[5].body, b"I");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_42_data_row_is_text_42() {
        let msgs = run_query("SELECT 42").await;
        let d = msgs.iter().find(|m| m.ty == b'D').expect("a DataRow");
        let mut dr = Vec::new();
        dr.extend_from_slice(&1u16.to_be_bytes());
        dr.extend_from_slice(&2u32.to_be_bytes()); // "42" is 2 bytes
        dr.extend_from_slice(b"42");
        assert_eq!(d.body, dr);

        let c = msgs.iter().find(|m| m.ty == b'C').expect("a CommandComplete");
        assert_eq!(c.body, b"SELECT 1\0"); // one row processed
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_two_columns() {
        let msgs = run_query("SELECT 1, 2").await;

        // RowDescription has 2 fields.
        let t = msgs.iter().find(|m| m.ty == b'T').expect("a RowDescription");
        let natts = u16::from_be_bytes([t.body[0], t.body[1]]);
        assert_eq!(natts, 2);

        // DataRow has 2 text columns "1","2".
        let d = msgs.iter().find(|m| m.ty == b'D').expect("a DataRow");
        let mut dr = Vec::new();
        dr.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
        dr.extend_from_slice(&1u32.to_be_bytes());
        dr.extend_from_slice(b"1");
        dr.extend_from_slice(&1u32.to_be_bytes());
        dr.extend_from_slice(b"2");
        assert_eq!(d.body, dr);
    }
}
