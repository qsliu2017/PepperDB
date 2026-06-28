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

use tokio::net::TcpStream;

use crate::backend::storage::ipc::procsignal::{self, ProcSignalSlot};
use crate::libpq::protocol::{
    PQMSG_BIND, PQMSG_CLOSE, PQMSG_COPY_DATA, PQMSG_COPY_DONE, PQMSG_COPY_FAIL, PQMSG_DESCRIBE,
    PQMSG_EXECUTE, PQMSG_FLUSH, PQMSG_FUNCTION_CALL, PQMSG_PARSE, PQMSG_QUERY, PQMSG_SYNC,
    PQMSG_TERMINATE,
};
use crate::miscadmin::{interrupts_can_be_processed, BackendType};
use crate::utils::errcodes::{
    ERRCODE_ADMIN_SHUTDOWN, ERRCODE_CONNECTION_FAILURE, ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    ERRCODE_IDLE_SESSION_TIMEOUT, ERRCODE_QUERY_CANCELED,
};

// ---------------------------------------------------------------------------
// PostgresMain -- the backend command loop.
// ---------------------------------------------------------------------------

/// PG `PostgresMain`. Sends this backend's startup responses, then loops reading
/// and dispatching client messages until Terminate/EOF.
///
/// `stream` is held so the socket stays open for the life of the backend; the
/// translated loop reaches the client through the `pq_*` helpers (deferred), not
/// through `stream` directly -- pqcomm owns the wire transport once it lands. The
/// loop calls `CHECK_FOR_INTERRUPTS` at the top of each iteration; that is now
/// live (see [`process_interrupts`]).
#[allow(
    clippy::unused_async,
    reason = "PostgresMain command loop is driven as a future (tokio::pin!/select! in run_backend); awaits land once pqcomm (read_command/ready_for_query) is ported"
)]
pub async fn postgres_main(stream: TcpStream, dbname: String, username: String) {
    // Keep the connection alive for the backend's lifetime. pqcomm will take it
    // over; until then it parks here so the socket is not dropped mid-loop.
    let _stream = stream;
    let _ = (&dbname, &username);

    // PG runs InitPostgres (auth + connect-to-database) here; that is the
    // deferred auth/catalog phase. backend_startup already ran the identity
    // slice (backend_task_init) and published the Session, so we only flip to
    // normal processing mode for the loop.
    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);

    // Send this backend's cancellation key to the frontend (BackendKeyData),
    // then enter the message loop. These wire sends go through the deferred pq_*
    // stubs; the structure is faithful to postgres.c.
    send_backend_key_data();

    let mut send_ready_for_query = true;
    loop {
        // (1) If idle, tell the frontend we're ready for a new query.
        if send_ready_for_query {
            ready_for_query();
            send_ready_for_query = false;
        }

        // Per-command recovery point (error.md s2.2, boundary 2 -- PG's top-level
        // sigsetjmp). Wrap the read + dispatch of one command in catch_unwind so
        // an ERROR (a panic carrying ErrorData) is recovered HERE, backend-local,
        // and the loop continues with the next command. FATAL and non-ErrorData
        // bug-panics are resumed so they reach the task boundary (end the
        // backend). No lock/guard is held across the catch.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            process_one_command()
        }));

        match outcome {
            Ok(CommandResult::Continue { ready }) => {
                send_ready_for_query |= ready;
            }
            // Terminate or EOF: the frontend is closing the socket. Normal exit.
            Ok(CommandResult::Done) => return,
            Err(payload) => {
                // error.md s2.3-2.4: an ERROR (elevel < FATAL) is recovered in
                // the backend; FATAL (or a non-ErrorData bug-panic) resumes to the
                // task boundary, which ends the backend.
                match payload.downcast::<crate::utils::elog::ErrorData>() {
                    Ok(edata) if edata.elevel < crate::utils::elog::FATAL => {
                        recover_from_error(&edata);
                        // After recovery the session is idle again -- announce it.
                        send_ready_for_query = true;
                    }
                    Ok(edata) => std::panic::resume_unwind(edata), // FATAL
                    Err(other) => std::panic::resume_unwind(other), // bug-panic
                }
            }
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

/// Read and dispatch exactly one client command (steps 3/5/7 of `PostgresMain`).
/// Sync (no `.await`) so the whole unit sits inside the per-command `catch_unwind`
/// recovery point. An `elog(ERROR/FATAL)` raised in here unwinds out as a panic.
fn process_one_command() -> CommandResult {
    // (2) Allow a query-cancel arriving while we block on the read to be a
    // no-op: ProcessInterrupts suppresses the cancel ERROR while DoingCommandRead.
    set_doing_command_read(true);

    // (3) Read a command (blocks here in PG via secure_read). pqcomm is
    // deferred: read_command hits the pq_* stub at runtime.
    let firstchar = read_command();

    // (5) Service any interrupts that arrived while we slept, before clearing
    // DoingCommandRead, so an idle cancel is reset rather than thrown. This is
    // the live CHECK_FOR_INTERRUPTS payoff.
    crate::miscadmin::check_for_interrupts();
    set_doing_command_read(false);

    // (7) Process the command.
    match firstchar {
        Some(PQMSG_QUERY) => {
            exec_simple_query("");
            CommandResult::Continue { ready: true }
        }
        Some(PQMSG_PARSE) => {
            exec_parse_message();
            CommandResult::Continue { ready: false }
        }
        Some(PQMSG_BIND) => {
            exec_bind_message();
            CommandResult::Continue { ready: false }
        }
        Some(PQMSG_EXECUTE) => {
            exec_execute_message();
            CommandResult::Continue { ready: false }
        }
        Some(PQMSG_FUNCTION_CALL) => {
            handle_function_request();
            CommandResult::Continue { ready: true }
        }
        Some(PQMSG_CLOSE) => {
            exec_close_message();
            CommandResult::Continue { ready: false }
        }
        Some(PQMSG_DESCRIBE) => {
            exec_describe_message();
            CommandResult::Continue { ready: false }
        }
        Some(PQMSG_FLUSH) => {
            pq_flush();
            CommandResult::Continue { ready: false }
        }
        Some(PQMSG_SYNC) => {
            finish_xact_command();
            CommandResult::Continue { ready: true }
        }
        // Terminate or EOF: the frontend is closing the socket. Normal exit.
        Some(PQMSG_TERMINATE) | None => CommandResult::Done,
        // COPY messages after a failed COPY: accept and ignore.
        Some(PQMSG_COPY_DATA | PQMSG_COPY_DONE | PQMSG_COPY_FAIL) => {
            CommandResult::Continue { ready: false }
        }
        Some(other) => {
            // PG: ereport(FATAL, ERRCODE_PROTOCOL_VIOLATION).
            crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("invalid frontend message type {other}"));
            });
            CommandResult::Continue { ready: false }
        }
    }
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

// --- Deferred wire/exec entries (pqcomm + parser/planner/executor) ---------
// These translate the postgres.c call sites onto the existing deferred stubs.
// They are thin so the loop above reads like postgres.c; each lands on an
// `unimplemented!()` at runtime until its subsystem is ported.

fn send_backend_key_data() {
    // PG: pq_beginmessage(BackendKeyData) + pid + MyCancelKey + pq_endmessage.
    crate::libpq::libpq::pq_putmessage(crate::libpq::protocol::PQMSG_BACKEND_KEY_DATA, &[]);
}

fn ready_for_query() {
    // PG: ReadyForQuery(whereToSendOutput) (commands/dest.c) -> 'Z' + flush.
    crate::libpq::libpq::pq_putmessage(crate::libpq::protocol::PQMSG_READY_FOR_QUERY, &[]);
    pq_flush();
}

/// PG `ReadCommand`/`SocketBackend`: read the message-type byte then the body.
/// Returns the firstchar, or `None` for EOF. pqcomm deferred -> pq_* stub.
fn read_command() -> Option<u8> {
    match crate::libpq::libpq::pq_getbyte() {
        -1 => None,
        b => Some(b as u8),
    }
}

fn pq_flush() {
    crate::libpq::libpq::pq_flush();
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

fn exec_simple_query(query_string: &str) {
    // PG exec_simple_query: pg_parse_query -> pg_analyze_and_rewrite ->
    // pg_plan_queries -> PortalRun. All deferred.
    crate::tcop::tcopprot::pg_parse_query(query_string);
}

fn exec_parse_message() {
    crate::tcop::tcopprot::pg_parse_query("");
}

fn exec_bind_message() {
    crate::tcop::tcopprot::pg_parse_query("");
}

fn exec_execute_message() {
    crate::tcop::tcopprot::pg_parse_query("");
}

fn exec_close_message() {
    crate::libpq::libpq::pq_putmessage(crate::libpq::protocol::PQMSG_CLOSE_COMPLETE, &[]);
}

fn exec_describe_message() {
    crate::tcop::tcopprot::pg_parse_query("");
}

fn handle_function_request() {
    crate::tcop::tcopprot::pg_parse_query("");
}

fn finish_xact_command() {
    // PG finish_xact_command -> CommitTransactionCommand (xact.c). Deferred.
    crate::tcop::tcopprot::pg_parse_query("");
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
