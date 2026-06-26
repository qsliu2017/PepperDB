//! Translated from PostgreSQL src/backend/tcop/postgres.c
//!
//! `PostgresMain` (the backend command loop) and `ProcessInterrupts` (the
//! deferred-interrupt service routine that `CHECK_FOR_INTERRUPTS` calls).
//!
//! Under the single-process async model:
//! - `PostgresMain` is an async task body, not a `setjmp`-anchored function. The
//!   outer-error `sigsetjmp` recovery block becomes the `catch_unwind` at the
//!   task boundary (postmaster.rs); an `elog(ERROR)` is a panic carrying
//!   `ErrorData`. The translated read-dispatch loop calls the libpq `pq_*` send/
//!   recv helpers and the parser/planner/executor entries, which are DEFERRED
//!   subsystems still on `unimplemented!()` stubs -- so the loop compiles and the
//!   connection path runs up to the first real wire message, where it hits a
//!   stub. pqcomm is NOT reimplemented here.
//! - `ProcessInterrupts` is implemented FOR REAL against the per-task
//!   [`ProcSignalSlot`] (step 04). It is SYNC (callable from arbitrary sync
//!   code), never `.await`s, only reads/clears atomic flags and panics
//!   (`elog(ERROR/FATAL)`). With no current slot (aux task / slotless test) it is
//!   a no-op.

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

        // (3) Read a command (blocks here in PG via secure_read). pqcomm is
        // deferred: read_command hits the pq_* stub at runtime.
        let firstchar = read_command();

        // (5) Service any interrupts that arrived while we slept. Query cancel is
        // a no-op when idle; ProcessInterrupts has that effect here. This is the
        // live CHECK_FOR_INTERRUPTS payoff.
        crate::miscadmin::check_for_interrupts();

        // (7) Process the command.
        match firstchar {
            Some(PQMSG_QUERY) => {
                exec_simple_query("");
                send_ready_for_query = true;
            }
            Some(PQMSG_PARSE) => exec_parse_message(),
            Some(PQMSG_BIND) => exec_bind_message(),
            Some(PQMSG_EXECUTE) => exec_execute_message(),
            Some(PQMSG_FUNCTION_CALL) => {
                handle_function_request();
                send_ready_for_query = true;
            }
            Some(PQMSG_CLOSE) => exec_close_message(),
            Some(PQMSG_DESCRIBE) => exec_describe_message(),
            Some(PQMSG_FLUSH) => pq_flush(),
            Some(PQMSG_SYNC) => {
                finish_xact_command();
                send_ready_for_query = true;
            }
            // Terminate or EOF: the frontend is closing the socket. Normal exit.
            Some(PQMSG_TERMINATE) | None => return,
            // COPY messages after a failed COPY: accept and ignore.
            Some(PQMSG_COPY_DATA | PQMSG_COPY_DONE | PQMSG_COPY_FAIL) => {}
            Some(other) => {
                // PG: ereport(FATAL, ERRCODE_PROTOCOL_VIOLATION).
                crate::ereport!(crate::utils::elog::FATAL, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(format!("invalid frontend message type {other}"));
                });
            }
        }
    }
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
        proc_die_fatal();
    }

    if f.client_connection_lost.swap(false, Ordering::AcqRel) {
        f.query_cancel_pending.store(false, Ordering::Release); // lost connection trumps cancel
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
        // case is a user cancel request.
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_QUERY_CANCELED)
                .errmsg("canceling statement due to user request");
        });
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
            procsignal::scope(slot, async {
                body();
            })
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
                procsignal::scope(slot, async {
                    body();
                })
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
}
