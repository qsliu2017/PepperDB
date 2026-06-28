//! Error logging and reporting. Translated from backend/utils/error/elog.c.
//!
//! Because log messages can be generated at an extremely high rate, this code is
//! careful about the cost of gathering anything that might be logged, and it must
//! stay robust when called from within an aborted transaction, where operations
//! such as catalog-cache lookups are unsafe. The principal entry points build an
//! `ErrorData` record through chained field accessors (`errcode`, `errmsg`,
//! `errdetail`, `errhint`, `errcontext_msg`, ...), then `errstart`/`errfinish`
//! decide whether a message is worth emitting and route it to the server log
//! and/or the connected client. Messages below `ERROR` severity return to the
//! caller; messages at `ERROR` and above do not.
//!
//! PepperDB collapses PostgreSQL's multi-level `errordata[]` recursion stack into
//! a single in-flight build slot: an error is constructed as one `ErrorData`
//! value and raised atomically, so there is no open span for a nested report to
//! interleave into. An `in_flight` flag marks that a build is open; a second
//! error-or-higher raise while one is open is treated as a double fault and
//! escalated to `PANIC`. Severity drives the unwinding model: `ERROR` and `FATAL`
//! raise a catchable panic carrying the `ErrorData`, which `PG_TRY`/`PG_CATCH`
//! (realized as `catch_unwind`) can intercept and re-throw, while `PANIC` aborts
//! the process outright so half-updated shared state is never flushed. Per-task
//! error state lives in thread-local storage rather than process globals, and
//! `printf`-style varargs are pre-formatted into owned `String`s at the call
//! site instead of being expanded here.
//!
//! Only the standard-error log destination is implemented. The syslog, event-log,
//! CSV-log, JSON-log, and syslogger pipe destinations, the protocol-encoded send
//! to a connected frontend, and the `log_line_prefix` timestamp expansion are not
//! yet implemented and are left as stubs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::cell::RefCell;
use std::fmt::Write as _;
use std::io::Write;

use crate::utils::elog::{
    pg_unsixbit, CLIENT_MIN_MESSAGES, DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5, ERROR,
    ERRCODE_INTERNAL_ERROR, ERRCODE_SUCCESSFUL_COMPLETION, ERRCODE_WARNING, ErrorContextCallback,
    ErrorData, FATAL, INFO, LOG, LOG_DESTINATION, LOG_DESTINATION_STDERR, LOG_ERROR_VERBOSITY,
    LOG_MIN_MESSAGES, LOG_SERVER_ONLY, NOTICE, PANIC, PgErrorVerbosity, WARNING,
    WARNING_CLIENT_ONLY,
};

// ---------------------------------------------------------------------------
// PANIC abort. Per the error model (error.md s2.5) a PANIC must crash the
// process via std::process::abort() -- uncatchable, no Drop -- so half-updated
// shared state is never flushed. In production this is a real abort(); under
// cfg(test) a real abort would kill the whole test binary, so tests redirect it
// to a catchable, distinguished panic carrying the ErrorData (marked PANIC) via
// a hook, letting them assert the PANIC path without aborting the runner.
// ---------------------------------------------------------------------------

/// Emit `edata` to the server log, then take the PANIC abort path. In production
/// this never returns (it aborts the process). Under cfg(test) the abort hook
/// raises a catchable panic instead, so it diverges either way.
#[allow(
    clippy::needless_pass_by_value,
    reason = "consumes edata: moved into panic_any (cfg(test) hook) or discarded at process::abort (prod)"
)]
fn abort_for_panic(edata: ErrorData) -> ! {
    send_message_to_server_log(&edata);
    #[cfg(test)]
    {
        test_abort::take_abort_path(edata)
    }
    #[cfg(not(test))]
    {
        let _ = edata;
        std::process::abort()
    }
}

#[cfg(test)]
mod test_abort {
    use super::ErrorData;

    /// Test substitute for `std::process::abort()`: records that the PANIC abort
    /// path was taken, then raises the ErrorData as a catchable panic so a test
    /// can `catch_unwind` it and assert the severity/sqlstate without killing the
    /// test binary. Diverges (`-> !`) like the real abort.
    pub fn take_abort_path(edata: ErrorData) -> ! {
        ABORT_TAKEN.with(|c| c.set(c.get() + 1));
        std::panic::panic_any(edata);
    }

    thread_local! {
        static ABORT_TAKEN: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    /// Number of times the PANIC abort path fired on this thread (test-only).
    pub fn abort_count() -> u32 {
        ABORT_TAKEN.with(std::cell::Cell::get)
    }

    /// Reset the per-thread abort counter (test-only).
    pub fn reset_abort_count() {
        ABORT_TAKEN.with(|c| c.set(0));
    }
}

// ---------------------------------------------------------------------------
// Per-task error state (replaces PG's process globals: errordata[],
// errordata_stack_depth, recursion_depth). error.md s2.8 collapses PG's
// multi-level errordata[] recursion stack to a single in-flight slot: an
// ereport!/elog! builds one ErrorData value atomically and raises, so there is
// no open span for a nested ereport to interleave into. `in_flight` marks that a
// build is open; a second >=ERROR raise while one is open is a double fault and
// escalates to PANIC (replacing the old ERRORDATA_STACK_SIZE overflow guard).
// recursion_depth is kept for the recursion-trouble checks (drop context
// callbacks when errors nest deeply). TODO(panic): once tasks are tokio tasks,
// move from thread_local! to tokio task-local storage.
// ---------------------------------------------------------------------------

struct ErrorState {
    in_flight: bool,
    recursion_depth: i32,
}

thread_local! {
    static ERROR_STATE: RefCell<ErrorState> =
        const { RefCell::new(ErrorState { in_flight: false, recursion_depth: 0 }) };
}

// ---------------------------------------------------------------------------
// Builder-style error field accessors. PG threads these (errcode/errmsg/...)
// as comma-expression "int" calls inside ereport(); here they build/mutate an
// ErrorData. printf varargs collapse to a pre-formatted `String` at the call
// site (callers use format!()).
// ---------------------------------------------------------------------------

impl ErrorData {
    pub fn errcode(&mut self, sqlerrcode: i32) -> &mut Self {
        self.sqlerrcode = sqlerrcode;
        self
    }

    pub fn errmsg(&mut self, msg: impl Into<String>) -> &mut Self {
        self.message = Some(msg.into());
        self
    }

    pub fn errmsg_internal(&mut self, msg: impl Into<String>) -> &mut Self {
        self.message = Some(msg.into());
        self
    }

    pub fn errdetail(&mut self, msg: impl Into<String>) -> &mut Self {
        self.detail = Some(msg.into());
        self
    }

    pub fn errdetail_internal(&mut self, msg: impl Into<String>) -> &mut Self {
        self.detail = Some(msg.into());
        self
    }

    pub fn errdetail_log(&mut self, msg: impl Into<String>) -> &mut Self {
        self.detail_log = Some(msg.into());
        self
    }

    pub fn errhint(&mut self, msg: impl Into<String>) -> &mut Self {
        self.hint = Some(msg.into());
        self
    }

    // errcontext_msg appends, building up a stack of context information.
    pub fn errcontext_msg(&mut self, msg: impl Into<String>) -> &mut Self {
        let msg = msg.into();
        self.context = Some(match self.context.take() {
            Some(existing) => format!("{existing}\n{msg}"),
            None => msg,
        });
        self
    }

    pub fn errhidestmt(&mut self, hide_stmt: bool) -> &mut Self {
        self.hide_stmt = hide_stmt;
        self
    }

    pub fn errhidecontext(&mut self, hide_ctx: bool) -> &mut Self {
        self.hide_ctx = hide_ctx;
        self
    }

    pub fn errposition(&mut self, cursorpos: i32) -> &mut Self {
        self.cursorpos = cursorpos;
        self
    }

    pub fn internalerrposition(&mut self, cursorpos: i32) -> &mut Self {
        self.internalpos = cursorpos;
        self
    }

    pub fn internalerrquery(&mut self, query: impl Into<String>) -> &mut Self {
        self.internalquery = Some(query.into());
        self
    }

    // errcode_for_file_access: derive a SQLSTATE from the saved errno-style
    // failure (errno snapshotted into saved_errno at errstart). We map via
    // portable std::io::ErrorKind rather than raw libc errno numbers (which
    // differ by OS). Mutates this one in-flight ErrorData (no split-brain).
    pub fn errcode_for_file_access(&mut self) -> &mut Self {
        use crate::utils::errcodes::{ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_FILE, ERRCODE_DUPLICATE_FILE, ERRCODE_WRONG_OBJECT_TYPE, ERRCODE_DISK_FULL, ERRCODE_OUT_OF_MEMORY, ERRCODE_INTERNAL_ERROR};
        use std::io::ErrorKind::{PermissionDenied, ReadOnlyFilesystem, NotFound, AlreadyExists, NotADirectory, IsADirectory, DirectoryNotEmpty, StorageFull, OutOfMemory};
        self.sqlerrcode = match std::io::Error::from_raw_os_error(self.saved_errno).kind() {
            PermissionDenied | ReadOnlyFilesystem => ERRCODE_INSUFFICIENT_PRIVILEGE,
            NotFound => ERRCODE_UNDEFINED_FILE,
            AlreadyExists => ERRCODE_DUPLICATE_FILE,
            NotADirectory | IsADirectory | DirectoryNotEmpty => ERRCODE_WRONG_OBJECT_TYPE,
            StorageFull => ERRCODE_DISK_FULL,
            OutOfMemory => ERRCODE_OUT_OF_MEMORY,
            _ => ERRCODE_INTERNAL_ERROR,
        };
        self
    }

    // errcode_for_socket_access: ALL_CONNECTION_FAILURE_ERRNOS in PG (EPIPE /
    // ECONNRESET et al.) -> connection_failure; mutates this in-flight ErrorData.
    pub fn errcode_for_socket_access(&mut self) -> &mut Self {
        use crate::utils::errcodes::{ERRCODE_CONNECTION_FAILURE, ERRCODE_INTERNAL_ERROR};
        use std::io::ErrorKind::{BrokenPipe, ConnectionReset, ConnectionAborted, NotConnected};
        self.sqlerrcode = match std::io::Error::from_raw_os_error(self.saved_errno).kind() {
            BrokenPipe | ConnectionReset | ConnectionAborted | NotConnected => {
                ERRCODE_CONNECTION_FAILURE
            }
            _ => ERRCODE_INTERNAL_ERROR,
        };
        self
    }

    // geterrcode / geterrposition / getinternalerrposition read back fields of
    // the in-flight error mid-build (PG reads errordata[] top). With a single
    // build target they read this same value.
    pub fn geterrcode(&self) -> i32 {
        self.sqlerrcode
    }
    pub fn geterrposition(&self) -> i32 {
        self.cursorpos
    }
    pub fn getinternalerrposition(&self) -> i32 {
        self.internalpos
    }

    // errbacktrace: capture a backtrace into this in-flight error.
    pub fn errbacktrace(&mut self) -> &mut Self {
        self.backtrace = Some(std::backtrace::Backtrace::force_capture().to_string());
        self
    }

    // set_errcontext_domain: domain used by errcontext message translation.
    pub fn set_errcontext_domain(&mut self, domain: Option<&str>) -> &mut Self {
        self.context_domain = Some(domain.unwrap_or("postgres").to_owned());
        self
    }

    // err_generic_string: set a PG_DIAG_* generic string field on this error.
    pub fn err_generic_string(&mut self, field: i32, s: &str) -> &mut Self {
        use crate::postgres_ext::{PG_DIAG_SCHEMA_NAME, PG_DIAG_TABLE_NAME, PG_DIAG_COLUMN_NAME, PG_DIAG_DATATYPE_NAME, PG_DIAG_CONSTRAINT_NAME};
        let f = field as u8;
        if f == PG_DIAG_SCHEMA_NAME {
            self.schema_name = Some(s.to_owned());
        } else if f == PG_DIAG_TABLE_NAME {
            self.table_name = Some(s.to_owned());
        } else if f == PG_DIAG_COLUMN_NAME {
            self.column_name = Some(s.to_owned());
        } else if f == PG_DIAG_DATATYPE_NAME {
            self.datatype_name = Some(s.to_owned());
        } else if f == PG_DIAG_CONSTRAINT_NAME {
            self.constraint_name = Some(s.to_owned());
        } else {
            // TODO(panic): PG elog(ERROR) here; deferred to keep this non-raising.
            panic!("unsupported ErrorData field id: {field}");
        }
        self
    }
}

// Plural variants collapse to a runtime pick of singular/plural; printf args are
// pre-formatted by the caller, so we just select the format string.
pub fn errmsg_plural<'a>(fmt_singular: &'a str, fmt_plural: &'a str, n: u64) -> &'a str {
    if n == 1 { fmt_singular } else { fmt_plural }
}
pub fn errdetail_plural<'a>(fmt_singular: &'a str, fmt_plural: &'a str, n: u64) -> &'a str {
    if n == 1 { fmt_singular } else { fmt_plural }
}
pub fn errdetail_log_plural<'a>(fmt_singular: &'a str, fmt_plural: &'a str, n: u64) -> &'a str {
    if n == 1 { fmt_singular } else { fmt_plural }
}
pub fn errhint_plural<'a>(fmt_singular: &'a str, fmt_plural: &'a str, n: u64) -> &'a str {
    if n == 1 { fmt_singular } else { fmt_plural }
}

// Default sqlerrcode for an elevel (errstart's fallback, see header comment).
pub const fn default_errcode_for(elevel: i32) -> i32 {
    if elevel >= ERROR {
        ERRCODE_INTERNAL_ERROR
    } else if elevel >= WARNING {
        ERRCODE_WARNING
    } else {
        ERRCODE_SUCCESSFUL_COMPLETION
    }
}

// ---------------------------------------------------------------------------
// Severity gating: is_log_level_output / should_output_to_*.
// ---------------------------------------------------------------------------

// is_log_level_output -- is elevel logically >= log_min_level? LOG sorts
// out-of-order, between ERROR and FATAL.
fn is_log_level_output(elevel: i32, log_min_level: i32) -> bool {
    if elevel == LOG || elevel == LOG_SERVER_ONLY {
        log_min_level == LOG || log_min_level <= ERROR
    } else if elevel == WARNING_CLIENT_ONLY {
        false
    } else if log_min_level == LOG {
        elevel >= FATAL
    } else {
        elevel >= log_min_level
    }
}

fn should_output_to_server(elevel: i32) -> bool {
    is_log_level_output(elevel, LOG_MIN_MESSAGES)
}

fn should_output_to_client(elevel: i32) -> bool {
    // PG checks whereToSendOutput == DestRemote (a connected frontend). With no
    // frontend wired up yet, route nothing to the client; the server log path
    // is what's exercised. TODO: honor whereToSendOutput / ClientAuthInProgress.
    let _ = elevel;
    false
}

// message_level_is_interesting -- would ereport/elog do anything?
pub fn message_level_is_interesting(elevel: i32) -> bool {
    elevel >= ERROR || should_output_to_server(elevel) || should_output_to_client(elevel)
}

// in_error_recursion_trouble -- pull the plug if we recurse more than once.
pub fn in_error_recursion_trouble() -> bool {
    ERROR_STATE.with(|st| st.borrow().recursion_depth > 2)
}

// ---------------------------------------------------------------------------
// errstart / errfinish core. In PG `errstart` returns whether the message is
// worth building; `errfinish` either logs (low severity) or longjmps (>=ERROR).
// Here >=ERROR -> panic carrying the ErrorData; lower -> log and return.
// ---------------------------------------------------------------------------

// Returns Some(ErrorData) when the message should be built, None to
// short-circuit (warning-or-less not enabled anywhere). The returned value IS
// the single in-flight build target (error.md s2.8): the ereport! closure and
// every builder method/helper mutate THAT one ErrorData; errfinish consumes it.
// A >=ERROR raise while a build is already in flight is a double fault -> PANIC.
pub fn errstart(elevel: i32, domain: Option<&str>) -> Option<ErrorData> {
    // ERROR stays ERROR under the panic model: the eventual catch_unwind at the
    // per-command/task boundary is the handler. CritSectionCount/ExitOnAnyError
    // are deferred.

    let output_to_server = should_output_to_server(elevel);
    let output_to_client = should_output_to_client(elevel);
    if elevel < ERROR && !output_to_server && !output_to_client {
        return None;
    }

    let recursion_trouble = ERROR_STATE.with(|st| {
        let mut st = st.borrow_mut();
        st.recursion_depth += 1;
        if st.in_flight && elevel >= ERROR {
            // Double fault: a >=ERROR raised while another error is still being
            // built (e.g. inside a build closure or an errcontext callback).
            // error.md s2.8: escalate to PANIC (uncatchable abort), replacing the
            // old ERRORDATA_STACK_SIZE overflow guard.
            st.in_flight = false;
            st.recursion_depth = 0;
            drop(st);
            #[allow(deprecated)]
            raise_panic(panic_frame(PANIC, "error raised while another error was in flight"));
        }
        let recursion_trouble = st.recursion_depth > 2;
        st.in_flight = true;
        st.recursion_depth -= 1;
        recursion_trouble
    });

    if recursion_trouble {
        // in_error_recursion_trouble(): drop context callbacks to avoid an
        // infinite error loop. TODO(panic): also clear debug_query_string.
        clear_error_context_stack();
    }

    let dom = domain.unwrap_or("postgres").to_owned();
    Some(ErrorData {
        elevel,
        output_to_server,
        output_to_client,
        context_domain: Some(dom.clone()),
        domain: Some(dom),
        sqlerrcode: default_errcode_for(elevel),
        // saved_errno: snapshot errno so message eval can't change it.
        saved_errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
        ..Default::default()
    })
}

// pg_attribute_cold variant taken for compile-time-constant elevel >= ERROR.
pub fn errstart_cold(elevel: i32, domain: Option<&str>) -> Option<ErrorData> {
    errstart(elevel, domain)
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
#[allow(clippy::needless_pass_by_value, reason = "consumes edata: the single in-flight error, moved into panic_any / abort / emit")]
pub fn errfinish(mut edata: ErrorData, filename: &str, lineno: i32, funcname: &str) {
    // TODO(panic): the >=ERROR path panics (caught by catch_unwind at task
    // spawn) carrying the ErrorData; lower severities log and return.

    // edata is the single in-flight build target (error.md s2.8): the closure
    // and every helper mutated this one value. Record the raise location.
    edata.filename = Some(set_stack_entry_filename(filename));
    edata.lineno = lineno;
    edata.funcname = (!funcname.is_empty()).then(|| funcname.to_owned());

    // Bump recursion_depth across the callback+emit phase (PG errfinish does the
    // same), so an ereport re-entered from a context callback or message eval
    // observes the elevated depth and in_error_recursion_trouble() fires.
    ERROR_STATE.with(|st| st.borrow_mut().recursion_depth += 1);

    // Run errcontext callbacks on the LIVE in-flight error before it is
    // finalized, so they append CONTEXT lines onto the actual ErrorData (PG runs
    // them on errordata[] top calling errcontext_msg), innermost -> outermost.
    run_error_context_callbacks(&mut edata);

    // The build is complete; clear the in-flight slot so the next ereport can
    // start (and so a >=ERROR raised from here on counts as a fresh error).
    ERROR_STATE.with(|st| st.borrow_mut().in_flight = false);

    let elevel = edata.elevel;

    // PANIC (error.md s2.5): crash the process via process::abort() -- never a
    // catchable panic, no Drop -- so a half-updated shared structure is never
    // observed or flushed. abort_for_panic emits to the server log first, then
    // aborts (production) or takes the test abort hook (cfg(test)).
    if elevel >= PANIC {
        abort_for_panic(edata);
    }

    // ERROR / FATAL (error.md s2.1-2.4): raise a catchable panic carrying the
    // ErrorData. ERROR is recovered at the per-command catch (backend-local);
    // FATAL escapes that catch to the task boundary (ends the backend). The
    // task-boundary catch_unwind downcasts the payload back to ErrorData.
    if elevel >= ERROR {
        // PG decrements recursion_depth before PG_RE_THROW; the recovery point
        // (flush_error_state) also resets it.
        ERROR_STATE.with(|st| st.borrow_mut().recursion_depth -= 1);
        // Emit to the server log first so the message is seen even if nobody
        // catches the panic, then raise it as a panic carrying the ErrorData.
        if edata.output_to_server {
            send_message_to_server_log(&edata);
        }
        std::panic::panic_any(edata);
    }

    // log-and-return for WARNING/LOG/NOTICE/INFO/DEBUGx.
    emit_error_report_for(&mut edata);
    ERROR_STATE.with(|st| st.borrow_mut().recursion_depth -= 1);
}

// set_stack_entry_location's filename normalization: keep only the base name.
fn set_stack_entry_filename(filename: &str) -> String {
    filename
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(filename)
        .to_owned()
}

// errsave / ereturn soft-error path. `context` is a *Node (ErrorSaveContext) or
// NULL; modeled as Option. With None it behaves like ereport(ERROR).
#[allow(clippy::needless_pass_by_value, reason = "context consumed (ErrorSaveContext stash); mirrors C signature")]
pub fn errsave_start(context: Option<&mut ()>, domain: Option<&str>) -> Option<ErrorData> {
    // TODO: real ErrorSaveContext stashes the error and returns without raising.
    let _ = context;
    errstart(ERROR, domain)
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
#[allow(clippy::needless_pass_by_value, reason = "consumes context + edata; mirrors C signature")]
pub fn errsave_finish(
    context: Option<&mut ()>,
    edata: ErrorData,
    filename: &str,
    lineno: i32,
    funcname: &str,
) {
    // TODO(panic): if context is a real ErrorSaveContext, stash edata and return;
    // otherwise behave like errfinish(ERROR).
    let _ = context;
    #[allow(deprecated)]
    errfinish(edata, filename, lineno, funcname);
}

// Build a one-shot frame and raise it as a panic (used for internal PANICs that
// must not recurse through the stack machinery, e.g. stack overflow).
fn panic_frame(elevel: i32, msg: &str) -> ErrorData {
    ErrorData {
        elevel,
        sqlerrcode: ERRCODE_INTERNAL_ERROR,
        message: Some(msg.to_owned()),
        output_to_server: true,
        ..Default::default()
    }
}

// Raise a standalone frame (used by re_throw_error / pg_re_throw / internal
// PANICs that bypass the stack machinery). Splits on severity per error.md:
// >= PANIC takes the uncatchable abort path; ERROR/FATAL raise a catchable
// panic carrying the ErrorData.
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
fn raise_panic(edata: ErrorData) -> ! {
    if edata.elevel >= PANIC {
        abort_for_panic(edata);
    }
    send_message_to_server_log(&edata);
    std::panic::panic_any(edata);
}

// Context / position / generic-string accessors (set_errcontext_domain,
// errbacktrace, err_generic_string, geterrcode, geterrposition,
// getinternalerrposition) are methods on ErrorData -- they mutate/read the one
// in-flight build target the closure holds, so there is no split-brain.

// ---------------------------------------------------------------------------
// Constructing error strings outside ereport(). pre_format_elog_string saves
// errno + domain; format_elog_string formats with them. printf varargs collapse
// to a pre-formatted String supplied by the caller.
// ---------------------------------------------------------------------------

thread_local! {
    static SAVE_FORMAT_ERRNUMBER: RefCell<i32> = const { RefCell::new(0) };
}

pub fn pre_format_elog_string(errnumber: i32, _domain: Option<&str>) {
    SAVE_FORMAT_ERRNUMBER.with(|n| *n.borrow_mut() = errnumber);
}

pub fn format_elog_string(fmt: &str) -> String {
    // The caller already expanded printf args via format!(); %m expansion is the
    // only thing left, handled by the same substitution used in messages.
    let errno = SAVE_FORMAT_ERRNUMBER.with(|n| *n.borrow());
    substitute_errno(fmt, errno)
}

// Replace PG's %m with the saved errno's strerror text.
fn substitute_errno(s: &str, errno: i32) -> String {
    if !s.contains("%m") {
        return s.to_owned();
    }
    let msg = std::io::Error::from_raw_os_error(errno).to_string();
    s.replace("%m", &msg)
}

// ---------------------------------------------------------------------------
// PG_TRY / PG_CATCH / PG_RE_THROW. The setjmp/longjmp frame maps to
// catch_unwind; pg_re_throw resumes the unwind (-> !).
// ---------------------------------------------------------------------------

// catch_unwind-based replacement for PG_TRY / PG_CATCH / PG_FINALLY (error.md
// s3.4). `pg_try(body)` runs `body` under catch_unwind and returns a `PgTry`
// builder; `.pg_catch(f)` handles an ErrorData unwind (f may re_throw_error to
// propagate); `.pg_finally(g)` runs g on both paths. Only ErrorData payloads are
// caught -- a non-ErrorData bug-panic, and the PANIC abort path, are never caught
// (pg_finally still runs for an ErrorData unwind, never for an abort).
pub fn pg_try<T>(body: impl FnOnce() -> T + std::panic::UnwindSafe) -> PgTry<T> {
    PgTry { outcome: std::panic::catch_unwind(body) }
}

/// Builder returned by [`pg_try`]. Holds the try closure's outcome: `Ok(T)` or a
/// caught panic payload. Resolve it with [`PgTry::pg_catch`], [`PgTry::pg_finally`],
/// or both; if neither catches an in-flight `ErrorData`, dropping into a resolver
/// re-raises it.
#[must_use = "a PgTry must be resolved with .pg_catch(...) / .pg_finally(...) to surface or re-raise an error"]
pub struct PgTry<T> {
    outcome: std::thread::Result<T>,
}

impl<T> PgTry<T> {
    /// PG_CATCH: if the try raised an `ErrorData`, run `catch(error)` (which
    /// handles it, or calls `re_throw_error` to propagate). Non-`ErrorData`
    /// payloads (bug-panics) are never caught -- they resume unwinding. Yields the
    /// try's `T` on the normal path.
    pub fn pg_catch(self, catch: impl FnOnce(ErrorData) -> T) -> PgCaught<T> {
        let resolved = match self.outcome {
            Ok(v) => Ok(v),
            Err(payload) => match payload.downcast::<ErrorData>() {
                Ok(edata) => Ok(catch(*edata)),
                // Not an ErrorData (bug-panic): stash to resume later so a
                // chained pg_finally still runs first.
                Err(other) => Err(other),
            },
        };
        PgCaught { resolved }
    }

    /// PG_FINALLY: run `finally` on both the normal and error paths, then yield
    /// the try's `T` or re-raise the (uncaught) panic payload.
    pub fn pg_finally(self, finally: impl FnOnce()) -> T {
        finally();
        match self.outcome {
            Ok(v) => v,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

/// Result of a [`PgTry::pg_catch`]: either the (possibly catch-produced) value, or
/// an uncaught non-`ErrorData` payload still to be resumed. Yields `T` directly via
/// [`From`]/deref-style use, or chains a [`PgCaught::pg_finally`].
#[must_use = "a PgCaught must be resolved (yield its value or chain .pg_finally(...))"]
pub struct PgCaught<T> {
    resolved: std::thread::Result<T>,
}

impl<T> PgCaught<T> {
    /// PG_TRY / PG_CATCH / PG_FINALLY combined: run `finally` on both paths after
    /// the catch, then yield the value or resume an uncaught bug-panic.
    pub fn pg_finally(self, finally: impl FnOnce()) -> T {
        finally();
        match self.resolved {
            Ok(v) => v,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Yield the value, resuming an uncaught (non-`ErrorData`) bug-panic. Use when
    /// the chain is `pg_try(t).pg_catch(c)` with no finally.
    pub fn done(self) -> T {
        match self.resolved {
            Ok(v) => v,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

// PG_RE_THROW(): resume the in-flight error. Never returns.
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn pg_re_throw() -> ! {
    // The in-flight ErrorData lives in the panic payload, which the surrounding
    // pg_try/catch_unwind resumes; this entry point is reached only with no
    // payload to resume. Under the single-slot model there is no stacked frame to
    // recover, so this is a lost error -> escalate to FATAL.
    ERROR_STATE.with(|st| st.borrow_mut().in_flight = false);
    #[allow(deprecated)]
    raise_panic(panic_frame(FATAL, "pg_re_throw with no error in progress"));
}

// ---------------------------------------------------------------------------
// ErrorData lifecycle + reporting.
// ---------------------------------------------------------------------------

// EmitErrorReport: output an error to its enabled destinations. Under the
// single-slot model the in-flight error is a value held by the raise path (or
// the caught panic payload), so the caller passes it in rather than reading a
// persistent errordata[] top.
pub fn emit_error_report(edata: &ErrorData) {
    let mut edata = edata.clone();
    emit_error_report_for(&mut edata);
}

// Report a recovered ERROR (caught at the per-command recovery point) to its
// enabled destinations. The errfinish that raised it already popped the stack, so
// recovery reports the caught ErrorData directly rather than the stack top.
pub fn report_recovered_error(edata: &ErrorData) {
    let mut edata = edata.clone();
    emit_error_report_for(&mut edata);
}

fn emit_error_report_for(edata: &mut ErrorData) {
    // emit_log_hook may turn off output_to_server.
    if edata.output_to_server {
        EMIT_LOG_HOOK.with(|h| {
            if let Some(hook) = h.borrow_mut().as_mut() {
                hook(edata);
            }
        });
    }
    if edata.output_to_server {
        send_message_to_server_log(edata);
    }
    if edata.output_to_client {
        send_message_to_frontend(edata);
    }
}

// CopyErrorData: a copy of an error for handling. Under Rust ownership the
// caught error is the panic payload (or the in-flight value); copying it is a
// plain clone of that value.
pub fn copy_error_data(edata: &ErrorData) -> ErrorData {
    edata.clone()
}

// FreeErrorData is a no-op under Rust ownership (drop handles it).
pub fn free_error_data(_edata: ErrorData) {}

// Reset the error state after recovery (clears any in-flight build slot).
pub fn flush_error_state() {
    ERROR_STATE.with(|st| {
        let mut st = st.borrow_mut();
        st.in_flight = false;
        st.recursion_depth = 0;
    });
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn re_throw_error(mut edata: ErrorData) -> ! {
    // ReThrowError: re-raise a previously copied ERROR.
    edata.elevel = ERROR;
    #[allow(deprecated)]
    raise_panic(edata);
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
#[allow(clippy::needless_pass_by_value, reason = "consumes edata (the error to throw)")]
pub fn throw_error_data(edata: ErrorData) {
    // ThrowErrorData: report an error described by a standalone ErrorData. Open
    // an in-flight slot for gating + raise semantics, carry the standalone
    // fields onto it (it is the single build target), then finish.
    let elevel = edata.elevel;
    let domain = edata.domain.clone();
    if let Some(mut frame) = errstart(elevel, domain.as_deref()) {
        let (file, line, func) = (
            edata.filename.clone().unwrap_or_default(),
            edata.lineno,
            edata.funcname.clone().unwrap_or_default(),
        );
        frame = ErrorData {
            elevel: frame.elevel,
            output_to_server: frame.output_to_server,
            output_to_client: frame.output_to_client,
            saved_errno: frame.saved_errno,
            ..edata
        };
        #[allow(deprecated)]
        errfinish(frame, &file, line, &func);
    }
}

// GetErrorContextStack: run the context callbacks against a throwaway error to
// build a context string (callbacks call errcontext_msg on the &mut ErrorData).
pub fn get_error_context_stack() -> String {
    let mut frame = ErrorData::default();
    run_error_context_callbacks(&mut frame);
    frame.context.unwrap_or_default()
}

// PG's `ErrorContextCallback *error_context_stack` global is per-task state under
// the async single-process model (error.md s2.8): the errcontext callback chain
// that produces CONTEXT: lines, registered/popped by RAII and walked at raise
// time. Held thread-local (TODO(panic): tokio task_local! once tasks are tokio
// tasks). Callbacks are pushed innermost-last; errfinish walks them in reverse
// (innermost -> outermost), matching PG.
thread_local! {
    static ERROR_CONTEXT_STACK: RefCell<Vec<ErrorContextCallback>> = const { RefCell::new(Vec::new()) };
}

/// Register an errcontext callback; returns its index for `pop_error_context_callback`.
/// (PG pushes onto `error_context_stack` via the intrusive `previous` link; RAII
/// guards at the call site pop it.)
pub fn push_error_context_callback(cb: ErrorContextCallback) -> usize {
    ERROR_CONTEXT_STACK.with(|s| {
        let mut s = s.borrow_mut();
        s.push(cb);
        s.len() - 1
    })
}

/// Pop the most-recently-registered errcontext callback(s) down to `index`.
pub fn pop_error_context_callback(index: usize) {
    ERROR_CONTEXT_STACK.with(|s| s.borrow_mut().truncate(index));
}

/// Drop all registered errcontext callbacks (recursion-trouble / reset).
pub fn clear_error_context_stack() {
    ERROR_CONTEXT_STACK.with(|s| s.borrow_mut().clear());
}

// Run each registered errcontext callback against the live in-flight error,
// innermost -> outermost, so they append CONTEXT lines via errcontext_msg.
fn run_error_context_callbacks(edata: &mut ErrorData) {
    ERROR_CONTEXT_STACK.with(|s| {
        for cb in s.borrow_mut().iter_mut().rev() {
            (cb.callback)(edata);
        }
    });
}

// emit_log_hook: void(*)(ErrorData*) -> optional captured closure. PG's global
// is installed once at module load; under the async model we hold it per-task
// (thread_local!, matching ERROR_STATE/ERROR_CONTEXT_STACK) -- no static mut, no
// cross-task data race. TODO(panic): tokio task_local! once tasks are tokio tasks.
pub type EmitLogHook = Box<dyn FnMut(&mut ErrorData)>;
thread_local! {
    pub static EMIT_LOG_HOOK: RefCell<Option<EmitLogHook>> = const { RefCell::new(None) };
}

// ---------------------------------------------------------------------------
// Log formatting / destinations.
// ---------------------------------------------------------------------------

// log_status_format: append log_line_prefix-formatted status to `buf`. The full
// %-grammar (pid/user/db/timestamps/...) needs GUC + session state that is
// deferred; we honor only the always-available codes and skip the rest.
pub fn log_status_format(buf: &mut String, format: &str, edata: &ErrorData) {
    if format.is_empty() {
        return;
    }
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            buf.push(c);
            continue;
        }
        match chars.next() {
            None => break,
            Some('%') => buf.push('%'),
            Some('e') => buf.push_str(&unpack_sql_state(edata.sqlerrcode)),
            // pid/user/db/host/timestamps/etc need deferred session+GUC state.
            Some(_) => {}
        }
    }
}

pub fn debug_file_open() {
    // OutputFileName redirection is a deferred GUC concern; nothing to do.
}

// Unpack a MAKE_SQLSTATE code into its 5-char text form.
pub fn unpack_sql_state(mut sql_state: i32) -> String {
    let mut buf = String::with_capacity(5);
    for _ in 0..5 {
        buf.push(pg_unsixbit(sql_state as u32) as char);
        sql_state >>= 6;
    }
    buf
}

pub fn reset_formatted_start_time() {
    // Formatted-timestamp caching is part of the deferred log_line_prefix path.
}
pub fn get_formatted_start_time() -> String {
    unimplemented!() // TODO: deferred log_line_prefix timestamp formatting
}
pub fn get_formatted_log_time() -> String {
    unimplemented!() // TODO: deferred log_line_prefix timestamp formatting
}
pub fn get_backend_type_for_log() -> &'static str {
    "backend" // TODO: deferred -- needs MyBackendType / bgworker entry
}
pub fn check_log_of_query(edata: &ErrorData) -> bool {
    // Needs debug_query_string + log_min_error_statement GUC; deferred.
    let _ = edata;
    false
}

pub fn error_severity(elevel: i32) -> &'static str {
    match elevel {
        DEBUG1 | DEBUG2 | DEBUG3 | DEBUG4 | DEBUG5 => "DEBUG",
        LOG | LOG_SERVER_ONLY => "LOG",
        INFO => "INFO",
        NOTICE => "NOTICE",
        WARNING | WARNING_CLIENT_ONLY => "WARNING",
        ERROR => "ERROR",
        FATAL => "FATAL",
        PANIC => "PANIC",
        _ => "???",
    }
}

pub fn write_pipe_chunks(_data: &[u8], _dest: i32) {
    unimplemented!() // TODO: deferred -- syslogger chunked-pipe protocol
}

pub fn write_csvlog(_edata: &ErrorData) {
    unimplemented!() // TODO: deferred -- csvlog.c is a separate stage
}
pub fn write_jsonlog(_edata: &ErrorData) {
    unimplemented!() // TODO: deferred -- jsonlog.c is a separate stage
}

// append_with_tabs: append, inserting a tab after every newline.
fn append_with_tabs(buf: &mut String, s: &str) {
    for ch in s.chars() {
        buf.push(ch);
        if ch == '\n' {
            buf.push('\t');
        }
    }
}

// send_message_to_server_log: format the full report and write it to stderr.
// syslog/eventlog/csvlog/jsonlog/syslogger-pipe destinations are deferred.
fn send_message_to_server_log(edata: &ErrorData) {
    let mut buf = String::new();

    log_status_format(&mut buf, "", edata); // Log_line_prefix deferred (empty)
    buf.push_str(error_severity(edata.elevel));
    buf.push_str(":  ");

    if LOG_ERROR_VERBOSITY == PgErrorVerbosity::Verbose {
        buf.push_str(&unpack_sql_state(edata.sqlerrcode));
        buf.push_str(": ");
    }

    match edata.message.as_deref() {
        Some(m) => append_with_tabs(&mut buf, &substitute_errno(m, edata.saved_errno)),
        None => append_with_tabs(&mut buf, "missing error text"),
    }

    if edata.cursorpos > 0 {
        write!(buf, " at character {}", edata.cursorpos).unwrap();
    } else if edata.internalpos > 0 {
        write!(buf, " at character {}", edata.internalpos).unwrap();
    }
    buf.push('\n');

    if LOG_ERROR_VERBOSITY != PgErrorVerbosity::Terse {
        if let Some(d) = edata.detail_log.as_deref().or(edata.detail.as_deref()) {
            buf.push_str("DETAIL:  ");
            append_with_tabs(&mut buf, d);
            buf.push('\n');
        }
        if let Some(h) = edata.hint.as_deref() {
            buf.push_str("HINT:  ");
            append_with_tabs(&mut buf, h);
            buf.push('\n');
        }
        if let Some(q) = edata.internalquery.as_deref() {
            buf.push_str("QUERY:  ");
            append_with_tabs(&mut buf, q);
            buf.push('\n');
        }
        if let Some(ctx) = edata.context.as_deref()
            && !edata.hide_ctx {
                buf.push_str("CONTEXT:  ");
                append_with_tabs(&mut buf, ctx);
                buf.push('\n');
            }
        if LOG_ERROR_VERBOSITY == PgErrorVerbosity::Verbose {
            match (edata.funcname.as_deref(), edata.filename.as_deref()) {
                (Some(func), Some(file)) => {
                    writeln!(buf, "LOCATION:  {func}, {file}:{}", edata.lineno).unwrap();
                }
                (None, Some(file)) => {
                    writeln!(buf, "LOCATION:  {file}:{}", edata.lineno).unwrap();
                }
                _ => {}
            }
        }
        if let Some(bt) = edata.backtrace.as_deref() {
            buf.push_str("BACKTRACE:  ");
            append_with_tabs(&mut buf, bt);
            buf.push('\n');
        }
    }

    if (LOG_DESTINATION & LOG_DESTINATION_STDERR) != 0 {
        write_console(&buf);
    }
}

// send_message_to_frontend: the protocol-encoded client send is deferred (needs
// the libpq be-side + connected frontend). TODO(panic): wire to pqcomm.
fn send_message_to_frontend(_edata: &ErrorData) {
    // TODO: deferred -- pq_beginmessage/pq_sendstring/pq_endmessage frontend send.
}

// write_console: raw write to stderr (PG's non-win32 path).
fn write_console(line: &str) {
    let _ = std::io::stderr().write_all(line.as_bytes());
    let _ = std::io::stderr().flush();
}

// Pre-elog stderr writers. Varargs collapse to a pre-formatted &str.
pub fn write_stderr(msg: &str) {
    eprint!("{msg}");
    let _ = std::io::stderr().flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::errcodes::make_sqlstate;
    use std::panic::catch_unwind;

    fn drain_state() {
        flush_error_state();
    }

    #[test]
    fn error_raises_panic_with_downcastable_errordata() {
        drain_state();
        let sqlstate = make_sqlstate(b'2', b'2', b'0', b'1', b'2'); // division_by_zero-ish
        let result = catch_unwind(|| {
            #[allow(unused_must_use)]
            {
                if let Some(mut e) = errstart(ERROR, None) {
                    e.errcode(sqlstate).errmsg("boom: it broke");
                    #[allow(deprecated)]
                    errfinish(e, "elog.rs", 1, "test_fn");
                }
            }
        });
        let payload = result.expect_err("ERROR must panic");
        let edata = payload
            .downcast_ref::<ErrorData>()
            .expect("panic payload must downcast to ErrorData");
        assert_eq!(edata.elevel, ERROR);
        assert_eq!(edata.sqlerrcode, sqlstate);
        assert_eq!(edata.message.as_deref(), Some("boom: it broke"));
        drain_state();
    }

    #[test]
    fn warning_returns_normally() {
        drain_state();
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(WARNING, None) {
                e.errmsg("just a warning");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 2, "test_fn");
            }
        });
        assert!(result.is_ok(), "WARNING must not panic");
        drain_state();
    }

    #[test]
    fn log_returns_normally() {
        drain_state();
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(LOG, None) {
                e.errmsg("log line");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 3, "test_fn");
            }
        });
        assert!(result.is_ok(), "LOG must not panic");
        drain_state();
    }

    #[test]
    fn unpack_sql_state_roundtrip() {
        let code = make_sqlstate(b'4', b'2', b'7', b'0', b'3');
        assert_eq!(unpack_sql_state(code), "42703");
    }

    #[test]
    fn panic_takes_abort_path_via_hook() {
        drain_state();
        test_abort::reset_abort_count();
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(PANIC, None) {
                e.errmsg("corruption: take abort path");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 1, "test_fn");
            }
        });
        // Under cfg(test) the abort hook raises a catchable panic carrying the
        // ErrorData (production would have process::abort()ed here).
        assert_eq!(test_abort::abort_count(), 1, "PANIC must take the abort path");
        let payload = result.expect_err("PANIC must diverge");
        let edata = payload
            .downcast_ref::<ErrorData>()
            .expect("abort-hook payload downcasts to ErrorData");
        assert_eq!(edata.elevel, PANIC);
        drain_state();
        test_abort::reset_abort_count();
    }

    // --- pg_try builder (error.md s3.4) ---

    fn raise_error(msg: &str) {
        if let Some(mut e) = errstart(ERROR, None) {
            e.errmsg(msg);
            #[allow(deprecated)]
            errfinish(e, "elog.rs", 1, "test_fn");
        }
    }

    #[test]
    fn pg_try_ok_passthrough() {
        drain_state();
        let v = pg_try(|| 7).pg_catch(|_e| 0).done();
        assert_eq!(v, 7);
        drain_state();
    }

    #[test]
    fn pg_try_error_caught_not_reraised() {
        drain_state();
        let mut caught = None;
        let v = pg_try(|| {
            raise_error("boom in try");
            0
        })
        .pg_catch(|e| {
            caught = Some(e.message);
            42 // handled: produce a recovery value
        })
        .done();
        assert_eq!(v, 42);
        assert_eq!(caught, Some(Some("boom in try".to_string())));
        drain_state();
    }

    #[test]
    fn pg_try_re_throw_propagates() {
        drain_state();
        let result = catch_unwind(|| {
            pg_try(|| {
                raise_error("propagate me");
                0
            })
            .pg_catch(|e| {
                #[allow(deprecated)]
                re_throw_error(e); // never returns
            })
            .done()
        });
        let payload = result.expect_err("re_throw_error must propagate");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, ERROR);
        assert_eq!(edata.message.as_deref(), Some("propagate me"));
        drain_state();
    }

    #[test]
    fn pg_finally_runs_on_ok_path() {
        drain_state();
        let mut ran = false;
        let v = pg_try(|| 5).pg_finally(|| ran = true);
        assert_eq!(v, 5);
        assert!(ran, "pg_finally must run on the ok path");
        drain_state();
    }

    #[test]
    fn pg_finally_runs_on_error_path() {
        drain_state();
        let mut ran = false;
        let result = catch_unwind(std::panic::AssertUnwindSafe(|| {
            pg_try(|| {
                raise_error("boom");
                0
            })
            .pg_finally(|| ran = true)
        }));
        assert!(ran, "pg_finally must run on the error path");
        let payload = result.expect_err("an unhandled error re-raises after finally");
        assert!(payload.downcast_ref::<ErrorData>().is_some());
        drain_state();
    }

    #[test]
    fn pg_catch_then_finally_combined() {
        drain_state();
        let mut order = Vec::new();
        let v = pg_try(|| {
            raise_error("boom");
            0
        })
        .pg_catch(|_e| {
            order.push("catch");
            9
        })
        .pg_finally(|| order.push("finally"));
        assert_eq!(v, 9);
        assert_eq!(order, ["catch", "finally"]);
        drain_state();
    }

    // --- unified build target (error.md s2.8, ITEM 1) ---

    // A sqlstate set by errcode_for_file_access plus a cursor position plus an
    // errmsg must ALL survive on the one in-flight ErrorData -- no clobber from a
    // separate working copy (the old split-brain builder lost the helper's code).
    #[test]
    fn unified_builder_helper_code_and_message_both_survive() {
        use crate::utils::errcodes::ERRCODE_UNDEFINED_FILE;
        drain_state();
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(ERROR, None) {
                // Simulate a "file not found" errno snapshot then derive the code.
                e.saved_errno =
                    std::io::Error::from(std::io::ErrorKind::NotFound).raw_os_error().unwrap_or(2);
                e.errcode_for_file_access().errposition(42).errmsg("could not open file");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 1, "test_fn");
            }
        });
        let payload = result.expect_err("ERROR must panic");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.sqlerrcode, ERRCODE_UNDEFINED_FILE, "helper's sqlstate must survive");
        assert_eq!(edata.message.as_deref(), Some("could not open file"));
        assert_eq!(edata.cursorpos, 42, "errposition must survive");
        // geterrcode reads back the same in-flight value mid-build.
        drain_state();
    }

    #[test]
    fn geterrcode_reads_in_flight_value() {
        drain_state();
        let mut e = errstart(ERROR, None).expect("ERROR always starts");
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FILE);
        assert_eq!(e.geterrcode(), crate::utils::errcodes::ERRCODE_UNDEFINED_FILE);
        // Discard without finishing: clear the in-flight slot.
        drain_state();
    }

    // --- errcontext callbacks (error.md s2.8, ITEM 2) ---

    // A registered errcontext callback must append a CONTEXT line onto the raised
    // error's context (it now receives &mut ErrorData and runs before finalize).
    #[test]
    fn errcontext_callback_appends_context_to_error() {
        drain_state();
        clear_error_context_stack();
        push_error_context_callback(ErrorContextCallback::new(|e: &mut ErrorData| {
            e.errcontext_msg("while doing the thing");
        }));
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(ERROR, None) {
                e.errmsg("boom with context");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 1, "test_fn");
            }
        });
        clear_error_context_stack();
        let payload = result.expect_err("ERROR must panic");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.context.as_deref(), Some("while doing the thing"));
        assert_eq!(edata.message.as_deref(), Some("boom with context"));
        drain_state();
    }

    // --- double-fault -> PANIC (error.md s2.8, ITEM 3) ---

    // Raising an ERROR while another error is in flight (e.g. from inside an
    // errcontext callback) is a double fault and must escalate to the PANIC abort
    // path (uncatchable in prod; the test hook makes it observable).
    #[test]
    fn double_fault_in_callback_escalates_to_panic() {
        drain_state();
        test_abort::reset_abort_count();
        clear_error_context_stack();
        push_error_context_callback(ErrorContextCallback::new(|_e: &mut ErrorData| {
            // Raise a second ERROR while the first is still in flight.
            if let Some(mut e2) = errstart(ERROR, None) {
                e2.errmsg("nested error during context");
                #[allow(deprecated)]
                errfinish(e2, "elog.rs", 2, "cb");
            }
        }));
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(ERROR, None) {
                e.errmsg("outer error");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 1, "test_fn");
            }
        });
        clear_error_context_stack();
        assert_eq!(test_abort::abort_count(), 1, "double fault must take the PANIC abort path");
        let payload = result.expect_err("double fault must diverge");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, PANIC);
        drain_state();
        test_abort::reset_abort_count();
    }

    // WARNING/NOTICE/LOG build+emit+return and clear the single slot, so a later
    // ERROR is not a spurious double fault.
    #[test]
    fn warning_clears_slot_then_error_raises_cleanly() {
        drain_state();
        if let Some(mut w) = errstart(WARNING, None) {
            w.errmsg("just a warning");
            #[allow(deprecated)]
            errfinish(w, "elog.rs", 1, "test_fn");
        }
        let result = catch_unwind(|| {
            if let Some(mut e) = errstart(ERROR, None) {
                e.errmsg("real error after warning");
                #[allow(deprecated)]
                errfinish(e, "elog.rs", 2, "test_fn");
            }
        });
        let payload = result.expect_err("ERROR must panic (not a double fault)");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, ERROR);
        assert_eq!(edata.message.as_deref(), Some("real error after warning"));
        drain_state();
    }

    // --- crate::assert! (error.md s3.2) ---

    #[test]
    fn crate_assert_pass_is_noop() {
        drain_state();
        test_abort::reset_abort_count();
        crate::assert!(1 + 1 == 2);
        crate::assert!(true, "should not fire: {}", 1);
        assert_eq!(test_abort::abort_count(), 0);
        drain_state();
    }

    #[cfg(debug_assertions)]
    #[test]
    fn crate_assert_fail_takes_panic_abort_path() {
        drain_state();
        test_abort::reset_abort_count();
        let result = catch_unwind(|| {
            crate::assert!(2 + 2 == 5, "math broke: {}", 5);
        });
        assert_eq!(test_abort::abort_count(), 1, "failing assert takes the abort path");
        let payload = result.expect_err("crate::assert! failure must diverge");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, PANIC);
        drain_state();
        test_abort::reset_abort_count();
    }
}
