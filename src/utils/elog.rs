//! Translated from PostgreSQL src/include/utils/elog.h
//!
//! Error model: PG's `elog`/`ereport` use setjmp/longjmp. We keep `>= ERROR`
//! semantics as a `panic!` whose payload is the structured `ErrorData` value
//! (`std::panic::panic_any(edata)`), to be caught by a future `catch_unwind` at
//! the task boundary and downcast back to `ErrorData`. Lower severities
//! log-and-return. Every >=ERROR raising path is marked
//! `#[deprecated(note = "TODO(panic): migrate to Result + ?")]` + `// TODO(panic)`.
//!
//! The process-global errordata stack, recursion_depth and error_context_stack
//! become per-task (thread-local) state under the single-process async model;
//! palloc/MemoryContext are tombstoned to Rust ownership (ErrorData owns its
//! `String`s). Subsystems not in this step (GUC knobs, libpq frontend send,
//! syslog/csvlog/jsonlog, ps_display, pgstat, log_line_prefix grammar) are
//! deferred: where elog.c calls into them we call the existing stubs.
//!
//! The function/global bodies declared here live in the backend definition
//! module (crate::backend::utils::error::elog) and are re-exported below.

use crate::utils::errcodes::make_sqlstate;

// StringInfo is tombstoned to std types (see lib::stringinfo); ErrorData strings
// are owned `String`s and log_status_format appends into a `String` buffer.

// ---------------------------------------------------------------------------
// Error level codes (elevel) -- consts, not an enum, to preserve the numeric
// ordering used pervasively (`elevel >= ERROR`).
// ---------------------------------------------------------------------------

pub const DEBUG5: i32 = 10;
pub const DEBUG4: i32 = 11;
pub const DEBUG3: i32 = 12;
pub const DEBUG2: i32 = 13;
pub const DEBUG1: i32 = 14;
pub const LOG: i32 = 15;
pub const LOG_SERVER_ONLY: i32 = 16;
pub const COMMERROR: i32 = LOG_SERVER_ONLY;
pub const INFO: i32 = 17;
pub const NOTICE: i32 = 18;
pub const WARNING: i32 = 19;
pub const PGWARNING: i32 = 19;
pub const WARNING_CLIENT_ONLY: i32 = 20;
pub const ERROR: i32 = 21;
pub const PGERROR: i32 = 21;
pub const FATAL: i32 = 22;
pub const PANIC: i32 = 23;

// ---------------------------------------------------------------------------
// SQLSTATE compact encoding. MAKE_SQLSTATE / PGSIXBIT live in errcodes (see
// crate::utils::errcodes::make_sqlstate / pg_six_bit); re-export PGUNSIXBIT and
// the category helpers here.
// ---------------------------------------------------------------------------

pub const fn pg_unsixbit(val: u32) -> u8 {
    ((val & 0x3F) + (b'0' as u32)) as u8
}

pub const fn errcode_to_category(ec: i32) -> i32 {
    ec & ((1 << 12) - 1)
}

pub const fn errcode_is_category(ec: i32) -> bool {
    (ec & !((1 << 12) - 1)) == 0
}

// Default SQLSTATE codes errcode() falls back to, by severity.
pub const ERRCODE_INTERNAL_ERROR: i32 = make_sqlstate(b'X', b'X', b'0', b'0', b'0');
pub const ERRCODE_WARNING: i32 = make_sqlstate(b'0', b'1', b'0', b'0', b'0');
pub const ERRCODE_SUCCESSFUL_COMPLETION: i32 = make_sqlstate(b'0', b'0', b'0', b'0', b'0');

// ---------------------------------------------------------------------------
// Log destination bitmap
// ---------------------------------------------------------------------------

pub const LOG_DESTINATION_STDERR: i32 = 1;
pub const LOG_DESTINATION_SYSLOG: i32 = 2;
pub const LOG_DESTINATION_EVENTLOG: i32 = 4;
pub const LOG_DESTINATION_CSVLOG: i32 = 8;
pub const LOG_DESTINATION_JSONLOG: i32 = 16;

// ---------------------------------------------------------------------------
// PGErrorVerbosity (GUC) -- exhaustive enum.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgErrorVerbosity {
    Terse,
    Default,
    Verbose,
}

// ---------------------------------------------------------------------------
// Deferred GUC parameters. errstart's gating reads these; real values come from
// the GUC subsystem later. For now they reflect elog.c's static initializers,
// so >=ERROR always raises and lower severities reach stderr.
// ---------------------------------------------------------------------------

pub const LOG_MIN_MESSAGES: i32 = WARNING;
pub const CLIENT_MIN_MESSAGES: i32 = NOTICE;
pub const LOG_DESTINATION: i32 = LOG_DESTINATION_STDERR;
pub const LOG_ERROR_VERBOSITY: PgErrorVerbosity = PgErrorVerbosity::Default;

// ---------------------------------------------------------------------------
// ErrorData -- in-memory accumulator for one ereport() cycle. All the palloc'd
// `char *` become owned `String`; the const message_id stays `&'static str`.
// `assoc_context` (MemoryContextData*) is dropped under the arena model.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ErrorData {
    pub elevel: i32,
    pub output_to_server: bool,
    pub output_to_client: bool,
    pub hide_stmt: bool,
    pub hide_ctx: bool,
    pub filename: Option<String>,
    pub lineno: i32,
    pub funcname: Option<String>,
    pub domain: Option<String>,
    pub context_domain: Option<String>,
    pub sqlerrcode: i32,
    pub message: Option<String>,
    pub detail: Option<String>,
    pub detail_log: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
    pub backtrace: Option<String>,
    pub message_id: Option<&'static str>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub datatype_name: Option<String>,
    pub constraint_name: Option<String>,
    pub cursorpos: i32,
    pub internalpos: i32,
    pub internalquery: Option<String>,
    pub saved_errno: i32,
    // assoc_context (MemoryContextData*) dropped under arena/RAII memory model.
}

// ---------------------------------------------------------------------------
// ErrorContextCallback. Per function-mapping 6.3 the C `void (*callback)(void
// *arg)` + `void *arg` collapse into a single captured closure. The intrusive
// `previous` link becomes an explicit stack (error_context_stack below).
// ---------------------------------------------------------------------------

// TODO(panic): error_context_stack should be task-local state, not a process
// global. Modeled here as a list of boxed closures. PG's callback receives the
// live errordata stack top; here it receives `&mut ErrorData` (the single
// in-flight error) so callbacks can append CONTEXT lines via errcontext_msg.
pub struct ErrorContextCallback {
    pub callback: Box<dyn FnMut(&mut ErrorData)>,
}

impl ErrorContextCallback {
    pub fn new(callback: impl FnMut(&mut ErrorData) + 'static) -> Self {
        Self { callback: Box::new(callback) }
    }
}

// ---------------------------------------------------------------------------
// Definitions translated from elog.c live in the backend module; re-export the
// header-declared API so `crate::utils::elog::<name>` call sites keep resolving.
// ---------------------------------------------------------------------------

pub use crate::backend::utils::error::elog::{
    check_log_of_query, copy_error_data, debug_file_open, emit_error_report, errdetail_log_plural,
    errdetail_plural, errhint_plural, errmsg_plural, errsave_start, errstart, errstart_cold,
    error_severity, flush_error_state, format_elog_string, free_error_data, get_backend_type_for_log,
    get_error_context_stack, get_formatted_log_time, get_formatted_start_time,
    clear_error_context_stack, in_error_recursion_trouble, log_status_format,
    message_level_is_interesting, pg_try, pop_error_context_callback, pre_format_elog_string,
    push_error_context_callback, report_recovered_error,
    reset_formatted_start_time, unpack_sql_state, write_csvlog,
    write_jsonlog, write_pipe_chunks, write_stderr, PgCaught, PgTry, EMIT_LOG_HOOK,
};

#[allow(deprecated)]
pub use crate::backend::utils::error::elog::{
    errfinish, errsave_finish, pg_re_throw, re_throw_error, throw_error_data,
};

// ---------------------------------------------------------------------------
// ereport! / elog! macros. printf varargs collapse to format!() at call sites.
//
//   ereport!(ERROR, |e| e.errcode(...).errmsg(format!(...)));
//   elog!(ERROR, format!("..."));
//
// For elevel >= ERROR these never return (panic). Marked TODO(panic).
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! ereport {
    ($elevel:expr, $build:expr) => {{
        // TODO(panic): >=ERROR panics; lower severities log and return.
        if let Some(mut __edata) = $crate::utils::elog::errstart($elevel, None) {
            let __build: &dyn Fn(&mut $crate::utils::elog::ErrorData) = &$build;
            __build(&mut __edata);
            #[allow(deprecated)]
            $crate::utils::elog::errfinish(__edata, file!(), line!() as i32, "");
        }
    }};
}

#[macro_export]
macro_rules! elog {
    ($elevel:expr, $msg:expr) => {{
        // TODO(panic): old-style elog; >=ERROR panics, lower logs.
        $crate::ereport!($elevel, |__e: &mut $crate::utils::elog::ErrorData| {
            __e.errmsg_internal($msg);
        });
    }};
}

// ---------------------------------------------------------------------------
// errsave! / ereturn! -- the "soft error" reporting macros (elog.h). The first
// argument is the soft-error context (`Option<&mut ErrorSaveContext>`); the rest
// is the same build closure as ereport!. With a context that requests soft
// handling this records the error WITHOUT unwinding; otherwise it raises ERROR.
//
//   errsave!(escontext, |e| e.errcode(...).errmsg(format!(...)));
//   ereturn!(escontext, <dummy>, |e| e.errcode(...).errmsg(format!(...)));
//
// `ereturn!` additionally `return`s the dummy value (PG's "return dummy_value")
// -- so the calling function short-circuits after a soft error just as it does
// after the hard ereport(ERROR) it replaces.
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! errsave {
    ($context:expr, $build:expr) => {{
        // Reborrow so the caller keeps its &mut for a later SOFT_ERROR_OCCURRED.
        let __ctx: ::core::option::Option<&mut $crate::nodes::miscnodes::ErrorSaveContext> =
            $context;
        match __ctx {
            ::core::option::Option::Some(__c) => {
                // Reborrow across the two calls: errsave_start borrows first, then
                // errsave_finish. Neither borrow is held across the other.
                if let Some(mut __edata) = $crate::utils::elog::errsave_start(
                    ::core::option::Option::Some(&mut *__c),
                    None,
                ) {
                    let __build: &dyn Fn(&mut $crate::utils::elog::ErrorData) = &$build;
                    __build(&mut __edata);
                    #[allow(deprecated)]
                    $crate::utils::elog::errsave_finish(
                        ::core::option::Option::Some(&mut *__c),
                        __edata,
                        file!(),
                        line!() as i32,
                        "",
                    );
                }
            }
            ::core::option::Option::None => {
                // No context: hard-error path, exactly ereport!(ERROR).
                if let Some(mut __edata) =
                    $crate::utils::elog::errsave_start(::core::option::Option::None, None)
                {
                    let __build: &dyn Fn(&mut $crate::utils::elog::ErrorData) = &$build;
                    __build(&mut __edata);
                    #[allow(deprecated)]
                    $crate::utils::elog::errsave_finish(
                        ::core::option::Option::None,
                        __edata,
                        file!(),
                        line!() as i32,
                        "",
                    );
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! ereturn {
    ($context:expr, $dummy:expr, $build:expr) => {{
        $crate::errsave!($context, $build);
        return $dummy;
    }};
}

// ---------------------------------------------------------------------------
// OrElog (error.md s3.3). The sanctioned replacement for bare
// `unwrap`/`expect`/`panic!` in non-test code: on None/Err it raises the named
// severity through the elog path (capturing the `Err` Display as errdetail)
// instead of an opaque panic. The `_with` variants take a LAZY closure so the
// happy path allocates nothing. `use crate::utils::elog::OrElog;` at call sites.
// ---------------------------------------------------------------------------

/// Raise `elevel` with `msg` (and optional `detail`) via errstart/errfinish.
/// For `>= ERROR` this diverges; the `-> !` callers below rely on that.
fn raise_or_elog(elevel: i32, msg: String, detail: Option<String>) -> ! {
    if let Some(mut edata) = errstart(elevel, None) {
        edata.errmsg(msg);
        if let Some(d) = detail {
            edata.errdetail(d);
        }
        #[allow(deprecated)]
        errfinish(edata, file!(), line!() as i32, "");
    }
    // errstart returned None (gated off) for a `>= ERROR` severity: this cannot
    // happen (errstart never gates out `>= ERROR`), but the contract is divergence.
    unreachable!("OrElog severity >= ERROR always raises");
}

/// Extension trait raising an elog error in place of `unwrap`/`expect`.
/// Implemented for `Option<T>` and `Result<T, E: Display>`.
pub trait OrElog<T> {
    /// On None/Err: raise `ERROR` with a generic default message.
    fn unwrap_or_error(self) -> T;
    /// On None/Err: raise `ERROR` with a lazily-built message.
    fn unwrap_or_error_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T;
    /// On None/Err: raise `FATAL` with a generic default message.
    fn unwrap_or_fatal(self) -> T;
    /// On None/Err: raise `FATAL` with a lazily-built message.
    fn unwrap_or_fatal_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T;
    /// On None/Err: raise `PANIC` (corruption -> uncatchable abort).
    fn unwrap_or_panic(self) -> T;
    /// On None/Err: raise `PANIC` with a lazily-built message.
    fn unwrap_or_panic_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T;
}

const OR_ELOG_NULL_MSG: &str = "unexpected null value";

impl<T> OrElog<T> for Option<T> {
    fn unwrap_or_error(self) -> T {
        self.unwrap_or_else(|| raise_or_elog(ERROR, OR_ELOG_NULL_MSG.to_owned(), None))
    }
    fn unwrap_or_error_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T {
        self.unwrap_or_else(|| raise_or_elog(ERROR, f().into(), None))
    }
    fn unwrap_or_fatal(self) -> T {
        self.unwrap_or_else(|| raise_or_elog(FATAL, OR_ELOG_NULL_MSG.to_owned(), None))
    }
    fn unwrap_or_fatal_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T {
        self.unwrap_or_else(|| raise_or_elog(FATAL, f().into(), None))
    }
    fn unwrap_or_panic(self) -> T {
        self.unwrap_or_else(|| raise_or_elog(PANIC, OR_ELOG_NULL_MSG.to_owned(), None))
    }
    fn unwrap_or_panic_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T {
        self.unwrap_or_else(|| raise_or_elog(PANIC, f().into(), None))
    }
}

impl<T, E: std::fmt::Display> OrElog<T> for Result<T, E> {
    fn unwrap_or_error(self) -> T {
        self.unwrap_or_else(|e| raise_or_elog(ERROR, e.to_string(), None))
    }
    fn unwrap_or_error_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T {
        self.unwrap_or_else(|e| raise_or_elog(ERROR, f().into(), Some(e.to_string())))
    }
    fn unwrap_or_fatal(self) -> T {
        self.unwrap_or_else(|e| raise_or_elog(FATAL, e.to_string(), None))
    }
    fn unwrap_or_fatal_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T {
        self.unwrap_or_else(|e| raise_or_elog(FATAL, f().into(), Some(e.to_string())))
    }
    fn unwrap_or_panic(self) -> T {
        self.unwrap_or_else(|e| raise_or_elog(PANIC, e.to_string(), None))
    }
    fn unwrap_or_panic_with<S: Into<String>>(self, f: impl FnOnce() -> S) -> T {
        self.unwrap_or_else(|e| raise_or_elog(PANIC, f().into(), Some(e.to_string())))
    }
}

#[cfg(test)]
mod or_elog_tests {
    use super::*;
    use std::panic::catch_unwind;

    #[test]
    fn option_some_and_ok_passthrough() {
        flush_error_state();
        assert_eq!(Some(5).unwrap_or_error(), 5);
        assert_eq!(Some(6).unwrap_or_error_with(|| "x"), 6);
        let ok: Result<i32, String> = Ok(7);
        assert_eq!(ok.unwrap_or_fatal(), 7);
        let ok2: Result<i32, &str> = Ok(8);
        assert_eq!(ok2.unwrap_or_error_with(|| "x"), 8);
        flush_error_state();
    }

    #[test]
    fn option_none_raises_error() {
        flush_error_state();
        let result = catch_unwind(|| {
            let v: Option<i32> = None;
            v.unwrap_or_error()
        });
        let payload = result.expect_err("None must raise ERROR");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, ERROR);
        assert_eq!(edata.message.as_deref(), Some(super::OR_ELOG_NULL_MSG));
        flush_error_state();
    }

    #[test]
    fn result_err_captures_display_as_detail() {
        flush_error_state();
        let result = catch_unwind(|| {
            let v: Result<i32, String> = Err("disk gone".to_string());
            v.unwrap_or_error_with(|| "could not read block")
        });
        let payload = result.expect_err("Err must raise ERROR");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, ERROR);
        assert_eq!(edata.message.as_deref(), Some("could not read block"));
        assert_eq!(edata.detail.as_deref(), Some("disk gone"));
        flush_error_state();
    }

    #[test]
    fn result_err_default_message_is_display() {
        flush_error_state();
        let result = catch_unwind(|| {
            let v: Result<i32, &str> = Err("boom");
            v.unwrap_or_error()
        });
        let payload = result.expect_err("Err must raise ERROR");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.message.as_deref(), Some("boom"));
        flush_error_state();
    }

    #[test]
    fn unwrap_or_fatal_raises_fatal() {
        flush_error_state();
        let result = catch_unwind(|| {
            let v: Option<i32> = None;
            v.unwrap_or_fatal_with(|| "connection unusable")
        });
        let payload = result.expect_err("None must raise FATAL");
        let edata = payload.downcast_ref::<ErrorData>().expect("ErrorData payload");
        assert_eq!(edata.elevel, FATAL);
        flush_error_state();
    }
}
