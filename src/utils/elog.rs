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
// global. Modeled here as a list of boxed closures.
pub struct ErrorContextCallback {
    pub callback: Box<dyn FnMut()>,
}

impl ErrorContextCallback {
    pub fn new(callback: impl FnMut() + 'static) -> Self {
        Self { callback: Box::new(callback) }
    }
}

// ---------------------------------------------------------------------------
// Definitions translated from elog.c live in the backend module; re-export the
// header-declared API so `crate::utils::elog::<name>` call sites keep resolving.
// ---------------------------------------------------------------------------

pub use crate::backend::utils::error::elog::{
    check_log_of_query, copy_error_data, debug_file_open, emit_error_report, err_generic_string,
    errbacktrace, errcode_for_file_access, errcode_for_socket_access, errdetail_log_plural,
    errdetail_plural, errhint_plural, errmsg_plural, errsave_start, errstart, errstart_cold,
    error_severity, flush_error_state, format_elog_string, free_error_data, get_backend_type_for_log,
    get_error_context_stack, get_formatted_log_time, get_formatted_start_time, geterrcode,
    geterrposition, getinternalerrposition, in_error_recursion_trouble, log_status_format,
    message_level_is_interesting, pg_try, pre_format_elog_string, reset_formatted_start_time,
    set_errcontext_domain, unpack_sql_state, write_csvlog, write_jsonlog, write_pipe_chunks,
    write_stderr, EMIT_LOG_HOOK, ERROR_CONTEXT_STACK,
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
