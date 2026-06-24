//! Translated from PostgreSQL src/include/utils/elog.h
//!
//! Error model: PG's `elog`/`ereport` use setjmp/longjmp. We keep `>= ERROR`
//! semantics as a `panic!` (the eventual catch_unwind at task spawn); lower
//! severities log-and-return. Every >=ERROR raising path is marked
//! `#[deprecated(note = "TODO(panic): migrate to Result + ?")]` + `// TODO(panic)`.

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

// PG's `ErrorContextCallback *error_context_stack` global + the `sigjmp_buf
// *PG_exception_stack` global are both task-local concerns under the async
// single-process model.
// TODO(panic): make these task-local (tokio task_local!) rather than statics.
pub static mut ERROR_CONTEXT_STACK: Vec<ErrorContextCallback> = Vec::new();

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

    pub fn errcontext_msg(&mut self, msg: impl Into<String>) -> &mut Self {
        self.context = Some(msg.into());
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

// errcode helpers that derive a SQLSTATE from errno-style failures.
pub fn errcode_for_file_access() -> i32 {
    unimplemented!()
}
pub fn errcode_for_socket_access() -> i32 {
    unimplemented!()
}

// Default sqlerrcode for an elevel (errstart's fallback, see header comment).
pub const fn default_errcode_for(elevel: i32) -> i32 {
    if elevel >= ERROR {
        ERRCODE_INTERNAL_ERROR
    } else if elevel == WARNING {
        ERRCODE_WARNING
    } else {
        ERRCODE_SUCCESSFUL_COMPLETION
    }
}

// ---------------------------------------------------------------------------
// errstart / errfinish core. In PG `errstart` returns whether the message is
// worth building; `errfinish` either logs (low severity) or longjmps (>=ERROR).
// Here >=ERROR -> panic carrying the ErrorData; lower -> log and return.
// ---------------------------------------------------------------------------

pub fn message_level_is_interesting(elevel: i32) -> bool {
    // Stub: real impl compares against client/log_min_messages GUCs.
    let _ = elevel;
    unimplemented!()
}

// Returns Some(ErrorData) when the message should be built (always, in the
// skeleton); the caller fills fields then calls errfinish.
pub fn errstart(elevel: i32, domain: Option<&str>) -> Option<ErrorData> {
    let mut edata = ErrorData::default();
    edata.elevel = elevel;
    edata.sqlerrcode = default_errcode_for(elevel);
    edata.domain = domain.map(|s| s.to_owned());
    Some(edata)
}

// pg_attribute_cold variant taken for compile-time-constant elevel >= ERROR.
pub fn errstart_cold(elevel: i32, domain: Option<&str>) -> Option<ErrorData> {
    errstart(elevel, domain)
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn errfinish(mut edata: ErrorData, filename: &str, lineno: i32, funcname: &str) {
    // TODO(panic): the >=ERROR path panics (caught by catch_unwind at task
    // spawn); lower severities should log and return.
    edata.filename = Some(filename.to_owned());
    edata.lineno = lineno;
    edata.funcname = Some(funcname.to_owned());
    if edata.elevel >= ERROR {
        // TODO(panic)
        panic!(
            "{}",
            edata.message.clone().unwrap_or_else(|| "error".to_owned())
        );
    }
    // log-and-return for WARNING/LOG/NOTICE/... -- real impl emits to log/client.
    let _ = edata;
}

// errsave / ereturn soft-error path. `context` is a *Node (ErrorSaveContext) or
// NULL; modeled as Option. With None it behaves like ereport(ERROR).
// TODO(struct-forward): repoint context to crate::nodes::* ErrorSaveContext.
pub fn errsave_start(context: Option<&mut ()>, domain: Option<&str>) -> Option<ErrorData> {
    let _ = context;
    errstart(ERROR, domain)
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
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
// Context / position / generic-string accessors. These mutate the in-flight
// ErrorData in PG; provided as free fns operating on a borrow for callers that
// don't go through the builder methods.
// ---------------------------------------------------------------------------

pub fn set_errcontext_domain(_domain: Option<&str>) -> i32 {
    0
}

pub fn errbacktrace() -> i32 {
    unimplemented!()
}

pub fn err_generic_string(_field: i32, _str: &str) -> i32 {
    unimplemented!()
}

pub fn geterrcode() -> i32 {
    unimplemented!()
}
pub fn geterrposition() -> i32 {
    unimplemented!()
}
pub fn getinternalerrposition() -> i32 {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Constructing error strings outside ereport().
// ---------------------------------------------------------------------------

pub fn pre_format_elog_string(_errnumber: i32, _domain: Option<&str>) {
    unimplemented!()
}

// printf varargs collapse to a pre-formatted String supplied by the caller.
pub fn format_elog_string(fmt: &str) -> String {
    let _ = fmt;
    unimplemented!()
}

// ---------------------------------------------------------------------------
// PG_TRY / PG_CATCH / PG_RE_THROW. The setjmp/longjmp frame maps to
// catch_unwind; pg_re_throw resumes the unwind (-> !).
// ---------------------------------------------------------------------------

// TODO(panic): catch_unwind-based replacement for PG_TRY/PG_CATCH. Runs `body`;
// on a (>=ERROR) panic runs `catch`. Real impl threads ErrorData through the
// panic payload and restores error_context_stack.
pub fn pg_try<T>(body: impl FnOnce() -> T + std::panic::UnwindSafe, catch: impl FnOnce()) -> T {
    match std::panic::catch_unwind(body) {
        Ok(v) => v,
        Err(_payload) => {
            catch();
            // TODO(panic): rethrow unless the catch handled it.
            #[allow(deprecated)]
            pg_re_throw();
        }
    }
}

// PG_RE_THROW(): resume the in-flight error. Never returns.
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn pg_re_throw() -> ! {
    // TODO(panic): resume_unwind with the saved ErrorData payload.
    panic!("pg_re_throw");
}

// ---------------------------------------------------------------------------
// ErrorData lifecycle + reporting.
// ---------------------------------------------------------------------------

pub fn emit_error_report() {
    unimplemented!()
}

pub fn copy_error_data() -> ErrorData {
    unimplemented!()
}

// FreeErrorData is a no-op under Rust ownership (drop handles it).
pub fn free_error_data(_edata: ErrorData) {}

pub fn flush_error_state() {
    unimplemented!()
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn re_throw_error(_edata: ErrorData) -> ! {
    // TODO(panic): pg_noreturn -- resume the unwind carrying edata.
    panic!("ReThrowError");
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn throw_error_data(_edata: ErrorData) {
    // TODO(panic): re-raise edata as an ERROR.
    unimplemented!()
}

pub fn get_error_context_stack() -> String {
    unimplemented!()
}

// emit_log_hook: void(*)(ErrorData*) -> optional captured closure.
// TODO(panic): make this task-local rather than a process global.
pub static mut EMIT_LOG_HOOK: Option<Box<dyn FnMut(&ErrorData)>> = None;

// ---------------------------------------------------------------------------
// Log formatting / destinations.
// ---------------------------------------------------------------------------

// StringInfo -> &mut String (see lib::stringinfo tombstone).
pub fn log_status_format(buf: &mut String, format: &str, edata: &ErrorData) {
    let _ = (buf, format, edata);
    unimplemented!()
}

pub fn debug_file_open() {
    unimplemented!()
}

pub fn unpack_sql_state(sql_state: i32) -> String {
    let _ = sql_state;
    unimplemented!()
}

pub fn in_error_recursion_trouble() -> bool {
    unimplemented!()
}

pub fn reset_formatted_start_time() {
    unimplemented!()
}
pub fn get_formatted_start_time() -> String {
    unimplemented!()
}
pub fn get_formatted_log_time() -> String {
    unimplemented!()
}
pub fn get_backend_type_for_log() -> &'static str {
    unimplemented!()
}
pub fn check_log_of_query(_edata: &ErrorData) -> bool {
    unimplemented!()
}
pub fn error_severity(elevel: i32) -> &'static str {
    let _ = elevel;
    unimplemented!()
}
pub fn write_pipe_chunks(_data: &[u8], _dest: i32) {
    unimplemented!()
}

pub fn write_csvlog(_edata: &ErrorData) {
    unimplemented!()
}
pub fn write_jsonlog(_edata: &ErrorData) {
    unimplemented!()
}

// Pre-elog stderr writers. Varargs collapse to a pre-formatted &str.
pub fn write_stderr(msg: &str) {
    eprint!("{msg}");
}
