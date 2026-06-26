//! Translated from PostgreSQL src/backend/utils/error/elog.c

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

// ERRORDATA_STACK_SIZE: small stack of ErrorData for re-entrant cases.
const ERRORDATA_STACK_SIZE: usize = 5;

// ---------------------------------------------------------------------------
// Per-task error state (replaces PG's process globals: errordata[],
// errordata_stack_depth, recursion_depth). TODO(panic): once tasks are tokio
// tasks, move from thread_local! to tokio task-local storage.
// ---------------------------------------------------------------------------

struct ErrorState {
    stack: Vec<ErrorData>,
    recursion_depth: i32,
}

thread_local! {
    static ERROR_STATE: RefCell<ErrorState> =
        const { RefCell::new(ErrorState { stack: Vec::new(), recursion_depth: 0 }) };
}

fn with_top<R>(f: impl FnOnce(&mut ErrorData) -> R) -> R {
    ERROR_STATE.with(|st| {
        let mut st = st.borrow_mut();
        let edata = st
            .stack
            .last_mut()
            .expect("errstart was not called"); // CHECK_STACK_DEPTH()
        f(edata)
    })
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

// errcode helpers that derive a SQLSTATE from errno-style failures. The errno
// comes from the saved value on the in-flight ErrorData. We map via portable
// std::io::ErrorKind rather than raw libc errno numbers (which differ by OS).
pub fn errcode_for_file_access() -> i32 {
    use crate::utils::errcodes::{ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_FILE, ERRCODE_DUPLICATE_FILE, ERRCODE_WRONG_OBJECT_TYPE, ERRCODE_DISK_FULL, ERRCODE_OUT_OF_MEMORY, ERRCODE_INTERNAL_ERROR};
    use std::io::ErrorKind::{PermissionDenied, ReadOnlyFilesystem, NotFound, AlreadyExists, NotADirectory, IsADirectory, DirectoryNotEmpty, StorageFull, OutOfMemory};
    let errno = with_top(|e| e.saved_errno);
    let sqlerrcode = match std::io::Error::from_raw_os_error(errno).kind() {
        PermissionDenied | ReadOnlyFilesystem => ERRCODE_INSUFFICIENT_PRIVILEGE,
        NotFound => ERRCODE_UNDEFINED_FILE,
        AlreadyExists => ERRCODE_DUPLICATE_FILE,
        NotADirectory | IsADirectory | DirectoryNotEmpty => ERRCODE_WRONG_OBJECT_TYPE,
        StorageFull => ERRCODE_DISK_FULL,
        OutOfMemory => ERRCODE_OUT_OF_MEMORY,
        _ => ERRCODE_INTERNAL_ERROR,
    };
    with_top(|e| e.sqlerrcode = sqlerrcode);
    0
}

pub fn errcode_for_socket_access() -> i32 {
    use crate::utils::errcodes::{ERRCODE_CONNECTION_FAILURE, ERRCODE_INTERNAL_ERROR};
    use std::io::ErrorKind::{BrokenPipe, ConnectionReset, ConnectionAborted, NotConnected};
    let errno = with_top(|e| e.saved_errno);
    // ALL_CONNECTION_FAILURE_ERRNOS in PG: EPIPE / ECONNRESET et al.
    let sqlerrcode = match std::io::Error::from_raw_os_error(errno).kind() {
        BrokenPipe | ConnectionReset | ConnectionAborted | NotConnected => {
            ERRCODE_CONNECTION_FAILURE
        }
        _ => ERRCODE_INTERNAL_ERROR,
    };
    with_top(|e| e.sqlerrcode = sqlerrcode);
    0
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
// short-circuit (warning-or-less not enabled anywhere). The returned frame is
// pushed onto the per-task stack; errfinish pops it.
pub fn errstart(mut elevel: i32, domain: Option<&str>) -> Option<ErrorData> {
    // Promote ERROR to FATAL when there is no handler to pass it to. Under the
    // panic model the eventual catch_unwind at task spawn is the handler, so we
    // keep ERROR as ERROR here. CritSectionCount/ExitOnAnyError are deferred.
    if elevel >= ERROR {
        // Make sure we panic if a stacked frame already warrants higher severity.
        elevel = ERROR_STATE.with(|st| {
            st.borrow().stack.iter().fold(elevel, |acc, e| acc.max(e.elevel))
        });
    }

    let output_to_server = should_output_to_server(elevel);
    let output_to_client = should_output_to_client(elevel);
    if elevel < ERROR && !output_to_server && !output_to_client {
        return None;
    }

    ERROR_STATE.with(|st| {
        let mut st = st.borrow_mut();
        st.recursion_depth += 1;
        if st.recursion_depth > 1 && elevel >= ERROR {
            // Error during error processing; abandon context callbacks if deep.
            if st.recursion_depth > 2 {
                // in_error_recursion_trouble(): drop context callbacks.
                // TODO(panic): also clear debug_query_string when wired up.
                unsafe {
                    let p = &raw mut ERROR_CONTEXT_STACK;
                    (*p).clear();
                }
            }
        }
        if st.stack.len() >= ERRORDATA_STACK_SIZE {
            // Almost certainly an infinite error loop; give up and PANIC.
            st.recursion_depth = 0;
            st.stack.clear();
            drop(st);
            #[allow(deprecated)]
            raise_panic(panic_frame(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        }
        let dom = domain.unwrap_or("postgres").to_owned();
        let edata = ErrorData {
            elevel,
            output_to_server,
            output_to_client,
            context_domain: Some(dom.clone()),
            domain: Some(dom),
            sqlerrcode: default_errcode_for(elevel),
            // saved_errno: snapshot errno so message eval can't change it.
            saved_errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            ..Default::default()
        };
        st.stack.push(edata);
        st.recursion_depth -= 1;
    });

    // Hand the caller a default frame to populate; errfinish reconciles it with
    // the pushed stack entry (saved_errno/domain stay authoritative there).
    Some(ErrorData {
        elevel,
        output_to_server,
        output_to_client,
        sqlerrcode: default_errcode_for(elevel),
        ..Default::default()
    })
}

// pg_attribute_cold variant taken for compile-time-constant elevel >= ERROR.
pub fn errstart_cold(elevel: i32, domain: Option<&str>) -> Option<ErrorData> {
    errstart(elevel, domain)
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
#[allow(clippy::needless_pass_by_value, reason = "consumes edata (merged then popped); macro caller passes by value")]
pub fn errfinish(edata: ErrorData, filename: &str, lineno: i32, funcname: &str) {
    // TODO(panic): the >=ERROR path panics (caught by catch_unwind at task
    // spawn) carrying the ErrorData; lower severities log and return.

    // Reconcile the caller-built frame into the pushed stack entry, then take
    // ownership of the completed entry (pops the stack).
    let mut edata = ERROR_STATE.with(|st| {
        let mut st = st.borrow_mut();
        st.recursion_depth += 1;
        let top = st.stack.last_mut().expect("errstart was not called");
        // The caller mutated its own copy via the builder methods; merge those
        // fields (everything but saved_errno/domain set up by errstart).
        merge_built_fields(top, &edata);
        top.filename = Some(set_stack_entry_filename(filename));
        top.lineno = lineno;
        top.funcname = (!funcname.is_empty()).then(|| funcname.to_owned());
        let done = st.stack.pop().unwrap();
        st.recursion_depth -= 1;
        done
    });

    // TODO(review,MAJOR): errcontext callbacks are inert here -- they are no-arg
    // closures with no handle to the in-flight ErrorData, and they run AFTER the
    // frame is popped. C runs them on the live stack top before throw, calling
    // errcontext_msg. Give the callback `&mut ErrorData` and move this before pop.
    // Run context callbacks. void(*)(void*)+arg collapse to closures.
    unsafe {
        let p = &raw mut ERROR_CONTEXT_STACK;
        for cb in &mut (*p) {
            (cb.callback)();
        }
    }

    let elevel = edata.elevel;

    // TODO(review,MAJOR): FATAL and PANIC are collapsed into the same catchable
    // panic. C terminates the backend on FATAL and abort()s the process on PANIC;
    // PANIC must not be swallowable by catch_unwind. Distinguish severity here
    // (e.g. process::abort() for >= PANIC) per the design's crit-section invariant.
    if elevel >= ERROR {
        // Emit to the server log first so the message is seen even if nobody
        // catches the panic, then raise it as a panic carrying the ErrorData.
        if edata.output_to_server {
            send_message_to_server_log(&edata);
        }
        // TODO(panic): a later catch_unwind at the task boundary downcasts the
        // payload back to ErrorData (sqlstate/severity/message/detail/context).
        std::panic::panic_any(edata);
    }

    // log-and-return for WARNING/LOG/NOTICE/INFO/DEBUGx.
    emit_error_report_for(&mut edata);
}

// Merge the fields the caller set via builder methods on its working copy into
// the authoritative stack entry. saved_errno and domain remain as errstart set.
fn merge_built_fields(dst: &mut ErrorData, src: &ErrorData) {
    // TODO(review,MAJOR): unconditional copy clobbers a sqlerrcode set on the
    // stack entry by errcode_for_file_access/_socket_access (split-brain builder:
    // those free fns write `dst`, the methods write `src`). Unify on one target.
    dst.sqlerrcode = src.sqlerrcode;
    dst.hide_stmt = src.hide_stmt;
    dst.hide_ctx = src.hide_ctx;
    dst.cursorpos = src.cursorpos;
    dst.internalpos = src.internalpos;
    dst.message_id = src.message_id.or(dst.message_id);
    macro_rules! take {
        ($f:ident) => {
            if src.$f.is_some() {
                dst.$f = src.$f.clone();
            }
        };
    }
    take!(message);
    take!(detail);
    take!(detail_log);
    take!(hint);
    take!(context);
    take!(backtrace);
    take!(internalquery);
    take!(schema_name);
    take!(table_name);
    take!(column_name);
    take!(datatype_name);
    take!(constraint_name);
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

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
fn raise_panic(edata: ErrorData) -> ! {
    // TODO(panic)
    send_message_to_server_log(&edata);
    std::panic::panic_any(edata);
}

// ---------------------------------------------------------------------------
// Context / position / generic-string accessors. These mutate the in-flight
// ErrorData (top of the per-task stack).
// ---------------------------------------------------------------------------

pub fn set_errcontext_domain(domain: Option<&str>) -> i32 {
    let dom = domain.unwrap_or("postgres").to_owned();
    with_top(|e| e.context_domain = Some(dom));
    0
}

pub fn errbacktrace() -> i32 {
    let bt = std::backtrace::Backtrace::force_capture().to_string();
    with_top(|e| e.backtrace = Some(bt));
    0
}

pub fn err_generic_string(field: i32, s: &str) -> i32 {
    use crate::postgres_ext::{PG_DIAG_SCHEMA_NAME, PG_DIAG_TABLE_NAME, PG_DIAG_COLUMN_NAME, PG_DIAG_DATATYPE_NAME, PG_DIAG_CONSTRAINT_NAME};
    with_top(|e| {
        let f = field as u8;
        if f == PG_DIAG_SCHEMA_NAME {
            e.schema_name = Some(s.to_owned());
        } else if f == PG_DIAG_TABLE_NAME {
            e.table_name = Some(s.to_owned());
        } else if f == PG_DIAG_COLUMN_NAME {
            e.column_name = Some(s.to_owned());
        } else if f == PG_DIAG_DATATYPE_NAME {
            e.datatype_name = Some(s.to_owned());
        } else if f == PG_DIAG_CONSTRAINT_NAME {
            e.constraint_name = Some(s.to_owned());
        } else {
            // TODO(panic): PG elog(ERROR) here; deferred to keep this non-raising.
            panic!("unsupported ErrorData field id: {field}");
        }
    });
    0
}

// TODO(review,MAJOR): these read the stack entry, but errcode()/errposition()
// set the caller's working copy (split-brain builder), so mid-build reads are
// stale. Same root cause as merge_built_fields; fix by unifying the build target.
pub fn geterrcode() -> i32 {
    with_top(|e| e.sqlerrcode)
}
pub fn geterrposition() -> i32 {
    with_top(|e| e.cursorpos)
}
pub fn getinternalerrposition() -> i32 {
    with_top(|e| e.internalpos)
}

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

// TODO(panic): catch_unwind-based replacement for PG_TRY/PG_CATCH. Runs `body`;
// on a (>=ERROR) panic runs `catch` then resumes the unwind carrying ErrorData.
pub fn pg_try<T>(body: impl FnOnce() -> T + std::panic::UnwindSafe, catch: impl FnOnce()) -> T {
    match std::panic::catch_unwind(body) {
        Ok(v) => v,
        Err(payload) => {
            catch();
            // TODO(panic): rethrow the original payload (carries ErrorData).
            std::panic::resume_unwind(payload);
        }
    }
}

// PG_RE_THROW(): resume the in-flight error. Never returns.
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn pg_re_throw() -> ! {
    // TODO(panic): the in-flight ErrorData lives in the panic payload, which the
    // surrounding pg_try/catch_unwind resumes; reaching here without a payload
    // means the stack top, promoted to FATAL.
    let edata = ERROR_STATE.with(|st| st.borrow_mut().stack.pop());
    match edata {
        Some(mut e) => {
            e.elevel = FATAL;
            #[allow(deprecated)]
            raise_panic(e);
        }
        None => panic!("pg_re_throw with no error in progress"),
    }
}

// ---------------------------------------------------------------------------
// ErrorData lifecycle + reporting.
// ---------------------------------------------------------------------------

// Output the top-of-stack error to its enabled destinations.
pub fn emit_error_report() {
    let mut edata = with_top(|e| e.clone());
    emit_error_report_for(&mut edata);
}

fn emit_error_report_for(edata: &mut ErrorData) {
    // emit_log_hook may turn off output_to_server.
    unsafe {
        let p = &raw mut EMIT_LOG_HOOK;
        if edata.output_to_server
            && let Some(hook) = (*p).as_mut() {
                hook(edata);
            }
    }
    if edata.output_to_server {
        send_message_to_server_log(edata);
    }
    if edata.output_to_client {
        send_message_to_frontend(edata);
    }
}

// CopyErrorData: copy of the topmost stack entry. Under Rust ownership this is a
// plain clone.
pub fn copy_error_data() -> ErrorData {
    with_top(|e| e.clone())
}

// FreeErrorData is a no-op under Rust ownership (drop handles it).
pub fn free_error_data(_edata: ErrorData) {}

// Reset the error stack to empty after recovery.
pub fn flush_error_state() {
    ERROR_STATE.with(|st| {
        let mut st = st.borrow_mut();
        st.stack.clear();
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
#[allow(clippy::needless_pass_by_value, reason = "consumes edata (merged into a fresh frame)")]
pub fn throw_error_data(edata: ErrorData) {
    // ThrowErrorData: report an error described by a standalone ErrorData. Run
    // it through errstart/errfinish so gating + raise semantics apply.
    let elevel = edata.elevel;
    let domain = edata.domain.clone();
    if let Some(mut frame) = errstart(elevel, domain.as_deref()) {
        merge_built_fields(&mut frame, &edata);
        let (file, line, func) = (
            edata.filename.clone().unwrap_or_default(),
            edata.lineno,
            edata.funcname.clone().unwrap_or_default(),
        );
        #[allow(deprecated)]
        errfinish(frame, &file, line, &func);
    }
}

// GetErrorContextStack: run the context callbacks to build a context string.
pub fn get_error_context_stack() -> String {
    // Push a throwaway frame, run callbacks (which call errcontext_msg), take it.
    let frame = errstart(LOG, None);
    if frame.is_none() {
        return String::new();
    }
    unsafe {
        let p = &raw mut ERROR_CONTEXT_STACK;
        for cb in &mut (*p) {
            (cb.callback)();
        }
    }
    ERROR_STATE.with(|st| {
        st.borrow_mut()
            .stack
            .pop()
            .and_then(|e| e.context)
            .unwrap_or_default()
    })
}

// PG's `ErrorContextCallback *error_context_stack` global + the `sigjmp_buf
// *PG_exception_stack` global are both task-local concerns under the async
// single-process model.
// TODO(panic): make these task-local (tokio task_local!) rather than statics.
pub static mut ERROR_CONTEXT_STACK: Vec<ErrorContextCallback> = Vec::new();

// emit_log_hook: void(*)(ErrorData*) -> optional captured closure.
// TODO(panic): make this task-local rather than a process global.
pub type EmitLogHook = Box<dyn FnMut(&mut ErrorData)>;
pub static mut EMIT_LOG_HOOK: Option<EmitLogHook> = None;

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
}
