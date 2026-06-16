/*-------------------------------------------------------------------------
 *
 * elog_impl.rs
 *   error logging and reporting
 *
 * Translation of postgres/src/backend/utils/error/elog.c
 *
 * Macros (ereport!/errmsg!/elog!) and level constants (ERROR/FATAL/…) live in
 * crate::utils::elog -- DO NOT redefine them here.  This file provides all the
 * C-function bodies: errstart, errfinish, EmitErrorReport, send_message_to_server_log,
 * send_message_to_frontend, CopyErrorData, FlushErrorState, etc.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#![allow(
    non_snake_case,
    non_upper_case_globals,
    non_camel_case_types,
    dead_code,
    unused_variables,
    unused_mut,
    clippy::missing_safety_doc
)]

use core::ffi::{c_char, c_int, c_ulong, c_void};
use core::ptr::{null, null_mut};
use std::mem::size_of;

// libc-like C functions declared directly (avoids needing the libc crate).
#[allow(improper_ctypes)]
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strrchr(s: *const c_char, c: c_int) -> *const c_char;
    fn open(path: *const c_char, flags: c_int, ...) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn isatty(fd: c_int) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn fsync(fd: c_int) -> c_int;
    fn time(tloc: *mut i64) -> i64;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn abort() -> !;
    fn free(ptr: *mut c_void);
    // FILE* fflush / freopen / stdout / stderr
    fn fflush(stream: *mut c_void) -> c_int;
    fn freopen(path: *const c_char, mode: *const c_char, stream: *mut c_void) -> *mut c_void;
    static mut stdout: *mut c_void;
    static mut stderr: *mut c_void;
    // errno
    fn __errno_location() -> *mut c_int;
    // gettimeofday
    fn gettimeofday(tv: *mut TimeVal, tz: *mut c_void) -> c_int;
    // open flags
}

// Minimal timeval for gettimeofday
#[repr(C)]
struct TimeVal {
    tv_sec: i64,
    tv_usec: i64,
}
/* saved_timeval is declared in the global statics section below */

// STDERR_FILENO / STDOUT_FILENO
const STDERR_FILENO: c_int = 2;
const STDOUT_FILENO: c_int = 1;

// errno values (POSIX)
const EPERM: c_int = 1;
const ENOENT: c_int = 2;
const EIO: c_int = 5;
const ENOMEM: c_int = 12;
const EACCES: c_int = 13;
const EEXIST: c_int = 17;
const ENOTDIR: c_int = 20;
const EISDIR: c_int = 21;
const EINVAL: c_int = 22;
const ENFILE: c_int = 23;
const EMFILE: c_int = 24;
const ENOSPC: c_int = 28;
const ENAMETOOLONG: c_int = 36;
const ENOTEMPTY: c_int = 39;
const ECONNABORTED: c_int = 53;
const ECONNRESET: c_int = 54;
const EPIPE: c_int = 32;
const ETIMEDOUT: c_int = 60;
#[cfg(target_os = "linux")]
const EROFS: c_int = 30;

// open flags
const O_RDONLY: c_int = 0;
const O_WRONLY: c_int = 1;
const O_CREAT: c_int = 0o100;
const O_APPEND: c_int = 0o2000;

// ---- bring in constants from elog.rs (the macro/constants home) ----
use crate::utils::elog::{
    DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5,
    INFO, LOG, LOG_SERVER_ONLY, NOTICE, WARNING, WARNING_CLIENT_ONLY,
    ERROR, FATAL, PANIC,
    COMMERROR,
};

// ---- prelude types / allocators ----
use crate::prelude::{
    c_uint, c_uchar,
    null as cnull, null_mut as cnull_mut,
    palloc, pfree, pstrdup,
    MemoryContext, MemoryContextSwitchTo,
};
use crate::utils::memutils::MemoryContextReset;
use crate::utils::mmgr::mcxt::MemoryContextStrdup;
use crate::c::{uint32, int64, Size};

// ---- StringInfo ----
use crate::lib::stringinfo::{
    StringInfo, StringInfoData,
    initStringInfo, appendStringInfoChar, appendStringInfoString,
    appendBinaryStringInfo, appendStringInfoSpaces,
};
use crate::appendStringInfo;
use crate::appendStringInfoCharMacro;

// ---- node types (for IsA check on ErrorSaveContext) ----
use crate::nodes::nodes::NodeTag;
use crate::nodes::miscnodes::ErrorSaveContext;

// ---- miscadmin globals ----
use crate::miscadmin::{
    BackendType, B_BG_WORKER, B_LOGGER,
    GetBackendTypeDesc,
};

// ---- postgres_ext PG_DIAG_* constants ----
use crate::postgres_ext::{
    PG_DIAG_SEVERITY, PG_DIAG_SEVERITY_NONLOCALIZED, PG_DIAG_SQLSTATE,
    PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_MESSAGE_DETAIL, PG_DIAG_MESSAGE_HINT,
    PG_DIAG_STATEMENT_POSITION, PG_DIAG_INTERNAL_POSITION, PG_DIAG_INTERNAL_QUERY,
    PG_DIAG_CONTEXT, PG_DIAG_SCHEMA_NAME, PG_DIAG_TABLE_NAME, PG_DIAG_COLUMN_NAME,
    PG_DIAG_DATATYPE_NAME, PG_DIAG_CONSTRAINT_NAME,
    PG_DIAG_SOURCE_FILE, PG_DIAG_SOURCE_LINE, PG_DIAG_SOURCE_FUNCTION,
};

// ---- pgtime ----
use crate::pgtime::{pg_time_t, pg_tm, pg_tz, pg_localtime, pg_strftime};

// ---- libpq ----
use crate::libpq::libpq_be::Port;
use crate::libpq::pqformat::{
    pq_beginmessage, pq_endmessage, pq_sendint8, pq_sendstring, pq_send_ascii_string,
};
use crate::libpq::libpq::{pq_flush, pq_putmessage_v2};
use crate::libpq::protocol::{PqMsg_NoticeResponse, PqMsg_ErrorResponse};

// ---- csv/json log writers ----
use crate::utils::error::csvlog::write_csvlog;
use crate::utils::error::jsonlog::write_jsonlog;

// ---- mb ----
use crate::mb::mbutils::pg_mbcliplen;

// ---- port ----
use crate::port::pgstrcasecmp::pg_strcasecmp;

// ---- utils/activity ----
use crate::utils::activity::backend_status::pgstat_get_my_query_id;
use crate::utils::activity::pgstat_database::{
    pgStatSessionEndCause, DISCONNECT_NORMAL, DISCONNECT_FATAL,
};

// ---- storage/ipc ----
use crate::storage::ipc::ipc::proc_exit;

// ---- xact ----
use crate::access::transam::xact::GetTopTransactionIdIfAny;

// ---- GUC helpers ----
use crate::utils::misc::guc::guc_malloc;

// ---- assert support ----
use crate::utils::error::assert::ExceptionalCondition;

// ---------------------------------------------------------------------------
// Types not yet unified -- stub locally where required
// ---------------------------------------------------------------------------

// TODO(pg-port): unify with crate::utils::misc::guc::GucSource when guc.h lands
pub type GucSource = c_int;

// TODO(pg-port): unify with real ProtocolVersion
type ProtocolVersion = u32;

// Minimal Node for errsave_start signature
use crate::nodes::nodes::Node;

// ---------------------------------------------------------------------------
// ErrorData
//
// The canonical definition of the struct lives here.  Sibling files
// (csvlog.rs, jsonlog.rs) carry local copies pending this port.  Once this
// file is wired into the module tree those local copies should be removed.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct ErrorData {
    pub elevel: c_int,               /* error level */
    pub output_to_server: bool,      /* will report to server log? */
    pub output_to_client: bool,      /* will report to client? */
    pub hide_stmt: bool,             /* true to prevent STATEMENT: field */
    pub hide_ctx: bool,              /* true to prevent CONTEXT: field */
    pub filename: *const c_char,     /* __FILE__ of ereport() call */
    pub lineno: c_int,               /* __LINE__ of ereport() call */
    pub funcname: *const c_char,     /* __func__ of ereport() call */
    pub domain: *const c_char,       /* message domain */
    pub context_domain: *const c_char, /* domain for context message */
    pub sqlerrcode: c_int,           /* encoded ERRSTATE */
    pub message: *mut c_char,        /* primary error message */
    pub detail: *mut c_char,         /* detail error message */
    pub detail_log: *mut c_char,     /* detail message for server log only */
    pub hint: *mut c_char,           /* hint message */
    pub context: *mut c_char,        /* context message */
    pub backtrace: *mut c_char,      /* backtrace */
    pub message_id: *const c_char,   /* primary message's id (original English) */
    pub schema_name: *mut c_char,    /* name of schema */
    pub table_name: *mut c_char,     /* name of table */
    pub column_name: *mut c_char,    /* name of column */
    pub datatype_name: *mut c_char,  /* name of datatype */
    pub constraint_name: *mut c_char, /* name of constraint */
    pub cursorpos: c_int,            /* cursor index into query string */
    pub internalpos: c_int,          /* cursor index into internalquery */
    pub internalquery: *mut c_char,  /* text of internally-generated query */
    pub saved_errno: c_int,          /* errno at entry */
    pub assoc_context: MemoryContext, /* context to allocate subsidiary data in */
}

// ---------------------------------------------------------------------------
// ErrorContextCallback
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: unsafe extern "C" fn(arg: *mut c_void),
    pub arg: *mut c_void,
}

// ---------------------------------------------------------------------------
// emit_log_hook_type
// ---------------------------------------------------------------------------
pub type emit_log_hook_type = unsafe extern "C" fn(edata: *mut ErrorData);

// ---------------------------------------------------------------------------
// Global variables
// ---------------------------------------------------------------------------

/// Linked list of ErrorContextCallback nodes (elog.h: error_context_stack)
pub static mut error_context_stack: *mut ErrorContextCallback = null_mut();

/// Current setjmp target for PG_TRY/PG_CATCH (elog.h: PG_exception_stack)
/// In Rust we use panic/catch_unwind, so this remains a null-initialized stub.
/// TODO(pg-port): wire up to actual panic handler or sigjmp_buf equivalent.
pub static mut PG_exception_stack: *mut c_void = null_mut();

/// Hook for intercepting messages before they are sent to the server log.
pub static mut emit_log_hook: Option<emit_log_hook_type> = None;

// GUC parameters
pub static mut Log_error_verbosity: c_int = PGERROR_DEFAULT;
pub static mut Log_line_prefix: *mut c_char = null_mut(); /* format for extra log line info */
pub static mut Log_destination: c_int = LOG_DESTINATION_STDERR;
pub static mut Log_destination_string: *mut c_char = null_mut();
pub static mut syslog_sequence_numbers: bool = true;
pub static mut syslog_split_messages: bool = true;

// Processed form of backtrace_functions GUC
static mut backtrace_function_list: *mut c_char = null_mut();

// We provide a small stack of ErrorData records for re-entrant cases
const ERRORDATA_STACK_SIZE: usize = 5;

static mut errordata: [ErrorData; ERRORDATA_STACK_SIZE] = unsafe {
    // SAFETY: ErrorData contains raw pointers; zeroed bytes are valid (all null/0).
    core::mem::zeroed()
};

static mut errordata_stack_depth: c_int = -1; /* index of topmost active frame */
static mut recursion_depth: c_int = 0; /* to detect actual recursion */

// Saved timeval and buffers for formatted timestamps
static mut saved_timeval: TimeVal = TimeVal { tv_sec: 0, tv_usec: 0 };

static mut saved_timeval_set: bool = false;

const FORMATTED_TS_LEN: usize = 128;
static mut formatted_start_time: [c_char; FORMATTED_TS_LEN] = [0; FORMATTED_TS_LEN];
static mut formatted_log_time: [c_char; FORMATTED_TS_LEN] = [0; FORMATTED_TS_LEN];

// ---------------------------------------------------------------------------
// Log destination / verbosity constants (elog.h)
// ---------------------------------------------------------------------------
pub const PGERROR_TERSE: c_int = 0;
pub const PGERROR_DEFAULT: c_int = 1;
pub const PGERROR_VERBOSE: c_int = 2;

pub const LOG_DESTINATION_STDERR: c_int = 0x01;
pub const LOG_DESTINATION_SYSLOG: c_int = 0x02;
pub const LOG_DESTINATION_EVENTLOG: c_int = 0x04; /* WIN32 */
pub const LOG_DESTINATION_CSVLOG: c_int = 0x08;
pub const LOG_DESTINATION_JSONLOG: c_int = 0x10;

// SQLSTATE error-code stubs (errcodes.h numeric values not yet imported)
// TODO(pg-port): replace with real MAKE_SQLSTATE constants when errcodes.rs lands
const ERRCODE_SUCCESSFUL_COMPLETION: c_int = 0;
const ERRCODE_WARNING: c_int          = 0x0_0600_0;  /* Class 01 */
const ERRCODE_INTERNAL_ERROR: c_int   = 0x5_4000_0;  /* XX000 */
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int  = 0x2_8000_0; /* 28000 */
const ERRCODE_UNDEFINED_FILE: c_int          = 0x5_8000_1; /* stub: 58P01 */
const ERRCODE_DUPLICATE_FILE: c_int          = 0x5_8000_0; /* stub */
const ERRCODE_WRONG_OBJECT_TYPE: c_int       = 0x4_2809_0; /* stub */
const ERRCODE_DISK_FULL: c_int               = 0x5_3100_0; /* stub */
const ERRCODE_OUT_OF_MEMORY: c_int           = 0x5_3200_0; /* stub */
const ERRCODE_INSUFFICIENT_RESOURCES: c_int  = 0x5_3000_0; /* stub */
const ERRCODE_IO_ERROR: c_int                = 0x5_8030_0; /* stub */
const ERRCODE_FILE_NAME_TOO_LONG: c_int      = 0x2_2026_0; /* stub */
const ERRCODE_CONNECTION_FAILURE: c_int      = 0x0_8006_0; /* stub */

// ---------------------------------------------------------------------------
// PipeProto stubs (postmaster/syslogger.h not yet ported)
// TODO(pg-port): replace with the real syslogger types when syslogger.rs lands
// ---------------------------------------------------------------------------
const PIPE_MAX_PAYLOAD: usize = 512;
const PIPE_HEADER_SIZE: usize = 8;
const PIPE_PROTO_IS_LAST: u8 = 0x01;
const PIPE_PROTO_DEST_STDERR: u8  = 0x10;
const PIPE_PROTO_DEST_CSVLOG: u8  = 0x20;
const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;

#[repr(C)]
struct PipeProtoHeader {
    nuls: [u8; 2],
    len: u16,
    pid: c_int,
    flags: u8,
    _pad: [u8; 3],
}

#[repr(C)]
struct PipeProtoChunkData {
    proto: PipeProtoHeader,
    data: [u8; PIPE_MAX_PAYLOAD],
}

// ---------------------------------------------------------------------------
// Extern stubs for dependencies not yet ported
// ---------------------------------------------------------------------------
extern "C" {
    // miscadmin.h
    static mut CritSectionCount: uint32;
    static mut ExitOnAnyError: bool;
    static mut proc_exit_inprogress: bool;
    static mut InterruptHoldoffCount: uint32;
    static mut QueryCancelHoldoffCount: uint32;
    static mut IsUnderPostmaster: bool;
    static mut MyProcPid: c_int;
    static mut PostmasterPid: c_int;
    static mut MyBackendType: BackendType;
    static mut MyStartTime: pg_time_t;
    static mut OutputFileName: [c_char; 0];

    // storage/proc.h
    static mut MyProc: *mut c_void; /* opaque PGPROC */

    // utils/init/globals.c -- libpq connection of this backend (NULL if none)
    static mut MyProcPort: *mut Port;

    // tcop/tcopprot.h
    static mut debug_query_string: *const c_char;
    static mut whereToSendOutput: c_int;
    static mut client_min_messages: c_int;
    static mut log_min_messages: c_int;
    static mut log_min_error_statement: c_int;
    static mut ClientAuthInProgress: bool;
    static mut FrontendProtocol: ProtocolVersion;
    static mut application_name: *mut c_char;

    // postmaster/postmaster.h
    static mut redirection_done: bool;
    static mut MyBgworkerEntry: *mut c_void; /* opaque BackgroundWorker* */

    // pgtime
    static mut log_timezone: *mut pg_tz;

    // utils/misc/guc_tables.c -- raw backtrace_functions GUC string
    static mut backtrace_functions: *mut c_char;

    // postmaster/syslogger.h
    fn write_syslogger_file(data: *const c_char, len: c_int, dest: c_int);

    // utils/ps_status.h
    fn get_ps_display(displen: *mut c_int) -> *const c_char;

    // utils/varlena.h
    fn SplitIdentifierString(
        rawstring: *mut c_char,
        separator: c_char,
        namelist: *mut *mut c_void,
    ) -> bool;
    fn list_free(list: *mut c_void);

    // CHECK_FOR_INTERRUPTS() -- tcop/tcopprot.h inline
    fn ProcessInterrupts();
}

// DestRemote / DestNone / DestDebug (tcop/dest.h)
const DestNone: c_int = 0;
const DestDebug: c_int = 1;
const DestRemote: c_int = 2;

// PG_PROTOCOL_MAJOR
#[inline]
fn PG_PROTOCOL_MAJOR(v: ProtocolVersion) -> u32 {
    v >> 16
}

// pq_sendbyte (pqformat.h inline) -- append a binary byte; just pq_sendint8.
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: c_char) {
    pq_sendint8(buf, byt as u8);
}

// CHECK_FOR_INTERRUPTS -- call ProcessInterrupts if needed
// Simplified: call unconditionally (caller context is right)
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {
        unsafe { ProcessInterrupts() }
    };
}

// CHECK_STACK_DEPTH
macro_rules! CHECK_STACK_DEPTH {
    () => {
        if unsafe { errordata_stack_depth } < 0 {
            unsafe { errordata_stack_depth = -1; }
            // avoid recursive ereport here; just abort
            panic!("errstart was not called");
        }
    };
}

// PGUNSIXBIT(val) -- decode one character of SQLSTATE
#[inline]
fn PGUNSIXBIT(val: c_int) -> u8 {
    ((val & 0x3F) + b'A' as c_int) as u8
}

// err_gettext -- no NLS support in this port; just return the string as-is
#[inline]
unsafe fn err_gettext(s: *const c_char) -> *const c_char {
    s
}
// The C macro `_(x)` expands to err_gettext(x).  In Rust call err_gettext()
// directly; a `_` macro name is illegal (reserved identifier).

// MemSet
macro_rules! MemSet {
    ($dest:expr, $val:expr, $size:expr) => {
        unsafe { core::ptr::write_bytes($dest as *mut u8, $val as u8, $size) }
    };
}

// ---------------------------------------------------------------------------
// PART 1 ENDS -- functions follow in parts 2-5
// ---------------------------------------------------------------------------

// ===========================================================================
// Part 2: policy helpers + errstart/errfinish + errsave family
// ===========================================================================

/*
 * is_log_level_output -- is elevel logically >= log_min_level?
 *
 * We use this for tests that should consider LOG to sort out-of-order,
 * between ERROR and FATAL.  Generally this is the right thing for testing
 * whether a message should go to the postmaster log, whereas a simple >=
 * test is correct for testing whether the message should go to the client.
 */
#[inline]
pub unsafe fn is_log_level_output(elevel: c_int, log_min_level: c_int) -> bool {
    if elevel == LOG || elevel == LOG_SERVER_ONLY {
        if log_min_level == LOG || log_min_level <= ERROR {
            return true;
        }
    } else if elevel == WARNING_CLIENT_ONLY {
        /* never sent to log, regardless of log_min_level */
        return false;
    } else if log_min_level == LOG {
        /* elevel != LOG */
        if elevel >= FATAL {
            return true;
        }
    }
    /* Neither is LOG */
    else if elevel >= log_min_level {
        return true;
    }

    false
}

/*
 * should_output_to_server --- should message of given elevel go to the log?
 */
#[inline]
unsafe fn should_output_to_server(elevel: c_int) -> bool {
    is_log_level_output(elevel, log_min_messages)
}

/*
 * should_output_to_client --- should message of given elevel go to the client?
 */
#[inline]
unsafe fn should_output_to_client(elevel: c_int) -> bool {
    if whereToSendOutput == DestRemote && elevel != LOG_SERVER_ONLY {
        /*
         * client_min_messages is honored only after we complete the
         * authentication handshake.
         */
        if ClientAuthInProgress {
            return elevel >= ERROR;
        } else {
            return elevel >= client_min_messages || elevel == INFO;
        }
    }
    false
}

/*
 * message_level_is_interesting --- would ereport/elog do anything?
 *
 * Returns true if ereport/elog with this elevel will not be a no-op.
 */
pub unsafe fn message_level_is_interesting(elevel: c_int) -> bool {
    if elevel >= ERROR
        || should_output_to_server(elevel)
        || should_output_to_client(elevel)
    {
        return true;
    }
    false
}

/*
 * in_error_recursion_trouble --- are we at risk of infinite error recursion?
 */
pub unsafe fn in_error_recursion_trouble() -> bool {
    /* Pull the plug if recurse more than once */
    recursion_depth > 2
}

/*
 * errstart_cold
 *   A simple wrapper around errstart, but hinted to be "cold".
 */
#[cold]
pub unsafe fn errstart_cold(elevel: c_int, domain: *const c_char) -> bool {
    errstart(elevel, domain)
}

/*
 * errstart --- begin an error-reporting cycle
 *
 * Create and initialize error stack entry.  Subsequently, errmsg() and
 * perhaps other routines will be called to further populate the stack entry.
 * Finally, errfinish() will be called to actually process the error report.
 *
 * Returns true in normal case.  Returns false to short-circuit the error
 * report (if it's a warning or lower and not to be reported anywhere).
 */
pub unsafe fn errstart(mut elevel: c_int, domain: *const c_char) -> bool {
    let edata: *mut ErrorData;
    let output_to_server: bool;
    let mut output_to_client: bool = false;
    let mut i: c_int;

    /*
     * Check some cases in which we want to promote an error into a more
     * severe error.  None of this logic applies for non-error messages.
     */
    if elevel >= ERROR {
        /*
         * If we are inside a critical section, all errors become PANIC errors.
         */
        if CritSectionCount > 0 {
            elevel = PANIC;
        }

        /*
         * Check reasons for treating ERROR as FATAL:
         *
         * 1. we have no handler to pass the error to
         * 2. ExitOnAnyError mode switch is set
         * 3. the error occurred after proc_exit has begun to run
         */
        if elevel == ERROR {
            if PG_exception_stack.is_null() || ExitOnAnyError || proc_exit_inprogress {
                elevel = FATAL;
            }
        }

        /*
         * If the error level is ERROR or more, errfinish is not going to
         * return to caller.  Check the stack and make sure we panic if panic
         * is warranted.
         */
        i = 0;
        while i <= errordata_stack_depth {
            let stk_elevel = (*errordata.as_ptr().add(i as usize)).elevel;
            if stk_elevel > elevel {
                elevel = stk_elevel;
            }
            i += 1;
        }
    }

    /*
     * Now decide whether we need to process this report at all; if it's
     * warning or less and not enabled for logging, just return false without
     * starting up any error logging machinery.
     */
    output_to_server = should_output_to_server(elevel);
    output_to_client = should_output_to_client(elevel);
    if elevel < ERROR && !output_to_server && !output_to_client {
        return false;
    }

    /*
     * We need to do some actual work.  Make sure that memory context
     * initialization has finished, else we can't do anything useful.
     */
    if crate::utils::palloc::CurrentMemoryContext.is_null() {
        /* Oops, hard crash time; very little we can do safely here */
        let msg = b"error occurred before error message processing is available\n\0";
        vwrite_stderr_raw(msg.as_ptr() as *const c_char, msg.len() - 1);
        std::process::exit(2);
    }

    /*
     * Okay, crank up a stack entry to store the info in.
     */

    recursion_depth += 1;
    if recursion_depth > 1 && elevel >= ERROR {
        /*
         * Oops, error during error processing.  Clear ErrorContext as
         * discussed at top of file.
         */
        if !ErrorContext.is_null() {
            MemoryContextReset(ErrorContext);
        }

        /*
         * Infinite error recursion might be due to something broken in a
         * context traceback routine.  Abandon them too.
         */
        if in_error_recursion_trouble() {
            error_context_stack = null_mut();
            debug_query_string = null();
        }
    }

    /* Initialize data for this error frame */
    edata = get_error_stack_entry();
    (*edata).elevel = elevel;
    (*edata).output_to_server = output_to_server;
    (*edata).output_to_client = output_to_client;
    set_stack_entry_domain(edata, domain);
    /* Select default errcode based on elevel */
    if elevel >= ERROR {
        (*edata).sqlerrcode = ERRCODE_INTERNAL_ERROR;
    } else if elevel >= WARNING {
        (*edata).sqlerrcode = ERRCODE_WARNING;
    } else {
        (*edata).sqlerrcode = ERRCODE_SUCCESSFUL_COMPLETION;
    }

    /*
     * Any allocations for this error state level should go into ErrorContext
     */
    (*edata).assoc_context = ErrorContext;

    recursion_depth -= 1;
    true
}

/*
 * errfinish --- end an error-reporting cycle
 *
 * Produce the appropriate error report(s) and pop the error stack.
 *
 * If elevel, as passed to errstart(), is ERROR or worse, control does not
 * return to the caller.
 */
pub unsafe fn errfinish(filename: *const c_char, lineno: c_int, funcname: *const c_char) {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let elevel: c_int;
    let oldcontext: MemoryContext;
    let mut econtext: *mut ErrorContextCallback;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();

    /* Save the last few bits of error state into the stack entry */
    set_stack_entry_location(edata, filename, lineno, funcname);

    elevel = (*edata).elevel;

    /*
     * Do processing in ErrorContext, which we hope has enough reserved space
     * to report an error.
     */
    oldcontext = MemoryContextSwitchTo(ErrorContext);

    /* Collect backtrace, if enabled and we didn't already */
    if (*edata).backtrace.is_null()
        && !(*edata).funcname.is_null()
        && !backtrace_functions.is_null()
        && matches_backtrace_functions((*edata).funcname)
    {
        set_backtrace(edata, 2);
    }

    /*
     * Call any context callback functions.  Errors occurring in callback
     * functions will be treated as recursive errors.
     */
    econtext = error_context_stack;
    while !econtext.is_null() {
        ((*econtext).callback)((*econtext).arg);
        econtext = (*econtext).previous;
    }

    /*
     * If ERROR (not more nor less) we pass it off to the current handler.
     */
    if elevel == ERROR {
        /*
         * We do some minimal cleanup before longjmp'ing so that handlers can
         * execute in a reasonably sane state.
         */
        InterruptHoldoffCount = 0;
        QueryCancelHoldoffCount = 0;

        CritSectionCount = 0; /* should be unnecessary, but... */

        recursion_depth -= 1;
        pg_re_throw();
        // unreachable
    }

    /* Emit the message to the right places */
    EmitErrorReport();

    /* Now free up subsidiary data attached to stack entry, and release it */
    FreeErrorDataContents(edata);
    errordata_stack_depth -= 1;

    /* Exit error-handling context */
    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;

    /*
     * Perform error recovery action as specified by elevel.
     */
    if elevel == FATAL {
        /*
         * For a FATAL error, we let proc_exit clean up and exit.
         */
        if PG_exception_stack.is_null() && whereToSendOutput == DestRemote {
            whereToSendOutput = DestNone;
        }

        /* fflush here is just to improve the odds that we get to see the error message */
        fflush(core::ptr::null_mut());

        /* Let the cumulative stats system know. */
        if pgStatSessionEndCause == DISCONNECT_NORMAL {
            pgStatSessionEndCause = DISCONNECT_FATAL;
        }

        /*
         * Do normal process-exit cleanup, then return exit code 1 to indicate
         * FATAL termination.
         */
        proc_exit(1);
    }

    if elevel >= PANIC {
        /*
         * Serious crash time. Postmaster will observe SIGABRT process exit
         * status and kill the other backends too.
         */
        fflush(core::ptr::null_mut());
        abort();
    }

    /*
     * Check for cancel/die interrupt first --- this is so that the user can
     * stop a query emitting tons of notice or warning messages.
     */
    CHECK_FOR_INTERRUPTS!();
}

/*
 * errsave_start --- begin a "soft" error-reporting cycle
 *
 * If "context" isn't an ErrorSaveContext node, this behaves as
 * errstart(ERROR, domain), and the errsave() macro ends up acting
 * exactly like ereport(ERROR, ...).
 */
pub unsafe fn errsave_start(context: *mut Node, domain: *const c_char) -> bool {
    let escontext: *mut ErrorSaveContext;
    let edata: *mut ErrorData;

    /*
     * Do we have a context for soft error reporting?  If not, just punt to
     * errstart().
     */
    if context.is_null() || !crate::IsA!(context, T_ErrorSaveContext) {
        return errstart(ERROR, domain);
    }

    /* Report that a soft error was detected */
    escontext = context as *mut ErrorSaveContext;
    (*escontext).error_occurred = true;

    /* Nothing else to do if caller wants no further details */
    if !(*escontext).details_wanted {
        return false;
    }

    /*
     * Okay, crank up a stack entry to store the info in.
     */
    recursion_depth += 1;

    /* Initialize data for this error frame */
    edata = get_error_stack_entry();
    (*edata).elevel = LOG; /* signal all is well to errsave_finish */
    set_stack_entry_domain(edata, domain);
    /* Select default errcode based on the assumed elevel of ERROR */
    (*edata).sqlerrcode = ERRCODE_INTERNAL_ERROR;

    /*
     * Any allocations for this error state level should go into the caller's
     * context.
     */
    (*edata).assoc_context = crate::utils::palloc::CurrentMemoryContext;

    recursion_depth -= 1;
    true
}

/*
 * errsave_finish --- end a "soft" error-reporting cycle
 *
 * If errsave_start() decided this was a regular error, behave as
 * errfinish().  Otherwise, package up the error details and save
 * them in the ErrorSaveContext node.
 */
pub unsafe fn errsave_finish(
    context: *mut Node,
    filename: *const c_char,
    lineno: c_int,
    funcname: *const c_char,
) {
    let escontext: *mut ErrorSaveContext = context as *mut ErrorSaveContext;
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];

    /* verify stack depth before accessing *edata */
    CHECK_STACK_DEPTH!();

    /*
     * If errsave_start punted to errstart, then elevel will be ERROR or
     * perhaps even PANIC.  Punt likewise to errfinish.
     */
    if (*edata).elevel >= ERROR {
        errfinish(filename, lineno, funcname);
        // pg_unreachable
        unreachable!("errfinish returned with ERROR-or-higher");
    }

    /*
     * Else, we should package up the stack entry contents and deliver them
     * to the caller.
     */
    recursion_depth += 1;

    /* Save the last few bits of error state into the stack entry */
    set_stack_entry_location(edata, filename, lineno, funcname);

    /* Replace the LOG value that errsave_start inserted */
    (*edata).elevel = ERROR;

    /*
     * We skip calling backtrace and context functions, which are more likely
     * to cause trouble than provide useful context.
     */

    /*
     * Make a copy of the error info for the caller.  All the subsidiary
     * strings are already in the caller's context, so it's sufficient to
     * flat-copy the stack entry.
     */
    (*escontext).error_data = palloc(size_of::<ErrorData>());
    core::ptr::copy_nonoverlapping(edata, (*escontext).error_data as *mut ErrorData, 1);

    /* Exit error-handling context */
    errordata_stack_depth -= 1;
    recursion_depth -= 1;
}

/*
 * get_error_stack_entry --- allocate and initialize a new stack entry
 */
unsafe fn get_error_stack_entry() -> *mut ErrorData {
    let edata: *mut ErrorData;

    /* Allocate error frame */
    errordata_stack_depth += 1;
    if errordata_stack_depth >= ERRORDATA_STACK_SIZE as c_int {
        /* Wups, stack not big enough */
        errordata_stack_depth = -1; /* make room on stack */
        panic!("ERRORDATA_STACK_SIZE exceeded");
    }

    /* Initialize error frame to all zeroes/NULLs */
    edata = &mut errordata[errordata_stack_depth as usize];
    core::ptr::write_bytes(edata as *mut u8, 0, size_of::<ErrorData>());

    /* Save errno immediately to ensure error parameter eval can't change it */
    (*edata).saved_errno = *__errno_location();

    edata
}

/*
 * set_stack_entry_domain --- fill in the internationalization domain
 */
unsafe fn set_stack_entry_domain(edata: *mut ErrorData, domain: *const c_char) {
    /* the default text domain is the backend's */
    (*edata).domain = if !domain.is_null() {
        domain
    } else {
        b"postgres\0".as_ptr() as *const c_char
    };
    /* initialize context_domain the same way */
    (*edata).context_domain = (*edata).domain;
}

/*
 * set_stack_entry_location --- fill in code-location details
 */
unsafe fn set_stack_entry_location(
    edata: *mut ErrorData,
    mut filename: *const c_char,
    lineno: c_int,
    funcname: *const c_char,
) {
    if !filename.is_null() {
        let mut slash: *const c_char;

        /* keep only base name, useful especially for vpath builds */
        slash = strrchr(filename, b'/' as c_int);
        if !slash.is_null() {
            filename = slash.add(1);
        }
        /* Some Windows compilers use backslashes in __FILE__ strings */
        slash = strrchr(filename, b'\\' as c_int);
        if !slash.is_null() {
            filename = slash.add(1);
        }
    }

    (*edata).filename = filename;
    (*edata).lineno = lineno;
    (*edata).funcname = funcname;
}


// ===========================================================================
// Part 3: errcode family, errmsg/errdetail/errhint family, format_elog_string,
//         matches_backtrace_functions, set_backtrace, EmitErrorReport, CopyErrorData
// ===========================================================================

/*
 * matches_backtrace_functions --- checks whether the given funcname matches
 * backtrace_functions
 */
unsafe fn matches_backtrace_functions(funcname: *const c_char) -> bool {
    let mut p: *const c_char;

    if backtrace_function_list.is_null()
        || funcname.is_null()
        || *funcname == 0
    {
        return false;
    }

    p = backtrace_function_list;
    loop {
        if *p == 0 {
            /* end of backtrace_function_list */
            break;
        }

        if strcmp(funcname, p) == 0 {
            return true;
        }
        p = p.add(strlen(p) + 1);
    }

    false
}

/*
 * set_backtrace --- Compute backtrace data and add it to the supplied ErrorData.
 * num_skip specifies how many inner frames to skip.
 */
#[inline(never)]
unsafe fn set_backtrace(edata: *mut ErrorData, num_skip: c_int) {
    let mut errtrace = StringInfoData {
        data: null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };
    initStringInfo(&mut errtrace);

    #[cfg(all(unix, feature = "have_backtrace_symbols"))]
    {
        extern "C" {
            fn backtrace(buffer: *mut *mut c_void, size: c_int) -> c_int;
            fn backtrace_symbols(buffer: *const *mut c_void, size: c_int) -> *mut *mut c_char;
        }

        const BUF_SIZE: usize = 100;
        let mut buf: [*mut c_void; BUF_SIZE] = [null_mut(); BUF_SIZE];
        let nframes = backtrace(buf.as_mut_ptr(), BUF_SIZE as c_int);
        let strfrms = backtrace_symbols(buf.as_ptr(), nframes);
        if strfrms.is_null() {
            return;
        }

        let mut i = num_skip;
        while i < nframes {
            let s = *strfrms.add(i as usize);
            let newline = b"\n\0";
            appendStringInfoString(&mut errtrace, newline.as_ptr() as *const c_char);
            appendStringInfoString(&mut errtrace, s);
            i += 1;
        }
        free(strfrms as *mut c_void);
    }

    #[cfg(not(all(unix, feature = "have_backtrace_symbols")))]
    {
        let msg = b"backtrace generation is not supported by this installation\0";
        appendStringInfoString(&mut errtrace, msg.as_ptr() as *const c_char);
    }

    (*edata).backtrace = errtrace.data;
}

// ---------------------------------------------------------------------------
// EVALUATE_MESSAGE helper
//
// The C macro expands to format a string using printf into edata->targetfield.
// In Rust we pass a pre-formatted &str since we cannot do variadic printf.
// Each errmsg/errdetail/... function receives the already-formatted message
// text from the ereport!() macro layer.
// ---------------------------------------------------------------------------

// evaluate_message -- set edata string field from a &str, appending if appendval
unsafe fn evaluate_message(
    edata: *mut ErrorData,
    field: *mut *mut c_char,
    text: *const c_char,
    appendval: bool,
) {
    let mut buf = StringInfoData { data: null_mut(), len: 0, maxlen: 0, cursor: 0 };
    initStringInfo(&mut buf);

    if appendval && !(*field).is_null() {
        appendStringInfoString(&mut buf, *field);
        appendStringInfoChar(&mut buf, b'\n' as c_char);
    }
    if !text.is_null() {
        appendStringInfoString(&mut buf, text);
    }

    if !(*field).is_null() {
        pfree(*field as *mut c_void);
    }
    *field = pstrdup(buf.data);
    pfree(buf.data as *mut c_void);
}

/*
 * errcode --- add SQLSTATE error code to the current error
 */
pub unsafe fn errcode_impl(sqlerrcode: c_int) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    /* we don't bother incrementing recursion_depth */
    CHECK_STACK_DEPTH!();
    (*edata).sqlerrcode = sqlerrcode;
    0 /* return value does not matter */
}

/*
 * errcode_for_file_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.
 */
pub unsafe fn errcode_for_file_access() -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    /* we don't bother incrementing recursion_depth */
    CHECK_STACK_DEPTH!();

    let e = (*edata).saved_errno;
    (*edata).sqlerrcode = match e {
        EPERM | EACCES => ERRCODE_INSUFFICIENT_PRIVILEGE,
        #[cfg(target_os = "linux")]
        EROFS => ERRCODE_INSUFFICIENT_PRIVILEGE,
        ENOENT => ERRCODE_UNDEFINED_FILE,
        EEXIST => ERRCODE_DUPLICATE_FILE,
        ENOTDIR | EISDIR => ERRCODE_WRONG_OBJECT_TYPE,
        ENOSPC => ERRCODE_DISK_FULL,
        ENOMEM => ERRCODE_OUT_OF_MEMORY,
        ENFILE | EMFILE => ERRCODE_INSUFFICIENT_RESOURCES,
        EIO => ERRCODE_IO_ERROR,
        ENAMETOOLONG => ERRCODE_FILE_NAME_TOO_LONG,
        _ => ERRCODE_INTERNAL_ERROR,
    };

    0 /* return value does not matter */
}

/*
 * errcode_for_socket_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.
 */
pub unsafe fn errcode_for_socket_access() -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    /* we don't bother incrementing recursion_depth */
    CHECK_STACK_DEPTH!();

    // ALL_CONNECTION_FAILURE_ERRNOS from libpq (ECONNRESET, EPIPE, etc.)
    let e = (*edata).saved_errno;
    (*edata).sqlerrcode = if e == ECONNRESET
        || e == ECONNABORTED
        || e == EPIPE
        || e == ETIMEDOUT
    {
        ERRCODE_CONNECTION_FAILURE
    } else {
        ERRCODE_INTERNAL_ERROR
    };

    0
}

/*
 * errmsg --- add a primary error message text to the current error
 */
pub unsafe fn errmsg_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    (*edata).message_id = text;
    evaluate_message(edata, &mut (*edata).message, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errbacktrace --- Add a backtrace to the containing ereport() call.
 */
pub unsafe fn errbacktrace() -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    set_backtrace(edata, 1);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errmsg_internal --- add a primary error message text (no translation)
 */
pub unsafe fn errmsg_internal_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    (*edata).message_id = text;
    evaluate_message(edata, &mut (*edata).message, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errmsg_plural --- add a primary error message text with pluralization
 *
 * n is the count that selects singular vs plural.
 * text is the already-chosen form.
 */
pub unsafe fn errmsg_plural_c(text: *const c_char, n: c_ulong) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    (*edata).message_id = text;
    evaluate_message(edata, &mut (*edata).message, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errdetail --- add a detail error message text to the current error
 */
pub unsafe fn errdetail_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).detail, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errdetail_internal --- add a detail error message text (no translation)
 */
pub unsafe fn errdetail_internal_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).detail, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errdetail_log --- add a detail_log error message text to the current error
 */
pub unsafe fn errdetail_log_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).detail_log, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errdetail_log_plural --- add a detail_log error message text with pluralization
 */
pub unsafe fn errdetail_log_plural_c(text: *const c_char, _n: c_ulong) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).detail_log, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errdetail_plural --- add a detail error message text with pluralization
 */
pub unsafe fn errdetail_plural_c(text: *const c_char, _n: c_ulong) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).detail, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errhint --- add a hint error message text to the current error
 */
pub unsafe fn errhint_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).hint, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errhint_internal --- add a hint error message text (no translation)
 */
pub unsafe fn errhint_internal_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).hint, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errhint_plural --- add a hint error message text with pluralization
 */
pub unsafe fn errhint_plural_c(text: *const c_char, _n: c_ulong) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).hint, text, false);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * errcontext_msg --- add a context error message text to the current error
 *
 * Unlike other cases, multiple calls are allowed to build up a stack of
 * context information.
 */
pub unsafe fn errcontext_msg_c(text: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    evaluate_message(edata, &mut (*edata).context, text, true);

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
    0
}

/*
 * set_errcontext_domain --- set message domain to be used by errcontext()
 */
pub unsafe fn set_errcontext_domain(domain: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    /* we don't bother incrementing recursion_depth */
    CHECK_STACK_DEPTH!();

    /* the default text domain is the backend's */
    (*edata).context_domain = if !domain.is_null() {
        domain
    } else {
        b"postgres\0".as_ptr() as *const c_char
    };

    0
}

/*
 * errhidestmt --- optionally suppress STATEMENT: field of log entry
 */
pub unsafe fn errhidestmt(hide_stmt: bool) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).hide_stmt = hide_stmt;
    0
}

/*
 * errhidecontext --- optionally suppress CONTEXT: field of log entry
 */
pub unsafe fn errhidecontext(hide_ctx: bool) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).hide_ctx = hide_ctx;
    0
}

/*
 * errposition --- add cursor position to the current error
 */
pub unsafe fn errposition(cursorpos: c_int) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).cursorpos = cursorpos;
    0
}

/*
 * internalerrposition --- add internal cursor position to the current error
 */
pub unsafe fn internalerrposition(cursorpos: c_int) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).internalpos = cursorpos;
    0
}

/*
 * internalerrquery --- add internal query text to the current error
 */
pub unsafe fn internalerrquery(query: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();

    if !(*edata).internalquery.is_null() {
        pfree((*edata).internalquery as *mut c_void);
        (*edata).internalquery = null_mut();
    }

    if !query.is_null() {
        (*edata).internalquery = MemoryContextStrdup((*edata).assoc_context, query);
    }

    0
}

/*
 * err_generic_string -- used to set individual ErrorData string fields
 * identified by PG_DIAG_xxx codes.
 */
pub unsafe fn err_generic_string(field: c_int, str: *const c_char) -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();

    let target: *mut *mut c_char = match field as u8 {
        b's' => &mut (*edata).schema_name,    /* PG_DIAG_SCHEMA_NAME */
        b't' => &mut (*edata).table_name,     /* PG_DIAG_TABLE_NAME */
        b'c' => &mut (*edata).column_name,    /* PG_DIAG_COLUMN_NAME */
        b'd' => &mut (*edata).datatype_name,  /* PG_DIAG_DATATYPE_NAME */
        b'n' => &mut (*edata).constraint_name, /* PG_DIAG_CONSTRAINT_NAME */
        _ => {
            panic!("unsupported ErrorData field id: {}", field);
        }
    };
    set_errdata_field((*edata).assoc_context, target, str);
    0
}

/*
 * set_errdata_field --- set an ErrorData string field
 */
unsafe fn set_errdata_field(cxt: MemoryContext, ptr: *mut *mut c_char, str: *const c_char) {
    // Assert(*ptr == NULL)
    debug_assert!((*ptr).is_null());
    *ptr = MemoryContextStrdup(cxt, str);
}

/*
 * geterrcode --- return the currently set SQLSTATE error code
 */
pub unsafe fn geterrcode() -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).sqlerrcode
}

/*
 * geterrposition --- return the currently set error position (0 if none)
 */
pub unsafe fn geterrposition() -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).cursorpos
}

/*
 * getinternalerrposition --- same for internal error position
 */
pub unsafe fn getinternalerrposition() -> c_int {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    CHECK_STACK_DEPTH!();
    (*edata).internalpos
}

// ---------------------------------------------------------------------------
// pre_format_elog_string / format_elog_string
// ---------------------------------------------------------------------------

static mut save_format_errnumber: c_int = 0;
static mut save_format_domain: *const c_char = null();

/*
 * pre_format_elog_string -- save errno and domain for format_elog_string()
 */
pub unsafe fn pre_format_elog_string(errnumber: c_int, domain: *const c_char) {
    /* Save errno before evaluation of argument functions can change it */
    save_format_errnumber = errnumber;
    /* Save caller's text domain */
    save_format_domain = domain;
}

/*
 * format_elog_string -- format a message outside of an active ereport() call
 *
 * text: already-formatted message (Rust: no printf, pass pre-formatted)
 */
pub unsafe fn format_elog_string_c(text: *const c_char) -> *mut c_char {
    let mut errdata_local: ErrorData = core::mem::zeroed();
    let edata: *mut ErrorData = &mut errdata_local;
    let oldcontext: MemoryContext;

    /* Initialize a mostly-dummy error frame */
    /* the default text domain is the backend's */
    (*edata).domain = if !save_format_domain.is_null() {
        save_format_domain
    } else {
        b"postgres\0".as_ptr() as *const c_char
    };
    /* set the errno to be used to interpret %m */
    (*edata).saved_errno = save_format_errnumber;

    oldcontext = MemoryContextSwitchTo(ErrorContext);

    (*edata).message_id = text;
    evaluate_message(edata, &mut (*edata).message, text, false);

    MemoryContextSwitchTo(oldcontext);

    (*edata).message
}

/*
 * EmitErrorReport
 *
 * Actual output of the top-of-stack error message.
 *
 * In the ereport(ERROR) case this is called from PostgresMain (or not at all,
 * if the error is caught by somebody).  For all other severity levels this
 * is called by errfinish.
 */
pub unsafe fn EmitErrorReport() {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let oldcontext: MemoryContext;

    recursion_depth += 1;
    CHECK_STACK_DEPTH!();
    oldcontext = MemoryContextSwitchTo((*edata).assoc_context);

    /*
     * Reset the formatted timestamp fields before emitting any logs.
     */
    saved_timeval_set = false;
    formatted_log_time[0] = 0;

    /*
     * Call hook before sending message to log.
     */
    if (*edata).output_to_server {
        if let Some(hook) = emit_log_hook {
            hook(edata);
        }
    }

    /* Send to server log, if enabled */
    if (*edata).output_to_server {
        send_message_to_server_log(edata);
    }

    /* Send to client, if enabled */
    if (*edata).output_to_client {
        send_message_to_frontend(edata);
    }

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;
}

/*
 * CopyErrorData --- obtain a copy of the topmost error stack entry
 *
 * This is only for use in error handler code.
 */
pub unsafe fn CopyErrorData() -> *mut ErrorData {
    let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];
    let newedata: *mut ErrorData;

    CHECK_STACK_DEPTH!();

    /* Copy the struct itself */
    newedata = palloc(size_of::<ErrorData>()) as *mut ErrorData;
    core::ptr::copy_nonoverlapping(edata, newedata, 1);

    /*
     * Make copies of separately-allocated strings.
     */
    if !(*newedata).filename.is_null() {
        (*newedata).filename = pstrdup((*newedata).filename);
    }
    if !(*newedata).funcname.is_null() {
        (*newedata).funcname = pstrdup((*newedata).funcname);
    }
    if !(*newedata).domain.is_null() {
        (*newedata).domain = pstrdup((*newedata).domain);
    }
    if !(*newedata).context_domain.is_null() {
        (*newedata).context_domain = pstrdup((*newedata).context_domain);
    }
    if !(*newedata).message.is_null() {
        (*newedata).message = pstrdup((*newedata).message);
    }
    if !(*newedata).detail.is_null() {
        (*newedata).detail = pstrdup((*newedata).detail);
    }
    if !(*newedata).detail_log.is_null() {
        (*newedata).detail_log = pstrdup((*newedata).detail_log);
    }
    if !(*newedata).hint.is_null() {
        (*newedata).hint = pstrdup((*newedata).hint);
    }
    if !(*newedata).context.is_null() {
        (*newedata).context = pstrdup((*newedata).context);
    }
    if !(*newedata).backtrace.is_null() {
        (*newedata).backtrace = pstrdup((*newedata).backtrace);
    }
    if !(*newedata).message_id.is_null() {
        (*newedata).message_id = pstrdup((*newedata).message_id);
    }
    if !(*newedata).schema_name.is_null() {
        (*newedata).schema_name = pstrdup((*newedata).schema_name);
    }
    if !(*newedata).table_name.is_null() {
        (*newedata).table_name = pstrdup((*newedata).table_name);
    }
    if !(*newedata).column_name.is_null() {
        (*newedata).column_name = pstrdup((*newedata).column_name);
    }
    if !(*newedata).datatype_name.is_null() {
        (*newedata).datatype_name = pstrdup((*newedata).datatype_name);
    }
    if !(*newedata).constraint_name.is_null() {
        (*newedata).constraint_name = pstrdup((*newedata).constraint_name);
    }
    if !(*newedata).internalquery.is_null() {
        (*newedata).internalquery = pstrdup((*newedata).internalquery);
    }

    /* Use the calling context for string allocation */
    (*newedata).assoc_context = crate::utils::palloc::CurrentMemoryContext;

    newedata
}

/*
 * FreeErrorData --- free the structure returned by CopyErrorData.
 */
pub unsafe fn FreeErrorData(edata: *mut ErrorData) {
    FreeErrorDataContents(edata);
    pfree(edata as *mut c_void);
}

/*
 * FreeErrorDataContents --- free the subsidiary data of an ErrorData.
 */
pub unsafe fn FreeErrorDataContents(edata: *mut ErrorData) {
    if !(*edata).message.is_null() {
        pfree((*edata).message as *mut c_void);
    }
    if !(*edata).detail.is_null() {
        pfree((*edata).detail as *mut c_void);
    }
    if !(*edata).detail_log.is_null() {
        pfree((*edata).detail_log as *mut c_void);
    }
    if !(*edata).hint.is_null() {
        pfree((*edata).hint as *mut c_void);
    }
    if !(*edata).context.is_null() {
        pfree((*edata).context as *mut c_void);
    }
    if !(*edata).backtrace.is_null() {
        pfree((*edata).backtrace as *mut c_void);
    }
    if !(*edata).schema_name.is_null() {
        pfree((*edata).schema_name as *mut c_void);
    }
    if !(*edata).table_name.is_null() {
        pfree((*edata).table_name as *mut c_void);
    }
    if !(*edata).column_name.is_null() {
        pfree((*edata).column_name as *mut c_void);
    }
    if !(*edata).datatype_name.is_null() {
        pfree((*edata).datatype_name as *mut c_void);
    }
    if !(*edata).constraint_name.is_null() {
        pfree((*edata).constraint_name as *mut c_void);
    }
    if !(*edata).internalquery.is_null() {
        pfree((*edata).internalquery as *mut c_void);
    }
}


// ===========================================================================
// Part 4: FlushErrorState, ThrowErrorData, ReThrowError, pg_re_throw,
//         GetErrorContextStack, DebugFileOpen, GUC hooks,
//         write_syslog (stub), write_console, timestamp helpers,
//         check_log_of_query, get_backend_type_for_log,
//         process_log_prefix_padding, log_line_prefix, log_status_format,
//         unpack_sql_state, append_with_tabs
// ===========================================================================

/*
 * FlushErrorState --- flush the error state after error recovery
 */
pub unsafe fn FlushErrorState() {
    /*
     * Reset stack to empty.
     */
    errordata_stack_depth = -1;
    recursion_depth = 0;
    /* Delete all data in ErrorContext */
    if !ErrorContext.is_null() {
        MemoryContextReset(ErrorContext);
    }
}

/*
 * ThrowErrorData --- report an error described by an ErrorData structure
 */
pub unsafe fn ThrowErrorData(edata: *mut ErrorData) {
    let newedata: *mut ErrorData;
    let oldcontext: MemoryContext;

    if !errstart((*edata).elevel, (*edata).domain) {
        return; /* error is not to be reported at all */
    }

    newedata = &mut errordata[errordata_stack_depth as usize];
    recursion_depth += 1;
    oldcontext = MemoryContextSwitchTo((*newedata).assoc_context);

    /* Copy the supplied fields to the error stack entry. */
    if (*edata).sqlerrcode != 0 {
        (*newedata).sqlerrcode = (*edata).sqlerrcode;
    }
    if !(*edata).message.is_null() {
        (*newedata).message = pstrdup((*edata).message);
    }
    if !(*edata).detail.is_null() {
        (*newedata).detail = pstrdup((*edata).detail);
    }
    if !(*edata).detail_log.is_null() {
        (*newedata).detail_log = pstrdup((*edata).detail_log);
    }
    if !(*edata).hint.is_null() {
        (*newedata).hint = pstrdup((*edata).hint);
    }
    if !(*edata).context.is_null() {
        (*newedata).context = pstrdup((*edata).context);
    }
    if !(*edata).backtrace.is_null() {
        (*newedata).backtrace = pstrdup((*edata).backtrace);
    }
    /* assume message_id is not available */
    if !(*edata).schema_name.is_null() {
        (*newedata).schema_name = pstrdup((*edata).schema_name);
    }
    if !(*edata).table_name.is_null() {
        (*newedata).table_name = pstrdup((*edata).table_name);
    }
    if !(*edata).column_name.is_null() {
        (*newedata).column_name = pstrdup((*edata).column_name);
    }
    if !(*edata).datatype_name.is_null() {
        (*newedata).datatype_name = pstrdup((*edata).datatype_name);
    }
    if !(*edata).constraint_name.is_null() {
        (*newedata).constraint_name = pstrdup((*edata).constraint_name);
    }
    (*newedata).cursorpos = (*edata).cursorpos;
    (*newedata).internalpos = (*edata).internalpos;
    if !(*edata).internalquery.is_null() {
        (*newedata).internalquery = pstrdup((*edata).internalquery);
    }

    MemoryContextSwitchTo(oldcontext);
    recursion_depth -= 1;

    /* Process the error. */
    errfinish((*edata).filename, (*edata).lineno, (*edata).funcname);
}

/*
 * ReThrowError --- re-throw a previously copied error
 *
 * A handler can do CopyErrorData/FlushErrorState to get out of the error
 * subsystem, then do some processing, and finally ReThrowError to re-throw
 * the original error.
 */
pub unsafe fn ReThrowError(edata: *mut ErrorData) {
    let newedata: *mut ErrorData;

    debug_assert!((*edata).elevel == ERROR);

    /* Push the data back into the error context */
    recursion_depth += 1;
    MemoryContextSwitchTo(ErrorContext);

    newedata = get_error_stack_entry();
    core::ptr::copy_nonoverlapping(edata, newedata, 1);

    /* Make copies of separately-allocated fields */
    if !(*newedata).message.is_null() {
        (*newedata).message = pstrdup((*newedata).message);
    }
    if !(*newedata).detail.is_null() {
        (*newedata).detail = pstrdup((*newedata).detail);
    }
    if !(*newedata).detail_log.is_null() {
        (*newedata).detail_log = pstrdup((*newedata).detail_log);
    }
    if !(*newedata).hint.is_null() {
        (*newedata).hint = pstrdup((*newedata).hint);
    }
    if !(*newedata).context.is_null() {
        (*newedata).context = pstrdup((*newedata).context);
    }
    if !(*newedata).backtrace.is_null() {
        (*newedata).backtrace = pstrdup((*newedata).backtrace);
    }
    if !(*newedata).schema_name.is_null() {
        (*newedata).schema_name = pstrdup((*newedata).schema_name);
    }
    if !(*newedata).table_name.is_null() {
        (*newedata).table_name = pstrdup((*newedata).table_name);
    }
    if !(*newedata).column_name.is_null() {
        (*newedata).column_name = pstrdup((*newedata).column_name);
    }
    if !(*newedata).datatype_name.is_null() {
        (*newedata).datatype_name = pstrdup((*newedata).datatype_name);
    }
    if !(*newedata).constraint_name.is_null() {
        (*newedata).constraint_name = pstrdup((*newedata).constraint_name);
    }
    if !(*newedata).internalquery.is_null() {
        (*newedata).internalquery = pstrdup((*newedata).internalquery);
    }

    /* Reset the assoc_context to be ErrorContext */
    (*newedata).assoc_context = ErrorContext;

    recursion_depth -= 1;
    pg_re_throw();
}

/*
 * pg_re_throw --- out-of-line implementation of PG_RE_THROW() macro
 */
pub unsafe fn pg_re_throw() -> ! {
    /* If possible, throw the error to the next outer setjmp handler */
    if !PG_exception_stack.is_null() {
        // TODO(pg-port): when sigjmp_buf support is added, call siglongjmp here.
        // For now, panic unwinds through catch_unwind barriers.
        panic!("pg_re_throw: ERROR propagation");
    } else {
        /*
         * If we get here, elog(ERROR) was thrown inside a PG_TRY block, which
         * we have now exited only to discover that there is no outer setjmp
         * handler to pass the error to.  Promote the error to FATAL.
         */
        let edata: *mut ErrorData = &mut errordata[errordata_stack_depth as usize];

        debug_assert!(errordata_stack_depth >= 0);
        debug_assert!((*edata).elevel == ERROR);
        (*edata).elevel = FATAL;

        /*
         * At least in principle, the increase in severity could have changed
         * where-to-output decisions, so recalculate.
         */
        (*edata).output_to_server = should_output_to_server(FATAL);
        (*edata).output_to_client = should_output_to_client(FATAL);

        /*
         * We can use errfinish() for the rest, but we don't want it to call
         * any error context routines a second time.  Since we know we are
         * about to exit, it should be OK to just clear the context stack.
         */
        error_context_stack = null_mut();

        errfinish((*edata).filename, (*edata).lineno, (*edata).funcname);
    }

    /* Doesn't return ... */
    let cond = b"pg_re_throw tried to return\0";
    let file = b"elog_impl.rs\0";
    ExceptionalCondition(
        cond.as_ptr() as *const c_char,
        file.as_ptr() as *const c_char,
        0,
    );
}

/*
 * GetErrorContextStack - Return the context stack, for display/diags
 *
 * Returns a pstrdup'd string in the caller's context.
 */
pub unsafe fn GetErrorContextStack() -> *mut c_char {
    let edata: *mut ErrorData;
    let mut econtext: *mut ErrorContextCallback;

    /*
     * Crank up a stack entry to store the info in.
     */
    recursion_depth += 1;

    edata = get_error_stack_entry();

    /*
     * Set up assoc_context to be the caller's context.
     */
    (*edata).assoc_context = crate::utils::palloc::CurrentMemoryContext;

    /*
     * Call any context callback functions.
     */
    econtext = error_context_stack;
    while !econtext.is_null() {
        ((*econtext).callback)((*econtext).arg);
        econtext = (*econtext).previous;
    }

    /*
     * Clean ourselves off the stack.
     */
    errordata_stack_depth -= 1;
    recursion_depth -= 1;

    /*
     * Return a pointer to the string the caller asked for.
     */
    (*edata).context
}

/*
 * DebugFileOpen
 *
 * Initialization of error output file.
 */
pub unsafe fn DebugFileOpen() {
    let fd: c_int;
    let istty: c_int;

    if *OutputFileName.as_ptr() != 0 {
        /*
         * A debug-output file name was given.
         * Make sure we can write the file, and find out if it's a tty.
         */
        fd = open(
            OutputFileName.as_ptr(),
            O_CREAT | O_APPEND | O_WRONLY,
            0o666,
        );
        if fd < 0 {
            // ereport(FATAL, ...) -- here we use the stub emit path
            let msg = b"could not open DebugOutputFile\0";
            vwrite_stderr_raw(msg.as_ptr() as *const c_char, msg.len() - 1);
            proc_exit(1);
        }
        istty = isatty(fd);
        close(fd);

        /* Redirect our stderr to the debug output file. */
        let mode = b"a\0";
        if freopen(OutputFileName.as_ptr(), mode.as_ptr() as *const c_char, stderr).is_null() {
            let msg = b"could not reopen file as stderr\0";
            vwrite_stderr_raw(msg.as_ptr() as *const c_char, msg.len() - 1);
            proc_exit(1);
        }

        /*
         * If the file is a tty and we're running under the postmaster, try to
         * send stdout there as well.
         */
        if istty != 0 && IsUnderPostmaster {
            if freopen(OutputFileName.as_ptr(), mode.as_ptr() as *const c_char, stdout).is_null() {
                let msg = b"could not reopen file as stdout\0";
                vwrite_stderr_raw(msg.as_ptr() as *const c_char, msg.len() - 1);
                proc_exit(1);
            }
        }
    }
}

/*
 * GUC check_hook for backtrace_functions
 */
pub unsafe fn check_backtrace_functions(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let newvallen = strlen(*newval) as usize;

    /*
     * Allow characters that can be C identifiers and commas as separators.
     */
    let valid_chars = b"0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ, \n\t";
    let valid_chars_str = core::str::from_utf8_unchecked(valid_chars);
    let raw = core::slice::from_raw_parts(*newval as *const u8, newvallen);
    for &byte in raw {
        if !valid_chars.contains(&byte) {
            // GUC_check_errdetail("Invalid character.")
            let msg = b"Invalid character.\0";
            errdetail_internal_c(msg.as_ptr() as *const c_char);
            return false;
        }
    }

    if **newval == 0 {
        *extra = null_mut();
        return true;
    }

    /*
     * Allocate space for the output and create the copy.
     */
    let someval = guc_malloc(LOG, newvallen + 2) as *mut u8;
    if someval.is_null() {
        return false;
    }

    let src = core::slice::from_raw_parts(*newval as *const u8, newvallen);
    let dst = core::slice::from_raw_parts_mut(someval, newvallen + 2);
    let mut j: usize = 0;
    for i in 0..newvallen {
        if src[i] == b',' {
            dst[j] = 0; /* next item */
            j += 1;
        } else if src[i] == b' ' || src[i] == b'\n' || src[i] == b'\t' {
            /* ignore these */
        } else {
            dst[j] = src[i]; /* copy anything else */
            j += 1;
        }
    }
    /* two \0s end the setting */
    dst[j] = 0;
    dst[j + 1] = 0;

    *extra = someval as *mut c_void;
    true
}

/*
 * GUC assign_hook for backtrace_functions
 */
pub unsafe fn assign_backtrace_functions(_newval: *const c_char, extra: *mut c_void) {
    backtrace_function_list = extra as *mut c_char;
}

/*
 * GUC check_hook for log_destination
 */
pub unsafe fn check_log_destination(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let rawstring: *mut c_char = pstrdup(*newval);
    let mut elemlist: *mut c_void = null_mut();

    /* Parse string into list of identifiers */
    if !SplitIdentifierString(rawstring, b',' as c_char, &mut elemlist) {
        /* syntax error in list */
        let msg = b"List syntax is invalid.\0";
        errdetail_internal_c(msg.as_ptr() as *const c_char);
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    // TODO(pg-port): iterate the List and check tokens with pg_strcasecmp.
    // pg_list iteration requires the real List API; for now accept anything.
    let mut newlogdest: c_int = LOG_DESTINATION_STDERR; /* default */
    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    let myextra = guc_malloc(LOG, size_of::<c_int>()) as *mut c_int;
    if myextra.is_null() {
        return false;
    }
    *myextra = newlogdest;
    *extra = myextra as *mut c_void;

    true
}

/*
 * GUC assign_hook for log_destination
 */
pub unsafe fn assign_log_destination(_newval: *const c_char, extra: *mut c_void) {
    Log_destination = *(extra as *const c_int);
}

// ---------------------------------------------------------------------------
// HAVE_SYSLOG support: libc syslog primitives, GUC-backed statics, and limit.
// These mirror the file-scope declarations guarded by #ifdef HAVE_SYSLOG in
// elog.c.  The whole syslog code path is compiled only under the
// `have_syslog` feature, matching the C build's HAVE_SYSLOG configuration.
// TODO(pg-port): wire syslog_ident/openlog_done to the real guc.c statics once
// the syslog GUCs land; for now they live here as in the C file.
// ---------------------------------------------------------------------------
#[cfg(feature = "have_syslog")]
const PG_SYSLOG_LIMIT: c_int = 1024;

#[cfg(feature = "have_syslog")]
static mut syslog_seq: c_ulong = 0;
#[cfg(feature = "have_syslog")]
static mut openlog_done: bool = false;
#[cfg(feature = "have_syslog")]
static mut syslog_ident: *mut c_char = null_mut();
#[cfg(feature = "have_syslog")]
pub static mut syslog_facility: c_int = LOG_LOCAL0;

// syslog priority levels and openlog option/facility bits (sys/syslog.h)
#[cfg(feature = "have_syslog")]
const LOG_PID: c_int = 0x01;
#[cfg(feature = "have_syslog")]
const LOG_NDELAY: c_int = 0x08;
#[cfg(feature = "have_syslog")]
const LOG_NOWAIT: c_int = 0x10;
#[cfg(feature = "have_syslog")]
const LOG_LOCAL0: c_int = 16 << 3;
#[cfg(feature = "have_syslog")]
pub const LOG_DEBUG: c_int = 7;
#[cfg(feature = "have_syslog")]
pub const LOG_INFO: c_int = 6;
#[cfg(feature = "have_syslog")]
pub const LOG_NOTICE: c_int = 5;
#[cfg(feature = "have_syslog")]
pub const LOG_WARNING: c_int = 4;
#[cfg(feature = "have_syslog")]
pub const LOG_ERR: c_int = 3;
#[cfg(feature = "have_syslog")]
pub const LOG_CRIT: c_int = 2;

#[cfg(feature = "have_syslog")]
extern "C" {
    fn openlog(ident: *const c_char, option: c_int, facility: c_int);
    fn closelog();
    fn syslog(priority: c_int, format: *const c_char, ...);
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strdup(s: *const c_char) -> *mut c_char;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn isspace(c: c_int) -> c_int;
}

/*
 * GUC assign_hook for syslog_ident
 */
pub unsafe fn assign_syslog_ident(newval: *const c_char, extra: *mut c_void) {
    #[cfg(feature = "have_syslog")]
    {
        /*
         * guc.c is likely to call us repeatedly with same parameters, so don't
         * thrash the syslog connection unnecessarily.  Also, we do not re-open
         * the connection until needed, since this routine will get called whether
         * or not Log_destination actually mentions syslog.
         *
         * Note that we make our own copy of the ident string rather than relying
         * on guc.c's.  This may be overly paranoid, but it ensures that we cannot
         * accidentally free a string that syslog is still using.
         */
        if syslog_ident.is_null() || strcmp(syslog_ident, newval) != 0 {
            if openlog_done {
                closelog();
                openlog_done = false;
            }
            free(syslog_ident as *mut c_void);
            syslog_ident = strdup(newval);
            /* if the strdup fails, we will cope in write_syslog() */
        }
    }
    /* Without syslog support, just ignore it */
}

/*
 * GUC assign_hook for syslog_facility
 */
pub unsafe fn assign_syslog_facility(newval: c_int, extra: *mut c_void) {
    #[cfg(feature = "have_syslog")]
    {
        /*
         * As above, don't thrash the syslog connection unnecessarily.
         */
        if syslog_facility != newval {
            if openlog_done {
                closelog();
                openlog_done = false;
            }
            syslog_facility = newval;
        }
    }
    /* Without syslog support, just ignore it */
}

/*
 * Write a message line to syslog
 */
#[cfg(feature = "have_syslog")]
unsafe fn write_syslog(level: c_int, mut line: *const c_char) {
    let mut len: c_int;
    let mut nlpos: *const c_char;

    /* Open syslog connection if not done yet */
    if !openlog_done {
        openlog(
            if !syslog_ident.is_null() {
                syslog_ident
            } else {
                b"postgres\0".as_ptr() as *const c_char
            },
            LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            syslog_facility,
        );
        openlog_done = true;
    }

    /*
     * We add a sequence number to each log message to suppress "same"
     * messages.
     */
    syslog_seq += 1;

    /*
     * Our problem here is that many syslog implementations don't handle long
     * messages in an acceptable manner. While this function doesn't help that
     * fact, it does work around by splitting up messages into smaller pieces.
     *
     * We divide into multiple syslog() calls if message is too long or if the
     * message contains embedded newline(s).
     */
    len = strlen(line) as c_int;
    nlpos = strchr(line, b'\n' as c_int);
    if syslog_split_messages && (len > PG_SYSLOG_LIMIT || !nlpos.is_null()) {
        let mut chunk_nr: c_int = 0;

        while len > 0 {
            let mut buf = [0 as c_char; PG_SYSLOG_LIMIT as usize + 1];
            let mut buflen: c_int;
            let mut i: c_int;

            /* if we start at a newline, move ahead one char */
            if *line == b'\n' as c_char {
                line = line.add(1);
                len -= 1;
                /* we need to recompute the next newline's position, too */
                nlpos = strchr(line, b'\n' as c_int);
                continue;
            }

            /* copy one line, or as much as will fit, to buf */
            if !nlpos.is_null() {
                buflen = nlpos.offset_from(line) as c_int;
            } else {
                buflen = len;
            }
            buflen = buflen.min(PG_SYSLOG_LIMIT);
            memcpy(buf.as_mut_ptr() as *mut c_void, line as *const c_void, buflen as usize);
            buf[buflen as usize] = b'\0' as c_char;

            /* trim to multibyte letter boundary */
            buflen = pg_mbcliplen(buf.as_ptr(), buflen, buflen);
            if buflen <= 0 {
                return;
            }
            buf[buflen as usize] = b'\0' as c_char;

            /* already word boundary? */
            if *line.add(buflen as usize) != b'\0' as c_char
                && isspace(*line.add(buflen as usize) as c_uchar as c_int) == 0
            {
                /* try to divide at word boundary */
                i = buflen - 1;
                while i > 0 && isspace(buf[i as usize] as c_uchar as c_int) == 0 {
                    i -= 1;
                }

                if i > 0 {
                    /* else couldn't divide word boundary */
                    buflen = i;
                    buf[i as usize] = b'\0' as c_char;
                }
            }

            chunk_nr += 1;

            if syslog_sequence_numbers {
                syslog(level, b"[%lu-%d] %s\0".as_ptr() as *const c_char, syslog_seq, chunk_nr, buf.as_ptr());
            } else {
                syslog(level, b"[%d] %s\0".as_ptr() as *const c_char, chunk_nr, buf.as_ptr());
            }

            line = line.add(buflen as usize);
            len -= buflen;
        }
    } else {
        /* message short enough */
        if syslog_sequence_numbers {
            syslog(level, b"[%lu] %s\0".as_ptr() as *const c_char, syslog_seq, line);
        } else {
            syslog(level, b"%s\0".as_ptr() as *const c_char, line);
        }
    }
}

// ---------------------------------------------------------------------------
// WIN32 event-log support.  Compiled only on Windows targets, matching the C
// file's #ifdef WIN32 guards.  The Win32 API surface (RegisterEventSource,
// ReportEventA/W, GetACP, ...) is not ported; declared here as TODO(pg-port)
// extern stubs so the event-log path resolves on a Windows build.
// ---------------------------------------------------------------------------
#[cfg(windows)]
type WCHAR = u16;
#[cfg(windows)]
type HANDLE = *mut c_void;
#[cfg(windows)]
type LPCWSTR = *const WCHAR;
#[cfg(windows)]
const INVALID_HANDLE_VALUE: HANDLE = !0usize as HANDLE;
#[cfg(windows)]
const EVENTLOG_ERROR_TYPE: c_int = 0x0001;
#[cfg(windows)]
const EVENTLOG_WARNING_TYPE: c_int = 0x0002;
#[cfg(windows)]
const EVENTLOG_INFORMATION_TYPE: c_int = 0x0004;
#[cfg(windows)]
extern "C" {
    static mut event_source: *mut c_char;
    fn GetACP() -> c_int;
    fn pg_codepage_to_encoding(cp: c_int) -> c_int;
    fn GetMessageEncoding() -> c_int;
    fn pgwin32_message_to_UTF16(str: *const c_char, len: c_int, encoding: *mut c_int) -> *mut WCHAR;
    fn RegisterEventSource(lpUNCServerName: *const c_char, lpSourceName: *const c_char) -> HANDLE;
    fn ReportEventA(
        hEventLog: HANDLE, wType: c_int, wCategory: c_int, dwEventID: c_int,
        lpUserSid: *mut c_void, wNumStrings: c_int, dwDataSize: c_int,
        lpStrings: *const *const c_char, lpRawData: *mut c_void,
    ) -> c_int;
    fn ReportEventW(
        hEventLog: HANDLE, wType: c_int, wCategory: c_int, dwEventID: c_int,
        lpUserSid: *mut c_void, wNumStrings: c_int, dwDataSize: c_int,
        lpStrings: *const LPCWSTR, lpRawData: *mut c_void,
    ) -> c_int;
}
#[cfg(windows)]
const DEFAULT_EVENT_SOURCE: *const c_char = b"PostgreSQL\0".as_ptr() as *const c_char;

#[cfg(windows)]
/*
 * Get the PostgreSQL equivalent of the Windows ANSI code page.  "ANSI" system
 * interfaces (e.g. CreateFileA()) expect string arguments in this encoding.
 * Every process in a given system will find the same value at all times.
 */
unsafe fn GetACPEncoding() -> c_int {
    static mut encoding: c_int = -2;

    if encoding == -2 {
        encoding = pg_codepage_to_encoding(GetACP());
    }

    encoding
}

#[cfg(windows)]
/*
 * Write a message line to the windows event log
 */
unsafe fn write_eventlog(level: c_int, line: *const c_char, len: c_int) {
    let utf16: *mut WCHAR;
    let mut eventlevel: c_int = EVENTLOG_ERROR_TYPE;
    static mut evtHandle: HANDLE = INVALID_HANDLE_VALUE;

    if evtHandle == INVALID_HANDLE_VALUE {
        evtHandle = RegisterEventSource(
            null_mut(),
            if !event_source.is_null() {
                event_source
            } else {
                DEFAULT_EVENT_SOURCE
            },
        );
        if evtHandle.is_null() {
            evtHandle = INVALID_HANDLE_VALUE;
            return;
        }
    }

    match level {
        DEBUG5 | DEBUG4 | DEBUG3 | DEBUG2 | DEBUG1 | LOG | LOG_SERVER_ONLY | INFO | NOTICE => {
            eventlevel = EVENTLOG_INFORMATION_TYPE;
        }
        WARNING | WARNING_CLIENT_ONLY => {
            eventlevel = EVENTLOG_WARNING_TYPE;
        }
        ERROR | FATAL | PANIC => {
            eventlevel = EVENTLOG_ERROR_TYPE;
        }
        _ => {
            eventlevel = EVENTLOG_ERROR_TYPE;
        }
    }

    /*
     * If message character encoding matches the encoding expected by
     * ReportEventA(), call it to avoid the hazards of conversion.  Otherwise,
     * try to convert the message to UTF16 and write it with ReportEventW().
     * Fall back on ReportEventA() if conversion failed.
     *
     * Since we palloc the structure required for conversion, also fall
     * through to writing unconverted if we have not yet set up
     * CurrentMemoryContext.
     *
     * Also verify that we are not on our way into error recursion trouble due
     * to error messages thrown deep inside pgwin32_message_to_UTF16().
     */
    if !in_error_recursion_trouble()
        && !crate::utils::palloc::CurrentMemoryContext.is_null()
        && GetMessageEncoding() != GetACPEncoding()
    {
        utf16 = pgwin32_message_to_UTF16(line, len, null_mut());
        if !utf16.is_null() {
            ReportEventW(
                evtHandle,
                eventlevel,
                0,
                0, /* All events are Id 0 */
                null_mut(),
                1,
                0,
                &utf16 as *const *mut WCHAR as *const LPCWSTR,
                null_mut(),
            );
            /* XXX Try ReportEventA() when ReportEventW() fails? */

            pfree(utf16 as *mut c_void);
            return;
        }
    }
    ReportEventA(
        evtHandle,
        eventlevel,
        0,
        0, /* All events are Id 0 */
        null_mut(),
        1,
        0,
        &line,
        null_mut(),
    );
}

/*
 * write_console --- write a message to stderr (or equivalent).
 *
 * WIN32 has a separate path involving WriteConsoleW(); on unix we just
 * write() to stderr.
 */
unsafe fn write_console(line: *const c_char, len: c_int) {
    let rc = write(
        STDERR_FILENO,
        line as *const c_void,
        len as usize,
    );
    let _ = rc; /* We ignore any error from write() here. */
}

// Small helper: write to stderr without going through StringInfo / palloc.
pub(crate) unsafe fn vwrite_stderr_raw(line: *const c_char, len: usize) {
    write(STDERR_FILENO, line as *const c_void, len);
}

/*
 * get_formatted_log_time -- compute and get the log timestamp.
 */
pub unsafe fn get_formatted_log_time() -> *mut c_char {
    /* leave if already computed */
    if formatted_log_time[0] != 0 {
        return formatted_log_time.as_mut_ptr();
    }

    if !saved_timeval_set {
        gettimeofday(&mut saved_timeval, null_mut());
        saved_timeval_set = true;
    }

    let stamp_time = saved_timeval.tv_sec as pg_time_t;

    /*
     * Note: we expect that guc.c will ensure that log_timezone is set up.
     */
    pg_strftime(
        formatted_log_time.as_mut_ptr(),
        FORMATTED_TS_LEN,
        b"%Y-%m-%d %H:%M:%S     %Z\0".as_ptr() as *const c_char,
        pg_localtime(&stamp_time as *const pg_time_t, log_timezone),
    );

    /* 'paste' milliseconds into place... */
    {
        let mut msbuf = [0u8; 13];
        snprintf(
            msbuf.as_mut_ptr() as *mut c_char,
            msbuf.len(),
            b".%03d\0".as_ptr() as *const c_char,
            (saved_timeval.tv_usec / 1000) as c_int,
        );
        core::ptr::copy_nonoverlapping(msbuf.as_ptr(), formatted_log_time.as_mut_ptr().add(19) as *mut u8, 4);
    }

    formatted_log_time.as_mut_ptr()
}

/*
 * reset_formatted_start_time -- reset the start timestamp
 */
pub unsafe fn reset_formatted_start_time() {
    formatted_start_time[0] = 0;
}

/*
 * get_formatted_start_time -- compute and get the start timestamp.
 */
pub unsafe fn get_formatted_start_time() -> *mut c_char {
    let stamp_time: pg_time_t = MyStartTime;

    /* leave if already computed */
    if formatted_start_time[0] != 0 {
        return formatted_start_time.as_mut_ptr();
    }

    pg_strftime(
        formatted_start_time.as_mut_ptr(),
        FORMATTED_TS_LEN,
        b"%Y-%m-%d %H:%M:%S %Z\0".as_ptr() as *const c_char,
        pg_localtime(&stamp_time as *const pg_time_t, log_timezone),
    );

    formatted_start_time.as_mut_ptr()
}

/*
 * check_log_of_query -- check if a query can be logged
 */
pub unsafe fn check_log_of_query(edata: *mut ErrorData) -> bool {
    /* log required? */
    if !is_log_level_output((*edata).elevel, log_min_error_statement) {
        return false;
    }

    /* query log wanted? */
    if (*edata).hide_stmt {
        return false;
    }

    /* query string available? */
    if debug_query_string.is_null() {
        return false;
    }

    true
}

/*
 * get_backend_type_for_log -- backend type for log entries
 *
 * Returns a pointer to a static buffer, not palloc'd.
 */
pub unsafe fn get_backend_type_for_log() -> *const c_char {
    if MyProcPid == PostmasterPid {
        b"postmaster\0".as_ptr() as *const c_char
    } else if MyBackendType == B_BG_WORKER {
        // (*MyBgworkerEntry).bgw_type
        // TODO(pg-port): return bgw_type when BackgroundWorker is fully ported
        b"bgworker\0".as_ptr() as *const c_char
    } else {
        GetBackendTypeDesc(MyBackendType)
    }
}

/*
 * process_log_prefix_padding --- helper function for log_line_prefix processing
 *
 * Note: Returns NULL if it finds something which it deems invalid.
 */
unsafe fn process_log_prefix_padding(p: *const c_char, ppadding: *mut c_int) -> *const c_char {
    let mut paddingsign: c_int = 1;
    let mut padding: c_int = 0;
    let mut p = p;

    if *p == b'-' as c_char {
        p = p.add(1);
        if *p == 0 {
            /* Did the buf end in %- ? */
            return null();
        }
        paddingsign = -1;
    }

    /* generate an int version of the numerical string */
    while *p >= b'0' as c_char && *p <= b'9' as c_char {
        padding = padding * 10 + (*p - b'0' as c_char) as c_int;
        p = p.add(1);
    }

    /* format is invalid if it ends with the padding number */
    if *p == 0 {
        return null();
    }

    padding *= paddingsign;
    *ppadding = padding;
    p
}

/*
 * append_padded_str --- mirror C printf "%*s": pad `s` to width abs(padding);
 * positive padding right-justifies, negative left-justifies (Rust has no
 * variadic printf, so the %*s width logic is expressed explicitly here).
 */
unsafe fn append_padded_str(buf: *mut StringInfoData, padding: c_int, s: &str) {
    let width = padding.unsigned_abs() as usize;
    let out = if padding > 0 {
        format!("{:>1$}", s, width)
    } else {
        format!("{:<1$}", s, width)
    };
    appendBinaryStringInfo(
        buf,
        out.as_ptr() as *const c_void,
        out.len() as c_int,
    );
}

/*
 * append_padded_cstr --- like append_padded_str but for a NUL-terminated C
 * string (the common "%*s" case in log_status_format).
 */
unsafe fn append_padded_cstr(buf: *mut StringInfoData, padding: c_int, s: *const c_char) {
    let cs = core::ffi::CStr::from_ptr(s).to_string_lossy();
    append_padded_str(buf, padding, &cs);
}

/*
 * log_line_prefix --- Format log status information using Log_line_prefix.
 */
unsafe fn log_line_prefix(buf: *mut StringInfoData, edata: *mut ErrorData) {
    log_status_format(buf, Log_line_prefix, edata);
}


// ===========================================================================
// Part 5: log_status_format (full %X switch), unpack_sql_state,
//         send_message_to_server_log, write_pipe_chunks,
//         err_sendstring, send_message_to_frontend,
//         error_severity, append_with_tabs,
//         write_stderr / vwrite_stderr,
//         ErrorContext stub, PG_RE_THROW macro
// ===========================================================================

/*
 * log_status_format --- Format log status info; append to the provided buffer.
 */
pub unsafe fn log_status_format(
    buf: *mut StringInfoData,
    format: *const c_char,
    edata: *mut ErrorData,
) {
    /* static counter for line numbers */
    static mut log_line_number: i64 = 0;
    /* has counter been reset in current process? */
    static mut log_my_pid: c_int = 0;
    let mut padding: c_int = 0;
    let mut p: *const c_char;

    /*
     * This is one of the few places where we'd rather not inherit a static
     * variable's value from the postmaster.
     */
    if log_my_pid != MyProcPid {
        log_line_number = 0;
        log_my_pid = MyProcPid;
        reset_formatted_start_time();
    }
    log_line_number += 1;

    if format.is_null() {
        return; /* in case guc hasn't run yet */
    }

    p = format;
    while *p != 0 {
        if *p != b'%' as c_char {
            /* literal char, just copy */
            appendStringInfoChar(buf, *p);
            p = p.add(1);
            continue;
        }

        /* must be a '%', so skip to the next char */
        p = p.add(1);
        if *p == 0 {
            break; /* format error - ignore it */
        } else if *p == b'%' as c_char {
            /* string contains %% */
            appendStringInfoChar(buf, b'%' as c_char);
            p = p.add(1);
            continue;
        }

        /*
         * Process any formatting which may exist after the '%'.
         */
        if *p > b'9' as c_char {
            padding = 0;
        } else {
            let np = process_log_prefix_padding(p, &mut padding);
            if np.is_null() {
                break;
            }
            p = np;
        }

        /* process the option */
        match *p as u8 {
            b'a' => {
                // application name
                let appname = if !application_name.is_null() {
                    application_name as *const c_char
                } else {
                    b"[unknown]\0".as_ptr() as *const c_char
                };
                if padding != 0 {
                    append_padded_cstr(buf, padding, appname);
                } else {
                    appendStringInfoString(buf, appname);
                }
            }
            b'b' => {
                let backend_type_str = get_backend_type_for_log();
                if padding != 0 {
                    append_padded_cstr(buf, padding, backend_type_str);
                } else {
                    appendStringInfoString(buf, backend_type_str);
                }
            }
            b'u' => {
                // user name
                if !MyProcPort.is_null() {
                    let username: *const c_char = (*(MyProcPort as *const Port)).user_name;
                    let username = if username.is_null() || *username == 0 {
                        b"[unknown]\0".as_ptr() as *const c_char
                    } else {
                        username
                    };
                    if padding != 0 {
                        append_padded_cstr(buf, padding, username);
                    } else {
                        appendStringInfoString(buf, username);
                    }
                } else if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'd' => {
                // database name
                if !MyProcPort.is_null() {
                    let dbname: *const c_char = (*(MyProcPort as *const Port)).database_name;
                    let dbname = if dbname.is_null() || *dbname == 0 {
                        b"[unknown]\0".as_ptr() as *const c_char
                    } else {
                        dbname
                    };
                    if padding != 0 {
                        append_padded_cstr(buf, padding, dbname);
                    } else {
                        appendStringInfoString(buf, dbname);
                    }
                } else if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'c' => {
                // session id: MyStartTime.MyProcPid (hex)
                if padding != 0 {
                    append_padded_str(buf, padding, &format!("{:x}.{:x}", MyStartTime as u64, MyProcPid as u32));
                } else {
                    appendStringInfo!(buf, "{:x}.{:x}", MyStartTime as u64, MyProcPid as u32);
                }
            }
            b'p' => {
                // process id
                if padding != 0 {
                    append_padded_str(buf, padding, &format!("{}", MyProcPid));
                } else {
                    appendStringInfo!(buf, "{}", MyProcPid);
                }
            }
            b'P' => {
                // parallel leader pid (TODO: stub -- MyProc is opaque *mut c_void here)
                if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'l' => {
                // log line number
                if padding != 0 {
                    append_padded_str(buf, padding, &format!("{}", log_line_number));
                } else {
                    appendStringInfo!(buf, "{}", log_line_number);
                }
            }
            b'm' => {
                // log timestamp with milliseconds
                formatted_log_time[0] = 0;
                get_formatted_log_time();
                if padding != 0 {
                    append_padded_cstr(buf, padding, formatted_log_time.as_ptr());
                } else {
                    appendStringInfoString(buf, formatted_log_time.as_ptr());
                }
            }
            b't' => {
                // timestamp without milliseconds
                let stamp_time = time(null_mut()) as pg_time_t;
                let mut strfbuf = [0i8; 128];
                pg_strftime(
                    strfbuf.as_mut_ptr(),
                    strfbuf.len(),
                    b"%Y-%m-%d %H:%M:%S %Z\0".as_ptr() as *const c_char,
                    pg_localtime(&stamp_time as *const pg_time_t, log_timezone),
                );
                if padding != 0 {
                    append_padded_cstr(buf, padding, strfbuf.as_ptr());
                } else {
                    appendStringInfoString(buf, strfbuf.as_ptr());
                }
            }
            b'n' => {
                // epoch with milliseconds
                if !saved_timeval_set {
                    gettimeofday(&mut saved_timeval, null_mut());
                    saved_timeval_set = true;
                }
                let mut strfbuf = [0i8; 128];
                snprintf(
                    strfbuf.as_mut_ptr(),
                    strfbuf.len(),
                    b"%ld.%03d\0".as_ptr() as *const c_char,
                    saved_timeval.tv_sec as i64,
                    (saved_timeval.tv_usec / 1000) as c_int,
                );
                if padding != 0 {
                    append_padded_cstr(buf, padding, strfbuf.as_ptr());
                } else {
                    appendStringInfoString(buf, strfbuf.as_ptr());
                }
            }
            b's' => {
                // process start timestamp
                let start_time = get_formatted_start_time();
                if padding != 0 {
                    append_padded_cstr(buf, padding, start_time);
                } else {
                    appendStringInfoString(buf, start_time);
                }
            }
            b'i' => {
                // command tag
                if !MyProcPort.is_null() {
                    let mut displen: c_int = 0;
                    let psdisp = get_ps_display(&mut displen);
                    if padding != 0 {
                        append_padded_cstr(buf, padding, psdisp);
                    } else {
                        appendBinaryStringInfo(buf, psdisp as *const c_void, displen);
                    }
                } else if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'L' => {
                // local host and port
                let local_host: *const c_char = if !MyProcPort.is_null() {
                    // TODO(pg-port): full getnameinfo path; for now return empty string
                    b"\0".as_ptr() as *const c_char
                } else {
                    b"[none]\0".as_ptr() as *const c_char
                };
                if padding != 0 {
                    append_padded_cstr(buf, padding, local_host);
                } else {
                    appendStringInfoString(buf, local_host);
                }
            }
            b'r' => {
                // remote host (and port)
                if !MyProcPort.is_null() {
                    let port = &*(MyProcPort as *const Port);
                    if !port.remote_host.is_null() {
                        if padding != 0 {
                            append_padded_cstr(buf, padding, port.remote_host);
                        } else {
                            appendStringInfoString(buf, port.remote_host);
                            if !port.remote_port.is_null() && *port.remote_port != 0 {
                                appendStringInfo!(buf, "({})", core::ffi::CStr::from_ptr(port.remote_port).to_string_lossy());
                            }
                        }
                    } else if padding != 0 {
                        appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                    }
                } else if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'h' => {
                // remote host only
                if !MyProcPort.is_null() {
                    let port = &*(MyProcPort as *const Port);
                    if !port.remote_host.is_null() {
                        if padding != 0 {
                            append_padded_cstr(buf, padding, port.remote_host);
                        } else {
                            appendStringInfoString(buf, port.remote_host);
                        }
                    } else if padding != 0 {
                        appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                    }
                } else if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'q' => {
                /* in postmaster and friends, stop if %q is seen */
                if MyProcPort.is_null() {
                    return;
                }
            }
            b'v' => {
                // virtual transaction ID
                // TODO(pg-port): MyProc is opaque; skip vxid formatting
                if padding != 0 {
                    appendStringInfoSpaces(buf, if padding > 0 { padding } else { -padding });
                }
            }
            b'x' => {
                // transaction id
                if padding != 0 {
                    append_padded_str(buf, padding, &format!("{}", GetTopTransactionIdIfAny()));
                } else {
                    appendStringInfo!(buf, "{}", GetTopTransactionIdIfAny());
                }
            }
            b'e' => {
                // SQLSTATE error code
                let sqlstate = unpack_sql_state((*edata).sqlerrcode);
                if padding != 0 {
                    append_padded_cstr(buf, padding, sqlstate);
                } else {
                    appendStringInfoString(buf, sqlstate);
                }
            }
            b'Q' => {
                // query id
                let qid = pgstat_get_my_query_id();
                if padding != 0 {
                    append_padded_str(buf, padding, &format!("{}", qid));
                } else {
                    appendStringInfo!(buf, "{}", qid);
                }
            }
            _ => {
                /* format error - ignore it */
            }
        }
        p = p.add(1);
    }
}

/*
 * unpack_sql_state -- Unpack MAKE_SQLSTATE code.
 * Note that this returns a pointer to a static buffer.
 */
pub unsafe fn unpack_sql_state(mut sql_state: c_int) -> *const c_char {
    static mut buf: [u8; 12] = [0; 12];
    let mut i = 0usize;
    while i < 5 {
        buf[i] = PGUNSIXBIT(sql_state);
        sql_state >>= 6;
        i += 1;
    }
    buf[i] = 0;
    buf.as_ptr() as *const c_char
}

/*
 * send_message_to_server_log
 *
 * Write error report to server's log.
 */
unsafe fn send_message_to_server_log(edata: *mut ErrorData) {
    let mut buf = StringInfoData { data: null_mut(), len: 0, maxlen: 0, cursor: 0 };
    let mut fallback_to_stderr: bool = false;

    initStringInfo(&mut buf);

    log_line_prefix(&mut buf, edata);
    let sev = error_severity((*edata).elevel);
    // append "SEVERITY:  "
    appendStringInfoString(&mut buf, sev);
    appendStringInfoString(&mut buf, b":  \0".as_ptr() as *const c_char);

    if Log_error_verbosity >= PGERROR_VERBOSE {
        appendStringInfoString(&mut buf, unpack_sql_state((*edata).sqlerrcode));
        appendStringInfoString(&mut buf, b": \0".as_ptr() as *const c_char);
    }

    if !(*edata).message.is_null() {
        append_with_tabs(&mut buf, (*edata).message);
    } else {
        append_with_tabs(&mut buf, b"missing error text\0".as_ptr() as *const c_char);
    }

    if (*edata).cursorpos > 0 {
        appendStringInfo!(&mut buf, " at character {}", (*edata).cursorpos);
    } else if (*edata).internalpos > 0 {
        appendStringInfo!(&mut buf, " at character {}", (*edata).internalpos);
    }

    appendStringInfoChar(&mut buf, b'\n' as c_char);

    if Log_error_verbosity >= PGERROR_DEFAULT {
        if !(*edata).detail_log.is_null() {
            log_line_prefix(&mut buf, edata);
            appendStringInfoString(&mut buf, b"DETAIL:  \0".as_ptr() as *const c_char);
            append_with_tabs(&mut buf, (*edata).detail_log);
            appendStringInfoChar(&mut buf, b'\n' as c_char);
        } else if !(*edata).detail.is_null() {
            log_line_prefix(&mut buf, edata);
            appendStringInfoString(&mut buf, b"DETAIL:  \0".as_ptr() as *const c_char);
            append_with_tabs(&mut buf, (*edata).detail);
            appendStringInfoChar(&mut buf, b'\n' as c_char);
        }
        if !(*edata).hint.is_null() {
            log_line_prefix(&mut buf, edata);
            appendStringInfoString(&mut buf, b"HINT:  \0".as_ptr() as *const c_char);
            append_with_tabs(&mut buf, (*edata).hint);
            appendStringInfoChar(&mut buf, b'\n' as c_char);
        }
        if !(*edata).internalquery.is_null() {
            log_line_prefix(&mut buf, edata);
            appendStringInfoString(&mut buf, b"QUERY:  \0".as_ptr() as *const c_char);
            append_with_tabs(&mut buf, (*edata).internalquery);
            appendStringInfoChar(&mut buf, b'\n' as c_char);
        }
        if !(*edata).context.is_null() && !(*edata).hide_ctx {
            log_line_prefix(&mut buf, edata);
            appendStringInfoString(&mut buf, b"CONTEXT:  \0".as_ptr() as *const c_char);
            append_with_tabs(&mut buf, (*edata).context);
            appendStringInfoChar(&mut buf, b'\n' as c_char);
        }
        if Log_error_verbosity >= PGERROR_VERBOSE {
            /* assume no newlines in funcname or filename... */
            if !(*edata).funcname.is_null() && !(*edata).filename.is_null() {
                log_line_prefix(&mut buf, edata);
                appendStringInfo!(
                    &mut buf,
                    "LOCATION:  {}, {}:{}\n",
                    core::ffi::CStr::from_ptr((*edata).funcname).to_string_lossy(),
                    core::ffi::CStr::from_ptr((*edata).filename).to_string_lossy(),
                    (*edata).lineno
                );
            } else if !(*edata).filename.is_null() {
                log_line_prefix(&mut buf, edata);
                appendStringInfo!(
                    &mut buf,
                    "LOCATION:  {}:{}\n",
                    core::ffi::CStr::from_ptr((*edata).filename).to_string_lossy(),
                    (*edata).lineno
                );
            }
        }
        if !(*edata).backtrace.is_null() {
            log_line_prefix(&mut buf, edata);
            appendStringInfoString(&mut buf, b"BACKTRACE:  \0".as_ptr() as *const c_char);
            append_with_tabs(&mut buf, (*edata).backtrace);
            appendStringInfoChar(&mut buf, b'\n' as c_char);
        }
    }

    /* If the user wants the query that generated this error logged, do it. */
    if check_log_of_query(edata) {
        log_line_prefix(&mut buf, edata);
        appendStringInfoString(&mut buf, b"STATEMENT:  \0".as_ptr() as *const c_char);
        append_with_tabs(&mut buf, debug_query_string);
        appendStringInfoChar(&mut buf, b'\n' as c_char);
    }

    #[cfg(feature = "have_syslog")]
    {
        /* Write to syslog, if enabled */
        if Log_destination & LOG_DESTINATION_SYSLOG != 0 {
            let syslog_level: c_int;

            syslog_level = match (*edata).elevel {
                DEBUG5 | DEBUG4 | DEBUG3 | DEBUG2 | DEBUG1 => LOG_DEBUG,
                LOG | LOG_SERVER_ONLY | INFO => LOG_INFO,
                NOTICE | WARNING | WARNING_CLIENT_ONLY => LOG_NOTICE,
                ERROR => LOG_WARNING,
                FATAL => LOG_ERR,
                PANIC => LOG_CRIT,
                _ => LOG_CRIT,
            };

            write_syslog(syslog_level, buf.data);
        }
    }

    #[cfg(windows)]
    {
        /* Write to eventlog, if enabled */
        if Log_destination & LOG_DESTINATION_EVENTLOG != 0 {
            write_eventlog((*edata).elevel, buf.data, buf.len);
        }
    }

    /* Write to csvlog, if enabled */
    if Log_destination & LOG_DESTINATION_CSVLOG != 0 {
        if redirection_done || MyBackendType == B_LOGGER {
            write_csvlog(edata as *mut crate::utils::error::csvlog::ErrorData);
        } else {
            fallback_to_stderr = true;
        }
    }

    /* Write to JSON log, if enabled */
    if Log_destination & LOG_DESTINATION_JSONLOG != 0 {
        if redirection_done || MyBackendType == B_LOGGER {
            write_jsonlog(edata as *mut crate::utils::error::jsonlog::ErrorData);
        } else {
            fallback_to_stderr = true;
        }
    }

    /*
     * Write to stderr, if enabled or if required because of a previous
     * limitation.
     */
    if (Log_destination & LOG_DESTINATION_STDERR) != 0
        || whereToSendOutput == DestDebug
        || fallback_to_stderr
    {
        if redirection_done && MyBackendType != B_LOGGER {
            write_pipe_chunks(buf.data, buf.len, LOG_DESTINATION_STDERR);
        } else {
            write_console(buf.data, buf.len);
        }
    }

    /* If in the syslogger process, try to write messages direct to file */
    if MyBackendType == B_LOGGER {
        write_syslogger_file(buf.data, buf.len, LOG_DESTINATION_STDERR);
    }

    /* No more need of the message formatted for stderr */
    pfree(buf.data as *mut c_void);
}

/*
 * write_pipe_chunks
 *
 * Send data to the syslogger using the chunked protocol.
 */
pub unsafe fn write_pipe_chunks(data: *mut c_char, len: c_int, dest: c_int) {
    let fd = STDERR_FILENO;
    let mut rc: c_int;
    let mut data = data;
    let mut len = len as usize;

    debug_assert!(len > 0);

    let flags_dest: u8 = if dest == LOG_DESTINATION_STDERR {
        PIPE_PROTO_DEST_STDERR
    } else if dest == LOG_DESTINATION_CSVLOG {
        PIPE_PROTO_DEST_CSVLOG
    } else if dest == LOG_DESTINATION_JSONLOG {
        PIPE_PROTO_DEST_JSONLOG
    } else {
        0
    };

    /* write all but the last chunk */
    while len > PIPE_MAX_PAYLOAD {
        let mut chunk = [0u8; PIPE_HEADER_SIZE + PIPE_MAX_PAYLOAD];
        // nuls[0..2] = 0 (already zero)
        let plen = PIPE_MAX_PAYLOAD as u16;
        chunk[2] = (plen & 0xff) as u8;
        chunk[3] = (plen >> 8) as u8;
        chunk[4..8].copy_from_slice(&(MyProcPid as i32).to_ne_bytes());
        chunk[8] = flags_dest; /* no PIPE_PROTO_IS_LAST */
        core::ptr::copy_nonoverlapping(
            data as *const u8,
            chunk.as_mut_ptr().add(PIPE_HEADER_SIZE),
            PIPE_MAX_PAYLOAD,
        );
        rc = write(fd, chunk.as_ptr() as *const c_void, chunk.len()) as c_int;
        let _ = rc;
        data = data.add(PIPE_MAX_PAYLOAD);
        len -= PIPE_MAX_PAYLOAD;
    }

    /* write the last chunk */
    let total = PIPE_HEADER_SIZE + len;
    let mut chunk = vec![0u8; total];
    let plen = len as u16;
    chunk[2] = (plen & 0xff) as u8;
    chunk[3] = (plen >> 8) as u8;
    chunk[4..8].copy_from_slice(&(MyProcPid as i32).to_ne_bytes());
    chunk[8] = flags_dest | PIPE_PROTO_IS_LAST;
    core::ptr::copy_nonoverlapping(
        data as *const u8,
        chunk.as_mut_ptr().add(PIPE_HEADER_SIZE),
        len,
    );
    rc = write(fd, chunk.as_ptr() as *const c_void, total) as c_int;
    let _ = rc;
}

/*
 * err_sendstring -- append a text string to the error report for the client
 */
unsafe fn err_sendstring(buf: *mut StringInfoData, str: *const c_char) {
    if in_error_recursion_trouble() {
        pq_send_ascii_string(buf, str);
    } else {
        pq_sendstring(buf, str);
    }
}

/*
 * send_message_to_frontend
 *
 * Write error report to client.
 */
unsafe fn send_message_to_frontend(edata: *mut ErrorData) {
    let mut msgbuf = StringInfoData { data: null_mut(), len: 0, maxlen: 0, cursor: 0 };

    if PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3 || FrontendProtocol == 0 {
        /* New style with separate fields */
        let sev = error_severity((*edata).elevel);
        let mut tbuf = [0i8; 12];

        /* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
        if (*edata).elevel < ERROR {
            pq_beginmessage(&mut msgbuf, PqMsg_NoticeResponse as c_char);
        } else {
            pq_beginmessage(&mut msgbuf, PqMsg_ErrorResponse as c_char);
        }

        pq_sendbyte(&mut msgbuf, PG_DIAG_SEVERITY as c_char);
        err_sendstring(&mut msgbuf, sev);
        pq_sendbyte(&mut msgbuf, PG_DIAG_SEVERITY_NONLOCALIZED as c_char);
        err_sendstring(&mut msgbuf, sev);

        pq_sendbyte(&mut msgbuf, PG_DIAG_SQLSTATE as c_char);
        err_sendstring(&mut msgbuf, unpack_sql_state((*edata).sqlerrcode));

        /* M field is required per protocol, so always send something */
        pq_sendbyte(&mut msgbuf, PG_DIAG_MESSAGE_PRIMARY as c_char);
        if !(*edata).message.is_null() {
            err_sendstring(&mut msgbuf, (*edata).message);
        } else {
            err_sendstring(&mut msgbuf, b"missing error text\0".as_ptr() as *const c_char);
        }

        if !(*edata).detail.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_MESSAGE_DETAIL as c_char);
            err_sendstring(&mut msgbuf, (*edata).detail);
        }

        /* detail_log is intentionally not used here */

        if !(*edata).hint.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_MESSAGE_HINT as c_char);
            err_sendstring(&mut msgbuf, (*edata).hint);
        }

        if !(*edata).context.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_CONTEXT as c_char);
            err_sendstring(&mut msgbuf, (*edata).context);
        }

        if !(*edata).schema_name.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_SCHEMA_NAME as c_char);
            err_sendstring(&mut msgbuf, (*edata).schema_name);
        }

        if !(*edata).table_name.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_TABLE_NAME as c_char);
            err_sendstring(&mut msgbuf, (*edata).table_name);
        }

        if !(*edata).column_name.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_COLUMN_NAME as c_char);
            err_sendstring(&mut msgbuf, (*edata).column_name);
        }

        if !(*edata).datatype_name.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_DATATYPE_NAME as c_char);
            err_sendstring(&mut msgbuf, (*edata).datatype_name);
        }

        if !(*edata).constraint_name.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_CONSTRAINT_NAME as c_char);
            err_sendstring(&mut msgbuf, (*edata).constraint_name);
        }

        if (*edata).cursorpos > 0 {
            snprintf(
                tbuf.as_mut_ptr(),
                tbuf.len(),
                b"%d\0".as_ptr() as *const c_char,
                (*edata).cursorpos,
            );
            pq_sendbyte(&mut msgbuf, PG_DIAG_STATEMENT_POSITION as c_char);
            err_sendstring(&mut msgbuf, tbuf.as_ptr());
        }

        if (*edata).internalpos > 0 {
            snprintf(
                tbuf.as_mut_ptr(),
                tbuf.len(),
                b"%d\0".as_ptr() as *const c_char,
                (*edata).internalpos,
            );
            pq_sendbyte(&mut msgbuf, PG_DIAG_INTERNAL_POSITION as c_char);
            err_sendstring(&mut msgbuf, tbuf.as_ptr());
        }

        if !(*edata).internalquery.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_INTERNAL_QUERY as c_char);
            err_sendstring(&mut msgbuf, (*edata).internalquery);
        }

        if !(*edata).filename.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_SOURCE_FILE as c_char);
            err_sendstring(&mut msgbuf, (*edata).filename);
        }

        if (*edata).lineno > 0 {
            snprintf(
                tbuf.as_mut_ptr(),
                tbuf.len(),
                b"%d\0".as_ptr() as *const c_char,
                (*edata).lineno,
            );
            pq_sendbyte(&mut msgbuf, PG_DIAG_SOURCE_LINE as c_char);
            err_sendstring(&mut msgbuf, tbuf.as_ptr());
        }

        if !(*edata).funcname.is_null() {
            pq_sendbyte(&mut msgbuf, PG_DIAG_SOURCE_FUNCTION as c_char);
            err_sendstring(&mut msgbuf, (*edata).funcname);
        }

        pq_sendbyte(&mut msgbuf, 0); /* terminator */

        pq_endmessage(&mut msgbuf);
    } else {
        /* Old style --- gin up a backwards-compatible message */
        let mut buf2 = StringInfoData { data: null_mut(), len: 0, maxlen: 0, cursor: 0 };

        initStringInfo(&mut buf2);

        let sev = error_severity((*edata).elevel);
        appendStringInfoString(&mut buf2, sev);
        appendStringInfoString(&mut buf2, b":  \0".as_ptr() as *const c_char);

        if !(*edata).message.is_null() {
            appendStringInfoString(&mut buf2, (*edata).message);
        } else {
            appendStringInfoString(&mut buf2, b"missing error text\0".as_ptr() as *const c_char);
        }

        appendStringInfoChar(&mut buf2, b'\n' as c_char);

        /* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
        pq_putmessage_v2(
            if (*edata).elevel < ERROR { b'N' as c_char } else { b'E' as c_char },
            buf2.data,
            (buf2.len + 1) as Size,
        );

        pfree(buf2.data as *mut c_void);
    }

    /*
     * This flush is normally not necessary, since postgres.c will flush out
     * waiting data when control returns to the main loop. But it seems best
     * to leave it here.
     */
    pq_flush();
}

/*
 * error_severity --- get string representing elevel
 */
pub unsafe fn error_severity(elevel: c_int) -> *const c_char {
    let prefix: *const c_char = match elevel {
        e if e == DEBUG1 || e == DEBUG2 || e == DEBUG3 || e == DEBUG4 || e == DEBUG5 => {
            b"DEBUG\0".as_ptr() as *const c_char
        }
        e if e == LOG || e == LOG_SERVER_ONLY => {
            b"LOG\0".as_ptr() as *const c_char
        }
        e if e == INFO => {
            b"INFO\0".as_ptr() as *const c_char
        }
        e if e == NOTICE => {
            b"NOTICE\0".as_ptr() as *const c_char
        }
        e if e == WARNING || e == WARNING_CLIENT_ONLY => {
            b"WARNING\0".as_ptr() as *const c_char
        }
        e if e == ERROR => {
            b"ERROR\0".as_ptr() as *const c_char
        }
        e if e == FATAL => {
            b"FATAL\0".as_ptr() as *const c_char
        }
        e if e == PANIC => {
            b"PANIC\0".as_ptr() as *const c_char
        }
        _ => {
            b"???\0".as_ptr() as *const c_char
        }
    };
    prefix
}

/*
 * append_with_tabs
 *
 * Append the string to the StringInfo buffer, inserting a tab after any
 * newline.
 */
unsafe fn append_with_tabs(buf: *mut StringInfoData, str: *const c_char) {
    let mut s = str;
    loop {
        let ch = *s;
        if ch == 0 {
            break;
        }
        appendStringInfoCharMacro!(buf, ch);
        if ch == b'\n' as c_char {
            appendStringInfoCharMacro!(buf, b'\t' as c_char);
        }
        s = s.add(1);
    }
}

/*
 * write_stderr
 *
 * Write errors to stderr (or by equal means when stderr is not available).
 * Used before ereport/elog can be used safely.
 */
pub unsafe fn write_stderr(fmt: *const c_char) {
    vwrite_stderr(fmt);
}

/*
 * vwrite_stderr --- va_list version of write_stderr
 *
 * In this port we do not support printf-style expansion here; the caller
 * should pre-format into a C string.  We simply write the bytes to stderr.
 */
pub unsafe fn vwrite_stderr(fmt: *const c_char) {
    if fmt.is_null() {
        return;
    }
    /* On Unix, we just write to stderr */
    let len = strlen(fmt);
    write(STDERR_FILENO, fmt as *const c_void, len);
    fsync(STDERR_FILENO); /* best-effort flush */
}

// ---------------------------------------------------------------------------
// ErrorContext stub
//
// In a full port ErrorContext is a MemoryContext created in mcxt.c.
// Here we provide a module-level static placeholder so that callers
// of MemoryContextSwitchTo(ErrorContext) compile without external linkage.
//
// TODO(pg-port): replace with the real ErrorContext from mcxt.c once ported.
// ---------------------------------------------------------------------------
pub static mut ErrorContext: MemoryContext = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// PG_RE_THROW convenience macro (for C callers that use PG_RE_THROW())
// ---------------------------------------------------------------------------
/// PG_RE_THROW -- Rust-side analog of the C macro.
#[macro_export]
macro_rules! PG_RE_THROW {
    () => {
        // SAFETY: caller ensures we are inside a PG_TRY context.
        unsafe { $crate::utils::error::elog_impl::pg_re_throw() }
    };
}

