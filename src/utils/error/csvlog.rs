//! utils/error/csvlog.c - CSV logging.

use crate::prelude::*;
use crate::appendStringInfoCharMacro;
use crate::appendStringInfo;

use crate::lib::stringinfo::{
    StringInfo, StringInfoData, initStringInfo, appendBinaryStringInfo, appendStringInfoChar,
    appendStringInfoString,
};
use crate::libpq::libpq_be::Port;
use crate::miscadmin::{MyProcPid, MyStartTime, MyBackendType, B_LOGGER};

use core::ffi::{c_char, c_int};

// ---------------------------------------------------------------------------
// Externs / dependencies not yet ported. These live in elog.c, ps_status.c,
// syslogger.c, libpq-be, proc.h, backend_status.c, xact.c, guc.c.
// TODO: replace stubs once those units are translated.
// ---------------------------------------------------------------------------

// ErrorData is defined in elog.h / elog.c (not yet ported). Only the fields used
// by write_csvlog are modeled here; layout will be reconciled when elog.c lands.
// TODO: replace with the real ErrorData from utils::elog once ported.
#[repr(C)]
pub struct ErrorData {
    pub elevel: c_int,
    pub sqlerrcode: c_int,
    pub message: *mut c_char,
    pub detail: *mut c_char,
    pub detail_log: *mut c_char,
    pub hint: *mut c_char,
    pub context: *mut c_char,
    pub hide_ctx: bool,
    pub internalquery: *mut c_char,
    pub internalpos: c_int,
    pub cursorpos: c_int,
    pub funcname: *const c_char,
    pub filename: *const c_char,
    pub lineno: c_int,
}

// PGPROC subset used here (storage/proc.h). proclist.rs has a PGPROC but with a
// different shape; declare the fields referenced by write_csvlog.
// TODO: unify with the real PGPROC.
#[repr(C)]
pub struct VirtualXactId {
    pub procNumber: c_int,
    pub lxid: u32,
}

#[repr(C)]
pub struct PGPROC {
    pub vxid: VirtualXactId,
    pub pid: c_int,
    pub lockGroupLeader: *mut PGPROC,
}

pub const INVALID_PROC_NUMBER: c_int = -1;

// Log destination bit for CSV (utils/elog.h). syslogger destination flags.
pub const LOG_DESTINATION_CSVLOG: c_int = 0x04;

// Log_error_verbosity values (utils/elog.h).
pub const PGERROR_TERSE: c_int = 0;
pub const PGERROR_DEFAULT: c_int = 1;
pub const PGERROR_VERBOSE: c_int = 2;

#[allow(improper_ctypes)]
extern "C" {
    // miscadmin.h / globals
    pub static mut MyProcPort: *mut Port;
    pub static mut MyProc: *mut PGPROC;
    // guc.c
    pub static mut Log_error_verbosity: c_int;
    pub static mut application_name: *mut c_char;
    // tcop/postgres.c
    pub static mut debug_query_string: *const c_char;
}

// ---- Stubs for not-yet-ported callees. ----

// elog.c
unsafe fn error_severity(_elevel: c_int) -> *const c_char {
    // TODO: port error_severity from elog.c
    b"\0".as_ptr() as *const c_char
}

unsafe fn unpack_sql_state(_sql_state: c_int) -> *const c_char {
    // TODO: port unpack_sql_state from elog.c
    b"\0".as_ptr() as *const c_char
}

unsafe fn check_log_of_query(_edata: *mut ErrorData) -> bool {
    // TODO: port check_log_of_query from elog.c
    false
}

unsafe fn get_formatted_log_time() -> *const c_char {
    // TODO: port get_formatted_log_time from elog.c
    b"\0".as_ptr() as *const c_char
}

unsafe fn get_formatted_start_time() -> *const c_char {
    // TODO: port get_formatted_start_time from elog.c
    b"\0".as_ptr() as *const c_char
}

unsafe fn reset_formatted_start_time() {
    // TODO: port reset_formatted_start_time from elog.c
}

unsafe fn get_backend_type_for_log() -> *const c_char {
    // TODO: port get_backend_type_for_log from elog.c
    b"\0".as_ptr() as *const c_char
}

// ps_status.c
unsafe fn get_ps_display(displen: *mut c_int) -> *const c_char {
    // TODO: port get_ps_display from ps_status.c
    *displen = 0;
    b"\0".as_ptr() as *const c_char
}

// syslogger.c
unsafe fn write_syslogger_file(_buffer: *const c_char, _count: c_int, _destination: c_int) {
    // TODO: port write_syslogger_file from syslogger.c
}

unsafe fn write_pipe_chunks(_data: *mut c_char, _len: c_int, _dest: c_int) {
    // TODO: port write_pipe_chunks from elog.c/syslogger.c
}

// xact.c
unsafe fn GetTopTransactionIdIfAny() -> u32 {
    // TODO: port GetTopTransactionIdIfAny from xact.c
    0
}

// backend_status.c
unsafe fn pgstat_get_my_query_id() -> i64 {
    // TODO: port pgstat_get_my_query_id from backend_status.c
    0
}

// guc / NLS translation: gettext_() is a no-op passthrough in this port.
#[inline]
unsafe fn gettext_(s: *const c_char) -> *const c_char {
    s
}

/*
 * append a CSV'd version of a string to a StringInfo
 * We use the PostgreSQL defaults for CSV, i.e. quote = escape = '"'
 * If it's NULL, append nothing.
 */
#[inline]
unsafe fn appendCSVLiteral(buf: StringInfo, data: *const c_char) {
    let mut p = data;
    let mut c: c_char;

    /* avoid confusing an empty string with NULL */
    if p.is_null() {
        return;
    }

    appendStringInfoCharMacro!(buf, b'"' as c_char);
    loop {
        c = *p;
        p = p.add(1);
        if c == 0 {
            break;
        }
        if c == b'"' as c_char {
            appendStringInfoCharMacro!(buf, b'"' as c_char);
        }
        appendStringInfoCharMacro!(buf, c);
    }
    appendStringInfoCharMacro!(buf, b'"' as c_char);
}

/*
 * write_csvlog -- Generate and write CSV log entry
 *
 * Constructs the error message, depending on the Errordata it gets, in a CSV
 * format which is described in doc/src/sgml/config.sgml.
 */
pub unsafe fn write_csvlog(edata: *mut ErrorData) {
    let mut buf: StringInfoData = core::mem::zeroed();
    let print_stmt: bool;

    /* static counter for line numbers */
    static mut log_line_number: i64 = 0;

    /* has counter been reset in current process? */
    static mut log_my_pid: c_int = 0;

    /*
     * This is one of the few places where we'd rather not inherit a static
     * variable's value from the postmaster.  But since we will, reset it when
     * MyProcPid changes.
     */
    if log_my_pid != MyProcPid {
        log_line_number = 0;
        log_my_pid = MyProcPid;
        reset_formatted_start_time();
    }
    log_line_number += 1;

    initStringInfo(&mut buf);

    /* timestamp with milliseconds */
    appendStringInfoString(&mut buf, get_formatted_log_time());
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* username */
    if !MyProcPort.is_null() {
        appendCSVLiteral(&mut buf, (*MyProcPort).user_name);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* database name */
    if !MyProcPort.is_null() {
        appendCSVLiteral(&mut buf, (*MyProcPort).database_name);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* Process id  */
    if MyProcPid != 0 {
        appendStringInfo!(&mut buf, "{}", MyProcPid);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* Remote host and port */
    if !MyProcPort.is_null() && !(*MyProcPort).remote_host.is_null() {
        appendStringInfoChar(&mut buf, b'"' as c_char);
        appendStringInfoString(&mut buf, (*MyProcPort).remote_host);
        if !(*MyProcPort).remote_port.is_null() && *(*MyProcPort).remote_port != 0 {
            appendStringInfoChar(&mut buf, b':' as c_char);
            appendStringInfoString(&mut buf, (*MyProcPort).remote_port);
        }
        appendStringInfoChar(&mut buf, b'"' as c_char);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* session id */
    appendStringInfo!(&mut buf, "{:x}.{:x}", MyStartTime, MyProcPid);
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* Line number */
    appendStringInfo!(&mut buf, "{}", log_line_number);
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* PS display */
    if !MyProcPort.is_null() {
        let mut msgbuf: StringInfoData = core::mem::zeroed();
        let psdisp: *const c_char;
        let mut displen: c_int = 0;

        initStringInfo(&mut msgbuf);

        psdisp = get_ps_display(&mut displen);
        appendBinaryStringInfo(&mut msgbuf, psdisp as *const c_void, displen);
        appendCSVLiteral(&mut buf, msgbuf.data);

        pfree(msgbuf.data as *mut c_void);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* session start timestamp */
    appendStringInfoString(&mut buf, get_formatted_start_time());
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* Virtual transaction id */
    /* keep VXID format in sync with lockfuncs.c */
    if !MyProc.is_null() && (*MyProc).vxid.procNumber != INVALID_PROC_NUMBER {
        appendStringInfo!(&mut buf, "{}/{}", (*MyProc).vxid.procNumber, (*MyProc).vxid.lxid);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* Transaction id */
    appendStringInfo!(&mut buf, "{}", GetTopTransactionIdIfAny());
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* Error severity */
    appendStringInfoString(&mut buf, gettext_(error_severity((*edata).elevel)));
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* SQL state code */
    appendStringInfoString(&mut buf, unpack_sql_state((*edata).sqlerrcode));
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* errmessage */
    appendCSVLiteral(&mut buf, (*edata).message);
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* errdetail or errdetail_log */
    if !(*edata).detail_log.is_null() {
        appendCSVLiteral(&mut buf, (*edata).detail_log);
    } else {
        appendCSVLiteral(&mut buf, (*edata).detail);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* errhint */
    appendCSVLiteral(&mut buf, (*edata).hint);
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* internal query */
    appendCSVLiteral(&mut buf, (*edata).internalquery);
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* if printed internal query, print internal pos too */
    if (*edata).internalpos > 0 && !(*edata).internalquery.is_null() {
        appendStringInfo!(&mut buf, "{}", (*edata).internalpos);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* errcontext */
    if !(*edata).hide_ctx {
        appendCSVLiteral(&mut buf, (*edata).context);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* user query --- only reported if not disabled by the caller */
    print_stmt = check_log_of_query(edata);
    if print_stmt {
        appendCSVLiteral(&mut buf, debug_query_string);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);
    if print_stmt && (*edata).cursorpos > 0 {
        appendStringInfo!(&mut buf, "{}", (*edata).cursorpos);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* file error location */
    if Log_error_verbosity >= PGERROR_VERBOSE {
        let mut msgbuf: StringInfoData = core::mem::zeroed();

        initStringInfo(&mut msgbuf);

        if !(*edata).funcname.is_null() && !(*edata).filename.is_null() {
            appendStringInfo!(
                &mut msgbuf,
                "{}, {}:{}",
                cstr(((*edata).funcname)),
                cstr(((*edata).filename)),
                (*edata).lineno
            );
        } else if !(*edata).filename.is_null() {
            appendStringInfo!(
                &mut msgbuf,
                "{}:{}",
                cstr(((*edata).filename)),
                (*edata).lineno
            );
        }
        appendCSVLiteral(&mut buf, msgbuf.data);
        pfree(msgbuf.data as *mut c_void);
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* application name */
    if !application_name.is_null() {
        appendCSVLiteral(&mut buf, application_name);
    }

    appendStringInfoChar(&mut buf, b',' as c_char);

    /* backend type */
    appendCSVLiteral(&mut buf, get_backend_type_for_log());
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* leader PID */
    if !MyProc.is_null() {
        let leader: *mut PGPROC = (*MyProc).lockGroupLeader;

        /*
         * Show the leader only for active parallel workers.  This leaves out
         * the leader of a parallel group.
         */
        if !leader.is_null() && (*leader).pid != MyProcPid {
            appendStringInfo!(&mut buf, "{}", (*leader).pid);
        }
    }
    appendStringInfoChar(&mut buf, b',' as c_char);

    /* query id */
    appendStringInfo!(&mut buf, "{}", pgstat_get_my_query_id());

    appendStringInfoChar(&mut buf, b'\n' as c_char);

    /* If in the syslogger process, try to write messages direct to file */
    if MyBackendType == B_LOGGER {
        write_syslogger_file(buf.data, buf.len, LOG_DESTINATION_CSVLOG);
    } else {
        write_pipe_chunks(buf.data, buf.len, LOG_DESTINATION_CSVLOG);
    }

    pfree(buf.data as *mut c_void);
}

/// Helper: render a NUL-terminated C string for `%s` substitution in the
/// printf-style locations above (the C code passes raw `char *` to `%s`).
#[inline]
unsafe fn cstr(p: *const c_char) -> &'static str {
    if p.is_null() {
        return "";
    }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}
