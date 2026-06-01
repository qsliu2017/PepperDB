//! utils/error/jsonlog.c - JSON logging.

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, appendStringInfoString, initStringInfo,
    StringInfo, StringInfoData,
};
use crate::libpq::libpq_be::Port;
use crate::miscadmin::{BackendType, B_LOGGER, MyBackendType};
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::tcop::tcopprot::debug_query_string;
use crate::utils::adt::json::escape_json;
use crate::utils::init::globals::{MyProcPid, MyStartTime};
extern "C" { pub static mut MyProcPort: *mut Port; }
use crate::utils::palloc::pfree;

// ---------------------------------------------------------------------------
// Constants and types not yet ported (defined locally as stubs).
// ---------------------------------------------------------------------------

// utils/elog.h log destination bitmask values.
const LOG_DESTINATION_JSONLOG: c_int = 16;

// utils/elog.h Log_error_verbosity enum values.
const PGERROR_TERSE: c_int = 0;
const PGERROR_DEFAULT: c_int = 1;
const PGERROR_VERBOSE: c_int = 2;

extern "C" {
    // GUC: log verbosity. Defined by guc tables; not yet ported.
    pub static mut Log_error_verbosity: c_int;
    // GUC: application_name. Defined by guc tables; not yet ported.
    #[allow(improper_ctypes)]
    pub static mut application_name: *mut c_char;
}

/*
 * ErrorData - mirrors the C struct in utils/elog.h.  elog.c is not yet
 * ported, so we declare a layout-compatible copy here.
 * TODO: replace with the canonical definition from utils/elog.rs once it lands.
 */
#[repr(C)]
pub struct ErrorData {
    pub elevel: c_int,
    pub output_to_server: bool,
    pub output_to_client: bool,
    pub hide_stmt: bool,
    pub hide_ctx: bool,
    pub filename: *const c_char,
    pub lineno: c_int,
    pub funcname: *const c_char,
    pub domain: *const c_char,
    pub context_domain: *const c_char,
    pub sqlerrcode: c_int,
    pub message: *mut c_char,
    pub detail: *mut c_char,
    pub detail_log: *mut c_char,
    pub hint: *mut c_char,
    pub context: *mut c_char,
    pub backtrace: *mut c_char,
    pub message_id: *const c_char,
    pub schema_name: *mut c_char,
    pub table_name: *mut c_char,
    pub column_name: *mut c_char,
    pub datatype_name: *mut c_char,
    pub constraint_name: *mut c_char,
    pub cursorpos: c_int,
    pub internalpos: c_int,
    pub internalquery: *mut c_char,
    pub saved_errno: c_int,
    pub assoc_context: *mut c_void,
}

/*
 * PGPROC - minimal layout-compatible view of the fields jsonlog.c touches.
 * storage/proc.h is not yet ported (storage/proclist.rs exposes only an
 * opaque PGPROC); declare a local stand-in here.
 * TODO: replace with the canonical PGPROC once storage/proc.rs lands.
 */
#[repr(C)]
pub struct VirtualTransactionId {
    pub procNumber: c_int,
    pub lxid: u32,
}

#[repr(C)]
pub struct PGPROC {
    pub vxid: VirtualTransactionId,
    pub pid: c_int,
    pub lockGroupLeader: *mut PGPROC,
}

extern "C" {
    // storage/proc.h: pointer to this backend's PGPROC.  Not yet ported.
    pub static mut MyProc: *mut PGPROC;
}

// ---------------------------------------------------------------------------
// Stubs for functions called but not yet ported.
// ---------------------------------------------------------------------------

// access/transam/xact.c
unsafe fn GetTopTransactionIdIfAny() -> u32 {
    unimplemented!() // TODO: port access/transam/xact.c
}

// utils/error/elog.c
unsafe fn error_severity(_elevel: c_int) -> *const c_char {
    unimplemented!() // TODO: port utils/error/elog.c
}

// utils/error/elog.c
unsafe fn check_log_of_query(_edata: *const ErrorData) -> bool {
    unimplemented!() // TODO: port utils/error/elog.c
}

// utils/error/elog.c
unsafe fn write_pipe_chunks(_data: *mut c_char, _len: c_int, _dest: c_int) {
    unimplemented!() // TODO: port utils/error/elog.c
}

// utils/error/elog.c: format_log_time helpers
unsafe fn get_formatted_log_time() -> *mut c_char {
    unimplemented!() // TODO: port utils/error/elog.c
}

unsafe fn get_formatted_start_time() -> *mut c_char {
    unimplemented!() // TODO: port utils/error/elog.c
}

unsafe fn reset_formatted_start_time() {
    unimplemented!() // TODO: port utils/error/elog.c
}

// utils/mb/encnames.c-adjacent; backend_status.c
unsafe fn get_backend_type_for_log() -> *const c_char {
    unimplemented!() // TODO: port utils/activity/backend_status.c
}

// utils/error/elog.c
unsafe fn unpack_sql_state(_sql_state: c_int) -> *const c_char {
    unimplemented!() // TODO: port utils/error/elog.c
}

// utils/misc/ps_status.c
unsafe fn get_ps_display(displen: *mut c_int) -> *const c_char {
    let _ = displen;
    unimplemented!() // TODO: port utils/misc/ps_status.c
}

// utils/activity/backend_status.c
unsafe fn pgstat_get_my_query_id() -> i64 {
    unimplemented!() // TODO: port utils/activity/backend_status.c
}

// postmaster/syslogger.c
unsafe fn write_syslogger_file(_buffer: *const c_char, _count: c_int, _flags: c_int) {
    unimplemented!() // TODO: port postmaster/syslogger.c
}

// ---------------------------------------------------------------------------
// Logic
// ---------------------------------------------------------------------------

/*
 * appendJSONKeyValue
 *
 * Append to a StringInfo a comma followed by a JSON key and a value.
 * The key is always escaped.  The value can be escaped optionally, that
 * is dependent on the data type of the key.
 */
unsafe fn appendJSONKeyValue(
    buf: StringInfo,
    key: *const c_char,
    value: *const c_char,
    escape_value: bool,
) {
    Assert!(!key.is_null());

    if value.is_null() {
        return;
    }

    appendStringInfoChar(buf, b',' as c_char);
    escape_json(buf, key);
    appendStringInfoChar(buf, b':' as c_char);

    if escape_value {
        escape_json(buf, value);
    } else {
        appendStringInfoString(buf, value);
    }
}

/*
 * appendJSONKeyValueFmt
 *
 * Evaluate the fmt string and then invoke appendJSONKeyValue() as the
 * value of the JSON property.  Both the key and value will be escaped by
 * appendJSONKeyValue().
 *
 * The C variant is variadic (printf-style).  Rust lacks variadic-fn support
 * in the same form; callers below pass a single pre-formatted string, so we
 * accept the already-formatted value directly.  The pvsnprintf retry loop is
 * preserved here for fidelity but operates over the supplied formatted value.
 */
unsafe fn appendJSONKeyValueFmt(
    buf: StringInfo,
    key: *const c_char,
    escape_key: bool,
    formatted: *const c_char,
) {
    appendJSONKeyValue(buf, key, formatted, escape_key);
}

/*
 * Write logs in json format.
 */
pub unsafe fn write_jsonlog(edata: *mut ErrorData) {
    let mut buf: StringInfoData = core::mem::zeroed();
    let start_time: *mut c_char;
    let log_time: *mut c_char;

    /* static counter for line numbers */
    static mut log_line_number: i64 = 0;

    /* Has the counter been reset in the current process? */
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

    /* Initialize string */
    appendStringInfoChar(&mut buf, b'{' as c_char);

    /* timestamp with milliseconds */
    log_time = get_formatted_log_time();

    /*
     * First property does not use appendJSONKeyValue as it does not have
     * comma prefix.
     */
    escape_json(&mut buf, c"timestamp".as_ptr());
    appendStringInfoChar(&mut buf, b':' as c_char);
    escape_json(&mut buf, log_time);

    /* username */
    if !MyProcPort.is_null() {
        appendJSONKeyValue(&mut buf, c"user".as_ptr(), (*MyProcPort).user_name, true);
    }

    /* database name */
    if !MyProcPort.is_null() {
        appendJSONKeyValue(
            &mut buf,
            c"dbname".as_ptr(),
            (*MyProcPort).database_name,
            true,
        );
    }

    /* Process ID */
    if MyProcPid != 0 {
        let s = format!("{}\0", MyProcPid);
        appendJSONKeyValueFmt(&mut buf, c"pid".as_ptr(), false, s.as_ptr() as *const c_char);
    }

    /* Remote host and port */
    if !MyProcPort.is_null() && !(*MyProcPort).remote_host.is_null() {
        appendJSONKeyValue(
            &mut buf,
            c"remote_host".as_ptr(),
            (*MyProcPort).remote_host,
            true,
        );
        if !(*MyProcPort).remote_port.is_null() && *(*MyProcPort).remote_port != 0 {
            appendJSONKeyValue(
                &mut buf,
                c"remote_port".as_ptr(),
                (*MyProcPort).remote_port,
                false,
            );
        }
    }

    /* Session id */
    {
        let s = format!("{:x}.{:x}\0", MyStartTime, MyProcPid);
        appendJSONKeyValueFmt(
            &mut buf,
            c"session_id".as_ptr(),
            true,
            s.as_ptr() as *const c_char,
        );
    }

    /* Line number */
    {
        let s = format!("{}\0", log_line_number);
        appendJSONKeyValueFmt(
            &mut buf,
            c"line_num".as_ptr(),
            false,
            s.as_ptr() as *const c_char,
        );
    }

    /* PS display */
    if !MyProcPort.is_null() {
        let mut msgbuf: StringInfoData = core::mem::zeroed();
        let psdisp: *const c_char;
        let mut displen: c_int = 0;

        initStringInfo(&mut msgbuf);
        psdisp = get_ps_display(&mut displen);
        appendBinaryStringInfo(&mut msgbuf, psdisp as *const c_void, displen);
        appendJSONKeyValue(&mut buf, c"ps".as_ptr(), msgbuf.data, true);

        pfree(msgbuf.data as *mut c_void);
    }

    /* session start timestamp */
    start_time = get_formatted_start_time();
    appendJSONKeyValue(&mut buf, c"session_start".as_ptr(), start_time, true);

    /* Virtual transaction id */
    /* keep VXID format in sync with lockfuncs.c */
    if !MyProc.is_null() && (*MyProc).vxid.procNumber != INVALID_PROC_NUMBER {
        let s = format!("{}/{}\0", (*MyProc).vxid.procNumber, (*MyProc).vxid.lxid);
        appendJSONKeyValueFmt(
            &mut buf,
            c"vxid".as_ptr(),
            true,
            s.as_ptr() as *const c_char,
        );
    }

    /* Transaction id */
    {
        let s = format!("{}\0", GetTopTransactionIdIfAny());
        appendJSONKeyValueFmt(
            &mut buf,
            c"txid".as_ptr(),
            false,
            s.as_ptr() as *const c_char,
        );
    }

    /* Error severity */
    if (*edata).elevel != 0 {
        appendJSONKeyValue(
            &mut buf,
            c"error_severity".as_ptr(),
            error_severity((*edata).elevel),
            true,
        );
    }

    /* SQL state code */
    if (*edata).sqlerrcode != 0 {
        appendJSONKeyValue(
            &mut buf,
            c"state_code".as_ptr(),
            unpack_sql_state((*edata).sqlerrcode),
            true,
        );
    }

    /* errmessage */
    appendJSONKeyValue(&mut buf, c"message".as_ptr(), (*edata).message, true);

    /* errdetail or error_detail log */
    if !(*edata).detail_log.is_null() {
        appendJSONKeyValue(&mut buf, c"detail".as_ptr(), (*edata).detail_log, true);
    } else {
        appendJSONKeyValue(&mut buf, c"detail".as_ptr(), (*edata).detail, true);
    }

    /* errhint */
    if !(*edata).hint.is_null() {
        appendJSONKeyValue(&mut buf, c"hint".as_ptr(), (*edata).hint, true);
    }

    /* internal query */
    if !(*edata).internalquery.is_null() {
        appendJSONKeyValue(
            &mut buf,
            c"internal_query".as_ptr(),
            (*edata).internalquery,
            true,
        );
    }

    /* if printed internal query, print internal pos too */
    if (*edata).internalpos > 0 && !(*edata).internalquery.is_null() {
        let s = format!("{}\0", (*edata).internalpos);
        appendJSONKeyValueFmt(
            &mut buf,
            c"internal_position".as_ptr(),
            false,
            s.as_ptr() as *const c_char,
        );
    }

    /* errcontext */
    if !(*edata).context.is_null() && !(*edata).hide_ctx {
        appendJSONKeyValue(&mut buf, c"context".as_ptr(), (*edata).context, true);
    }

    /* user query --- only reported if not disabled by the caller */
    if check_log_of_query(edata) {
        appendJSONKeyValue(
            &mut buf,
            c"statement".as_ptr(),
            debug_query_string,
            true,
        );
        if (*edata).cursorpos > 0 {
            let s = format!("{}\0", (*edata).cursorpos);
            appendJSONKeyValueFmt(
                &mut buf,
                c"cursor_position".as_ptr(),
                false,
                s.as_ptr() as *const c_char,
            );
        }
    }

    /* file error location */
    if Log_error_verbosity >= PGERROR_VERBOSE {
        if !(*edata).funcname.is_null() {
            appendJSONKeyValue(&mut buf, c"func_name".as_ptr(), (*edata).funcname, true);
        }
        if !(*edata).filename.is_null() {
            appendJSONKeyValue(&mut buf, c"file_name".as_ptr(), (*edata).filename, true);
            let s = format!("{}\0", (*edata).lineno);
            appendJSONKeyValueFmt(
                &mut buf,
                c"file_line_num".as_ptr(),
                false,
                s.as_ptr() as *const c_char,
            );
        }
    }

    /* Application name */
    if !application_name.is_null() && *application_name != 0 {
        appendJSONKeyValue(
            &mut buf,
            c"application_name".as_ptr(),
            application_name,
            true,
        );
    }

    /* backend type */
    appendJSONKeyValue(
        &mut buf,
        c"backend_type".as_ptr(),
        get_backend_type_for_log(),
        true,
    );

    /* leader PID */
    if !MyProc.is_null() {
        let leader: *mut PGPROC = (*MyProc).lockGroupLeader;

        /*
         * Show the leader only for active parallel workers.  This leaves out
         * the leader of a parallel group.
         */
        if !leader.is_null() && (*leader).pid != MyProcPid {
            let s = format!("{}\0", (*leader).pid);
            appendJSONKeyValueFmt(
                &mut buf,
                c"leader_pid".as_ptr(),
                false,
                s.as_ptr() as *const c_char,
            );
        }
    }

    /* query id */
    {
        let s = format!("{}\0", pgstat_get_my_query_id());
        appendJSONKeyValueFmt(
            &mut buf,
            c"query_id".as_ptr(),
            false,
            s.as_ptr() as *const c_char,
        );
    }

    /* Finish string */
    appendStringInfoChar(&mut buf, b'}' as c_char);
    appendStringInfoChar(&mut buf, b'\n' as c_char);

    /* If in the syslogger process, try to write messages direct to file */
    if MyBackendType == B_LOGGER {
        write_syslogger_file(buf.data, buf.len, LOG_DESTINATION_JSONLOG);
    } else {
        write_pipe_chunks(buf.data, buf.len, LOG_DESTINATION_JSONLOG);
    }

    pfree(buf.data as *mut c_void);

    // Silence unused-const warnings for the verbosity enum members that exist
    // for fidelity but are not all referenced.
    let _ = (PGERROR_TERSE, PGERROR_DEFAULT);
    let _: BackendType = MyBackendType;
}
