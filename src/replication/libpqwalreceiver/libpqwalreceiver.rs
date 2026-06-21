/*-------------------------------------------------------------------------
 *
 * libpqwalreceiver.rs
 *
 * This file contains the libpq-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * libpq.
 *
 * Apart from walreceiver, the libpq-specific routines are now being used by
 * logical replication workers and slot synchronization.
 *
 * Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *    src/backend/replication/libpqwalreceiver/libpqwalreceiver.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;
use crate::appendStringInfo;

use core::ffi::{c_char, c_int, c_void};
use core::ptr::null_mut;

use crate::access::transam::xlogdefs::{XLogRecPtr, LSN_FORMAT_ARGS};
use crate::access::transam::xlogbackup::TimeLineID;
use crate::lib::stringinfo::{
    StringInfoData, initStringInfo, appendStringInfoString, appendStringInfoChar,
    appendBinaryStringInfo,
};
use crate::libpq::libpq_be_fe_helpers::{
    PGconn, PGresult, libpqsrv_connect_params, libpqsrv_disconnect,
    libpqsrv_exec, libpqsrv_get_result,
};
use crate::utils::builtins::{pg_strtoint32, quote_identifier};
use crate::utils::mmgr::mcxt::{palloc, palloc0, pfree, pstrdup, pchomp};
use crate::utils::memutils::{
    MemoryContextDelete, MemoryContextReset, ALLOCSET_DEFAULT_SIZES,
};
use crate::postgres_ext::{Oid, InvalidOid};
use crate::nodes::pg_list::List;
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, MyDatabaseId, work_mem};
use crate::mb::pg_wchar::GetDatabaseEncodingName;
use crate::common::connect::ALWAYS_SECURE_SEARCH_PATH_SQL;
use crate::c::{uint32, Size};
use crate::postgres::Datum;

// ---------------------------------------------------------------------------
// libpq client API (interfaces/libpq)
//
// PGconn / PGresult are opaque handles; they are already declared as c_void
// aliases in crate::libpq::libpq_be_fe_helpers.  All PQ* entry points below
// are the *client-side* libpq functions that live outside the backend proper.
// ---------------------------------------------------------------------------
extern "C" {
    // libpq client API (interfaces/libpq)

    fn PQstatus(conn: *mut PGconn) -> c_int;
    fn PQconnectionUsedPassword(conn: *const PGconn) -> c_int;
    fn PQerrorMessage(conn: *const PGconn) -> *const c_char;
    fn PQresultStatus(res: *const PGresult) -> c_int;
    fn PQclear(res: *mut PGresult);
    fn PQfreemem(ptr: *mut c_void);
    fn PQconninfoParse(conninfo: *const c_char, errmsg: *mut *mut c_char) -> *mut PQconninfoOption;
    fn PQconninfoFree(connOptions: *mut PQconninfoOption);
    fn PQconninfo(conn: *mut PGconn) -> *mut PQconninfoOption;
    fn PQhost(conn: *const PGconn) -> *const c_char;
    fn PQport(conn: *const PGconn) -> *const c_char;
    fn PQserverVersion(conn: *const PGconn) -> c_int;
    fn PQnfields(res: *const PGresult) -> c_int;
    fn PQntuples(res: *const PGresult) -> c_int;
    fn PQgetvalue(res: *const PGresult, tup_num: c_int, field_num: c_int) -> *mut c_char;
    fn PQgetlength(res: *const PGresult, tup_num: c_int, field_num: c_int) -> c_int;
    fn PQgetisnull(res: *const PGresult, tup_num: c_int, field_num: c_int) -> c_int;
    fn PQfname(res: *const PGresult, field_num: c_int) -> *const c_char;
    fn PQresultErrorField(res: *const PGresult, fieldcode: c_int) -> *const c_char;
    fn PQputCopyEnd(conn: *mut PGconn, errormsg: *const c_char) -> c_int;
    fn PQputCopyData(conn: *mut PGconn, buffer: *const c_char, nbytes: c_int) -> c_int;
    fn PQgetCopyData(conn: *mut PGconn, buffer: *mut *mut c_char, async_: c_int) -> c_int;
    fn PQflush(conn: *mut PGconn) -> c_int;
    fn PQconsumeInput(conn: *mut PGconn) -> c_int;
    fn PQsocket(conn: *const PGconn) -> c_int;
    fn PQbackendPID(conn: *const PGconn) -> c_int;
    fn PQescapeLiteral(conn: *mut PGconn, str_: *const c_char, len: usize) -> *mut c_char;
    fn PQescapeIdentifier(conn: *mut PGconn, str_: *const c_char, len: usize) -> *mut c_char;
    fn PQendcopy(conn: *mut PGconn) -> c_int;
}

// ---------------------------------------------------------------------------
// libpq ExecStatusType constants (libpq-fe.h)
// ---------------------------------------------------------------------------
const CONNECTION_OK: c_int = 0;
const PGRES_COMMAND_OK: c_int = 1;
const PGRES_TUPLES_OK: c_int = 2;
const PGRES_COPY_OUT: c_int = 3;
const PGRES_COPY_IN: c_int = 4;
const PGRES_COPY_BOTH: c_int = 8;
const PGRES_SINGLE_TUPLE: c_int = 9;
const PGRES_EMPTY_QUERY: c_int = 0;  // PGRES_EMPTY_QUERY = 0 in libpq-fe.h
const PGRES_NONFATAL_ERROR: c_int = 6;
const PGRES_FATAL_ERROR: c_int = 7;
const PGRES_BAD_RESPONSE: c_int = 5;
const PGRES_TUPLES_CHUNK: c_int = 10;
const PGRES_PIPELINE_SYNC: c_int = 11;
const PGRES_PIPELINE_ABORTED: c_int = 12;

// PG_DIAG_SQLSTATE from postgres_ext.h
const PG_DIAG_SQLSTATE: c_int = b'C' as c_int;

// ---------------------------------------------------------------------------
// PQconninfoOption - libpq client API (interfaces/libpq)
// ---------------------------------------------------------------------------
/// PQconninfoOption: one option from PQconninfoParse / PQconninfo.
#[repr(C)]
pub struct PQconninfoOption {
    /// The keyword of the option
    pub keyword: *mut c_char,
    /// Fallback environment variable name
    pub envvar: *mut c_char,
    /// Fallback compiled-in default value
    pub compiled: *mut c_char,
    /// Option's current value, or NULL
    pub val: *mut c_char,
    /// Label for field in connect dialog
    pub label: *mut c_char,
    /// Indicates how to display this field in a connect dialog.
    /// Values are: "" Display entered value as is
    ///             "*" Password field - hide value
    ///             "D"  Debug option - don't show by default
    pub dispchar: *mut c_char,
    /// Field size in characters for dialog
    pub dispsize: c_int,
}

// ---------------------------------------------------------------------------
// PQExpBuffer - libpq client API (interfaces/libpq)
// ---------------------------------------------------------------------------
/// Minimal PQExpBufferData for obfuscated conninfo building.
#[repr(C)]
pub struct PQExpBufferData {
    pub data: *mut c_char,
    pub len: usize,
    pub maxlen: usize,
}

// TODO(pg-port): real initPQExpBuffer/appendPQExpBuffer/termPQExpBuffer/PQExpBufferDataBroken
// live in interfaces/libpq/pqexpbuffer.c
unsafe fn initPQExpBuffer(buf: *mut PQExpBufferData) {
    unimplemented!() // TODO(pg-port): real initPQExpBuffer lives in interfaces/libpq/pqexpbuffer.c
}
unsafe fn appendPQExpBuffer(buf: *mut PQExpBufferData, fmt: *const c_char, kw: *const c_char, val: *const c_char) {
    unimplemented!() // TODO(pg-port): real appendPQExpBuffer lives in interfaces/libpq/pqexpbuffer.c
}
unsafe fn termPQExpBuffer(buf: *mut PQExpBufferData) {
    unimplemented!() // TODO(pg-port): real termPQExpBuffer lives in interfaces/libpq/pqexpbuffer.c
}
unsafe fn PQExpBufferDataBroken(buf: PQExpBufferData) -> bool {
    unimplemented!() // TODO(pg-port): real PQExpBufferDataBroken lives in interfaces/libpq/pqexpbuffer.c
}

// ---------------------------------------------------------------------------
// MAKE_SQLSTATE - utils/elog.h
// ---------------------------------------------------------------------------
/// MAKE_SQLSTATE: pack five SQL-state chars into a single int.
/// Mirrors the macro in src/include/utils/errcodes.h.
#[inline]
fn MAKE_SQLSTATE(c1: u8, c2: u8, c3: u8, c4: u8, c5: u8) -> c_int {
    // TODO(pg-port): real MAKE_SQLSTATE lives in utils/errcodes.h (generated).
    // Encoding: each char occupies 6 bits; packed into 30 bits.
    let encode = |c: u8| -> c_int { (c - b'0') as c_int };
    encode(c1)
        | (encode(c2) << 6)
        | (encode(c3) << 12)
        | (encode(c4) << 18)
        | (encode(c5) << 24)
}

// ---------------------------------------------------------------------------
// psprintf - utils/mmgr/mcxt.c
// ---------------------------------------------------------------------------
/// psprintf: printf into a palloc'd buffer. For the two-arg pattern used here.
// TODO(pg-port): real psprintf lives in utils/mmgr/mcxt.c (varargs C function).
// Concrete helper for the specific call patterns in this file.
unsafe fn psprintf_2(fmt_prefix: &str, arg: *const c_char) -> *mut c_char {
    let arg_str = if arg.is_null() {
        String::new()
    } else {
        core::ffi::CStr::from_ptr(arg).to_string_lossy().into_owned()
    };
    let result = format!("{}{}", fmt_prefix, arg_str);
    let bytes = result.as_bytes();
    let out = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, out, bytes.len());
    *out.add(bytes.len()) = 0;
    out
}

unsafe fn psprintf_opt_str(fixed: &str, opt: *const c_char, suffix: &str) -> *mut c_char {
    let opt_str = if opt.is_null() {
        String::new()
    } else {
        core::ffi::CStr::from_ptr(opt).to_string_lossy().into_owned()
    };
    let result = format!("{}{}{}", opt_str, fixed, suffix);
    let bytes = result.as_bytes();
    let out = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, out, bytes.len());
    *out.add(bytes.len()) = 0;
    out
}

// ---------------------------------------------------------------------------
// Wait event constants
// ---------------------------------------------------------------------------
// TODO(pg-port): real wait-event constants live in generated pgstat.h /
// wait_event_types.h (src/backend/utils/activity/).
const WAIT_EVENT_LIBPQWALRECEIVER_CONNECT: uint32 = 0;
const WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE: uint32 = 0;

// ---------------------------------------------------------------------------
// ERRCODE stubs
// ---------------------------------------------------------------------------
// TODO(pg-port): real ERRCODE_* live in utils/errcodes.h (generated).
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ERRCODE_OUT_OF_MEMORY: c_int = 0;
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0;
const ERRCODE_CONNECTION_FAILURE: c_int = 0;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED: c_int = 0;

// ---------------------------------------------------------------------------
// pgsocket
// ---------------------------------------------------------------------------
pub type pgsocket = c_int;

// ---------------------------------------------------------------------------
// HeapTuple / AttInMetadata / TupleDesc / Tuplestorestate stubs
// ---------------------------------------------------------------------------
// TODO(pg-port): real types live in access/htup_details.h, funcapi.h,
// utils/tuplestore.h.
pub type HeapTuple = *mut c_void;
pub type TupleDesc = *mut c_void;
pub type Tuplestorestate = c_void;
pub type AttInMetadata = c_void;
pub type AttrNumber = i16;

unsafe fn tuplestore_begin_heap(
    _randomaccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    unimplemented!() // TODO(pg-port): real tuplestore_begin_heap lives in utils/sort/tuplestore.c
}

unsafe fn tuplestore_puttuple(state: *mut Tuplestorestate, tuple: HeapTuple) { crate::utils::sort::tuplestore::tuplestore_puttuple(state as _, tuple as _) }

unsafe fn CreateTemplateTupleDesc(natts: c_int) -> TupleDesc { unimplemented!() }

unsafe fn TupleDescInitEntry(
    desc: TupleDesc,
    attributeNumber: AttrNumber,
    attributeName: *const c_char,
    oidtypeid: Oid,
    typmod: i32,
    attdim: c_int,
) { unimplemented!() }

unsafe fn TupleDescGetAttInMetadata(tupdesc: TupleDesc) -> *mut AttInMetadata { unimplemented!() }

unsafe fn BuildTupleFromCStrings(
    attinmeta: *mut AttInMetadata,
    values: *mut *mut c_char,
) -> HeapTuple { unimplemented!() }

/// MaxTupleAttributeNumber - access/htup_details.h
const MaxTupleAttributeNumber: usize = 1664;

// ---------------------------------------------------------------------------
// DatumGetLSN / DirectFunctionCall1Coll / pg_lsn_in / CStringGetDatum
// ---------------------------------------------------------------------------
// TODO(pg-port): real DatumGetLSN lives in utils/adt/pg_lsn.c / pg_lsn.h.
#[inline]
unsafe fn DatumGetLSN(d: Datum) -> XLogRecPtr { crate::utils::adt::pg_lsn::DatumGetLSN(d as _) }

// TODO(pg-port): real DirectFunctionCall1Coll lives in utils/fmgr/fmgr.c.
#[inline]
unsafe fn DirectFunctionCall1Coll(
    _func: unsafe extern "C" fn(crate::utils::fmgr::FunctionCallInfo) -> Datum,
    _collation: Oid,
    _arg1: Datum,
) -> Datum {
    unimplemented!() // TODO(pg-port): real DirectFunctionCall1Coll lives in utils/fmgr/fmgr.c
}

// TODO(pg-port): real pg_lsn_in lives in utils/adt/pg_lsn.c.
unsafe extern "C" fn pg_lsn_in(
    fcinfo: crate::utils::fmgr::FunctionCallInfo,
) -> Datum { crate::utils::adt::pg_lsn::pg_lsn_in(fcinfo) }

// TODO(pg-port): real CStringGetDatum lives in utils/adt/datum.c / fmgr.h.
#[inline]
fn CStringGetDatum(ptr: *const c_char) -> Datum {
    ptr as Datum
}

// ---------------------------------------------------------------------------
// WalRcvStreamOptions - replication/walreceiver.h
// (WalRcvStreamOptions is currently a c_void stub in worker_internal.rs;
//  we define the real layout here for use within this module.)
// ---------------------------------------------------------------------------
/// Physical replication protocol sub-options.
#[repr(C)]
pub struct WalRcvStreamPhysical {
    pub startpointTLI: TimeLineID,
}

/// Logical replication protocol sub-options.
#[repr(C)]
pub struct WalRcvStreamLogical {
    pub proto_version: uint32,
    pub publication_names: *mut List,
    pub binary: bool,
    pub streaming_str: *mut c_char,
    pub twophase: bool,
    pub origin: *mut c_char,
}

#[repr(C)]
pub union WalRcvStreamProto {
    pub physical: core::mem::ManuallyDrop<WalRcvStreamPhysical>,
    pub logical: core::mem::ManuallyDrop<WalRcvStreamLogical>,
}

/// WalRcvStreamOptions - replication/walreceiver.h
#[repr(C)]
pub struct WalRcvStreamOptions {
    /// True if logical replication stream, false if physical.
    pub logical: bool,
    /// Name of the replication slot or NULL.
    pub slotname: *const c_char,
    /// LSN of starting point.
    pub startpoint: XLogRecPtr,
    pub proto: WalRcvStreamProto,
}

// ---------------------------------------------------------------------------
// CRSSnapshotAction - replication/walsender.h
// ---------------------------------------------------------------------------
/// CRSSnapshotAction: action to take with CREATE_REPLICATION_SLOT snapshot.
pub type CRSSnapshotAction = c_int;
pub const CRS_EXPORT_SNAPSHOT: CRSSnapshotAction = 0;
pub const CRS_NOEXPORT_SNAPSHOT: CRSSnapshotAction = 1;
pub const CRS_USE_SNAPSHOT: CRSSnapshotAction = 2;

// ---------------------------------------------------------------------------
// WalRcvExecStatus / WalRcvExecResult - replication/walreceiver.h
// ---------------------------------------------------------------------------
/// Status of walreceiver query execution.
pub type WalRcvExecStatus = c_int;
pub const WALRCV_ERROR: WalRcvExecStatus = 0;
pub const WALRCV_OK_COMMAND: WalRcvExecStatus = 1;
pub const WALRCV_OK_TUPLES: WalRcvExecStatus = 2;
pub const WALRCV_OK_COPY_IN: WalRcvExecStatus = 3;
pub const WALRCV_OK_COPY_OUT: WalRcvExecStatus = 4;
pub const WALRCV_OK_COPY_BOTH: WalRcvExecStatus = 5;

/// Return value for walrcv_exec.
#[repr(C)]
pub struct WalRcvExecResult {
    pub status: WalRcvExecStatus,
    pub sqlstate: c_int,
    pub err: *mut c_char,
    pub tuplestore: *mut Tuplestorestate,
    pub tupledesc: TupleDesc,
}

// ---------------------------------------------------------------------------
// WalReceiverFunctionsType / WalReceiverFunctions - replication/walreceiver.h
// ---------------------------------------------------------------------------
/// Vtable of callbacks that a libpqwalreceiver module must fill in.
#[repr(C)]
pub struct WalReceiverFunctionsType {
    pub walrcv_connect: unsafe fn(
        conninfo: *const c_char,
        replication: bool,
        logical: bool,
        must_use_password: bool,
        appname: *const c_char,
        err: *mut *mut c_char,
    ) -> *mut WalReceiverConn,

    pub walrcv_check_conninfo:
        unsafe fn(conninfo: *const c_char, must_use_password: bool),

    pub walrcv_get_conninfo:
        unsafe fn(conn: *mut WalReceiverConn) -> *mut c_char,

    pub walrcv_get_senderinfo: unsafe fn(
        conn: *mut WalReceiverConn,
        sender_host: *mut *mut c_char,
        sender_port: *mut c_int,
    ),

    pub walrcv_identify_system: unsafe fn(
        conn: *mut WalReceiverConn,
        primary_tli: *mut TimeLineID,
    ) -> *mut c_char,

    pub walrcv_server_version:
        unsafe fn(conn: *mut WalReceiverConn) -> c_int,

    pub walrcv_readtimelinehistoryfile: unsafe fn(
        conn: *mut WalReceiverConn,
        tli: TimeLineID,
        filename: *mut *mut c_char,
        content: *mut *mut c_char,
        len: *mut c_int,
    ),

    pub walrcv_startstreaming: unsafe fn(
        conn: *mut WalReceiverConn,
        options: *const WalRcvStreamOptions,
    ) -> bool,

    pub walrcv_endstreaming: unsafe fn(
        conn: *mut WalReceiverConn,
        next_tli: *mut TimeLineID,
    ),

    pub walrcv_receive: unsafe fn(
        conn: *mut WalReceiverConn,
        buffer: *mut *mut c_char,
        wait_fd: *mut pgsocket,
    ) -> c_int,

    pub walrcv_send: unsafe fn(
        conn: *mut WalReceiverConn,
        buffer: *const c_char,
        nbytes: c_int,
    ),

    pub walrcv_create_slot: unsafe fn(
        conn: *mut WalReceiverConn,
        slotname: *const c_char,
        temporary: bool,
        two_phase: bool,
        failover: bool,
        snapshot_action: CRSSnapshotAction,
        lsn: *mut XLogRecPtr,
    ) -> *mut c_char,

    pub walrcv_alter_slot: unsafe fn(
        conn: *mut WalReceiverConn,
        slotname: *const c_char,
        failover: *const bool,
        two_phase: *const bool,
    ),

    pub walrcv_get_dbname_from_conninfo:
        unsafe fn(conninfo: *const c_char) -> *mut c_char,

    pub walrcv_get_backend_pid:
        unsafe fn(conn: *mut WalReceiverConn) -> c_int,

    pub walrcv_exec: unsafe fn(
        conn: *mut WalReceiverConn,
        query: *const c_char,
        nRetTypes: c_int,
        retTypes: *const Oid,
    ) -> *mut WalRcvExecResult,

    pub walrcv_disconnect: unsafe fn(conn: *mut WalReceiverConn),
}

// TODO(pg-port): real WalReceiverFunctions global lives in replication/walreceiver.c.
// Declared locally (not extern "C") since the vtable holds Rust-ABI fn pointers.
static mut WalReceiverFunctions: *mut WalReceiverFunctionsType = std::ptr::null_mut();

// ---------------------------------------------------------------------------
// WalReceiverConn
// ---------------------------------------------------------------------------
/// Private connection handle for this libpq-based implementation.
#[repr(C)]
pub struct WalReceiverConn {
    /// Current connection to the primary, if any.
    streamConn: *mut PGconn,
    /// Used to remember if the connection is logical or physical.
    logical: bool,
    /// Buffer for currently read records.
    recvBuf: *mut c_char,
}

// ---------------------------------------------------------------------------
// Module initialisation (replaces PG_MODULE_MAGIC_EXT + _PG_init)
// ---------------------------------------------------------------------------

static PQWalReceiverFunctions: WalReceiverFunctionsType = WalReceiverFunctionsType {
    walrcv_connect: libpqrcv_connect,
    walrcv_check_conninfo: libpqrcv_check_conninfo,
    walrcv_get_conninfo: libpqrcv_get_conninfo,
    walrcv_get_senderinfo: libpqrcv_get_senderinfo,
    walrcv_identify_system: libpqrcv_identify_system,
    walrcv_server_version: libpqrcv_server_version,
    walrcv_readtimelinehistoryfile: libpqrcv_readtimelinehistoryfile,
    walrcv_startstreaming: libpqrcv_startstreaming,
    walrcv_endstreaming: libpqrcv_endstreaming,
    walrcv_receive: libpqrcv_receive,
    walrcv_send: libpqrcv_send,
    walrcv_create_slot: libpqrcv_create_slot,
    walrcv_alter_slot: libpqrcv_alter_slot,
    walrcv_get_dbname_from_conninfo: libpqrcv_get_dbname_from_conninfo,
    walrcv_get_backend_pid: libpqrcv_get_backend_pid,
    walrcv_exec: libpqrcv_exec,
    walrcv_disconnect: libpqrcv_disconnect,
};

/*
 * Module initialization function
 */
pub unsafe fn _PG_init() {
    if !WalReceiverFunctions.is_null() {
        elog!(ERROR, "libpqwalreceiver already loaded");
    }
    WalReceiverFunctions =
        &PQWalReceiverFunctions as *const WalReceiverFunctionsType as *mut WalReceiverFunctionsType;
}

// ---------------------------------------------------------------------------
// Interface functions
// ---------------------------------------------------------------------------

/*
 * Establish the connection to the primary server.
 *
 * This function can be used for both replication and regular connections.
 * If it is a replication connection, it could be either logical or physical
 * based on input argument 'logical'.
 *
 * If an error occurs, this function will normally return NULL and set *err
 * to a palloc'ed error message. However, if must_use_password is true and
 * the connection fails to use the password, this function will ereport(ERROR).
 */
unsafe fn libpqrcv_connect(
    conninfo: *const c_char,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: *const c_char,
    err: *mut *mut c_char,
) -> *mut WalReceiverConn {
    let mut keys: [*const c_char; 6] = [null_mut(); 6];
    let mut vals: [*const c_char; 6] = [null_mut(); 6];
    let mut i: usize = 0;
    let mut options_val: *mut c_char = null_mut();

    /*
     * Re-validate connection string. The validation already happened at DDL
     * time, but the subscription owner may have changed. If we don't recheck
     * with the correct must_use_password, it's possible that the connection
     * will obtain the password from a different source, such as PGPASSFILE or
     * PGPASSWORD.
     */
    libpqrcv_check_conninfo(conninfo, must_use_password);

    /*
     * We use the expand_dbname parameter to process the connection string (or
     * URI), and pass some extra options.
     */
    keys[i] = c"dbname".as_ptr();
    vals[i] = conninfo;

    /* We can not have logical without replication */
    debug_assert!(replication || !logical);

    if replication {
        i += 1;
        keys[i] = c"replication".as_ptr();
        vals[i] = if logical {
            c"database".as_ptr()
        } else {
            c"true".as_ptr()
        };

        if logical {
            let mut opt: *mut c_char = null_mut();

            /* Tell the publisher to translate to our encoding */
            i += 1;
            keys[i] = c"client_encoding".as_ptr();
            vals[i] = GetDatabaseEncodingName();

            /*
             * Force assorted GUC parameters to settings that ensure that the
             * publisher will output data values in a form that is unambiguous
             * to the subscriber.  (We don't want to modify the subscriber's
             * GUC settings, since that might surprise user-defined code
             * running in the subscriber, such as triggers.)  This should
             * match what pg_dump does.
             */
            opt = libpqrcv_get_option_from_conninfo(conninfo, c"options".as_ptr());
            options_val = psprintf_opt_str(
                " -c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3",
                opt,
                "",
            );
            i += 1;
            keys[i] = c"options".as_ptr();
            vals[i] = options_val;
            if !opt.is_null() {
                pfree(opt as *mut c_void);
            }
        } else {
            /*
             * The database name is ignored by the server in replication mode,
             * but specify "replication" for .pgpass lookup.
             */
            i += 1;
            keys[i] = c"dbname".as_ptr();
            vals[i] = c"replication".as_ptr();
        }
    }

    i += 1;
    keys[i] = c"fallback_application_name".as_ptr();
    vals[i] = appname;

    i += 1;
    keys[i] = null_mut();
    vals[i] = null_mut();

    debug_assert!(i < 6);

    let conn = palloc0(core::mem::size_of::<WalReceiverConn>()) as *mut WalReceiverConn;
    (*conn).streamConn = libpqsrv_connect_params(
        keys.as_ptr(),
        vals.as_ptr(),
        /* expand_dbname = */ 1,
        WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
    );

    if !options_val.is_null() {
        pfree(options_val as *mut c_void);
    }

    if PQstatus((*conn).streamConn) != CONNECTION_OK {
        // bad_connection_errmsg path
        *err = pchomp(PQerrorMessage((*conn).streamConn));
        libpqsrv_disconnect((*conn).streamConn);
        pfree(conn as *mut c_void);
        return null_mut();
    }

    if must_use_password && PQconnectionUsedPassword((*conn).streamConn) == 0 {
        libpqsrv_disconnect((*conn).streamConn);
        pfree(conn as *mut c_void);

        ereport!(
            ERROR,
            errmsg!(
                "password is required: Non-superuser cannot connect if the server does not \
                 request a password. Target server's authentication method must be changed, \
                 or set password_required=false in the subscription parameters."
            )
        );
    }

    /*
     * Set always-secure search path for the cases where the connection is
     * used to run SQL queries, so malicious users can't get control.
     */
    if !replication || logical {
        let res = libpqsrv_exec(
            (*conn).streamConn,
            ALWAYS_SECURE_SEARCH_PATH_SQL.as_ptr() as *const c_char,
            WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
        );
        if PQresultStatus(res) != PGRES_TUPLES_OK {
            PQclear(res);
            *err = psprintf_2(
                "could not clear search path: ",
                pchomp(PQerrorMessage((*conn).streamConn)),
            );
            // bad_connection path
            libpqsrv_disconnect((*conn).streamConn);
            pfree(conn as *mut c_void);
            return null_mut();
        }
        PQclear(res);
    }

    (*conn).logical = logical;

    conn
}

/*
 * Validate connection info string.
 *
 * If the connection string can't be parsed, this function will raise
 * an error. If must_use_password is true, the function raises an error
 * if no password is provided in the connection string. In any other case
 * it successfully completes.
 */
unsafe fn libpqrcv_check_conninfo(conninfo: *const c_char, must_use_password: bool) {
    let mut err: *mut c_char = null_mut();

    let opts = PQconninfoParse(conninfo, &mut err);
    if opts.is_null() {
        /* The error string is malloc'd, so we must free it explicitly */
        let errcopy: *mut c_char = if !err.is_null() {
            pstrdup(err)
        } else {
            c"out of memory".as_ptr() as *mut c_char
        };
        PQfreemem(err as *mut c_void);
        ereport!(
            ERROR,
            errmsg!(
                "invalid connection string syntax: {}",
                core::ffi::CStr::from_ptr(errcopy).to_string_lossy()
            )
        );
    }

    if must_use_password {
        let mut uses_password = false;

        let mut opt = opts;
        while !(*opt).keyword.is_null() {
            /* Ignore connection options that are not present. */
            if (*opt).val.is_null() {
                opt = opt.add(1);
                continue;
            }

            if libc_strcmp((*opt).keyword, c"password".as_ptr()) == 0
                && *(*opt).val != 0
            {
                uses_password = true;
                break;
            }
            opt = opt.add(1);
        }

        if !uses_password {
            PQconninfoFree(opts);

            ereport!(
                ERROR,
                errmsg!(
                    "password is required: Non-superusers must provide a password in the \
                     connection string."
                )
            );
        }
    }

    PQconninfoFree(opts);
}

/*
 * Return a user-displayable conninfo string.  Any security-sensitive fields
 * are obfuscated.
 */
unsafe fn libpqrcv_get_conninfo(conn: *mut WalReceiverConn) -> *mut c_char {
    let mut buf: PQExpBufferData = core::mem::zeroed();

    debug_assert!(!(*conn).streamConn.is_null());

    initPQExpBuffer(&mut buf);
    let conn_opts = PQconninfo((*conn).streamConn);

    if conn_opts.is_null() {
        ereport!(
            ERROR,
            errmsg!("could not parse connection string: out of memory")
        );
    }

    /* build a clean connection string from pieces */
    let mut conn_opt = conn_opts;
    while !(*conn_opt).keyword.is_null() {
        /* Skip debug and empty options */
        let dispchar = (*conn_opt).dispchar;
        let has_D = !dispchar.is_null() && libc_strchr(dispchar, b'D' as c_int);
        let val_empty = (*conn_opt).val.is_null() || *(*conn_opt).val == 0;
        if has_D || val_empty {
            conn_opt = conn_opt.add(1);
            continue;
        }

        /* Obfuscate security-sensitive options */
        let obfuscate = !dispchar.is_null() && libc_strchr(dispchar, b'*' as c_int);
        let display_val: *const c_char = if obfuscate {
            c"********".as_ptr()
        } else {
            (*conn_opt).val
        };
        let sep: *const c_char = if buf.len == 0 {
            c"".as_ptr()
        } else {
            c" ".as_ptr()
        };
        appendPQExpBuffer(
            &mut buf,
            c"%s%s=%s".as_ptr(),
            sep,
            display_val,
        );
        conn_opt = conn_opt.add(1);
    }

    PQconninfoFree(conn_opts);

    let retval = if PQExpBufferDataBroken(core::ptr::read(&buf)) {
        null_mut()
    } else {
        pstrdup(buf.data)
    };
    termPQExpBuffer(&mut buf);
    retval
}

/*
 * Provides information of sender this WAL receiver is connected to.
 */
unsafe fn libpqrcv_get_senderinfo(
    conn: *mut WalReceiverConn,
    sender_host: *mut *mut c_char,
    sender_port: *mut c_int,
) {
    *sender_host = null_mut();
    *sender_port = 0;

    debug_assert!(!(*conn).streamConn.is_null());

    let ret = PQhost((*conn).streamConn);
    if !ret.is_null() && *ret != 0 {
        *sender_host = pstrdup(ret);
    }

    let ret = PQport((*conn).streamConn);
    if !ret.is_null() && *ret != 0 {
        *sender_port = libc_atoi(ret);
    }
}

/*
 * Check that primary's system identifier matches ours, and fetch the current
 * timeline ID of the primary.
 */
unsafe fn libpqrcv_identify_system(
    conn: *mut WalReceiverConn,
    primary_tli: *mut TimeLineID,
) -> *mut c_char {
    /*
     * Get the system identifier and timeline ID as a DataRow message from the
     * primary server.
     */
    let res = libpqsrv_exec(
        (*conn).streamConn,
        c"IDENTIFY_SYSTEM".as_ptr(),
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );
    if PQresultStatus(res) != PGRES_TUPLES_OK {
        PQclear(res);
        ereport!(
            ERROR,
            errmsg!(
                "could not receive database system identifier and timeline ID from \
                 the primary server: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }

    /*
     * IDENTIFY_SYSTEM returns 3 columns in 9.3 and earlier, and 4 columns in
     * 9.4 and onwards.
     */
    if PQnfields(res) < 3 || PQntuples(res) != 1 {
        let ntuples = PQntuples(res);
        let nfields = PQnfields(res);

        PQclear(res);
        ereport!(
            ERROR,
            errmsg!(
                "invalid response from primary server: Could not identify system: \
                 got {} rows and {} fields, expected {} rows and {} or more fields.",
                ntuples,
                nfields,
                1,
                3
            )
        );
    }
    let primary_sysid = pstrdup(PQgetvalue(res, 0, 0));
    *primary_tli = pg_strtoint32(PQgetvalue(res, 0, 1)) as TimeLineID;
    PQclear(res);

    primary_sysid
}

/*
 * Thin wrapper around libpq to obtain server version.
 */
unsafe fn libpqrcv_server_version(conn: *mut WalReceiverConn) -> c_int {
    PQserverVersion((*conn).streamConn)
}

/*
 * Get database name from the primary server's conninfo.
 *
 * If dbname is not found in connInfo, return NULL value.
 */
unsafe fn libpqrcv_get_dbname_from_conninfo(connInfo: *const c_char) -> *mut c_char {
    libpqrcv_get_option_from_conninfo(connInfo, c"dbname".as_ptr())
}

/*
 * Get the value of the option with the given keyword from the primary
 * server's conninfo.
 *
 * If the option is not found in connInfo, return NULL value.
 */
unsafe fn libpqrcv_get_option_from_conninfo(
    connInfo: *const c_char,
    keyword: *const c_char,
) -> *mut c_char {
    let mut option: *mut c_char = null_mut();
    let mut err: *mut c_char = null_mut();

    let opts = PQconninfoParse(connInfo, &mut err);
    if opts.is_null() {
        /* The error string is malloc'd, so we must free it explicitly */
        let errcopy: *mut c_char = if !err.is_null() {
            pstrdup(err)
        } else {
            c"out of memory".as_ptr() as *mut c_char
        };
        PQfreemem(err as *mut c_void);
        ereport!(
            ERROR,
            errmsg!(
                "invalid connection string syntax: {}",
                core::ffi::CStr::from_ptr(errcopy).to_string_lossy()
            )
        );
    }

    let mut opt = opts;
    while !(*opt).keyword.is_null() {
        /*
         * If the same option appears multiple times, then the last one will
         * be returned
         */
        if libc_strcmp((*opt).keyword, keyword) == 0
            && !(*opt).val.is_null()
            && *(*opt).val != 0
        {
            if !option.is_null() {
                pfree(option as *mut c_void);
            }
            option = pstrdup((*opt).val);
        }
        opt = opt.add(1);
    }

    PQconninfoFree(opts);
    option
}

/*
 * Start streaming WAL data from given streaming options.
 *
 * Returns true if we switched successfully to copy-both mode. False
 * means the server received the command and executed it successfully, but
 * didn't switch to copy-mode.  That means that there was no WAL on the
 * requested timeline and starting point, because the server switched to
 * another timeline at or before the requested starting point. On failure,
 * throws an ERROR.
 */
unsafe fn libpqrcv_startstreaming(
    conn: *mut WalReceiverConn,
    options: *const WalRcvStreamOptions,
) -> bool {
    let mut cmd: StringInfoData = core::mem::zeroed();

    debug_assert!((*options).logical == (*conn).logical);
    debug_assert!(!(*options).slotname.is_null() || !(*options).logical);

    initStringInfo(&mut cmd);

    /* Build the command. */
    appendStringInfoString(&mut cmd, c"START_REPLICATION".as_ptr());
    if !(*options).slotname.is_null() {
        appendStringInfo!(&mut cmd, " SLOT \"{}\"",
            core::ffi::CStr::from_ptr((*options).slotname).to_string_lossy());
    }

    if (*options).logical {
        appendStringInfoString(&mut cmd, c" LOGICAL".as_ptr());
    }

    let (lsn_hi, lsn_lo) = LSN_FORMAT_ARGS((*options).startpoint);
    appendStringInfo!(&mut cmd, " {:X}/{:X}", lsn_hi, lsn_lo);

    /*
     * Additional options are different depending on if we are doing logical
     * or physical replication.
     */
    if (*options).logical {
        appendStringInfoString(&mut cmd, c" (".as_ptr());

        let proto_version = (&(*options).proto.logical).proto_version;
        appendStringInfo!(&mut cmd, "proto_version '{}'", proto_version);

        let streaming_str = (&(*options).proto.logical).streaming_str;
        if !streaming_str.is_null() {
            appendStringInfo!(&mut cmd, ", streaming '{}'",
                core::ffi::CStr::from_ptr(streaming_str).to_string_lossy());
        }

        if (&(*options).proto.logical).twophase && PQserverVersion((*conn).streamConn) >= 150000 {
            appendStringInfoString(&mut cmd, c", two_phase 'on'".as_ptr());
        }

        let origin = (&(*options).proto.logical).origin;
        if !origin.is_null() && PQserverVersion((*conn).streamConn) >= 160000 {
            appendStringInfo!(&mut cmd, ", origin '{}'",
                core::ffi::CStr::from_ptr(origin).to_string_lossy());
        }

        let pubnames: *mut List = (&(*options).proto.logical).publication_names;
        let pubnames_str = stringlist_to_identifierstr((*conn).streamConn, pubnames);
        if pubnames_str.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "could not start WAL streaming: {}",
                    core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                        .to_string_lossy()
                )
            );
        }
        let pubnames_len = libc_strlen(pubnames_str);
        let pubnames_literal =
            PQescapeLiteral((*conn).streamConn, pubnames_str, pubnames_len);
        if pubnames_literal.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "could not start WAL streaming: {}",
                    core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                        .to_string_lossy()
                )
            );
        }
        appendStringInfo!(&mut cmd, ", publication_names {}",
            core::ffi::CStr::from_ptr(pubnames_literal).to_string_lossy());
        PQfreemem(pubnames_literal as *mut c_void);
        pfree(pubnames_str as *mut c_void);

        if (&(*options).proto.logical).binary && PQserverVersion((*conn).streamConn) >= 140000 {
            appendStringInfoString(&mut cmd, c", binary 'true'".as_ptr());
        }

        appendStringInfoChar(&mut cmd, b')' as c_char);
    } else {
        let startpoint_tli = (&(*options).proto.physical).startpointTLI;
        appendStringInfo!(&mut cmd, " TIMELINE {}", startpoint_tli);
    }

    /* Start streaming. */
    let res = libpqsrv_exec(
        (*conn).streamConn,
        cmd.data,
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );
    pfree(cmd.data as *mut c_void);

    if PQresultStatus(res) == PGRES_COMMAND_OK {
        PQclear(res);
        return false;
    } else if PQresultStatus(res) != PGRES_COPY_BOTH {
        PQclear(res);
        ereport!(
            ERROR,
            errmsg!(
                "could not start WAL streaming: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }
    PQclear(res);
    true
}

/*
 * Stop streaming WAL data. Returns the next timeline's ID in *next_tli, as
 * reported by the server, or 0 if it did not report it.
 */
unsafe fn libpqrcv_endstreaming(conn: *mut WalReceiverConn, next_tli: *mut TimeLineID) {
    /*
     * Send copy-end message.  As in libpqsrv_exec, this could theoretically
     * block, but the risk seems small.
     */
    if PQputCopyEnd((*conn).streamConn, null_mut()) <= 0
        || PQflush((*conn).streamConn) != 0
    {
        ereport!(
            ERROR,
            errmsg!(
                "could not send end-of-streaming message to primary: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }

    *next_tli = 0;

    /*
     * After COPY is finished, we should receive a result set indicating the
     * next timeline's ID, or just CommandComplete if the server was shut
     * down.
     *
     * If we had not yet received CopyDone from the backend, PGRES_COPY_OUT is
     * also possible in case we aborted the copy in mid-stream.
     */
    let mut res = libpqsrv_get_result((*conn).streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    if PQresultStatus(res) == PGRES_TUPLES_OK {
        /*
         * Read the next timeline's ID. The server also sends the timeline's
         * starting point, but it is ignored.
         */
        if PQnfields(res) < 2 || PQntuples(res) != 1 {
            ereport!(
                ERROR,
                errmsg!("unexpected result set after end-of-streaming")
            );
        }
        *next_tli = pg_strtoint32(PQgetvalue(res, 0, 0)) as TimeLineID;
        PQclear(res);

        /* the result set should be followed by CommandComplete */
        res = libpqsrv_get_result((*conn).streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    } else if PQresultStatus(res) == PGRES_COPY_OUT {
        PQclear(res);

        /* End the copy */
        if PQendcopy((*conn).streamConn) != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "error while shutting down streaming COPY: {}",
                    core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                        .to_string_lossy()
                )
            );
        }

        /* CommandComplete should follow */
        res = libpqsrv_get_result((*conn).streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    }

    if PQresultStatus(res) != PGRES_COMMAND_OK {
        ereport!(
            ERROR,
            errmsg!(
                "error reading result of streaming command: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }
    PQclear(res);

    /* Verify that there are no more results */
    res = libpqsrv_get_result((*conn).streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    if !res.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "unexpected result after CommandComplete: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }
}

/*
 * Fetch the timeline history file for 'tli' from primary.
 */
unsafe fn libpqrcv_readtimelinehistoryfile(
    conn: *mut WalReceiverConn,
    tli: TimeLineID,
    filename: *mut *mut c_char,
    content: *mut *mut c_char,
    len: *mut c_int,
) {
    debug_assert!(!(*conn).logical);

    /*
     * Request the primary to send over the history file for given timeline.
     */
    let mut cmd_buf = [0i8; 64];
    let cmd_str = format!("TIMELINE_HISTORY {}", tli);
    let cmd_bytes = cmd_str.as_bytes();
    let copy_len = cmd_bytes.len().min(63);
    core::ptr::copy_nonoverlapping(
        cmd_bytes.as_ptr() as *const i8,
        cmd_buf.as_mut_ptr(),
        copy_len,
    );

    let res = libpqsrv_exec(
        (*conn).streamConn,
        cmd_buf.as_ptr(),
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );
    if PQresultStatus(res) != PGRES_TUPLES_OK {
        PQclear(res);
        ereport!(
            ERROR,
            errmsg!(
                "could not receive timeline history file from the primary server: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }
    if PQnfields(res) != 2 || PQntuples(res) != 1 {
        let ntuples = PQntuples(res);
        let nfields = PQnfields(res);

        PQclear(res);
        ereport!(
            ERROR,
            errmsg!(
                "invalid response from primary server: \
                 Expected 1 tuple with 2 fields, got {} tuples with {} fields.",
                ntuples,
                nfields
            )
        );
    }
    *filename = pstrdup(PQgetvalue(res, 0, 0));

    *len = PQgetlength(res, 0, 1);
    *content = palloc(*len as Size) as *mut c_char;
    core::ptr::copy_nonoverlapping(PQgetvalue(res, 0, 1), *content, *len as usize);
    PQclear(res);
}

/*
 * Disconnect connection to primary, if any.
 */
unsafe fn libpqrcv_disconnect(conn: *mut WalReceiverConn) {
    libpqsrv_disconnect((*conn).streamConn);
    PQfreemem((*conn).recvBuf as *mut c_void);
    pfree(conn as *mut c_void);
}

/*
 * Receive a message available from XLOG stream.
 *
 * Returns:
 *
 *   If data was received, returns the length of the data. *buffer is set to
 *   point to a buffer holding the received message. The buffer is only valid
 *   until the next libpqrcv_* call.
 *
 *   If no data was available immediately, returns 0, and *wait_fd is set to a
 *   socket descriptor which can be waited on before trying again.
 *
 *   -1 if the server ended the COPY.
 *
 * ereports on error.
 */
unsafe fn libpqrcv_receive(
    conn: *mut WalReceiverConn,
    buffer: *mut *mut c_char,
    wait_fd: *mut pgsocket,
) -> c_int {
    PQfreemem((*conn).recvBuf as *mut c_void);
    (*conn).recvBuf = null_mut();

    /* Try to receive a CopyData message */
    let mut rawlen = PQgetCopyData((*conn).streamConn, &mut (*conn).recvBuf, 1);
    if rawlen == 0 {
        /* Try consuming some data. */
        if PQconsumeInput((*conn).streamConn) == 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "could not receive data from WAL stream: {}",
                    core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                        .to_string_lossy()
                )
            );
        }

        /* Now that we've consumed some input, try again */
        rawlen = PQgetCopyData((*conn).streamConn, &mut (*conn).recvBuf, 1);
        if rawlen == 0 {
            /* Tell caller to try again when our socket is ready. */
            *wait_fd = PQsocket((*conn).streamConn);
            return 0;
        }
    }
    if rawlen == -1 {
        /* end-of-streaming or error */
        let res = libpqsrv_get_result((*conn).streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
        if PQresultStatus(res) == PGRES_COMMAND_OK {
            PQclear(res);

            /* Verify that there are no more results. */
            let res2 =
                libpqsrv_get_result((*conn).streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
            if !res2.is_null() {
                PQclear(res2);

                /*
                 * If the other side closed the connection orderly (otherwise
                 * we'd seen an error, or PGRES_COPY_IN) don't report an error
                 * here, but let callers deal with it.
                 */
                if PQstatus((*conn).streamConn) == CONNECTION_OK + 1
                    || PQstatus((*conn).streamConn) != CONNECTION_OK
                {
                    // CONNECTION_BAD is typically 1 in libpq
                    return -1;
                }

                ereport!(
                    ERROR,
                    errmsg!(
                        "unexpected result after CommandComplete: {}",
                        core::ffi::CStr::from_ptr(PQerrorMessage((*conn).streamConn))
                            .to_string_lossy()
                    )
                );
            }

            return -1;
        } else if PQresultStatus(res) == PGRES_COPY_IN {
            PQclear(res);
            return -1;
        } else {
            PQclear(res);
            ereport!(
                ERROR,
                errmsg!(
                    "could not receive data from WAL stream: {}",
                    core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                        .to_string_lossy()
                )
            );
        }
    }
    if rawlen < -1 {
        ereport!(
            ERROR,
            errmsg!(
                "could not receive data from WAL stream: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }

    /* Return received messages to caller */
    *buffer = (*conn).recvBuf;
    rawlen
}

/*
 * Send a message to XLOG stream.
 *
 * ereports on error.
 */
unsafe fn libpqrcv_send(conn: *mut WalReceiverConn, buffer: *const c_char, nbytes: c_int) {
    if PQputCopyData((*conn).streamConn, buffer, nbytes) <= 0
        || PQflush((*conn).streamConn) != 0
    {
        ereport!(
            ERROR,
            errmsg!(
                "could not send data to WAL stream: {}",
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }
}

/*
 * Create new replication slot.
 * Returns the name of the exported snapshot for logical slot or NULL for
 * physical slot.
 */
unsafe fn libpqrcv_create_slot(
    conn: *mut WalReceiverConn,
    slotname: *const c_char,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    snapshot_action: CRSSnapshotAction,
    lsn: *mut XLogRecPtr,
) -> *mut c_char {
    let mut cmd: StringInfoData = core::mem::zeroed();

    let use_new_options_syntax = PQserverVersion((*conn).streamConn) >= 150000;

    initStringInfo(&mut cmd);

    appendStringInfo!(&mut cmd, "CREATE_REPLICATION_SLOT \"{}\"",
        core::ffi::CStr::from_ptr(slotname).to_string_lossy());

    if temporary {
        appendStringInfoString(&mut cmd, c" TEMPORARY".as_ptr());
    }

    if (*conn).logical {
        appendStringInfoString(&mut cmd, c" LOGICAL pgoutput ".as_ptr());
        if use_new_options_syntax {
            appendStringInfoChar(&mut cmd, b'(' as c_char);
        }
        if two_phase {
            appendStringInfoString(&mut cmd, c"TWO_PHASE".as_ptr());
            if use_new_options_syntax {
                appendStringInfoString(&mut cmd, c", ".as_ptr());
            } else {
                appendStringInfoChar(&mut cmd, b' ' as c_char);
            }
        }

        if failover {
            appendStringInfoString(&mut cmd, c"FAILOVER".as_ptr());
            if use_new_options_syntax {
                appendStringInfoString(&mut cmd, c", ".as_ptr());
            } else {
                appendStringInfoChar(&mut cmd, b' ' as c_char);
            }
        }

        if use_new_options_syntax {
            match snapshot_action {
                CRS_EXPORT_SNAPSHOT => {
                    appendStringInfoString(&mut cmd, c"SNAPSHOT 'export'".as_ptr());
                }
                CRS_NOEXPORT_SNAPSHOT => {
                    appendStringInfoString(&mut cmd, c"SNAPSHOT 'nothing'".as_ptr());
                }
                CRS_USE_SNAPSHOT => {
                    appendStringInfoString(&mut cmd, c"SNAPSHOT 'use'".as_ptr());
                }
                _ => {}
            }
        } else {
            match snapshot_action {
                CRS_EXPORT_SNAPSHOT => {
                    appendStringInfoString(&mut cmd, c"EXPORT_SNAPSHOT".as_ptr());
                }
                CRS_NOEXPORT_SNAPSHOT => {
                    appendStringInfoString(&mut cmd, c"NOEXPORT_SNAPSHOT".as_ptr());
                }
                CRS_USE_SNAPSHOT => {
                    appendStringInfoString(&mut cmd, c"USE_SNAPSHOT".as_ptr());
                }
                _ => {}
            }
        }

        if use_new_options_syntax {
            appendStringInfoChar(&mut cmd, b')' as c_char);
        }
    } else if use_new_options_syntax {
        appendStringInfoString(&mut cmd, c" PHYSICAL (RESERVE_WAL)".as_ptr());
    } else {
        appendStringInfoString(&mut cmd, c" PHYSICAL RESERVE_WAL".as_ptr());
    }

    let res = libpqsrv_exec(
        (*conn).streamConn,
        cmd.data,
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );
    pfree(cmd.data as *mut c_void);

    if PQresultStatus(res) != PGRES_TUPLES_OK {
        PQclear(res);
        ereport!(
            ERROR,
            errmsg!(
                "could not create replication slot \"{}\": {}",
                core::ffi::CStr::from_ptr(slotname).to_string_lossy(),
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }

    if !lsn.is_null() {
        *lsn = DatumGetLSN(DirectFunctionCall1Coll(
            pg_lsn_in,
            InvalidOid,
            CStringGetDatum(PQgetvalue(res, 0, 1)),
        ));
    }

    let snapshot = if PQgetisnull(res, 0, 2) == 0 {
        pstrdup(PQgetvalue(res, 0, 2))
    } else {
        null_mut()
    };

    PQclear(res);

    snapshot
}

/*
 * Change the definition of the replication slot.
 */
unsafe fn libpqrcv_alter_slot(
    conn: *mut WalReceiverConn,
    slotname: *const c_char,
    failover: *const bool,
    two_phase: *const bool,
) {
    let mut cmd: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut cmd);
    appendStringInfo!(&mut cmd, "ALTER_REPLICATION_SLOT {} ( ",
        core::ffi::CStr::from_ptr(quote_identifier(slotname)).to_string_lossy());

    if !failover.is_null() {
        appendStringInfo!(&mut cmd, "FAILOVER {}", if *failover { "true" } else { "false" });
    }

    if !failover.is_null() && !two_phase.is_null() {
        appendStringInfoString(&mut cmd, c", ".as_ptr());
    }

    if !two_phase.is_null() {
        appendStringInfo!(&mut cmd, "TWO_PHASE {}", if *two_phase { "true" } else { "false" });
    }

    appendStringInfoString(&mut cmd, c" );".as_ptr());

    let res = libpqsrv_exec(
        (*conn).streamConn,
        cmd.data,
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );
    pfree(cmd.data as *mut c_void);

    if PQresultStatus(res) != PGRES_COMMAND_OK {
        ereport!(
            ERROR,
            errmsg!(
                "could not alter replication slot \"{}\": {}",
                core::ffi::CStr::from_ptr(slotname).to_string_lossy(),
                core::ffi::CStr::from_ptr(pchomp(PQerrorMessage((*conn).streamConn)))
                    .to_string_lossy()
            )
        );
    }

    PQclear(res);
}

/*
 * Return PID of remote backend process.
 */
unsafe fn libpqrcv_get_backend_pid(conn: *mut WalReceiverConn) -> c_int {
    PQbackendPID((*conn).streamConn)
}

/*
 * Convert tuple query result to tuplestore.
 */
unsafe fn libpqrcv_processTuples(
    pgres: *mut PGresult,
    walres: *mut WalRcvExecResult,
    nRetTypes: c_int,
    retTypes: *const Oid,
) {
    let nfields = PQnfields(pgres);

    /* Make sure we got expected number of fields. */
    if nfields != nRetTypes {
        ereport!(
            ERROR,
            errmsg!(
                "invalid query response: Expected {} fields, got {} fields.",
                nRetTypes,
                nfields
            )
        );
    }

    (*walres).tuplestore = tuplestore_begin_heap(true, false, work_mem);

    /* Create tuple descriptor corresponding to expected result. */
    (*walres).tupledesc = CreateTemplateTupleDesc(nRetTypes);
    for coln in 0..nRetTypes {
        TupleDescInitEntry(
            (*walres).tupledesc,
            (coln + 1) as AttrNumber,
            PQfname(pgres, coln),
            *retTypes.add(coln as usize),
            -1,
            0,
        );
    }
    let attinmeta = TupleDescGetAttInMetadata((*walres).tupledesc);

    /* No point in doing more here if there were no tuples returned. */
    if PQntuples(pgres) == 0 {
        return;
    }

    /* Create temporary context for local allocations. */
    let rowcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"libpqrcv query result context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /* Process returned rows. */
    for tupn in 0..PQntuples(pgres) {
        let mut cstrs: [*mut c_char; MaxTupleAttributeNumber] =
            [null_mut(); MaxTupleAttributeNumber];

        CHECK_FOR_INTERRUPTS();

        /* Do the allocations in temporary context. */
        let oldcontext = MemoryContextSwitchTo(rowcontext);

        /*
         * Fill cstrs with null-terminated strings of column values.
         */
        for coln in 0..nfields {
            if PQgetisnull(pgres, tupn, coln) != 0 {
                cstrs[coln as usize] = null_mut();
            } else {
                cstrs[coln as usize] = PQgetvalue(pgres, tupn, coln);
            }
        }

        /* Convert row to a tuple, and add it to the tuplestore */
        let tuple = BuildTupleFromCStrings(attinmeta, cstrs.as_mut_ptr());
        tuplestore_puttuple((*walres).tuplestore, tuple);

        /* Clean up */
        MemoryContextSwitchTo(oldcontext);
        MemoryContextReset(rowcontext);
    }

    MemoryContextDelete(rowcontext);
}

/*
 * Public interface for sending generic queries (and commands).
 *
 * This can only be called from process connected to database.
 */
unsafe fn libpqrcv_exec(
    conn: *mut WalReceiverConn,
    query: *const c_char,
    nRetTypes: c_int,
    retTypes: *const Oid,
) -> *mut WalRcvExecResult {
    let mut pgres: *mut PGresult = null_mut();
    let walres = palloc0(core::mem::size_of::<WalRcvExecResult>()) as *mut WalRcvExecResult;

    if MyDatabaseId == InvalidOid {
        ereport!(
            ERROR,
            errmsg!("the query interface requires a database connection")
        );
    }

    pgres = libpqsrv_exec(
        (*conn).streamConn,
        query,
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );

    let status = PQresultStatus(pgres);
    if status == PGRES_TUPLES_OK
        || status == PGRES_SINGLE_TUPLE
        || status == PGRES_TUPLES_CHUNK
    {
        (*walres).status = WALRCV_OK_TUPLES;
        libpqrcv_processTuples(pgres, walres, nRetTypes, retTypes);
    } else if status == PGRES_COPY_IN {
        (*walres).status = WALRCV_OK_COPY_IN;
    } else if status == PGRES_COPY_OUT {
        (*walres).status = WALRCV_OK_COPY_OUT;
    } else if status == PGRES_COPY_BOTH {
        (*walres).status = WALRCV_OK_COPY_BOTH;
    } else if status == PGRES_COMMAND_OK {
        (*walres).status = WALRCV_OK_COMMAND;
    } else if status == PGRES_EMPTY_QUERY {
        (*walres).status = WALRCV_ERROR;
        (*walres).err = c"empty query".as_ptr() as *mut c_char;
    } else if status == PGRES_PIPELINE_SYNC || status == PGRES_PIPELINE_ABORTED {
        (*walres).status = WALRCV_ERROR;
        (*walres).err = c"unexpected pipeline mode".as_ptr() as *mut c_char;
    } else if status == PGRES_NONFATAL_ERROR
        || status == PGRES_FATAL_ERROR
        || status == PGRES_BAD_RESPONSE
    {
        (*walres).status = WALRCV_ERROR;
        (*walres).err = pchomp(PQerrorMessage((*conn).streamConn));
        let diag_sqlstate = PQresultErrorField(pgres, PG_DIAG_SQLSTATE);
        if !diag_sqlstate.is_null() {
            let s = core::slice::from_raw_parts(diag_sqlstate as *const u8, 5);
            (*walres).sqlstate = MAKE_SQLSTATE(s[0], s[1], s[2], s[3], s[4]);
        }
    }

    PQclear(pgres);

    walres
}

/*
 * Given a List of strings, return it as single comma separated
 * string, quoting identifiers as needed.
 *
 * This is essentially the reverse of SplitIdentifierString.
 *
 * The caller should free the result.
 */
unsafe fn stringlist_to_identifierstr(conn: *mut PGconn, strings: *mut List) -> *mut c_char {
    let mut res: StringInfoData = core::mem::zeroed();
    let first = true;

    initStringInfo(&mut res);

    // TODO(pg-port): real foreach!/list iteration lives in nodes/pg_list.h (lfirst, list_head).
    // Stubbed: iterate via raw List internals if available; for now unimplemented.
    let _ = (conn, strings);
    unimplemented!("stringlist_to_identifierstr: TODO(pg-port): foreach! List iteration lives in nodes/pg_list.h");
}

// ---------------------------------------------------------------------------
// Minimal libc shims (avoid extern "C" libc dependency)
// ---------------------------------------------------------------------------

/// Minimal strcmp for NUL-terminated C strings.
#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i = 0usize;
    loop {
        let ca = *a.add(i) as u8;
        let cb = *b.add(i) as u8;
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/// Returns true if the NUL-terminated string contains the byte `c`.
#[inline]
unsafe fn libc_strchr(s: *const c_char, c: c_int) -> bool {
    let mut p = s;
    while *p != 0 {
        if *p as c_int == c {
            return true;
        }
        p = p.add(1);
    }
    false
}

/// Minimal atoi for NUL-terminated C strings.
#[inline]
unsafe fn libc_atoi(s: *const c_char) -> c_int {
    let mut p = s;
    while *p == b' ' as c_char {
        p = p.add(1);
    }
    let neg = *p == b'-' as c_char;
    if neg || *p == b'+' as c_char {
        p = p.add(1);
    }
    let mut val: c_int = 0;
    while *p >= b'0' as c_char && *p <= b'9' as c_char {
        val = val * 10 + (*p as c_int - b'0' as c_int);
        p = p.add(1);
    }
    if neg { -val } else { val }
}

/// Minimal strlen for NUL-terminated C strings.
#[inline]
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}
