//! libpq/libpq-be-fe-helpers.h - Helper functions for using libpq in extensions.
//!
//! Header-only library of `static inline` functions. Every function is
//! translated to `pub unsafe fn ... { unimplemented!() }`; the bodies are not
//! ported because they depend on libpq frontend internals not present in this
//! tree. Referenced libpq/system types are stubbed locally as opaque aliases.

use std::ffi::{c_char, c_int};

use crate::c::{int64, uint32};
use crate::postgres_ext::Oid;

// TODO: dedup - libpq frontend opaque connection/result handles. Stubbed
// locally as opaque c_void aliases (matches stubs in src/fe_utils/*).
pub type PGconn = std::ffi::c_void;
pub type PGresult = std::ffi::c_void;
pub type PGcancelConn = std::ffi::c_void;

// TODO: dedup - utils/timestamp.h TimestampTz.
pub type TimestampTz = int64;

// TODO: dedup - libpq-fe.h PostgresPollingStatusType enum. Modeled as c_int
// plus the variant constants referenced by this header.
pub type PostgresPollingStatusType = c_int;
pub const PGRES_POLLING_FAILED: PostgresPollingStatusType = 0;
pub const PGRES_POLLING_READING: PostgresPollingStatusType = 1;
pub const PGRES_POLLING_WRITING: PostgresPollingStatusType = 2;
pub const PGRES_POLLING_OK: PostgresPollingStatusType = 3;
pub const PGRES_POLLING_ACTIVE: PostgresPollingStatusType = 4;

/*
 * PQconnectdb() wrapper that reserves a file descriptor and processes
 * interrupts during connection establishment.
 *
 * Throws an error if AcquireExternalFD() fails, but does not throw if
 * connection establishment itself fails. Callers need to use PQstatus() to
 * check if connection establishment succeeded.
 */
#[inline]
pub unsafe fn libpqsrv_connect(conninfo: *const c_char, wait_event_info: uint32) -> *mut PGconn {
    unimplemented!()
}

/*
 * Like libpqsrv_connect(), except that this is a wrapper for
 * PQconnectdbParams().
 */
#[inline]
pub unsafe fn libpqsrv_connect_params(
    keywords: *const *const c_char,
    values: *const *const c_char,
    expand_dbname: c_int,
    wait_event_info: uint32,
) -> *mut PGconn {
    unimplemented!()
}

/*
 * PQfinish() wrapper that additionally releases the reserved file descriptor.
 *
 * It is allowed to call this with a NULL pgconn iff NULL was returned by
 * libpqsrv_connect*.
 */
#[inline]
pub unsafe fn libpqsrv_disconnect(conn: *mut PGconn) {
    unimplemented!()
}

/* internal helper functions follow */

/*
 * Helper function for all connection establishment functions.
 */
#[inline]
pub unsafe fn libpqsrv_connect_prepare() {
    unimplemented!()
}

/*
 * Helper function for all connection establishment functions.
 */
#[inline]
pub unsafe fn libpqsrv_connect_internal(conn: *mut PGconn, wait_event_info: uint32) {
    unimplemented!()
}

/*
 * PQexec() wrapper that processes interrupts.
 *
 * Unless PQsetnonblocking(conn, 1) is in effect, this can't process
 * interrupts while pushing the query text to the server.  Consider that
 * setting if query strings can be long relative to TCP buffer size.
 *
 * This has the preconditions of PQsendQuery(), not those of PQexec().  Most
 * notably, PQexec() would silently discard any prior query results.
 */
#[inline]
pub unsafe fn libpqsrv_exec(
    conn: *mut PGconn,
    query: *const c_char,
    wait_event_info: uint32,
) -> *mut PGresult {
    unimplemented!()
}

/*
 * PQexecParams() wrapper that processes interrupts.
 *
 * See notes at libpqsrv_exec().
 */
#[inline]
pub unsafe fn libpqsrv_exec_params(
    conn: *mut PGconn,
    command: *const c_char,
    nParams: c_int,
    paramTypes: *const Oid,
    paramValues: *const *const c_char,
    paramLengths: *const c_int,
    paramFormats: *const c_int,
    resultFormat: c_int,
    wait_event_info: uint32,
) -> *mut PGresult {
    unimplemented!()
}

/*
 * Like PQexec(), loop over PQgetResult() until it returns NULL or another
 * terminal state.  Return the last non-NULL result or the terminal state.
 */
#[inline]
pub unsafe fn libpqsrv_get_result_last(conn: *mut PGconn, wait_event_info: uint32) -> *mut PGresult {
    unimplemented!()
}

/*
 * Perform the equivalent of PQgetResult(), but watch for interrupts.
 */
#[inline]
pub unsafe fn libpqsrv_get_result(conn: *mut PGconn, wait_event_info: uint32) -> *mut PGresult {
    unimplemented!()
}

/*
 * Submit a cancel request to the given connection, waiting only until
 * the given time.
 *
 * We sleep interruptibly until we receive confirmation that the cancel
 * request has been accepted, and if it is, return NULL; if the cancel
 * request fails, return an error message string (which is not to be
 * freed).
 *
 * For other problems (to wit: OOM when strdup'ing an error message from
 * libpq), this function can ereport(ERROR).
 *
 * Note: this function leaks a string's worth of memory when reporting
 * libpq errors.  Make sure to call it in a transient memory context.
 */
#[inline]
pub unsafe fn libpqsrv_cancel(conn: *mut PGconn, endtime: TimestampTz) -> *const c_char {
    unimplemented!()
}
