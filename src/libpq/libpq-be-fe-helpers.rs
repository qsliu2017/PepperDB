//! Translated from PostgreSQL src/include/libpq/libpq-be-fe-helpers.h
//! Helper functions for using libpq in extensions (frontend-in-backend).
//!
//! These wrap the libpq *frontend* client library (libpq-fe.h), which lives
//! outside the backend tree, so the PGconn/PGresult/PGcancelConn types are
//! forward-declared here (rule 7). The C bodies poll via WaitLatchOrSocket +
//! PG_TRY/PG_CATCH; under the async model those become tokio awaits with
//! `catch_unwind` boundaries. Header-only library -> stubs here.

use crate::datatype::timestamp::TimestampTz;

// TODO(struct-forward): PGconn/PGresult/PGcancelConn are libpq-fe (client lib)
// types; repoint to the libpq frontend bindings in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to the libpq-fe PGconn bindings in Phase 2")]
pub struct PGconn;
#[deprecated(note = "TODO(struct-forward): repoint to the libpq-fe PGresult bindings in Phase 2")]
pub struct PGresult;
#[deprecated(note = "TODO(struct-forward): repoint to the libpq-fe PGcancelConn bindings in Phase 2")]
pub struct PGcancelConn;

/// PQconnectdb() wrapper that reserves an FD and processes interrupts. Returns
/// None if the connection object could not be created; otherwise the caller
/// must check `PQstatus`.
#[allow(deprecated)]
pub fn libpqsrv_connect(_conninfo: &str, _wait_event_info: u32) -> Option<PGconn> {
    unimplemented!()
}

/// Like `libpqsrv_connect`, but a wrapper for PQconnectdbParams().
#[allow(deprecated)]
pub fn libpqsrv_connect_params(
    _keywords: &[&str],
    _values: &[&str],
    _expand_dbname: i32,
    _wait_event_info: u32,
) -> Option<PGconn> {
    unimplemented!()
}

/// PQfinish() wrapper that also releases the reserved FD.
#[allow(deprecated)]
pub fn libpqsrv_disconnect(_conn: PGconn) {
    unimplemented!()
}

/// PQexec() wrapper that processes interrupts. None means a send failure.
#[allow(deprecated)]
pub fn libpqsrv_exec(_conn: &mut PGconn, _query: &str, _wait_event_info: u32) -> Option<PGresult> {
    unimplemented!()
}

/// PQexecParams() wrapper that processes interrupts.
#[allow(deprecated)]
pub fn libpqsrv_exec_params(
    _conn: &mut PGconn,
    _command: &str,
    _param_types: &[crate::postgres_ext::Oid],
    _param_values: &[&str],
    _param_lengths: &[i32],
    _param_formats: &[i32],
    _result_format: i32,
    _wait_event_info: u32,
) -> Option<PGresult> {
    unimplemented!()
}

/// Loop over PQgetResult() until NULL/terminal; return the last result.
#[allow(deprecated)]
pub fn libpqsrv_get_result_last(_conn: &mut PGconn, _wait_event_info: u32) -> Option<PGresult> {
    unimplemented!()
}

/// PQgetResult() equivalent that watches for interrupts.
#[allow(deprecated)]
pub fn libpqsrv_get_result(_conn: &mut PGconn, _wait_event_info: u32) -> Option<PGresult> {
    unimplemented!()
}

/// Submit a cancel request, waiting until `endtime`. None on success, otherwise
/// a (static) error message.
#[allow(deprecated)]
pub fn libpqsrv_cancel(_conn: &mut PGconn, _endtime: TimestampTz) -> Option<&'static str> {
    unimplemented!()
}
