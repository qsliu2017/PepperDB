//! fe_utils/query_utils.h - facilities for frontend code to query a database.

use std::ffi::{c_char, c_void};

// libpq frontend types - not yet defined in src/; stub locally.
// TODO: dedup
pub type PGconn = c_void;
pub type PGresult = c_void;

// extern PGresult *executeQuery(PGconn *conn, const char *query, bool echo);
pub unsafe fn executeQuery(conn: *mut PGconn, query: *const c_char, echo: bool) -> *mut PGresult {
    unimplemented!()
}

// extern void executeCommand(PGconn *conn, const char *query, bool echo);
pub unsafe fn executeCommand(conn: *mut PGconn, query: *const c_char, echo: bool) {
    unimplemented!()
}

// extern bool executeMaintenanceCommand(PGconn *conn, const char *query, bool echo);
pub unsafe fn executeMaintenanceCommand(
    conn: *mut PGconn,
    query: *const c_char,
    echo: bool,
) -> bool {
    unimplemented!()
}
