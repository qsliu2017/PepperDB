//! fe_utils/connect_utils.h - facilities for frontend code to connect to/disconnect from databases.

use std::ffi::{c_char, c_int};

// PGconn is a libpq frontend type; stub locally as in sibling fe_utils files.
// TODO: dedup
pub type PGconn = std::ffi::c_void;

// enum trivalue { TRI_DEFAULT, TRI_NO, TRI_YES };
pub type trivalue = c_int;
pub const TRI_DEFAULT: trivalue = 0;
pub const TRI_NO: trivalue = 1;
pub const TRI_YES: trivalue = 2;

// Parameters needed by connectDatabase/connectMaintenanceDatabase
#[repr(C)]
pub struct _connParams {
    // These fields record the actual command line parameters
    pub dbname: *const c_char, // this may be a connstring!
    pub pghost: *const c_char,
    pub pgport: *const c_char,
    pub pguser: *const c_char,
    pub prompt_password: trivalue,
    // If not NULL, this overrides the dbname obtained from command line
    // (but *only* the DB name, not anything else in the connstring)
    pub override_dbname: *const c_char,
}
pub type ConnParams = _connParams;

// extern PGconn *connectDatabase(const ConnParams *cparams, const char *progname,
//                                bool echo, bool fail_ok, bool allow_password_reuse);
pub unsafe fn connectDatabase(
    cparams: *const ConnParams,
    progname: *const c_char,
    echo: bool,
    fail_ok: bool,
    allow_password_reuse: bool,
) -> *mut PGconn {
    unimplemented!()
}

// extern PGconn *connectMaintenanceDatabase(ConnParams *cparams, const char *progname, bool echo);
pub unsafe fn connectMaintenanceDatabase(
    cparams: *mut ConnParams,
    progname: *const c_char,
    echo: bool,
) -> *mut PGconn {
    unimplemented!()
}

// extern void disconnectDatabase(PGconn *conn);
pub unsafe fn disconnectDatabase(conn: *mut PGconn) {
    unimplemented!()
}
