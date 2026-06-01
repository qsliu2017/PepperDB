//! fe_utils/cancel.h - Query cancellation support for frontend code.

use std::ffi::c_int;

// libpq frontend type, not found in src/ - stub locally.
// TODO: dedup
pub type PGconn = std::ffi::c_void;

// sig_atomic_t from <signal.h>; on most platforms this is c_int.
pub type sig_atomic_t = c_int;

// extern PGDLLIMPORT volatile sig_atomic_t CancelRequested;
#[allow(non_upper_case_globals)]
pub static mut CancelRequested: sig_atomic_t = 0;

// extern void SetCancelConn(PGconn *conn);
pub unsafe fn SetCancelConn(conn: *mut PGconn) {
    let _ = conn;
    unimplemented!()
}

// extern void ResetCancelConn(void);
pub unsafe fn ResetCancelConn() {
    unimplemented!()
}

// extern void setup_cancel_handler(void (*query_cancel_callback) (void));
pub unsafe fn setup_cancel_handler(query_cancel_callback: Option<unsafe extern "C" fn()>) {
    let _ = query_cancel_callback;
    unimplemented!()
}
