//! Translated from PostgreSQL src/include/fe_utils/cancel.h

// PGconn is the libpq client connection handle (interfaces/libpq/libpq-fe.h),
// not part of this batch.
// TODO(struct-forward): repoint to crate::interfaces::libpq::PGconn in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGconn in Phase 2")]
pub struct PGconn {
    _private: (),
}

// volatile sig_atomic_t set from the SIGINT handler.
pub static CANCEL_REQUESTED: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

#[allow(deprecated)]
pub fn set_cancel_conn(_conn: &PGconn) {
    unimplemented!()
}

pub fn reset_cancel_conn() {
    unimplemented!()
}

/// Optionally set a callback invoked at cancellation time. The C `void *arg`
/// pattern collapses into a captured closure.
pub fn setup_cancel_handler(_query_cancel_callback: impl Fn()) {
    unimplemented!()
}
