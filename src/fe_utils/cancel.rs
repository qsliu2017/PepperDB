//! Translated from PostgreSQL src/include/fe_utils/cancel.h

/// Opaque frontend libpq handle; client lib not ported.
pub struct PGconn {
    _private: (),
}

// volatile sig_atomic_t set from the SIGINT handler.
pub static CANCEL_REQUESTED: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

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
