//! Translated from PostgreSQL src/include/postmaster/interrupt.h

// volatile sig_atomic_t flags set from signal handlers.
pub static CONFIG_RELOAD_PENDING: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);
pub static SHUTDOWN_REQUEST_PENDING: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

pub fn process_main_loop_interrupts() {
    unimplemented!()
}

pub fn signal_handler_for_config_reload(_signo: i32) {
    unimplemented!()
}

pub fn signal_handler_for_crash_exit(_signo: i32) {
    unimplemented!()
}

pub fn signal_handler_for_shutdown_request(_signo: i32) {
    unimplemented!()
}
