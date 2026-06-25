//! Translated from PostgreSQL src/include/postmaster/interrupt.h

// volatile sig_atomic_t flags set from signal handlers.
pub static CONFIG_RELOAD_PENDING: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);
pub static SHUTDOWN_REQUEST_PENDING: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

// Non-type-centric (free functions over the flags): the definitions live in the
// backend module; re-export them here.
pub use crate::backend::postmaster::interrupt::{
    process_main_loop_interrupts, signal_handler_for_config_reload,
    signal_handler_for_crash_exit, signal_handler_for_shutdown_request,
};

#[cfg(unix)]
pub use crate::backend::postmaster::interrupt::install_signal_handlers;
