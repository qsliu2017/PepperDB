//! Translated from PostgreSQL src/include/postmaster/startup.h
//!
//! Non-type-centric header: free functions over the startup-process flags, so this
//! re-exports the backend implementation (`pub use`) under the C names rather than
//! holding deprecated shims. Bodies live in `crate::backend::postmaster::startup`.

pub use crate::backend::postmaster::startup::{
    begin_startup_progress_phase, disable_startup_progress_timeout,
    enable_startup_progress_timeout, has_startup_progress_timeout_expired,
    is_promote_signaled as IsPromoteSignaled, post_restore_command as PostRestoreCommand,
    pre_restore_command as PreRestoreCommand,
    process_startup_proc_interrupts as ProcessStartupProcInterrupts,
    reset_promote_signaled as ResetPromoteSignaled,
    startup_process_main as StartupProcessMain, startup_progress_timeout_handler,
    wakeup_recovery as WakeupRecovery,
};

/// PG GUC `log_startup_progress_interval` accessor/setter (the C global is a
/// `static` atomic in the backend module; expose it here for header consumers).
pub use crate::backend::postmaster::startup::{
    log_startup_progress_interval, set_log_startup_progress_interval,
};
