//! Translated from PostgreSQL src/include/postmaster/startup.h

// GUC option.
pub static mut LOG_STARTUP_PROGRESS_INTERVAL: i32 = 0;

pub fn process_startup_proc_interrupts() {
    unimplemented!()
}

pub fn startup_process_main(_startup_data: &[u8]) -> ! {
    unimplemented!()
}

pub fn pre_restore_command() {
    unimplemented!()
}

pub fn post_restore_command() {
    unimplemented!()
}

pub fn is_promote_signaled() -> bool {
    unimplemented!()
}

pub fn reset_promote_signaled() {
    unimplemented!()
}

pub fn enable_startup_progress_timeout() {
    unimplemented!()
}

pub fn disable_startup_progress_timeout() {
    unimplemented!()
}

pub fn begin_startup_progress_phase() {
    unimplemented!()
}

pub fn startup_progress_timeout_handler() {
    unimplemented!()
}

/// C: `bool has_startup_progress_timeout_expired(long *secs, int *usecs)`.
/// Returns `Some((secs, usecs))` when the timer expired, `None` otherwise.
pub fn has_startup_progress_timeout_expired() -> Option<(i64, i32)> {
    unimplemented!()
}
