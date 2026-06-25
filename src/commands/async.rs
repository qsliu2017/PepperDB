//! Translated from PostgreSQL src/include/commands/async.h
//
// Asynchronous notification: NOTIFY, LISTEN, UNLISTEN.

use crate::c::TransactionId;

// GUCs.
pub static mut TRACE_NOTIFY: bool = false;
pub static mut MAX_NOTIFY_QUEUE_PAGES: i32 = 0;

// volatile sig_atomic_t set from the notify signal handler.
pub static NOTIFY_INTERRUPT_PENDING: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

pub fn async_shmem_size() -> usize {
    unimplemented!()
}

pub fn async_shmem_init() {
    unimplemented!()
}

pub fn notify_my_front_end(_channel: &str, _payload: &str, _src_pid: i32) {
    unimplemented!()
}

// notify-related SQL statements.
pub fn async_notify(_channel: &str, _payload: &str) {
    unimplemented!()
}

pub fn async_listen(_channel: &str) {
    unimplemented!()
}

pub fn async_unlisten(_channel: &str) {
    unimplemented!()
}

pub fn async_unlisten_all() {
    unimplemented!()
}

// perform (or cancel) outbound notify processing at transaction commit.
pub fn pre_commit_notify() {
    unimplemented!()
}

pub fn at_commit_notify() {
    unimplemented!()
}

pub fn at_abort_notify() {
    unimplemented!()
}

pub fn at_sub_commit_notify() {
    unimplemented!()
}

pub fn at_sub_abort_notify() {
    unimplemented!()
}

pub fn at_prepare_notify() {
    unimplemented!()
}

pub fn handle_notify_interrupt() {
    unimplemented!()
}

pub fn process_notify_interrupt(_flush: bool) {
    unimplemented!()
}

pub fn async_notify_freeze_xids(_new_frozen_xid: TransactionId) {
    unimplemented!()
}
