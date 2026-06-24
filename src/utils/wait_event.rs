//! Translated from PostgreSQL src/include/utils/wait_event.h
//!
//! Wait event reporting. The wait-event ENUMS come from the generated
//! wait_event_types (build.rs over wait_event_names.txt) - the only import cycle
//! with this header, so both collapse into one utils::wait_event module
//! (generated-header-files.md). This file is the hand-written API; the generated
//! enum/name tables are pulled in from crate::utils::wait_event_types for now.

// The wait-event info word is 4 bytes: byte 0 = wait class, bytes 1-2 = event,
// byte 3 reserved. Stored in a process-global pointer; under the async model this
// becomes task-local state, kept here as a static for the skeleton.
pub static mut my_wait_event_info: *mut u32 = core::ptr::null_mut();

pub fn pgstat_get_wait_event(wait_event_info: u32) -> Option<&'static str> {
    unimplemented!()
}
pub fn pgstat_get_wait_event_type(wait_event_info: u32) -> Option<&'static str> {
    unimplemented!()
}

/// Report start of a wait: store the 4-byte info word (read/written atomically).
#[inline]
pub fn pgstat_report_wait_start(wait_event_info: u32) {
    unsafe {
        core::ptr::write_volatile(my_wait_event_info, wait_event_info);
    }
}

/// Report end of a wait.
#[inline]
pub fn pgstat_report_wait_end() {
    unsafe {
        core::ptr::write_volatile(my_wait_event_info, 0);
    }
}

pub fn pgstat_set_wait_event_storage(wait_event_info: *mut u32) {
    unimplemented!()
}
pub fn pgstat_reset_wait_event_storage() {
    unimplemented!()
}

// Extension / InjectionPoint custom wait events.
pub fn WaitEventExtensionNew(wait_event_name: &str) -> u32 {
    unimplemented!()
}
pub fn WaitEventInjectionPointNew(wait_event_name: &str) -> u32 {
    unimplemented!()
}

// Custom wait-event shared state -> Arc-shared single-process state later.
pub fn WaitEventCustomShmemInit() {
    unimplemented!()
}
pub fn WaitEventCustomShmemSize() -> usize {
    unimplemented!()
}
/// `int *nwaitevents` out-param folded into the returned Vec.
pub fn GetWaitEventCustomNames(class_id: u32) -> Vec<String> {
    unimplemented!()
}
