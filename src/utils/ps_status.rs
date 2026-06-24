//! Translated from PostgreSQL src/include/utils/ps_status.h
//
// Process-title (ps display) support. Non-Windows target, so the default is always
// true. `set_ps_display` is inline in the header and translated in full.

pub const DEFAULT_UPDATE_PROCESS_TITLE: bool = true;

// Process global. TODO(global): move to Session/task-local state.
pub static mut update_process_title: bool = DEFAULT_UPDATE_PROCESS_TITLE;

pub fn save_ps_display_args(_args: Vec<String>) -> Vec<String> {
    unimplemented!()
}

pub fn init_ps_display(_fixed_part: &str) {
    unimplemented!()
}

pub fn set_ps_display_suffix(_suffix: &str) {
    unimplemented!()
}

pub fn set_ps_display_remove_suffix() {
    unimplemented!()
}

pub fn set_ps_display_with_len(_activity: &str, _len: usize) {
    unimplemented!()
}

/// Inlined in C so strlen folds for string constants; here just forward the len.
pub fn set_ps_display(activity: &str) {
    set_ps_display_with_len(activity, activity.len())
}

/// Returns the current display string and its length.
pub fn get_ps_display() -> (String, i32) {
    unimplemented!()
}
