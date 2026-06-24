//! Translated from PostgreSQL src/include/nodes/readfuncs.h
//!
//! Internal to the `stringToNode` interface; not for general use.

use crate::nodes::nodes::Node;

/// C: `restore_location_fields` (under DEBUG_NODE_TESTS_ENABLED).
pub static mut RESTORE_LOCATION_FIELDS: bool = false;

/// C: `pg_strtok(int *length)` -- next token + its length, or None at end.
pub fn pg_strtok() -> Option<(*const u8, i32)> {
    unimplemented!()
}

/// C: `debackslash(token, length)` -- unescape a token.
pub fn debackslash(_token: &str, _length: i32) -> String {
    unimplemented!()
}

/// C: `nodeRead(token, tok_len)` -- parse one node from the token stream.
pub fn nodeRead(_token: &str, _tok_len: i32) -> *mut core::ffi::c_void {
    unimplemented!()
}

/// C: `parseNodeString()` -- dispatch on the leading node label.
pub fn parseNodeString() -> *mut Node {
    unimplemented!()
}
