//! Translated from PostgreSQL src/include/utils/usercontext.h
//
// Run code as a different database user. UserContext saves the state to restore.
// In-memory; idiomatic struct. SwitchToUntrustedUser fills the context out-param ->
// here it returns the saved context.

use crate::postgres_ext::Oid;

/// Saved state for restoring the original user after a temporary switch.
pub struct UserContext {
    pub userid: Oid,
    pub sec_context: i32,
    pub nestlevel: i32,
}

/// Switch to `userid`, returning the context needed to restore the prior state.
pub fn SwitchToUntrustedUser(_userid: Oid) -> UserContext {
    unimplemented!()
}

pub fn RestoreUserContext(_context: &UserContext) {
    unimplemented!()
}
