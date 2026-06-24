//! Translated from PostgreSQL src/include/utils/rls.h
//
// Row-Level Security helpers. CheckEnableRlsResult is a small ordinal enum returned
// directly by check_enable_rls (it is the result value, not a fallible status).

use crate::postgres_ext::Oid;

// GUC variable (process global). TODO(global): Session-thread.
pub static mut row_security: bool = false;

/// Result of check_enable_rls.
pub enum CheckEnableRlsResult {
    None,
    NoneEnv,
    Enabled,
}

pub fn check_enable_rls(
    _relid: Oid,
    _check_as_user: Oid,
    _no_error: bool,
) -> CheckEnableRlsResult {
    unimplemented!()
}
