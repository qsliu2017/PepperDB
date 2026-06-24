//! Translated from PostgreSQL src/include/common/restricted_token.h
// CreateRestrictedProcess is WIN32-only; dropped (targets: Linux x86_64, macOS aarch64).

/// On Windows ensure a restricted token; on other platforms a no-op.
pub fn get_restricted_token() {
    unimplemented!()
}
