//! Translated from PostgreSQL src/include/common/connect.h

/// Installs an always-secure search path to prevent privilege hijacking.
pub const ALWAYS_SECURE_SEARCH_PATH_SQL: &str =
    "SELECT pg_catalog.set_config('search_path', '', false);";
