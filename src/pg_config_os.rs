//! Translated from PostgreSQL src/include/pg_config_os.h
//!
//! Tombstone. pg_config_os.h is a per-platform shim selected by the build (it
//! is `#include "port/<template>.h"`), defining OS-specific macros for headers,
//! socket/path quirks, and signal handling. For the Linux x86_64 + macOS
//! aarch64 targets these are covered by Rust std and `#[cfg(target_os = ...)]`,
//! so nothing carries over.
