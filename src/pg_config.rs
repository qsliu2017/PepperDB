//! Translation of the configure-generated `pg_config.h` plus the hand-maintained
//! `pg_config_manual.h` (postgres/src/include/pg_config_manual.h).
//!
//! In C these are produced by `configure`/`meson` for the host platform. We hard-code
//! the values for a standard 64-bit little-endian build (the common case), which is
//! what the rest of the port assumes. Platform-specific `#ifdef` branches in the C
//! source are resolved to this configuration.

// ---- Sizes and alignments (configure: AC_CHECK_SIZEOF / alignment probes) ----
pub const SIZEOF_VOID_P: usize = 8;
pub const SIZEOF_LONG: usize = 8;
pub const SIZEOF_SIZE_T: usize = 8;
pub const SIZEOF_DATUM: usize = SIZEOF_VOID_P;

pub const ALIGNOF_SHORT: usize = 2;
pub const ALIGNOF_INT: usize = 4;
pub const ALIGNOF_LONG: usize = 8;
pub const ALIGNOF_DOUBLE: usize = 8;
pub const MAXIMUM_ALIGNOF: usize = 8;
/// Buffers (shared-buffer pages) are aligned to this. PostgreSQL default.
pub const ALIGNOF_BUFFER: usize = 32;
pub const PG_CACHE_LINE_SIZE: usize = 128;
pub const PG_IO_ALIGN_SIZE: usize = 4096;

// ---- Pass-by-value choices (configure: USE_FLOAT8_BYVAL on 64-bit) ----
pub const USE_FLOAT8_BYVAL: bool = true;

// ---- pg_config_manual.h: compile-time tunables ----

/// Maximum length for identifiers (e.g. table names, column names, function names).
/// Includes the trailing NUL, so the effective limit is NAMEDATALEN - 1 characters.
pub const NAMEDATALEN: usize = 64;

/// Size of a disk block / a buffer-pool page, in bytes.
pub const BLCKSZ: usize = 8192;

/// Size of a WAL (xlog) block, in bytes.
pub const XLOG_BLCKSZ: usize = 8192;

/// Number of bits used in a relation segment's block number; segment size = RELSEG_SIZE.
pub const RELSEG_SIZE: usize = 131072;

/// Index tuples/values per page limits are derived elsewhere; kept minimal here.
pub const PG_MAJORVERSION: &str = "18";
pub const PG_VERSION: &str = "18.3";
pub const PG_VERSION_NUM: u32 = 180003;

/// `USE_ASSERT_CHECKING` in C is mirrored by Rust's `cfg!(debug_assertions)`;
/// see [`crate::c`]'s `Assert!` macro. This constant exposes the same flag for
/// code that branches on it directly.
pub const USE_ASSERT_CHECKING: bool = cfg!(debug_assertions);
