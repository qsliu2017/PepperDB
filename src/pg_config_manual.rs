//! Translated from PostgreSQL src/include/pg_config_manual.h
//! Hand-written configuration symbols and limits.

/// Default wal_segment_size when initdb runs without --wal-segsize (16MB).
pub const DEFAULT_XLOG_SEG_SIZE: u32 = 16 * 1024 * 1024;

/// Max identifier length incl. trailing NUL (changing requires initdb).
pub const NAMEDATALEN: usize = 64;

/// Max number of arguments to a function (full backend recompile to change).
pub const FUNC_MAX_ARGS: usize = 100;

/// ABI-extra string, surfaced on module ABI mismatch.
pub const FMGR_ABI_EXTRA: &str = "PostgreSQL";

/// Max number of columns in an index (changing requires initdb).
pub const INDEX_MAX_KEYS: usize = 32;

/// Max number of columns in a partition key.
pub const PARTITION_MAX_KEYS: usize = 32;

/// Standard pathname buffer size; usable length is one less.
pub const MAXPGPATH: usize = 1024;

pub const BITS_PER_BYTE: usize = 8;

/// Preferred alignment for disk I/O buffers.
pub const ALIGNOF_BUFFER: usize = 32;

/// Upper limit for backend/bgwriter/checkpoint flush_after (in blocks).
pub const WRITEBACK_MAX_PENDING_FLUSHES: u32 = 256;

/// Default AF_UNIX socket directory (non-Windows targets).
pub const DEFAULT_PGSOCKET_DIR: &str = "/tmp";

/// Default event source for Windows event log.
pub const DEFAULT_EVENT_SOURCE: &str = "PostgreSQL";

/// Assumed cache line size, used for struct padding.
pub const PG_CACHE_LINE_SIZE: usize = 128;

/// Assumed alignment requirement for direct I/O.
pub const PG_IO_ALIGN_SIZE: usize = 4096;

// Linux enables sync_file_range-based flush defaults; macOS does not. The
// remaining symbols in this header are compile-time #ifdef debugging toggles
// (USE_VALGRIND, CLOBBER_FREED_MEMORY, WAL_DEBUG, ...) mapped to Rust cfg
// features in a later phase, so they are not translated as consts here.
#[cfg(target_os = "linux")]
pub const DEFAULT_BACKEND_FLUSH_AFTER: u32 = 0; // never enabled by default
#[cfg(target_os = "linux")]
pub const DEFAULT_BGWRITER_FLUSH_AFTER: u32 = 64;
#[cfg(target_os = "linux")]
pub const DEFAULT_CHECKPOINT_FLUSH_AFTER: u32 = 32;
#[cfg(not(target_os = "linux"))]
pub const DEFAULT_BACKEND_FLUSH_AFTER: u32 = 0;
#[cfg(not(target_os = "linux"))]
pub const DEFAULT_BGWRITER_FLUSH_AFTER: u32 = 0;
#[cfg(not(target_os = "linux"))]
pub const DEFAULT_CHECKPOINT_FLUSH_AFTER: u32 = 0;
