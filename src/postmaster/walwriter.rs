//! Translated from PostgreSQL src/include/postmaster/walwriter.h

// DEFAULT_WAL_WRITER_FLUSH_AFTER = (1 MiB) / XLOG_BLCKSZ.
pub const DEFAULT_WAL_WRITER_FLUSH_AFTER: i32 =
    (1024 * 1024) / crate::pg_config::XLOG_BLCKSZ as i32;

// GUC options. PG declares these in walwriter.h; the GUC copies live as
// process-global atomics in walwriter.rs (step 17b) with accessor functions (no
// `static mut`).

/// PG `WalWriterDelay` GUC accessor (backed by walwriter.rs, step 17b).
pub use crate::backend::postmaster::walwriter::wal_writer_delay as WalWriterDelay;
/// PG `WalWriterFlushAfter` GUC accessor (backed by walwriter.rs, step 17b).
pub use crate::backend::postmaster::walwriter::wal_writer_flush_after as WalWriterFlushAfter;

/// PG `WalWriterMain` - the long-lived walwriter aux task (step 17b).
pub use crate::backend::postmaster::walwriter::wal_writer_main as WalWriterMain;
