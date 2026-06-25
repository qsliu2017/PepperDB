//! Translated from PostgreSQL src/include/postmaster/walwriter.h

// DEFAULT_WAL_WRITER_FLUSH_AFTER = (1 MiB) / XLOG_BLCKSZ.
pub const DEFAULT_WAL_WRITER_FLUSH_AFTER: i32 =
    (1024 * 1024) / crate::pg_config::XLOG_BLCKSZ as i32;

// GUC options.
pub static mut WAL_WRITER_DELAY: i32 = 0;
pub static mut WAL_WRITER_FLUSH_AFTER: i32 = 0;

pub fn wal_writer_main(_startup_data: &[u8]) -> ! {
    unimplemented!()
}
