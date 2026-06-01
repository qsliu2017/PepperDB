//! Base-backup subsystem (postgres/src/backend/backup + postgres/src/include/backup).
//!
//! So far: the output-sink filter chain (`basebackup_sink`) and the
//! target-handler registry (`basebackup_target`).

pub mod backup_manifest;
pub mod basebackup_copy;
pub mod basebackup_gzip;
pub mod basebackup_lz4;
pub mod basebackup_progress;
pub mod basebackup_server;
pub mod basebackup_sink;
pub mod basebackup_zstd;
pub mod basebackup_target;
pub mod basebackup_throttle;
pub mod walsummary;
pub mod walsummaryfuncs;
pub mod basebackup_incremental;
