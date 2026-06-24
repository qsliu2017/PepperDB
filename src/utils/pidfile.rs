//! Translated from PostgreSQL src/include/utils/pidfile.h
//
// Layout of the data-directory lock file (postmaster.pid): line-number constants and
// the PM_STATUS strings. Plain consts; the status strings are padded to equal length
// in C (per AddToDataDirLockFile), so keep the exact bytes including trailing spaces.

pub const LOCK_FILE_LINE_PID: i32 = 1;
pub const LOCK_FILE_LINE_DATA_DIR: i32 = 2;
pub const LOCK_FILE_LINE_START_TIME: i32 = 3;
pub const LOCK_FILE_LINE_PORT: i32 = 4;
pub const LOCK_FILE_LINE_SOCKET_DIR: i32 = 5;
pub const LOCK_FILE_LINE_LISTEN_ADDR: i32 = 6;
pub const LOCK_FILE_LINE_SHMEM_KEY: i32 = 7;
pub const LOCK_FILE_LINE_PM_STATUS: i32 = 8;

// All PM_STATUS strings must be equal length; padded with spaces.
pub const PM_STATUS_STARTING: &str = "starting";
pub const PM_STATUS_STOPPING: &str = "stopping";
pub const PM_STATUS_READY: &str = "ready   ";
pub const PM_STATUS_STANDBY: &str = "standby ";
