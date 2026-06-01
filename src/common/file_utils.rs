//! common/file_utils.h - Assorted utility functions to work on files.

use crate::c::Size;
use std::ffi::{c_char, c_int};

// off_t / ssize_t are platform types; model as i64 / isize for the port.
// TODO: dedup - centralize off_t/ssize_t aliases if added elsewhere.
pub type off_t = i64;
pub type ssize_t = isize;

// struct dirent comes from <dirent.h>; opaque stub for prototypes.
// TODO: dedup
#[repr(C)]
pub struct dirent {
    _private: [u8; 0],
}

// struct iovec; /* avoid including port/pg_iovec.h here */
// TODO: dedup - real definition lives in port/pg_iovec.rs.
#[repr(C)]
pub struct iovec {
    _private: [u8; 0],
}

// typedef enum PGFileType
pub type PGFileType = c_int;
pub const PGFILETYPE_ERROR: PGFileType = 0;
pub const PGFILETYPE_UNKNOWN: PGFileType = 1;
pub const PGFILETYPE_REG: PGFileType = 2;
pub const PGFILETYPE_DIR: PGFileType = 3;
pub const PGFILETYPE_LNK: PGFileType = 4;

// typedef enum DataDirSyncMethod
pub type DataDirSyncMethod = c_int;
pub const DATA_DIR_SYNC_METHOD_FSYNC: DataDirSyncMethod = 0;
pub const DATA_DIR_SYNC_METHOD_SYNCFS: DataDirSyncMethod = 1;

// #ifdef FRONTEND prototypes - kept unconditionally for the port.
pub unsafe fn pre_sync_fname(fname: *const c_char, isdir: bool) -> c_int {
    unimplemented!()
}

pub unsafe fn fsync_fname(fname: *const c_char, isdir: bool) -> c_int {
    unimplemented!()
}

pub unsafe fn sync_pgdata(
    pg_data: *const c_char,
    serverVersion: c_int,
    sync_method: DataDirSyncMethod,
    sync_data_files: bool,
) {
    unimplemented!()
}

pub unsafe fn sync_dir_recurse(dir: *const c_char, sync_method: DataDirSyncMethod) {
    unimplemented!()
}

pub unsafe fn durable_rename(oldfile: *const c_char, newfile: *const c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn fsync_parent_path(fname: *const c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn get_dirent_type(
    path: *const c_char,
    de: *const dirent,
    look_through_symlinks: bool,
    elevel: c_int,
) -> PGFileType {
    unimplemented!()
}

pub unsafe fn compute_remaining_iovec(
    destination: *mut iovec,
    source: *const iovec,
    iovcnt: c_int,
    transferred: Size,
) -> c_int {
    unimplemented!()
}

pub unsafe fn pg_pwritev_with_retry(
    fd: c_int,
    iov: *const iovec,
    iovcnt: c_int,
    offset: off_t,
) -> ssize_t {
    unimplemented!()
}

pub unsafe fn pg_pwrite_zeros(fd: c_int, size: Size, offset: off_t) -> ssize_t {
    unimplemented!()
}

// Filename components
pub const PG_TEMP_FILES_DIR: &str = "pgsql_tmp";
pub const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";
