//! Translated from PostgreSQL src/include/common/file_utils.h
//! Assorted file utilities. The fd-level I/O maps onto std at implementation
//! time; here we keep the enums, constants, and signatures.

use std::io::IoSlice;

/// Result of classifying a directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgFileType {
    Error,
    Unknown,
    Reg,
    Dir,
    Lnk,
}

/// How to sync the data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataDirSyncMethod {
    Fsync,
    Syncfs,
}

/// Filename components.
pub const PG_TEMP_FILES_DIR: &str = "pgsql_tmp";
pub const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";

// --- FRONTEND-only helpers (build the page cache / sync the data dir) ---

/// Advise the OS to start flushing `fname` before a later fsync. Err on failure.
pub fn pre_sync_fname(fname: &str, isdir: bool) -> Result<(), ()> {
    let _ = (fname, isdir);
    unimplemented!()
}

/// fsync a file or directory. Err on failure.
pub fn fsync_fname(fname: &str, isdir: bool) -> Result<(), ()> {
    let _ = (fname, isdir);
    unimplemented!()
}

/// Recursively sync the whole data directory.
pub fn sync_pgdata(
    pg_data: &str,
    server_version: i32,
    sync_method: DataDirSyncMethod,
    sync_data_files: bool,
) {
    let _ = (pg_data, server_version, sync_method, sync_data_files);
    unimplemented!()
}

/// Recursively sync a directory tree.
pub fn sync_dir_recurse(dir: &str, sync_method: DataDirSyncMethod) {
    let _ = (dir, sync_method);
    unimplemented!()
}

/// Durably rename a file. Err on failure.
pub fn durable_rename(oldfile: &str, newfile: &str) -> Result<(), ()> {
    let _ = (oldfile, newfile);
    unimplemented!()
}

/// fsync the parent directory of `fname`. Err on failure.
pub fn fsync_parent_path(fname: &str) -> Result<(), ()> {
    let _ = fname;
    unimplemented!()
}

/// Classify a directory entry, optionally following symlinks.
/// `elevel` is the elog level for reporting errors. Returns `Error` on failure.
pub fn get_dirent_type(
    path: &str,
    de_name: &str,
    look_through_symlinks: bool,
    elevel: i32,
) -> PgFileType {
    let _ = (path, de_name, look_through_symlinks, elevel);
    unimplemented!()
}

/// Compute the iovec slice remaining after `transferred` bytes were written.
pub fn compute_remaining_iovec<'a>(
    destination: &mut [IoSlice<'a>],
    source: &[IoSlice<'a>],
    transferred: usize,
) -> i32 {
    let _ = (destination, source, transferred);
    unimplemented!()
}

/// pwritev with retry on partial writes. Ok holds the number of bytes written.
pub fn pg_pwritev_with_retry(fd: i32, iov: &[IoSlice<'_>], offset: i64) -> std::io::Result<usize> {
    let _ = (fd, iov, offset);
    unimplemented!()
}

/// Write `size` zero bytes at `offset`. Ok holds the number of bytes written.
pub fn pg_pwrite_zeros(fd: i32, size: usize, offset: i64) -> std::io::Result<usize> {
    let _ = (fd, size, offset);
    unimplemented!()
}
