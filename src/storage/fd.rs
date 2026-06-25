//! Translated from PostgreSQL src/include/storage/fd.h

use std::io::{IoSlice, IoSliceMut};

use bitflags::bitflags;

use crate::c::SubTransactionId;
use crate::postgres_ext::Oid;
use crate::storage::aio_internal::PgAioHandle;

/// A virtual file descriptor (index into fd.c's VFD table).
pub type File = i32;

bitflags! {
    /// io_direct_flags: which categories of I/O bypass the OS page cache.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IoDirectFlags: i32 {
        const DATA     = 0x01;
        const WAL      = 0x02;
        const WAL_INIT = 0x04;
    }
}

/// How FileFallocate-style extension is performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FileExtendMethod {
    // posix_fallocate is available on Linux; macOS falls back to write-zeros.
    PosixFallocate = 0,
    WriteZeros = 1,
}

pub const DEFAULT_FILE_EXTEND_METHOD: i32 = 0;

// GUC parameters (process globals; to become session/global state later).
pub static mut max_files_per_process: i32 = 0;
pub static mut data_sync_retry: bool = false;
pub static mut recovery_init_sync_method: i32 = 0;
pub static mut io_direct_flags: i32 = 0;
pub static mut file_extend_method: i32 = 0;
pub static mut max_safe_fds: i32 = 0;

// ENOENT value (errno.h is identical across Linux/macOS for this code).
const ENOENT: i32 = 2;

/// True iff err indicates a possibly-deleted file (ENOENT). On non-Windows only.
pub const fn file_possibly_deleted(err: i32) -> bool {
    err == ENOENT
}

// O_DIRECT handling: Linux uses O_DIRECT (0o40000); macOS has no O_DIRECT and
// simulates it with fcntl(F_NOCACHE), so PG uses a sentinel high bit instead.
#[cfg(target_os = "linux")]
pub const PG_O_DIRECT: i32 = 0o40000;
#[cfg(target_os = "macos")]
pub const PG_O_DIRECT: i32 = 0x80000000u32 as i32;

// Operations on virtual Files --- equivalent to Unix kernel file ops.
pub fn PathNameOpenFile(_file_name: &str, _file_flags: i32) -> File {
    unimplemented!()
}

pub fn PathNameOpenFilePerm(_file_name: &str, _file_flags: i32, _file_mode: u32) -> File {
    unimplemented!()
}

pub fn OpenTemporaryFile(_inter_xact: bool) -> File {
    unimplemented!()
}

pub fn FileClose(_file: File) {
    unimplemented!()
}

pub fn FilePrefetch(_file: File, _offset: i64, _amount: i64, _wait_event_info: u32) -> i32 {
    unimplemented!()
}

pub fn FileReadV(
    _file: File,
    _iov: &mut [IoSliceMut],
    _offset: i64,
    _wait_event_info: u32,
) -> isize {
    unimplemented!()
}

pub fn FileWriteV(_file: File, _iov: &[IoSlice], _offset: i64, _wait_event_info: u32) -> isize {
    unimplemented!()
}

pub fn FileStartReadV(
    _ioh: &mut PgAioHandle,
    _file: File,
    _iovcnt: i32,
    _offset: i64,
    _wait_event_info: u32,
) -> i32 {
    unimplemented!()
}

pub fn FileSync(_file: File, _wait_event_info: u32) -> i32 {
    unimplemented!()
}

pub fn FileZero(_file: File, _offset: i64, _amount: i64, _wait_event_info: u32) -> i32 {
    unimplemented!()
}

pub fn FileFallocate(_file: File, _offset: i64, _amount: i64, _wait_event_info: u32) -> i32 {
    unimplemented!()
}

pub fn FileSize(_file: File) -> i64 {
    unimplemented!()
}

pub fn FileTruncate(_file: File, _offset: i64, _wait_event_info: u32) -> i32 {
    unimplemented!()
}

pub fn FileWriteback(_file: File, _offset: i64, _nbytes: i64, _wait_event_info: u32) {
    unimplemented!()
}

pub fn FilePathName(_file: File) -> String {
    unimplemented!()
}

pub fn FileGetRawDesc(_file: File) -> i32 {
    unimplemented!()
}

pub fn FileGetRawFlags(_file: File) -> i32 {
    unimplemented!()
}

pub fn FileGetRawMode(_file: File) -> u32 {
    unimplemented!()
}

// Operations used for sharing named temporary files.
pub fn PathNameCreateTemporaryFile(_path: &str, _error_on_failure: bool) -> File {
    unimplemented!()
}

pub fn PathNameOpenTemporaryFile(_path: &str, _mode: i32) -> File {
    unimplemented!()
}

pub fn PathNameDeleteTemporaryFile(_path: &str, _error_on_failure: bool) -> bool {
    unimplemented!()
}

pub fn PathNameCreateTemporaryDir(_basedir: &str, _directory: &str) {
    unimplemented!()
}

pub fn PathNameDeleteTemporaryDir(_dirname: &str) {
    unimplemented!()
}

pub fn TempTablespacePath(_path: &mut str, _tablespace: Oid) {
    unimplemented!()
}

// Operations that allow use of regular stdio --- USE WITH CAUTION.
// FILE* maps to a std::fs::File handle stub; modeled as opaque for the skeleton.
pub fn AllocateFile(_name: &str, _mode: &str) -> Option<std::fs::File> {
    unimplemented!()
}

pub fn FreeFile(_file: std::fs::File) -> i32 {
    unimplemented!()
}

pub fn OpenPipeStream(_command: &str, _mode: &str) -> Option<std::fs::File> {
    unimplemented!()
}

pub fn ClosePipeStream(_file: std::fs::File) -> i32 {
    unimplemented!()
}

// Operations to allow use of the <dirent.h> library routines.
// DIR*/struct dirent map to std::fs::ReadDir / DirEntry stubs.
pub fn AllocateDir(_dirname: &str) -> Option<std::fs::ReadDir> {
    unimplemented!()
}

pub fn ReadDir(_dir: &mut std::fs::ReadDir, _dirname: &str) -> Option<std::fs::DirEntry> {
    unimplemented!()
}

pub fn ReadDirExtended(
    _dir: &mut std::fs::ReadDir,
    _dirname: &str,
    _elevel: i32,
) -> Option<std::fs::DirEntry> {
    unimplemented!()
}

pub fn FreeDir(_dir: std::fs::ReadDir) -> i32 {
    unimplemented!()
}

// Operations to allow use of a plain kernel FD, with automatic cleanup.
pub fn OpenTransientFile(_file_name: &str, _file_flags: i32) -> i32 {
    unimplemented!()
}

pub fn OpenTransientFilePerm(_file_name: &str, _file_flags: i32, _file_mode: u32) -> i32 {
    unimplemented!()
}

pub fn CloseTransientFile(_fd: i32) -> i32 {
    unimplemented!()
}

// If you've really really gotta have a plain kernel FD, use this.
pub fn BasicOpenFile(_file_name: &str, _file_flags: i32) -> i32 {
    unimplemented!()
}

pub fn BasicOpenFilePerm(_file_name: &str, _file_flags: i32, _file_mode: u32) -> i32 {
    unimplemented!()
}

pub fn AcquireExternalFD() -> bool {
    unimplemented!()
}

pub fn ReserveExternalFD() {
    unimplemented!()
}

pub fn ReleaseExternalFD() {
    unimplemented!()
}

pub fn MakePGDirectory(_directory_name: &str) -> i32 {
    unimplemented!()
}

// Miscellaneous support routines.
pub fn InitFileAccess() {
    unimplemented!()
}

pub fn InitTemporaryFileAccess() {
    unimplemented!()
}

pub fn set_max_safe_fds() {
    unimplemented!()
}

pub fn closeAllVfds() {
    unimplemented!()
}

pub fn SetTempTablespaces(_table_spaces: &[Oid]) {
    unimplemented!()
}

pub fn TempTablespacesAreSet() -> bool {
    unimplemented!()
}

pub fn GetTempTablespaces(_table_spaces: &mut [Oid]) -> i32 {
    unimplemented!()
}

pub fn GetNextTempTableSpace() -> Oid {
    unimplemented!()
}

pub fn AtEOXact_Files(_is_commit: bool) {
    unimplemented!()
}

pub fn AtEOSubXact_Files(_is_commit: bool, _my_subid: SubTransactionId, _parent_subid: SubTransactionId) {
    unimplemented!()
}

pub fn RemovePgTempFiles() {
    unimplemented!()
}

pub fn RemovePgTempFilesInDir(_tmpdirname: &str, _missing_ok: bool, _unlink_all: bool) {
    unimplemented!()
}

pub fn looks_like_temp_rel_name(_name: &str) -> bool {
    unimplemented!()
}

pub fn pg_fsync(_fd: i32) -> i32 {
    unimplemented!()
}

pub fn pg_fsync_no_writethrough(_fd: i32) -> i32 {
    unimplemented!()
}

pub fn pg_fsync_writethrough(_fd: i32) -> i32 {
    unimplemented!()
}

pub fn pg_fdatasync(_fd: i32) -> i32 {
    unimplemented!()
}

pub fn pg_file_exists(_name: &str) -> bool {
    unimplemented!()
}

pub fn pg_flush_data(_fd: i32, _offset: i64, _nbytes: i64) {
    unimplemented!()
}

pub fn pg_truncate(_path: &str, _length: i64) -> i32 {
    unimplemented!()
}

pub fn fsync_fname(_fname: &str, _isdir: bool) {
    unimplemented!()
}

pub fn fsync_fname_ext(_fname: &str, _isdir: bool, _ignore_perm: bool, _elevel: i32) -> i32 {
    unimplemented!()
}

pub fn durable_rename(_oldfile: &str, _newfile: &str, _elevel: i32) -> i32 {
    unimplemented!()
}

pub fn durable_unlink(_fname: &str, _elevel: i32) -> i32 {
    unimplemented!()
}

pub fn SyncDataDirectory() {
    unimplemented!()
}

pub fn data_sync_elevel(_elevel: i32) -> i32 {
    unimplemented!()
}

/// Read `buffer.len()` bytes at `offset` (single-iovec wrapper over FileReadV).
pub fn FileRead(file: File, buffer: &mut [u8], offset: i64, wait_event_info: u32) -> isize {
    let mut iov = [IoSliceMut::new(buffer)];
    FileReadV(file, &mut iov, offset, wait_event_info)
}

/// Write `buffer.len()` bytes at `offset` (single-iovec wrapper over FileWriteV).
pub fn FileWrite(file: File, buffer: &[u8], offset: i64, wait_event_info: u32) -> isize {
    let iov = [IoSlice::new(buffer)];
    FileWriteV(file, &iov, offset, wait_event_info)
}
