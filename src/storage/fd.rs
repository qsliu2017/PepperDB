//! Translated from PostgreSQL src/include/storage/fd.h
//!
//! The VFD layer's types and behavior live in the backend definition module
//! (`crate::backend::storage::file::fd`): `File` is now a generational handle
//! (not `i32`), the VFD pool is `FdManager` over the async `IoBackend`, and
//! transient/dir/temp resources are RAII guards. This header keeps the
//! header-origin GUC globals + bitflags + enums and re-exports the backend
//! types; the C-named free functions remain as `#[deprecated]` `#[inline]`
//! async shims that delegate to the methods (taking the `FdManager`/`File` as a
//! parameter where there is no `self`; the manager comes from `SharedState`
//! later).

use std::io::{IoSlice, IoSliceMut};
use std::sync::Arc;

use bitflags::bitflags;

use crate::postgres_ext::Oid;
use crate::storage::io_backend::OpenFlags;

// The VFD layer types (definitions in the backend module). `File` REPLACES the
// old `File = i32`: a stale handle fails safely via its generational key.
pub use crate::backend::storage::file::fd::{DirScan, FdManager, File, TempFile, TransientFile};

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

// ---------------------------------------------------------------------------
// Deprecated C-named shims. The behavior is on `File`/`FdManager`; these remain
// for mechanical-port cross-reference. Async because the I/O leaf is async.
//
// Deleted by redesign (no shim): Windows paths, sync_file_range, posix_fadvise
// portability, F_NOCACHE, the data_sync_retry retry loop (we abort), EXEC_BACKEND
// fd inheritance, the dup()-based AllocateFile FILE*/AllocateDir registry beyond
// what the RAII guards (TransientFile/DirScan/TempFile) need, and the external-FD
// reservation accounting (the IoBackend semaphore is the single budget).
// ---------------------------------------------------------------------------

#[deprecated(note = "use `mgr.open(path, flags)`")]
#[inline]
pub async fn PathNameOpenFile(
    mgr: &Arc<FdManager>,
    file_name: &str,
    flags: OpenFlags,
) -> std::io::Result<File> {
    mgr.open(file_name, flags).await
}

#[deprecated(note = "use `mgr.open(path, flags)` (mode is applied via flags)")]
#[inline]
pub async fn PathNameOpenFilePerm(
    mgr: &Arc<FdManager>,
    file_name: &str,
    flags: OpenFlags,
    _file_mode: u32,
) -> std::io::Result<File> {
    mgr.open(file_name, flags).await
}

#[deprecated(note = "use `mgr.open_temporary_file()`")]
#[inline]
pub async fn OpenTemporaryFile(mgr: &Arc<FdManager>, _inter_xact: bool) -> std::io::Result<TempFile> {
    mgr.open_temporary_file().await
}

#[deprecated(note = "drop the `File` (Drop closes the fd + frees the slot)")]
#[inline]
pub fn FileClose(file: File) {
    file.close();
}

#[deprecated(note = "use `file.prefetch(offset, amount)`")]
#[inline]
pub fn FilePrefetch(file: &File, offset: u64, amount: u64, _wait_event_info: u32) {
    file.prefetch(offset, amount);
}

#[deprecated(note = "use `file.read_v(iov, offset)`")]
#[inline]
pub async fn FileReadV(
    file: &File,
    iov: &mut [IoSliceMut<'_>],
    offset: u64,
    _wait_event_info: u32,
) -> std::io::Result<usize> {
    file.read_v(iov, offset).await
}

#[deprecated(note = "use `file.write_v(iov, offset)`")]
#[inline]
pub async fn FileWriteV(
    file: &File,
    iov: &[IoSlice<'_>],
    offset: u64,
    _wait_event_info: u32,
) -> std::io::Result<usize> {
    file.write_v(iov, offset).await
}

#[deprecated(note = "use `file.sync()` (aborts on fsync failure)")]
#[inline]
pub async fn FileSync(file: &File, _wait_event_info: u32) -> std::io::Result<()> {
    file.sync().await
}

#[deprecated(note = "use `file.extend(offset, amount)` (write-zeros stand-in)")]
#[inline]
pub async fn FileZero(file: &File, offset: u64, amount: u64, _wait_event_info: u32) -> std::io::Result<()> {
    file.extend(offset, amount).await
}

#[deprecated(note = "use `file.extend(offset, amount)`")]
#[inline]
pub async fn FileFallocate(file: &File, offset: u64, amount: u64, _wait_event_info: u32) -> std::io::Result<()> {
    file.extend(offset, amount).await
}

#[deprecated(note = "use `file.size()`")]
#[inline]
pub async fn FileSize(file: &File) -> std::io::Result<u64> {
    file.size().await
}

#[deprecated(note = "use `file.truncate(offset)`")]
#[inline]
pub async fn FileTruncate(file: &File, offset: u64, _wait_event_info: u32) -> std::io::Result<()> {
    file.truncate(offset).await
}

#[deprecated(note = "use `TransientFile::open(mgr, path, flags)` (RAII)")]
#[inline]
pub async fn OpenTransientFile(
    mgr: &FdManager,
    file_name: &str,
    flags: OpenFlags,
) -> std::io::Result<TransientFile> {
    TransientFile::open(mgr, file_name, flags).await
}

#[deprecated(note = "use `DirScan::open(dirname)` (RAII; closes on Drop)")]
#[inline]
pub async fn AllocateDir(dirname: &str) -> std::io::Result<DirScan> {
    DirScan::open(dirname).await
}

#[deprecated(note = "use `mgr.fsync_fname(fname, isdir)`")]
#[inline]
pub async fn fsync_fname(mgr: &FdManager, fname: &str, isdir: bool) -> std::io::Result<()> {
    mgr.fsync_fname(fname, isdir).await
}

#[deprecated(note = "use `mgr.durable_rename(old, new)`")]
#[inline]
pub async fn durable_rename(mgr: &FdManager, oldfile: &str, newfile: &str, _elevel: i32) -> std::io::Result<()> {
    mgr.durable_rename(oldfile, newfile).await
}

#[deprecated(note = "use `mgr.durable_unlink(fname)`")]
#[inline]
pub async fn durable_unlink(mgr: &FdManager, fname: &str, _elevel: i32) -> std::io::Result<()> {
    mgr.durable_unlink(fname).await
}

#[deprecated(note = "use `mgr.sync_data_directory(datadir)` (provisional)")]
#[inline]
pub async fn SyncDataDirectory(mgr: &Arc<FdManager>, datadir: &str) -> std::io::Result<()> {
    mgr.sync_data_directory(datadir).await
}

/// Configure the kernel-fd budget. Single-process: a configured/default cap
/// suffices (no probe-until-EMFILE). The IoBackend semaphore enforces it; this
/// records the GUC-derived value for diagnostics. Real wiring lands when the
/// manager moves into SharedState.
#[deprecated(note = "fd budget is set at FdManager::new(io, max_open)")]
#[inline]
pub fn set_max_safe_fds() {
    // Use max_files_per_process if configured, else a conservative default.
    let cap = unsafe {
        if max_files_per_process > 0 {
            max_files_per_process
        } else {
            crate::storage::io_backend::DEFAULT_FD_BUDGET as i32
        }
    };
    unsafe { max_safe_fds = cap };
}

#[deprecated(note = "single-process startup: handled at FdManager construction")]
#[inline]
pub fn InitFileAccess() {} // no-op: the cache is created with the FdManager

#[deprecated(note = "TempTablespace routing is deferred (provisional)")]
#[inline]
pub fn TempTablespacePath(_path: &mut str, _tablespace: Oid) {} // deferred

// FileRead/FileWrite single-buffer convenience wrappers map to File::read/write.
#[deprecated(note = "use `file.read(buf, offset)`")]
#[inline]
pub async fn FileRead(file: &File, buffer: &mut [u8], offset: u64, _wait_event_info: u32) -> std::io::Result<usize> {
    file.read(buffer, offset).await
}

#[deprecated(note = "use `file.write(buf, offset)`")]
#[inline]
pub async fn FileWrite(file: &File, buffer: &[u8], offset: u64, _wait_event_info: u32) -> std::io::Result<usize> {
    file.write(buffer, offset).await
}
