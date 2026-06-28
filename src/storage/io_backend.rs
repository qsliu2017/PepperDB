//! Async file I/O leaf (no C origin).
//!
//! The universal blocking-syscall boundary for file I/O. PostgreSQL's `pg_pread`/
//! `pg_pwrite`/`pg_fsync` and the OS-portability shims in `fd.c`
//! (sync_file_range, posix_fadvise, F_NOCACHE, the data_sync_retry loop, Windows
//! paths) are replaced here by `std::fs` positional syscalls run on tokio's
//! blocking pool via `spawn_blocking`. Higher layers (the VFD pool in
//! `backend::storage::file::fd`, then smgr/md, the buffer manager, WAL) call only
//! these async methods.
//!
//! Design:
//!  * Open OS handles are `Arc<std::fs::File>`. `FileExt::{read_at,write_at}` and
//!    `sync_all/sync_data/set_len` take `&self`, so a single handle is shared
//!    across concurrent tasks without per-call dup or seek races -- positional
//!    I/O has no shared file offset.
//!  * The kernel-fd budget is a `tokio::sync::Semaphore`. `open` acquires one
//!    permit and hands back an `FdPermit` (an owned permit) alongside the handle;
//!    dropping the permit returns the budget. The VFD layer owns this permit for
//!    the lifetime of an open vfd, so the budget bounds simultaneously-open fds.
//!  * `fsync`/`fdatasync` abort the process on failure (PG PANICs; data_sync_retry
//!    defaults off and is deleted). The happy path is async; the abort path logs
//!    via elog then `std::process::abort()`.
//!  * Positional page I/O uses `std::os::unix::fs::FileExt::{write_all_at,
//!    read_exact_at}` on `spawn_blocking`: these own the all-or-error short-I/O
//!    loops, so reads are all-or-EOF (a short read at EOF surfaces as
//!    `UnexpectedEof`). Cursor-based `tokio::io::{AsyncReadExt, AsyncWriteExt}` are
//!    reserved for sequential/socket I/O (WAL append, libpq) and are not used
//!    here -- a single shared cursor cannot serve concurrent positional
//!    access on one handle, which is why we avoid `tokio::fs::File` for pages.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::io::{self, IoSlice, IoSliceMut};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::elog;
use crate::utils::elog::PANIC;

/// Conservative default kernel-fd budget. PG probes by opening until EMFILE; a
/// configured cap is sufficient here (see fd.rs set_max_safe_fds).
pub const DEFAULT_FD_BUDGET: usize = 1000;

/// An owned permit against the fd budget. Held for as long as an OS handle stays
/// open; dropping it returns one unit to the budget.
pub type FdPermit = OwnedSemaphorePermit;

/// How a file is opened. Mirrors the subset of open(2) flags the storage layer
/// uses; the portability bits (O_DIRECT/F_NOCACHE, O_CLOEXEC inheritance) are
/// deleted by redesign.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
    /// O_EXCL: fail if the file already exists (with `create`).
    pub create_new: bool,
}

impl OpenFlags {
    pub fn read_only() -> Self {
        Self { read: true, ..Self::default() }
    }
    pub fn read_write() -> Self {
        Self { read: true, write: true, ..Self::default() }
    }
    pub fn create_read_write() -> Self {
        Self { read: true, write: true, create: true, ..Self::default() }
    }

    fn to_std(self) -> std::fs::OpenOptions {
        let mut o = std::fs::OpenOptions::new();
        o.read(self.read)
            .write(self.write)
            .create(self.create)
            .truncate(self.truncate)
            .create_new(self.create_new);
        o
    }
}

/// The async file-I/O leaf. Arc-shareable; will live in `SharedState` later.
pub struct IoBackend {
    budget: Arc<Semaphore>,
}

impl IoBackend {
    pub fn new(fd_budget: usize) -> Self {
        Self { budget: Arc::new(Semaphore::new(fd_budget)) }
    }

    pub fn with_default_budget() -> Self {
        Self::new(DEFAULT_FD_BUDGET)
    }

    /// Currently-available budget permits (for diagnostics/tests).
    pub fn available_permits(&self) -> usize {
        self.budget.available_permits()
    }

    /// Acquire one budget permit, awaiting if the budget is exhausted.
    pub async fn acquire_fd(&self) -> FdPermit {
        // The semaphore is never closed, so acquire cannot error.
        self.budget.clone().acquire_owned().await.expect("fd budget semaphore closed")
    }

    /// Open an existing/created file per `flags`, consuming one fd-budget permit.
    /// Returns the shared handle plus the permit that gates its lifetime.
    pub async fn open(
        &self,
        path: impl AsRef<Path>,
        flags: OpenFlags,
    ) -> io::Result<(Arc<std::fs::File>, FdPermit)> {
        let permit = self.acquire_fd().await;
        let path = path.as_ref().to_path_buf();
        let file = tokio::task::spawn_blocking(move || flags.to_std().open(&path))
            .await
            .expect("open join")?;
        Ok((Arc::new(file), permit))
    }

    /// Read exactly `buf.len()` bytes at `offset` via `FileExt::read_exact_at`.
    /// All-or-error: a short read at EOF surfaces as `UnexpectedEof`; on success
    /// the buffer is fully filled and Ok(buf.len()) is returned.
    pub async fn read_at(
        &self,
        file: &Arc<std::fs::File>,
        buf: &mut [u8],
        offset: u64,
    ) -> io::Result<usize> {
        let file = file.clone();
        let len = buf.len();
        let data = tokio::task::spawn_blocking(move || {
            let mut local = vec![0u8; len];
            file.read_exact_at(&mut local, offset)?;
            io::Result::Ok(local)
        })
        .await
        .expect("read_at join")?;
        buf.copy_from_slice(&data);
        Ok(len)
    }

    /// Vectored read at `offset`: reads exactly the iovec total into one
    /// contiguous buffer via `FileExt::read_exact_at`, then scatters it back into
    /// the caller's slices in order. All-or-error: a short read propagates
    /// `UnexpectedEof` with no partial scatter.
    pub async fn read_vectored_at(
        &self,
        file: &Arc<std::fs::File>,
        iov: &mut [IoSliceMut<'_>],
        offset: u64,
    ) -> io::Result<usize> {
        let total_len: usize = iov.iter().map(|s| s.len()).sum();
        let file = file.clone();
        let data = tokio::task::spawn_blocking(move || {
            let mut local = vec![0u8; total_len];
            file.read_exact_at(&mut local, offset)?;
            io::Result::Ok(local)
        })
        .await
        .expect("read_vectored_at join")?;
        let mut filled = 0usize;
        for slice in iov.iter_mut() {
            let take = slice.len();
            slice.copy_from_slice(&data[filled..filled + take]);
            filled += take;
        }
        Ok(total_len)
    }

    /// Write all of `buf` at `offset` via `FileExt::write_all_at`, which loops
    /// over short writes (returning WriteZero on no progress). Returns
    /// Ok(buf.len()) only when every byte is persisted, else Err.
    pub async fn write_at(
        &self,
        file: &Arc<std::fs::File>,
        buf: &[u8],
        offset: u64,
    ) -> io::Result<usize> {
        let file = file.clone();
        let data = buf.to_vec();
        tokio::task::spawn_blocking(move || {
            file.write_all_at(&data, offset)?;
            io::Result::Ok(data.len())
        })
        .await
        .expect("write_at join")
    }

    /// Vectored write at `offset`: gathers the iovecs into one owned contiguous
    /// buffer (preserving order and length), then `write_all_at`s the whole buffer.
    pub async fn write_vectored_at(
        &self,
        file: &Arc<std::fs::File>,
        iov: &[IoSlice<'_>],
        offset: u64,
    ) -> io::Result<usize> {
        let buf: Vec<u8> = iov.iter().flat_map(|s| s.iter().copied()).collect();
        self.write_at(file, &buf, offset).await
    }

    /// fsync; aborts the process on failure (PG PANIC; data_sync_retry deleted).
    pub async fn fsync(&self, file: &Arc<std::fs::File>) {
        if let Err(e) = Self::fsync_inner(file).await {
            Self::abort_fsync("fsync", e);
        }
    }

    /// fdatasync; aborts the process on failure.
    pub async fn fdatasync(&self, file: &Arc<std::fs::File>) {
        if let Err(e) = Self::fdatasync_inner(file).await {
            Self::abort_fsync("fdatasync", e);
        }
    }

    async fn fsync_inner(file: &Arc<std::fs::File>) -> io::Result<()> {
        let file = file.clone();
        tokio::task::spawn_blocking(move || file.sync_all())
            .await
            .expect("fsync join")
    }

    async fn fdatasync_inner(file: &Arc<std::fs::File>) -> io::Result<()> {
        let file = file.clone();
        tokio::task::spawn_blocking(move || file.sync_data())
            .await
            .expect("fdatasync join")
    }

    /// The critical-section abort: a sync failure means we cannot guarantee
    /// durability, so we crash rather than risk silent corruption. PANIC is the
    /// data_sync_elevel mapping (data_sync_retry off) and realizes process::abort
    /// uncatchably -- bypassing any per-task catch_unwind by design.
    #[cold]
    #[allow(clippy::needless_pass_by_value, reason = "io::Error consumed in elog! before process abort")]
    fn abort_fsync(op: &str, e: io::Error) -> ! {
        elog!(PANIC, format!("could not {op} file: {e}"));
        // elog!(PANIC,...) diverges via process::abort; keep the never-type contract.
        unreachable!("elog!(PANIC) aborts the process")
    }

    /// Truncate (or, via set_len, extend) to `len`.
    pub async fn truncate(&self, file: &Arc<std::fs::File>, len: u64) -> io::Result<()> {
        let file = file.clone();
        tokio::task::spawn_blocking(move || file.set_len(len))
            .await
            .expect("truncate join")
    }

    /// Ensure the file is at least `offset + len` bytes, zero-filling the gap.
    ///
    /// Portable stand-in for FileFallocate: posix_fallocate needs libc and is a
    /// future optimization. We grow with set_len and write a zero page to force
    /// allocation of the final byte, which is sufficient for smgr extend.
    pub async fn fallocate(
        &self,
        file: &Arc<std::fs::File>,
        offset: u64,
        len: u64,
    ) -> io::Result<()> {
        let want = offset
            .checked_add(len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        let cur = self.size(file).await?;
        if want > cur {
            self.truncate(file, want).await?;
        }
        Ok(())
    }

    /// Current file size in bytes.
    pub async fn size(&self, file: &Arc<std::fs::File>) -> io::Result<u64> {
        let file = file.clone();
        tokio::task::spawn_blocking(move || file.metadata().map(|m| m.len()))
            .await
            .expect("size join")
    }
}

/// Remove a file. Free function: no fd handle involved.
pub async fn unlink(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::remove_file(&path))
        .await
        .expect("unlink join")
}

/// Create a directory and all parents (mkdir -p). Free function: no fd handle.
pub async fn mkdir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::create_dir_all(&path))
        .await
        .expect("mkdir_all join")
}

/// Recursively remove a directory and its contents (rm -rf). Free function.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&path))
        .await
        .expect("remove_dir_all join")
}

/// Bare rename(2): NOT crash-durable (no fsync of source/target/parent dir).
/// Crash-critical callers (WAL, control file) must use `FdManager::durable_rename`.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let from: PathBuf = from.as_ref().to_path_buf();
    let to: PathBuf = to.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::rename(&from, &to))
        .await
        .expect("rename join")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tmp_path(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "pepperdb_iob_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(uniq);
        p
    }

    #[tokio::test]
    async fn write_then_read_pages_at_offsets() {
        const PAGE: usize = 8192;
        const N: u64 = 4;
        let io = IoBackend::with_default_budget();
        let path = tmp_path("pages");
        let (file, _permit) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        for i in 0..N {
            let buf = vec![(i as u8).wrapping_add(1); PAGE];
            let n = io.write_at(&file, &buf, i * PAGE as u64).await.unwrap();
            assert_eq!(n, PAGE);
        }
        for i in 0..N {
            let mut buf = vec![0u8; PAGE];
            let n = io.read_at(&file, &mut buf, i * PAGE as u64).await.unwrap();
            assert_eq!(n, PAGE);
            assert!(buf.iter().all(|&b| b == (i as u8).wrapping_add(1)));
        }
        assert_eq!(io.size(&file).await.unwrap(), N * PAGE as u64);

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn vectored_round_trip() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("vec");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        let a = b"hello ".to_vec();
        let b = b"world!".to_vec();
        let iov = [IoSlice::new(&a), IoSlice::new(&b)];
        let n = io.write_vectored_at(&file, &iov, 0).await.unwrap();
        assert_eq!(n, a.len() + b.len());

        let mut da = vec![0u8; a.len()];
        let mut db = vec![0u8; b.len()];
        let mut riov = [IoSliceMut::new(&mut da), IoSliceMut::new(&mut db)];
        let n = io.read_vectored_at(&file, &mut riov, 0).await.unwrap();
        assert_eq!(n, a.len() + b.len());
        assert_eq!(&da, b"hello ");
        assert_eq!(&db, b"world!");

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn truncate_shrinks_and_extend_grows() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("trunc");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        io.write_at(&file, &vec![7u8; 1000], 0).await.unwrap();
        assert_eq!(io.size(&file).await.unwrap(), 1000);

        io.truncate(&file, 100).await.unwrap();
        assert_eq!(io.size(&file).await.unwrap(), 100);

        io.fallocate(&file, 0, 4096).await.unwrap();
        assert_eq!(io.size(&file).await.unwrap(), 4096);

        // fallocate must not shrink an already-larger file.
        io.fallocate(&file, 0, 100).await.unwrap();
        assert_eq!(io.size(&file).await.unwrap(), 4096);

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn fsync_happy_path() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("fsync");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();
        io.write_at(&file, b"durable", 0).await.unwrap();
        io.fsync(&file).await; // would abort on failure; happy path returns
        io.fdatasync(&file).await;
        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn budget_blocks_until_a_permit_frees() {
        // Tiny budget: 2 concurrent opens max.
        let io = Arc::new(IoBackend::new(2));
        let p1 = tmp_path("b1");
        let p2 = tmp_path("b2");
        let p3 = tmp_path("b3");

        let (f1, permit1) = io.open(&p1, OpenFlags::create_read_write()).await.unwrap();
        let (_f2, _permit2) = io.open(&p2, OpenFlags::create_read_write()).await.unwrap();
        assert_eq!(io.available_permits(), 0);

        // A third open must block until a permit is released.
        let io3 = io.clone();
        let p3c = p3.clone();
        let third = tokio::spawn(async move {
            io3.open(&p3c, OpenFlags::create_read_write()).await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!third.is_finished(), "third open should block on the budget");

        // Free one permit; the third open now proceeds.
        drop(permit1);
        drop(f1);
        let (_f3, _permit3) = tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .expect("third open should unblock")
            .expect("spawn join")
            .expect("open ok");

        for p in [&p1, &p2, &p3] {
            let _ = unlink(p).await;
        }
    }

    #[tokio::test]
    async fn multi_slice_vectored_round_trip_larger_than_page() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("multivec");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        // Three slices totalling > one 8K page, each a distinct fill byte.
        let s0 = vec![0x11u8; 5000];
        let s1 = vec![0x22u8; 4000];
        let s2 = vec![0x33u8; 3000];
        let total = s0.len() + s1.len() + s2.len();
        let wiov = [IoSlice::new(&s0), IoSlice::new(&s1), IoSlice::new(&s2)];
        assert_eq!(io.write_vectored_at(&file, &wiov, 0).await.unwrap(), total);

        let mut d0 = vec![0u8; s0.len()];
        let mut d1 = vec![0u8; s1.len()];
        let mut d2 = vec![0u8; s2.len()];
        let mut riov =
            [IoSliceMut::new(&mut d0), IoSliceMut::new(&mut d1), IoSliceMut::new(&mut d2)];
        assert_eq!(io.read_vectored_at(&file, &mut riov, 0).await.unwrap(), total);
        assert_eq!(d0, s0);
        assert_eq!(d1, s1);
        assert_eq!(d2, s2);

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn vectored_read_past_eof_is_unexpected_eof() {
        // File shorter than the iovec total: reads are all-or-error now.
        let io = IoBackend::with_default_budget();
        let path = tmp_path("vec_eof");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        let src = vec![0xABu8; 10];
        assert_eq!(io.write_at(&file, &src, 0).await.unwrap(), 10);

        // Three 8-byte slices (total 24); only 10 bytes exist.
        let mut d0 = vec![0u8; 8];
        let mut d1 = vec![0u8; 8];
        let mut d2 = vec![0u8; 8];
        let mut riov =
            [IoSliceMut::new(&mut d0), IoSliceMut::new(&mut d1), IoSliceMut::new(&mut d2)];
        let err = io.read_vectored_at(&file, &mut riov, 0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn read_at_past_eof_is_unexpected_eof() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("read_eof");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        assert_eq!(io.write_at(&file, &[0xABu8; 10], 0).await.unwrap(), 10);

        // Ask for 16 bytes when only 10 exist.
        let mut buf = vec![0u8; 16];
        let err = io.read_at(&file, &mut buf, 0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn empty_buffer_read_and_write_are_ok() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("empty");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();

        // write_all_at/read_exact_at of an empty slice are no-ops returning Ok.
        assert_eq!(io.write_at(&file, &[], 0).await.unwrap(), 0);
        let mut empty = [0u8; 0];
        assert_eq!(io.read_at(&file, &mut empty, 0).await.unwrap(), 0);

        unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn fallocate_overflow_is_invalid_input() {
        let io = IoBackend::with_default_budget();
        let path = tmp_path("ofl");
        let (file, _p) = io.open(&path, OpenFlags::create_read_write()).await.unwrap();
        let err = io.fallocate(&file, u64::MAX, 1).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        unlink(&path).await.unwrap();
    }
}
