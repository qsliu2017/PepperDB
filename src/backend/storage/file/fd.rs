//! Translated from PostgreSQL src/backend/storage/file/fd.c
//!
//! The Virtual File Descriptor (VFD) pool over [`IoBackend`]. PG keeps more vfds
//! than the OS allows real fds, LRU-closing idle ones so an open never fails with
//! EMFILE. We keep that contract: a [`File`] is a generational handle into a
//! [`GenSlab<Vfd>`]; a vfd may have its OS fd LRU-closed (handle dropped) and
//! lazily reopened on next access. The kernel-fd budget is the IoBackend
//! semaphore: each currently-open vfd holds one [`FdPermit`].
//!
//! Deleted by redesign (vs fd.c): Windows paths; sync_file_range / posix_fadvise
//! used as portability shims; F_NOCACHE simulation; the data_sync_retry loop (we
//! abort on fsync failure instead); EXEC_BACKEND fd inheritance; the dup()-based
//! AllocateFile FILE* machinery and the allocatedDescs registry (replaced by RAII
//! guards whose Drop closes the resource). VfdCache realloc/doubling is replaced
//! by GenSlab growth; the intrusive lruMoreRecently/lruLessRecently ring is a
//! plain VecDeque of MRU keys.
//!
//! Transaction/error cleanup model: PG's AtEOXact_Files / CleanupTempFiles /
//! shmem-exit hooks are gone. Transient files (OpenTransientFile), directory
//! scans (AllocateDir), and temporary files are RAII guard types -- their Drop
//! closes the fd / unlinks the temp file, so unwind and normal scope-exit both
//! clean up without an explicit registry.

use std::collections::VecDeque;
use std::io::{self, IoSlice, IoSliceMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::storage::io_backend::{self, FdPermit, IoBackend, OpenFlags};
use crate::storage::procnumber::{GenSlab, Key};

/// A virtual file descriptor: the path/flags needed to (re)open it plus the
/// optional live OS handle. `handle`/`permit` are `Some` only while open; LRU
/// closing takes them, dropping the fd and returning the budget permit.
struct Vfd {
    path: PathBuf,
    flags: OpenFlags,
    handle: Option<Arc<std::fs::File>>,
    permit: Option<FdPermit>,
}

struct Inner {
    cache: GenSlab<Vfd>,
    /// MRU-ordered keys of currently-open vfds; front = most recently used, back
    /// = LRU victim. Closed vfds are absent.
    lru: VecDeque<Key<Vfd>>,
    /// Soft cap on simultaneously-open vfds (mirrors max_safe_fds). The IoBackend
    /// semaphore is the hard enforcer; this drives proactive LRU closing so we
    /// rarely block on the budget.
    max_open: usize,
}

impl Inner {
    fn touch_lru(&mut self, key: Key<Vfd>) {
        self.lru.retain(|k| *k != key);
        self.lru.push_front(key);
    }

    fn drop_from_lru(&mut self, key: Key<Vfd>) {
        self.lru.retain(|k| *k != key);
    }

    /// Close the least-recently-used open vfd (skipping `keep`), returning its fd
    /// and budget permit. Returns false if no evictable open vfd exists.
    fn release_one_lru(&mut self, keep: Key<Vfd>) -> bool {
        let Some(victim) = self.lru.iter().rev().copied().find(|k| *k != keep) else {
            return false;
        };
        self.drop_from_lru(victim);
        if let Some(vfd) = self.cache.get_mut(victim) {
            vfd.handle = None; // drop the OS fd
            vfd.permit = None; // return the budget unit
        }
        true
    }
}

/// The process-wide VFD cache. The `IoBackend` lives outside the Mutex (as an
/// `Arc`) so it can be borrowed across `.await` without holding the cache lock;
/// the Mutex guards only the in-memory bookkeeping and is never held across a
/// suspension point. Constructible for tests; will live in `SharedState` later.
pub struct FdManager {
    io: Arc<IoBackend>,
    inner: Mutex<Inner>,
    /// Monotonic counter for unique temp-file names.
    temp_seq: AtomicU64,
}

impl FdManager {
    pub fn new(io: IoBackend, max_open: usize) -> Arc<Self> {
        Arc::new(Self {
            io: Arc::new(io),
            inner: Mutex::new(Inner {
                cache: GenSlab::new(),
                lru: VecDeque::new(),
                max_open,
            }),
            temp_seq: AtomicU64::new(0),
        })
    }

    pub fn with_defaults() -> Arc<Self> {
        Self::new(IoBackend::with_default_budget(), io_backend::DEFAULT_FD_BUDGET)
    }

    pub fn io(&self) -> &Arc<IoBackend> {
        &self.io
    }

    /// Open a path as a managed [`File`]. The OS fd is opened eagerly so failures
    /// surface here; later LRU pressure may close and reopen it transparently.
    pub async fn open(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
        flags: OpenFlags,
    ) -> io::Result<File> {
        let key = self.inner.lock().unwrap().cache.insert(Vfd {
            path: path.as_ref().to_path_buf(),
            flags,
            handle: None,
            permit: None,
        });
        match self.ensure_open(key).await {
            Ok(_) => Ok(File::new(key, self.clone())),
            Err(e) => {
                self.inner.lock().unwrap().cache.remove(key);
                Err(e)
            }
        }
    }

    /// Ensure `key`'s vfd has a live OS handle, reopening (and LRU-closing other
    /// idle vfds first if at the soft cap) if needed. Returns the shared handle.
    async fn ensure_open(&self, key: Key<Vfd>) -> io::Result<Arc<std::fs::File>> {
        // Fast path + LRU trim, all under one short critical section.
        {
            let mut g = self.inner.lock().unwrap();
            match g.cache.get(key) {
                None => return Err(stale()),
                Some(vfd) => {
                    if let Some(h) = vfd.handle.clone() {
                        g.touch_lru(key);
                        return Ok(h);
                    }
                }
            }
            while g.lru.len() >= g.max_open {
                if !g.release_one_lru(key) {
                    break;
                }
            }
        }

        // Snapshot the reopen parameters, then open with the lock dropped.
        let (path, mut flags) = {
            let g = self.inner.lock().unwrap();
            let vfd = g.cache.get(key).ok_or_else(stale)?;
            (vfd.path.clone(), vfd.flags)
        };
        // A reopen of an existing vfd must not re-truncate or require exclusive
        // creation; the file already exists.
        flags.truncate = false;
        flags.create_new = false;

        let (handle, permit) = self.io.open(&path, flags).await?;

        let mut g = self.inner.lock().unwrap();
        // The vfd may have been removed while we awaited; if so, fail safe.
        let vfd = g.cache.get_mut(key).ok_or_else(stale)?;
        vfd.handle = Some(handle.clone());
        vfd.permit = Some(permit);
        g.touch_lru(key);
        Ok(handle)
    }

    // --- durable ops (used by WAL/checkpoint later) ---

    /// rename(2) with the fsyncs needed for crash durability: fsync old + target
    /// (if present), rename, then fsync target and its parent directory. fsync
    /// failure aborts the process (PG PANIC).
    pub async fn durable_rename(&self, old: impl AsRef<Path>, new: impl AsRef<Path>) -> io::Result<()> {
        let old = old.as_ref();
        let new = new.as_ref();
        self.fsync_fname(old, false).await?;
        // fsync the target if it already exists. ENOENT means it doesn't yet --
        // skip it; any other open error is a hard failure (matches C).
        match self.io.open(new, OpenFlags::read_write()).await {
            Ok((h, _p)) => self.io.fsync(&h).await,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        io_backend::rename(old, new).await?;
        self.fsync_fname(new, false).await?;
        self.fsync_parent_dir(new).await
    }

    /// unlink(2) followed by fsync of the parent directory for durability.
    pub async fn durable_unlink(&self, fname: impl AsRef<Path>) -> io::Result<()> {
        let fname = fname.as_ref();
        io_backend::unlink(fname).await?;
        self.fsync_parent_dir(fname).await
    }

    /// fsync a file or directory by name. fsync failure aborts (PG PANIC). The
    /// `isdir` flag is accepted for call-site parity; opening read-only suffices
    /// for both files and directories on the platforms we target.
    pub async fn fsync_fname(&self, fname: impl AsRef<Path>, _isdir: bool) -> io::Result<()> {
        let (h, _permit) = self.io.open(fname.as_ref(), OpenFlags::read_only()).await?;
        self.io.fsync(&h).await;
        Ok(())
    }

    async fn fsync_parent_dir(&self, path: &Path) -> io::Result<()> {
        let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
        match parent {
            Some(p) => self.fsync_fname(p, true).await,
            None => self.fsync_fname(".", true).await,
        }
    }

    // --- temporary files (provisional) ---

    /// Create a uniquely-named temporary file under the OS temp dir, returning a
    /// guard that unlinks it on Drop. Provisional: full per-tablespace temp
    /// routing and work-file accounting are consumer-driven (sort/hash) and
    /// deferred.
    pub async fn open_temporary_file(self: &Arc<Self>) -> io::Result<TempFile> {
        let seq = self.temp_seq.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("pepperdb_tmp_{}_{seq}", std::process::id()));
        let file = self.open(&path, OpenFlags::create_read_write()).await?;
        Ok(TempFile { file: Some(file), path })
    }

    // --- startup data-dir sync (provisional) ---

    /// Recursively fsync every file under the data directory. Provisional and
    /// straightforward (no recovery_init_sync_method variants -- syncfs/parallel
    /// are a step-17 startup concern). fsync failure aborts.
    pub async fn sync_data_directory(self: &Arc<Self>, datadir: impl AsRef<Path>) -> io::Result<()> {
        let mut stack = vec![datadir.as_ref().to_path_buf()];
        while let Some(dir) = stack.pop() {
            self.fsync_fname(&dir, true).await.ok(); // dirs may be unsyncable on some FS
            let rd = match std::fs::read_dir(&dir) {
                Ok(rd) => rd,
                Err(_) => continue,
            };
            for entry in rd.flatten() {
                let p = entry.path();
                match entry.file_type() {
                    Ok(ft) if ft.is_dir() => stack.push(p),
                    Ok(_) => {
                        let _ = self.fsync_fname(&p, false).await;
                    }
                    Err(_) => {}
                }
            }
        }
        Ok(())
    }
}

fn stale() -> io::Error {
    io::Error::other("stale File handle (vfd closed/reused)")
}

/// A generational handle to a managed file. Replaces fd.h's `File = i32`. A stale
/// handle (used after close) fails safely: the generational key no longer
/// resolves in the slab, so there is no reuse hazard.
///
/// `File` is a cheap `Clone` (an `Arc` bump): the vfd is closed only when the
/// LAST clone drops. This lets the smgr/md layer clone a segment's handle out of
/// its bookkeeping and `.await` an I/O on it without holding any borrow/lock
/// across the suspension point.
#[derive(Clone)]
pub struct File(Arc<FileInner>);

struct FileInner {
    key: Key<Vfd>,
    mgr: Arc<FdManager>,
}

impl File {
    fn new(key: Key<Vfd>, mgr: Arc<FdManager>) -> Self {
        File(Arc::new(FileInner { key, mgr }))
    }

    /// Vectored positional read at `offset`.
    pub async fn read_v(&self, iov: &mut [IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.read_vectored_at(&handle, iov, offset).await
    }

    /// Vectored positional write at `offset`.
    pub async fn write_v(&self, iov: &[IoSlice<'_>], offset: u64) -> io::Result<usize> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.write_vectored_at(&handle, iov, offset).await
    }

    /// Single-buffer positional read (convenience over `read_v`).
    pub async fn read(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.read_at(&handle, buf, offset).await
    }

    /// Single-buffer positional write (convenience over `write_v`).
    pub async fn write(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.write_at(&handle, buf, offset).await
    }

    /// fsync; aborts the process on failure (PG PANIC; data_sync_retry deleted).
    pub async fn sync(&self) -> io::Result<()> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.fsync(&handle).await;
        Ok(())
    }

    pub async fn truncate(&self, len: u64) -> io::Result<()> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.truncate(&handle, len).await
    }

    /// Extend the file to at least `offset + len`, zero-filling (provisional;
    /// posix_fallocate is a future optimization -- see IoBackend::fallocate).
    pub async fn extend(&self, offset: u64, len: u64) -> io::Result<()> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.fallocate(&handle, offset, len).await
    }

    pub async fn size(&self) -> io::Result<u64> {
        let handle = self.0.mgr.ensure_open(self.0.key).await?;
        self.0.mgr.io.size(&handle).await
    }

    /// Best-effort prefetch. Provisional: posix_fadvise(WILLNEED) is a
    /// portability shim deleted by redesign; a no-op is correct (just not an
    /// optimization). Kept for call-site compatibility.
    pub fn prefetch(&self, _offset: u64, _amount: u64) {}

    /// Close this handle. The OS fd / slab slot / budget permit are freed only
    /// when the last clone drops; an explicit close is optional.
    pub fn close(self) {}
}

impl Drop for FileInner {
    fn drop(&mut self) {
        let mut g = self.mgr.inner.lock().unwrap();
        g.drop_from_lru(self.key);
        g.cache.remove(self.key); // drops handle (fd) + permit (budget)
    }
}

// ---------------------------------------------------------------------------
// RAII guards replacing AllocateFile/OpenTransientFile/AllocateDir + the
// allocatedDescs registry + AtEOXact_Files/CleanupTempFiles. Cleanup is
// Drop-driven, so both normal scope exit and unwind release the resource.
// ---------------------------------------------------------------------------

/// A plain kernel fd with automatic cleanup (replaces OpenTransientFile). Holds
/// an `Arc<std::fs::File>` and a budget permit; both drop on scope exit.
pub struct TransientFile {
    pub file: Arc<std::fs::File>,
    _permit: FdPermit,
}

impl TransientFile {
    pub async fn open(
        mgr: &FdManager,
        path: impl AsRef<Path>,
        flags: OpenFlags,
    ) -> io::Result<Self> {
        let (file, permit) = mgr.io.open(path, flags).await?;
        Ok(Self { file, _permit: permit })
    }
}

/// A directory scan with automatic cleanup (replaces AllocateDir/FreeDir). Wraps
/// `std::fs::ReadDir`, which closes the underlying DIR* on Drop.
pub struct DirScan {
    inner: std::fs::ReadDir,
}

impl DirScan {
    pub async fn open(dirname: impl AsRef<Path>) -> io::Result<Self> {
        let dirname = dirname.as_ref().to_path_buf();
        let inner = tokio::task::spawn_blocking(move || std::fs::read_dir(&dirname))
            .await
            .expect("read_dir join")?;
        Ok(Self { inner })
    }
}

impl Iterator for DirScan {
    type Item = io::Result<std::fs::DirEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// A temporary file that unlinks itself on Drop (provisional; basic version of
/// OpenTemporaryFile -- no tablespace routing or work-file accounting yet).
pub struct TempFile {
    file: Option<File>,
    path: PathBuf,
}

impl TempFile {
    pub fn file(&self) -> &File {
        self.file.as_ref().expect("TempFile used after take")
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        // Close the managed File first (releases fd + slab slot), then unlink.
        self.file = None;
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tmp(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "pepperdb_fd_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    #[tokio::test]
    async fn open_write_read_close() {
        let mgr = FdManager::with_defaults();
        let path = tmp("rw");
        let f = mgr.open(&path, OpenFlags::create_read_write()).await.unwrap();
        let data = vec![0xABu8; 8192];
        assert_eq!(f.write(&data, 0).await.unwrap(), 8192);
        let mut buf = vec![0u8; 8192];
        assert_eq!(f.read(&mut buf, 0).await.unwrap(), 8192);
        assert_eq!(buf, data);
        assert_eq!(f.size().await.unwrap(), 8192);
        f.sync().await.unwrap();
        f.close();
        io_backend::unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn vectored_io() {
        let mgr = FdManager::with_defaults();
        let path = tmp("vec");
        let f = mgr.open(&path, OpenFlags::create_read_write()).await.unwrap();
        let a = b"abc".to_vec();
        let b = b"defg".to_vec();
        let n = f.write_v(&[IoSlice::new(&a), IoSlice::new(&b)], 0).await.unwrap();
        assert_eq!(n, 7);
        let mut da = vec![0u8; 3];
        let mut db = vec![0u8; 4];
        let n = f
            .read_v(&mut [IoSliceMut::new(&mut da), IoSliceMut::new(&mut db)], 0)
            .await
            .unwrap();
        assert_eq!(n, 7);
        assert_eq!(&da, b"abc");
        assert_eq!(&db, b"defg");
        drop(f);
        io_backend::unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn truncate_and_extend() {
        let mgr = FdManager::with_defaults();
        let path = tmp("trunc");
        let f = mgr.open(&path, OpenFlags::create_read_write()).await.unwrap();
        f.write(&vec![1u8; 500], 0).await.unwrap();
        f.truncate(100).await.unwrap();
        assert_eq!(f.size().await.unwrap(), 100);
        f.extend(0, 4096).await.unwrap();
        assert_eq!(f.size().await.unwrap(), 4096);
        drop(f);
        io_backend::unlink(&path).await.unwrap();
    }

    #[tokio::test]
    async fn stale_handle_after_close_fails_safe() {
        // A File can't be used after close() (it's consumed), but reopening over
        // an LRU-closed vfd must still work. Exercise LRU eviction + reopen.
        let mgr = FdManager::new(IoBackend::new(8), 1); // soft cap 1 open vfd
        let pa = tmp("lru_a");
        let pb = tmp("lru_b");
        let fa = mgr.open(&pa, OpenFlags::create_read_write()).await.unwrap();
        fa.write(b"AAAA", 0).await.unwrap();
        // Opening fb with max_open=1 should LRU-close fa's OS fd.
        let fb = mgr.open(&pb, OpenFlags::create_read_write()).await.unwrap();
        fb.write(b"BBBB", 0).await.unwrap();
        // fa is now LRU-closed; reading it must transparently reopen.
        let mut buf = vec![0u8; 4];
        assert_eq!(fa.read(&mut buf, 0).await.unwrap(), 4);
        assert_eq!(&buf, b"AAAA");
        drop(fa);
        drop(fb);
        io_backend::unlink(&pa).await.unwrap();
        io_backend::unlink(&pb).await.unwrap();
    }

    #[tokio::test]
    async fn lru_keeps_open_count_bounded() {
        let mgr = FdManager::new(IoBackend::new(100), 3);
        let mut files = Vec::new();
        let mut paths = Vec::new();
        for i in 0..10 {
            let p = tmp(&format!("bound{i}"));
            let f = mgr.open(&p, OpenFlags::create_read_write()).await.unwrap();
            f.write(b"x", 0).await.unwrap();
            files.push(f);
            paths.push(p);
        }
        // At most max_open vfds hold an OS fd at once.
        assert!(mgr.inner.lock().unwrap().lru.len() <= 3);
        for p in &paths {
            let _ = io_backend::unlink(p).await;
        }
    }

    #[tokio::test]
    async fn durable_rename_and_unlink() {
        let mgr = FdManager::with_defaults();
        let src = tmp("dr_src");
        let dst = tmp("dr_dst");
        let f = mgr.open(&src, OpenFlags::create_read_write()).await.unwrap();
        f.write(b"payload", 0).await.unwrap();
        f.sync().await.unwrap();
        drop(f); // close before rename
        mgr.durable_rename(&src, &dst).await.unwrap();
        assert!(!src.exists());
        assert!(dst.exists());
        mgr.durable_unlink(&dst).await.unwrap();
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn durable_rename_over_existing_target() {
        // Target exists: durable_rename must fsync it then overwrite (ENOENT
        // branch not taken). The rename still succeeds.
        let mgr = FdManager::with_defaults();
        let src = tmp("dro_src");
        let dst = tmp("dro_dst");
        std::fs::write(&src, b"new").unwrap();
        std::fs::write(&dst, b"old").unwrap();
        mgr.durable_rename(&src, &dst).await.unwrap();
        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"new");
        io_backend::unlink(&dst).await.unwrap();
    }

    #[tokio::test]
    async fn temp_file_unlinks_on_drop() {
        let mgr = FdManager::with_defaults();
        let tf = mgr.open_temporary_file().await.unwrap();
        let path = tf.path().to_path_buf();
        tf.file().write(b"scratch", 0).await.unwrap();
        assert!(path.exists());
        drop(tf);
        assert!(!path.exists(), "TempFile Drop must unlink");
    }

    #[tokio::test]
    async fn transient_file_guard() {
        let mgr = FdManager::with_defaults();
        let path = tmp("transient");
        std::fs::write(&path, b"hello").unwrap();
        let before = mgr.io.available_permits();
        {
            let _t = TransientFile::open(&mgr, &path, OpenFlags::read_only())
                .await
                .unwrap();
            assert_eq!(mgr.io.available_permits(), before - 1);
        }
        // Permit returned on Drop.
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(mgr.io.available_permits(), before);
        io_backend::unlink(&path).await.unwrap();
    }
}
