//! Translated from PostgreSQL src/backend/storage/smgr/smgr.c
//!
//! Public interface to the storage-manager switch. An [`SmgrRelation`] is a
//! cached set of open file handles for one physical relation. smgr.c dispatches
//! every relation file op to a storage-manager backend; only magnetic disk
//! ([`md`]) exists, so the C `f_smgr` vtable collapses to direct calls into md.
//!
//! Ownership model (chosen for Send-safety): the I/O ops are `async` methods on
//! `&mut SmgrRelation`, so the *caller* owns the relation on its stack -- there
//! is NO RefCell/lock borrow held across an `.await`. The buffer manager
//! (step 12) will own these. A per-task `smgropen` cache (a tokio `task_local`
//! `RefCell<HashMap<..>>`) provides the C "same locator -> same object"
//! semantics; it is only ever borrowed inside synchronous sections (insert /
//! lookup / take), never across the I/O await -- ops run on a value taken out of
//! (or never placed in) the cache.
//!
//! Deleted vs smgr.c: the pin/unpin dlist GC (Rust ownership), the
//! PROCSIGNAL_BARRIER_SMGRRELEASE early-close dance, the AIO target machinery
//! (smgr_aio_reopen / pgaio_io_set_target_smgr), and HOLD/RESUME_INTERRUPTS
//! (cooperative async, not signal-driven). The cache-invalidation hooks
//! (CacheInvalidateSmgr / DropRelationBuffers) are TODO(step16 sinval) /
//! TODO(step12 bufmgr).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::backend::storage::smgr::md;
use crate::common::relpath::{ForkNumber, MAX_FORKNUM};
use crate::shared_state::SharedState;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::procnumber::ProcNumber;
use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorBackend};
use crate::storage::smgr::{SMgrImpl, SmgrRelation, NUM_FORKS};

tokio::task_local! {
    /// Per-task smgr handle cache (smgr.c `SMgrRelationHash`, per backend).
    static SMGR_CACHE: RefCell<HashMap<RelFileLocatorBackend, SmgrRelation>>;
}

impl SmgrRelation {
    /// smgropen() core: construct a fresh handle (no I/O). Caching is layered on
    /// top via [`smgr_cache_open`].
    pub fn open(rlocator: RelFileLocator, backend: ProcNumber) -> Self {
        debug_assert!(rlocator.relNumber.0 != 0, "relNumber must be valid");
        let mut reln = Self {
            rlocator: RelFileLocatorBackend { locator: rlocator, backend },
            targblock: INVALID_BLOCK_NUMBER,
            cached_nblocks: [INVALID_BLOCK_NUMBER; NUM_FORKS],
            which: SMgrImpl::MagneticDisk,
            md_seg_fds: Default::default(),
        };
        md::mdopen(&mut reln);
        reln
    }

    /// smgrexists() -- does the underlying file for `forknum` exist?
    pub async fn exists(&mut self, shared: &Arc<SharedState>, forknum: ForkNumber) -> bool {
        md::mdexists(shared, self, forknum).await
    }

    /// smgrcreate() -- create the underlying storage for `forknum`.
    pub async fn create(&mut self, shared: &Arc<SharedState>, forknum: ForkNumber, is_redo: bool) {
        md::mdcreate(shared, self, forknum, is_redo).await;
    }

    /// smgrextend() -- append `buffer` at `blocknum`, extending the fork. Updates
    /// the cached size like smgr.c.
    pub async fn extend(
        &mut self,
        shared: &Arc<SharedState>,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &crate::storage::bufpage::Page,
        skip_fsync: bool,
    ) {
        md::mdextend(shared, self, forknum, blocknum, buffer, skip_fsync).await;
        let fk = forknum as usize;
        self.cached_nblocks[fk] = if self.cached_nblocks[fk] == blocknum {
            blocknum + 1
        } else {
            INVALID_BLOCK_NUMBER
        };
    }

    /// smgrzeroextend() -- extend by `nblocks` zero-filled blocks at `blocknum`.
    pub async fn zeroextend(
        &mut self,
        shared: &Arc<SharedState>,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: i32,
        skip_fsync: bool,
    ) {
        md::mdzeroextend(shared, self, forknum, blocknum, nblocks, skip_fsync).await;
        let fk = forknum as usize;
        self.cached_nblocks[fk] = if self.cached_nblocks[fk] == blocknum {
            blocknum + nblocks as BlockNumber
        } else {
            INVALID_BLOCK_NUMBER
        };
    }

    /// smgrprefetch() -- best-effort prefetch (no-op).
    pub fn prefetch(&mut self, forknum: ForkNumber, blocknum: BlockNumber, nblocks: i32) -> bool {
        md::mdprefetch(self, forknum, blocknum, nblocks)
    }

    /// smgrmaxcombine() -- max blocks combinable into one IO at `blocknum`.
    pub fn maxcombine(&mut self, forknum: ForkNumber, blocknum: BlockNumber) -> u32 {
        md::mdmaxcombine(self, forknum, blocknum)
    }

    /// smgrreadv() -- read `buffers.len()` blocks starting at `blocknum`.
    pub async fn readv(
        &mut self,
        shared: &Arc<SharedState>,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffers: &mut [&mut crate::storage::bufpage::Page],
    ) {
        md::mdreadv(shared, self, forknum, blocknum, buffers).await;
    }

    /// smgrread() -- read one block (wrapper over readv).
    pub async fn read(
        &mut self,
        shared: &Arc<SharedState>,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &mut crate::storage::bufpage::Page,
    ) {
        md::mdreadv(shared, self, forknum, blocknum, &mut [buffer]).await;
    }

    /// smgrwritev() -- write `buffers.len()` blocks (all before EOF).
    pub async fn writev(
        &mut self,
        shared: &Arc<SharedState>,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffers: &[&crate::storage::bufpage::Page],
        skip_fsync: bool,
    ) {
        md::mdwritev(shared, self, forknum, blocknum, buffers, skip_fsync).await;
    }

    /// smgrwrite() -- write one block (wrapper over writev).
    pub async fn write(
        &mut self,
        shared: &Arc<SharedState>,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &crate::storage::bufpage::Page,
        skip_fsync: bool,
    ) {
        md::mdwritev(shared, self, forknum, blocknum, &[buffer], skip_fsync).await;
    }

    /// smgrwriteback() -- kernel writeback hint (no-op).
    pub fn writeback(&mut self, forknum: ForkNumber, blocknum: BlockNumber, nblocks: BlockNumber) {
        md::mdwriteback(self, forknum, blocknum, nblocks);
    }

    /// smgrnblocks() -- number of blocks in `forknum`, caching the result.
    pub async fn nblocks(&mut self, shared: &Arc<SharedState>, forknum: ForkNumber) -> BlockNumber {
        if let Some(cached) = self.nblocks_cached(forknum) {
            return cached;
        }
        let result = md::mdnblocks(shared, self, forknum).await;
        self.cached_nblocks[forknum as usize] = result;
        result
    }

    /// smgrnblocks_cached() -- the cached size, if known. Only trusted in
    /// recovery (no shared size invalidation); we currently always honor the
    /// cache when set. TODO(InRecovery): gate on recovery like smgr.c.
    pub fn nblocks_cached(&self, forknum: ForkNumber) -> Option<BlockNumber> {
        let v = self.cached_nblocks[forknum as usize];
        (v != INVALID_BLOCK_NUMBER).then_some(v)
    }

    /// smgrtruncate() -- truncate the listed forks to the given new sizes.
    pub async fn truncate(
        &mut self,
        shared: &Arc<SharedState>,
        truncate: &[(ForkNumber, BlockNumber, BlockNumber)],
    ) {
        // TODO(step12 bufmgr): DropRelationBuffers for the removed blocks.
        // TODO(step16 sinval): CacheInvalidateSmgr to force other tasks to close.
        for &(forknum, old_nblocks, nblocks) in truncate {
            self.cached_nblocks[forknum as usize] = INVALID_BLOCK_NUMBER;
            md::mdtruncate(shared, self, forknum, old_nblocks, nblocks).await;
            self.cached_nblocks[forknum as usize] =
                if nblocks > old_nblocks { old_nblocks } else { nblocks };
        }
    }

    /// smgrregistersync() -- request a deferred fsync of the whole fork.
    pub async fn registersync(&mut self, shared: &Arc<SharedState>, forknum: ForkNumber) {
        md::mdregistersync(shared, self, forknum).await;
    }

    /// smgrimmedsync() -- immediately fsync `forknum`.
    pub async fn immedsync(&mut self, shared: &Arc<SharedState>, forknum: ForkNumber) {
        md::mdimmedsync(shared, self, forknum).await;
    }

    /// smgrrelease() -- close all forks' open files; the object stays valid.
    pub fn release(&mut self) {
        for f in 0..=(MAX_FORKNUM as i32) {
            md::mdclose(self, fork_from_i32(f));
            self.cached_nblocks[f as usize] = INVALID_BLOCK_NUMBER;
        }
        self.targblock = INVALID_BLOCK_NUMBER;
    }

    /// smgrclose() -- synonym for release (we don't track external references).
    pub fn close(&mut self) {
        self.release();
    }
}

fn fork_from_i32(f: i32) -> ForkNumber {
    match f {
        0 => ForkNumber::MAIN_FORKNUM,
        1 => ForkNumber::FSM_FORKNUM,
        2 => ForkNumber::VISIBILITYMAP_FORKNUM,
        3 => ForkNumber::INIT_FORKNUM,
        _ => ForkNumber::InvalidForkNumber,
    }
}

// ---------------------------------------------------------------------------
// Per-task handle cache (smgr.c SMgrRelationHash). Borrowed only synchronously.
// ---------------------------------------------------------------------------

/// Run `f` with the per-task smgr cache, scoping it if not already present.
pub async fn with_smgr_cache<F, Fut, T>(f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    if SMGR_CACHE.try_with(|_| ()).is_ok() {
        f().await
    } else {
        SMGR_CACHE.scope(RefCell::new(HashMap::new()), f()).await
    }
}

/// True if `rlocator`/`backend` is currently cached in this task. Used to model
/// the smgr.c "smgropen twice returns the same object" semantics in tests; the
/// real consumers (bufmgr) own the SmgrRelation directly.
pub fn smgr_is_cached(rlocator: RelFileLocator, backend: ProcNumber) -> bool {
    let key = RelFileLocatorBackend { locator: rlocator, backend };
    SMGR_CACHE.try_with(|c| c.borrow().contains_key(&key)).unwrap_or(false)
}

/// smgropen() -- take the cached handle for `rlocator`/`backend`, creating it if
/// absent. The handle is *removed* from the cache and returned to the caller (so
/// async ops run on an owned value, never a cache borrow held across `.await`);
/// return it with [`smgr_cache_put`] when done. No I/O.
pub fn smgr_cache_open(rlocator: RelFileLocator, backend: ProcNumber) -> SmgrRelation {
    let key = RelFileLocatorBackend { locator: rlocator, backend };
    let existing = SMGR_CACHE.try_with(|c| c.borrow_mut().remove(&key)).ok().flatten();
    existing.unwrap_or_else(|| SmgrRelation::open(rlocator, backend))
}

/// Return an SmgrRelation taken via [`smgr_cache_open`] back to the cache.
pub fn smgr_cache_put(reln: SmgrRelation) {
    let key = reln.rlocator;
    let _ = SMGR_CACHE.try_with(|c| c.borrow_mut().insert(key, reln));
}

/// AtEOXact_SMgr() -- drop all cached handles at end of transaction.
pub fn at_eo_xact_smgr() {
    let _ = SMGR_CACHE.try_with(|c| c.borrow_mut().clear());
}

/// smgrdestroyall() -- close and destroy every open smgr relation. The bgwriter
/// calls this after each checkpoint to release dropped-relation handles (it does
/// not see invalidation messages). Here it drops every cached handle for this
/// task; the underlying close is I/O-free.
pub fn smgrdestroyall() {
    let _ = SMGR_CACHE.try_with(|c| c.borrow_mut().clear());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::relpath::ForkNumber;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::bufpage::Page;

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: Oid(1663), dbOid: Oid(50000 + rel), relNumber: Oid(16000 + rel) }
    }

    async fn shared_with_tmpdir(tag: &str) -> (Arc<SharedState>, std::path::PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "pepperdb_smgr_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        crate::storage::io_backend::mkdir_all(dir.join("base").join((50000 + 1).to_string()))
            .await
            .ok();
        let s = SharedState::new(SharedStateConfig::default());
        s.config().set_data_dir(dir.to_string_lossy().into_owned());
        (s, dir)
    }

    fn pattern_page(byte: u8) -> Box<Page> {
        let mut p = Page::boxed_zeroed();
        p.as_mut_bytes().fill(byte);
        p
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extend_nblocks_read_truncate_roundtrip() {
        let (s, dir) = shared_with_tmpdir("rt").await;
        let mut reln = SmgrRelation::open(rloc(1), crate::storage::procnumber::INVALID_PROC_NUMBER);
        let fork = ForkNumber::MAIN_FORKNUM;

        reln.create(&s, fork, false).await;
        assert_eq!(reln.nblocks(&s, fork).await, 0);

        // Extend 5 distinct pages with known patterns.
        for i in 0..5u8 {
            let page = pattern_page(0x10 + i);
            reln.extend(&s, fork, BlockNumber::from(i), &page, true).await;
        }
        assert_eq!(reln.nblocks(&s, fork).await, 5);

        // Read each back and verify.
        for i in 0..5u8 {
            let mut buf = Page::boxed_zeroed();
            reln.read(&s, fork, BlockNumber::from(i), &mut buf).await;
            assert!(buf.as_bytes().iter().all(|&b| b == 0x10 + i), "block {i} mismatch");
        }

        // Truncate to 2 blocks.
        let cur = reln.nblocks(&s, fork).await;
        reln.truncate(&s, &[(fork, cur, 2)]).await;
        assert_eq!(reln.nblocks(&s, fork).await, 2);

        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_registers_sync_request_and_process_drains() {
        let (s, dir) = shared_with_tmpdir("sync").await;
        let mut reln = SmgrRelation::open(rloc(1), crate::storage::procnumber::INVALID_PROC_NUMBER);
        let fork = ForkNumber::MAIN_FORKNUM;
        reln.create(&s, fork, false).await;
        let page = pattern_page(0xAA);
        reln.extend(&s, fork, 0, &page, true).await;

        // write with skip_fsync = false registers a sync request.
        reln.write(&s, fork, 0, &page, false).await;
        assert!(s.sync_requests().pending_op_count() >= 1);

        // ProcessSyncRequests drains it (fsyncs the segment).
        crate::backend::storage::sync::sync::ProcessSyncRequests(&s).await;
        assert_eq!(s.sync_requests().pending_op_count(), 0);

        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_past_eof_zero_fills() {
        let (s, dir) = shared_with_tmpdir("eof").await;
        let mut reln = SmgrRelation::open(rloc(1), crate::storage::procnumber::INVALID_PROC_NUMBER);
        let fork = ForkNumber::MAIN_FORKNUM;
        reln.create(&s, fork, false).await;
        reln.extend(&s, fork, 0, &pattern_page(0x77), true).await;

        // Reading block 0 and a nonexistent block 1 zero-fills block 1.
        let mut b0 = Page::boxed_zeroed();
        let mut b1 = pattern_page(0xFF);
        {
            let mut bufs: Vec<&mut Page> = vec![&mut b0, &mut b1];
            reln.readv(&s, fork, 0, &mut bufs).await;
        }
        assert!(b0.as_bytes().iter().all(|&b| b == 0x77));
        assert!(b1.as_bytes().iter().all(|&b| b == 0), "past-EOF block must be zero-filled");

        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn smgropen_caches() {
        with_smgr_cache(|| async {
            let r = rloc(9);
            let backend = crate::storage::procnumber::INVALID_PROC_NUMBER;
            assert!(!smgr_is_cached(r, backend));
            let reln = smgr_cache_open(r, backend);
            smgr_cache_put(reln);
            assert!(smgr_is_cached(r, backend), "first open should populate the cache");
            // Second open finds the same cached entry (removes then we re-put).
            let reln2 = smgr_cache_open(r, backend);
            assert_eq!(reln2.rlocator.locator, r);
            smgr_cache_put(reln2);
            assert!(smgr_is_cached(r, backend));
            at_eo_xact_smgr();
            assert!(!smgr_is_cached(r, backend), "AtEOXact clears the cache");
        })
        .await;
    }

    #[test]
    fn smgr_io_future_is_send() {
        // The buffer manager spawns backends on the multi-thread runtime, so the
        // smgr I/O future must be Send (no !Send borrow held across an .await).
        fn assert_send<T: Send>(_: T) {}
        let s = SharedState::new(SharedStateConfig::default());
        let mut reln = SmgrRelation::open(rloc(1), crate::storage::procnumber::INVALID_PROC_NUMBER);
        assert_send(async move { reln.nblocks(&s, ForkNumber::MAIN_FORKNUM).await });
    }

    #[test]
    fn segment_math() {
        // block N -> segment N / RELSEG_SIZE at byte offset (N % RELSEG_SIZE)*BLCKSZ.
        use crate::pg_config::{BLCKSZ, RELSEG_SIZE};
        let n: BlockNumber = RELSEG_SIZE + 7;
        assert_eq!(n / RELSEG_SIZE, 1);
        assert_eq!(u64::from(n % RELSEG_SIZE) * u64::from(BLCKSZ), 7 * u64::from(BLCKSZ));
        let zero: BlockNumber = 0;
        assert_eq!(zero / RELSEG_SIZE, 0);
    }
}
