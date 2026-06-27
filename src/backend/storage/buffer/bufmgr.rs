//! Translated from PostgreSQL src/backend/storage/buffer/bufmgr.c -- the shared-
//! buffer I/O paths on top of the Part-A buffer pool core (`buf_init`,
//! `buf_table`, `freelist`).
//!
//! What lives here: the read/pin/dirty/flush entry points
//! (`ReadBuffer_common`, `BufferAlloc`, `GetVictimBuffer`, `FlushBuffer`,
//! `MarkBufferDirty`, `LockBuffer`, `ReleaseBuffer`, ...). The header lock, pin
//! cache, clock sweep, buffer table, and the IO-in-progress handshake are
//! Part A.
//!
//! THE AIO collapse (file-list note): PG18's batched read path
//! (`StartReadBuffers` / `WaitReadBuffers` / pgaio / read_stream) is DELETED. A
//! miss does a single direct `smgrreadv` `.await` for one block, coordinated by
//! `BM_IO_IN_PROGRESS` so two racers never both read.
//!
//! Lock-across-await discipline (rules.md section 5): a buffer-table shard lock,
//! the header lock, and the content lock are ALL brief synchronous critical
//! sections. The pattern for every I/O is: resolve the buffer under the lock,
//! DROP the lock, `.await` the smgr op (with `BM_IO_IN_PROGRESS` claimed so the
//! page slot is ours), then re-lock to finalize via `terminate_buffer_io`. No
//! sync lock guard is ever held across an `.await`; the backend future stays
//! `Send` (the pin cache is `task_local`, the page slot access is by raw split,
//! and the smgr future is `Send`).

use std::sync::Arc;

use crate::backend::access::transam::xlog::xlog_flush;
use crate::catalog::pg_class::RELPERSISTENCE_PERMANENT;
use crate::common::relpath::ForkNumber;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::buf::{BufId, Buffer};
use crate::storage::buf_internals::{
    BUF_USAGECOUNT_ONE, BufFlags, BufferTag, buf_state_get_refcount,
};
use crate::storage::bufmgr::ReadBufferMode;
use crate::storage::bufpage::Page;
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;

use super::buf_init::BufferPool;

/// Content-lock modes for [`lock_buffer`] (C: `BUFFER_LOCK_*` in bufmgr.h).
pub const BUFFER_LOCK_UNLOCK: i32 = 0;
pub const BUFFER_LOCK_SHARE: i32 = 1;
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

/// C: `AbortBufferIO`. An unwind guard around an smgr read/write `.await` held
/// under `BM_IO_IN_PROGRESS`. smgr raises errors as panics; if the await
/// unwinds, the IO would otherwise stay claimed forever and every later reader
/// plus any task parked in `wait_io` would hang. While `armed`, `Drop` clears
/// `BM_IO_IN_PROGRESS`, sets `BM_IO_ERROR`, and wakes all waiters (which then
/// re-claim the IO via `start_buffer_io`). The happy path disarms it after the
/// normal `terminate_buffer_io` runs, so Drop is a no-op on success.
///
/// Holds only `&pool + buf_id + a bool`: no lock is held across the await.
struct InProgressIo<'a> {
    pool: &'a BufferPool,
    buf_id: i32,
    armed: bool,
}

impl<'a> InProgressIo<'a> {
    /// Arm a guard for an IO this task has already claimed via `start_buffer_io`.
    fn new(pool: &'a BufferPool, buf_id: i32) -> Self {
        InProgressIo { pool, buf_id, armed: true }
    }

    /// Disarm after the normal terminate_buffer_io has run: Drop becomes a no-op.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InProgressIo<'_> {
    fn drop(&mut self) {
        if self.armed {
            // Error path (panicked await): clear BM_IO_IN_PROGRESS, set
            // BM_IO_ERROR, leave the buffer not-valid and (for a write) dirty,
            // and wake all waiters so the next caller re-claims the IO.
            self.pool
                .terminate_buffer_io(self.buf_id, false, BufFlags::IO_ERROR.bits());
        }
    }
}

impl BufferPool {
    /// C: `BufferAlloc`. Look up (and pin) the buffer for `tag`, allocating and
    /// evicting a victim on a miss. Returns `(buf_id, found)`: `found == true`
    /// means the page is already resident (a hit, or a concurrent inserter beat
    /// us). `found == false` means THIS task must read/zero the page into the
    /// returned (pinned, tag-valid, contents-invalid) buffer.
    ///
    /// No buffer-table shard lock or header lock is held across the eviction
    /// flush `.await`; the victim is pinned (so it cannot be re-stolen) before
    /// the flush, exactly as C.
    ///
    /// `shared` is needed because evicting a dirty victim flushes it via smgr.
    #[allow(
        clippy::never_loop,
        reason = "retry is encapsulated in the awaited callee (get_victim_buffer); loop kept for PG structural parity"
    )]
    async fn buffer_alloc(
        self: &Arc<Self>,
        shared: &Arc<SharedState>,
        tag: BufferTag,
        relpersistence: i8,
    ) -> (i32, bool) {
        let hash = crate::backend::storage::buffer::buf_table::BufTable::hash_code(&tag);

        // 1) Fast path: already in the pool? (shard lock taken + dropped here).
        if let Some(existing) = self.buf_table.lookup(&tag, hash) {
            let valid = self.pin_buffer(existing);
            // found, but if not valid another task is mid-read -> we must wait/read.
            return (existing, valid);
        }

        // 2) Miss. Loop because a chosen victim can become unusable concurrently.
        loop {
            // Acquire a victim (pinned, possibly flushed). No lock held now.
            let victim = self.get_victim_buffer(shared).await;

            // Try to claim the tag. Shard lock taken + dropped synchronously.
            if let Some(existing) = self.buf_table.insert(&tag, hash, victim) {
                // Someone inserted the same tag first. Give up the victim
                // (unpin + free) and use the existing buffer (C double-check).
                self.unpin_buffer(victim);
                self.strategy.free_buffer(self, victim);
                let valid = self.pin_buffer(existing);
                return (existing, valid);
            }
            // We own the tag. Set up the victim's descriptor: assign the
            // tag and BM_TAG_VALID + a starting usagecount under the
            // header lock. The victim is pinned (refcount == 1), invalid.
            {
                let desc = self.descriptor(victim);
                let buf_state = desc.lock_hdr();
                debug_assert_eq!(buf_state_get_refcount(buf_state), 1);
                debug_assert_eq!(
                    buf_state
                        & (BufFlags::TAG_VALID.bits()
                            | BufFlags::VALID.bits()
                            | BufFlags::DIRTY.bits()
                            | BufFlags::IO_IN_PROGRESS.bits()), 0
                );
                // SAFETY: header lock held; tag write is serialized by it and
                // the victim is pinned so no clock-sweep can take it.
                self.descriptor(victim).set_tag(tag);
                let mut new = buf_state | BufFlags::TAG_VALID.bits() | BUF_USAGECOUNT_ONE;
                if relpersistence == RELPERSISTENCE_PERMANENT
                    || tag.fork_num() == ForkNumber::INIT_FORKNUM
                {
                    new |= BufFlags::PERMANENT.bits();
                }
                desc.unlock_hdr(new);
                return (victim, false);
            }
        }
    }

    /// C: `GetVictimBuffer`. Obtain a reusable, pinned buffer: pick a clock-sweep
    /// victim (header-locked candidate from Part A), pin it, flush it if dirty,
    /// and detach its old tag from the buffer table. Returns the 0-based buf_id,
    /// pinned exactly once, tag/contents invalid.
    ///
    /// The flush `.await` runs with NO buffer-table or header lock held; the
    /// victim is pinned across it so it stays ours.
    async fn get_victim_buffer(self: &Arc<Self>, shared: &Arc<SharedState>) -> i32 {
        loop {
            // Part A: clock-sweep candidate returned with header lock held.
            let (buf_id, buf_state) = self.strategy.get_buffer(self);
            debug_assert_eq!(buf_state_get_refcount(buf_state), 0);
            // Pin (releases the header lock in the same publish).
            self.pin_buffer_locked(buf_id, buf_state);

            if buf_state & BufFlags::DIRTY.bits() != 0 {
                // Flush the dirty victim before reusing it. The page is read
                // under BM_IO_IN_PROGRESS inside FlushBuffer; we hold the pin.
                // (C share-locks the content here against hint-bit writers; in
                // this port hint-bit updates also take the content lock, and the
                // single-writer page invariant is upheld by BM_IO_IN_PROGRESS,
                // so the explicit content lock is unnecessary for correctness of
                // the bytes. TODO: take content share-lock once readers exist.)
                self.flush_buffer(shared, buf_id, None).await;
            }

            // Detach the old tag from the buffer table, if any. Can fail if
            // another task re-pinned/re-dirtied the victim meanwhile -> retry.
            if buf_state & BufFlags::TAG_VALID.bits() != 0 && !self.invalidate_victim(buf_id) {
                self.unpin_buffer(buf_id);
                continue;
            }
            return buf_id;
        }
    }

    /// C: `InvalidateVictimBuffer`. With the victim pinned (refcount must be 1),
    /// remove its tag from the buffer table and clear its tag/flags/usagecount.
    /// Returns `false` if it was re-pinned or re-dirtied since (caller retries).
    fn invalidate_victim(&self, buf_id: i32) -> bool {
        let desc = self.descriptor(buf_id);
        // Pinned, so the tag is stable to read without the header lock.
        let tag = desc.tag_copy();
        let hash = crate::backend::storage::buffer::buf_table::BufTable::hash_code(&tag);

        // Header lock to inspect/clear; the shard delete is a separate brief
        // critical section (both sync, neither across an await).
        let buf_state = desc.lock_hdr();
        debug_assert!(buf_state & BufFlags::TAG_VALID.bits() != 0);
        if buf_state_get_refcount(buf_state) != 1 || buf_state & BufFlags::DIRTY.bits() != 0 {
            desc.unlock_hdr(buf_state);
            return false;
        }
        desc.clear_tag();
        let cleared =
            buf_state & !(crate::storage::buf_internals::BUF_FLAG_MASK
                | crate::storage::buf_internals::BUF_USAGECOUNT_MASK);
        desc.unlock_hdr(cleared);

        self.buf_table.delete(&tag, hash);
        true
    }

    /// C: `FlushBuffer`. Write a dirty buffer to disk. WAL-before-data: flush WAL
    /// up to the page LSN first (stub now -- step 13), then `smgrwrite` the page,
    /// then `terminate_buffer_io(clear_dirty)`. If `StartBufferIO(for_input=false)`
    /// reports the buffer is already clean / being flushed, this is a no-op.
    ///
    /// `reln`: the victim/flush relation may differ from the caller's; when
    /// `None` we open an smgr for the buffer's own tag.
    ///
    /// The `smgrwrite` `.await` runs with no buffer lock held; `BM_IO_IN_PROGRESS`
    /// (set by `start_buffer_io`) gives this task sole right to the page slot.
    pub async fn flush_buffer(
        self: &Arc<Self>,
        shared: &Arc<SharedState>,
        buf_id: i32,
        reln: Option<&mut SmgrRelation>,
    ) {
        // Claim the write IO; bail if someone else already flushed it.
        if !self.start_buffer_io(buf_id, false).await {
            return;
        }
        // Unwind guard: a panicking smgrwrite must not leak BM_IO_IN_PROGRESS.
        let mut io_guard = InProgressIo::new(self, buf_id);

        let desc = self.descriptor(buf_id);
        // Read the page LSN under the header lock, and clear BM_JUST_DIRTIED so
        // a concurrent dirtier after this point keeps the buffer dirty (C).
        let buf_state = desc.lock_hdr();
        let recptr = self.block(buf_id).get_lsn();
        let permanent = buf_state & BufFlags::PERMANENT.bits() != 0;
        desc.unlock_hdr(buf_state & !BufFlags::JUST_DIRTIED.bits());

        // WAL-before-data rule: log must hit disk before the data page it
        // describes. Skipped for non-permanent buffers (no real LSNs). The
        // group-commit point; xlog_flush fast-paths an invalid/already-flushed
        // LSN, so no is_valid guard is needed here.
        if permanent {
            xlog_flush(shared.xlog(), recptr).await;
        }

        let tag = desc.tag_copy();
        let forknum = tag.fork_num();
        let blocknum = tag.block_num;

        // Do the write with no buffer lock held; BM_IO_IN_PROGRESS protects the
        // page slot. The page bytes are read-only here (smgrwrite takes &Page).
        let page: &Page = self.block(buf_id);
        if let Some(r) = reln { r.write(shared, forknum, blocknum, page, false).await } else {
            // Open an smgr for the buffer's own relation (C: smgropen on the
            // tag with INVALID_PROC_NUMBER). Owned on the stack, no cache
            // borrow across the await.
            let rlocator: RelFileLocator = tag.rel_file_locator();
            let mut smgr = SmgrRelation::open(rlocator, INVALID_PROC_NUMBER);
            smgr.write(shared, forknum, blocknum, page, false).await;
        }

        // Mark clean (unless re-dirtied) and end the IO; wakes any WaitIO waiter.
        self.terminate_buffer_io(buf_id, true, 0);
        io_guard.disarm();
    }

    /// C: `MarkBufferDirty`. Set `BM_DIRTY | BM_JUST_DIRTIED` via a header-lock
    /// CAS loop. The caller must hold the buffer pinned and (in PG) the content
    /// lock exclusively; we assert the pin.
    pub fn mark_buffer_dirty(&self, buffer: Buffer) {
        debug_assert!(crate::backend::storage::buffer::buf_init::private_refcount(buffer) > 0);
        let buf_id = global_buf_id(buffer);
        let desc = self.descriptor(buf_id);
        let buf_state = desc.lock_hdr();
        debug_assert!(buf_state_get_refcount(buf_state) > 0);
        desc.unlock_hdr(buf_state | BufFlags::DIRTY.bits() | BufFlags::JUST_DIRTIED.bits());
    }

    /// C: `MarkBufferDirtyHint`. The non-critical, possibly-share-locked dirty
    /// mark. WAL/checksum FPI handling (C cases 1-3) is deferred; here
    /// it sets `BM_DIRTY | BM_JUST_DIRTIED` like `mark_buffer_dirty`.
    pub fn mark_buffer_dirty_hint(&self, buffer: Buffer, _buffer_std: bool) {
        // TODO(xlog): XLOG_FPI_FOR_HINT record when checksums/wal_log_hints on.
        let buf_id = global_buf_id(buffer);
        let desc = self.descriptor(buf_id);
        let buf_state = desc.lock_hdr();
        if buf_state_get_refcount(buf_state) > 0 {
            desc.unlock_hdr(buf_state | BufFlags::DIRTY.bits() | BufFlags::JUST_DIRTIED.bits());
        } else {
            desc.unlock_hdr(buf_state);
        }
    }

    /// C: `LockBuffer`. Acquire/release the buffer's content lock (the naked
    /// `RwLock`). UNLOCK drops a held guard; SHARE/EXCLUSIVE acquire it. The
    /// returned guard (for SHARE/EXCLUSIVE) keeps the lock until dropped.
    ///
    /// The content lock guards SYNCHRONOUS page access between lock and unlock --
    /// it is NEVER held across an smgr `.await`. The pin (not this lock) keeps
    /// the page resident.
    ///
    /// Modeled as explicit guard acquisition rather than C's mode-int + global
    /// "which lock do I hold" tracking; see [`content_share`] / [`content_exclusive`].
    pub fn content_share(&self, buffer: Buffer) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.descriptor(global_buf_id(buffer)).content_lock.read()
    }

    /// Exclusive content lock guard. See [`content_share`].
    pub fn content_exclusive(&self, buffer: Buffer) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.descriptor(global_buf_id(buffer)).content_lock.write()
    }

    /// C: `LockBufferForCleanup`. Exclusive content lock plus a wait until this
    /// task is the sole pinner (refcount == 1). The cleanup wait is a simple
    /// await loop on the per-buffer IO queue for now.
    ///
    /// TODO: a full ConditionVariable (BM_PIN_COUNT_WAITER + wait_backend) like
    /// C, so an unpinner wakes the cleanup waiter precisely instead of polling.
    /// The caller must already hold the buffer pinned exactly once. Returns the
    /// held exclusive content-lock guard (the cleanup lock); the page may be
    /// mutated until it is dropped.
    pub async fn lock_buffer_for_cleanup(
        self: &Arc<Self>,
        buffer: Buffer,
    ) -> parking_lot::RwLockWriteGuard<'_, ()> {
        let buf_id = global_buf_id(buffer);
        debug_assert_eq!(
            crate::backend::storage::buffer::buf_init::private_refcount(buffer),
            1
        );
        loop {
            let guard = self.content_exclusive(buffer);
            let buf_state = self.descriptor(buf_id).lock_hdr();
            let refcount = buf_state_get_refcount(buf_state);
            self.descriptor(buf_id).unlock_hdr(buf_state);
            if refcount == 1 {
                // Sole pinner: hand the exclusive guard back, lock still held.
                return guard;
            }
            // Another pinner exists. Drop the lock and wait for an unpin, then
            // retry. TODO: a full ConditionVariable (BM_PIN_COUNT_WAITER +
            // wait_backend_pgprocno) so UnpinBuffer wakes us precisely instead of
            // this poll-yield loop.
            drop(guard);
            tokio::task::yield_now().await;
        }
    }

    /// C: `ReleaseBuffer`. Drop one pin on a shared buffer. (Local buffers are
    /// `localbuf.c`, not yet ported.)
    pub fn release_buffer(&self, buffer: Buffer) {
        self.unpin_buffer(global_buf_id(buffer));
    }

    /// C: `IncrBufferRefCount`. Add a pin to an already-pinned buffer. Cheap: a
    /// same-task pin only bumps the private count (Part A `pin_buffer`).
    pub fn incr_buffer_ref_count(self: &Arc<Self>, buffer: Buffer) {
        debug_assert!(
            crate::backend::storage::buffer::buf_init::private_refcount(buffer) > 0,
            "IncrBufferRefCount on an unpinned buffer"
        );
        self.pin_buffer(global_buf_id(buffer));
    }

    /// C: `BufferGetBlockNumber`. The block number held by a pinned buffer.
    pub fn buffer_get_block_number(&self, buffer: Buffer) -> BlockNumber {
        self.descriptor(global_buf_id(buffer)).tag_copy().block_num
    }

    /// C: `BufferGetTag`. (rlocator, forknum, blocknum) for a pinned buffer.
    pub fn buffer_get_tag(&self, buffer: Buffer) -> (RelFileLocator, ForkNumber, BlockNumber) {
        let tag = self.descriptor(global_buf_id(buffer)).tag_copy();
        (tag.rel_file_locator(), tag.fork_num(), tag.block_num)
    }

    /// C: `BufferGetPage`. The page image of a pinned buffer (read-only view).
    /// Callers mutating the page hold the exclusive content lock and use
    /// [`BufferPool::block_mut`].
    pub fn buffer_get_page(&self, buffer: Buffer) -> &Page {
        self.block(global_buf_id(buffer))
    }
}

/// The 0-based shared-pool index of a global buffer handle, as the i32 the
/// internal pool methods use. Panics on a non-global handle (these entry points
/// are shared-buffer ops; local buffers go through `localbuf`).
#[inline]
fn global_buf_id(buffer: Buffer) -> i32 {
    #[allow(
        clippy::expect_used,
        reason = "callers pass only shared buffers to this private helper"
    )]
    let id = buffer.as_global().expect("shared (global) buffer expected") as i32;
    id
}

/// C: `ReadBuffer_common`. The shared-buffer read core, collapsed to a direct
/// async read (no pgaio). Returns a pinned buffer holding `blocknum` of
/// `forknum`. On a miss this task reads (or zeroes) the page; on a hit it just
/// pins. Concurrent misses on the same block are serialized by
/// `BM_IO_IN_PROGRESS`: exactly one reads, the rest wait and observe the result.
///
/// `relpersistence` selects BM_PERMANENT. P_NEW (extend) is handled here by
/// extending the fork to a new last block. RBM_ZERO* modes zero the page instead
/// of reading.
///
/// No buffer-table/header/content lock is held across the smgr `.await`.
#[allow(
    clippy::never_loop,
    reason = "retry is encapsulated in the awaited callee (start_buffer_io); loop kept for PG structural parity"
)]
pub async fn read_buffer_common(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    mode: ReadBufferMode,
    _strategy: Option<&crate::storage::buf::BufferAccessStrategy>,
) -> Buffer {
    let pool = shared.buffers().clone();

    // P_NEW: extend the relation by one block, read that new block number.
    let is_new = blocknum == crate::storage::bufmgr::P_NEW;
    let target_block = if is_new {
        smgr.nblocks(shared, forknum).await
    } else {
        blocknum
    };

    let tag = BufferTag::init(&smgr.rlocator.locator, forknum, target_block);
    let (buf_id, found) = pool.buffer_alloc(shared, tag, relpersistence).await;
    let buffer = BufId::Global(buf_id as u32);

    let zero = matches!(
        mode,
        ReadBufferMode::ZERO_AND_LOCK | ReadBufferMode::ZERO_AND_CLEANUP_LOCK
    ) || is_new;

    if found && pool.descriptor(buf_id).state.load(std::sync::atomic::Ordering::Acquire)
        & BufFlags::VALID.bits()
        != 0
    {
        // Hit on an already-valid page: done.
        return buffer;
    }

    // Either a miss (we own a fresh, invalid, pinned buffer) or a hit on a
    // not-yet-valid buffer (another task is mid-read, or a prior read failed and
    // left BM_IO_ERROR). `start_buffer_io` handles both: it returns true if THIS
    // task must do the read (including re-attempting after a prior failure, since
    // a failed IO leaves the buffer not-valid + IO_IN_PROGRESS clear), or false
    // (after awaiting the in-flight read) once the page is valid. Loop so that a
    // waiter woken to a still-not-valid buffer (the doer failed) re-claims the IO
    // rather than returning an invalid buffer.
    loop {
        if !pool.start_buffer_io(buf_id, true).await {
            // Another task completed the read successfully; it's valid.
            return buffer;
        }
        // We claimed the read IO. Guard against a panicking smgr read leaking
        // BM_IO_IN_PROGRESS (would hang every later reader + parked waiter).
        let mut io_guard = InProgressIo::new(&pool, buf_id);
        if zero {
            // Zero the page instead of reading. SAFETY: BM_IO_IN_PROGRESS held.
            unsafe { pool.block_mut(buf_id) }.as_mut_bytes().fill(0);
            if is_new {
                // Materialize the new block on disk so nblocks reflects it.
                let page = pool.block(buf_id);
                smgr.extend(shared, forknum, target_block, page, false).await;
            }
        } else {
            // SAFETY: this task won start_buffer_io, so BM_IO_IN_PROGRESS gives
            // it sole access to the page slot; no other &mut Page is live.
            let page = unsafe { pool.block_mut(buf_id) };
            smgr.read(shared, forknum, target_block, page).await;
        }
        // Publish BM_VALID and wake any waiters.
        pool.terminate_buffer_io(buf_id, false, BufFlags::VALID.bits());
        io_guard.disarm();
        return buffer;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::buffer::buf_init::with_private_refcount;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::procnumber::INVALID_PROC_NUMBER;

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: Oid(1663), dbOid: Oid(50000 + rel), relNumber: Oid(16000 + rel) }
    }

    async fn shared_with_tmpdir(tag: &str, nbuffers: usize) -> (Arc<SharedState>, std::path::PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "pepperdb_bufmgr_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        crate::storage::io_backend::mkdir_all(dir.join("base").join((50000 + 1).to_string()))
            .await
            .ok();
        let cfg = SharedStateConfig { nbuffers, ..SharedStateConfig::default() };
        let s = SharedState::new(cfg);
        s.config().set_data_dir(dir.to_string_lossy().into_owned());
        (s, dir)
    }

    /// Create a relation and extend it with `n` pages of distinct byte patterns
    /// via smgr (bypassing the buffer pool), so reads through bufmgr hit disk.
    async fn make_rel_with_pages(
        s: &Arc<SharedState>,
        rel: u32,
        n: u8,
    ) -> SmgrRelation {
        let mut reln = SmgrRelation::open(rloc(rel), INVALID_PROC_NUMBER);
        let fork = ForkNumber::MAIN_FORKNUM;
        reln.create(s, fork, false).await;
        for i in 0..n {
            let mut p = Page::boxed_zeroed();
            p.as_mut_bytes().fill(0x10 + i);
            // Keep the page LSN (first 8 bytes) zero: no WAL writer exists in the
            // foundation stage, so a real data page has no LSN yet, and a zero
            // LSN makes FlushBuffer's WAL-before-data flush a no-op (step 13).
            p.set_lsn(crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
            reln.extend(s, fork, BlockNumber::from(i), &p, true).await;
        }
        reln
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn miss_reads_page_then_hit_reuses_without_io() {
        let (s, dir) = shared_with_tmpdir("hit", 16).await;
        with_private_refcount(|| async {
            let mut reln = make_rel_with_pages(&s, 1, 3).await;
            let fork = ForkNumber::MAIN_FORKNUM;
            let pool = s.buffers().clone();

            // Miss: read block 1 -> valid pinned buffer with the right contents.
            let b = read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, fork, 1,
                ReadBufferMode::NORMAL, None,
            )
            .await;
            assert!(b.is_valid());
            let buf_id = b.as_global().unwrap() as i32;
            assert!(
                pool.descriptor(buf_id).state.load(std::sync::atomic::Ordering::Acquire)
                    & BufFlags::VALID.bits()
                    != 0
            );
            assert!(pool.buffer_get_page(b).as_bytes()[8..].iter().all(|&x| x == 0x11));
            let priv_before =
                crate::backend::storage::buffer::buf_init::private_refcount(b);
            assert_eq!(priv_before, 1);

            // Hit: second read of the same block returns the SAME buffer and pins
            // again, with no new IO. A same-TASK re-pin only bumps the private
            // count (PG PrivateRefCount fast path); the shared refcount stays put.
            let b2 = read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, fork, 1,
                ReadBufferMode::NORMAL, None,
            )
            .await;
            assert_eq!(b2, b, "hit returns the same buffer");
            assert_eq!(
                crate::backend::storage::buffer::buf_init::private_refcount(b),
                priv_before + 1,
                "same-task hit bumps the private pin count"
            );
            // The buffer stays valid and resident (no re-read).
            assert!(
                pool.descriptor(buf_id).state.load(std::sync::atomic::Ordering::Acquire)
                    & BufFlags::VALID.bits()
                    != 0
            );

            pool.release_buffer(b);
            pool.release_buffer(b2);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_racers_one_reads_both_get_valid_buffer() {
        let (s, dir) = shared_with_tmpdir("race", 16).await;
        let reln = make_rel_with_pages(&s, 1, 2).await;
        let fork = ForkNumber::MAIN_FORKNUM;

        // Two tasks read the same missing block concurrently. Each opens its own
        // smgr handle (owned per task). Exactly one performs the smgr read; both
        // end with a valid buffer for the same block.
        let s1 = s.clone();
        let s2 = s.clone();
        let loc = reln.rlocator.locator;
        let t1 = tokio::spawn(async move {
            with_private_refcount(|| async {
                let mut r = SmgrRelation::open(loc, INVALID_PROC_NUMBER);
                read_buffer_common(&s1, &mut r, RELPERSISTENCE_PERMANENT, fork, 0, ReadBufferMode::NORMAL, None).await
            })
            .await
        });
        let t2 = tokio::spawn(async move {
            with_private_refcount(|| async {
                let mut r = SmgrRelation::open(loc, INVALID_PROC_NUMBER);
                read_buffer_common(&s2, &mut r, RELPERSISTENCE_PERMANENT, fork, 0, ReadBufferMode::NORMAL, None).await
            })
            .await
        });
        let b1 = t1.await.unwrap();
        let b2 = t2.await.unwrap();
        assert_eq!(b1, b2, "both racers land on the same buffer");
        let pool = s.buffers();
        assert!(
            pool.descriptor(b1.as_global().unwrap() as i32).state.load(std::sync::atomic::Ordering::Acquire)
                & BufFlags::VALID.bits()
                != 0
        );
        assert!(pool.buffer_get_page(b1).as_bytes()[8..].iter().all(|&x| x == 0x10));
        assert!(pool.descriptor(b1.as_global().unwrap() as i32).io_cv.is_empty(), "no lingering IO waiters");

        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mark_dirty_then_flush_writes_back() {
        let (s, dir) = shared_with_tmpdir("flush", 16).await;
        with_private_refcount(|| async {
            let mut reln = make_rel_with_pages(&s, 1, 1).await;
            let fork = ForkNumber::MAIN_FORKNUM;
            let pool = s.buffers().clone();

            let b = read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, fork, 0,
                ReadBufferMode::NORMAL, None,
            )
            .await;
            let buf_id = b.as_global().unwrap() as i32;

            // Mutate the page under the exclusive content lock, mark dirty.
            {
                let _g = pool.content_exclusive(b);
                // SAFETY: exclusive content lock held -> sole writer.
                let page = unsafe { pool.block_mut(buf_id) };
                page.as_mut_bytes().fill(0xC3);
                page.set_lsn(crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
            }
            pool.mark_buffer_dirty(b);
            assert!(
                pool.descriptor(buf_id).state.load(std::sync::atomic::Ordering::Acquire)
                    & BufFlags::DIRTY.bits()
                    != 0
            );

            // Flush to disk via smgr.
            pool.flush_buffer(&s, buf_id, Some(&mut reln)).await;
            assert!(
                pool.descriptor(buf_id).state.load(std::sync::atomic::Ordering::Acquire)
                    & BufFlags::DIRTY.bits()
                    == 0,
                "flush clears BM_DIRTY"
            );

            // Read back through smgr directly: the new bytes are on disk. (The
            // first 8 bytes are the zeroed LSN; the body carries the pattern.)
            let mut check = Page::boxed_zeroed();
            reln.read(&s, fork, 0, &mut check).await;
            assert!(check.as_bytes()[8..].iter().all(|&x| x == 0xC3), "flushed bytes hit disk");

            pool.release_buffer(b);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn eviction_flushes_dirty_victim_and_swaps_tag() {
        // Pool of 1 buffer: read block 0 (dirty it), then read block 1 -> the
        // sole buffer is evicted; the dirty block-0 page is flushed and block 1
        // is loaded. Block 0's tag must be gone from the buffer table.
        let (s, dir) = shared_with_tmpdir("evict", 1).await;
        with_private_refcount(|| async {
            let mut reln = make_rel_with_pages(&s, 1, 2).await;
            let fork = ForkNumber::MAIN_FORKNUM;
            let pool = s.buffers().clone();

            let b0 = read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, fork, 0,
                ReadBufferMode::NORMAL, None,
            )
            .await;
            // Dirty block 0 with a new pattern, then unpin so it is evictable.
            {
                let _g = pool.content_exclusive(b0);
                let page = unsafe { pool.block_mut(b0.as_global().unwrap() as i32) };
                page.as_mut_bytes().fill(0x5A);
                page.set_lsn(crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
            }
            pool.mark_buffer_dirty(b0);
            let tag0 = pool.descriptor(b0.as_global().unwrap() as i32).tag_copy();
            pool.release_buffer(b0);

            // Read block 1: forces eviction of block 0 (flush + tag swap).
            let b1 = read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, fork, 1,
                ReadBufferMode::NORMAL, None,
            )
            .await;
            assert!(pool.buffer_get_page(b1).as_bytes()[8..].iter().all(|&x| x == 0x11));

            // Block 0's tag is no longer mapped.
            let hash = crate::backend::storage::buffer::buf_table::BufTable::hash_code(&tag0);
            assert_eq!(pool.buf_table.lookup(&tag0, hash), None, "evicted tag removed");

            // The dirty block 0 was flushed: its 0x5A body bytes are on disk.
            let mut check = Page::boxed_zeroed();
            reln.read(&s, fork, 0, &mut check).await;
            assert!(check.as_bytes()[8..].iter().all(|&x| x == 0x5A), "dirty victim flushed on evict");

            pool.release_buffer(b1);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lock_buffer_share_and_exclusive() {
        let (s, dir) = shared_with_tmpdir("lock", 16).await;
        with_private_refcount(|| async {
            let mut reln = make_rel_with_pages(&s, 1, 1).await;
            let fork = ForkNumber::MAIN_FORKNUM;
            let pool = s.buffers().clone();
            let b = read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, fork, 0,
                ReadBufferMode::NORMAL, None,
            )
            .await;

            // Two share locks coexist; an exclusive lock is mutually exclusive.
            {
                let _s1 = pool.content_share(b);
                let _s2 = pool.content_share(b);
                assert!(pool.descriptor(b.as_global().unwrap() as i32).content_lock.try_write().is_none());
            }
            {
                let _x = pool.content_exclusive(b);
                assert!(pool.descriptor(b.as_global().unwrap() as i32).content_lock.try_read().is_none());
            }
            // Released again.
            assert!(pool.descriptor(b.as_global().unwrap() as i32).content_lock.try_write().is_some());
            pool.release_buffer(b);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn io_error_guard_on_unwind_clears_in_progress_sets_error_and_wakes_waiter() {
        use std::sync::atomic::Ordering;
        let p = std::sync::Arc::new(BufferPool::new(1));

        // This task claims the read IO (buffer not valid, not dirty -> needs IO).
        assert!(p.start_buffer_io(0, true).await);

        // A second task parks in wait_io while the IO is "in progress".
        let pw = p.clone();
        let waiter = tokio::spawn(async move {
            pw.wait_io(0).await;
            // After waking, the doer FAILED (BM_IO_ERROR set, not valid), so this
            // task must re-claim the IO itself rather than trust the buffer.
            let must_redo = pw.start_buffer_io(0, true).await;
            (must_redo, pw.descriptor(0).state.load(Ordering::Acquire))
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter blocks while IO in progress");

        // Simulate a panicked smgr await: build the guard and drop it WITHOUT
        // disarming. Drop must mirror AbortBufferIO.
        {
            let _g = InProgressIo::new(&p, 0);
            // not disarmed -> Drop fires here
        }

        let st = p.descriptor(0).state.load(Ordering::Acquire);
        assert!(st & BufFlags::IO_IN_PROGRESS.bits() == 0, "IO_IN_PROGRESS cleared");
        assert!(st & BufFlags::IO_ERROR.bits() != 0, "BM_IO_ERROR set on error path");
        assert!(st & BufFlags::VALID.bits() == 0, "buffer not marked valid on error");

        // The parked waiter wakes, re-claims the IO (start_buffer_io returns true
        // because the buffer is neither valid nor mid-IO), and clears IO_ERROR.
        let (must_redo, st_after_claim) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            waiter,
        )
        .await
        .expect("waiter wakes after the guard's error Drop")
        .unwrap();
        assert!(must_redo, "woken waiter re-claims the IO after an error");
        assert!(
            st_after_claim & BufFlags::IO_IN_PROGRESS.bits() != 0,
            "the re-claimer now owns the IO"
        );
        assert!(
            st_after_claim & BufFlags::IO_ERROR.bits() == 0,
            "claiming the IO clears the prior BM_IO_ERROR"
        );
        // Finish the re-attempted IO so no waiters linger.
        p.terminate_buffer_io(0, false, BufFlags::VALID.bits());
        assert!(p.descriptor(0).io_cv.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn io_error_guard_disarm_on_success_leaves_no_error() {
        use std::sync::atomic::Ordering;
        let p = std::sync::Arc::new(BufferPool::new(1));
        assert!(p.start_buffer_io(0, true).await);
        {
            let mut g = InProgressIo::new(&p, 0);
            // Happy path: terminate normally then disarm.
            p.terminate_buffer_io(0, false, BufFlags::VALID.bits());
            g.disarm();
        } // Drop is a no-op (disarmed).
        let st = p.descriptor(0).state.load(Ordering::Acquire);
        assert!(st & BufFlags::VALID.bits() != 0, "buffer is valid");
        assert!(st & BufFlags::IO_ERROR.bits() == 0, "no spurious BM_IO_ERROR on success");
        assert!(st & BufFlags::IO_IN_PROGRESS.bits() == 0, "IO finished");
    }

    #[test]
    fn read_future_is_send() {
        // The backend spawns on the multi-thread runtime, so the read future
        // must be Send (no !Send borrow across an await).
        fn assert_send<T: Send>(_: T) {}
        let s = SharedState::new(SharedStateConfig::default());
        let mut reln = SmgrRelation::open(rloc(1), INVALID_PROC_NUMBER);
        assert_send(async move {
            read_buffer_common(
                &s, &mut reln, RELPERSISTENCE_PERMANENT, ForkNumber::MAIN_FORKNUM, 0,
                ReadBufferMode::NORMAL, None,
            )
            .await
        });
    }
}
