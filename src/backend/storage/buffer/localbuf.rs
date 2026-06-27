//! Translated from PostgreSQL src/backend/storage/buffer/localbuf.c -- the local
//! buffer manager for temporary relations, which never need WAL, checkpointing,
//! or shared locking.
//!
//! C shape: a backend-private set of parallel arrays (`LocalBufferDescriptors`,
//! `LocalBufferBlockPointers`, `LocalRefCount`) plus a `LocalBufHash`, all
//! process-local statics, with a clock sweep (`GetLocalVictimBuffer`) and no
//! atomics/locks (no other process can see a temp relation). C identifies local
//! buffers by a negative `Buffer` (`-i - 1`); here that is [`BufId::Local`].
//!
//! PepperDB shape (rules.md 6.1: per-task state must be `Send`): a temp relation
//! is private to its backend, which is a tokio task -- so the whole local pool is
//! ONE per-task value behind a tokio `task_local!` `RefCell<LocalBufferPool>`.
//! The map/descriptors/blocks are touched only inside synchronous sections
//! (borrow, mutate, drop the borrow); the `RefCell` is never held across an
//! `.await`, so the backend future stays `Send` (the `task_local` follows the
//! task across thread migration, exactly like `PrivateRefCount`).
//!
//! Buffer handle: a user-facing local buffer is the [`BufId::Local`] variant
//! carrying `index`, the
//! 0-based pool index. (C overloaded a negative `Buffer = -index - 1`; the enum
//! replaces the sign trick.) We index the pool arrays by the 0-based `index`
//! directly and convert at the API edge via [`local_buf_index`] / [`local_buffer`].
//!
//! No `BM_IO_IN_PROGRESS` / IO CV / WaitQueue: a temp buffer is single-task, so
//! there is no concurrent reader/writer to coordinate. The only async part is the
//! smgr flush write; the local-pool borrow is dropped before that `.await`
//! (`FlushLocalBuffer` copies the page bytes out, releases the borrow, awaits).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::relpath::ForkNumber;
use crate::shared_state::SharedState;
use crate::storage::block::{BlockNumber, MAX_BLOCK_NUMBER};
use crate::storage::buf::{BufId, Buffer};
use crate::storage::buf_internals::{
    BUF_USAGECOUNT_ONE, BUF_FLAG_MASK, BUF_REFCOUNT_ONE, BUF_USAGECOUNT_MASK, BM_MAX_USAGE_COUNT,
    BufFlags, BufferTag, buf_state_get_refcount, buf_state_get_usagecount,
};
use crate::storage::bufpage::Page;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;

/// Default number of local (temp) buffers per backend. C: the `temp_buffers`
/// GUC (`num_temp_buffers`), default 1024. Kept as a module constant until the
/// GUC plumbing reaches this subsystem.
pub const DEFAULT_NUM_TEMP_BUFFERS: usize = 1024;

/// The 0-based pool index of a local buffer handle. Panics on a non-local handle.
/// C: the negative-`Buffer` decode `-buffer - 1`, now [`BufId::Local`] extraction.
#[inline]
pub fn local_buf_index(buffer: Buffer) -> usize {
    buffer.as_local().expect("expected a local buffer") as usize
}

/// The local buffer handle for a 0-based pool index. C: the negative-`Buffer`
/// encode `-index - 1`, now a [`BufId::Local`].
#[inline]
pub fn local_buffer(index: usize) -> Buffer {
    BufId::Local(index as u32)
}

/// Per-local-buffer descriptor. C: `BufferDesc`, but for a single-task pool the
/// concurrency bits are gone -- `buf_state` is a plain `u32` (only `BM_DIRTY` /
/// `BM_VALID` / `BM_TAG_VALID` and the usagecount are meaningful) and the pin
/// count is a separate field (C: `LocalRefCount[]`).
pub struct LocalBufferDesc {
    /// Tag of the page held, or the invalid sentinel when not tag-valid.
    pub tag: BufferTag,
    /// Packed flags + usagecount + refcount, like the shared `state` word, but a
    /// plain `u32` (no atomics: single task). Only `BM_DIRTY`/`BM_VALID`/
    /// `BM_TAG_VALID` and the usagecount/refcount fields are used.
    pub buf_state: u32,
    /// Pin count for this buffer in the owning task. C: `LocalRefCount[bufid]`.
    pub refcount: i32,
}

impl LocalBufferDesc {
    fn new() -> Self {
        let mut tag = BufferTag {
            spc_oid: crate::postgres_ext::InvalidOid,
            db_oid: crate::postgres_ext::InvalidOid,
            rel_number: crate::common::relpath::InvalidRelFileNumber,
            fork_num: ForkNumber::InvalidForkNumber,
            block_num: crate::storage::block::INVALID_BLOCK_NUMBER,
        };
        tag.clear();
        Self { tag, buf_state: 0, refcount: 0 }
    }
}

/// The backend-private local buffer pool. C: the `LocalBuffer*` static arrays +
/// `LocalBufHash` + the `nextFreeLocalBufId` / `NLocalPinnedBuffers` cursors,
/// all rolled into one per-task value.
pub struct LocalBufferPool {
    /// Max number of local buffers (C: `num_temp_buffers` / `NLocBuffer`).
    num_temp_buffers: usize,
    /// Per-buffer descriptors, allocated up front (cheap: no page storage yet).
    descriptors: Vec<LocalBufferDesc>,
    /// Lazily-allocated page storage. `None` until a buffer is first used, then
    /// a boxed zeroed page (C: `LocalBufferBlockPointers` + lazy
    /// `GetLocalBufferStorage`). Indexed by the 0-based buffer index.
    blocks: Vec<Option<Box<Page>>>,
    /// Tag -> 0-based buffer index. C: `LocalBufHash`.
    hash: HashMap<BufferTag, usize>,
    /// Clock-sweep cursor. C: `nextFreeLocalBufId`.
    next_free: usize,
    /// Number of buffers pinned at least once. C: `NLocalPinnedBuffers`.
    pinned: usize,
}

impl LocalBufferPool {
    /// C: `InitLocalBuffers`. Allocate the descriptors + block slots and the
    /// lookup hash. Page storage is allocated lazily on first use.
    fn new(num_temp_buffers: usize) -> Self {
        assert!(num_temp_buffers > 0, "num_temp_buffers must be positive");
        Self {
            num_temp_buffers,
            descriptors: (0..num_temp_buffers).map(|_| LocalBufferDesc::new()).collect(),
            blocks: (0..num_temp_buffers).map(|_| None).collect(),
            hash: HashMap::new(),
            next_free: 0,
            pinned: 0,
        }
    }

    /// C: `NLocBuffer`.
    #[inline]
    pub fn num_buffers(&self) -> usize {
        self.num_temp_buffers
    }

    /// Descriptor for a 0-based index.
    #[inline]
    pub fn descriptor(&self, index: usize) -> &LocalBufferDesc {
        &self.descriptors[index]
    }

    /// Shared view of a local buffer's page bytes (the storage is allocated by
    /// the time a buffer is tag-valid).
    #[inline]
    pub fn block(&self, index: usize) -> &Page {
        self.blocks[index].as_deref().expect("local buffer storage not allocated")
    }

    /// Exclusive view of a local buffer's page bytes (single task, no IO CV).
    #[inline]
    pub fn block_mut(&mut self, index: usize) -> &mut Page {
        self.blocks[index].as_deref_mut().expect("local buffer storage not allocated")
    }

    /// C: `GetLocalBufferStorage` (lazy). Materialize the page storage for a slot.
    fn ensure_storage(&mut self, index: usize) {
        if self.blocks[index].is_none() {
            self.blocks[index] = Some(Page::boxed_zeroed());
        }
    }

    /// C: `PinLocalBuffer`. Bump the pin count (and, on the first pin, the shared
    /// refcount field + usagecount). Returns whether the buffer is `BM_VALID`.
    pub fn pin(&mut self, index: usize, adjust_usagecount: bool) -> bool {
        let d = &mut self.descriptors[index];
        if d.refcount == 0 {
            self.pinned += 1;
            d.buf_state += BUF_REFCOUNT_ONE;
            if adjust_usagecount && buf_state_get_usagecount(d.buf_state) < BM_MAX_USAGE_COUNT {
                d.buf_state += BUF_USAGECOUNT_ONE;
            }
        }
        d.refcount += 1;
        d.buf_state & BufFlags::VALID.bits() != 0
    }

    /// C: `UnpinLocalBuffer`. Drop one pin; on the last, decrement the refcount
    /// field.
    pub fn unpin(&mut self, index: usize) {
        let d = &mut self.descriptors[index];
        debug_assert!(d.refcount > 0, "unpin of an unpinned local buffer");
        d.refcount -= 1;
        if d.refcount == 0 {
            debug_assert!(self.pinned > 0);
            self.pinned -= 1;
            debug_assert!(buf_state_get_refcount(d.buf_state) > 0);
            d.buf_state -= BUF_REFCOUNT_ONE;
        }
    }

    /// C: `GetLocalVictimBuffer`. Clock-sweep for a reusable buffer index: skip
    /// pinned buffers, age usagecounts, pick the first with usagecount 0 and no
    /// pin. The chosen buffer is pinned. Returns `(index, needs_flush)` where
    /// `needs_flush` is true if the victim is `BM_DIRTY` and must be written out
    /// before reuse (the flush is async and is done by the caller after dropping
    /// the borrow). Storage is materialized for the chosen slot.
    fn get_victim(&mut self) -> usize {
        let n = self.num_temp_buffers;
        let mut trycounter = n;
        loop {
            let index = self.next_free;
            self.next_free += 1;
            if self.next_free >= n {
                self.next_free = 0;
            }

            let d = &mut self.descriptors[index];
            if d.refcount == 0 {
                if buf_state_get_usagecount(d.buf_state) > 0 {
                    d.buf_state -= BUF_USAGECOUNT_ONE;
                    trycounter = n;
                } else {
                    // Usable: pin it (no usagecount bump, matching C's
                    // PinLocalBuffer(bufHdr, false)).
                    self.pin(index, false);
                    self.ensure_storage(index);
                    return index;
                }
            } else {
                trycounter -= 1;
                // C: ereport(ERROR, "no empty local buffer available").
                assert!(trycounter != 0, "no empty local buffer available");
            }
        }
    }

    /// C: `InvalidateLocalBuffer`. Remove the tag from the hash and clear the
    /// descriptor's flags/usagecount. `check_unreferenced` errors if the buffer
    /// is still pinned (used when dropping, not when reusing identity).
    fn invalidate(&mut self, index: usize, check_unreferenced: bool) {
        let d = &mut self.descriptors[index];
        assert!(!(check_unreferenced && (d.refcount != 0 || buf_state_get_refcount(d.buf_state) != 0)), "local buffer {:?} is still referenced", local_buffer(index));
        let tag = d.tag;
        let removed = self.hash.remove(&tag);
        debug_assert!(removed.is_some(), "local buffer hash table corrupted");
        let d = &mut self.descriptors[index];
        d.tag.clear();
        d.buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
    }

    /// C: `MarkLocalBufferDirty`. Set `BM_DIRTY` on a pinned local buffer.
    pub fn mark_dirty(&mut self, buffer: Buffer) {
        let index = local_buf_index(buffer);
        debug_assert!(self.descriptors[index].refcount > 0);
        self.descriptors[index].buf_state |= BufFlags::DIRTY.bits();
    }

    /// C: `StartLocalBufferIO`. Returns true if this caller must perform the IO.
    /// For local buffers there is no concurrency, so this only checks whether the
    /// work was already done (`BM_VALID` for input, `!BM_DIRTY` for output).
    fn start_io(&self, index: usize, for_input: bool) -> bool {
        let s = self.descriptors[index].buf_state;
        if for_input {
            s & BufFlags::VALID.bits() == 0
        } else {
            s & BufFlags::DIRTY.bits() != 0
        }
    }

    /// C: `TerminateLocalBufferIO`. Clear `BM_IO_ERROR`, optionally clear
    /// `BM_DIRTY`, and OR in `set_flag_bits` (e.g. `BM_VALID`).
    fn terminate_io(&mut self, index: usize, clear_dirty: bool, set_flag_bits: u32) {
        let d = &mut self.descriptors[index];
        d.buf_state &= !BufFlags::IO_ERROR.bits();
        if clear_dirty {
            d.buf_state &= !BufFlags::DIRTY.bits();
        }
        d.buf_state |= set_flag_bits;
    }

    /// C: `DropRelationLocalBuffers`. Invalidate every tag-valid buffer of
    /// `(rlocator, forknum)` with block >= `first_del_block`. Dirty pages are
    /// dropped without flushing (NOT rollback-safe; matches C).
    pub fn drop_relation_buffers(
        &mut self,
        rlocator: &RelFileLocator,
        forknum: ForkNumber,
        first_del_block: BlockNumber,
    ) {
        for i in 0..self.num_temp_buffers {
            let d = &self.descriptors[i];
            if d.buf_state & BufFlags::TAG_VALID.bits() != 0
                && d.tag.matches_rel_file_locator(rlocator)
                && d.tag.fork_num() == forknum
                && d.tag.block_num >= first_del_block
            {
                self.invalidate(i, true);
            }
        }
    }

    /// C: `DropRelationAllLocalBuffers`. Invalidate all forks of a relation.
    pub fn drop_relation_all_buffers(&mut self, rlocator: &RelFileLocator) {
        for i in 0..self.num_temp_buffers {
            let d = &self.descriptors[i];
            if d.buf_state & BufFlags::TAG_VALID.bits() != 0
                && d.tag.matches_rel_file_locator(rlocator)
            {
                self.invalidate(i, true);
            }
        }
    }
}

tokio::task_local! {
    /// The per-task local buffer pool. Established by [`with_local_buffers`];
    /// lazily initialized on first access via [`with_pool`].
    static LOCAL_BUFFERS: RefCell<Option<LocalBufferPool>>;
}

/// Run `f` with the per-task local buffer pool established, scoping it if not
/// already present. A backend owns its task scope; tests/entry points that touch
/// local buffers wrap their body in this.
pub async fn with_local_buffers<F, Fut, T>(f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    if LOCAL_BUFFERS.try_with(|_| ()).is_ok() {
        f().await
    } else {
        LOCAL_BUFFERS.scope(RefCell::new(None), f()).await
    }
}

/// Access the per-task local buffer pool synchronously (initializing it on first
/// use). MUST NOT be called across an `.await` boundary (the borrow is internal
/// to `f`). Panics if no [`with_local_buffers`] scope is active.
fn with_pool<R>(f: impl FnOnce(&mut LocalBufferPool) -> R) -> R {
    LOCAL_BUFFERS.with(|cell| {
        let mut opt = cell.borrow_mut();
        let pool = opt.get_or_insert_with(|| LocalBufferPool::new(DEFAULT_NUM_TEMP_BUFFERS));
        f(pool)
    })
}

/// Test/inspection helper: this task's pin count for a local buffer.
pub fn local_refcount(buffer: Buffer) -> i32 {
    with_pool(|p| p.descriptor(local_buf_index(buffer)).refcount)
}

/// C: `MarkLocalBufferDirty`. Free-function entry point (the pool is per-task).
pub fn mark_local_buffer_dirty(buffer: Buffer) {
    with_pool(|p| p.mark_dirty(buffer));
}

/// C: `UnpinLocalBuffer`.
pub fn unpin_local_buffer(buffer: Buffer) {
    with_pool(|p| p.unpin(local_buf_index(buffer)));
}

/// C: `BufferGetBlockNumber` for a local buffer.
pub fn local_buffer_get_block_number(buffer: Buffer) -> BlockNumber {
    with_pool(|p| p.descriptor(local_buf_index(buffer)).tag.block_num)
}

/// C: `LocalBufferAlloc`. Find or create a local buffer for `(forknum, blocknum)`
/// of `smgr`. Returns `(buffer, found)`: `found == true` means the page is
/// already resident (caller need not read it); `found == false` means the caller
/// owns a fresh, pinned, tag-valid, contents-invalid buffer to read into.
///
/// The clock-sweep victim may be dirty; if so it is flushed (async smgr write)
/// before reuse. The local-pool borrow is released before that flush `.await`
/// (see [`flush_local_buffer`]).
pub async fn local_buffer_alloc(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
) -> (Buffer, bool) {
    let new_tag = BufferTag::init(&smgr.rlocator.locator, forknum, blocknum);

    // Fast path: already resident? (sync section, borrow dropped before return.)
    if let Some(index) = with_pool(|p| p.hash.get(&new_tag).copied()) {
        let found = with_pool(|p| p.pin(index, true));
        return (local_buffer(index), found);
    }

    // Miss: get a victim (clock-sweep, pins it, materializes storage). If the
    // victim is dirty, flush it BEFORE claiming the new tag, releasing the borrow
    // across the smgr write.
    let index = with_pool(LocalBufferPool::get_victim);

    let dirty = with_pool(|p| p.descriptor(index).buf_state & BufFlags::DIRTY.bits() != 0);
    if dirty {
        flush_local_buffer(shared, smgr, index).await;
    }

    // Detach the victim's old tag (if any) and set the new identity.
    with_pool(|p| {
        let was_tag_valid = p.descriptor(index).buf_state & BufFlags::TAG_VALID.bits() != 0;
        if was_tag_valid {
            p.invalidate(index, false);
        }
        let prev = p.hash.insert(new_tag, index);
        debug_assert!(prev.is_none(), "local buffer hash table corrupted");
        let d = &mut p.descriptors[index];
        d.tag = new_tag;
        d.buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        d.buf_state |= BufFlags::TAG_VALID.bits() | BUF_USAGECOUNT_ONE;
    });

    (local_buffer(index), false)
}

/// C: `FlushLocalBuffer`. Write a dirty local buffer to its temp segment via
/// smgr, then mark it clean. No-op if the buffer is already clean.
///
/// The local-pool borrow is NOT held across the smgr `.await`: this copies the
/// page bytes out under the borrow, drops the borrow, awaits the write, then
/// re-borrows to clear `BM_DIRTY`. Single-task, so nothing can dirty it in
/// between.
pub async fn flush_local_buffer(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    index: usize,
) {
    // Claim the (trivial) IO and snapshot the page + tag under the borrow.
    let snapshot = with_pool(|p| {
        debug_assert!(p.descriptor(index).refcount > 0);
        if !p.start_io(index, false) {
            return None;
        }
        let tag = p.descriptor(index).tag;
        let mut page = Page::boxed_zeroed();
        page.as_mut_bytes().copy_from_slice(p.block(index).as_bytes());
        Some((tag, page))
    });

    let Some((tag, page)) = snapshot else {
        return;
    };

    // No borrow held: write to disk.
    smgr.write(shared, tag.fork_num(), tag.block_num, &page, false).await;

    // Mark clean.
    with_pool(|p| p.terminate_io(index, true, 0));
}

/// C: `ExtendBufferedRelLocal` (single-block form). Extend the temp relation by
/// one zero-filled block, materialize it on disk, and return a pinned, valid
/// local buffer for the new block. (The PG18 batched `extend_by` form is
/// collapsed to one block here, matching the bufmgr AIO collapse.)
pub async fn extend_buffered_rel_local(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    forknum: ForkNumber,
) -> Buffer {
    // Pick + zero a victim buffer for the new block.
    let index = with_pool(|p| {
        let i = p.get_victim();
        p.block_mut(i).as_mut_bytes().fill(0);
        i
    });

    let first_block = smgr.nblocks(shared, forknum).await;

    assert!(u64::from(first_block) + 1 < u64::from(MAX_BLOCK_NUMBER), "cannot extend relation beyond {MAX_BLOCK_NUMBER} blocks");

    let new_tag = BufferTag::init(&smgr.rlocator.locator, forknum, first_block);

    // Claim identity for the victim, or reuse an existing buffer for that block.
    let (buffer, victim_to_unpin) = with_pool(|p| {
        match p.hash.get(&new_tag).copied() {
            Some(existing) if existing != index => {
                // A buffer already holds this block: pin it, drop the victim.
                p.pin(existing, false);
                let d = &mut p.descriptors[existing];
                d.buf_state &= !BufFlags::VALID.bits();
                (local_buffer(existing), Some(index))
            }
            _ => {
                let was_tag_valid =
                    p.descriptor(index).buf_state & BufFlags::TAG_VALID.bits() != 0;
                if was_tag_valid {
                    p.invalidate(index, false);
                }
                p.hash.insert(new_tag, index);
                let d = &mut p.descriptors[index];
                d.tag = new_tag;
                d.buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
                d.buf_state |= BufFlags::TAG_VALID.bits() | BUF_USAGECOUNT_ONE;
                (local_buffer(index), None)
            }
        }
    });

    if let Some(v) = victim_to_unpin {
        with_pool(|p| p.unpin(v));
    }

    // Materialize the new block on disk (no borrow held).
    let zero = Page::boxed_zeroed();
    smgr.extend(shared, forknum, first_block, &zero, false).await;

    // Publish BM_VALID.
    with_pool(|p| {
        let idx = local_buf_index(buffer);
        p.descriptors[idx].buf_state |= BufFlags::VALID.bits();
    });

    buffer
}

/// C: `DropRelationLocalBuffers`.
pub fn drop_relation_local_buffers(
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    first_del_block: BlockNumber,
) {
    with_pool(|p| p.drop_relation_buffers(rlocator, forknum, first_del_block));
}

/// C: `DropRelationAllLocalBuffers`.
pub fn drop_relation_all_local_buffers(rlocator: &RelFileLocator) {
    with_pool(|p| p.drop_relation_all_buffers(rlocator));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::procnumber::ProcNumber;

    fn temp_rloc(rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: Oid(1663), dbOid: Oid(60000 + rel), relNumber: Oid(17000 + rel) }
    }

    async fn shared_with_tmpdir(tag: &str) -> (Arc<SharedState>, std::path::PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "pepperdb_localbuf_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        crate::storage::io_backend::mkdir_all(dir.join("base").join((60000 + 1).to_string()))
            .await
            .ok();
        let s = SharedState::new(SharedStateConfig::default());
        s.config().set_data_dir(dir.to_string_lossy().into_owned());
        (s, dir)
    }

    /// A temp smgr handle (backend != INVALID_PROC_NUMBER marks it local).
    fn temp_smgr(rel: u32) -> SmgrRelation {
        SmgrRelation::open(temp_rloc(rel), 7 as ProcNumber)
    }

    #[test]
    fn buffer_encoding_round_trips() {
        assert_eq!(local_buffer(0), BufId::Local(0));
        assert_eq!(local_buffer(1), BufId::Local(1));
        assert_eq!(local_buf_index(BufId::Local(0)), 0);
        assert_eq!(local_buf_index(BufId::Local(1)), 1);
        assert!(local_buffer(5).is_local());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alloc_write_flush_round_trips_to_disk() {
        let (s, dir) = shared_with_tmpdir("flush").await;
        with_local_buffers(|| async {
            let mut smgr = temp_smgr(1);
            let fork = ForkNumber::MAIN_FORKNUM;
            smgr.create(&s, fork, false).await;

            // Extend by one block -> a valid, pinned local buffer.
            let b = extend_buffered_rel_local(&s, &mut smgr, fork).await;
            assert!(b.is_local());
            let idx = local_buf_index(b);

            // Write a pattern, mark dirty, flush.
            with_pool(|p| {
                p.block_mut(idx).as_mut_bytes().fill(0xAB);
                p.block_mut(idx).set_lsn(crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
            });
            mark_local_buffer_dirty(b);
            assert!(with_pool(|p| p.descriptor(idx).buf_state & BufFlags::DIRTY.bits() != 0));

            flush_local_buffer(&s, &mut smgr, idx).await;
            assert!(with_pool(|p| p.descriptor(idx).buf_state & BufFlags::DIRTY.bits() == 0));

            // The bytes are on disk.
            let mut check = Page::boxed_zeroed();
            smgr.read(&s, fork, 0, &mut check).await;
            assert!(check.as_bytes()[8..].iter().all(|&x| x == 0xAB), "flushed to temp segment");

            unpin_local_buffer(b);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alloc_then_realloc_is_a_hit() {
        let (s, dir) = shared_with_tmpdir("hit").await;
        with_local_buffers(|| async {
            let mut smgr = temp_smgr(1);
            let fork = ForkNumber::MAIN_FORKNUM;
            smgr.create(&s, fork, false).await;
            extend_buffered_rel_local(&s, &mut smgr, fork).await; // block 0 exists

            // First alloc of block 0: a miss (extend made it valid, so actually a
            // hit via the existing buffer). Allocate block 0 fresh after dropping:
            let (b1, _found1) = local_buffer_alloc(&s, &mut smgr, fork, 0).await;
            // Same block again -> same buffer, hit.
            let (b2, found2) = local_buffer_alloc(&s, &mut smgr, fork, 0).await;
            assert_eq!(b1, b2, "same block returns the same local buffer");
            assert!(found2, "second alloc of a resident block is a hit");
            assert_eq!(local_refcount(b1), 3, "two allocs + the extend pin");

            unpin_local_buffer(b1);
            unpin_local_buffer(b2);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clock_sweep_evicts_when_pool_full() {
        // A tiny pool: force eviction and verify the dirty victim is flushed.
        let (s, dir) = shared_with_tmpdir("evict").await;
        with_local_buffers(|| async {
            // Shrink the pool to 2 buffers for this task.
            LOCAL_BUFFERS.with(|cell| {
                *cell.borrow_mut() = Some(LocalBufferPool::new(2));
            });
            let mut smgr = temp_smgr(1);
            let fork = ForkNumber::MAIN_FORKNUM;
            smgr.create(&s, fork, false).await;

            // Make 3 blocks on disk.
            for _ in 0..3 {
                let b = extend_buffered_rel_local(&s, &mut smgr, fork).await;
                unpin_local_buffer(b);
            }

            // Read block 0, dirty it, unpin so it is evictable.
            let (b0, _) = local_buffer_alloc(&s, &mut smgr, fork, 0).await;
            let i0 = local_buf_index(b0);
            with_pool(|p| {
                p.block_mut(i0).as_mut_bytes().fill(0x5A);
                p.block_mut(i0).set_lsn(crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
            });
            mark_local_buffer_dirty(b0);
            let tag0 = with_pool(|p| p.descriptor(i0).tag);
            unpin_local_buffer(b0);

            // Allocate two more distinct blocks to force the 2-buffer pool to
            // evict block 0 (clock sweep). After this, block 0's tag is gone.
            let (a, _) = local_buffer_alloc(&s, &mut smgr, fork, 1).await;
            let (c, _) = local_buffer_alloc(&s, &mut smgr, fork, 2).await;
            assert!(with_pool(|p| !p.hash.contains_key(&tag0)), "evicted tag removed");

            // The dirty block-0 bytes were flushed to disk on eviction.
            let mut check = Page::boxed_zeroed();
            smgr.read(&s, fork, 0, &mut check).await;
            assert!(check.as_bytes()[8..].iter().all(|&x| x == 0x5A), "dirty victim flushed");

            unpin_local_buffer(a);
            unpin_local_buffer(c);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_relation_local_buffers_invalidates() {
        let (s, dir) = shared_with_tmpdir("drop").await;
        with_local_buffers(|| async {
            let mut smgr = temp_smgr(1);
            let fork = ForkNumber::MAIN_FORKNUM;
            smgr.create(&s, fork, false).await;
            for _ in 0..2 {
                let b = extend_buffered_rel_local(&s, &mut smgr, fork).await;
                unpin_local_buffer(b);
            }
            let loc = smgr.rlocator.locator;
            // Both blocks resident and tag-valid.
            assert_eq!(with_pool(|p| p.hash.len()), 2);

            drop_relation_local_buffers(&loc, fork, 0);
            assert_eq!(with_pool(|p| p.hash.len()), 0, "all blocks invalidated");
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn smgr_truncate_drops_local_buffers() {
        use crate::backend::storage::ipc::sinvaladt::{SInvalBuffer, with_sinval_buffer};
        use crate::storage::proc::{my_proc_scope, set_current_proc_number};
        let (s, dir) = shared_with_tmpdir("smgrtrunc").await;
        let buf = Arc::new(SInvalBuffer::new_for_test());
        // smgr.truncate's DropRelationBuffers only takes the local branch when the
        // temp rel's backend == MyProcNumber; temp_smgr uses backend 7.
        my_proc_scope(with_sinval_buffer(buf, with_local_buffers(|| async {
            set_current_proc_number(7 as ProcNumber);
            let mut smgr = temp_smgr(1);
            let fork = ForkNumber::MAIN_FORKNUM;
            smgr.create(&s, fork, false).await;
            for _ in 0..3 {
                let b = extend_buffered_rel_local(&s, &mut smgr, fork).await;
                unpin_local_buffer(b);
            }
            assert_eq!(with_pool(|p| p.hash.len()), 3);

            // smgrtruncate to 1 block drops the buffers for the removed blocks
            // (first_del_block = the new size, 1) via DropRelationBuffers.
            smgr.truncate(&s, &[(fork, 3, 1)]).await;
            assert_eq!(with_pool(|p| p.hash.len()), 1, "blocks >= 1 dropped on truncate");
            assert_eq!(smgr.nblocks(&s, fork).await, 1);
        })))
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pin_survives_thread_migration() {
        // The per-task pool follows the task across an await that migrates it.
        let (s, dir) = shared_with_tmpdir("migrate").await;
        with_local_buffers(|| async {
            let mut smgr = temp_smgr(1);
            let fork = ForkNumber::MAIN_FORKNUM;
            smgr.create(&s, fork, false).await;
            let b = extend_buffered_rel_local(&s, &mut smgr, fork).await;
            assert_eq!(local_refcount(b), 1);

            let start = std::thread::current().id();
            for _ in 0..10_000 {
                if std::thread::current().id() != start {
                    break;
                }
                let _ = tokio::task::spawn_blocking(|| {}).await;
                tokio::task::yield_now().await;
            }
            // The pin is still tracked after the (likely) migration.
            assert_eq!(local_refcount(b), 1, "pin tracked across thread hop");
            unpin_local_buffer(b);
            assert_eq!(local_refcount(b), 0);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }
}
