//! Translated from PostgreSQL src/backend/storage/freespace/freespace.c -- the
//! Free Space Map: track how much free space each heap (or index) page has and
//! quickly find a page with enough.
//!
//! The FSM is a separate relation fork (`FSM_FORKNUM`) holding a 3-level (for the
//! default 8 KB BLCKSZ) tree of FSM pages. Each leaf FSM page covers
//! `SLOTS_PER_FSM_PAGE` heap blocks; an upper FSM page's slots index lower FSM
//! pages. `fsm_logical_to_physical` maps a logical (level, page) address to the
//! physical block number in the fork (the tree is stored depth-first). One byte
//! per slot encodes the free-space *category* (0..255), where the byte count is
//! `category * FSM_CAT_STEP` (and 255 means >= MaxFSMRequestSize).
//!
//! Buffer-manager integration (rules.md 5): every FSM-fork page is read through
//! `read_buffer_common` (the async part), then the synchronous `fsmpage` tree op
//! runs between taking and dropping the content lock, then `mark_buffer_dirty`.
//! No content lock or header lock is ever held across an `.await`.
//!
//! Catalog note: C threads a `Relation` through these. The relcache (`Relation`)
//! is deferred, so the backend functions take the smgr-level inputs the buffer
//! manager already uses -- `&Arc<SharedState>`, `&mut SmgrRelation`, and the
//! relation persistence -- and the header `src/storage/freespace.rs` keeps the
//! `Relation`-based C signatures as TODO shims.

use std::sync::Arc;

use crate::backend::storage::buffer::bufmgr::{
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, read_buffer_common,
};
use crate::backend::storage::freespace::fsmpage::{FsmPage, FsmPageMut};
use crate::catalog::pg_class::RELPERSISTENCE_PERMANENT;
use crate::common::relpath::ForkNumber;
use crate::pg_config::BLCKSZ;
use crate::shared_state::SharedState;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::ReadBufferMode;
use crate::storage::fsm_internals::SLOTS_PER_FSM_PAGE;
use crate::storage::smgr::SmgrRelation;

/// We divide a page's possible free space into 256 categories. C: `FSM_CATEGORIES`.
const FSM_CATEGORIES: usize = 256;
/// Bytes per category step. C: `FSM_CAT_STEP`.
const FSM_CAT_STEP: usize = (BLCKSZ as usize) / FSM_CATEGORIES;
/// Largest request the FSM tracks. C: `MaxFSMRequestSize = MaxHeapTupleSize`.
const MAX_FSM_REQUEST_SIZE: usize = crate::access::htup_details::MaxHeapTupleSize;

/// On-disk tree depth: 3 for BLCKSZ >= 4 KB (SLOTS_PER_FSM_PAGE >= 1626), else 4.
/// C: `FSM_TREE_DEPTH`.
const FSM_TREE_DEPTH: i32 = if SLOTS_PER_FSM_PAGE >= 1626 { 3 } else { 4 };
const FSM_ROOT_LEVEL: i32 = FSM_TREE_DEPTH - 1;
const FSM_BOTTOM_LEVEL: i32 = 0;

/// A logical FSM address: a (level, page-within-level) pair. C: `FSMAddress`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FsmAddress {
    level: i32,
    logpageno: u32,
}

/// Address of the root page. C: `FSM_ROOT_ADDRESS`.
const FSM_ROOT_ADDRESS: FsmAddress = FsmAddress { level: FSM_ROOT_LEVEL, logpageno: 0 };

// ---- category <-> bytes conversion (C: fsm_space_*_to_cat / cat_to_avail) ----

/// C: `fsm_space_avail_to_cat`. Rounds down; the top category is reserved for
/// `MaxFSMRequestSize` or more.
fn fsm_space_avail_to_cat(avail: usize) -> u8 {
    debug_assert!(avail < BLCKSZ as usize);
    if avail >= MAX_FSM_REQUEST_SIZE {
        return 255;
    }
    let cat = avail / FSM_CAT_STEP;
    if cat > 254 { 254 } else { cat as u8 }
}

/// C: `fsm_space_cat_to_avail`. The lower bound of a category's byte range.
fn fsm_space_cat_to_avail(cat: u8) -> usize {
    if cat == 255 {
        MAX_FSM_REQUEST_SIZE
    } else {
        cat as usize * FSM_CAT_STEP
    }
}

/// C: `fsm_space_needed_to_cat`. Rounds up (a request needs at least this cat).
fn fsm_space_needed_to_cat(needed: usize) -> u8 {
    if needed > MAX_FSM_REQUEST_SIZE {
        panic!("invalid FSM request size {needed}");
    }
    if needed == 0 {
        return 1;
    }
    let cat = needed.div_ceil(FSM_CAT_STEP);
    if cat > 255 { 255 } else { cat as u8 }
}

// ---- tree navigation (C: fsm_get_child / fsm_get_parent / ... ) -------------

/// C: `fsm_logical_to_physical`. Physical block number of a logical FSM address.
fn fsm_logical_to_physical(addr: FsmAddress) -> BlockNumber {
    // Logical page number of the first leaf below this page.
    let mut leafno: u64 = addr.logpageno as u64;
    for _ in 0..addr.level {
        leafno *= SLOTS_PER_FSM_PAGE as u64;
    }
    // Count upper-level nodes needed to address that leaf.
    let mut pages: u64 = 0;
    for _ in 0..FSM_TREE_DEPTH {
        pages += leafno + 1;
        leafno /= SLOTS_PER_FSM_PAGE as u64;
    }
    // Subtract the lower-level pages counted for a non-bottom request.
    pages -= addr.level as u64;
    (pages - 1) as BlockNumber
}

/// C: `fsm_get_location`. FSM location of a heap block, plus the slot in it.
fn fsm_get_location(heapblk: BlockNumber) -> (FsmAddress, u16) {
    let addr = FsmAddress {
        level: FSM_BOTTOM_LEVEL,
        logpageno: heapblk / SLOTS_PER_FSM_PAGE as u32,
    };
    let slot = (heapblk % SLOTS_PER_FSM_PAGE as u32) as u16;
    (addr, slot)
}

/// C: `fsm_get_heap_blk`. Heap block for a bottom-level FSM location + slot.
fn fsm_get_heap_blk(addr: FsmAddress, slot: u16) -> BlockNumber {
    debug_assert_eq!(addr.level, FSM_BOTTOM_LEVEL);
    addr.logpageno * SLOTS_PER_FSM_PAGE as u32 + slot as u32
}

/// C: `fsm_get_parent`. Parent address of a child page, plus the slot in it.
fn fsm_get_parent(child: FsmAddress) -> (FsmAddress, u16) {
    debug_assert!(child.level < FSM_ROOT_LEVEL);
    let parent = FsmAddress {
        level: child.level + 1,
        logpageno: child.logpageno / SLOTS_PER_FSM_PAGE as u32,
    };
    let slot = (child.logpageno % SLOTS_PER_FSM_PAGE as u32) as u16;
    (parent, slot)
}

/// C: `fsm_get_child`. Child address for a parent page + slot.
fn fsm_get_child(parent: FsmAddress, slot: u16) -> FsmAddress {
    debug_assert!(parent.level > FSM_BOTTOM_LEVEL);
    FsmAddress {
        level: parent.level - 1,
        logpageno: parent.logpageno * SLOTS_PER_FSM_PAGE as u32 + slot as u32,
    }
}

// ---- FSM-fork buffer access (C: fsm_readbuf / fsm_extend) --------------------

/// C: `fsm_readbuf` + `fsm_extend`. Read the FSM-fork page for `addr`. If the
/// page is past EOF and `extend` is false, return `None`; if `extend` is true,
/// the fork is extended (zero-filled) up to that block first. A freshly
/// extended / zero page is `PageInit`'d under the content lock.
///
/// Returns a pinned `Buffer` (caller releases). No lock held across the await.
async fn fsm_readbuf(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    addr: FsmAddress,
    extend: bool,
) -> Option<Buffer> {
    let blkno = fsm_logical_to_physical(addr);
    // C: smgrnblocks is only safe once the fork exists; a missing fork means 0.
    let exists = smgr.exists(shared, ForkNumber::FSM_FORKNUM).await;
    let nblocks = if exists {
        smgr.nblocks(shared, ForkNumber::FSM_FORKNUM).await
    } else {
        0
    };

    if blkno >= nblocks {
        if !extend {
            return None;
        }
        // C: EB_CREATE_FORK_IF_NEEDED. The buffer-manager P_NEW extend below
        // relies on the fork file existing, so create it first.
        if !exists {
            smgr.create(shared, ForkNumber::FSM_FORKNUM, false).await;
        }
        // Extend the FSM fork with zero pages up to blkno (inclusive). Each new
        // block is materialized via the buffer manager's P_NEW path.
        let mut cur = nblocks;
        while cur <= blkno {
            let b = read_buffer_common(
                shared,
                smgr,
                relpersistence,
                ForkNumber::FSM_FORKNUM,
                crate::storage::bufmgr::P_NEW,
                ReadBufferMode::NORMAL,
                None,
            )
            .await;
            // The extended block is zero; PageInit it under the content lock.
            init_if_new(shared, b).await;
            shared.buffers().release_buffer(b);
            cur += 1;
        }
    }

    let buf = read_buffer_common(
        shared,
        smgr,
        relpersistence,
        ForkNumber::FSM_FORKNUM,
        blkno,
        ReadBufferMode::ZERO_ON_ERROR,
        None,
    )
    .await;

    init_if_new(shared, buf).await;
    Some(buf)
}

/// PageInit a freshly-read FSM page if it is still new (zeroed). The check is
/// done under the exclusive content lock to avoid two tasks both initializing.
async fn init_if_new(shared: &Arc<SharedState>, buf: Buffer) {
    let pool = shared.buffers();
    if pool.buffer_get_page(buf).is_new() {
        let _g = pool.content_exclusive(buf);
        let buf_id = buf.as_global().unwrap() as i32;
        // SAFETY: exclusive content lock held -> sole writer to this page slot.
        let page = unsafe { pool.block_mut(buf_id) };
        if page.is_new() {
            page.init(BLCKSZ as usize, 0);
        }
    }
}

/// C: `fsm_set_and_search`. Set `addr`/`slot` to `new_value`, and (if
/// `min_value > 0`) search the same page for a slot with >= `min_value`,
/// returning it. Reads/extends the FSM page, takes the exclusive content lock for
/// the set+search, marks dirty, releases.
async fn fsm_set_and_search(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    addr: FsmAddress,
    slot: u16,
    new_value: u8,
    min_value: u8,
) -> Option<u16> {
    let buf = fsm_readbuf(shared, smgr, relpersistence, addr, true)
        .await
        .expect("fsm_readbuf with extend must return a buffer");
    let pool = shared.buffers();

    let mut newslot = None;
    {
        let _g = pool.content_exclusive(buf);
        let buf_id = buf.as_global().unwrap() as i32;
        // SAFETY: exclusive content lock held.
        let page = unsafe { pool.block_mut(buf_id) };
        let mut fp = FsmPageMut::new(page);
        let changed = fp.set_avail(slot as usize, new_value);
        if min_value != 0 {
            newslot = fp
                .search_avail(min_value, addr.level == FSM_BOTTOM_LEVEL)
                .map(|s| s as u16);
        }
        if changed {
            pool.mark_buffer_dirty(buf);
        }
    }
    pool.release_buffer(buf);
    newslot
}

/// C: `fsm_search`. Descend the tree from the root for a heap block with at
/// least `min_cat` free space. Returns the heap block, or `None`.
async fn fsm_search(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    min_cat: u8,
) -> Option<BlockNumber> {
    let pool = shared.buffers();
    let mut restarts = 0;
    let mut addr = FSM_ROOT_ADDRESS;

    loop {
        let mut max_avail = 0u8;
        // Read the FSM page (no extend on search).
        let buf = fsm_readbuf(shared, smgr, relpersistence, addr, false).await;

        let slot: Option<u16> = match buf {
            Some(buf) => {
                let _g = pool.content_share(buf);
                // Read-only search under the SHARED content lock: descend the
                // max-tree via `&Page` only (no `block_mut`, no `&mut Page`).
                // Two concurrent searchers both holding the share lock are sound
                // because nothing here writes the page.
                // TODO(perf): PG updates fp_next_slot under the shared lock as a
                // benign byte race to spread inserts; reintroduce later via an
                // atomic byte through the UnsafeCell (never as &mut Page under a
                // shared lock).
                let view = FsmPage::new(pool.buffer_get_page(buf));
                let s = view.search_avail(min_cat);
                if s.is_none() {
                    max_avail = view.get_max_avail();
                }
                drop(_g);
                pool.release_buffer(buf);
                s.map(|x| x as u16)
            }
            None => None,
        };

        if let Some(slot) = slot {
            if addr.level == FSM_BOTTOM_LEVEL {
                let blkno = fsm_get_heap_blk(addr, slot);
                // The block-exists recheck (C: fsm_does_block_exist) needs the
                // heap fork size; trust the FSM here (TODO: recheck once vacuum /
                // recovery can leave dangling FSM entries).
                return Some(blkno);
            }
            addr = fsm_get_child(addr, slot);
        } else if addr.level == FSM_ROOT_LEVEL {
            // No page with enough space anywhere.
            return None;
        } else {
            // Lower-level miss: the parent's node was stale. Update it and retry
            // from the root (C's self-healing path).
            let (parent, parentslot) = fsm_get_parent(addr);
            fsm_set_and_search(shared, smgr, relpersistence, parent, parentslot, max_avail, 0)
                .await;
            restarts += 1;
            if restarts > 10000 {
                return None;
            }
            addr = FSM_ROOT_ADDRESS;
        }
    }
}

// ---- Public API (C: the exported FSM routines) ------------------------------

/// C: `GetPageWithFreeSpace`. Find a page in the relation with at least
/// `space_needed` bytes free; `None` if none (caller should extend).
pub async fn get_page_with_free_space(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    space_needed: usize,
) -> Option<BlockNumber> {
    let min_cat = fsm_space_needed_to_cat(space_needed);
    fsm_search(shared, smgr, relpersistence, min_cat).await
}

/// C: `RecordPageWithFreeSpace`. Record that `heap_blk` has `space_avail` bytes
/// free. (Until the next FSM vacuum, a freed-up value may not be visible to
/// searchers above the bottom level.)
pub async fn record_page_with_free_space(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    heap_blk: BlockNumber,
    space_avail: usize,
) {
    let new_cat = fsm_space_avail_to_cat(space_avail);
    let (addr, slot) = fsm_get_location(heap_blk);
    fsm_set_and_search(shared, smgr, relpersistence, addr, slot, new_cat, 0).await;
}

/// C: `RecordAndGetPageWithFreeSpace`. Record `old_page`'s free space and look
/// for a nearby page with at least `space_needed`; fall back to a full search.
pub async fn record_and_get_page_with_free_space(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    old_page: BlockNumber,
    old_space_avail: usize,
    space_needed: usize,
) -> Option<BlockNumber> {
    let old_cat = fsm_space_avail_to_cat(old_space_avail);
    let search_cat = fsm_space_needed_to_cat(space_needed);
    let (addr, slot) = fsm_get_location(old_page);
    if let Some(search_slot) =
        fsm_set_and_search(shared, smgr, relpersistence, addr, slot, old_cat, search_cat).await
    {
        return Some(fsm_get_heap_blk(addr, search_slot));
    }
    fsm_search(shared, smgr, relpersistence, search_cat).await
}

/// C: `GetRecordedFreeSpace`. The free space the FSM records for `heap_blk`.
pub async fn get_recorded_free_space(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    heap_blk: BlockNumber,
) -> usize {
    let (addr, slot) = fsm_get_location(heap_blk);
    let buf = match fsm_readbuf(shared, smgr, relpersistence, addr, false).await {
        Some(b) => b,
        None => return 0,
    };
    let pool = shared.buffers();
    let cat = FsmPage::new(pool.buffer_get_page(buf)).get_avail(slot as usize);
    pool.release_buffer(buf);
    fsm_space_cat_to_avail(cat)
}

/// C: `XLogRecordPageWithFreeSpace`. WAL-replay form: extend the FSM fork as
/// needed, set the slot, and `mark_buffer_dirty` (regular, not hint -- replay).
pub async fn xlog_record_page_with_free_space(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    heap_blk: BlockNumber,
    space_avail: usize,
) {
    let new_cat = fsm_space_avail_to_cat(space_avail);
    let (addr, slot) = fsm_get_location(heap_blk);
    let buf = fsm_readbuf(shared, smgr, relpersistence, addr, true)
        .await
        .expect("fsm_readbuf with extend must return a buffer");
    let pool = shared.buffers();
    {
        let _g = pool.content_exclusive(buf);
        let buf_id = buf.as_global().unwrap() as i32;
        // SAFETY: exclusive content lock held.
        let page = unsafe { pool.block_mut(buf_id) };
        if FsmPageMut::new(page).set_avail(slot as usize, new_cat) {
            pool.mark_buffer_dirty(buf);
        }
    }
    pool.release_buffer(buf);
}

/// C: `FreeSpaceMapPrepareTruncateRel`. Compute the new FSM-fork size for a heap
/// truncated to `nblocks`, zeroing the tail slots of the last surviving FSM page.
/// Returns the new FSM block count, or `None` if there is nothing to truncate.
/// The caller is responsible for the actual `smgrtruncate` + a vacuum-range.
pub async fn free_space_map_prepare_truncate_rel(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    nblocks: BlockNumber,
) -> Option<BlockNumber> {
    if !smgr.exists(shared, ForkNumber::FSM_FORKNUM).await {
        return None;
    }
    let (first_removed_address, first_removed_slot) = fsm_get_location(nblocks);

    if first_removed_slot > 0 {
        let buf = fsm_readbuf(shared, smgr, relpersistence, first_removed_address, false).await?;
        let pool = shared.buffers();
        {
            let _g = pool.content_exclusive(buf);
            let buf_id = buf.as_global().unwrap() as i32;
            // SAFETY: exclusive content lock held.
            let page = unsafe { pool.block_mut(buf_id) };
            FsmPageMut::new(page).truncate_avail(first_removed_slot as usize);
            pool.mark_buffer_dirty(buf);
        }
        pool.release_buffer(buf);
        Some(fsm_logical_to_physical(first_removed_address) + 1)
    } else {
        let new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
        if smgr.nblocks(shared, ForkNumber::FSM_FORKNUM).await <= new_nfsmblocks {
            None
        } else {
            Some(new_nfsmblocks)
        }
    }
}

/// C: `FreeSpaceMapVacuum`. Recompute upper-level pages from the leaves over the
/// whole relation.
pub async fn free_space_map_vacuum(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
) {
    fsm_vacuum_page(shared, smgr, relpersistence, FSM_ROOT_ADDRESS, 0, INVALID_BLOCK_NUMBER).await;
}

/// C: `FreeSpaceMapVacuumRange`. As above, but only for the slots covering heap
/// blocks in `[start, end)`.
pub async fn free_space_map_vacuum_range(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    start: BlockNumber,
    end: BlockNumber,
) {
    if end > start {
        fsm_vacuum_page(shared, smgr, relpersistence, FSM_ROOT_ADDRESS, start, end).await;
    }
}

/// C: `fsm_vacuum_page` (recursive guts of FreeSpaceMapVacuum). Returns the max
/// category on `addr`'s page (and `false`/EOF folds into a 0 return). Boxed
/// future because it recurses.
fn fsm_vacuum_page<'a>(
    shared: &'a Arc<SharedState>,
    smgr: &'a mut SmgrRelation,
    relpersistence: i8,
    addr: FsmAddress,
    start: BlockNumber,
    end: BlockNumber,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = (u8, bool)> + Send + 'a>> {
    Box::pin(async move {
        let buf = match fsm_readbuf(shared, smgr, relpersistence, addr, false).await {
            Some(b) => b,
            None => return (0, true), // EOF
        };
        let pool = shared.buffers();

        if addr.level > FSM_BOTTOM_LEVEL {
            // Slot range on this page covering [start, end-1].
            let (mut fsm_start, mut fsm_start_slot) = fsm_get_location(start);
            let end_blk = if end == INVALID_BLOCK_NUMBER { end } else { end - 1 };
            let (mut fsm_end, mut fsm_end_slot) = fsm_get_location(end_blk);
            while fsm_start.level < addr.level {
                let (ps, pss) = fsm_get_parent(fsm_start);
                fsm_start = ps;
                fsm_start_slot = pss;
                let (pe, pes) = fsm_get_parent(fsm_end);
                fsm_end = pe;
                fsm_end_slot = pes;
            }
            let start_slot: i32 = if fsm_start.logpageno == addr.logpageno {
                fsm_start_slot as i32
            } else if fsm_start.logpageno > addr.logpageno {
                SLOTS_PER_FSM_PAGE as i32
            } else {
                0
            };
            let end_slot: i32 = if fsm_end.logpageno == addr.logpageno {
                fsm_end_slot as i32
            } else if fsm_end.logpageno > addr.logpageno {
                SLOTS_PER_FSM_PAGE as i32 - 1
            } else {
                -1
            };

            let mut eof = false;
            let mut slot = start_slot;
            while slot <= end_slot {
                let child_avail = if !eof {
                    let (ca, child_eof) = fsm_vacuum_page(
                        shared,
                        smgr,
                        relpersistence,
                        fsm_get_child(addr, slot as u16),
                        start,
                        end,
                    )
                    .await;
                    eof = child_eof;
                    if child_eof { 0 } else { ca }
                } else {
                    0
                };

                let cur = FsmPage::new(pool.buffer_get_page(buf)).get_avail(slot as usize);
                if cur != child_avail {
                    let _g = pool.content_exclusive(buf);
                    let buf_id = buf.as_global().unwrap() as i32;
                    // SAFETY: exclusive content lock held.
                    let page = unsafe { pool.block_mut(buf_id) };
                    FsmPageMut::new(page).set_avail(slot as usize, child_avail);
                    pool.mark_buffer_dirty(buf);
                }
                slot += 1;
            }
        }

        let max_avail = FsmPage::new(pool.buffer_get_page(buf)).get_max_avail();
        pool.release_buffer(buf);
        (max_avail, false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::buffer::buf_init::with_private_refcount;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::procnumber::INVALID_PROC_NUMBER;
    use crate::storage::relfilelocator::RelFileLocator;

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: Oid(1663), dbOid: Oid(70000 + rel), relNumber: Oid(18000 + rel) }
    }

    async fn shared_with_tmpdir(tag: &str) -> (Arc<SharedState>, std::path::PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "pepperdb_fsm_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        crate::storage::io_backend::mkdir_all(dir.join("base").join((70000 + 1).to_string()))
            .await
            .ok();
        let s = SharedState::new(SharedStateConfig { nbuffers: 64, ..SharedStateConfig::default() });
        s.config().set_data_dir(dir.to_string_lossy().into_owned());
        (s, dir)
    }

    async fn make_rel(s: &Arc<SharedState>, rel: u32) -> SmgrRelation {
        let mut reln = SmgrRelation::open(rloc(rel), INVALID_PROC_NUMBER);
        reln.create(s, ForkNumber::MAIN_FORKNUM, false).await;
        reln
    }

    #[test]
    fn category_conversion_boundaries() {
        // Step size is BLCKSZ/256 = 32 for 8 KB pages.
        assert_eq!(FSM_CAT_STEP, 32);
        assert_eq!(fsm_space_avail_to_cat(0), 0);
        assert_eq!(fsm_space_avail_to_cat(31), 0);
        assert_eq!(fsm_space_avail_to_cat(32), 1);
        // Top category is reserved for >= MaxFSMRequestSize.
        assert_eq!(fsm_space_avail_to_cat(MAX_FSM_REQUEST_SIZE), 255);
        // needed rounds up.
        assert_eq!(fsm_space_needed_to_cat(0), 1);
        assert_eq!(fsm_space_needed_to_cat(1), 1);
        assert_eq!(fsm_space_needed_to_cat(32), 1);
        assert_eq!(fsm_space_needed_to_cat(33), 2);
        // cat -> avail lower bound round-trips for mid categories.
        assert_eq!(fsm_space_cat_to_avail(1), 32);
        assert_eq!(fsm_space_cat_to_avail(255), MAX_FSM_REQUEST_SIZE);
    }

    #[test]
    fn three_level_addressing() {
        // Default BLCKSZ -> 3-level tree.
        assert_eq!(FSM_TREE_DEPTH, 3);
        // The first heap block maps to bottom level, slot 0; its physical block
        // is the depth-first first leaf.
        let (addr0, slot0) = fsm_get_location(0);
        assert_eq!(addr0.level, FSM_BOTTOM_LEVEL);
        assert_eq!(slot0, 0);
        // Root is the highest physical block among {root, mid, leaf0} chain ends;
        // logical->physical is monotonic per the depth-first layout.
        let root_phys = fsm_logical_to_physical(FSM_ROOT_ADDRESS);
        let leaf0_phys = fsm_logical_to_physical(addr0);
        assert!(leaf0_phys > root_phys, "leaf is deeper in the depth-first file");
        // parent/child are inverses.
        let (parent, pslot) = fsm_get_parent(addr0);
        assert_eq!(fsm_get_child(parent, pslot), addr0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_then_get_round_trips_through_fork() {
        let (s, dir) = shared_with_tmpdir("roundtrip").await;
        with_private_refcount(|| async {
            let mut reln = make_rel(&s, 1).await;
            let p = RELPERSISTENCE_PERMANENT;
            let heap_blk = 5;
            let space = 2048;

            record_page_with_free_space(&s, &mut reln, p, heap_blk, space).await;

            // GetRecorded reflects the category lower bound for that space (the
            // bottom-level leaf is set immediately, no vacuum needed).
            let got = get_recorded_free_space(&s, &mut reln, p, heap_blk).await;
            assert_eq!(got, fsm_space_cat_to_avail(fsm_space_avail_to_cat(space)));

            // RecordPageWithFreeSpace only sets the bottom leaf; the upper FSM
            // pages become visible to searchers after a vacuum (C semantics).
            free_space_map_vacuum(&s, &mut reln, p).await;

            // Now a search for less-or-equal space finds that heap block.
            let found = get_page_with_free_space(&s, &mut reln, p, 1024).await;
            assert_eq!(found, Some(heap_blk));
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_fsm_search_is_read_only_under_shared_lock() {
        // Guards the BLOCKER fix: two tasks search the SAME relation's FSM
        // concurrently. The search path takes only the SHARED content lock and
        // reads the page via `&Page` (FsmPage::search_avail) -- no `block_mut`,
        // so two share-lock holders never form aliasing &mut/& to one Page. If a
        // &mut under the shared lock were ever reintroduced this would deadlock
        // or double-borrow; here both must complete and find the recorded block.
        let (s, dir) = shared_with_tmpdir("concurrent").await;
        with_private_refcount(|| async {
            let mut reln = make_rel(&s, 1).await;
            let p = RELPERSISTENCE_PERMANENT;
            let heap_blk = 7;
            record_page_with_free_space(&s, &mut reln, p, heap_blk, 4096).await;
            free_space_map_vacuum(&s, &mut reln, p).await;
        })
        .await;

        let loc = rloc(1);
        let s1 = s.clone();
        let s2 = s.clone();
        let t1 = tokio::spawn(async move {
            with_private_refcount(|| async {
                let mut r = SmgrRelation::open(loc, INVALID_PROC_NUMBER);
                let mut last = None;
                for _ in 0..200 {
                    last = get_page_with_free_space(&s1, &mut r, RELPERSISTENCE_PERMANENT, 1024)
                        .await;
                }
                last
            })
            .await
        });
        let t2 = tokio::spawn(async move {
            with_private_refcount(|| async {
                let mut r = SmgrRelation::open(loc, INVALID_PROC_NUMBER);
                let mut last = None;
                for _ in 0..200 {
                    last = get_page_with_free_space(&s2, &mut r, RELPERSISTENCE_PERMANENT, 1024)
                        .await;
                }
                last
            })
            .await
        });
        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();
        assert_eq!(r1, Some(7));
        assert_eq!(r2, Some(7));
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn search_returns_none_when_nothing_fits() {
        let (s, dir) = shared_with_tmpdir("none").await;
        with_private_refcount(|| async {
            let mut reln = make_rel(&s, 1).await;
            let p = RELPERSISTENCE_PERMANENT;
            // Record a small amount on one block.
            record_page_with_free_space(&s, &mut reln, p, 0, 100).await;
            // Ask for more than that block has -> the search updates upper nodes
            // but ultimately finds nothing big enough.
            let found = get_page_with_free_space(&s, &mut reln, p, MAX_FSM_REQUEST_SIZE).await;
            assert_eq!(found, None);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }
}
