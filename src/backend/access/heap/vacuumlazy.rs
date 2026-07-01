//! Lazy (in-place) heap vacuum. Translated from the M13-reachable core of
//! backend/access/heap/vacuumlazy.c.
//!
//! M13 scope (step 46): `lazy_scan_heap` -- the heap-vacuum driver. It scans every
//! heap page, prunes dead tuples (`heap_page_prune`, marking their line pointers
//! LP_DEAD), collects the dead heap TIDs, vacuums the indexes (removing index
//! entries that point at the dead tuples via the genam bulk-delete entry), then
//! runs the second heap pass (`lazy_vacuum_heap_page`: set the dead line pointers
//! LP_UNUSED, `PageRepairFragmentation` to reclaim the space), updates the FSM with
//! the reclaimed free space, and returns the live-tuple / page counts so the driver
//! can update pg_class.relpages/reltuples.
//!
//! Staged (rules.md s4): parallel vacuum, the multi-pass index vacuum with a huge
//! dead-TID store (here the dead TIDs fit in one in-memory `Vec`, one index pass),
//! aggressive freeze / wraparound handling, the visibility-map maintenance
//! (all-visible/all-frozen bits), `lazy_truncate_heap` (relation truncation), and
//! the WAL vacuum records (the rewritten pages are covered by the buffer's next
//! full-page flush). The single-pass dead-tuple-removal + space-reclaim core is
//! complete.
//!
//! Async coloring (rules.md s5): every page read/write goes through the buffer
//! pool, so the scan is `async`; content locks are never held across `.await`
//! (each page's prune / second-pass rewrite takes the lock, mutates, drops it).

use std::collections::HashSet;
use std::sync::Arc;

use crate::backend::access::heap::pruneheap::heap_page_prune;
use crate::backend::access::index::genam::index_vacuum_bulk_delete;
use crate::c::TransactionId;
use crate::common::relpath::ForkNumber;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::ReadBufferMode;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::RelationData;

/// Summary of one heap vacuum: the reclaimed / retained counts the driver writes
/// back to pg_class.
#[derive(Debug, Default)]
pub struct LazyVacuumResult {
    /// Total heap blocks scanned (-> pg_class.relpages).
    pub num_pages: BlockNumber,
    /// Live (still-visible) tuples remaining (-> pg_class.reltuples).
    pub num_live_tuples: f64,
    /// Dead tuples removed across all pages.
    pub tuples_deleted: u64,
    /// Index entries removed across all indexes.
    pub index_tuples_removed: u64,
}

/// `lazy_scan_heap` (M13 single-pass): vacuum `relation`'s heap in place using the
/// `oldest_xmin` cutoff to decide tuple deadness, vacuuming its `indexes`.
///
/// Pass 1: scan every page, `heap_page_prune` marks dead line pointers LP_DEAD and
/// yields their (offset, TID). Index vacuum: remove the dead TIDs' index entries.
/// Pass 2: `lazy_vacuum_heap_page` sets the LP_DEAD pointers LP_UNUSED +
/// `PageRepairFragmentation`, then the FSM records the page's reclaimed free space.
///
/// The caller must hold ShareUpdateExclusive on `relation` (VACUUM's lock), which
/// keeps concurrent writers out so the two-pass mark/reclaim is race-free.
pub async fn lazy_scan_heap(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    indexes: &[Arc<RelationData>],
    oldest_xmin: TransactionId,
) -> LazyVacuumResult {
    let nblocks = heap_nblocks(shared, relation).await;

    // Pass 1: prune each page, collecting per-page dead offsets + the global dead
    // TID set for the index vacuum.
    let mut per_page_dead: Vec<(BlockNumber, Vec<OffsetNumber>)> = Vec::new();
    let mut all_dead_tids: HashSet<ItemPointerData> = HashSet::new();
    let mut num_live_tuples: f64 = 0.0;
    let mut tuples_deleted: u64 = 0;

    for block in 0..nblocks {
        let buffer = read_heap_block(shared, relation, block).await;
        let result = heap_page_prune(shared, relation, buffer, block, oldest_xmin).await;
        num_live_tuples += result.num_live as f64;
        tuples_deleted += result.dead_offsets.len() as u64;
        for tid in &result.dead_tids {
            all_dead_tids.insert(*tid);
        }
        if !result.dead_offsets.is_empty() {
            per_page_dead.push((block, result.dead_offsets));
        }
        shared.buffers().release_buffer(buffer);
    }

    // Index vacuum: remove every index entry pointing at a dead heap tuple. One
    // pass (the dead-TID set fits in memory); the multi-pass path is staged.
    let mut index_tuples_removed: u64 = 0;
    if !all_dead_tids.is_empty() {
        for index in indexes {
            let (removed, _remaining) =
                index_vacuum_bulk_delete(shared, index, &all_dead_tids).await;
            index_tuples_removed += removed;
        }
    }

    // Pass 2: for each page with dead line pointers, set them LP_UNUSED, repair
    // fragmentation (reclaim space), and record the reclaimed free space in the FSM.
    for (block, offsets) in &per_page_dead {
        lazy_vacuum_heap_page(shared, relation, *block, offsets).await;
    }

    LazyVacuumResult {
        num_pages: nblocks,
        num_live_tuples,
        tuples_deleted,
        index_tuples_removed,
    }
}

/// `lazy_vacuum_heap_page`: the VACUUM second pass for one page. Sets each dead
/// (LP_DEAD) line pointer in `offsets` LP_UNUSED, compacts the page
/// (`PageRepairFragmentation`, reclaiming the dead tuples' space), marks the buffer
/// dirty, then records the page's now-larger free space in the FSM so a later
/// insert reuses it.
async fn lazy_vacuum_heap_page(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    block: BlockNumber,
    offsets: &[OffsetNumber],
) {
    let buffer = read_heap_block(shared, relation, block).await;
    let pool = shared.buffers();
    let buf_id = buf_id_of(buffer);

    let freespace = {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        for &off in offsets {
            let mut lp = page.get_item_id(off);
            lp.set_unused();
            page.set_item_id(off, lp);
        }
        page.repair_fragmentation();
        pool.mark_buffer_dirty(buffer);
        page.get_heap_free_space()
    };

    // Record the reclaimed free space so future inserts reuse it.
    let relpersistence = relation.form().relpersistence;
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle, valid while the rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    crate::backend::storage::freespace::freespace::record_page_with_free_space(
        shared,
        smgr,
        relpersistence,
        block,
        freespace,
    )
    .await;

    pool.release_buffer(buffer);
}

/// The block count of `relation`'s main fork.
async fn heap_nblocks(shared: &Arc<SharedState>, relation: &RelationData) -> BlockNumber {
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle, valid while the rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await
}

/// Read a main-fork block of `relation` into a pinned buffer.
async fn read_heap_block(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    block: BlockNumber,
) -> Buffer {
    let relpersistence = relation.form().relpersistence;
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle valid while rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    crate::backend::storage::buffer::bufmgr::read_buffer_common(
        shared,
        smgr,
        relpersistence,
        ForkNumber::MAIN_FORKNUM,
        block,
        ReadBufferMode::NORMAL,
        None,
    )
    .await
}

/// The global buffer-pool slot index for a pinned shared buffer.
fn buf_id_of(buffer: Buffer) -> i32 {
    #[allow(clippy::expect_used, reason = "heap pages are always shared (global) buffers")]
    let id = buffer.as_global().expect("shared buffer expected") as i32;
    id
}
