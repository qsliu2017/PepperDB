//! Heap page pruning. Translated from backend/access/heap/pruneheap.c.
//!
//! M13 scope (step 46): `heap_page_prune` -- the page-level dead-tuple removal
//! primitive VACUUM's first pass calls per heap page. It scans the page's line
//! pointers, tests each tuple's deadness against the VACUUM cutoff
//! (`HeapTupleSatisfiesVacuum(oldest_xmin)`), and marks every removable tuple's
//! line pointer LP_DEAD, collecting the (offset, heap-TID) list for the index
//! vacuum and the second heap pass. Storage is reclaimed later by
//! `lazy_vacuum_heap_page` (set LP_UNUSED) + `PageRepairFragmentation`.
//!
//! Staged (rules.md s4): HOT-chain pruning (`heap_prune_chain` following t_ctid
//! redirects, LP_REDIRECT root management), opportunistic `heap_page_prune_opt`,
//! the combined prune-and-freeze pass, and WAL logging of the prune
//! (`XLOG_HEAP2_PRUNE`). The M2 heap produces no HOT chains (every update is a
//! full new-version insert, both tuples not-HOT), so the single-tuple removal path
//! is the complete correctness core; the WAL prune record is deferred (VACUUM
//! rewrites the page image, which the checkpoint/full-page path already covers on
//! the buffer's next flush -- correctness is unchanged, only crash-recovery replay
//! granularity differs, which the M13 milestone does not exercise).
//!
//! Async coloring (rules.md s5): the deadness test reaches clog/procarray, so the
//! page scan is `async`; the buffer content lock is NOT held across the `.await`s
//! -- the caller (`lazy_scan_heap`) passes an owned copy of each candidate header
//! (read under a brief lock), and the line-pointer marking happens under a fresh
//! exclusive lock with no `.await` inside.

use std::sync::Arc;

use crate::access::heapam::HTSV_Result;
use crate::access::htup_details::SizeofHeapTupleHeader;
use crate::c::TransactionId;
use crate::backend::access::heap::heapam_visibility::HeapTupleSatisfiesVacuum;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::{OffsetNumber, FIRST_OFFSET_NUMBER};
use crate::utils::rel::RelationData;

/// Result of pruning one heap page: the offsets whose line pointers were marked
/// LP_DEAD (to be set LP_UNUSED in the second pass) and the corresponding heap TIDs
/// (fed to the index bulk-delete callback). `ndeleted` counts the tuples removed.
#[derive(Debug, Default)]
pub struct PruneResult {
    /// Offsets marked LP_DEAD on this page (its second-pass "unused" set).
    pub dead_offsets: Vec<OffsetNumber>,
    /// Heap TIDs of the dead tuples (for the index vacuum callback).
    pub dead_tids: Vec<ItemPointerData>,
    /// Number of live (still-visible) tuples remaining on the page.
    pub num_live: i64,
}

/// `heap_page_prune` (M13 single-tuple subset): prune the heap page in `buffer` of
/// `relation`, marking every tuple dead relative to `oldest_xmin` LP_DEAD.
///
/// The caller must hold a cleanup/exclusive lock on `buffer` for the whole call
/// (PG requires a cleanup lock). Because the deadness test awaits, this function
/// reads each candidate header under a brief share of the already-held exclusive
/// lock semantics (the caller passes the buffer already exclusively locked is not
/// possible across `.await`; instead we drop and re-take the content lock around
/// the awaiting test, which is sound under the ShareUpdateExclusive relation lock
/// VACUUM holds -- no concurrent writer can change the page's line pointers).
///
/// Returns the `PruneResult` (dead offsets + TIDs + live count); the line pointers
/// are marked LP_DEAD in place and the buffer is marked dirty if anything changed.
pub async fn heap_page_prune(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    buffer: Buffer,
    block: BlockNumber,
    oldest_xmin: TransactionId,
) -> PruneResult {
    let pool = shared.buffers();
    let buf_id = buf_id_of(buffer);

    // Snapshot the page's line count under a brief share lock.
    let maxoff = {
        let _g = pool.content_share(buffer);
        pool.buffer_get_page(buffer).get_max_offset_number()
    };

    let mut result = PruneResult::default();

    for offnum in FIRST_OFFSET_NUMBER..=maxoff {
        // Read the candidate header (copied out) under a brief share lock, then drop
        // it before the awaiting deadness test.
        let hdr = {
            let _g = pool.content_share(buffer);
            let page = pool.buffer_get_page(buffer);
            let item_id = page.get_item_id(offnum);
            if item_id.is_normal() {
                let item = page.get_item(&item_id);
                debug_assert!(item.len() >= SizeofHeapTupleHeader);
                // SAFETY: a normal heap item begins with a HeapTupleHeaderData.
                Some(unsafe {
                    std::ptr::read_unaligned(
                        item.as_ptr().cast::<crate::access::htup_details::HeapTupleHeaderData>(),
                    )
                })
            } else {
                None
            }
        };

        let Some(hdr) = hdr else { continue };

        let status = HeapTupleSatisfiesVacuum(shared, &hdr, oldest_xmin).await;
        if status == HTSV_Result::Dead {
            let mut tid = ItemPointerData {
                blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
                posid: 0,
            };
            tid.set(block, offnum);
            result.dead_offsets.push(offnum);
            result.dead_tids.push(tid);
        } else {
            result.num_live += 1;
        }
    }

    // Mark the dead line pointers LP_DEAD in place under one exclusive lock (no
    // `.await` inside). Under VACUUM's ShareUpdateExclusive lock no writer can have
    // changed the page since we scanned it.
    if !result.dead_offsets.is_empty() {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        heap_prune_mark_dead(page, &result.dead_offsets);
        pool.mark_buffer_dirty(buffer);
    }

    result
}

/// Mark each offset's line pointer LP_DEAD (keeps its storage until the second
/// pass sets it LP_UNUSED). Mirrors the LP_DEAD arm of `heap_page_prune_execute`.
fn heap_prune_mark_dead(page: &mut Page, offsets: &[OffsetNumber]) {
    for &off in offsets {
        let mut lp = page.get_item_id(off);
        lp.set_dead();
        page.set_item_id(off, lp);
    }
}

/// The global buffer-pool slot index for a pinned shared buffer.
fn buf_id_of(buffer: Buffer) -> i32 {
    #[allow(clippy::expect_used, reason = "heap pages are always shared (global) buffers")]
    let id = buffer.as_global().expect("shared buffer expected") as i32;
    id
}
