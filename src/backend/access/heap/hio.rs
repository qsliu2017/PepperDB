//! Heap insertion-target buffer management. Translated from
//! backend/access/heap/hio.c.
//!
//! `RelationGetBufferForTuple` finds (or extends to) a page with room for a
//! tuple; `RelationPutHeapTuple` places a prepared tuple on a locked page and
//! patches its CTID. These are the storage-placement half of `heap_insert`.
//!
//! Async coloring (rules.md s5): reading/extending a page and consulting the FSM
//! hit the buffer pool / smgr async leaves, so `relation_get_buffer_for_tuple` is
//! `async`. It returns a *pinned* buffer whose page has been verified to have
//! room; the caller (`heap_insert`) re-takes the exclusive content lock
//! synchronously and inserts. No content lock is ever held across an `.await`
//! (the C "return a locked buffer" contract is split: select+verify here, lock
//! +mutate in the caller, with a cheap re-check under the lock to cover the
//! single rare race window). `relation_put_heap_tuple` is sync (it runs under the
//! already-held exclusive content lock).
//!
//! M2 scope (step 12): insert into a heap, no `otherBuffer` (update path), no
//! visibility-map pins, no bulk extension, no `BulkInsertState`. Those are grow
//! guards added at M6/M8; the FSM lookup + single-page extend core is faithful.

use std::sync::Arc;

use crate::access::htup::HeapTuple;
use crate::access::htup_details::{MaxHeapTupleSize, MaxHeapTuplesPerPage};
use crate::backend::access::heap::heapam::{SendPtr, SendRelation};
use crate::backend::storage::buffer::bufmgr::read_buffer_common;
use crate::backend::storage::freespace::freespace::{
    get_page_with_free_space, record_and_get_page_with_free_space,
};
use crate::common::relpath::ForkNumber;
use crate::shared_state::SharedState;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::{ReadBufferMode, P_NEW};
use crate::storage::bufpage::Page;
use crate::storage::itemid::ItemIdData;
use crate::storage::off::{OffsetNumber, INVALID_OFFSET_NUMBER};
use crate::storage::smgr::SmgrRelation;
use crate::utils::elog::ERROR;
use crate::utils::rel::{RelationData, HEAP_DEFAULT_FILLFACTOR};
use crate::utils::relcache::Relation;

/// `BLCKSZ` as a `usize` page size for `Page::init`.
const PAGE_SIZE: usize = crate::pg_config::BLCKSZ as usize;

/// Item-id size, used in the "nearly empty" free-space threshold.
const SIZEOF_ITEM_ID_DATA: usize = core::mem::size_of::<ItemIdData>();

/// `RelationPutHeapTuple`: add a prepared `tuple` to `buffer`'s page and patch
/// the stored tuple's CTID to its own location. The caller MUST hold the buffer
/// pinned and exclusively content-locked (this only touches the page bytes).
///
/// Returns the offset number where the tuple landed (C updates `tuple.t_self`,
/// which the in-memory `HeapTupleData` reflects; the on-page copy's `t_ctid` is
/// patched to match). `token` (speculative insert) is M8 scope -> false here.
///
/// SAFETY: `tuple` is a live in-memory heap tuple; `page` is the sole writer's
/// view of the pinned, exclusively-locked buffer.
pub fn relation_put_heap_tuple(page: &mut Page, block: BlockNumber, tuple: &mut HeapTuple) {
    // `tuple` is `&mut *mut HeapTupleData`; deref once to the HeapTupleData.
    // SAFETY: caller guarantees `(*tuple).t_data` points at a valid header+data
    // block of `t_len` bytes (built by heap_form_tuple / heap_prepare_insert).
    let (item_bytes, t_len) = unsafe {
        let td = (**tuple).t_data;
        let len = (**tuple).t_len as usize;
        (core::slice::from_raw_parts(td.cast::<u8>(), len), len)
    };

    let offnum = page.add_item(item_bytes, t_len, INVALID_OFFSET_NUMBER, false, true);
    crate::assert!(offnum != INVALID_OFFSET_NUMBER, "failed to add tuple to page");

    // Update the in-memory tuple's self-pointer to where it was stored.
    // SAFETY: live in-memory tuple (see above).
    unsafe {
        (**tuple).t_self.set(block, offnum);
    }

    // Patch the on-page copy's t_ctid to point at itself (non-speculative). The
    // stored header begins at the item's lp_off; the page is 8-aligned and the
    // offset is MAXALIGN'd by add_item, so the HeapTupleHeaderData overlay is
    // soundly aligned.
    let item_off = page.get_item_id(offnum).lp_off() as usize;
    let bytes = page.as_mut_bytes();
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "sound overlay: the page is 8-aligned and PageAddItem MAXALIGNs item offsets, so HeapTupleHeaderData's align (4) divides the address"
    )]
    let hdr = bytes[item_off..]
        .as_mut_ptr()
        .cast::<crate::access::htup_details::HeapTupleHeaderData>();
    // SAFETY: exclusive content lock held -> sole writer; `item_off` addresses
    // the header we just stored, aligned (see above).
    unsafe {
        (*hdr).ctid.set(block, offnum);
    }
}

/// `RelationGetBufferForTuple` (M2 core): return a pinned buffer whose page has
/// at least `len` bytes of heap free space, extending the relation if no
/// existing page qualifies. The page is NOT locked on return; the caller takes
/// the exclusive content lock and re-checks (a cheap guard against the rare race
/// where another inserter filled the page between our check and the caller's
/// lock).
///
/// `options` carries `HEAP_INSERT_SKIP_FSM` (disables FSM consultation). The
/// `otherBuffer`/`vmbuffer`/`bistate`/`num_pages` parameters of the C signature
/// are M6/M8 scope and omitted.
#[allow(
    clippy::too_many_lines,
    reason = "faithful translation of RelationGetBufferForTuple's target-select / read / extend loop"
)]
pub async fn relation_get_buffer_for_tuple(
    shared: &Arc<SharedState>,
    relation: SendRelation,
    len: usize,
    options: i32,
) -> Buffer {
    use crate::access::heapam::HEAP_INSERT_SKIP_FSM;

    let use_fsm = (options & HEAP_INSERT_SKIP_FSM) == 0;

    // Pull the Send scalars + the (Send) smgr handle out under a short borrow, so
    // no `&mut RelationData` (it is `!Send`) is held across an `.await`.
    let (relpersistence, save_free_space, smgr_ptr) = {
        // SAFETY: `relation` is a live, open relation (caller holds it open).
        let rel: &mut RelationData = unsafe { &mut *relation.get() };
        let relpersistence = unsafe { (*rel.rd_rel).relpersistence };
        let save_free_space = rel.target_page_free_space(HEAP_DEFAULT_FILLFACTOR) as usize;
        (relpersistence, save_free_space, SendPtr(rel.smgr()))
    };
    // SAFETY: relcache-owned smgr handle, valid while the relation is open; Send
    // (no raw pointers inside SmgrRelation), so the borrow may cross `.await`.
    let smgr: &mut SmgrRelation = unsafe { &mut *smgr_ptr.get() };

    let len = crate::c::MAXALIGN(len);

    if len > MaxHeapTupleSize {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_PROGRAM_LIMIT_EXCEEDED).errmsg(format!(
                "row is too big: size {len}, maximum size {MaxHeapTupleSize}"
            ));
        });
    }

    // Pages with only line pointers count as "empty" when the unavailable space
    // is slight; this avoids needless extensions for large tuples.
    let nearly_empty_free_space =
        MaxHeapTupleSize - (MaxHeapTuplesPerPage as usize / 8 * SIZEOF_ITEM_ID_DATA);
    let target_free_space = if len + save_free_space > nearly_empty_free_space {
        len.max(nearly_empty_free_space)
    } else {
        len + save_free_space
    };

    // Try the cached target page first, then the FSM.
    // SAFETY: short borrow, no await held.
    let mut target_block = unsafe { (*relation.get()).target_block() };

    if target_block == INVALID_BLOCK_NUMBER && use_fsm {
        target_block = get_page_with_free_space(shared, smgr, relpersistence, target_free_space)
            .await
            .unwrap_or(INVALID_BLOCK_NUMBER);
    }

    // If the FSM knows nothing, try the last page before extending (avoids
    // one-tuple-per-page during bootstrapping / a freshly started system).
    if target_block == INVALID_BLOCK_NUMBER {
        let nblocks = smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await;
        if nblocks > 0 {
            target_block = nblocks - 1;
        }
    }

    while target_block != INVALID_BLOCK_NUMBER {
        let buffer = read_buffer_common(
            shared,
            smgr,
            relpersistence,
            ForkNumber::MAIN_FORKNUM,
            target_block,
            ReadBufferMode::NORMAL,
            None,
        )
        .await;

        // Initialize the page if it is brand new, then measure free space, both
        // under the exclusive content lock (the sole-writer invariant).
        let page_free_space = {
            let pool = shared.buffers();
            let _g = pool.content_exclusive(buffer);
            let buf_id = buf_id_of(buffer);
            // SAFETY: exclusive content lock held -> sole writer to this slot.
            let page = unsafe { pool.block_mut(buf_id) };
            if page.is_new() {
                page.init(PAGE_SIZE, 0);
                pool.mark_buffer_dirty(buffer);
            }
            page.get_heap_free_space()
        };

        if target_free_space <= page_free_space {
            // Use this page as the future insert target, too.
            // SAFETY: short borrow, no await held.
            unsafe { (*relation.get()).set_target_block(target_block) };
            return buffer;
        }

        // Not enough room: release and ask the FSM for another page.
        shared.buffers().release_buffer(buffer);

        if !use_fsm {
            break;
        }

        target_block = record_and_get_page_with_free_space(
            shared,
            smgr,
            relpersistence,
            target_block,
            page_free_space,
            target_free_space,
        )
        .await
        .unwrap_or(INVALID_BLOCK_NUMBER);
    }

    // Extend the relation by one block. RBM_ZERO_AND_LOCK gives a zeroed,
    // exclusively-locked new page; we release the implicit lock and return the
    // pinned buffer (the caller re-locks, matching the existing-page path).
    let buffer = read_buffer_common(
        shared,
        smgr,
        relpersistence,
        ForkNumber::MAIN_FORKNUM,
        P_NEW,
        ReadBufferMode::ZERO_AND_LOCK,
        None,
    )
    .await;

    let new_block = shared.buffers().buffer_get_block_number(buffer);

    {
        let pool = shared.buffers();
        // ZERO_AND_LOCK returned the page exclusively locked; re-acquire to init
        // under our own guard scope, then drop (the caller re-locks to insert).
        let _g = pool.content_exclusive(buffer);
        let buf_id = buf_id_of(buffer);
        // SAFETY: exclusive content lock held -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        if page.is_new() {
            page.init(PAGE_SIZE, 0);
        }
        pool.mark_buffer_dirty(buffer);
    }

    // SAFETY: short borrow, no await held.
    unsafe { (*relation.get()).set_target_block(new_block) };
    buffer
}

/// The global buffer-pool slot index for a pinned shared buffer.
fn buf_id_of(buffer: Buffer) -> i32 {
    #[allow(clippy::expect_used, reason = "heap pages are always shared (global) buffers")]
    let id = buffer.as_global().expect("shared buffer expected") as i32;
    id
}

