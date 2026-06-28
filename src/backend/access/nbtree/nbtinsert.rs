//! Item insertion into a btree index. Translated from the M2-reachable core of
//! `src/backend/access/nbtree/nbtinsert.c`.
//!
//! `bt_doinsert` is the entry the index AM `btinsert` drives: descend to the leaf
//! that should hold the new key, insert it, and -- if the leaf overflows -- split
//! it and propagate a downlink up the tree (creating a new root when the root
//! itself splits). It emits WAL (`XLOG_BTREE_INSERT_LEAF` / `XLOG_BTREE_SPLIT_*` /
//! `XLOG_BTREE_NEWROOT`) via the foundation `XLogInsert`; replay is deferred to
//! step 48 (the records carry full page images so a future redo can restore them).
//!
//! M2 scope: no unique-check enforcement, no deduplication, no posting-list
//! splits, no LP_DEAD reclamation, no incomplete-split finishing (a single-process
//! build/insert never observes a concurrent incomplete split). Suffix truncation
//! is omitted (the split's new high key keeps the full separator key -- correct,
//! just not space-optimal), matching nbtsort's M2 build.
//!
//! Async-lock discipline (rules.md s5/s8): like the scan, the descent and split
//! operate on OWNED page copies. A page is read into a `Box<Page>` under a brief
//! content lock, the lock dropped, the page mutated in memory, then written back
//! in one exclusive-lock window via `bt_write_page`. No `parking_lot` guard is
//! held across an `.await`. The descent records a stack of (block, child-offset)
//! so a split can re-find the parent to insert the new downlink.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: insert holds per-backend raw Relation handles task-confined for the operation; the futures never migrate the pointee between tasks. await_holding_lock is clean (enforced)."
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "btree insert takes raw Relation/IndexTuple pointers per the C API; faithful to C"
)]

use std::sync::Arc;

use crate::access::itup::IndexTupleData;
use crate::access::nbtxlog::{
    xl_btree_insert, xl_btree_split, SizeOfBtreeInsert, SizeOfBtreeSplit, XLOG_BTREE_INSERT_LEAF,
    XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L,
};
use crate::access::nbtree::{
    BTPageGetOpaque, BTMetaPageData, BTScanInsertData, BTreeTupleSetDownLink, BTreeTupleSetNAtts,
    BTREE_METAPAGE, BTP_LEAF, BTP_ROOT, BTPageOpaqueData, P_FIRSTDATAKEY, P_FIRSTKEY, P_HIKEY,
    P_ISLEAF, P_NONE, P_RIGHTMOST,
};
use crate::access::rmgrlist::RmgrId;
use crate::access::xlogdefs::XLogRecPtr;
use crate::backend::access::nbtree::nbtpage::{
    bt_allocbuf, bt_pageinit, bt_read_buffer, bt_read_page_copy, bt_relbuf, bt_write_page,
};
use crate::backend::access::nbtree::nbtsearch::{bt_binsrch_one, bt_search_internal_path};
use crate::backend::access::nbtree::nbtutils::{bt_compare, bt_mkscankey};
use crate::backend::access::transam::xloginsert::{
    begin_insert, register_block, register_data, xlog_insert,
};
use crate::common::relpath::ForkNumber;
use crate::pg_config::BLCKSZ;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::backend::access::heap::heapam::SendPtr;
use crate::utils::rel::RelationData;
use crate::utils::relcache::Relation;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// Mutable view of a page's btree opaque area.
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Page is align(8); the special area is MAXALIGN'd, so the BTPageOpaqueData overlay is well-aligned"
)]
fn opaque_mut(page: &mut Page) -> &mut BTPageOpaqueData {
    let special = page.get_special_size() as usize;
    let off = BLCKSZ as usize - special;
    // SAFETY: off is the special-area offset; the area is sized for BTPageOpaqueData.
    unsafe { &mut *page.as_mut_bytes()[off..].as_mut_ptr().cast::<BTPageOpaqueData>() }
}

/// Set the line pointer at `off` (1-based) to `id`.
fn set_item_id(page: &mut Page, off: OffsetNumber, id: ItemIdData) {
    let idx = (off - 1) as usize;
    let base = SizeOfPageHeaderData + idx * core::mem::size_of::<ItemIdData>();
    // SAFETY: ItemIdData is a 4-byte POD.
    let bytes = unsafe { core::mem::transmute_copy::<ItemIdData, [u8; 4]>(&id) };
    page.as_mut_bytes()[base..base + 4].copy_from_slice(&bytes);
}

/// Bytes of the item at `off`.
fn item_bytes(page: &Page, off: OffsetNumber) -> Vec<u8> {
    let id = page.get_item_id(off);
    page.get_item(&id).to_vec()
}

/// Copy `key_bytes` (an index tuple image) and set its downlink TID to `child`.
fn make_downlink(key_bytes: &[u8], child: BlockNumber) -> Vec<u8> {
    let mut v = key_bytes.to_vec();
    // SAFETY: v is a valid IndexTupleData image copied from a page item.
    #[allow(clippy::cast_ptr_alignment, reason = "Vec aligned for IndexTupleData")]
    let t = v.as_mut_ptr().cast::<IndexTupleData>();
    // SAFETY: t points at the copied IndexTupleData.
    unsafe { BTreeTupleSetDownLink(&mut *t, child) };
    v
}

/// Read the meta page (owned copy).
async fn read_meta(shared: &Arc<SharedState>, rel: Relation) -> (Buffer, Box<Page>) {
    let buf = bt_read_buffer(shared, SendPtr(rel), BTREE_METAPAGE).await;
    let page = bt_read_page_copy(shared, buf);
    (buf, page)
}

/// `bt_doinsert` (M2): insert the formed leaf index tuple `itup_bytes` (its
/// `t_tid` already set to the heap TID) into index `rel`. Descends to the target
/// leaf, inserts, and splits + propagates upward as needed. Returns true.
pub async fn bt_doinsert(
    shared: &Arc<SharedState>,
    rel: Relation,
    itup_bytes: &[u8],
) -> bool {
    // Build the insertion scankey from the new tuple's key columns (the descent +
    // in-page placement comparator).
    // SAFETY: itup_bytes is a valid leaf index-tuple image; reinterpret to get the
    // IndexTuple handle bt_mkscankey reads the key attributes from.
    #[allow(clippy::cast_ptr_alignment, reason = "Vec/slice from a formed index tuple; align ok")]
    let itup_ptr = itup_bytes.as_ptr().cast::<IndexTupleData>().cast_mut();
    let mut key = bt_mkscankey(rel, Some(itup_ptr));

    // Ensure the index has a root; create the first leaf+meta if empty.
    ensure_root(shared, rel).await;

    // Descend recording the internal-page stack, landing on the target leaf.
    let (leaf_blk, mut leaf_page, stack) =
        bt_search_internal_path(shared, rel, &mut key).await;

    // Find the in-leaf insert offset (first offset whose key is >= new key).
    let firstdatakey = {
        // SAFETY: btree leaf page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(&leaf_page) };
        P_FIRSTDATAKEY(opaque)
    };
    let maxoff = leaf_page.get_max_offset_number();
    let newitemoff = bt_binsrch_one(rel, &mut key, &leaf_page, firstdatakey, maxoff);

    bt_insertonpg(shared, rel, leaf_blk, &mut leaf_page, newitemoff, itup_bytes, stack).await;
    true
}

/// Insert `itup_bytes` at `newitemoff` on page `blk` (already read into `page`).
/// If it fits, write the page back + WAL. Otherwise split and propagate.
async fn bt_insertonpg(
    shared: &Arc<SharedState>,
    rel: Relation,
    blk: BlockNumber,
    page: &mut Page,
    newitemoff: OffsetNumber,
    itup_bytes: &[u8],
    stack: Vec<(BlockNumber, OffsetNumber)>,
) {
    let itemsz = maxalign(itup_bytes.len());
    let isleaf = {
        // SAFETY: btree page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(page) };
        P_ISLEAF(opaque)
    };

    if page.get_free_space() >= itemsz {
        // Fits: add the item and write the page back.
        let off = page.add_item(itup_bytes, itup_bytes.len(), newitemoff, false, false);
        crate::assert!(off != 0, "failed to add item to index page");

        let recptr = emit_insert_wal(shared, rel, blk, page, off, isleaf).await;
        let buf = bt_read_buffer(shared, SendPtr(rel), blk).await;
        bt_write_page(shared, buf, page, recptr);
        bt_relbuf(shared, buf);
        return;
    }

    // Overflow: split the page.
    Box::pin(bt_split_and_propagate(shared, rel, blk, page, newitemoff, itup_bytes, isleaf, stack))
        .await;
}

/// Split page `blk` (`page`) around the new item, write both halves, and insert the
/// new downlink into the parent (creating a new root if `blk` was the root).
#[allow(clippy::too_many_arguments, reason = "mirrors the C _bt_split locals")]
#[allow(clippy::too_many_lines, reason = "faithful translation of _bt_split's build-both-halves + propagate body")]
async fn bt_split_and_propagate(
    shared: &Arc<SharedState>,
    rel: Relation,
    blk: BlockNumber,
    page: &mut Page,
    newitemoff: OffsetNumber,
    itup_bytes: &[u8],
    isleaf: bool,
    mut stack: Vec<(BlockNumber, OffsetNumber)>,
) {
    // Collect all current items + the new item into a sorted vector (by insert
    // offset), then split into left/right halves.
    let firstdatakey = {
        // SAFETY: btree page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(page) };
        P_FIRSTDATAKEY(opaque)
    };
    let maxoff = page.get_max_offset_number();
    let was_rightmost = {
        // SAFETY: btree page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(page) };
        P_RIGHTMOST(opaque)
    };
    let orig_next = {
        // SAFETY: btree page opaque.
        unsafe { (*BTPageGetOpaque(page)).next }
    };
    let orig_hikey = if was_rightmost { None } else { Some(item_bytes(page, P_HIKEY)) };
    let level = {
        // SAFETY: btree page opaque.
        unsafe { (*BTPageGetOpaque(page)).level }
    };

    // Build the full ordered list of data items including the new one.
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut off = firstdatakey;
    let mut inserted = false;
    while off <= maxoff {
        if !inserted && off == newitemoff {
            items.push(itup_bytes.to_vec());
            inserted = true;
        }
        items.push(item_bytes(page, off));
        off += 1;
    }
    if !inserted {
        items.push(itup_bytes.to_vec());
    }

    // Split point: roughly half (the C _bt_findsplitloc is fillfactor-aware; M2
    // uses the midpoint, which keeps the tree balanced for ordered + random loads).
    let nitems = items.len();
    let split_at = nitems / 2;
    let split_at = split_at.max(1).min(nitems - 1);

    // Allocate the right page.
    let right_buf = bt_allocbuf(shared, SendPtr(rel)).await;
    let right_blk = shared.buffers().buffer_get_block_number(right_buf);

    // Build the new left page in place of `page`.
    let mut left = Page::boxed_zeroed();
    bt_pageinit(&mut left, BLCKSZ as usize);
    {
        let lo = opaque_mut(&mut left);
        lo.level = level;
        lo.flags = if isleaf { BTP_LEAF } else { 0 };
        lo.prev = unsafe { (*BTPageGetOpaque(page)).prev };
        lo.next = right_blk;
    }
    // The left high key is the first item of the right page (the separator).
    let sep_bytes = items[split_at].clone();
    add_hikey(&mut left, &sep_bytes, isleaf);
    for it in items.iter().take(split_at) {
        let added = left.add_item(it, it.len(), 0, false, false);
        crate::assert!(added != 0, "failed to add item to left split page");
    }

    // Build the right page.
    let mut right = Page::boxed_zeroed();
    bt_pageinit(&mut right, BLCKSZ as usize);
    {
        let ro = opaque_mut(&mut right);
        ro.level = level;
        ro.flags = if isleaf { BTP_LEAF } else { 0 };
        ro.prev = blk;
        ro.next = orig_next;
    }
    if let Some(ref hk) = orig_hikey {
        add_hikey(&mut right, hk, isleaf);
    }
    for it in items.iter().skip(split_at) {
        // For a non-leaf right page, the first data item is the minus-infinity
        // downlink (truncated). For a leaf, items are added as-is.
        let first = std::ptr::eq(it.as_ptr(), items[split_at].as_ptr());
        add_data_item(&mut right, it, !isleaf && first);
    }

    // Write both halves (WAL: split record + full page images).
    let recptr = emit_split_wal(shared, rel, blk, &left, right_blk, &right, level, newitemoff).await;
    let lbuf = bt_read_buffer(shared, SendPtr(rel), blk).await;
    bt_write_page(shared, lbuf, &left, recptr);
    bt_relbuf(shared, lbuf);
    bt_write_page(shared, right_buf, &right, recptr);
    bt_relbuf(shared, right_buf);

    // Fix the old right-sibling's prev pointer to point at the new right page.
    if !was_rightmost && orig_next != P_NONE {
        let nbuf = bt_read_buffer(shared, SendPtr(rel), orig_next).await;
        let mut npage = bt_read_page_copy(shared, nbuf);
        opaque_mut(&mut npage).prev = right_blk;
        bt_write_page(shared, nbuf, &npage, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
        bt_relbuf(shared, nbuf);
    }

    // Insert the downlink for the right page into the parent.
    let downlink = make_downlink(&sep_bytes, right_blk);

    match stack.pop() {
        None => {
            // `blk` was the root: build a new root with two downlinks (minus-inf ->
            // left, sep -> right).
            bt_newroot(shared, rel, blk, &sep_bytes, right_blk, level).await;
        }
        Some((parent_blk, _child_off)) => {
            // Insert the downlink into the parent (which may itself split).
            let parent_buf = bt_read_buffer(shared, SendPtr(rel), parent_blk).await;
            let mut parent_page = bt_read_page_copy(shared, parent_buf);
            bt_relbuf(shared, parent_buf);

            // Find where the downlink goes (just after the child's old downlink).
            // Re-derive the offset by comparing the separator against the parent.
            let mut parent_key = {
                #[allow(clippy::cast_ptr_alignment, reason = "Vec from a formed pivot; align ok")]
                let sep_ptr = sep_bytes.as_ptr().cast::<IndexTupleData>().cast_mut();
                bt_mkscankey(rel, Some(sep_ptr))
            };
            let parent_first = {
                // SAFETY: parent page opaque.
                let opaque = unsafe { &*BTPageGetOpaque(&parent_page) };
                P_FIRSTDATAKEY(opaque)
            };
            let parent_max = parent_page.get_max_offset_number();
            let insoff = bt_binsrch_one(rel, &mut parent_key, &parent_page, parent_first, parent_max);

            Box::pin(bt_insertonpg(shared, rel, parent_blk, &mut parent_page, insoff, &downlink, stack))
                .await;
        }
    }
}

/// Add a high key (offset P_HIKEY) to a fresh split page. The high key is the
/// separator (first key of the right page); on a leaf it is added as-is (suffix
/// truncation deferred), on a non-leaf it is the pivot key.
fn add_hikey(page: &mut Page, sep_bytes: &[u8], _isleaf: bool) {
    let added = page.add_item(sep_bytes, sep_bytes.len(), P_HIKEY, false, false);
    crate::assert!(added != 0, "failed to add high key to split page");
}

/// Add a data item to a page; if `minusinf` truncate to a 0-attr downlink pivot.
fn add_data_item(page: &mut Page, bytes: &[u8], minusinf: bool) {
    if minusinf {
        let mut trunc = vec![0u8; core::mem::size_of::<IndexTupleData>()];
        let n = bytes.len().min(trunc.len());
        trunc[..n].copy_from_slice(&bytes[..n]);
        // SAFETY: trunc sized for IndexTupleData (Vec aligned).
        #[allow(clippy::cast_ptr_alignment, reason = "Vec aligned for IndexTupleData")]
        let t = trunc.as_mut_ptr().cast::<IndexTupleData>();
        // SAFETY: t points at the sized IndexTupleData.
        unsafe {
            (*t).t_info = core::mem::size_of::<IndexTupleData>() as u16;
            BTreeTupleSetNAtts(&mut *t, 0, false);
        }
        let added = page.add_item(&trunc, trunc.len(), 0, false, false);
        crate::assert!(added != 0, "failed to add minus-inf downlink");
        return;
    }
    let added = page.add_item(bytes, bytes.len(), 0, false, false);
    crate::assert!(added != 0, "failed to add data item to split page");
}

/// `_bt_newroot`: the root at `lblk` split; build a new root one level up with the
/// minus-infinity downlink to the old root and the separator downlink to the new
/// right page, then update the meta page to point at it.
async fn bt_newroot(
    shared: &Arc<SharedState>,
    rel: Relation,
    lblk: BlockNumber,
    sep_bytes: &[u8],
    rblk: BlockNumber,
    child_level: u32,
) {
    // Clear BTP_ROOT on the old root (now an internal/leaf child).
    {
        let buf = bt_read_buffer(shared, SendPtr(rel), lblk).await;
        let mut page = bt_read_page_copy(shared, buf);
        opaque_mut(&mut page).flags &= !BTP_ROOT;
        bt_write_page(shared, buf, &page, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
        bt_relbuf(shared, buf);
    }

    // Build the new root page.
    let root_buf = bt_allocbuf(shared, SendPtr(rel)).await;
    let root_blk = shared.buffers().buffer_get_block_number(root_buf);
    let mut root = Page::boxed_zeroed();
    bt_pageinit(&mut root, BLCKSZ as usize);
    {
        let ro = opaque_mut(&mut root);
        ro.level = child_level + 1;
        ro.flags = BTP_ROOT;
        ro.prev = P_NONE;
        ro.next = P_NONE;
    }
    // Item 1 (P_HIKEY slot is unused on a rightmost page): the minus-infinity
    // downlink to the old root.
    let mut left_dl = make_downlink(sep_bytes, lblk);
    {
        // Truncate to 0 attrs (minus infinity), keep the downlink.
        #[allow(clippy::cast_ptr_alignment, reason = "Vec aligned for IndexTupleData")]
        let t = left_dl.as_mut_ptr().cast::<IndexTupleData>();
        // SAFETY: t points at the copied pivot.
        let dl = unsafe { crate::access::nbtree::BTreeTupleGetDownLink(&*t) };
        let mut minus = vec![0u8; core::mem::size_of::<IndexTupleData>()];
        let n = left_dl.len().min(minus.len());
        minus[..n].copy_from_slice(&left_dl[..n]);
        // SAFETY: minus sized for IndexTupleData.
        #[allow(clippy::cast_ptr_alignment, reason = "Vec aligned")]
        let mt = minus.as_mut_ptr().cast::<IndexTupleData>();
        // SAFETY: mt points at the sized IndexTupleData.
        unsafe {
            (*mt).t_info = core::mem::size_of::<IndexTupleData>() as u16;
            BTreeTupleSetNAtts(&mut *mt, 0, false);
            BTreeTupleSetDownLink(&mut *mt, dl);
        }
        // A new root is rightmost (no high key): data begins at offset 1. Append.
        let added = root.add_item(&minus, minus.len(), 0, false, false);
        crate::assert!(added != 0, "failed to add minus-inf root downlink");
    }
    // The separator downlink to the new right page (appended after the minus-inf).
    let right_dl = make_downlink(sep_bytes, rblk);
    let added = root.add_item(&right_dl, right_dl.len(), 0, false, false);
    crate::assert!(added != 0, "failed to add right root downlink");

    // WAL: newroot record + page image.
    let recptr = emit_newroot_wal(shared, rel, root_blk, &root, child_level + 1).await;
    bt_write_page(shared, root_buf, &root, recptr);
    bt_relbuf(shared, root_buf);

    // Update the meta page to point at the new root.
    update_meta_root(shared, rel, root_blk, child_level + 1).await;
}

/// Ensure the index has a root page. If the meta page says the index is empty
/// (root == P_NONE), create the first leaf page (also the root) and point the meta
/// page at it.
async fn ensure_root(shared: &Arc<SharedState>, rel: Relation) {
    let (metabuf, metapage) = read_meta(shared, rel).await;
    let root = {
        // SAFETY: meta page contents begin with BTMetaPageData.
        let metad = read_meta_data(&metapage);
        metad.root
    };
    if root != P_NONE {
        bt_relbuf(shared, metabuf);
        return;
    }
    bt_relbuf(shared, metabuf);

    // Create the first leaf+root page.
    let root_buf = bt_allocbuf(shared, SendPtr(rel)).await;
    let root_blk = shared.buffers().buffer_get_block_number(root_buf);
    let mut page = Page::boxed_zeroed();
    bt_pageinit(&mut page, BLCKSZ as usize);
    {
        let o = opaque_mut(&mut page);
        o.flags = BTP_LEAF | BTP_ROOT;
        o.level = 0;
        o.prev = P_NONE;
        o.next = P_NONE;
    }
    bt_write_page(shared, root_buf, &page, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
    bt_relbuf(shared, root_buf);

    update_meta_root(shared, rel, root_blk, 0).await;
}

/// Read-only view of a page's BTMetaPageData.
#[allow(clippy::cast_ptr_alignment, reason = "meta contents are MAXALIGN'd")]
fn read_meta_data(page: &Page) -> &BTMetaPageData {
    let off = maxalign(SizeOfPageHeaderData);
    // SAFETY: a meta page's contents begin with BTMetaPageData at the MAXALIGN'd offset.
    unsafe { &*page.as_bytes()[off..].as_ptr().cast::<BTMetaPageData>() }
}

/// Update the meta page's root/level/fastroot/fastlevel to `(root_blk, level)`.
async fn update_meta_root(shared: &Arc<SharedState>, rel: Relation, root_blk: BlockNumber, level: u32) {
    let metabuf = bt_read_buffer(shared, SendPtr(rel), BTREE_METAPAGE).await;
    let mut metapage = bt_read_page_copy(shared, metabuf);
    {
        let off = maxalign(SizeOfPageHeaderData);
        // SAFETY: meta contents begin with BTMetaPageData.
        #[allow(clippy::cast_ptr_alignment, reason = "meta contents MAXALIGN'd")]
        let metad = unsafe { &mut *metapage.as_mut_bytes()[off..].as_mut_ptr().cast::<BTMetaPageData>() };
        metad.root = root_blk;
        metad.level = level;
        metad.fastroot = root_blk;
        metad.fastlevel = level;
    }
    bt_write_page(shared, metabuf, &metapage, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
    bt_relbuf(shared, metabuf);
}

// ---------------------------------------------------------------------------
// WAL emission (full-page-image records; replay deferred to step 48).
// ---------------------------------------------------------------------------

async fn emit_insert_wal(
    shared: &Arc<SharedState>,
    rel: Relation,
    blk: BlockNumber,
    page: &Page,
    offnum: OffsetNumber,
    _isleaf: bool,
) -> XLogRecPtr {
    if !needs_wal(rel) {
        return crate::access::xlogdefs::INVALID_XLOG_REC_PTR;
    }
    let locator = unsafe { (*rel).rd_locator };
    let xlrec = xl_btree_insert { offnum };
    begin_insert();
    register_data(as_bytes(&xlrec, SizeOfBtreeInsert));
    register_block(0, &locator, ForkNumber::MAIN_FORKNUM, blk, page, crate::access::xloginsert::RegBuf::STANDARD);
    xlog_insert(shared.xlog(), RmgrId::Btree as u8, XLOG_BTREE_INSERT_LEAF).await
}

#[allow(clippy::too_many_arguments, reason = "mirrors the C XLOG_BTREE_SPLIT block")]
async fn emit_split_wal(
    shared: &Arc<SharedState>,
    rel: Relation,
    lblk: BlockNumber,
    left: &Page,
    rblk: BlockNumber,
    right: &Page,
    level: u32,
    newitemoff: OffsetNumber,
) -> XLogRecPtr {
    if !needs_wal(rel) {
        return crate::access::xlogdefs::INVALID_XLOG_REC_PTR;
    }
    let locator = unsafe { (*rel).rd_locator };
    let xlrec = xl_btree_split {
        level,
        firstrightoff: P_FIRSTKEY,
        newitemoff,
        postingoff: 0,
    };
    begin_insert();
    register_data(as_bytes(&xlrec, SizeOfBtreeSplit));
    register_block(0, &locator, ForkNumber::MAIN_FORKNUM, lblk, left, crate::access::xloginsert::RegBuf::WILL_INIT | crate::access::xloginsert::RegBuf::STANDARD);
    register_block(1, &locator, ForkNumber::MAIN_FORKNUM, rblk, right, crate::access::xloginsert::RegBuf::WILL_INIT | crate::access::xloginsert::RegBuf::STANDARD);
    xlog_insert(shared.xlog(), RmgrId::Btree as u8, XLOG_BTREE_SPLIT_L).await
}

async fn emit_newroot_wal(
    shared: &Arc<SharedState>,
    rel: Relation,
    root_blk: BlockNumber,
    root: &Page,
    _level: u32,
) -> XLogRecPtr {
    if !needs_wal(rel) {
        return crate::access::xlogdefs::INVALID_XLOG_REC_PTR;
    }
    let locator = unsafe { (*rel).rd_locator };
    begin_insert();
    register_block(0, &locator, ForkNumber::MAIN_FORKNUM, root_blk, root, crate::access::xloginsert::RegBuf::WILL_INIT | crate::access::xloginsert::RegBuf::STANDARD);
    xlog_insert(shared.xlog(), RmgrId::Btree as u8, XLOG_BTREE_NEWROOT).await
}

/// Whether the index needs WAL (a permanent rel with WAL on).
fn needs_wal(rel: Relation) -> bool {
    // SAFETY: live relation.
    unsafe { (*rel).needs_wal() }
}

/// Reinterpret a `#[repr(C)]` WAL fixed-part struct as its leading `size` bytes.
fn as_bytes<T>(v: &T, size: usize) -> &[u8] {
    // SAFETY: T is a #[repr(C)] POD; we read its leading `size` bytes.
    unsafe { core::slice::from_raw_parts(std::ptr::from_ref::<T>(v).cast::<u8>(), size) }
}
