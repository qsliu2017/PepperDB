//! Bottom-up btree index build. Translated from the M2-reachable core of
//! `src/backend/access/nbtree/nbtsort.c`.
//!
//! `btbuild` (in nbtree.rs) drives this: scan the heap, sort the index tuples,
//! then [`bt_load`] packs them bottom-up into fully-formed leaf and internal
//! pages. This is the path `index_build` uses for both catalog and user indexes.
//!
//! M2 scope: the single-spool sorted build -- no spool2/dead-tuple spool, no
//! parallel build, no deduplication, and no suffix truncation (the high key keeps
//! the full separator key, which is correct, just not space-optimal). Pages are
//! written through the foundation buffer pool: blocks are extended in order
//! (block 0 = meta, data at 1..) and the finished meta page rewrites block 0.
//!
//! The build mirrors nbtsort.c's [`BtPageState`] per-level chain: items are added
//! to the current leaf page; when it fills, its last item is promoted to the high
//! key, the page is written, and a downlink pivot is added to the parent level
//! (created on demand, adding a tree level). `bt_uppershutdown` finishes every
//! level's rightmost page and writes the meta page.
//!
//! Async coloring (rules.md s5): writing each page reads/extends the buffer pool,
//! so [`bt_load`] and its helpers are `async`. No content lock is held across an
//! `.await`: each page is built in an owned `Box<Page>` and copied into its buffer
//! in one lock window.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: holds per-backend raw Relation handles task-confined for the operation; futures never migrate the pointee between tasks"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "btree build takes raw Relation/IndexTuple pointers per the C API; faithful to C"
)]

use std::sync::Arc;

use crate::access::itup::IndexTupleData;
use crate::access::nbtree::{
    BTreeTupleSetDownLink, BTreeTupleSetNAtts, BTP_LEAF, BTP_ROOT, BTPageOpaqueData, P_FIRSTKEY,
    P_HIKEY, P_NONE,
};
use crate::backend::access::common::indextuple::index_form_tuple;
use crate::backend::access::nbtree::nbtpage::{
    bt_allocbuf, bt_initmetapage, bt_pageinit, bt_read_buffer, bt_relbuf, bt_write_page,
};
use crate::backend::access::heap::heapam::SendPtr;
use crate::common::relpath::ForkNumber;
use crate::pg_config::BLCKSZ;
use crate::utils::rel::RelationData;

/// A `Send` index-relation handle for build helpers that hold it across `.await`.
type SendRelation = SendPtr<RelationData>;
use crate::postgres::Datum;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};
use crate::storage::itemid::ItemIdData;
use crate::storage::off::OffsetNumber;
use crate::utils::relcache::Relation;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// Per-level page-building state (nbtsort.c `BTPageState`).
struct BtPageState {
    page: Box<Page>,
    blkno: BlockNumber,
    /// The page's low key (separator inherited from the previous page's high key);
    /// becomes the downlink pivot when this page links into its parent. `None` for
    /// the leftmost page of a level (its downlink is minus-infinity).
    lowkey: Option<Vec<u8>>,
    lastoff: OffsetNumber,
    level: u32,
    full: usize,
    next: Option<Box<Self>>,
}

/// Build write-state: the index relation + the running block allocator (block 0 is
/// reserved for the meta page, data starts at block 1) + the index-tuple count.
struct BtWriteState {
    index: Relation,
    pages_alloced: BlockNumber,
    index_tuples: f64,
}

// SAFETY: the raw Relation handle is task-confined for the build's lifetime
// (the build runs in one task; the pointee is not shared concurrently).
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "deliberate: the raw Relation pointer is task-confined for the build's lifetime"
)]
unsafe impl Send for BtWriteState {}
// SAFETY: BtPageState's only !Send content is the page (Box<Page>, Send) and the
// owned key Vecs (Send); no raw pointers. The marker is defensive for the boxed
// future's Send bound.
unsafe impl Send for BtPageState {}

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

/// `_bt_blnewpage`: a fresh in-memory page for `level`, with the P_HIKEY line
/// pointer pre-allocated (pd_lower bumped past it).
fn bt_blnewpage(level: u32) -> Box<Page> {
    let mut page = Page::boxed_zeroed();
    bt_pageinit(&mut page, BLCKSZ as usize);
    {
        let opaque = opaque_mut(&mut page);
        opaque.prev = P_NONE;
        opaque.next = P_NONE;
        opaque.level = level;
        opaque.flags = if level > 0 { 0 } else { BTP_LEAF };
        opaque.cycleid = 0;
    }
    // Make the P_HIKEY line pointer appear allocated.
    let lower = page.header().lower + core::mem::size_of::<ItemIdData>() as u16;
    page.header_mut().lower = lower;
    page
}

/// `_bt_sortaddtup`: add `itup_bytes` at `itup_off`. If `newfirstdataitem` (the
/// first data item of a non-leaf page, the minus-infinity downlink), the tuple is
/// truncated to a 0-attribute pivot (keeping its downlink TID).
fn bt_sortaddtup(page: &mut Page, itup_bytes: &[u8], itup_off: OffsetNumber, newfirstdataitem: bool) {
    if newfirstdataitem {
        let mut trunc = vec![0u8; core::mem::size_of::<IndexTupleData>()];
        let n = itup_bytes.len().min(trunc.len());
        trunc[..n].copy_from_slice(&itup_bytes[..n]);
        // SAFETY: trunc is a sized IndexTupleData image (a Vec is well-aligned).
        #[allow(clippy::cast_ptr_alignment, reason = "Vec aligned for IndexTupleData (align 2)")]
        let t = trunc.as_mut_ptr().cast::<IndexTupleData>();
        // SAFETY: t points at the IndexTupleData we just sized.
        unsafe {
            (*t).t_info = core::mem::size_of::<IndexTupleData>() as u16;
            BTreeTupleSetNAtts(&mut *t, 0, false);
        }
        let off = page.add_item(&trunc, trunc.len(), itup_off, false, false);
        crate::assert!(off != 0, "failed to add minus-infinity item to index page");
        return;
    }
    let off = page.add_item(itup_bytes, itup_bytes.len(), itup_off, false, false);
    crate::assert!(off != 0, "failed to add item to the index page");
}

/// `_bt_pagestate`: a fresh page-building state for `level` (allocating its block).
fn bt_pagestate(wstate: &mut BtWriteState, level: u32) -> BtPageState {
    let blkno = wstate.pages_alloced;
    wstate.pages_alloced += 1;
    let fillfactor = if level > 0 { 70 } else { 90 };
    let full = (BLCKSZ as usize) * (100 - fillfactor) / 100;
    BtPageState {
        page: bt_blnewpage(level),
        blkno,
        lowkey: None,
        lastoff: P_HIKEY,
        level,
        full,
        next: None,
    }
}

/// Bytes of the item at `off` on `page`.
fn page_item_bytes(page: &Page, off: OffsetNumber) -> Vec<u8> {
    let id = page.get_item_id(off);
    page.get_item(&id).to_vec()
}

/// Copy `key_bytes` (an index tuple image) and set its downlink TID to `child`.
fn make_downlink_pivot(key_bytes: &[u8], child: BlockNumber) -> Vec<u8> {
    let mut v = key_bytes.to_vec();
    // SAFETY: v holds a valid IndexTupleData image copied from a page item.
    #[allow(clippy::cast_ptr_alignment, reason = "Vec aligned for IndexTupleData")]
    let t = v.as_mut_ptr().cast::<IndexTupleData>();
    // SAFETY: t points at the copied IndexTupleData.
    unsafe { BTreeTupleSetDownLink(&mut *t, child) };
    v
}

/// `_bt_buildadd`: add `itup_bytes` to the page-building `state` at its level,
/// finishing + linking the page into its parent (creating it if needed) when full.
/// Recursive across levels via a boxed future.
fn bt_buildadd<'a>(
    shared: &'a Arc<SharedState>,
    wstate: &'a mut BtWriteState,
    state: &'a mut BtPageState,
    itup_bytes: Vec<u8>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let isleaf = state.level == 0;
        let itupsz = maxalign(itup_bytes.len());
        let pgspc = state.page.get_free_space();
        let need_tid = if isleaf { maxalign(6) } else { 0 };
        let must_split = pgspc < itupsz + need_tid
            || (pgspc < state.full && state.lastoff > P_FIRSTKEY);

        if must_split {
            crate::assert!(state.lastoff > P_FIRSTKEY, "build page too small to split");

            // The last item becomes the new page's first data item AND the old
            // page's high key.
            let last_off = state.lastoff;
            let last_bytes = page_item_bytes(&state.page, last_off);

            // New same-level page; move 'last' onto it.
            let new_blkno = wstate.pages_alloced;
            wstate.pages_alloced += 1;
            let oblkno = state.blkno;
            let mut npage = bt_blnewpage(state.level);
            bt_sortaddtup(&mut npage, &last_bytes, P_FIRSTKEY, !isleaf);
            // Link the new page back to the old (sibling chain).
            opaque_mut(&mut npage).prev = oblkno;

            // Move 'last' into P_HIKEY on the old page and reclaim its data slot,
            // and link the old page forward to the new one.
            {
                let last_id = state.page.get_item_id(last_off);
                set_item_id(&mut state.page, P_HIKEY, last_id);
                let lower =
                    state.page.header().lower - core::mem::size_of::<ItemIdData>() as u16;
                state.page.header_mut().lower = lower;
                opaque_mut(&mut state.page).next = new_blkno;
            }

            let hikey_bytes = page_item_bytes(&state.page, P_HIKEY);

            // Write the finished old page.
            let opage_img = std::mem::replace(&mut state.page, npage);
            write_build_page(shared, SendPtr(wstate.index), oblkno, &opage_img).await;

            // Link the old page into its parent via its low key (downlink). The
            // leftmost page's downlink is minus-infinity: bt_buildadd truncates the
            // first data item of a non-leaf page automatically (see below), so pass
            // the full pivot and let the parent's first-item rule truncate it.
            if state.next.is_none() {
                let parent = bt_pagestate(wstate, state.level + 1);
                state.next = Some(Box::new(parent));
            }
            let downlink = state.lowkey.as_ref().map_or_else(
                || make_downlink_pivot(&hikey_bytes, oblkno),
                |lk| make_downlink_pivot(lk, oblkno),
            );
            {
                let next = state.next.as_mut().unwrap_or_else(|| unreachable!());
                bt_buildadd(shared, wstate, next, downlink).await;
            }

            // The old high key is the new page's low key.
            state.lowkey = Some(hikey_bytes);
            state.blkno = new_blkno;
            state.lastoff = P_FIRSTKEY;
            // Fall through to add the incoming item below.
        }

        // Add the incoming item. The very first data item of a non-leaf page is the
        // minus-infinity downlink (truncated to 0 attrs); detect it (lastoff still
        // at the reserved high-key slot and this is an internal level).
        let off = state.lastoff + 1;
        let first_data = state.lastoff == P_HIKEY;
        bt_sortaddtup(&mut state.page, &itup_bytes, off, !isleaf && first_data);
        state.lastoff = off;
        if isleaf {
            wstate.index_tuples += 1.0;
        }
    })
}

/// `_bt_slideleft`: on a rightmost page (no high key), slide the ItemId array back
/// one slot so the reserved P_HIKEY slot is reclaimed.
fn bt_slideleft(page: &mut Page) {
    let maxoff = page.get_max_offset_number();
    if maxoff >= P_FIRSTKEY {
        for off in P_FIRSTKEY..=maxoff {
            let id = page.get_item_id(off);
            set_item_id(page, off - 1, id);
        }
    }
    let lower = page
        .header()
        .lower
        .saturating_sub(core::mem::size_of::<ItemIdData>() as u16);
    page.header_mut().lower = lower;
}

/// Overwrite the line pointer at `off` (1-based) with `id`.
fn set_item_id(page: &mut Page, off: OffsetNumber, id: ItemIdData) {
    let idx = (off - 1) as usize;
    let base = SizeOfPageHeaderData + idx * core::mem::size_of::<ItemIdData>();
    let bytes = id_to_bytes(id);
    page.as_mut_bytes()[base..base + bytes.len()].copy_from_slice(&bytes);
}

/// Raw bytes of an ItemIdData (a 4-byte `#[repr(C)]` bitfield POD).
fn id_to_bytes(id: ItemIdData) -> [u8; 4] {
    // SAFETY: ItemIdData is exactly 4 POD bytes.
    unsafe { core::mem::transmute_copy::<ItemIdData, [u8; 4]>(&id) }
}

/// Copy a finished build page into its (pre-extended) block.
async fn write_build_page(shared: &Arc<SharedState>, index: SendRelation, blkno: BlockNumber, page: &Page) {
    reserve_block(shared, index, blkno).await;
    let buf = bt_read_buffer(shared, index, blkno).await;
    bt_write_page(shared, buf, page, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
    bt_relbuf(shared, buf);
}

/// Ensure the index has a block at `blkno` by extending with zeroed pages.
async fn reserve_block(shared: &Arc<SharedState>, index: SendRelation, blkno: BlockNumber) {
    loop {
        let nblocks = {
            // SAFETY: live index relation.
            let rel = unsafe { &mut *index.0 };
            // SAFETY: relcache-owned smgr handle, valid while the rel is open; the
            // SmgrRelation is Send so this borrow may cross the `.await`.
            let smgr = unsafe { &mut *rel.smgr() };
            smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await
        };
        if nblocks > blkno {
            return;
        }
        let buf = bt_allocbuf(shared, index).await;
        bt_relbuf(shared, buf);
    }
}

/// `_bt_uppershutdown`: finish every level's rightmost page (linking to its parent)
/// then write the meta page pointing at the root.
async fn bt_uppershutdown(
    shared: &Arc<SharedState>,
    wstate: &mut BtWriteState,
    mut state: Box<BtPageState>,
    allequalimage: bool,
) {
    let mut rootblkno = P_NONE;
    let mut rootlevel = 0u32;

    loop {
        let blkno = state.blkno;
        let is_root = state.next.is_none();

        if is_root {
            opaque_mut(&mut state.page).flags |= BTP_ROOT;
            rootblkno = blkno;
            rootlevel = state.level;
        } else {
            let downlink = state.lowkey.as_ref().map_or_else(
                || make_downlink_pivot(&page_item_bytes(&state.page, P_HIKEY), blkno),
                |lk| make_downlink_pivot(lk, blkno),
            );
            let next = state.next.as_mut().unwrap_or_else(|| unreachable!());
            bt_buildadd(shared, wstate, next, downlink).await;
        }

        bt_slideleft(&mut state.page);
        let page_img = std::mem::replace(&mut state.page, Page::boxed_zeroed());
        write_build_page(shared, SendPtr(wstate.index), blkno, &page_img).await;

        match state.next.take() {
            Some(parent) => state = parent,
            None => break,
        }
    }

    let mut metapage = Page::boxed_zeroed();
    bt_initmetapage(&mut metapage, rootblkno, rootlevel, allequalimage);
    write_build_page(shared, SendPtr(wstate.index), crate::access::nbtree::BTREE_METAPAGE, &metapage).await;
}

/// One sorted index tuple for the build: key datums + null flags + heap TID.
pub struct BuildTuple {
    pub values: Vec<Datum>,
    pub isnull: Vec<bool>,
    pub heap_tid: crate::storage::itemptr::ItemPointerData,
}

/// `_bt_load`: pack the SORTED index tuples bottom-up into the tree. Returns the
/// number of index tuples written. `sorted` must be in index order (the build
/// caller sorts before calling).
pub async fn bt_load(
    shared: &Arc<SharedState>,
    index: Relation,
    sorted: &[BuildTuple],
    allequalimage: bool,
) -> f64 {
    let mut wstate = BtWriteState { index, pages_alloced: 1, index_tuples: 0.0 };

    // SAFETY: live index relation with a tuple descriptor.
    let itupdesc = unsafe { (*index).rd_att.clone() }
        .unwrap_or_else(|| unreachable!("index relation has a tuple descriptor"));

    let mut leaf_state: Option<Box<BtPageState>> = None;

    for bt in sorted {
        let itup = index_form_tuple(&itupdesc, &bt.values, &bt.isnull);
        // SAFETY: itup is a freshly formed valid index tuple.
        unsafe {
            (*itup).tid = bt.heap_tid;
        }
        let size = unsafe { (*itup).size() };
        let bytes = unsafe { core::slice::from_raw_parts(itup.cast::<u8>(), size) }.to_vec();
        // SAFETY: itup came from index_form_tuple.
        unsafe { crate::backend::access::common::indextuple::pfree_index_tuple(itup) };

        if leaf_state.is_none() {
            leaf_state = Some(Box::new(bt_pagestate(&mut wstate, 0)));
        }
        let st = leaf_state.as_mut().unwrap_or_else(|| unreachable!());
        bt_buildadd(shared, &mut wstate, st, bytes).await;
    }

    let index_tuples = wstate.index_tuples;

    if let Some(st) = leaf_state { bt_uppershutdown(shared, &mut wstate, st, allequalimage).await } else {
        // Empty index: a meta page with no root.
        let mut metapage = Page::boxed_zeroed();
        bt_initmetapage(&mut metapage, P_NONE, 0, allequalimage);
        write_build_page(shared, SendPtr(index), crate::access::nbtree::BTREE_METAPAGE, &metapage).await;
    }

    index_tuples
}
