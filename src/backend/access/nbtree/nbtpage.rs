//! BTree-specific page management code for the Postgres btree access method.
//! Translated from src/backend/access/nbtree/nbtpage.c.
//!
//! This file covers the page-format mechanics that are foundation-independent
//! and therefore fully real + tested here: `_bt_pageinit` (initialize a btree
//! page with the BTPageOpaqueData special area), `_bt_initmetapage` (lay out the
//! meta page), the meta-page reader (`bt_getmeta`/`_bt_metaversion` over an
//! already-read page), and a few small opaque-area helpers.
//!
//! The buffer-pool / relcache-bound entry points (`_bt_getbuf`, `_bt_allocbuf`,
//! `_bt_relbuf`, `_bt_getroot`, `_bt_lockbuf`, page deletion, the FSM-pending
//! machinery, ...) descend the tree through pinned/locked buffers keyed by a
//! `Relation` relcache entry. The relcache (step 12) and the heap AM are not yet
//! translated, so those remain the header `unimplemented!()` stubs (rules.md s4)
//! until the relation-open + buffer-by-relation path exists; this module wires
//! the page-level half that the higher levels build on.

use std::sync::Arc;

use crate::access::nbtree::{
    BTMetaPageData, BTPageGetOpaque, BTPageOpaqueData, BTREE_MAGIC, BTREE_METAPAGE,
    BTREE_NOVAC_VERSION, BTREE_VERSION, BTP_LEAF, BTP_META, P_ISMETA, P_NONE,
};
use crate::common::relpath::ForkNumber;
use crate::pg_config::BLCKSZ;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::{ReadBufferMode, P_NEW};
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};
use crate::utils::rel::RelationData;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// PG `_bt_pageinit`: initialize a btree page to have the standard special-space
/// area (a [`BTPageOpaqueData`]) and otherwise empty contents.
pub fn bt_pageinit(page: &mut Page, size: usize) {
    page.init(size, core::mem::size_of::<BTPageOpaqueData>());
}

/// Mutable view of a page's [`BTPageOpaqueData`] special area.
///
/// The header's `BTPageGetOpaque` only yields a `*mut` from a `&Page`; writing
/// through that on a shared page is unsound, so the writer paths take `&mut Page`
/// and go through this helper. `Page` is `#[repr(C, align(8))]`, so the cast to
/// `*mut BTPageOpaqueData` (16-byte struct, 4-byte alignment) is well-aligned.
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Page is align(8); the special area starts at a MAXALIGN offset, so the \
              BTPageOpaqueData overlay is well-aligned (matches bufpage.rs)"
)]
fn bt_opaque_mut(page: &mut Page) -> &mut BTPageOpaqueData {
    let special = page.get_special_size() as usize;
    let off = BLCKSZ as usize - special;
    let p = page.as_mut_bytes()[off..].as_mut_ptr().cast::<BTPageOpaqueData>();
    // SAFETY: `off` is the page's special-area offset (PageInit set pd_special to
    // BLCKSZ - MAXALIGN(special)); the area is exactly sized for BTPageOpaqueData.
    unsafe { &mut *p }
}

/// Mutable view of a page's [`BTMetaPageData`] (lives in the page contents, just
/// past the page header).
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Page is align(8); page contents start at MAXALIGN(SizeOfPageHeaderData), \
              so the BTMetaPageData overlay is well-aligned"
)]
fn bt_meta_mut(page: &mut Page) -> &mut BTMetaPageData {
    let off = maxalign(SizeOfPageHeaderData);
    let p = page.as_mut_bytes()[off..].as_mut_ptr().cast::<BTMetaPageData>();
    // SAFETY: contents begin at MAXALIGN(SizeOfPageHeaderData); a fresh meta page
    // reserves room for BTMetaPageData there (asserted by the layout sizes).
    unsafe { &mut *p }
}

/// PG `_bt_initmetapage`: fill in a freshly-allocated page as the btree meta page.
/// `rootbknum`/`level` give the initial root location and tree height (P_NONE/0
/// for a brand-new empty index).
pub fn bt_initmetapage(page: &mut Page, rootbknum: u32, level: u32, allequalimage: bool) {
    bt_pageinit(page, BLCKSZ as usize);

    {
        let metad = bt_meta_mut(page);
        metad.magic = BTREE_MAGIC;
        metad.version = BTREE_VERSION;
        metad.root = rootbknum;
        metad.level = level;
        metad.fastroot = rootbknum;
        metad.fastlevel = level;
        metad.last_cleanup_num_delpages = 0;
        metad.last_cleanup_num_heap_tuples = -1.0;
        metad.allequalimage = allequalimage;
    }

    bt_opaque_mut(page).flags = BTP_META;

    // Set pd_lower just past the end of the metadata. Essential so the metadata
    // is not lost if xlog.c compresses the page.
    let new_lower = (maxalign(SizeOfPageHeaderData) + core::mem::size_of::<BTMetaPageData>()) as u16;
    page.header_mut().lower = new_lower;
}

/// Read-only view of a page's [`BTMetaPageData`].
///
/// SAFETY note: like the header accessors, this reinterprets the page contents;
/// the caller must pass an actual btree meta page.
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Page is align(8); contents are MAXALIGN'd so the overlay is well-aligned"
)]
pub fn bt_meta(page: &Page) -> &BTMetaPageData {
    let off = maxalign(SizeOfPageHeaderData);
    let p = page.get_contents().as_ptr().cast::<BTMetaPageData>();
    debug_assert_eq!(off, maxalign(SizeOfPageHeaderData));
    // SAFETY: contents begin at the MAXALIGN'd offset; caller passes a meta page.
    unsafe { &*p }
}

/// PG `_bt_getmeta`: extract the cached metadata fields from a meta page buffer.
/// Here it takes the already-read meta [`Page`] (the buffer-read half is the
/// caller's, once the buffer-by-relation path exists).
pub fn bt_getmeta(page: &Page) -> &BTMetaPageData {
    let opaque = BTPageGetOpaque(page);
    // SAFETY: opaque points into the page's special area.
    let opaque = unsafe { &*opaque };
    debug_assert!(P_ISMETA(opaque));
    debug_assert_eq!(bt_meta(page).magic, BTREE_MAGIC);
    bt_meta(page)
}

/// PG `_bt_metaversion` core: given a meta page, return `(heapkeyspace,
/// allequalimage)`. heapkeyspace is true for version >= BTREE_NOVAC_VERSION's
/// successor (v4); allequalimage comes straight from the meta page (only valid
/// for >= BTREE_NOVAC_VERSION).
pub fn bt_metaversion_from_page(page: &Page) -> (bool, bool) {
    let metad = bt_getmeta(page);

    // An empty/uninitialized root means the index was just built; treat as the
    // current version (heapkeyspace true, allequalimage from the page).
    if metad.root == crate::access::nbtree::P_NONE {
        return (metad.version >= BTREE_VERSION, metad.allequalimage);
    }

    let heapkeyspace = metad.version >= BTREE_VERSION;
    let allequalimage = if metad.version >= BTREE_NOVAC_VERSION {
        metad.allequalimage
    } else {
        false
    };
    (heapkeyspace, allequalimage)
}

/// True iff the meta page block number is the conventional first block.
pub const fn bt_is_metapage_block(blkno: u32) -> bool {
    blkno == BTREE_METAPAGE
}

/// Initialize a fresh leaf page that is also the root (the first data page of a
/// one-page index). PG does this inline in `_bt_getroot`/`btbuild`; factored out
/// here as a tested page-level helper.
pub fn bt_init_root_leaf(page: &mut Page) {
    bt_pageinit(page, BLCKSZ as usize);
    let opaque = bt_opaque_mut(page);
    opaque.flags = BTP_LEAF | crate::access::nbtree::BTP_ROOT;
    opaque.level = 0;
    opaque.prev = crate::access::nbtree::P_NONE;
    opaque.next = crate::access::nbtree::P_NONE;
}

// ===========================================================================
// Relation-bound buffer access (the buffer-by-relation path).
//
// Async-lock discipline (rules.md s5/s8): the foundation buffer pool exposes a
// page either read-only (`buffer_get_page`, `&Page`, under a content lock) or
// mutably (`block_mut`, only while the exclusive content lock guard is held,
// with NO `.await` in that window). The btree descends across pages; we cannot
// hold a `parking_lot` content-lock guard across the `.await` of the next page
// read. So the relation-bound helpers below COPY a page out under a brief lock
// into an owned `Box<Page>` ([`bt_read_page_copy`]), drop the lock, and the
// caller decides/descends on the copy; a writer re-takes the exclusive lock and
// copies its finished page image back ([`bt_write_page`]) in one lock window.
// This preserves the L&Y "decide under the page lock, then move on" semantics
// without ever holding a sync guard across an await.
// ===========================================================================

/// The global buffer-pool slot index for a pinned shared buffer.
fn buf_id_of(buffer: Buffer) -> i32 {
    #[allow(clippy::expect_used, reason = "btree index pages are always shared (global) buffers")]
    let id = buffer.as_global().expect("shared buffer expected") as i32;
    id
}

/// Pull the smgr handle + relpersistence out of an index relation.
fn index_smgr(relation: &RelationData) -> (*mut crate::storage::smgr::SmgrRelation, i8) {
    let relpersistence = relation.form().relpersistence;
    (relation.smgr(), relpersistence)
}

/// `ReadBuffer(rel, blkno)` for an index: pin `blkno` of the index's main fork
/// (async; no lock taken -- the caller locks via the copy/write helpers).
pub async fn bt_read_buffer(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    blkno: BlockNumber,
) -> Buffer {
    let (smgr_ptr, relpersistence) = index_smgr(relation);
    // SAFETY: relcache-owned smgr handle, valid while the rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    crate::backend::storage::buffer::bufmgr::read_buffer_common(
        shared,
        smgr,
        relpersistence,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::NORMAL,
        None,
    )
    .await
}

/// Read an owned COPY of a pinned buffer's page under a brief share lock. The
/// pin is kept (the caller releases it). Decoupling the page bytes from the lock
/// is what lets the descent await the next read without holding a sync guard.
pub fn bt_read_page_copy(shared: &Arc<SharedState>, buffer: Buffer) -> Box<Page> {
    let pool = shared.buffers();
    let _g = pool.content_share(buffer);
    let page = pool.buffer_get_page(buffer);
    let mut copy = Page::boxed_zeroed();
    copy.as_mut_bytes().copy_from_slice(page.as_bytes());
    copy
}

/// Copy a finished page image back into a pinned buffer under the exclusive
/// content lock (one lock window, no await), mark it dirty, and (if `recptr` is
/// given) set the page LSN. The caller WAL-logs first, then calls this.
pub fn bt_write_page(shared: &Arc<SharedState>, buffer: Buffer, src: &Page, recptr: crate::access::xlogdefs::XLogRecPtr) {
    let pool = shared.buffers();
    let _g = pool.content_exclusive(buffer);
    let buf_id = buf_id_of(buffer);
    // SAFETY: exclusive content lock held -> sole writer to this slot.
    let page = unsafe { pool.block_mut(buf_id) };
    page.as_mut_bytes().copy_from_slice(src.as_bytes());
    if recptr != crate::access::xlogdefs::INVALID_XLOG_REC_PTR {
        page.set_lsn(recptr);
    }
    pool.mark_buffer_dirty(buffer);
}

/// `_bt_relbuf`: drop the pin on an index buffer (the content lock, if any, was
/// released when the copy/write helper's guard dropped).
pub fn bt_relbuf(shared: &Arc<SharedState>, buffer: Buffer) {
    shared.buffers().release_buffer(buffer);
}

/// `_bt_allocbuf` (M2 core): obtain a fresh page for the index by extending the
/// relation by one block. FSM recycling of deleted pages is deferred (M-vacuum);
/// here we always extend, which is correct (just not space-optimal). Returns the
/// pinned buffer for the new, zero-initialized, btree-pageinit'd page (NOT
/// locked; the caller fills it via [`bt_write_page`]).
pub async fn bt_allocbuf(shared: &Arc<SharedState>, relation: &RelationData) -> Buffer {
    let buffer = {
        let (smgr_ptr, relpersistence) = index_smgr(relation);
        // SAFETY: relcache-owned smgr handle valid while rel open.
        let smgr = unsafe { &mut *smgr_ptr };
        crate::backend::storage::buffer::bufmgr::read_buffer_common(
            shared,
            smgr,
            relpersistence,
            ForkNumber::MAIN_FORKNUM,
            P_NEW,
            ReadBufferMode::ZERO_AND_LOCK,
            None,
        )
        .await
    };
    // Initialize as an empty btree page under the exclusive lock.
    {
        let pool = shared.buffers();
        let _g = pool.content_exclusive(buffer);
        let buf_id = buf_id_of(buffer);
        // SAFETY: exclusive content lock held -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        bt_pageinit(page, BLCKSZ as usize);
        pool.mark_buffer_dirty(buffer);
    }
    buffer
}

/// `_bt_getroot` (read path, M2): return an owned copy of the current root page
/// plus its block number, or `None` if the index has no root yet (empty index,
/// read access). Reads the meta page, follows `btm_fastroot`. The write path
/// (create the root on first insert) lives in nbtinsert via the metapage update.
pub async fn bt_getroot_read(
    shared: &Arc<SharedState>,
    relation: &RelationData,
) -> Option<(BlockNumber, Box<Page>)> {
    let metabuf = bt_read_buffer(shared, relation, BTREE_METAPAGE).await;
    let metapage = bt_read_page_copy(shared, metabuf);
    bt_relbuf(shared, metabuf);
    let metad = bt_getmeta(&metapage);
    if metad.root == P_NONE {
        return None;
    }
    let rootblk = metad.fastroot;
    let rootbuf = bt_read_buffer(shared, relation, rootblk).await;
    let rootpage = bt_read_page_copy(shared, rootbuf);
    bt_relbuf(shared, rootbuf);
    Some((rootblk, rootpage))
}

/// `_bt_metaversion`: `(heapkeyspace, allequalimage)` from the index's meta page.
pub async fn bt_metaversion(shared: &Arc<SharedState>, relation: &RelationData) -> (bool, bool) {
    let metabuf = bt_read_buffer(shared, relation, BTREE_METAPAGE).await;
    let metapage = bt_read_page_copy(shared, metabuf);
    bt_relbuf(shared, metabuf);
    bt_metaversion_from_page(&metapage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::nbtree::{P_ISLEAF, P_ISROOT};

    #[test]
    fn pageinit_sets_btree_special() {
        let mut page = Page::boxed_zeroed();
        bt_pageinit(&mut page, BLCKSZ as usize);
        assert_eq!(
            page.get_special_size() as usize,
            maxalign(core::mem::size_of::<BTPageOpaqueData>())
        );
        assert_eq!(page.get_max_offset_number(), 0);
    }

    #[test]
    fn initmetapage_roundtrip() {
        let mut page = Page::boxed_zeroed();
        bt_initmetapage(&mut page, P_NONE, 0, true);

        let meta = bt_getmeta(&page);
        assert_eq!(meta.magic, BTREE_MAGIC);
        assert_eq!(meta.version, BTREE_VERSION);
        assert_eq!(meta.root, P_NONE);
        assert_eq!(meta.level, 0);
        assert!(meta.allequalimage);

        // pd_lower advanced past the metadata so xlog compression won't lose it.
        let expected_lower =
            maxalign(SizeOfPageHeaderData) + core::mem::size_of::<BTMetaPageData>();
        assert_eq!(page.header().lower as usize, expected_lower);
    }

    #[test]
    fn metaversion_from_fresh_meta() {
        let mut page = Page::boxed_zeroed();
        bt_initmetapage(&mut page, P_NONE, 0, true);
        let (heapkeyspace, allequalimage) = bt_metaversion_from_page(&page);
        assert!(heapkeyspace);
        assert!(allequalimage);
    }

    #[test]
    fn root_leaf_flags() {
        let mut page = Page::boxed_zeroed();
        bt_init_root_leaf(&mut page);
        let opaque = BTPageGetOpaque(&page);
        let opaque = unsafe { &*opaque };
        assert!(P_ISLEAF(opaque));
        assert!(P_ISROOT(opaque));
        assert_eq!(opaque.level, 0);
    }
}
