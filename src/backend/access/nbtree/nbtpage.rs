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

use crate::access::nbtree::{
    BTMetaPageData, BTPageGetOpaque, BTPageOpaqueData, BTREE_MAGIC, BTREE_METAPAGE,
    BTREE_NOVAC_VERSION, BTREE_VERSION, BTP_LEAF, BTP_META, P_ISMETA,
};
use crate::pg_config::BLCKSZ;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::nbtree::{P_ISLEAF, P_ISROOT, P_NONE};

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
