//! access/brin_page.h - Prototypes and definitions for BRIN page layouts.

use std::ffi::c_int;
use std::mem::offset_of;

use crate::c::{uint16, uint32, MAXALIGN};
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, PageGetSpecialPointer, SizeOfPageHeaderData};
use crate::storage::itemptr::ItemPointerData;

/*
 * Special area of BRIN pages.
 *
 * We define it in this odd way so that it always occupies the last
 * MAXALIGN-sized element of each page.
 *
 * MAXALIGN(1) / sizeof(uint16) == 8 / 2 == 4 on a MAXIMUM_ALIGNOF==8 platform.
 */
#[repr(C)]
pub struct BrinSpecialSpace {
    pub vector: [uint16; MAXALIGN(1) / size_of::<uint16>()],
}

/*
 * Make the page type be the last half-word in the page, for consumption by
 * pg_filedump and similar utilities.  We don't really care much about the
 * position of the "flags" half-word, but it's simpler to apply a consistent
 * rule to both.
 *
 * See comments above GinPageOpaqueData.
 */
#[inline]
pub unsafe fn BrinPageType(page: Page) -> uint16 {
    let sp = PageGetSpecialPointer(page) as *mut BrinSpecialSpace;
    (*sp).vector[MAXALIGN(1) / size_of::<uint16>() - 1]
}

#[inline]
pub unsafe fn BrinPageFlags(page: Page) -> uint16 {
    let sp = PageGetSpecialPointer(page) as *mut BrinSpecialSpace;
    (*sp).vector[MAXALIGN(1) / size_of::<uint16>() - 2]
}

/* special space on all BRIN pages stores a "type" identifier */
pub const BRIN_PAGETYPE_META: uint16 = 0xF091;
pub const BRIN_PAGETYPE_REVMAP: uint16 = 0xF092;
pub const BRIN_PAGETYPE_REGULAR: uint16 = 0xF093;

#[inline]
pub unsafe fn BRIN_IS_META_PAGE(page: Page) -> bool {
    BrinPageType(page) == BRIN_PAGETYPE_META
}

#[inline]
pub unsafe fn BRIN_IS_REVMAP_PAGE(page: Page) -> bool {
    BrinPageType(page) == BRIN_PAGETYPE_REVMAP
}

#[inline]
pub unsafe fn BRIN_IS_REGULAR_PAGE(page: Page) -> bool {
    BrinPageType(page) == BRIN_PAGETYPE_REGULAR
}

/* flags for BrinSpecialSpace */
pub const BRIN_EVACUATE_PAGE: uint16 = 1 << 0;

/* Metapage definitions */
#[repr(C)]
pub struct BrinMetaPageData {
    pub brinMagic: uint32,
    pub brinVersion: uint32,
    pub pagesPerRange: BlockNumber,
    pub lastRevmapPage: BlockNumber,
}

pub const BRIN_CURRENT_VERSION: uint32 = 1;
pub const BRIN_META_MAGIC: uint32 = 0xA8109CFA;

pub const BRIN_METAPAGE_BLKNO: BlockNumber = 0;

/* Definitions for revmap pages */
#[repr(C)]
pub struct RevmapContents {
    /*
     * This array will fill all available space on the page.  It should be
     * declared [FLEXIBLE_ARRAY_MEMBER], but for some reason you can't do that
     * in an otherwise-empty struct.
     */
    pub rm_tids: [ItemPointerData; 1],
}

pub const REVMAP_CONTENT_SIZE: usize = BLCKSZ
    - MAXALIGN(SizeOfPageHeaderData)
    - offset_of!(RevmapContents, rm_tids)
    - MAXALIGN(size_of::<BrinSpecialSpace>());

/* max num of items in the array */
pub const REVMAP_PAGE_MAXITEMS: usize = REVMAP_CONTENT_SIZE / size_of::<ItemPointerData>();
