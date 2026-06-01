//! Translation of postgres/src/backend/access/common/bufmask.c
//! (merged with access/bufmask.h: MASK_MARKER + the fn decls).
//!
//! WAL-consistency page masking.  Used by wal_consistency_checking to zero out
//! bytes of a page that legitimately differ between the time WAL is generated
//! and the time it is replayed (LSN, checksum, hint bits, unused free space,
//! line-pointer flags), so the two copies can be compared.
//!
//! #include "storage/block.h"  -> (via bufpage) crate::storage::block
//! #include "storage/bufmgr.h" -> only pulls in the Page/PageHeader page layer,
//!                                which lives in crate::storage::bufpage here.
//!   The page accessors (PageHeader, PageXLogRecPtrSet, PageClearFull,
//!   PageClearHasFreeLinePointers, PageClearAllVisible, PageGetMaxOffsetNumber,
//!   PageGetItemId, SizeOfPageHeaderData, the pd_* fields) come from bufpage;
//!   ItemId/ItemIdIsUsed/LP_UNUSED from storage::itemid; OffsetNumber helpers
//!   from storage::off.
//!
//! NOTE: the real C mask_page_hint_bits touches ONLY the PageHeader (pd_prune_xid,
//! PD_PAGE_FULL, PD_HAS_FREE_LINES, PD_ALL_VISIBLE).  It does NOT reach into heap
//! tuple infomask bits, so htup_details is not needed here.
//!
//! Portions Copyright (c) 2016-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use crate::c::uint16;
use crate::storage::bufpage::{
    Page, PageClearAllVisible, PageClearFull, PageClearHasFreeLinePointers,
    PageGetItemId, PageGetMaxOffsetNumber, PageHeader, PageXLogRecPtrSet,
    SizeOfPageHeaderData, XLogRecPtr,
};
use crate::storage::itemid::{ItemIdIsUsed, LP_UNUSED};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::pg_config::BLCKSZ;
use core::ffi::{c_int, c_void};
use core::mem::size_of;

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/* Marker used to mask pages consistently (bufmask.h). */
pub const MASK_MARKER: c_int = 0;

/*
 * mask_page_lsn_and_checksum
 *
 * In consistency checks, the LSN of the two pages compared will likely be
 * different because of concurrent operations when the WAL is generated and
 * the state of the page when WAL is applied. Also, mask out checksum as
 * masking anything else on page means checksum is not going to match as well.
 */
pub unsafe fn mask_page_lsn_and_checksum(page: Page) {
    let phdr = page as PageHeader;

    PageXLogRecPtrSet(&mut (*phdr).pd_lsn, MASK_MARKER as XLogRecPtr);
    (*phdr).pd_checksum = MASK_MARKER as uint16;
}

/*
 * mask_page_hint_bits
 *
 * Mask hint bits in PageHeader. We want to ignore differences in hint bits,
 * since they can be set without emitting any WAL.
 */
pub unsafe fn mask_page_hint_bits(page: Page) {
    let phdr = page as PageHeader;

    /* Ignore prune_xid (it's like a hint-bit) */
    (*phdr).pd_prune_xid = MASK_MARKER as TransactionId;

    /* Ignore PD_PAGE_FULL and PD_HAS_FREE_LINES flags, they are just hints. */
    PageClearFull(page);
    PageClearHasFreeLinePointers(page);

    /*
     * During replay, if the page LSN has advanced past our XLOG record's LSN,
     * we don't mark the page all-visible. See heap_xlog_visible() for
     * details.
     */
    PageClearAllVisible(page);
}

/*
 * mask_unused_space
 *
 * Mask the unused space of a page between pd_lower and pd_upper.
 */
pub unsafe fn mask_unused_space(page: Page) {
    let pd_lower = (*(page as PageHeader)).pd_lower as c_int;
    let pd_upper = (*(page as PageHeader)).pd_upper as c_int;
    let pd_special = (*(page as PageHeader)).pd_special as c_int;

    /* Sanity check */
    if pd_lower > pd_upper
        || pd_special < pd_upper
        || (pd_lower as usize) < SizeOfPageHeaderData
        || (pd_special as usize) > BLCKSZ
    {
        elog!(
            ERROR,
            "invalid page pd_lower {} pd_upper {} pd_special {}",
            pd_lower,
            pd_upper,
            pd_special
        );
    }

    memset(
        page.add(pd_lower as usize) as *mut c_void,
        MASK_MARKER,
        (pd_upper - pd_lower) as usize,
    );
}

/*
 * mask_lp_flags
 *
 * In some index AMs, line pointer flags can be modified on the primary
 * without emitting any WAL record.
 */
pub unsafe fn mask_lp_flags(page: Page) {
    let maxoff: OffsetNumber = PageGetMaxOffsetNumber(page);
    let mut offnum: OffsetNumber = FirstOffsetNumber;
    while offnum <= maxoff {
        let item_id = PageGetItemId(page, offnum);

        if ItemIdIsUsed(item_id) {
            /* itemId->lp_flags = LP_UNUSED;  -- clear just the flags field */
            set_lp_flags(item_id, LP_UNUSED);
        }
        offnum = OffsetNumberNext(offnum);
    }
}

/*
 * mask_page_content
 *
 * In some index AMs, the contents of deleted pages need to be almost
 * completely ignored.
 */
pub unsafe fn mask_page_content(page: Page) {
    /* Mask Page Content */
    memset(
        page.add(SizeOfPageHeaderData) as *mut c_void,
        MASK_MARKER,
        BLCKSZ - SizeOfPageHeaderData,
    );

    /* Mask pd_lower and pd_upper */
    let phdr = page as PageHeader;
    memset(
        &mut (*phdr).pd_lower as *mut _ as *mut c_void,
        MASK_MARKER,
        size_of::<uint16>(),
    );
    memset(
        &mut (*phdr).pd_upper as *mut _ as *mut c_void,
        MASK_MARKER,
        size_of::<uint16>(),
    );
}

/*
 * Helper for mask_lp_flags: the C code does `itemId->lp_flags = LP_UNUSED;`,
 * writing only the 2-bit lp_flags field of the ItemIdData bitfield while
 * preserving lp_off/lp_len.  itemid.rs hides the bit layout behind accessors
 * but exposes none that set only the flags, so reproduce the bit math here
 * (lp_off:15 | lp_flags:2 | lp_len:15, little-endian first-field-low-bits).
 */
#[inline]
unsafe fn set_lp_flags(item_id: crate::storage::itemid::ItemId, flags: u32) {
    let bits = &mut *(item_id as *mut u32);
    *bits = (*bits & !(0x3u32 << 15)) | ((flags & 0x3) << 15);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bufpage::PageInit;
    use crate::storage::itemid::{ItemId, ItemIdData, ItemIdSetNormal, ItemIdGetOffset, ItemIdGetLength};
    use core::mem::MaybeUninit;

    /// Allocate a BLCKSZ-aligned page buffer.
    fn fresh_page() -> Vec<u8> {
        vec![0u8; BLCKSZ]
    }

    #[test]
    fn lsn_and_checksum_zeroed() {
        let mut buf = fresh_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            PageInit(page, BLCKSZ, 0);
            let phdr = page as PageHeader;
            (*phdr).pd_lsn.xlogid = 0xDEAD_BEEF;
            (*phdr).pd_lsn.xrecoff = 0x0BAD_F00D;
            (*phdr).pd_checksum = 0xABCD;

            mask_page_lsn_and_checksum(page);

            assert_eq!((*phdr).pd_lsn.xlogid, 0);
            assert_eq!((*phdr).pd_lsn.xrecoff, 0);
            assert_eq!((*phdr).pd_checksum, MASK_MARKER as uint16);
        }
    }

    #[test]
    fn unused_space_filled_with_marker() {
        let mut buf = fresh_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            PageInit(page, BLCKSZ, 0);
            let phdr = page as PageHeader;
            let lower = (*phdr).pd_lower as usize;
            let upper = (*phdr).pd_upper as usize;

            /* dirty the free space so we can see it get masked */
            for i in lower..upper {
                *(page.add(i) as *mut u8) = 0x5A;
            }

            mask_unused_space(page);

            for i in lower..upper {
                assert_eq!(*(page.add(i) as *const u8), MASK_MARKER as u8);
            }
        }
    }

    #[test]
    fn hint_bits_cleared() {
        let mut buf = fresh_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            PageInit(page, BLCKSZ, 0);
            let phdr = page as PageHeader;
            (*phdr).pd_prune_xid = 12345;
            crate::storage::bufpage::PageSetFull(page);
            crate::storage::bufpage::PageSetHasFreeLinePointers(page);
            crate::storage::bufpage::PageSetAllVisible(page);

            mask_page_hint_bits(page);

            assert_eq!((*phdr).pd_prune_xid, MASK_MARKER as TransactionId);
            assert!(!crate::storage::bufpage::PageIsFull(page));
            assert!(!crate::storage::bufpage::PageHasFreeLinePointers(page));
            assert!(!crate::storage::bufpage::PageIsAllVisible(page));
        }
    }

    #[test]
    fn lp_flags_cleared_but_off_len_preserved() {
        let mut id: MaybeUninit<ItemIdData> = MaybeUninit::zeroed();
        let p: ItemId = id.as_mut_ptr() as ItemId;
        unsafe {
            ItemIdSetNormal(p, 1234, 56);
            set_lp_flags(p, LP_UNUSED);
            assert!(!ItemIdIsUsed(p));
            /* off + len untouched */
            assert_eq!(ItemIdGetOffset(p), 1234);
            assert_eq!(ItemIdGetLength(p), 56);
        }
    }

    #[test]
    fn page_content_masks_body_and_lower_upper() {
        let mut buf = fresh_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            PageInit(page, BLCKSZ, 0);
            let phdr = page as PageHeader;
            /* dirty a byte past the header */
            *page.add(SizeOfPageHeaderData) = 0x7F;

            mask_page_content(page);

            assert_eq!(*(page.add(SizeOfPageHeaderData) as *const u8), MASK_MARKER as u8);
            assert_eq!((*phdr).pd_lower, MASK_MARKER as uint16);
            assert_eq!((*phdr).pd_upper, MASK_MARKER as uint16);
        }
    }
}
