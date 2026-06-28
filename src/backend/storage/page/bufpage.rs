//! Standard buffer-page support routines. Translated from backend/storage/page/bufpage.c.
//!
//! Implements PostgreSQL's slotted-page format: a fixed page header followed by a
//! growing line-pointer (item id) array at the low end and tuple data filling in from
//! the high end, with an optional special space reserved at the very top for access
//! methods. The principal entry points initialize a page (`init`), validate one freshly
//! read from disk (`is_verified`), add and remove items (`add_item`,
//! `add_item_extended`, the `index_tuple_*` and `index_multi_delete` deletions),
//! reclaim free space (`repair_fragmentation`, `truncate_line_pointer_array`), report
//! available space (`get_free_space`, `get_exact_free_space`, `get_heap_free_space`),
//! produce scratch copies (`get_temp_page*`, `restore_temp_page`), and compute or stamp
//! the page checksum (`set_checksum_copy`, `set_checksum_inplace`).
//!
//! The page layout, MAXALIGN rounding, item placement, and fragmentation repair are
//! byte-for-byte identical to PostgreSQL so that an on-disk data directory remains
//! readable. Operations are realized as methods on the `#[repr(C, align(8))]` page
//! block, with the fixed header overlaid on its leading bytes; there are no layout
//! changes.
//!
//! These routines are pure synchronous operations on an in-memory page buffer: nothing
//! here awaits, acquires a lock, or performs I/O. The caller is responsible for holding
//! the buffer's content lock. Detected corruption, which PostgreSQL reports via
//! ereport(ERROR) or ereport(PANIC), is signalled here as a panic.

use crate::access::htup_details::MaxHeapTuplesPerPage;
use crate::access::itup::MaxIndexTuplesPerPage;
use crate::access::xlog::data_checksums_enabled;
use crate::pg_config::{BLCKSZ, MAXIMUM_ALIGNOF};
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{
    Page, PageAddItemFlags, PageIsVerifiedFlags, PG_PAGE_LAYOUT_VERSION, SizeOfPageHeaderData,
};
use crate::storage::item::Item;
use crate::storage::itemid::ItemIdData;
use crate::storage::off::{
    offset_number_next, offset_number_is_valid, OffsetNumber, FIRST_OFFSET_NUMBER,
    INVALID_OFFSET_NUMBER,
};
use crate::utils::elog::{ERROR, PANIC, WARNING};
use crate::utils::errcodes::ERRCODE_DATA_CORRUPTED;
use crate::{ereport, elog};

const fn maxalign(n: usize) -> usize {
    (n + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

const SIZE_OF_ITEM_ID: usize = core::mem::size_of::<ItemIdData>();

/// PD_VALID_FLAG_BITS (= OR of all valid PageFlags). Kept local; the header's
/// bitflags type exposes it via VALID_FLAG_BITS.
const PageFlagsValidBits: u16 = 0x0007;

// === raw line-pointer access (crate-internal helpers) ===
//
// The line-pointer array is accessed slot-by-slot at or beyond the current item
// count (e.g. appending at `limit`), so we cast individual ItemIdData slots, the
// same pointer-cast the header accessors use. 1-based offset like C's
// PageGetItemId. SAFETY: callers stay within the page buffer (>= BLCKSZ) and the
// line-pointer region [SizeOfPageHeaderData, pd_lower).

impl Page {
    #[inline]
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "page buffer is MAXALIGN/8-byte aligned by construction and SizeOfPageHeaderData is a multiple of 4, so the ItemIdData (4-byte) base is aligned"
    )]
    fn set_item_id_raw(&mut self, offset_number: OffsetNumber, value: ItemIdData) {
        let idx = (offset_number - 1) as usize;
        let base = unsafe { self.as_mut_bytes().as_mut_ptr().add(SizeOfPageHeaderData).cast::<ItemIdData>() };
        unsafe { *base.add(idx) = value };
    }

    /// memmove within the page (overlapping-safe), byte offsets into the buffer.
    #[inline]
    fn page_memmove(&mut self, dst: usize, src: usize, len: usize) {
        if len == 0 {
            return;
        }
        let p = self.as_mut_bytes().as_mut_ptr();
        unsafe { core::ptr::copy(p.add(src), p.add(dst), len) };
    }
}

impl Page {
    /// PageInit
    ///     Initializes the contents of a page.
    ///     Note: no initial checksum is calculated here; that's done at write time.
    pub fn init(&mut self, page_size: usize, special_size: usize) {
        let special_size = maxalign(special_size);

        debug_assert_eq!(page_size, BLCKSZ as usize);
        debug_assert!(page_size > special_size + SizeOfPageHeaderData);

        // Make sure all fields of page are zero, as well as unused space.
        self.as_mut_bytes()[..page_size].fill(0);

        let p = self.header_mut();
        p.flags = 0;
        p.lower = SizeOfPageHeaderData as u16;
        p.upper = (page_size - special_size) as u16;
        p.special = (page_size - special_size) as u16;
        self.set_page_size_and_version(page_size, PG_PAGE_LAYOUT_VERSION as u8);
        // prune_xid = InvalidTransactionId already done by the zero-fill above.
    }

    /// PageIsVerified
    ///     Check that the page header and checksum (if any) appear valid.
    ///
    /// Called when a page has just been read in from disk: a cheap sanity check
    /// before following line pointers. Zeroed pages are allowed (a crash after
    /// extending a relation can leave one). Returns (verified, checksum_failure).
    ///
    /// Note: `>= ERROR` is never raised here; only WARNING/LOG (per the C contract).
    pub fn is_verified(&self, blkno: BlockNumber, flags: PageIsVerifiedFlags) -> (bool, bool) {
        let mut checksum_failure = false;
        let mut header_sane = false;
        let mut checksum: u16 = 0;

        // Don't verify page data unless the page passes the basic non-zero test.
        if !self.is_new() {
            let p = self.header();

            if data_checksums_enabled() {
                checksum = self.checksum(blkno);
                if checksum != p.checksum {
                    checksum_failure = true;
                }
            }

            // These checks only show the header looks sane enough for the pool.
            if (p.flags & !PageFlagsValidBits) == 0
                && p.lower <= p.upper
                && p.upper <= p.special
                && u32::from(p.special) <= BLCKSZ
                && p.special as usize == maxalign(p.special as usize)
            {
                header_sane = true;
            }

            if header_sane && !checksum_failure {
                return (true, checksum_failure);
            }
        }

        // Check all-zeroes case.
        if self.as_bytes()[..BLCKSZ as usize].iter().all(|&b| b == 0) {
            return (true, checksum_failure);
        }

        // Throw a WARNING/LOG (never >= ERROR), but only after the all-zeroes check.
        if checksum_failure {
            if flags.intersects(PageIsVerifiedFlags::LOG_WARNING | PageIsVerifiedFlags::LOG_LOG) {
                let lvl = if flags.contains(PageIsVerifiedFlags::LOG_WARNING) {
                    WARNING
                } else {
                    crate::utils::elog::LOG
                };
                let expected = self.header().checksum;
                ereport!(lvl, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
                        "page verification failed, calculated checksum {checksum} but expected {expected}"
                    ));
                });
            }

            if header_sane && flags.contains(PageIsVerifiedFlags::IGNORE_CHECKSUM_FAILURE) {
                return (true, checksum_failure);
            }
        }

        (false, checksum_failure)
    }

    /// PageAddItemExtended
    ///
    /// Add an item to a page. Returns the offset it was inserted at, or
    /// InvalidOffsetNumber if it was not inserted (a WARNING explains why).
    ///
    /// offset_number is either InvalidOffsetNumber (find a free line pointer) or a
    /// value in [FirstOffsetNumber, max+1]. With OVERWRITE the item is stored at the
    /// given (unused or one-past-end) slot; without it, existing items shuffle later.
    /// IS_HEAP caps the line pointers at MaxHeapTuplesPerPage.
    ///
    /// !!! ereport(ERROR) IS DISALLOWED HERE (except PANIC on corruption) !!!
    pub fn add_item_extended(
        &mut self,
        item: Item,
        size: usize,
        mut offset_number: OffsetNumber,
        flags: PageAddItemFlags,
    ) -> OffsetNumber {
        // Be wary about corrupted page pointers.
        {
            let phdr = self.header();
            if (phdr.lower as usize) < SizeOfPageHeaderData
                || phdr.lower > phdr.upper
                || phdr.upper > phdr.special
                || u32::from(phdr.special) > BLCKSZ
            {
                corrupted_page_pointers_panic(phdr.lower, phdr.upper, phdr.special);
            }
        }

        let mut needshuffle = false;

        // Select offsetNumber to place the new item at.
        let limit = offset_number_next(self.get_max_offset_number());

        if offset_number_is_valid(offset_number) {
            // offsetNumber was passed in; check it.
            if flags.contains(PageAddItemFlags::OVERWRITE) {
                if offset_number < limit {
                    let item_id = self.get_item_id(offset_number);
                    if item_id.is_used() || item_id.has_storage() {
                        elog!(WARNING, "will not overwrite a used ItemId");
                        return INVALID_OFFSET_NUMBER;
                    }
                }
            } else if offset_number < limit {
                needshuffle = true; // need to move existing linp's
            }
        } else {
            // offsetNumber not passed in, so find a free slot; else put it at limit.
            if self.has_free_line_pointers() {
                // Scan the line pointer array for a recyclable (unused) ItemId.
                // Always use earlier items first.
                offset_number = FIRST_OFFSET_NUMBER;
                while offset_number < limit {
                    let item_id = self.get_item_id(offset_number);
                    debug_assert!(item_id.is_used() || !item_id.has_storage());
                    if !item_id.is_used() && !item_id.has_storage() {
                        break;
                    }
                    offset_number += 1;
                }
                if offset_number >= limit {
                    // The hint is wrong, so reset it.
                    self.clear_has_free_line_pointers();
                }
            } else {
                // Don't bother searching if the hint says there's no free slot.
                offset_number = limit;
            }
        }

        // Reject placing items beyond the first unused line pointer.
        if offset_number > limit {
            elog!(WARNING, "specified item offset is too large");
            return INVALID_OFFSET_NUMBER;
        }

        // Reject placing items beyond the heap boundary, if heap.
        if flags.contains(PageAddItemFlags::IS_HEAP)
            && i32::from(offset_number) > MaxHeapTuplesPerPage
        {
            elog!(
                WARNING,
                "can't put more than MaxHeapTuplesPerPage items in a heap page"
            );
            return INVALID_OFFSET_NUMBER;
        }

        // Compute new lower and upper pointers; signed to avoid wraparound mistakes.
        let lower: i32 = if offset_number == limit || needshuffle {
            i32::from(self.header().lower) + SIZE_OF_ITEM_ID as i32
        } else {
            i32::from(self.header().lower)
        };

        let aligned_size = maxalign(size);
        let upper = i32::from(self.header().upper) - aligned_size as i32;

        if lower > upper {
            return INVALID_OFFSET_NUMBER;
        }

        // OK to insert. First, shuffle the existing pointers if needed.
        if needshuffle {
            // memmove the line-pointer array entries [offset_number, limit) up one.
            let base = (offset_number - 1) as usize;
            let n = (limit - offset_number) as usize;
            let dst = SizeOfPageHeaderData + (base + 1) * SIZE_OF_ITEM_ID;
            let src = SizeOfPageHeaderData + base * SIZE_OF_ITEM_ID;
            self.page_memmove(dst, src, n * SIZE_OF_ITEM_ID);
        }

        // Set the line pointer.
        let mut item_id = ItemIdData(0);
        item_id.set_normal(upper as u16, size as u16);
        self.set_item_id_raw(offset_number, item_id);

        // Copy the item's data onto the page.
        self.as_mut_bytes()[upper as usize..upper as usize + size].copy_from_slice(item);

        // Adjust the page header.
        let phdr = self.header_mut();
        phdr.lower = lower as u16;
        phdr.upper = upper as u16;

        offset_number
    }

    /// PageAddItem -- the common-case wrapper (no OVERWRITE).
    pub fn add_item(
        &mut self,
        item: Item,
        size: usize,
        offset_number: OffsetNumber,
        overwrite: bool,
        is_heap: bool,
    ) -> OffsetNumber {
        let mut flags = PageAddItemFlags::empty();
        if overwrite {
            flags |= PageAddItemFlags::OVERWRITE;
        }
        if is_heap {
            flags |= PageAddItemFlags::IS_HEAP;
        }
        self.add_item_extended(item, size, offset_number, flags)
    }

    /// PageGetTempPage
    ///     Get a temporary page in local memory. Not initialized; caller does that.
    pub fn get_temp_page(&self) -> Box<Self> {
        Self::boxed_zeroed()
    }

    /// PageGetTempPageCopy
    ///     Temp page initialized by copying the contents of the given page.
    pub fn get_temp_page_copy(&self) -> Box<Self> {
        let page_size = self.get_page_size();
        let mut temp = Self::boxed_zeroed();
        temp.as_mut_bytes()[..page_size].copy_from_slice(&self.as_bytes()[..page_size]);
        temp
    }

    /// PageGetTempPageCopySpecial
    ///     Temp page PageInit'd with the same special-space size, special copied over.
    pub fn get_temp_page_copy_special(&self) -> Box<Self> {
        let page_size = self.get_page_size();
        let special_size = self.get_special_size() as usize;
        let mut temp = Self::boxed_zeroed();
        temp.init(page_size, special_size);
        // Copy the special space (the trailing special_size bytes).
        let src_special = self.header().special as usize;
        let dst_special = temp.header().special as usize;
        temp.as_mut_bytes()[dst_special..dst_special + special_size]
            .copy_from_slice(&self.as_bytes()[src_special..src_special + special_size]);
        temp
    }

    /// PageRestoreTempPage
    ///     Copy a temporary page back over a permanent page after special processing.
    ///     (C also pfree's tempPage; here the Box is dropped by the caller.)
    pub fn restore_temp_page(&mut self, temp_page: &Self) {
        let page_size = temp_page.get_page_size();
        self.as_mut_bytes()[..page_size].copy_from_slice(&temp_page.as_bytes()[..page_size]);
    }

    /// PageRepairFragmentation
    ///
    /// Frees fragmented space on a heap page following pruning. Heap pages only (see
    /// PageIndexMultiDelete). Removes unused line pointers from the end of the array
    /// and sets/clears the PD_HAS_FREE_LINES hint. Caller had better have a full
    /// cleanup lock on the page's buffer.
    pub fn repair_fragmentation(&mut self) {
        let pd_lower = i32::from(self.header().lower);
        let pd_upper = i32::from(self.header().upper);
        let pd_special = i32::from(self.header().special);

        // Be more paranoid than usual: we're about to reshuffle a shared buffer.
        if pd_lower < SizeOfPageHeaderData as i32
            || pd_lower > pd_upper
            || pd_upper > pd_special
            || pd_special > BLCKSZ as i32
            || pd_special != maxalign(pd_special as usize) as i32
        {
            corrupted_page_pointers_panic_error(
                pd_lower as u16,
                pd_upper as u16,
                pd_special as u16,
            );
        }

        // Run through the line pointer array and collect data about live items.
        let nline = self.get_max_offset_number();
        let mut itemidbase = [ItemIdCompact::default(); MaxHeapTuplesPerPage as usize];
        let mut nstorage = 0usize;
        let mut nunused = 0i32;
        let mut totallen: usize = 0;
        let mut last_offset = pd_special;
        let mut presorted = true;
        let mut finalusedlp = INVALID_OFFSET_NUMBER;

        let mut i = FIRST_OFFSET_NUMBER;
        while i <= nline {
            let lp = self.get_item_id(i);
            if lp.is_used() {
                if lp.has_storage() {
                    let itemoff = i32::from(lp.lp_off());
                    if last_offset > itemoff {
                        last_offset = itemoff;
                    } else {
                        presorted = false;
                    }

                    if itemoff < pd_upper || itemoff >= pd_special {
                        corrupted_line_pointer_panic(itemoff as u32);
                    }
                    let alignedlen = maxalign(lp.lp_len() as usize);
                    itemidbase[nstorage] = ItemIdCompact {
                        offsetindex: (i - 1),
                        itemoff: itemoff as i16,
                        alignedlen: alignedlen as u16,
                    };
                    totallen += alignedlen;
                    nstorage += 1;
                }
                finalusedlp = i; // Could be the final non-LP_UNUSED item.
            } else {
                // Unused entries should have lp_len = 0, but make sure.
                debug_assert!(!lp.has_storage());
                let mut u = lp;
                u.set_unused();
                self.set_item_id_raw(i, u);
                nunused += 1;
            }
            i += 1;
        }

        if nstorage == 0 {
            // Page completely empty: just reset it quickly.
            self.header_mut().upper = pd_special as u16;
        } else {
            // Need to compact the page the hard way.
            if totallen > (pd_special - pd_lower) as usize {
                corrupted_item_lengths_panic(totallen as u32, (pd_special - pd_lower) as u32);
            }
            self.compactify_tuples(&itemidbase, nstorage, presorted);
        }

        if finalusedlp != nline {
            // The last line pointer is not the last used line pointer.
            let nunusedend = i32::from(nline - finalusedlp);
            debug_assert!(nunused >= nunusedend && nunusedend > 0);
            nunused -= nunusedend;
            self.header_mut().lower -= (SIZE_OF_ITEM_ID as i32 * nunusedend) as u16;
        }

        // Set hint bit for PageAddItemExtended.
        if nunused > 0 {
            self.set_has_free_line_pointers();
        } else {
            self.clear_has_free_line_pointers();
        }
    }

    /// PageTruncateLinePointerArray
    ///
    /// Removes unused line pointers at the end of the line pointer array. Heap pages
    /// only; called by VACUUM's second pass. Avoids truncating to 0 items (leaves one
    /// LP_UNUSED behind if needed). Sets/clears PD_HAS_FREE_LINES accordingly.
    pub fn truncate_line_pointer_array(&mut self) {
        let mut countdone = false;
        let mut sethint = false;
        let mut nunusedend = 0i32;

        // Scan line pointer array back-to-front.
        let mut i = i32::from(self.get_max_offset_number());
        while i >= i32::from(FIRST_OFFSET_NUMBER) {
            let lp = self.get_item_id(i as OffsetNumber);

            if !countdone && i > i32::from(FIRST_OFFSET_NUMBER) {
                if lp.is_used() {
                    countdone = true;
                } else {
                    nunusedend += 1;
                }
            } else if !lp.is_used() {
                // An unused line pointer we won't truncate -- so there is at least one.
                sethint = true;
                break;
            }
            i -= 1;
        }

        if nunusedend > 0 {
            let freed = (SIZE_OF_ITEM_ID as i32 * nunusedend) as u16;
            let old_lower = self.header().lower;
            self.header_mut().lower -= freed;
            // CLOBBER_FREED_MEMORY: clobber the truncated line-pointer bytes (debug only).
            #[cfg(debug_assertions)]
            {
                let new_lower = self.header().lower as usize;
                self[new_lower..old_lower as usize].fill(0x7F);
            }
            #[cfg(not(debug_assertions))]
            let _ = old_lower;
        } else {
            debug_assert!(sethint);
        }

        if sethint {
            self.set_has_free_line_pointers();
        } else {
            self.clear_has_free_line_pointers();
        }
    }

    /// PageGetFreeSpace
    ///     Free (allocatable) space, reduced by one new line pointer.
    ///     Usually for index pages; use get_heap_free_space on heap pages.
    pub fn get_free_space(&self) -> usize {
        let phdr = self.header();
        // Signed so pd_lower > pd_upper behaves sensibly.
        let mut space = i32::from(phdr.upper) - i32::from(phdr.lower);
        if space < SIZE_OF_ITEM_ID as i32 {
            return 0;
        }
        space -= SIZE_OF_ITEM_ID as i32;
        space as usize
    }

    /// PageGetFreeSpaceForMultipleTuples
    ///     Free space reduced by `ntups` new line pointers.
    pub fn get_free_space_for_multiple_tuples(&self, ntups: i32) -> usize {
        let phdr = self.header();
        let mut space = i32::from(phdr.upper) - i32::from(phdr.lower);
        if space < ntups * SIZE_OF_ITEM_ID as i32 {
            return 0;
        }
        space -= ntups * SIZE_OF_ITEM_ID as i32;
        space as usize
    }

    /// PageGetExactFreeSpace
    ///     Free space with no consideration for adding/removing line pointers.
    pub fn get_exact_free_space(&self) -> usize {
        let phdr = self.header();
        let space = i32::from(phdr.upper) - i32::from(phdr.lower);
        if space < 0 {
            return 0;
        }
        space as usize
    }

    /// PageGetHeapFreeSpace
    ///     Like get_free_space, but returns 0 if there are already
    ///     MaxHeapTuplesPerPage line pointers and none are free (enforces the hard
    ///     cap on line pointers per heap page).
    pub fn get_heap_free_space(&self) -> usize {
        let mut space = self.get_free_space();
        if space > 0 {
            let nline = self.get_max_offset_number();
            if nline >= MaxHeapTuplesPerPage as OffsetNumber {
                if self.has_free_line_pointers() {
                    // Just a hint; confirm there is indeed a free line pointer.
                    let mut offnum = FIRST_OFFSET_NUMBER;
                    while offnum <= nline {
                        let lp = self.get_item_id(offnum);
                        if !lp.is_used() {
                            break;
                        }
                        offnum = offset_number_next(offnum);
                    }
                    if offnum > nline {
                        // The hint is wrong, but we can't clear it (can't dirty here).
                        space = 0;
                    }
                } else {
                    // The hint might be wrong, but PageAddItem believes it, so must we.
                    space = 0;
                }
            }
        }
        space
    }

    /// PageIndexTupleDelete
    ///
    /// Removes a tuple from an index page. Unlike heap pages, the line pointer for
    /// the removed tuple is compacted out.
    pub fn index_tuple_delete(&mut self, offnum: OffsetNumber) {
        self.paranoid_page_check();

        let nline = self.get_max_offset_number();
        if offnum == 0 || offnum > nline {
            invalid_index_offnum_panic(offnum);
        }

        let offidx = (offnum - 1) as usize;

        let tup = self.get_item_id(offnum);
        debug_assert!(tup.has_storage());
        let mut size = tup.lp_len() as usize;
        let offset = tup.lp_off() as usize;

        {
            let phdr = self.header();
            if offset < phdr.upper as usize
                || offset + size > phdr.special as usize
                || offset != maxalign(offset)
            {
                corrupted_line_pointer_offset_panic(offset as u32, size as u32);
            }
        }

        // Amount of space to actually be deleted.
        size = maxalign(size);

        // Get rid of the pd_linp entry for the tuple: copy subsequent linp's back one
        // slot in the array. Operates on the array, not individual linp's.
        let lower = self.header().lower as usize;
        let linp_start = SizeOfPageHeaderData;
        let nbytes = lower as i32 - (linp_start + (offidx + 1) * SIZE_OF_ITEM_ID) as i32;
        if nbytes > 0 {
            let dst = linp_start + offidx * SIZE_OF_ITEM_ID;
            let src = linp_start + (offidx + 1) * SIZE_OF_ITEM_ID;
            self.page_memmove(dst, src, nbytes as usize);
        }

        // Move tuple data between old upper bound and the deleted tuple forward, so
        // the freed space is left in the middle. No copy if the tuple was at upper.
        let pd_upper = self.header().upper as usize;
        if offset > pd_upper {
            self.page_memmove(pd_upper + size, pd_upper, offset - pd_upper);
        }

        // Adjust free space boundary pointers.
        {
            let phdr = self.header_mut();
            phdr.upper += size as u16;
            phdr.lower -= SIZE_OF_ITEM_ID as u16;
        }

        // Adjust the linp entries that remain: anything before the deleted tuple's
        // data was moved forward by the size of the deleted tuple.
        if !self.is_empty() {
            let nline = nline - 1; // one fewer than when we started
            let mut i = 1;
            while i <= nline {
                let mut ii = self.get_item_id(i);
                debug_assert!(ii.has_storage());
                if (ii.lp_off() as usize) <= offset {
                    ii.set_off(ii.lp_off() + size as u16);
                    self.set_item_id_raw(i, ii);
                }
                i += 1;
            }
        }
    }

    /// PageIndexMultiDelete
    ///
    /// Deletes multiple tuples from an index page at once (much faster than a loop of
    /// PageIndexTupleDelete). The caller MUST supply itemnos in item-number order.
    pub fn index_multi_delete(&mut self, itemnos: &[OffsetNumber]) {
        let nitems = itemnos.len();
        debug_assert!(nitems <= MaxIndexTuplesPerPage);

        // Few items => retail index_tuple_delete is best. Delete in reverse order so
        // we don't have to adjust item numbers for previous deletions.
        if nitems <= 2 {
            for &it in itemnos.iter().rev() {
                self.index_tuple_delete(it);
            }
            return;
        }

        let pd_lower = i32::from(self.header().lower);
        let pd_upper = i32::from(self.header().upper);
        let pd_special = i32::from(self.header().special);

        if pd_lower < SizeOfPageHeaderData as i32
            || pd_lower > pd_upper
            || pd_upper > pd_special
            || pd_special > BLCKSZ as i32
            || pd_special != maxalign(pd_special as usize) as i32
        {
            corrupted_page_pointers_panic_error(
                pd_lower as u16,
                pd_upper as u16,
                pd_special as u16,
            );
        }

        // Scan line pointers and build a list of the ones we keep. Don't modify the
        // page yet (still validity-checking).
        let nline = self.get_max_offset_number();
        let mut itemidbase = [ItemIdCompact::default(); MaxIndexTuplesPerPage];
        let mut newitemids = [ItemIdData(0); MaxIndexTuplesPerPage];
        let mut totallen: usize = 0;
        let mut nused = 0usize;
        let mut nextitm = 0usize;
        let mut last_offset = pd_special;
        let mut presorted = true;

        let mut offnum = FIRST_OFFSET_NUMBER;
        while offnum <= nline {
            let lp = self.get_item_id(offnum);
            debug_assert!(lp.has_storage());
            let size = lp.lp_len() as usize;
            let offset = i32::from(lp.lp_off());
            if offset < pd_upper
                || offset + size as i32 > pd_special
                || offset != maxalign(offset as usize) as i32
            {
                corrupted_line_pointer_offset_panic(offset as u32, size as u32);
            }

            if nextitm < nitems && offnum == itemnos[nextitm] {
                // skip item to be deleted
                nextitm += 1;
            } else {
                if last_offset > offset {
                    last_offset = offset;
                } else {
                    presorted = false;
                }
                itemidbase[nused] = ItemIdCompact {
                    offsetindex: nused as u16, // where it will go
                    itemoff: offset as i16,
                    alignedlen: maxalign(size) as u16,
                };
                totallen += maxalign(size);
                newitemids[nused] = lp;
                nused += 1;
            }
            offnum = offset_number_next(offnum);
        }

        // This catches invalid or out-of-order itemnos[].
        if nextitm != nitems {
            elog!(ERROR, "incorrect index offsets supplied");
        }

        if totallen > (pd_special - pd_lower) as usize {
            corrupted_item_lengths_panic(totallen as u32, (pd_special - pd_lower) as u32);
        }

        // Overwrite the line pointers with the copy (unused items removed).
        {
            #[allow(
                clippy::cast_ptr_alignment,
                reason = "page buffer is MAXALIGN/8-byte aligned by construction and SizeOfPageHeaderData is a multiple of 4, so the ItemIdData (4-byte) base is aligned"
            )]
            let base = unsafe {
                self.as_mut_bytes().as_mut_ptr().add(SizeOfPageHeaderData).cast::<ItemIdData>()
            };
            for (k, &v) in newitemids[..nused].iter().enumerate() {
                unsafe { *base.add(k) = v };
            }
        }
        self.header_mut().lower = (SizeOfPageHeaderData + nused * SIZE_OF_ITEM_ID) as u16;

        // And compactify the tuple data.
        if nused > 0 {
            self.compactify_tuples(&itemidbase, nused, presorted);
        } else {
            self.header_mut().upper = pd_special as u16;
        }
    }

    /// PageIndexTupleDeleteNoCompact
    ///
    /// Remove a tuple from an index page, but set its line pointer "unused" instead of
    /// compacting it out (except it's removed if it's the last line pointer). For
    /// index AMs that require existing live TIDs to remain unchanged.
    pub fn index_tuple_delete_no_compact(&mut self, offnum: OffsetNumber) {
        self.paranoid_page_check();

        let mut nline = self.get_max_offset_number();
        if offnum == 0 || offnum > nline {
            invalid_index_offnum_panic(offnum);
        }

        let tup = self.get_item_id(offnum);
        debug_assert!(tup.has_storage());
        let mut size = tup.lp_len() as usize;
        let offset = tup.lp_off() as usize;

        {
            let phdr = self.header();
            if offset < phdr.upper as usize
                || offset + size > phdr.special as usize
                || offset != maxalign(offset)
            {
                corrupted_line_pointer_offset_panic(offset as u32, size as u32);
            }
        }

        size = maxalign(size);

        // Either mark the line pointer "unused", or zap it if it's the last one.
        if offnum < nline {
            let mut t = tup;
            t.set_unused();
            self.set_item_id_raw(offnum, t);
        } else {
            self.header_mut().lower -= SIZE_OF_ITEM_ID as u16;
            nline -= 1;
        }

        // Move tuple data forward to leave the freed space in the middle.
        let pd_upper = self.header().upper as usize;
        if offset > pd_upper {
            self.page_memmove(pd_upper + size, pd_upper, offset - pd_upper);
        }

        self.header_mut().upper += size as u16;

        // Adjust the linp entries that remain.
        if !self.is_empty() {
            let mut i = 1;
            while i <= nline {
                let mut ii = self.get_item_id(i);
                if ii.has_storage() && (ii.lp_off() as usize) <= offset {
                    ii.set_off(ii.lp_off() + size as u16);
                    self.set_item_id_raw(i, ii);
                }
                i += 1;
            }
        }
    }

    /// PageIndexTupleOverwrite
    ///
    /// Replace a tuple on an index page in place, shifting other tuples' data up/down
    /// to keep the page compacted. Better than delete+reinsert: avoids data shifting
    /// when the size is unchanged, and never moves line pointers. Returns false if
    /// there's insufficient space; corruption is an elog (panic).
    pub fn index_tuple_overwrite(
        &mut self,
        offnum: OffsetNumber,
        newtup: Item,
        newsize: usize,
    ) -> bool {
        self.paranoid_page_check();

        let itemcount = self.get_max_offset_number();
        if offnum == 0 || offnum > itemcount {
            invalid_index_offnum_panic(offnum);
        }

        let tupid = self.get_item_id(offnum);
        debug_assert!(tupid.has_storage());
        let mut oldsize = i32::from(tupid.lp_len());
        let offset = tupid.lp_off() as usize;

        {
            let phdr = self.header();
            if offset < phdr.upper as usize
                || offset + oldsize as usize > phdr.special as usize
                || offset != maxalign(offset)
            {
                corrupted_line_pointer_offset_panic(offset as u32, oldsize as u32);
            }
        }

        // Actual change in space requirement; check for page overflow.
        oldsize = maxalign(oldsize as usize) as i32;
        let alignednewsize = maxalign(newsize) as i32;
        {
            let phdr = self.header();
            if alignednewsize > oldsize + (i32::from(phdr.upper) - i32::from(phdr.lower)) {
                return false;
            }
        }

        // Relocate existing data before the target tuple unless the size is unchanged.
        // size_diff is the amount the tuple's size is *decreasing*, i.e. the delta to
        // add to pd_upper and affected line pointers.
        let size_diff = oldsize - alignednewsize;
        if size_diff != 0 {
            let pd_upper = self.header().upper as usize;
            // Relocate all tuple data before the target tuple.
            let len = offset - pd_upper;
            // memmove(addr + size_diff, addr, len). size_diff may be negative.
            let dst = (pd_upper as i32 + size_diff) as usize;
            self.page_memmove(dst, pd_upper, len);

            self.header_mut().upper = (pd_upper as i32 + size_diff) as u16;

            // Adjust affected line pointers.
            let mut i = FIRST_OFFSET_NUMBER;
            while i <= itemcount {
                let mut ii = self.get_item_id(i);
                // Allow items without storage (currently only BRIN needs that).
                if ii.has_storage() && (ii.lp_off() as usize) <= offset {
                    ii.set_off((i32::from(ii.lp_off()) + size_diff) as u16);
                    self.set_item_id_raw(i, ii);
                }
                i += 1;
            }
        }

        // Update the item's tuple length without changing its lp_flags field.
        let mut newtupid = self.get_item_id(offnum);
        newtupid.set_off((offset as i32 + size_diff) as u16);
        newtupid.set_len(newsize as u16);
        self.set_item_id_raw(offnum, newtupid);

        // Copy new tuple data onto page.
        let dst = newtupid.lp_off() as usize;
        self.as_mut_bytes()[dst..dst + newsize].copy_from_slice(newtup);

        true
    }

    /// PageSetChecksumCopy
    ///
    /// Set the checksum for a page in shared buffers, working on a private copy so a
    /// concurrent modification (e.g. setting hint bits) can't invalidate the stored
    /// checksum. Returns the block-sized data to write (the copy), or a copy of the
    /// input unchanged if checksums are disabled / the page is new.
    ///
    /// (C returns a pointer into a reused static buffer; we return an owned Box.)
    pub fn set_checksum_copy(&self, blkno: BlockNumber) -> Box<Self> {
        let mut copy = Self::boxed_zeroed();
        copy.as_mut_bytes()[..BLCKSZ as usize].copy_from_slice(&self.as_bytes()[..BLCKSZ as usize]);
        if self.is_new() || !data_checksums_enabled() {
            return copy;
        }
        let cksum = copy.checksum(blkno);
        copy.header_mut().checksum = cksum;
        copy
    }

    /// PageSetChecksumInplace
    ///
    /// Set the checksum for a page in private memory. Only use when no other process
    /// can be modifying the page buffer.
    pub fn set_checksum_inplace(&mut self, blkno: BlockNumber) {
        if self.is_new() || !data_checksums_enabled() {
            return;
        }
        let cksum = self.checksum(blkno);
        self.header_mut().checksum = cksum;
    }

    /// Shared paranoia check used by the index delete/overwrite paths (the common
    /// "corrupted page pointers" ereport(ERROR)).
    #[inline]
    fn paranoid_page_check(&self) {
        let phdr = self.header();
        if (phdr.lower as usize) < SizeOfPageHeaderData
            || phdr.lower > phdr.upper
            || phdr.upper > phdr.special
            || u32::from(phdr.special) > BLCKSZ
            || phdr.special as usize != maxalign(phdr.special as usize)
        {
            corrupted_page_pointers_panic_error(phdr.lower, phdr.upper, phdr.special);
        }
    }

    /// compactify_tuples
    ///
    /// After removing/marking some line pointers unused, move the tuples to remove
    /// the gaps and reorder them into reverse line-pointer order.
    ///
    /// `presorted` => itemidbase is in descending itemoff order, so we can memmove
    /// tuples toward the end without overwriting unmoved tuples. Otherwise we stage
    /// the to-be-moved tuples in a scratch buffer first. Callers ensure nitems > 0.
    #[allow(
        clippy::too_many_lines,
        reason = "1:1 port of C compactify_tuples; splitting would diverge from PG structure"
    )]
    fn compactify_tuples(&mut self, itemidbase: &[ItemIdCompact], nitems: usize, presorted: bool) {
        debug_assert!(nitems > 0);

        let pd_special = i32::from(self.header().special);
        let pd_upper = i32::from(self.header().upper);

        let upper = if presorted {
            #[cfg(debug_assertions)]
            {
                // Verify no caller incorrectly passed a true presorted value.
                let mut lastoff = pd_special;
                for it in &itemidbase[..nitems] {
                    debug_assert!(lastoff > i32::from(it.itemoff));
                    lastoff = i32::from(it.itemoff);
                }
            }

            // itemidbase already optimal: lower item pointers have higher offset, so
            // we can memmove to the end without clobbering unmoved tuples. Skip over
            // any tuples already at the end of the page first. (C uses a do-while, so
            // it always examines at least base[0]; nitems > 0 guarantees the same.)
            let mut up = pd_special;
            let mut i = 0;
            let mut idx_ptr = 0; // mirrors C's `itemidptr` after the skip loop
            while i < nitems {
                idx_ptr = i;
                let it = &itemidbase[i];
                if up != i32::from(it.itemoff) + i32::from(it.alignedlen) {
                    break;
                }
                up -= i32::from(it.alignedlen);
                i += 1;
            }

            // Now compactify. Minimize memmove() calls: only move when there's a gap.
            let mut copy_tail =
                i32::from(itemidbase[idx_ptr].itemoff) + i32::from(itemidbase[idx_ptr].alignedlen);
            let mut copy_head = copy_tail;
            while i < nitems {
                let it = itemidbase[i];

                if copy_head != i32::from(it.itemoff) + i32::from(it.alignedlen) {
                    self.page_memmove(
                        up as usize,
                        copy_head as usize,
                        (copy_tail - copy_head) as usize,
                    );
                    copy_tail = i32::from(it.itemoff) + i32::from(it.alignedlen);
                }
                up -= i32::from(it.alignedlen);
                copy_head = i32::from(it.itemoff);

                // Update the line pointer to reference the new offset.
                let off = (it.offsetindex + 1) as OffsetNumber;
                let mut lp = self.get_item_id(off);
                lp.set_off(up as u16);
                self.set_item_id_raw(off, lp);

                i += 1;
            }

            // Move the remaining tuples.
            self.page_memmove(
                up as usize,
                copy_head as usize,
                (copy_tail - copy_head) as usize,
            );

            up
        } else {
            // Non-presorted: tuples may be in any order, so stage to-be-moved tuples
            // in a scratch buffer before copying them back at the new offsets.
            let mut scratch = vec![0u8; BLCKSZ as usize];
            let mut up: i32;
            let mut i: usize;
            let mut idx_ptr: usize; // mirrors C's `itemidptr` into the compaction loop

            let maxoff = self.get_max_offset_number() as usize;
            if nitems < maxoff / 4 {
                // >75% pruned: copy tuple-by-tuple into the temp buffer.
                for it in &itemidbase[..nitems] {
                    let off = it.itemoff as usize;
                    let len = it.alignedlen as usize;
                    scratch[off..off + len].copy_from_slice(&self.as_bytes()[off..off + len]);
                }
                i = 0;
                idx_ptr = 0;
                up = pd_special;
            } else {
                up = pd_special;
                // Many tuples are likely already in the correct location; skip
                // forward to the first one that needs moving. (C do-while; nitems > 0.)
                i = 0;
                idx_ptr = 0;
                while i < nitems {
                    idx_ptr = i;
                    let it = &itemidbase[i];
                    if up != i32::from(it.itemoff) + i32::from(it.alignedlen) {
                        break;
                    }
                    up -= i32::from(it.alignedlen);
                    i += 1;
                }
                // Copy all tuples that need to be moved into the temp buffer.
                let from = pd_upper as usize;
                let len = (up - pd_upper) as usize;
                scratch[from..from + len].copy_from_slice(&self.as_bytes()[from..from + len]);
            }

            // Do the compactification; idx_ptr points at the first tuple to move.
            let mut copy_tail =
                i32::from(itemidbase[idx_ptr].itemoff) + i32::from(itemidbase[idx_ptr].alignedlen);
            let mut copy_head = copy_tail;
            while i < nitems {
                let it = itemidbase[i];

                if copy_head != i32::from(it.itemoff) + i32::from(it.alignedlen) {
                    let dst = up as usize;
                    let src = copy_head as usize;
                    let len = (copy_tail - copy_head) as usize;
                    self.as_mut_bytes()[dst..dst + len].copy_from_slice(&scratch[src..src + len]);
                    copy_tail = i32::from(it.itemoff) + i32::from(it.alignedlen);
                }
                up -= i32::from(it.alignedlen);
                copy_head = i32::from(it.itemoff);

                let off = (it.offsetindex + 1) as OffsetNumber;
                let mut lp = self.get_item_id(off);
                lp.set_off(up as u16);
                self.set_item_id_raw(off, lp);

                i += 1;
            }

            // Copy the remaining chunk.
            let dst = up as usize;
            let src = copy_head as usize;
            let len = (copy_tail - copy_head) as usize;
            self.as_mut_bytes()[dst..dst + len].copy_from_slice(&scratch[src..src + len]);

            up
        };

        self.header_mut().upper = upper as u16;
    }
}

/// Tuple defrag support for PageRepairFragmentation and PageIndexMultiDelete.
#[derive(Clone, Copy, Default)]
struct ItemIdCompact {
    offsetindex: u16, // linp array index (0-based)
    itemoff: i16,     // page offset of item data
    alignedlen: u16,  // MAXALIGN(item data len)
}

// === corruption / elog (panic) helpers ===
//
// These mirror the C ereport(ERROR/PANIC) / elog(ERROR) corruption paths. Each is
// a divergent panic (never returns). TODO(panic): migrate to Result + ?.

#[cold]
fn corrupted_page_pointers_panic(lower: u16, upper: u16, special: u16) -> ! {
    // TODO(panic): C ereport(PANIC, ERRCODE_DATA_CORRUPTED).
    ereport!(PANIC, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
            "corrupted page pointers: lower = {lower}, upper = {upper}, special = {special}"
        ));
    });
    unreachable!()
}

#[cold]
fn corrupted_page_pointers_panic_error(lower: u16, upper: u16, special: u16) -> ! {
    // TODO(panic): C ereport(ERROR, ERRCODE_DATA_CORRUPTED).
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
            "corrupted page pointers: lower = {lower}, upper = {upper}, special = {special}"
        ));
    });
    unreachable!()
}

#[cold]
fn corrupted_line_pointer_panic(itemoff: u32) -> ! {
    // TODO(panic): C ereport(ERROR, ERRCODE_DATA_CORRUPTED).
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!("corrupted line pointer: {itemoff}"));
    });
    unreachable!()
}

#[cold]
fn corrupted_line_pointer_offset_panic(offset: u32, size: u32) -> ! {
    // TODO(panic): C ereport(ERROR, ERRCODE_DATA_CORRUPTED).
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
            "corrupted line pointer: offset = {offset}, size = {size}"
        ));
    });
    unreachable!()
}

#[cold]
fn corrupted_item_lengths_panic(total: u32, available: u32) -> ! {
    // TODO(panic): C ereport(ERROR, ERRCODE_DATA_CORRUPTED).
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
            "corrupted item lengths: total {total}, available space {available}"
        ));
    });
    unreachable!()
}

#[cold]
fn invalid_index_offnum_panic(offnum: OffsetNumber) -> ! {
    // TODO(panic): C elog(ERROR).
    elog!(ERROR, format!("invalid index offnum: {}", offnum));
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: usize = BLCKSZ as usize;

    fn new_page() -> Box<Page> {
        let mut p = Page::boxed_zeroed();
        p.init(PAGE_SIZE, 0);
        p
    }

    fn add(page: &mut Page, data: &[u8]) -> OffsetNumber {
        page.add_item(data, data.len(), INVALID_OFFSET_NUMBER, false, false)
    }

    #[test]
    fn page_size_and_align() {
        assert_eq!(core::mem::size_of::<Page>(), 8192);
        assert!(core::mem::align_of::<Page>() >= 4);
    }

    #[test]
    fn init_sets_header() {
        let p = new_page();
        let h = p.header();
        assert_eq!(h.lower as usize, SizeOfPageHeaderData);
        assert_eq!(u32::from(h.upper), BLCKSZ);
        assert_eq!(u32::from(h.special), BLCKSZ);
        assert!(p.is_empty());
        assert!(!p.is_new());
        assert_eq!(p.get_max_offset_number(), 0);
    }

    #[test]
    fn add_and_read_items() {
        let mut p = new_page();
        let items: [&[u8]; 3] = [b"hello", b"world!!", b"third-item"];
        let mut offs = vec![];
        for it in items {
            let off = add(&mut p, it);
            assert_ne!(off, INVALID_OFFSET_NUMBER);
            offs.push(off);
        }
        assert_eq!(offs, vec![1, 2, 3]);
        assert_eq!(p.get_max_offset_number(), 3);

        for (i, it) in items.iter().enumerate() {
            let id = p.get_item_id(offs[i]);
            assert_eq!(p.get_item(&id), *it);
            assert!(id.is_normal());
        }

        // Items are placed top-down: each new item's offset < the previous one's.
        let o1 = p.get_item_id(1).lp_off();
        let o2 = p.get_item_id(2).lp_off();
        let o3 = p.get_item_id(3).lp_off();
        assert!(o1 > o2 && o2 > o3);
        // Each offset is MAXALIGN'd.
        assert_eq!(o3 as usize, maxalign(o3 as usize));
    }

    #[test]
    fn free_space_accounting() {
        let mut p = new_page();
        let exact0 = p.get_exact_free_space();
        assert_eq!(exact0, (BLCKSZ as usize) - SizeOfPageHeaderData);
        assert_eq!(p.get_free_space(), exact0 - SIZE_OF_ITEM_ID);

        let data: &[u8] = b"0123456789"; // 10 bytes -> MAXALIGN = 16
        add(&mut p, data);
        let exact1 = p.get_exact_free_space();
        // One item costs: line pointer (SIZE_OF_ITEM_ID) + MAXALIGN(len).
        assert_eq!(exact1, exact0 - SIZE_OF_ITEM_ID - maxalign(data.len()));
        assert_eq!(p.get_free_space(), exact1 - SIZE_OF_ITEM_ID);

        let multi = p.get_free_space_for_multiple_tuples(3);
        assert_eq!(multi, p.get_exact_free_space() - 3 * SIZE_OF_ITEM_ID);
    }

    #[test]
    fn repair_fragmentation_reclaims_space() {
        // Heap-style: delete a middle line pointer (mark unused) then repair.
        let mut p = new_page();
        let items: [&[u8]; 4] = [b"aaaaaa", b"bbbbbbbb", b"cc", b"dddddddddd"];
        for it in items {
            assert_ne!(add(&mut p, it), INVALID_OFFSET_NUMBER);
        }
        let before = p.get_exact_free_space();

        // Mark item 2 (offset 2) LP_DEAD->unused-with-no-storage like pruning does:
        // set it unused (no storage) so repair_fragmentation reclaims its space.
        let mut id2 = p.get_item_id(2);
        id2.set_unused();
        p.set_item_id_raw(2, id2);

        p.repair_fragmentation();
        let after = p.get_exact_free_space();
        // Reclaimed at least the deleted tuple's aligned length.
        assert!(after > before);
        assert_eq!(after - before, maxalign(items[1].len()));

        // Remaining items still readable at their offsets (1,3,4 had storage).
        for off in [1u16, 3, 4] {
            let id = p.get_item_id(off);
            assert!(id.is_used());
            let want = items[(off - 1) as usize];
            assert_eq!(p.get_item(&id), want);
        }
        // The unused middle pointer remains, hint bit set.
        assert!(p.has_free_line_pointers());
    }

    #[test]
    fn repair_fragmentation_no_gap_is_noop() {
        // All survivors already packed at the page end => compactify's presorted
        // skip loop consumes every item (the full-skip edge case). Deleting the
        // *last* line pointer (lowest itemoff, at pd_upper) leaves no gap.
        let mut p = new_page();
        let items: [&[u8]; 3] = [b"aaaaaa", b"bbbbbbbb", b"cccccccc"];
        for it in items {
            assert_ne!(add(&mut p, it), INVALID_OFFSET_NUMBER);
        }
        let upper_before = p.header().upper;
        // Mark the last item unused (its data sits at pd_upper, so no gap forms).
        let mut last = p.get_item_id(3);
        last.set_unused();
        p.set_item_id_raw(3, last);

        p.repair_fragmentation();
        // Trailing unused LP truncated; survivors 1,2 untouched in place.
        assert_eq!(p.get_max_offset_number(), 2);
        assert!(p.header().upper >= upper_before); // upper moved up (reclaimed) or equal
        assert_eq!(p.get_item(&p.get_item_id(1)), b"aaaaaa");
        assert_eq!(p.get_item(&p.get_item_id(2)), b"bbbbbbbb");
    }

    #[test]
    fn repair_fragmentation_first_deleted() {
        // Delete the first item (highest itemoff). Survivors are still presorted
        // but must be moved toward the page end to close the leading gap.
        let mut p = new_page();
        let items: [&[u8]; 3] = [b"aaaaaa", b"bbbbbbbb", b"cccccccc"];
        for it in items {
            assert_ne!(add(&mut p, it), INVALID_OFFSET_NUMBER);
        }
        let mut first = p.get_item_id(1);
        first.set_unused();
        p.set_item_id_raw(1, first);

        p.repair_fragmentation();
        // Item 1 is now the unused (middle) LP; items 2,3 keep their data.
        assert!(!p.get_item_id(1).is_used());
        assert_eq!(p.get_item(&p.get_item_id(2)), b"bbbbbbbb");
        assert_eq!(p.get_item(&p.get_item_id(3)), b"cccccccc");
        assert!(p.has_free_line_pointers());
    }

    #[test]
    fn index_tuple_delete_compacts() {
        // Index-style: index_tuple_delete removes the line pointer entirely.
        let mut p = new_page();
        let items: [&[u8]; 4] = [b"aaaaaa", b"bbbbbbbb", b"cc", b"dddddddddd"];
        for it in items {
            assert_ne!(add(&mut p, it), INVALID_OFFSET_NUMBER);
        }
        p.index_tuple_delete(2);
        assert_eq!(p.get_max_offset_number(), 3);
        // Items 1,3,4 -> now at offsets 1,2,3 and still correct.
        let expected: [&[u8]; 3] = [b"aaaaaa", b"cc", b"dddddddddd"];
        for (i, want) in expected.iter().enumerate() {
            let id = p.get_item_id((i + 1) as OffsetNumber);
            assert_eq!(p.get_item(&id), *want);
        }
    }

    #[test]
    fn index_multi_delete() {
        let mut p = new_page();
        let items: [&[u8]; 6] = [b"a1", b"b22", b"c333", b"d4444", b"e55555", b"f6"];
        for it in items {
            assert_ne!(add(&mut p, it), INVALID_OFFSET_NUMBER);
        }
        // Delete offsets 2,4,5 (in order).
        p.index_multi_delete(&[2, 4, 5]);
        assert_eq!(p.get_max_offset_number(), 3);
        let expected: [&[u8]; 3] = [b"a1", b"c333", b"f6"];
        for (i, want) in expected.iter().enumerate() {
            let id = p.get_item_id((i + 1) as OffsetNumber);
            assert_eq!(p.get_item(&id), *want);
        }
    }

    #[test]
    fn index_tuple_overwrite_same_and_smaller() {
        let mut p = new_page();
        for it in [b"aaaa".as_slice(), b"bbbbbbbb", b"cccc"] {
            assert_ne!(add(&mut p, it), INVALID_OFFSET_NUMBER);
        }
        // Overwrite middle with same length.
        assert!(p.index_tuple_overwrite(2, b"XXXXXXXX", 8));
        assert_eq!(p.get_item(&p.get_item_id(2)), b"XXXXXXXX");
        // Neighbors intact.
        assert_eq!(p.get_item(&p.get_item_id(1)), b"aaaa");
        assert_eq!(p.get_item(&p.get_item_id(3)), b"cccc");

        // Overwrite middle with shorter data.
        assert!(p.index_tuple_overwrite(2, b"yy", 2));
        assert_eq!(p.get_item(&p.get_item_id(2)), b"yy");
        assert_eq!(p.get_item(&p.get_item_id(1)), b"aaaa");
        assert_eq!(p.get_item(&p.get_item_id(3)), b"cccc");
    }

    #[test]
    fn temp_page_roundtrip() {
        let mut p = new_page();
        add(&mut p, b"keepme");
        let temp = p.get_temp_page_copy();
        assert_eq!(temp.as_bytes(), p.as_bytes());
        let mut dst = Page::boxed_zeroed();
        dst.restore_temp_page(&temp);
        assert_eq!(dst.as_bytes(), p.as_bytes());
    }
}
