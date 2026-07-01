//! Translated from PostgreSQL src/include/storage/bufpage.h
//! Standard POSTGRES buffer page definitions.
#![allow(clippy::cast_ptr_alignment, reason = "PG on-disk/varlena pointer reinterpretation, faithful to C")]

use bitflags::bitflags;

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::pg_config::{BLCKSZ, MAXIMUM_ALIGNOF};
use crate::storage::block::BlockNumber;
use crate::storage::item::Item;
use crate::storage::itemid::ItemIdData;
use crate::storage::off::OffsetNumber;

// GUC variable (process global; to become session/global state later).
pub static mut ignore_checksum_failure: bool = false;

/// A postgres disk page laid out as a slotted page:
///   [ PageHeaderData | linp1..linpN -> lower ... upper <- tupleN..tuple1 | special ]
///
/// C models `Page` as `char *` (PageData = char). We model it as a concrete
/// 8192-byte block. `align(8)` (= MAXIMUM_ALIGNOF) makes the in-place overlay
/// casts to `PageHeaderData` / `ItemIdData` sound by construction.
#[repr(C, align(8))]
pub struct Page([u8; BLCKSZ as usize]);

pub type PageData = u8;

/// Byte offset within a page. Limited to 2^15 (lp_off/lp_len are 15 bits).
pub type LocationIndex = u16;

const _: () = assert!(core::mem::size_of::<Page>() == BLCKSZ as usize);
const _: () = assert!(core::mem::align_of::<Page>() >= core::mem::align_of::<PageHeaderData>());
const _: () = assert!(core::mem::align_of::<Page>() >= core::mem::align_of::<ItemIdData>());

impl Page {
    /// An all-zero page block (an uninitialized / new page).
    pub const fn zeroed() -> Self {
        Self([0u8; BLCKSZ as usize])
    }

    /// An all-zero page block on the heap (avoids an 8KB stack move).
    pub fn boxed_zeroed() -> Box<Self> {
        // SAFETY: Page is a plain byte array; all-zero is a valid bit pattern and
        // the alloc is sized/aligned for Page via Layout::new.
        unsafe {
            let layout = core::alloc::Layout::new::<Self>();
            let ptr = std::alloc::alloc_zeroed(layout).cast::<Self>();
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Box::from_raw(ptr)
        }
    }

    /// Byte view of the page (for the smgr / IoBackend layer).
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Mutable byte view of the page (for reading a page off disk).
    #[inline]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl core::ops::Deref for Page {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl core::ops::DerefMut for Page {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

pub(crate) const fn maxalign(n: usize) -> usize {
    (n + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// On-disk 64-bit LSN stored as two 32-bit halves (historical).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct PageXLogRecPtr {
    pub xlogid: u32,  // high bits
    pub xrecoff: u32, // low bits
}

const _: () = assert!(core::mem::size_of::<PageXLogRecPtr>() == 8);

impl PageXLogRecPtr {
    /// Reassemble the 64-bit LSN.
    pub const fn get(self) -> XLogRecPtr {
        XLogRecPtr(((self.xlogid as u64) << 32) | self.xrecoff as u64)
    }

    /// Split a 64-bit LSN into the two halves.
    pub const fn set(&mut self, lsn: XLogRecPtr) {
        self.xlogid = (lsn.0 >> 32) as u32;
        self.xrecoff = lsn.0 as u32;
    }
}

/// AM-generic per-page header (the fixed 24 bytes before the line-pointer array).
/// On-disk: exact field order/types, `#[repr(C)]`. The trailing `pd_linp` FAM is
/// not a struct field here; access it via the slice accessors below.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PageHeaderData {
    pub lsn: PageXLogRecPtr,    // LSN of last change to this page
    pub checksum: u16,          // page checksum
    pub flags: u16,             // flag bits, see PageFlags
    pub lower: LocationIndex,   // offset to start of free space
    pub upper: LocationIndex,   // offset to end of free space
    pub special: LocationIndex, // offset to start of special space
    pub pagesize_version: u16,  // page size (high 8 bits) | layout version (low 8)
    pub prune_xid: TransactionId, // oldest prunable XID, or zero if none
}

const _: () = assert!(core::mem::size_of::<PageHeaderData>() == 24);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, lsn) == 0);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, checksum) == 8);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, flags) == 10);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, lower) == 12);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, upper) == 14);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, special) == 16);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pagesize_version) == 18);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, prune_xid) == 20);

/// Line pointers do not count as part of the header. (= offsetof pd_linp.)
pub const SizeOfPageHeaderData: usize = core::mem::size_of::<PageHeaderData>();

bitflags! {
    /// flags bits. Undefined bits are initialized to zero.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        const HAS_FREE_LINES = 0x0001; // any unused line pointers?
        const PAGE_FULL      = 0x0002; // not enough free space for new tuple?
        const ALL_VISIBLE    = 0x0004; // all tuples on page visible to everyone
        const VALID_FLAG_BITS = 0x0007; // OR of all valid flags bits
    }
}

pub const PG_PAGE_LAYOUT_VERSION: u16 = 4;
pub const PG_DATA_CHECKSUM_VERSION: u16 = 1;

bitflags! {
    /// Flags for PageAddItemExtended().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageAddItemFlags: i32 {
        const OVERWRITE = 1 << 0;
        const IS_HEAP   = 1 << 1;
    }
}

bitflags! {
    /// Flags for PageIsVerified().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageIsVerifiedFlags: i32 {
        const LOG_WARNING            = 1 << 0;
        const LOG_LOG                = 1 << 1;
        const IGNORE_CHECKSUM_FAILURE = 1 << 2;
    }
}

// === in-place header overlay accessors (crate-internal) ===
//
// Read/write the fixed header by reinterpreting the leading bytes of the page.
// SOUND: Page is `#[repr(C, align(8))]`, so its base pointer is suitably aligned
// for PageHeaderData (asserted above). The page is always >= SizeOfPageHeaderData.

impl Page {
    #[inline]
    pub(crate) fn header(&self) -> &PageHeaderData {
        unsafe { &*self.0.as_ptr().cast::<PageHeaderData>() }
    }

    #[inline]
    pub(crate) fn header_mut(&mut self) -> &mut PageHeaderData {
        unsafe { &mut *self.0.as_mut_ptr().cast::<PageHeaderData>() }
    }

    /// True iff no itemid has been allocated on the page.
    #[inline]
    pub fn is_empty(&self) -> bool {
        (self.header().lower as usize) <= SizeOfPageHeaderData
    }

    /// True iff page has not been initialized (by init).
    #[inline]
    pub fn is_new(&self) -> bool {
        self.header().upper == 0
    }

    /// Returns the requested item identifier (line pointer). 1-based offset.
    #[inline]
    pub fn get_item_id(&self, offset_number: OffsetNumber) -> ItemIdData {
        let idx = (offset_number - 1) as usize;
        let base = unsafe { self.0.as_ptr().add(SizeOfPageHeaderData).cast::<ItemIdData>() };
        unsafe { *base.add(idx) }
    }

    /// Overwrite the line pointer at `offset_number` (1-based). Used by the VACUUM
    /// prune/mark passes (`heap_page_prune_execute` / `lazy_vacuum_heap_page`) to set
    /// an item's flags to LP_DEAD or LP_UNUSED in place. Caller holds the buffer's
    /// exclusive/cleanup content lock.
    #[inline]
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "page buffer is MAXALIGN/8-byte aligned and SizeOfPageHeaderData is a multiple of 4, so the ItemIdData (4-byte) base is aligned"
    )]
    pub fn set_item_id(&mut self, offset_number: OffsetNumber, value: ItemIdData) {
        let idx = (offset_number - 1) as usize;
        let base = unsafe { self.0.as_mut_ptr().add(SizeOfPageHeaderData).cast::<ItemIdData>() };
        unsafe { *base.add(idx) = value };
    }

    /// Contents start, for pages with no line pointers. MAXALIGN'd.
    #[inline]
    pub fn get_contents(&self) -> &[u8] {
        &self.0[maxalign(SizeOfPageHeaderData)..]
    }

    /// Page size, from a formatted page (high 8 bits of pagesize_version).
    #[inline]
    pub fn get_page_size(&self) -> usize {
        (self.header().pagesize_version & 0xFF00) as usize
    }

    /// Page layout version (low 8 bits of pagesize_version).
    #[inline]
    pub fn get_page_layout_version(&self) -> u8 {
        (self.header().pagesize_version & 0x00FF) as u8
    }

    /// Set page size and layout version together.
    #[inline]
    pub fn set_page_size_and_version(&mut self, size: usize, version: u8) {
        debug_assert_eq!((size & 0xFF00), size);
        self.header_mut().pagesize_version = (size as u16) | u16::from(version);
    }

    /// Size of special space on a page.
    #[inline]
    pub fn get_special_size(&self) -> u16 {
        (self.get_page_size() - self.header().special as usize) as u16
    }

    /// Validate the special pointer (catches use before initialization).
    #[inline]
    pub fn validate_special_pointer(&self) {
        debug_assert!(u32::from(self.header().special) <= BLCKSZ);
        debug_assert!((self.header().special as usize) >= SizeOfPageHeaderData);
    }

    /// Special space as a byte slice (page + special).
    #[inline]
    pub fn get_special_pointer(&self) -> &[u8] {
        self.validate_special_pointer();
        &self.0[self.header().special as usize..]
    }

    /// Retrieve an item on the page given its line pointer.
    #[inline]
    pub fn get_item(&self, item_id: &ItemIdData) -> Item<'_> {
        debug_assert!(item_id.has_storage());
        let off = item_id.lp_off() as usize;
        let len = item_id.lp_len() as usize;
        &self.0[off..off + len]
    }

    /// Maximum offset number used (= number of items). 0 if uninitialized.
    #[inline]
    pub fn get_max_offset_number(&self) -> OffsetNumber {
        let lower = self.header().lower as usize;
        if lower <= SizeOfPageHeaderData {
            0
        } else {
            ((lower - SizeOfPageHeaderData) / core::mem::size_of::<ItemIdData>()) as OffsetNumber
        }
    }

    /// Reassemble the page LSN.
    #[inline]
    pub fn get_lsn(&self) -> XLogRecPtr {
        self.header().lsn.get()
    }

    /// Store the page LSN.
    #[inline]
    pub fn set_lsn(&mut self, lsn: XLogRecPtr) {
        self.header_mut().lsn.set(lsn);
    }

    #[inline]
    pub fn has_free_line_pointers(&self) -> bool {
        PageFlags::from_bits_truncate(self.header().flags).contains(PageFlags::HAS_FREE_LINES)
    }
    #[inline]
    pub fn set_has_free_line_pointers(&mut self) {
        self.header_mut().flags |= PageFlags::HAS_FREE_LINES.bits();
    }
    #[inline]
    pub fn clear_has_free_line_pointers(&mut self) {
        self.header_mut().flags &= !PageFlags::HAS_FREE_LINES.bits();
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        PageFlags::from_bits_truncate(self.header().flags).contains(PageFlags::PAGE_FULL)
    }
    #[inline]
    pub fn set_full(&mut self) {
        self.header_mut().flags |= PageFlags::PAGE_FULL.bits();
    }
    #[inline]
    pub fn clear_full(&mut self) {
        self.header_mut().flags &= !PageFlags::PAGE_FULL.bits();
    }

    #[inline]
    pub fn is_all_visible(&self) -> bool {
        PageFlags::from_bits_truncate(self.header().flags).contains(PageFlags::ALL_VISIBLE)
    }
    #[inline]
    pub fn set_all_visible(&mut self) {
        self.header_mut().flags |= PageFlags::ALL_VISIBLE.bits();
    }
    #[inline]
    pub fn clear_all_visible(&mut self) {
        self.header_mut().flags &= !PageFlags::ALL_VISIBLE.bits();
    }

    /// Lower prune_xid toward `xid` (C `PageSetPrunable`).
    #[inline]
    pub fn set_prunable(&mut self, xid: TransactionId) {
        let h = self.header_mut();
        // TransactionIdIsValid == nonzero; precedes == <, modulo-32 elsewhere.
        if h.prune_xid.0 == 0 || xid < h.prune_xid {
            h.prune_xid = xid;
        }
    }

    #[inline]
    pub fn clear_prunable(&mut self) {
        self.header_mut().prune_xid = TransactionId(0); // InvalidTransactionId
    }
}

// === deprecated C-named shims ===
//
// Every former header inline accessor and the .c functions keep a thin C-named
// shim delegating to the `impl Page` method. New code should use the method.
// These must NOT be called internally (no deprecation warnings).

#[deprecated(note = "use `page.is_empty()`")]
#[inline]
pub fn PageIsEmpty(page: &Page) -> bool {
    page.is_empty()
}

#[deprecated(note = "use `page.is_new()`")]
#[inline]
pub fn PageIsNew(page: &Page) -> bool {
    page.is_new()
}

#[deprecated(note = "use `page.get_item_id(off)`")]
#[inline]
pub fn PageGetItemId(page: &Page, offset_number: OffsetNumber) -> ItemIdData {
    page.get_item_id(offset_number)
}

#[deprecated(note = "use `page.get_contents()`")]
#[inline]
pub fn PageGetContents(page: &Page) -> &[u8] {
    page.get_contents()
}

#[deprecated(note = "use `page.get_page_size()`")]
#[inline]
pub fn PageGetPageSize(page: &Page) -> usize {
    page.get_page_size()
}

#[deprecated(note = "use `page.get_page_layout_version()`")]
#[inline]
pub fn PageGetPageLayoutVersion(page: &Page) -> u8 {
    page.get_page_layout_version()
}

#[deprecated(note = "use `page.set_page_size_and_version(size, version)`")]
#[inline]
pub fn PageSetPageSizeAndVersion(page: &mut Page, size: usize, version: u8) {
    page.set_page_size_and_version(size, version);
}

#[deprecated(note = "use `page.get_special_size()`")]
#[inline]
pub fn PageGetSpecialSize(page: &Page) -> u16 {
    page.get_special_size()
}

#[deprecated(note = "use `page.validate_special_pointer()`")]
#[inline]
pub fn PageValidateSpecialPointer(page: &Page) {
    page.validate_special_pointer();
}

#[deprecated(note = "use `page.get_special_pointer()`")]
#[inline]
pub fn PageGetSpecialPointer(page: &Page) -> &[u8] {
    page.get_special_pointer()
}

#[deprecated(note = "use `page.get_item(item_id)`")]
#[inline]
pub fn PageGetItem<'a>(page: &'a Page, item_id: &ItemIdData) -> Item<'a> {
    page.get_item(item_id)
}

#[deprecated(note = "use `page.get_max_offset_number()`")]
#[inline]
pub fn PageGetMaxOffsetNumber(page: &Page) -> OffsetNumber {
    page.get_max_offset_number()
}

#[deprecated(note = "use `page.get_lsn()`")]
#[inline]
pub fn PageGetLSN(page: &Page) -> XLogRecPtr {
    page.get_lsn()
}

#[deprecated(note = "use `page.set_lsn(lsn)`")]
#[inline]
pub fn PageSetLSN(page: &mut Page, lsn: XLogRecPtr) {
    page.set_lsn(lsn);
}

#[deprecated(note = "use `page.has_free_line_pointers()`")]
#[inline]
pub fn PageHasFreeLinePointers(page: &Page) -> bool {
    page.has_free_line_pointers()
}
#[deprecated(note = "use `page.set_has_free_line_pointers()`")]
#[inline]
pub fn PageSetHasFreeLinePointers(page: &mut Page) {
    page.set_has_free_line_pointers();
}
#[deprecated(note = "use `page.clear_has_free_line_pointers()`")]
#[inline]
pub fn PageClearHasFreeLinePointers(page: &mut Page) {
    page.clear_has_free_line_pointers();
}

#[deprecated(note = "use `page.is_full()`")]
#[inline]
pub fn PageIsFull(page: &Page) -> bool {
    page.is_full()
}
#[deprecated(note = "use `page.set_full()`")]
#[inline]
pub fn PageSetFull(page: &mut Page) {
    page.set_full();
}
#[deprecated(note = "use `page.clear_full()`")]
#[inline]
pub fn PageClearFull(page: &mut Page) {
    page.clear_full();
}

#[deprecated(note = "use `page.is_all_visible()`")]
#[inline]
pub fn PageIsAllVisible(page: &Page) -> bool {
    page.is_all_visible()
}
#[deprecated(note = "use `page.set_all_visible()`")]
#[inline]
pub fn PageSetAllVisible(page: &mut Page) {
    page.set_all_visible();
}
#[deprecated(note = "use `page.clear_all_visible()`")]
#[inline]
pub fn PageClearAllVisible(page: &mut Page) {
    page.clear_all_visible();
}

#[deprecated(note = "use `page.set_prunable(xid)`")]
#[inline]
pub fn PageSetPrunable(page: &mut Page, xid: TransactionId) {
    page.set_prunable(xid);
}
#[deprecated(note = "use `page.clear_prunable()`")]
#[inline]
pub fn PageClearPrunable(page: &mut Page) {
    page.clear_prunable();
}

// === .c function shims (bodies are `impl Page` methods in the backend module) ===

#[deprecated(note = "use `page.init(page_size, special_size)`")]
#[inline]
pub fn PageInit(page: &mut Page, page_size: usize, special_size: usize) {
    page.init(page_size, special_size);
}

#[deprecated(note = "use `page.is_verified(blkno, flags)`")]
#[inline]
pub fn PageIsVerified(page: &Page, blkno: BlockNumber, flags: PageIsVerifiedFlags) -> (bool, bool) {
    page.is_verified(blkno, flags)
}

#[deprecated(note = "use `page.add_item_extended(...)`")]
#[inline]
pub fn PageAddItemExtended(
    page: &mut Page,
    item: Item,
    size: usize,
    offset_number: OffsetNumber,
    flags: PageAddItemFlags,
) -> OffsetNumber {
    page.add_item_extended(item, size, offset_number, flags)
}

#[deprecated(note = "use `page.add_item(...)`")]
#[inline]
pub fn PageAddItem(
    page: &mut Page,
    item: Item,
    size: usize,
    offset_number: OffsetNumber,
    overwrite: bool,
    is_heap: bool,
) -> OffsetNumber {
    page.add_item(item, size, offset_number, overwrite, is_heap)
}

#[deprecated(note = "use `page.get_temp_page()`")]
#[inline]
pub fn PageGetTempPage(page: &Page) -> Box<Page> {
    page.get_temp_page()
}

#[deprecated(note = "use `page.get_temp_page_copy()`")]
#[inline]
pub fn PageGetTempPageCopy(page: &Page) -> Box<Page> {
    page.get_temp_page_copy()
}

#[deprecated(note = "use `page.get_temp_page_copy_special()`")]
#[inline]
pub fn PageGetTempPageCopySpecial(page: &Page) -> Box<Page> {
    page.get_temp_page_copy_special()
}

#[deprecated(note = "use `old_page.restore_temp_page(temp_page)`")]
#[inline]
pub fn PageRestoreTempPage(temp_page: &Page, old_page: &mut Page) {
    old_page.restore_temp_page(temp_page);
}

#[deprecated(note = "use `page.repair_fragmentation()`")]
#[inline]
pub fn PageRepairFragmentation(page: &mut Page) {
    page.repair_fragmentation();
}

#[deprecated(note = "use `page.truncate_line_pointer_array()`")]
#[inline]
pub fn PageTruncateLinePointerArray(page: &mut Page) {
    page.truncate_line_pointer_array();
}

#[deprecated(note = "use `page.get_free_space()`")]
#[inline]
pub fn PageGetFreeSpace(page: &Page) -> usize {
    page.get_free_space()
}

#[deprecated(note = "use `page.get_free_space_for_multiple_tuples(ntups)`")]
#[inline]
pub fn PageGetFreeSpaceForMultipleTuples(page: &Page, ntups: i32) -> usize {
    page.get_free_space_for_multiple_tuples(ntups)
}

#[deprecated(note = "use `page.get_exact_free_space()`")]
#[inline]
pub fn PageGetExactFreeSpace(page: &Page) -> usize {
    page.get_exact_free_space()
}

#[deprecated(note = "use `page.get_heap_free_space()`")]
#[inline]
pub fn PageGetHeapFreeSpace(page: &Page) -> usize {
    page.get_heap_free_space()
}

#[deprecated(note = "use `page.index_tuple_delete(offnum)`")]
#[inline]
pub fn PageIndexTupleDelete(page: &mut Page, offnum: OffsetNumber) {
    page.index_tuple_delete(offnum);
}

#[deprecated(note = "use `page.index_multi_delete(itemnos)`")]
#[inline]
pub fn PageIndexMultiDelete(page: &mut Page, itemnos: &[OffsetNumber]) {
    page.index_multi_delete(itemnos);
}

#[deprecated(note = "use `page.index_tuple_delete_no_compact(offnum)`")]
#[inline]
pub fn PageIndexTupleDeleteNoCompact(page: &mut Page, offnum: OffsetNumber) {
    page.index_tuple_delete_no_compact(offnum);
}

#[deprecated(note = "use `page.index_tuple_overwrite(offnum, newtup, newsize)`")]
#[inline]
pub fn PageIndexTupleOverwrite(
    page: &mut Page,
    offnum: OffsetNumber,
    newtup: Item,
    newsize: usize,
) -> bool {
    page.index_tuple_overwrite(offnum, newtup, newsize)
}

#[deprecated(note = "use `page.set_checksum_copy(blkno)`")]
#[inline]
pub fn PageSetChecksumCopy(page: &Page, blkno: BlockNumber) -> Box<Page> {
    page.set_checksum_copy(blkno)
}

#[deprecated(note = "use `page.set_checksum_inplace(blkno)`")]
#[inline]
pub fn PageSetChecksumInplace(page: &mut Page, blkno: BlockNumber) {
    page.set_checksum_inplace(blkno);
}
