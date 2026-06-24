//! Translated from PostgreSQL src/include/storage/bufpage.h
//! Standard POSTGRES buffer page definitions.

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

// A postgres disk page is a byte buffer laid out as a slotted page:
//   [ PageHeaderData | linp1..linpN -> pd_lower ... pd_upper <- tupleN..tuple1 | special ]
// C models `Page` as `char *` (PageData = char). We model it as a byte slice.
pub type PageData = u8;
pub type Page<'a> = &'a [u8];
pub type PageMut<'a> = &'a mut [u8];

/// Byte offset within a page. Limited to 2^15 (lp_off/lp_len are 15 bits).
pub type LocationIndex = u16;

const fn maxalign(n: usize) -> usize {
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
    pub pd_lsn: PageXLogRecPtr,    // LSN of last change to this page
    pub pd_checksum: u16,          // page checksum
    pub pd_flags: u16,             // flag bits, see PageFlags
    pub pd_lower: LocationIndex,   // offset to start of free space
    pub pd_upper: LocationIndex,   // offset to end of free space
    pub pd_special: LocationIndex, // offset to start of special space
    pub pd_pagesize_version: u16,  // page size (high 8 bits) | layout version (low 8)
    pub pd_prune_xid: TransactionId, // oldest prunable XID, or zero if none
}

const _: () = assert!(core::mem::size_of::<PageHeaderData>() == 24);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_lsn) == 0);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_checksum) == 8);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_flags) == 10);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_lower) == 12);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_upper) == 14);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_special) == 16);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_pagesize_version) == 18);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_prune_xid) == 20);

/// Line pointers do not count as part of the header. (= offsetof pd_linp.)
pub const SizeOfPageHeaderData: usize = core::mem::size_of::<PageHeaderData>();

bitflags! {
    /// pd_flags bits. Undefined bits are initialized to zero.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        const HAS_FREE_LINES = 0x0001; // any unused line pointers?
        const PAGE_FULL      = 0x0002; // not enough free space for new tuple?
        const ALL_VISIBLE    = 0x0004; // all tuples on page visible to everyone
        const VALID_FLAG_BITS = 0x0007; // OR of all valid pd_flags bits
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

// === page support functions (the static-inline header accessors) ===
//
// These read/write the fixed header by reinterpreting the leading bytes of the
// page buffer. SAFETY in each: `page` is a real page buffer >= SizeOfPageHeaderData.

#[inline]
fn header(page: &[u8]) -> &PageHeaderData {
    unsafe { &*(page.as_ptr() as *const PageHeaderData) }
}

#[inline]
fn header_mut(page: &mut [u8]) -> &mut PageHeaderData {
    unsafe { &mut *(page.as_mut_ptr() as *mut PageHeaderData) }
}

/// True iff no itemid has been allocated on the page.
pub fn PageIsEmpty(page: Page) -> bool {
    (header(page).pd_lower as usize) <= SizeOfPageHeaderData
}

/// True iff page has not been initialized (by PageInit).
pub fn PageIsNew(page: Page) -> bool {
    header(page).pd_upper == 0
}

/// Returns the requested item identifier (line pointer). 1-based offset number.
pub fn PageGetItemId(page: Page, offset_number: OffsetNumber) -> ItemIdData {
    let linp = page_linp(page);
    linp[(offset_number - 1) as usize]
}

/// The line-pointer array as a typed slice (MAXALIGN guarantees alignment at
/// SizeOfPageHeaderData). Length is bounded by pd_lower.
pub fn page_linp(page: Page) -> &[ItemIdData] {
    let n = PageGetMaxOffsetNumber(page) as usize;
    let base = unsafe { page.as_ptr().add(SizeOfPageHeaderData) as *const ItemIdData };
    unsafe { core::slice::from_raw_parts(base, n) }
}

/// Mutable line-pointer array view.
pub fn page_linp_mut(page: PageMut) -> &mut [ItemIdData] {
    let n = PageGetMaxOffsetNumber(page) as usize;
    let base = unsafe { page.as_mut_ptr().add(SizeOfPageHeaderData) as *mut ItemIdData };
    unsafe { core::slice::from_raw_parts_mut(base, n) }
}

/// Contents start, for pages with no line pointers. MAXALIGN'd.
pub fn PageGetContents(page: Page) -> &[u8] {
    &page[maxalign(SizeOfPageHeaderData)..]
}

/// Page size, from a formatted page (high 8 bits of pd_pagesize_version).
pub fn PageGetPageSize(page: Page) -> usize {
    (header(page).pd_pagesize_version & 0xFF00) as usize
}

/// Page layout version (low 8 bits of pd_pagesize_version).
pub fn PageGetPageLayoutVersion(page: Page) -> u8 {
    (header(page).pd_pagesize_version & 0x00FF) as u8
}

/// Set page size and layout version together.
pub fn PageSetPageSizeAndVersion(page: PageMut, size: usize, version: u8) {
    debug_assert!((size & 0xFF00) == size);
    header_mut(page).pd_pagesize_version = (size as u16) | version as u16;
}

/// Size of special space on a page.
pub fn PageGetSpecialSize(page: Page) -> u16 {
    (PageGetPageSize(page) - header(page).pd_special as usize) as u16
}

/// Validate the special pointer (catches use before initialization).
pub fn PageValidateSpecialPointer(page: Page) {
    debug_assert!((header(page).pd_special as u32) <= BLCKSZ);
    debug_assert!((header(page).pd_special as usize) >= SizeOfPageHeaderData);
}

/// Special space as a byte slice (page + pd_special).
pub fn PageGetSpecialPointer(page: Page) -> &[u8] {
    PageValidateSpecialPointer(page);
    &page[header(page).pd_special as usize..]
}

/// Retrieve an item on the page given its line pointer.
pub fn PageGetItem<'a>(page: Page<'a>, item_id: &ItemIdData) -> Item<'a> {
    debug_assert!(item_id.has_storage());
    let off = item_id.lp_off() as usize;
    let len = item_id.lp_len() as usize;
    &page[off..off + len]
}

/// Maximum offset number used (= number of items). 0 if uninitialized.
pub fn PageGetMaxOffsetNumber(page: Page) -> OffsetNumber {
    let lower = header(page).pd_lower as usize;
    if lower <= SizeOfPageHeaderData {
        0
    } else {
        ((lower - SizeOfPageHeaderData) / core::mem::size_of::<ItemIdData>()) as OffsetNumber
    }
}

/// Reassemble the page LSN.
pub fn PageGetLSN(page: Page) -> XLogRecPtr {
    header(page).pd_lsn.get()
}

/// Store the page LSN.
pub fn PageSetLSN(page: PageMut, lsn: XLogRecPtr) {
    header_mut(page).pd_lsn.set(lsn);
}

pub fn PageHasFreeLinePointers(page: Page) -> bool {
    PageFlags::from_bits_truncate(header(page).pd_flags).contains(PageFlags::HAS_FREE_LINES)
}
pub fn PageSetHasFreeLinePointers(page: PageMut) {
    header_mut(page).pd_flags |= PageFlags::HAS_FREE_LINES.bits();
}
pub fn PageClearHasFreeLinePointers(page: PageMut) {
    header_mut(page).pd_flags &= !PageFlags::HAS_FREE_LINES.bits();
}

pub fn PageIsFull(page: Page) -> bool {
    PageFlags::from_bits_truncate(header(page).pd_flags).contains(PageFlags::PAGE_FULL)
}
pub fn PageSetFull(page: PageMut) {
    header_mut(page).pd_flags |= PageFlags::PAGE_FULL.bits();
}
pub fn PageClearFull(page: PageMut) {
    header_mut(page).pd_flags &= !PageFlags::PAGE_FULL.bits();
}

pub fn PageIsAllVisible(page: Page) -> bool {
    PageFlags::from_bits_truncate(header(page).pd_flags).contains(PageFlags::ALL_VISIBLE)
}
pub fn PageSetAllVisible(page: PageMut) {
    header_mut(page).pd_flags |= PageFlags::ALL_VISIBLE.bits();
}
pub fn PageClearAllVisible(page: PageMut) {
    header_mut(page).pd_flags &= !PageFlags::ALL_VISIBLE.bits();
}

/// Lower pd_prune_xid toward `xid` (C `PageSetPrunable`; needs transam, kept as fn).
pub fn PageSetPrunable(page: PageMut, xid: TransactionId) {
    let h = header_mut(page);
    // TransactionIdIsValid == nonzero; precedes == <, modulo-32 elsewhere.
    if h.pd_prune_xid.0 == 0 || xid < h.pd_prune_xid {
        h.pd_prune_xid = xid;
    }
}

pub fn PageClearPrunable(page: PageMut) {
    header_mut(page).pd_prune_xid = TransactionId(0); // InvalidTransactionId
}

// === extern declarations ===

pub fn PageInit(_page: PageMut, _page_size: usize, _special_size: usize) {
    unimplemented!()
}

/// Returns (verified, checksum_failure).
pub fn PageIsVerified(_page: PageMut, _blkno: BlockNumber, _flags: PageIsVerifiedFlags) -> (bool, bool) {
    unimplemented!()
}

/// Returns the offset number where the item was placed (InvalidOffsetNumber on failure).
pub fn PageAddItemExtended(
    _page: PageMut,
    _item: Item,
    _size: usize,
    _offset_number: OffsetNumber,
    _flags: PageAddItemFlags,
) -> OffsetNumber {
    unimplemented!()
}

pub fn PageGetTempPage(_page: Page) -> Vec<u8> {
    unimplemented!()
}
pub fn PageGetTempPageCopy(_page: Page) -> Vec<u8> {
    unimplemented!()
}
pub fn PageGetTempPageCopySpecial(_page: Page) -> Vec<u8> {
    unimplemented!()
}
pub fn PageRestoreTempPage(_temp_page: PageMut, _old_page: PageMut) {
    unimplemented!()
}
pub fn PageRepairFragmentation(_page: PageMut) {
    unimplemented!()
}
pub fn PageTruncateLinePointerArray(_page: PageMut) {
    unimplemented!()
}
pub fn PageGetFreeSpace(_page: Page) -> usize {
    unimplemented!()
}
pub fn PageGetFreeSpaceForMultipleTuples(_page: Page, _ntups: i32) -> usize {
    unimplemented!()
}
pub fn PageGetExactFreeSpace(_page: Page) -> usize {
    unimplemented!()
}
pub fn PageGetHeapFreeSpace(_page: Page) -> usize {
    unimplemented!()
}
pub fn PageIndexTupleDelete(_page: PageMut, _offnum: OffsetNumber) {
    unimplemented!()
}
pub fn PageIndexMultiDelete(_page: PageMut, _itemnos: &[OffsetNumber]) {
    unimplemented!()
}
pub fn PageIndexTupleDeleteNoCompact(_page: PageMut, _offnum: OffsetNumber) {
    unimplemented!()
}
pub fn PageIndexTupleOverwrite(
    _page: PageMut,
    _offnum: OffsetNumber,
    _newtup: Item,
    _newsize: usize,
) -> bool {
    unimplemented!()
}
pub fn PageSetChecksumCopy(_page: PageMut, _blkno: BlockNumber) -> Vec<u8> {
    unimplemented!()
}
pub fn PageSetChecksumInplace(_page: PageMut, _blkno: BlockNumber) {
    unimplemented!()
}
