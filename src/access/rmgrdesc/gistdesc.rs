//! Translation of postgres/src/backend/access/rmgrdesc/gistdesc.c
//!                + the gistxlog* record structs and XLOG_GIST_* opcodes it
//!                  reads from postgres/src/include/access/gistxlog.h
//!
//! rmgr descriptor routines for the GiST AM (used by pg_waldump). gist_desc
//! casts the WAL record payload to the appropriate gistxlog* struct (selected
//! by the record's info byte) and appends a human-readable summary of its
//! fields; gist_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h            -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   storage/off.h               -> OffsetNumber
//!   storage/block.h             -> BlockNumber
//!   access/transam.h            -> FullTransactionId + Epoch/Xid accessors
//!   storage/relfilelocator.h    -> RelFileLocator (spcOid/dbOid/relNumber)
//!   c.h types                   -> uint8/uint16/uint32, bool, TransactionId
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the SizeOf* offsets, the XLOG_GIST_* opcode values, and
//! the gist_identify name table are REAL (faithful to gistxlog.h / gistdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;

use crate::access::transam::{
    EpochFromFullTransactionId, FullTransactionId, XidFromFullTransactionId,
};
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// Base types (from c.h / storage/off.h / storage/block.h / common/relpath.h)
// ---------------------------------------------------------------------------

pub type OffsetNumber = uint16;
pub type BlockNumber = uint32;
pub type RelFileNumber = Oid;

/// GistNSN is XLogRecPtr (a 64-bit LSN); see access/gist.h.
pub type GistNSN = uint64;

/// RelFileLocator: (tablespace, database, relfilenumber) tuple identifying
/// physical relation storage. Real layout from storage/relfilelocator.h.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

// ---------------------------------------------------------------------------
// XLOG records for GiST operations (gistxlog.h)
// ---------------------------------------------------------------------------

pub const XLOG_GIST_PAGE_UPDATE: uint8 = 0x00;
/// delete leaf index tuples for a page
pub const XLOG_GIST_DELETE: uint8 = 0x10;
/// old page is about to be reused from FSM
pub const XLOG_GIST_PAGE_REUSE: uint8 = 0x20;
pub const XLOG_GIST_PAGE_SPLIT: uint8 = 0x30;
// 0x40 XLOG_GIST_INSERT_COMPLETE -- not used anymore
// 0x50 XLOG_GIST_CREATE_INDEX    -- not used anymore
pub const XLOG_GIST_PAGE_DELETE: uint8 = 0x60;
/// nop, assign new LSN
pub const XLOG_GIST_ASSIGN_LSN: uint8 = 0x70;

/// XLOG_GIST_PAGE_UPDATE.
///
/// Backup Blk 0: updated page.
/// Backup Blk 1: if this completes a page split (downlink insert), the left
/// half of the split.
///
/// In payload of blk 0: 1. todelete OffsetNumbers, 2. tuples to insert.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gistxlogPageUpdate {
    /// number of deleted offsets
    pub ntodelete: uint16,
    pub ntoinsert: uint16,
}

/// XLOG_GIST_DELETE.
///
/// Backup Blk 0: leaf page, whose index tuples are deleted.
///
/// In C this has a trailing `OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER]`; the
/// flexible array is omitted here (callers index past the fixed header).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gistxlogDelete {
    pub snapshotConflictHorizon: TransactionId,
    /// number of deleted offsets
    pub ntodelete: uint16,
    /// to handle recovery conflict during logical decoding on standby
    pub isCatalogRel: bool,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] -- omitted (flexible array).
}

/// XLOG_GIST_PAGE_SPLIT.
///
/// Backup Blk 0: if this completes a page split (downlink insert), the left
/// half of the split.
/// Backup Blk 1 - npage: split pages (1 is the original page).
///
/// Followed by: gistxlogPage and an array of IndexTupleData per page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gistxlogPageSplit {
    /// rightlink of the page before split
    pub origrlink: BlockNumber,
    /// NSN of the page before split
    pub orignsn: GistNSN,
    /// was split page a leaf page?
    pub origleaf: bool,

    /// # of pages in the split
    pub npage: uint16,
    /// set F_FOLLOW_RIGHT flags
    pub markfollowright: bool,
}

/// XLOG_GIST_PAGE_DELETE.
///
/// Backup Blk 0: page that was deleted.
/// Backup Blk 1: parent page, containing the downlink to the deleted page.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gistxlogPageDelete {
    /// last Xid which could see page in scan
    pub deleteXid: FullTransactionId,
    /// Offset of downlink referencing this page
    pub downlinkOffset: OffsetNumber,
}

/// XLOG_GIST_PAGE_REUSE: what we need to know about page reuse, for hot standby.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gistxlogPageReuse {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    /// to handle recovery conflict during logical decoding on standby
    pub isCatalogRel: bool,
}

// ---------------------------------------------------------------------------
// SizeOf* macros (gistxlog.h).
// ---------------------------------------------------------------------------

/// offsetof(gistxlogDelete, offsets) -- the fixed header size (the flexible
/// `offsets` array begins here).
pub const SizeOfGistxlogDelete: usize = core::mem::size_of::<gistxlogDelete>();

/// offsetof(gistxlogPageDelete, downlinkOffset) + sizeof(OffsetNumber)
pub const SizeOfGistxlogPageDelete: usize =
    core::mem::offset_of!(gistxlogPageDelete, downlinkOffset)
        + core::mem::size_of::<OffsetNumber>();

/// offsetof(gistxlogPageReuse, isCatalogRel) + sizeof(bool)
pub const SizeOfGistxlogPageReuse: usize =
    core::mem::offset_of!(gistxlogPageReuse, isCatalogRel) + core::mem::size_of::<bool>();

// ---------------------------------------------------------------------------
// Descriptor routines (gistdesc.c)
// ---------------------------------------------------------------------------

/// Render a single 'T'/'F' from a bool, matching the C `(cond) ? 'T' : 'F'`.
#[inline]
fn tf(b: bool) -> char {
    if b {
        'T'
    } else {
        'F'
    }
}

/// out_gistxlogPageUpdate: no details to write out (matches C empty body).
#[inline]
fn out_gistxlogPageUpdate(_buf: StringInfo, _xlrec: *const gistxlogPageUpdate) {}

/// out_gistxlogPageReuse.
///
/// # Safety
/// `xlrec` must point to a valid gistxlogPageReuse.
unsafe fn out_gistxlogPageReuse(buf: StringInfo, xlrec: *const gistxlogPageReuse) {
    appendStringInfo!(
        buf,
        "rel {}/{}/{}; blk {}; snapshotConflictHorizon {}:{}, isCatalogRel {}",
        (*xlrec).locator.spcOid,
        (*xlrec).locator.dbOid,
        (*xlrec).locator.relNumber,
        (*xlrec).block,
        EpochFromFullTransactionId((*xlrec).snapshotConflictHorizon),
        XidFromFullTransactionId((*xlrec).snapshotConflictHorizon),
        tf((*xlrec).isCatalogRel)
    );
}

/// out_gistxlogDelete.
///
/// # Safety
/// `xlrec` must point to a valid gistxlogDelete.
unsafe fn out_gistxlogDelete(buf: StringInfo, xlrec: *const gistxlogDelete) {
    appendStringInfo!(
        buf,
        "delete: snapshotConflictHorizon {}, nitems: {}, isCatalogRel {}",
        (*xlrec).snapshotConflictHorizon,
        (*xlrec).ntodelete,
        tf((*xlrec).isCatalogRel)
    );
}

/// out_gistxlogPageSplit.
///
/// # Safety
/// `xlrec` must point to a valid gistxlogPageSplit.
unsafe fn out_gistxlogPageSplit(buf: StringInfo, xlrec: *const gistxlogPageSplit) {
    appendStringInfo!(buf, "page_split: splits to {} pages", (*xlrec).npage);
}

/// out_gistxlogPageDelete.
///
/// # Safety
/// `xlrec` must point to a valid gistxlogPageDelete.
unsafe fn out_gistxlogPageDelete(buf: StringInfo, xlrec: *const gistxlogPageDelete) {
    appendStringInfo!(
        buf,
        "deleteXid {}:{}; downlink {}",
        EpochFromFullTransactionId((*xlrec).deleteXid),
        XidFromFullTransactionId((*xlrec).deleteXid),
        (*xlrec).downlinkOffset
    );
}

/// gist_desc: describe a GiST-AM WAL record into `buf`.
///
/// # Safety
/// `record` is an opaque XLogReaderState pointer; the data pointer it yields
/// (via the stubbed XLogRecGetData) is cast to the per-opcode struct.
pub unsafe fn gist_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec: *mut c_char = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_GIST_PAGE_UPDATE => {
            out_gistxlogPageUpdate(buf, rec as *const gistxlogPageUpdate);
        }
        XLOG_GIST_PAGE_REUSE => {
            out_gistxlogPageReuse(buf, rec as *const gistxlogPageReuse);
        }
        XLOG_GIST_DELETE => {
            out_gistxlogDelete(buf, rec as *const gistxlogDelete);
        }
        XLOG_GIST_PAGE_SPLIT => {
            out_gistxlogPageSplit(buf, rec as *const gistxlogPageSplit);
        }
        XLOG_GIST_PAGE_DELETE => {
            out_gistxlogPageDelete(buf, rec as *const gistxlogPageDelete);
        }
        XLOG_GIST_ASSIGN_LSN => {
            // No details to write out.
        }
        _ => {}
    }
}

/// gist_identify: return the name of the GiST-AM opcode encoded in `info`.
/// Returns a NUL-terminated C string, or null for an unknown opcode (matching
/// the C `const char *id = NULL`).
pub fn gist_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        XLOG_GIST_PAGE_UPDATE => b"PAGE_UPDATE\0",
        XLOG_GIST_DELETE => b"DELETE\0",
        XLOG_GIST_PAGE_REUSE => b"PAGE_REUSE\0",
        XLOG_GIST_PAGE_SPLIT => b"PAGE_SPLIT\0",
        XLOG_GIST_PAGE_DELETE => b"PAGE_DELETE\0",
        XLOG_GIST_ASSIGN_LSN => b"ASSIGN_LSN\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    #[test]
    fn identify_page_update_returns_name() {
        let p = gist_identify(XLOG_GIST_PAGE_UPDATE);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "PAGE_UPDATE");
    }

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (XLOG_GIST_PAGE_UPDATE, "PAGE_UPDATE"),
            (XLOG_GIST_DELETE, "DELETE"),
            (XLOG_GIST_PAGE_REUSE, "PAGE_REUSE"),
            (XLOG_GIST_PAGE_SPLIT, "PAGE_SPLIT"),
            (XLOG_GIST_PAGE_DELETE, "PAGE_DELETE"),
            (XLOG_GIST_ASSIGN_LSN, "ASSIGN_LSN"),
        ];
        for &(op, name) in cases {
            let p = gist_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should have a name", op);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name, "opcode {:#x}", op);
        }
    }

    #[test]
    fn identify_masks_info_bits() {
        // The XLR_INFO_MASK high bits are ignored before matching.
        let p = gist_identify(XLOG_GIST_PAGE_SPLIT | 0x0F);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "PAGE_SPLIT");
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0x40 / 0x50 are retired opcodes; no name after masking.
        assert!(gist_identify(0x40).is_null());
        assert!(gist_identify(0x50).is_null());
        assert!(gist_identify(0x80).is_null());
    }

    #[test]
    fn page_update_struct_size() {
        // gistxlogPageUpdate is two uint16.
        assert_eq!(core::mem::size_of::<gistxlogPageUpdate>(), 4);
    }

    #[test]
    fn sizeof_macros_match_layout() {
        // PageDelete: FullTransactionId (8) + OffsetNumber (2), padded to 8 in
        // the struct, but SizeOf is offsetof(downlinkOffset)+sizeof = 8 + 2.
        assert_eq!(
            SizeOfGistxlogPageDelete,
            core::mem::offset_of!(gistxlogPageDelete, downlinkOffset) + 2
        );
        // PageReuse: up to and including isCatalogRel (bool).
        assert_eq!(
            SizeOfGistxlogPageReuse,
            core::mem::offset_of!(gistxlogPageReuse, isCatalogRel) + 1
        );
        // Delete: fixed header size (flexible offsets array begins here).
        assert_eq!(SizeOfGistxlogDelete, core::mem::size_of::<gistxlogDelete>());
    }
}
