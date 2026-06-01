//! spgdesc.rs
//!   rmgr descriptor routines for access/spgist/spgxlog.c
//!
//! 1:1 translation of src/backend/access/rmgrdesc/spgdesc.c, merged with the
//! `spgxlog*` WAL-record struct definitions and `XLOG_SPGIST_*` opcode constants
//! from src/include/access/spgxlog.h.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK};
use crate::lib::stringinfo::{appendStringInfoString, StringInfo};
use crate::storage::off::OffsetNumber;

// ---------------------------------------------------------------------------
// XLOG record types for SPGiST (spgxlog.h)
// ---------------------------------------------------------------------------

/* #define XLOG_SPGIST_CREATE_INDEX 0x00 */ // not used anymore
pub const XLOG_SPGIST_ADD_LEAF: uint8 = 0x10;
pub const XLOG_SPGIST_MOVE_LEAFS: uint8 = 0x20;
pub const XLOG_SPGIST_ADD_NODE: uint8 = 0x30;
pub const XLOG_SPGIST_SPLIT_TUPLE: uint8 = 0x40;
pub const XLOG_SPGIST_PICKSPLIT: uint8 = 0x50;
pub const XLOG_SPGIST_VACUUM_LEAF: uint8 = 0x60;
pub const XLOG_SPGIST_VACUUM_ROOT: uint8 = 0x70;
pub const XLOG_SPGIST_VACUUM_REDIRECT: uint8 = 0x80;

// ---------------------------------------------------------------------------
// WAL record structs (spgxlog.h)
//
// These mirror the C struct layouts. The flexible-array `offsets` members are
// omitted from the Rust structs (the trailing payload is accessed by offset,
// not as a field) -- spg_desc never touches them, matching the C source.
// ---------------------------------------------------------------------------

/// Some redo functions need an SpGistState, although only a few of its fields
/// need to be valid. `spgxlogState` carries the required info in xlog records.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogState {
    pub redirectXid: TransactionId,
    pub isBuild: bool,
}

/// Backup Blk 0: destination page for leaf tuple
/// Backup Blk 1: parent page (if any)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogAddLeaf {
    pub newPage: bool,                  // init dest page?
    pub storesNulls: bool,              // page is in the nulls tree?
    pub offnumLeaf: OffsetNumber,       // offset where leaf tuple gets placed
    pub offnumHeadLeaf: OffsetNumber,   // offset of head tuple in chain, if any

    pub offnumParent: OffsetNumber,     // where the parent downlink is, if any
    pub nodeI: uint16,
    // new leaf tuple follows (unaligned!)
}

/// Backup Blk 0: source leaf page
/// Backup Blk 1: destination leaf page
/// Backup Blk 2: parent page
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogMoveLeafs {
    pub nMoves: uint16,        // number of tuples moved from source page
    pub newPage: bool,         // init dest page?
    pub replaceDead: bool,     // are we replacing a DEAD source tuple?
    pub storesNulls: bool,     // pages are in the nulls tree?

    pub offnumParent: OffsetNumber, // where the parent downlink is
    pub nodeI: uint16,

    pub stateSrc: spgxlogState,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] payload follows
}

/// Backup Blk 0: original page
/// Backup Blk 1: where new tuple goes, if not same place
/// Backup Blk 2: where parent downlink is, if updated and different from
///               the old and new
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogAddNode {
    /// Offset of the original inner tuple, in the original page (backup blk 0).
    pub offnum: OffsetNumber,

    /// Offset of the new tuple, on the new page (backup blk 1). Invalid if we
    /// overwrote the old tuple in the original page.
    pub offnumNew: OffsetNumber,
    pub newPage: bool,         // init new page?

    /// Where is the parent downlink? parentBlk indicates which page it's on,
    /// offnumParent is the offset within the page. parentBlk values:
    ///   0: parent == original page
    ///   1: parent == new page
    ///   2: parent == different page (blk ref 2)
    ///  -1: parent not updated
    pub parentBlk: int8,
    pub offnumParent: OffsetNumber, // offset within the parent page

    pub nodeI: uint16,

    pub stateSrc: spgxlogState,
    // updated inner tuple follows (unaligned!)
}

/// Backup Blk 0: where the prefix tuple goes
/// Backup Blk 1: where the postfix tuple goes (if different page)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogSplitTuple {
    pub offnumPrefix: OffsetNumber,  // where the prefix tuple goes

    pub offnumPostfix: OffsetNumber, // where the postfix tuple goes
    pub newPage: bool,               // need to init that page?
    pub postfixBlkSame: bool,        // postfix put on same page as prefix?
    // new prefix inner tuple follows, then new postfix inner tuple (unaligned!)
}

/// Buffer references in the rdata array are:
/// Backup Blk 0: Src page (only if not root)
/// Backup Blk 1: Dest page (if used)
/// Backup Blk 2: Inner page
/// Backup Blk 3: Parent page (if any, and different from Inner)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogPickSplit {
    pub isRootSplit: bool,

    pub nDelete: uint16,       // n to delete from Src
    pub nInsert: uint16,       // n to insert on Src and/or Dest
    pub initSrc: bool,         // re-init the Src page?
    pub initDest: bool,        // re-init the Dest page?

    pub offnumInner: OffsetNumber, // where to put new inner tuple
    pub initInner: bool,           // re-init the Inner page?

    pub storesNulls: bool,     // pages are in the nulls tree?

    pub innerIsParent: bool,   // is parent the same as inner page?
    pub offnumParent: OffsetNumber, // where the parent downlink is, if any
    pub nodeI: uint16,

    pub stateSrc: spgxlogState,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] payload follows
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogVacuumLeaf {
    pub nDead: uint16,         // number of tuples to become DEAD
    pub nPlaceholder: uint16,  // number of tuples to become PLACEHOLDER
    pub nMove: uint16,         // number of tuples to move
    pub nChain: uint16,        // number of tuples to re-chain

    pub stateSrc: spgxlogState,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] payload follows
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogVacuumRoot {
    pub nDelete: uint16,       // number of tuples to delete (root-as-leaf page)

    pub stateSrc: spgxlogState,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] payload follows
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogVacuumRedirect {
    pub nToPlaceholder: uint16,        // number of redirects to make placeholders
    pub firstPlaceholder: OffsetNumber, // first placeholder tuple to remove
    pub snapshotConflictHorizon: TransactionId, // newest XID of removed redirects
    pub isCatalogRel: bool,            // handle recovery conflict during logical
                                       // decoding on standby
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER] payload follows
}

// SizeOf* macros (offsetof of the flexible-array member == size of the fixed
// part). Since we drop the FAM field, that is just size_of of the struct.
pub const SizeOfSpgxlogMoveLeafs: usize = core::mem::size_of::<spgxlogMoveLeafs>();
pub const SizeOfSpgxlogPickSplit: usize = core::mem::size_of::<spgxlogPickSplit>();
pub const SizeOfSpgxlogVacuumLeaf: usize = core::mem::size_of::<spgxlogVacuumLeaf>();
pub const SizeOfSpgxlogVacuumRoot: usize = core::mem::size_of::<spgxlogVacuumRoot>();
pub const SizeOfSpgxlogVacuumRedirect: usize = core::mem::size_of::<spgxlogVacuumRedirect>();

// ---------------------------------------------------------------------------
// spg_desc
// ---------------------------------------------------------------------------

/// # Safety
/// `record` must be a valid decoded XLOG record whose rmgr is SP-GiST and whose
/// payload matches the opcode in its info byte. `buf` must be a writable
/// StringInfo.
pub unsafe fn spg_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_SPGIST_ADD_LEAF => {
            let xlrec = &*(rec as *const spgxlogAddLeaf);

            appendStringInfo!(
                buf,
                "off: {}, headoff: {}, parentoff: {}, nodeI: {}",
                xlrec.offnumLeaf,
                xlrec.offnumHeadLeaf,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.newPage {
                appendStringInfoString(buf, c" (newpage)".as_ptr());
            }
            if xlrec.storesNulls {
                appendStringInfoString(buf, c" (nulls)".as_ptr());
            }
        }
        XLOG_SPGIST_MOVE_LEAFS => {
            let xlrec = &*(rec as *const spgxlogMoveLeafs);

            appendStringInfo!(
                buf,
                "nmoves: {}, parentoff: {}, nodeI: {}",
                xlrec.nMoves,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.newPage {
                appendStringInfoString(buf, c" (newpage)".as_ptr());
            }
            if xlrec.replaceDead {
                appendStringInfoString(buf, c" (replacedead)".as_ptr());
            }
            if xlrec.storesNulls {
                appendStringInfoString(buf, c" (nulls)".as_ptr());
            }
        }
        XLOG_SPGIST_ADD_NODE => {
            let xlrec = &*(rec as *const spgxlogAddNode);

            appendStringInfo!(
                buf,
                "off: {}, newoff: {}, parentBlk: {}, parentoff: {}, nodeI: {}",
                xlrec.offnum,
                xlrec.offnumNew,
                xlrec.parentBlk,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.newPage {
                appendStringInfoString(buf, c" (newpage)".as_ptr());
            }
        }
        XLOG_SPGIST_SPLIT_TUPLE => {
            let xlrec = &*(rec as *const spgxlogSplitTuple);

            appendStringInfo!(
                buf,
                "prefixoff: {}, postfixoff: {}",
                xlrec.offnumPrefix,
                xlrec.offnumPostfix
            );
            if xlrec.newPage {
                appendStringInfoString(buf, c" (newpage)".as_ptr());
            }
            if xlrec.postfixBlkSame {
                appendStringInfoString(buf, c" (same)".as_ptr());
            }
        }
        XLOG_SPGIST_PICKSPLIT => {
            let xlrec = &*(rec as *const spgxlogPickSplit);

            appendStringInfo!(
                buf,
                "ndelete: {}, ninsert: {}, inneroff: {}, parentoff: {}, nodeI: {}",
                xlrec.nDelete,
                xlrec.nInsert,
                xlrec.offnumInner,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.innerIsParent {
                appendStringInfoString(buf, c" (innerIsParent)".as_ptr());
            }
            if xlrec.storesNulls {
                appendStringInfoString(buf, c" (nulls)".as_ptr());
            }
            if xlrec.isRootSplit {
                appendStringInfoString(buf, c" (isRootSplit)".as_ptr());
            }
        }
        XLOG_SPGIST_VACUUM_LEAF => {
            let xlrec = &*(rec as *const spgxlogVacuumLeaf);

            appendStringInfo!(
                buf,
                "ndead: {}, nplaceholder: {}, nmove: {}, nchain: {}",
                xlrec.nDead,
                xlrec.nPlaceholder,
                xlrec.nMove,
                xlrec.nChain
            );
        }
        XLOG_SPGIST_VACUUM_ROOT => {
            let xlrec = &*(rec as *const spgxlogVacuumRoot);

            appendStringInfo!(buf, "ndelete: {}", xlrec.nDelete);
        }
        XLOG_SPGIST_VACUUM_REDIRECT => {
            let xlrec = &*(rec as *const spgxlogVacuumRedirect);

            appendStringInfo!(
                buf,
                "ntoplaceholder: {}, firstplaceholder: {}, snapshotConflictHorizon: {}, isCatalogRel: {}",
                xlrec.nToPlaceholder,
                xlrec.firstPlaceholder,
                xlrec.snapshotConflictHorizon,
                if xlrec.isCatalogRel { 'T' } else { 'F' }
            );
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// spg_identify
// ---------------------------------------------------------------------------

/// Return a static name for the SP-GiST WAL opcode in `info`, or NULL if the
/// opcode is unrecognized (mirrors the C `const char *` contract).
pub fn spg_identify(info: uint8) -> *const c_char {
    let mut id: *const c_char = null();

    match info & !XLR_INFO_MASK {
        XLOG_SPGIST_ADD_LEAF => id = c"ADD_LEAF".as_ptr(),
        XLOG_SPGIST_MOVE_LEAFS => id = c"MOVE_LEAFS".as_ptr(),
        XLOG_SPGIST_ADD_NODE => id = c"ADD_NODE".as_ptr(),
        XLOG_SPGIST_SPLIT_TUPLE => id = c"SPLIT_TUPLE".as_ptr(),
        XLOG_SPGIST_PICKSPLIT => id = c"PICKSPLIT".as_ptr(),
        XLOG_SPGIST_VACUUM_LEAF => id = c"VACUUM_LEAF".as_ptr(),
        XLOG_SPGIST_VACUUM_ROOT => id = c"VACUUM_ROOT".as_ptr(),
        XLOG_SPGIST_VACUUM_REDIRECT => id = c"VACUUM_REDIRECT".as_ptr(),
        _ => {}
    }

    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    unsafe fn id_str(info: uint8) -> Option<&'static str> {
        let p = spg_identify(info);
        if p.is_null() {
            None
        } else {
            Some(CStr::from_ptr(p).to_str().unwrap())
        }
    }

    #[test]
    fn identify_add_leaf() {
        unsafe {
            assert_eq!(id_str(XLOG_SPGIST_ADD_LEAF), Some("ADD_LEAF"));
        }
    }

    #[test]
    fn identify_all_opcodes() {
        unsafe {
            assert_eq!(id_str(XLOG_SPGIST_MOVE_LEAFS), Some("MOVE_LEAFS"));
            assert_eq!(id_str(XLOG_SPGIST_ADD_NODE), Some("ADD_NODE"));
            assert_eq!(id_str(XLOG_SPGIST_SPLIT_TUPLE), Some("SPLIT_TUPLE"));
            assert_eq!(id_str(XLOG_SPGIST_PICKSPLIT), Some("PICKSPLIT"));
            assert_eq!(id_str(XLOG_SPGIST_VACUUM_LEAF), Some("VACUUM_LEAF"));
            assert_eq!(id_str(XLOG_SPGIST_VACUUM_ROOT), Some("VACUUM_ROOT"));
            assert_eq!(id_str(XLOG_SPGIST_VACUUM_REDIRECT), Some("VACUUM_REDIRECT"));
        }
    }

    #[test]
    fn identify_masks_info_low_bits() {
        // The low XLR_INFO_MASK bits must be ignored.
        unsafe {
            assert_eq!(id_str(XLOG_SPGIST_ADD_LEAF | XLR_INFO_MASK), Some("ADD_LEAF"));
        }
    }

    #[test]
    fn identify_unknown_returns_null() {
        unsafe {
            assert_eq!(id_str(0x00), None);
        }
    }
}
