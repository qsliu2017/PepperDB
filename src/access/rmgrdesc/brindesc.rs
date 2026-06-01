//! brindesc.rs
//!   rmgr descriptor routines for BRIN indexes
//!
//! Translated 1:1 from postgres/src/backend/access/rmgrdesc/brindesc.c,
//! with the WAL record structs merged in from access/brin_xlog.h.
//!
//! Used by pg_waldump to render BRIN WAL records.

use crate::prelude::*;
use crate::appendStringInfo;
use crate::lib::stringinfo::StringInfo;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// WAL record opcode definitions for BRIN's WAL operations (from brin_xlog.h).
//
// XLOG allows storing some information in the high 4 bits of the log
// record xl_info field.
// ---------------------------------------------------------------------------
const XLOG_BRIN_CREATE_INDEX: uint8 = 0x00;
const XLOG_BRIN_INSERT: uint8 = 0x10;
const XLOG_BRIN_UPDATE: uint8 = 0x20;
const XLOG_BRIN_SAMEPAGE_UPDATE: uint8 = 0x30;
const XLOG_BRIN_REVMAP_EXTEND: uint8 = 0x40;
const XLOG_BRIN_DESUMMARIZE: uint8 = 0x50;

const XLOG_BRIN_OPMASK: uint8 = 0x70;

// When we insert the first item on a new page, we restore the entire page in
// redo.
const XLOG_BRIN_INIT_PAGE: uint8 = 0x80;

// ---------------------------------------------------------------------------
// WAL record structs (real layouts from brin_xlog.h).
// ---------------------------------------------------------------------------

/// This is what we need to know about a BRIN index create.
///
/// Backup block 0: metapage
#[repr(C)]
pub struct xl_brin_createidx {
    pub pagesPerRange: BlockNumber,
    pub version: uint16,
}

/// This is what we need to know about a BRIN tuple insert.
///
/// Backup block 0: main page, block data is the new BrinTuple.
/// Backup block 1: revmap page
#[repr(C)]
pub struct xl_brin_insert {
    pub heapBlk: BlockNumber,

    /// extra information needed to update the revmap
    pub pagesPerRange: BlockNumber,

    /// offset number in the main page to insert the tuple to.
    pub offnum: OffsetNumber,
}

/// A cross-page update is the same as an insert, but also stores information
/// about the old tuple.
///
/// Backup block 0: new page, block data includes the new BrinTuple.
/// Backup block 1: revmap page
/// Backup block 2: old page
#[repr(C)]
pub struct xl_brin_update {
    /// offset number of old tuple on old page
    pub oldOffnum: OffsetNumber,

    pub insert: xl_brin_insert,
}

/// This is what we need to know about a BRIN tuple samepage update.
///
/// Backup block 0: updated page, with new BrinTuple as block data
#[repr(C)]
pub struct xl_brin_samepage_update {
    pub offnum: OffsetNumber,
}

/// This is what we need to know about a revmap extension.
///
/// Backup block 0: metapage
/// Backup block 1: new revmap page
#[repr(C)]
pub struct xl_brin_revmap_extend {
    /// XXX: This is actually redundant - the block number is stored as part of
    /// backup block 1.
    pub targetBlk: BlockNumber,
}

/// This is what we need to know about a range de-summarization.
///
/// Backup block 0: revmap page
/// Backup block 1: regular page
#[repr(C)]
pub struct xl_brin_desummarize {
    pub pagesPerRange: BlockNumber,
    /// page number location to set to invalid
    pub heapBlk: BlockNumber,
    /// offset of item to delete in regular index page
    pub regOffset: OffsetNumber,
}

// ---------------------------------------------------------------------------
// Descriptor routines.
// ---------------------------------------------------------------------------

pub unsafe fn brin_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let mut info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    info &= XLOG_BRIN_OPMASK;
    if info == XLOG_BRIN_CREATE_INDEX {
        let xlrec = rec as *const xl_brin_createidx;

        appendStringInfo!(
            buf,
            "v{} pagesPerRange {}",
            (*xlrec).version,
            (*xlrec).pagesPerRange
        );
    } else if info == XLOG_BRIN_INSERT {
        let xlrec = rec as *const xl_brin_insert;

        appendStringInfo!(
            buf,
            "heapBlk {} pagesPerRange {} offnum {}",
            (*xlrec).heapBlk,
            (*xlrec).pagesPerRange,
            (*xlrec).offnum
        );
    } else if info == XLOG_BRIN_UPDATE {
        let xlrec = rec as *const xl_brin_update;

        appendStringInfo!(
            buf,
            "heapBlk {} pagesPerRange {} old offnum {}, new offnum {}",
            (*xlrec).insert.heapBlk,
            (*xlrec).insert.pagesPerRange,
            (*xlrec).oldOffnum,
            (*xlrec).insert.offnum
        );
    } else if info == XLOG_BRIN_SAMEPAGE_UPDATE {
        let xlrec = rec as *const xl_brin_samepage_update;

        appendStringInfo!(buf, "offnum {}", (*xlrec).offnum);
    } else if info == XLOG_BRIN_REVMAP_EXTEND {
        let xlrec = rec as *const xl_brin_revmap_extend;

        appendStringInfo!(buf, "targetBlk {}", (*xlrec).targetBlk);
    } else if info == XLOG_BRIN_DESUMMARIZE {
        let xlrec = rec as *const xl_brin_desummarize;

        appendStringInfo!(
            buf,
            "pagesPerRange {}, heapBlk {}, page offset {}",
            (*xlrec).pagesPerRange,
            (*xlrec).heapBlk,
            (*xlrec).regOffset
        );
    }
}

pub fn brin_identify(info: uint8) -> *const c_char {
    let id: *const c_char = match info & !XLR_INFO_MASK {
        x if x == XLOG_BRIN_CREATE_INDEX => c"CREATE_INDEX".as_ptr(),
        x if x == XLOG_BRIN_INSERT => c"INSERT".as_ptr(),
        x if x == (XLOG_BRIN_INSERT | XLOG_BRIN_INIT_PAGE) => c"INSERT+INIT".as_ptr(),
        x if x == XLOG_BRIN_UPDATE => c"UPDATE".as_ptr(),
        x if x == (XLOG_BRIN_UPDATE | XLOG_BRIN_INIT_PAGE) => c"UPDATE+INIT".as_ptr(),
        x if x == XLOG_BRIN_SAMEPAGE_UPDATE => c"SAMEPAGE_UPDATE".as_ptr(),
        x if x == XLOG_BRIN_REVMAP_EXTEND => c"REVMAP_EXTEND".as_ptr(),
        x if x == XLOG_BRIN_DESUMMARIZE => c"DESUMMARIZE".as_ptr(),
        _ => null(),
    };

    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    unsafe fn id_str(info: uint8) -> Option<&'static str> {
        let p = brin_identify(info);
        if p.is_null() {
            None
        } else {
            Some(CStr::from_ptr(p).to_str().unwrap())
        }
    }

    #[test]
    fn identify_known_opcodes() {
        unsafe {
            assert_eq!(id_str(XLOG_BRIN_CREATE_INDEX), Some("CREATE_INDEX"));
            assert_eq!(id_str(XLOG_BRIN_INSERT), Some("INSERT"));
            assert_eq!(
                id_str(XLOG_BRIN_INSERT | XLOG_BRIN_INIT_PAGE),
                Some("INSERT+INIT")
            );
            assert_eq!(id_str(XLOG_BRIN_UPDATE), Some("UPDATE"));
            assert_eq!(
                id_str(XLOG_BRIN_UPDATE | XLOG_BRIN_INIT_PAGE),
                Some("UPDATE+INIT")
            );
            assert_eq!(id_str(XLOG_BRIN_SAMEPAGE_UPDATE), Some("SAMEPAGE_UPDATE"));
            assert_eq!(id_str(XLOG_BRIN_REVMAP_EXTEND), Some("REVMAP_EXTEND"));
            assert_eq!(id_str(XLOG_BRIN_DESUMMARIZE), Some("DESUMMARIZE"));
        }
    }

    #[test]
    fn identify_unknown_is_null() {
        assert!(brin_identify(0x60).is_null());
    }

    #[test]
    fn struct_size_sanity() {
        // xl_brin_insert: 2x BlockNumber (u32) + OffsetNumber (u16), C layout
        // pads to 12 bytes. xl_brin_createidx: BlockNumber + uint16 -> 8 bytes.
        assert_eq!(std::mem::size_of::<xl_brin_createidx>(), 8);
        assert_eq!(std::mem::size_of::<xl_brin_samepage_update>(), 2);
        assert_eq!(
            std::mem::size_of::<xl_brin_insert>(),
            std::mem::size_of::<BlockNumber>() * 2 + std::mem::size_of::<OffsetNumber>() + 2
        );
    }
}
