//! Translation of postgres/src/backend/access/rmgrdesc/seqdesc.c
//!                + the xl_seq_rec record struct and XLOG_SEQ_LOG opcode it
//!                  reads from postgres/src/include/commands/sequence.h
//!
//! rmgr descriptor routines for commands/sequence.c (used by pg_waldump).
//! seq_desc casts the WAL record payload to xl_seq_rec (the only sequence WAL
//! record) and appends a human-readable summary of the relation locator;
//! seq_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   c.h types                -> uint8, Oid
//!   common/relpath.h         -> RelFileNumber
//!   storage/relfilelocator.h -> RelFileLocator (spcOid/dbOid/relNumber)
//!   commands/sequence.h      -> xl_seq_rec, XLOG_SEQ_LOG
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The xl_seq_rec layout, the XLOG_SEQ_LOG opcode value, and the seq_identify
//! name table are REAL (faithful to sequence.h / seqdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK};
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from c.h / common/relpath.h)
// ---------------------------------------------------------------------------

pub type RelFileNumber = Oid;

/// RelFileLocator (storage/relfilelocator.h): physical locator of a relation.
/// Note: there must be no unused padding bytes in this struct (all Oid-typed
/// fields), since it is used in hashtable keys.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,    // tablespace
    pub dbOid: Oid,     // database
    pub relNumber: RelFileNumber, // relation
}

// ---------------------------------------------------------------------------
// Sequence WAL records / opcodes (from commands/sequence.h)
// ---------------------------------------------------------------------------

/// XLOG stuff: the only sequence WAL record opcode.
pub const XLOG_SEQ_LOG: uint8 = 0x00;

/// WAL record for a sequence update (XLOG_SEQ_LOG).
/// The sequence tuple data follows at the end of the record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_seq_rec {
    pub locator: RelFileLocator,
    // SEQUENCE TUPLE DATA FOLLOWS AT THE END
}

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// rmgr "desc" callback for sequence WAL records (pg_waldump).
///
/// # Safety
/// `record` is an opaque WAL reader pointer; the payload it points at must be a
/// valid sequence WAL record when the stub accessors are replaced by real ones.
pub unsafe fn seq_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let xlrec = rec as *const xl_seq_rec;

    if info == XLOG_SEQ_LOG {
        appendStringInfo!(
            buf,
            "rel {}/{}/{}",
            (*xlrec).locator.spcOid,
            (*xlrec).locator.dbOid,
            (*xlrec).locator.relNumber
        );
    }
}

/// rmgr "identify" callback: maps an info byte to the record-type name, or null
/// for unrecognized opcodes.
pub fn seq_identify(info: uint8) -> *const c_char {
    let mut id: *const c_char = null();

    match info & !XLR_INFO_MASK {
        XLOG_SEQ_LOG => id = c"LOG".as_ptr(),
        _ => {}
    }

    id
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn identify_known_and_unknown() {
        unsafe {
            let log = seq_identify(XLOG_SEQ_LOG);
            assert!(!log.is_null());
            assert_eq!(CStr::from_ptr(log).to_bytes(), b"LOG");

            // The low XLR_INFO_MASK (0x0F) flag bits must be masked off before
            // matching the opcode (the high 4 bits).
            let log_masked = seq_identify(XLOG_SEQ_LOG | 0x0F);
            assert!(!log_masked.is_null());
            assert_eq!(CStr::from_ptr(log_masked).to_bytes(), b"LOG");

            // An opcode (high 4 bits) the rmgr doesn't define has no name.
            assert!(seq_identify(0x90).is_null());
        }
    }

    #[test]
    fn layout_sanity() {
        // RelFileLocator is three Oid (u32) fields with no padding.
        assert_eq!(core::mem::size_of::<RelFileLocator>(), 12);
        assert_eq!(core::mem::align_of::<RelFileLocator>(), 4);
        // xl_seq_rec is exactly a RelFileLocator (tuple data trails the record).
        assert_eq!(core::mem::size_of::<xl_seq_rec>(), 12);
        assert_eq!(core::mem::offset_of!(xl_seq_rec, locator), 0);
        assert_eq!(core::mem::offset_of!(RelFileLocator, spcOid), 0);
        assert_eq!(core::mem::offset_of!(RelFileLocator, dbOid), 4);
        assert_eq!(core::mem::offset_of!(RelFileLocator, relNumber), 8);
    }
}
