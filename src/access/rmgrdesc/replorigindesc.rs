//! Translation of postgres/src/backend/access/rmgrdesc/replorigindesc.c
//!                + the xl_replorigin_set / xl_replorigin_drop record structs
//!                  and XLOG_REPLORIGIN_SET / XLOG_REPLORIGIN_DROP opcodes it
//!                  reads from postgres/src/include/replication/origin.h
//!
//! rmgr descriptor routines for replication/logical/origin.c (used by
//! pg_waldump). replorigin_desc casts the WAL record payload to the matching
//! replication-origin record struct and appends a human-readable summary;
//! replorigin_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   c.h types                -> uint8, uint16, uint32
//!   access/xlogdefs.h        -> XLogRecPtr (uint64), RepOriginId (uint16),
//!                               LSN_FORMAT_ARGS (split a 64-bit LSN into two
//!                               uint32 halves, printed "%X/%X")
//!   replication/origin.h     -> xl_replorigin_set, xl_replorigin_drop,
//!                               XLOG_REPLORIGIN_SET, XLOG_REPLORIGIN_DROP
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!
//! The record layouts, the opcode values, and the replorigin_identify name
//! table are REAL (faithful to origin.h / replorigindesc.c). The desc output
//! text is reproduced exactly.

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from access/xlogdefs.h)
// ---------------------------------------------------------------------------

/// Pointer/offset into the WAL stream (access/xlogdefs.h).
pub type XLogRecPtr = uint64;

/// Identifier for a replication origin (access/xlogdefs.h).
pub type RepOriginId = uint16;

/// LSN_FORMAT_ARGS(lsn): split a 64-bit LSN into (high uint32, low uint32).
/// The C macro expands inside an appendStringInfo "%X/%X" - i.e. uppercase hex.
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (uint32, uint32) {
    ((lsn >> 32) as uint32, lsn as uint32)
}

// ---------------------------------------------------------------------------
// Replication-origin WAL records / opcodes (from replication/origin.h)
// ---------------------------------------------------------------------------

/// XLOG stuff: opcodes for replication-origin WAL records.
pub const XLOG_REPLORIGIN_SET: uint8 = 0x00;
pub const XLOG_REPLORIGIN_DROP: uint8 = 0x10;

/// WAL record for XLOG_REPLORIGIN_SET: advance a replication origin's progress.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_replorigin_set {
    pub remote_lsn: XLogRecPtr,
    pub node_id: RepOriginId,
    pub force: bool,
}

/// WAL record for XLOG_REPLORIGIN_DROP: drop a replication origin.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_replorigin_drop {
    pub node_id: RepOriginId,
}

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// rmgr "desc" callback for replication-origin WAL records (pg_waldump).
///
/// # Safety
/// `record` is an opaque WAL reader pointer; the payload it points at must be a
/// valid replication-origin WAL record when the stub accessors are replaced by
/// real ones.
pub unsafe fn replorigin_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_REPLORIGIN_SET => {
            let xlrec = rec as *const xl_replorigin_set;
            let (lsn_hi, lsn_lo) = LSN_FORMAT_ARGS((*xlrec).remote_lsn);
            // C prints force (a bool) with %d -> 0/1.
            appendStringInfo!(
                buf,
                "set {}; lsn {:X}/{:X}; force: {}",
                (*xlrec).node_id,
                lsn_hi,
                lsn_lo,
                (*xlrec).force as c_int
            );
        }
        XLOG_REPLORIGIN_DROP => {
            let xlrec = rec as *const xl_replorigin_drop;
            appendStringInfo!(buf, "drop {}", (*xlrec).node_id);
        }
        _ => {}
    }
}

/// rmgr "identify" callback: maps an info byte to the record-type name, or null
/// for unrecognized opcodes.
pub fn replorigin_identify(info: uint8) -> *const c_char {
    match info {
        XLOG_REPLORIGIN_SET => c"SET".as_ptr(),
        XLOG_REPLORIGIN_DROP => c"DROP".as_ptr(),
        _ => null(),
    }
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
        let set = replorigin_identify(XLOG_REPLORIGIN_SET);
        assert!(!set.is_null());
        assert_eq!(unsafe { CStr::from_ptr(set) }.to_bytes(), b"SET");

        let drop = replorigin_identify(XLOG_REPLORIGIN_DROP);
        assert!(!drop.is_null());
        assert_eq!(unsafe { CStr::from_ptr(drop) }.to_bytes(), b"DROP");

        // Note: replorigin_identify (faithful to the C) matches the full info
        // byte, so the bare opcode values resolve; an undefined opcode is null.
        assert!(replorigin_identify(0x90).is_null());
        assert!(replorigin_identify(0x20).is_null());
    }

    #[test]
    fn lsn_format_args_splits_halves() {
        let lsn: XLogRecPtr = 0x0123_4567_89AB_CDEF;
        let (hi, lo) = LSN_FORMAT_ARGS(lsn);
        assert_eq!(hi, 0x0123_4567);
        assert_eq!(lo, 0x89AB_CDEF);
    }

    #[test]
    fn layout_sanity() {
        // xl_replorigin_set: XLogRecPtr (u64, 8-align) then RepOriginId (u16)
        // then bool. The u64 forces 8-byte alignment; trailing padding rounds
        // the struct size up to 16.
        assert_eq!(core::mem::offset_of!(xl_replorigin_set, remote_lsn), 0);
        assert_eq!(core::mem::offset_of!(xl_replorigin_set, node_id), 8);
        assert_eq!(core::mem::offset_of!(xl_replorigin_set, force), 10);
        assert_eq!(core::mem::align_of::<xl_replorigin_set>(), 8);
        assert_eq!(core::mem::size_of::<xl_replorigin_set>(), 16);

        // xl_replorigin_drop: a single RepOriginId (u16).
        assert_eq!(core::mem::offset_of!(xl_replorigin_drop, node_id), 0);
        assert_eq!(core::mem::size_of::<xl_replorigin_drop>(), 2);
    }
}
