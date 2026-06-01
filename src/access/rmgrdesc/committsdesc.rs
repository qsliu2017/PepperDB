//! Translation of postgres/src/backend/access/rmgrdesc/committsdesc.c
//!                + the WAL record structs and COMMIT_TS_* opcodes it reads
//!                  from postgres/src/include/access/commit_ts.h
//!
//! rmgr descriptor routines for the commit-timestamp SLRU manager, used by
//! pg_waldump. commit_ts_desc inspects the record's info byte and renders a
//! human-readable summary of the WAL payload; commit_ts_identify maps an opcode
//! to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h      -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   access/commit_ts.h    -> xl_commit_ts_set, xl_commit_ts_truncate,
//!                            COMMIT_TS_ZEROPAGE, COMMIT_TS_TRUNCATE
//!   datatype/timestamp.h  -> TimestampTz (int64)
//!   replication/origin.h  -> RepOriginId (uint16)
//!   c.h types             -> uint8, int64, TransactionId
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the COMMIT_TS_* opcode values, and the commit_ts_identify
//! name table are REAL (faithful to commit_ts.h / committsdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// Base types (from c.h / datatype/timestamp.h / replication/origin.h)
// ---------------------------------------------------------------------------

/// Transaction identifier (c.h: typedef uint32 TransactionId).
pub type TransactionId = uint32;

/// Timestamp with time zone (datatype/timestamp.h: typedef int64 TimestampTz).
pub type TimestampTz = int64;

/// Replication-origin identifier (replication/origin.h: typedef uint16 RepOriginId).
pub type RepOriginId = uint16;

// ---------------------------------------------------------------------------
// Commit-timestamp XLOG opcodes + record structs (access/commit_ts.h)
// ---------------------------------------------------------------------------

pub const COMMIT_TS_ZEROPAGE: uint8 = 0x00;
pub const COMMIT_TS_TRUNCATE: uint8 = 0x10;

/// WAL record for setting commit-timestamp data (access/commit_ts.h).
/// The main xact's subxact Xids follow this struct in the WAL payload.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_commit_ts_set {
    pub timestamp: TimestampTz,
    pub nodeid: RepOriginId,
    pub mainxid: TransactionId,
    // subxact Xids follow
}

/// SizeOfCommitTsSet = offsetof(xl_commit_ts_set, mainxid) + sizeof(TransactionId)
/// (access/commit_ts.h). Uses offset_of! so it tracks the real #[repr(C)] layout
/// rather than size_of (which would include trailing padding).
pub const SizeOfCommitTsSet: usize =
    core::mem::offset_of!(xl_commit_ts_set, mainxid) + core::mem::size_of::<TransactionId>();

/// WAL record for COMMIT_TS_TRUNCATE (access/commit_ts.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_commit_ts_truncate {
    pub pageno: int64,
    pub oldestXid: TransactionId,
}

/// SizeOfCommitTsTruncate = offsetof(xl_commit_ts_truncate, oldestXid) +
/// sizeof(TransactionId) (access/commit_ts.h).
pub const SizeOfCommitTsTruncate: usize =
    core::mem::offset_of!(xl_commit_ts_truncate, oldestXid) + core::mem::size_of::<TransactionId>();

// ---------------------------------------------------------------------------
// commit_ts_desc / commit_ts_identify (committsdesc.c)
// ---------------------------------------------------------------------------

/// rmgr "desc" routine: append a human-readable summary of the commit-timestamp
/// WAL record to `buf`. Mirrors committsdesc.c commit_ts_desc exactly (same
/// labels, same order, same output text).
///
/// # Safety
/// `record` is an opaque WAL-reader pointer; the data pointer it yields is read
/// as the opcode-specific payload. With the stubbed accessors the data pointer
/// is null, so this is effectively inert until a real reader is wired in.
pub unsafe fn commit_ts_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == COMMIT_TS_ZEROPAGE {
        let mut pageno: int64 = 0;
        core::ptr::copy_nonoverlapping(
            rec as *const u8,
            (&mut pageno as *mut int64) as *mut u8,
            core::mem::size_of::<int64>(),
        );
        appendStringInfo!(buf, "{}", pageno);
    } else if info == COMMIT_TS_TRUNCATE {
        let trunc = rec as *const xl_commit_ts_truncate;
        appendStringInfo!(
            buf,
            "pageno {}, oldestXid {}",
            (*trunc).pageno,
            (*trunc).oldestXid
        );
    }
}

/// rmgr "identify" routine: map a commit-timestamp opcode to its name string,
/// or null for an unknown opcode. Mirrors commit_ts_identify.
pub fn commit_ts_identify(info: uint8) -> *const c_char {
    let id: *const c_char = match info {
        COMMIT_TS_ZEROPAGE => c"ZEROPAGE".as_ptr(),
        COMMIT_TS_TRUNCATE => c"TRUNCATE".as_ptr(),
        _ => null(),
    };
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (COMMIT_TS_ZEROPAGE, "ZEROPAGE"),
            (COMMIT_TS_TRUNCATE, "TRUNCATE"),
        ];
        for &(op, name) in cases {
            let p = commit_ts_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should have a name", op);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name, "opcode {:#x}", op);
        }
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0x90 is a high-nibble opcode the commit-ts rmgr does not define.
        assert!(commit_ts_identify(0x90).is_null());
    }

    #[test]
    fn xl_commit_ts_truncate_layout() {
        // Real #[repr(C)] layout: int64 forces 8-byte alignment. pageno@0,
        // oldestXid@8 (uint32); 4 bytes of trailing pad -> total 16 bytes.
        assert_eq!(core::mem::offset_of!(xl_commit_ts_truncate, pageno), 0);
        assert_eq!(core::mem::offset_of!(xl_commit_ts_truncate, oldestXid), 8);
        assert_eq!(core::mem::size_of::<xl_commit_ts_truncate>(), 16);
        assert_eq!(core::mem::align_of::<xl_commit_ts_truncate>(), 8);
        // SizeOfCommitTsTruncate excludes trailing pad: 8 + 4 = 12.
        assert_eq!(SizeOfCommitTsTruncate, 12);
    }

    #[test]
    fn xl_commit_ts_set_layout() {
        // timestamp(int64)@0, nodeid(uint16)@8, mainxid(uint32)@12 (4-byte
        // aligned after 2 bytes of pad). SizeOfCommitTsSet = 12 + 4 = 16.
        assert_eq!(core::mem::offset_of!(xl_commit_ts_set, timestamp), 0);
        assert_eq!(core::mem::offset_of!(xl_commit_ts_set, nodeid), 8);
        assert_eq!(core::mem::offset_of!(xl_commit_ts_set, mainxid), 12);
        assert_eq!(SizeOfCommitTsSet, 16);
    }
}
