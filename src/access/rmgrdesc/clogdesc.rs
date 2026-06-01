//! Translation of postgres/src/backend/access/rmgrdesc/clogdesc.c
//!                + the xl_clog_truncate record struct and CLOG_* opcodes it
//!                  reads from postgres/src/include/access/clog.h
//!
//! rmgr descriptor routines for CLOG (the transaction-commit-log manager),
//! used by pg_waldump. clog_desc inspects the record's info byte and renders a
//! human-readable summary of the WAL payload; clog_identify maps an opcode to
//! its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h     -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   access/clog.h        -> xl_clog_truncate, CLOG_ZEROPAGE, CLOG_TRUNCATE
//!   c.h types            -> uint8, int64, Oid, TransactionId
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layout, the CLOG_* opcode values, and the clog_identify name
//! table are REAL (faithful to clog.h / clogdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// Base types (from c.h)
// ---------------------------------------------------------------------------

/// Transaction identifier (c.h: typedef uint32 TransactionId).
pub type TransactionId = uint32;

// ---------------------------------------------------------------------------
// CLOG XLOG opcodes + record struct (access/clog.h)
// ---------------------------------------------------------------------------

pub const CLOG_ZEROPAGE: uint8 = 0x00;
pub const CLOG_TRUNCATE: uint8 = 0x10;

/// WAL record for CLOG_TRUNCATE (access/clog.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_clog_truncate {
    pub pageno: int64,
    pub oldestXact: TransactionId,
    pub oldestXactDb: Oid,
}

// ---------------------------------------------------------------------------
// clog_desc / clog_identify (clogdesc.c)
// ---------------------------------------------------------------------------

/// rmgr "desc" routine: append a human-readable summary of the CLOG WAL record
/// to `buf`. Mirrors clogdesc.c clog_desc exactly (same labels, same order).
///
/// # Safety
/// `record` is an opaque WAL-reader pointer; the data pointer it yields is read
/// as the opcode-specific payload. With the stubbed accessors the data pointer
/// is null, so this is effectively inert until a real reader is wired in.
pub unsafe fn clog_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == CLOG_ZEROPAGE {
        let mut pageno: int64 = 0;
        core::ptr::copy_nonoverlapping(
            rec as *const u8,
            (&mut pageno as *mut int64) as *mut u8,
            core::mem::size_of::<int64>(),
        );
        appendStringInfo!(buf, "page {}", pageno);
    } else if info == CLOG_TRUNCATE {
        let mut xlrec: xl_clog_truncate =
            core::mem::MaybeUninit::<xl_clog_truncate>::zeroed().assume_init();
        core::ptr::copy_nonoverlapping(
            rec as *const u8,
            (&mut xlrec as *mut xl_clog_truncate) as *mut u8,
            core::mem::size_of::<xl_clog_truncate>(),
        );
        appendStringInfo!(buf, "page {}; oldestXact {}", xlrec.pageno, xlrec.oldestXact);
    }
}

/// rmgr "identify" routine: map a CLOG opcode (after masking off XLR_INFO_MASK)
/// to its name string, or null for an unknown opcode. Mirrors clog_identify.
pub fn clog_identify(info: uint8) -> *const c_char {
    let id: *const c_char = match info & !XLR_INFO_MASK {
        CLOG_ZEROPAGE => c"ZEROPAGE".as_ptr(),
        CLOG_TRUNCATE => c"TRUNCATE".as_ptr(),
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
            (CLOG_ZEROPAGE, "ZEROPAGE"),
            (CLOG_TRUNCATE, "TRUNCATE"),
        ];
        for &(op, name) in cases {
            let p = clog_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should have a name", op);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name, "opcode {:#x}", op);
        }
    }

    #[test]
    fn identify_respects_info_mask() {
        // Low XLR_INFO_MASK bits are masked off before lookup.
        let p = clog_identify(CLOG_TRUNCATE | XLR_INFO_MASK);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "TRUNCATE");
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0x20 has no CLOG opcode after masking off XLR_INFO_MASK.
        assert!(clog_identify(0x20).is_null());
    }

    #[test]
    fn xl_clog_truncate_layout() {
        // Real #[repr(C)] layout: int64 forces 8-byte alignment, so the struct
        // is 16 bytes (pageno@0, oldestXact@8, oldestXactDb@12, no tail pad
        // needed since the two uint32 fields fill the trailing 8 bytes).
        assert_eq!(core::mem::offset_of!(xl_clog_truncate, pageno), 0);
        assert_eq!(core::mem::offset_of!(xl_clog_truncate, oldestXact), 8);
        assert_eq!(core::mem::offset_of!(xl_clog_truncate, oldestXactDb), 12);
        assert_eq!(core::mem::size_of::<xl_clog_truncate>(), 16);
        assert_eq!(core::mem::align_of::<xl_clog_truncate>(), 8);
    }
}
