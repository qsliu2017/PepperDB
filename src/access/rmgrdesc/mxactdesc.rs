//! Translation of postgres/src/backend/access/rmgrdesc/mxactdesc.c
//!                + the MultiXact WAL record structs / opcodes and the
//!                  MultiXactMember / MultiXactStatus definitions it reads from
//!                  postgres/src/include/access/multixact.h
//!
//! rmgr descriptor routines for the MultiXact SLRUs (used by pg_waldump).
//! multixact_desc casts the WAL record payload to the appropriate struct
//! (selected by the record's info byte) and appends a human-readable summary;
//! multixact_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo,
//!                               appendStringInfo!, appendStringInfoString)
//!   access/multixact.h       -> MultiXactId/MultiXactOffset/MultiXactStatus,
//!                               MultiXactMember, xl_multixact_* records,
//!                               XLOG_MULTIXACT_* opcodes
//!   c.h types                -> uint8/int32/int64, Oid, TransactionId
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the XLOG_MULTIXACT_* opcode values, the
//! MultiXactStatus enum values, and the multixact_identify name table are REAL
//! (faithful to multixact.h / mxactdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};
use crate::lib::stringinfo::{appendStringInfoString, StringInfo};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from c.h / access/transam.h)
// ---------------------------------------------------------------------------

/// MultiXactId and MultiXactOffset are 32-bit, like TransactionId.
pub type MultiXactId = TransactionId;
pub type MultiXactOffset = uint32;

// ---------------------------------------------------------------------------
// MultiXact lock modes ("status") and member (access/multixact.h)
// ---------------------------------------------------------------------------

/// Possible multixact lock modes ("status"). The first four modes are for
/// tuple locks (FOR KEY SHARE, FOR SHARE, FOR NO KEY UPDATE, FOR UPDATE); the
/// next two are used for update and delete modes.
pub type MultiXactStatus = c_int;

pub const MultiXactStatusForKeyShare: MultiXactStatus = 0x00;
pub const MultiXactStatusForShare: MultiXactStatus = 0x01;
pub const MultiXactStatusForNoKeyUpdate: MultiXactStatus = 0x02;
pub const MultiXactStatusForUpdate: MultiXactStatus = 0x03;
/// an update that doesn't touch "key" columns
pub const MultiXactStatusNoKeyUpdate: MultiXactStatus = 0x04;
/// other updates, and delete
pub const MultiXactStatusUpdate: MultiXactStatus = 0x05;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: MultiXactStatus,
}

// ---------------------------------------------------------------------------
// MultiXact-related XLOG entries (access/multixact.h)
// ---------------------------------------------------------------------------

pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: uint8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: uint8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: uint8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: uint8 = 0x30;

/// XLOG_MULTIXACT_CREATE_ID: a newly-created MultiXact and its members.
/// `members` is a FLEXIBLE_ARRAY_MEMBER in C; modelled here as a zero-length
/// array so the struct size matches offsetof(xl_multixact_create, members)
/// (SizeOfMultiXactCreate).
#[repr(C)]
pub struct xl_multixact_create {
    pub mid: MultiXactId,        // new MultiXact's ID
    pub moff: MultiXactOffset,   // its starting offset in members file
    pub nmembers: int32,         // number of member XIDs
    pub members: [MultiXactMember; 0], // FLEXIBLE_ARRAY_MEMBER
}

/// SizeOfMultiXactCreate = offsetof(xl_multixact_create, members)
pub const SizeOfMultiXactCreate: usize = core::mem::offset_of!(xl_multixact_create, members);

/// XLOG_MULTIXACT_TRUNCATE_ID: a MultiXact SLRU truncation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_multixact_truncate {
    pub oldestMultiDB: Oid,

    /// to-be-truncated range of multixact offsets
    pub startTruncOff: MultiXactId, // just for completeness' sake
    pub endTruncOff: MultiXactId,

    /// to-be-truncated range of multixact members
    pub startTruncMemb: MultiXactOffset,
    pub endTruncMemb: MultiXactOffset,
}

/// SizeOfMultiXactTruncate = sizeof(xl_multixact_truncate)
pub const SizeOfMultiXactTruncate: usize = core::mem::size_of::<xl_multixact_truncate>();

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// Append a single MultiXactMember ("<xid> (<mode>) ") to `buf`.
///
/// # Safety
/// `member` must point to a valid MultiXactMember.
unsafe fn out_member(buf: StringInfo, member: *const MultiXactMember) {
    appendStringInfo!(buf, "{} ", (*member).xid);
    match (*member).status {
        MultiXactStatusForKeyShare => appendStringInfoString(buf, c"(keysh) ".as_ptr()),
        MultiXactStatusForShare => appendStringInfoString(buf, c"(sh) ".as_ptr()),
        MultiXactStatusForNoKeyUpdate => appendStringInfoString(buf, c"(fornokeyupd) ".as_ptr()),
        MultiXactStatusForUpdate => appendStringInfoString(buf, c"(forupd) ".as_ptr()),
        MultiXactStatusNoKeyUpdate => appendStringInfoString(buf, c"(nokeyupd) ".as_ptr()),
        MultiXactStatusUpdate => appendStringInfoString(buf, c"(upd) ".as_ptr()),
        _ => appendStringInfoString(buf, c"(unk) ".as_ptr()),
    }
}

/// rmgr desc routine: describe a MultiXact WAL record into `buf`.
///
/// # Safety
/// `record` is an opaque WAL-reader pointer (currently stubbed).
pub unsafe fn multixact_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec: *mut c_char = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        let mut pageno: int64 = 0;
        core::ptr::copy_nonoverlapping(
            rec as *const u8,
            (&mut pageno as *mut int64) as *mut u8,
            core::mem::size_of::<int64>(),
        );
        appendStringInfo!(buf, "{}", pageno);
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        let xlrec = rec as *const xl_multixact_create;
        appendStringInfo!(
            buf,
            "{} offset {} nmembers {}: ",
            (*xlrec).mid,
            (*xlrec).moff,
            (*xlrec).nmembers
        );
        let members = core::ptr::addr_of!((*xlrec).members) as *const MultiXactMember;
        let mut i: int32 = 0;
        while i < (*xlrec).nmembers {
            out_member(buf, members.add(i as usize));
            i += 1;
        }
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        let xlrec = rec as *const xl_multixact_truncate;
        appendStringInfo!(
            buf,
            "offsets [{}, {}), members [{}, {})",
            (*xlrec).startTruncOff,
            (*xlrec).endTruncOff,
            (*xlrec).startTruncMemb,
            (*xlrec).endTruncMemb
        );
    }
}

/// rmgr identify routine: map an info byte to a MultiXact opcode name, or null.
pub fn multixact_identify(info: uint8) -> *const c_char {
    match info & !XLR_INFO_MASK {
        XLOG_MULTIXACT_ZERO_OFF_PAGE => c"ZERO_OFF_PAGE".as_ptr(),
        XLOG_MULTIXACT_ZERO_MEM_PAGE => c"ZERO_MEM_PAGE".as_ptr(),
        XLOG_MULTIXACT_CREATE_ID => c"CREATE_ID".as_ptr(),
        XLOG_MULTIXACT_TRUNCATE_ID => c"TRUNCATE_ID".as_ptr(),
        _ => null(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    fn id_str(info: uint8) -> Option<&'static str> {
        let p = multixact_identify(info);
        if p.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(p) }.to_str().unwrap())
        }
    }

    #[test]
    fn identify_table_matches_c() {
        assert_eq!(id_str(XLOG_MULTIXACT_ZERO_OFF_PAGE), Some("ZERO_OFF_PAGE"));
        assert_eq!(id_str(XLOG_MULTIXACT_ZERO_MEM_PAGE), Some("ZERO_MEM_PAGE"));
        assert_eq!(id_str(XLOG_MULTIXACT_CREATE_ID), Some("CREATE_ID"));
        assert_eq!(id_str(XLOG_MULTIXACT_TRUNCATE_ID), Some("TRUNCATE_ID"));
        // High XLR_INFO_MASK bits are stripped before lookup.
        assert_eq!(id_str(XLOG_MULTIXACT_CREATE_ID | XLR_INFO_MASK), Some("CREATE_ID"));
        // Unknown opcode -> null.
        assert_eq!(id_str(0x40), None);
    }

    #[test]
    fn opcode_values() {
        assert_eq!(XLOG_MULTIXACT_ZERO_OFF_PAGE, 0x00);
        assert_eq!(XLOG_MULTIXACT_ZERO_MEM_PAGE, 0x10);
        assert_eq!(XLOG_MULTIXACT_CREATE_ID, 0x20);
        assert_eq!(XLOG_MULTIXACT_TRUNCATE_ID, 0x30);
    }

    #[test]
    fn member_status_values() {
        assert_eq!(MultiXactStatusForKeyShare, 0x00);
        assert_eq!(MultiXactStatusForShare, 0x01);
        assert_eq!(MultiXactStatusForNoKeyUpdate, 0x02);
        assert_eq!(MultiXactStatusForUpdate, 0x03);
        assert_eq!(MultiXactStatusNoKeyUpdate, 0x04);
        assert_eq!(MultiXactStatusUpdate, 0x05);
    }

    #[test]
    fn layout_sanity() {
        // MultiXactMember { TransactionId(u32), MultiXactStatus(c_int=i32) } = 8 bytes.
        assert_eq!(core::mem::size_of::<MultiXactMember>(), 8);
        assert_eq!(core::mem::offset_of!(MultiXactMember, xid), 0);
        assert_eq!(core::mem::offset_of!(MultiXactMember, status), 4);

        // xl_multixact_create: mid(u32) moff(u32) nmembers(i32) then FAM.
        // SizeOfMultiXactCreate = offsetof(.., members) = 12.
        assert_eq!(core::mem::offset_of!(xl_multixact_create, mid), 0);
        assert_eq!(core::mem::offset_of!(xl_multixact_create, moff), 4);
        assert_eq!(core::mem::offset_of!(xl_multixact_create, nmembers), 8);
        assert_eq!(SizeOfMultiXactCreate, 12);

        // xl_multixact_truncate: Oid(u32) + 2*MultiXactId(u32) + 2*MultiXactOffset(u32) = 20.
        assert_eq!(core::mem::offset_of!(xl_multixact_truncate, oldestMultiDB), 0);
        assert_eq!(core::mem::offset_of!(xl_multixact_truncate, startTruncOff), 4);
        assert_eq!(core::mem::offset_of!(xl_multixact_truncate, endTruncOff), 8);
        assert_eq!(core::mem::offset_of!(xl_multixact_truncate, startTruncMemb), 12);
        assert_eq!(core::mem::offset_of!(xl_multixact_truncate, endTruncMemb), 16);
        assert_eq!(SizeOfMultiXactTruncate, 20);
    }
}
