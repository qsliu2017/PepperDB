//! Translation of postgres/src/backend/access/rmgrdesc/smgrdesc.c
//!                + the xl_smgr_* record structs and XLOG_SMGR_* opcodes it
//!                  reads from postgres/src/include/catalog/storage_xlog.h
//!
//! rmgr descriptor routines for catalog/storage.c (used by pg_waldump).
//! smgr_desc casts the WAL record payload to the xl_smgr_create /
//! xl_smgr_truncate struct (selected by the record's info byte) and appends a
//! human-readable summary; smgr_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   catalog/storage_xlog.h   -> the xl_smgr_* structs + XLOG_SMGR_* opcodes
//!                               (merged below, REAL layouts/values)
//!   storage/relfilelocator.h -> RelFileLocator (spcOid/dbOid/relNumber)
//!   storage/block.h          -> BlockNumber (crate::prelude / storage::block)
//!   common/relpath.h         -> ForkNumber, MAIN_FORKNUM, GetRelationPath
//!                               (relpathperm), RelFileNumber
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo,
//!                               appendStringInfo!, appendStringInfoString)
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the XLOG_SMGR_* opcode values, the SMGR_TRUNCATE_*
//! flags, and the smgr_identify name table are REAL (faithful to
//! storage_xlog.h / smgrdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK};
use crate::common::relpath::{ForkNumber, GetRelationPath, RelFileNumber, MAIN_FORKNUM};
use crate::lib::stringinfo::{appendStringInfoString, StringInfo};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Base types (from storage/block.h / storage/relfilelocator.h)
// ---------------------------------------------------------------------------

pub type BlockNumber = uint32;

/// RelFileLocator: (tablespace, database, relfilenumber) tuple identifying
/// physical relation storage. Real layout from storage/relfilelocator.h.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

/// INVALID_PROC_NUMBER (storage/procnumber.h): no temp-rel backend.  Private in
/// common/relpath.rs, so redeclared here with the same value for relpathperm().
const INVALID_PROC_NUMBER: c_int = -1;

// ---------------------------------------------------------------------------
// XLOG records for smgr operations (catalog/storage_xlog.h)
//
// Note: we log file creation and truncation here, but logging of deletion
// actions is handled by xact.c, because it is part of transaction commit.
// ---------------------------------------------------------------------------

/* XLOG gives us high 4 bits */
pub const XLOG_SMGR_CREATE: uint8 = 0x10;
pub const XLOG_SMGR_TRUNCATE: uint8 = 0x20;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_smgr_create {
    pub rlocator: RelFileLocator,
    pub forkNum: ForkNumber,
}

/* flags for xl_smgr_truncate */
pub const SMGR_TRUNCATE_HEAP: c_int = 0x0001;
pub const SMGR_TRUNCATE_VM: c_int = 0x0002;
pub const SMGR_TRUNCATE_FSM: c_int = 0x0004;
pub const SMGR_TRUNCATE_ALL: c_int = SMGR_TRUNCATE_HEAP | SMGR_TRUNCATE_VM | SMGR_TRUNCATE_FSM;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_smgr_truncate {
    pub blkno: BlockNumber,
    pub rlocator: RelFileLocator,
    pub flags: c_int,
}

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// smgr_desc: append a human-readable summary of an smgr WAL record to `buf`.
///
/// Dispatches on (XLogRecGetInfo(record) & ~XLR_INFO_MASK), casts the record
/// data to the matching xl_smgr_* struct, and reproduces the C desc output.
///
/// # Safety
/// `record` is an opaque WAL-reader pointer; the data pointer it yields is cast
/// per-opcode to the corresponding fixed-layout record struct.
pub unsafe fn smgr_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_SMGR_CREATE {
        let xlrec = rec as *const xl_smgr_create;
        // relpathperm(rlocator, forkNum) == GetRelationPath(dbOid, spcOid,
        //   relNumber, INVALID_PROC_NUMBER, forkNum).str
        let path = GetRelationPath(
            (*xlrec).rlocator.dbOid,
            (*xlrec).rlocator.spcOid,
            (*xlrec).rlocator.relNumber,
            INVALID_PROC_NUMBER,
            (*xlrec).forkNum,
        );
        appendStringInfoString(buf, path.str.as_ptr());
    } else if info == XLOG_SMGR_TRUNCATE {
        let xlrec = rec as *const xl_smgr_truncate;
        let path = GetRelationPath(
            (*xlrec).rlocator.dbOid,
            (*xlrec).rlocator.spcOid,
            (*xlrec).rlocator.relNumber,
            INVALID_PROC_NUMBER,
            MAIN_FORKNUM,
        );
        // C: appendStringInfo(buf, "%s to %u blocks flags %d", path, blkno, flags)
        // The %s path is a *const c_char, so emit it separately (cannot {}-format).
        appendStringInfoString(buf, path.str.as_ptr());
        appendStringInfo!(buf, " to {} blocks flags {}", (*xlrec).blkno, (*xlrec).flags);
    }
}

/// smgr_identify: map an smgr opcode (info byte) to its name string, or null
/// for an unknown opcode.
pub fn smgr_identify(info: uint8) -> *const c_char {
    let mut id: *const c_char = null();

    match info & !XLR_INFO_MASK {
        XLOG_SMGR_CREATE => id = c"CREATE".as_ptr(),
        XLOG_SMGR_TRUNCATE => id = c"TRUNCATE".as_ptr(),
        _ => {}
    }

    id
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        if p.is_null() {
            return false;
        }
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn identify_table() {
        unsafe {
            assert!(cstr_eq(smgr_identify(XLOG_SMGR_CREATE), "CREATE"));
            assert!(cstr_eq(smgr_identify(XLOG_SMGR_TRUNCATE), "TRUNCATE"));
            // info-byte high bits (XLR_INFO_MASK) are stripped before matching.
            assert!(cstr_eq(smgr_identify(XLOG_SMGR_CREATE | XLR_INFO_MASK), "CREATE"));
            assert!(cstr_eq(smgr_identify(XLOG_SMGR_TRUNCATE | 0x0F), "TRUNCATE"));
            // unknown opcode -> null
            assert!(smgr_identify(0x30).is_null());
            assert!(smgr_identify(0x00).is_null());
        }
    }

    #[test]
    fn opcode_and_flag_values() {
        // REAL opcode values (high 4 bits) from storage_xlog.h.
        assert_eq!(XLOG_SMGR_CREATE, 0x10);
        assert_eq!(XLOG_SMGR_TRUNCATE, 0x20);
        // REAL truncate flag values.
        assert_eq!(SMGR_TRUNCATE_HEAP, 0x0001);
        assert_eq!(SMGR_TRUNCATE_VM, 0x0002);
        assert_eq!(SMGR_TRUNCATE_FSM, 0x0004);
        assert_eq!(SMGR_TRUNCATE_ALL, 0x0007);
    }

    #[test]
    fn layout_sanity() {
        use core::mem::{align_of, offset_of, size_of};

        // RelFileLocator: three 4-byte Oids, no padding.
        assert_eq!(size_of::<RelFileLocator>(), 12);
        assert_eq!(offset_of!(RelFileLocator, spcOid), 0);
        assert_eq!(offset_of!(RelFileLocator, dbOid), 4);
        assert_eq!(offset_of!(RelFileLocator, relNumber), 8);

        // xl_smgr_create: RelFileLocator (12) + ForkNumber (c_int, 4) = 16.
        assert_eq!(offset_of!(xl_smgr_create, rlocator), 0);
        assert_eq!(offset_of!(xl_smgr_create, forkNum), 12);
        assert_eq!(size_of::<xl_smgr_create>(), 16);

        // xl_smgr_truncate: BlockNumber (4) + RelFileLocator (12) + int (4) = 20.
        assert_eq!(offset_of!(xl_smgr_truncate, blkno), 0);
        assert_eq!(offset_of!(xl_smgr_truncate, rlocator), 4);
        assert_eq!(offset_of!(xl_smgr_truncate, flags), 16);
        assert_eq!(size_of::<xl_smgr_truncate>(), 20);

        // 4-byte alignment for all (largest member is 4-byte).
        assert_eq!(align_of::<xl_smgr_create>(), 4);
        assert_eq!(align_of::<xl_smgr_truncate>(), 4);
    }
}
