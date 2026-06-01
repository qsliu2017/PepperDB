//! Translation of postgres/src/backend/access/rmgrdesc/dbasedesc.c
//!                + the xl_dbase_* record structs and XLOG_DBASE_* opcodes it
//!                  reads from postgres/src/include/commands/dbcommands_xlog.h
//!
//! rmgr descriptor routines for CREATE/DROP DATABASE WAL records (used by
//! pg_waldump). dbase_desc casts the WAL record payload to the appropriate
//! xl_dbase_* struct (selected by the record's info byte) and appends a
//! human-readable summary of its fields; dbase_identify maps an opcode to its
//! name string.
//!
//! Header mapping:
//!   commands/dbcommands_xlog.h -> the xl_dbase_* structs + XLOG_DBASE_* consts
//!                                 (merged below, real layouts/values)
//!   lib/stringinfo.h           -> crate::lib::stringinfo (StringInfo,
//!                                 appendStringInfo!, appendStringInfoString)
//!   postgres_ext.h (Oid)       -> crate::prelude (Oid = uint32)
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the XLOG_DBASE_* opcode values, and the dbase_identify
//! name table are REAL (faithful to dbcommands_xlog.h / dbasedesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::appendStringInfoString;
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};

// ---------------------------------------------------------------------------
// XLOG record types for CREATE/DROP DATABASE (dbcommands_xlog.h)
// ---------------------------------------------------------------------------

pub const XLOG_DBASE_CREATE_FILE_COPY: uint8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: uint8 = 0x10;
pub const XLOG_DBASE_DROP: uint8 = 0x20;

/// Single WAL record for an entire CREATE DATABASE operation. This is used by
/// the FILE_COPY strategy.
#[repr(C)]
pub struct xl_dbase_create_file_copy_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

/// WAL record for the beginning of a CREATE DATABASE operation, when the
/// WAL_LOG strategy is used. Each individual block will be logged separately
/// afterward.
#[repr(C)]
pub struct xl_dbase_create_wal_log_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
}

/// WAL record for a DROP DATABASE operation.
///
/// The C struct ends in `Oid tablespace_ids[FLEXIBLE_ARRAY_MEMBER]`; this is
/// the fixed header only. The flexible array is read via pointer arithmetic in
/// dbase_desc (see tablespace_ids() below).
#[repr(C)]
pub struct xl_dbase_drop_rec {
    pub db_id: Oid,
    pub ntablespaces: c_int, // number of tablespace IDs
    // Oid tablespace_ids[FLEXIBLE_ARRAY_MEMBER] follows here.
}

/// offsetof(xl_dbase_drop_rec, tablespace_ids): the minimum size of the record,
/// i.e. the size of the fixed header before the flexible array.
pub const MinSizeOfDbaseDropRec: usize = core::mem::size_of::<xl_dbase_drop_rec>();

impl xl_dbase_drop_rec {
    /// Pointer to the start of the trailing `tablespace_ids` flexible array.
    ///
    /// # Safety
    /// `self` must point at a real xl_dbase_drop_rec whose trailing array has at
    /// least `ntablespaces` Oid entries.
    pub unsafe fn tablespace_ids(&self) -> *const Oid {
        (self as *const xl_dbase_drop_rec).add(1) as *const Oid
    }
}

// ---------------------------------------------------------------------------
// dbase_desc / dbase_identify
// ---------------------------------------------------------------------------

/// Append a human-readable description of a CREATE/DROP DATABASE WAL record.
///
/// # Safety
/// `record` is an opaque WAL reader pointer; `XLogRecGetData` is dereferenced
/// per the record's info byte.
pub unsafe fn dbase_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let xlrec = rec as *const xl_dbase_create_file_copy_rec;

        appendStringInfo!(
            buf,
            "copy dir {}/{} to {}/{}",
            (*xlrec).src_tablespace_id,
            (*xlrec).src_db_id,
            (*xlrec).tablespace_id,
            (*xlrec).db_id
        );
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let xlrec = rec as *const xl_dbase_create_wal_log_rec;

        appendStringInfo!(buf, "create dir {}/{}", (*xlrec).tablespace_id, (*xlrec).db_id);
    } else if info == XLOG_DBASE_DROP {
        let xlrec = rec as *const xl_dbase_drop_rec;

        appendStringInfoString(buf, c"dir".as_ptr());
        let ids = (*xlrec).tablespace_ids();
        let mut i: c_int = 0;
        while i < (*xlrec).ntablespaces {
            appendStringInfo!(buf, " {}/{}", *ids.offset(i as isize), (*xlrec).db_id);
            i += 1;
        }
    }
}

/// Map a CREATE/DROP DATABASE WAL opcode to its name string, or null if unknown.
pub fn dbase_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        XLOG_DBASE_CREATE_FILE_COPY => b"CREATE_FILE_COPY\0",
        XLOG_DBASE_CREATE_WAL_LOG => b"CREATE_WAL_LOG\0",
        XLOG_DBASE_DROP => b"DROP\0",
        _ => return null(),
    };
    id.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    #[test]
    fn identify_covers_all_opcodes() {
        let cases: &[(uint8, &str)] = &[
            (XLOG_DBASE_CREATE_FILE_COPY, "CREATE_FILE_COPY"),
            (XLOG_DBASE_CREATE_WAL_LOG, "CREATE_WAL_LOG"),
            (XLOG_DBASE_DROP, "DROP"),
        ];
        for &(op, name) in cases {
            let p = dbase_identify(op);
            assert!(!p.is_null(), "opcode {:#x} should have a name", op);
            let s = unsafe { CStr::from_ptr(p) };
            assert_eq!(s.to_str().unwrap(), name, "opcode {:#x}", op);
        }
    }

    #[test]
    fn identify_ignores_info_mask_bits() {
        // The low XLR_INFO_MASK bits must be masked off before lookup.
        let p = dbase_identify(XLOG_DBASE_DROP | XLR_INFO_MASK);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "DROP");
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0x30 (and 0x40..) have no defined opcode after masking.
        assert!(dbase_identify(0x30).is_null());
    }

    #[test]
    fn opcode_values() {
        assert_eq!(XLOG_DBASE_CREATE_FILE_COPY, 0x00);
        assert_eq!(XLOG_DBASE_CREATE_WAL_LOG, 0x10);
        assert_eq!(XLOG_DBASE_DROP, 0x20);
    }

    #[test]
    fn record_layouts() {
        // Four Oids (uint32) packed, no padding.
        assert_eq!(core::mem::size_of::<xl_dbase_create_file_copy_rec>(), 16);
        assert_eq!(
            core::mem::offset_of!(xl_dbase_create_file_copy_rec, src_tablespace_id),
            12
        );

        // Two Oids.
        assert_eq!(core::mem::size_of::<xl_dbase_create_wal_log_rec>(), 8);

        // db_id (Oid=4) + ntablespaces (int=4); flexible array starts at offset 8.
        assert_eq!(core::mem::size_of::<xl_dbase_drop_rec>(), 8);
        assert_eq!(core::mem::offset_of!(xl_dbase_drop_rec, ntablespaces), 4);
        assert_eq!(MinSizeOfDbaseDropRec, 8);
    }
}
