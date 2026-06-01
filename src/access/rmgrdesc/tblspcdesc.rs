//! Translation of postgres/src/backend/access/rmgrdesc/tblspcdesc.c
//!                + the xl_tblspc_* record structs and XLOG_TBLSPC_* opcodes it
//!                  reads from postgres/src/include/commands/tablespace.h
//!
//! rmgr descriptor routines for commands/tablespace.c (used by pg_waldump).
//! tblspc_desc casts the WAL record payload to the xl_tblspc_create_rec /
//! xl_tblspc_drop_rec struct (selected by the record's info byte) and appends a
//! human-readable summary; tblspc_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   commands/tablespace.h    -> the xl_tblspc_* structs + XLOG_TBLSPC_*
//!                               opcodes (merged below, REAL layouts/values)
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo,
//!                               appendStringInfo!, appendStringInfoString,
//!                               appendStringInfoChar)
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layouts, the XLOG_TBLSPC_* opcode values, and the
//! tblspc_identify name table are REAL (faithful to tablespace.h /
//! tblspcdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK};
use crate::lib::stringinfo::{appendStringInfoChar, appendStringInfoString, StringInfo};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// XLOG records for tablespace operations (commands/tablespace.h)
// ---------------------------------------------------------------------------

/* XLOG gives us high 4 bits */
pub const XLOG_TBLSPC_CREATE: uint8 = 0x00;
pub const XLOG_TBLSPC_DROP: uint8 = 0x10;

/// xl_tblspc_create_rec: a CREATE TABLESPACE WAL record. `ts_path` is a
/// FLEXIBLE_ARRAY_MEMBER (a null-terminated string trailing the fixed Oid).
/// Modeled here as a zero-length trailing array; the in-record bytes are
/// addressed via `ts_path.as_ptr()`.
#[repr(C)]
pub struct xl_tblspc_create_rec {
    pub ts_id: Oid,
    pub ts_path: [c_char; 0], /* null-terminated string */
}

/// xl_tblspc_drop_rec: a DROP TABLESPACE WAL record (just the tablespace Oid).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_tblspc_drop_rec {
    pub ts_id: Oid,
}

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// tblspc_desc: append a human-readable summary of a tablespace WAL record to
/// `buf`.
///
/// Dispatches on (XLogRecGetInfo(record) & ~XLR_INFO_MASK), casts the record
/// data to the matching xl_tblspc_* struct, and reproduces the C desc output.
///
/// # Safety
/// `record` is an opaque WAL-reader pointer; the data pointer it yields is cast
/// per-opcode to the corresponding fixed-layout record struct.
pub unsafe fn tblspc_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_TBLSPC_CREATE {
        let xlrec = rec as *const xl_tblspc_create_rec;
        // C: appendStringInfo(buf, "%u \"%s\"", xlrec->ts_id, xlrec->ts_path);
        // ts_path is a *const c_char (cannot {}-format), so emit it via
        // appendStringInfoString and wrap it in literal double-quote chars.
        appendStringInfo!(buf, "{} ", (*xlrec).ts_id);
        appendStringInfoChar(buf, b'"' as c_char);
        appendStringInfoString(buf, (*xlrec).ts_path.as_ptr());
        appendStringInfoChar(buf, b'"' as c_char);
    } else if info == XLOG_TBLSPC_DROP {
        let xlrec = rec as *const xl_tblspc_drop_rec;
        // C: appendStringInfo(buf, "%u", xlrec->ts_id);
        appendStringInfo!(buf, "{}", (*xlrec).ts_id);
    }
}

/// tblspc_identify: map a tablespace opcode (info byte) to its name string, or
/// null for an unknown opcode.
pub fn tblspc_identify(info: uint8) -> *const c_char {
    let mut id: *const c_char = null();

    match info & !XLR_INFO_MASK {
        XLOG_TBLSPC_CREATE => id = c"CREATE".as_ptr(),
        XLOG_TBLSPC_DROP => id = c"DROP".as_ptr(),
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
            assert!(cstr_eq(tblspc_identify(XLOG_TBLSPC_CREATE), "CREATE"));
            assert!(cstr_eq(tblspc_identify(XLOG_TBLSPC_DROP), "DROP"));
            // info-byte high bits (XLR_INFO_MASK) are stripped before matching.
            // XLOG_TBLSPC_CREATE is 0x00, so OR'ing the low nibble flags keeps it
            // mapping to CREATE.
            assert!(cstr_eq(tblspc_identify(XLOG_TBLSPC_CREATE | XLR_INFO_MASK), "CREATE"));
            assert!(cstr_eq(tblspc_identify(XLOG_TBLSPC_DROP | 0x0F), "DROP"));
            // unknown opcode (high nibble the rmgr does not define) -> null.
            assert!(tblspc_identify(0x90).is_null());
            assert!(tblspc_identify(0x20).is_null());
        }
    }

    #[test]
    fn opcode_values() {
        // REAL opcode values (high 4 bits) from commands/tablespace.h.
        assert_eq!(XLOG_TBLSPC_CREATE, 0x00);
        assert_eq!(XLOG_TBLSPC_DROP, 0x10);
    }

    #[test]
    fn layout_sanity() {
        use core::mem::{align_of, offset_of, size_of};

        // xl_tblspc_create_rec: Oid (4) then the flexible array member begins.
        assert_eq!(offset_of!(xl_tblspc_create_rec, ts_id), 0);
        assert_eq!(offset_of!(xl_tblspc_create_rec, ts_path), 4);
        // Only the fixed Oid contributes to the struct's nominal size.
        assert_eq!(size_of::<xl_tblspc_create_rec>(), 4);
        assert_eq!(align_of::<xl_tblspc_create_rec>(), 4);

        // xl_tblspc_drop_rec: a single Oid.
        assert_eq!(offset_of!(xl_tblspc_drop_rec, ts_id), 0);
        assert_eq!(size_of::<xl_tblspc_drop_rec>(), 4);
        assert_eq!(align_of::<xl_tblspc_drop_rec>(), 4);
    }
}
