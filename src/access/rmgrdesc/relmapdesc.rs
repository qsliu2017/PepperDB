//! Translation of postgres/src/backend/access/rmgrdesc/relmapdesc.c
//!                + the xl_relmap_update record struct and XLOG_RELMAP_UPDATE
//!                  opcode it reads from postgres/src/include/utils/relmapper.h
//!
//! rmgr descriptor routines for the relation-map (catalog-to-filenumber map)
//! WAL records, used by pg_waldump. relmap_desc casts the WAL record payload to
//! xl_relmap_update and appends a human-readable summary of its fields;
//! relmap_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   utils/relmapper.h -> the xl_relmap_update struct + XLOG_RELMAP_UPDATE const
//!                        (merged below, real layout/value)
//!   lib/stringinfo.h  -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   postgres_ext.h    -> crate::prelude (Oid = uint32)
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The struct layout, the XLOG_RELMAP_UPDATE opcode value, and the
//! relmap_identify name table are REAL (faithful to relmapper.h / relmapdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;

// ---------------------------------------------------------------------------
// relmap-related XLOG entries (utils/relmapper.h)
// ---------------------------------------------------------------------------

pub const XLOG_RELMAP_UPDATE: uint8 = 0x00;

/// WAL record for an update of the relation map. The trailing `data` flexible
/// array holds `nbytes` of serialized relmap contents.
///
/// The C struct ends in `char data[FLEXIBLE_ARRAY_MEMBER]`; this is the fixed
/// header only.
#[repr(C)]
pub struct xl_relmap_update {
    pub dbid: Oid,     // database ID, or 0 for shared map
    pub tsid: Oid,     // database's tablespace, or pg_global
    pub nbytes: int32, // size of relmap data
                       // char data[FLEXIBLE_ARRAY_MEMBER] follows here.
}

/// offsetof(xl_relmap_update, data): the minimum size of the record, i.e. the
/// size of the fixed header before the flexible array.
pub const MinSizeOfRelmapUpdate: usize = core::mem::size_of::<xl_relmap_update>();

// ---------------------------------------------------------------------------
// relmap_desc / relmap_identify
// ---------------------------------------------------------------------------

/// Append a human-readable description of a relation-map WAL record.
///
/// # Safety
/// `record` is an opaque WAL reader pointer; `XLogRecGetData` is dereferenced
/// per the record's info byte.
pub unsafe fn relmap_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_RELMAP_UPDATE {
        let xlrec = rec as *const xl_relmap_update;

        appendStringInfo!(
            buf,
            "database {} tablespace {} size {}",
            (*xlrec).dbid,
            (*xlrec).tsid,
            (*xlrec).nbytes
        );
    }
}

/// Map a relation-map WAL opcode to its name string, or null if unknown.
pub fn relmap_identify(info: uint8) -> *const c_char {
    let id: &[u8] = match info & !XLR_INFO_MASK {
        XLOG_RELMAP_UPDATE => b"UPDATE\0",
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
        let p = relmap_identify(XLOG_RELMAP_UPDATE);
        assert!(!p.is_null(), "opcode {:#x} should have a name", XLOG_RELMAP_UPDATE);
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "UPDATE");
    }

    #[test]
    fn identify_ignores_info_mask_bits() {
        // The low XLR_INFO_MASK bits must be masked off before lookup.
        let p = relmap_identify(XLOG_RELMAP_UPDATE | XLR_INFO_MASK);
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) };
        assert_eq!(s.to_str().unwrap(), "UPDATE");
    }

    #[test]
    fn identify_unknown_returns_null() {
        // 0x90 is a high-nibble opcode the relmap rmgr does not define.
        assert!(relmap_identify(0x90).is_null());
    }

    #[test]
    fn opcode_value() {
        assert_eq!(XLOG_RELMAP_UPDATE, 0x00);
    }

    #[test]
    fn record_layout() {
        // Two Oids (uint32) + one int32, all 4-byte, packed with no padding.
        assert_eq!(core::mem::size_of::<xl_relmap_update>(), 12);
        assert_eq!(core::mem::align_of::<xl_relmap_update>(), 4);
        assert_eq!(core::mem::offset_of!(xl_relmap_update, dbid), 0);
        assert_eq!(core::mem::offset_of!(xl_relmap_update, tsid), 4);
        assert_eq!(core::mem::offset_of!(xl_relmap_update, nbytes), 8);
        // MinSizeOfRelmapUpdate == offsetof(xl_relmap_update, data) == 12.
        assert_eq!(MinSizeOfRelmapUpdate, 12);
    }
}
