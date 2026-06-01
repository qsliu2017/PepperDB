//! Translation of postgres/src/backend/access/rmgrdesc/genericdesc.c
//!                + the generic-xlog opcode/flag constants it relates to from
//!                  postgres/src/include/access/generic_xlog.h
//!
//! rmgr descriptor routines for access/transam/generic_xlog.c (used by
//! pg_waldump). A generic xlog record is a single record type (no subtypes):
//! generic_desc walks the per-block "page region" fragments stored in the
//! record payload (each a (offset, length) pair followed by `length` bytes of
//! replacement data) and appends a human-readable summary; generic_identify
//! always returns "Generic".
//!
//! Header mapping:
//!   access/generic_xlog.h -> MAX_GENERIC_XLOG_PAGES, GENERIC_XLOG_FULL_IMAGE,
//!                            the generic_* rmgr function prototypes (merged
//!                            below; the constants are REAL).
//!   storage/off.h         -> OffsetNumber (uint16; crate::storage::off)
//!   lib/stringinfo.h      -> crate::lib::stringinfo (StringInfo,
//!                            appendStringInfo!)
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetDataLen / XLogRecGetInfo: stubbed to return
//!     null / 0 with a TODO. The desc body walks the fragment list over the
//!     stubbed pointer/length, so it compiles and is runtime-stubbed (a real
//!     reader will feed it real bytes later). The fragment-walk loop is kept
//!     faithful to genericdesc.c.

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;
use crate::storage::off::OffsetNumber;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetDataLen, XLR_INFO_MASK,
};
use core::ptr::copy_nonoverlapping;

// ---------------------------------------------------------------------------
// Constants from access/generic_xlog.h (REAL values)
// ---------------------------------------------------------------------------

/// Maximum number of pages a single generic xlog record may touch.
/// == XLR_NORMAL_MAX_BLOCK_ID (access/xlogrecord.h).  TODO: re-export from
/// access/xlogrecord.rs once ported; value reproduced here.
pub const MAX_GENERIC_XLOG_PAGES: c_int = XLR_NORMAL_MAX_BLOCK_ID;

/// XLR_NORMAL_MAX_BLOCK_ID (access/xlogrecord.h): highest "normal" block id,
/// i.e. (XLR_MAX_BLOCK_ID - 2) with XLR_MAX_BLOCK_ID == 32 -> 32.  (Two block
/// ids are reserved for XLR_BLOCK_ID_* sentinels, leaving 0..=32 usable.)
pub const XLR_NORMAL_MAX_BLOCK_ID: c_int = 32;

/// Flag bit for GenericXLogRegisterBuffer: write a full-page image.
pub const GENERIC_XLOG_FULL_IMAGE: c_int = 0x0001;

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// generic_desc: describe a generic xlog record by listing the page regions it
/// overrides.
///
/// The payload is a flat sequence of fragments; each fragment is an
/// OffsetNumber `offset`, an OffsetNumber `length`, then `length` bytes of
/// replacement data.  We walk fragment-by-fragment (skipping the data bytes)
/// and append "offset %u, length %u" for each, joined by "; ".  Output text is
/// reproduced exactly from genericdesc.c.
///
/// # Safety
/// `record` is an opaque WAL-reader pointer; the data pointer/length it yields
/// are read as raw bytes per the fragment layout above.
pub unsafe fn generic_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let mut ptr: *const c_char = XLogRecGetData(record);
    let end: *const c_char = ptr.add(XLogRecGetDataLen(record) as usize);

    while ptr < end {
        let mut offset: OffsetNumber = 0;
        let mut length: OffsetNumber = 0;

        copy_nonoverlapping(
            ptr as *const u8,
            (&mut offset as *mut OffsetNumber) as *mut u8,
            core::mem::size_of::<OffsetNumber>(),
        );
        ptr = ptr.add(core::mem::size_of::<OffsetNumber>());
        copy_nonoverlapping(
            ptr as *const u8,
            (&mut length as *mut OffsetNumber) as *mut u8,
            core::mem::size_of::<OffsetNumber>(),
        );
        ptr = ptr.add(core::mem::size_of::<OffsetNumber>());
        ptr = ptr.add(length as usize);

        if ptr < end {
            appendStringInfo!(buf, "offset {}, length {}; ", offset, length);
        } else {
            appendStringInfo!(buf, "offset {}, length {}", offset, length);
        }
    }
}

/// generic_identify: generic xlog records have no subtypes, so this always
/// returns "Generic" (the `info` byte is ignored, faithful to genericdesc.c).
pub fn generic_identify(_info: uint8) -> *const c_char {
    c"Generic".as_ptr()
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
    fn identify_always_generic() {
        unsafe {
            // No subtypes: every info byte maps to "Generic".
            assert!(cstr_eq(generic_identify(0x00), "Generic"));
            assert!(cstr_eq(generic_identify(0x10), "Generic"));
            assert!(cstr_eq(generic_identify(XLR_INFO_MASK), "Generic"));
            assert!(cstr_eq(generic_identify(0x90), "Generic"));
        }
    }

    #[test]
    fn constant_values() {
        // REAL values from access/generic_xlog.h / access/xlogrecord.h.
        assert_eq!(GENERIC_XLOG_FULL_IMAGE, 0x0001);
        assert_eq!(XLR_NORMAL_MAX_BLOCK_ID, 32);
        assert_eq!(MAX_GENERIC_XLOG_PAGES, XLR_NORMAL_MAX_BLOCK_ID);
        assert_eq!(XLR_INFO_MASK, 0x0F);
        // OffsetNumber is the 16-bit fragment header field used by the walk.
        assert_eq!(core::mem::size_of::<OffsetNumber>(), 2);
    }

    /// Walk a hand-built fragment buffer to confirm the (offset, length, data)
    /// fragment loop + the "; " join match genericdesc.c exactly.
    #[test]
    fn desc_fragment_walk() {
        use crate::lib::stringinfo::{initStringInfo, StringInfoData};

        // Two fragments: (offset=5, length=3, 3 data bytes) and
        //                (offset=20, length=0, 0 data bytes).
        let mut bytes: Vec<u8> = Vec::new();
        let push_off = |b: &mut Vec<u8>, v: OffsetNumber| b.extend_from_slice(&v.to_ne_bytes());
        push_off(&mut bytes, 5);
        push_off(&mut bytes, 3);
        bytes.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        push_off(&mut bytes, 20);
        push_off(&mut bytes, 0);

        // Inline the desc loop against our local buffer (the production fn reads
        // from the stubbed reader, which yields null/0).
        unsafe {
            let mut sid: StringInfoData = core::mem::zeroed();
            let buf: StringInfo = &mut sid;
            initStringInfo(buf);

            let mut ptr: *const c_char = bytes.as_ptr() as *const c_char;
            let end: *const c_char = ptr.add(bytes.len());

            while ptr < end {
                let mut offset: OffsetNumber = 0;
                let mut length: OffsetNumber = 0;
                copy_nonoverlapping(
                    ptr as *const u8,
                    (&mut offset as *mut OffsetNumber) as *mut u8,
                    2,
                );
                ptr = ptr.add(2);
                copy_nonoverlapping(
                    ptr as *const u8,
                    (&mut length as *mut OffsetNumber) as *mut u8,
                    2,
                );
                ptr = ptr.add(2);
                ptr = ptr.add(length as usize);
                if ptr < end {
                    appendStringInfo!(buf, "offset {}, length {}; ", offset, length);
                } else {
                    appendStringInfo!(buf, "offset {}, length {}", offset, length);
                }
            }

            let out = core::slice::from_raw_parts(sid.data as *const u8, sid.len as usize);
            assert_eq!(out, b"offset 5, length 3; offset 20, length 0");
        }
    }
}
