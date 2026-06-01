//! Translation of postgres/src/backend/access/rmgrdesc/logicalmsgdesc.c
//!                + the xl_logical_message record struct and XLOG_LOGICAL_MESSAGE
//!                  opcode it reads from postgres/src/include/replication/message.h
//!
//! rmgr descriptor routines for replication/logical/message.c (used by
//! pg_waldump). logicalmsg_desc casts the WAL record payload to
//! xl_logical_message (the only logical-decoding message WAL record) and appends
//! a human-readable summary (transactional flag, prefix, and the payload as a
//! series of hex bytes); logicalmsg_identify maps an opcode to its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h         -> crate::lib::stringinfo (StringInfo, appendStringInfo!)
//!   c.h types                -> uint8, Oid, Size (= usize)
//!   replication/message.h    -> xl_logical_message, XLOG_LOGICAL_MESSAGE,
//!                               SizeOfLogicalMessage
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo: stubbed to return null / 0 with a TODO.
//!     The desc body reads its record from the stubbed pointer, so it compiles
//!     and is runtime-stubbed (a real reader will feed it real bytes later).
//!
//! The xl_logical_message layout, the XLOG_LOGICAL_MESSAGE opcode value, and the
//! logicalmsg_identify name table are REAL (faithful to message.h / logicalmsgdesc.c).

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};
use crate::lib::stringinfo::StringInfo;
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Logical decoding message WAL record / opcode (from replication/message.h)
// ---------------------------------------------------------------------------

/// XLOG stuff: the only logical-decoding message WAL record opcode.
pub const XLOG_LOGICAL_MESSAGE: uint8 = 0x00;

/// Generic logical decoding message WAL record (XLOG_LOGICAL_MESSAGE).
///
/// The payload (`message`) is a flexible array member in C; here it is a
/// zero-length array so the struct size equals SizeOfLogicalMessage (the
/// offset of `message`). It holds, in order: a null-terminated prefix of
/// length `prefix_size`, immediately followed by `message_size` payload bytes.
#[repr(C)]
pub struct xl_logical_message {
    pub dbId: Oid,            // database Oid emitted from
    pub transactional: bool,  // is message transactional?
    pub prefix_size: Size,    // length of prefix
    pub message_size: Size,   // size of the message
    // payload, including null-terminated prefix of length prefix_size
    pub message: [c_char; 0], // FLEXIBLE_ARRAY_MEMBER
}

/// offsetof(xl_logical_message, message): the fixed header size of the record.
pub const SizeOfLogicalMessage: Size = core::mem::offset_of!(xl_logical_message, message);

// ---------------------------------------------------------------------------
// Descriptor routines
// ---------------------------------------------------------------------------

/// rmgr "desc" callback for logical-decoding message WAL records (pg_waldump).
///
/// # Safety
/// `record` is an opaque WAL reader pointer; the payload it points at must be a
/// valid logical message WAL record when the stub accessors are replaced by
/// real ones.
pub unsafe fn logicalmsg_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_LOGICAL_MESSAGE {
        let xlrec = rec as *const xl_logical_message;
        let prefix = (*xlrec).message.as_ptr();
        let message = prefix.add((*xlrec).prefix_size);
        let mut sep: &str = "";

        // Assert(prefix[xlrec->prefix_size - 1] == '\0');
        Assert!(*prefix.add((*xlrec).prefix_size - 1) == 0);

        appendStringInfo!(
            buf,
            "{}, prefix \"{}\"; payload ({} bytes): ",
            if (*xlrec).transactional {
                "transactional"
            } else {
                "non-transactional"
            },
            // prefix is a NUL-terminated C string of length prefix_size.
            std::ffi::CStr::from_ptr(prefix).to_string_lossy(),
            (*xlrec).message_size
        );
        // Write message payload as a series of hex bytes
        let mut cnt: Size = 0;
        while cnt < (*xlrec).message_size {
            let byte = *message.add(cnt) as u8;
            appendStringInfo!(buf, "{}{:02X}", sep, byte);
            sep = " ";
            cnt += 1;
        }
    }
}

/// rmgr "identify" callback: maps an info byte to the record-type name, or null
/// for unrecognized opcodes.
pub fn logicalmsg_identify(info: uint8) -> *const c_char {
    if (info & !XLR_INFO_MASK) == XLOG_LOGICAL_MESSAGE {
        return c"MESSAGE".as_ptr();
    }

    null()
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
        // The only defined opcode maps to "MESSAGE".
        let m = logicalmsg_identify(XLOG_LOGICAL_MESSAGE);
        assert!(!m.is_null());
        unsafe {
            assert_eq!(CStr::from_ptr(m).to_bytes(), b"MESSAGE");
        }

        // The low XLR_INFO_MASK (0x0F) flag bits must be masked off before
        // matching the opcode (the high 4 bits).
        let m_masked = logicalmsg_identify(XLOG_LOGICAL_MESSAGE | 0x0F);
        assert!(!m_masked.is_null());
        unsafe {
            assert_eq!(CStr::from_ptr(m_masked).to_bytes(), b"MESSAGE");
        }

        // An opcode (high 4 bits) the rmgr doesn't define has no name.
        assert!(logicalmsg_identify(0x90).is_null());
    }

    #[test]
    fn layout_sanity() {
        // Field offsets follow #[repr(C)] with natural alignment: dbId (Oid=u32)
        // at 0, transactional (bool=u8) at 4, then 3 bytes padding to align the
        // usize-sized fields at an 8-byte boundary on LP64.
        assert_eq!(core::mem::offset_of!(xl_logical_message, dbId), 0);
        assert_eq!(core::mem::offset_of!(xl_logical_message, transactional), 4);
        assert_eq!(
            core::mem::offset_of!(xl_logical_message, prefix_size),
            std::mem::size_of::<Size>()
        );
        assert_eq!(
            core::mem::offset_of!(xl_logical_message, message_size),
            2 * std::mem::size_of::<Size>()
        );
        // SizeOfLogicalMessage == offsetof(.., message): header before the FAM.
        assert_eq!(SizeOfLogicalMessage, 3 * std::mem::size_of::<Size>());
        assert_eq!(
            core::mem::offset_of!(xl_logical_message, message),
            SizeOfLogicalMessage
        );
    }
}
