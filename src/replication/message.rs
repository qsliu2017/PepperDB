//! Translated from PostgreSQL src/include/replication/message.h
//!
//! Exports from replication/logical/message.c -- generic logical decoding message.

use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;
use crate::postgres_ext::Oid;

/// Generic logical decoding message WAL record (on-disk).
/// Fixed header; the `message[]` FAM payload (null-terminated prefix of
/// `prefix_size`, then `message_size` bytes) lives in the buffer.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct xl_logical_message {
    pub db_id: Oid,            // database Oid emitted from
    pub transactional: bool,   // is message transactional?
    pub prefix_size: usize,    // length of prefix
    pub message_size: usize,   // size of the message
                               // char message[FLEXIBLE_ARRAY_MEMBER]
}
const _: () = assert!(core::mem::offset_of!(xl_logical_message, db_id) == 0);
const _: () = assert!(core::mem::offset_of!(xl_logical_message, prefix_size) == 8);
const _: () = assert!(core::mem::offset_of!(xl_logical_message, message_size) == 16);

/// offsetof(xl_logical_message, message) -- size of the fixed part.
pub const SIZE_OF_LOGICAL_MESSAGE: usize = 24;

pub fn LogLogicalMessage(
    _prefix: &str,
    _message: &[u8],
    _size: usize,
    _transactional: bool,
    _flush: bool,
) -> XLogRecPtr {
    unimplemented!()
}

/* RMGR API */
pub const XLOG_LOGICAL_MESSAGE: u8 = 0x00;

pub fn logicalmsg_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn logicalmsg_desc(_buf: &mut String, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn logicalmsg_identify(_info: u8) -> &'static str {
    unimplemented!()
}
