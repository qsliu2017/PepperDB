//! Generic logical messages.
//!
//! Translated 1:1 from PostgreSQL 18.3:
//!   - postgres/src/backend/replication/logical/message.c
//!   - postgres/src/include/replication/message.h (merged in)
//!
//! Copyright (c) 2013-2025, PostgreSQL Global Development Group
//!
//! NOTES
//!
//! Generic logical messages allow XLOG logging of arbitrary binary blobs that
//! get passed to the logical decoding plugin. In normal XLOG processing they
//! are same as NOOP.
//!
//! These messages can be either transactional or non-transactional.
//! Transactional messages are part of current transaction and will be sent to
//! decoding plugin using in a same way as DML operations.
//! Non-transactional messages are sent to the plugin at the time when the
//! logical decoding reads them from XLOG. This also means that transactional
//! messages won't be delivered if the transaction was rolled back but the
//! non-transactional one will always be delivered.
//!
//! Every message carries prefix to avoid conflicts between different decoding
//! plugins. The plugin authors must take extra care to use unique prefix,
//! good options seems to be for example to use the name of the extension.

use crate::prelude::*;

use std::ffi::c_char;

use crate::c::{Size, FLEXIBLE_ARRAY_MEMBER, uint8};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::postgres_ext::Oid;

// from access/xlogreader.h
use crate::access::transam::xlogreader::{XLogReaderState, XLogRecGetInfo, XLR_INFO_MASK};
use crate::access::rmgrlist::RM_LOGICALMSG_ID;
use crate::miscadmin::MyDatabaseId;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// XLOG_INCLUDE_ORIGIN (access/xlog.h) - record-flag asking the WAL machinery to
// include the replication origin.
const XLOG_INCLUDE_ORIGIN: uint8 = 0x01;

// --- Stubs for not-yet-ported deep deps (faithful signatures). ---

unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO: access/transam/xact.c
}

unsafe fn GetCurrentTransactionId() -> crate::c::TransactionId {
    unimplemented!() // TODO: access/transam/xact.c
}

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogRegisterData(_data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogSetRecordFlags(_flags: uint8) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogFlush(_record: XLogRecPtr) {
    unimplemented!() // TODO: access/transam/xlog.c
}

/*
 * Generic logical decoding message wal record.
 */
#[repr(C)]
pub struct xl_logical_message {
    pub dbId: Oid,                 /* database Oid emitted from */
    pub transactional: bool,       /* is message transactional? */
    pub prefix_size: Size,         /* length of prefix */
    pub message_size: Size,        /* size of the message */
    /* payload, including null-terminated prefix of length prefix_size */
    pub message: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

// #define SizeOfLogicalMessage (offsetof(xl_logical_message, message))
pub const SizeOfLogicalMessage: Size =
    core::mem::offset_of!(xl_logical_message, message) as Size;

/* RMGR API */
// #define XLOG_LOGICAL_MESSAGE 0x00
pub const XLOG_LOGICAL_MESSAGE: u8 = 0x00;

/*
 * Write logical decoding message into XLog.
 */
pub unsafe fn LogLogicalMessage(
    prefix: *const c_char,
    message: *const c_char,
    size: Size,
    transactional: bool,
    flush: bool,
) -> XLogRecPtr {
    let mut xlrec: xl_logical_message = core::mem::zeroed();
    let lsn: XLogRecPtr;

    /*
     * Force xid to be allocated if we're emitting a transactional message.
     */
    if transactional {
        Assert!(IsTransactionState());
        GetCurrentTransactionId();
    }

    xlrec.dbId = MyDatabaseId;
    xlrec.transactional = transactional;
    /* trailing zero is critical; see logicalmsg_desc */
    xlrec.prefix_size = (strlen(prefix) + 1) as Size;
    xlrec.message_size = size;

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut xl_logical_message as *mut c_char,
        SizeOfLogicalMessage as c_int,
    );
    XLogRegisterData(prefix as *mut c_char, xlrec.prefix_size as c_int);
    XLogRegisterData(message as *mut c_char, size as c_int);

    /* allow origin filtering */
    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    lsn = XLogInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE);

    /*
     * Make sure that the message hits disk before leaving if emitting a
     * non-transactional message when flush is requested.
     */
    if !transactional && flush {
        XLogFlush(lsn);
    }
    return lsn;
}

/*
 * Redo is basically just noop for logical decoding messages.
 */
pub unsafe fn logicalmsg_redo(record: *mut XLogReaderState) {
    let info: u8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info != XLOG_LOGICAL_MESSAGE {
        elog!(PANIC, "logicalmsg_redo: unknown op code {}", info);
    }

    /* This is only interesting for logical decoding, see decode.c. */
}
