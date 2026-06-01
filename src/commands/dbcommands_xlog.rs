//! dbcommands_xlog.h - Database resource manager XLOG definitions (create/drop database).

use std::ffi::{c_char, c_int};

use crate::access::transam::xlogreader::XLogReaderState;
use crate::c::{uint8, FLEXIBLE_ARRAY_MEMBER};
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;

/* record types */
pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
pub const XLOG_DBASE_DROP: u8 = 0x20;

/// Single WAL record for an entire CREATE DATABASE operation. This is used
/// by the FILE_COPY strategy.
#[repr(C)]
#[derive(Clone, Copy)]
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
#[derive(Clone, Copy)]
pub struct xl_dbase_create_wal_log_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
}

#[repr(C)]
pub struct xl_dbase_drop_rec {
    pub db_id: Oid,
    /// number of tablespace IDs
    pub ntablespaces: c_int,
    pub tablespace_ids: [Oid; FLEXIBLE_ARRAY_MEMBER],
}

/// MinSizeOfDbaseDropRec == offsetof(xl_dbase_drop_rec, tablespace_ids)
pub const MinSizeOfDbaseDropRec: usize =
    std::mem::offset_of!(xl_dbase_drop_rec, tablespace_ids);

pub unsafe fn dbase_redo(record: *mut XLogReaderState) {
    unimplemented!()
}

pub unsafe fn dbase_desc(buf: StringInfo, record: *mut XLogReaderState) {
    unimplemented!()
}

pub unsafe fn dbase_identify(info: uint8) -> *const c_char {
    unimplemented!()
}
