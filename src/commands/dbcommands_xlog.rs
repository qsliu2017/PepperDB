//! Translated from PostgreSQL src/include/commands/dbcommands_xlog.h

#![allow(
    clippy::ptr_arg,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::postgres_ext::Oid;

// XLOG record types (info high bits) -- raw opcode consts.
pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
pub const XLOG_DBASE_DROP: u8 = 0x20;

/// On-disk WAL record: entire CREATE DATABASE (FILE_COPY strategy).
#[repr(C)]
pub struct xl_dbase_create_file_copy_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

const _: () = assert!(core::mem::size_of::<xl_dbase_create_file_copy_rec>() == 16);

/// On-disk WAL record: beginning of CREATE DATABASE (WAL_LOG strategy).
#[repr(C)]
pub struct xl_dbase_create_wal_log_rec {
    pub db_id: Oid,
    pub tablespace_id: Oid,
}

const _: () = assert!(core::mem::size_of::<xl_dbase_create_wal_log_rec>() == 8);

/// On-disk WAL record: DROP DATABASE. Fixed header; the `tablespace_ids` FAM
/// tail (`ntablespaces` Oids) follows in the record buffer.
#[repr(C)]
pub struct xl_dbase_drop_rec {
    pub db_id: Oid,
    pub ntablespaces: i32,
    // tablespace_ids: [Oid; FLEXIBLE_ARRAY_MEMBER] -- FAM tail, not in fixed part.
}

/// offsetof(xl_dbase_drop_rec, tablespace_ids)
pub const MIN_SIZE_OF_DBASE_DROP_REC: usize = core::mem::size_of::<xl_dbase_drop_rec>();

pub fn dbase_redo(record: &mut crate::access::xlogreader::XLogReaderState) {
    unimplemented!()
}

// StringInfo -> &mut String (stringinfo tombstone).
pub fn dbase_desc(buf: &mut String, record: &mut crate::access::xlogreader::XLogReaderState) {
    unimplemented!()
}

pub fn dbase_identify(info: u8) -> Option<&'static str> {
    unimplemented!()
}
