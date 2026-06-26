//! Translated from PostgreSQL src/include/commands/tablespace.h

#![allow(
    clippy::ptr_arg,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::xlogreader::XLogReaderState;
use crate::c::float8;
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{
    AlterTableSpaceOptionsStmt, CreateTableSpaceStmt, DropTableSpaceStmt,
};
use crate::postgres_ext::Oid;

// GUCs (process-global in C) -> TODO: move to session/execution context.
pub static mut default_tablespace: Option<String> = None;
pub static mut temp_tablespaces: Option<String> = None;
pub static mut allow_in_place_tablespaces: bool = false;

/* XLOG stuff */
pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
pub const XLOG_TBLSPC_DROP: u8 = 0x10;

/// On-disk WAL record. The null-terminated `ts_path` string follows the fixed
/// header in the record buffer (FAM tail, not in the fixed part).
#[repr(C)]
pub struct xl_tblspc_create_rec {
    pub ts_id: Oid,
    // ts_path: [c_char; FLEXIBLE_ARRAY_MEMBER] -- FAM tail, not in fixed part.
}

/// On-disk WAL record: DROP TABLESPACE.
#[repr(C)]
pub struct xl_tblspc_drop_rec {
    pub ts_id: Oid,
}

const _: () = assert!(core::mem::size_of::<xl_tblspc_drop_rec>() == 4);

/// On-disk reloptions blob (varlena-prefixed); `vl_len_` is the varlena header.
#[repr(C)]
pub struct TableSpaceOpts {
    pub vl_len_: i32, // varlena header (do not touch directly!)
    pub random_page_cost: float8,
    pub seq_page_cost: float8,
    pub effective_io_concurrency: i32,
    pub maintenance_io_concurrency: i32,
}

pub fn CreateTableSpace(stmt: &mut CreateTableSpaceStmt) -> Oid {
    unimplemented!()
}

pub fn DropTableSpace(stmt: &mut DropTableSpaceStmt) {
    unimplemented!()
}

pub fn RenameTableSpace(oldname: &str, newname: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterTableSpaceOptions(stmt: &mut AlterTableSpaceOptionsStmt) -> Oid {
    unimplemented!()
}

pub fn TablespaceCreateDbspace(spcOid: Oid, dbOid: Oid, isRedo: bool) {
    unimplemented!()
}

pub fn GetDefaultTablespace(relpersistence: i8, partitioned: bool) -> Oid {
    unimplemented!()
}

pub fn PrepareTempTablespaces() {
    unimplemented!()
}

// missing_ok sentinel (InvalidOid) -> Option.
pub fn get_tablespace_oid(tablespacename: &str, missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

// char * return (NULL when not found) -> Option<String>.
pub fn get_tablespace_name(spc_oid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn directory_is_empty(path: &str) -> bool {
    unimplemented!()
}

pub fn remove_tablespace_symlink(linkloc: &str) {
    unimplemented!()
}

pub fn tblspc_redo(record: &mut XLogReaderState) {
    unimplemented!()
}

// StringInfo -> &mut String (stringinfo tombstone).
pub fn tblspc_desc(buf: &mut String, record: &mut XLogReaderState) {
    unimplemented!()
}

pub fn tblspc_identify(info: u8) -> Option<&'static str> {
    unimplemented!()
}
