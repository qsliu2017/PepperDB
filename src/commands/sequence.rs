//! Translated from PostgreSQL src/include/commands/sequence.h

use crate::access::xlogreader::XLogReaderState;
use crate::catalog::objectaddress::ObjectAddress;
use crate::fmgr::FunctionCallInfo;
use crate::nodes::parsenodes::{AlterSeqStmt, CreateSeqStmt, DefElem};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

/// On-disk sequence tuple data (the three sequence-relation columns).
#[repr(C)]
pub struct FormData_pg_sequence_data {
    pub last_value: i64,
    pub log_cnt: i64,
    pub is_called: bool,
}

pub type Form_pg_sequence_data = *mut FormData_pg_sequence_data; // TODO(ptr)

/* Columns of a sequence relation */
pub const SEQ_COL_LASTVAL: i32 = 1;
pub const SEQ_COL_LOG: i32 = 2;
pub const SEQ_COL_CALLED: i32 = 3;

pub const SEQ_COL_FIRSTCOL: i32 = SEQ_COL_LASTVAL;
pub const SEQ_COL_LASTCOL: i32 = SEQ_COL_CALLED;

/* XLOG stuff */
pub const XLOG_SEQ_LOG: u8 = 0x00;

/// On-disk WAL record. The sequence tuple data follows the fixed header in the
/// record buffer (FAM tail, not in the fixed part).
#[repr(C)]
pub struct xl_seq_rec {
    pub locator: RelFileLocator,
}

const _: () = assert!(core::mem::size_of::<xl_seq_rec>() == 12);

pub fn nextval_internal(relid: Oid, check_permissions: bool) -> i64 {
    unimplemented!()
}

// fmgr-callable: Datum nextval(PG_FUNCTION_ARGS).
pub fn nextval(fcinfo: FunctionCallInfo<'_>) -> Datum {
    unimplemented!()
}

// List * -> Vec<Box<DefElem>>.
pub fn sequence_options(relid: Oid) -> Vec<Box<DefElem>> {
    unimplemented!()
}

pub fn DefineSequence(pstate: &mut ParseState, seq: &mut CreateSeqStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterSequence(pstate: &mut ParseState, stmt: &mut AlterSeqStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn SequenceChangePersistence(relid: Oid, newrelpersistence: i8) {
    unimplemented!()
}

pub fn DeleteSequenceTuple(relid: Oid) {
    unimplemented!()
}

pub fn ResetSequence(seq_relid: Oid) {
    unimplemented!()
}

pub fn ResetSequenceCaches() {
    unimplemented!()
}

pub fn seq_redo(record: &mut XLogReaderState) {
    unimplemented!()
}

// StringInfo -> &mut String (stringinfo tombstone).
pub fn seq_desc(buf: &mut String, record: &mut XLogReaderState) {
    unimplemented!()
}

pub fn seq_identify(info: u8) -> Option<&'static str> {
    unimplemented!()
}

pub fn seq_mask(page: &mut [u8], blkno: BlockNumber) {
    unimplemented!()
}
