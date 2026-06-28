//! Translated from PostgreSQL src/include/replication/logicalproto.h

// Logical replication wire protocol. Messages are in-memory structs that
// serialize via explicit big-endian (de)serializers over byte buffers, NOT
// struct-punning. StringInfo is tombstoned: `out: &mut Vec<u8>`, `in: &[u8]`.

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::catalog::pg_publication::PublishGencolsType;
use crate::datatype::timestamp::TimestampTz;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::bitmapset::Bitmapset;
use crate::postgres_ext::Oid;
use crate::replication::reorderbuffer::ReorderBufferTXN;
use crate::utils::rel::Relation;

// GIDSIZE for two-phase commit identifiers (from access/xact.h).
use crate::access::xact::GIDSIZE;

// Protocol capabilities.
pub const LOGICALREP_PROTO_MIN_VERSION_NUM: u32 = 1;
pub const LOGICALREP_PROTO_VERSION_NUM: u32 = 1;
pub const LOGICALREP_PROTO_STREAM_VERSION_NUM: u32 = 2;
pub const LOGICALREP_PROTO_TWOPHASE_VERSION_NUM: u32 = 3;
pub const LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM: u32 = 4;
pub const LOGICALREP_PROTO_MAX_VERSION_NUM: u32 = LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM;

// Logical message types: single-byte wire codes (human-readable chars).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalRepMsgType {
    Begin = b'B',
    Commit = b'C',
    Origin = b'O',
    Insert = b'I',
    Update = b'U',
    Delete = b'D',
    Truncate = b'T',
    Relation = b'R',
    Type = b'Y',
    Message = b'M',
    BeginPrepare = b'b',
    Prepare = b'P',
    CommitPrepared = b'K',
    RollbackPrepared = b'r',
    StreamStart = b'S',
    StreamStop = b'E',
    StreamCommit = b'c',
    StreamAbort = b'A',
    StreamPrepare = b'p',
}

// Possible values for LogicalRepTupleData column status (on-the-wire bytes).
pub const LOGICALREP_COLUMN_NULL: u8 = b'n';
pub const LOGICALREP_COLUMN_UNCHANGED: u8 = b'u';
pub const LOGICALREP_COLUMN_TEXT: u8 = b't';
pub const LOGICALREP_COLUMN_BINARY: u8 = b'b'; // added in PG14

pub type LogicalRepRelId = u32;

// A tuple received via logical replication (columns are the *remote* table's).
pub struct LogicalRepTupleData {
    pub colvalues: Vec<Vec<u8>>, // one buffer per column; some may be unused
    pub colstatus: Vec<u8>,      // null/unchanged/text/binary marker per column
    // ncols folds into the Vec lengths.
}

// Relation information.
pub struct LogicalRepRelation {
    pub remoteid: LogicalRepRelId, // unique id of the relation
    pub nspname: Option<String>,   // schema name
    pub relname: String,           // relation name
    pub attnames: Vec<String>,     // column names (natts folds into Vec lengths)
    pub atttyps: Vec<Oid>,         // column types
    pub replident: u8,             // replica identity
    pub relkind: u8,               // remote relation kind
    pub attkeys: Bitmapset,        // key columns
}

// Type mapping info.
pub struct LogicalRepTyp {
    pub remoteid: Oid,           // unique id of the remote type
    pub nspname: Option<String>, // schema name of remote type
    pub typname: String,         // name of the remote type
}

// Transaction info.
pub struct LogicalRepBeginData {
    pub final_lsn: XLogRecPtr,
    pub committime: TimestampTz,
    pub xid: TransactionId,
}

pub struct LogicalRepCommitData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub committime: TimestampTz,
}

// Prepared-transaction info for begin_prepare and prepare.
pub struct LogicalRepPreparedTxnData {
    pub prepare_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [u8; GIDSIZE],
}

// Prepared-transaction info for commit prepared.
pub struct LogicalRepCommitPreparedTxnData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub commit_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [u8; GIDSIZE],
}

// Rollback-prepared-transaction info.
pub struct LogicalRepRollbackPreparedTxnData {
    pub prepare_end_lsn: XLogRecPtr,
    pub rollback_end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub rollback_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [u8; GIDSIZE],
}

// Transaction info for stream abort.
pub struct LogicalRepStreamAbortData {
    pub xid: TransactionId,
    pub subxid: TransactionId,
    pub abort_lsn: XLogRecPtr,
    pub abort_time: TimestampTz,
}

// read fns return their parsed value/out-struct; the StringInfo cursor advances
// internally. write fns append to `out: &mut Vec<u8>`.

pub fn logicalrep_write_begin(_out: &mut Vec<u8>, _txn: &ReorderBufferTXN) {
    unimplemented!()
}

pub fn logicalrep_read_begin(_input: &[u8]) -> LogicalRepBeginData {
    unimplemented!()
}

pub fn logicalrep_write_commit(_out: &mut Vec<u8>, _txn: &ReorderBufferTXN, _commit_lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn logicalrep_read_commit(_input: &[u8]) -> LogicalRepCommitData {
    unimplemented!()
}

pub fn logicalrep_write_begin_prepare(_out: &mut Vec<u8>, _txn: &ReorderBufferTXN) {
    unimplemented!()
}

pub fn logicalrep_read_begin_prepare(_input: &[u8]) -> LogicalRepPreparedTxnData {
    unimplemented!()
}

pub fn logicalrep_write_prepare(_out: &mut Vec<u8>, _txn: &ReorderBufferTXN, _prepare_lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn logicalrep_read_prepare(_input: &[u8]) -> LogicalRepPreparedTxnData {
    unimplemented!()
}

pub fn logicalrep_write_commit_prepared(
    _out: &mut Vec<u8>,
    _txn: &ReorderBufferTXN,
    _commit_lsn: XLogRecPtr,
) {
    unimplemented!()
}

pub fn logicalrep_read_commit_prepared(_input: &[u8]) -> LogicalRepCommitPreparedTxnData {
    unimplemented!()
}

pub fn logicalrep_write_rollback_prepared(
    _out: &mut Vec<u8>,
    _txn: &ReorderBufferTXN,
    _prepare_end_lsn: XLogRecPtr,
    _prepare_time: TimestampTz,
) {
    unimplemented!()
}

pub fn logicalrep_read_rollback_prepared(_input: &[u8]) -> LogicalRepRollbackPreparedTxnData {
    unimplemented!()
}

pub fn logicalrep_write_stream_prepare(
    _out: &mut Vec<u8>,
    _txn: &ReorderBufferTXN,
    _prepare_lsn: XLogRecPtr,
) {
    unimplemented!()
}

pub fn logicalrep_read_stream_prepare(_input: &[u8]) -> LogicalRepPreparedTxnData {
    unimplemented!()
}

pub fn logicalrep_write_origin(_out: &mut Vec<u8>, _origin: &str, _origin_lsn: XLogRecPtr) {
    unimplemented!()
}

// returns the origin name plus origin_lsn out-param folded into a tuple.
pub fn logicalrep_read_origin(_input: &[u8]) -> (String, XLogRecPtr) {
    unimplemented!()
}

pub fn logicalrep_write_insert(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _rel: &Relation,
    _newslot: &TupleTableSlot,
    _binary: bool,
    _columns: &Bitmapset,
    _include_gencols_type: PublishGencolsType,
) {
    unimplemented!()
}

pub fn logicalrep_read_insert(_input: &[u8], _newtup: &mut LogicalRepTupleData) -> LogicalRepRelId {
    unimplemented!()
}

pub fn logicalrep_write_update(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _rel: &Relation,
    _oldslot: &TupleTableSlot,
    _newslot: &TupleTableSlot,
    _binary: bool,
    _columns: &Bitmapset,
    _include_gencols_type: PublishGencolsType,
) {
    unimplemented!()
}

// has_oldtuple out-param folds into Option<oldtup>; returns rel id.
pub fn logicalrep_read_update(
    _input: &[u8],
    _oldtup: &mut LogicalRepTupleData,
    _newtup: &mut LogicalRepTupleData,
) -> (LogicalRepRelId, bool) {
    unimplemented!()
}

pub fn logicalrep_write_delete(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _rel: &Relation,
    _oldslot: &TupleTableSlot,
    _binary: bool,
    _columns: &Bitmapset,
    _include_gencols_type: PublishGencolsType,
) {
    unimplemented!()
}

pub fn logicalrep_read_delete(_input: &[u8], _oldtup: &mut LogicalRepTupleData) -> LogicalRepRelId {
    unimplemented!()
}

pub fn logicalrep_write_truncate(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _relids: &[Oid],
    _cascade: bool,
    _restart_seqs: bool,
) {
    unimplemented!()
}

// returns the relid list plus cascade/restart_seqs out-params.
pub fn logicalrep_read_truncate(_input: &[u8]) -> (Vec<Oid>, bool, bool) {
    unimplemented!()
}

pub fn logicalrep_write_message(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _transactional: bool,
    _prefix: &str,
    _message: &[u8],
) {
    unimplemented!()
}

pub fn logicalrep_write_rel(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _rel: &Relation,
    _columns: &Bitmapset,
    _include_gencols_type: PublishGencolsType,
) {
    unimplemented!()
}

pub fn logicalrep_read_rel(_input: &[u8]) -> LogicalRepRelation {
    unimplemented!()
}

pub fn logicalrep_write_typ(_out: &mut Vec<u8>, _xid: TransactionId, _typoid: Oid) {
    unimplemented!()
}

pub fn logicalrep_read_typ(_input: &[u8], _ltyp: &mut LogicalRepTyp) {
    unimplemented!()
}

pub fn logicalrep_write_stream_start(_out: &mut Vec<u8>, _xid: TransactionId, _first_segment: bool) {
    unimplemented!()
}

// returns xid plus first_segment out-param.
pub fn logicalrep_read_stream_start(_input: &[u8]) -> (TransactionId, bool) {
    unimplemented!()
}

pub fn logicalrep_write_stream_stop(_out: &mut Vec<u8>) {
    unimplemented!()
}

pub fn logicalrep_write_stream_commit(
    _out: &mut Vec<u8>,
    _txn: &ReorderBufferTXN,
    _commit_lsn: XLogRecPtr,
) {
    unimplemented!()
}

pub fn logicalrep_read_stream_commit(
    _input: &[u8],
    _commit_data: &mut LogicalRepCommitData,
) -> TransactionId {
    unimplemented!()
}

pub fn logicalrep_write_stream_abort(
    _out: &mut Vec<u8>,
    _xid: TransactionId,
    _subxid: TransactionId,
    _abort_lsn: XLogRecPtr,
    _abort_time: TimestampTz,
    _write_abort_info: bool,
) {
    unimplemented!()
}

pub fn logicalrep_read_stream_abort(
    _input: &[u8],
    _abort_data: &mut LogicalRepStreamAbortData,
    _read_abort_info: bool,
) {
    unimplemented!()
}

pub fn logicalrep_message_type(_action: LogicalRepMsgType) -> &'static str {
    unimplemented!()
}

pub fn logicalrep_should_publish_column(
    _att: &crate::catalog::pg_attribute::FormData_pg_attribute,
    _columns: &Bitmapset,
    _include_gencols_type: PublishGencolsType,
) -> bool {
    unimplemented!()
}
