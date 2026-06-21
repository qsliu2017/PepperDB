//! replication/logicalproto.h - logical replication protocol

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use std::ffi::{c_char, c_int, c_void};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::{uint32, Size, TransactionId};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::nodes::pg_list::List;
use crate::postgres_ext::Oid;

// Bitmapset: crate::nodes::bitmapset
use crate::nodes::bitmapset::Bitmapset;

// TimestampTz is declared in c.h / datetime.h; not yet in a canonical shared
// module here. Stub locally.
// TODO: dedup when datatype/timestamp.h (TimestampTz) lands.
pub type TimestampTz = crate::c::int64;

// GIDSIZE comes from access/xact.h (xactdesc.rs has a copy at 200).
// TODO: dedup when access/xact.h lands.
pub const GIDSIZE: usize = 200;

// Relation: crate::utils::rel::Relation. Avoid pulling the whole rel module in;
// referenced only in prototypes.
// TODO: dedup when utils/rel.h is wired here.
pub type Relation = *mut c_void;

// TupleTableSlot: crate::executor::tuptable::TupleTableSlot. Used only in
// prototypes; stub to a void pointer target.
// TODO: dedup when executor/tuptable.h is wired here.
pub type TupleTableSlot = c_void;

// ReorderBufferTXN: replication/reorderbuffer.h (not yet ported).
// TODO: dedup when replication/reorderbuffer.h lands.
pub type ReorderBufferTXN = c_void;

// Form_pg_attribute: catalog/pg_attribute.h (= *mut FormData_pg_attribute).
// TODO: dedup when catalog/pg_attribute.h is wired here.
pub type Form_pg_attribute = *mut c_void;

// PublishGencolsType: catalog/pg_publication.h declares the enum; only the
// value constants exist in pg_publication.rs, not a type alias. Stub the type.
// TODO: dedup when catalog/pg_publication.h exports the PublishGencolsType alias.
pub type PublishGencolsType = c_int;

/*
 * Protocol capabilities
 */
pub const LOGICALREP_PROTO_MIN_VERSION_NUM: c_int = 1;
pub const LOGICALREP_PROTO_VERSION_NUM: c_int = 1;
pub const LOGICALREP_PROTO_STREAM_VERSION_NUM: c_int = 2;
pub const LOGICALREP_PROTO_TWOPHASE_VERSION_NUM: c_int = 3;
pub const LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM: c_int = 4;
pub const LOGICALREP_PROTO_MAX_VERSION_NUM: c_int = LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM;

/*
 * Logical message types
 *
 * Used by logical replication wire protocol.
 *
 * Note: though this is an enum, the values are used to identify message types
 * in logical replication protocol, which uses a single byte to identify a
 * message type. Hence the values should be single-byte wide and preferably
 * human-readable characters.
 */
pub type LogicalRepMsgType = c_int;
pub const LOGICAL_REP_MSG_BEGIN: LogicalRepMsgType = b'B' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_COMMIT: LogicalRepMsgType = b'C' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_ORIGIN: LogicalRepMsgType = b'O' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_INSERT: LogicalRepMsgType = b'I' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_UPDATE: LogicalRepMsgType = b'U' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_DELETE: LogicalRepMsgType = b'D' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_TRUNCATE: LogicalRepMsgType = b'T' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_RELATION: LogicalRepMsgType = b'R' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_TYPE: LogicalRepMsgType = b'Y' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_MESSAGE: LogicalRepMsgType = b'M' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_BEGIN_PREPARE: LogicalRepMsgType = b'b' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_PREPARE: LogicalRepMsgType = b'P' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_COMMIT_PREPARED: LogicalRepMsgType = b'K' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_ROLLBACK_PREPARED: LogicalRepMsgType = b'r' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_STREAM_START: LogicalRepMsgType = b'S' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_STREAM_STOP: LogicalRepMsgType = b'E' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_STREAM_COMMIT: LogicalRepMsgType = b'c' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_STREAM_ABORT: LogicalRepMsgType = b'A' as LogicalRepMsgType;
pub const LOGICAL_REP_MSG_STREAM_PREPARE: LogicalRepMsgType = b'p' as LogicalRepMsgType;

/*
 * This struct stores a tuple received via logical replication.
 * Keep in mind that the columns correspond to the *remote* table.
 */
#[repr(C)]
pub struct LogicalRepTupleData {
    /* Array of StringInfos, one per column; some may be unused */
    pub colvalues: *mut StringInfoData,
    /* Array of markers for null/unchanged/text/binary, one per column */
    pub colstatus: *mut c_char,
    /* Length of above arrays */
    pub ncols: c_int,
}

/* Possible values for LogicalRepTupleData.colstatus[colnum] */
/* These values are also used in the on-the-wire protocol */
pub const LOGICALREP_COLUMN_NULL: c_char = b'n' as c_char;
pub const LOGICALREP_COLUMN_UNCHANGED: c_char = b'u' as c_char;
pub const LOGICALREP_COLUMN_TEXT: c_char = b't' as c_char;
pub const LOGICALREP_COLUMN_BINARY: c_char = b'b' as c_char; /* added in PG14 */

pub type LogicalRepRelId = uint32;

/* Relation information */
#[repr(C)]
pub struct LogicalRepRelation {
    /* Info coming from the remote side. */
    pub remoteid: LogicalRepRelId, /* unique id of the relation */
    pub nspname: *mut c_char,      /* schema name */
    pub relname: *mut c_char,      /* relation name */
    pub natts: c_int,              /* number of columns */
    pub attnames: *mut *mut c_char, /* column names */
    pub atttyps: *mut Oid,         /* column types */
    pub replident: c_char,         /* replica identity */
    pub relkind: c_char,           /* remote relation kind */
    pub attkeys: *mut Bitmapset,   /* Bitmap of key columns */
}

/* Type mapping info */
#[repr(C)]
pub struct LogicalRepTyp {
    pub remoteid: Oid,        /* unique id of the remote type */
    pub nspname: *mut c_char, /* schema name of remote type */
    pub typname: *mut c_char, /* name of the remote type */
}

/* Transaction info */
#[repr(C)]
pub struct LogicalRepBeginData {
    pub final_lsn: XLogRecPtr,
    pub committime: TimestampTz,
    pub xid: TransactionId,
}

#[repr(C)]
pub struct LogicalRepCommitData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub committime: TimestampTz,
}

/*
 * Prepared transaction protocol information for begin_prepare, and prepare.
 */
#[repr(C)]
pub struct LogicalRepPreparedTxnData {
    pub prepare_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [c_char; GIDSIZE],
}

/*
 * Prepared transaction protocol information for commit prepared.
 */
#[repr(C)]
pub struct LogicalRepCommitPreparedTxnData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub commit_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [c_char; GIDSIZE],
}

/*
 * Rollback Prepared transaction protocol information. The prepare information
 * prepare_end_lsn and prepare_time are used to check if the downstream has
 * received this prepared transaction in which case it can apply the rollback,
 * otherwise, it can skip the rollback operation. The gid alone is not
 * sufficient because the downstream node can have a prepared transaction with
 * same identifier.
 */
#[repr(C)]
pub struct LogicalRepRollbackPreparedTxnData {
    pub prepare_end_lsn: XLogRecPtr,
    pub rollback_end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub rollback_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [c_char; GIDSIZE],
}

/*
 * Transaction protocol information for stream abort.
 */
#[repr(C)]
pub struct LogicalRepStreamAbortData {
    pub xid: TransactionId,
    pub subxid: TransactionId,
    pub abort_lsn: XLogRecPtr,
    pub abort_time: TimestampTz,
}

pub unsafe fn logicalrep_write_begin(out: StringInfo, txn: *mut ReorderBufferTXN) { crate::replication::logical::proto::logicalrep_write_begin(out as _, txn as _) }
pub unsafe fn logicalrep_read_begin(in_: StringInfo, begin_data: *mut LogicalRepBeginData) { crate::replication::logical::proto::logicalrep_read_begin(in_ as _, begin_data as _) }
pub unsafe fn logicalrep_write_commit(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) { crate::replication::logical::proto::logicalrep_write_commit(out, txn as _, commit_lsn as _) }
pub unsafe fn logicalrep_read_commit(in_: StringInfo, commit_data: *mut LogicalRepCommitData) { crate::replication::logical::proto::logicalrep_read_commit(in_ as _, commit_data as _) }
pub unsafe fn logicalrep_write_begin_prepare(out: StringInfo, txn: *mut ReorderBufferTXN) { crate::replication::logical::proto::logicalrep_write_begin_prepare(out as _, txn as _) }
#[no_mangle]
pub unsafe fn logicalrep_read_begin_prepare(
    in_: StringInfo,
    begin_data: *mut LogicalRepPreparedTxnData,
) { crate::replication::logical::proto::logicalrep_read_begin_prepare(in_, begin_data as _) }
pub unsafe fn logicalrep_write_prepare(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) { crate::replication::logical::proto::logicalrep_write_prepare(out, txn as _, prepare_lsn as _) }
#[no_mangle]
pub unsafe fn logicalrep_read_prepare(
    in_: StringInfo,
    prepare_data: *mut LogicalRepPreparedTxnData,
) { crate::replication::logical::proto::logicalrep_read_prepare(in_, prepare_data as _) }
pub unsafe fn logicalrep_write_commit_prepared(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) { crate::replication::logical::proto::logicalrep_write_commit_prepared(out, txn as _, commit_lsn as _) }
#[no_mangle]
pub unsafe fn logicalrep_read_commit_prepared(
    in_: StringInfo,
    prepare_data: *mut LogicalRepCommitPreparedTxnData,
) { crate::replication::logical::proto::logicalrep_read_commit_prepared(in_, prepare_data as _) }
pub unsafe fn logicalrep_write_rollback_prepared(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
) { crate::replication::logical::proto::logicalrep_write_rollback_prepared(out, txn as _, prepare_end_lsn as _, prepare_time as _) }
#[no_mangle]
pub unsafe fn logicalrep_read_rollback_prepared(
    in_: StringInfo,
    rollback_data: *mut LogicalRepRollbackPreparedTxnData,
) { crate::replication::logical::proto::logicalrep_read_rollback_prepared(in_, rollback_data as _) }
pub unsafe fn logicalrep_write_stream_prepare(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) { crate::replication::logical::proto::logicalrep_write_stream_prepare(out, txn as _, prepare_lsn as _) }
#[no_mangle]
pub unsafe fn logicalrep_read_stream_prepare(
    in_: StringInfo,
    prepare_data: *mut LogicalRepPreparedTxnData,
) { crate::replication::logical::proto::logicalrep_read_stream_prepare(in_, prepare_data as _) }

pub unsafe fn logicalrep_write_origin(out: StringInfo, origin: *const c_char, origin_lsn: XLogRecPtr) { crate::replication::logical::proto::logicalrep_write_origin(out as _, origin as _, origin_lsn as _) }
pub unsafe fn logicalrep_read_origin(in_: StringInfo, origin_lsn: *mut XLogRecPtr) -> *mut c_char { crate::replication::logical::proto::logicalrep_read_origin(in_ as _, origin_lsn as _) }
pub unsafe fn logicalrep_write_insert(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    newslot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) { unimplemented!() }
#[no_mangle]
pub unsafe fn logicalrep_read_insert(
    in_: StringInfo,
    newtup: *mut LogicalRepTupleData,
) -> LogicalRepRelId { crate::replication::logical::proto::logicalrep_read_insert(in_, newtup as _) }
pub unsafe fn logicalrep_write_update(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) { unimplemented!() }
#[no_mangle]
pub unsafe fn logicalrep_read_update(
    in_: StringInfo,
    has_oldtuple: *mut bool,
    oldtup: *mut LogicalRepTupleData,
    newtup: *mut LogicalRepTupleData,
) -> LogicalRepRelId { crate::replication::logical::proto::logicalrep_read_update(in_, has_oldtuple as _, oldtup as _, newtup as _) }
pub unsafe fn logicalrep_write_delete(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    oldslot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) { unimplemented!() }
#[no_mangle]
pub unsafe fn logicalrep_read_delete(
    in_: StringInfo,
    oldtup: *mut LogicalRepTupleData,
) -> LogicalRepRelId { crate::replication::logical::proto::logicalrep_read_delete(in_, oldtup as _) }
pub unsafe fn logicalrep_write_truncate(
    out: StringInfo,
    xid: TransactionId,
    nrelids: c_int,
    relids: *mut Oid,
    cascade: bool,
    restart_seqs: bool,
) { crate::replication::logical::proto::logicalrep_write_truncate(out, xid as _, nrelids as _, relids as _, cascade, restart_seqs) }
#[no_mangle]
pub unsafe fn logicalrep_read_truncate(
    in_: StringInfo,
    cascade: *mut bool,
    restart_seqs: *mut bool,
) -> *mut List { crate::replication::logical::proto::logicalrep_read_truncate(in_, cascade as _, restart_seqs as _) }
pub unsafe fn logicalrep_write_message(
    out: StringInfo,
    xid: TransactionId,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const c_char,
    sz: Size,
    message: *const c_char,
) { crate::replication::logical::proto::logicalrep_write_message(out, xid as _, lsn as _, transactional, prefix as _, sz, message as _) }
pub unsafe fn logicalrep_write_rel(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) { unimplemented!() }
pub unsafe fn logicalrep_read_rel(in_: StringInfo) -> *mut LogicalRepRelation { crate::replication::logical::proto::logicalrep_read_rel(in_ as _) }
pub unsafe fn logicalrep_write_typ(out: StringInfo, xid: TransactionId, typoid: Oid) { crate::replication::logical::proto::logicalrep_write_typ(out as _, xid as _, typoid as _) }
pub unsafe fn logicalrep_read_typ(in_: StringInfo, ltyp: *mut LogicalRepTyp) { crate::replication::logical::proto::logicalrep_read_typ(in_ as _, ltyp as _) }
pub unsafe fn logicalrep_write_stream_start(
    out: StringInfo,
    xid: TransactionId,
    first_segment: bool,
) { crate::replication::logical::proto::logicalrep_write_stream_start(out, xid as _, first_segment) }
#[no_mangle]
pub unsafe fn logicalrep_read_stream_start(
    in_: StringInfo,
    first_segment: *mut bool,
) -> TransactionId { crate::replication::logical::proto::logicalrep_read_stream_start(in_, first_segment as _) }
pub unsafe fn logicalrep_write_stream_stop(out: StringInfo) { crate::replication::logical::proto::logicalrep_write_stream_stop(out as _) }
pub unsafe fn logicalrep_write_stream_commit(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) { crate::replication::logical::proto::logicalrep_write_stream_commit(out, txn as _, commit_lsn as _) }
#[no_mangle]
pub unsafe fn logicalrep_read_stream_commit(
    in_: StringInfo,
    commit_data: *mut LogicalRepCommitData,
) -> TransactionId { crate::replication::logical::proto::logicalrep_read_stream_commit(in_, commit_data as _) }
pub unsafe fn logicalrep_write_stream_abort(
    out: StringInfo,
    xid: TransactionId,
    subxid: TransactionId,
    abort_lsn: XLogRecPtr,
    abort_time: TimestampTz,
    write_abort_info: bool,
) { crate::replication::logical::proto::logicalrep_write_stream_abort(out, xid as _, subxid as _, abort_lsn as _, abort_time as _, write_abort_info) }
#[no_mangle]
pub unsafe fn logicalrep_read_stream_abort(
    in_: StringInfo,
    abort_data: *mut LogicalRepStreamAbortData,
    read_abort_info: bool,
) { crate::replication::logical::proto::logicalrep_read_stream_abort(in_, abort_data as _, read_abort_info) }
pub unsafe fn logicalrep_message_type(action: LogicalRepMsgType) -> *const c_char { crate::replication::logical::proto::logicalrep_message_type(action) }
pub unsafe fn logicalrep_should_publish_column(
    att: Form_pg_attribute,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) -> bool { unimplemented!() }
