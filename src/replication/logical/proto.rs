//! proto.c
//!		logical replication protocol functions
//!
//! Translated 1:1 from PostgreSQL 18.3:
//!   - postgres/src/backend/replication/logical/proto.c
//!   - postgres/src/include/replication/logicalproto.h (the on-the-wire structs,
//!     LOGICALREP_* / LOGICAL_REP_MSG_* constants, and prototypes live in the
//!     header-only module crate::replication::logicalproto; reused here).
//!
//! Copyright (c) 2015-2025, PostgreSQL Global Development Group

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused_variables)]

use crate::prelude::*;
use crate::access::htup_details::HeapTuple;

use core::ffi::{c_char, c_int, c_void};

use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, XLogRecPtr};
use crate::access::transam::{InvalidTransactionId, TransactionIdIsValid};
use crate::c::{uint16, uint8, Size, TransactionId};
use crate::lib::stringinfo::{initStringInfoFromString, StringInfo, StringInfoData};
use crate::nodes::pg_list::{lappend_oid, List, NIL};
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::Oid;

// Bitmapset operations (nodes/bitmapset.h).
use crate::nodes::bitmapset::{bms_add_member, bms_free, bms_is_member, Bitmapset};

// On-the-wire structs / message-type and column-status constants, and the
// protocol type aliases; merged from replication/logicalproto.h, which already
// lives in the header-only module crate::replication::logicalproto.
use crate::replication::logicalproto::{
    LogicalRepBeginData, LogicalRepCommitData, LogicalRepCommitPreparedTxnData, LogicalRepMsgType,
    LogicalRepPreparedTxnData, LogicalRepRelId, LogicalRepRelation,
    LogicalRepRollbackPreparedTxnData, LogicalRepStreamAbortData, LogicalRepTupleData,
    LogicalRepTyp, TimestampTz, LOGICALREP_COLUMN_BINARY, LOGICALREP_COLUMN_NULL,
    LOGICALREP_COLUMN_TEXT, LOGICALREP_COLUMN_UNCHANGED, LOGICAL_REP_MSG_BEGIN,
    LOGICAL_REP_MSG_BEGIN_PREPARE, LOGICAL_REP_MSG_COMMIT, LOGICAL_REP_MSG_COMMIT_PREPARED,
    LOGICAL_REP_MSG_DELETE, LOGICAL_REP_MSG_INSERT, LOGICAL_REP_MSG_MESSAGE,
    LOGICAL_REP_MSG_ORIGIN, LOGICAL_REP_MSG_PREPARE, LOGICAL_REP_MSG_RELATION,
    LOGICAL_REP_MSG_ROLLBACK_PREPARED, LOGICAL_REP_MSG_STREAM_ABORT, LOGICAL_REP_MSG_STREAM_COMMIT,
    LOGICAL_REP_MSG_STREAM_PREPARE, LOGICAL_REP_MSG_STREAM_START, LOGICAL_REP_MSG_STREAM_STOP,
    LOGICAL_REP_MSG_TRUNCATE, LOGICAL_REP_MSG_TYPE, LOGICAL_REP_MSG_UPDATE, GIDSIZE,
};

// libpq/pqformat.h - wire (de)serialization helpers.  pq_sendbyte is an inline
// in pqformat.h that forwards to pq_sendint8; the Rust pqformat does not export
// it, so it is provided locally below.
use crate::libpq::pqformat::{
    pq_copymsgbytes, pq_getmsgbyte, pq_getmsgint, pq_getmsgint64, pq_getmsgstring, pq_sendbytes,
    pq_sendcountedtext, pq_sendint, pq_sendint16, pq_sendint32, pq_sendint64, pq_sendint8,
    pq_sendstring,
};

// access/sysattr.h
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;

// catalog/pg_namespace.h
use crate::catalog::pg_known_oids::PG_CATALOG_NAMESPACE;

// catalog/pg_class.h - replica-identity codes.
use crate::catalog::pg_class::{
    REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX,
};

// catalog/pg_attribute.h - attribute form + ATTRIBUTE_GENERATED_STORED.
use crate::catalog::pg_attribute::{Form_pg_attribute, ATTRIBUTE_GENERATED_STORED};

// catalog/pg_publication.h - publish-generated-columns mode value.
use crate::catalog::pg_publication::PUBLISH_GENCOLS_STORED;

// catalog/pg_type.h - type form.
use crate::catalog::pg_type::Form_pg_type;

// utils/rel.h - Relation + accessors.
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetNamespace, RelationGetRelationName, RelationGetRelid,
};

// access/common/tupdesc.h - TupleDescAttr.
use crate::access::common::tupdesc::TupleDescAttr;

// executor/tuptable.h - TupleTableSlot + slot_getallattrs.
use crate::executor::tuptable::{slot_getallattrs, TupleTableSlot};

// access/htup_details.h - HeapTuple, HeapTupleIsValid, GETSTRUCT.
use crate::access::htup_details::{HeapTupleIsValid, GETSTRUCT};

// utils/syscache.h - syscache search/release.
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1};

// utils/fmgr.h - call output / send functions by Oid.
use crate::utils::fmgr::{OidOutputFunctionCall, OidSendFunctionCall};

// access/varatt.h - VARDATA / VARSIZE.
use crate::varatt::{VARDATA, VARSIZE};

// utils/palloc.h / port: pstrdup, palloc, palloc0, strlcpy.
use crate::port::strlcpy::strlcpy;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// ---------------------------------------------------------------------------
//   Stubs for symbols whose home is not yet translated in the port.
// ---------------------------------------------------------------------------

// pq_sendbyte: inline in libpq/pqformat.h forwarding to pq_sendint8.  Not
// exported by the Rust pqformat module yet.
// TODO(pg-port): real pq_sendbyte lives in libpq/pqformat.h (inline).
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: uint8) {
    pq_sendint8(buf, byt);
}

// PublishGencolsType: catalog/pg_publication.h declares the enum; pg_publication.rs
// exports only the value constants (as c_char), not the type alias.  Match the
// constants' width here.
// TODO(pg-port): real PublishGencolsType lives in catalog/pg_publication.h.
type PublishGencolsType = c_char;

// ReorderBufferTXN: replication/reorderbuffer.h is not yet ported.  This stub
// declares only the fields proto.c reads, mirroring the C struct's layout for
// those members.
// TODO(pg-port): real ReorderBufferTXN lives in replication/reorderbuffer.h.
#[repr(C)]
pub union ReorderBufferTXNXactTime {
    pub commit_time: TimestampTz,
    pub prepare_time: TimestampTz,
    pub abort_time: TimestampTz,
}

#[repr(C)]
pub struct ReorderBufferTXN {
    pub txn_flags: crate::c::bits32,
    pub xid: TransactionId,
    pub gid: *mut c_char,
    pub final_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub xact_time: ReorderBufferTXNXactTime,
}

// RBTXN_IS_PREPARED / rbtxn_is_prepared: replication/reorderbuffer.h.
// TODO(pg-port): real definitions live in replication/reorderbuffer.h.
const RBTXN_IS_PREPARED: crate::c::bits32 = 0x0010;
#[inline]
unsafe fn rbtxn_is_prepared(txn: *const ReorderBufferTXN) -> bool {
    ((*txn).txn_flags & RBTXN_IS_PREPARED) != 0
}

// VARATT_IS_EXTERNAL_ONDISK: access/varatt.h; not exported by crate::varatt yet.
// (VARTAG_EXTERNAL(PTR) == VARTAG_1B_E(PTR).)
// TODO(pg-port): real VARATT_IS_EXTERNAL_ONDISK lives in access/varatt.h.
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const c_char) -> bool {
    crate::varatt::VARATT_IS_EXTERNAL(ptr)
        && crate::varatt::VARTAG_1B_E(ptr) == crate::varatt::VARTAG_ONDISK
}

// TYPEOID: utils/syscache.h enum SysCacheIdentifier (the type cache id).
// TODO(pg-port): real TYPEOID lives in utils/syscache.h (generated).
const TYPEOID: c_int = 0;

// RelationGetIdentityKeyBitmap: utils/cache/relcache.c is not yet ported.
// TODO(pg-port): real RelationGetIdentityKeyBitmap lives in utils/cache/relcache.c.
unsafe fn RelationGetIdentityKeyBitmap(relation: Relation) -> *mut Bitmapset {
    let _ = relation;
    unimplemented!("RelationGetIdentityKeyBitmap: utils/cache/relcache.c not yet translated")
}

// getBaseType: utils/cache/lsyscache.c is not yet ported.
// TODO(pg-port): real getBaseType lives in utils/cache/lsyscache.c.
unsafe fn getBaseType(typid: Oid) -> Oid {
    let _ = typid;
    unimplemented!("getBaseType: utils/cache/lsyscache.c not yet translated")
}

// get_namespace_name: utils/cache/lsyscache.c is not yet ported.
// TODO(pg-port): real get_namespace_name lives in utils/cache/lsyscache.c.
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char {
    let _ = nspid;
    unimplemented!("get_namespace_name: utils/cache/lsyscache.c not yet translated")
}

// pstrdup: utils/mmgr/mcxt.c (re-exported from the prelude).  Kept explicit for
// call-site fidelity with the C source.
use crate::utils::palloc::{palloc, palloc0, pfree, pstrdup};

/*
 * Protocol message flags.
 */
const LOGICALREP_IS_REPLICA_IDENTITY: uint8 = 1;

const MESSAGE_TRANSACTIONAL: uint8 = 1 << 0;
const TRUNCATE_CASCADE: uint8 = 1 << 0;
const TRUNCATE_RESTART_SEQS: uint8 = 1 << 1;

/*
 * Write BEGIN to the output stream.
 */
pub unsafe fn logicalrep_write_begin(out: StringInfo, txn: *mut ReorderBufferTXN) {
    pq_sendbyte(out, LOGICAL_REP_MSG_BEGIN as uint8);

    /* fixed fields */
    pq_sendint64(out, (*txn).final_lsn);
    pq_sendint64(out, (*txn).xact_time.commit_time as u64);
    pq_sendint32(out, (*txn).xid);
}

/*
 * Read transaction BEGIN from the stream.
 */
pub unsafe fn logicalrep_read_begin(in_: StringInfo, begin_data: *mut LogicalRepBeginData) {
    /* read fields */
    (*begin_data).final_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*begin_data).final_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "final_lsn not set in begin message");
    }
    (*begin_data).committime = pq_getmsgint64(in_) as TimestampTz;
    (*begin_data).xid = pq_getmsgint(in_, 4);
}

/*
 * Write COMMIT to the output stream.
 */
pub unsafe fn logicalrep_write_commit(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let flags: uint8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_COMMIT as uint8);

    /* send the flags field (unused for now) */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, commit_lsn);
    pq_sendint64(out, (*txn).end_lsn);
    pq_sendint64(out, (*txn).xact_time.commit_time as u64);
}

/*
 * Read transaction COMMIT from the stream.
 */
pub unsafe fn logicalrep_read_commit(in_: StringInfo, commit_data: *mut LogicalRepCommitData) {
    /* read flags (unused for now) */
    let flags: uint8 = pq_getmsgbyte(in_) as uint8;

    if flags != 0 {
        elog!(ERROR, "unrecognized flags {} in commit message", flags);
    }

    /* read fields */
    (*commit_data).commit_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    (*commit_data).end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    (*commit_data).committime = pq_getmsgint64(in_) as TimestampTz;
}

/*
 * Write BEGIN PREPARE to the output stream.
 */
pub unsafe fn logicalrep_write_begin_prepare(out: StringInfo, txn: *mut ReorderBufferTXN) {
    pq_sendbyte(out, LOGICAL_REP_MSG_BEGIN_PREPARE as uint8);

    /* fixed fields */
    pq_sendint64(out, (*txn).final_lsn);
    pq_sendint64(out, (*txn).end_lsn);
    pq_sendint64(out, (*txn).xact_time.prepare_time as u64);
    pq_sendint32(out, (*txn).xid);

    /* send gid */
    pq_sendstring(out, (*txn).gid);
}

/*
 * Read transaction BEGIN PREPARE from the stream.
 */
pub unsafe fn logicalrep_read_begin_prepare(
    in_: StringInfo,
    begin_data: *mut LogicalRepPreparedTxnData,
) {
    /* read fields */
    (*begin_data).prepare_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*begin_data).prepare_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "prepare_lsn not set in begin prepare message");
    }
    (*begin_data).end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*begin_data).end_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "end_lsn not set in begin prepare message");
    }
    (*begin_data).prepare_time = pq_getmsgint64(in_) as TimestampTz;
    (*begin_data).xid = pq_getmsgint(in_, 4);

    /* read gid (copy it into a pre-allocated buffer) */
    strlcpy(
        (*begin_data).gid.as_mut_ptr(),
        pq_getmsgstring(in_),
        GIDSIZE as Size,
    );
}

/*
 * The core functionality for logicalrep_write_prepare and
 * logicalrep_write_stream_prepare.
 */
unsafe fn logicalrep_write_prepare_common(
    out: StringInfo,
    type_: LogicalRepMsgType,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    let flags: uint8 = 0;

    pq_sendbyte(out, type_ as uint8);

    /*
     * This should only ever happen for two-phase commit transactions, in
     * which case we expect to have a valid GID.
     */
    Assert!(!(*txn).gid.is_null());
    Assert!(rbtxn_is_prepared(txn));
    Assert!(TransactionIdIsValid((*txn).xid));

    /* send the flags field */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, prepare_lsn);
    pq_sendint64(out, (*txn).end_lsn);
    pq_sendint64(out, (*txn).xact_time.prepare_time as u64);
    pq_sendint32(out, (*txn).xid);

    /* send gid */
    pq_sendstring(out, (*txn).gid);
}

/*
 * Write PREPARE to the output stream.
 */
pub unsafe fn logicalrep_write_prepare(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    logicalrep_write_prepare_common(out, LOGICAL_REP_MSG_PREPARE, txn, prepare_lsn);
}

/*
 * The core functionality for logicalrep_read_prepare and
 * logicalrep_read_stream_prepare.
 */
unsafe fn logicalrep_read_prepare_common(
    in_: StringInfo,
    msgtype: *const c_char,
    prepare_data: *mut LogicalRepPreparedTxnData,
) {
    /* read flags */
    let flags: uint8 = pq_getmsgbyte(in_) as uint8;

    if flags != 0 {
        elog!(
            ERROR,
            "unrecognized flags {} in {} message",
            flags,
            cstr(msgtype)
        );
    }

    /* read fields */
    (*prepare_data).prepare_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*prepare_data).prepare_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "prepare_lsn is not set in {} message", cstr(msgtype));
    }
    (*prepare_data).end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*prepare_data).end_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "end_lsn is not set in {} message", cstr(msgtype));
    }
    (*prepare_data).prepare_time = pq_getmsgint64(in_) as TimestampTz;
    (*prepare_data).xid = pq_getmsgint(in_, 4);
    if (*prepare_data).xid == InvalidTransactionId {
        elog!(
            ERROR,
            "invalid two-phase transaction ID in {} message",
            cstr(msgtype)
        );
    }

    /* read gid (copy it into a pre-allocated buffer) */
    strlcpy(
        (*prepare_data).gid.as_mut_ptr(),
        pq_getmsgstring(in_),
        GIDSIZE as Size,
    );
}

/*
 * Read transaction PREPARE from the stream.
 */
pub unsafe fn logicalrep_read_prepare(
    in_: StringInfo,
    prepare_data: *mut LogicalRepPreparedTxnData,
) {
    logicalrep_read_prepare_common(in_, c"prepare".as_ptr(), prepare_data);
}

/*
 * Write COMMIT PREPARED to the output stream.
 */
pub unsafe fn logicalrep_write_commit_prepared(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let flags: uint8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_COMMIT_PREPARED as uint8);

    /*
     * This should only ever happen for two-phase commit transactions, in
     * which case we expect to have a valid GID.
     */
    Assert!(!(*txn).gid.is_null());

    /* send the flags field */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, commit_lsn);
    pq_sendint64(out, (*txn).end_lsn);
    pq_sendint64(out, (*txn).xact_time.commit_time as u64);
    pq_sendint32(out, (*txn).xid);

    /* send gid */
    pq_sendstring(out, (*txn).gid);
}

/*
 * Read transaction COMMIT PREPARED from the stream.
 */
pub unsafe fn logicalrep_read_commit_prepared(
    in_: StringInfo,
    prepare_data: *mut LogicalRepCommitPreparedTxnData,
) {
    /* read flags */
    let flags: uint8 = pq_getmsgbyte(in_) as uint8;

    if flags != 0 {
        elog!(
            ERROR,
            "unrecognized flags {} in commit prepared message",
            flags
        );
    }

    /* read fields */
    (*prepare_data).commit_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*prepare_data).commit_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "commit_lsn is not set in commit prepared message");
    }
    (*prepare_data).end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*prepare_data).end_lsn == InvalidXLogRecPtr {
        elog!(ERROR, "end_lsn is not set in commit prepared message");
    }
    (*prepare_data).commit_time = pq_getmsgint64(in_) as TimestampTz;
    (*prepare_data).xid = pq_getmsgint(in_, 4);

    /* read gid (copy it into a pre-allocated buffer) */
    strlcpy(
        (*prepare_data).gid.as_mut_ptr(),
        pq_getmsgstring(in_),
        GIDSIZE as Size,
    );
}

/*
 * Write ROLLBACK PREPARED to the output stream.
 */
pub unsafe fn logicalrep_write_rollback_prepared(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
) {
    let flags: uint8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_ROLLBACK_PREPARED as uint8);

    /*
     * This should only ever happen for two-phase commit transactions, in
     * which case we expect to have a valid GID.
     */
    Assert!(!(*txn).gid.is_null());

    /* send the flags field */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, prepare_end_lsn);
    pq_sendint64(out, (*txn).end_lsn);
    pq_sendint64(out, prepare_time as u64);
    pq_sendint64(out, (*txn).xact_time.commit_time as u64);
    pq_sendint32(out, (*txn).xid);

    /* send gid */
    pq_sendstring(out, (*txn).gid);
}

/*
 * Read transaction ROLLBACK PREPARED from the stream.
 */
pub unsafe fn logicalrep_read_rollback_prepared(
    in_: StringInfo,
    rollback_data: *mut LogicalRepRollbackPreparedTxnData,
) {
    /* read flags */
    let flags: uint8 = pq_getmsgbyte(in_) as uint8;

    if flags != 0 {
        elog!(
            ERROR,
            "unrecognized flags {} in rollback prepared message",
            flags
        );
    }

    /* read fields */
    (*rollback_data).prepare_end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*rollback_data).prepare_end_lsn == InvalidXLogRecPtr {
        elog!(
            ERROR,
            "prepare_end_lsn is not set in rollback prepared message"
        );
    }
    (*rollback_data).rollback_end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    if (*rollback_data).rollback_end_lsn == InvalidXLogRecPtr {
        elog!(
            ERROR,
            "rollback_end_lsn is not set in rollback prepared message"
        );
    }
    (*rollback_data).prepare_time = pq_getmsgint64(in_) as TimestampTz;
    (*rollback_data).rollback_time = pq_getmsgint64(in_) as TimestampTz;
    (*rollback_data).xid = pq_getmsgint(in_, 4);

    /* read gid (copy it into a pre-allocated buffer) */
    strlcpy(
        (*rollback_data).gid.as_mut_ptr(),
        pq_getmsgstring(in_),
        GIDSIZE as Size,
    );
}

/*
 * Write STREAM PREPARE to the output stream.
 */
pub unsafe fn logicalrep_write_stream_prepare(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    logicalrep_write_prepare_common(out, LOGICAL_REP_MSG_STREAM_PREPARE, txn, prepare_lsn);
}

/*
 * Read STREAM PREPARE from the stream.
 */
pub unsafe fn logicalrep_read_stream_prepare(
    in_: StringInfo,
    prepare_data: *mut LogicalRepPreparedTxnData,
) {
    logicalrep_read_prepare_common(in_, c"stream prepare".as_ptr(), prepare_data);
}

/*
 * Write ORIGIN to the output stream.
 */
pub unsafe fn logicalrep_write_origin(
    out: StringInfo,
    origin: *const c_char,
    origin_lsn: XLogRecPtr,
) {
    pq_sendbyte(out, LOGICAL_REP_MSG_ORIGIN as uint8);

    /* fixed fields */
    pq_sendint64(out, origin_lsn);

    /* origin string */
    pq_sendstring(out, origin);
}

/*
 * Read ORIGIN from the output stream.
 */
pub unsafe fn logicalrep_read_origin(in_: StringInfo, origin_lsn: *mut XLogRecPtr) -> *mut c_char {
    /* fixed fields */
    *origin_lsn = pq_getmsgint64(in_) as XLogRecPtr;

    /* return origin */
    pstrdup(pq_getmsgstring(in_))
}

/*
 * Write INSERT to the output stream.
 */
pub unsafe fn logicalrep_write_insert(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    newslot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) {
    pq_sendbyte(out, LOGICAL_REP_MSG_INSERT as uint8);

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    pq_sendbyte(out, b'N'); /* new tuple follows */
    logicalrep_write_tuple(out, rel, newslot, binary, columns, include_gencols_type);
}

/*
 * Read INSERT from stream.
 *
 * Fills the new tuple.
 */
pub unsafe fn logicalrep_read_insert(
    in_: StringInfo,
    newtup: *mut LogicalRepTupleData,
) -> LogicalRepRelId {
    let action: c_char;
    let relid: LogicalRepRelId;

    /* read the relation id */
    relid = pq_getmsgint(in_, 4);

    action = pq_getmsgbyte(in_) as c_char;
    if action != b'N' as c_char {
        elog!(ERROR, "expected new tuple but got {}", action as c_int);
    }

    logicalrep_read_tuple(in_, newtup);

    relid
}

/*
 * Write UPDATE to the output stream.
 */
pub unsafe fn logicalrep_write_update(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) {
    pq_sendbyte(out, LOGICAL_REP_MSG_UPDATE as uint8);

    Assert!(
        (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_DEFAULT
            || (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_FULL
            || (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_INDEX
    );

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    if !oldslot.is_null() {
        if (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
            pq_sendbyte(out, b'O'); /* old tuple follows */
        } else {
            pq_sendbyte(out, b'K'); /* old key follows */
        }
        logicalrep_write_tuple(out, rel, oldslot, binary, columns, include_gencols_type);
    }

    pq_sendbyte(out, b'N'); /* new tuple follows */
    logicalrep_write_tuple(out, rel, newslot, binary, columns, include_gencols_type);
}

/*
 * Read UPDATE from stream.
 */
pub unsafe fn logicalrep_read_update(
    in_: StringInfo,
    has_oldtuple: *mut bool,
    oldtup: *mut LogicalRepTupleData,
    newtup: *mut LogicalRepTupleData,
) -> LogicalRepRelId {
    let mut action: c_char;
    let relid: LogicalRepRelId;

    /* read the relation id */
    relid = pq_getmsgint(in_, 4);

    /* read and verify action */
    action = pq_getmsgbyte(in_) as c_char;
    if action != b'K' as c_char && action != b'O' as c_char && action != b'N' as c_char {
        elog!(
            ERROR,
            "expected action 'N', 'O' or 'K', got {}",
            action as u8 as char
        );
    }

    /* check for old tuple */
    if action == b'K' as c_char || action == b'O' as c_char {
        logicalrep_read_tuple(in_, oldtup);
        *has_oldtuple = true;

        action = pq_getmsgbyte(in_) as c_char;
    } else {
        *has_oldtuple = false;
    }

    /* check for new  tuple */
    if action != b'N' as c_char {
        elog!(ERROR, "expected action 'N', got {}", action as u8 as char);
    }

    logicalrep_read_tuple(in_, newtup);

    relid
}

/*
 * Write DELETE to the output stream.
 */
pub unsafe fn logicalrep_write_delete(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    oldslot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) {
    Assert!(
        (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_DEFAULT
            || (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_FULL
            || (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_INDEX
    );

    pq_sendbyte(out, LOGICAL_REP_MSG_DELETE as uint8);

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    if (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
        pq_sendbyte(out, b'O'); /* old tuple follows */
    } else {
        pq_sendbyte(out, b'K'); /* old key follows */
    }

    logicalrep_write_tuple(out, rel, oldslot, binary, columns, include_gencols_type);
}

/*
 * Read DELETE from stream.
 *
 * Fills the old tuple.
 */
pub unsafe fn logicalrep_read_delete(
    in_: StringInfo,
    oldtup: *mut LogicalRepTupleData,
) -> LogicalRepRelId {
    let action: c_char;
    let relid: LogicalRepRelId;

    /* read the relation id */
    relid = pq_getmsgint(in_, 4);

    /* read and verify action */
    action = pq_getmsgbyte(in_) as c_char;
    if action != b'K' as c_char && action != b'O' as c_char {
        elog!(ERROR, "expected action 'O' or 'K', got {}", action as u8 as char);
    }

    logicalrep_read_tuple(in_, oldtup);

    relid
}

/*
 * Write TRUNCATE to the output stream.
 */
pub unsafe fn logicalrep_write_truncate(
    out: StringInfo,
    xid: TransactionId,
    nrelids: c_int,
    relids: *mut Oid,
    cascade: bool,
    restart_seqs: bool,
) {
    let mut i: c_int;
    let mut flags: uint8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_TRUNCATE as uint8);

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    pq_sendint32(out, nrelids as u32);

    /* encode and send truncate flags */
    if cascade {
        flags |= TRUNCATE_CASCADE;
    }
    if restart_seqs {
        flags |= TRUNCATE_RESTART_SEQS;
    }
    pq_sendint8(out, flags);

    i = 0;
    while i < nrelids {
        pq_sendint32(out, *relids.add(i as usize));
        i += 1;
    }
}

/*
 * Read TRUNCATE from stream.
 */
pub unsafe fn logicalrep_read_truncate(
    in_: StringInfo,
    cascade: *mut bool,
    restart_seqs: *mut bool,
) -> *mut List {
    let mut i: c_int;
    let nrelids: c_int;
    let mut relids: *mut List = NIL;
    let flags: uint8;

    nrelids = pq_getmsgint(in_, 4) as c_int;

    /* read and decode truncate flags */
    flags = pq_getmsgint(in_, 1) as uint8;
    *cascade = (flags & TRUNCATE_CASCADE) > 0;
    *restart_seqs = (flags & TRUNCATE_RESTART_SEQS) > 0;

    i = 0;
    while i < nrelids {
        relids = lappend_oid(relids, pq_getmsgint(in_, 4));
        i += 1;
    }

    relids
}

/*
 * Write MESSAGE to stream
 */
pub unsafe fn logicalrep_write_message(
    out: StringInfo,
    xid: TransactionId,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const c_char,
    sz: Size,
    message: *const c_char,
) {
    let mut flags: uint8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_MESSAGE as uint8);

    /* encode and send message flags */
    if transactional {
        flags |= MESSAGE_TRANSACTIONAL;
    }

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    pq_sendint8(out, flags);
    pq_sendint64(out, lsn);
    pq_sendstring(out, prefix);
    pq_sendint32(out, sz as u32);
    pq_sendbytes(out, message as *const c_void, sz as c_int);
}

/*
 * Write relation description to the output stream.
 */
pub unsafe fn logicalrep_write_rel(
    out: StringInfo,
    xid: TransactionId,
    rel: Relation,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) {
    let relname: *mut c_char;

    pq_sendbyte(out, LOGICAL_REP_MSG_RELATION as uint8);

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    /* send qualified relation name */
    logicalrep_write_namespace(out, RelationGetNamespace(rel));
    relname = RelationGetRelationName(rel);
    pq_sendstring(out, relname);

    /* send replica identity */
    pq_sendbyte(out, (*(*rel).rd_rel).relreplident as uint8);

    /* send the attribute info */
    logicalrep_write_attrs(out, rel, columns, include_gencols_type);
}

/*
 * Read the relation info from stream and return as LogicalRepRelation.
 */
pub unsafe fn logicalrep_read_rel(in_: StringInfo) -> *mut LogicalRepRelation {
    let rel = palloc(core::mem::size_of::<LogicalRepRelation>()) as *mut LogicalRepRelation;

    (*rel).remoteid = pq_getmsgint(in_, 4);

    /* Read relation name from stream */
    (*rel).nspname = pstrdup(logicalrep_read_namespace(in_));
    (*rel).relname = pstrdup(pq_getmsgstring(in_));

    /* Read the replica identity. */
    (*rel).replident = pq_getmsgbyte(in_) as c_char;

    /* Get attribute description */
    logicalrep_read_attrs(in_, rel);

    rel
}

/*
 * Write type info to the output stream.
 *
 * This function will always write base type info.
 */
pub unsafe fn logicalrep_write_typ(out: StringInfo, xid: TransactionId, typoid: Oid) {
    let basetypoid: Oid = getBaseType(typoid);
    let tup: HeapTuple;
    let typtup: Form_pg_type;

    pq_sendbyte(out, LOGICAL_REP_MSG_TYPE as uint8);

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid);
    }

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(basetypoid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", basetypoid);
    }
    typtup = GETSTRUCT(tup) as Form_pg_type;

    /* use Oid as type identifier */
    pq_sendint32(out, typoid);

    /* send qualified type name */
    logicalrep_write_namespace(out, (*typtup).typnamespace);
    pq_sendstring(out, crate::c::NameStr(&(*typtup).typname));

    ReleaseSysCache(tup);
}

/*
 * Read type info from the output stream.
 */
pub unsafe fn logicalrep_read_typ(in_: StringInfo, ltyp: *mut LogicalRepTyp) {
    (*ltyp).remoteid = pq_getmsgint(in_, 4);

    /* Read type name from stream */
    (*ltyp).nspname = pstrdup(logicalrep_read_namespace(in_));
    (*ltyp).typname = pstrdup(pq_getmsgstring(in_));
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
unsafe fn logicalrep_write_tuple(
    out: StringInfo,
    rel: Relation,
    slot: *mut TupleTableSlot,
    binary: bool,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) {
    let desc: crate::access::common::tupdesc::TupleDesc;
    let values: *mut Datum;
    let isnull: *mut bool;
    let mut i: c_int;
    let mut nliveatts: uint16 = 0;

    desc = RelationGetDescr(rel);

    i = 0;
    while i < (*desc).natts {
        let att: Form_pg_attribute = TupleDescAttr(desc, i);

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            i += 1;
            continue;
        }

        nliveatts += 1;
        i += 1;
    }
    pq_sendint16(out, nliveatts);

    slot_getallattrs(slot);
    values = (*slot).tts_values;
    isnull = (*slot).tts_isnull;

    /* Write the values */
    i = 0;
    while i < (*desc).natts {
        let typtup: HeapTuple;
        let typclass: Form_pg_type;
        let att: Form_pg_attribute = TupleDescAttr(desc, i);

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            i += 1;
            continue;
        }

        if *isnull.add(i as usize) {
            pq_sendbyte(out, LOGICALREP_COLUMN_NULL as uint8);
            i += 1;
            continue;
        }

        if (*att).attlen == -1
            && VARATT_IS_EXTERNAL_ONDISK(DatumGetPointer(*values.add(i as usize)) as *const c_char)
        {
            /*
             * Unchanged toasted datum.  (Note that we don't promise to detect
             * unchanged data in general; this is just a cheap check to avoid
             * sending large values unnecessarily.)
             */
            pq_sendbyte(out, LOGICALREP_COLUMN_UNCHANGED as uint8);
            i += 1;
            continue;
        }

        typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum((*att).atttypid));
        if !HeapTupleIsValid(typtup) {
            elog!(ERROR, "cache lookup failed for type {}", (*att).atttypid);
        }
        typclass = GETSTRUCT(typtup) as Form_pg_type;

        /*
         * Send in binary if requested and type has suitable send function.
         */
        if binary && OidIsValid((*typclass).typsend) {
            let outputbytes: *mut crate::c::bytea;
            let len: c_int;

            pq_sendbyte(out, LOGICALREP_COLUMN_BINARY as uint8);
            outputbytes = OidSendFunctionCall((*typclass).typsend, *values.add(i as usize));
            len = VARSIZE(outputbytes as *const c_char) as c_int - VARHDRSZ;
            pq_sendint(out, len as u32, 4); /* length */
            pq_sendbytes(
                out,
                VARDATA(outputbytes as *const c_char) as *const c_void,
                len,
            ); /* data */
            pfree(outputbytes as *mut c_void);
        } else {
            let outputstr: *mut c_char;

            pq_sendbyte(out, LOGICALREP_COLUMN_TEXT as uint8);
            outputstr = OidOutputFunctionCall((*typclass).typoutput, *values.add(i as usize));
            pq_sendcountedtext(out, outputstr, strlen(outputstr) as c_int);
            pfree(outputstr as *mut c_void);
        }

        ReleaseSysCache(typtup);
        i += 1;
    }
}

/*
 * Read tuple in logical replication format from stream.
 */
unsafe fn logicalrep_read_tuple(in_: StringInfo, tuple: *mut LogicalRepTupleData) {
    let mut i: c_int;
    let natts: c_int;

    /* Get number of attributes */
    natts = pq_getmsgint(in_, 2) as c_int;

    /* Allocate space for per-column values; zero out unused StringInfoDatas */
    (*tuple).colvalues =
        palloc0(natts as Size * core::mem::size_of::<StringInfoData>() as Size) as *mut StringInfoData;
    (*tuple).colstatus = palloc(natts as Size * core::mem::size_of::<c_char>() as Size) as *mut c_char;
    (*tuple).ncols = natts;

    /* Read the data */
    i = 0;
    while i < natts {
        let buff: *mut c_char;
        let kind: c_char;
        let len: c_int;
        let value: StringInfo = (*tuple).colvalues.add(i as usize);

        kind = pq_getmsgbyte(in_) as c_char;
        *(*tuple).colstatus.add(i as usize) = kind;

        match kind {
            _ if kind == LOGICALREP_COLUMN_NULL => {
                /* nothing more to do */
            }
            _ if kind == LOGICALREP_COLUMN_UNCHANGED => {
                /* we don't receive the value of an unchanged column */
            }
            _ if kind == LOGICALREP_COLUMN_TEXT || kind == LOGICALREP_COLUMN_BINARY => {
                len = pq_getmsgint(in_, 4) as c_int; /* read length */

                /* and data */
                buff = palloc((len + 1) as Size) as *mut c_char;
                pq_copymsgbytes(in_, buff as *mut c_void, len);

                /*
                 * NUL termination is required for LOGICALREP_COLUMN_TEXT mode
                 * as input functions require that.  For
                 * LOGICALREP_COLUMN_BINARY it's not technically required, but
                 * it's harmless.
                 */
                *buff.add(len as usize) = b'\0' as c_char;

                initStringInfoFromString(value, buff, len);
            }
            _ => {
                elog!(
                    ERROR,
                    "unrecognized data representation type '{}'",
                    kind as u8 as char
                );
            }
        }
        i += 1;
    }
}

/*
 * Write relation attribute metadata to the stream.
 */
unsafe fn logicalrep_write_attrs(
    out: StringInfo,
    rel: Relation,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) {
    let desc: crate::access::common::tupdesc::TupleDesc;
    let mut i: c_int;
    let mut nliveatts: uint16 = 0;
    let mut idattrs: *mut Bitmapset = core::ptr::null_mut();
    let replidentfull: bool;

    desc = RelationGetDescr(rel);

    /* send number of live attributes */
    i = 0;
    while i < (*desc).natts {
        let att: Form_pg_attribute = TupleDescAttr(desc, i);

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            i += 1;
            continue;
        }

        nliveatts += 1;
        i += 1;
    }
    pq_sendint16(out, nliveatts);

    /* fetch bitmap of REPLICATION IDENTITY attributes */
    replidentfull = (*(*rel).rd_rel).relreplident == REPLICA_IDENTITY_FULL;
    if !replidentfull {
        idattrs = RelationGetIdentityKeyBitmap(rel);
    }

    /* send the attributes */
    i = 0;
    while i < (*desc).natts {
        let att: Form_pg_attribute = TupleDescAttr(desc, i);
        let mut flags: uint8 = 0;

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            i += 1;
            continue;
        }

        /* REPLICA IDENTITY FULL means all columns are sent as part of key. */
        if replidentfull
            || bms_is_member(
                (*att).attnum as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                idattrs,
            )
        {
            flags |= LOGICALREP_IS_REPLICA_IDENTITY;
        }

        pq_sendbyte(out, flags);

        /* attribute name */
        pq_sendstring(out, crate::c::NameStr(&(*att).attname));

        /* attribute type id */
        pq_sendint32(out, (*att).atttypid as c_int as u32);

        /* attribute mode */
        pq_sendint32(out, (*att).atttypmod as u32);

        i += 1;
    }

    bms_free(idattrs);
}

/*
 * Read relation attribute metadata from the stream.
 */
unsafe fn logicalrep_read_attrs(in_: StringInfo, rel: *mut LogicalRepRelation) {
    let mut i: c_int;
    let natts: c_int;
    let attnames: *mut *mut c_char;
    let atttyps: *mut Oid;
    let mut attkeys: *mut Bitmapset = core::ptr::null_mut();

    natts = pq_getmsgint(in_, 2) as c_int;
    attnames = palloc(natts as Size * core::mem::size_of::<*mut c_char>() as Size) as *mut *mut c_char;
    atttyps = palloc(natts as Size * core::mem::size_of::<Oid>() as Size) as *mut Oid;

    /* read the attributes */
    i = 0;
    while i < natts {
        let flags: uint8;

        /* Check for replica identity column */
        flags = pq_getmsgbyte(in_) as uint8;
        if flags & LOGICALREP_IS_REPLICA_IDENTITY != 0 {
            attkeys = bms_add_member(attkeys, i);
        }

        /* attribute name */
        *attnames.add(i as usize) = pstrdup(pq_getmsgstring(in_));

        /* attribute type id */
        *atttyps.add(i as usize) = pq_getmsgint(in_, 4) as Oid;

        /* we ignore attribute mode for now */
        let _ = pq_getmsgint(in_, 4);

        i += 1;
    }

    (*rel).attnames = attnames;
    (*rel).atttyps = atttyps;
    (*rel).attkeys = attkeys;
    (*rel).natts = natts;
}

/*
 * Write the namespace name or empty string for pg_catalog (to save space).
 */
unsafe fn logicalrep_write_namespace(out: StringInfo, nspid: Oid) {
    if nspid == PG_CATALOG_NAMESPACE {
        pq_sendbyte(out, b'\0');
    } else {
        let nspname: *mut c_char = get_namespace_name(nspid);

        if nspname.is_null() {
            elog!(ERROR, "cache lookup failed for namespace {}", nspid);
        }

        pq_sendstring(out, nspname);
    }
}

/*
 * Read the namespace name while treating empty string as pg_catalog.
 */
unsafe fn logicalrep_read_namespace(in_: StringInfo) -> *const c_char {
    let mut nspname: *const c_char = pq_getmsgstring(in_);

    if *nspname == 0 {
        nspname = c"pg_catalog".as_ptr();
    }

    nspname
}

/*
 * Write the information for the start stream message to the output stream.
 */
pub unsafe fn logicalrep_write_stream_start(
    out: StringInfo,
    xid: TransactionId,
    first_segment: bool,
) {
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_START as uint8);

    Assert!(TransactionIdIsValid(xid));

    /* transaction ID (we're starting to stream, so must be valid) */
    pq_sendint32(out, xid);

    /* 1 if this is the first streaming segment for this xid */
    pq_sendbyte(out, if first_segment { 1 } else { 0 });
}

/*
 * Read the information about the start stream message from output stream.
 */
pub unsafe fn logicalrep_read_stream_start(
    in_: StringInfo,
    first_segment: *mut bool,
) -> TransactionId {
    let xid: TransactionId;

    Assert!(!first_segment.is_null());

    xid = pq_getmsgint(in_, 4);
    *first_segment = pq_getmsgbyte(in_) == 1;

    xid
}

/*
 * Write the stop stream message to the output stream.
 */
pub unsafe fn logicalrep_write_stream_stop(out: StringInfo) {
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_STOP as uint8);
}

/*
 * Write STREAM COMMIT to the output stream.
 */
pub unsafe fn logicalrep_write_stream_commit(
    out: StringInfo,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let flags: uint8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_COMMIT as uint8);

    Assert!(TransactionIdIsValid((*txn).xid));

    /* transaction ID */
    pq_sendint32(out, (*txn).xid);

    /* send the flags field (unused for now) */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, commit_lsn);
    pq_sendint64(out, (*txn).end_lsn);
    pq_sendint64(out, (*txn).xact_time.commit_time as u64);
}

/*
 * Read STREAM COMMIT from the output stream.
 */
pub unsafe fn logicalrep_read_stream_commit(
    in_: StringInfo,
    commit_data: *mut LogicalRepCommitData,
) -> TransactionId {
    let xid: TransactionId;
    let flags: uint8;

    xid = pq_getmsgint(in_, 4);

    /* read flags (unused for now) */
    flags = pq_getmsgbyte(in_) as uint8;

    if flags != 0 {
        elog!(ERROR, "unrecognized flags {} in commit message", flags);
    }

    /* read fields */
    (*commit_data).commit_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    (*commit_data).end_lsn = pq_getmsgint64(in_) as XLogRecPtr;
    (*commit_data).committime = pq_getmsgint64(in_) as TimestampTz;

    xid
}

/*
 * Write STREAM ABORT to the output stream. Note that xid and subxid will be
 * same for the top-level transaction abort.
 *
 * If write_abort_info is true, send the abort_lsn and abort_time fields,
 * otherwise don't.
 */
pub unsafe fn logicalrep_write_stream_abort(
    out: StringInfo,
    xid: TransactionId,
    subxid: TransactionId,
    abort_lsn: XLogRecPtr,
    abort_time: TimestampTz,
    write_abort_info: bool,
) {
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_ABORT as uint8);

    Assert!(TransactionIdIsValid(xid) && TransactionIdIsValid(subxid));

    /* transaction ID */
    pq_sendint32(out, xid);
    pq_sendint32(out, subxid);

    if write_abort_info {
        pq_sendint64(out, abort_lsn);
        pq_sendint64(out, abort_time as u64);
    }
}

/*
 * Read STREAM ABORT from the output stream.
 *
 * If read_abort_info is true, read the abort_lsn and abort_time fields,
 * otherwise don't.
 */
pub unsafe fn logicalrep_read_stream_abort(
    in_: StringInfo,
    abort_data: *mut LogicalRepStreamAbortData,
    read_abort_info: bool,
) {
    Assert!(!abort_data.is_null());

    (*abort_data).xid = pq_getmsgint(in_, 4);
    (*abort_data).subxid = pq_getmsgint(in_, 4);

    if read_abort_info {
        (*abort_data).abort_lsn = pq_getmsgint64(in_) as XLogRecPtr;
        (*abort_data).abort_time = pq_getmsgint64(in_) as TimestampTz;
    } else {
        (*abort_data).abort_lsn = InvalidXLogRecPtr;
        (*abort_data).abort_time = 0;
    }
}

/*
 * Get string representing LogicalRepMsgType.
 */
pub unsafe fn logicalrep_message_type(action: LogicalRepMsgType) -> *const c_char {
    // static char err_unknown[20];
    static mut ERR_UNKNOWN: [c_char; 20] = [0; 20];

    match action {
        _ if action == LOGICAL_REP_MSG_BEGIN => return c"BEGIN".as_ptr(),
        _ if action == LOGICAL_REP_MSG_COMMIT => return c"COMMIT".as_ptr(),
        _ if action == LOGICAL_REP_MSG_ORIGIN => return c"ORIGIN".as_ptr(),
        _ if action == LOGICAL_REP_MSG_INSERT => return c"INSERT".as_ptr(),
        _ if action == LOGICAL_REP_MSG_UPDATE => return c"UPDATE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_DELETE => return c"DELETE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_TRUNCATE => return c"TRUNCATE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_RELATION => return c"RELATION".as_ptr(),
        _ if action == LOGICAL_REP_MSG_TYPE => return c"TYPE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_MESSAGE => return c"MESSAGE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_BEGIN_PREPARE => return c"BEGIN PREPARE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_PREPARE => return c"PREPARE".as_ptr(),
        _ if action == LOGICAL_REP_MSG_COMMIT_PREPARED => return c"COMMIT PREPARED".as_ptr(),
        _ if action == LOGICAL_REP_MSG_ROLLBACK_PREPARED => return c"ROLLBACK PREPARED".as_ptr(),
        _ if action == LOGICAL_REP_MSG_STREAM_START => return c"STREAM START".as_ptr(),
        _ if action == LOGICAL_REP_MSG_STREAM_STOP => return c"STREAM STOP".as_ptr(),
        _ if action == LOGICAL_REP_MSG_STREAM_COMMIT => return c"STREAM COMMIT".as_ptr(),
        _ if action == LOGICAL_REP_MSG_STREAM_ABORT => return c"STREAM ABORT".as_ptr(),
        _ if action == LOGICAL_REP_MSG_STREAM_PREPARE => return c"STREAM PREPARE".as_ptr(),
        _ => {}
    }

    /*
     * This message provides context in the error raised when applying a
     * logical message. So we can't throw an error here. Return an unknown
     * indicator value so that the original error is still reported.
     */
    let s = format!("??? ({})\0", action);
    let bytes = s.as_bytes();
    let n = core::cmp::min(bytes.len(), 20);
    let dst = core::ptr::addr_of_mut!(ERR_UNKNOWN) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, dst, n);
    *dst.add(19) = 0;

    dst
}

/*
 * Check if the column 'att' of a table should be published.
 *
 * 'columns' represents the publication column list (if any) for that table.
 *
 * 'include_gencols_type' value indicates whether generated columns should be
 * published when there is no column list. Typically, this will have the same
 * value as the 'publish_generated_columns' publication parameter.
 *
 * Note that generated columns can be published only when present in a
 * publication column list, or when include_gencols_type is
 * PUBLISH_GENCOLS_STORED.
 */
pub unsafe fn logicalrep_should_publish_column(
    att: Form_pg_attribute,
    columns: *mut Bitmapset,
    include_gencols_type: PublishGencolsType,
) -> bool {
    if (*att).attisdropped {
        return false;
    }

    /* If a column list is provided, publish only the cols in that list. */
    if !columns.is_null() {
        return bms_is_member((*att).attnum as c_int, columns);
    }

    /* All non-generated columns are always published. */
    if (*att).attgenerated == 0 {
        return true;
    }

    /*
     * Stored generated columns are only published when the user sets
     * publish_generated_columns as stored.
     */
    if (*att).attgenerated == ATTRIBUTE_GENERATED_STORED {
        return include_gencols_type == PUBLISH_GENCOLS_STORED;
    }

    false
}

/// Helper: render a NUL-terminated C string for use in Rust `{}` formatting in
/// elog!/ereport! messages (the C source passes `char *` to elog %s).
unsafe fn cstr(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "(null)";
    }
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    core::str::from_utf8(bytes).unwrap_or("(invalid)")
}
