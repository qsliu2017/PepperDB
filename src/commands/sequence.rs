/*-------------------------------------------------------------------------
 *
 * sequence.rs
 *	  PostgreSQL sequences support code.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/sequence.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use crate::{foreach, current_cell, makeNode};

use core::ffi::{c_char, c_int};

use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{List, ListCell, NIL, lappend, list_length, list_copy_head};
use crate::nodes::parsenodes::{
    AlterSeqStmt, CreateSeqStmt, CreateStmt, DefElem,
};
use crate::nodes::primnodes::{RangeVar, OnCommitAction::ONCOMMIT_NOOP};
use crate::catalog::objectaccess::ObjectAddress;
use crate::parser::parse_node::ParseState;

/* --------------------------------------------------------------------------
 * Local type aliases and stubs for unported dependencies  TODO(pg-port)
 * -------------------------------------------------------------------------- */

// HeapTuple is a single pointer (HeapTupleData*).
use crate::access::htup_details::HeapTupleData;
type HeapTuple = *mut HeapTupleData;

// HeapTupleHeader
type HeapTupleHeader = *mut crate::access::htup_details::HeapTupleHeaderData;

// Relation pointer
type RelationData = crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// RelFileLocator - storage/relfilelocator.h shape (same as utils::rel uses)
use crate::common::blkreftable::RelFileLocator;

// TupleDesc
use crate::access::common::tupdesc::TupleDesc;

// Buffer / Page / etc.
type Buffer = c_int;
type Page = *mut c_char;
type Item = *mut c_char;
type OffsetNumber = u16;
type BlockNumber = u32;
type ForkNumber = c_int;
type Size = usize;
type LocalTransactionId = u32;
type RelFileNumber = u32;
type AttrNumber = i16;
type Datum = crate::postgres::Datum;
type FunctionCallInfo = *mut crate::utils::fmgr::FunctionCallInfoBaseData;
type XLogRecPtr = u64;
type XLogReaderState = c_void;
type SMgrRelation = *mut c_void;
type ResourceOwner = *mut c_void;
type HTAB = c_void;
type AclResult = c_int;
type DependencyType = c_int;

// Form_pg_sequence / Form_pg_sequence_data  TODO(pg-port)
#[repr(C)]
pub struct FormData_pg_sequence {
    pub seqrelid: Oid,
    pub seqtypid: Oid,
    pub seqstart: i64,
    pub seqincrement: i64,
    pub seqmax: i64,
    pub seqmin: i64,
    pub seqcache: i64,
    pub seqcycle: bool,
}
type Form_pg_sequence = *mut FormData_pg_sequence;

#[repr(C)]
pub struct FormData_pg_sequence_data {
    pub last_value: i64,
    pub log_cnt: i64,
    pub is_called: bool,
}
type Form_pg_sequence_data = *mut FormData_pg_sequence_data;

// ColumnDef / TypeName  TODO(pg-port)
#[repr(C)] pub struct ColumnDef { pub is_not_null: bool, _opaque: [u8; 0] }
#[repr(C)] pub struct TypeName { _opaque: [u8; 0] }

// xl_seq_rec - WAL record for sequences  TODO(pg-port)
#[repr(C)]
pub struct xl_seq_rec {
    pub locator: RelFileLocator,
}

// MyProc-ish vxid struct (storage/proc.h)  TODO(pg-port)
#[repr(C)]
pub struct PGPROC {
    pub vxid: VXIDStruct,
}
#[repr(C)]
pub struct VXIDStruct {
    pub lxid: LocalTransactionId,
}

/* --------------------------------------------------------------------------
 * Constants
 * -------------------------------------------------------------------------- */

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
const SEQ_LOG_VALS: i64 = 32;

/*
 * The "special area" of a sequence's buffer page looks like this.
 */
const SEQ_MAGIC: u32 = 0x1717;

#[repr(C)]
struct sequence_magic {
    magic: u32,
}

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 */
#[repr(C)]
struct SeqTableData {
    relid: Oid,			/* pg_class OID of this sequence (hash key) */
    filenumber: RelFileNumber,	/* last seen relfilenumber of this sequence */
    lxid: LocalTransactionId,	/* xact in which we last did a seq op */
    last_valid: bool,		/* do we have a valid "last" value? */
    last: i64,			/* value last returned by nextval */
    cached: i64,			/* last value already cached for nextval */
    /* if last != cached, we have not used up all the cached values */
    increment: i64,		/* copy of sequence's increment field */
    /* note that increment is zero until we first do nextval_internal() */
}

type SeqTable = *mut SeqTableData;

static mut seqhashtab: *mut HTAB = core::ptr::null_mut(); /* hash table for SeqTable items */

/*
 * last_used_seq is updated by nextval() to point to the last used
 * sequence.
 */
static mut last_used_seq: *mut SeqTableData = core::ptr::null_mut();

/* SEQ_COL_* / Natts_pg_sequence column indexes  TODO(pg-port) */
const SEQ_COL_LASTVAL: c_int = 1;
const SEQ_COL_LOG: c_int = 2;
const SEQ_COL_CALLED: c_int = 3;
const SEQ_COL_FIRSTCOL: c_int = SEQ_COL_LASTVAL;
const SEQ_COL_LASTCOL: c_int = SEQ_COL_CALLED;

const Natts_pg_sequence: usize = 8;
const Anum_pg_sequence_seqrelid: c_int = 1;
const Anum_pg_sequence_seqtypid: c_int = 2;
const Anum_pg_sequence_seqstart: c_int = 3;
const Anum_pg_sequence_seqincrement: c_int = 4;
const Anum_pg_sequence_seqmax: c_int = 5;
const Anum_pg_sequence_seqmin: c_int = 6;
const Anum_pg_sequence_seqcache: c_int = 7;
const Anum_pg_sequence_seqcycle: c_int = 8;

/* misc constants  TODO(pg-port) */
const NoLock: c_int = 0;
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 3;
const ShareRowExclusiveLock: c_int = 5;
const AccessExclusiveLock: c_int = 8;

const InvalidOid: Oid = 0;
const InvalidOffsetNumber: OffsetNumber = 0;
const FirstOffsetNumber: OffsetNumber = 1;
const InvalidRelFileNumber: RelFileNumber = 0;
const InvalidLocalTransactionId: LocalTransactionId = 0;
const InvalidTransactionId: u32 = 0;
const InvalidMultiXactId: u32 = 0;
const FrozenTransactionId: u32 = 2;
const FirstCommandId: u32 = 0;
const INVALID_PROC_NUMBER: c_int = -1;

const MAIN_FORKNUM: ForkNumber = 0;
const INIT_FORKNUM: ForkNumber = 3;

const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
const RELKIND_SEQUENCE: c_char = b'S' as c_char;
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
const RELKIND_VIEW: c_char = b'v' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

const RM_SEQ_ID: u8 = 1;
const XLOG_SEQ_LOG: u8 = 0x00;
const XLR_INFO_MASK: u8 = 0x0F;
const REGBUF_WILL_INIT: c_int = 0x04 | 0x02;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

const EB_LOCK_FIRST: c_int = 1 << 1;
const EB_SKIP_EXTENSION_LOCK: c_int = 1 << 0;


const HEAP_XMAX_INVALID: u16 = 0x0800;
const HEAP_XMAX_COMMITTED: u16 = 0x0400;
const HEAP_XMAX_IS_MULTI: u16 = 0x1000;

const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const BOOLOID: Oid = 16;

const PG_INT16_MIN: i64 = -32768;
const PG_INT16_MAX: i64 = 32767;
const PG_INT32_MIN: i64 = -2147483648;
const PG_INT32_MAX: i64 = 2147483647;
const PG_INT64_MIN: i64 = i64::MIN;
const PG_INT64_MAX: i64 = i64::MAX;

const ACL_USAGE: u32 = 1 << 8;
const ACL_UPDATE: u32 = 1 << 2;
const ACL_SELECT: u32 = 1 << 1;
const ACLCHECK_OK: AclResult = 0;

const DEPENDENCY_INTERNAL: DependencyType = b'i' as DependencyType;
const DEPENDENCY_AUTO: DependencyType = b'a' as DependencyType;

const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0010;

const InvalidAttrNumber: AttrNumber = 0;

const RVR_MISSING_OK: c_int = 1 << 0;

const TYPEFUNC_COMPOSITE: c_int = 0;

// Syscache ids  TODO(pg-port)
const SEQRELID: c_int = 61;
const RELOID: c_int = 57;

// Catalog OIDs  TODO(pg-port)
const RelationRelationId: Oid = 1259;
const SequenceRelationId: Oid = 2224;

// ereport errcodes referenced via errcode!() folds  TODO(pg-port)

const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

/* --------------------------------------------------------------------------
 * Stub implementations for unported dependencies  TODO(pg-port)
 * These are functions defined in OTHER .c files.
 * -------------------------------------------------------------------------- */

unsafe fn makeColumnDef(colname: *const c_char, typeOid: Oid, typmod: i32, collOid: Oid) -> *mut ColumnDef { crate::nodes::makefuncs::makeColumnDef(colname as _, typeOid as _, typmod as _, collOid as _) as _ }
unsafe fn DefineRelation(stmt: *mut CreateStmt, relkind: c_char, ownerId: Oid, typaddress: *mut ObjectAddress, queryString: *const c_char) -> ObjectAddress { crate::commands::tablecmds::DefineRelation(stmt as _, relkind as _, ownerId as _, typaddress as _, queryString as _) as _ }
unsafe fn RangeVarGetAndCheckCreationNamespace(relation: *mut RangeVar, lockmode: c_int, existing_relation_id: *mut Oid) -> Oid { crate::catalog::namespace::RangeVarGetAndCheckCreationNamespace(relation as _, lockmode as _, existing_relation_id as _) as _ }
unsafe fn checkMembershipInCurrentExtension(object: *const ObjectAddress) { crate::catalog::pg_depend::checkMembershipInCurrentExtension(object as _); }
unsafe fn sequence_open(relid: Oid, lockmode: c_int) -> Relation { crate::access::sequence::sequence::sequence_open(relid as _, lockmode as _) as _ }
unsafe fn sequence_close(relation: Relation, lockmode: c_int) { crate::access::sequence::sequence::sequence_close(relation as _, lockmode as _); }
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation { crate::access::table::table::table_open(relationId as _, lockmode as _) as _ }
unsafe fn table_close(relation: Relation, lockmode: c_int) { crate::access::table::table::table_close(relation as _, lockmode as _); }
unsafe fn relation_openrv(relation: *const RangeVar, lockmode: c_int) -> Relation { crate::access::common::relation::relation_openrv(relation as _, lockmode as _) as _ }
unsafe fn relation_close(relation: Relation, lockmode: c_int) { crate::access::common::relation::relation_close(relation as _, lockmode as _); }
unsafe fn try_relation_open(relationId: Oid, lockmode: c_int) -> Relation { crate::access::common::relation::try_relation_open(relationId as _, lockmode as _) as _ }
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc { crate::utils::rel::RelationGetDescr(relation as _) as _ }
unsafe fn RelationGetRelationName(relation: Relation) -> *const c_char { crate::utils::rel::RelationGetRelationName(relation as _) as _ }
unsafe fn RelationGetRelid(relation: Relation) -> Oid { crate::utils::rel::RelationGetRelid(relation as _) as _ }
unsafe fn RelationGetNamespace(relation: Relation) -> Oid { crate::utils::rel::RelationGetNamespace(relation as _) as _ }
unsafe fn RelationNeedsWAL(relation: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(relation as _) as _ }
unsafe fn RelationIsPermanent(relation: Relation) -> bool { crate::utils::cache::relcache::RelationIsPermanent(relation as _) as _ }
unsafe fn RELATION_IS_OTHER_TEMP(relation: Relation) -> bool { crate::backend_link_shims::RELATION_IS_OTHER_TEMP(relation as _) as _ }
unsafe fn RelationSetNewRelfilenumber(relation: Relation, persistence: c_char) { crate::utils::cache::relcache::RelationSetNewRelfilenumber(relation as _, persistence as _); }
unsafe fn heap_form_tuple(tupleDescriptor: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple { crate::access::common::heaptuple::heap_form_tuple(tupleDescriptor as _, values as _, isnull as _) as _ }
unsafe fn heap_copytuple(tuple: HeapTuple) -> HeapTuple { crate::access::common::heaptuple::heap_copytuple(tuple as _) as _ }
unsafe fn heap_freetuple(htup: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(htup as _); }
unsafe fn HeapTupleGetDatum(tuple: HeapTuple) -> Datum { crate::executor::execTuples::HeapTupleHeaderGetDatum((*tuple).t_data as _) as _ }
unsafe fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void { crate::access::htup_details::GETSTRUCT(tuple as _) as _ }
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) -> Oid { crate::catalog::indexing::CatalogTupleInsert(heapRel as _, tup as _); 0 as Oid }
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *mut c_void, tup: HeapTuple) { crate::catalog::indexing::CatalogTupleUpdate(heapRel as _, otid as _, tup as _); }
unsafe fn CatalogTupleDelete(heapRel: Relation, otid: *mut c_void) { crate::catalog::indexing::CatalogTupleDelete(heapRel as _, otid as _); }
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache1(cacheId as _, key1 as _) as _ }
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple {
    let tup = SearchSysCache1(cacheId, key1);
    if tup.is_null() { return tup; }
    let newtup = heap_copytuple(tup);
    ReleaseSysCache(tup);
    newtup
}
unsafe fn SearchSysCacheExists1(cacheId: c_int, key1: Datum) -> bool { crate::utils::cache::syscache::SearchSysCacheExists1(cacheId as _, key1 as _) as _ }
unsafe fn ReleaseSysCache(tuple: HeapTuple) { crate::utils::cache::syscache::ReleaseSysCache(tuple as _); }
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool { !tuple.is_null() }
unsafe fn OidIsValid(objectId: Oid) -> bool { objectId != InvalidOid }
unsafe fn ObjectIdGetDatum(X: Oid) -> Datum { crate::postgres::ObjectIdGetDatum(X) }
unsafe fn Int64GetDatum(X: i64) -> Datum { crate::postgres::Int64GetDatum(X) }
unsafe fn Int64GetDatumFast(X: i64) -> Datum { crate::postgres::Int64GetDatum(X) }
unsafe fn BoolGetDatum(X: bool) -> Datum { crate::postgres::BoolGetDatum(X) }

unsafe fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: u32) -> AclResult { crate::catalog::aclchk::pg_class_aclcheck(table_oid as _, roleid as _, mode as _) as _ }
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() as _ }
unsafe fn PreventCommandIfReadOnly(cmdname: *const c_char) { crate::tcop::utility::PreventCommandIfReadOnly(cmdname as _); }
unsafe fn PreventCommandIfParallelMode(cmdname: *const c_char) { crate::tcop::utility::PreventCommandIfParallelMode(cmdname as _); }

unsafe fn RangeVarGetRelid(relation: *const RangeVar, lockmode: c_int, missing_ok: bool) -> Oid { crate::catalog::namespace::RangeVarGetRelid(relation as _, lockmode as _, missing_ok as _) as _ }
unsafe fn RangeVarGetRelidExtended(relation: *const RangeVar, lockmode: c_int, flags: c_int, callback: RangeVarGetRelidCallback, callback_arg: *mut c_void) -> Oid { crate::catalog::namespace::RangeVarGetRelidExtended(relation as _, lockmode as _, flags as _, core::mem::transmute(callback), callback_arg as _) as _ }
type RangeVarGetRelidCallback = Option<unsafe extern "C" fn(*const RangeVar, Oid, Oid, *mut c_void)>;
unsafe extern "C" fn RangeVarCallbackOwnsRelation(relation: *const RangeVar, relId: Oid, oldRelId: Oid, arg: *mut c_void) { crate::commands::tablecmds::RangeVarCallbackOwnsRelation(relation as _, relId as _, oldRelId as _, arg as _); }
unsafe fn makeRangeVarFromNameList(names: *mut List) -> *mut RangeVar { crate::catalog::namespace::makeRangeVarFromNameList(names as _) as _ }
unsafe fn textToQualifiedNameList(textval: *mut c_void) -> *mut List { crate::utils::adt::varlena::textToQualifiedNameList(textval as _) as _ }

unsafe fn LockRelationOid(relid: Oid, lockmode: c_int) { crate::storage::lmgr::lmgr::LockRelationOid(relid as _, lockmode as _); }

unsafe fn ExtendBufferedRel(bmr: BulkMaxRelHandle, fork: ForkNumber, strategy: *mut c_void, flags: c_int) -> Buffer { crate::storage::buffer::bufmgr::ExtendBufferedRel(crate::storage::buffer::bufmgr::BMR_REL(bmr as _), fork as _, strategy as _, flags as _) as _ }
type BulkMaxRelHandle = *mut c_void;
unsafe fn BMR_REL(rel: Relation) -> BulkMaxRelHandle { rel as _ }
unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber { crate::storage::buffer::bufmgr::BufferGetBlockNumber(buffer as _) as _ }
unsafe fn BufferGetPage(buffer: Buffer) -> Page { crate::storage::buffer::bufmgr::BufferGetPage(buffer as _) as _ }
unsafe fn BufferGetPageSize(buffer: Buffer) -> Size { crate::access::nbtree::nbtpage::BufferGetPageSize(buffer as _) as _ }
unsafe fn ReadBuffer(reln: Relation, blockNum: BlockNumber) -> Buffer { crate::storage::buffer::bufmgr::ReadBuffer(reln as _, blockNum as _) as _ }
unsafe fn LockBuffer(buffer: Buffer, mode: c_int) { crate::storage::buffer::bufmgr::LockBuffer(buffer as _, mode as _); }
unsafe fn MarkBufferDirty(buffer: Buffer) { crate::storage::buffer::bufmgr::MarkBufferDirty(buffer as _); }
unsafe fn MarkBufferDirtyHint(buffer: Buffer, buffer_std: bool) { crate::storage::buffer::bufmgr::MarkBufferDirtyHint(buffer as _, buffer_std as _); }
unsafe fn UnlockReleaseBuffer(buffer: Buffer) { crate::storage::buffer::bufmgr::UnlockReleaseBuffer(buffer as _); }
unsafe fn PageInit(page: Page, pageSize: Size, specialSize: Size) { crate::storage::bufpage::PageInit(page as _, pageSize as _, specialSize as _); }
unsafe fn PageGetSpecialPointer(page: Page) -> *mut c_char { crate::storage::bufpage::PageGetSpecialPointer(page as _) as _ }
unsafe fn PageGetItemId(page: Page, offsetNumber: OffsetNumber) -> ItemId { crate::storage::bufpage::PageGetItemId(page as _, offsetNumber as _) as _ }
type ItemId = *mut c_void;
unsafe fn PageGetItem(page: Page, itemId: ItemId) -> *mut c_char { crate::storage::bufpage::PageGetItem(page as _, itemId as _) as _ }
unsafe fn PageAddItem(page: Page, item: Item, size: Size, offsetNumber: OffsetNumber, overwrite: bool, is_heap: bool) -> OffsetNumber { crate::storage::bufpage::PageAddItem(page as _, item as _, size as _, offsetNumber as _, overwrite as _, is_heap as _) as _ }
unsafe fn PageGetLSN(page: Page) -> XLogRecPtr { crate::storage::bufpage::PageGetLSN(page as _) as _ }
unsafe fn PageSetLSN(page: Page, lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(page as _, lsn as _); }
unsafe fn ItemIdIsNormal(lp: ItemId) -> bool { crate::storage::itemid::ItemIdIsNormal(lp as _) as _ }
unsafe fn ItemIdGetLength(lp: ItemId) -> u32 { crate::storage::itemid::ItemIdGetLength(lp as _) as _ }
unsafe fn ItemPointerSet(pointer: *mut c_void, blockNumber: BlockNumber, offNum: OffsetNumber) { crate::storage::itemptr::ItemPointerSet(pointer as _, blockNumber as _, offNum as _); }

unsafe fn HeapTupleHeaderSetXmin(tup: HeapTupleHeader, xid: u32) { crate::access::htup_details::HeapTupleHeaderSetXmin(tup as _, xid as _); }
unsafe fn HeapTupleHeaderSetXminFrozen(tup: HeapTupleHeader) { crate::access::htup_details::HeapTupleHeaderSetXminFrozen(tup as _); }
unsafe fn HeapTupleHeaderSetCmin(tup: HeapTupleHeader, cid: u32) { crate::access::htup_details::HeapTupleHeaderSetCmin(tup as _, cid as _); }
unsafe fn HeapTupleHeaderSetXmax(tup: HeapTupleHeader, xid: u32) { crate::access::htup_details::HeapTupleHeaderSetXmax(tup as _, xid as _); }
unsafe fn HeapTupleHeaderGetRawXmax(tup: HeapTupleHeader) -> u32 { crate::access::htup_details::HeapTupleHeaderGetRawXmax(tup as _) as _ }

unsafe fn GetTopTransactionId() -> u32 { crate::access::transam::xact::GetTopTransactionId() as _ }
unsafe fn GetRedoRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetRedoRecPtr() as _ }
unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() as _ }

unsafe fn smgropen(rlocator: RelFileLocator, backend: c_int) -> SMgrRelation { crate::storage::smgr::smgr::smgropen(rlocator as _, backend as _) as _ }
unsafe fn smgrcreate(reln: SMgrRelation, forknum: ForkNumber, isRedo: bool) { crate::storage::smgr::smgr::smgrcreate(reln as _, forknum as _, isRedo as _); }
unsafe fn smgrclose(reln: SMgrRelation) { crate::storage::smgr::smgr::smgrclose(reln as _); }
unsafe fn log_smgrcreate(rlocator: *const RelFileLocator, forkNum: ForkNumber) { crate::catalog::storage::log_smgrcreate(rlocator as _, forkNum as _); }
unsafe fn FlushRelationBuffers(rel: Relation) { crate::storage::buffer::bufmgr::FlushRelationBuffers(rel as _); }

unsafe fn XLogBeginInsert() { crate::access::transam::xloginsert::XLogBeginInsert(); }
unsafe fn XLogRegisterBuffer(block_id: u8, buffer: Buffer, flags: c_int) { crate::access::transam::xloginsert::XLogRegisterBuffer(block_id as _, buffer as _, flags as _); }
unsafe fn XLogRegisterData(data: *mut c_void, len: c_int) { crate::access::transam::xloginsert::XLogRegisterData(data as _, len as _); }
unsafe fn XLogInsert(rmid: u8, info: u8) -> XLogRecPtr { crate::access::transam::xloginsert::XLogInsert(rmid as _, info as _) as _ }
unsafe fn XLogInitBufferForRedo(record: *mut XLogReaderState, block_id: u8) -> Buffer { crate::access::transam::xlogutils::XLogInitBufferForRedo(record as _, block_id as _) as _ }
unsafe fn XLogRecGetInfo(record: *mut XLogReaderState) -> u8 { crate::access::transam::xlogreader::XLogRecGetInfo(record as _) as _ }
unsafe fn XLogRecGetData(record: *mut XLogReaderState) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetData(record as _) as _ }
unsafe fn XLogRecGetDataLen(record: *mut XLogReaderState) -> u32 { crate::access::transam::xlogreader::XLogRecGetDataLen(record as _) as _ }
unsafe fn record_EndRecPtr(record: *mut XLogReaderState) -> XLogRecPtr { (*(record as *mut crate::access::transam::xlogreader::XLogReaderState)).EndRecPtr as _ }

unsafe fn mask_page_lsn_and_checksum(page: Page) { crate::access::common::bufmask::mask_page_lsn_and_checksum(page as _); }
unsafe fn mask_unused_space(page: Page) { crate::access::common::bufmask::mask_unused_space(page as _); }

unsafe fn hash_create(tabname: *const c_char, nelem: c_long, info: *mut HASHCTL, flags: c_int) -> *mut HTAB {
    let mut ctl: crate::utils::hash::dynahash::HASHCTL = core::mem::zeroed();
    ctl.keysize = (*info).keysize;
    ctl.entrysize = (*info).entrysize;
    crate::utils::hash::dynahash::hash_create(tabname as _, nelem as _, &ctl, flags as _) as _
}
unsafe fn hash_search(hashp: *mut HTAB, keyPtr: *const c_void, action: c_int, foundPtr: *mut bool) -> *mut c_void { crate::utils::hash::dynahash::hash_search(hashp as _, keyPtr as _, core::mem::transmute(action), foundPtr as _) as _ }
unsafe fn hash_destroy(hashp: *mut HTAB) { crate::utils::hash::dynahash::hash_destroy(hashp as _); }
const HASH_ENTER: c_int = 1;
#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
}

unsafe fn defGetInt64(def: *mut DefElem) -> i64 { crate::commands::define::defGetInt64(def as _) as _ }
unsafe fn defGetTypeName(def: *mut DefElem) -> *mut TypeName { crate::commands::define::defGetTypeName(def as _) as _ }
unsafe fn defGetQualifiedName(def: *mut DefElem) -> *mut List { crate::commands::define::defGetQualifiedName(def as _) as _ }
unsafe fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState) { crate::commands::define::errorConflictingDefElem(defel as _, pstate as _); }
unsafe fn typenameTypeId(pstate: *mut ParseState, typeName: *mut TypeName) -> Oid { crate::parser::parse_type::typenameTypeId(pstate as _, typeName as _) as _ }
unsafe fn format_type_be(type_oid: Oid) -> *mut c_char { crate::utils::adt::format_type::format_type_be(type_oid as _) as _ }
unsafe fn boolVal(node: *mut Node) -> bool { crate::boolVal!(node) }
unsafe fn strVal(node: *mut c_void) -> *mut c_char { crate::strVal!(node) as _ }
unsafe fn makeFloat(numericStr: *mut c_char) -> *mut Node { crate::nodes::value::makeFloat(numericStr as _) as _ }
unsafe fn makeBoolean(val: bool) -> *mut Node { crate::nodes::value::makeBoolean(val as _) as _ }
unsafe fn makeDefElem(name: *mut c_char, arg: *mut Node, location: c_int) -> *mut DefElem { crate::nodes::makefuncs::makeDefElem(name as _, arg as _, location as _) as _ }
unsafe fn psprintf_int64(val: i64) -> *mut c_char {
    let buf = crate::utils::mmgr::mcxt::palloc(32) as *mut c_char;
    crate::utils::adt::numutils::pg_lltoa(val as _, buf as _);
    buf
}
unsafe fn linitial(l: *mut List) -> *mut c_void { crate::nodes::pg_list::linitial(l as _) as _ }
unsafe fn llast(l: *mut List) -> *mut c_void { crate::nodes::pg_list::llast(l as _) as _ }
unsafe fn lfirst(lc: *mut ListCell) -> *mut c_void { crate::nodes::pg_list::lfirst(lc as _) as _ }

unsafe fn get_attnum(relid: Oid, attname: *const c_char) -> AttrNumber { crate::utils::cache::lsyscache::get_attnum(relid as _, attname as _) as _ }
unsafe fn get_rel_name(relid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_rel_name(relid as _) as _ }
unsafe fn sequenceIsOwned(seqId: Oid, deptype: c_char, tableId: *mut Oid, colId: *mut i32) -> bool { crate::catalog::pg_depend::sequenceIsOwned(seqId as _, deptype as _, tableId as _, colId as _) as _ }
unsafe fn deleteDependencyRecordsForClass(classId: Oid, objectId: Oid, refclassId: Oid, deptype: c_char) -> c_long { crate::catalog::pg_depend::deleteDependencyRecordsForClass(classId as _, objectId as _, refclassId as _, deptype as _) as _ }
unsafe fn recordDependencyOn(depender: *const ObjectAddress, referenced: *const ObjectAddress, behavior: DependencyType) { crate::catalog::pg_depend::recordDependencyOn(depender as _, referenced as _, behavior as _); }
unsafe fn errdetail_relkind_not_supported(relkind: c_char) -> c_int { crate::catalog::pg_class::errdetail_relkind_not_supported(relkind as _) as _ }
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) { /* no-op: object_access_hook not installed */ }
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

unsafe fn get_call_result_type(fcinfo: FunctionCallInfo, resultTypeId: *mut Oid, resultTupleDesc: *mut TupleDesc) -> c_int { crate::utils::fmgr::funcapi::get_call_result_type(fcinfo as _, resultTypeId as _, resultTupleDesc as _) as _ }
unsafe fn CreateTemplateTupleDesc(natts: c_int) -> TupleDesc { crate::access::common::tupdesc::CreateTemplateTupleDesc(natts as _) as _ }
unsafe fn TupleDescInitEntry(desc: TupleDesc, attributeNumber: AttrNumber, attributeName: *const c_char, oidtypeid: Oid, typmod: i32, attdim: c_int) { crate::access::common::tupdesc::TupleDescInitEntry(desc as _, attributeNumber as _, attributeName as _, oidtypeid as _, typmod as _, attdim as _); }
unsafe fn BlessTupleDesc(tupdesc: TupleDesc) -> TupleDesc { crate::executor::execTuples::BlessTupleDesc(tupdesc as _) as _ }

// Accessors for MyProc / resource-owner globals  TODO(pg-port)
unsafe fn MyProc_vxid_lxid() -> LocalTransactionId { (*crate::storage::lmgr::proc::MyProc).vxid.lxid as _ }
unsafe fn CurrentResourceOwner_get() -> ResourceOwner { crate::utils::resowner::resowner::CurrentResourceOwner as _ }
unsafe fn CurrentResourceOwner_set(owner: ResourceOwner) { crate::utils::resowner::resowner::CurrentResourceOwner = owner as _; }
unsafe fn TopTransactionResourceOwner_get() -> ResourceOwner { crate::utils::resowner::resowner::TopTransactionResourceOwner as _ }

unsafe fn START_CRIT_SECTION() { crate::miscadmin::START_CRIT_SECTION(); }
unsafe fn END_CRIT_SECTION() { crate::miscadmin::END_CRIT_SECTION(); }

unsafe fn palloc_page(size: Size) -> Page { crate::utils::mmgr::mcxt::palloc(size as _) as Page }

unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int { libc::strcmp(a, b) }
unsafe fn BoolIsValid(b: bool) -> bool { b == false || b == true }

/* --------------------------------------------------------------------------
 * Translated functions
 * -------------------------------------------------------------------------- */

/*
 * DefineSequence
 *				Creates a new sequence relation
 */
pub unsafe fn DefineSequence(pstate: *mut ParseState, seq: *mut CreateSeqStmt) -> ObjectAddress {
    let mut seqform: FormData_pg_sequence = core::mem::zeroed();
    let mut seqdataform: FormData_pg_sequence_data = core::mem::zeroed();
    let mut need_seq_rewrite: bool = false;
    let mut owned_by: *mut List = core::ptr::null_mut();
    let stmt: *mut CreateStmt = makeNode!(CreateStmt, T_CreateStmt);
    let mut seqoid: Oid;
    let mut address: ObjectAddress = core::mem::zeroed();
    let mut rel: Relation;
    let mut tuple: HeapTuple;
    let mut tupDesc: TupleDesc;
    let mut value: [Datum; SEQ_COL_LASTCOL as usize] = [0; SEQ_COL_LASTCOL as usize];
    let mut null: [bool; SEQ_COL_LASTCOL as usize] = [false; SEQ_COL_LASTCOL as usize];
    let mut pgs_values: [Datum; Natts_pg_sequence] = [0; Natts_pg_sequence];
    let mut pgs_nulls: [bool; Natts_pg_sequence] = [false; Natts_pg_sequence];
    let mut i: c_int;

    /*
     * If if_not_exists was given and a relation with the same name already
     * exists, bail out. (Note: we needn't check this when not if_not_exists,
     * because DefineRelation will complain anyway.)
     */
    if (*seq).if_not_exists {
        seqoid = InvalidOid;
        RangeVarGetAndCheckCreationNamespace((*seq).sequence, NoLock, &mut seqoid);
        if OidIsValid(seqoid) {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            ObjectAddressSet(&mut address, RelationRelationId, seqoid);
            checkMembershipInCurrentExtension(&address);

            /* OK to skip */
            ereport!(NOTICE, errmsg!("relation \"{}\" already exists, skipping",
                std::ffi::CStr::from_ptr((*(*seq).sequence).relname).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_TABLE) */
            return InvalidObjectAddress;
        }
    }

    /* Check and set all option values */
    init_params(pstate, (*seq).options, (*seq).for_identity, true,
                &mut seqform, &mut seqdataform,
                &mut need_seq_rewrite, &mut owned_by);

    /*
     * Create relation (and fill value[] and null[] for the tuple)
     */
    (*stmt).tableElts = NIL;
    i = SEQ_COL_FIRSTCOL;
    while i <= SEQ_COL_LASTCOL {
        let mut coldef: *mut ColumnDef = core::ptr::null_mut();

        if i == SEQ_COL_LASTVAL {
            coldef = makeColumnDef(c"last_value".as_ptr(), INT8OID, -1, InvalidOid);
            value[(i - 1) as usize] = Int64GetDatumFast(seqdataform.last_value);
        } else if i == SEQ_COL_LOG {
            coldef = makeColumnDef(c"log_cnt".as_ptr(), INT8OID, -1, InvalidOid);
            value[(i - 1) as usize] = Int64GetDatum(0i64);
        } else if i == SEQ_COL_CALLED {
            coldef = makeColumnDef(c"is_called".as_ptr(), BOOLOID, -1, InvalidOid);
            value[(i - 1) as usize] = BoolGetDatum(false);
        }

        (*coldef).is_not_null = true;
        null[(i - 1) as usize] = false;

        (*stmt).tableElts = lappend((*stmt).tableElts, coldef as *mut c_void);

        i += 1;
    }

    (*stmt).relation = (*seq).sequence;
    (*stmt).inhRelations = NIL;
    (*stmt).constraints = NIL;
    (*stmt).options = NIL;
    (*stmt).oncommit = ONCOMMIT_NOOP;
    (*stmt).tablespacename = core::ptr::null_mut();
    (*stmt).if_not_exists = (*seq).if_not_exists;

    address = DefineRelation(stmt, RELKIND_SEQUENCE, (*seq).ownerId, core::ptr::null_mut(), core::ptr::null());
    seqoid = address.objectId;
    Assert!(seqoid != InvalidOid);

    rel = sequence_open(seqoid, AccessExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* now initialize the sequence's data */
    tuple = heap_form_tuple(tupDesc, value.as_mut_ptr(), null.as_mut_ptr());
    fill_seq_with_data(rel, tuple);

    /* process OWNED BY if given */
    if !owned_by.is_null() {
        process_owned_by(rel, owned_by, (*seq).for_identity);
    }

    sequence_close(rel, NoLock);

    /* fill in pg_sequence */
    rel = table_open(SequenceRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    core::ptr::write_bytes(pgs_nulls.as_mut_ptr(), 0, pgs_nulls.len());

    pgs_values[(Anum_pg_sequence_seqrelid - 1) as usize] = ObjectIdGetDatum(seqoid);
    pgs_values[(Anum_pg_sequence_seqtypid - 1) as usize] = ObjectIdGetDatum(seqform.seqtypid);
    pgs_values[(Anum_pg_sequence_seqstart - 1) as usize] = Int64GetDatumFast(seqform.seqstart);
    pgs_values[(Anum_pg_sequence_seqincrement - 1) as usize] = Int64GetDatumFast(seqform.seqincrement);
    pgs_values[(Anum_pg_sequence_seqmax - 1) as usize] = Int64GetDatumFast(seqform.seqmax);
    pgs_values[(Anum_pg_sequence_seqmin - 1) as usize] = Int64GetDatumFast(seqform.seqmin);
    pgs_values[(Anum_pg_sequence_seqcache - 1) as usize] = Int64GetDatumFast(seqform.seqcache);
    pgs_values[(Anum_pg_sequence_seqcycle - 1) as usize] = BoolGetDatum(seqform.seqcycle);

    tuple = heap_form_tuple(tupDesc, pgs_values.as_mut_ptr(), pgs_nulls.as_mut_ptr());
    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);
    table_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Reset a sequence to its initial value.
 *
 * The change is made transactionally, so that on failure of the current
 * transaction, the sequence will be restored to its previous state.
 * We do that by creating a whole new relfilenumber for the sequence; so this
 * works much like the rewriting forms of ALTER TABLE.
 *
 * Caller is assumed to have acquired AccessExclusiveLock on the sequence,
 * which must not be released until end of transaction.  Caller is also
 * responsible for permissions checking.
 */
pub unsafe fn ResetSequence(seq_relid: Oid) {
    let mut seq_rel: Relation = core::ptr::null_mut();
    let mut elm: SeqTable = core::ptr::null_mut();
    let seq: Form_pg_sequence_data;
    let mut buf: Buffer = 0;
    let mut seqdatatuple: HeapTupleData = core::mem::zeroed();
    let tuple: HeapTuple;
    let pgstuple: HeapTuple;
    let pgsform: Form_pg_sequence;
    let startv: i64;

    /*
     * Read the old sequence.  This does a bit more work than really
     * necessary, but it's simple, and we do want to double-check that it's
     * indeed a sequence.
     */
    init_sequence(seq_relid, &mut elm, &mut seq_rel);
    read_seq_tuple(seq_rel, &mut buf, &mut seqdatatuple);

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seq_relid));
    if !HeapTupleIsValid(pgstuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", seq_relid);
    }
    pgsform = GETSTRUCT(pgstuple) as Form_pg_sequence;
    startv = (*pgsform).seqstart;
    ReleaseSysCache(pgstuple);

    /*
     * Copy the existing sequence tuple.
     */
    tuple = heap_copytuple(&mut seqdatatuple);

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);

    /*
     * Modify the copied tuple to execute the restart (compare the RESTART
     * action in AlterSequence)
     */
    seq = GETSTRUCT(tuple) as Form_pg_sequence_data;
    (*seq).last_value = startv;
    (*seq).is_called = false;
    (*seq).log_cnt = 0;

    /*
     * Create a new storage file for the sequence.
     */
    RelationSetNewRelfilenumber(seq_rel, (*(*seq_rel).rd_rel).relpersistence);

    /*
     * Ensure sequence's relfrozenxid is at 0, since it won't contain any
     * unfrozen XIDs.  Same with relminmxid, since a sequence will never
     * contain multixacts.
     */
    Assert!((*(*seq_rel).rd_rel).relfrozenxid == InvalidTransactionId);
    Assert!((*(*seq_rel).rd_rel).relminmxid == InvalidMultiXactId);

    /*
     * Insert the modified tuple into the new storage file.
     */
    fill_seq_with_data(seq_rel, tuple);

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    (*elm).cached = (*elm).last;

    sequence_close(seq_rel, NoLock);
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 *
 * This handles unlogged sequences by writing to both the main and the init
 * fork as necessary.
 */
unsafe fn fill_seq_with_data(rel: Relation, tuple: HeapTuple) {
    fill_seq_fork_with_data(rel, tuple, MAIN_FORKNUM);

    if (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED {
        let srel: SMgrRelation;

        srel = smgropen((*rel).rd_locator, INVALID_PROC_NUMBER);
        smgrcreate(srel, INIT_FORKNUM, false);
        log_smgrcreate(&(*rel).rd_locator, INIT_FORKNUM);
        fill_seq_fork_with_data(rel, tuple, INIT_FORKNUM);
        FlushRelationBuffers(rel);
        smgrclose(srel);
    }
}

/*
 * Initialize a sequence's relation fork with the specified tuple as content
 */
unsafe fn fill_seq_fork_with_data(rel: Relation, tuple: HeapTuple, forkNum: ForkNumber) {
    let buf: Buffer;
    let page: Page;
    let sm: *mut sequence_magic;
    let offnum: OffsetNumber;

    /* Initialize first page of relation with special magic number */

    buf = ExtendBufferedRel(BMR_REL(rel), forkNum, core::ptr::null_mut(),
                            EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
    Assert!(BufferGetBlockNumber(buf) == 0);

    page = BufferGetPage(buf);

    PageInit(page, BufferGetPageSize(buf), core::mem::size_of::<sequence_magic>());
    sm = PageGetSpecialPointer(page) as *mut sequence_magic;
    (*sm).magic = SEQ_MAGIC;

    /* Now insert sequence tuple */

    /*
     * Since VACUUM does not process sequences, we have to force the tuple to
     * have xmin = FrozenTransactionId now.  Otherwise it would become
     * invisible to SELECTs after 2G transactions.  It is okay to do this
     * because if the current transaction aborts, no other xact will ever
     * examine the sequence tuple anyway.
     */
    HeapTupleHeaderSetXmin((*tuple).t_data, FrozenTransactionId);
    HeapTupleHeaderSetXminFrozen((*tuple).t_data);
    HeapTupleHeaderSetCmin((*tuple).t_data, FirstCommandId);
    HeapTupleHeaderSetXmax((*tuple).t_data, InvalidTransactionId);
    (*(*tuple).t_data).t_infomask |= HEAP_XMAX_INVALID;
    ItemPointerSet(&mut (*(*tuple).t_data).t_ctid as *mut _ as *mut c_void, 0, FirstOffsetNumber);

    /* check the comment above nextval_internal()'s equivalent call. */
    if RelationNeedsWAL(rel) {
        GetTopTransactionId();
    }

    START_CRIT_SECTION();

    MarkBufferDirty(buf);

    offnum = PageAddItem(page, (*tuple).t_data as Item, (*tuple).t_len as Size,
                         InvalidOffsetNumber, false, false);
    if offnum != FirstOffsetNumber {
        elog!(ERROR, "failed to add sequence tuple to page");
    }

    /* XLOG stuff */
    if RelationNeedsWAL(rel) || forkNum == INIT_FORKNUM {
        let mut xlrec: xl_seq_rec = core::mem::zeroed();
        let recptr: XLogRecPtr;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        xlrec.locator = (*rel).rd_locator;

        XLogRegisterData(&mut xlrec as *mut _ as *mut c_void, core::mem::size_of::<xl_seq_rec>() as c_int);
        XLogRegisterData((*tuple).t_data as *mut c_void, (*tuple).t_len as c_int);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 */
pub unsafe fn AlterSequence(pstate: *mut ParseState, stmt: *mut AlterSeqStmt) -> ObjectAddress {
    let relid: Oid;
    let mut elm: SeqTable = core::ptr::null_mut();
    let mut seqrel: Relation = core::ptr::null_mut();
    let mut buf: Buffer = 0;
    let mut datatuple: HeapTupleData = core::mem::zeroed();
    let seqform: Form_pg_sequence;
    let newdataform: Form_pg_sequence_data;
    let mut need_seq_rewrite: bool = false;
    let mut owned_by: *mut List = core::ptr::null_mut();
    let mut address: ObjectAddress = core::mem::zeroed();
    let rel: Relation;
    let seqtuple: HeapTuple;
    let newdatatuple: HeapTuple;

    /* Open and lock sequence, and check for ownership along the way. */
    relid = RangeVarGetRelidExtended((*stmt).sequence,
                                     ShareRowExclusiveLock,
                                     if (*stmt).missing_ok { RVR_MISSING_OK } else { 0 },
                                     Some(RangeVarCallbackOwnsRelation),
                                     core::ptr::null_mut());
    if relid == InvalidOid {
        ereport!(NOTICE, errmsg!("relation \"{}\" does not exist, skipping",
            std::ffi::CStr::from_ptr((*(*stmt).sequence).relname).to_string_lossy()));
        return InvalidObjectAddress;
    }

    init_sequence(relid, &mut elm, &mut seqrel);

    rel = table_open(SequenceRelationId, RowExclusiveLock);
    seqtuple = SearchSysCacheCopy1(SEQRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(seqtuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", relid);
    }

    seqform = GETSTRUCT(seqtuple) as Form_pg_sequence;

    /* lock page buffer and read tuple into new sequence structure */
    read_seq_tuple(seqrel, &mut buf, &mut datatuple);

    /* copy the existing sequence data tuple, so it can be modified locally */
    newdatatuple = heap_copytuple(&mut datatuple);
    newdataform = GETSTRUCT(newdatatuple) as Form_pg_sequence_data;

    UnlockReleaseBuffer(buf);

    /* Check and set new values */
    init_params(pstate, (*stmt).options, (*stmt).for_identity, false,
                seqform, newdataform,
                &mut need_seq_rewrite, &mut owned_by);

    /* If needed, rewrite the sequence relation itself */
    if need_seq_rewrite {
        /* check the comment above nextval_internal()'s equivalent call. */
        if RelationNeedsWAL(seqrel) {
            GetTopTransactionId();
        }

        /*
         * Create a new storage file for the sequence, making the state
         * changes transactional.
         */
        RelationSetNewRelfilenumber(seqrel, (*(*seqrel).rd_rel).relpersistence);

        /*
         * Ensure sequence's relfrozenxid is at 0, since it won't contain any
         * unfrozen XIDs.  Same with relminmxid, since a sequence will never
         * contain multixacts.
         */
        Assert!((*(*seqrel).rd_rel).relfrozenxid == InvalidTransactionId);
        Assert!((*(*seqrel).rd_rel).relminmxid == InvalidMultiXactId);

        /*
         * Insert the modified tuple into the new storage file.
         */
        fill_seq_with_data(seqrel, newdatatuple);
    }

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    (*elm).cached = (*elm).last;

    /* process OWNED BY if given */
    if !owned_by.is_null() {
        process_owned_by(seqrel, owned_by, (*stmt).for_identity);
    }

    /* update the pg_sequence tuple (we could skip this in some cases...) */
    CatalogTupleUpdate(rel, &mut (*seqtuple).t_self as *mut _ as *mut c_void, seqtuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

    ObjectAddressSet(&mut address, RelationRelationId, relid);

    table_close(rel, RowExclusiveLock);
    sequence_close(seqrel, NoLock);

    return address;
}

pub unsafe fn SequenceChangePersistence(relid: Oid, newrelpersistence: c_char) {
    let mut elm: SeqTable = core::ptr::null_mut();
    let mut seqrel: Relation = core::ptr::null_mut();
    let mut buf: Buffer = 0;
    let mut seqdatatuple: HeapTupleData = core::mem::zeroed();

    /*
     * ALTER SEQUENCE acquires this lock earlier.  If we're processing an
     * owned sequence for ALTER TABLE, lock now.  Without the lock, we'd
     * discard increments from nextval() calls (in other sessions) between
     * this function's buffer unlock and this transaction's commit.
     */
    LockRelationOid(relid, AccessExclusiveLock);
    init_sequence(relid, &mut elm, &mut seqrel);

    /* check the comment above nextval_internal()'s equivalent call. */
    if RelationNeedsWAL(seqrel) {
        GetTopTransactionId();
    }

    read_seq_tuple(seqrel, &mut buf, &mut seqdatatuple);
    RelationSetNewRelfilenumber(seqrel, newrelpersistence);
    fill_seq_with_data(seqrel, &mut seqdatatuple);
    UnlockReleaseBuffer(buf);

    sequence_close(seqrel, NoLock);
}

pub unsafe fn DeleteSequenceTuple(relid: Oid) {
    let rel: Relation;
    let tuple: HeapTuple;

    rel = table_open(SequenceRelationId, RowExclusiveLock);

    tuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", relid);
    }

    CatalogTupleDelete(rel, &mut (*tuple).t_self as *mut _ as *mut c_void);

    ReleaseSysCache(tuple);
    table_close(rel, RowExclusiveLock);
}

/*
 * Note: nextval with a text argument is no longer exported as a pg_proc
 * entry, but we keep it around to ease porting of C code that may have
 * called the function directly.
 */
pub unsafe extern "C" fn nextval(fcinfo: FunctionCallInfo) -> Datum {
    let seqin = crate::PG_GETARG_TEXT_PP!(fcinfo, 0);
    let sequence: *mut RangeVar;
    let relid: Oid;

    sequence = makeRangeVarFromNameList(textToQualifiedNameList(seqin as _));

    /*
     * XXX: This is not safe in the presence of concurrent DDL, but acquiring
     * a lock here is more expensive than letting nextval_internal do it,
     * since the latter maintains a cache that keeps us from hitting the lock
     * manager more than once per transaction.  It's not clear whether the
     * performance penalty is material in practice, but for now, we do it this
     * way.
     */
    relid = RangeVarGetRelid(sequence, NoLock, false);

    crate::PG_RETURN_INT64!(nextval_internal(relid, true));
}

pub unsafe fn nextval_oid(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);

    crate::PG_RETURN_INT64!(nextval_internal(relid, true));
}

pub unsafe fn nextval_internal(relid: Oid, check_permissions: bool) -> i64 {
    let mut elm: SeqTable = core::ptr::null_mut();
    let mut seqrel: Relation = core::ptr::null_mut();
    let mut buf: Buffer = 0;
    let page: Page;
    let pgstuple: HeapTuple;
    let pgsform: Form_pg_sequence;
    let mut seqdatatuple: HeapTupleData = core::mem::zeroed();
    let seq: Form_pg_sequence_data;
    let incby: i64;
    let maxv: i64;
    let minv: i64;
    let cache: i64;
    let mut log: i64;
    let mut fetch: i64;
    let mut last: i64;
    let mut result: i64;
    let mut next: i64;
    let mut rescnt: i64 = 0;
    let cycle: bool;
    let mut logit: bool = false;

    /* open and lock sequence */
    init_sequence(relid, &mut elm, &mut seqrel);

    if check_permissions &&
        pg_class_aclcheck((*elm).relid, GetUserId(),
                          ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK {
        ereport!(ERROR, errmsg!("permission denied for sequence {}",
            std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /* read-only transactions may only modify temp sequences */
    if !(*seqrel).rd_islocaltemp {
        PreventCommandIfReadOnly(c"nextval()".as_ptr());
    }

    /*
     * Forbid this during parallel operation because, to make it work, the
     * cooperating backends would need to share the backend-local cached
     * sequence information.  Currently, we don't support that.
     */
    PreventCommandIfParallelMode(c"nextval()".as_ptr());

    if (*elm).last != (*elm).cached	/* some numbers were cached */
    {
        Assert!((*elm).last_valid);
        Assert!((*elm).increment != 0);
        (*elm).last += (*elm).increment;
        sequence_close(seqrel, NoLock);
        last_used_seq = elm;
        return (*elm).last;
    }

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(pgstuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", relid);
    }
    pgsform = GETSTRUCT(pgstuple) as Form_pg_sequence;
    incby = (*pgsform).seqincrement;
    maxv = (*pgsform).seqmax;
    minv = (*pgsform).seqmin;
    cache = (*pgsform).seqcache;
    cycle = (*pgsform).seqcycle;
    ReleaseSysCache(pgstuple);

    /* lock page buffer and read tuple */
    seq = read_seq_tuple(seqrel, &mut buf, &mut seqdatatuple);
    page = BufferGetPage(buf);

    last = (*seq).last_value;
    next = (*seq).last_value;
    result = (*seq).last_value;
    fetch = cache;
    log = (*seq).log_cnt;

    if !(*seq).is_called {
        rescnt += 1;				/* return last_value if not is_called */
        fetch -= 1;
    }

    /*
     * Decide whether we should emit a WAL log record.  If so, force up the
     * fetch count to grab SEQ_LOG_VALS more values than we actually need to
     * cache.  (These will then be usable without logging.)
     *
     * If this is the first nextval after a checkpoint, we must force a new
     * WAL record to be written anyway, else replay starting from the
     * checkpoint would fail to advance the sequence past the logged values.
     * In this case we may as well fetch extra values.
     */
    if log < fetch || !(*seq).is_called {
        /* forced log to satisfy local demand for values */
        fetch = fetch + SEQ_LOG_VALS;
        log = fetch;
        logit = true;
    } else {
        let redoptr: XLogRecPtr = GetRedoRecPtr();

        if PageGetLSN(page) <= redoptr {
            /* last update of seq was before checkpoint */
            fetch = fetch + SEQ_LOG_VALS;
            log = fetch;
            logit = true;
        }
    }

    while fetch != 0				/* try to fetch cache [+ log ] numbers */
    {
        /*
         * Check MAXVALUE for ascending sequences and MINVALUE for descending
         * sequences
         */
        if incby > 0 {
            /* ascending sequence */
            if (maxv >= 0 && next > maxv - incby) ||
                (maxv < 0 && next + incby > maxv) {
                if rescnt > 0 {
                    break;		/* stop fetching */
                }
                if !cycle {
                    ereport!(ERROR, errmsg!("nextval: reached maximum value of sequence \"{}\" ({})",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy(),
                        maxv));
                    /* C also: errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED) */
                }
                next = minv;
            } else {
                next += incby;
            }
        } else {
            /* descending sequence */
            if (minv < 0 && next < minv - incby) ||
                (minv >= 0 && next + incby < minv) {
                if rescnt > 0 {
                    break;		/* stop fetching */
                }
                if !cycle {
                    ereport!(ERROR, errmsg!("nextval: reached minimum value of sequence \"{}\" ({})",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy(),
                        minv));
                    /* C also: errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED) */
                }
                next = maxv;
            } else {
                next += incby;
            }
        }
        fetch -= 1;
        if rescnt < cache {
            log -= 1;
            rescnt += 1;
            last = next;
            if rescnt == 1 {	/* if it's first result - */
                result = next;	/* it's what to return */
            }
        }
    }

    log -= fetch;				/* adjust for any unfetched numbers */
    Assert!(log >= 0);

    /* save info in local cache */
    (*elm).increment = incby;
    (*elm).last = result;			/* last returned number */
    (*elm).cached = last;			/* last fetched number */
    (*elm).last_valid = true;

    last_used_seq = elm;

    /*
     * If something needs to be WAL logged, acquire an xid, so this
     * transaction's commit will trigger a WAL flush and wait for syncrep.
     * It's sufficient to ensure the toplevel transaction has an xid, no need
     * to assign xids subxacts, that'll already trigger an appropriate wait.
     * (Have to do that here, so we're outside the critical section)
     */
    if logit && RelationNeedsWAL(seqrel) {
        GetTopTransactionId();
    }

    /* ready to change the on-disk (or really, in-buffer) tuple */
    START_CRIT_SECTION();

    /*
     * We must mark the buffer dirty before doing XLogInsert(); see notes in
     * SyncOneBuffer().  However, we don't apply the desired changes just yet.
     * This looks like a violation of the buffer update protocol, but it is in
     * fact safe because we hold exclusive lock on the buffer.  Any other
     * process, including a checkpoint, that tries to examine the buffer
     * contents will block until we release the lock, and then will see the
     * final state that we install below.
     */
    MarkBufferDirty(buf);

    /* XLOG stuff */
    if logit && RelationNeedsWAL(seqrel) {
        let mut xlrec: xl_seq_rec = core::mem::zeroed();
        let recptr: XLogRecPtr;

        /*
         * We don't log the current state of the tuple, but rather the state
         * as it would appear after "log" more fetches.  This lets us skip
         * that many future WAL records, at the cost that we lose those
         * sequence values if we crash.
         */
        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        /* set values that will be saved in xlog */
        (*seq).last_value = next;
        (*seq).is_called = true;
        (*seq).log_cnt = 0;

        xlrec.locator = (*seqrel).rd_locator;

        XLogRegisterData(&mut xlrec as *mut _ as *mut c_void, core::mem::size_of::<xl_seq_rec>() as c_int);
        XLogRegisterData(seqdatatuple.t_data as *mut c_void, seqdatatuple.t_len as c_int);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

        PageSetLSN(page, recptr);
    }

    /* Now update sequence tuple to the intended final state */
    (*seq).last_value = last;		/* last fetched number */
    (*seq).is_called = true;
    (*seq).log_cnt = log;			/* how much is logged */

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);

    sequence_close(seqrel, NoLock);

    return result;
}

pub unsafe fn currval_oid(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);
    let result: i64;
    let mut elm: SeqTable = core::ptr::null_mut();
    let mut seqrel: Relation = core::ptr::null_mut();

    /* open and lock sequence */
    init_sequence(relid, &mut elm, &mut seqrel);

    if pg_class_aclcheck((*elm).relid, GetUserId(),
                         ACL_SELECT | ACL_USAGE) != ACLCHECK_OK {
        ereport!(ERROR, errmsg!("permission denied for sequence {}",
            std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    if !(*elm).last_valid {
        ereport!(ERROR, errmsg!("currval of sequence \"{}\" is not yet defined in this session",
            std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
    }

    result = (*elm).last;

    sequence_close(seqrel, NoLock);

    crate::PG_RETURN_INT64!(result);
}

pub unsafe fn lastval(fcinfo: FunctionCallInfo) -> Datum {
    let seqrel: Relation;
    let result: i64;

    if last_used_seq.is_null() {
        ereport!(ERROR, errmsg!("lastval is not yet defined in this session"));
        /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
    }

    /* Someone may have dropped the sequence since the last nextval() */
    if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum((*last_used_seq).relid)) {
        ereport!(ERROR, errmsg!("lastval is not yet defined in this session"));
        /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
    }

    seqrel = lock_and_open_sequence(last_used_seq);

    /* nextval() must have already been called for this sequence */
    Assert!((*last_used_seq).last_valid);

    if pg_class_aclcheck((*last_used_seq).relid, GetUserId(),
                         ACL_SELECT | ACL_USAGE) != ACLCHECK_OK {
        ereport!(ERROR, errmsg!("permission denied for sequence {}",
            std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    result = (*last_used_seq).last;
    sequence_close(seqrel, NoLock);

    crate::PG_RETURN_INT64!(result);
}

/*
 * Main internal procedure that handles 2 & 3 arg forms of SETVAL.
 *
 * Note that the 3 arg version (which sets the is_called flag) is
 * only for use in pg_dump, and setting the is_called flag may not
 * work if multiple users are attached to the database and referencing
 * the sequence (unlikely if pg_dump is restoring it).
 *
 * It is necessary to have the 3 arg version so that pg_dump can
 * restore the state of a sequence exactly during data-only restores -
 * it is the only way to clear the is_called flag in an existing
 * sequence.
 */
unsafe fn do_setval(relid: Oid, next: i64, iscalled: bool) {
    let mut elm: SeqTable = core::ptr::null_mut();
    let mut seqrel: Relation = core::ptr::null_mut();
    let mut buf: Buffer = 0;
    let mut seqdatatuple: HeapTupleData = core::mem::zeroed();
    let seq: Form_pg_sequence_data;
    let pgstuple: HeapTuple;
    let pgsform: Form_pg_sequence;
    let maxv: i64;
    let minv: i64;

    /* open and lock sequence */
    init_sequence(relid, &mut elm, &mut seqrel);

    if pg_class_aclcheck((*elm).relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK {
        ereport!(ERROR, errmsg!("permission denied for sequence {}",
            std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(pgstuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", relid);
    }
    pgsform = GETSTRUCT(pgstuple) as Form_pg_sequence;
    maxv = (*pgsform).seqmax;
    minv = (*pgsform).seqmin;
    ReleaseSysCache(pgstuple);

    /* read-only transactions may only modify temp sequences */
    if !(*seqrel).rd_islocaltemp {
        PreventCommandIfReadOnly(c"setval()".as_ptr());
    }

    /*
     * Forbid this during parallel operation because, to make it work, the
     * cooperating backends would need to share the backend-local cached
     * sequence information.  Currently, we don't support that.
     */
    PreventCommandIfParallelMode(c"setval()".as_ptr());

    /* lock page buffer and read tuple */
    seq = read_seq_tuple(seqrel, &mut buf, &mut seqdatatuple);

    if (next < minv) || (next > maxv) {
        ereport!(ERROR, errmsg!("setval: value {} is out of bounds for sequence \"{}\" ({}..{})",
            next,
            std::ffi::CStr::from_ptr(RelationGetRelationName(seqrel)).to_string_lossy(),
            minv, maxv));
        /* C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE) */
    }

    /* Set the currval() state only if iscalled = true */
    if iscalled {
        (*elm).last = next;		/* last returned number */
        (*elm).last_valid = true;
    }

    /* In any case, forget any future cached numbers */
    (*elm).cached = (*elm).last;

    /* check the comment above nextval_internal()'s equivalent call. */
    if RelationNeedsWAL(seqrel) {
        GetTopTransactionId();
    }

    /* ready to change the on-disk (or really, in-buffer) tuple */
    START_CRIT_SECTION();

    (*seq).last_value = next;		/* last fetched number */
    (*seq).is_called = iscalled;
    (*seq).log_cnt = 0;

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if RelationNeedsWAL(seqrel) {
        let mut xlrec: xl_seq_rec = core::mem::zeroed();
        let recptr: XLogRecPtr;
        let page: Page = BufferGetPage(buf);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        xlrec.locator = (*seqrel).rd_locator;
        XLogRegisterData(&mut xlrec as *mut _ as *mut c_void, core::mem::size_of::<xl_seq_rec>() as c_int);
        XLogRegisterData(seqdatatuple.t_data as *mut c_void, seqdatatuple.t_len as c_int);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);

    sequence_close(seqrel, NoLock);
}

/*
 * Implement the 2 arg setval procedure.
 * See do_setval for discussion.
 */
pub unsafe fn setval_oid(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);
    let next: i64 = crate::PG_GETARG_INT64!(fcinfo, 1);

    do_setval(relid, next, true);

    crate::PG_RETURN_INT64!(next);
}

/*
 * Implement the 3 arg setval procedure.
 * See do_setval for discussion.
 */
pub unsafe fn setval3_oid(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);
    let next: i64 = crate::PG_GETARG_INT64!(fcinfo, 1);
    let iscalled: bool = crate::PG_GETARG_BOOL!(fcinfo, 2);

    do_setval(relid, next, iscalled);

    crate::PG_RETURN_INT64!(next);
}


/*
 * Open the sequence and acquire lock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire a lock.  We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
unsafe fn lock_and_open_sequence(seq: SeqTable) -> Relation {
    let thislxid: LocalTransactionId = MyProc_vxid_lxid();

    /* Get the lock if not already held in this xact */
    if (*seq).lxid != thislxid {
        let currentOwner: ResourceOwner;

        currentOwner = CurrentResourceOwner_get();
        CurrentResourceOwner_set(TopTransactionResourceOwner_get());

        LockRelationOid((*seq).relid, RowExclusiveLock);

        CurrentResourceOwner_set(currentOwner);

        /* Flag that we have a lock in the current xact */
        (*seq).lxid = thislxid;
    }

    /* We now know we have the lock, and can safely open the rel */
    return sequence_open((*seq).relid, NoLock);
}

/*
 * Creates the hash table for storing sequence data
 */
unsafe fn create_seq_hashtable() {
    let mut ctl: HASHCTL = core::mem::zeroed();

    ctl.keysize = core::mem::size_of::<Oid>();
    ctl.entrysize = core::mem::size_of::<SeqTableData>();

    seqhashtab = hash_create(c"Sequence values".as_ptr(), 16, &mut ctl,
                             HASH_ELEM | HASH_BLOBS);
}

/*
 * Given a relation OID, open and lock the sequence.  p_elm and p_rel are
 * output parameters.
 */
unsafe fn init_sequence(relid: Oid, p_elm: *mut SeqTable, p_rel: *mut Relation) {
    let elm: SeqTable;
    let seqrel: Relation;
    let mut found: bool = false;

    /* Find or create a hash table entry for this sequence */
    if seqhashtab.is_null() {
        create_seq_hashtable();
    }

    elm = hash_search(seqhashtab, &relid as *const _ as *const c_void, HASH_ENTER, &mut found) as SeqTable;

    /*
     * Initialize the new hash table entry if it did not exist already.
     *
     * NOTE: seqhashtab entries are stored for the life of a backend (unless
     * explicitly discarded with DISCARD). If the sequence itself is deleted
     * then the entry becomes wasted memory, but it's small enough that this
     * should not matter.
     */
    if !found {
        /* relid is the hash key; set it explicitly (don't rely on keycopy) */
        (*elm).relid = relid;
        (*elm).filenumber = InvalidRelFileNumber;
        (*elm).lxid = InvalidLocalTransactionId;
        (*elm).last_valid = false;
        (*elm).last = 0;
        (*elm).cached = 0;
    }

    /*
     * Open the sequence relation.
     */
    seqrel = lock_and_open_sequence(elm);

    /*
     * If the sequence has been transactionally replaced since we last saw it,
     * discard any cached-but-unissued values.  We do not touch the currval()
     * state, however.
     */
    if (*(*seqrel).rd_rel).relfilenode != (*elm).filenumber {
        (*elm).filenumber = (*(*seqrel).rd_rel).relfilenode;
        (*elm).cached = (*elm).last;
    }

    /* Return results */
    *p_elm = elm;
    *p_rel = seqrel;
}


/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqdatatuple receives the reference to the sequence tuple proper
 *		(this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
unsafe fn read_seq_tuple(rel: Relation, buf: *mut Buffer, seqdatatuple: *mut HeapTupleData) -> Form_pg_sequence_data {
    let page: Page;
    let lp: ItemId;
    let sm: *mut sequence_magic;
    let seq: Form_pg_sequence_data;

    *buf = ReadBuffer(rel, 0);
    LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(*buf);
    sm = PageGetSpecialPointer(page) as *mut sequence_magic;

    if (*sm).magic != SEQ_MAGIC {
        elog!(ERROR, "bad magic number in sequence \"{}\": {:08X}",
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(), (*sm).magic);
    }

    lp = PageGetItemId(page, FirstOffsetNumber);
    Assert!(ItemIdIsNormal(lp));

    /* Note we currently only bother to set these two fields of *seqdatatuple */
    (*seqdatatuple).t_data = PageGetItem(page, lp) as HeapTupleHeader;
    (*seqdatatuple).t_len = ItemIdGetLength(lp);

    /*
     * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
     * a sequence, which would leave a non-frozen XID in the sequence tuple's
     * xmax, which eventually leads to clog access failures or worse. If we
     * see this has happened, clean up after it.  We treat this like a hint
     * bit update, ie, don't bother to WAL-log it, since we can certainly do
     * this again if the update gets lost.
     */
    Assert!(((*(*seqdatatuple).t_data).t_infomask & HEAP_XMAX_IS_MULTI) == 0);
    if HeapTupleHeaderGetRawXmax((*seqdatatuple).t_data) != InvalidTransactionId {
        HeapTupleHeaderSetXmax((*seqdatatuple).t_data, InvalidTransactionId);
        (*(*seqdatatuple).t_data).t_infomask &= !HEAP_XMAX_COMMITTED;
        (*(*seqdatatuple).t_data).t_infomask |= HEAP_XMAX_INVALID;
        MarkBufferDirtyHint(*buf, true);
    }

    seq = GETSTRUCT(seqdatatuple) as Form_pg_sequence_data;

    return seq;
}

/*
 * init_params: process the options list of CREATE or ALTER SEQUENCE, and
 * store the values into appropriate fields of seqform, for changes that go
 * into the pg_sequence catalog, and fields of seqdataform for changes to the
 * sequence relation itself.  Set *need_seq_rewrite to true if we changed any
 * parameters that require rewriting the sequence's relation (interesting for
 * ALTER SEQUENCE).  Also set *owned_by to any OWNED BY option, or to NIL if
 * there is none.
 *
 * If isInit is true, fill any unspecified options with default values;
 * otherwise, do not change existing options that aren't explicitly overridden.
 *
 * Note: we force a sequence rewrite whenever we change parameters that affect
 * generation of future sequence values, even if the seqdataform per se is not
 * changed.  This allows ALTER SEQUENCE to behave transactionally.  Currently,
 * the only option that doesn't cause that is OWNED BY.  It's *necessary* for
 * ALTER SEQUENCE OWNED BY to not rewrite the sequence, because that would
 * break pg_upgrade by causing unwanted changes in the sequence's
 * relfilenumber.
 */
unsafe fn init_params(pstate: *mut ParseState, options: *mut List, for_identity: bool,
                      isInit: bool,
                      seqform: Form_pg_sequence,
                      seqdataform: Form_pg_sequence_data,
                      need_seq_rewrite: *mut bool,
                      owned_by: *mut *mut List) {
    let mut as_type: *mut DefElem = core::ptr::null_mut();
    let mut start_value: *mut DefElem = core::ptr::null_mut();
    let mut restart_value: *mut DefElem = core::ptr::null_mut();
    let mut increment_by: *mut DefElem = core::ptr::null_mut();
    let mut max_value: *mut DefElem = core::ptr::null_mut();
    let mut min_value: *mut DefElem = core::ptr::null_mut();
    let mut cache_value: *mut DefElem = core::ptr::null_mut();
    let mut is_cycled: *mut DefElem = core::ptr::null_mut();
    let mut reset_max_value: bool = false;
    let mut reset_min_value: bool = false;

    *need_seq_rewrite = false;
    *owned_by = NIL;

    foreach!(option, options, {
        let defel: *mut DefElem = lfirst(current_cell!(option)) as *mut DefElem;

        if libc_strcmp((*defel).defname, c"as".as_ptr()) == 0 {
            if !as_type.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            as_type = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"increment".as_ptr()) == 0 {
            if !increment_by.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            increment_by = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"start".as_ptr()) == 0 {
            if !start_value.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            start_value = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"restart".as_ptr()) == 0 {
            if !restart_value.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            restart_value = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"maxvalue".as_ptr()) == 0 {
            if !max_value.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            max_value = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"minvalue".as_ptr()) == 0 {
            if !min_value.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            min_value = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"cache".as_ptr()) == 0 {
            if !cache_value.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            cache_value = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"cycle".as_ptr()) == 0 {
            if !is_cycled.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            is_cycled = defel;
            *need_seq_rewrite = true;
        } else if libc_strcmp((*defel).defname, c"owned_by".as_ptr()) == 0 {
            if !(*owned_by).is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            *owned_by = defGetQualifiedName(defel);
        } else if libc_strcmp((*defel).defname, c"sequence_name".as_ptr()) == 0 {
            /*
             * The parser allows this, but it is only for identity columns, in
             * which case it is filtered out in parse_utilcmd.c.  We only get
             * here if someone puts it into a CREATE SEQUENCE, where it'd be
             * redundant.  (The same is true for the equally-nonstandard
             * LOGGED and UNLOGGED options, but for those, the default error
             * below seems sufficient.)
             */
            ereport!(ERROR, errmsg!("invalid sequence option SEQUENCE NAME"));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition(pstate, (*defel).location) */
        } else {
            elog!(ERROR, "option \"{}\" not recognized",
                std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy());
        }
    });

    /*
     * We must reset log_cnt when isInit or when changing any parameters that
     * would affect future nextval allocations.
     */
    if isInit {
        (*seqdataform).log_cnt = 0;
    }

    /* AS type */
    if !as_type.is_null() {
        let newtypid: Oid = typenameTypeId(pstate, defGetTypeName(as_type));

        if newtypid != INT2OID &&
            newtypid != INT4OID &&
            newtypid != INT8OID {
            ereport!(ERROR, errmsg!("{}",
                if for_identity {
                    "identity column type must be smallint, integer, or bigint"
                } else {
                    "sequence type must be smallint, integer, or bigint"
                }));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        if !isInit {
            /*
             * When changing type and the old sequence min/max values were the
             * min/max of the old type, adjust sequence min/max values to
             * min/max of new type.  (Otherwise, the user chose explicit
             * min/max values, which we'll leave alone.)
             */
            if ((*seqform).seqtypid == INT2OID && (*seqform).seqmax == PG_INT16_MAX) ||
                ((*seqform).seqtypid == INT4OID && (*seqform).seqmax == PG_INT32_MAX) ||
                ((*seqform).seqtypid == INT8OID && (*seqform).seqmax == PG_INT64_MAX) {
                reset_max_value = true;
            }
            if ((*seqform).seqtypid == INT2OID && (*seqform).seqmin == PG_INT16_MIN) ||
                ((*seqform).seqtypid == INT4OID && (*seqform).seqmin == PG_INT32_MIN) ||
                ((*seqform).seqtypid == INT8OID && (*seqform).seqmin == PG_INT64_MIN) {
                reset_min_value = true;
            }
        }

        (*seqform).seqtypid = newtypid;
    } else if isInit {
        (*seqform).seqtypid = INT8OID;
    }

    /* INCREMENT BY */
    if !increment_by.is_null() {
        (*seqform).seqincrement = defGetInt64(increment_by);
        if (*seqform).seqincrement == 0 {
            ereport!(ERROR, errmsg!("INCREMENT must not be zero"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
        (*seqdataform).log_cnt = 0;
    } else if isInit {
        (*seqform).seqincrement = 1;
    }

    /* CYCLE */
    if !is_cycled.is_null() {
        (*seqform).seqcycle = boolVal((*is_cycled).arg as *mut Node);
        Assert!(BoolIsValid((*seqform).seqcycle));
        (*seqdataform).log_cnt = 0;
    } else if isInit {
        (*seqform).seqcycle = false;
    }

    /* MAXVALUE (null arg means NO MAXVALUE) */
    if !max_value.is_null() && !(*max_value).arg.is_null() {
        (*seqform).seqmax = defGetInt64(max_value);
        (*seqdataform).log_cnt = 0;
    } else if isInit || !max_value.is_null() || reset_max_value {
        if (*seqform).seqincrement > 0 || reset_max_value {
            /* ascending seq */
            if (*seqform).seqtypid == INT2OID {
                (*seqform).seqmax = PG_INT16_MAX;
            } else if (*seqform).seqtypid == INT4OID {
                (*seqform).seqmax = PG_INT32_MAX;
            } else {
                (*seqform).seqmax = PG_INT64_MAX;
            }
        } else {
            (*seqform).seqmax = -1;	/* descending seq */
        }
        (*seqdataform).log_cnt = 0;
    }

    /* Validate maximum value.  No need to check INT8 as seqmax is an int64 */
    if ((*seqform).seqtypid == INT2OID && ((*seqform).seqmax < PG_INT16_MIN || (*seqform).seqmax > PG_INT16_MAX))
        || ((*seqform).seqtypid == INT4OID && ((*seqform).seqmax < PG_INT32_MIN || (*seqform).seqmax > PG_INT32_MAX)) {
        ereport!(ERROR, errmsg!("MAXVALUE ({}) is out of range for sequence data type {}",
            (*seqform).seqmax,
            std::ffi::CStr::from_ptr(format_type_be((*seqform).seqtypid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /* MINVALUE (null arg means NO MINVALUE) */
    if !min_value.is_null() && !(*min_value).arg.is_null() {
        (*seqform).seqmin = defGetInt64(min_value);
        (*seqdataform).log_cnt = 0;
    } else if isInit || !min_value.is_null() || reset_min_value {
        if (*seqform).seqincrement < 0 || reset_min_value {
            /* descending seq */
            if (*seqform).seqtypid == INT2OID {
                (*seqform).seqmin = PG_INT16_MIN;
            } else if (*seqform).seqtypid == INT4OID {
                (*seqform).seqmin = PG_INT32_MIN;
            } else {
                (*seqform).seqmin = PG_INT64_MIN;
            }
        } else {
            (*seqform).seqmin = 1;	/* ascending seq */
        }
        (*seqdataform).log_cnt = 0;
    }

    /* Validate minimum value.  No need to check INT8 as seqmin is an int64 */
    if ((*seqform).seqtypid == INT2OID && ((*seqform).seqmin < PG_INT16_MIN || (*seqform).seqmin > PG_INT16_MAX))
        || ((*seqform).seqtypid == INT4OID && ((*seqform).seqmin < PG_INT32_MIN || (*seqform).seqmin > PG_INT32_MAX)) {
        ereport!(ERROR, errmsg!("MINVALUE ({}) is out of range for sequence data type {}",
            (*seqform).seqmin,
            std::ffi::CStr::from_ptr(format_type_be((*seqform).seqtypid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /* crosscheck min/max */
    if (*seqform).seqmin >= (*seqform).seqmax {
        ereport!(ERROR, errmsg!("MINVALUE ({}) must be less than MAXVALUE ({})",
            (*seqform).seqmin,
            (*seqform).seqmax));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /* START WITH */
    if !start_value.is_null() {
        (*seqform).seqstart = defGetInt64(start_value);
    } else if isInit {
        if (*seqform).seqincrement > 0 {
            (*seqform).seqstart = (*seqform).seqmin;	/* ascending seq */
        } else {
            (*seqform).seqstart = (*seqform).seqmax;	/* descending seq */
        }
    }

    /* crosscheck START */
    if (*seqform).seqstart < (*seqform).seqmin {
        ereport!(ERROR, errmsg!("START value ({}) cannot be less than MINVALUE ({})",
            (*seqform).seqstart,
            (*seqform).seqmin));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }
    if (*seqform).seqstart > (*seqform).seqmax {
        ereport!(ERROR, errmsg!("START value ({}) cannot be greater than MAXVALUE ({})",
            (*seqform).seqstart,
            (*seqform).seqmax));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /* RESTART [WITH] */
    if !restart_value.is_null() {
        if !(*restart_value).arg.is_null() {
            (*seqdataform).last_value = defGetInt64(restart_value);
        } else {
            (*seqdataform).last_value = (*seqform).seqstart;
        }
        (*seqdataform).is_called = false;
        (*seqdataform).log_cnt = 0;
    } else if isInit {
        (*seqdataform).last_value = (*seqform).seqstart;
        (*seqdataform).is_called = false;
    }

    /* crosscheck RESTART (or current value, if changing MIN/MAX) */
    if (*seqdataform).last_value < (*seqform).seqmin {
        ereport!(ERROR, errmsg!("RESTART value ({}) cannot be less than MINVALUE ({})",
            (*seqdataform).last_value,
            (*seqform).seqmin));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }
    if (*seqdataform).last_value > (*seqform).seqmax {
        ereport!(ERROR, errmsg!("RESTART value ({}) cannot be greater than MAXVALUE ({})",
            (*seqdataform).last_value,
            (*seqform).seqmax));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /* CACHE */
    if !cache_value.is_null() {
        (*seqform).seqcache = defGetInt64(cache_value);
        if (*seqform).seqcache <= 0 {
            ereport!(ERROR, errmsg!("CACHE ({}) must be greater than zero",
                (*seqform).seqcache));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
        (*seqdataform).log_cnt = 0;
    } else if isInit {
        (*seqform).seqcache = 1;
    }
}

/*
 * Process an OWNED BY option for CREATE/ALTER SEQUENCE
 *
 * Ownership permissions on the sequence are already checked,
 * but if we are establishing a new owned-by dependency, we must
 * enforce that the referenced table has the same owner and namespace
 * as the sequence.
 */
unsafe fn process_owned_by(seqrel: Relation, owned_by: *mut List, for_identity: bool) {
    let deptype: DependencyType;
    let nnames: c_int;
    let mut tablerel: Relation;
    let mut attnum: AttrNumber;

    deptype = if for_identity { DEPENDENCY_INTERNAL } else { DEPENDENCY_AUTO };

    nnames = list_length(owned_by);
    Assert!(nnames > 0);
    if nnames == 1 {
        /* Must be OWNED BY NONE */
        if libc_strcmp(strVal(linitial(owned_by)), c"none".as_ptr()) != 0 {
            ereport!(ERROR, errmsg!("invalid OWNED BY option"));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR),
             * errhint("Specify OWNED BY table.column or OWNED BY NONE.") */
        }
        tablerel = core::ptr::null_mut();
        attnum = 0;
    } else {
        let relname: *mut List;
        let attrname: *mut c_char;
        let rel: *mut RangeVar;

        /* Separate relname and attr name */
        relname = list_copy_head(owned_by, nnames - 1);
        attrname = strVal(llast(owned_by));

        /* Open and lock rel to ensure it won't go away meanwhile */
        rel = makeRangeVarFromNameList(relname);
        tablerel = relation_openrv(rel, AccessShareLock);

        /* Must be a regular or foreign table */
        if !((*(*tablerel).rd_rel).relkind == RELKIND_RELATION ||
              (*(*tablerel).rd_rel).relkind == RELKIND_FOREIGN_TABLE ||
              (*(*tablerel).rd_rel).relkind == RELKIND_VIEW ||
              (*(*tablerel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE) {
            ereport!(ERROR, errmsg!("sequence cannot be owned by relation \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(tablerel)).to_string_lossy()));
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
             * errdetail_relkind_not_supported(tablerel->rd_rel->relkind) */
        }

        /* We insist on same owner and schema */
        if (*(*seqrel).rd_rel).relowner != (*(*tablerel).rd_rel).relowner {
            ereport!(ERROR, errmsg!("sequence must have same owner as table it is linked to"));
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        }
        if RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel) {
            ereport!(ERROR, errmsg!("sequence must be in same schema as table it is linked to"));
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        }

        /* Now, fetch the attribute number from the system cache */
        attnum = get_attnum(RelationGetRelid(tablerel), attrname);
        if attnum == InvalidAttrNumber {
            ereport!(ERROR, errmsg!("column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(attrname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(tablerel)).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
        }
    }

    /*
     * Catch user explicitly running OWNED BY on identity sequence.
     */
    if deptype == DEPENDENCY_AUTO {
        let mut tableId: Oid = 0;
        let mut colId: i32 = 0;

        if sequenceIsOwned(RelationGetRelid(seqrel), DEPENDENCY_INTERNAL as c_char, &mut tableId, &mut colId) {
            ereport!(ERROR, errmsg!("cannot change ownership of identity sequence"));
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             * errdetail("Sequence \"%s\" is linked to table \"%s\".",
             *           RelationGetRelationName(seqrel), get_rel_name(tableId)) */
        }
    }

    /*
     * OK, we are ready to update pg_depend.  First remove any existing
     * dependencies for the sequence, then optionally add a new one.
     */
    deleteDependencyRecordsForClass(RelationRelationId, RelationGetRelid(seqrel),
                                    RelationRelationId, deptype as c_char);

    if !tablerel.is_null() {
        let mut refobject: ObjectAddress = core::mem::zeroed();
        let mut depobject: ObjectAddress = core::mem::zeroed();

        refobject.classId = RelationRelationId;
        refobject.objectId = RelationGetRelid(tablerel);
        refobject.objectSubId = attnum as i32;
        depobject.classId = RelationRelationId;
        depobject.objectId = RelationGetRelid(seqrel);
        depobject.objectSubId = 0;
        recordDependencyOn(&depobject, &refobject, deptype);
    }

    /* Done, but hold lock until commit */
    if !tablerel.is_null() {
        relation_close(tablerel, NoLock);
    }
}


/*
 * Return sequence parameters in a list of the form created by the parser.
 */
pub unsafe fn sequence_options(relid: Oid) -> *mut List {
    let pgstuple: HeapTuple;
    let pgsform: Form_pg_sequence;
    let mut options: *mut List = NIL;

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(pgstuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", relid);
    }
    pgsform = GETSTRUCT(pgstuple) as Form_pg_sequence;

    /* Use makeFloat() for 64-bit integers, like gram.y does. */
    options = lappend(options,
                      makeDefElem(c"cache".as_ptr() as *mut c_char, makeFloat(psprintf_int64((*pgsform).seqcache)), -1) as *mut c_void);
    options = lappend(options,
                      makeDefElem(c"cycle".as_ptr() as *mut c_char, makeBoolean((*pgsform).seqcycle), -1) as *mut c_void);
    options = lappend(options,
                      makeDefElem(c"increment".as_ptr() as *mut c_char, makeFloat(psprintf_int64((*pgsform).seqincrement)), -1) as *mut c_void);
    options = lappend(options,
                      makeDefElem(c"maxvalue".as_ptr() as *mut c_char, makeFloat(psprintf_int64((*pgsform).seqmax)), -1) as *mut c_void);
    options = lappend(options,
                      makeDefElem(c"minvalue".as_ptr() as *mut c_char, makeFloat(psprintf_int64((*pgsform).seqmin)), -1) as *mut c_void);
    options = lappend(options,
                      makeDefElem(c"start".as_ptr() as *mut c_char, makeFloat(psprintf_int64((*pgsform).seqstart)), -1) as *mut c_void);

    ReleaseSysCache(pgstuple);

    return options;
}

/*
 * Return sequence parameters (formerly for use by information schema)
 */
pub unsafe extern "C" fn pg_sequence_parameters(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let mut values: [Datum; 7] = [0; 7];
    let mut isnull: [bool; 7] = [false; 7];
    let pgstuple: HeapTuple;
    let pgsform: Form_pg_sequence;

    if pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK {
        ereport!(ERROR, errmsg!("permission denied for sequence {}",
            std::ffi::CStr::from_ptr(get_rel_name(relid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    core::ptr::write_bytes(isnull.as_mut_ptr(), 0, isnull.len());

    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(pgstuple) {
        elog!(ERROR, "cache lookup failed for sequence {}", relid);
    }
    pgsform = GETSTRUCT(pgstuple) as Form_pg_sequence;

    values[0] = Int64GetDatum((*pgsform).seqstart);
    values[1] = Int64GetDatum((*pgsform).seqmin);
    values[2] = Int64GetDatum((*pgsform).seqmax);
    values[3] = Int64GetDatum((*pgsform).seqincrement);
    values[4] = BoolGetDatum((*pgsform).seqcycle);
    values[5] = Int64GetDatum((*pgsform).seqcache);
    values[6] = ObjectIdGetDatum((*pgsform).seqtypid);

    ReleaseSysCache(pgstuple);

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), isnull.as_mut_ptr()));
}


/*
 * Return the sequence tuple.
 *
 * This is primarily intended for use by pg_dump to gather sequence data
 * without needing to individually query each sequence relation.
 */
pub unsafe extern "C" fn pg_get_sequence_data(fcinfo: FunctionCallInfo) -> Datum {
    const PG_GET_SEQUENCE_DATA_COLS: c_int = 2;
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);
    let seqrel: Relation;
    let mut values: [Datum; PG_GET_SEQUENCE_DATA_COLS as usize] = [0; PG_GET_SEQUENCE_DATA_COLS as usize];
    let mut isnull: [bool; PG_GET_SEQUENCE_DATA_COLS as usize] = [false; PG_GET_SEQUENCE_DATA_COLS as usize];
    let mut resultTupleDesc: TupleDesc;
    let resultHeapTuple: HeapTuple;
    let result: Datum;

    resultTupleDesc = CreateTemplateTupleDesc(PG_GET_SEQUENCE_DATA_COLS);
    TupleDescInitEntry(resultTupleDesc, 1 as AttrNumber, c"last_value".as_ptr(),
                       INT8OID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, 2 as AttrNumber, c"is_called".as_ptr(),
                       BOOLOID, -1, 0);
    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    seqrel = try_relation_open(relid, AccessShareLock);

    /*
     * Return all NULLs for missing sequences, sequences for which we lack
     * privileges, other sessions' temporary sequences, and unlogged sequences
     * on standbys.
     */
    if !seqrel.is_null() && (*(*seqrel).rd_rel).relkind == RELKIND_SEQUENCE &&
        pg_class_aclcheck(relid, GetUserId(), ACL_SELECT) == ACLCHECK_OK &&
        !RELATION_IS_OTHER_TEMP(seqrel) &&
        (RelationIsPermanent(seqrel) || !RecoveryInProgress()) {
        let mut buf: Buffer = 0;
        let mut seqtuple: HeapTupleData = core::mem::zeroed();
        let seq: Form_pg_sequence_data;

        seq = read_seq_tuple(seqrel, &mut buf, &mut seqtuple);

        values[0] = Int64GetDatum((*seq).last_value);
        values[1] = BoolGetDatum((*seq).is_called);

        UnlockReleaseBuffer(buf);
    } else {
        core::ptr::write_bytes(isnull.as_mut_ptr() as *mut u8, 1, core::mem::size_of_val(&isnull));
    }

    if !seqrel.is_null() {
        relation_close(seqrel, AccessShareLock);
    }

    resultHeapTuple = heap_form_tuple(resultTupleDesc, values.as_mut_ptr(), isnull.as_mut_ptr());
    result = HeapTupleGetDatum(resultHeapTuple);
    crate::PG_RETURN_DATUM!(result);
}


/*
 * Return the last value from the sequence
 *
 * Note: This has a completely different meaning than lastval().
 */
pub unsafe extern "C" fn pg_sequence_last_value(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = crate::PG_GETARG_OID!(fcinfo, 0);
    let mut elm: SeqTable = core::ptr::null_mut();
    let mut seqrel: Relation = core::ptr::null_mut();
    let mut is_called: bool = false;
    let mut result: i64 = 0;

    /* open and lock sequence */
    init_sequence(relid, &mut elm, &mut seqrel);

    /*
     * We return NULL for other sessions' temporary sequences.  The
     * pg_sequences system view already filters those out, but this offers a
     * defense against ERRORs in case someone invokes this function directly.
     *
     * Also, for the benefit of the pg_sequences view, we return NULL for
     * unlogged sequences on standbys and for sequences for which the current
     * user lacks privileges instead of throwing an error.
     */
    if pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_USAGE) == ACLCHECK_OK &&
        !RELATION_IS_OTHER_TEMP(seqrel) &&
        (RelationIsPermanent(seqrel) || !RecoveryInProgress()) {
        let mut buf: Buffer = 0;
        let mut seqtuple: HeapTupleData = core::mem::zeroed();
        let seq: Form_pg_sequence_data;

        seq = read_seq_tuple(seqrel, &mut buf, &mut seqtuple);

        is_called = (*seq).is_called;
        result = (*seq).last_value;

        UnlockReleaseBuffer(buf);
    }
    sequence_close(seqrel, NoLock);

    if is_called {
        crate::PG_RETURN_INT64!(result);
    } else {
        crate::PG_RETURN_NULL!(fcinfo);
    }
}


pub unsafe fn seq_redo(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = record_EndRecPtr(record);
    let info: u8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let buffer: Buffer;
    let page: Page;
    let localpage: Page;
    let item: *mut c_char;
    let itemsz: Size;
    let xlrec: *mut xl_seq_rec = XLogRecGetData(record) as *mut xl_seq_rec;
    let sm: *mut sequence_magic;

    if info != XLOG_SEQ_LOG {
        elog!(PANIC, "seq_redo: unknown op code {}", info);
    }

    buffer = XLogInitBufferForRedo(record, 0);
    page = BufferGetPage(buffer);

    /*
     * We always reinit the page.  However, since this WAL record type is also
     * used for updating sequences, it's possible that a hot-standby backend
     * is examining the page concurrently; so we mustn't transiently trash the
     * buffer.  The solution is to build the correct new page contents in
     * local workspace and then memcpy into the buffer.  Then only bytes that
     * are supposed to change will change, even transiently. We must palloc
     * the local page for alignment reasons.
     */
    localpage = palloc(BufferGetPageSize(buffer)) as Page;

    PageInit(localpage, BufferGetPageSize(buffer), core::mem::size_of::<sequence_magic>());
    sm = PageGetSpecialPointer(localpage) as *mut sequence_magic;
    (*sm).magic = SEQ_MAGIC;

    item = (xlrec as *mut c_char).add(core::mem::size_of::<xl_seq_rec>());
    itemsz = (XLogRecGetDataLen(record) as usize) - core::mem::size_of::<xl_seq_rec>();

    if PageAddItem(localpage, item as Item, itemsz,
                    FirstOffsetNumber, false, false) == InvalidOffsetNumber {
        elog!(PANIC, "seq_redo: failed to add item to page");
    }

    PageSetLSN(localpage, lsn);

    core::ptr::copy_nonoverlapping(localpage, page, BufferGetPageSize(buffer));
    MarkBufferDirty(buffer);
    UnlockReleaseBuffer(buffer);

    pfree(localpage as *mut c_void);
}

/*
 * Flush cached sequence information.
 */
pub unsafe fn ResetSequenceCaches() {
    if !seqhashtab.is_null() {
        hash_destroy(seqhashtab);
        seqhashtab = core::ptr::null_mut();
    }

    last_used_seq = core::ptr::null_mut();
}

/*
 * Mask a Sequence page before performing consistency checks on it.
 */
pub unsafe fn seq_mask(page: *mut c_char, blkno: BlockNumber) {
    mask_page_lsn_and_checksum(page);

    mask_unused_space(page);
}
