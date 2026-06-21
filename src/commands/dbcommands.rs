/*-------------------------------------------------------------------------
 *
 * dbcommands.rs
 *      Database management commands (create/drop database).
 *
 * Note: database creation/destruction commands use exclusive locks on
 * the database objects (as expressed by LockSharedObject()) to avoid
 * stepping on each others' toes.  Formerly we used table-level locks
 * on pg_database, but that's too coarse-grained.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/commands/dbcommands.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};
use core::ffi::CStr;
use std::ptr;

use crate::access::htup_details::{HeapTupleData, HeapTupleHeader};
use crate::access::htup_details::HeapTuple;
use crate::access::table::table::{table_open, table_close};
use crate::c::TransactionId;
use crate::access::transam::InvalidTransactionId;
use crate::catalog::objectaccess::ObjectAddress;
use crate::nodes::pg_list::{List, ListCell, list_nth};
use crate::nodes::parsenodes::{
    AlterDatabaseRefreshCollStmt, AlterDatabaseSetStmt, AlterDatabaseStmt,
    CreatedbStmt, DefElem, DropdbStmt, ObjectType,
};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::commands::dbcommands_xlog::{
    xl_dbase_create_file_copy_rec, xl_dbase_create_wal_log_rec, xl_dbase_drop_rec,
    MinSizeOfDbaseDropRec,
    XLOG_DBASE_CREATE_FILE_COPY, XLOG_DBASE_CREATE_WAL_LOG, XLOG_DBASE_DROP,
};
use crate::{foreach, current_cell};

/* -------------------------------------------------------------------------
 * Local type stubs for unported dependencies
 * ------------------------------------------------------------------------- */

// Relation pointer  TODO(pg-port)
pub use crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// HeapTuple / HeapTupleData already from htup_details

// SysScanDesc / ScanKeyData  TODO(pg-port)
pub use crate::access::relscan::SysScanDescData;
type SysScanDesc = *mut SysScanDescData;
#[repr(C)] pub struct ScanKeyDataStruct { _opaque: [u8; 64] }
type ScanKeyData = ScanKeyDataStruct;

// Form_pg_database
pub use crate::catalog::pg_database::FormData_pg_database;
type Form_pg_database = *mut FormData_pg_database;

// Form_pg_tablespace  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_tablespace { _opaque: [u8; 0] }
type Form_pg_tablespace = *mut FormData_pg_tablespace;

// Form_pg_authid  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_authid { _opaque: [u8; 0] }
type Form_pg_authid = *mut FormData_pg_authid;

// Form_pg_class
pub use crate::catalog::pg_class::FormData_pg_class;
type Form_pg_class = *mut FormData_pg_class;

// TableScanDesc  TODO(pg-port)
#[repr(C)] pub struct TableScanDescData { _opaque: [u8; 0] }
type TableScanDesc = *mut TableScanDescData;

// Buffer / Page / BlockNumber  TODO(pg-port)
type Buffer = i32;
type Page = *mut u8;
type BlockNumber = u32;

// RelFileLocator  TODO(pg-port)
#[repr(C)] #[derive(Clone, Copy)] pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: Oid,
}

// LockRelId  TODO(pg-port)
#[repr(C)] #[derive(Clone, Copy)] pub struct LockRelId {
    pub relId: Oid,
    pub dbId: Oid,
}

// SMgrRelation  TODO(pg-port)
#[repr(C)] pub struct SMgrRelationData { _opaque: [u8; 0] }
type SMgrRelation = *mut SMgrRelationData;

// BufferAccessStrategy  TODO(pg-port)
#[repr(C)] pub struct BufferAccessStrategyData { _opaque: [u8; 0] }
type BufferAccessStrategy = *mut BufferAccessStrategyData;

// Snapshot  TODO(pg-port)
#[repr(C)] pub struct SnapshotData { _opaque: [u8; 0] }
type Snapshot = *mut SnapshotData;

// XLogReaderState  TODO(pg-port)
use crate::access::transam::xlogreader::XLogReaderState;

// MultiXactId  TODO(pg-port)
type MultiXactId = u32;
const InvalidMultiXactId: MultiXactId = 0;

// AclResult  TODO(pg-port)
#[repr(C)] #[derive(PartialEq)] pub enum AclResult { ACLCHECK_OK = 0, ACLCHECK_NOT_OWNER, ACLCHECK_NO_PRIV }
use AclResult::*;

// Acl  TODO(pg-port)
#[repr(C)] pub struct AclType { _opaque: [u8; 0] }
type Acl = AclType;

// Datum  TODO(pg-port)
type Datum = usize;

// DIR / dirent  TODO(pg-port)
#[repr(C)] pub struct DIR { _opaque: [u8; 0] }
#[repr(C)] pub struct dirent { pub d_name: [c_char; 256] }

// ItemPointerData  TODO(pg-port)
pub use crate::storage::itemptr::ItemPointerData;

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const NoLock: LOCKMODE = 0;
const AccessShareLock: LOCKMODE = 1;
const ShareLock: LOCKMODE = 5;
const RowExclusiveLock: LOCKMODE = 3;
const AccessExclusiveLock: LOCKMODE = 8;
const InplaceUpdateTupleLock: LOCKMODE = 4;

// RelFileNumber
type RelFileNumber = Oid;
const InvalidRelFileNumber: RelFileNumber = 0;

/* -------------------------------------------------------------------------
 * Compile-time constant stubs  TODO(pg-port)
 * ------------------------------------------------------------------------- */

extern "C" {
    static DatabaseRelationId: Oid;
    static TableSpaceRelationId: Oid;
    static GLOBALTABLESPACE_OID: Oid;
    static DatabaseOidIndexId: Oid;
    static DatabaseNameIndexId: Oid;
    static RelationRelationId: Oid;
    static InvalidOid: Oid;
    static FirstNormalObjectId: Oid;
    static MyDatabaseId: Oid;
    static IsBinaryUpgrade: bool;
    static allowSystemTableMods: bool;
    static reachedConsistency: bool;
    static allow_in_place_tablespaces: bool;
    static icu_validation_level: c_int;
    static XactLastRecEnd: u64;
}

const DATCONNLIMIT_UNLIMITED: c_int = -1;
const DATCONNLIMIT_INVALID_DB: c_int = -2;
const INVALID_PROC_NUMBER: c_int = -1;
const MAIN_FORKNUM: c_int = 0;
const COLLPROVIDER_LIBC: c_char = b'c' as c_char;
const COLLPROVIDER_ICU: c_char = b'i' as c_char;
const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;
const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
const BAS_BULKREAD: c_int = 1;
const RBM_NORMAL: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;

// XLog constants  TODO(pg-port)
const RM_DBASE_ID: u8 = 6;
const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;
const XLR_INFO_MASK: u8 = 0x0f;

// Checkpoint flags  TODO(pg-port)
const CHECKPOINT_IMMEDIATE: c_int = 0x0001;
const CHECKPOINT_FORCE: c_int = 0x0002;
const CHECKPOINT_WAIT: c_int = 0x0004;
const CHECKPOINT_FLUSH_ALL: c_int = 0x0008;

// Wait event IDs  TODO(pg-port)
const WAIT_EVENT_VERSION_FILE_WRITE: u32 = 0;
const WAIT_EVENT_VERSION_FILE_SYNC: u32 = 1;

// pg_class constants  TODO(pg-port)
const RELKIND_HAS_STORAGE_MASK: u8 = 0;

// Natts_pg_database  TODO(pg-port)
const Natts_pg_database: usize = 22;

// pg_database Anum constants  TODO(pg-port)
const Anum_pg_database_oid: usize = 1;
const Anum_pg_database_datname: usize = 2;
const Anum_pg_database_datdba: usize = 3;
const Anum_pg_database_encoding: usize = 4;
const Anum_pg_database_datlocprovider: usize = 5;
const Anum_pg_database_datistemplate: usize = 6;
const Anum_pg_database_datallowconn: usize = 7;
const Anum_pg_database_dathasloginevt: usize = 8;
const Anum_pg_database_datconnlimit: usize = 9;
const Anum_pg_database_datfrozenxid: usize = 10;
const Anum_pg_database_datminmxid: usize = 11;
const Anum_pg_database_dattablespace: usize = 12;
const Anum_pg_database_datcollate: usize = 13;
const Anum_pg_database_datctype: usize = 14;
const Anum_pg_database_datlocale: usize = 15;
const Anum_pg_database_daticurules: usize = 16;
const Anum_pg_database_datcollversion: usize = 17;
const Anum_pg_database_datacl: usize = 18;

// PROCSIGNAL_BARRIER_SMGRRELEASE  TODO(pg-port)
const PROCSIGNAL_BARRIER_SMGRRELEASE: c_int = 1;

// OBJECT_DATABASE / OBJECT_TABLESPACE  TODO(pg-port)
const OBJECT_DATABASE: c_int = 1;
const OBJECT_TABLESPACE: c_int = 2;

// ACL_CREATE  TODO(pg-port)
const ACL_CREATE: c_int = 0x0004;

// ACLCHECK_OK == 0 already; ACLCHECK_NOT_OWNER / ACLCHECK_NO_PRIV handled above

// BTEqualStrategyNumber  TODO(pg-port)
const BTEqualStrategyNumber: c_int = 3;

// F_NAMEEQ -- nameeq(name, name)
const F_NAMEEQ: Oid = 62;

// FATAL / PANIC  TODO(pg-port)
const FATAL: c_int = 23;
const PANIC: c_int = 24;

// NOTICE  TODO(pg-port)
const NOTICE: c_int = 18;

// WARNING  TODO(pg-port)
const WARNING: c_int = 19;

// DEBUG1  TODO(pg-port)
const DEBUG1: c_int = 15;

// ForwardScanDirection  TODO(pg-port)
const ForwardScanDirection: c_int = 1;

// DATABASEOID syscache id  TODO(pg-port)
const DATABASEOID: c_int = 21;
// AUTHOID syscache id  TODO(pg-port)
const AUTHOID: c_int = 11;

/* -------------------------------------------------------------------------
 * External function stubs  TODO(pg-port)
 * ------------------------------------------------------------------------- */

extern "C" {
    fn GetDatabasePath(dbOid: Oid, spcOid: Oid) -> *mut c_char;
    fn RelationMapCopy(dbid: Oid, tsid: Oid, srcpath: *const c_char, dstpath: *const c_char);
    fn RelationMapOidToFilenumberForDatabase(srcpath: *const c_char, relid: Oid) -> RelFileNumber;
    fn LockRelationId(relid: *mut LockRelId, lockmode: LOCKMODE);
    fn UnlockRelationId(relid: *mut LockRelId, lockmode: LOCKMODE);
    fn smgropen(rlocator: RelFileLocator, backend: c_int) -> SMgrRelation;
    fn smgrnblocks(smgr: SMgrRelation, forknum: c_int) -> BlockNumber;
    fn smgrclose(smgr: SMgrRelation);
    fn GetAccessStrategy(strategytype: c_int) -> BufferAccessStrategy;
    fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot;
    fn UnregisterSnapshot(snapshot: Snapshot);
    fn GetLatestSnapshot() -> Snapshot;
    fn ReadBufferWithoutRelcache(rlocator: RelFileLocator, forknum: c_int, blocknum: BlockNumber,
                                  mode: c_int, strategy: BufferAccessStrategy, permanent: bool) -> Buffer;
    fn LockBuffer(buf: Buffer, mode: c_int);
    fn BufferGetPage(buf: Buffer) -> Page;
    fn PageIsNew(page: Page) -> bool;
    fn PageIsEmpty(page: Page) -> bool;
    fn UnlockReleaseBuffer(buf: Buffer);
    fn BufferGetBlockNumber(buf: Buffer) -> BlockNumber;
    fn PageGetMaxOffsetNumber(page: Page) -> u16;
    fn PageGetItemId(page: Page, offnum: u16) -> *mut u8;
    fn ItemIdIsUsed(itemid: *const u8) -> bool;
    fn ItemIdIsDead(itemid: *const u8) -> bool;
    fn ItemIdIsRedirected(itemid: *const u8) -> bool;
    fn ItemIdIsNormal(itemid: *const u8) -> bool;
    fn ItemPointerSet(pointer: *mut ItemPointerData, blockno: BlockNumber, off: u16);
    fn PageGetItem(page: Page, itemid: *const u8) -> *mut u8;
    fn ItemIdGetLength(itemid: *const u8) -> u32;
    fn HeapTupleSatisfiesVisibility(tuple: *mut HeapTupleData, snapshot: Snapshot, buf: Buffer) -> bool;
    fn lappend(list: *mut List, datum: *mut c_void) -> *mut List;
    fn lappend_oid(list: *mut List, datum: Oid) -> *mut List;
    fn list_free(list: *mut List);
    fn list_free_deep(list: *mut List);
    fn list_length(list: *const List) -> c_int;
    fn palloc(size: usize) -> *mut c_void;
    fn pfree(ptr: *mut c_void);
    fn pstrdup(str_: *const c_char) -> *mut c_char;
    fn CreateAndCopyRelationData(srcrlocator: RelFileLocator, dstrlocator: RelFileLocator, permanent: bool);
    fn MakePGDirectory(path: *const c_char) -> c_int;
    fn OpenTransientFile(path: *const c_char, flags: c_int) -> c_int;
    fn CloseTransientFile(fd: c_int) -> c_int;
    fn pgstat_report_wait_start(event: u32);
    fn pgstat_report_wait_end();
    fn pg_fsync(fd: c_int) -> c_int;
    fn fsync_fname(fname: *const c_char, isdir: bool);
    fn data_sync_elevel(elevel: c_int) -> c_int;
    fn START_CRIT_SECTION();
    fn END_CRIT_SECTION();
    fn XLogBeginInsert();
    fn XLogRegisterData(data: *const c_void, len: usize);
    fn XLogInsert(rmid: u8, info: u8) -> u64;
    fn XLogFlush(lsn: u64);
    fn XLogDropDatabase(dbid: Oid);
    fn RequestCheckpoint(flags: c_int);
    fn table_beginscan_catalog(rel: Relation, nkeys: c_int, key: *const ScanKeyData) -> TableScanDesc;
    fn heap_getnext(scan: TableScanDesc, direction: c_int) -> HeapTuple;
    fn table_endscan(scan: TableScanDesc);
    fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void;
    fn copydir(src: *const c_char, dst: *const c_char, recurse: bool);
    fn directory_is_empty(path: *const c_char) -> bool;
    fn get_role_oid(rolname: *const c_char, missing_ok: bool) -> Oid;
    fn GetUserId() -> Oid;
    fn check_can_set_role(member: Oid, role: Oid);
    fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool;
    fn aclcheck_error(aclerr: AclResult, objtype: c_int, objectname: *const c_char);
    fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: c_int) -> AclResult;
    fn defGetString(def: *mut DefElem) -> *mut c_char;
    fn defGetBoolean(def: *mut DefElem) -> bool;
    fn defGetInt32(def: *mut DefElem) -> c_int;
    fn defGetObjectId(def: *mut DefElem) -> Oid;
    fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState);
    fn parser_errposition(pstate: *mut ParseState, location: c_int) -> c_int;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn pg_encoding_to_char(encoding: c_int) -> *const c_char;
    fn pg_valid_server_encoding(name: *const c_char) -> c_int;
    fn pg_get_encoding_from_locale(ctype: *const c_char, warn: bool) -> c_int;
    fn check_locale(category: c_int, locale: *const c_char, canonname: *mut *mut c_char) -> bool;
    fn is_encoding_supported_by_icu(encoding: c_int) -> bool;
    fn builtin_validate_locale(encoding: c_int, locale: *const c_char) -> *const c_char;
    fn icu_validate_locale(locale: *const c_char);
    fn icu_language_tag(locale: *const c_char, level: c_int) -> *mut c_char;
    fn get_collation_actual_version(provider: c_char, locale: *const c_char) -> *mut c_char;
    fn collprovider_name(provider: c_char) -> *const c_char;
    fn PG_VALID_BE_ENCODING(encoding: c_int) -> bool;
    fn get_tablespace_oid(tablespacename: *const c_char, missing_ok: bool) -> Oid;
    fn CountOtherDBBackends(databaseId: Oid, nbackends: *mut c_int, npreparedxacts: *mut c_int) -> bool;
    fn TerminateOtherDBBackends(databaseId: Oid);
    fn OidIsValid(oid: Oid) -> bool;
    fn GetNewOidWithIndex(rel: Relation, indexId: Oid, oidcolumn: c_int) -> Oid;
    fn RelationGetDescr(rel: Relation) -> *mut c_void;
    fn ObjectIdGetDatum(oid: Oid) -> Datum;
    fn DirectFunctionCall1(func: unsafe extern "C" fn(Datum) -> Datum, arg: Datum) -> Datum;
    fn namein(arg: Datum) -> Datum;
    fn CStringGetDatum(s: *const c_char) -> Datum;
    fn Int32GetDatum(val: c_int) -> Datum;
    fn CharGetDatum(val: c_char) -> Datum;
    fn BoolGetDatum(val: bool) -> Datum;
    fn TransactionIdGetDatum(xid: TransactionId) -> Datum;
    fn CStringGetTextDatum(s: *const c_char) -> Datum;
    fn TextDatumGetCString(datum: Datum) -> *mut c_char;
    fn DatumGetAclP(datum: Datum) -> *mut Acl;
    fn PointerGetDatum(ptr: *const c_void) -> Datum;
    fn DatumGetPointer(datum: Datum) -> *mut c_void;
    fn heap_form_tuple(tupdesc: *mut c_void, values: *mut Datum, isnull: *mut bool) -> HeapTuple;
    fn heap_modify_tuple(tuple: HeapTuple, tupdesc: *mut c_void,
                         values: *mut Datum, isnull: *mut bool, repl: *mut bool) -> HeapTuple;
    fn heap_freetuple(tuple: HeapTuple);
    fn HeapTupleIsValid(tuple: HeapTuple) -> bool;
    fn CatalogTupleInsert(relation: Relation, tup: HeapTuple) -> Oid;
    fn CatalogTupleUpdate(relation: Relation, otid: *mut ItemPointerData, tup: HeapTuple);
    fn CatalogTupleDelete(relation: Relation, tid: *mut ItemPointerData);
    fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid);
    fn copyTemplateDependencies(templateDbId: Oid, newDbId: Oid);
    fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectDropHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int);
    fn LockSharedObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE);
    fn UnlockSharedObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE);
    fn LockSharedObjectForSession(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE);
    fn UnlockSharedObjectForSession(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE);
    fn PG_ENSURE_ERROR_CLEANUP(callback: unsafe extern "C" fn(c_int, Datum), arg: Datum);
    fn PG_END_ENSURE_ERROR_CLEANUP(callback: unsafe extern "C" fn(c_int, Datum), arg: Datum);
    fn ForceSyncCommit();
    fn DropDatabaseBuffers(dbid: Oid);
    fn ForgetDatabaseSyncRequests(dbid: Oid);
    fn FlushDatabaseBuffers(dbid: Oid);
    fn DeleteSharedComments(objectid: Oid, classoid: Oid);
    fn DeleteSharedSecurityLabel(objectid: Oid, classoid: Oid);
    fn DropSetting(databaseid: Oid, roleid: Oid);
    fn dropDatabaseDependencies(databaseId: Oid);
    fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwnerId: Oid);
    fn pgstat_drop_database(dbid: Oid);
    fn ScanKeyInit(entry: *mut ScanKeyData, attributeNumber: c_int,
                   strategy: c_int, procedure: Oid, argument: Datum);
    fn systable_beginscan(rel: Relation, indexId: Oid, indexOK: bool,
                          snapshot: Snapshot, nkeys: c_int, key: *mut ScanKeyData) -> SysScanDesc;
    fn systable_getnext(scan: SysScanDesc) -> HeapTuple;
    fn systable_endscan(scan: SysScanDesc);
    fn systable_inplace_update_begin(rel: Relation, indexId: Oid, indexOK: bool,
                                     snapshot: Snapshot, nkeys: c_int, key: *mut ScanKeyData,
                                     tup: *mut HeapTuple, state: *mut *mut c_void);
    fn systable_inplace_update_finish(state: *mut c_void, tup: HeapTuple);
    fn SearchSysCacheLockedCopy1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn SysCacheGetAttrNotNull(cacheId: c_int, tup: HeapTuple, attributeNumber: c_int) -> Datum;
    fn SysCacheGetAttr(cacheId: c_int, tup: HeapTuple, attributeNumber: c_int, isnull: *mut bool) -> Datum;
    fn heap_getattr(tup: HeapTuple, attnum: c_int, tupdesc: *mut c_void, isnull: *mut bool) -> Datum;
    fn namestrcpy(name: *mut c_void, str_: *const c_char);
    fn NameStr(name: *const c_void) -> *const c_char;
    fn quote_identifier(ident: *const c_char) -> *const c_char;
    fn LockTuple(rel: Relation, tid: *mut ItemPointerData, lockmode: LOCKMODE);
    fn UnlockTuple(rel: Relation, tid: *mut ItemPointerData, lockmode: LOCKMODE);
    fn ObjectAddressSet(address: *mut ObjectAddress, classId: Oid, objectId: Oid);
    fn AlterSetting(databaseid: Oid, roleid: Oid, setstmt: *mut c_void);
    fn shdepLockAndCheckObject(classid: Oid, objectid: Oid);
    fn aclnewowner(old_acl: *mut Acl, oldOwnerId: Oid, newOwnerId: Oid) -> *mut Acl;
    fn cstring_to_text(s: *const c_char) -> *mut c_void;
    fn PreventInTransactionBlock(isTopLevel: bool, stmtType: *const c_char);
    fn superuser() -> bool;
    fn IsUnderPostmaster() -> bool;
    fn ReplicationSlotsCountDBSlots(dbid: Oid, nslots: *mut c_int, nslots_active: *mut c_int) -> bool;
    fn ReplicationSlotsDropDBSlots(dbid: Oid);
    fn CountDBSubscriptions(dbid: Oid) -> c_int;
    fn AllocateDir(dirname: *const c_char) -> *mut DIR;
    fn ReadDir(dir: *mut DIR, dirname: *const c_char) -> *mut dirent;
    fn FreeDir(dir: *mut DIR);
    fn rmtree(path: *const c_char, rmtopdir: bool) -> bool;
    fn get_parent_directory(path: *mut c_char);
    fn pg_mkdir_p(path: *mut c_char, omode: c_int) -> c_int;
    fn RecoveryInProgress() -> bool;
    fn InHotStandby() -> bool;
    fn ResolveRecoveryConflictWithDatabase(dbid: Oid);
    fn EmitProcSignalBarrier(barrier_type: c_int) -> u64;
    fn WaitForProcSignalBarrier(token: u64);
    fn PopActiveSnapshot();
    fn CommitTransactionCommand();
    fn StartTransactionCommand();
    fn CHECK_FOR_INTERRUPTS();
    fn errcode(sqlerrcode: c_int) -> c_int;
    fn errcode_for_file_access() -> c_int;
    fn errmsg(fmt: *const c_char, ...) -> c_int;
    fn errdetail(fmt: *const c_char, ...) -> c_int;
    fn errdetail_plural(fmt_singular: *const c_char, fmt_plural: *const c_char, n: c_int, ...) -> c_int;
    fn errhint(fmt: *const c_char, ...) -> c_int;
    fn ereport_domain(elevel: c_int, ...) -> bool;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn rmdir(path: *const c_char) -> c_int;
    fn lstat(path: *const c_char, buf: *mut libc_stat) -> c_int;
    fn PG_RETURN_TEXT_P(x: *mut c_void) -> Datum;
    fn PG_RETURN_NULL() -> Datum;
    fn PG_GETARG_OID(n: c_int) -> Oid;
    fn XLogRecGetInfo(record: *mut XLogReaderState) -> u8;
    fn XLogRecGetData(record: *mut XLogReaderState) -> *mut c_void;
    fn XLogRecHasAnyBlockRefs(record: *mut XLogReaderState) -> bool;
}

// libc stat stub
#[repr(C)] pub struct libc_stat { _opaque: [u8; 144] }
fn S_ISDIR(mode: u32) -> bool { (mode & 0o170000) == 0o040000 }

// OFile flags  TODO(pg-port)
const O_WRONLY: c_int = libc::O_WRONLY;
const O_CREAT: c_int = libc::O_CREAT;
const O_EXCL: c_int = libc::O_EXCL;
const O_TRUNC: c_int = libc::O_TRUNC;
const PG_BINARY: c_int = 0;

// LC_* constants  TODO(pg-port)
const LC_COLLATE: c_int = 6;
const LC_CTYPE: c_int = 0;

// PG encoding stubs  TODO(pg-port)
const PG_SQL_ASCII: c_int = 0;
const PG_UTF8: c_int = 6;

// ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS is a compile-time feature flag; skip.

/*
 * Create database strategy.
 *
 * CREATEDB_WAL_LOG will copy the database at the block level and WAL log each
 * copied block.
 *
 * CREATEDB_FILE_COPY will simply perform a file system level copy of the
 * database and log a single record for each tablespace copied. To make this
 * safe, it also triggers checkpoints before and after the operation.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub enum CreateDBStrategy {
    CREATEDB_WAL_LOG,
    CREATEDB_FILE_COPY,
}
use CreateDBStrategy::*;

#[repr(C)]
pub struct createdb_failure_params {
    src_dboid: Oid,   /* source (template) DB */
    dest_dboid: Oid,  /* DB we are trying to create */
    strategy: CreateDBStrategy, /* create db strategy */
}

#[repr(C)]
pub struct movedb_failure_params {
    dest_dboid: Oid, /* DB we are trying to move */
    dest_tsoid: Oid, /* tablespace we are trying to move to */
}

/*
 * Information about a relation to be copied when creating a database.
 */
#[repr(C)]
pub struct CreateDBRelInfo {
    pub rlocator: RelFileLocator,  /* physical relation identifier */
    pub reloid: Oid,               /* relation oid */
    pub permanent: bool,           /* relation is permanent or unlogged */
}

/*
 * Create a new database using the WAL_LOG strategy.
 *
 * Each copied block is separately written to the write-ahead log.
 */
unsafe fn CreateDatabaseUsingWalLog(src_dboid: Oid, dst_dboid: Oid,
                                     src_tsid: Oid, dst_tsid: Oid) {
    let srcpath: *mut c_char;
    let dstpath: *mut c_char;
    let mut rlocatorlist: *mut List = ptr::null_mut();
    let mut srcrelid: LockRelId = LockRelId { relId: 0, dbId: 0 };
    let mut dstrelid: LockRelId = LockRelId { relId: 0, dbId: 0 };
    let mut srcrlocator: RelFileLocator;
    let mut dstrlocator: RelFileLocator = RelFileLocator { spcOid: 0, dbOid: 0, relNumber: 0 };
    let relinfo: *mut CreateDBRelInfo;

    /* Get source and destination database paths. */
    srcpath = GetDatabasePath(src_dboid, src_tsid);
    dstpath = GetDatabasePath(dst_dboid, dst_tsid);

    /* Create database directory and write PG_VERSION file. */
    CreateDirAndVersionFile(dstpath, dst_dboid, dst_tsid, false);

    /* Copy relmap file from source database to the destination database. */
    RelationMapCopy(dst_dboid, dst_tsid, srcpath, dstpath);

    /* Get list of relfilelocators to copy from the source database. */
    rlocatorlist = ScanSourceDatabasePgClass(src_tsid, src_dboid, srcpath);
    // Assert(rlocatorlist != NIL)

    /*
     * Database IDs will be the same for all relations so set them before
     * entering the loop.
     */
    srcrelid.dbId = src_dboid;
    dstrelid.dbId = dst_dboid;

    /* Loop over our list of relfilelocators and copy each one. */
    let __rlocator_len = if rlocatorlist.is_null() { 0 } else { (*rlocatorlist).length };
    let mut __rlocator_i: c_int = 0;
    while __rlocator_i < __rlocator_len {
        let relinfo = list_nth(rlocatorlist, __rlocator_i) as *mut CreateDBRelInfo;
        srcrlocator = (*relinfo).rlocator;

        /*
         * If the relation is from the source db's default tablespace then we
         * need to create it in the destination db's default tablespace.
         * Otherwise, we need to create in the same tablespace as it is in the
         * source database.
         */
        if srcrlocator.spcOid == src_tsid {
            dstrlocator.spcOid = dst_tsid;
        } else {
            dstrlocator.spcOid = srcrlocator.spcOid;
        }

        dstrlocator.dbOid = dst_dboid;
        dstrlocator.relNumber = srcrlocator.relNumber;

        /*
         * Acquire locks on source and target relations before copying.
         *
         * We typically do not read relation data into shared_buffers without
         * holding a relation lock. It's unclear what could go wrong if we
         * skipped it in this case, because nobody can be modifying either the
         * source or destination database at this point, and we have locks on
         * both databases, too, but let's take the conservative route.
         */
        dstrelid.relId = (*relinfo).reloid;
        srcrelid.relId = (*relinfo).reloid;
        LockRelationId(&mut srcrelid, AccessShareLock);
        LockRelationId(&mut dstrelid, AccessShareLock);

        /* Copy relation storage from source to the destination. */
        CreateAndCopyRelationData(srcrlocator, dstrlocator, (*relinfo).permanent);

        /* Release the relation locks. */
        UnlockRelationId(&mut srcrelid, AccessShareLock);
        UnlockRelationId(&mut dstrelid, AccessShareLock);

        __rlocator_i += 1;
    }

    /*
     * Write the copied pages out to the destination files so that the new
     * database's relations have correct on-disk sizes for backends that open
     * it fresh (the block copy above only dirtied shared buffers).
     */
    crate::storage::buffer::bufmgr::FlushDatabaseBuffers(dst_dboid);

    pfree(srcpath as *mut c_void);
    pfree(dstpath as *mut c_void);
    list_free_deep(rlocatorlist);
}

/*
 * Scan the pg_class table in the source database to identify the relations
 * that need to be copied to the destination database.
 *
 * This is an exception to the usual rule that cross-database access is
 * not possible. We can make it work here because we know that there are no
 * connections to the source database and (since there can't be prepared
 * transactions touching that database) no in-doubt tuples either. This
 * means that we don't need to worry about pruning removing anything from
 * under us, and we don't need to be too picky about our snapshot either.
 * As long as it sees all previously-committed XIDs as committed and all
 * aborted XIDs as aborted, we should be fine: nothing else is possible
 * here.
 *
 * We can't rely on the relcache for anything here, because that only knows
 * about the database to which we are connected, and can't handle access to
 * other databases. That also means we can't rely on the heap scan
 * infrastructure, which would be a bad idea anyway since it might try
 * to do things like HOT pruning which we definitely can't do safely in
 * a database to which we're not even connected.
 */
unsafe fn ScanSourceDatabasePgClass(tbid: Oid, dbid: Oid, srcpath: *mut c_char) -> *mut List {
    let mut rlocator: RelFileLocator = RelFileLocator { spcOid: 0, dbOid: 0, relNumber: 0 };
    let nblocks: BlockNumber;
    let mut blkno: BlockNumber;
    let buf: Buffer;
    let relfilenumber: RelFileNumber;
    let page: Page;
    let mut rlocatorlist: *mut List = ptr::null_mut();
    let mut relid: LockRelId = LockRelId { relId: 0, dbId: 0 };
    let snapshot: Snapshot;
    let smgr: SMgrRelation;
    let bstrategy: BufferAccessStrategy;

    /* Get pg_class relfilenumber. */
    relfilenumber = RelationMapOidToFilenumberForDatabase(srcpath,
                                                          RelationRelationId);

    /* Don't read data into shared_buffers without holding a relation lock. */
    relid.dbId = dbid;
    relid.relId = RelationRelationId;
    LockRelationId(&mut relid, AccessShareLock);

    /* Prepare a RelFileLocator for the pg_class relation. */
    rlocator.spcOid = tbid;
    rlocator.dbOid = dbid;
    rlocator.relNumber = relfilenumber;

    smgr = smgropen(rlocator, INVALID_PROC_NUMBER);
    nblocks = smgrnblocks(smgr, MAIN_FORKNUM);
    smgrclose(smgr);

    /* Use a buffer access strategy since this is a bulk read operation. */
    bstrategy = GetAccessStrategy(BAS_BULKREAD);

    /*
     * As explained in the function header comments, we need a snapshot that
     * will see all committed transactions as committed, and our transaction
     * snapshot - or the active snapshot - might not be new enough for that,
     * but the return value of GetLatestSnapshot() should work fine.
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());

    /* Process the relation block by block. */
    blkno = 0;
    while blkno < nblocks {
        CHECK_FOR_INTERRUPTS();

        let buf = ReadBufferWithoutRelcache(rlocator, MAIN_FORKNUM, blkno,
                                            RBM_NORMAL, bstrategy, true);

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        let page = BufferGetPage(buf);
        if PageIsNew(page) || PageIsEmpty(page) {
            UnlockReleaseBuffer(buf);
            blkno += 1;
            continue;
        }

        /* Append relevant pg_class tuples for current page to rlocatorlist. */
        rlocatorlist = ScanSourceDatabasePgClassPage(page, buf, tbid, dbid,
                                                     srcpath, rlocatorlist,
                                                     snapshot);

        UnlockReleaseBuffer(buf);
        blkno += 1;
    }
    UnregisterSnapshot(snapshot);

    /* Release relation lock. */
    UnlockRelationId(&mut relid, AccessShareLock);

    rlocatorlist
}

/*
 * Scan one page of the source database's pg_class relation and add relevant
 * entries to rlocatorlist. The return value is the updated list.
 */
unsafe fn ScanSourceDatabasePgClassPage(page: Page, buf: Buffer, tbid: Oid, dbid: Oid,
                                         srcpath: *mut c_char, mut rlocatorlist: *mut List,
                                         snapshot: Snapshot) -> *mut List {
    let blkno: BlockNumber = BufferGetBlockNumber(buf);
    let mut offnum: u16;
    let maxoff: u16;
    let mut tuple: HeapTupleData = unsafe { std::mem::zeroed() };

    maxoff = PageGetMaxOffsetNumber(page);

    /* Loop over offsets. */
    offnum = 1; // FirstOffsetNumber
    while offnum <= maxoff {
        let itemid = PageGetItemId(page, offnum);

        /* Nothing to do if slot is empty or already dead. */
        if !ItemIdIsUsed(itemid) || ItemIdIsDead(itemid) ||
            ItemIdIsRedirected(itemid) {
            offnum += 1;
            continue;
        }

        // Assert(ItemIdIsNormal(itemid))
        ItemPointerSet(&raw mut tuple.t_self, blkno, offnum);

        /* Initialize a HeapTupleData structure. */
        tuple.t_data = PageGetItem(page, itemid) as HeapTupleHeader;
        tuple.t_len = ItemIdGetLength(itemid);
        tuple.t_tableOid = RelationRelationId;

        /* Skip tuples that are not visible to this snapshot. */
        if HeapTupleSatisfiesVisibility(&mut tuple, snapshot, buf) {
            /*
             * ScanSourceDatabasePgClassTuple is in charge of constructing a
             * CreateDBRelInfo object for this tuple, but can also decide that
             * this tuple isn't something we need to copy. If we do need to
             * copy the relation, add it to the list.
             */
            let relinfo = ScanSourceDatabasePgClassTuple(&mut tuple, tbid, dbid,
                                                          srcpath);
            if !relinfo.is_null() {
                rlocatorlist = lappend(rlocatorlist, relinfo as *mut c_void);
            }
        }

        offnum += 1;
    }

    rlocatorlist
}

/*
 * Decide whether a certain pg_class tuple represents something that
 * needs to be copied from the source database to the destination database,
 * and if so, construct a CreateDBRelInfo for it.
 *
 * Visibility checks are handled by the caller, so our job here is just
 * to assess the data stored in the tuple.
 */
pub unsafe fn ScanSourceDatabasePgClassTuple(tuple: *mut HeapTupleData, tbid: Oid, dbid: Oid,
                                               srcpath: *mut c_char) -> *mut CreateDBRelInfo {
    let relinfo: *mut CreateDBRelInfo;
    let classForm: Form_pg_class;
    let mut relfilenumber: RelFileNumber = InvalidRelFileNumber;

    classForm = GETSTRUCT(tuple) as Form_pg_class;

    /*
     * Return NULL if this object does not need to be copied.
     *
     * Shared objects don't need to be copied, because they are shared.
     * Objects without storage can't be copied, because there's nothing to
     * copy. Temporary relations don't need to be copied either, because they
     * are inaccessible outside of the session that created them, which must
     * be gone already, and couldn't connect to a different database if it
     * still existed. autovacuum will eventually remove the pg_class entries
     * as well.
     */
    // if classForm->reltablespace == GLOBALTABLESPACE_OID ||
    //    !RELKIND_HAS_STORAGE(classForm->relkind) ||
    //    classForm->relpersistence == RELPERSISTENCE_TEMP
    //      return NULL;
    // (These fields are accessed via the opaque Form_pg_class pointer; stub below)
    if pg_class_should_skip(classForm) {
        return ptr::null_mut();
    }

    /*
     * If relfilenumber is valid then directly use it.  Otherwise, consult the
     * relmap.
     */
    let raw_relfilenode = pg_class_relfilenode(classForm);
    if RelFileNumberIsValid(raw_relfilenode) {
        relfilenumber = raw_relfilenode;
    } else {
        relfilenumber = RelationMapOidToFilenumberForDatabase(srcpath,
                                                              pg_class_oid(classForm));
    }

    /* We must have a valid relfilenumber. */
    if !RelFileNumberIsValid(relfilenumber) {
        elog!(ERROR, "relation with OID {} does not have a valid relfilenumber", pg_class_oid(classForm));
    }

    /* Prepare a rel info element and add it to the list. */
    relinfo = palloc(std::mem::size_of::<CreateDBRelInfo>()) as *mut CreateDBRelInfo;
    let reltblspc = pg_class_reltablespace(classForm);
    if OidIsValid(reltblspc) {
        (*relinfo).rlocator.spcOid = reltblspc;
    } else {
        (*relinfo).rlocator.spcOid = tbid;
    }

    (*relinfo).rlocator.dbOid = dbid;
    (*relinfo).rlocator.relNumber = relfilenumber;
    (*relinfo).reloid = pg_class_oid(classForm);

    /* Temporary relations were rejected above. */
    // Assert(classForm->relpersistence != RELPERSISTENCE_TEMP)
    let persistence = pg_class_relpersistence(classForm);
    (*relinfo).permanent = persistence == RELPERSISTENCE_PERMANENT;

    relinfo
}

/* Form_pg_class field accessors */
unsafe fn pg_class_should_skip(classForm: Form_pg_class) -> bool {
    let relkind = (*classForm).relkind as u8;
    /* RELKIND_HAS_STORAGE: r,i,S,t,m have physical storage */
    let has_storage = matches!(relkind, b'r' | b'i' | b'S' | b't' | b'm');
    (*classForm).reltablespace == GLOBALTABLESPACE_OID
        || !has_storage
        || (*classForm).relpersistence == RELPERSISTENCE_TEMP
}
unsafe fn pg_class_relfilenode(classForm: Form_pg_class) -> RelFileNumber { (*classForm).relfilenode as _ }
unsafe fn pg_class_oid(classForm: Form_pg_class) -> Oid { (*classForm).oid }
unsafe fn pg_class_reltablespace(classForm: Form_pg_class) -> Oid { (*classForm).reltablespace }
unsafe fn pg_class_relpersistence(classForm: Form_pg_class) -> c_char { (*classForm).relpersistence }

fn RelFileNumberIsValid(n: RelFileNumber) -> bool { n != InvalidRelFileNumber }

/* elog helper for variadic C calls  TODO(pg-port) */
unsafe fn elog_c(elevel: c_int, fmt: *const c_char, arg: Oid) {
    unimplemented!()
}

// ERROR elevel constant  TODO(pg-port)
const ERROR: c_int = 21;

/*
 * Create database directory and write out the PG_VERSION file in the database
 * path.  If isRedo is true, it's okay for the database directory to exist
 * already.
 */
unsafe fn CreateDirAndVersionFile(dbpath: *mut c_char, dbid: Oid, tsid: Oid, isRedo: bool) {
    let fd: c_int;
    let nbytes: c_int;
    let mut versionfile: [c_char; 1024] = [0; 1024]; // MAXPGPATH
    let mut buf: [c_char; 16] = [0; 16];

    /*
     * Note that we don't have to copy version data from the source database;
     * there's only one legal value.
     */
    // sprintf(buf, "%s\n", PG_MAJORVERSION);
    let majorversion = b"18\n\0";
    let nbytes: usize = majorversion.len() - 1; // exclude NUL, include \n
    for (i, &b) in majorversion.iter().enumerate() {
        if i < buf.len() { buf[i] = b as c_char; }
    }

    /* Create database directory. */
    if MakePGDirectory(dbpath) < 0 {
        // Failure other than already exists or not in WAL replay?
        let errno_val = *libc_errno();
        if errno_val != 17 /* EEXIST */ || !isRedo {
            ereport!(ERROR, /* C also: errcode_for_file_access() */ errmsg!("could not create directory \"{}\": %m", CStr::from_ptr(dbpath).to_string_lossy()));
        }
    }

    /*
     * Create PG_VERSION file in the database path.  If the file already
     * exists and we are in WAL replay then try again to open it in write
     * mode.
     */
    // snprintf(versionfile, sizeof(versionfile), "%s/%s", dbpath, "PG_VERSION");
    snprintf_path(&mut versionfile, dbpath, b"PG_VERSION\0".as_ptr() as *const c_char);

    let mut fd = OpenTransientFile(versionfile.as_ptr(), O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
    let errno_val = *libc_errno();
    if fd < 0 && errno_val == 17 /* EEXIST */ && isRedo {
        fd = OpenTransientFile(versionfile.as_ptr(), O_WRONLY | O_TRUNC | PG_BINARY);
    }

    if fd < 0 {
        ereport!(ERROR, /* C also: errcode_for_file_access() */ errmsg!("could not create file \"{}\": %m", CStr::from_ptr(versionfile.as_ptr()).to_string_lossy()));
    }

    /* Write PG_MAJORVERSION in the PG_VERSION file. */
    pgstat_report_wait_start(WAIT_EVENT_VERSION_FILE_WRITE);
    *libc_errno() = 0;
    if write(fd, buf.as_ptr() as *const c_void, nbytes) != nbytes as isize {
        /* If write didn't set errno, assume problem is no disk space. */
        if *libc_errno() == 0 {
            *libc_errno() = 28; /* ENOSPC */
        }
        ereport!(ERROR, /* C also: errcode_for_file_access() */ errmsg!("could not write to file \"{}\": %m", CStr::from_ptr(versionfile.as_ptr()).to_string_lossy()));
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_VERSION_FILE_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(data_sync_elevel(ERROR), /* C also: errcode_for_file_access() */ errmsg!("could not fsync file \"{}\": %m", CStr::from_ptr(versionfile.as_ptr()).to_string_lossy()));
    }
    fsync_fname(dbpath, true);
    pgstat_report_wait_end();

    /* Close the version file. */
    CloseTransientFile(fd);

    /* If we are not in WAL replay then write the WAL. */
    if !isRedo {
        let mut xlrec = xl_dbase_create_wal_log_rec {
            db_id: dbid,
            tablespace_id: tsid,
        };

        START_CRIT_SECTION();

        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_dbase_create_wal_log_rec as *const c_void,
                         std::mem::size_of::<xl_dbase_create_wal_log_rec>());

        let _ = XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE_WAL_LOG);

        END_CRIT_SECTION();
    }
}

/* Helper to build versionfile path: dst = "dir/file" */
unsafe fn snprintf_path(dst: &mut [c_char; 1024], dir: *const c_char, file: *const c_char) {
    libc::snprintf(dst.as_mut_ptr(), 1024, b"%s/%s\0".as_ptr() as *const c_char, dir, file);
}

/* libc errno accessor  TODO(pg-port) */
unsafe fn libc_errno() -> *mut c_int {
    extern "C" { fn __error() -> *mut c_int; }
    __error()
}

/*
 * Create a new database using the FILE_COPY strategy.
 *
 * Copy each tablespace at the filesystem level, and log a single WAL record
 * for each tablespace copied.  This requires a checkpoint before and after the
 * copy, which may be expensive, but it does greatly reduce WAL generation
 * if the copied database is large.
 */
unsafe fn CreateDatabaseUsingFileCopy(src_dboid: Oid, dst_dboid: Oid,
                                       src_tsid: Oid, dst_tsid: Oid) {
    let scan: TableScanDesc;
    let rel: Relation;
    let tuple: HeapTuple;

    /*
     * Force a checkpoint before starting the copy. This will force all dirty
     * buffers, including those of unlogged tables, out to disk, to ensure
     * source database is up-to-date on disk for the copy.
     * FlushDatabaseBuffers() would suffice for that, but we also want to
     * process any pending unlink requests. Otherwise, if a checkpoint
     * happened while we're copying files, a file might be deleted just when
     * we're about to copy it, causing the lstat() call in copydir() to fail
     * with ENOENT.
     *
     * In binary upgrade mode, we can skip this checkpoint because pg_upgrade
     * is careful to ensure that template0 is fully written to disk prior to
     * any CREATE DATABASE commands.
     */
    if !IsBinaryUpgrade {
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE |
                          CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL);
    }

    /*
     * Iterate through all tablespaces of the template database, and copy each
     * one to the new database.
     */
    let rel = table_open(TableSpaceRelationId, AccessShareLock);
    let scan = table_beginscan_catalog(rel, 0, ptr::null());
    loop {
        let tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() { break; }

        let spaceform = GETSTRUCT(tuple) as Form_pg_tablespace;
        let srctablespace = pg_tablespace_oid(spaceform);
        let dsttablespace: Oid;
        let srcpath: *mut c_char;
        let dstpath: *mut c_char;
        let mut st: libc_stat = std::mem::zeroed();

        /* No need to copy global tablespace */
        if srctablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        srcpath = GetDatabasePath(src_dboid, srctablespace);

        if lstat(srcpath, &mut st) < 0 || !S_ISDIR(libc_stat_mode(&st)) ||
            directory_is_empty(srcpath) {
            /* Assume we can ignore it */
            pfree(srcpath as *mut c_void);
            continue;
        }

        if srctablespace == src_tsid {
            dsttablespace = dst_tsid;
        } else {
            dsttablespace = srctablespace;
        }

        dstpath = GetDatabasePath(dst_dboid, dsttablespace);

        /*
         * Copy this subdirectory to the new location
         *
         * We don't need to copy subdirectories
         */
        copydir(srcpath, dstpath, false);

        /* Record the filesystem change in XLOG */
        {
            let mut xlrec = xl_dbase_create_file_copy_rec {
                db_id: dst_dboid,
                tablespace_id: dsttablespace,
                src_db_id: src_dboid,
                src_tablespace_id: srctablespace,
            };

            XLogBeginInsert();
            XLogRegisterData(&xlrec as *const xl_dbase_create_file_copy_rec as *const c_void,
                             std::mem::size_of::<xl_dbase_create_file_copy_rec>());

            let _ = XLogInsert(RM_DBASE_ID,
                               XLOG_DBASE_CREATE_FILE_COPY | XLR_SPECIAL_REL_UPDATE);
        }
        pfree(srcpath as *mut c_void);
        pfree(dstpath as *mut c_void);
    }
    table_endscan(scan);
    table_close(rel, AccessShareLock);

    /*
     * We force a checkpoint before committing.  This effectively means that
     * committed XLOG_DBASE_CREATE_FILE_COPY operations will never need to be
     * replayed (at least not in ordinary crash recovery; we still have to
     * make the XLOG entry for the benefit of PITR operations). This avoids
     * two nasty scenarios:
     *
     * #1: At wal_level=minimal, we don't XLOG the contents of newly created
     * relfilenodes; therefore the drop-and-recreate-whole-directory behavior
     * of DBASE_CREATE replay would lose such files created in the new
     * database between our commit and the next checkpoint.
     *
     * #2: Since we have to recopy the source database during DBASE_CREATE
     * replay, we run the risk of copying changes in it that were committed
     * after the original CREATE DATABASE command but before the system crash
     * that led to the replay.  This is at least unexpected and at worst could
     * lead to inconsistencies, eg duplicate table names.
     *
     * (Both of these were real bugs in releases 8.0 through 8.0.3.)
     *
     * In PITR replay, the first of these isn't an issue, and the second is
     * only a risk if the CREATE DATABASE and subsequent template database
     * change both occur while a base backup is being taken. There doesn't
     * seem to be much we can do about that except document it as a
     * limitation.
     *
     * In binary upgrade mode, we can skip this checkpoint because neither of
     * these problems applies: we don't ever replay the WAL generated during
     * pg_upgrade, and we don't support taking base backups during pg_upgrade
     * (not to mention that we don't concurrently modify template0, either).
     *
     * See CreateDatabaseUsingWalLog() for a less cheesy CREATE DATABASE
     * strategy that avoids these problems.
     */
    if !IsBinaryUpgrade {
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE |
                          CHECKPOINT_WAIT);
    }
}

/* Stubs for opaque Form_pg_tablespace field access  TODO(pg-port) */
unsafe fn pg_tablespace_oid(_spaceform: Form_pg_tablespace) -> Oid { 0 }
unsafe fn libc_stat_mode(_st: &libc_stat) -> u32 { 0 }

/*
 * CREATE DATABASE
 */
pub unsafe fn createdb(pstate: *mut ParseState, stmt: *const CreatedbStmt) -> Oid {
    let mut src_dboid: Oid = 0;
    let mut src_owner: Oid = 0;
    let mut src_encoding: c_int = -1;
    let mut src_collate: *mut c_char = ptr::null_mut();
    let mut src_ctype: *mut c_char = ptr::null_mut();
    let mut src_locale: *mut c_char = ptr::null_mut();
    let mut src_icurules: *mut c_char = ptr::null_mut();
    let mut src_locprovider: c_char = b'\0' as c_char;
    let mut src_collversion: *mut c_char = ptr::null_mut();
    let mut src_istemplate: bool = false;
    let mut src_hasloginevt: bool = false;
    let mut src_allowconn: bool = false;
    let mut src_frozenxid: TransactionId = InvalidTransactionId;
    let mut src_minmxid: MultiXactId = InvalidMultiXactId;
    let mut src_deftablespace: Oid = 0;
    let mut dst_deftablespace: Oid = 0;
    let pg_database_rel: Relation;
    let tuple: HeapTuple;
    let mut new_record: [Datum; Natts_pg_database] = [0usize; Natts_pg_database];
    let mut new_record_nulls: [bool; Natts_pg_database] = [false; Natts_pg_database];
    let mut dboid: Oid = InvalidOid;
    let mut datdba: Oid;
    let mut tablespacenameEl: *mut DefElem = ptr::null_mut();
    let mut ownerEl: *mut DefElem = ptr::null_mut();
    let mut templateEl: *mut DefElem = ptr::null_mut();
    let mut encodingEl: *mut DefElem = ptr::null_mut();
    let mut localeEl: *mut DefElem = ptr::null_mut();
    let mut builtinlocaleEl: *mut DefElem = ptr::null_mut();
    let mut collateEl: *mut DefElem = ptr::null_mut();
    let mut ctypeEl: *mut DefElem = ptr::null_mut();
    let mut iculocaleEl: *mut DefElem = ptr::null_mut();
    let mut icurulesEl: *mut DefElem = ptr::null_mut();
    let mut locproviderEl: *mut DefElem = ptr::null_mut();
    let mut istemplateEl: *mut DefElem = ptr::null_mut();
    let mut allowconnectionsEl: *mut DefElem = ptr::null_mut();
    let mut connlimitEl: *mut DefElem = ptr::null_mut();
    let mut collversionEl: *mut DefElem = ptr::null_mut();
    let mut strategyEl: *mut DefElem = ptr::null_mut();
    let dbname: *const c_char = (*stmt).dbname;
    let mut dbowner: *mut c_char = ptr::null_mut();
    let mut dbtemplate: *const c_char = ptr::null();
    let mut dbcollate: *mut c_char = ptr::null_mut();
    let mut dbctype: *mut c_char = ptr::null_mut();
    let mut dblocale: *const c_char = ptr::null();
    let mut dbicurules: *mut c_char = ptr::null_mut();
    let mut dblocprovider: c_char = b'\0' as c_char;
    let mut canonname: *mut c_char = ptr::null_mut();
    let mut encoding: c_int = -1;
    let mut dbistemplate: bool = false;
    let mut dballowconnections: bool = true;
    let mut dbconnlimit: c_int = DATCONNLIMIT_UNLIMITED;
    let mut dbcollversion: *mut c_char = ptr::null_mut();
    let mut notherbackends: c_int = 0;
    let mut npreparedxacts: c_int = 0;
    let mut dbstrategy: CreateDBStrategy = CREATEDB_WAL_LOG;
    let mut fparms: createdb_failure_params = createdb_failure_params {
        src_dboid: 0, dest_dboid: 0, strategy: CREATEDB_WAL_LOG,
    };

    /* Extract options from the statement node tree */
    let __options_len = if (*stmt).options.is_null() { 0 } else { (*(*stmt).options).length };
    let mut __option_i: c_int = 0;
    while __option_i < __options_len {
        let defel = list_nth((*stmt).options, __option_i) as *mut DefElem;

        if strcmp_rs((*defel).defname, "tablespace") {
            if !tablespacenameEl.is_null() { errorConflictingDefElem(defel, pstate); }
            tablespacenameEl = defel;
        } else if strcmp_rs((*defel).defname, "owner") {
            if !ownerEl.is_null() { errorConflictingDefElem(defel, pstate); }
            ownerEl = defel;
        } else if strcmp_rs((*defel).defname, "template") {
            if !templateEl.is_null() { errorConflictingDefElem(defel, pstate); }
            templateEl = defel;
        } else if strcmp_rs((*defel).defname, "encoding") {
            if !encodingEl.is_null() { errorConflictingDefElem(defel, pstate); }
            encodingEl = defel;
        } else if strcmp_rs((*defel).defname, "locale") {
            if !localeEl.is_null() { errorConflictingDefElem(defel, pstate); }
            localeEl = defel;
        } else if strcmp_rs((*defel).defname, "builtin_locale") {
            if !builtinlocaleEl.is_null() { errorConflictingDefElem(defel, pstate); }
            builtinlocaleEl = defel;
        } else if strcmp_rs((*defel).defname, "lc_collate") {
            if !collateEl.is_null() { errorConflictingDefElem(defel, pstate); }
            collateEl = defel;
        } else if strcmp_rs((*defel).defname, "lc_ctype") {
            if !ctypeEl.is_null() { errorConflictingDefElem(defel, pstate); }
            ctypeEl = defel;
        } else if strcmp_rs((*defel).defname, "icu_locale") {
            if !iculocaleEl.is_null() { errorConflictingDefElem(defel, pstate); }
            iculocaleEl = defel;
        } else if strcmp_rs((*defel).defname, "icu_rules") {
            if !icurulesEl.is_null() { errorConflictingDefElem(defel, pstate); }
            icurulesEl = defel;
        } else if strcmp_rs((*defel).defname, "locale_provider") {
            if !locproviderEl.is_null() { errorConflictingDefElem(defel, pstate); }
            locproviderEl = defel;
        } else if strcmp_rs((*defel).defname, "is_template") {
            if !istemplateEl.is_null() { errorConflictingDefElem(defel, pstate); }
            istemplateEl = defel;
        } else if strcmp_rs((*defel).defname, "allow_connections") {
            if !allowconnectionsEl.is_null() { errorConflictingDefElem(defel, pstate); }
            allowconnectionsEl = defel;
        } else if strcmp_rs((*defel).defname, "connection_limit") {
            if !connlimitEl.is_null() { errorConflictingDefElem(defel, pstate); }
            connlimitEl = defel;
        } else if strcmp_rs((*defel).defname, "collation_version") {
            if !collversionEl.is_null() { errorConflictingDefElem(defel, pstate); }
            collversionEl = defel;
        } else if strcmp_rs((*defel).defname, "location") {
            ereport!(WARNING, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); errhint(b"Consider using tablespaces instead.\0".as_ptr() as *const c_char); parser_errposition(pstate, (*defel).location) */ errmsg!("LOCATION is not supported anymore"));
        } else if strcmp_rs((*defel).defname, "oid") {
            dboid = defGetObjectId(defel);

            /*
             * We don't normally permit new databases to be created with
             * system-assigned OIDs. pg_upgrade tries to preserve database
             * OIDs, so we can't allow any database to be created with an OID
             * that might be in use in a freshly-initialized cluster created
             * by some future version. We assume all such OIDs will be from
             * the system-managed OID range.
             *
             * As an exception, however, we permit any OID to be assigned when
             * allow_system_table_mods=on (so that initdb can assign system
             * OIDs to template0 and postgres) or when performing a binary
             * upgrade (so that pg_upgrade can preserve whatever OIDs it finds
             * in the source cluster).
             */
            if dboid < FirstNormalObjectId
                && !allowSystemTableMods && !IsBinaryUpgrade {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("OIDs less than {} are reserved for system objects", FirstNormalObjectId));
            }
        } else if strcmp_rs((*defel).defname, "strategy") {
            if !strategyEl.is_null() { errorConflictingDefElem(defel, pstate); }
            strategyEl = defel;
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_SYNTAX_ERROR); parser_errposition(pstate, (*defel).location) */ errmsg!("option \"{}\" not recognized", CStr::from_ptr((*defel).defname).to_string_lossy()));
        }

        __option_i += 1;
    }

    if !ownerEl.is_null() && !(*ownerEl).arg.is_null() {
        dbowner = defGetString(ownerEl);
    }
    if !templateEl.is_null() && !(*templateEl).arg.is_null() {
        dbtemplate = defGetString(templateEl);
    }
    if !encodingEl.is_null() && !(*encodingEl).arg.is_null() {
        // if (IsA(encodingEl->arg, Integer))
        if pg_node_is_integer((*encodingEl).arg as *mut c_void) {
            encoding = defGetInt32(encodingEl);
            let encoding_name = pg_encoding_to_char(encoding);
            if *encoding_name == 0
                || pg_valid_server_encoding(encoding_name) < 0 {
                ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_OBJECT); parser_errposition(pstate, (*encodingEl).location) */ errmsg!("{} is not a valid encoding code", encoding));
            }
        } else {
            let encoding_name = defGetString(encodingEl);
            encoding = pg_valid_server_encoding(encoding_name);
            if encoding < 0 {
                ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_OBJECT); parser_errposition(pstate, (*encodingEl).location) */ errmsg!("{} is not a valid encoding name", CStr::from_ptr(encoding_name).to_string_lossy()));
            }
        }
    }
    if !localeEl.is_null() && !(*localeEl).arg.is_null() {
        dbcollate = defGetString(localeEl);
        dbctype = defGetString(localeEl);
        dblocale = defGetString(localeEl);
    }
    if !builtinlocaleEl.is_null() && !(*builtinlocaleEl).arg.is_null() {
        dblocale = defGetString(builtinlocaleEl);
    }
    if !collateEl.is_null() && !(*collateEl).arg.is_null() {
        dbcollate = defGetString(collateEl);
    }
    if !ctypeEl.is_null() && !(*ctypeEl).arg.is_null() {
        dbctype = defGetString(ctypeEl);
    }
    if !iculocaleEl.is_null() && !(*iculocaleEl).arg.is_null() {
        dblocale = defGetString(iculocaleEl);
    }
    if !icurulesEl.is_null() && !(*icurulesEl).arg.is_null() {
        dbicurules = defGetString(icurulesEl);
    }
    if !locproviderEl.is_null() && !(*locproviderEl).arg.is_null() {
        let locproviderstr = defGetString(locproviderEl);
        if pg_strcasecmp(locproviderstr, b"builtin\0".as_ptr() as *const c_char) == 0 {
            dblocprovider = COLLPROVIDER_BUILTIN;
        } else if pg_strcasecmp(locproviderstr, b"icu\0".as_ptr() as *const c_char) == 0 {
            dblocprovider = COLLPROVIDER_ICU;
        } else if pg_strcasecmp(locproviderstr, b"libc\0".as_ptr() as *const c_char) == 0 {
            dblocprovider = COLLPROVIDER_LIBC;
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */ errmsg!("unrecognized locale provider: {}", CStr::from_ptr(locproviderstr).to_string_lossy()));
        }
    }
    if !istemplateEl.is_null() && !(*istemplateEl).arg.is_null() {
        dbistemplate = defGetBoolean(istemplateEl);
    }
    if !allowconnectionsEl.is_null() && !(*allowconnectionsEl).arg.is_null() {
        dballowconnections = defGetBoolean(allowconnectionsEl);
    }
    if !connlimitEl.is_null() && !(*connlimitEl).arg.is_null() {
        dbconnlimit = defGetInt32(connlimitEl);
        if dbconnlimit < DATCONNLIMIT_UNLIMITED {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("invalid connection limit: {}", dbconnlimit));
        }
    }
    if !collversionEl.is_null() {
        dbcollversion = defGetString(collversionEl);
    }

    /* obtain OID of proposed owner */
    if !dbowner.is_null() {
        datdba = get_role_oid(dbowner, false);
    } else {
        datdba = GetUserId();
    }

    /*
     * To create a database, must have createdb privilege and must be able to
     * become the target role (this does not imply that the target role itself
     * must have createdb privilege).  The latter provision guards against
     * "giveaway" attacks.  Note that a superuser will always have both of
     * these privileges a fortiori.
     */
    if !have_createdb_privilege() {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */ errmsg!("permission denied to create database"));
    }

    check_can_set_role(GetUserId(), datdba);

    /*
     * Lookup database (template) to be cloned, and obtain share lock on it.
     * ShareLock allows two CREATE DATABASEs to work from the same template
     * concurrently, while ensuring no one is busy dropping it in parallel
     * (which would be Very Bad since we'd likely get an incomplete copy
     * without knowing it).  This also prevents any new connections from being
     * made to the source until we finish copying it, so we can be sure it
     * won't change underneath us.
     */
    if dbtemplate.is_null() {
        dbtemplate = b"template1\0".as_ptr() as *const c_char; /* Default template database name */
    }

    if !get_db_info(dbtemplate, ShareLock,
                    &mut src_dboid, &mut src_owner, &mut src_encoding,
                    &mut src_istemplate, &mut src_allowconn, &mut src_hasloginevt,
                    &mut src_frozenxid, &mut src_minmxid, &mut src_deftablespace,
                    &mut src_collate, &mut src_ctype, &mut src_locale, &mut src_icurules,
                    &mut src_locprovider,
                    &mut src_collversion) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("template database \"{}\" does not exist", CStr::from_ptr(dbtemplate).to_string_lossy()));
    }

    /*
     * If the source database was in the process of being dropped, we can't
     * use it as a template.
     */
    if database_is_invalid_oid(src_dboid) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE); errhint(b"Use DROP DATABASE to drop invalid databases.\0".as_ptr() as *const c_char) */ errmsg!("cannot use invalid database \"{}\" as template", CStr::from_ptr(dbtemplate).to_string_lossy()));
    }

    /*
     * Permission check: to copy a DB that's not marked datistemplate, you
     * must be superuser or the owner thereof.
     */
    if !src_istemplate {
        if !object_ownercheck(DatabaseRelationId, src_dboid, GetUserId()) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */ errmsg!("permission denied to copy database \"{}\"", CStr::from_ptr(dbtemplate).to_string_lossy()));
        }
    }

    /* Validate the database creation strategy. */
    if !strategyEl.is_null() && !(*strategyEl).arg.is_null() {
        let strategy = defGetString(strategyEl);
        if pg_strcasecmp(strategy, b"wal_log\0".as_ptr() as *const c_char) == 0 {
            dbstrategy = CREATEDB_WAL_LOG;
        } else if pg_strcasecmp(strategy, b"file_copy\0".as_ptr() as *const c_char) == 0 {
            dbstrategy = CREATEDB_FILE_COPY;
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Valid strategies are \"wal_log\" and \"file_copy\".\0".as_ptr() as *const c_char) */ errmsg!("invalid create database strategy \"{}\"", CStr::from_ptr(strategy).to_string_lossy()));
        }
    }

    /* If encoding or locales are defaulted, use source's setting */
    if encoding < 0 { encoding = src_encoding; }
    if dbcollate.is_null() { dbcollate = src_collate; }
    if dbctype.is_null() { dbctype = src_ctype; }
    if dblocprovider == b'\0' as c_char { dblocprovider = src_locprovider; }
    if dblocale.is_null() && dblocprovider == src_locprovider { dblocale = src_locale; }
    if dbicurules.is_null() { dbicurules = src_icurules; }

    /* Some encodings are client only */
    if !PG_VALID_BE_ENCODING(encoding) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */ errmsg!("invalid server encoding {}", encoding));
    }

    /* Check that the chosen locales are valid, and get canonical spellings */
    if !check_locale(LC_COLLATE, dbcollate, &mut canonname) {
        if dblocprovider == COLLPROVIDER_BUILTIN {
            ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE); errhint(b"If the locale name is specific to the builtin provider, use BUILTIN_LOCALE.\0".as_ptr() as *const c_char) */ errmsg!("invalid LC_COLLATE locale name: \"{}\"", CStr::from_ptr(dbcollate).to_string_lossy()));
        } else if dblocprovider == COLLPROVIDER_ICU {
            ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE); errhint(b"If the locale name is specific to the ICU provider, use ICU_LOCALE.\0".as_ptr() as *const c_char) */ errmsg!("invalid LC_COLLATE locale name: \"{}\"", CStr::from_ptr(dbcollate).to_string_lossy()));
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */ errmsg!("invalid LC_COLLATE locale name: \"{}\"", CStr::from_ptr(dbcollate).to_string_lossy()));
        }
    }
    dbcollate = canonname;

    if !check_locale(LC_CTYPE, dbctype, &mut canonname) {
        if dblocprovider == COLLPROVIDER_BUILTIN {
            ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE); errhint(b"If the locale name is specific to the builtin provider, use BUILTIN_LOCALE.\0".as_ptr() as *const c_char) */ errmsg!("invalid LC_CTYPE locale name: \"{}\"", CStr::from_ptr(dbctype).to_string_lossy()));
        } else if dblocprovider == COLLPROVIDER_ICU {
            ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE); errhint(b"If the locale name is specific to the ICU provider, use ICU_LOCALE.\0".as_ptr() as *const c_char) */ errmsg!("invalid LC_CTYPE locale name: \"{}\"", CStr::from_ptr(dbctype).to_string_lossy()));
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */ errmsg!("invalid LC_CTYPE locale name: \"{}\"", CStr::from_ptr(dbctype).to_string_lossy()));
        }
    }

    dbctype = canonname;

    check_encoding_locale_matches(encoding, dbcollate, dbctype);

    /* validate provider-specific parameters */
    if dblocprovider != COLLPROVIDER_BUILTIN {
        if !builtinlocaleEl.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */ errmsg!("BUILTIN_LOCALE cannot be specified unless locale provider is builtin"));
        }
    }

    if dblocprovider != COLLPROVIDER_ICU {
        if !iculocaleEl.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */ errmsg!("ICU locale cannot be specified unless locale provider is ICU"));
        }
        if !dbicurules.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */ errmsg!("ICU rules cannot be specified unless locale provider is ICU"));
        }
    }

    /* validate and canonicalize locale for the provider */
    if dblocprovider == COLLPROVIDER_BUILTIN {
        /*
         * This would happen if template0 uses the libc provider but the new
         * database uses builtin.
         */
        if dblocale.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("LOCALE or BUILTIN_LOCALE must be specified"));
        }
        dblocale = builtin_validate_locale(encoding, dblocale);
    } else if dblocprovider == COLLPROVIDER_ICU {
        if !is_encoding_supported_by_icu(encoding) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("encoding \"{}\" is not supported with ICU provider", CStr::from_ptr(pg_encoding_to_char(encoding)).to_string_lossy()));
        }

        /*
         * This would happen if template0 uses the libc provider but the new
         * database uses icu.
         */
        if dblocale.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("LOCALE or ICU_LOCALE must be specified"));
        }

        /*
         * During binary upgrade, or when the locale came from the template
         * database, preserve locale string. Otherwise, canonicalize to a
         * language tag.
         */
        if !IsBinaryUpgrade && dblocale != src_locale {
            let langtag = icu_language_tag(dblocale, icu_validation_level);
            if !langtag.is_null() && strcmp_ptr(dblocale, langtag) != 0 {
                ereport!(NOTICE, errmsg!("using standard form \"{}\" for ICU locale \"{}\"", CStr::from_ptr(langtag).to_string_lossy(), CStr::from_ptr(dblocale).to_string_lossy()));
                dblocale = langtag;
            }
        }

        icu_validate_locale(dblocale);
    }

    /* for libc, locale comes from datcollate and datctype */
    if dblocprovider == COLLPROVIDER_LIBC {
        dblocale = ptr::null();
    }

    /*
     * Check that the new encoding and locale settings match the source
     * database.  We insist on this because we simply copy the source data ---
     * any non-ASCII data would be wrongly encoded, and any indexes sorted
     * according to the source locale would be wrong.
     *
     * However, we assume that template0 doesn't contain any non-ASCII data
     * nor any indexes that depend on collation or ctype, so template0 can be
     * used as template for creating a database with any encoding or locale.
     */
    if strcmp_ptr(dbtemplate, b"template0\0".as_ptr() as *const c_char) != 0 {
        if encoding != src_encoding {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Use the same encoding as in the template database, or use template0 as template.\0".as_ptr() as *const c_char) */ errmsg!("new encoding ({}) is incompatible with the encoding of the template database ({})", CStr::from_ptr(pg_encoding_to_char(encoding)).to_string_lossy(), CStr::from_ptr(pg_encoding_to_char(src_encoding)).to_string_lossy()));
        }

        if strcmp_ptr(dbcollate, src_collate) != 0 {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Use the same collation as in the template database, or use template0 as template.\0".as_ptr() as *const c_char) */ errmsg!("new collation ({}) is incompatible with the collation of the template database ({})", CStr::from_ptr(dbcollate).to_string_lossy(), CStr::from_ptr(src_collate).to_string_lossy()));
        }

        if strcmp_ptr(dbctype, src_ctype) != 0 {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Use the same LC_CTYPE as in the template database, or use template0 as template.\0".as_ptr() as *const c_char) */ errmsg!("new LC_CTYPE ({}) is incompatible with the LC_CTYPE of the template database ({})", CStr::from_ptr(dbctype).to_string_lossy(), CStr::from_ptr(src_ctype).to_string_lossy()));
        }

        if dblocprovider != src_locprovider {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Use the same locale provider as in the template database, or use template0 as template.\0".as_ptr() as *const c_char) */ errmsg!("new locale provider ({}) does not match locale provider of the template database ({})", CStr::from_ptr(collprovider_name(dblocprovider)).to_string_lossy(), CStr::from_ptr(collprovider_name(src_locprovider)).to_string_lossy()));
        }

        if dblocprovider == COLLPROVIDER_ICU {
            // Assert(dblocale) Assert(src_locale)
            if strcmp_ptr(dblocale, src_locale) != 0 {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Use the same ICU locale as in the template database, or use template0 as template.\0".as_ptr() as *const c_char) */ errmsg!("new ICU locale ({}) is incompatible with the ICU locale of the template database ({})", CStr::from_ptr(dblocale).to_string_lossy(), CStr::from_ptr(src_locale).to_string_lossy()));
            }

            let val1 = if !dbicurules.is_null() { dbicurules } else { b"\0".as_ptr() as *mut c_char };
            let val2 = if !src_icurules.is_null() { src_icurules } else { b"\0".as_ptr() as *mut c_char };
            if strcmp_ptr(val1, val2) != 0 {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errhint(b"Use the same ICU collation rules as in the template database, or use template0 as template.\0".as_ptr() as *const c_char) */ errmsg!("new ICU collation rules ({}) are incompatible with the ICU collation rules of the template database ({})", CStr::from_ptr(val1).to_string_lossy(), CStr::from_ptr(val2).to_string_lossy()));
            }
        }
    }

    /*
     * If we got a collation version for the template database, check that it
     * matches the actual OS collation version.  Otherwise error; the user
     * needs to fix the template database first.  Don't complain if a
     * collation version was specified explicitly as a statement option; that
     * is used by pg_upgrade to reproduce the old state exactly.
     *
     * (If the template database has no collation version, then either the
     * platform/provider does not support collation versioning, or it's
     * template0, for which we stipulate that it does not contain
     * collation-using objects.)
     */
    if !src_collversion.is_null() && collversionEl.is_null() {
        let actual_versionstr: *mut c_char;
        let locale: *const c_char;

        if dblocprovider == COLLPROVIDER_LIBC {
            locale = dbcollate;
        } else {
            locale = dblocale;
        }

        actual_versionstr = get_collation_actual_version(dblocprovider, locale);
        if actual_versionstr.is_null() {
            ereport!(ERROR, errmsg!("template database \"{}\" has a collation version, but no actual collation version could be determined", CStr::from_ptr(dbtemplate).to_string_lossy()));
        }

        if strcmp_ptr(actual_versionstr, src_collversion) != 0 {
            ereport!(ERROR, /* C also: errdetail(b"The template database was created using collation version %s, but the operating system provides version %s.\0".as_ptr() as *const c_char, src_collversion, actual_versionstr); errhint(b"Rebuild all objects in the template database that use the default collation and run ALTER DATABASE %s REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.\0".as_ptr() as *const c_char, quote_identifier(dbtemplate)) */ errmsg!("template database \"{}\" has a collation version mismatch", CStr::from_ptr(dbtemplate).to_string_lossy()));
        }
    }

    if dbcollversion.is_null() {
        dbcollversion = src_collversion;
    }

    /*
     * Normally, we copy the collation version from the template database.
     * This last resort only applies if the template database does not have a
     * collation version, which is normally only the case for template0.
     */
    if dbcollversion.is_null() {
        let locale: *const c_char;

        if dblocprovider == COLLPROVIDER_LIBC {
            locale = dbcollate;
        } else {
            locale = dblocale;
        }

        dbcollversion = get_collation_actual_version(dblocprovider, locale);
    }

    /* Resolve default tablespace for new database */
    if !tablespacenameEl.is_null() && !(*tablespacenameEl).arg.is_null() {
        let tablespacename: *mut c_char;
        let aclresult: AclResult;

        tablespacename = defGetString(tablespacenameEl);
        dst_deftablespace = get_tablespace_oid(tablespacename, false);
        /* check permissions */
        let aclresult = object_aclcheck(TableSpaceRelationId, dst_deftablespace, GetUserId(),
                                        ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TABLESPACE, tablespacename);
        }

        /* pg_global must never be the default tablespace */
        if dst_deftablespace == GLOBALTABLESPACE_OID {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("pg_global cannot be used as default tablespace"));
        }

        /*
         * If we are trying to change the default tablespace of the template,
         * we require that the template not have any files in the new default
         * tablespace.  This is necessary because otherwise the copied
         * database would contain pg_class rows that refer to its default
         * tablespace both explicitly (by OID) and implicitly (as zero), which
         * would cause problems.  For example another CREATE DATABASE using
         * the copied database as template, and trying to change its default
         * tablespace again, would yield outright incorrect results (it would
         * improperly move tables to the new default tablespace that should
         * stay in the same tablespace).
         */
        if dst_deftablespace != src_deftablespace {
            let srcpath: *mut c_char;
            let mut st: libc_stat = std::mem::zeroed();

            srcpath = GetDatabasePath(src_dboid, dst_deftablespace);

            if lstat(srcpath, &mut st) == 0
                && S_ISDIR(libc_stat_mode(&st))
                && !directory_is_empty(srcpath) {
                ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); errdetail(b"There is a conflict because database \"%s\" already has some tables in this tablespace.\0".as_ptr() as *const c_char, dbtemplate) */ errmsg!("cannot assign new default tablespace \"{}\"", CStr::from_ptr(tablespacename).to_string_lossy()));
            }
            pfree(srcpath as *mut c_void);
        }
    } else {
        /* Use template database's default tablespace */
        dst_deftablespace = src_deftablespace;
        /* Note there is no additional permission check in this path */
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for database names are violated.  But don't complain during
     * initdb.
     */
    // #ifdef ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS
    // if (IsUnderPostmaster && strstr(dbname, "regression") == NULL)
    //     elog(WARNING, "databases created by regression test cases should have names including \"regression\"");
    // #endif

    /*
     * Check for db name conflict.  This is just to give a more friendly error
     * message than "unique index violation".  There's a race condition but
     * we're willing to accept the less friendly message in that case.
     */
    if OidIsValid(get_database_oid(dbname, true)) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_DUPLICATE_DATABASE) */ errmsg!("database \"{}\" already exists", CStr::from_ptr(dbname).to_string_lossy()));
    }

    /*
     * The source DB can't have any active backends, except this one
     * (exception is to allow CREATE DB while connected to template1).
     * Otherwise we might copy inconsistent data.
     *
     * This should be last among the basic error checks, because it involves
     * potential waiting; we may as well throw an error first if we're gonna
     * throw one.
     */
    if CountOtherDBBackends(src_dboid, &mut notherbackends, &mut npreparedxacts) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE); errdetail_busy_db(notherbackends, npreparedxacts) */ errmsg!("source database \"{}\" is being accessed by other users", CStr::from_ptr(dbtemplate).to_string_lossy()));
    }

    /*
     * Select an OID for the new database, checking that it doesn't have a
     * filename conflict with anything already existing in the tablespace
     * directories.
     */
    let pg_database_rel = table_open(DatabaseRelationId, RowExclusiveLock);

    /*
     * If database OID is configured, check if the OID is already in use or
     * data directory already exists.
     */
    if OidIsValid(dboid) {
        let existing_dbname = get_database_name(dboid);
        if !existing_dbname.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("database OID {} is already in use by database \"{}\"", dboid, CStr::from_ptr(existing_dbname).to_string_lossy()));
        }
        if check_db_file_conflict(dboid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("data directory with the specified OID {} already exists", dboid));
        }
    } else {
        /* Select an OID for the new database if is not explicitly configured. */
        loop {
            dboid = GetNewOidWithIndex(pg_database_rel, DatabaseOidIndexId,
                                       Anum_pg_database_oid as c_int);
            if !check_db_file_conflict(dboid) { break; }
        }
    }

    /*
     * Insert a new tuple into pg_database.  This establishes our ownership of
     * the new database name (anyone else trying to insert the same name will
     * block on the unique index, and fail after we commit).
     */

    // Assert((dblocprovider != COLLPROVIDER_LIBC && dblocale) ||
    //        (dblocprovider == COLLPROVIDER_LIBC && !dblocale));

    /* Form tuple */
    new_record[Anum_pg_database_oid - 1] = ObjectIdGetDatum(dboid);
    new_record[Anum_pg_database_datname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(dbname));
    new_record[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(datdba);
    new_record[Anum_pg_database_encoding - 1] = Int32GetDatum(encoding);
    new_record[Anum_pg_database_datlocprovider - 1] = CharGetDatum(dblocprovider);
    new_record[Anum_pg_database_datistemplate - 1] = BoolGetDatum(dbistemplate);
    new_record[Anum_pg_database_datallowconn - 1] = BoolGetDatum(dballowconnections);
    new_record[Anum_pg_database_dathasloginevt - 1] = BoolGetDatum(src_hasloginevt);
    new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(dbconnlimit);
    new_record[Anum_pg_database_datfrozenxid - 1] = TransactionIdGetDatum(src_frozenxid);
    new_record[Anum_pg_database_datminmxid - 1] = TransactionIdGetDatum(src_minmxid);
    new_record[Anum_pg_database_dattablespace - 1] = ObjectIdGetDatum(dst_deftablespace);
    new_record[Anum_pg_database_datcollate - 1] = CStringGetTextDatum(dbcollate);
    new_record[Anum_pg_database_datctype - 1] = CStringGetTextDatum(dbctype);
    if !dblocale.is_null() {
        new_record[Anum_pg_database_datlocale - 1] = CStringGetTextDatum(dblocale);
    } else {
        new_record_nulls[Anum_pg_database_datlocale - 1] = true;
    }
    if !dbicurules.is_null() {
        new_record[Anum_pg_database_daticurules - 1] = CStringGetTextDatum(dbicurules);
    } else {
        new_record_nulls[Anum_pg_database_daticurules - 1] = true;
    }
    if !dbcollversion.is_null() {
        new_record[Anum_pg_database_datcollversion - 1] = CStringGetTextDatum(dbcollversion);
    } else {
        new_record_nulls[Anum_pg_database_datcollversion - 1] = true;
    }

    /*
     * We deliberately set datacl to default (NULL), rather than copying it
     * from the template database.  Copying it would be a bad idea when the
     * owner is not the same as the template's owner.
     */
    new_record_nulls[Anum_pg_database_datacl - 1] = true;

    let tuple = heap_form_tuple(RelationGetDescr(pg_database_rel),
                                new_record.as_mut_ptr(), new_record_nulls.as_mut_ptr());

    CatalogTupleInsert(pg_database_rel, tuple);

    /*
     * Now generate additional catalog entries associated with the new DB
     */

    /* Register owner dependency */
    recordDependencyOnOwner(DatabaseRelationId, dboid, datdba);

    /* Create pg_shdepend entries for objects within database */
    copyTemplateDependencies(src_dboid, dboid);

    /* Post creation hook for new database */
    InvokeObjectPostCreateHook(DatabaseRelationId, dboid, 0);

    /*
     * If we're going to be reading data for the to-be-created database into
     * shared_buffers, take a lock on it. Nobody should know that this
     * database exists yet, but it's good to maintain the invariant that an
     * AccessExclusiveLock on the database is sufficient to drop all of its
     * buffers without worrying about more being read later.
     *
     * Note that we need to do this before entering the
     * PG_ENSURE_ERROR_CLEANUP block below, because createdb_failure_callback
     * expects this lock to be held already.
     */
    if dbstrategy == CREATEDB_WAL_LOG {
        LockSharedObject(DatabaseRelationId, dboid, 0, AccessShareLock);
    }

    /*
     * Once we start copying subdirectories, we need to be able to clean 'em
     * up if we fail.  Use an ENSURE block to make sure this happens.  (This
     * is not a 100% solution, because of the possibility of failure during
     * transaction commit after we leave this routine, but it should handle
     * most scenarios.)
     */
    fparms.src_dboid = src_dboid;
    fparms.dest_dboid = dboid;
    fparms.strategy = dbstrategy;

    PG_ENSURE_ERROR_CLEANUP(createdb_failure_callback,
                             PointerGetDatum(&fparms as *const createdb_failure_params as *const c_void));
    {
        /*
         * If the user has asked to create a database with WAL_LOG strategy
         * then call CreateDatabaseUsingWalLog, which will copy the database
         * at the block level and it will WAL log each copied block.
         * Otherwise, call CreateDatabaseUsingFileCopy that will copy the
         * database file by file.
         */
        if dbstrategy == CREATEDB_WAL_LOG {
            CreateDatabaseUsingWalLog(src_dboid, dboid, src_deftablespace,
                                      dst_deftablespace);
        } else {
            CreateDatabaseUsingFileCopy(src_dboid, dboid, src_deftablespace,
                                        dst_deftablespace);
        }

        /*
         * Close pg_database, but keep lock till commit.
         */
        table_close(pg_database_rel, NoLock);

        /*
         * Force synchronous commit, thus minimizing the window between
         * creation of the database files and committal of the transaction. If
         * we crash before committing, we'll have a DB that's taking up disk
         * space but is not in pg_database, which is not good.
         */
        ForceSyncCommit();
    }
    PG_END_ENSURE_ERROR_CLEANUP(createdb_failure_callback,
                                 PointerGetDatum(&fparms as *const createdb_failure_params as *const c_void));

    dboid
}

/* strcmp helper for *const c_char vs literal */
unsafe fn strcmp_rs(s: *const c_char, lit: &str) -> bool {
    if s.is_null() { return false; }
    core::ffi::CStr::from_ptr(s).to_bytes() == lit.as_bytes()
}

/* strcmp for two *const c_char pointers */
unsafe fn strcmp_ptr(a: *const c_char, b: *const c_char) -> c_int {
    libc::strcmp(a, b)
}

/* IsA check for Integer node */
unsafe fn pg_node_is_integer(node: *mut c_void) -> bool {
    !node.is_null() && *(node as *const c_int) == crate::nodes::nodes::NodeTag::T_Integer as c_int
}

// errcode constants referenced above  TODO(pg-port)
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 1;
const ERRCODE_UNDEFINED_OBJECT: c_int = 2;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 3;
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 4;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 5;
const ERRCODE_UNDEFINED_DATABASE: c_int = 6;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 7;
const ERRCODE_DUPLICATE_DATABASE: c_int = 8;
const ERRCODE_OBJECT_IN_USE: c_int = 9;
const ERRCODE_SYNTAX_ERROR: c_int = 10;
const ERRCODE_DUPLICATE_OBJECT: c_int = 11;

/* -------------------------------------------------------------------------
 * Continuation: helpers and remaining functions translated from
 * postgres/src/backend/commands/dbcommands.c (check_encoding_locale_matches
 * through dbase_redo).  Uses the same ereport!/errcode/errmsg shim convention
 * established by the createdb() translation above.
 * ------------------------------------------------------------------------- */

/*
 * Form_pg_database field accessors (opaque struct)  TODO(pg-port)
 */
unsafe fn pg_database_oid(form: Form_pg_database) -> Oid { (*form).oid }
unsafe fn pg_database_datdba(form: Form_pg_database) -> Oid { (*form).datdba }
unsafe fn pg_database_encoding(form: Form_pg_database) -> c_int { (*form).encoding }
unsafe fn pg_database_datistemplate(form: Form_pg_database) -> bool { (*form).datistemplate }
unsafe fn pg_database_dathasloginevt(form: Form_pg_database) -> bool { (*form).dathasloginevt }
unsafe fn pg_database_datallowconn(form: Form_pg_database) -> bool { (*form).datallowconn }
unsafe fn pg_database_datfrozenxid(form: Form_pg_database) -> TransactionId { (*form).datfrozenxid as _ }
unsafe fn pg_database_datminmxid(form: Form_pg_database) -> MultiXactId { (*form).datminmxid as _ }
unsafe fn pg_database_dattablespace(form: Form_pg_database) -> Oid { (*form).dattablespace }
unsafe fn pg_database_datlocprovider(form: Form_pg_database) -> c_char { (*form).datlocprovider }
unsafe fn pg_database_datname_ptr(form: Form_pg_database) -> *const c_void { &(*form).datname as *const _ as *const c_void }
unsafe fn pg_database_datconnlimit(form: Form_pg_database) -> c_int { (*form).datconnlimit }
/* Mutators for in-place / GETSTRUCT manipulation */
unsafe fn pg_database_set_datconnlimit(form: Form_pg_database, val: c_int) { (*form).datconnlimit = val; }
unsafe fn pg_database_datname_mut(form: Form_pg_database) -> *mut c_void { &mut (*form).datname as *mut _ as *mut c_void }

/*
 * Form_pg_authid field accessor (opaque struct)  TODO(pg-port)
 */
unsafe fn pg_authid_rolcreatedb(_form: Form_pg_authid) -> bool { unimplemented!() }

/* C library stat() wrapper  TODO(pg-port) */
extern "C" {
    fn stat(path: *const c_char, buf: *mut libc_stat) -> c_int;
}

/*
 * check_encoding_locale_matches
 *
 * Verify that the chosen encoding matches the requested LC_COLLATE/LC_CTYPE
 * settings.
 */
pub unsafe fn check_encoding_locale_matches(encoding: c_int, collate: *const c_char, ctype: *const c_char) {
    let ctype_encoding = pg_get_encoding_from_locale(ctype, true);
    let collate_encoding = pg_get_encoding_from_locale(collate, true);

    if !(ctype_encoding == encoding ||
         ctype_encoding == PG_SQL_ASCII ||
         ctype_encoding == -1 ||
         (encoding == PG_SQL_ASCII && superuser())) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errdetail(b"The chosen LC_CTYPE setting requires encoding \"%s\".\0".as_ptr() as *const c_char, pg_encoding_to_char(ctype_encoding)) */ errmsg!("encoding \"{}\" does not match locale \"{}\"", CStr::from_ptr(pg_encoding_to_char(encoding)).to_string_lossy(), CStr::from_ptr(ctype).to_string_lossy()));
    }

    if !(collate_encoding == encoding ||
         collate_encoding == PG_SQL_ASCII ||
         collate_encoding == -1 ||
         (encoding == PG_SQL_ASCII && superuser())) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); errdetail(b"The chosen LC_COLLATE setting requires encoding \"%s\".\0".as_ptr() as *const c_char, pg_encoding_to_char(collate_encoding)) */ errmsg!("encoding \"{}\" does not match locale \"{}\"", CStr::from_ptr(pg_encoding_to_char(encoding)).to_string_lossy(), CStr::from_ptr(collate).to_string_lossy()));
    }
}

/* Error cleanup callback for createdb */
unsafe extern "C" fn createdb_failure_callback(_code: c_int, arg: Datum) {
    let fparms = DatumGetPointer(arg) as *mut createdb_failure_params;

    /*
     * If we were copying database at block levels then drop pages for the
     * destination database that are in the shared buffer cache.  And tell
     * checkpointer to forget any pending fsync and unlink requests for files
     * in the database.  The reasoning behind doing this is same as explained
     * in dropdb function.  But unlike dropdb we don't need to call
     * pgstat_drop_database because this database is still not created so
     * there should not be any stat for this.
     */
    if (*fparms).strategy == CREATEDB_WAL_LOG {
        DropDatabaseBuffers((*fparms).dest_dboid);
        ForgetDatabaseSyncRequests((*fparms).dest_dboid);

        /* Release lock on the target database. */
        UnlockSharedObject(DatabaseRelationId, (*fparms).dest_dboid, 0,
                           AccessShareLock);
    }

    /*
     * Release lock on source database before doing recursive remove. This is
     * not essential but it seems desirable to release the lock as soon as
     * possible.
     */
    UnlockSharedObject(DatabaseRelationId, (*fparms).src_dboid, 0, ShareLock);

    /* Throw away any successfully copied subdirectories */
    remove_dbtablespaces((*fparms).dest_dboid);
}

/*
 * DROP DATABASE
 */
pub unsafe fn dropdb(dbname: *const c_char, missing_ok: bool, force: bool) {
    let db_id: Oid;
    let mut db_istemplate: bool = false;
    let pgdbrel: Relation;
    let tup: HeapTuple;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let mut inplace_state: *mut c_void = ptr::null_mut();
    let datform: Form_pg_database;
    let mut notherbackends: c_int = 0;
    let mut npreparedxacts: c_int = 0;
    let mut nslots: c_int = 0;
    let mut nslots_active: c_int = 0;
    let nsubscriptions: c_int;

    /*
     * Look up the target database's OID, and get exclusive lock on it. We
     * need this to ensure that no new backend starts up in the target
     * database while we are deleting it (see postinit.c), and that no one is
     * using it as a CREATE DATABASE template or trying to delete it for
     * themselves.
     */
    pgdbrel = table_open(DatabaseRelationId, RowExclusiveLock);

    let mut db_id_out: Oid = InvalidOid;
    if !get_db_info(dbname, AccessExclusiveLock, &mut db_id_out, ptr::null_mut(), ptr::null_mut(),
                    &mut db_istemplate, ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut()) {
        if !missing_ok {
            ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr(dbname).to_string_lossy()));
        } else {
            /* Close pg_database, release the lock, since we changed nothing */
            table_close(pgdbrel, RowExclusiveLock);
            ereport!(NOTICE, errmsg!("database \"{}\" does not exist, skipping", CStr::from_ptr(dbname).to_string_lossy()));
            return;
        }
    }
    db_id = db_id_out;

    /*
     * Permission checks
     */
    if !object_ownercheck(DatabaseRelationId, db_id, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname);
    }

    /* DROP hook for the database being removed */
    InvokeObjectDropHook(DatabaseRelationId, db_id, 0);

    /*
     * Disallow dropping a DB that is marked istemplate.  This is just to
     * prevent people from accidentally dropping template0 or template1; they
     * can do so if they're really determined ...
     */
    if db_istemplate {
        ereport!(ERROR, /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */ errmsg!("cannot drop a template database"));
    }

    /* Obviously can't drop my own database */
    if db_id == MyDatabaseId {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE) */ errmsg!("cannot drop the currently open database"));
    }

    /*
     * Check whether there are active logical slots that refer to the
     * to-be-dropped database. The database lock we are holding prevents the
     * creation of new slots using the database or existing slots becoming
     * active.
     */
    ReplicationSlotsCountDBSlots(db_id, &mut nslots, &mut nslots_active);
    if nslots_active != 0 {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE); errdetail_plural(b"There is %d active slot.\0".as_ptr() as *const c_char, b"There are %d active slots.\0".as_ptr() as *const c_char, nslots_active, nslots_active) */ errmsg!("database \"{}\" is used by an active logical replication slot", CStr::from_ptr(dbname).to_string_lossy()));
    }

    /*
     * Check if there are subscriptions defined in the target database.
     *
     * We can't drop them automatically because they might be holding
     * resources in other databases/instances.
     */
    nsubscriptions = CountDBSubscriptions(db_id);
    if nsubscriptions > 0 {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE); errdetail_plural(b"There is %d subscription.\0".as_ptr() as *const c_char, b"There are %d subscriptions.\0".as_ptr() as *const c_char, nsubscriptions, nsubscriptions) */ errmsg!("database \"{}\" is being used by logical replication subscription", CStr::from_ptr(dbname).to_string_lossy()));
    }

    /*
     * Attempt to terminate all existing connections to the target database if
     * the user has requested to do so.
     */
    if force {
        TerminateOtherDBBackends(db_id);
    }

    /*
     * Check for other backends in the target database.  (Because we hold the
     * database lock, no new ones can start after this.)
     *
     * As in CREATE DATABASE, check this after other error conditions.
     */
    if CountOtherDBBackends(db_id, &mut notherbackends, &mut npreparedxacts) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE); errdetail_busy_db(notherbackends, npreparedxacts) */ errmsg!("database \"{}\" is being accessed by other users", CStr::from_ptr(dbname).to_string_lossy()));
    }

    /*
     * Delete any comments or security labels associated with the database.
     */
    DeleteSharedComments(db_id, DatabaseRelationId);
    DeleteSharedSecurityLabel(db_id, DatabaseRelationId);

    /*
     * Remove settings associated with this database
     */
    DropSetting(db_id, InvalidOid);

    /*
     * Remove shared dependency references for the database.
     */
    dropDatabaseDependencies(db_id);

    /*
     * Tell the cumulative stats system to forget it immediately, too.
     */
    pgstat_drop_database(db_id);

    /*
     * Except for the deletion of the catalog row, subsequent actions are not
     * transactional (consider DropDatabaseBuffers() discarding modified
     * buffers). But we might crash or get interrupted below. To prevent
     * accesses to a database with invalid contents, mark the database as
     * invalid using an in-place update.
     *
     * We need to flush the WAL before continuing, to guarantee the
     * modification is durable before performing irreversible filesystem
     * operations.
     */
    ScanKeyInit(&mut scankey,
                Anum_pg_database_datname as c_int,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dbname));
    let mut tup_out: HeapTuple = ptr::null_mut();
    systable_inplace_update_begin(pgdbrel, DatabaseNameIndexId, true,
                                  ptr::null_mut(), 1, &mut scankey, &mut tup_out, &mut inplace_state);
    tup = tup_out;
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for database {}", db_id);
    }
    datform = GETSTRUCT(tup) as Form_pg_database;
    pg_database_set_datconnlimit(datform, DATCONNLIMIT_INVALID_DB);
    systable_inplace_update_finish(inplace_state, tup);
    XLogFlush(XactLastRecEnd);

    /*
     * Also delete the tuple - transactionally. If this transaction commits,
     * the row will be gone, but if we fail, dropdb() can be invoked again.
     */
    CatalogTupleDelete(pgdbrel, &mut (*tup).t_self);
    heap_freetuple(tup);

    /*
     * Drop db-specific replication slots.
     */
    ReplicationSlotsDropDBSlots(db_id);

    /*
     * Drop pages for this database that are in the shared buffer cache. This
     * is important to ensure that no remaining backend tries to write out a
     * dirty buffer to the dead database later...
     */
    DropDatabaseBuffers(db_id);

    /*
     * Tell checkpointer to forget any pending fsync and unlink requests for
     * files in the database; else the fsyncs will fail at next checkpoint, or
     * worse, it will delete files that belong to a newly created database
     * with the same OID.
     */
    ForgetDatabaseSyncRequests(db_id);

    /*
     * Force a checkpoint to make sure the checkpointer has received the
     * message sent by ForgetDatabaseSyncRequests.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /* Close all smgr fds in all backends. */
    WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

    /*
     * Remove all tablespace subdirs belonging to the database.
     */
    remove_dbtablespaces(db_id);

    /*
     * Close pg_database, but keep lock till commit.
     */
    table_close(pgdbrel, NoLock);

    /*
     * Force synchronous commit, thus minimizing the window between removal of
     * the database files and committal of the transaction. If we crash before
     * committing, we'll have a DB that's gone on disk but still there
     * according to pg_database, which is not good.
     */
    ForceSyncCommit();
}

/*
 * Rename database
 */
pub unsafe fn RenameDatabase(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let db_id: Oid;
    let newtup: HeapTuple;
    let mut otid: ItemPointerData = core::mem::zeroed();
    let rel: Relation;
    let mut notherbackends: c_int = 0;
    let mut npreparedxacts: c_int = 0;
    let mut address: ObjectAddress = core::mem::zeroed();

    /*
     * Look up the target database's OID, and get exclusive lock on it. We
     * need this for the same reasons as DROP DATABASE.
     */
    rel = table_open(DatabaseRelationId, RowExclusiveLock);

    let mut db_id_out: Oid = InvalidOid;
    if !get_db_info(oldname, AccessExclusiveLock, &mut db_id_out, ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut()) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr(oldname).to_string_lossy()));
    }
    db_id = db_id_out;

    /* must be owner */
    if !object_ownercheck(DatabaseRelationId, db_id, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, oldname);
    }

    /* must have createdb rights */
    if !have_createdb_privilege() {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */ errmsg!("permission denied to rename database"));
    }

    /*
     * Make sure the new name doesn't exist.  See notes for same error in
     * CREATE DATABASE.
     */
    if OidIsValid(get_database_oid(newname, true)) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_DUPLICATE_DATABASE) */ errmsg!("database \"{}\" already exists", CStr::from_ptr(newname).to_string_lossy()));
    }

    /*
     * XXX Client applications probably store the current database somewhere,
     * so renaming it could cause confusion.  On the other hand, there may not
     * be an actual problem besides a little confusion, so think about this
     * and decide.
     */
    if db_id == MyDatabaseId {
        ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */ errmsg!("current database cannot be renamed"));
    }

    /*
     * Make sure the database does not have active sessions.  This is the same
     * concern as above, but applied to other sessions.
     *
     * As in CREATE DATABASE, check this after other error conditions.
     */
    if CountOtherDBBackends(db_id, &mut notherbackends, &mut npreparedxacts) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE); errdetail_busy_db(notherbackends, npreparedxacts) */ errmsg!("database \"{}\" is being accessed by other users", CStr::from_ptr(oldname).to_string_lossy()));
    }

    /* rename */
    newtup = SearchSysCacheLockedCopy1(DATABASEOID, ObjectIdGetDatum(db_id));
    if !HeapTupleIsValid(newtup) {
        elog!(ERROR, "cache lookup failed for database {}", db_id);
    }
    otid = (*newtup).t_self;
    namestrcpy(pg_database_datname_mut(GETSTRUCT(newtup) as Form_pg_database), newname);
    CatalogTupleUpdate(rel, &mut otid, newtup);
    UnlockTuple(rel, &mut otid, InplaceUpdateTupleLock);

    InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

    ObjectAddressSet(&mut address, DatabaseRelationId, db_id);

    /*
     * Close pg_database, but keep lock till commit.
     */
    table_close(rel, NoLock);

    address
}

/* strcmp helper for dirent d_name vs literal "." / ".."  TODO(pg-port) */
unsafe fn dirent_name_is(d_name: *const c_char, lit: &str) -> bool {
    strcmp_rs(d_name, lit)
}

/*
 * ALTER DATABASE SET TABLESPACE
 */
unsafe fn movedb(dbname: *const c_char, tblspcname: *const c_char) {
    let db_id: Oid;
    let pgdbrel: Relation;
    let mut notherbackends: c_int = 0;
    let mut npreparedxacts: c_int = 0;
    let oldtuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut src_tblspcoid: Oid = InvalidOid;
    let dst_tblspcoid: Oid;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let sysscan: SysScanDesc;
    let aclresult: AclResult;
    let src_dbpath: *mut c_char;
    let dst_dbpath: *mut c_char;
    let dstdir: *mut DIR;
    let mut xlde: *mut dirent;
    let mut fparms: movedb_failure_params = movedb_failure_params { dest_dboid: 0, dest_tsoid: 0 };

    /*
     * Look up the target database's OID, and get exclusive lock on it. We
     * need this to ensure that no new backend starts up in the database while
     * we are moving it, and that no one is using it as a CREATE DATABASE
     * template or trying to delete it.
     */
    pgdbrel = table_open(DatabaseRelationId, RowExclusiveLock);

    let mut db_id_out: Oid = InvalidOid;
    if !get_db_info(dbname, AccessExclusiveLock, &mut db_id_out, ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), &mut src_tblspcoid, ptr::null_mut(), ptr::null_mut(),
                    ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut()) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr(dbname).to_string_lossy()));
    }
    db_id = db_id_out;

    /*
     * We actually need a session lock, so that the lock will persist across
     * the commit/restart below.  (We could almost get away with letting the
     * lock be released at commit, except that someone could try to move
     * relations of the DB back into the old directory while we rmtree() it.)
     */
    LockSharedObjectForSession(DatabaseRelationId, db_id, 0,
                               AccessExclusiveLock);

    /*
     * Permission checks
     */
    if !object_ownercheck(DatabaseRelationId, db_id, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname);
    }

    /*
     * Obviously can't move the tables of my own database
     */
    if db_id == MyDatabaseId {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE) */ errmsg!("cannot change the tablespace of the currently open database"));
    }

    /*
     * Get tablespace's oid
     */
    dst_tblspcoid = get_tablespace_oid(tblspcname, false);

    /*
     * Permission checks
     */
    aclresult = object_aclcheck(TableSpaceRelationId, dst_tblspcoid, GetUserId(),
                                ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_TABLESPACE, tblspcname);
    }

    /*
     * pg_global must never be the default tablespace
     */
    if dst_tblspcoid == GLOBALTABLESPACE_OID {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("pg_global cannot be used as default tablespace"));
    }

    /*
     * No-op if same tablespace
     */
    if src_tblspcoid == dst_tblspcoid {
        table_close(pgdbrel, NoLock);
        UnlockSharedObjectForSession(DatabaseRelationId, db_id, 0,
                                     AccessExclusiveLock);
        return;
    }

    /*
     * Check for other backends in the target database.  (Because we hold the
     * database lock, no new ones can start after this.)
     *
     * As in CREATE DATABASE, check this after other error conditions.
     */
    if CountOtherDBBackends(db_id, &mut notherbackends, &mut npreparedxacts) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE); errdetail_busy_db(notherbackends, npreparedxacts) */ errmsg!("database \"{}\" is being accessed by other users", CStr::from_ptr(dbname).to_string_lossy()));
    }

    /*
     * Get old and new database paths
     */
    src_dbpath = GetDatabasePath(db_id, src_tblspcoid);
    dst_dbpath = GetDatabasePath(db_id, dst_tblspcoid);

    /*
     * Force a checkpoint before proceeding. This will force all dirty
     * buffers, including those of unlogged tables, out to disk, to ensure
     * source database is up-to-date on disk for the copy.
     * FlushDatabaseBuffers() would suffice for that, but we also want to
     * process any pending unlink requests. Otherwise, the check for existing
     * files in the target directory might fail unnecessarily, not to mention
     * that the copy might fail due to source files getting deleted under it.
     * On Windows, this also ensures that background procs don't hold any open
     * files, which would cause rmdir() to fail.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT
                      | CHECKPOINT_FLUSH_ALL);

    /* Close all smgr fds in all backends. */
    WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

    /*
     * Now drop all buffers holding data of the target database; they should
     * no longer be dirty so DropDatabaseBuffers is safe.
     *
     * It might seem that we could just let these buffers age out of shared
     * buffers naturally, since they should not get referenced anymore.  The
     * problem with that is that if the user later moves the database back to
     * its original tablespace, any still-surviving buffers would appear to
     * contain valid data again --- but they'd be missing any changes made in
     * the database while it was in the new tablespace.  In any case, freeing
     * buffers that should never be used again seems worth the cycles.
     *
     * Note: it'd be sufficient to get rid of buffers matching db_id and
     * src_tblspcoid, but bufmgr.c presently provides no API for that.
     */
    DropDatabaseBuffers(db_id);

    /*
     * Check for existence of files in the target directory, i.e., objects of
     * this database that are already in the target tablespace.  We can't
     * allow the move in such a case, because we would need to change those
     * relations' pg_class.reltablespace entries to zero, and we don't have
     * access to the DB's pg_class to do so.
     */
    dstdir = AllocateDir(dst_dbpath);
    if !dstdir.is_null() {
        loop {
            xlde = ReadDir(dstdir, dst_dbpath);
            if xlde.is_null() {
                break;
            }
            if dirent_name_is((*xlde).d_name.as_ptr(), ".") ||
               dirent_name_is((*xlde).d_name.as_ptr(), "..") {
                continue;
            }

            ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE); errhint(b"You must move them back to the database's default tablespace before using this command.\0".as_ptr() as *const c_char) */ errmsg!("some relations of database \"{}\" are already in tablespace \"{}\"", CStr::from_ptr(dbname).to_string_lossy(), CStr::from_ptr(tblspcname).to_string_lossy()));
        }

        FreeDir(dstdir);

        /*
         * The directory exists but is empty. We must remove it before using
         * the copydir function.
         */
        if rmdir(dst_dbpath) != 0 {
            elog!(ERROR, "could not remove directory \"{}\": %m", 0);
        }
    }

    /*
     * Use an ENSURE block to make sure we remove the debris if the copy fails
     * (eg, due to out-of-disk-space).  This is not a 100% solution, because
     * of the possibility of failure during transaction commit, but it should
     * handle most scenarios.
     */
    fparms.dest_dboid = db_id;
    fparms.dest_tsoid = dst_tblspcoid;
    PG_ENSURE_ERROR_CLEANUP(movedb_failure_callback,
                            PointerGetDatum(&fparms as *const movedb_failure_params as *const c_void));
    {
        let mut new_record: [Datum; Natts_pg_database] = [0; Natts_pg_database];
        let mut new_record_nulls: [bool; Natts_pg_database] = [false; Natts_pg_database];
        let mut new_record_repl: [bool; Natts_pg_database] = [false; Natts_pg_database];

        /*
         * Copy files from the old tablespace to the new one
         */
        copydir(src_dbpath, dst_dbpath, false);

        /*
         * Record the filesystem change in XLOG
         */
        {
            let xlrec = xl_dbase_create_file_copy_rec {
                db_id,
                tablespace_id: dst_tblspcoid,
                src_db_id: db_id,
                src_tablespace_id: src_tblspcoid,
            };

            XLogBeginInsert();
            XLogRegisterData(&xlrec as *const xl_dbase_create_file_copy_rec as *const c_void,
                             std::mem::size_of::<xl_dbase_create_file_copy_rec>());

            let _ = XLogInsert(RM_DBASE_ID,
                               XLOG_DBASE_CREATE_FILE_COPY | XLR_SPECIAL_REL_UPDATE);
        }

        /*
         * Update the database's pg_database tuple
         */
        ScanKeyInit(&mut scankey,
                    Anum_pg_database_datname as c_int,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(dbname));
        sysscan = systable_beginscan(pgdbrel, DatabaseNameIndexId, true,
                                     ptr::null_mut(), 1, &mut scankey);
        oldtuple = systable_getnext(sysscan);
        if !HeapTupleIsValid(oldtuple) {
            /* shouldn't happen... */
            ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr(dbname).to_string_lossy()));
        }
        LockTuple(pgdbrel, &mut (*oldtuple).t_self, InplaceUpdateTupleLock);

        new_record[Anum_pg_database_dattablespace - 1] = ObjectIdGetDatum(dst_tblspcoid);
        new_record_repl[Anum_pg_database_dattablespace - 1] = true;

        newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(pgdbrel),
                                     new_record.as_mut_ptr(),
                                     new_record_nulls.as_mut_ptr(), new_record_repl.as_mut_ptr());
        CatalogTupleUpdate(pgdbrel, &mut (*oldtuple).t_self, newtuple);
        UnlockTuple(pgdbrel, &mut (*oldtuple).t_self, InplaceUpdateTupleLock);

        InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

        systable_endscan(sysscan);

        /*
         * Force another checkpoint here.  As in CREATE DATABASE, this is to
         * ensure that we don't have to replay a committed
         * XLOG_DBASE_CREATE_FILE_COPY operation, which would cause us to lose
         * any unlogged operations done in the new DB tablespace before the
         * next checkpoint.
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

        /*
         * Force synchronous commit, thus minimizing the window between
         * copying the database files and committal of the transaction. If we
         * crash before committing, we'll leave an orphaned set of files on
         * disk, which is not fatal but not good either.
         */
        ForceSyncCommit();

        /*
         * Close pg_database, but keep lock till commit.
         */
        table_close(pgdbrel, NoLock);
    }
    PG_END_ENSURE_ERROR_CLEANUP(movedb_failure_callback,
                                PointerGetDatum(&fparms as *const movedb_failure_params as *const c_void));

    /*
     * Commit the transaction so that the pg_database update is committed. If
     * we crash while removing files, the database won't be corrupt, we'll
     * just leave some orphaned files in the old directory.
     *
     * (This is OK because we know we aren't inside a transaction block.)
     *
     * XXX would it be safe/better to do this inside the ensure block?	Not
     * convinced it's a good idea; consider elog just after the transaction
     * really commits.
     */
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Start new transaction for the remaining work; don't need a snapshot */
    StartTransactionCommand();

    /*
     * Remove files from the old tablespace
     */
    if !rmtree(src_dbpath, true) {
        ereport!(WARNING, errmsg!("some useless files may be left behind in old database directory \"{}\"", CStr::from_ptr(src_dbpath).to_string_lossy()));
    }

    /*
     * Record the filesystem change in XLOG
     */
    {
        let xlrec = xl_dbase_drop_rec {
            db_id,
            ntablespaces: 1,
            tablespace_ids: [],
        };

        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_dbase_drop_rec as *const c_void,
                         std::mem::size_of::<xl_dbase_drop_rec>());
        XLogRegisterData(&src_tblspcoid as *const Oid as *const c_void,
                         std::mem::size_of::<Oid>());

        let _ = XLogInsert(RM_DBASE_ID,
                           XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);
    }

    /* Now it's safe to release the database lock */
    UnlockSharedObjectForSession(DatabaseRelationId, db_id, 0,
                                 AccessExclusiveLock);

    pfree(src_dbpath as *mut c_void);
    pfree(dst_dbpath as *mut c_void);
}

/* Error cleanup callback for movedb */
unsafe extern "C" fn movedb_failure_callback(_code: c_int, arg: Datum) {
    let fparms = DatumGetPointer(arg) as *mut movedb_failure_params;
    let dstpath: *mut c_char;

    /* Get rid of anything we managed to copy to the target directory */
    dstpath = GetDatabasePath((*fparms).dest_dboid, (*fparms).dest_tsoid);

    rmtree(dstpath, true);

    pfree(dstpath as *mut c_void);
}

/*
 * Process options and call dropdb function.
 */
pub unsafe fn DropDatabase(pstate: *mut ParseState, stmt: *mut DropdbStmt) {
    let mut force: bool = false;

    foreach!(lc, (*stmt).options, {
        let opt = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut DefElem;

        if strcmp_rs((*opt).defname, "force") {
            force = true;
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_SYNTAX_ERROR); parser_errposition(pstate, (*opt).location) */ errmsg!("unrecognized {} option \"{}\"", CStr::from_ptr(b"DROP DATABASE\0".as_ptr() as *const c_char).to_string_lossy(), CStr::from_ptr((*opt).defname).to_string_lossy()));
        }
    });

    dropdb((*stmt).dbname, (*stmt).missing_ok, force);
}

/*
 * ALTER DATABASE name ...
 */
pub unsafe fn AlterDatabase(pstate: *mut ParseState, stmt: *mut AlterDatabaseStmt, isTopLevel: bool) -> Oid {
    let rel: Relation;
    let dboid: Oid;
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let datform: Form_pg_database;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut dbistemplate: bool = false;
    let mut dballowconnections: bool = true;
    let mut dbconnlimit: c_int = DATCONNLIMIT_UNLIMITED;
    let mut distemplate: *mut DefElem = ptr::null_mut();
    let mut dallowconnections: *mut DefElem = ptr::null_mut();
    let mut dconnlimit: *mut DefElem = ptr::null_mut();
    let mut dtablespace: *mut DefElem = ptr::null_mut();
    let mut new_record: [Datum; Natts_pg_database] = [0; Natts_pg_database];
    let mut new_record_nulls: [bool; Natts_pg_database] = [false; Natts_pg_database];
    let mut new_record_repl: [bool; Natts_pg_database] = [false; Natts_pg_database];

    /* Extract options from the statement node tree */
    foreach!(option, (*stmt).options, {
        let defel = crate::nodes::pg_list::lfirst(current_cell!(option)) as *mut DefElem;

        if strcmp_rs((*defel).defname, "is_template") {
            if !distemplate.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            distemplate = defel;
        } else if strcmp_rs((*defel).defname, "allow_connections") {
            if !dallowconnections.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dallowconnections = defel;
        } else if strcmp_rs((*defel).defname, "connection_limit") {
            if !dconnlimit.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dconnlimit = defel;
        } else if strcmp_rs((*defel).defname, "tablespace") {
            if !dtablespace.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dtablespace = defel;
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_SYNTAX_ERROR); parser_errposition(pstate, (*defel).location) */ errmsg!("option \"{}\" not recognized", CStr::from_ptr((*defel).defname).to_string_lossy()));
        }
    });

    if !dtablespace.is_null() {
        /*
         * While the SET TABLESPACE syntax doesn't allow any other options,
         * somebody could write "WITH TABLESPACE ...".  Forbid any other
         * options from being specified in that case.
         */
        if list_length((*stmt).options) != 1 {
            ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); parser_errposition(pstate, (*dtablespace).location) */ errmsg!("option \"{}\" cannot be specified with other options", CStr::from_ptr((*dtablespace).defname).to_string_lossy()));
        }
        /* this case isn't allowed within a transaction block */
        PreventInTransactionBlock(isTopLevel, b"ALTER DATABASE SET TABLESPACE\0".as_ptr() as *const c_char);
        movedb((*stmt).dbname, defGetString(dtablespace));
        return InvalidOid;
    }

    if !distemplate.is_null() && !(*distemplate).arg.is_null() {
        dbistemplate = defGetBoolean(distemplate);
    }
    if !dallowconnections.is_null() && !(*dallowconnections).arg.is_null() {
        dballowconnections = defGetBoolean(dallowconnections);
    }
    if !dconnlimit.is_null() && !(*dconnlimit).arg.is_null() {
        dbconnlimit = defGetInt32(dconnlimit);
        if dbconnlimit < DATCONNLIMIT_UNLIMITED {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("invalid connection limit: {}", dbconnlimit));
        }
    }

    /*
     * Get the old tuple.  We don't need a lock on the database per se,
     * because we're not going to do anything that would mess up incoming
     * connections.
     */
    rel = table_open(DatabaseRelationId, RowExclusiveLock);
    ScanKeyInit(&mut scankey,
                Anum_pg_database_datname as c_int,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum((*stmt).dbname));
    scan = systable_beginscan(rel, DatabaseNameIndexId, true,
                              ptr::null_mut(), 1, &mut scankey);
    tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr((*stmt).dbname).to_string_lossy()));
    }
    LockTuple(rel, &mut (*tuple).t_self, InplaceUpdateTupleLock);

    datform = GETSTRUCT(tuple) as Form_pg_database;
    dboid = pg_database_oid(datform);

    if database_is_invalid_form(datform) {
        ereport!(FATAL, /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE); errhint(b"Use DROP DATABASE to drop invalid databases.\0".as_ptr() as *const c_char) */ errmsg!("cannot alter invalid database \"{}\"", CStr::from_ptr((*stmt).dbname).to_string_lossy()));
    }

    if !object_ownercheck(DatabaseRelationId, dboid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, (*stmt).dbname);
    }

    /*
     * In order to avoid getting locked out and having to go through
     * standalone mode, we refuse to disallow connections to the database
     * we're currently connected to.  Lockout can still happen with concurrent
     * sessions but the likeliness of that is not high enough to worry about.
     */
    if !dballowconnections && dboid == MyDatabaseId {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("cannot disallow connections for current database"));
    }

    /*
     * Build an updated tuple, perusing the information just obtained
     */
    if !distemplate.is_null() {
        new_record[Anum_pg_database_datistemplate - 1] = BoolGetDatum(dbistemplate);
        new_record_repl[Anum_pg_database_datistemplate - 1] = true;
    }
    if !dallowconnections.is_null() {
        new_record[Anum_pg_database_datallowconn - 1] = BoolGetDatum(dballowconnections);
        new_record_repl[Anum_pg_database_datallowconn - 1] = true;
    }
    if !dconnlimit.is_null() {
        new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(dbconnlimit);
        new_record_repl[Anum_pg_database_datconnlimit - 1] = true;
    }

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), new_record.as_mut_ptr(),
                                 new_record_nulls.as_mut_ptr(), new_record_repl.as_mut_ptr());
    CatalogTupleUpdate(rel, &mut (*tuple).t_self, newtuple);
    UnlockTuple(rel, &mut (*tuple).t_self, InplaceUpdateTupleLock);

    InvokeObjectPostAlterHook(DatabaseRelationId, dboid, 0);

    systable_endscan(scan);

    /* Close pg_database, but keep lock till commit */
    table_close(rel, NoLock);

    dboid
}

/*
 * ALTER DATABASE name REFRESH COLLATION VERSION
 */
pub unsafe fn AlterDatabaseRefreshColl(stmt: *mut AlterDatabaseRefreshCollStmt) -> ObjectAddress {
    let rel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let db_id: Oid;
    let tuple: HeapTuple;
    let datForm: Form_pg_database;
    let mut address: ObjectAddress = core::mem::zeroed();
    let mut datum: Datum;
    let mut isnull: bool = false;
    let oldversion: *mut c_char;
    let newversion: *mut c_char;

    rel = table_open(DatabaseRelationId, RowExclusiveLock);
    ScanKeyInit(&mut scankey,
                Anum_pg_database_datname as c_int,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum((*stmt).dbname));
    scan = systable_beginscan(rel, DatabaseNameIndexId, true,
                              ptr::null_mut(), 1, &mut scankey);
    tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr((*stmt).dbname).to_string_lossy()));
    }

    datForm = GETSTRUCT(tuple) as Form_pg_database;
    db_id = pg_database_oid(datForm);

    if !object_ownercheck(DatabaseRelationId, db_id, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, (*stmt).dbname);
    }
    LockTuple(rel, &mut (*tuple).t_self, InplaceUpdateTupleLock);

    datum = heap_getattr(tuple, Anum_pg_database_datcollversion as c_int, RelationGetDescr(rel), &mut isnull);
    oldversion = if isnull { ptr::null_mut() } else { TextDatumGetCString(datum) };

    if pg_database_datlocprovider(datForm) == COLLPROVIDER_LIBC {
        datum = heap_getattr(tuple, Anum_pg_database_datcollate as c_int, RelationGetDescr(rel), &mut isnull);
        if isnull {
            elog!(ERROR, "unexpected null in pg_database");
        }
    } else {
        datum = heap_getattr(tuple, Anum_pg_database_datlocale as c_int, RelationGetDescr(rel), &mut isnull);
        if isnull {
            elog!(ERROR, "unexpected null in pg_database");
        }
    }

    newversion = get_collation_actual_version(pg_database_datlocprovider(datForm),
                                              TextDatumGetCString(datum));

    /* cannot change from NULL to non-NULL or vice versa */
    if (oldversion.is_null() && !newversion.is_null()) || (!oldversion.is_null() && newversion.is_null()) {
        elog!(ERROR, "invalid collation version change");
    } else if !oldversion.is_null() && !newversion.is_null() && strcmp_ptr(newversion, oldversion) != 0 {
        let mut nulls: [bool; Natts_pg_database] = [false; Natts_pg_database];
        let mut replaces: [bool; Natts_pg_database] = [false; Natts_pg_database];
        let mut values: [Datum; Natts_pg_database] = [0; Natts_pg_database];
        let newtuple: HeapTuple;

        ereport!(NOTICE, errmsg!("changing version from {} to {}", CStr::from_ptr(oldversion).to_string_lossy(), CStr::from_ptr(newversion).to_string_lossy()));

        values[Anum_pg_database_datcollversion - 1] = CStringGetTextDatum(newversion);
        replaces[Anum_pg_database_datcollversion - 1] = true;

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
                                     values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr());
        CatalogTupleUpdate(rel, &mut (*tuple).t_self, newtuple);
        heap_freetuple(newtuple);
    } else {
        ereport!(NOTICE, errmsg!("version has not changed"));
    }
    UnlockTuple(rel, &mut (*tuple).t_self, InplaceUpdateTupleLock);

    InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

    ObjectAddressSet(&mut address, DatabaseRelationId, db_id);

    systable_endscan(scan);

    table_close(rel, NoLock);

    address
}

/*
 * ALTER DATABASE name SET ...
 */
pub unsafe fn AlterDatabaseSet(stmt: *mut AlterDatabaseSetStmt) -> Oid {
    let datid: Oid = get_database_oid((*stmt).dbname, false);

    /*
     * Obtain a lock on the database and make sure it didn't go away in the
     * meantime.
     */
    shdepLockAndCheckObject(DatabaseRelationId, datid);

    if !object_ownercheck(DatabaseRelationId, datid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, (*stmt).dbname);
    }

    AlterSetting(datid, InvalidOid, (*stmt).setstmt as *mut c_void);

    UnlockSharedObject(DatabaseRelationId, datid, 0, AccessShareLock);

    datid
}

/*
 * ALTER DATABASE name OWNER TO newowner
 */
pub unsafe fn AlterDatabaseOwner(dbname: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let db_id: Oid;
    let tuple: HeapTuple;
    let rel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let datForm: Form_pg_database;
    let mut address: ObjectAddress = core::mem::zeroed();

    /*
     * Get the old tuple.  We don't need a lock on the database per se,
     * because we're not going to do anything that would mess up incoming
     * connections.
     */
    rel = table_open(DatabaseRelationId, RowExclusiveLock);
    ScanKeyInit(&mut scankey,
                Anum_pg_database_datname as c_int,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dbname));
    scan = systable_beginscan(rel, DatabaseNameIndexId, true,
                              ptr::null_mut(), 1, &mut scankey);
    tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr(dbname).to_string_lossy()));
    }

    datForm = GETSTRUCT(tuple) as Form_pg_database;
    db_id = pg_database_oid(datForm);

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is to be consistent with other
     * objects.
     */
    if pg_database_datdba(datForm) != newOwnerId {
        let mut repl_val: [Datum; Natts_pg_database] = [0; Natts_pg_database];
        let mut repl_null: [bool; Natts_pg_database] = [false; Natts_pg_database];
        let mut repl_repl: [bool; Natts_pg_database] = [false; Natts_pg_database];
        let newAcl: *mut Acl;
        let aclDatum: Datum;
        let mut isNull: bool = false;
        let newtuple: HeapTuple;

        /* Otherwise, must be owner of the existing object */
        if !object_ownercheck(DatabaseRelationId, db_id, GetUserId()) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname);
        }

        /* Must be able to become new owner */
        check_can_set_role(GetUserId(), newOwnerId);

        /*
         * must have createdb rights
         *
         * NOTE: This is different from other alter-owner checks in that the
         * current user is checked for createdb privileges instead of the
         * destination owner.  This is consistent with the CREATE case for
         * databases.  Because superusers will always have this right, we need
         * no special case for them.
         */
        if !have_createdb_privilege() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */ errmsg!("permission denied to change owner of database"));
        }

        LockTuple(rel, &mut (*tuple).t_self, InplaceUpdateTupleLock);

        repl_repl[Anum_pg_database_datdba - 1] = true;
        repl_val[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = heap_getattr(tuple,
                                Anum_pg_database_datacl as c_int,
                                RelationGetDescr(rel),
                                &mut isNull);
        if !isNull {
            newAcl = aclnewowner(DatumGetAclP(aclDatum),
                                 pg_database_datdba(datForm), newOwnerId);
            repl_repl[Anum_pg_database_datacl - 1] = true;
            repl_val[Anum_pg_database_datacl - 1] = PointerGetDatum(newAcl as *const c_void);
        }

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());
        CatalogTupleUpdate(rel, &mut (*newtuple).t_self, newtuple);
        UnlockTuple(rel, &mut (*tuple).t_self, InplaceUpdateTupleLock);

        heap_freetuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(DatabaseRelationId, db_id, newOwnerId);
    }

    InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

    ObjectAddressSet(&mut address, DatabaseRelationId, db_id);

    systable_endscan(scan);

    /* Close pg_database, but keep lock till commit */
    table_close(rel, NoLock);

    address
}

/*
 * pg_database_collation_actual_version
 *
 * SQL-callable: return the actual collation version of a database's locale.
 */
pub unsafe fn pg_database_collation_actual_version(_fcinfo: *mut c_void) -> Datum {
    let dbid: Oid = PG_GETARG_OID(0);
    let tp: HeapTuple;
    let datlocprovider: c_char;
    let datum: Datum;
    let version: *mut c_char;

    tp = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
    if !HeapTupleIsValid(tp) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */ errmsg!("database with OID {} does not exist", dbid));
    }

    datlocprovider = pg_database_datlocprovider(GETSTRUCT(tp) as Form_pg_database);

    if datlocprovider == COLLPROVIDER_LIBC {
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datcollate as c_int);
    } else {
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datlocale as c_int);
    }

    version = get_collation_actual_version(datlocprovider,
                                           TextDatumGetCString(datum));

    ReleaseSysCache(tp);

    if !version.is_null() {
        PG_RETURN_TEXT_P(cstring_to_text(version))
    } else {
        PG_RETURN_NULL()
    }
}

/*
 * Helper functions
 */

/*
 * Look up info about the database named "name".  If the database exists,
 * obtain the specified lock type on it, fill in any of the remaining
 * parameters that aren't NULL, and return true.  If no such database,
 * return false.
 */
pub unsafe fn get_db_info(name: *const c_char, lockmode: LOCKMODE,
                          dbIdP: *mut Oid, ownerIdP: *mut Oid,
                          encodingP: *mut c_int, dbIsTemplateP: *mut bool, dbAllowConnP: *mut bool,
                          dbHasLoginEvtP: *mut bool,
                          dbFrozenXidP: *mut TransactionId, dbMinMultiP: *mut MultiXactId,
                          dbTablespace: *mut Oid, dbCollate: *mut *mut c_char,
                          dbCtype: *mut *mut c_char, dbLocale: *mut *mut c_char,
                          dbIcurules: *mut *mut c_char,
                          dbLocProvider: *mut c_char,
                          dbCollversion: *mut *mut c_char) -> bool {
    let mut result: bool = false;
    let relation: Relation;

    // Assert(name)

    /* Caller may wish to grab a better lock on pg_database beforehand... */
    relation = table_open(DatabaseRelationId, AccessShareLock);

    /*
     * Loop covers the rare case where the database is renamed before we can
     * lock it.  We try again just in case we can find a new one of the same
     * name.
     */
    loop {
        let mut scanKey: ScanKeyData = core::mem::zeroed();
        let scan: SysScanDesc;
        let mut tuple: HeapTuple;
        let dbOid: Oid;

        /*
         * there's no syscache for database-indexed-by-name, so must do it the
         * hard way
         */
        ScanKeyInit(&mut scanKey,
                    Anum_pg_database_datname as c_int,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(name));

        scan = systable_beginscan(relation, DatabaseNameIndexId, true,
                                  ptr::null_mut(), 1, &mut scanKey);

        tuple = systable_getnext(scan);

        if !HeapTupleIsValid(tuple) {
            /* definitely no database of that name */
            systable_endscan(scan);
            break;
        }

        dbOid = pg_database_oid(GETSTRUCT(tuple) as Form_pg_database);

        systable_endscan(scan);

        /*
         * Now that we have a database OID, we can try to lock the DB.
         */
        if lockmode != NoLock {
            LockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);
        }

        /*
         * And now, re-fetch the tuple by OID.  If it's still there and still
         * the same name, we win; else, drop the lock and loop back to try
         * again.
         */
        tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbOid));
        if HeapTupleIsValid(tuple) {
            let dbform: Form_pg_database = GETSTRUCT(tuple) as Form_pg_database;

            if strcmp_ptr(name, NameStr(pg_database_datname_ptr(dbform))) == 0 {
                let datum: Datum;
                let mut isnull: bool = false;

                /* oid of the database */
                if !dbIdP.is_null() {
                    *dbIdP = dbOid;
                }
                /* oid of the owner */
                if !ownerIdP.is_null() {
                    *ownerIdP = pg_database_datdba(dbform);
                }
                /* character encoding */
                if !encodingP.is_null() {
                    *encodingP = pg_database_encoding(dbform);
                }
                /* allowed as template? */
                if !dbIsTemplateP.is_null() {
                    *dbIsTemplateP = pg_database_datistemplate(dbform);
                }
                /* Has on login event trigger? */
                if !dbHasLoginEvtP.is_null() {
                    *dbHasLoginEvtP = pg_database_dathasloginevt(dbform);
                }
                /* allowing connections? */
                if !dbAllowConnP.is_null() {
                    *dbAllowConnP = pg_database_datallowconn(dbform);
                }
                /* limit of frozen XIDs */
                if !dbFrozenXidP.is_null() {
                    *dbFrozenXidP = pg_database_datfrozenxid(dbform);
                }
                /* minimum MultiXactId */
                if !dbMinMultiP.is_null() {
                    *dbMinMultiP = pg_database_datminmxid(dbform);
                }
                /* default tablespace for this database */
                if !dbTablespace.is_null() {
                    *dbTablespace = pg_database_dattablespace(dbform);
                }
                /* default locale settings for this database */
                if !dbLocProvider.is_null() {
                    *dbLocProvider = pg_database_datlocprovider(dbform);
                }
                if !dbCollate.is_null() {
                    let d = SysCacheGetAttrNotNull(DATABASEOID, tuple, Anum_pg_database_datcollate as c_int);
                    *dbCollate = TextDatumGetCString(d);
                }
                if !dbCtype.is_null() {
                    let d = SysCacheGetAttrNotNull(DATABASEOID, tuple, Anum_pg_database_datctype as c_int);
                    *dbCtype = TextDatumGetCString(d);
                }
                if !dbLocale.is_null() {
                    let d = SysCacheGetAttr(DATABASEOID, tuple, Anum_pg_database_datlocale as c_int, &mut isnull);
                    if isnull {
                        *dbLocale = ptr::null_mut();
                    } else {
                        *dbLocale = TextDatumGetCString(d);
                    }
                }
                if !dbIcurules.is_null() {
                    let d = SysCacheGetAttr(DATABASEOID, tuple, Anum_pg_database_daticurules as c_int, &mut isnull);
                    if isnull {
                        *dbIcurules = ptr::null_mut();
                    } else {
                        *dbIcurules = TextDatumGetCString(d);
                    }
                }
                if !dbCollversion.is_null() {
                    datum = SysCacheGetAttr(DATABASEOID, tuple, Anum_pg_database_datcollversion as c_int, &mut isnull);
                    if isnull {
                        *dbCollversion = ptr::null_mut();
                    } else {
                        *dbCollversion = TextDatumGetCString(datum);
                    }
                }
                ReleaseSysCache(tuple);
                result = true;
                break;
            }
            /* can only get here if it was just renamed */
            ReleaseSysCache(tuple);
        }

        if lockmode != NoLock {
            UnlockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);
        }
    }

    table_close(relation, AccessShareLock);

    result
}

/* Check if current user has createdb privileges */
pub unsafe fn have_createdb_privilege() -> bool {
    let mut result: bool = false;
    let utup: HeapTuple;

    /* Superusers can always do everything */
    if superuser() {
        return true;
    }

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
    if HeapTupleIsValid(utup) {
        result = pg_authid_rolcreatedb(GETSTRUCT(utup) as Form_pg_authid);
        ReleaseSysCache(utup);
    }
    result
}

/* Extern statics and consts needed by recovery_create_dbdir / dbase_redo  TODO(pg-port) */
extern "C" {
    static pg_dir_create_mode: c_int;
}
/* PG_TBLSPC_DIR_SLASH = "pg_tblspc/"  TODO(pg-port) */
const PG_TBLSPC_DIR_SLASH: &[u8] = b"pg_tblspc/\0";
/* ENOENT errno value  TODO(pg-port) */
const ENOENT: c_int = 2;

/* strstr() wrapper: returns true when needle is found in haystack  TODO(pg-port) */
unsafe fn strstr_contains(_haystack: *const c_char, _needle: *const c_char) -> bool { unimplemented!() }

/*
 * Remove tablespace directories
 *
 * We don't know what tablespaces db_id is using, so iterate through all
 * tablespaces removing <tablespace>/db_id
 */
unsafe fn remove_dbtablespaces(db_id: Oid) {
    let rel: Relation;
    let scan: TableScanDesc;
    let mut tuple: HeapTuple;
    let mut ltblspc: *mut List = ptr::null_mut(); /* NIL */
    let ntblspc: c_int;
    let mut i: c_int;
    let tablespace_ids: *mut Oid;

    rel = table_open(TableSpaceRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, ptr::null());
    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let spcform: Form_pg_tablespace = GETSTRUCT(tuple) as Form_pg_tablespace;
        let dsttablespace: Oid = pg_tablespace_oid(spcform);
        let dstpath: *mut c_char;
        let mut st: libc_stat = core::mem::zeroed();

        /* Don't mess with the global tablespace */
        if dsttablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        dstpath = GetDatabasePath(db_id, dsttablespace);

        if lstat(dstpath, &mut st) < 0 || !S_ISDIR(libc_stat_mode(&st)) {
            /* Assume we can ignore it */
            pfree(dstpath as *mut c_void);
            continue;
        }

        if !rmtree(dstpath, true) {
            ereport!(WARNING, errmsg!("some useless files may be left behind in old database directory \"{}\"", CStr::from_ptr(dstpath).to_string_lossy()));
        }

        ltblspc = lappend_oid(ltblspc, dsttablespace);
        pfree(dstpath as *mut c_void);
    }

    ntblspc = list_length(ltblspc);
    if ntblspc == 0 {
        table_endscan(scan);
        table_close(rel, AccessShareLock);
        return;
    }

    tablespace_ids = palloc(ntblspc as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    i = 0;
    foreach!(cell, ltblspc, {
        *tablespace_ids.add(i as usize) = crate::nodes::pg_list::lfirst_oid(current_cell!(cell));
        i += 1;
    });

    /* Record the filesystem change in XLOG */
    {
        let xlrec = xl_dbase_drop_rec {
            db_id,
            ntablespaces: ntblspc,
            tablespace_ids: [],
        };

        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_dbase_drop_rec as *const c_void, MinSizeOfDbaseDropRec);
        XLogRegisterData(tablespace_ids as *const c_void, ntblspc as usize * core::mem::size_of::<Oid>());

        let _ = XLogInsert(RM_DBASE_ID,
                           XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);
    }

    list_free(ltblspc);
    pfree(tablespace_ids as *mut c_void);

    table_endscan(scan);
    table_close(rel, AccessShareLock);
}

/*
 * Check for existing files that conflict with a proposed new DB OID;
 * return true if there are any
 *
 * If there were a subdirectory in any tablespace matching the proposed new
 * OID, we'd get a create failure due to the duplicate name ... and then we'd
 * try to remove that already-existing subdirectory during the cleanup in
 * remove_dbtablespaces.  Nuking existing files seems like a bad idea, so
 * instead we make this extra check before settling on the OID of the new
 * database.  This exactly parallels what GetNewRelFileNumber() does for table
 * relfilenumber values.
 */
pub unsafe fn check_db_file_conflict(db_id: Oid) -> bool {
    let mut result: bool = false;
    let rel: Relation;
    let scan: TableScanDesc;
    let mut tuple: HeapTuple;

    rel = table_open(TableSpaceRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, ptr::null());
    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let spcform: Form_pg_tablespace = GETSTRUCT(tuple) as Form_pg_tablespace;
        let dsttablespace: Oid = pg_tablespace_oid(spcform);
        let dstpath: *mut c_char;
        let mut st: libc_stat = core::mem::zeroed();

        /* Don't mess with the global tablespace */
        if dsttablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        dstpath = GetDatabasePath(db_id, dsttablespace);

        if lstat(dstpath, &mut st) == 0 {
            /* Found a conflicting file (or directory, whatever) */
            pfree(dstpath as *mut c_void);
            result = true;
            break;
        }

        pfree(dstpath as *mut c_void);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    result
}

/*
 * Issue a suitable errdetail message for a busy database
 */
unsafe fn errdetail_busy_db(notherbackends: c_int, npreparedxacts: c_int) -> c_int {
    if notherbackends > 0 && npreparedxacts > 0 {
        /*
         * We don't deal with singular versus plural here, since gettext
         * doesn't support multiple plurals in one string.
         */
        errdetail(b"There are %d other session(s) and %d prepared transaction(s) using the database.\0".as_ptr() as *const c_char,
                  notherbackends, npreparedxacts);
    } else if notherbackends > 0 {
        errdetail_plural(b"There is %d other session using the database.\0".as_ptr() as *const c_char,
                         b"There are %d other sessions using the database.\0".as_ptr() as *const c_char,
                         notherbackends,
                         notherbackends);
    } else {
        errdetail_plural(b"There is %d prepared transaction using the database.\0".as_ptr() as *const c_char,
                         b"There are %d prepared transactions using the database.\0".as_ptr() as *const c_char,
                         npreparedxacts,
                         npreparedxacts);
    }
    0 /* just to keep ereport macro happy */
}

/*
 * get_database_oid - given a database name, look up the OID
 *
 * If missing_ok is false, throw an error if database name not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_database_oid(dbname: *const c_char, missing_ok: bool) -> Oid {
    let pg_database: Relation;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed()];
    let scan: SysScanDesc;
    let dbtuple: HeapTuple;
    let oid: Oid;

    /*
     * There's no syscache for pg_database indexed by name, so we must look
     * the hard way.
     */
    pg_database = table_open(DatabaseRelationId, AccessShareLock);
    ScanKeyInit(&mut entry[0],
                Anum_pg_database_datname as c_int,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dbname));
    scan = systable_beginscan(pg_database, DatabaseNameIndexId, true,
                              ptr::null_mut(), 1, entry.as_mut_ptr());

    dbtuple = systable_getnext(scan);

    /* We assume that there can be at most one matching tuple */
    if HeapTupleIsValid(dbtuple) {
        oid = pg_database_oid(GETSTRUCT(dbtuple) as Form_pg_database);
    } else {
        oid = InvalidOid;
    }

    systable_endscan(scan);
    table_close(pg_database, AccessShareLock);

    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_DATABASE) */ errmsg!("database \"{}\" does not exist", CStr::from_ptr(dbname).to_string_lossy()));
    }

    oid
}

/*
 * get_database_name - given a database OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such database.
 */
pub unsafe fn get_database_name(dbid: Oid) -> *mut c_char {
    let dbtuple: HeapTuple;
    let result: *mut c_char;

    dbtuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
    if HeapTupleIsValid(dbtuple) {
        result = pstrdup(NameStr(pg_database_datname_ptr(GETSTRUCT(dbtuple) as Form_pg_database)));
        ReleaseSysCache(dbtuple);
    } else {
        result = ptr::null_mut();
    }

    result
}

/*
 * While dropping a database the pg_database row is marked invalid, but the
 * catalog contents still exist. Connections to such a database are not
 * allowed.
 */
pub unsafe fn database_is_invalid_form(datform: Form_pg_database) -> bool {
    pg_database_datconnlimit(datform) == DATCONNLIMIT_INVALID_DB
}

/*
 * Convenience wrapper around database_is_invalid_form()
 */
pub unsafe fn database_is_invalid_oid(dboid: Oid) -> bool {
    let dbtup: HeapTuple;
    let dbform: Form_pg_database;
    let invalid: bool;

    dbtup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid));
    if !HeapTupleIsValid(dbtup) {
        elog!(ERROR, "cache lookup failed for database {}", dboid);
    }
    dbform = GETSTRUCT(dbtup) as Form_pg_database;

    invalid = database_is_invalid_form(dbform);

    ReleaseSysCache(dbtup);

    invalid
}

/*
 * recovery_create_dbdir()
 *
 * During recovery, there's a case where we validly need to recover a missing
 * tablespace directory so that recovery can continue.  This happens when
 * recovery wants to create a database but the holding tablespace has been
 * removed before the server stopped.  Since we expect that the directory will
 * be gone before reaching recovery consistency, and we have no knowledge about
 * the tablespace other than its OID here, we create a real directory under
 * pg_tblspc here instead of restoring the symlink.
 *
 * If only_tblspc is true, then the requested directory must be in pg_tblspc/
 */
unsafe fn recovery_create_dbdir(path: *mut c_char, only_tblspc: bool) {
    let mut st: libc_stat = core::mem::zeroed();

    // Assert(RecoveryInProgress())

    if stat(path, &mut st) == 0 {
        return;
    }

    if only_tblspc && !strstr_contains(path, PG_TBLSPC_DIR_SLASH.as_ptr() as *const c_char) {
        elog!(PANIC, "requested to created invalid directory: {}", 0);
    }

    if reachedConsistency && !allow_in_place_tablespaces {
        ereport!(PANIC, errmsg!("missing directory \"{}\"", CStr::from_ptr(path).to_string_lossy()));
    }

    elog!(if reachedConsistency { WARNING } else { DEBUG1 }, "creating missing directory: {}", 0);

    if pg_mkdir_p(path, pg_dir_create_mode) != 0 {
        ereport!(PANIC, errmsg!("could not create missing directory \"{}\": %m", CStr::from_ptr(path).to_string_lossy()));
    }
}

/*
 * DATABASE resource manager's routines
 */
pub unsafe fn dbase_redo(record: *mut XLogReaderState) {
    let info: u8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in dbase records */
    // Assert(!XLogRecHasAnyBlockRefs(record))

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let xlrec = XLogRecGetData(record) as *mut xl_dbase_create_file_copy_rec;
        let src_path: *mut c_char;
        let dst_path: *mut c_char;
        let parent_path: *mut c_char;
        let mut st: libc_stat = core::mem::zeroed();

        src_path = GetDatabasePath((*xlrec).src_db_id, (*xlrec).src_tablespace_id);
        dst_path = GetDatabasePath((*xlrec).db_id, (*xlrec).tablespace_id);

        /*
         * Our theory for replaying a CREATE is to forcibly drop the target
         * subdirectory if present, then re-copy the source data. This may be
         * more work than needed, but it is simple to implement.
         */
        if stat(dst_path, &mut st) == 0 && S_ISDIR(libc_stat_mode(&st)) {
            if !rmtree(dst_path, true) {
                /* If this failed, copydir() below is going to error. */
                ereport!(WARNING, errmsg!("some useless files may be left behind in old database directory \"{}\"", CStr::from_ptr(dst_path).to_string_lossy()));
            }
        }

        /*
         * If the parent of the target path doesn't exist, create it now. This
         * enables us to create the target underneath later.
         */
        parent_path = pstrdup(dst_path);
        get_parent_directory(parent_path);
        if stat(parent_path, &mut st) < 0 {
            if *libc_errno() != ENOENT {
                ereport!(FATAL, errmsg!("could not stat directory \"{}\": %m", CStr::from_ptr(dst_path).to_string_lossy()));
            }

            /* create the parent directory if needed and valid */
            recovery_create_dbdir(parent_path, true);
        }
        pfree(parent_path as *mut c_void);

        /*
         * There's a case where the copy source directory is missing for the
         * same reason above.  Create the empty source directory so that
         * copydir below doesn't fail.  The directory will be dropped soon by
         * recovery.
         */
        if stat(src_path, &mut st) < 0 && *libc_errno() == ENOENT {
            recovery_create_dbdir(src_path, false);
        }

        /*
         * Force dirty buffers out to disk, to ensure source database is
         * up-to-date for the copy.
         */
        FlushDatabaseBuffers((*xlrec).src_db_id);

        /* Close all smgr fds in all backends. */
        WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

        /*
         * Copy this subdirectory to the new location
         *
         * We don't need to copy subdirectories
         */
        copydir(src_path, dst_path, false);

        pfree(src_path as *mut c_void);
        pfree(dst_path as *mut c_void);
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let xlrec = XLogRecGetData(record) as *mut xl_dbase_create_wal_log_rec;
        let dbpath: *mut c_char;
        let parent_path: *mut c_char;

        dbpath = GetDatabasePath((*xlrec).db_id, (*xlrec).tablespace_id);

        /* create the parent directory if needed and valid */
        parent_path = pstrdup(dbpath);
        get_parent_directory(parent_path);
        recovery_create_dbdir(parent_path, true);
        pfree(parent_path as *mut c_void);

        /* Create the database directory with the version file. */
        CreateDirAndVersionFile(dbpath, (*xlrec).db_id, (*xlrec).tablespace_id,
                                true);
        pfree(dbpath as *mut c_void);
    } else if info == XLOG_DBASE_DROP {
        let xlrec = XLogRecGetData(record) as *mut xl_dbase_drop_rec;
        let mut dst_path: *mut c_char;
        let mut i: c_int;

        if InHotStandby() {
            /*
             * Lock database while we resolve conflicts to ensure that
             * InitPostgres() cannot fully re-execute concurrently. This
             * avoids backends re-connecting automatically to same database,
             * which can happen in some cases.
             *
             * This will lock out walsenders trying to connect to db-specific
             * slots for logical decoding too, so it's safe for us to drop
             * slots.
             */
            LockSharedObjectForSession(DatabaseRelationId, (*xlrec).db_id, 0, AccessExclusiveLock);
            ResolveRecoveryConflictWithDatabase((*xlrec).db_id);
        }

        /* Drop any database-specific replication slots */
        ReplicationSlotsDropDBSlots((*xlrec).db_id);

        /* Drop pages for this database that are in the shared buffer cache */
        DropDatabaseBuffers((*xlrec).db_id);

        /* Also, clean out any fsync requests that might be pending in md.c */
        ForgetDatabaseSyncRequests((*xlrec).db_id);

        /* Clean out the xlog relcache too */
        XLogDropDatabase((*xlrec).db_id);

        /* Close all smgr fds in all backends. */
        WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

        i = 0;
        while i < (*xlrec).ntablespaces {
            dst_path = GetDatabasePath((*xlrec).db_id, *(*xlrec).tablespace_ids.as_ptr().add(i as usize));

            /* And remove the physical files */
            if !rmtree(dst_path, true) {
                ereport!(WARNING, errmsg!("some useless files may be left behind in old database directory \"{}\"", CStr::from_ptr(dst_path).to_string_lossy()));
            }
            pfree(dst_path as *mut c_void);
            i += 1;
        }

        if InHotStandby() {
            /*
             * Release locks prior to commit. XXX There is a race condition
             * here that may allow backends to reconnect, but the window for
             * this is small because the gap between here and commit is mostly
             * fairly small and it is unlikely that people will be dropping
             * databases that we are trying to connect to anyway.
             */
            UnlockSharedObjectForSession(DatabaseRelationId, (*xlrec).db_id, 0, AccessExclusiveLock);
        }
    } else {
        elog!(PANIC, "dbase_redo: unknown op code {}", info as Oid);
    }
}
