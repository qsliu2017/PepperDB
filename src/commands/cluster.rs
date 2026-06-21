/*-------------------------------------------------------------------------
 *
 * cluster.c
 *	  CLUSTER a table on an index.  This is now also used for VACUUM FULL.
 *
 * There is hardly anything left of Paul Brown's original implementation...
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/cluster.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use crate::{foreach, current_cell};
use core::ffi::{c_char, c_int, c_void};

use crate::c::{int32, float4, TransactionId};
use crate::postgres::{Datum, ObjectIdGetDatum, BoolGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::OidIsValid;
use crate::nodes::pg_list::{List, ListCell, lfirst, lfirst_oid, lappend, NIL};
use crate::nodes::nodes::Node;

/* ---------------------------------------------------------------------------
 * Local type aliases
 * ---------------------------------------------------------------------------
 */

// HeapTuple is a single pointer (HeapTupleData*).
use crate::access::htup_details::{HeapTupleData, HeapTupleIsValid};
type HeapTuple = *mut HeapTupleData;

// Relation pointer
use crate::utils::rel::RelationData;
type Relation = *mut RelationData;
use crate::access::index::amapi::IndexAmRoutine;

// TupleDesc
#[repr(C)] pub struct TupleDescData { pub natts: c_int, _opaque: [u8; 0] }
type TupleDesc = *mut TupleDescData;

// MemoryContext comes from the prelude (utils/palloc).

// BlockNumber
type BlockNumber = u32;

// MultiXactId / RelFileNumber
type MultiXactId = TransactionId;
type RelFileNumber = Oid;

// LOCKMODE
type LOCKMODE = c_int;
const NoLock: LOCKMODE = 0;
const AccessShareLock: LOCKMODE = 1;
const RowExclusiveLock: LOCKMODE = 3;
const AccessExclusiveLock: LOCKMODE = 8;

/* ---------------------------------------------------------------------------
 * Stub types for not-yet-ported modules
 * ---------------------------------------------------------------------------
 */

// Form_pg_class / Form_pg_index (real catalog row layouts)
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_index::Form_pg_index;

// Parse nodes  TODO(pg-port)
#[repr(C)] pub struct ParseState { _opaque: [u8; 0] }
#[repr(C)] pub struct RangeVar {
    pub r#type: i32,
    pub catalogname: *mut c_char,
    pub schemaname: *mut c_char,
    pub relname: *mut c_char,
}
#[repr(C)] pub struct DefElem {
    pub r#type: i32,
    pub defnamespace: *mut c_char,
    pub defname: *mut c_char,
    pub arg: *mut Node,
    pub defaction: c_int,
    pub location: c_int,
}
#[repr(C)] pub struct ClusterStmt {
    pub r#type: i32,
    pub relation: *mut RangeVar,
    pub indexname: *mut c_char,
    pub params: *mut List,
}

// ClusterParams  TODO(pg-port): commands/cluster.h
#[repr(C)] pub struct ClusterParams {
    pub options: bits32,
}
type bits32 = u32;
const CLUOPT_RECHECK: bits32 = 1 << 0;              /* recheck relation state */
const CLUOPT_RECHECK_ISCLUSTERED: bits32 = 1 << 1;  /* recheck relation state for indisclustered */
const CLUOPT_VERBOSE: bits32 = 1 << 2;              /* print progress info */

// RelToCluster
#[repr(C)]
struct RelToCluster {
    tableOid: Oid,
    indexOid: Oid,
}

// VacuumParams / VacuumCutoffs  TODO(pg-port): commands/vacuum.h
#[repr(C)] pub struct VacuumParams { _opaque: [u8; 256] }
#[repr(C)]
pub struct VacuumCutoffs {
    pub relfrozenxid: TransactionId,
    pub relminmxid: MultiXactId,
    pub OldestXmin: TransactionId,
    pub OldestMxact: MultiXactId,
    pub FreezeLimit: TransactionId,
    pub MultiXactCutoff: MultiXactId,
}

// ObjectAddress  TODO(pg-port): catalog/objectaddress.h
#[repr(C)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: int32,
}

// ReindexParams  TODO(pg-port): catalog/index.h
#[repr(C)] pub struct ReindexParams { _opaque: [u8; 16] }

// PGRUsage  TODO(pg-port): utils/pg_rusage.h
#[repr(C)] struct PGRUsage { tv: [i64; 4] }
unsafe fn pg_rusage_init(_ru0: *mut PGRUsage) { unimplemented!("STUB pg_rusage_init") }
unsafe fn pg_rusage_show(_ru0: *const PGRUsage) -> *const c_char { b"\0".as_ptr() as *const c_char }

/* ---------------------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------------------
 */

const NAMEDATALEN: usize = 64;

/* TODO(pg-port): catalog/pg_class.h relkind / relpersistence */
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_INDEX: c_char = b'i' as c_char;
const RELKIND_MATVIEW: c_char = b'm' as c_char;
const RELKIND_TOASTVALUE: c_char = b't' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;
const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;

/* TODO(pg-port): catalog/catalog.h relation OIDs */
const RelationRelationId: Oid = 1259;
const IndexRelationId: Oid = 2610;
const AccessMethodRelationId: Oid = 2601;

/* TODO(pg-port): catalog/pg_am.h */
const BTREE_AM_OID: Oid = 403;

/* TODO(pg-port): access/transam.h invalid ids */
const InvalidTransactionId: TransactionId = 0;
const InvalidMultiXactId: MultiXactId = 0;

/* INFO / DEBUG2 message levels come from prelude (utils/elog) */

/* TODO(pg-port): miscadmin.h security restricted operation */
const SECURITY_RESTRICTED_OPERATION: c_int = 0x1;

/* TODO(pg-port): access/stratnum.h */
const BTEqualStrategyNumber: u16 = 3;

/* TODO(pg-port): access/sdir.h */
type ScanDirection = c_int;
const ForwardScanDirection: ScanDirection = 1;

/* TODO(pg-port): utils/fmgroids.h */
const F_BOOLEQ: Oid = 60;

/* TODO(pg-port): catalog/pg_index.h attribute numbers */
const Anum_pg_index_indpred: c_int = 18;
const Anum_pg_index_indisclustered: c_int = 11;
const Anum_pg_class_reloptions: c_int = 33;

/* TODO(pg-port): utils/syscache.h cache IDs */
const RELOID: c_int = 57;
const INDEXRELID: c_int = 34;

/* TODO(pg-port): catalog/dependency.h drop behavior / flags */
const DROP_RESTRICT: c_int = 0;
const PERFORM_DELETION_INTERNAL: c_int = 1 << 0;
const DEPENDENCY_INTERNAL: c_char = b'i' as c_char;

/* TODO(pg-port): catalog/heap.h ONCOMMIT */
const ONCOMMIT_NOOP: c_int = 0;

/* TODO(pg-port): nodes/parsenodes.h ERRCODE syntax error placeholder */
const ERRCODE_SYNTAX_ERROR: c_int = 0;

/* TODO(pg-port): utils/acl.h */
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACL_MAINTAIN: u32 = 1 << 11;

/* TODO(pg-port): commands/progress.h */
const PROGRESS_COMMAND_CLUSTER: c_int = 1;
const PROGRESS_CLUSTER_COMMAND: c_int = 0;
const PROGRESS_CLUSTER_PHASE: c_int = 1;
const PROGRESS_CLUSTER_COMMAND_CLUSTER: i64 = 1;
const PROGRESS_CLUSTER_COMMAND_VACUUM_FULL: i64 = 2;
const PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES: i64 = 5;
const PROGRESS_CLUSTER_PHASE_REBUILD_INDEX: i64 = 6;
const PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP: i64 = 7;

/* TODO(pg-port): catalog/index.h reindex flags */
const REINDEX_REL_PROCESS_TOAST: c_int = 1 << 0;
const REINDEX_REL_SUPPRESS_INDEX_USE: c_int = 1 << 1;
const REINDEX_REL_CHECK_CONSTRAINTS: c_int = 1 << 2;
const REINDEX_REL_FORCE_INDEXES_UNLOGGED: c_int = 1 << 3;
const REINDEX_REL_FORCE_INDEXES_PERMANENT: c_int = 1 << 4;

/* ---------------------------------------------------------------------------
 * libc shims
 * ---------------------------------------------------------------------------
 */
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/* ---------------------------------------------------------------------------
 * Dependency stubs (functions defined in other .c files)  TODO(pg-port)
 * ---------------------------------------------------------------------------
 */

/* commands/defrem.h */
unsafe fn defGetBoolean(_def: *mut DefElem) -> bool { unimplemented!("STUB defGetBoolean") }

/* parser/parse_node.h */
unsafe fn parser_errposition(_pstate: *mut ParseState, _location: c_int) -> c_int { unimplemented!("STUB parser_errposition") }

/* access/table/tableam + relcache: open/close */
unsafe fn table_open(_relid: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!("STUB table_open") }
unsafe fn table_close(_rel: Relation, _lockmode: LOCKMODE) { unimplemented!("STUB table_close") }
unsafe fn relation_open(_relid: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!("STUB relation_open") }
unsafe fn relation_close(_rel: Relation, _lockmode: LOCKMODE) { unimplemented!("STUB relation_close") }
unsafe fn index_open(_relid: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!("STUB index_open") }
unsafe fn index_close(_rel: Relation, _lockmode: LOCKMODE) { unimplemented!("STUB index_close") }

/* catalog/namespace.h */
unsafe fn RangeVarGetRelidExtended(
    _relation: *mut RangeVar,
    _lockmode: LOCKMODE,
    _flags: c_int,
    _callback: unsafe fn(*mut RangeVar, Oid, Oid, *mut c_void),
    _callback_arg: *mut c_void,
) -> Oid { /* TODO(pg-port) */ InvalidOid }
unsafe fn RangeVarCallbackMaintainsTable(_relation: *mut RangeVar, _relId: Oid, _oldRelId: Oid, _arg: *mut c_void) { unimplemented!("STUB RangeVarCallbackMaintainsTable") }
unsafe fn get_relname_relid(_relname: *const c_char, _relnamespace: Oid) -> Oid { unimplemented!("STUB get_relname_relid") }
unsafe fn LookupCreationNamespace(_nspname: *const c_char) -> Oid { unimplemented!("STUB LookupCreationNamespace") }
unsafe fn RangeVarCallbackOwnsTable(_relation: *mut RangeVar, _relId: Oid, _oldRelId: Oid, _arg: *mut c_void) { unimplemented!("STUB RangeVarCallbackOwnsTable") }

/* utils/lsyscache.h */
unsafe fn get_index_isclustered(_index_oid: Oid) -> bool { unimplemented!("STUB get_index_isclustered") }
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char { unimplemented!("STUB get_namespace_name") }
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char { unimplemented!("STUB get_rel_name") }
unsafe fn get_rel_namespace(_relid: Oid) -> Oid { unimplemented!("STUB get_rel_namespace") }
unsafe fn get_rel_relkind(_relid: Oid) -> c_char { unimplemented!("STUB get_rel_relkind") }

/* miscadmin.h */
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn GetUserIdAndSecContext(_userid: *mut Oid, _sec_context: *mut c_int) { unimplemented!("STUB GetUserIdAndSecContext") }
unsafe fn SetUserIdAndSecContext(_userid: Oid, _sec_context: c_int) { unimplemented!("STUB SetUserIdAndSecContext") }
unsafe fn PreventInTransactionBlock(_isTopLevel: bool, _stmtType: *const c_char) { unimplemented!("STUB PreventInTransactionBlock") }
unsafe fn CHECK_FOR_INTERRUPTS() { unimplemented!("STUB CHECK_FOR_INTERRUPTS") }

/* utils/guc.h */
unsafe fn NewGUCNestLevel() -> c_int { unimplemented!("STUB NewGUCNestLevel") }
unsafe fn RestrictSearchPath() { unimplemented!("STUB RestrictSearchPath") }
unsafe fn AtEOXact_GUC(_isCommit: bool, _nestLevel: c_int) { unimplemented!("STUB AtEOXact_GUC") }

/* utils/memutils.h (MemoryContextDelete / MemoryContextSwitchTo come from prelude) */
static mut PortalContext: MemoryContext = core::ptr::null_mut();
unsafe fn AllocSetContextCreate(_parent: MemoryContext, _name: *const c_char, _minsize: usize, _initsize: usize, _maxsize: usize) -> MemoryContext { unimplemented!("STUB AllocSetContextCreate") }
const ALLOCSET_DEFAULT_MINSIZE: usize = 0;
const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;
const ALLOCSET_DEFAULT_MAXSIZE: usize = 8 * 1024 * 1024;

/* palloc comes from prelude (utils/palloc) */

/* access/xact.h */
unsafe fn StartTransactionCommand() { unimplemented!("STUB StartTransactionCommand") }
unsafe fn CommitTransactionCommand() { unimplemented!("STUB CommitTransactionCommand") }
unsafe fn CommandCounterIncrement() { /* DDL no-op (no event triggers in bring-up) */ }

/* utils/snapmgr.h */
#[repr(C)] pub struct SnapshotData { _opaque: [u8; 0] }
type Snapshot = *mut SnapshotData;
unsafe fn GetTransactionSnapshot() -> Snapshot { unimplemented!("STUB GetTransactionSnapshot") }
unsafe fn PushActiveSnapshot(_snap: Snapshot) { unimplemented!("STUB PushActiveSnapshot") }
unsafe fn PopActiveSnapshot() { unimplemented!("STUB PopActiveSnapshot") }

/* commands/tablecmds.h */
unsafe fn CheckTableNotInUse(_rel: Relation, _stmt: *const c_char) { unimplemented!("STUB CheckTableNotInUse") }
unsafe fn RenameRelationInternal(_myrelid: Oid, _newrelname: *const c_char, _is_internal: bool, _is_index: bool) { unimplemented!("STUB RenameRelationInternal") }
unsafe fn ResetRelRewrite(_myrelid: Oid) { unimplemented!("STUB ResetRelRewrite") }

/* commands/vacuum.h */
unsafe fn vacuum_get_cutoffs(_rel: Relation, _params: *const VacuumParams, _cutoffs: *mut VacuumCutoffs) { unimplemented!("STUB vacuum_get_cutoffs") }

/* access/multixact.h */
unsafe fn MultiXactIdIsValid(multi: MultiXactId) -> bool { multi != InvalidMultiXactId }
unsafe fn MultiXactIdPrecedes(_multi1: MultiXactId, _multi2: MultiXactId) -> bool { unimplemented!("STUB MultiXactIdPrecedes") }

/* access/transam.h */
unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool { xid != InvalidTransactionId }
unsafe fn TransactionIdIsNormal(xid: TransactionId) -> bool { xid >= 3 }
unsafe fn TransactionIdPrecedes(_id1: TransactionId, _id2: TransactionId) -> bool { unimplemented!("STUB TransactionIdPrecedes") }

/* storage/lmgr.h */
unsafe fn LockRelationOid(_relid: Oid, _lockmode: LOCKMODE) { unimplemented!("STUB LockRelationOid") }
unsafe fn CheckRelationLockedByMe(_relation: Relation, _lockmode: LOCKMODE, _orstronger: bool) -> bool { unimplemented!("STUB CheckRelationLockedByMe") }
unsafe fn CheckRelationOidLockedByMe(_relid: Oid, _lockmode: LOCKMODE, _orstronger: bool) -> bool { unimplemented!("STUB CheckRelationOidLockedByMe") }

/* storage/predicate.h */
unsafe fn TransferPredicateLocksToHeapRelation(_relation: Relation) { unimplemented!("STUB TransferPredicateLocksToHeapRelation") }

/* storage/bufmgr.h */
unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber { unimplemented!("STUB RelationGetNumberOfBlocks") }

/* utils/acl.h */
unsafe fn pg_class_aclcheck(_table_oid: Oid, _roleid: Oid, _mode: u32) -> AclResult { /* TODO(pg-port) */ ACLCHECK_OK }

/* utils/syscache.h */
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!("STUB SearchSysCache1") }
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!("STUB SearchSysCacheCopy1") }
unsafe fn SearchSysCacheExists1(_cacheId: c_int, _key1: Datum) -> bool { unimplemented!("STUB SearchSysCacheExists1") }
unsafe fn SysCacheGetAttr(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int, _isNull: *mut bool) -> Datum { unimplemented!("STUB SysCacheGetAttr") }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) { unimplemented!("STUB ReleaseSysCache") }

/* access/htup_details.h */
unsafe fn GETSTRUCT_pg_class(tup: HeapTuple) -> Form_pg_class { unimplemented!("STUB GETSTRUCT_pg_class") }
unsafe fn GETSTRUCT_pg_index(tup: HeapTuple) -> Form_pg_index { unimplemented!("STUB GETSTRUCT_pg_index") }
unsafe fn heap_freetuple(_htup: HeapTuple) { unimplemented!("STUB heap_freetuple") }
unsafe fn heap_attisnull(_tup: HeapTuple, _attnum: c_int, _tupleDesc: TupleDesc) -> bool { unimplemented!("STUB heap_attisnull") }

/* catalog/indexing.h */
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut c_void, _tup: HeapTuple) { unimplemented!("STUB CatalogTupleUpdate") }
unsafe fn CatalogTupleUpdateWithInfo(_heapRel: Relation, _otid: *mut c_void, _tup: HeapTuple, _indstate: CatalogIndexState) { unimplemented!("STUB CatalogTupleUpdateWithInfo") }
type CatalogIndexState = *mut c_void;
unsafe fn CatalogOpenIndexes(_heapRel: Relation) -> CatalogIndexState { unimplemented!("STUB CatalogOpenIndexes") }
unsafe fn CatalogCloseIndexes(_indstate: CatalogIndexState) { unimplemented!("STUB CatalogCloseIndexes") }

/* utils/inval.h */
unsafe fn CacheInvalidateRelcacheByTuple(_classTuple: HeapTuple) { unimplemented!("STUB CacheInvalidateRelcacheByTuple") }
unsafe fn CacheInvalidateCatalog(_catalogId: Oid) { unimplemented!("STUB CacheInvalidateCatalog") }

/* catalog/dependency.h */
unsafe fn performDeletion(_object: *const ObjectAddress, _behavior: c_int, _flags: c_int) { unimplemented!("STUB performDeletion") }
unsafe fn changeDependencyFor(_classId: Oid, _objectId: Oid, _refClassId: Oid, _oldRefObjectId: Oid, _newRefObjectId: Oid) -> c_long { unimplemented!("STUB changeDependencyFor") }
unsafe fn deleteDependencyRecordsFor(_classId: Oid, _objectId: Oid, _skipExtensionDeps: bool) -> c_long { unimplemented!("STUB deleteDependencyRecordsFor") }
unsafe fn recordDependencyOn(_depender: *const ObjectAddress, _referenced: *const ObjectAddress, _behavior: c_char) { unimplemented!("STUB recordDependencyOn") }
/* c_long comes from prelude (core::ffi::c_long) and matches C long */

/* catalog/catalog.h */
unsafe fn IsSystemRelation(_relation: Relation) -> bool { unimplemented!("STUB IsSystemRelation") }
unsafe fn IsSystemClass(_relid: Oid, _reltuple: Form_pg_class) -> bool { unimplemented!("STUB IsSystemClass") }

/* catalog/heap.h */
unsafe fn heap_create_with_catalog(
    _relname: *const c_char, _relnamespace: Oid, _reltablespace: Oid,
    _relid: Oid, _reltypeid: Oid, _reloftypeid: Oid, _ownerid: Oid,
    _accessmtd: Oid, _tupdesc: TupleDesc, _cooked_constraints: *mut List,
    _relkind: c_char, _relpersistence: c_char, _shared_relation: bool,
    _mapped_relation: bool, _oncommit: c_int, _reloptions: Datum,
    _use_user_acl: bool, _allow_system_table_mods: bool, _is_internal: bool,
    _relrewrite: Oid, _typaddress: *mut ObjectAddress,
) -> Oid { /* TODO(pg-port) */ InvalidOid }
unsafe fn RelationClearMissing(_rel: Relation) { unimplemented!("STUB RelationClearMissing") }

/* catalog/toasting.h */
unsafe fn NewHeapCreateToastTable(_relOid: Oid, _reloptions: Datum, _lockmode: LOCKMODE, _OIDOldToastTable: Oid) { unimplemented!("STUB NewHeapCreateToastTable") }

/* access/toast_internals.h */
unsafe fn toast_get_valid_index(_toastoid: Oid, _lock: LOCKMODE) -> Oid { unimplemented!("STUB toast_get_valid_index") }

/* utils/relmapper.h */
unsafe fn RelationMapOidToFilenumber(_relationId: Oid, _shared: bool) -> RelFileNumber { unimplemented!("STUB RelationMapOidToFilenumber") }
unsafe fn RelationMapUpdateMap(_relationId: Oid, _filenumber: RelFileNumber, _shared: bool, _immediate: bool) { unimplemented!("STUB RelationMapUpdateMap") }
unsafe fn RelationMapRemoveMapping(_relationId: Oid) { unimplemented!("STUB RelationMapRemoveMapping") }
unsafe fn RelFileNumberIsValid(fnum: RelFileNumber) -> bool { fnum != InvalidOid }

/* utils/rel.h relcache-side helpers */
unsafe fn RelationAssumeNewRelfilelocator(_relation: Relation) { unimplemented!("STUB RelationAssumeNewRelfilelocator") }
unsafe fn RelationIsMapped(_relation: Relation) -> bool { unimplemented!("STUB RelationIsMapped") }
unsafe fn RelationIsPopulated(_relation: Relation) -> bool { unimplemented!("STUB RelationIsPopulated") }
unsafe fn RELATION_IS_OTHER_TEMP(_relation: Relation) -> bool { unimplemented!("STUB RELATION_IS_OTHER_TEMP") }
unsafe fn RelationGetRelid(_relation: Relation) -> Oid { unimplemented!("STUB RelationGetRelid") }
unsafe fn RelationGetDescr(_relation: Relation) -> TupleDesc { unimplemented!("STUB RelationGetDescr") }
unsafe fn RelationGetNamespace(_relation: Relation) -> Oid { unimplemented!("STUB RelationGetNamespace") }
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char { /* TODO(pg-port) */ b"\0".as_ptr() as *const c_char }
unsafe fn RelationGetIndexList(_relation: Relation) -> *mut List { unimplemented!("STUB RelationGetIndexList") }
unsafe fn NameStr_relname(_relform: Form_pg_class) -> *const c_char { /* TODO(pg-port) */ b"\0".as_ptr() as *const c_char }

/* access/tableam.h */
unsafe fn table_beginscan_catalog(_relation: Relation, _nkeys: c_int, _key: *mut ScanKeyData) -> TableScanDesc { unimplemented!("STUB table_beginscan_catalog") }
unsafe fn table_endscan(_scan: TableScanDesc) { unimplemented!("STUB table_endscan") }
unsafe fn heap_getnext(_scan: TableScanDesc, _direction: ScanDirection) -> HeapTuple { unimplemented!("STUB heap_getnext") }
unsafe fn table_relation_copy_for_cluster(
    _OldHeap: Relation, _NewHeap: Relation, _OldIndex: Relation, _use_sort: bool,
    _OldestXmin: TransactionId, _xid_cutoff: *mut TransactionId, _multi_cutoff: *mut MultiXactId,
    _num_tuples: *mut f64, _tups_vacuumed: *mut f64, _tups_recently_dead: *mut f64,
) { /* TODO(pg-port) */ }
#[repr(C)] pub struct TableScanDescData { _opaque: [u8; 0] }
type TableScanDesc = *mut TableScanDescData;

/* access/skey.h */
#[repr(C)] pub struct ScanKeyData { _opaque: [u8; 64] }
unsafe fn ScanKeyInit(_entry: *mut ScanKeyData, _attributeNumber: c_int, _strategy: u16, _procedure: Oid, _argument: Datum) { unimplemented!("STUB ScanKeyInit") }

/* catalog/index.h */
unsafe fn reindex_relation(_stmt: *const c_void, _relid: Oid, _flags: c_int, _params: *const ReindexParams) -> bool { unimplemented!("STUB reindex_relation") }
unsafe fn IndexGetRelation(_indexId: Oid, _missing_ok: bool) -> Oid { unimplemented!("STUB IndexGetRelation") }

/* catalog/objectaccess.h InvokeObjectPostAlterHookArg */
unsafe fn InvokeObjectPostAlterHookArg(_classId: Oid, _objectId: Oid, _subId: c_int, _auxiliaryId: Oid, _is_internal: bool) { unimplemented!("STUB InvokeObjectPostAlterHookArg") }

/* catalog/pg_inherits.h */
unsafe fn find_all_inheritors(_relId: Oid, _lockmode: LOCKMODE, _numparents: *mut c_int) -> *mut List { unimplemented!("STUB find_all_inheritors") }

/* optimizer/optimizer.h */
unsafe fn plan_cluster_use_sort(_tableOid: Oid, _indexOid: Oid) -> bool { unimplemented!("STUB plan_cluster_use_sort") }

/* pgstat / progress */
unsafe fn pgstat_progress_start_command(_cmdtype: c_int, _relid: Oid) { unimplemented!("STUB pgstat_progress_start_command") }
#[no_mangle]
unsafe fn pgstat_progress_update_param(_index: c_int, _val: i64) { unimplemented!("STUB pgstat_progress_update_param") }
unsafe fn pgstat_progress_end_command() { unimplemented!("STUB pgstat_progress_end_command") }

/*---------------------------------------------------------------------------
 * This cluster code allows for clustering multiple tables at once. Because
 * of this, we cannot just run everything on a single transaction, or we
 * would be forced to acquire exclusive locks on all the tables being
 * clustered, simultaneously --- very likely leading to deadlock.
 *
 * To solve this we follow a similar strategy to VACUUM code,
 * clustering each relation in a separate transaction. For this to work,
 * we need to:
 *	- provide a separate memory context so that we can pass information in
 *	  a way that survives across transactions
 *	- start a new transaction every time a new relation is clustered
 *	- check for validity of the information on to-be-clustered relations,
 *	  as someone might have deleted a relation behind our back, or
 *	  clustered one on a different index
 *	- end the transaction
 *
 * The single-relation case does not have any such overhead.
 *
 * We also allow a relation to be specified without index.  In that case,
 * the indisclustered bit will be looked up, and an ERROR will be thrown
 * if there is no index with the bit set.
 *---------------------------------------------------------------------------
 */
pub unsafe fn cluster(pstate: *mut ParseState, stmt: *mut ClusterStmt, isTopLevel: bool)
{
    let mut lc: *mut ListCell;
    let mut params: ClusterParams = ClusterParams { options: 0 };
    let mut verbose: bool = false;
    let mut rel: Relation = core::ptr::null_mut();
    let mut indexOid: Oid = InvalidOid;
    let cluster_context: MemoryContext;
    let rtcs: *mut List;

    /* Parse option list */
    foreach!(lc, (*stmt).params, {
        let opt = lfirst(current_cell!(lc)) as *mut DefElem;

        if strcmp((*opt).defname, b"verbose\0".as_ptr() as *const c_char) == 0 {
            verbose = defGetBoolean(opt);
        } else {
            ereport!(ERROR,
                     errmsg!("unrecognized {} option \"{}\"",
                             "CLUSTER",
                             std::ffi::CStr::from_ptr((*opt).defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition(pstate, opt->location) */
        }
    });

    params.options = if verbose { CLUOPT_VERBOSE } else { 0 };

    if !(*stmt).relation.is_null() {
        /* This is the single-relation case. */
        let tableOid: Oid;

        /*
         * Find, lock, and check permissions on the table.  We obtain
         * AccessExclusiveLock right away to avoid lock-upgrade hazard in the
         * single-transaction case.
         */
        tableOid = RangeVarGetRelidExtended((*stmt).relation,
                                            AccessExclusiveLock,
                                            0,
                                            RangeVarCallbackMaintainsTable,
                                            core::ptr::null_mut());
        rel = table_open(tableOid, NoLock);

        /*
         * Reject clustering a remote temp table ... their local buffer
         * manager is not going to cope.
         */
        if RELATION_IS_OTHER_TEMP(rel) {
            ereport!(ERROR,
                     errmsg!("cannot cluster temporary tables of other sessions"));
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        }

        if (*stmt).indexname.is_null() {
            let mut index: *mut ListCell;

            /* We need to find the index that has indisclustered set. */
            foreach!(index, RelationGetIndexList(rel), {
                indexOid = lfirst_oid(current_cell!(index));
                if get_index_isclustered(indexOid) {
                    break;
                }
                indexOid = InvalidOid;
            });

            if !OidIsValid(indexOid) {
                ereport!(ERROR,
                         errmsg!("there is no previously clustered index for table \"{}\"",
                                 std::ffi::CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            }
        } else {
            /*
             * The index is expected to be in the same namespace as the
             * relation.
             */
            indexOid = get_relname_relid((*stmt).indexname,
                                         (*(*rel).rd_rel).relnamespace);
            if !OidIsValid(indexOid) {
                ereport!(ERROR,
                         errmsg!("index \"{}\" for table \"{}\" does not exist",
                                 std::ffi::CStr::from_ptr((*stmt).indexname).to_string_lossy(),
                                 std::ffi::CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            }
        }

        /* For non-partitioned tables, do what we came here to do. */
        if (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE {
            cluster_rel(rel, indexOid, &mut params);
            /* cluster_rel closes the relation, but keeps lock */

            return;
        }
    }

    /*
     * By here, we know we are in a multi-table situation.  In order to avoid
     * holding locks for too long, we want to process each table in its own
     * transaction.  This forces us to disallow running inside a user
     * transaction block.
     */
    PreventInTransactionBlock(isTopLevel, b"CLUSTER\0".as_ptr() as *const c_char);

    /* Also, we need a memory context to hold our list of relations */
    cluster_context = AllocSetContextCreate(PortalContext,
                                            b"Cluster\0".as_ptr() as *const c_char,
                                            ALLOCSET_DEFAULT_MINSIZE,
                                            ALLOCSET_DEFAULT_INITSIZE,
                                            ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Either we're processing a partitioned table, or we were not given any
     * table name at all.  In either case, obtain a list of relations to
     * process.
     *
     * In the former case, an index name must have been given, so we don't
     * need to recheck its "indisclustered" bit, but we have to check that it
     * is an index that we can cluster on.  In the latter case, we set the
     * option bit to have indisclustered verified.
     *
     * Rechecking the relation itself is necessary here in all cases.
     */
    params.options |= CLUOPT_RECHECK;
    if !rel.is_null() {
        debug_assert!((*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE);
        check_index_is_clusterable(rel, indexOid, AccessShareLock);
        rtcs = get_tables_to_cluster_partitioned(cluster_context, indexOid);

        /* close relation, releasing lock on parent table */
        table_close(rel, AccessExclusiveLock);
    } else {
        rtcs = get_tables_to_cluster(cluster_context);
        params.options |= CLUOPT_RECHECK_ISCLUSTERED;
    }

    /* Do the job. */
    cluster_multiple_rels(rtcs, &mut params);

    /* Start a new transaction for the cleanup work. */
    StartTransactionCommand();

    /* Clean up working storage */
    MemoryContextDelete(cluster_context);
}

/*
 * Given a list of relations to cluster, process each of them in a separate
 * transaction.
 *
 * We expect to be in a transaction at start, but there isn't one when we
 * return.
 */
unsafe fn cluster_multiple_rels(rtcs: *mut List, params: *mut ClusterParams)
{
    let mut lc: *mut ListCell;

    /* Commit to get out of starting transaction */
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Cluster the tables, each in a separate transaction */
    foreach!(lc, rtcs, {
        let rtc = lfirst(current_cell!(lc)) as *mut RelToCluster;
        let rel: Relation;

        /* Start a new transaction for each relation. */
        StartTransactionCommand();

        /* functions in indexes may want a snapshot set */
        PushActiveSnapshot(GetTransactionSnapshot());

        rel = table_open((*rtc).tableOid, AccessExclusiveLock);

        /* Process this table */
        cluster_rel(rel, (*rtc).indexOid, params);
        /* cluster_rel closes the relation, but keeps lock */

        PopActiveSnapshot();
        CommitTransactionCommand();
    });
}

/*
 * cluster_rel
 *
 * This clusters the table by creating a new, clustered table and
 * swapping the relfilenumbers of the new table and the old table, so
 * the OID of the original table is preserved.  Thus we do not lose
 * GRANT, inheritance nor references to this table.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new table, it's better to create the indexes afterwards than to fill
 * them incrementally while we load the table.
 *
 * If indexOid is InvalidOid, the table will be rewritten in physical order
 * instead of index order.  This is the new implementation of VACUUM FULL,
 * and error messages should refer to the operation as VACUUM not CLUSTER.
 */
pub unsafe fn cluster_rel(OldHeap: Relation, indexOid: Oid, params: *mut ClusterParams)
{
    let tableOid: Oid = RelationGetRelid(OldHeap);
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    let save_nestlevel: c_int;
    let verbose: bool = ((*params).options & CLUOPT_VERBOSE) != 0;
    let recheck: bool = ((*params).options & CLUOPT_RECHECK) != 0;
    let mut index: Relation;

    debug_assert!(CheckRelationLockedByMe(OldHeap, AccessExclusiveLock, false));

    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();

    pgstat_progress_start_command(PROGRESS_COMMAND_CLUSTER, tableOid);
    if OidIsValid(indexOid) {
        pgstat_progress_update_param(PROGRESS_CLUSTER_COMMAND,
                                     PROGRESS_CLUSTER_COMMAND_CLUSTER);
    } else {
        pgstat_progress_update_param(PROGRESS_CLUSTER_COMMAND,
                                     PROGRESS_CLUSTER_COMMAND_VACUUM_FULL);
    }

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext((*(*OldHeap).rd_rel).relowner,
                           save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    /*
     * Since we may open a new transaction for each relation, we have to check
     * that the relation still is what we think it is.
     *
     * If this is a single-transaction CLUSTER, we can skip these tests. We
     * *must* skip the one on indisclustered since it would reject an attempt
     * to cluster a not-previously-clustered index.
     */
    'out: loop {
        if recheck {
            /* Check that the user still has privileges for the relation */
            if !cluster_is_permitted_for_relation(tableOid, save_userid) {
                relation_close(OldHeap, AccessExclusiveLock);
                break 'out;
            }

            /*
             * Silently skip a temp table for a remote session.  Only doing this
             * check in the "recheck" case is appropriate (which currently means
             * somebody is executing a database-wide CLUSTER or on a partitioned
             * table), because there is another check in cluster() which will stop
             * any attempt to cluster remote temp tables by name.  There is
             * another check in cluster_rel which is redundant, but we leave it
             * for extra safety.
             */
            if RELATION_IS_OTHER_TEMP(OldHeap) {
                relation_close(OldHeap, AccessExclusiveLock);
                break 'out;
            }

            if OidIsValid(indexOid) {
                /*
                 * Check that the index still exists
                 */
                if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(indexOid)) {
                    relation_close(OldHeap, AccessExclusiveLock);
                    break 'out;
                }

                /*
                 * Check that the index is still the one with indisclustered set,
                 * if needed.
                 */
                if ((*params).options & CLUOPT_RECHECK_ISCLUSTERED) != 0 &&
                    !get_index_isclustered(indexOid) {
                    relation_close(OldHeap, AccessExclusiveLock);
                    break 'out;
                }
            }
        }

        /*
         * We allow VACUUM FULL, but not CLUSTER, on shared catalogs.  CLUSTER
         * would work in most respects, but the index would only get marked as
         * indisclustered in the current database, leading to unexpected behavior
         * if CLUSTER were later invoked in another database.
         */
        if OidIsValid(indexOid) && (*(*OldHeap).rd_rel).relisshared {
            ereport!(ERROR,
                     errmsg!("cannot cluster a shared catalog"));
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        }

        /*
         * Don't process temp tables of other backends ... their local buffer
         * manager is not going to cope.
         */
        if RELATION_IS_OTHER_TEMP(OldHeap) {
            if OidIsValid(indexOid) {
                ereport!(ERROR,
                         errmsg!("cannot cluster temporary tables of other sessions"));
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            } else {
                ereport!(ERROR,
                         errmsg!("cannot vacuum temporary tables of other sessions"));
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            }
        }

        /*
         * Also check for active uses of the relation in the current transaction,
         * including open scans and pending AFTER trigger events.
         */
        CheckTableNotInUse(OldHeap,
                           if OidIsValid(indexOid) { b"CLUSTER\0".as_ptr() as *const c_char }
                           else { b"VACUUM\0".as_ptr() as *const c_char });

        /* Check heap and index are valid to cluster on */
        if OidIsValid(indexOid) {
            /* verify the index is good and lock it */
            check_index_is_clusterable(OldHeap, indexOid, AccessExclusiveLock);
            /* also open it */
            index = index_open(indexOid, NoLock);
        } else {
            index = core::ptr::null_mut();
        }

        /*
         * Quietly ignore the request if this is a materialized view which has not
         * been populated from its query. No harm is done because there is no data
         * to deal with, and we don't want to throw an error if this is part of a
         * multi-relation request -- for example, CLUSTER was run on the entire
         * database.
         */
        if (*(*OldHeap).rd_rel).relkind == RELKIND_MATVIEW &&
            !RelationIsPopulated(OldHeap) {
            relation_close(OldHeap, AccessExclusiveLock);
            break 'out;
        }

        debug_assert!((*(*OldHeap).rd_rel).relkind == RELKIND_RELATION ||
                      (*(*OldHeap).rd_rel).relkind == RELKIND_MATVIEW ||
                      (*(*OldHeap).rd_rel).relkind == RELKIND_TOASTVALUE);

        /*
         * All predicate locks on the tuples or pages are about to be made
         * invalid, because we move tuples around.  Promote them to relation
         * locks.  Predicate locks on indexes will be promoted when they are
         * reindexed.
         */
        TransferPredicateLocksToHeapRelation(OldHeap);

        /* rebuild_relation does all the dirty work */
        rebuild_relation(OldHeap, index, verbose);
        /* rebuild_relation closes OldHeap, and index if valid */

        break 'out;
    }

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    pgstat_progress_end_command();
}

/*
 * Verify that the specified heap and index are valid to cluster on
 *
 * Side effect: obtains lock on the index.  The caller may
 * in some cases already have AccessExclusiveLock on the table, but
 * not in all cases so we can't rely on the table-level lock for
 * protection here.
 */
pub unsafe fn check_index_is_clusterable(OldHeap: Relation, indexOid: Oid, lockmode: LOCKMODE)
{
    let OldIndex: Relation;

    OldIndex = index_open(indexOid, lockmode);

    /*
     * Check that index is in fact an index on the given relation
     */
    if (*OldIndex).rd_index.is_null() ||
        (*(*OldIndex).rd_index).indrelid != RelationGetRelid(OldHeap) {
        ereport!(ERROR,
                 errmsg!("\"{}\" is not an index for table \"{}\"",
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldIndex)).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* Index AM must allow clustering */
    if !(*(*OldIndex).rd_indam).amclusterable {
        ereport!(ERROR,
                 errmsg!("cannot cluster on index \"{}\" because access method does not support clustering",
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldIndex)).to_string_lossy()));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /*
     * Disallow clustering on incomplete indexes (those that might not index
     * every row of the relation).  We could relax this by making a separate
     * seqscan pass over the table to copy the missing rows, but that seems
     * expensive and tedious.
     */
    if !heap_attisnull((*OldIndex).rd_indextuple as HeapTuple, Anum_pg_index_indpred, core::ptr::null_mut()) {
        ereport!(ERROR,
                 errmsg!("cannot cluster on partial index \"{}\"",
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldIndex)).to_string_lossy()));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /*
     * Disallow if index is left over from a failed CREATE INDEX CONCURRENTLY;
     * it might well not contain entries for every heap row, or might not even
     * be internally consistent.  (But note that we don't check indcheckxmin;
     * the worst consequence of following broken HOT chains would be that we
     * might put recently-dead tuples out-of-order in the new table, and there
     * is little harm in that.)
     */
    if !(*(*OldIndex).rd_index).indisvalid {
        ereport!(ERROR,
                 errmsg!("cannot cluster on invalid index \"{}\"",
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldIndex)).to_string_lossy()));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /* Drop relcache refcnt on OldIndex, but keep lock */
    index_close(OldIndex, NoLock);
}

/*
 * mark_index_clustered: mark the specified index as the one clustered on
 *
 * With indexOid == InvalidOid, will mark all indexes of rel not-clustered.
 */
pub unsafe fn mark_index_clustered(rel: Relation, indexOid: Oid, is_internal: bool)
{
    let mut indexTuple: HeapTuple;
    let mut indexForm: Form_pg_index;
    let pg_index: Relation;
    let mut index: *mut ListCell;

    /* Disallow applying to a partitioned table */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        ereport!(ERROR,
                 errmsg!("cannot mark index clustered in partitioned table"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /*
     * If the index is already marked clustered, no need to do anything.
     */
    if OidIsValid(indexOid) {
        if get_index_isclustered(indexOid) {
            return;
        }
    }

    /*
     * Check each index of the relation and set/clear the bit as needed.
     */
    pg_index = table_open(IndexRelationId, RowExclusiveLock);

    foreach!(index, RelationGetIndexList(rel), {
        let thisIndexOid: Oid = lfirst_oid(current_cell!(index));

        indexTuple = SearchSysCacheCopy1(INDEXRELID,
                                         ObjectIdGetDatum(thisIndexOid));
        if !HeapTupleIsValid(indexTuple) {
            elog!(ERROR, "cache lookup failed for index {}", thisIndexOid);
        }
        indexForm = GETSTRUCT_pg_index(indexTuple);

        /*
         * Unset the bit if set.  We know it's wrong because we checked this
         * earlier.
         */
        if (*indexForm).indisclustered {
            (*indexForm).indisclustered = false;
            CatalogTupleUpdate(pg_index, &mut (*indexTuple).t_self as *mut _ as *mut c_void, indexTuple);
        } else if thisIndexOid == indexOid {
            /* this was checked earlier, but let's be real sure */
            if !(*indexForm).indisvalid {
                elog!(ERROR, "cannot cluster on invalid index {}", indexOid);
            }
            (*indexForm).indisclustered = true;
            CatalogTupleUpdate(pg_index, &mut (*indexTuple).t_self as *mut _ as *mut c_void, indexTuple);
        }

        InvokeObjectPostAlterHookArg(IndexRelationId, thisIndexOid, 0,
                                     InvalidOid, is_internal);

        heap_freetuple(indexTuple);
    });

    table_close(pg_index, RowExclusiveLock);
}

/*
 * rebuild_relation: rebuild an existing relation in index or physical order
 *
 * OldHeap: table to rebuild.
 * index: index to cluster by, or NULL to rewrite in physical order.
 *
 * On entry, heap and index (if one is given) must be open, and
 * AccessExclusiveLock held on them.
 * On exit, they are closed, but locks on them are not released.
 */
unsafe fn rebuild_relation(OldHeap: Relation, index: Relation, verbose: bool)
{
    let tableOid: Oid = RelationGetRelid(OldHeap);
    let accessMethod: Oid = (*(*OldHeap).rd_rel).relam;
    let tableSpace: Oid = (*(*OldHeap).rd_rel).reltablespace;
    let OIDNewHeap: Oid;
    let NewHeap: Relation;
    let relpersistence: c_char;
    let is_system_catalog: bool;
    let mut swap_toast_by_content: bool = false;
    let mut frozenXid: TransactionId = 0;
    let mut cutoffMulti: MultiXactId = 0;

    debug_assert!(CheckRelationLockedByMe(OldHeap, AccessExclusiveLock, false) &&
                  (index.is_null() || CheckRelationLockedByMe(index, AccessExclusiveLock, false)));

    if !index.is_null() {
        /* Mark the correct index as clustered */
        mark_index_clustered(OldHeap, RelationGetRelid(index), true);
    }

    /* Remember info about rel before closing OldHeap */
    relpersistence = (*(*OldHeap).rd_rel).relpersistence;
    is_system_catalog = IsSystemRelation(OldHeap);

    /*
     * Create the transient table that will receive the re-ordered data.
     *
     * OldHeap is already locked, so no need to lock it again.  make_new_heap
     * obtains AccessExclusiveLock on the new heap and its toast table.
     */
    OIDNewHeap = make_new_heap(tableOid, tableSpace,
                               accessMethod,
                               relpersistence,
                               NoLock);
    debug_assert!(CheckRelationOidLockedByMe(OIDNewHeap, AccessExclusiveLock, false));
    NewHeap = table_open(OIDNewHeap, NoLock);

    /* Copy the heap data into the new table in the desired order */
    copy_table_data(NewHeap, OldHeap, index, verbose,
                    &mut swap_toast_by_content, &mut frozenXid, &mut cutoffMulti);


    /* Close relcache entries, but keep lock until transaction commit */
    table_close(OldHeap, NoLock);
    if !index.is_null() {
        index_close(index, NoLock);
    }

    /*
     * Close the new relation so it can be dropped as soon as the storage is
     * swapped. The relation is not visible to others, so no need to unlock it
     * explicitly.
     */
    table_close(NewHeap, NoLock);

    /*
     * Swap the physical files of the target and transient tables, then
     * rebuild the target's indexes and throw away the transient table.
     */
    finish_heap_swap(tableOid, OIDNewHeap, is_system_catalog,
                     swap_toast_by_content, false, true,
                     frozenXid, cutoffMulti,
                     relpersistence);
}


/*
 * Create the transient table that will be filled with new data during
 * CLUSTER, ALTER TABLE, and similar operations.  The transient table
 * duplicates the logical structure of the OldHeap; but will have the
 * specified physical storage properties NewTableSpace, NewAccessMethod, and
 * relpersistence.
 *
 * After this, the caller should load the new heap with transferred/modified
 * data, then call finish_heap_swap to complete the operation.
 */
pub unsafe fn make_new_heap(OIDOldHeap: Oid, NewTableSpace: Oid, NewAccessMethod: Oid,
                            relpersistence: c_char, lockmode: LOCKMODE) -> Oid
{
    let OldHeapDesc: TupleDesc;
    let mut NewHeapName: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let OIDNewHeap: Oid;
    let toastid: Oid;
    let OldHeap: Relation;
    let mut tuple: HeapTuple;
    let mut reloptions: Datum;
    let mut isNull: bool = false;
    let namespaceid: Oid;

    OldHeap = table_open(OIDOldHeap, lockmode);
    OldHeapDesc = RelationGetDescr(OldHeap);

    /*
     * Note that the NewHeap will not receive any of the defaults or
     * constraints associated with the OldHeap; we don't need 'em, and there's
     * no reason to spend cycles inserting them into the catalogs only to
     * delete them.
     */

    /*
     * But we do want to use reloptions of the old heap for new heap.
     */
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(OIDOldHeap));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", OIDOldHeap);
    }
    reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
                                 &mut isNull);
    if isNull {
        reloptions = 0 as Datum;
    }

    if relpersistence == RELPERSISTENCE_TEMP {
        namespaceid = LookupCreationNamespace(b"pg_temp\0".as_ptr() as *const c_char);
    } else {
        namespaceid = RelationGetNamespace(OldHeap);
    }

    /*
     * Create the new heap, using a temporary name in the same namespace as
     * the existing table.  NOTE: there is some risk of collision with user
     * relnames.  Working around this seems more trouble than it's worth; in
     * particular, we can't create the new heap in a different namespace from
     * the old, or we will have problems with the TEMP status of temp tables.
     *
     * Note: the new heap is not a shared relation, even if we are rebuilding
     * a shared rel.  However, we do make the new heap mapped if the source is
     * mapped.  This simplifies swap_relation_files, and is absolutely
     * necessary for rebuilding pg_class, for reasons explained there.
     */
    snprintf(NewHeapName.as_mut_ptr(), core::mem::size_of::<[c_char; NAMEDATALEN]>(),
             b"pg_temp_%u\0".as_ptr() as *const c_char, OIDOldHeap);

    OIDNewHeap = heap_create_with_catalog(NewHeapName.as_ptr(),
                                          namespaceid,
                                          NewTableSpace,
                                          InvalidOid,
                                          InvalidOid,
                                          InvalidOid,
                                          (*(*OldHeap).rd_rel).relowner,
                                          NewAccessMethod,
                                          OldHeapDesc,
                                          NIL as *mut List,
                                          RELKIND_RELATION,
                                          relpersistence,
                                          false,
                                          RelationIsMapped(OldHeap),
                                          ONCOMMIT_NOOP,
                                          reloptions,
                                          false,
                                          true,
                                          true,
                                          OIDOldHeap,
                                          core::ptr::null_mut());
    debug_assert!(OIDNewHeap != InvalidOid);

    ReleaseSysCache(tuple);

    /*
     * Advance command counter so that the newly-created relation's catalog
     * tuples will be visible to table_open.
     */
    CommandCounterIncrement();

    /*
     * If necessary, create a TOAST table for the new relation.
     *
     * If the relation doesn't have a TOAST table already, we can't need one
     * for the new relation.  The other way around is possible though: if some
     * wide columns have been dropped, NewHeapCreateToastTable can decide that
     * no TOAST table is needed for the new table.
     *
     * Note that NewHeapCreateToastTable ends with CommandCounterIncrement, so
     * that the TOAST table will be visible for insertion.
     */
    toastid = (*(*OldHeap).rd_rel).reltoastrelid;
    if OidIsValid(toastid) {
        /* keep the existing toast table's reloptions, if any */
        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", toastid);
        }
        reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
                                     &mut isNull);
        if isNull {
            reloptions = 0 as Datum;
        }

        NewHeapCreateToastTable(OIDNewHeap, reloptions, lockmode, toastid);

        ReleaseSysCache(tuple);
    }

    table_close(OldHeap, NoLock);

    return OIDNewHeap;
}

/*
 * Do the physical copying of table data.
 *
 * There are three output parameters:
 * *pSwapToastByContent is set true if toast tables must be swapped by content.
 * *pFreezeXid receives the TransactionId used as freeze cutoff point.
 * *pCutoffMulti receives the MultiXactId used as a cutoff point.
 */
unsafe fn copy_table_data(NewHeap: Relation, OldHeap: Relation, OldIndex: Relation, verbose: bool,
                          pSwapToastByContent: *mut bool, pFreezeXid: *mut TransactionId,
                          pCutoffMulti: *mut MultiXactId)
{
    let relRelation: Relation;
    let reltup: HeapTuple;
    let relform: Form_pg_class;
    let oldTupDesc: TupleDesc; /* PG_USED_FOR_ASSERTS_ONLY */
    let newTupDesc: TupleDesc; /* PG_USED_FOR_ASSERTS_ONLY */
    let mut params: VacuumParams = core::mem::zeroed();
    let mut cutoffs: VacuumCutoffs = core::mem::zeroed();
    let use_sort: bool;
    let mut num_tuples: f64 = 0.0;
    let mut tups_vacuumed: f64 = 0.0;
    let mut tups_recently_dead: f64 = 0.0;
    let num_pages: BlockNumber;
    let elevel: c_int = if verbose { INFO } else { DEBUG2 };
    let mut ru0: PGRUsage = core::mem::zeroed();
    let nspname: *mut c_char;

    pg_rusage_init(&mut ru0);

    /* Store a copy of the namespace name for logging purposes */
    nspname = get_namespace_name(RelationGetNamespace(OldHeap));

    /*
     * Their tuple descriptors should be exactly alike, but here we only need
     * assume that they have the same number of columns.
     */
    oldTupDesc = RelationGetDescr(OldHeap);
    newTupDesc = RelationGetDescr(NewHeap);
    debug_assert!((*newTupDesc).natts == (*oldTupDesc).natts);

    /*
     * If the OldHeap has a toast table, get lock on the toast table to keep
     * it from being vacuumed.  This is needed because autovacuum processes
     * toast tables independently of their main tables, with no lock on the
     * latter.  If an autovacuum were to start on the toast table after we
     * compute our OldestXmin below, it would use a later OldestXmin, and then
     * possibly remove as DEAD toast tuples belonging to main tuples we think
     * are only RECENTLY_DEAD.  Then we'd fail while trying to copy those
     * tuples.
     *
     * We don't need to open the toast relation here, just lock it.  The lock
     * will be held till end of transaction.
     */
    if (*(*OldHeap).rd_rel).reltoastrelid != InvalidOid {
        LockRelationOid((*(*OldHeap).rd_rel).reltoastrelid, AccessExclusiveLock);
    }

    /*
     * If both tables have TOAST tables, perform toast swap by content.  It is
     * possible that the old table has a toast table but the new one doesn't,
     * if toastable columns have been dropped.  In that case we have to do
     * swap by links.  This is okay because swap by content is only essential
     * for system catalogs, and we don't support schema changes for them.
     */
    if (*(*OldHeap).rd_rel).reltoastrelid != InvalidOid && (*(*NewHeap).rd_rel).reltoastrelid != InvalidOid {
        *pSwapToastByContent = true;

        /*
         * When doing swap by content, any toast pointers written into NewHeap
         * must use the old toast table's OID, because that's where the toast
         * data will eventually be found.  Set this up by setting rd_toastoid.
         * This also tells toast_save_datum() to preserve the toast value
         * OIDs, which we want so as not to invalidate toast pointers in
         * system catalog caches, and to avoid making multiple copies of a
         * single toast value.
         *
         * Note that we must hold NewHeap open until we are done writing data,
         * since the relcache will not guarantee to remember this setting once
         * the relation is closed.  Also, this technique depends on the fact
         * that no one will try to read from the NewHeap until after we've
         * finished writing it and swapping the rels --- otherwise they could
         * follow the toast pointers to the wrong place.  (It would actually
         * work for values copied over from the old toast table, but not for
         * any values that we toast which were previously not toasted.)
         */
        (*NewHeap).rd_toastoid = (*(*OldHeap).rd_rel).reltoastrelid;
    } else {
        *pSwapToastByContent = false;
    }

    /*
     * Compute xids used to freeze and weed out dead tuples and multixacts.
     * Since we're going to rewrite the whole table anyway, there's no reason
     * not to be aggressive about this.
     */
    memset(&mut params as *mut VacuumParams as *mut c_void, 0, core::mem::size_of::<VacuumParams>());
    vacuum_get_cutoffs(OldHeap, &params, &mut cutoffs);

    /*
     * FreezeXid will become the table's new relfrozenxid, and that mustn't go
     * backwards, so take the max.
     */
    {
        let relfrozenxid: TransactionId = (*(*OldHeap).rd_rel).relfrozenxid;

        if TransactionIdIsValid(relfrozenxid) &&
            TransactionIdPrecedes(cutoffs.FreezeLimit, relfrozenxid) {
            cutoffs.FreezeLimit = relfrozenxid;
        }
    }

    /*
     * MultiXactCutoff, similarly, shouldn't go backwards either.
     */
    {
        let relminmxid: MultiXactId = (*(*OldHeap).rd_rel).relminmxid;

        if MultiXactIdIsValid(relminmxid) &&
            MultiXactIdPrecedes(cutoffs.MultiXactCutoff, relminmxid) {
            cutoffs.MultiXactCutoff = relminmxid;
        }
    }

    /*
     * Decide whether to use an indexscan or seqscan-and-optional-sort to scan
     * the OldHeap.  We know how to use a sort to duplicate the ordering of a
     * btree index, and will use seqscan-and-sort for that case if the planner
     * tells us it's cheaper.  Otherwise, always indexscan if an index is
     * provided, else plain seqscan.
     */
    if !OldIndex.is_null() && (*(*OldIndex).rd_rel).relam == BTREE_AM_OID {
        use_sort = plan_cluster_use_sort(RelationGetRelid(OldHeap),
                                         RelationGetRelid(OldIndex));
    } else {
        use_sort = false;
    }

    /* Log what we're doing */
    if !OldIndex.is_null() && !use_sort {
        ereport!(elevel,
                 errmsg!("clustering \"{}.{}\" using index scan on \"{}\"",
                         std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldIndex)).to_string_lossy()));
    } else if use_sort {
        ereport!(elevel,
                 errmsg!("clustering \"{}.{}\" using sequential scan and sort",
                         std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy()));
    } else {
        ereport!(elevel,
                 errmsg!("vacuuming \"{}.{}\"",
                         std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy()));
    }

    /*
     * Hand off the actual copying to AM specific function, the generic code
     * cannot know how to deal with visibility across AMs. Note that this
     * routine is allowed to set FreezeXid / MultiXactCutoff to different
     * values (e.g. because the AM doesn't use freezing).
     */
    table_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
                                    cutoffs.OldestXmin, &mut cutoffs.FreezeLimit,
                                    &mut cutoffs.MultiXactCutoff,
                                    &mut num_tuples, &mut tups_vacuumed,
                                    &mut tups_recently_dead);

    /* return selected values to caller, get set as relfrozenxid/minmxid */
    *pFreezeXid = cutoffs.FreezeLimit;
    *pCutoffMulti = cutoffs.MultiXactCutoff;

    /* Reset rd_toastoid just to be tidy --- it shouldn't be looked at again */
    (*NewHeap).rd_toastoid = InvalidOid;

    num_pages = RelationGetNumberOfBlocks(NewHeap);

    /* Log what we did */
    ereport!(elevel,
             errmsg!("\"{}.{}\": found {:.0} removable, {:.0} nonremovable row versions in {} pages",
                     std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                     std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy(),
                     tups_vacuumed, num_tuples,
                     RelationGetNumberOfBlocks(OldHeap)));
    /* C also: errdetail("%.0f dead row versions cannot be removed yet.\n%s.",
       tups_recently_dead, pg_rusage_show(&ru0)) */

    /* Update pg_class to reflect the correct values of pages and tuples. */
    relRelation = table_open(RelationRelationId, RowExclusiveLock);

    reltup = SearchSysCacheCopy1(RELOID,
                                 ObjectIdGetDatum(RelationGetRelid(NewHeap)));
    if !HeapTupleIsValid(reltup) {
        elog!(ERROR, "cache lookup failed for relation {}",
              RelationGetRelid(NewHeap));
    }
    relform = GETSTRUCT_pg_class(reltup);

    (*relform).relpages = num_pages as int32;
    (*relform).reltuples = num_tuples as float4;

    /* Don't update the stats for pg_class.  See swap_relation_files. */
    if RelationGetRelid(OldHeap) != RelationRelationId {
        CatalogTupleUpdate(relRelation, &mut (*reltup).t_self as *mut _ as *mut c_void, reltup);
    } else {
        CacheInvalidateRelcacheByTuple(reltup);
    }

    /* Clean up. */
    heap_freetuple(reltup);
    table_close(relRelation, RowExclusiveLock);

    /* Make the update visible */
    CommandCounterIncrement();
}

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace, relfilenumber) while keeping
 * the same logical identities of the two relations.  relpersistence is also
 * swapped, which is critical since it determines where buffers live for each
 * relation.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries.  The former is needed
 * to manage rewrites of shared catalogs (where we cannot change the pg_class
 * links) while the latter is the only way to handle cases in which a toast
 * table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 * Lastly, if r2 and its toast table and toast index (if any) are mapped,
 * their OIDs are emitted into mapped_tables[].  This is hacky but beats
 * having to look the information up again later in finish_heap_swap.
 */
unsafe fn swap_relation_files(r1: Oid, r2: Oid, target_is_pg_class: bool,
                              swap_toast_by_content: bool,
                              is_internal: bool,
                              frozenXid: TransactionId,
                              cutoffMulti: MultiXactId,
                              mapped_tables: *mut Oid)
{
    let relRelation: Relation;
    let reltup1: HeapTuple;
    let reltup2: HeapTuple;
    let relform1: Form_pg_class;
    let relform2: Form_pg_class;
    let mut relfilenumber1: RelFileNumber;
    let mut relfilenumber2: RelFileNumber;
    let mut swaptemp: RelFileNumber;
    let swptmpchr: c_char;
    let relam1: Oid;
    let relam2: Oid;
    let mut mapped_tables: *mut Oid = mapped_tables;

    /* We need writable copies of both pg_class tuples. */
    relRelation = table_open(RelationRelationId, RowExclusiveLock);

    reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
    if !HeapTupleIsValid(reltup1) {
        elog!(ERROR, "cache lookup failed for relation {}", r1);
    }
    relform1 = GETSTRUCT_pg_class(reltup1);

    reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
    if !HeapTupleIsValid(reltup2) {
        elog!(ERROR, "cache lookup failed for relation {}", r2);
    }
    relform2 = GETSTRUCT_pg_class(reltup2);

    relfilenumber1 = (*relform1).relfilenode;
    relfilenumber2 = (*relform2).relfilenode;
    relam1 = (*relform1).relam;
    relam2 = (*relform2).relam;

    if RelFileNumberIsValid(relfilenumber1) &&
        RelFileNumberIsValid(relfilenumber2) {
        /*
         * Normal non-mapped relations: swap relfilenumbers, reltablespaces,
         * relpersistence
         */
        debug_assert!(!target_is_pg_class);

        swaptemp = (*relform1).relfilenode;
        (*relform1).relfilenode = (*relform2).relfilenode;
        (*relform2).relfilenode = swaptemp;

        swaptemp = (*relform1).reltablespace;
        (*relform1).reltablespace = (*relform2).reltablespace;
        (*relform2).reltablespace = swaptemp;

        swaptemp = (*relform1).relam;
        (*relform1).relam = (*relform2).relam;
        (*relform2).relam = swaptemp;

        swptmpchr = (*relform1).relpersistence;
        (*relform1).relpersistence = (*relform2).relpersistence;
        (*relform2).relpersistence = swptmpchr;

        /* Also swap toast links, if we're swapping by links */
        if !swap_toast_by_content {
            swaptemp = (*relform1).reltoastrelid;
            (*relform1).reltoastrelid = (*relform2).reltoastrelid;
            (*relform2).reltoastrelid = swaptemp;
        }
    } else {
        /*
         * Mapped-relation case.  Here we have to swap the relation mappings
         * instead of modifying the pg_class columns.  Both must be mapped.
         */
        if RelFileNumberIsValid(relfilenumber1) ||
            RelFileNumberIsValid(relfilenumber2) {
            elog!(ERROR, "cannot swap mapped relation \"{}\" with non-mapped relation",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform1)).to_string_lossy());
        }

        /*
         * We can't change the tablespace nor persistence of a mapped rel, and
         * we can't handle toast link swapping for one either, because we must
         * not apply any critical changes to its pg_class row.  These cases
         * should be prevented by upstream permissions tests, so these checks
         * are non-user-facing emergency backstop.
         */
        if (*relform1).reltablespace != (*relform2).reltablespace {
            elog!(ERROR, "cannot change tablespace of mapped relation \"{}\"",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform1)).to_string_lossy());
        }
        if (*relform1).relpersistence != (*relform2).relpersistence {
            elog!(ERROR, "cannot change persistence of mapped relation \"{}\"",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform1)).to_string_lossy());
        }
        if (*relform1).relam != (*relform2).relam {
            elog!(ERROR, "cannot change access method of mapped relation \"{}\"",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform1)).to_string_lossy());
        }
        if !swap_toast_by_content &&
            ((*relform1).reltoastrelid != InvalidOid || (*relform2).reltoastrelid != InvalidOid) {
            elog!(ERROR, "cannot swap toast by links for mapped relation \"{}\"",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform1)).to_string_lossy());
        }

        /*
         * Fetch the mappings --- shouldn't fail, but be paranoid
         */
        relfilenumber1 = RelationMapOidToFilenumber(r1, (*relform1).relisshared);
        if !RelFileNumberIsValid(relfilenumber1) {
            elog!(ERROR, "could not find relation mapping for relation \"{}\", OID {}",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform1)).to_string_lossy(), r1);
        }
        relfilenumber2 = RelationMapOidToFilenumber(r2, (*relform2).relisshared);
        if !RelFileNumberIsValid(relfilenumber2) {
            elog!(ERROR, "could not find relation mapping for relation \"{}\", OID {}",
                  std::ffi::CStr::from_ptr(NameStr_relname(relform2)).to_string_lossy(), r2);
        }

        /*
         * Send replacement mappings to relmapper.  Note these won't actually
         * take effect until CommandCounterIncrement.
         */
        RelationMapUpdateMap(r1, relfilenumber2, (*relform1).relisshared, false);
        RelationMapUpdateMap(r2, relfilenumber1, (*relform2).relisshared, false);

        /* Pass OIDs of mapped r2 tables back to caller */
        *mapped_tables = r2;
        mapped_tables = mapped_tables.add(1);
    }

    /*
     * Recognize that rel1's relfilenumber (swapped from rel2) is new in this
     * subtransaction. The rel2 storage (swapped from rel1) may or may not be
     * new.
     */
    {
        let rel1: Relation;
        let rel2: Relation;

        rel1 = relation_open(r1, NoLock);
        rel2 = relation_open(r2, NoLock);
        (*rel2).rd_createSubid = (*rel1).rd_createSubid;
        (*rel2).rd_newRelfilelocatorSubid = (*rel1).rd_newRelfilelocatorSubid;
        (*rel2).rd_firstRelfilelocatorSubid = (*rel1).rd_firstRelfilelocatorSubid;
        RelationAssumeNewRelfilelocator(rel1);
        relation_close(rel1, NoLock);
        relation_close(rel2, NoLock);
    }

    /*
     * In the case of a shared catalog, these next few steps will only affect
     * our own database's pg_class row; but that's okay, because they are all
     * noncritical updates.  That's also an important fact for the case of a
     * mapped catalog, because it's possible that we'll commit the map change
     * and then fail to commit the pg_class update.
     */

    /* set rel1's frozen Xid and minimum MultiXid */
    if (*relform1).relkind != RELKIND_INDEX {
        debug_assert!(!TransactionIdIsValid(frozenXid) ||
                      TransactionIdIsNormal(frozenXid));
        (*relform1).relfrozenxid = frozenXid;
        (*relform1).relminmxid = cutoffMulti;
    }

    /* swap size statistics too, since new rel has freshly-updated stats */
    {
        let mut swap_pages: int32;
        let mut swap_tuples: float4;
        let mut swap_allvisible: int32;
        let mut swap_allfrozen: int32;

        swap_pages = (*relform1).relpages;
        (*relform1).relpages = (*relform2).relpages;
        (*relform2).relpages = swap_pages;

        swap_tuples = (*relform1).reltuples;
        (*relform1).reltuples = (*relform2).reltuples;
        (*relform2).reltuples = swap_tuples;

        swap_allvisible = (*relform1).relallvisible;
        (*relform1).relallvisible = (*relform2).relallvisible;
        (*relform2).relallvisible = swap_allvisible;

        swap_allfrozen = (*relform1).relallfrozen;
        (*relform1).relallfrozen = (*relform2).relallfrozen;
        (*relform2).relallfrozen = swap_allfrozen;
    }

    /*
     * Update the tuples in pg_class --- unless the target relation of the
     * swap is pg_class itself.  In that case, there is zero point in making
     * changes because we'd be updating the old data that we're about to throw
     * away.  Because the real work being done here for a mapped relation is
     * just to change the relation map settings, it's all right to not update
     * the pg_class rows in this case. The most important changes will instead
     * performed later, in finish_heap_swap() itself.
     */
    if !target_is_pg_class {
        let indstate: CatalogIndexState;

        indstate = CatalogOpenIndexes(relRelation);
        CatalogTupleUpdateWithInfo(relRelation, &mut (*reltup1).t_self as *mut _ as *mut c_void, reltup1,
                                   indstate);
        CatalogTupleUpdateWithInfo(relRelation, &mut (*reltup2).t_self as *mut _ as *mut c_void, reltup2,
                                   indstate);
        CatalogCloseIndexes(indstate);
    } else {
        /* no update ... but we do still need relcache inval */
        CacheInvalidateRelcacheByTuple(reltup1);
        CacheInvalidateRelcacheByTuple(reltup2);
    }

    /*
     * Now that pg_class has been updated with its relevant information for
     * the swap, update the dependency of the relations to point to their new
     * table AM, if it has changed.
     */
    if relam1 != relam2 {
        if changeDependencyFor(RelationRelationId,
                               r1,
                               AccessMethodRelationId,
                               relam1,
                               relam2) != 1 {
            elog!(ERROR, "could not change access method dependency for relation \"{}.{}\"",
                  std::ffi::CStr::from_ptr(get_namespace_name(get_rel_namespace(r1))).to_string_lossy(),
                  std::ffi::CStr::from_ptr(get_rel_name(r1)).to_string_lossy());
        }
        if changeDependencyFor(RelationRelationId,
                               r2,
                               AccessMethodRelationId,
                               relam2,
                               relam1) != 1 {
            elog!(ERROR, "could not change access method dependency for relation \"{}.{}\"",
                  std::ffi::CStr::from_ptr(get_namespace_name(get_rel_namespace(r2))).to_string_lossy(),
                  std::ffi::CStr::from_ptr(get_rel_name(r2)).to_string_lossy());
        }
    }

    /*
     * Post alter hook for modified relations. The change to r2 is always
     * internal, but r1 depends on the invocation context.
     */
    InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0,
                                 InvalidOid, is_internal);
    InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0,
                                 InvalidOid, true);

    /*
     * If we have toast tables associated with the relations being swapped,
     * deal with them too.
     */
    if (*relform1).reltoastrelid != InvalidOid || (*relform2).reltoastrelid != InvalidOid {
        if swap_toast_by_content {
            if (*relform1).reltoastrelid != InvalidOid && (*relform2).reltoastrelid != InvalidOid {
                /* Recursively swap the contents of the toast tables */
                swap_relation_files((*relform1).reltoastrelid,
                                    (*relform2).reltoastrelid,
                                    target_is_pg_class,
                                    swap_toast_by_content,
                                    is_internal,
                                    frozenXid,
                                    cutoffMulti,
                                    mapped_tables);
            } else {
                /* caller messed up */
                elog!(ERROR, "cannot swap toast files by content when there's only one");
            }
        } else {
            /*
             * We swapped the ownership links, so we need to change dependency
             * data to match.
             *
             * NOTE: it is possible that only one table has a toast table.
             *
             * NOTE: at present, a TOAST table's only dependency is the one on
             * its owning table.  If more are ever created, we'd need to use
             * something more selective than deleteDependencyRecordsFor() to
             * get rid of just the link we want.
             */
            let mut baseobject: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
            let mut toastobject: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
            let mut count: c_long;

            /*
             * We disallow this case for system catalogs, to avoid the
             * possibility that the catalog we're rebuilding is one of the
             * ones the dependency changes would change.  It's too late to be
             * making any data changes to the target catalog.
             */
            if IsSystemClass(r1, relform1) {
                elog!(ERROR, "cannot swap toast files by links for system catalogs");
            }

            /* Delete old dependencies */
            if (*relform1).reltoastrelid != InvalidOid {
                count = deleteDependencyRecordsFor(RelationRelationId,
                                                   (*relform1).reltoastrelid,
                                                   false);
                if count != 1 {
                    elog!(ERROR, "expected one dependency record for TOAST table, found {}",
                          count);
                }
            }
            if (*relform2).reltoastrelid != InvalidOid {
                count = deleteDependencyRecordsFor(RelationRelationId,
                                                   (*relform2).reltoastrelid,
                                                   false);
                if count != 1 {
                    elog!(ERROR, "expected one dependency record for TOAST table, found {}",
                          count);
                }
            }

            /* Register new dependencies */
            baseobject.classId = RelationRelationId;
            baseobject.objectSubId = 0;
            toastobject.classId = RelationRelationId;
            toastobject.objectSubId = 0;

            if (*relform1).reltoastrelid != InvalidOid {
                baseobject.objectId = r1;
                toastobject.objectId = (*relform1).reltoastrelid;
                recordDependencyOn(&toastobject, &baseobject,
                                   DEPENDENCY_INTERNAL);
            }

            if (*relform2).reltoastrelid != InvalidOid {
                baseobject.objectId = r2;
                toastobject.objectId = (*relform2).reltoastrelid;
                recordDependencyOn(&toastobject, &baseobject,
                                   DEPENDENCY_INTERNAL);
            }
        }
    }

    /*
     * If we're swapping two toast tables by content, do the same for their
     * valid index. The swap can actually be safely done only if the relations
     * have indexes.
     */
    if swap_toast_by_content &&
        (*relform1).relkind == RELKIND_TOASTVALUE &&
        (*relform2).relkind == RELKIND_TOASTVALUE {
        let toastIndex1: Oid;
        let toastIndex2: Oid;

        /* Get valid index for each relation */
        toastIndex1 = toast_get_valid_index(r1,
                                            AccessExclusiveLock);
        toastIndex2 = toast_get_valid_index(r2,
                                            AccessExclusiveLock);

        swap_relation_files(toastIndex1,
                            toastIndex2,
                            target_is_pg_class,
                            swap_toast_by_content,
                            is_internal,
                            InvalidTransactionId,
                            InvalidMultiXactId,
                            mapped_tables);
    }

    /* Clean up. */
    heap_freetuple(reltup1);
    heap_freetuple(reltup2);

    table_close(relRelation, RowExclusiveLock);
}

/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 */
pub unsafe fn finish_heap_swap(OIDOldHeap: Oid, OIDNewHeap: Oid,
                               is_system_catalog: bool,
                               swap_toast_by_content: bool,
                               check_constraints: bool,
                               is_internal: bool,
                               frozenXid: TransactionId,
                               cutoffMulti: MultiXactId,
                               newrelpersistence: c_char)
{
    let mut object: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut mapped_tables: [Oid; 4] = [0; 4];
    let mut reindex_flags: c_int;
    let reindex_params: ReindexParams = core::mem::zeroed();
    let mut i: c_int;

    /* Report that we are now swapping relation files */
    pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
                                 PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES);

    /* Zero out possible results from swapped_relation_files */
    memset(mapped_tables.as_mut_ptr() as *mut c_void, 0, core::mem::size_of::<[Oid; 4]>());

    /*
     * Swap the contents of the heap relations (including any toast tables).
     * Also set old heap's relfrozenxid to frozenXid.
     */
    swap_relation_files(OIDOldHeap, OIDNewHeap,
                        OIDOldHeap == RelationRelationId,
                        swap_toast_by_content, is_internal,
                        frozenXid, cutoffMulti, mapped_tables.as_mut_ptr());

    /*
     * If it's a system catalog, queue a sinval message to flush all catcaches
     * on the catalog when we reach CommandCounterIncrement.
     */
    if is_system_catalog {
        CacheInvalidateCatalog(OIDOldHeap);
    }

    /*
     * Rebuild each index on the relation (but not the toast table, which is
     * all-new at this point).  It is important to do this before the DROP
     * step because if we are processing a system catalog that will be used
     * during DROP, we want to have its indexes available.  There is no
     * advantage to the other order anyway because this is all transactional,
     * so no chance to reclaim disk space before commit.  We do not need a
     * final CommandCounterIncrement() because reindex_relation does it.
     *
     * Note: because index_build is called via reindex_relation, it will never
     * set indcheckxmin true for the indexes.  This is OK even though in some
     * sense we are building new indexes rather than rebuilding existing ones,
     * because the new heap won't contain any HOT chains at all, let alone
     * broken ones, so it can't be necessary to set indcheckxmin.
     */
    reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
    if check_constraints {
        reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;
    }

    /*
     * Ensure that the indexes have the same persistence as the parent
     * relation.
     */
    if newrelpersistence == RELPERSISTENCE_UNLOGGED {
        reindex_flags |= REINDEX_REL_FORCE_INDEXES_UNLOGGED;
    } else if newrelpersistence == RELPERSISTENCE_PERMANENT {
        reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;
    }

    /* Report that we are now reindexing relations */
    pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
                                 PROGRESS_CLUSTER_PHASE_REBUILD_INDEX);

    reindex_relation(core::ptr::null(), OIDOldHeap, reindex_flags, &reindex_params);

    /* Report that we are now doing clean up */
    pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
                                 PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP);

    /*
     * If the relation being rebuilt is pg_class, swap_relation_files()
     * couldn't update pg_class's own pg_class entry (check comments in
     * swap_relation_files()), thus relfrozenxid was not updated. That's
     * annoying because a potential reason for doing a VACUUM FULL is a
     * imminent or actual anti-wraparound shutdown.  So, now that we can
     * access the new relation using its indices, update relfrozenxid.
     * pg_class doesn't have a toast relation, so we don't need to update the
     * corresponding toast relation. Not that there's little point moving all
     * relfrozenxid updates here since swap_relation_files() needs to write to
     * pg_class for non-mapped relations anyway.
     */
    if OIDOldHeap == RelationRelationId {
        let relRelation: Relation;
        let reltup: HeapTuple;
        let relform: Form_pg_class;

        relRelation = table_open(RelationRelationId, RowExclusiveLock);

        reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDOldHeap));
        if !HeapTupleIsValid(reltup) {
            elog!(ERROR, "cache lookup failed for relation {}", OIDOldHeap);
        }
        relform = GETSTRUCT_pg_class(reltup);

        (*relform).relfrozenxid = frozenXid;
        (*relform).relminmxid = cutoffMulti;

        CatalogTupleUpdate(relRelation, &mut (*reltup).t_self as *mut _ as *mut c_void, reltup);

        table_close(relRelation, RowExclusiveLock);
    }

    /* Destroy new heap with old filenumber */
    object.classId = RelationRelationId;
    object.objectId = OIDNewHeap;
    object.objectSubId = 0;

    /*
     * The new relation is local to our transaction and we know nothing
     * depends on it, so DROP_RESTRICT should be OK.
     */
    performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    /* performDeletion does CommandCounterIncrement at end */

    /*
     * Now we must remove any relation mapping entries that we set up for the
     * transient table, as well as its toast table and toast index if any. If
     * we fail to do this before commit, the relmapper will complain about new
     * permanent map entries being added post-bootstrap.
     */
    i = 0;
    while OidIsValid(mapped_tables[i as usize]) {
        RelationMapRemoveMapping(mapped_tables[i as usize]);
        i += 1;
    }

    /*
     * At this point, everything is kosher except that, if we did toast swap
     * by links, the toast table's name corresponds to the transient table.
     * The name is irrelevant to the backend because it's referenced by OID,
     * but users looking at the catalogs could be confused.  Rename it to
     * prevent this problem.
     *
     * Note no lock required on the relation, because we already hold an
     * exclusive lock on it.
     */
    if !swap_toast_by_content {
        let newrel: Relation;

        newrel = table_open(OIDOldHeap, NoLock);
        if OidIsValid((*(*newrel).rd_rel).reltoastrelid) {
            let toastidx: Oid;
            let mut NewToastName: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

            /* Get the associated valid index to be renamed */
            toastidx = toast_get_valid_index((*(*newrel).rd_rel).reltoastrelid,
                                             NoLock);

            /* rename the toast table ... */
            snprintf(NewToastName.as_mut_ptr(), NAMEDATALEN,
                     b"pg_toast_%u\0".as_ptr() as *const c_char,
                     OIDOldHeap);
            RenameRelationInternal((*(*newrel).rd_rel).reltoastrelid,
                                   NewToastName.as_ptr(), true, false);

            /* ... and its valid index too. */
            snprintf(NewToastName.as_mut_ptr(), NAMEDATALEN,
                     b"pg_toast_%u_index\0".as_ptr() as *const c_char,
                     OIDOldHeap);

            RenameRelationInternal(toastidx,
                                   NewToastName.as_ptr(), true, true);

            /*
             * Reset the relrewrite for the toast. The command-counter
             * increment is required here as we are about to update the tuple
             * that is updated as part of RenameRelationInternal.
             */
            CommandCounterIncrement();
            ResetRelRewrite((*(*newrel).rd_rel).reltoastrelid);
        }
        relation_close(newrel, NoLock);
    }

    /* if it's not a catalog table, clear any missing attribute settings */
    if !is_system_catalog {
        let newrel: Relation;

        newrel = table_open(OIDOldHeap, NoLock);
        RelationClearMissing(newrel);
        relation_close(newrel, NoLock);
    }
}


/*
 * Get a list of tables that the current user has privileges on and
 * have indisclustered set.  Return the list in a List * of RelToCluster
 * (stored in the specified memory context), each one giving the tableOid
 * and the indexOid on which the table is already clustered.
 */
unsafe fn get_tables_to_cluster(cluster_context: MemoryContext) -> *mut List
{
    let indRelation: Relation;
    let scan: TableScanDesc;
    let mut entry: ScanKeyData = core::mem::zeroed();
    let mut indexTuple: HeapTuple;
    let mut index: Form_pg_index;
    let mut old_context: MemoryContext;
    let mut rtcs: *mut List = NIL as *mut List;

    /*
     * Get all indexes that have indisclustered set and that the current user
     * has the appropriate privileges for.
     */
    indRelation = table_open(IndexRelationId, AccessShareLock);
    ScanKeyInit(&mut entry,
                Anum_pg_index_indisclustered,
                BTEqualStrategyNumber, F_BOOLEQ,
                BoolGetDatum(true));
    scan = table_beginscan_catalog(indRelation, 1, &mut entry);
    loop {
        indexTuple = heap_getnext(scan, ForwardScanDirection);
        if indexTuple.is_null() {
            break;
        }

        let rtc: *mut RelToCluster;

        index = GETSTRUCT_pg_index(indexTuple);

        if !cluster_is_permitted_for_relation((*index).indrelid, GetUserId()) {
            continue;
        }

        /* Use a permanent memory context for the result list */
        old_context = MemoryContextSwitchTo(cluster_context);

        rtc = palloc(core::mem::size_of::<RelToCluster>()) as *mut RelToCluster;
        (*rtc).tableOid = (*index).indrelid;
        (*rtc).indexOid = (*index).indexrelid;
        rtcs = lappend(rtcs, rtc as *mut c_void);

        MemoryContextSwitchTo(old_context);
    }
    table_endscan(scan);

    relation_close(indRelation, AccessShareLock);

    return rtcs;
}

/*
 * Given an index on a partitioned table, return a list of RelToCluster for
 * all the children leaves tables/indexes.
 *
 * Like expand_vacuum_rel, but here caller must hold AccessExclusiveLock
 * on the table containing the index.
 */
unsafe fn get_tables_to_cluster_partitioned(cluster_context: MemoryContext, indexOid: Oid) -> *mut List
{
    let inhoids: *mut List;
    let mut lc: *mut ListCell;
    let mut rtcs: *mut List = NIL as *mut List;
    let mut old_context: MemoryContext;

    /* Do not lock the children until they're processed */
    inhoids = find_all_inheritors(indexOid, NoLock, core::ptr::null_mut());

    foreach!(lc, inhoids, {
        let indexrelid: Oid = lfirst_oid(current_cell!(lc));
        let relid: Oid = IndexGetRelation(indexrelid, false);
        let rtc: *mut RelToCluster;

        /* consider only leaf indexes */
        if get_rel_relkind(indexrelid) != RELKIND_INDEX {
            continue;
        }

        /*
         * It's possible that the user does not have privileges to CLUSTER the
         * leaf partition despite having such privileges on the partitioned
         * table.  We skip any partitions which the user is not permitted to
         * CLUSTER.
         */
        if !cluster_is_permitted_for_relation(relid, GetUserId()) {
            continue;
        }

        /* Use a permanent memory context for the result list */
        old_context = MemoryContextSwitchTo(cluster_context);

        rtc = palloc(core::mem::size_of::<RelToCluster>()) as *mut RelToCluster;
        (*rtc).tableOid = relid;
        (*rtc).indexOid = indexrelid;
        rtcs = lappend(rtcs, rtc as *mut c_void);

        MemoryContextSwitchTo(old_context);
    });

    return rtcs;
}

/*
 * Return whether userid has privileges to CLUSTER relid.  If not, this
 * function emits a WARNING.
 */
unsafe fn cluster_is_permitted_for_relation(relid: Oid, userid: Oid) -> bool
{
    if pg_class_aclcheck(relid, userid, ACL_MAINTAIN) == ACLCHECK_OK {
        return true;
    }

    ereport!(WARNING,
             errmsg!("permission denied to cluster \"{}\", skipping it",
                     std::ffi::CStr::from_ptr(get_rel_name(relid)).to_string_lossy()));
    return false;
}
