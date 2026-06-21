/*-------------------------------------------------------------------------
 *
 * indexcmds.c
 *   POSTGRES define and remove index code.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/commands/indexcmds.c
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
use core::ffi::{c_char, c_int, c_void};

use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::cmptype::CompareType;
use crate::access::stratnum::StrategyNumber;
use crate::access::transam::InvalidTransactionId;
use crate::c::TransactionId;
use crate::access::common::attmap::AttrMap;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::index::amapi::IndexAmRoutine;
use crate::access::index::indexam::{index_close, index_open};
use crate::access::table::table::{table_close, table_open};
use crate::c::{bits16, int16, uint16};
use crate::catalog::catalog_oids::{
    InheritsRelationId, IndexRelationId, NamespaceRelationId, OperatorClassRelationId,
    RelationRelationId, TableSpaceRelationId,
};
use crate::catalog::index::{
    ReindexParams, INDEX_CREATE_ADD_CONSTRAINT, INDEX_CREATE_CONCURRENT,
    INDEX_CREATE_IF_NOT_EXISTS, INDEX_CREATE_INVALID, INDEX_CREATE_IS_PRIMARY,
    INDEX_CREATE_PARTITIONED, INDEX_CREATE_SKIP_BUILD, INDEX_CONSTR_CREATE_DEFERRABLE,
    INDEX_CONSTR_CREATE_INIT_DEFERRED, INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS,
    REINDEXOPT_CONCURRENTLY, REINDEXOPT_VERBOSE, REINDEXOPT_REPORT_PROGRESS,
    REINDEXOPT_MISSING_OK, REINDEX_REL_PROCESS_TOAST, REINDEX_REL_CHECK_CONSTRAINTS,
};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_class::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_TOASTVALUE, RELKIND_FOREIGN_TABLE,
};
use crate::utils::activity::backend_progress::ProgressCommandType::PROGRESS_COMMAND_CREATE_INDEX;
use crate::commands::progress::{
    PROGRESS_CREATEIDX_ACCESS_METHOD_OID,
    PROGRESS_CREATEIDX_COMMAND, PROGRESS_CREATEIDX_COMMAND_CREATE,
    PROGRESS_CREATEIDX_COMMAND_CREATE_CONCURRENTLY,
    PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY, PROGRESS_CREATEIDX_INDEX_OID,
    PROGRESS_CREATEIDX_PARTITIONS_DONE, PROGRESS_CREATEIDX_PARTITIONS_TOTAL,
    PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_BUILD,
    PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN, PROGRESS_CREATEIDX_PHASE_WAIT_1,
    PROGRESS_CREATEIDX_PHASE_WAIT_2, PROGRESS_CREATEIDX_PHASE_WAIT_3,
    PROGRESS_CREATEIDX_PHASE_WAIT_4, PROGRESS_CREATEIDX_PHASE_WAIT_5,
    PROGRESS_WAITFOR_CURRENT_PID, PROGRESS_WAITFOR_DONE, PROGRESS_WAITFOR_TOTAL,
};
use crate::miscadmin::{
    GetUserId, GetUserIdAndSecContext, SetUserIdAndSecContext, SECURITY_RESTRICTED_OPERATION,
};
use crate::nodes::execnodes::{IndexInfo, INDEX_MAX_KEYS};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    DefElem, IndexElem, IndexStmt, ObjectType, ReindexObjectType, ReindexObjectType::*,
    ReindexStmt,
};
use crate::nodes::pg_list::{
    list_head, list_length, lcons_oid, lappend, lappend_oid, list_concat_copy, list_free,
    lfirst_oid, lnext, NIL,
};
use crate::list_make1_oid;
use crate::nodes::primnodes::Expr;
use crate::postgres::{Datum, ObjectIdGetDatum, PointerGetDatum, Int32GetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::OidIsValid;
use crate::storage::ipc::procarray::{
    VirtualTransactionId, GetCurrentVirtualXIDs,
    PROC_IS_AUTOVACUUM, PROC_IN_VACUUM,
};
use crate::storage::lockdefs::{
    AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock, ShareLock,
    ShareUpdateExclusiveLock, LOCKMODE,
};
use crate::storage::lmgr::lmgr::{
    LOCKTAGData as LOCKTAG, LockRelationIdForSession, LockRelationOid,
    UnlockRelationIdForSession, UnlockRelationOid, WaitForLockers, WaitForLockersMultiple,
};
use crate::utils::rel::LockRelId;
use crate::storage::lmgr::proc::{PROC_IN_SAFE_IC, PGPROC, ProcGlobal};
use crate::utils::cache::lsyscache::{
    get_namespace_name, get_rel_name, get_rel_namespace, get_rel_persistence, get_rel_relkind,
    get_opclass_family, get_op_opfamily_strategy, get_opcode, get_commutator,
    get_opclass_opfamily_and_input_type, get_opclass_method, get_opfamily_member,
    get_opfamily_member_for_cmptype, get_opfamily_name, get_opname,
    get_opfamily_method, get_index_isvalid,
    get_attoptions,
};
use crate::commands::defrem::{GetDefaultOpClass, get_am_name};
use crate::catalog::namespace::OpclassnameGetOpcid;
use crate::utils::cache::syscache::{
    SearchSysCache1, SearchSysCache3, SysCacheGetAttrNotNull,
    ReleaseSysCache, CLAOID, CLAAMNAMENSP, INDEXRELID,
};
use crate::access::htup_details::HeapTupleIsValid;
use crate::utils::misc::guc::{
    NewGUCNestLevel, RestrictSearchPath, AtEOXact_GUC, set_config_option,
    GucContext::PGC_USERSET, GucSource::PGC_S_SESSION, GucAction::GUC_ACTION_SAVE,
};
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetRelationName, RelationGetRelid, RelationGetNamespace,
};
use crate::utils::activity::backend_progress::{
    pgstat_progress_start_command, pgstat_progress_update_param, pgstat_progress_end_command,
    pgstat_progress_update_multi_param, pgstat_progress_incr_param,
};
use crate::access::transam::xact::{
    CommitTransactionCommand, StartTransactionCommand, CommandCounterIncrement,
    PreventInTransactionBlock,
};
use crate::nodes::primnodes::RangeVar;
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext, SysScanDesc};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::catalog::pg_attribute::FormData_pg_attribute;
use crate::storage::lmgr::proc::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LWLock};
use crate::utils::snapshot::SnapshotData;
use crate::access::cmptype::{COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_OVERLAP};

/* ---------------------------------------------------------------------------
 * Stub types for not-yet-ported modules
 * ---------------------------------------------------------------------------
 */

use crate::access::index::amapi::amoptions_function;

/* TODO(pg-port): catalog/indexing.h */
unsafe fn CatalogTupleUpdate(_rel: Relation, _tid: *mut crate::storage::itemptr::ItemPointerData, _tup: HeapTuple) { /* stub no-op (restored: test_setup path) */ }
unsafe fn CatalogTupleDelete(_rel: Relation, _tid: *mut crate::storage::itemptr::ItemPointerData) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/index.h */
unsafe fn index_check_primary_key(_rel: Relation, _indexInfo: *mut IndexInfo, _is_alter_table: bool, _stmt: *mut IndexStmt) { /* stub no-op (restored: test_setup path) */ }
unsafe fn index_set_state_flags(_indexRelationId: Oid, _flags: bits16) { /* stub no-op (restored: test_setup path) */ }
unsafe fn index_concurrently_build(_tableId: Oid, _indexRelationId: Oid) { /* stub no-op (restored: test_setup path) */ }
unsafe fn index_concurrently_create_copy(_heapRel: Relation, _oldIndexId: Oid, _tablespaceOid: Oid, _concurrentName: *const c_char) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn index_concurrently_swap(_newIndexId: Oid, _oldIndexId: Oid, _oldName: *const c_char) { /* stub no-op (restored: test_setup path) */ }
unsafe fn index_concurrently_set_dead(_tableId: Oid, _indexId: Oid) { /* stub no-op (restored: test_setup path) */ }
unsafe fn validate_index(_tableId: Oid, _indexId: Oid, _snapshot: Snapshot) { /* stub no-op (restored: test_setup path) */ }
unsafe fn makeIndexInfo(
    numattrs: c_int, numkeyattrs: c_int, amoid: Oid,
    expressions: *mut crate::nodes::pg_list::List, predicate: *mut crate::nodes::pg_list::List,
    unique: bool, nulls_not_distinct: bool,
    isready: bool, concurrent: bool, summarizing: bool, iswithoutoverlaps: bool,
) -> *mut IndexInfo {
    crate::nodes::makefuncs::makeIndexInfo(
        numattrs, numkeyattrs, amoid, expressions as _, predicate as _,
        unique, nulls_not_distinct, isready, concurrent, summarizing, iswithoutoverlaps,
    ) as _
}
unsafe fn BuildIndexInfo(indexRel: Relation) -> *mut IndexInfo {
    crate::catalog::index::BuildIndexInfo(indexRel) as _
}
unsafe fn CompareIndexInfo(
    _info1: *mut IndexInfo, _info2: *mut IndexInfo,
    _coll1: *mut Oid, _coll2: *mut Oid,
    _opf1: *mut Oid, _opf2: *mut Oid,
    _attmap: *mut AttrMap,
) -> bool { /* TODO(pg-port) */ false }

/* TODO(pg-port): catalog/index.h: index_create */
unsafe fn index_create(
    heapRelation: Relation,
    indexRelationName: *const c_char,
    indexRelationId: Oid,
    parentIndexRelid: Oid,
    parentConstraintId: Oid,
    relFileNumber: RelFileNumber,
    indexInfo: *mut IndexInfo,
    indexColNames: *mut crate::nodes::pg_list::List,
    accessMethodObjectId: Oid,
    tableSpaceId: Oid,
    collationIds: *const Oid,
    opclassIds: *const Oid,
    opclassOptions: *const Datum,
    coloptions: *const int16,
    stattargets: *const c_void,
    reloptions: Datum,
    flags: bits16,
    constr_flags: bits16,
    allow_system_table_mods: bool,
    is_internal: bool,
    constraintId: *mut Oid,
) -> Oid {
    crate::catalog::index::index_create(
        heapRelation, indexRelationName, indexRelationId, parentIndexRelid,
        parentConstraintId, relFileNumber, indexInfo as _, indexColNames as _,
        accessMethodObjectId, tableSpaceId, collationIds, opclassIds, opclassOptions,
        coloptions, stattargets as _, reloptions, flags, constr_flags,
        allow_system_table_mods, is_internal, constraintId,
    )
}

/* TODO(pg-port): catalog/index.h: reindex_index, reindex_relation */
unsafe fn reindex_index(
    _stmt: *const ReindexStmt,
    _indexId: Oid,
    _skip_constraint_checks: bool,
    _persistence: c_char,
    _params: *const ReindexParams,
) { /* TODO(pg-port) */ }
unsafe fn reindex_relation(
    _stmt: *const ReindexStmt,
    _relid: Oid,
    _flags: c_int,
    _params: *const ReindexParams,
) -> bool { /* TODO(pg-port) */ false }

/* TODO(pg-port): catalog/catalog.h */
unsafe fn IsCatalogRelationOid(_relid: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn IsSystemClass(_relid: Oid, _classtuple: *const c_void) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn IsSystemRelation(_rel: Relation) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn IsToastNamespace(_ns: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn isTempNamespace(_ns: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): catalog/namespace.h */
unsafe fn DeconstructQualifiedName(_names: *const crate::nodes::pg_list::List, _schemaname: *mut *mut c_char, _objname: *mut *mut c_char) { /* stub no-op (restored: test_setup path) */ }
unsafe fn LookupExplicitNamespace(_nspname: *const c_char, _missing_ok: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn RangeVarGetRelidExtended(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _flags: c_int,
    _callback: unsafe fn(*const RangeVar, Oid, Oid, *mut c_void),
    _callback_arg: *mut c_void,
) -> Oid { /* TODO(pg-port) */ InvalidOid }
unsafe fn RangeVarCallbackMaintainsTable(_relation: *const RangeVar, _relId: Oid, _oldRelId: Oid, _arg: *mut c_void) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_inherits.h */
unsafe fn find_all_inheritors(_relId: Oid, _lockmode: LOCKMODE, _numparents: *mut c_int) -> *mut crate::nodes::pg_list::List { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn has_superclass(_classOid: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn StoreSingleInheritance(_relid: Oid, _parentOid: Oid, _seqNumber: i32) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_constraint.h */
unsafe fn ConstraintSetParentConstraint(_constrOid: Oid, _parentConstrOid: Oid, _childRelid: Oid) { /* stub no-op (restored: test_setup path) */ }
unsafe fn ConstraintNameExists(_name: *const c_char, _namespaceId: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): catalog/indexing.h */
unsafe fn SearchSysCacheLockedCopy1(_cacheId: c_int, _key: Datum) -> HeapTuple { unimplemented!("STUB SearchSysCacheLockedCopy1") }
unsafe fn UnlockTuple(_rel: Relation, _tid: *mut crate::storage::itemptr::ItemPointerData, _lockmode: LOCKMODE) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/objectaddress.h */
unsafe fn IndexGetRelation(_indexId: Oid, _missing_ok: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn recordDependencyOn(_depender: *const ObjectAddress, _referenced: *const ObjectAddress, _deptype: c_char) { /* stub no-op (restored: test_setup path) */ }
unsafe fn deleteDependencyRecordsForClass(_classId: Oid, _objectId: Oid, _refClassId: Oid, _deptype: c_char) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): utils/acl.h */
type AclResult = c_int;
type AclMode = u64;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 1;
const ACL_CREATE: AclMode = 1 << 2;
const ACL_MAINTAIN: AclMode = 1 << 16;
unsafe fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult { /* TODO(pg-port) */ ACLCHECK_OK }
unsafe fn pg_class_aclcheck(_tableoid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult { /* TODO(pg-port) */ ACLCHECK_OK }
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _userid: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_authid.h */
const ROLE_PG_MAINTAIN: Oid = 4544;

/* TODO(pg-port): access/xact.h snapshot */
type Snapshot = *mut SnapshotData;

/* TODO(pg-port): nodes/parsenodes.h */
type RelFileNumber = crate::nodes::parsenodes::RelFileNumber;

/* TODO(pg-port): utils/lsyscache.h */
unsafe fn get_opclass_input_type(_opclassOid: Oid) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn IsPolymorphicType(_typeId: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn IsBinaryCoercible(_srctype: Oid, _targtype: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn type_is_collatable(_typeId: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn TypeCategory(_typeId: Oid) -> c_char { /* TODO(pg-port) */ b'S' as c_char }
type TYPCATEGORY = c_char;
unsafe fn IsPreferredType(_category: TYPCATEGORY, _typeId: Oid) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn getBaseType(_typeId: Oid) -> Oid { /* TODO(pg-port) */ _typeId }
unsafe fn format_type_be(_typeId: Oid) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn format_operator(_opId: Oid) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn get_collation_oid(_name: *mut crate::nodes::pg_list::List, _missing_ok: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn NameListToString(_names: *const crate::nodes::pg_list::List) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn get_namespace_oid(_nspname: *const c_char, _missing_ok: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn get_tablespace_oid(_spcname: *const c_char, _missing_ok: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn get_tablespace_name(_spcoid: Oid) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): parser/parse_utilcmd.h */
unsafe fn generateClonedIndexStmt(
    _heapRel: *mut c_void,
    _source_idx: Relation,
    _attmap: *mut AttrMap,
    _constraintOid: *mut Oid,
) -> *mut IndexStmt { /* TODO(pg-port) */ core::ptr::null_mut() }

/* TODO(pg-port): catalog/pg_am.h type aliases */
type Form_pg_am = *mut FormData_pg_am;
#[repr(C)]
struct FormData_pg_am {
    pub oid: Oid,
    pub amname: crate::c::NameData,
    pub amhandler: Oid, /* regproc */
    pub amtype: c_char,
}

/* TODO(pg-port): access/amapi.h accessor */
unsafe fn GetIndexAmRoutine(amhandler: Oid) -> *mut IndexAmRoutine { crate::access::index::amapi::GetIndexAmRoutine(amhandler) }

/* TODO(pg-port): access/tableam.h */
unsafe fn try_table_open(_relId: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!("STUB try_table_open") }
unsafe fn table_beginscan_catalog(_rel: Relation, _nkeys: c_int, _key: *const ScanKeyData) -> *mut c_void { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn heap_getnext(_scan: *mut c_void, _dir: c_int) -> HeapTuple { unimplemented!("STUB heap_getnext") }
unsafe fn table_endscan(_scan: *mut c_void) { /* stub no-op (restored: test_setup path) */ }

const ForwardScanDirection: c_int = 1;

/* TODO(pg-port): utils/rel.h tuple desc attr */
unsafe fn TupleDescAttr(desc: TupleDesc, attno: c_int) -> *mut FormData_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(desc, attno) as *mut FormData_pg_attribute
}

/* TODO(pg-port): utils/rel.h partition info */
type PartitionKey = *mut PartitionKeyData;
#[repr(C)]
struct PartitionKeyData {
    pub strategy: c_char,
    pub partnatts: c_int,
    pub partattrs: *mut AttrNumber,
    pub partopfamily: *mut Oid,
    pub partopcintype: *mut Oid,
    pub partcollation: *mut Oid,
}
type PartitionDesc = *mut PartitionDescData;
#[repr(C)]
struct PartitionDescData {
    pub nparts: c_int,
    pub oids: *mut Oid,
}

unsafe fn RelationGetPartitionKey_(_rel: Relation) -> PartitionKey { unimplemented!("STUB RelationGetPartitionKey_") }
unsafe fn RelationGetPartitionDesc_(_rel: Relation, _include_detached: bool) -> PartitionDesc { unimplemented!("STUB RelationGetPartitionDesc_") }
unsafe fn RelationGetIndexList_(_rel: Relation) -> *mut crate::nodes::pg_list::List { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn RelationGetIndexExpressions_(_rel: Relation) -> *mut crate::nodes::pg_list::List { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn RelationGetIndexPredicate_(_rel: Relation) -> *mut crate::nodes::pg_list::List { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn RelationGetExclusionInfo(_rel: Relation, _operators: *mut *mut Oid, _procs: *mut *mut Oid, _strats: *mut *mut uint16) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_attribute.h */
const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;

/* TODO(pg-port): nodes/bitmapset.h */
type Bitmapset = c_void;
unsafe fn pull_varattnos(_expr: *mut Node, _varno: c_int, _varattnos: *mut *mut Bitmapset) { /* stub no-op (restored: test_setup path) */ }
unsafe fn bms_is_member(_x: c_int, _a: *const Bitmapset) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn bms_next_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int { /* TODO(pg-port) */ -2 }

/* TODO(pg-port): include/access/attnum.h */
const FirstLowInvalidHeapAttributeNumber: c_int = -8;

/* TODO(pg-port): nodes/makefuncs.h */
unsafe fn make_ands_implicit(_expr: *mut Expr) -> *mut crate::nodes::pg_list::List { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): optimizer/optimizer.h */
unsafe fn contain_mutable_functions_after_planning(_expr: *mut Expr) -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): nodes/nodeFuncs.h exprType, exprCollation */
unsafe fn exprType(_expr: *mut Node) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn exprCollation(_expr: *mut Node) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): nodes/primnodes.h CollateExpr, Var */
#[repr(C)]
struct CollateExpr {
    pub xpr: crate::nodes::nodes::Node,
    pub arg: *mut Expr,
    pub collOid: Oid,
    pub location: c_int,
}
#[repr(C)]
struct Var {
    pub xpr: crate::nodes::nodes::Node,
    pub varno: c_int,
    pub varattno: AttrNumber,
}

use crate::nodes::nodes::NodeTag;
macro_rules! IsA_local {
    ($p:expr, CollateExpr) => { (*($p as *const crate::nodes::nodes::Node)).r#type == NodeTag::T_CollateExpr };
    ($p:expr, Var) => { (*($p as *const crate::nodes::nodes::Node)).r#type == NodeTag::T_Var };
    ($p:expr, $T:ident) => { (*($p as *const crate::nodes::nodes::Node)).r#type == NodeTag::$T };
}

/* TODO(pg-port): utils/rel.h: RELATION_IS_OTHER_TEMP */
unsafe fn RELATION_IS_OTHER_TEMP(_rel: Relation) -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): miscadmin.h */
static mut allowSystemTableMods: bool = false;
unsafe fn IsBootstrapProcessingMode() -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): catalog/heap.h */
unsafe fn CheckTableNotInUse(_rel: Relation, _stmt: *const c_char) { /* stub no-op (restored: test_setup path) */ }
unsafe fn heap_copytuple(_tup: HeapTuple) -> HeapTuple { unimplemented!("STUB heap_copytuple") }
unsafe fn heap_freetuple(_tup: HeapTuple) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_inherits.h attribute numbers */
const Anum_pg_inherits_inhrelid: AttrNumber = 1;
const Anum_pg_inherits_inhseqno: AttrNumber = 2;
const InheritsRelidSeqnoIndexId: Oid = 2677;
type Form_pg_inherits = *mut FormData_pg_inherits;
#[repr(C)]
struct FormData_pg_inherits {
    pub inhrelid: Oid,
    pub inhparent: Oid,
    pub inhseqno: i32,
}

/* TODO(pg-port): catalog/pg_class.h Form_pg_class */
use crate::catalog::pg_class::FormData_pg_class;
type Form_pg_class = *mut FormData_pg_class;

/* TODO(pg-port): utils/fmgr.h */
type FmgrInfo = c_void;
const F_ARRAY_EQ: Oid = 382;
unsafe fn fmgr_info(_functionId: Oid, _finfo: *mut FmgrInfo) { /* stub no-op (restored: test_setup path) */ }
unsafe fn FunctionCall2Coll(_flinfo: *mut FmgrInfo, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Datum { 0 /* stub dummy (restored: test_setup path) */ }
const C_COLLATION_OID: Oid = 950;

/* TODO(pg-port): utils/snapshot.h */
unsafe fn InitDirtySnapshot(_snapshot: *mut SnapshotData) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/namespace.h */
unsafe fn get_database_name(_dbid: Oid) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_opclass.h */
type Form_pg_opclass = *mut FormData_pg_opclass;
#[repr(C)]
struct FormData_pg_opclass {
    pub oid: Oid,
    pub opcmethod: Oid,
    pub opcname: [c_char; 64],
    pub opcnamespace: Oid,
    pub opcowner: Oid,
    pub opcfamily: Oid,
    pub opcintype: Oid,
    pub opcdefault: bool,
    pub opckeytype: Oid,
}

/* TODO(pg-port): catalog/pg_class.h attribute constants */
const Anum_pg_class_relname: AttrNumber = 2;
const Anum_pg_class_relnamespace: AttrNumber = 3;
const ClassNameNspIndexId: Oid = 2663;
const OpclassAmNameNspIndexId: Oid = 2686;

/* TODO(pg-port): catalog/pg_index.h constants */
const Anum_pg_index_indpred: AttrNumber = 21;
const Anum_pg_index_indexprs: AttrNumber = 20;
const Anum_pg_index_indcollation: AttrNumber = 17;
const Anum_pg_index_indclass: AttrNumber = 18;
type Form_pg_index = *mut FormData_pg_index;
#[repr(C)]
struct FormData_pg_index {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: int16,
    pub indnkeyatts: int16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
}

type oidvector = crate::c::oidvector;
unsafe fn DatumGetPointer(d: Datum) -> *mut c_void { d as *mut c_void }
unsafe fn DatumGetBool(d: Datum) -> bool { d != 0 }

/* TODO(pg-port): catalog/indexing.h heap_attisnull */
unsafe fn heap_attisnull(_tup: HeapTuple, _attno: c_int, _tupDesc: TupleDesc) -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): access/reloptions.h */
unsafe fn transformRelOptions(_baseOptions: Datum, _defList: *mut crate::nodes::pg_list::List, _namspace: *const c_char, _validnsps: *const *const c_char, _acceptOidsOff: bool, _isReset: bool) -> Datum { 0 /* stub dummy (restored: test_setup path) */ }
unsafe fn index_reloptions(_amoptions: amoptions_function, _reloptions: Datum, _validate: bool) -> *mut c_void { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): commands/tablespace.h */
unsafe fn GetDefaultTablespace(_relpersistence: c_char, _partitioned: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): commands/tablecmds.h */
unsafe fn build_attrmap_by_name(_dst: TupleDesc, _src: TupleDesc, _missing_ok: bool) -> *mut AttrMap { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn free_attrmap(_map: *mut AttrMap) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): commands/defrem.h */
unsafe fn defGetBoolean(_def: *const DefElem) -> bool { false /* stub dummy (restored: test_setup path) */ }
unsafe fn defGetString(_def: *const DefElem) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): commands/comment.h */
unsafe fn CreateComments(_objectId: Oid, _classId: Oid, _subId: i32, _comment: *const c_char) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): commands/event_trigger.h */
unsafe fn EventTriggerCollectSimpleCommand(_address: ObjectAddress, _secondaryObject: ObjectAddress, _parsetree: *mut Node) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/objectaddress.h */
unsafe fn op_input_types(_opId: Oid, _lefttype: *mut Oid, _righttype: *mut Oid) { /* stub no-op (restored: test_setup path) */ }
unsafe fn compatible_oper_opid(_op: *const crate::nodes::pg_list::List, _arg1: Oid, _arg2: Oid, _noError: bool) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): access/index/amapi.h */
unsafe fn IndexAmTranslateCompareType(_cmptype: CompareType, _amid: Oid, _opfamilyId: Oid, _errorOK: bool) -> StrategyNumber { unimplemented!("STUB IndexAmTranslateCompareType") }
const InvalidStrategy: StrategyNumber = 0;

/* TODO(pg-port): catalog/catalog.h */
unsafe fn SetRelationHasSubclass(_relid: Oid, _relhassubclass: bool) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): access/partitioning */
const PARTITION_STRATEGY_HASH: c_char = b'h' as c_char;
const HTEqualStrategyNumber: StrategyNumber = 1;

/* TODO(pg-port): storage/procarray.h */
unsafe fn SetInvalidVirtualTransactionId(_vxid: *mut VirtualTransactionId) { /* stub no-op (restored: test_setup path) */ }
unsafe fn VirtualXactLock(_vxid: VirtualTransactionId, _wait: bool) { /* stub no-op (restored: test_setup path) */ }
unsafe fn VirtualTransactionIdIsValid(vxid: VirtualTransactionId) -> bool { vxid.localTransactionId != 0 }
unsafe fn VirtualTransactionIdEquals(a: VirtualTransactionId, b: VirtualTransactionId) -> bool {
    a.procNumber == b.procNumber && a.localTransactionId == b.localTransactionId
}

/* TODO(pg-port): storage/proc.h ProcNumberGetProc */
unsafe fn ProcNumberGetProc(_procNumber: crate::storage::ipc::procarray::ProcNumber) -> *mut PGPROC { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn ProcArrayLock() -> *mut LWLock {
    crate::backend_link_shims::ProcArrayLock as *mut LWLock
}
unsafe fn MyProc() -> *mut PGPROC { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): utils/pg_rusage.h */
#[repr(C)]
struct PGRUsage { tv: [i64; 4] }
unsafe fn pg_rusage_init(_ru0: *mut PGRUsage) { /* stub no-op (restored: test_setup path) */ }
unsafe fn pg_rusage_show(_ru0: *const PGRUsage) -> *const c_char { b"\0".as_ptr() as *const c_char }

/* TODO(pg-port): catalog/pg_database.h */
static mut MyDatabaseId: Oid = 0;
static mut MyDatabaseTableSpace: Oid = 0;
const GLOBALTABLESPACE_OID: Oid = 1664;
const DatabaseRelationId: Oid = 1262;

/* TODO(pg-port): utils/builtins.h */
unsafe fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize { 0 /* stub dummy (restored: test_setup path) */ }
const NAMEDATALEN: usize = 64;

/* sort order constants imported from nodes/parsenodes */
use crate::nodes::parsenodes::{SortByDir, SortByNulls, SortByDir::*, SortByNulls::*};

/* TODO(pg-port): access/htup_details.h */
const INDOPTION_DESC: int16 = 0x0001;
const INDOPTION_NULLS_FIRST: int16 = 0x0002;

/* TODO(pg-port): catalog/pg_class.h persistence */
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;
const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;

/* TODO(pg-port): catalog/pg_class.h helpers */
unsafe fn RELKIND_HAS_STORAGE(relkind: c_char) -> bool {
    matches!(relkind as u8 as char, 'r' | 'i' | 'S' | 't' | 'm')
}
unsafe fn RELKIND_HAS_PARTITIONS(relkind: c_char) -> bool {
    relkind == RELKIND_PARTITIONED_TABLE || relkind == RELKIND_PARTITIONED_INDEX
}

/* TODO(pg-port): mb/pg_wchar.h */
unsafe fn pg_mbcliplen(_mbstr: *const c_char, _len: usize, _limit: usize) -> usize { /* TODO(pg-port) */ _limit }

/* TODO(pg-port): catalog/pg_class.h RelFileNumberIsValid */
unsafe fn RelFileNumberIsValid(_fnum: RelFileNumber) -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): catalog/dependency.h */
type ObjectAddresses = c_void;
unsafe fn new_object_addresses() -> *mut ObjectAddresses { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) { /* stub no-op (restored: test_setup path) */ }
unsafe fn performMultipleDeletions(_objects: *mut ObjectAddresses, _behavior: c_int, _flags: c_int) { /* stub no-op (restored: test_setup path) */ }
const DROP_RESTRICT: c_int = 0;
const PERFORM_DELETION_CONCURRENT_LOCK: c_int = 1 << 2;
const PERFORM_DELETION_INTERNAL: c_int = 1 << 0;

/* TODO(pg-port): catalog/indexing.h */
const DEPENDENCY_PARTITION_PRI: c_char = b'P' as c_char;
const DEPENDENCY_PARTITION_SEC: c_char = b'p' as c_char; /* TODO(pg-port): real value */
const InplaceUpdateTupleLock: LOCKMODE = ShareLock; /* TODO(pg-port): real value = 3 */
unsafe fn relation_open(_relid: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!("STUB relation_open") }
unsafe fn relation_close(_rel: Relation, _lockmode: LOCKMODE) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/pg_namespace.h */
const Anum_pg_opclass_opcmethod: AttrNumber = 2;

/* TODO(pg-port): utils/injection_point.h */
unsafe fn INJECTION_POINT(_name: *const c_char, _arg: *mut c_void) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): utils/builtins.h */
unsafe fn pstrdup(s: *const c_char) -> *mut c_char { crate::utils::palloc::pstrdup(s) }

/* TODO(pg-port): xact.h CHECK_FOR_INTERRUPTS */
unsafe fn CHECK_FOR_INTERRUPTS() { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): utils/memutils.h - PortalContext */
static mut PortalContext: MemoryContext = core::ptr::null_mut();

/* TODO(pg-port): utils/snap.h -- SearchSysCacheAttName */
unsafe fn SearchSysCacheAttName(relid: Oid, attname: *const c_char) -> HeapTuple { crate::utils::cache::syscache::SearchSysCacheAttName(relid, attname) }

unsafe fn ResolveOpClass(
    opclass: *const crate::nodes::pg_list::List,
    attrType: Oid,
    accessMethodName: *const c_char,
    accessMethodId: Oid,
) -> Oid { ResolveOpClass_full(opclass, attrType, accessMethodName, accessMethodId) }

/* TODO(pg-port): catalog/index.h INDEX_CREATE_SET_VALID */
const INDEX_CREATE_SET_VALID: bits16 = 0x0020; /* TODO(pg-port) */

/* TODO(pg-port): utils/rel.h CacheInvalidateRelcacheByRelid */
unsafe fn CacheInvalidateRelcacheByRelid(_relid: Oid) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): utils/rel.h SET_LOCKTAG_RELATION */
unsafe fn SET_LOCKTAG_RELATION(tag: *mut LOCKTAG, db_id: Oid, rel_id: Oid) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): catalog/index.h ObjectAddressSet */
unsafe fn ObjectAddressSet(addr: *mut ObjectAddress, classId: Oid, objectId: Oid) {
    (*addr).classId = classId;
    (*addr).objectId = objectId;
    (*addr).objectSubId = 0;
}
unsafe fn InvalidObjectAddress() -> ObjectAddress {
    ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 }
}

/* TODO(pg-port): utils/pg_list helpers */
unsafe fn list_length_(_list: *const crate::nodes::pg_list::List) -> c_int {
    list_length(_list as *const crate::nodes::pg_list::List)
}

/* TODO(pg-port): catalog/pg_class.h GETSTRUCT variants */
unsafe fn GETSTRUCT_pg_am(tup: HeapTuple) -> Form_pg_am { GETSTRUCT(tup) as _ }
unsafe fn GETSTRUCT_pg_index(tup: HeapTuple) -> Form_pg_index { GETSTRUCT(tup) as _ }
unsafe fn GETSTRUCT_pg_opclass(tup: HeapTuple) -> Form_pg_opclass { GETSTRUCT(tup) as _ }
unsafe fn GETSTRUCT_pg_class(tup: HeapTuple) -> Form_pg_class { GETSTRUCT(tup) as _ }
unsafe fn GETSTRUCT_pg_inherits(tup: HeapTuple) -> Form_pg_inherits { GETSTRUCT(tup) as _ }
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void { crate::access::htup_details::GETSTRUCT(tup as _) }

/* TODO(pg-port): utils/snapshot.h snapshot stack */
unsafe fn GetTransactionSnapshot() -> Snapshot { unimplemented!("STUB GetTransactionSnapshot") }
unsafe fn PushActiveSnapshot(_snap: Snapshot) { /* stub no-op (restored: test_setup path) */ }
unsafe fn PopActiveSnapshot() { /* stub no-op (restored: test_setup path) */ }
unsafe fn RegisterSnapshot(snap: Snapshot) -> Snapshot { /* TODO(pg-port) */ snap }
unsafe fn UnregisterSnapshot(_snap: Snapshot) { /* stub no-op (restored: test_setup path) */ }
unsafe fn ActiveSnapshotSet() -> bool { false /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): syscache IDs from utils/syscache.h */
const AMNAME: c_int = 1;
const RELOID: c_int = 57;
const F_OIDEQ: Oid = 184; /* oideq from fmgroids.h */

/* TODO(pg-port): SearchSysCacheExists1 */
unsafe fn SearchSysCacheExists1(cacheId: c_int, key1: crate::postgres::Datum) -> bool {
    !HeapTupleIsValid(SearchSysCache1(cacheId, key1))
}

/* TODO(pg-port): lsyscache.h get_relation_idx_constraint_oid */
unsafe fn get_relation_idx_constraint_oid(_relid: Oid, _indexId: Oid) -> Oid { 0 /* stub dummy (restored: test_setup path) */ }

/* palloc array helpers */
macro_rules! palloc_array {
    ($ty:ty, $n:expr) => {
        crate::utils::palloc::palloc(core::mem::size_of::<$ty>() * ($n)) as *mut $ty
    };
}
macro_rules! palloc_object {
    ($ty:ty) => {
        crate::utils::palloc::palloc(core::mem::size_of::<$ty>()) as *mut $ty
    };
}

/* libc shims */
extern "C" {
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}
unsafe fn libc_memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int { memcmp(s1, s2, n) }

/*
 * callback argument type for RangeVarCallbackForReindexIndex()
 */
struct ReindexIndexCallbackState {
    params: ReindexParams, /* options from statement */
    locked_table_oid: Oid, /* tracks previously locked table */
}

/*
 * callback arguments for reindex_error_callback()
 */
struct ReindexErrorInfo {
    relname: *mut c_char,
    relnamespace: *mut c_char,
    relkind: c_char,
}

/* helper: display a *const c_char or *mut c_char as &str for format strings */
unsafe fn cstr_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() { return std::borrow::Cow::Borrowed("(null)"); }
    std::ffi::CStr::from_ptr(s).to_string_lossy()
}

/* TODO(pg-port): utils/errcodes.h error code stubs used below */
unsafe fn errcontext(_fmt: *const c_char) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): utils/elog.h ErrorContextCallback */
#[repr(C)]
struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: unsafe fn(*mut c_void),
    pub arg: *mut c_void,
}
static mut error_context_stack: *mut ErrorContextCallback = core::ptr::null_mut();

/* ---------------------------------------------------------------------------
 * Additional stubs needed by translated function bodies below
 * ---------------------------------------------------------------------------
 */

/* TODO(pg-port): utils/memutils.h */
type MemoryContext = *mut c_void;
unsafe fn AllocSetContextCreate(_parent: MemoryContext, _name: *const c_char, _mincontextsize: usize, _initblocksize: usize, _maxblocksize: usize) -> MemoryContext { unimplemented!("STUB AllocSetContextCreate") }
unsafe fn MemoryContextSwitchTo(_context: MemoryContext) -> MemoryContext { unimplemented!("STUB MemoryContextSwitchTo") }
unsafe fn MemoryContextDelete(_context: MemoryContext) { /* stub no-op (restored: test_setup path) */ }
const ALLOCSET_SMALL_SIZES: usize = 0; /* TODO(pg-port) */
const ALLOCSET_DEFAULT_SIZES: usize = 0; /* TODO(pg-port) */

unsafe fn pfree(_ptr: *mut c_void) { /* stub no-op (restored: test_setup path) */ }
unsafe fn palloc(size: usize) -> *mut c_void { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }
unsafe fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): access/htup_details.h NameStr */
unsafe fn NameStr(name: *const [c_char; 64]) -> *const c_char { (*name).as_ptr() }
unsafe fn NameStr_ref(name: &[c_char; 64]) -> *const c_char { name.as_ptr() }

/* TODO(pg-port): catalog/dependency.h */
const DEPENDENCY_PARTITION_PRI_REAL: c_char = b'i' as c_char; /* TODO(pg-port): use correct value */

/* TODO(pg-port): access/heapam.h */
type TableScanDesc = *mut c_void;

/* TODO(pg-port): commands/tablecmds.h */
unsafe fn IndexSetParentIndex_(_idx: Relation, _parent: Oid) { /* stub no-op (restored: test_setup path) */ }

/* TODO(pg-port): parser/parse_utilcmd.h ParseState */
type ParseState = c_void;
unsafe fn parser_errposition(_pstate: *const ParseState, _location: c_int) -> c_int { 0 /* stub dummy (restored: test_setup path) */ }

/* TODO(pg-port): commands/indexcmds.h ChooseRelationName exposed */
unsafe fn ChooseRelationName_stub(_name1: *const c_char, _name2: *const c_char, _label: *const c_char, _namespaceid: Oid, _isconstraint: bool) -> *mut c_char { core::ptr::null_mut() /* stub dummy (restored: test_setup path) */ }

/* ===========================================================================
 * CheckIndexCompatible
 * src/backend/commands/indexcmds.c:177
 * ===========================================================================
 */
/*
 * CheckIndexCompatible
 *   Determine whether an existing index definition is compatible with a
 *   prospective index definition, such that the existing index storage
 *   could become the storage of the new index, avoiding a rebuild.
 */
pub unsafe fn CheckIndexCompatible(
    oldId: Oid,
    accessMethodName: *const c_char,
    attributeList: *const crate::nodes::pg_list::List,
    exclusionOpNames: *const crate::nodes::pg_list::List,
    isWithoutOverlaps: bool,
) -> bool {
    let mut isconstraint: bool;
    let typeIds: *mut Oid;
    let collationIds: *mut Oid;
    let opclassIds: *mut Oid;
    let opclassOptions: *mut Datum;
    let mut accessMethodId: Oid;
    let mut relationId: Oid;
    let tuple: HeapTuple;
    let indexForm: Form_pg_index;
    let accessMethodForm: Form_pg_am;
    let amRoutine: *mut IndexAmRoutine;
    let amcanorder: bool;
    let amsummarizing: bool;
    let coloptions: *mut int16;
    let indexInfo: *mut IndexInfo;
    let numberOfAttributes: c_int;
    let old_natts: c_int;
    let mut ret: bool = true;
    let old_indclass: *mut oidvector;
    let old_indcollation: *mut oidvector;
    let irel: Relation;
    let mut i: c_int;
    let mut d: Datum;

    /* Caller should already have the relation locked in some way. */
    relationId = IndexGetRelation(oldId, false);

    /*
     * We can pretend isconstraint = false unconditionally.  It only serves to
     * decide the text of an error message that should never happen for us.
     */
    isconstraint = false;

    numberOfAttributes = list_length(attributeList);
    /* Assert(numberOfAttributes > 0) */
    /* Assert(numberOfAttributes <= INDEX_MAX_KEYS) */

    /* look up the access method */
    let tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName as *mut c_void));
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!("access method \"{}\" does not exist", cstr_display(accessMethodName)));
    }
    accessMethodForm = GETSTRUCT_pg_am(tuple);
    accessMethodId = (*accessMethodForm).oid;
    amRoutine = GetIndexAmRoutine((*accessMethodForm).amhandler);
    ReleaseSysCache(tuple);

    amcanorder = (*amRoutine).amcanorder;
    amsummarizing = (*amRoutine).amsummarizing;

    /*
     * Compute the operator classes, collations, and exclusion operators for
     * the new index, so we can test whether it's compatible with the existing
     * one.
     */
    indexInfo = makeIndexInfo(
        numberOfAttributes, numberOfAttributes,
        accessMethodId, NIL, NIL,
        false, false, false, false, amsummarizing, isWithoutOverlaps,
    );
    let typeIds = palloc_array!(Oid, numberOfAttributes as usize);
    let collationIds = palloc_array!(Oid, numberOfAttributes as usize);
    let opclassIds = palloc_array!(Oid, numberOfAttributes as usize);
    let opclassOptions = palloc_array!(Datum, numberOfAttributes as usize);
    let coloptions = palloc_array!(int16, numberOfAttributes as usize);
    ComputeIndexAttrs(
        indexInfo,
        typeIds, collationIds, opclassIds, opclassOptions,
        coloptions,
        attributeList as *const crate::nodes::pg_list::List,
        exclusionOpNames as *const crate::nodes::pg_list::List,
        relationId,
        accessMethodName, accessMethodId,
        amcanorder, isconstraint, isWithoutOverlaps,
        InvalidOid, 0, core::ptr::null_mut(),
    );

    /* Get the soon-obsolete pg_index tuple. */
    let tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(oldId));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for index {}", oldId);
    }
    let indexForm = GETSTRUCT_pg_index(tuple);

    /*
     * We don't assess expressions or predicates; assume incompatibility.
     * Also, if the index is invalid for any reason, treat it as incompatible.
     */
    if !(heap_attisnull(tuple, Anum_pg_index_indpred as c_int, core::ptr::null_mut())
        && heap_attisnull(tuple, Anum_pg_index_indexprs as c_int, core::ptr::null_mut())
        && (*indexForm).indisvalid)
    {
        ReleaseSysCache(tuple);
        return false;
    }

    /* Any change in operator class or collation breaks compatibility. */
    let old_natts = (*indexForm).indnkeyatts as c_int;
    /* Assert(old_natts == numberOfAttributes) */

    d = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indcollation);
    let old_indcollation = DatumGetPointer(d) as *mut oidvector;

    d = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indclass);
    let old_indclass = DatumGetPointer(d) as *mut oidvector;

    ret = (libc_memcmp(
               (*old_indclass).values.as_ptr() as *const c_void,
               opclassIds as *const c_void,
               old_natts as usize * core::mem::size_of::<Oid>(),
           ) == 0
        && libc_memcmp(
               (*old_indcollation).values.as_ptr() as *const c_void,
               collationIds as *const c_void,
               old_natts as usize * core::mem::size_of::<Oid>(),
           ) == 0);

    ReleaseSysCache(tuple);

    if !ret {
        return false;
    }

    /* For polymorphic opcintype, column type changes break compatibility. */
    let irel = index_open(oldId, AccessShareLock); /* caller probably has a lock */
    i = 0;
    while i < old_natts {
        if IsPolymorphicType(get_opclass_input_type(*opclassIds.add(i as usize)))
            && (*TupleDescAttr((*irel).rd_att, i)).atttypid != *typeIds.add(i as usize)
        {
            ret = false;
            break;
        }
        i += 1;
    }

    /* Any change in opclass options break compatibility. */
    if ret {
        let oldOpclassOptions = palloc_array!(Datum, old_natts as usize);
        i = 0;
        while i < old_natts {
            *oldOpclassOptions.add(i as usize) = get_attoptions(oldId, (i + 1) as i16);
            i += 1;
        }
        ret = CompareOpclassOptions(oldOpclassOptions, opclassOptions, old_natts);
        pfree(oldOpclassOptions as *mut c_void);
    }

    /* Any change in exclusion operator selections breaks compatibility. */
    if ret && !(*indexInfo).ii_ExclusionOps.is_null() {
        let mut old_operators: *mut Oid = core::ptr::null_mut();
        let mut old_procs: *mut Oid = core::ptr::null_mut();
        let mut old_strats: *mut uint16 = core::ptr::null_mut();

        RelationGetExclusionInfo(irel, &mut old_operators, &mut old_procs, &mut old_strats);
        ret = libc_memcmp(
            old_operators as *const c_void,
            (*indexInfo).ii_ExclusionOps as *const c_void,
            old_natts as usize * core::mem::size_of::<Oid>(),
        ) == 0;

        /* Require an exact input type match for polymorphic operators. */
        if ret {
            i = 0;
            while i < old_natts && ret {
                let mut left: Oid = InvalidOid;
                let mut right: Oid = InvalidOid;
                op_input_types(*(*indexInfo).ii_ExclusionOps.add(i as usize), &mut left, &mut right);
                if (IsPolymorphicType(left) || IsPolymorphicType(right))
                    && (*TupleDescAttr((*irel).rd_att, i)).atttypid != *typeIds.add(i as usize)
                {
                    ret = false;
                    break;
                }
                i += 1;
            }
        }
    }

    index_close(irel, NoLock);
    ret
}

/* ===========================================================================
 * CompareOpclassOptions
 * src/backend/commands/indexcmds.c:361
 * ===========================================================================
 */
/*
 * CompareOpclassOptions
 *
 * Compare per-column opclass options which are represented by arrays of text[]
 * datums.
 */
unsafe fn CompareOpclassOptions(opts1: *const Datum, opts2: *const Datum, natts: c_int) -> bool {
    let mut i: c_int;
    let mut fm = core::mem::zeroed::<FmgrInfo>();

    if opts1.is_null() && opts2.is_null() {
        return true;
    }

    fmgr_info(F_ARRAY_EQ, &mut fm as *mut FmgrInfo);
    i = 0;
    while i < natts {
        let opt1: Datum = if !opts1.is_null() { *opts1.add(i as usize) } else { 0 };
        let opt2: Datum = if !opts2.is_null() { *opts2.add(i as usize) } else { 0 };

        if opt1 == 0 {
            if opt2 == 0 {
                i += 1;
                continue;
            } else {
                return false;
            }
        } else if opt2 == 0 {
            return false;
        }

        /*
         * Compare non-NULL text[] datums.  Use C collation to enforce binary
         * equivalence of texts.
         */
        if !DatumGetBool(FunctionCall2Coll(&mut fm as *mut FmgrInfo, C_COLLATION_OID, opt1, opt2)) {
            return false;
        }
        i += 1;
    }

    true
}

/* ===========================================================================
 * WaitForOlderSnapshots
 * src/backend/commands/indexcmds.c:434
 * ===========================================================================
 */
/*
 * WaitForOlderSnapshots
 *
 * Wait for transactions that might have an older snapshot than the given xmin
 * limit.
 */
pub unsafe fn WaitForOlderSnapshots(limitXmin: TransactionId, progress: bool) {
    let mut n_old_snapshots: c_int = 0;
    let mut i: c_int;
    let old_snapshots: *mut VirtualTransactionId;

    old_snapshots = GetCurrentVirtualXIDs(
        limitXmin, true, false,
        (PROC_IS_AUTOVACUUM | PROC_IN_VACUUM | PROC_IN_SAFE_IC) as c_int,
        &mut n_old_snapshots,
    );
    if progress {
        pgstat_progress_update_param(PROGRESS_WAITFOR_TOTAL, n_old_snapshots as i64);
    }

    i = 0;
    while i < n_old_snapshots {
        if !VirtualTransactionIdIsValid(*old_snapshots.add(i as usize)) {
            i += 1;
            continue; /* found uninteresting in previous cycle */
        }

        if i > 0 {
            /* see if anything's changed ... */
            let mut n_newer_snapshots: c_int = 0;
            let newer_snapshots: *mut VirtualTransactionId = GetCurrentVirtualXIDs(
                limitXmin, true, false,
                (PROC_IS_AUTOVACUUM | PROC_IN_VACUUM | PROC_IN_SAFE_IC) as c_int,
                &mut n_newer_snapshots,
            );
            let mut j: c_int = i;
            while j < n_old_snapshots {
                if !VirtualTransactionIdIsValid(*old_snapshots.add(j as usize)) {
                    j += 1;
                    continue; /* found uninteresting in previous cycle */
                }
                let mut k: c_int = 0;
                while k < n_newer_snapshots {
                    if VirtualTransactionIdEquals(
                        *old_snapshots.add(j as usize),
                        *newer_snapshots.add(k as usize),
                    ) {
                        break;
                    }
                    k += 1;
                }
                if k >= n_newer_snapshots {
                    /* not there anymore */
                    SetInvalidVirtualTransactionId(old_snapshots.add(j as usize));
                }
                j += 1;
            }
            pfree(newer_snapshots as *mut c_void);
        }

        if VirtualTransactionIdIsValid(*old_snapshots.add(i as usize)) {
            /* If requested, publish who we're going to wait for. */
            if progress {
                let holder: *mut PGPROC =
                    ProcNumberGetProc((*old_snapshots.add(i as usize)).procNumber);
                if !holder.is_null() {
                    pgstat_progress_update_param(
                        PROGRESS_WAITFOR_CURRENT_PID,
                        (*holder).pid as i64,
                    );
                }
            }
            VirtualXactLock(*old_snapshots.add(i as usize), true);
        }

        if progress {
            pgstat_progress_update_param(PROGRESS_WAITFOR_DONE, (i + 1) as i64);
        }
        i += 1;
    }
}

/* ===========================================================================
 * DefineIndex
 * src/backend/commands/indexcmds.c:541
 * ===========================================================================
 */
/*
 * DefineIndex
 *   Creates a new index.
 */
pub unsafe fn DefineIndex(
    tableId: Oid,
    stmt: *mut IndexStmt,
    mut indexRelationId: Oid,
    parentIndexId: Oid,
    parentConstraintId: Oid,
    total_parts: c_int,
    is_alter_table: bool,
    check_rights: bool,
    check_not_in_use: bool,
    skip_build: bool,
    quiet: bool,
) -> ObjectAddress {
    let concurrent: bool;
    let mut indexRelationName: *const c_char;
    let accessMethodName: *const c_char;
    let typeIds: *mut Oid;
    let collationIds: *mut Oid;
    let opclassIds: *mut Oid;
    let opclassOptions: *mut Datum;
    let mut accessMethodId: Oid;
    let namespaceId: Oid;
    let mut tablespaceId: Oid;
    let mut createdConstraintId: Oid = InvalidOid;
    let indexColNames: *mut crate::nodes::pg_list::List;
    let allIndexParams: *mut crate::nodes::pg_list::List;
    let rel: Relation;
    let mut tuple: HeapTuple;
    let accessMethodForm: Form_pg_am;
    let amRoutine: *mut IndexAmRoutine;
    let amcanorder: bool;
    let amissummarizing: bool;
    let amoptions: amoptions_function;
    let exclusion: bool;
    let partitioned: bool;
    let safe_index: bool;
    let reloptions: Datum;
    let coloptions: *mut int16;
    let indexInfo: *mut IndexInfo;
    let mut flags: bits16;
    let mut constr_flags: bits16;
    let numberOfAttributes: c_int;
    let numberOfKeyAttributes: c_int;
    let limitXmin: TransactionId;
    let mut address: ObjectAddress = core::mem::zeroed();
    let heaprelid: LockRelId;
    let mut heaplocktag: LOCKTAG = core::mem::zeroed();
    let lockmode: LOCKMODE;
    let snapshot: *mut SnapshotData;
    let mut root_save_userid: Oid = 0;
    let mut root_save_sec_context: c_int = 0;
    let mut root_save_nestlevel: c_int;

    root_save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    /*
     * Some callers need us to run with an empty default_tablespace.
     */
    if (*stmt).reset_default_tblspc {
        let _ = set_config_option(
            b"default_tablespace\0".as_ptr() as *const c_char,
            b"\0".as_ptr() as *const c_char,
            PGC_USERSET, PGC_S_SESSION,
            GUC_ACTION_SAVE, true, 0, false,
        );
    }

    /*
     * Force non-concurrent build on temporary relations.
     */
    if (*stmt).concurrent
        && get_rel_persistence(tableId) != RELPERSISTENCE_TEMP
    {
        concurrent = true;
    } else {
        concurrent = false;
    }

    /*
     * Start progress report.  If we're building a partition, this was already
     * done.
     */
    if !OidIsValid(parentIndexId) {
        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, tableId);
        pgstat_progress_update_param(
            PROGRESS_CREATEIDX_COMMAND,
            if concurrent {
                PROGRESS_CREATEIDX_COMMAND_CREATE_CONCURRENTLY as i64
            } else {
                PROGRESS_CREATEIDX_COMMAND_CREATE as i64
            },
        );
    }

    /* No index OID to report yet */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_INDEX_OID, InvalidOid as i64);

    /* count key attributes in index */
    numberOfKeyAttributes = list_length((*stmt).indexParams);

    /*
     * Calculate the new list of index columns including both key columns and
     * INCLUDE columns.
     */
    allIndexParams = list_concat_copy((*stmt).indexParams, (*stmt).indexIncludingParams);
    numberOfAttributes = list_length(allIndexParams);

    if numberOfKeyAttributes <= 0 {
        ereport!(ERROR, errmsg!("must specify at least one column"));
    }
    if numberOfAttributes > INDEX_MAX_KEYS as c_int {
        ereport!(ERROR, errmsg!("cannot use more than {} columns in an index", INDEX_MAX_KEYS));
    }

    lockmode = if concurrent { ShareUpdateExclusiveLock } else { ShareLock };
    let rel = table_open(tableId, lockmode);

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.
     */
    GetUserIdAndSecContext(&mut root_save_userid, &mut root_save_sec_context);
    SetUserIdAndSecContext(
        (*(*rel).rd_rel).relowner,
        root_save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );

    let namespaceId = RelationGetNamespace(rel);

    /*
     * It has exclusion constraint behavior if it's an EXCLUDE constraint or a
     * temporal PRIMARY KEY/UNIQUE constraint
     */
    let exclusion = !(*stmt).excludeOpNames.is_null() || (*stmt).iswithoutoverlaps;

    /* Ensure that it makes sense to index this kind of relation */
    match (*(*rel).rd_rel).relkind as u8 as char {
        'r' /* RELKIND_RELATION */ | 'm' /* RELKIND_MATVIEW */ | 'p' /* RELKIND_PARTITIONED_TABLE */ => {
            /* OK */
        }
        _ => {
            ereport!(ERROR, errmsg!(
                "cannot create index on relation \"{}\"",
                cstr_display(RelationGetRelationName(rel))
            ));
        }
    }

    /*
     * Establish behavior for partitioned tables.
     */
    let partitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char;
    if partitioned {
        if (*stmt).concurrent {
            ereport!(ERROR, errmsg!(
                "cannot create index on partitioned table \"{}\" concurrently",
                cstr_display(RelationGetRelationName(rel))
            ));
        }
    }

    /* Don't try to CREATE INDEX on temp tables of other backends. */
    if RELATION_IS_OTHER_TEMP(rel) {
        ereport!(ERROR, errmsg!(
            "cannot create indexes on temporary tables of other sessions"
        ));
    }

    if check_not_in_use {
        CheckTableNotInUse(rel, b"CREATE INDEX\0".as_ptr() as *const c_char);
    }

    if check_rights && !IsBootstrapProcessingMode() {
        let aclresult = object_aclcheck(NamespaceRelationId, namespaceId, root_save_userid, ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, ObjectType::OBJECT_SCHEMA, get_namespace_name(namespaceId));
        }
    }

    if !(*stmt).tableSpace.is_null() {
        tablespaceId = get_tablespace_oid((*stmt).tableSpace, false);
        if partitioned && tablespaceId == MyDatabaseTableSpace {
            ereport!(ERROR, errmsg!(
                "cannot specify default tablespace for partitioned relations"
            ));
        }
    } else {
        tablespaceId = GetDefaultTablespace((*(*rel).rd_rel).relpersistence, partitioned);
        /* note InvalidOid is OK in this case */
    }

    /* Check tablespace permissions */
    if check_rights && OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace {
        let aclresult = object_aclcheck(TableSpaceRelationId, tablespaceId, root_save_userid, ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, ObjectType::OBJECT_TABLESPACE, get_tablespace_name(tablespaceId));
        }
    }

    /*
     * Force shared indexes into the pg_global tablespace.
     */
    if (*(*rel).rd_rel).relisshared {
        tablespaceId = GLOBALTABLESPACE_OID;
    } else if tablespaceId == GLOBALTABLESPACE_OID {
        ereport!(ERROR, errmsg!(
            "only shared relations can be placed in pg_global tablespace"
        ));
    }

    /* Choose the index column names. */
    let indexColNames = ChooseIndexColumnNames(allIndexParams);

    /* Select name for index if caller didn't specify */
    indexRelationName = (*stmt).idxname;
    if indexRelationName.is_null() {
        indexRelationName = ChooseIndexName(
            RelationGetRelationName(rel),
            namespaceId,
            indexColNames,
            (*stmt).excludeOpNames,
            (*stmt).primary,
            (*stmt).isconstraint,
        );
    }

    /*
     * look up the access method, verify it can handle the requested features
     */
    accessMethodName = (*stmt).accessMethod;
    let mut tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName as *mut c_void));
    if !HeapTupleIsValid(tuple) {
        /* Hack to provide more-or-less-transparent updating of old RTREE indexes */
        if strcmp(accessMethodName, b"rtree\0".as_ptr() as *const c_char) == 0 {
            ereport!(NOTICE, errmsg!(
                "substituting access method \"gist\" for obsolete method \"rtree\""
            ));
            let accessMethodName = b"gist\0".as_ptr() as *const c_char;
            tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName as *mut c_void));
        }
        if !HeapTupleIsValid(tuple) {
            ereport!(ERROR, errmsg!(
                "access method \"{}\" does not exist",
                cstr_display(accessMethodName)
            ));
        }
    }
    let accessMethodForm = GETSTRUCT_pg_am(tuple);
    accessMethodId = (*accessMethodForm).oid;
    let amRoutine = GetIndexAmRoutine((*accessMethodForm).amhandler);

    pgstat_progress_update_param(PROGRESS_CREATEIDX_ACCESS_METHOD_OID, accessMethodId as i64);

    if (*stmt).unique && !(*stmt).iswithoutoverlaps && !(*amRoutine).amcanunique {
        ereport!(ERROR, errmsg!(
            "access method \"{}\" does not support unique indexes",
            cstr_display(accessMethodName)
        ));
    }
    if !(*stmt).indexIncludingParams.is_null() && (*stmt).indexIncludingParams != NIL && !(*amRoutine).amcaninclude {
        ereport!(ERROR, errmsg!(
            "access method \"{}\" does not support included columns",
            cstr_display(accessMethodName)
        ));
    }
    if numberOfKeyAttributes > 1 && !(*amRoutine).amcanmulticol {
        ereport!(ERROR, errmsg!(
            "access method \"{}\" does not support multicolumn indexes",
            cstr_display(accessMethodName)
        ));
    }
    if exclusion && (*amRoutine).amgettuple.is_none() {
        ereport!(ERROR, errmsg!(
            "access method \"{}\" does not support exclusion constraints",
            cstr_display(accessMethodName)
        ));
    }
    if (*stmt).iswithoutoverlaps
        && strcmp(accessMethodName, b"gist\0".as_ptr() as *const c_char) != 0
    {
        ereport!(ERROR, errmsg!(
            "access method \"{}\" does not support WITHOUT OVERLAPS constraints",
            cstr_display(accessMethodName)
        ));
    }

    let amcanorder = (*amRoutine).amcanorder;
    let amoptions: amoptions_function = (*amRoutine).amoptions;
    let amissummarizing = (*amRoutine).amsummarizing;

    pfree(amRoutine as *mut c_void);
    ReleaseSysCache(tuple);

    /* Validate predicate, if given */
    if !(*stmt).whereClause.is_null() {
        CheckPredicate((*stmt).whereClause as *mut Expr);
    }

    /* Parse AM-specific options */
    reloptions = transformRelOptions(
        0, (*stmt).options, core::ptr::null(), core::ptr::null(), false, false,
    );
    let _ = index_reloptions(amoptions, reloptions, true);

    /*
     * Prepare arguments for index_create, primarily an IndexInfo structure.
     */
    let indexInfo = makeIndexInfo(
        numberOfAttributes, numberOfKeyAttributes, accessMethodId,
        NIL, /* expressions, NIL for now */
        make_ands_implicit((*stmt).whereClause as *mut Expr),
        (*stmt).unique,
        (*stmt).nulls_not_distinct,
        !concurrent,
        concurrent,
        amissummarizing,
        (*stmt).iswithoutoverlaps,
    );

    let typeIds = palloc_array!(Oid, numberOfAttributes as usize);
    let collationIds = palloc_array!(Oid, numberOfAttributes as usize);
    let opclassIds = palloc_array!(Oid, numberOfAttributes as usize);
    let opclassOptions = palloc_array!(Datum, numberOfAttributes as usize);
    let coloptions = palloc_array!(int16, numberOfAttributes as usize);
    ComputeIndexAttrs(
        indexInfo,
        typeIds, collationIds, opclassIds, opclassOptions,
        coloptions, allIndexParams,
        (*stmt).excludeOpNames, tableId,
        accessMethodName, accessMethodId,
        amcanorder, (*stmt).isconstraint, (*stmt).iswithoutoverlaps,
        root_save_userid, root_save_sec_context,
        &mut root_save_nestlevel,
    );

    /* Extra checks when creating a PRIMARY KEY index. */
    if (*stmt).primary {
        index_check_primary_key(rel, indexInfo, is_alter_table, stmt);
    }

    /*
     * If this table is partitioned and we're creating a unique/exclusive index,
     * make sure partition key is a subset of index columns.
     */
    if partitioned && ((*stmt).unique || exclusion) {
        let key = RelationGetPartitionKey_(rel);
        let constraint_type: *const c_char;

        if (*stmt).primary {
            constraint_type = b"PRIMARY KEY\0".as_ptr() as *const c_char;
        } else if (*stmt).unique {
            constraint_type = b"UNIQUE\0".as_ptr() as *const c_char;
        } else if !(*stmt).excludeOpNames.is_null() {
            constraint_type = b"EXCLUDE\0".as_ptr() as *const c_char;
        } else {
            elog!(ERROR, "unknown constraint type");
            constraint_type = core::ptr::null(); /* keep compiler quiet */
        }

        let mut i = 0;
        while i < (*key).partnatts {
            let mut found = false;
            let eq_strategy: c_int;
            let ptkey_eqop: Oid;

            if (*key).strategy == PARTITION_STRATEGY_HASH {
                eq_strategy = HTEqualStrategyNumber as c_int;
            } else {
                eq_strategy = BTEqualStrategyNumber as c_int;
            }

            ptkey_eqop = get_opfamily_member(
                *(*key).partopfamily.add(i as usize),
                *(*key).partopcintype.add(i as usize),
                *(*key).partopcintype.add(i as usize),
                eq_strategy as i16,
            );
            if !OidIsValid(ptkey_eqop) {
                elog!(ERROR,
                    "missing operator {}({},{}) in partition opfamily {}",
                    eq_strategy,
                    *(*key).partopcintype.add(i as usize),
                    *(*key).partopcintype.add(i as usize),
                    *(*key).partopfamily.add(i as usize)
                );
            }

            if *(*key).partattrs.add(i as usize) == 0 {
                ereport!(ERROR, errmsg!(
                    "unsupported {} constraint with partition key definition",
                    cstr_display(constraint_type)
                ));
            }

            let mut j = 0;
            while j < (*indexInfo).ii_NumIndexKeyAttrs as c_int {
                if *(*key).partattrs.add(i as usize) == (*indexInfo).ii_IndexAttrNumbers[j as usize] {
                    let mut idx_opfamily: Oid = InvalidOid;
                    let mut idx_opcintype: Oid = InvalidOid;

                    if (*key).partcollation.add(i as usize).read() != *collationIds.add(j as usize) {
                        j += 1;
                        continue;
                    }

                    if get_opclass_opfamily_and_input_type(*opclassIds.add(j as usize), &mut idx_opfamily, &mut idx_opcintype) {
                        let mut idx_eqop: Oid = InvalidOid;

                        if (*stmt).unique && !(*stmt).iswithoutoverlaps {
                            idx_eqop = get_opfamily_member_for_cmptype(
                                idx_opfamily, idx_opcintype, idx_opcintype, COMPARE_EQ,
                            );
                        } else if exclusion {
                            idx_eqop = *(*indexInfo).ii_ExclusionOps.add(j as usize);
                        }

                        if !OidIsValid(idx_eqop) {
                            ereport!(ERROR, errmsg!(
                                "could not identify an equality operator for type {}",
                                cstr_display(format_type_be(idx_opcintype))
                            ));
                        }

                        if ptkey_eqop == idx_eqop {
                            found = true;
                            break;
                        } else if exclusion {
                            let att = TupleDescAttr(RelationGetDescr(rel), (*(*key).partattrs.add(i as usize) - 1) as c_int);
                            ereport!(ERROR, errmsg!(
                                "cannot match partition key to index on column \"{}\" using non-equal operator \"{}\"",
                                cstr_display(NameStr_ref(&(*att).attname.data)),
                                cstr_display(get_opname(*(*indexInfo).ii_ExclusionOps.add(j as usize)))
                            ));
                        }
                    }
                }
                j += 1;
            }

            if !found {
                let att = TupleDescAttr(
                    RelationGetDescr(rel),
                    (*(*key).partattrs.add(i as usize) - 1) as c_int,
                );
                ereport!(ERROR, errmsg!(
                    "unique constraint on partitioned table must include all partitioning columns"
                ));
            }
            i += 1;
        }
    }

    /*
     * We disallow indexes on system columns.
     */
    {
        let mut i = 0;
        while i < (*indexInfo).ii_NumIndexAttrs as c_int {
            let attno = (*indexInfo).ii_IndexAttrNumbers[i as usize];
            if (attno as c_int) < 0 {
                ereport!(ERROR, errmsg!("index creation on system columns is not supported"));
            }
            if (*TupleDescAttr(RelationGetDescr(rel), attno as c_int - 1)).attgenerated
                == ATTRIBUTE_GENERATED_VIRTUAL
            {
                ereport!(ERROR, errmsg!(
                    "indexes on virtual generated columns are not supported"
                ));
            }
            i += 1;
        }
    }

    /*
     * Also check for system and generated columns used in expressions or predicates.
     */
    if !(*indexInfo).ii_Expressions.is_null() || !(*indexInfo).ii_Predicate.is_null() {
        let mut indexattrs: *mut Bitmapset = core::ptr::null_mut();
        pull_varattnos((*indexInfo).ii_Expressions as *mut Node, 1, &mut indexattrs);
        pull_varattnos((*indexInfo).ii_Predicate as *mut Node, 1, &mut indexattrs);

        let mut i = FirstLowInvalidHeapAttributeNumber + 1;
        while i < 0 {
            if bms_is_member(i - FirstLowInvalidHeapAttributeNumber, indexattrs) {
                ereport!(ERROR, errmsg!("index creation on system columns is not supported"));
            }
            i += 1;
        }

        let mut j: c_int = -1;
        loop {
            j = bms_next_member(indexattrs, j);
            if j < 0 { break; }
            let attno = j + FirstLowInvalidHeapAttributeNumber;
            if (*TupleDescAttr(RelationGetDescr(rel), attno - 1)).attgenerated
                == ATTRIBUTE_GENERATED_VIRTUAL
            {
                ereport!(ERROR, errmsg!(
                    "indexes on virtual generated columns are not supported"
                ));
            }
        }
    }

    /* Is index safe for others to ignore?  See set_indexsafe_procflags() */
    let safe_index = (*indexInfo).ii_Expressions == NIL
        && (*indexInfo).ii_Predicate == NIL;

    /* Report index creation if appropriate */
    if (*stmt).isconstraint && !quiet {
        let constraint_type: *const c_char;
        if (*stmt).primary {
            constraint_type = b"PRIMARY KEY\0".as_ptr() as *const c_char;
        } else if (*stmt).unique {
            constraint_type = b"UNIQUE\0".as_ptr() as *const c_char;
        } else if !(*stmt).excludeOpNames.is_null() {
            constraint_type = b"EXCLUDE\0".as_ptr() as *const c_char;
        } else {
            elog!(ERROR, "unknown constraint type");
            constraint_type = core::ptr::null();
        }
        ereport!(DEBUG1, errmsg!(
            "{} {} will create implicit index \"{}\" for table \"{}\"",
            cstr_display(if is_alter_table { b"ALTER TABLE / ADD\0".as_ptr() as *const c_char }
            else { b"CREATE TABLE /\0".as_ptr() as *const c_char }),
            cstr_display(constraint_type),
            cstr_display(indexRelationName),
            cstr_display(RelationGetRelationName(rel))
        ));
    }

    /*
     * Make the catalog entries for the index, including constraints.
     */
    flags = 0;
    constr_flags = 0;
    if (*stmt).isconstraint { flags |= INDEX_CREATE_ADD_CONSTRAINT; }
    if skip_build || concurrent || partitioned { flags |= INDEX_CREATE_SKIP_BUILD; }
    if (*stmt).if_not_exists { flags |= INDEX_CREATE_IF_NOT_EXISTS; }
    if concurrent { flags |= INDEX_CREATE_CONCURRENT; }
    if partitioned { flags |= INDEX_CREATE_PARTITIONED; }
    if (*stmt).primary { flags |= INDEX_CREATE_IS_PRIMARY; }

    if partitioned && !(*stmt).relation.is_null() && !(*(*stmt).relation).inh {
        let pd = RelationGetPartitionDesc_(rel, true);
        if (*pd).nparts != 0 {
            flags |= INDEX_CREATE_INVALID;
        }
    }

    if (*stmt).deferrable { constr_flags |= INDEX_CONSTR_CREATE_DEFERRABLE; }
    if (*stmt).initdeferred { constr_flags |= INDEX_CONSTR_CREATE_INIT_DEFERRED; }
    if (*stmt).iswithoutoverlaps { constr_flags |= INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS; }

    indexRelationId = index_create(
        rel, indexRelationName, indexRelationId, parentIndexId,
        parentConstraintId,
        (*stmt).oldNumber, indexInfo, indexColNames,
        accessMethodId, tablespaceId,
        collationIds, opclassIds, opclassOptions,
        coloptions, core::ptr::null_mut(), reloptions,
        flags, constr_flags,
        allowSystemTableMods, !check_rights,
        &mut createdConstraintId,
    );

    ObjectAddressSet(&mut address, RelationRelationId, indexRelationId);

    if !OidIsValid(indexRelationId) {
        AtEOXact_GUC(false, root_save_nestlevel);
        SetUserIdAndSecContext(root_save_userid, root_save_sec_context);
        table_close(rel, NoLock);
        if !OidIsValid(parentIndexId) {
            pgstat_progress_end_command();
        }
        return address;
    }

    AtEOXact_GUC(false, root_save_nestlevel);
    root_save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    /* Add any requested comment */
    if !(*stmt).idxcomment.is_null() {
        CreateComments(indexRelationId, RelationRelationId, 0, (*stmt).idxcomment);
    }

    if partitioned {
        let partdesc: PartitionDesc;

        /*
         * Unless caller specified to skip this step (via ONLY), process each
         * partition to make sure they all contain a corresponding index.
         */
        partdesc = RelationGetPartitionDesc_(rel, true);
        if (!(*stmt).relation.is_null() && (*(*stmt).relation).inh || (*stmt).relation.is_null())
            && (*partdesc).nparts > 0
        {
            let nparts = (*partdesc).nparts;
            let part_oids = palloc_array!(Oid, nparts as usize);
            let mut invalidate_parent = false;
            let parentIndex: Relation;
            let parentDesc: TupleDesc;

            if !OidIsValid(parentIndexId) {
                let mut total_parts = total_parts;
                if total_parts < 0 {
                    let children = find_all_inheritors(tableId, NoLock, core::ptr::null_mut());
                    total_parts = list_length(children) - 1;
                    list_free(children);
                }
                pgstat_progress_update_param(
                    PROGRESS_CREATEIDX_PARTITIONS_TOTAL,
                    total_parts as i64,
                );
            }

            memcpy(
                part_oids as *mut c_void,
                (*partdesc).oids as *const c_void,
                core::mem::size_of::<Oid>() * nparts as usize,
            );

            parentIndex = index_open(indexRelationId, lockmode);
            let indexInfo = BuildIndexInfo(parentIndex);
            parentDesc = RelationGetDescr(rel);

            let mut i = 0;
            while i < nparts {
                let childRelid = *part_oids.add(i as usize);
                let childrel: Relation;
                let mut child_save_userid: Oid = 0;
                let mut child_save_sec_context: c_int = 0;
                let mut child_save_nestlevel: c_int;
                let childidxs: *mut crate::nodes::pg_list::List;
                let attmap: *mut AttrMap;
                let mut found = false;

                let childrel = table_open(childRelid, lockmode);

                GetUserIdAndSecContext(&mut child_save_userid, &mut child_save_sec_context);
                SetUserIdAndSecContext(
                    (*(*childrel).rd_rel).relowner,
                    child_save_sec_context | SECURITY_RESTRICTED_OPERATION,
                );
                child_save_nestlevel = NewGUCNestLevel();
                RestrictSearchPath();

                /*
                 * Don't try to create indexes on foreign tables.
                 */
                if (*(*childrel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as c_char {
                    if (*stmt).unique || (*stmt).primary {
                        ereport!(ERROR, errmsg!(
                            "cannot create unique index on partitioned table \"{}\"",
                            cstr_display(RelationGetRelationName(rel))
                        ));
                    }
                    AtEOXact_GUC(false, child_save_nestlevel);
                    SetUserIdAndSecContext(child_save_userid, child_save_sec_context);
                    table_close(childrel, lockmode);
                    i += 1;
                    continue;
                }

                childidxs = RelationGetIndexList_(childrel);
                attmap = build_attrmap_by_name(
                    RelationGetDescr(childrel), parentDesc, false,
                );

                let mut cell = list_head(childidxs);
                while !cell.is_null() {
                    let cldidxid = lfirst_oid(cell);

                    if has_superclass(cldidxid) {
                        cell = lnext(childidxs, cell);
                        continue;
                    }

                    let cldidx = index_open(cldidxid, lockmode);
                    let cldIdxInfo = BuildIndexInfo(cldidx);
                    if CompareIndexInfo(
                        cldIdxInfo, indexInfo,
                        (*cldidx).rd_indcollation,
                        (*parentIndex).rd_indcollation,
                        (*cldidx).rd_opfamily,
                        (*parentIndex).rd_opfamily,
                        attmap,
                    ) {
                        let mut cldConstrOid: Oid = InvalidOid;

                        if createdConstraintId != InvalidOid {
                            cldConstrOid = get_relation_idx_constraint_oid(childRelid, cldidxid);
                            if cldConstrOid == InvalidOid {
                                index_close(cldidx, lockmode);
                                cell = lnext(childidxs, cell);
                                continue;
                            }
                        }

                        /* Attach index to parent and we're done. */
                        IndexSetParentIndex(cldidx, indexRelationId);
                        if createdConstraintId != InvalidOid {
                            ConstraintSetParentConstraint(cldConstrOid, createdConstraintId, childRelid);
                        }

                        if !(*(*cldidx).rd_index).indisvalid {
                            invalidate_parent = true;
                        }

                        found = true;

                        pgstat_progress_incr_param(PROGRESS_CREATEIDX_PARTITIONS_DONE, 1);

                        /* keep lock till commit */
                        index_close(cldidx, NoLock);
                        break;
                    }

                    index_close(cldidx, lockmode);
                    cell = lnext(childidxs, cell);
                }

                list_free(childidxs);
                AtEOXact_GUC(false, child_save_nestlevel);
                SetUserIdAndSecContext(child_save_userid, child_save_sec_context);
                table_close(childrel, NoLock);

                /* If no matching index was found, create our own. */
                if !found {
                    let childStmt: *mut IndexStmt = generateClonedIndexStmt(
                        core::ptr::null_mut(),
                        parentIndex,
                        attmap,
                        core::ptr::null_mut(),
                    );

                    /* Recurse as the starting user ID. */
                    SetUserIdAndSecContext(root_save_userid, root_save_sec_context);
                    let childAddr = DefineIndex(
                        childRelid, childStmt,
                        InvalidOid, /* no predefined OID */
                        indexRelationId, /* this is our child */
                        createdConstraintId,
                        -1,
                        is_alter_table, check_rights,
                        check_not_in_use,
                        skip_build, quiet,
                    );
                    SetUserIdAndSecContext(child_save_userid, child_save_sec_context);

                    if !get_index_isvalid(childAddr.objectId) {
                        invalidate_parent = true;
                    }
                }

                free_attrmap(attmap);
                i += 1;
            }

            index_close(parentIndex, lockmode);

            /*
             * The pg_index row we inserted for this index was marked
             * indisvalid=true.  But if we attached an existing index that is
             * invalid, update our row to invalid too.
             */
            if invalidate_parent {
                let pg_index = table_open(IndexRelationId, RowExclusiveLock);
                let tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexRelationId));
                if !HeapTupleIsValid(tup) {
                    elog!(ERROR, "cache lookup failed for index {}", indexRelationId);
                }
                let newtup = heap_copytuple(tup);
                (*GETSTRUCT_pg_index(newtup)).indisvalid = false;
                CatalogTupleUpdate(pg_index, &mut (*tup).t_self, newtup);
                ReleaseSysCache(tup);
                table_close(pg_index, RowExclusiveLock);
                heap_freetuple(newtup);
                CommandCounterIncrement();
            }
        }

        /*
         * Indexes on partitioned tables are not themselves built, so we're done here.
         */
        AtEOXact_GUC(false, root_save_nestlevel);
        SetUserIdAndSecContext(root_save_userid, root_save_sec_context);
        table_close(rel, NoLock);
        if !OidIsValid(parentIndexId) {
            pgstat_progress_end_command();
        } else {
            pgstat_progress_incr_param(PROGRESS_CREATEIDX_PARTITIONS_DONE, 1);
        }
        return address;
    }

    AtEOXact_GUC(false, root_save_nestlevel);
    SetUserIdAndSecContext(root_save_userid, root_save_sec_context);

    if !concurrent {
        /* Close the heap and we're done, in the non-concurrent case */
        table_close(rel, NoLock);
        if !OidIsValid(parentIndexId) {
            pgstat_progress_end_command();
        } else {
            pgstat_progress_incr_param(PROGRESS_CREATEIDX_PARTITIONS_DONE, 1);
        }
        return address;
    }

    /* save lockrelid and locktag for below, then close rel */
    let heaprelid = (*rel).rd_lockInfo.lockRelId;
    SET_LOCKTAG_RELATION(&mut heaplocktag, heaprelid.dbId, heaprelid.relId);
    table_close(rel, NoLock);

    /*
     * For a concurrent build, commit our current transaction so that the
     * index becomes visible.
     */
    LockRelationIdForSession(&heaprelid as *const _ as *mut _, ShareUpdateExclusiveLock);

    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();

    /* Tell concurrent index builds to ignore us, if index qualifies */
    if safe_index { set_indexsafe_procflags(); }

    {
        let progress_cols = [PROGRESS_CREATEIDX_INDEX_OID, PROGRESS_CREATEIDX_PHASE];
        let progress_vals: [i64; 2] = [
            indexRelationId as i64,
            PROGRESS_CREATEIDX_PHASE_WAIT_1 as i64,
        ];
        pgstat_progress_update_multi_param(2, progress_cols.as_ptr(), progress_vals.as_ptr());
    }

    /*
     * Phase 2 of concurrent index build: wait for lockers and build.
     */
    WaitForLockers(heaplocktag, ShareLock, true);

    PushActiveSnapshot(GetTransactionSnapshot());
    index_concurrently_build(tableId, indexRelationId);
    PopActiveSnapshot();

    CommitTransactionCommand();
    StartTransactionCommand();

    if safe_index { set_indexsafe_procflags(); }

    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_2 as i64);
    WaitForLockers(heaplocktag, ShareLock, true);

    let snapshot = RegisterSnapshot(GetTransactionSnapshot());
    PushActiveSnapshot(snapshot);

    validate_index(tableId, indexRelationId, snapshot);

    let limitXmin = (*snapshot).xmin;

    PopActiveSnapshot();
    UnregisterSnapshot(snapshot);

    CommitTransactionCommand();
    StartTransactionCommand();

    if safe_index { set_indexsafe_procflags(); }

    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_3 as i64);
    WaitForOlderSnapshots(limitXmin, true);

    PushActiveSnapshot(GetTransactionSnapshot());

    /* Index can now be marked valid */
    index_set_state_flags(indexRelationId, INDEX_CREATE_SET_VALID);

    PopActiveSnapshot();

    CacheInvalidateRelcacheByRelid(heaprelid.relId);

    UnlockRelationIdForSession(&heaprelid as *const _ as *mut _, ShareUpdateExclusiveLock);

    pgstat_progress_end_command();

    address
}

/* ===========================================================================
 * CheckPredicate
 * src/backend/commands/indexcmds.c:1842
 * ===========================================================================
 */
/*
 * CheckPredicate
 *   Checks that the given partial-index predicate is valid.
 */
unsafe fn CheckPredicate(predicate: *mut Expr) {
    /*
     * transformExpr() should have already rejected subqueries, aggregates,
     * and window functions, based on the EXPR_KIND_ for a predicate.
     */

    /*
     * A predicate using mutable functions is probably wrong.
     */
    if contain_mutable_functions_after_planning(predicate) {
        ereport!(ERROR, errmsg!(
            "functions in index predicate must be marked IMMUTABLE"
        ));
    }
}

/* ===========================================================================
 * ComputeIndexAttrs
 * src/backend/commands/indexcmds.c:1869
 * ===========================================================================
 */
/*
 * Compute per-index-column information, including indexed column numbers
 * or index expressions, opclasses and their options.
 */
unsafe fn ComputeIndexAttrs(
    indexInfo: *mut IndexInfo,
    typeOids: *mut Oid,
    collationOids: *mut Oid,
    opclassOids: *mut Oid,
    opclassOptions: *mut Datum,
    colOptions: *mut int16,
    attList: *const crate::nodes::pg_list::List, /* list of IndexElem's */
    exclusionOpNames: *const crate::nodes::pg_list::List,
    relId: Oid,
    accessMethodName: *const c_char,
    accessMethodId: Oid,
    amcanorder: bool,
    isconstraint: bool,
    iswithoutoverlaps: bool,
    ddl_userid: Oid,
    ddl_sec_context: c_int,
    ddl_save_nestlevel: *mut c_int,
) {
    let mut nextExclOp: *mut crate::nodes::pg_list::ListCell;
    let nkeycols = (*indexInfo).ii_NumIndexKeyAttrs as c_int;
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;

    /* Allocate space for exclusion operator info, if needed */
    if !exclusionOpNames.is_null() && exclusionOpNames != NIL {
        /* Assert(list_length(exclusionOpNames) == nkeycols) */
        (*indexInfo).ii_ExclusionOps = palloc_array!(Oid, nkeycols as usize);
        (*indexInfo).ii_ExclusionProcs = palloc_array!(Oid, nkeycols as usize);
        (*indexInfo).ii_ExclusionStrats = palloc_array!(uint16, nkeycols as usize);
        nextExclOp = list_head(exclusionOpNames as *const crate::nodes::pg_list::List);
    } else {
        nextExclOp = core::ptr::null_mut();
    }

    /*
     * If this is a WITHOUT OVERLAPS constraint, we need space for exclusion
     * ops, but we don't need to parse anything.
     */
    if iswithoutoverlaps {
        if exclusionOpNames.is_null() || exclusionOpNames == NIL {
            (*indexInfo).ii_ExclusionOps = palloc_array!(Oid, nkeycols as usize);
            (*indexInfo).ii_ExclusionProcs = palloc_array!(Oid, nkeycols as usize);
            (*indexInfo).ii_ExclusionStrats = palloc_array!(uint16, nkeycols as usize);
        }
        nextExclOp = core::ptr::null_mut();
    }

    if OidIsValid(ddl_userid) {
        GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    }

    /* process attributeList */
    let mut attn = 0;
    let mut lc = list_head(attList);
    while !lc.is_null() {
        let attribute = crate::nodes::pg_list::lfirst(lc) as *mut IndexElem;
        let mut atttype: Oid;
        let mut attcollation: Oid;

        /* Process the column-or-expression to be indexed. */
        if !(*attribute).name.is_null() {
            /* Simple index attribute */
            let atttuple = SearchSysCacheAttName(relId, (*attribute).name);
            if !HeapTupleIsValid(atttuple) {
                if isconstraint {
                    ereport!(ERROR, errmsg!(
                        "column \"{}\" named in key does not exist",
                        cstr_display((*attribute).name)
                    ));
                } else {
                    ereport!(ERROR, errmsg!(
                        "column \"{}\" does not exist",
                        cstr_display((*attribute).name)
                    ));
                }
            }
            let attform = GETSTRUCT(atttuple) as *mut FormData_pg_attribute;
            (*indexInfo).ii_IndexAttrNumbers[attn as usize] = (*attform).attnum;
            atttype = (*attform).atttypid;
            attcollation = (*attform).attcollation;
            ReleaseSysCache(atttuple);
        } else {
            /* Index expression */
            let mut expr = (*attribute).expr as *mut Node;
            /* Assert(!expr.is_null()) */

            if attn >= nkeycols {
                ereport!(ERROR, errmsg!(
                    "expressions are not supported in included columns"
                ));
            }
            atttype = exprType(expr);
            attcollation = exprCollation(expr);

            /* Strip any top-level COLLATE clause. */
            while IsA_local!(expr, CollateExpr) {
                expr = (*( expr as *mut CollateExpr)).arg as *mut Node;
                /* C also: expr = (Node *) ((CollateExpr *) expr)->arg; */
            }

            if IsA_local!(expr, Var)
                && (*( expr as *mut Var)).varattno != InvalidAttrNumber
            {
                /* User wrote "(column)" or "(column COLLATE something)". */
                (*indexInfo).ii_IndexAttrNumbers[attn as usize] = (*(expr as *mut Var)).varattno;
            } else {
                (*indexInfo).ii_IndexAttrNumbers[attn as usize] = 0; /* marks expression */
                (*indexInfo).ii_Expressions = lappend(
                    (*indexInfo).ii_Expressions,
                    expr as *mut c_void,
                );

                if contain_mutable_functions_after_planning(expr as *mut Expr) {
                    ereport!(ERROR, errmsg!(
                        "functions in index expression must be marked IMMUTABLE"
                    ));
                }
            }
        }

        *typeOids.add(attn as usize) = atttype;

        /* Included columns have no collation, no opclass and no ordering options. */
        if attn >= nkeycols {
            if !(*attribute).collation.is_null() {
                ereport!(ERROR, errmsg!("including column does not support a collation"));
            }
            if !(*attribute).opclass.is_null() {
                ereport!(ERROR, errmsg!("including column does not support an operator class"));
            }
            if (*attribute).ordering != SORTBY_DEFAULT {
                ereport!(ERROR, errmsg!("including column does not support ASC/DESC options"));
            }
            if (*attribute).nulls_ordering != SORTBY_NULLS_DEFAULT {
                ereport!(ERROR, errmsg!("including column does not support NULLS FIRST/LAST options"));
            }

            *opclassOids.add(attn as usize) = InvalidOid;
            *opclassOptions.add(attn as usize) = 0;
            *colOptions.add(attn as usize) = 0;
            *collationOids.add(attn as usize) = InvalidOid;
            attn += 1;
            lc = lnext(attList, lc);
            continue;
        }

        /* Apply collation override if any. */
        if !(*attribute).collation.is_null() {
            if OidIsValid(ddl_userid) {
                AtEOXact_GUC(false, *ddl_save_nestlevel);
                SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
            }
            attcollation = get_collation_oid((*attribute).collation, false);
            if OidIsValid(ddl_userid) {
                SetUserIdAndSecContext(save_userid, save_sec_context);
                *ddl_save_nestlevel = NewGUCNestLevel();
                RestrictSearchPath();
            }
        }

        /* Check we have a collation iff it's a collatable type. */
        if type_is_collatable(atttype) {
            if !OidIsValid(attcollation) {
                ereport!(ERROR, errmsg!(
                    "could not determine which collation to use for index expression"
                ));
            }
        } else {
            if OidIsValid(attcollation) {
                ereport!(ERROR, errmsg!(
                    "collations are not supported by type {}",
                    cstr_display(format_type_be(atttype))
                ));
            }
        }

        *collationOids.add(attn as usize) = attcollation;

        /* Identify the opclass to use. */
        if OidIsValid(ddl_userid) {
            AtEOXact_GUC(false, *ddl_save_nestlevel);
            SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
        }
        *opclassOids.add(attn as usize) = ResolveOpClass(
            (*attribute).opclass,
            atttype,
            accessMethodName,
            accessMethodId,
        );
        if OidIsValid(ddl_userid) {
            SetUserIdAndSecContext(save_userid, save_sec_context);
            *ddl_save_nestlevel = NewGUCNestLevel();
            RestrictSearchPath();
        }

        /* Identify the exclusion operator, if any. */
        if !nextExclOp.is_null() {
            let opname = crate::nodes::pg_list::lfirst(nextExclOp) as *const crate::nodes::pg_list::List;
            let opid: Oid;
            let opfamily: Oid;
            let strat: c_int;

            if OidIsValid(ddl_userid) {
                AtEOXact_GUC(false, *ddl_save_nestlevel);
                SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
            }
            let opid = compatible_oper_opid(opname, atttype, atttype, false);
            if OidIsValid(ddl_userid) {
                SetUserIdAndSecContext(save_userid, save_sec_context);
                *ddl_save_nestlevel = NewGUCNestLevel();
                RestrictSearchPath();
            }

            /* Only allow commutative operators in exclusion constraints. */
            if get_commutator(opid) != opid {
                ereport!(ERROR, errmsg!(
                    "operator {} is not commutative",
                    cstr_display(format_operator(opid))
                ));
            }

            /* Operator must be a member of the right opfamily */
            let opfamily = get_opclass_family(*opclassOids.add(attn as usize));
            let strat = get_op_opfamily_strategy(opid, opfamily);
            if strat == 0 {
                ereport!(ERROR, errmsg!(
                    "operator {} is not a member of operator family \"{}\"",
                    cstr_display(format_operator(opid)),
                    cstr_display(get_opfamily_name(opfamily, false))
                ));
            }

            *(*indexInfo).ii_ExclusionOps.add(attn as usize) = opid;
            *(*indexInfo).ii_ExclusionProcs.add(attn as usize) = get_opcode(opid);
            *(*indexInfo).ii_ExclusionStrats.add(attn as usize) = strat as uint16;
            nextExclOp = lnext(exclusionOpNames as *const crate::nodes::pg_list::List, nextExclOp);
        } else if iswithoutoverlaps {
            let cmptype: CompareType;
            let mut strat: StrategyNumber = 0;
            let mut opid: Oid = InvalidOid;

            if attn == nkeycols - 1 {
                cmptype = COMPARE_OVERLAP;
            } else {
                cmptype = COMPARE_EQ;
            }
            GetOperatorFromCompareType(*opclassOids.add(attn as usize), InvalidOid, cmptype, &mut opid, &mut strat);
            *(*indexInfo).ii_ExclusionOps.add(attn as usize) = opid;
            *(*indexInfo).ii_ExclusionProcs.add(attn as usize) = get_opcode(opid);
            *(*indexInfo).ii_ExclusionStrats.add(attn as usize) = strat;
        }

        /*
         * Set up the per-column options (indoption field).
         */
        *colOptions.add(attn as usize) = 0;
        if amcanorder {
            /* default ordering is ASC */
            if (*attribute).ordering == SORTBY_DESC {
                *colOptions.add(attn as usize) |= INDOPTION_DESC;
            }
            /* default null ordering is LAST for ASC, FIRST for DESC */
            if (*attribute).nulls_ordering == SORTBY_NULLS_DEFAULT {
                if (*attribute).ordering == SORTBY_DESC {
                    *colOptions.add(attn as usize) |= INDOPTION_NULLS_FIRST;
                }
            } else if (*attribute).nulls_ordering == SORTBY_NULLS_FIRST {
                *colOptions.add(attn as usize) |= INDOPTION_NULLS_FIRST;
            }
        } else {
            /* index AM does not support ordering */
            if (*attribute).ordering != SORTBY_DEFAULT {
                ereport!(ERROR, errmsg!(
                    "access method \"{}\" does not support ASC/DESC options",
                    cstr_display(accessMethodName)
                ));
            }
            if (*attribute).nulls_ordering != SORTBY_NULLS_DEFAULT {
                ereport!(ERROR, errmsg!(
                    "access method \"{}\" does not support NULLS FIRST/LAST options",
                    cstr_display(accessMethodName)
                ));
            }
        }

        /* Set up the per-column opclass options (attoptions field). */
        if !(*attribute).opclassopts.is_null() {
            /* Assert(attn < nkeycols) */
            *opclassOptions.add(attn as usize) = transformRelOptions(
                0, (*attribute).opclassopts, core::ptr::null(), core::ptr::null(), false, false,
            );
        } else {
            *opclassOptions.add(attn as usize) = 0;
        }

        attn += 1;
        lc = lnext(attList, lc);
    }
}

/* ===========================================================================
 * ResolveOpClass (full body)
 * src/backend/commands/indexcmds.c:2259
 * ===========================================================================
 */
/*
 * ResolveOpClass
 *   Resolve possibly-defaulted operator class specification.
 */
pub unsafe fn ResolveOpClass_full(
    opclass: *const crate::nodes::pg_list::List,
    attrType: Oid,
    accessMethodName: *const c_char,
    accessMethodId: Oid,
) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut opcname: *mut c_char = core::ptr::null_mut();
    let tuple: HeapTuple;
    let opform: Form_pg_opclass;
    let opClassId: Oid;
    let opInputType: Oid;

    if opclass.is_null() || opclass == NIL {
        /* no operator class specified, so find the default */
        let opClassId = GetDefaultOpClass(attrType, accessMethodId);
        if !OidIsValid(opClassId) {
            ereport!(ERROR, errmsg!(
                "data type {} has no default operator class for access method \"{}\"",
                cstr_display(format_type_be(attrType)), cstr_display(accessMethodName)
            ));
        }
        return opClassId;
    }

    /* Specific opclass name given, so look up the opclass. */

    /* deconstruct the name list */
    DeconstructQualifiedName(opclass, &mut schemaname, &mut opcname);

    let tuple = if !schemaname.is_null() {
        /* Look in specific schema only */
        let namespaceId = LookupExplicitNamespace(schemaname, false);
        SearchSysCache3(
            CLAAMNAMENSP,
            ObjectIdGetDatum(accessMethodId),
            PointerGetDatum(opcname as *mut c_void),
            ObjectIdGetDatum(namespaceId),
        )
    } else {
        /* Unqualified opclass name, so search the search path */
        let opClassId = OpclassnameGetOpcid(accessMethodId, opcname);
        if !OidIsValid(opClassId) {
            ereport!(ERROR, errmsg!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                cstr_display(opcname), cstr_display(accessMethodName)
            ));
        }
        SearchSysCache1(CLAOID, ObjectIdGetDatum(opClassId))
    };

    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!(
            "operator class \"{}\" does not exist for access method \"{}\"",
            cstr_display(NameListToString(opclass)), cstr_display(accessMethodName)
        ));
    }

    /*
     * Verify that the index operator class accepts this datatype.
     */
    let opform = GETSTRUCT_pg_opclass(tuple);
    let opClassId = (*opform).oid;
    let opInputType = (*opform).opcintype;

    if !IsBinaryCoercible(attrType, opInputType) {
        ereport!(ERROR, errmsg!(
            "operator class \"{}\" does not accept data type {}",
            cstr_display(NameListToString(opclass)), cstr_display(format_type_be(attrType))
        ));
    }

    ReleaseSysCache(tuple);

    opClassId
}

/* ===========================================================================
 * GetDefaultOpClass (full body)
 * src/backend/commands/indexcmds.c:2344
 * ===========================================================================
 */
/*
 * GetDefaultOpClass
 *
 * Given the OIDs of a datatype and an access method, find the default
 * operator class, if any.  Returns InvalidOid if there is none.
 */
pub unsafe fn GetDefaultOpClass_full(type_id_in: Oid, am_id: Oid) -> Oid {
    let mut result: Oid = InvalidOid;
    let mut nexact: c_int = 0;
    let mut ncompatible: c_int = 0;
    let mut ncompatiblepreferred: c_int = 0;
    let rel: Relation;
    let mut skey: [ScanKeyData; 1] = [core::mem::zeroed()];
    let scan: *mut SysScanDesc;
    let tup: HeapTuple;
    let tcategory: TYPCATEGORY;

    /* If it's a domain, look at the base type instead */
    let type_id = getBaseType(type_id_in);

    let tcategory = TypeCategory(type_id);

    /*
     * We scan through all the opclasses available for the access method,
     * looking for one that is marked default and matches the target type.
     */
    let rel = table_open(OperatorClassRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_opclass_opcmethod,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(am_id),
    );

    let scan = systable_beginscan(rel, OpclassAmNameNspIndexId, true, core::ptr::null_mut(), 1, skey.as_mut_ptr());

    let mut _it = 0;
    loop {
        _it += 1;
        let tup = systable_getnext(scan as *mut crate::access::index::genam::SysScanDescData) as HeapTuple;
        if !HeapTupleIsValid(tup) { break; }

        let opclass = GETSTRUCT_pg_opclass(tup);

        /* ignore altogether if not a default opclass */
        if !(*opclass).opcdefault { continue; }

        if (*opclass).opcintype == type_id {
            nexact += 1;
            result = (*opclass).oid;
        } else if nexact == 0 && IsBinaryCoercible(type_id, (*opclass).opcintype) {
            if IsPreferredType(tcategory, (*opclass).opcintype) {
                ncompatiblepreferred += 1;
                result = (*opclass).oid;
            } else if ncompatiblepreferred == 0 {
                ncompatible += 1;
                result = (*opclass).oid;
            }
        }
    }

    systable_endscan(scan as *mut crate::access::index::genam::SysScanDescData);

    table_close(rel, AccessShareLock);

    /* raise error if pg_opclass contains inconsistent data */
    if nexact > 1 {
        ereport!(ERROR, errmsg!(
            "there are multiple default operator classes for data type {}",
            cstr_display(format_type_be(type_id))
        ));
    }

    if nexact == 1
        || ncompatiblepreferred == 1
        || (ncompatiblepreferred == 0 && ncompatible == 1)
    {
        return result;
    }

    InvalidOid
}

/* TYPCATEGORY already defined above */

/* ===========================================================================
 * GetOperatorFromCompareType
 * src/backend/commands/indexcmds.c:2446
 * ===========================================================================
 */
/*
 * GetOperatorFromCompareType
 *
 * Finds an operator from a CompareType.
 */
pub unsafe fn GetOperatorFromCompareType(
    opclass: Oid,
    rhstype: Oid,
    cmptype: CompareType,
    opid: *mut Oid,
    strat: *mut StrategyNumber,
) {
    let amid: Oid;
    let mut opfamily: Oid = InvalidOid;
    let mut opcintype: Oid = InvalidOid;

    /* Assert(cmptype == COMPARE_EQ || cmptype == COMPARE_OVERLAP || cmptype == COMPARE_CONTAINED_BY) */

    let amid = get_opclass_method(opclass);

    *opid = InvalidOid;

    if get_opclass_opfamily_and_input_type(opclass, &mut opfamily, &mut opcintype) {
        /* Ask the index AM to translate to its internal stratnum */
        *strat = IndexAmTranslateCompareType(cmptype, amid, opfamily, true);
        if *strat == InvalidStrategy {
            ereport!(ERROR, errmsg!(
                "could not translate compare type {} for operator family of access method",
                cmptype as c_int
            ));
        }

        /* We parameterize rhstype so foreign keys can ask for a <@ operator */
        let rhstype = if !OidIsValid(rhstype) { opcintype } else { rhstype };
        *opid = get_opfamily_member(opfamily, opcintype, rhstype, *strat as i16);
    }

    if !OidIsValid(*opid) {
        ereport!(ERROR, errmsg!(
            "could not identify an operator for type {}",
            cstr_display(format_type_be(opcintype))
        ));
    }
}

/* ===========================================================================
 * makeObjectName
 * src/backend/commands/indexcmds.c:2517
 * ===========================================================================
 */
/*
 * makeObjectName
 *
 * Create a name for an implicitly created index, sequence, constraint, etc.
 */
pub unsafe fn makeObjectName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
) -> *mut c_char {
    let name: *mut c_char;
    let mut overhead: usize = 0;
    let mut availchars: usize;
    let mut name1chars: usize;
    let mut name2chars: usize;
    let mut ndx: usize;

    name1chars = strlen(name1);
    if !name2.is_null() {
        name2chars = strlen(name2);
        overhead += 1; /* allow for separating underscore */
    } else {
        name2chars = 0;
    }
    if !label.is_null() {
        overhead += strlen(label) + 1;
    }

    availchars = NAMEDATALEN - 1 - overhead;
    /* Assert(availchars > 0) */

    /*
     * If we must truncate, preferentially truncate the longer name.
     */
    while name1chars + name2chars > availchars {
        if name1chars > name2chars {
            name1chars -= 1;
        } else {
            name2chars -= 1;
        }
    }

    name1chars = pg_mbcliplen(name1, name1chars, name1chars);
    if !name2.is_null() {
        name2chars = pg_mbcliplen(name2, name2chars, name2chars);
    }

    /* Now construct the string using the chosen lengths */
    let name = crate::utils::palloc::palloc(name1chars + name2chars + overhead + 1) as *mut c_char;
    memcpy(name as *mut c_void, name1 as *const c_void, name1chars);
    ndx = name1chars;
    if !name2.is_null() {
        *name.add(ndx) = b'_' as c_char;
        ndx += 1;
        memcpy(name.add(ndx) as *mut c_void, name2 as *const c_void, name2chars);
        ndx += name2chars;
    }
    if !label.is_null() {
        *name.add(ndx) = b'_' as c_char;
        ndx += 1;
        strcpy(name.add(ndx), label);
    } else {
        *name.add(ndx) = 0;
    }

    name
}

/* ===========================================================================
 * ChooseRelationName
 * src/backend/commands/indexcmds.c:2605
 * ===========================================================================
 */
/*
 * ChooseRelationName
 *
 * Select a nonconflicting name for a new relation.
 */
pub unsafe fn ChooseRelationName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
    namespaceid: Oid,
    isconstraint: bool,
) -> *mut c_char {
    let mut pass: c_int = 0;
    let mut relname: *mut c_char = core::ptr::null_mut();
    let mut modlabel: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let mut SnapshotDirty: SnapshotData = core::mem::zeroed();
    let pgclassrel: Relation;

    /* prepare to search pg_class with a dirty snapshot */
    InitDirtySnapshot(&mut SnapshotDirty);
    let pgclassrel = table_open(RelationRelationId, AccessShareLock);

    /* try the unmodified label first */
    strlcpy(modlabel.as_mut_ptr(), label, NAMEDATALEN);

    loop {
        let mut key: [ScanKeyData; 2] = [core::mem::zeroed(); 2];
        let scan: *mut SysScanDesc;
        let collides: bool;

        relname = makeObjectName(name1, name2, modlabel.as_ptr());

        /* is there any conflicting relation name? */
        ScanKeyInit(
            &mut key[0],
            Anum_pg_class_relname,
            BTEqualStrategyNumber,
            F_NAMEEQ,
            CStringGetDatum(relname),
        );
        ScanKeyInit(
            &mut key[1],
            Anum_pg_class_relnamespace,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(namespaceid),
        );

        let scan = systable_beginscan(
            pgclassrel, ClassNameNspIndexId,
            true, /* indexOK */
            (&mut SnapshotDirty) as *mut SnapshotData as *mut c_void,
            2, key.as_mut_ptr(),
        );

        let collides = HeapTupleIsValid(systable_getnext(scan as *mut crate::access::index::genam::SysScanDescData) as HeapTuple);

        systable_endscan(scan as *mut crate::access::index::genam::SysScanDescData);

        if !collides {
            if !isconstraint || !ConstraintNameExists(relname, namespaceid) {
                break;
            }
        }

        /* found a conflict, so try a new name component */
        pfree(relname as *mut c_void);
        pass += 1;
        snprintf(
            modlabel.as_mut_ptr(), NAMEDATALEN,
            b"{}{}\0".as_ptr() as *const c_char,
            label, pass,
        );
    }

    table_close(pgclassrel, AccessShareLock);

    relname
}

/* TODO(pg-port): utils/builtins.h */
unsafe fn CStringGetDatum(s: *const c_char) -> Datum { s as Datum }
const F_NAMEEQ: Oid = 69; /* TODO(pg-port) */

/* ===========================================================================
 * ChooseIndexName (static)
 * src/backend/commands/indexcmds.c:2673
 * ===========================================================================
 */
unsafe fn ChooseIndexName(
    tabname: *const c_char,
    namespaceId: Oid,
    colnames: *const crate::nodes::pg_list::List,
    exclusionOpNames: *const crate::nodes::pg_list::List,
    primary: bool,
    isconstraint: bool,
) -> *const c_char {
    let indexname: *mut c_char;

    if primary {
        /* the primary key's name does not depend on the specific column(s) */
        let indexname = ChooseRelationName(tabname, core::ptr::null(), b"pkey\0".as_ptr() as *const c_char, namespaceId, true);
        return indexname;
    } else if !exclusionOpNames.is_null() && exclusionOpNames != NIL {
        let indexname = ChooseRelationName(
            tabname,
            ChooseIndexNameAddition(colnames),
            b"excl\0".as_ptr() as *const c_char,
            namespaceId,
            true,
        );
        return indexname;
    } else if isconstraint {
        let indexname = ChooseRelationName(
            tabname,
            ChooseIndexNameAddition(colnames),
            b"key\0".as_ptr() as *const c_char,
            namespaceId,
            true,
        );
        return indexname;
    } else {
        let indexname = ChooseRelationName(
            tabname,
            ChooseIndexNameAddition(colnames),
            b"idx\0".as_ptr() as *const c_char,
            namespaceId,
            false,
        );
        return indexname;
    }
}

/* ===========================================================================
 * ChooseIndexNameAddition (static)
 * src/backend/commands/indexcmds.c:2728
 * ===========================================================================
 */
unsafe fn ChooseIndexNameAddition(colnames: *const crate::nodes::pg_list::List) -> *const c_char {
    let mut buf: [c_char; NAMEDATALEN * 2] = [0; NAMEDATALEN * 2];
    let mut buflen: usize = 0;

    buf[0] = 0;
    let mut lc = list_head(colnames);
    while !lc.is_null() {
        let name = crate::nodes::pg_list::lfirst(lc) as *const c_char;

        if buflen > 0 {
            buf[buflen] = b'_' as c_char; /* insert _ between names */
            buflen += 1;
        }

        strlcpy(buf.as_mut_ptr().add(buflen), name, NAMEDATALEN);
        buflen += strlen(buf.as_ptr().add(buflen));
        if buflen >= NAMEDATALEN {
            break;
        }
        lc = lnext(colnames, lc);
    }
    pstrdup(buf.as_ptr())
}

/* ===========================================================================
 * ChooseIndexColumnNames (static)
 * src/backend/commands/indexcmds.c:2762
 * ===========================================================================
 */
unsafe fn ChooseIndexColumnNames(
    indexElems: *const crate::nodes::pg_list::List,
) -> *mut crate::nodes::pg_list::List {
    let mut result: *mut crate::nodes::pg_list::List = NIL;
    let mut lc = list_head(indexElems);

    while !lc.is_null() {
        let ielem = crate::nodes::pg_list::lfirst(lc) as *const IndexElem;
        let origname: *const c_char;
        let mut curname: *const c_char;
        let mut i: c_int;
        let mut buf: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

        /* Get the preliminary name from the IndexElem */
        if !(*ielem).indexcolname.is_null() {
            origname = (*ielem).indexcolname; /* caller-specified name */
        } else if !(*ielem).name.is_null() {
            origname = (*ielem).name; /* simple column reference */
        } else {
            origname = b"expr\0".as_ptr() as *const c_char; /* default name for expression */
        }

        /* If it conflicts with any previous column, tweak it */
        curname = origname;
        i = 1;
        loop {
            let mut lc2 = list_head(result as *const crate::nodes::pg_list::List);
            while !lc2.is_null() {
                if strcmp(curname, crate::nodes::pg_list::lfirst(lc2) as *const c_char) == 0 {
                    break;
                }
                lc2 = lnext(result as *const crate::nodes::pg_list::List, lc2);
            }
            if lc2.is_null() {
                break; /* found nonconflicting name */
            }

            let mut nbuf: [c_char; 32] = [0; 32];
            snprintf(nbuf.as_mut_ptr(), 32, b"{}\0".as_ptr() as *const c_char, i);
            let nlen = pg_mbcliplen(origname, strlen(origname), NAMEDATALEN - 1 - strlen(nbuf.as_ptr()));
            memcpy(buf.as_mut_ptr() as *mut c_void, origname as *const c_void, nlen);
            strcpy(buf.as_mut_ptr().add(nlen), nbuf.as_ptr());
            curname = buf.as_ptr();
            i += 1;
        }

        /* And attach to the result list */
        result = lappend(result, pstrdup(curname) as *mut c_void);
        lc = lnext(indexElems, lc);
    }
    result
}

/* ===========================================================================
 * ExecReindex
 * src/backend/commands/indexcmds.c:2823
 * ===========================================================================
 */
/*
 * ExecReindex
 *
 * Primary entry point for manual REINDEX commands.
 */
pub unsafe fn ExecReindex(
    pstate: *mut ParseState,
    stmt: *const ReindexStmt,
    isTopLevel: bool,
) {
    let mut params: ReindexParams = core::mem::zeroed();
    let mut concurrently = false;
    let mut verbose = false;
    let mut tablespacename: *const c_char = core::ptr::null();

    /* Parse option list */
    let mut lc = list_head((*stmt).params as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let opt = crate::nodes::pg_list::lfirst(lc) as *const DefElem;

        if strcmp((*opt).defname, b"verbose\0".as_ptr() as *const c_char) == 0 {
            verbose = defGetBoolean(opt);
        } else if strcmp((*opt).defname, b"concurrently\0".as_ptr() as *const c_char) == 0 {
            concurrently = defGetBoolean(opt);
        } else if strcmp((*opt).defname, b"tablespace\0".as_ptr() as *const c_char) == 0 {
            tablespacename = defGetString(opt);
        } else {
            ereport!(ERROR, errmsg!(
                "unrecognized {} option \"{}\"",
                cstr_display(b"REINDEX\0".as_ptr() as *const c_char),
                cstr_display((*opt).defname)
            ));
        }
        lc = lnext((*stmt).params as *const crate::nodes::pg_list::List, lc);
    }

    if concurrently {
        PreventInTransactionBlock(isTopLevel, b"REINDEX CONCURRENTLY\0".as_ptr() as *const c_char);
    }

    params.options =
        (if verbose { REINDEXOPT_VERBOSE } else { 0 })
        | (if concurrently { REINDEXOPT_CONCURRENTLY } else { 0 });

    if !tablespacename.is_null() {
        params.tablespaceOid = get_tablespace_oid(tablespacename, false);

        /* Check permissions except when moving to database's default */
        if OidIsValid(params.tablespaceOid) && params.tablespaceOid != MyDatabaseTableSpace {
            let aclresult = object_aclcheck(
                TableSpaceRelationId, params.tablespaceOid, GetUserId(), ACL_CREATE,
            );
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, ObjectType::OBJECT_TABLESPACE, get_tablespace_name(params.tablespaceOid));
            }
        }
    } else {
        params.tablespaceOid = InvalidOid;
    }

    match (*stmt).kind {
        REINDEX_OBJECT_INDEX => {
            ReindexIndex(stmt, &params, isTopLevel);
        }
        REINDEX_OBJECT_TABLE => {
            let _ = ReindexTable(stmt, &params, isTopLevel);
        }
        REINDEX_OBJECT_SCHEMA | REINDEX_OBJECT_SYSTEM | REINDEX_OBJECT_DATABASE => {
            /*
             * This cannot run inside a user transaction block.
             */
            let block_name = if (*stmt).kind == REINDEX_OBJECT_SCHEMA {
                b"REINDEX SCHEMA\0".as_ptr() as *const c_char
            } else if (*stmt).kind == REINDEX_OBJECT_SYSTEM {
                b"REINDEX SYSTEM\0".as_ptr() as *const c_char
            } else {
                b"REINDEX DATABASE\0".as_ptr() as *const c_char
            };
            PreventInTransactionBlock(isTopLevel, block_name);
            ReindexMultipleTables(stmt, &params);
        }
        /* C also: default: elog(ERROR, ...) - all enum variants covered above */
    }
}

/* ===========================================================================
 * ReindexIndex (static)
 * src/backend/commands/indexcmds.c:2918
 * ===========================================================================
 */
unsafe fn ReindexIndex(
    stmt: *const ReindexStmt,
    params: *const ReindexParams,
    isTopLevel: bool,
) {
    let indexRelation = (*stmt).relation;
    let mut state = ReindexIndexCallbackState {
        params: *params,
        locked_table_oid: InvalidOid,
    };
    let indOid: Oid;
    let persistence: c_char;
    let relkind: c_char;

    /*
     * Find and lock index, and check permissions on table.
     */
    state.params = *params;
    state.locked_table_oid = InvalidOid;
    let indOid = RangeVarGetRelidExtended(
        indexRelation,
        if ((*params).options & REINDEXOPT_CONCURRENTLY) != 0 {
            ShareUpdateExclusiveLock
        } else {
            AccessExclusiveLock
        },
        0,
        RangeVarCallbackForReindexIndex,
        &mut state as *mut ReindexIndexCallbackState as *mut c_void,
    );

    let persistence = get_rel_persistence(indOid);
    let relkind = get_rel_relkind(indOid);

    if relkind == RELKIND_PARTITIONED_INDEX as c_char {
        ReindexPartitions(stmt, indOid, params, isTopLevel);
    } else if ((*params).options & REINDEXOPT_CONCURRENTLY) != 0
        && persistence != RELPERSISTENCE_TEMP
    {
        ReindexRelationConcurrently(stmt, indOid, params);
    } else {
        let mut newparams = *params;
        newparams.options |= REINDEXOPT_REPORT_PROGRESS;
        reindex_index(stmt, indOid, false, persistence, &newparams);
    }
}

/* ===========================================================================
 * RangeVarCallbackForReindexIndex (static)
 * src/backend/commands/indexcmds.c:2972
 * ===========================================================================
 */
/*
 * Check permissions on table before acquiring relation lock; also lock
 * the heap before the RangeVarGetRelidExtended takes the index lock.
 */
unsafe fn RangeVarCallbackForReindexIndex(
    relation: *const RangeVar,
    relId: Oid,
    oldRelId: Oid,
    arg: *mut c_void,
) {
    let mut relkind: c_char;
    let state = arg as *mut ReindexIndexCallbackState;
    let table_lockmode: LOCKMODE;
    let mut table_oid: Oid;

    table_lockmode = if ((*state).params.options & REINDEXOPT_CONCURRENTLY) != 0 {
        ShareUpdateExclusiveLock
    } else {
        ShareLock
    };

    /*
     * If we previously locked some other index's heap, release it.
     */
    if relId != oldRelId && OidIsValid(oldRelId) {
        UnlockRelationOid((*state).locked_table_oid, table_lockmode);
        (*state).locked_table_oid = InvalidOid;
    }

    /* If the relation does not exist, there's nothing more to do. */
    if !OidIsValid(relId) {
        return;
    }

    let relkind = get_rel_relkind(relId);
    if relkind == 0 as c_char {
        return;
    }
    if relkind != RELKIND_INDEX as c_char
        && relkind != RELKIND_PARTITIONED_INDEX as c_char
    {
        ereport!(ERROR, errmsg!(
            "\"{}\" is not an index",
            cstr_display((*relation).relname)
        ));
    }

    /* Check permissions */
    let table_oid = IndexGetRelation(relId, true);
    if OidIsValid(table_oid) {
        let aclresult = pg_class_aclcheck(table_oid, GetUserId(), ACL_MAINTAIN);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, ObjectType::OBJECT_INDEX, (*relation).relname);
        }
    }

    /* Lock heap before index to avoid deadlock. */
    if relId != oldRelId {
        if OidIsValid(table_oid) {
            LockRelationOid(table_oid, table_lockmode);
            (*state).locked_table_oid = table_oid;
        }
    }
}

/* ===========================================================================
 * ReindexTable (static)
 * src/backend/commands/indexcmds.c:3048
 * ===========================================================================
 */
unsafe fn ReindexTable(
    stmt: *const ReindexStmt,
    params: *const ReindexParams,
    isTopLevel: bool,
) -> Oid {
    let heapOid: Oid;
    let result: bool;
    let relation = (*stmt).relation;

    let heapOid = RangeVarGetRelidExtended(
        relation,
        if ((*params).options & REINDEXOPT_CONCURRENTLY) != 0 {
            ShareUpdateExclusiveLock
        } else {
            ShareLock
        },
        0,
        RangeVarCallbackMaintainsTable,
        core::ptr::null_mut(),
    );

    if get_rel_relkind(heapOid) == RELKIND_PARTITIONED_TABLE as c_char {
        ReindexPartitions(stmt, heapOid, params, isTopLevel);
    } else if ((*params).options & REINDEXOPT_CONCURRENTLY) != 0
        && get_rel_persistence(heapOid) != RELPERSISTENCE_TEMP
    {
        let result = ReindexRelationConcurrently(stmt, heapOid, params);
        if !result {
            ereport!(NOTICE, errmsg!(
                "table \"{}\" has no indexes that can be reindexed concurrently",
                cstr_display((*relation).relname)
            ));
        }
    } else {
        let mut newparams = *params;
        newparams.options |= REINDEXOPT_REPORT_PROGRESS;
        let result = reindex_relation(
            stmt, heapOid,
            REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
            &newparams,
        );
        if !result {
            ereport!(NOTICE, errmsg!(
                "table \"{}\" has no indexes to reindex",
                cstr_display((*relation).relname)
            ));
        }
    }

    heapOid
}

/* ===========================================================================
 * ReindexMultipleTables (static)
 * src/backend/commands/indexcmds.c:3107
 * ===========================================================================
 */
unsafe fn ReindexMultipleTables(stmt: *const ReindexStmt, params: *const ReindexParams) {
    let objectOid: Oid;
    let relationRelation: Relation;
    let scan: TableScanDesc;
    let mut scan_keys: [ScanKeyData; 1] = [core::mem::zeroed()];
    let tup: HeapTuple;
    let private_context: MemoryContext;
    let mut old: MemoryContext;
    let mut relids: *mut crate::nodes::pg_list::List = NIL;
    let num_keys: c_int;
    let mut concurrent_warning = false;
    let mut tablespace_warning = false;
    let objectName = (*stmt).name;
    let objectKind = (*stmt).kind;

    /* Assert: objectKind is SCHEMA/SYSTEM/DATABASE */

    if objectKind == REINDEX_OBJECT_SYSTEM
        && ((*params).options & REINDEXOPT_CONCURRENTLY) != 0
    {
        ereport!(ERROR, errmsg!("cannot reindex system catalogs concurrently"));
    }

    if objectKind == REINDEX_OBJECT_SCHEMA {
        let objectOid = get_namespace_oid(objectName, false);
        if !object_ownercheck(NamespaceRelationId, objectOid, GetUserId())
            && !has_privs_of_role(GetUserId(), ROLE_PG_MAINTAIN)
        {
            aclcheck_error(ACLCHECK_NOT_OWNER, ObjectType::OBJECT_SCHEMA, objectName);
        }
        let objectOid = objectOid;
        num_keys = 1;
        ScanKeyInit(
            &mut scan_keys[0],
            Anum_pg_class_relnamespace,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(objectOid),
        );
    } else {
        let objectOid = MyDatabaseId;
        if !objectName.is_null()
            && strcmp(objectName, get_database_name(objectOid)) != 0
        {
            ereport!(ERROR, errmsg!("can only reindex the currently open database"));
        }
        if !object_ownercheck(DatabaseRelationId, objectOid, GetUserId())
            && !has_privs_of_role(GetUserId(), ROLE_PG_MAINTAIN)
        {
            aclcheck_error(ACLCHECK_NOT_OWNER, ObjectType::OBJECT_DATABASE, get_database_name(objectOid));
        }
        num_keys = 0;
    }

    private_context = AllocSetContextCreate(
        PortalContext,
        b"ReindexMultipleTables\0".as_ptr() as *const c_char,
        ALLOCSET_SMALL_SIZES, ALLOCSET_SMALL_SIZES, ALLOCSET_SMALL_SIZES,
    );

    let relationRelation = table_open(RelationRelationId, AccessShareLock);
    let scan = table_beginscan_catalog(relationRelation, num_keys, scan_keys.as_ptr());
    loop {
        let tup = heap_getnext(scan, ForwardScanDirection);
        if tup.is_null() { break; }

        let classtuple = GETSTRUCT_pg_class(tup);
        let relid = (*classtuple).oid;

        /* Only regular tables and matviews can have indexes */
        if (*classtuple).relkind != RELKIND_RELATION as c_char
            && (*classtuple).relkind != RELKIND_MATVIEW as c_char
        {
            continue;
        }

        /* Skip temp tables of other backends */
        if (*classtuple).relpersistence == RELPERSISTENCE_TEMP
            && !isTempNamespace((*classtuple).relnamespace)
        {
            continue;
        }

        /* Check user/system classification. */
        if objectKind == REINDEX_OBJECT_SYSTEM && !IsCatalogRelationOid(relid) {
            continue;
        } else if objectKind == REINDEX_OBJECT_DATABASE && IsCatalogRelationOid(relid) {
            continue;
        }

        /* Restrict reindexing shared catalogs */
        if (*classtuple).relisshared
            && pg_class_aclcheck(relid, GetUserId(), ACL_MAINTAIN) != ACLCHECK_OK
        {
            continue;
        }

        /* Skip system tables for concurrent reindex */
        if ((*params).options & REINDEXOPT_CONCURRENTLY) != 0 && IsCatalogRelationOid(relid) {
            if !concurrent_warning {
                ereport!(WARNING, errmsg!(
                    "cannot reindex system catalogs concurrently, skipping all"
                ));
            }
            concurrent_warning = true;
            continue;
        }

        /* If a new tablespace is set, check if this relation has to be skipped. */
        if OidIsValid((*params).tablespaceOid) {
            let mut skip_rel = false;

            if RELKIND_HAS_STORAGE((*classtuple).relkind)
                && !RelFileNumberIsValid((*classtuple).relfilenode)
            {
                skip_rel = true;
            }

            if IsSystemClass(relid, classtuple as *const c_void) {
                skip_rel = true;
            }

            if skip_rel {
                if !tablespace_warning {
                    ereport!(WARNING, errmsg!("cannot move system relations, skipping all"));
                }
                tablespace_warning = true;
                continue;
            }
        }

        /* Save the list of relation OIDs in private context */
        old = MemoryContextSwitchTo(private_context);

        if relid == RelationRelationId {
            relids = lcons_oid(relid, relids);
        } else {
            relids = lappend_oid(relids, relid);
        }

        MemoryContextSwitchTo(old);
    }
    table_endscan(scan);
    table_close(relationRelation, AccessShareLock);

    /* Process each relation listed in a separate transaction. */
    ReindexMultipleInternal(stmt, relids, params);

    MemoryContextDelete(private_context);
}

/* ===========================================================================
 * reindex_error_callback (static)
 * src/backend/commands/indexcmds.c:3326
 * ===========================================================================
 */
unsafe fn reindex_error_callback(arg: *mut c_void) {
    let errinfo = arg as *const ReindexErrorInfo;
    /* Assert(RELKIND_HAS_PARTITIONS((*errinfo).relkind)) */

    if (*errinfo).relkind == RELKIND_PARTITIONED_TABLE as c_char {
        errcontext(b"while reindexing partitioned table\0".as_ptr() as *const c_char);
        /* C also: errcontext("while reindexing partitioned table \"{}.{}\"", relnamespace, relname) */
    } else if (*errinfo).relkind == RELKIND_PARTITIONED_INDEX as c_char {
        errcontext(b"while reindexing partitioned index\0".as_ptr() as *const c_char);
        /* C also: errcontext("while reindexing partitioned index \"{}.{}\"", relnamespace, relname) */
    }
}

/* ===========================================================================
 * ReindexPartitions (static)
 * src/backend/commands/indexcmds.c:3347
 * ===========================================================================
 */
unsafe fn ReindexPartitions(
    stmt: *const ReindexStmt,
    relid: Oid,
    params: *const ReindexParams,
    isTopLevel: bool,
) {
    let mut partitions: *mut crate::nodes::pg_list::List = NIL;
    let relkind = get_rel_relkind(relid);
    let relname = get_rel_name(relid);
    let relnamespace = get_namespace_name(get_rel_namespace(relid));
    let reindex_context: MemoryContext;
    let inhoids: *mut crate::nodes::pg_list::List;
    let errcallback: ErrorContextCallback;
    let errinfo: ReindexErrorInfo;

    /* Assert(RELKIND_HAS_PARTITIONS(relkind)) */

    /*
     * Check if this runs in a transaction block, with error callback.
     */
    let mut errinfo = ReindexErrorInfo {
        relname: pstrdup(relname),
        relnamespace: pstrdup(relnamespace),
        relkind,
    };
    let mut errcallback = ErrorContextCallback {
        previous: error_context_stack,
        callback: reindex_error_callback,
        arg: &mut errinfo as *mut ReindexErrorInfo as *mut c_void,
    };
    error_context_stack = &mut errcallback;

    PreventInTransactionBlock(
        isTopLevel,
        if relkind == RELKIND_PARTITIONED_TABLE as c_char {
            b"REINDEX TABLE\0".as_ptr() as *const c_char
        } else {
            b"REINDEX INDEX\0".as_ptr() as *const c_char
        },
    );

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;

    reindex_context = AllocSetContextCreate(
        PortalContext,
        b"Reindex\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES, ALLOCSET_DEFAULT_SIZES, ALLOCSET_DEFAULT_SIZES,
    );

    /* ShareLock is enough to prevent schema modifications */
    let inhoids = find_all_inheritors(relid, ShareLock, core::ptr::null_mut());

    let mut lc = list_head(inhoids as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let partoid = lfirst_oid(lc);
        let partkind = get_rel_relkind(partoid);
        let old_context: MemoryContext;

        /* Discard partitioned tables, partitioned indexes and foreign tables. */
        if !RELKIND_HAS_STORAGE(partkind) {
            lc = lnext(inhoids as *const crate::nodes::pg_list::List, lc);
            continue;
        }

        /* Assert: partkind == RELKIND_INDEX || partkind == RELKIND_RELATION */

        let old_context = MemoryContextSwitchTo(reindex_context);
        partitions = lappend_oid(partitions, partoid);
        MemoryContextSwitchTo(old_context);

        lc = lnext(inhoids as *const crate::nodes::pg_list::List, lc);
    }

    ReindexMultipleInternal(stmt, partitions, params);

    MemoryContextDelete(reindex_context);
}

/* ===========================================================================
 * ReindexMultipleInternal (static)
 * src/backend/commands/indexcmds.c:3441
 * ===========================================================================
 */
unsafe fn ReindexMultipleInternal(
    stmt: *const ReindexStmt,
    relids: *const crate::nodes::pg_list::List,
    params: *const ReindexParams,
) {
    PopActiveSnapshot();
    CommitTransactionCommand();

    let mut l = list_head(relids);
    while !l.is_null() {
        let relid = lfirst_oid(l);
        let relkind: c_char;
        let relpersistence: c_char;

        StartTransactionCommand();

        /* functions in indexes may want a snapshot set */
        PushActiveSnapshot(GetTransactionSnapshot());

        /* check if the relation still exists */
        if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)) {
            PopActiveSnapshot();
            CommitTransactionCommand();
            l = lnext(relids, l);
            continue;
        }

        /*
         * Check permissions - extra check here as this runs across multiple transactions.
         */
        if OidIsValid((*params).tablespaceOid)
            && (*params).tablespaceOid != MyDatabaseTableSpace
        {
            let aclresult = object_aclcheck(
                TableSpaceRelationId, (*params).tablespaceOid, GetUserId(), ACL_CREATE,
            );
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, ObjectType::OBJECT_TABLESPACE, get_tablespace_name((*params).tablespaceOid));
            }
        }

        let relkind = get_rel_relkind(relid);
        let relpersistence = get_rel_persistence(relid);

        /* Assert(!RELKIND_HAS_PARTITIONS(relkind)) */

        if ((*params).options & REINDEXOPT_CONCURRENTLY) != 0
            && relpersistence != RELPERSISTENCE_TEMP
        {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_MISSING_OK;
            let _ = ReindexRelationConcurrently(stmt, relid, &newparams);
            if ActiveSnapshotSet() {
                PopActiveSnapshot();
            }
        } else if relkind == RELKIND_INDEX as c_char {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            reindex_index(stmt, relid, false, relpersistence, &newparams);
            PopActiveSnapshot();
        } else {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            let result = reindex_relation(
                stmt, relid,
                REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
                &newparams,
            );
            if result && ((*params).options & REINDEXOPT_VERBOSE) != 0 {
                ereport!(INFO, errmsg!(
                    "table \"{}.{}\" was reindexed",
                    cstr_display(get_namespace_name(get_rel_namespace(relid))),
                    cstr_display(get_rel_name(relid))
                ));
            }
            PopActiveSnapshot();
        }

        CommitTransactionCommand();
        l = lnext(relids, l);
    }

    StartTransactionCommand();
}

/* ===========================================================================
 * ReindexRelationConcurrently (static)
 * src/backend/commands/indexcmds.c:3567
 * ===========================================================================
 */
/*
 * ReindexRelationConcurrently - process REINDEX CONCURRENTLY for given
 * relation OID
 */
unsafe fn ReindexRelationConcurrently(
    stmt: *const ReindexStmt,
    relationOid: Oid,
    params: *const ReindexParams,
) -> bool {
    struct ReindexIndexInfo {
        indexId: Oid,
        tableId: Oid,
        amId: Oid,
        safe: bool, /* for set_indexsafe_procflags */
    }

    let mut heapRelationIds: *mut crate::nodes::pg_list::List = NIL;
    let mut indexIds: *mut crate::nodes::pg_list::List = NIL;
    let mut newIndexIds: *mut crate::nodes::pg_list::List = NIL;
    let mut relationLocks: *mut crate::nodes::pg_list::List = NIL;
    let mut lockTags: *mut crate::nodes::pg_list::List = NIL;
    let private_context: MemoryContext;
    let mut oldcontext: MemoryContext;
    let relkind: c_char;
    let mut relationName: *const c_char = core::ptr::null();
    let mut relationNamespace: *const c_char = core::ptr::null();
    let mut ru0: PGRUsage = core::mem::zeroed();
    let progress_index = [
        PROGRESS_CREATEIDX_COMMAND,
        PROGRESS_CREATEIDX_PHASE,
        PROGRESS_CREATEIDX_INDEX_OID,
        PROGRESS_CREATEIDX_ACCESS_METHOD_OID,
    ];
    let mut progress_vals: [i64; 4] = [0; 4];

    private_context = AllocSetContextCreate(
        PortalContext,
        b"ReindexConcurrent\0".as_ptr() as *const c_char,
        ALLOCSET_SMALL_SIZES, ALLOCSET_SMALL_SIZES, ALLOCSET_SMALL_SIZES,
    );

    if ((*params).options & REINDEXOPT_VERBOSE) != 0 {
        oldcontext = MemoryContextSwitchTo(private_context);
        relationName = get_rel_name(relationOid);
        relationNamespace = get_namespace_name(get_rel_namespace(relationOid));
        pg_rusage_init(&mut ru0);
        MemoryContextSwitchTo(oldcontext);
    }

    let relkind = get_rel_relkind(relationOid);

    /* Extract the list of indexes based on the relation Oid. */
    match relkind as u8 as char {
        'r' /* RELKIND_RELATION */ | 'm' /* RELKIND_MATVIEW */ | 't' /* RELKIND_TOASTVALUE */ => {
            let heapRelation: Relation;

            oldcontext = MemoryContextSwitchTo(private_context);
            heapRelationIds = lappend_oid(heapRelationIds, relationOid);
            MemoryContextSwitchTo(oldcontext);

            if IsCatalogRelationOid(relationOid) {
                ereport!(ERROR, errmsg!("cannot reindex system catalogs concurrently"));
            }

            let heapRelation = if ((*params).options & REINDEXOPT_MISSING_OK) != 0 {
                let r = try_table_open(relationOid, ShareUpdateExclusiveLock);
                if r.is_null() {
                    /* leave if relation does not exist */
                    /* fall through to the NIL check at end */
                    r
                } else { r }
            } else {
                table_open(relationOid, ShareUpdateExclusiveLock)
            };

            if heapRelation.is_null() {
                /* break out of match */
            } else {
                if OidIsValid((*params).tablespaceOid) && IsSystemRelation(heapRelation) {
                    ereport!(ERROR, errmsg!(
                        "cannot move system relation \"{}\"",
                        cstr_display(RelationGetRelationName(heapRelation))
                    ));
                }

                /* Add all the valid indexes of relation to list */
                let mut cell = list_head(RelationGetIndexList_(heapRelation) as *const crate::nodes::pg_list::List);
                while !cell.is_null() {
                    let cellOid = lfirst_oid(cell);
                    let indexRelation = index_open(cellOid, ShareUpdateExclusiveLock);

                    if !(*(*indexRelation).rd_index).indisvalid {
                        ereport!(WARNING, errmsg!(
                            "skipping reindex of invalid index \"{}.{}\"",
                            cstr_display(get_namespace_name(get_rel_namespace(cellOid))),
                            cstr_display(get_rel_name(cellOid))
                        ));
                    } else if (*(*indexRelation).rd_index).indisexclusion {
                        ereport!(WARNING, errmsg!(
                            "cannot reindex exclusion constraint index \"{}.{}\" concurrently, skipping",
                            cstr_display(get_namespace_name(get_rel_namespace(cellOid))),
                            cstr_display(get_rel_name(cellOid))
                        ));
                    } else {
                        oldcontext = MemoryContextSwitchTo(private_context);
                        let idx = palloc_object!(ReindexIndexInfo);
                        (*idx).indexId = cellOid;
                        /* other fields set later */
                        indexIds = lappend(indexIds, idx as *mut c_void);
                        MemoryContextSwitchTo(oldcontext);
                    }

                    index_close(indexRelation, NoLock);
                    cell = lnext(RelationGetIndexList_(heapRelation) as *const crate::nodes::pg_list::List, cell);
                }

                /* Also add the toast indexes */
                if OidIsValid((*(*heapRelation).rd_rel).reltoastrelid) {
                    let toastOid = (*(*heapRelation).rd_rel).reltoastrelid;
                    let toastRelation = table_open(toastOid, ShareUpdateExclusiveLock);

                    oldcontext = MemoryContextSwitchTo(private_context);
                    heapRelationIds = lappend_oid(heapRelationIds, toastOid);
                    MemoryContextSwitchTo(oldcontext);

                    let mut cell2 = list_head(RelationGetIndexList_(toastRelation) as *const crate::nodes::pg_list::List);
                    while !cell2.is_null() {
                        let cellOid = lfirst_oid(cell2);
                        let indexRelation = index_open(cellOid, ShareUpdateExclusiveLock);

                        if !(*(*indexRelation).rd_index).indisvalid {
                            ereport!(WARNING, errmsg!(
                                "skipping reindex of invalid index \"{}.{}\"",
                                cstr_display(get_namespace_name(get_rel_namespace(cellOid))),
                                cstr_display(get_rel_name(cellOid))
                            ));
                        } else {
                            oldcontext = MemoryContextSwitchTo(private_context);
                            let idx = palloc_object!(ReindexIndexInfo);
                            (*idx).indexId = cellOid;
                            indexIds = lappend(indexIds, idx as *mut c_void);
                            MemoryContextSwitchTo(oldcontext);
                        }

                        index_close(indexRelation, NoLock);
                        cell2 = lnext(RelationGetIndexList_(toastRelation) as *const crate::nodes::pg_list::List, cell2);
                    }

                    table_close(toastRelation, NoLock);
                }

                table_close(heapRelation, NoLock);
            }
        }
        'i' /* RELKIND_INDEX */ => {
            let heapId = IndexGetRelation(
                relationOid,
                ((*params).options & REINDEXOPT_MISSING_OK) != 0,
            );

            if !OidIsValid(heapId) {
                /* leave */
            } else {
                if IsCatalogRelationOid(heapId) {
                    ereport!(ERROR, errmsg!("cannot reindex system catalogs concurrently"));
                }

                if IsToastNamespace(get_rel_namespace(relationOid))
                    && !get_index_isvalid(relationOid)
                {
                    ereport!(ERROR, errmsg!("cannot reindex invalid index on TOAST table"));
                }

                let heapRelation = if ((*params).options & REINDEXOPT_MISSING_OK) != 0 {
                    let r = try_table_open(heapId, ShareUpdateExclusiveLock);
                    if r.is_null() {
                        r
                    } else { r }
                } else {
                    table_open(heapId, ShareUpdateExclusiveLock)
                };

                if heapRelation.is_null() {
                    /* leave */
                } else {
                    if OidIsValid((*params).tablespaceOid) && IsSystemRelation(heapRelation) {
                        ereport!(ERROR, errmsg!(
                            "cannot move system relation \"{}\"",
                            cstr_display(get_rel_name(relationOid))
                        ));
                    }
                    table_close(heapRelation, NoLock);

                    oldcontext = MemoryContextSwitchTo(private_context);
                    heapRelationIds = list_make1_oid!(heapId);
                    let idx = palloc_object!(ReindexIndexInfo);
                    (*idx).indexId = relationOid;
                    indexIds = lappend(indexIds, idx as *mut c_void);
                    MemoryContextSwitchTo(oldcontext);
                }
            }
        }
        _ => {
            ereport!(ERROR, errmsg!("cannot reindex this type of relation concurrently"));
        }
    }

    if indexIds == NIL {
        return false;
    }

    /* It's not a shared catalog, so refuse to move it to shared tablespace */
    if (*params).tablespaceOid == GLOBALTABLESPACE_OID {
        ereport!(ERROR, errmsg!(
            "cannot move non-shared relation to tablespace \"{}\"",
            cstr_display(get_tablespace_name((*params).tablespaceOid))
        ));
    }

    /* Assert(heapRelationIds != NIL) */

    /*
     * Phase 1 of REINDEX CONCURRENTLY
     * Create new indexes in the catalog.
     */
    let mut lc = list_head(indexIds as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let concurrentName: *mut c_char;
        let idx = crate::nodes::pg_list::lfirst(lc) as *mut ReindexIndexInfo;
        let newidx: *mut ReindexIndexInfo;
        let newIndexId: Oid;
        let indexRel: Relation;
        let heapRel: Relation;
        let mut save_userid: Oid = 0;
        let mut save_sec_context: c_int = 0;
        let mut save_nestlevel: c_int;
        let newIndexRel: Relation;
        let lockrelid: *mut LockRelId;
        let tablespaceid: Oid;

        let indexRel = index_open((*idx).indexId, ShareUpdateExclusiveLock);
        let heapRel = table_open((*(*indexRel).rd_index).indrelid, ShareUpdateExclusiveLock);

        GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
        SetUserIdAndSecContext(
            (*(*heapRel).rd_rel).relowner,
            save_sec_context | SECURITY_RESTRICTED_OPERATION,
        );
        save_nestlevel = NewGUCNestLevel();
        RestrictSearchPath();

        /* determine safety of this index */
        (*idx).safe = RelationGetIndexExpressions_(indexRel) == NIL
            && RelationGetIndexPredicate_(indexRel) == NIL;

        (*idx).tableId = RelationGetRelid(heapRel);
        (*idx).amId = (*(*indexRel).rd_rel).relam;

        if (*(*indexRel).rd_rel).relpersistence == RELPERSISTENCE_TEMP {
            elog!(ERROR, "cannot reindex a temporary table concurrently");
        }

        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, (*idx).tableId);

        progress_vals[0] = PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY as i64;
        progress_vals[1] = 0; /* initializing */
        progress_vals[2] = (*idx).indexId as i64;
        progress_vals[3] = (*idx).amId as i64;
        pgstat_progress_update_multi_param(4, progress_index.as_ptr(), progress_vals.as_ptr());

        /* Choose a temporary relation name for the new index */
        let concurrentName = ChooseRelationName(
            get_rel_name((*idx).indexId),
            core::ptr::null(),
            b"ccnew\0".as_ptr() as *const c_char,
            get_rel_namespace((*(*indexRel).rd_index).indrelid),
            false,
        );

        /* Choose the new tablespace */
        let tablespaceid = if OidIsValid((*params).tablespaceOid)
            && (*(*heapRel).rd_rel).relkind != RELKIND_TOASTVALUE as c_char
        {
            (*params).tablespaceOid
        } else {
            (*(*indexRel).rd_rel).reltablespace
        };

        let newIndexId = index_concurrently_create_copy(heapRel, (*idx).indexId, tablespaceid, concurrentName);

        let newIndexRel = index_open(newIndexId, ShareUpdateExclusiveLock);

        oldcontext = MemoryContextSwitchTo(private_context);

        let newidx = palloc_object!(ReindexIndexInfo);
        (*newidx).indexId = newIndexId;
        (*newidx).safe = (*idx).safe;
        (*newidx).tableId = (*idx).tableId;
        (*newidx).amId = (*idx).amId;

        newIndexIds = lappend(newIndexIds, newidx as *mut c_void);

        let lockrelid = palloc_object!(LockRelId);
        *lockrelid = (*indexRel).rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid as *mut c_void);
        let lockrelid = palloc_object!(LockRelId);
        *lockrelid = (*newIndexRel).rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid as *mut c_void);

        MemoryContextSwitchTo(oldcontext);

        index_close(indexRel, NoLock);
        index_close(newIndexRel, NoLock);

        AtEOXact_GUC(false, save_nestlevel);
        SetUserIdAndSecContext(save_userid, save_sec_context);
        table_close(heapRel, NoLock);

        if !stmt.is_null() {
            let mut address: ObjectAddress = core::mem::zeroed();
            ObjectAddressSet(&mut address, RelationRelationId, newIndexId);
            EventTriggerCollectSimpleCommand(address, InvalidObjectAddress(), (stmt as *mut Node));
        }

        lc = lnext(indexIds as *const crate::nodes::pg_list::List, lc);
    }

    /* Save the heap lock for visibility checks. */
    let mut lc = list_head(heapRelationIds as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let heapRelation = table_open(lfirst_oid(lc), ShareUpdateExclusiveLock);

        oldcontext = MemoryContextSwitchTo(private_context);

        let lockrelid = palloc_object!(LockRelId);
        *lockrelid = (*heapRelation).rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid as *mut c_void);

        let heaplocktag = palloc_object!(LOCKTAG);
        SET_LOCKTAG_RELATION(heaplocktag, (*lockrelid).dbId, (*lockrelid).relId);
        lockTags = lappend(lockTags, heaplocktag as *mut c_void);

        MemoryContextSwitchTo(oldcontext);

        table_close(heapRelation, NoLock);
        lc = lnext(heapRelationIds as *const crate::nodes::pg_list::List, lc);
    }

    /* Get a session-level lock on each table. */
    let mut lc = list_head(relationLocks as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let lockrelid = crate::nodes::pg_list::lfirst(lc) as *const LockRelId;
        LockRelationIdForSession(lockrelid as *mut LockRelId, ShareUpdateExclusiveLock);
        lc = lnext(relationLocks as *const crate::nodes::pg_list::List, lc);
    }

    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 2 of REINDEX CONCURRENTLY: build new indexes.
     */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_1 as i64);
    WaitForLockersMultiple(lockTags, ShareLock, true);
    CommitTransactionCommand();

    let mut lc = list_head(newIndexIds as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let newidx = crate::nodes::pg_list::lfirst(lc) as *const ReindexIndexInfo;

        StartTransactionCommand();
        CHECK_FOR_INTERRUPTS();

        if (*newidx).safe { set_indexsafe_procflags(); }

        PushActiveSnapshot(GetTransactionSnapshot());

        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, (*newidx).tableId);
        progress_vals[0] = PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY as i64;
        progress_vals[1] = PROGRESS_CREATEIDX_PHASE_BUILD as i64;
        progress_vals[2] = (*newidx).indexId as i64;
        progress_vals[3] = (*newidx).amId as i64;
        pgstat_progress_update_multi_param(4, progress_index.as_ptr(), progress_vals.as_ptr());

        index_concurrently_build((*newidx).tableId, (*newidx).indexId);

        PopActiveSnapshot();
        CommitTransactionCommand();
        lc = lnext(newIndexIds as *const crate::nodes::pg_list::List, lc);
    }

    StartTransactionCommand();

    /*
     * Phase 3 of REINDEX CONCURRENTLY: validate new indexes.
     */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_2 as i64);
    WaitForLockersMultiple(lockTags, ShareLock, true);
    CommitTransactionCommand();

    let mut lc = list_head(newIndexIds as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let newidx = crate::nodes::pg_list::lfirst(lc) as *const ReindexIndexInfo;
        let limitXmin: TransactionId;
        let snapshot: *mut SnapshotData;

        StartTransactionCommand();
        CHECK_FOR_INTERRUPTS();

        if (*newidx).safe { set_indexsafe_procflags(); }

        let snapshot = RegisterSnapshot(GetTransactionSnapshot());
        PushActiveSnapshot(snapshot);

        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, (*newidx).tableId);
        progress_vals[0] = PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY as i64;
        progress_vals[1] = PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN as i64;
        progress_vals[2] = (*newidx).indexId as i64;
        progress_vals[3] = (*newidx).amId as i64;
        pgstat_progress_update_multi_param(4, progress_index.as_ptr(), progress_vals.as_ptr());

        validate_index((*newidx).tableId, (*newidx).indexId, snapshot);

        let limitXmin = (*snapshot).xmin;

        PopActiveSnapshot();
        UnregisterSnapshot(snapshot);

        CommitTransactionCommand();
        StartTransactionCommand();

        pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_3 as i64);
        WaitForOlderSnapshots(limitXmin, true);

        CommitTransactionCommand();
        lc = lnext(newIndexIds as *const crate::nodes::pg_list::List, lc);
    }

    /*
     * Phase 4 of REINDEX CONCURRENTLY: swap indexes.
     */
    StartTransactionCommand();
    set_indexsafe_procflags();

    {
        let mut lc = list_head(indexIds as *const crate::nodes::pg_list::List);
        let mut lc2 = list_head(newIndexIds as *const crate::nodes::pg_list::List);
        while !lc.is_null() && !lc2.is_null() {
            let oldidx = crate::nodes::pg_list::lfirst(lc) as *const ReindexIndexInfo;
            let newidx = crate::nodes::pg_list::lfirst(lc2) as *const ReindexIndexInfo;

            CHECK_FOR_INTERRUPTS();

            let oldName = ChooseRelationName(
                get_rel_name((*oldidx).indexId),
                core::ptr::null(),
                b"ccold\0".as_ptr() as *const c_char,
                get_rel_namespace((*oldidx).tableId),
                false,
            );

            PushActiveSnapshot(GetTransactionSnapshot());

            index_concurrently_swap((*newidx).indexId, (*oldidx).indexId, oldName);

            PopActiveSnapshot();

            CacheInvalidateRelcacheByRelid((*oldidx).tableId);

            CommandCounterIncrement();

            lc = lnext(indexIds as *const crate::nodes::pg_list::List, lc);
            lc2 = lnext(newIndexIds as *const crate::nodes::pg_list::List, lc2);
        }
    }

    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 5 of REINDEX CONCURRENTLY: mark old indexes as dead.
     */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_4 as i64);
    WaitForLockersMultiple(lockTags, AccessExclusiveLock, true);

    let mut lc = list_head(indexIds as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let oldidx = crate::nodes::pg_list::lfirst(lc) as *const ReindexIndexInfo;

        CHECK_FOR_INTERRUPTS();

        PushActiveSnapshot(GetTransactionSnapshot());
        index_concurrently_set_dead((*oldidx).tableId, (*oldidx).indexId);
        PopActiveSnapshot();

        lc = lnext(indexIds as *const crate::nodes::pg_list::List, lc);
    }

    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 6 of REINDEX CONCURRENTLY: drop old indexes.
     */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_5 as i64);
    WaitForLockersMultiple(lockTags, AccessExclusiveLock, true);

    PushActiveSnapshot(GetTransactionSnapshot());

    {
        let objects = new_object_addresses();
        let mut lc = list_head(indexIds as *const crate::nodes::pg_list::List);
        while !lc.is_null() {
            let idx = crate::nodes::pg_list::lfirst(lc) as *const ReindexIndexInfo;
            let mut object: ObjectAddress = core::mem::zeroed();
            object.classId = RelationRelationId;
            object.objectId = (*idx).indexId;
            object.objectSubId = 0;
            add_exact_object_address(&object, objects);
            lc = lnext(indexIds as *const crate::nodes::pg_list::List, lc);
        }

        performMultipleDeletions(
            objects, DROP_RESTRICT,
            PERFORM_DELETION_CONCURRENT_LOCK | PERFORM_DELETION_INTERNAL,
        );
    }

    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Release the session-level lock on the table. */
    let mut lc = list_head(relationLocks as *const crate::nodes::pg_list::List);
    while !lc.is_null() {
        let lockrelid = crate::nodes::pg_list::lfirst(lc) as *const LockRelId;
        UnlockRelationIdForSession(lockrelid as *mut LockRelId, ShareUpdateExclusiveLock);
        lc = lnext(relationLocks as *const crate::nodes::pg_list::List, lc);
    }

    StartTransactionCommand();

    /* Log what we did */
    if ((*params).options & REINDEXOPT_VERBOSE) != 0 {
        if relkind == RELKIND_INDEX as c_char {
            ereport!(INFO, errmsg!(
                "index \"{}.{}\" was reindexed",
                cstr_display(relationNamespace), cstr_display(relationName)
            ));
        } else {
            let mut lc = list_head(newIndexIds as *const crate::nodes::pg_list::List);
            while !lc.is_null() {
                let idx = crate::nodes::pg_list::lfirst(lc) as *const ReindexIndexInfo;
                let indOid = (*idx).indexId;
                ereport!(INFO, errmsg!(
                    "index \"{}.{}\" was reindexed",
                    cstr_display(get_namespace_name(get_rel_namespace(indOid))),
                    cstr_display(get_rel_name(indOid))
                ));
                lc = lnext(newIndexIds as *const crate::nodes::pg_list::List, lc);
            }
            ereport!(INFO, errmsg!(
                "table \"{}.{}\" was reindexed",
                cstr_display(relationNamespace), cstr_display(relationName)
            ));
        }
    }

    MemoryContextDelete(private_context);
    pgstat_progress_end_command();

    true
}

/* ===========================================================================
 * IndexSetParentIndex
 * src/backend/commands/indexcmds.c:4442
 * ===========================================================================
 */
/*
 * IndexSetParentIndex
 *
 * Insert or delete an appropriate pg_inherits tuple to make the given index
 * be a partition of the indicated parent index.
 */
pub unsafe fn IndexSetParentIndex(partitionIdx: Relation, parentOid: Oid) {
    let pg_inherits: Relation;
    let mut key: [ScanKeyData; 2] = [core::mem::zeroed(); 2];
    let scan: *mut crate::access::index::genam::SysScanDescData;
    let partRelid = RelationGetRelid(partitionIdx);
    let tuple: HeapTuple;
    let fix_dependencies: bool;

    /* Make sure this is an index */
    /* Assert: relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX */

    /* Scan pg_inherits for rows linking our index to some parent. */
    let pg_inherits = relation_open(InheritsRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(partRelid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_inherits_inhseqno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(1),
    );
    let scan = systable_beginscan(pg_inherits, InheritsRelidSeqnoIndexId, true, core::ptr::null_mut(), 2, key.as_mut_ptr());
    let tuple = systable_getnext(scan) as HeapTuple;

    let fix_dependencies: bool;
    if !HeapTupleIsValid(tuple) {
        if parentOid == InvalidOid {
            /* No pg_inherits row, and no parent wanted: nothing to do. */
            fix_dependencies = false;
        } else {
            StoreSingleInheritance(partRelid, parentOid, 1);
            fix_dependencies = true;
        }
    } else {
        let inhForm = GETSTRUCT_pg_inherits(tuple);

        if parentOid == InvalidOid {
            /* There exists a pg_inherits row, which we want to clear. */
            CatalogTupleDelete(pg_inherits, &mut (*tuple).t_self);
            fix_dependencies = true;
        } else {
            /* A pg_inherits row exists. */
            if (*inhForm).inhparent != parentOid {
                elog!(ERROR,
                    "bogus pg_inherit row: inhrelid {} inhparent {}",
                    (*inhForm).inhrelid, (*inhForm).inhparent
                );
            }
            /* already in the right state */
            fix_dependencies = false;
        }
    }

    /* done with pg_inherits */
    systable_endscan(scan);
    relation_close(pg_inherits, RowExclusiveLock);

    /* set relhassubclass if an index partition has been added to the parent */
    if OidIsValid(parentOid) {
        LockRelationOid(parentOid, ShareUpdateExclusiveLock);
        SetRelationHasSubclass(parentOid, true);
    }

    /* set relispartition correctly on the partition */
    update_relispartition(partRelid, OidIsValid(parentOid));

    if fix_dependencies {
        if OidIsValid(parentOid) {
            let mut partIdx: ObjectAddress = core::mem::zeroed();
            let mut parentIdx: ObjectAddress = core::mem::zeroed();
            let mut partitionTbl: ObjectAddress = core::mem::zeroed();

            ObjectAddressSet(&mut partIdx, RelationRelationId, partRelid);
            ObjectAddressSet(&mut parentIdx, RelationRelationId, parentOid);
            ObjectAddressSet(
                &mut partitionTbl,
                RelationRelationId,
                (*(*partitionIdx).rd_index).indrelid,
            );
            recordDependencyOn(&partIdx, &parentIdx, DEPENDENCY_PARTITION_PRI);
            recordDependencyOn(&partIdx, &partitionTbl, DEPENDENCY_PARTITION_SEC);
        } else {
            deleteDependencyRecordsForClass(
                RelationRelationId, partRelid,
                RelationRelationId,
                DEPENDENCY_PARTITION_PRI,
            );
            deleteDependencyRecordsForClass(
                RelationRelationId, partRelid,
                RelationRelationId,
                DEPENDENCY_PARTITION_SEC,
            );
        }

        /* make our updates visible */
        CommandCounterIncrement();
    }
}

/* TODO(pg-port): additional stubs for IndexSetParentIndex and update_relispartition */
const F_INT4EQ: Oid = 65; /* TODO(pg-port) */

/* ===========================================================================
 * update_relispartition (static)
 * src/backend/commands/indexcmds.c:4574
 * ===========================================================================
 */
unsafe fn update_relispartition(relationId: Oid, newval: bool) {
    let tup: HeapTuple;
    let classRel: Relation;
    let otid: crate::storage::itemptr::ItemPointerData;

    let classRel = table_open(RelationRelationId, RowExclusiveLock);
    let tup = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(relationId));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for relation {}", relationId);
    }
    let mut otid = (*tup).t_self;
    /* Assert: current relispartition != newval */
    (*GETSTRUCT_pg_class(tup)).relispartition = newval;
    CatalogTupleUpdate(classRel, &mut otid, tup);
    UnlockTuple(classRel, &mut otid, InplaceUpdateTupleLock);
    heap_freetuple(tup);
    table_close(classRel, RowExclusiveLock);
}

/* ===========================================================================
 * set_indexsafe_procflags (static inline)
 * src/backend/commands/indexcmds.c:4612
 * ===========================================================================
 */
/*
 * Set the PROC_IN_SAFE_IC flag in MyProc->statusFlags.
 */
unsafe fn set_indexsafe_procflags() {
    /*
     * This should only be called before installing xid or xmin in MyProc.
     */
    /* Assert(MyProc->xid == InvalidTransactionId && MyProc->xmin == InvalidTransactionId) */

    let myproc = MyProc();
    let pg = ProcGlobal;
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
    (*myproc).statusFlags |= PROC_IN_SAFE_IC;
    *(*pg).statusFlags.add((*myproc).pgxactoff as usize) = (*myproc).statusFlags;
    LWLockRelease(ProcArrayLock());
}
