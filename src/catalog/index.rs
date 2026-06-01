/*-------------------------------------------------------------------------
 *
 * index.rs
 *    code to create and destroy POSTGRES index relations
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/catalog/index.c
 *
 *
 * INTERFACE ROUTINES
 *        index_create()          - Create a cataloged index relation
 *        index_drop()            - Removes index relation from catalogs
 *        BuildIndexInfo()        - Prepare to insert index tuples
 *        FormIndexDatum()        - Construct datum vector for one index tuple
 *
 *-------------------------------------------------------------------------
 */

#![allow(
    non_snake_case,
    non_upper_case_globals,
    non_camel_case_types,
    unused_variables,
    unused_imports,
    dead_code,
    unused_mut,
    clippy::all,
)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

// -- core types -----------------------------------------------------------
use crate::utils::rel::{Relation, RelationData};
use crate::access::common::tupdesc::{
    TupleDesc, TupleDescData, TupleDescAttr,
    CreateTupleDesc, CreateTemplateTupleDesc, FreeTupleDesc,
    TYPALIGN_SHORT, TYPALIGN_INT, TYPALIGN_DOUBLE,
    TYPSTORAGE_PLAIN, TYPSTORAGE_EXTENDED,
};
use crate::access::htup_details::{
    HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT,
};
/// Stub: heap_attisnull -- real impl needs TupleDesc to check var-len nulls.
#[inline]
unsafe fn heap_attisnull(
    tup: *mut HeapTupleData,
    attnum: AttrNumber,
    _tupdesc: TupleDesc,
) -> bool {
    // conservative stub: assume not null so DROP INDEX proceeds
    false /* TODO(pg-port) */
}
use crate::access::common::heaptuple::{
    heap_form_tuple, heap_modify_tuple, heap_freetuple, heap_copytuple,
};
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::storage::lockdefs::{
    LOCKMODE, NoLock, AccessShareLock, RowShareLock, RowExclusiveLock,
    ShareUpdateExclusiveLock, ShareLock, ShareRowExclusiveLock,
    ExclusiveLock, AccessExclusiveLock,
};
use crate::nodes::pg_list::{
    List, ListCell,
    list_head, lnext, lfirst, lfirst_oid, lfirst_int,
    lappend, lappend_oid, lappend_int,
    list_length, list_member_oid, list_copy,
    list_delete_last, list_delete_cell, list_delete_nth_cell,
    list_nth, list_append_unique_oid,
    list_free, list_sort,
};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::nodes::nodes::Node;
use crate::access::transam::InvalidTransactionId;
use crate::access::transam::multixact::InvalidMultiXactId;
use crate::access::transam::xlogprefetcher::InvalidRelFileNumber;
use crate::access::common::relation::{relation_open, relation_close};
use crate::access::index::genam::{systable_beginscan, systable_endscan, SysScanDesc};
use crate::access::index::genam::systable_getnext as _systable_getnext_void;
/// Wrapper: cast genam's *mut c_void result to *mut HeapTupleData
#[inline]
unsafe fn systable_getnext(scan: SysScanDesc) -> *mut HeapTupleData {
    _systable_getnext_void(scan) as *mut HeapTupleData
}
use crate::access::index::indexam::{index_open, index_close};
use crate::access::table::table::{table_open, table_close};

// -- pg_index -----------------------------------------------------------------
use crate::catalog::pg_index::{
    FormData_pg_index, Form_pg_index,
    INDOPTION_DESC, INDOPTION_NULLS_FIRST,
};
// Anum_pg_index_* constants (the fixed + variable columns)
const INDEXRELID: c_int          = 26; // SysCacheIdentifier
const Anum_pg_index_indexrelid: AttrNumber    = 1;
const Anum_pg_index_indrelid: AttrNumber      = 2;
const Anum_pg_index_indnatts: AttrNumber      = 3;
const Anum_pg_index_indnkeyatts: AttrNumber   = 4;
const Anum_pg_index_indisunique: AttrNumber   = 5;
const Anum_pg_index_indnullsnotdistinct: AttrNumber = 6;
const Anum_pg_index_indisprimary: AttrNumber  = 7;
const Anum_pg_index_indisexclusion: AttrNumber = 8;
const Anum_pg_index_indimmediate: AttrNumber  = 9;
const Anum_pg_index_indisclustered: AttrNumber = 10;
const Anum_pg_index_indisvalid: AttrNumber    = 11;
const Anum_pg_index_indcheckxmin: AttrNumber  = 12;
const Anum_pg_index_indisready: AttrNumber    = 13;
const Anum_pg_index_indislive: AttrNumber     = 14;
const Anum_pg_index_indisreplident: AttrNumber = 15;
const Anum_pg_index_indkey: AttrNumber        = 16;
const Anum_pg_index_indcollation: AttrNumber  = 17;
const Anum_pg_index_indclass: AttrNumber      = 18;
const Anum_pg_index_indoption: AttrNumber     = 19;
const Anum_pg_index_indexprs: AttrNumber      = 20;
const Anum_pg_index_indpred: AttrNumber       = 21;
const Natts_pg_index: usize                   = 21;

// -- pg_class, pg_attribute, pg_description, pg_constraint, pg_trigger Anum --
const Anum_pg_class_oid: AttrNumber           = 1;
const Anum_pg_class_reloptions: AttrNumber    = 33;
const Anum_pg_description_objoid: AttrNumber  = 1;
const Anum_pg_description_classoid: AttrNumber = 2;
const Anum_pg_description_objsubid: AttrNumber = 3;
const Natts_pg_description: usize             = 3;
const Anum_pg_trigger_tgconstraint: AttrNumber = 11;
const Anum_pg_trigger_tgconstrindid: AttrNumber = 12;
const Anum_pg_attribute_attstattarget: AttrNumber = 21;

// -- Catalog OIDs ------------------------------------------------------------
use crate::catalog::catalog_oids::{
    AttributeRelationId, RelationRelationId, CollationRelationId,
    ConstraintRelationId, OperatorClassRelationId,
};
const IndexRelationId: Oid          = 2610; // pg_index
const DescriptionRelationId: Oid    = 2609; // pg_description
const TriggerRelationId: Oid        = 2620; // pg_trigger
const DescriptionObjIndexId: Oid    = 2675; // pg_description_o_c_o_index
const TriggerConstraintIndexId: Oid = 2699; // pg_trigger_tgconstraint_index
const ClassOidIndexId: Oid          = 2662; // pg_class_oid_index
const RELOID: c_int                 = 52;   // SysCacheIdentifier
const CONSTROID: c_int              = 12;   // SysCacheIdentifier
const ATTNUM: c_int                 = 4;    // SysCacheIdentifier
const TYPEOID: c_int                = 76;   // SysCacheIdentifier
const CLAOID: c_int                 = 10;   // SysCacheIdentifier
const TEXT_BTREE_PATTERN_OPS_OID: Oid    = 2774;
const VARCHAR_BTREE_PATTERN_OPS_OID: Oid = 2775;
const BPCHAR_BTREE_PATTERN_OPS_OID: Oid  = 2180;
const ANYELEMENTOID: Oid            = 2283;

// -- catalog helpers ---------------------------------------------------------
use crate::catalog::objectaddress_impl::{
    ObjectAddress, OidIsValid, ObjectAddressSet,
    ObjectIdGetDatum, Int16GetDatum, Int32GetDatum,
    BTEqualStrategyNumber, F_OIDEQ, TEXTOID,
    INVALID_OBJECT_ADDRESS,
    SearchSysCache1, SearchSysCacheCopy1, ReleaseSysCache,
    RelationGetRelid, RelationGetRelationName,
    strVal, linitial, NIL,
    format_type_be, TextDatumGetCString, CStringGetTextDatum,
};
const F_INT4EQ: u32 = 65; // fmgroids.h
/// ObjectAddressSubSet!(addr, classId, objectId, subId)
macro_rules! ObjectAddressSubSet {
    ($addr:expr, $classId:expr, $objectId:expr, $subId:expr) => {
        $addr.classId  = $classId;
        $addr.objectId = $objectId;
        $addr.objectSubId = $subId as i32;
    }
}
/// linitial_oid -- first OID in list
#[inline]
unsafe fn linitial_oid(list: *mut crate::nodes::pg_list::List) -> Oid {
    crate::nodes::pg_list::lfirst_oid(crate::nodes::pg_list::list_head(list))
}
use crate::postgres::{BoolGetDatum, CharGetDatum, PointerGetDatum};
use crate::catalog::indexing::{
    CatalogIndexState, CatalogOpenIndexes, CatalogCloseIndexes,
    CatalogTupleInsert, CatalogTupleUpdate, CatalogTupleDelete,
};
use crate::catalog::catalog_oids;

// -- types from c.rs ---------------------------------------------------------
use crate::c::{bits16, int2vector, oidvector, uint16};

// -- IndexInfo, IndexBuildResult, etc. from execnodes / amapi ----------------
use crate::nodes::execnodes::{IndexInfo, INDEX_MAX_KEYS};
use crate::access::index::amapi::{
    IndexAmRoutine, IndexBuildResult, IndexVacuumInfo,
    GetIndexAmRoutineByAmId,
};
use crate::access::table::tableam::ValidateIndexState;

// -- NullableDatum from postgres.rs ------------------------------------------
use crate::postgres::NullableDatum;

// -- pg_class Form -----------------------------------------------------------
use crate::catalog::pg_class::{
    FormData_pg_class, Form_pg_class,
    RELKIND_RELATION, RELKIND_INDEX, RELKIND_TOASTVALUE,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_PARTITIONED_INDEX,
};

// -- pg_attribute Form -------------------------------------------------------
use crate::catalog::pg_attribute::{
    FormData_pg_attribute, Form_pg_attribute, FormExtraData_pg_attribute,
};
use crate::catalog::pg_type_d::ANYARRAYOID;

// -- pg_constraint Form ------------------------------------------------------
use crate::catalog::pg_constraint::{FormData_pg_constraint, Form_pg_constraint};
const CONSTRAINT_PRIMARY: c_char   = b'p' as c_char;
const CONSTRAINT_UNIQUE: c_char    = b'u' as c_char;
const CONSTRAINT_EXCLUSION: c_char = b'x' as c_char;

// -- pg_trigger Form (opaque pointer for swap) --------------------------------
#[repr(C)]
struct FormData_pg_trigger {
    tgconstrindid: Oid,
    // other fields irrelevant here
}
type Form_pg_trigger = *mut FormData_pg_trigger;

// -- lsyscache helpers -------------------------------------------------------
use crate::utils::cache::lsyscache::{
    get_namespace_name, get_rel_name, get_rel_relispartition,
    get_rel_namespace, get_rel_persistence,
    get_relname_relid, get_collation_isdeterministic,
    get_index_isvalid,
    get_base_element_type,
};
/// get_index_constraint -- stub for catalog/index.c usage
#[inline]
unsafe fn get_index_constraint(indexId: Oid) -> Oid {
    InvalidOid /* TODO(pg-port) */
}
/// get_index_ref_constraints -- stub for catalog/index.c usage
#[inline]
unsafe fn get_index_ref_constraints(indexId: Oid) -> *mut crate::nodes::pg_list::List {
    NIL
}

// -- relcache helpers --------------------------------------------------------
use crate::utils::cache::relcache::{
    RelationGetIndexList, IsBinaryUpgrade,
    RelationGetNamespace, RelationGetForm,
    RELKIND_HAS_STORAGE,
};

// -- syscache extras ----------------------------------------------------------
use crate::utils::cache::syscache::{
    SearchSysCache2, SysCacheGetAttr, SysCacheGetAttrNotNull,
};

// -- utils/rel macros (RelationIsValid, etc.) ----------------------------------
use crate::utils::rel::RelationGetDescr;
// Inline: RelationIsMapped, IsSystemRelation, IsCatalogRelation
unsafe fn RelationIsMapped(rel: Relation) -> bool { (*(*rel).rd_rel).relfilenode == 0 }
unsafe fn IsSystemRelation(rel: Relation) -> bool {
    // pg_namespace < FirstUnpinnedObjectId heuristic; real: IsSystemNamespace
    IsSystemNamespace((*(*rel).rd_rel).relnamespace)
}
unsafe fn IsCatalogRelation(rel: Relation) -> bool {
    crate::catalog::catalog::IsCatalogNamespace((*(*rel).rd_rel).relnamespace)
}
use crate::catalog::catalog::{
    IsCatalogNamespace, IsToastNamespace,
    GetNewRelFileNumber,
};
/// IsSystemNamespace -- stub (real impl checks namespaceId < FirstUnpinnedObjectId)
#[inline]
fn IsSystemNamespace(namespaceId: Oid) -> bool {
    namespaceId == 11 /* pg_catalog */ || namespaceId == 99 /* pg_toast */
}

// -- miscadmin ----------------------------------------------------------------
use crate::miscadmin::{IsBootstrapProcessingMode, IsNormalProcessingMode};

// -- heap (this file's sibling) -----------------------------------------------
use crate::catalog::heap::{
    heap_create, InsertPgClassTuple, InsertPgAttributeTuples,
    DeleteRelationTuple, DeleteAttributeTuples,
    CheckAttributeType,
};

// -- storage ------------------------------------------------------------------
use crate::catalog::storage::{RelationDropStorage, RelationCreateStorage};
use crate::catalog::storage_xlog::log_smgrcreate;

// -- lmgr ---------------------------------------------------------------------
use crate::storage::lmgr::lmgr::{
    LockRelation, LockRelationOid,
    LockRelationIdForSession, UnlockRelationIdForSession,
    WaitForLockers,
};
use crate::utils::rel::LockRelId;
use crate::storage::lmgr::lmgr::LOCKTAGData as LOCKTAG;
/// SET_LOCKTAG_RELATION -- initialize a LOCKTAG for a relation lock
#[inline]
unsafe fn SET_LOCKTAG_RELATION(tag: &mut LOCKTAG, dbId: Oid, relId: Oid) {
    // LOCKTAGData layout: locktag_field1..4, locktag_type, locktag_lockmethodid
    tag.locktag_field1 = dbId;
    tag.locktag_field2 = relId;
    tag.locktag_field3 = 0;
    tag.locktag_field4 = 0;
    tag.locktag_type = 0; // LOCKTAG_RELATION
    tag.locktag_lockmethodid = 1; // DEFAULT_LOCKMETHOD
}
macro_rules! SET_LOCKTAG_RELATION {
    ($tag:expr, $dbId:expr, $relId:expr) => {
        SET_LOCKTAG_RELATION(&mut $tag, $dbId, $relId)
    }
}

// -- inval --------------------------------------------------------------------
// utils/inval module not yet wired; provide stubs.
#[inline]
unsafe fn CacheInvalidateRelcache(_rel: Relation) { /* TODO(pg-port) */ }
#[inline]
unsafe fn CacheInvalidateRelcacheByTuple(_tup: *mut HeapTupleData) { /* TODO(pg-port) */ }

// -- binary_upgrade -----------------------------------------------------------
// binary_upgrade globals re-imported below at the usage site (PART 2)

// -- partition / inherits -----------------------------------------------------
use crate::catalog::partition::get_partition_ancestors;
/// StoreSingleInheritance stub (catalog/pg_inherits.c)
#[inline]
unsafe fn StoreSingleInheritance(_inhrelid: Oid, _inhparent: Oid, _inhseqno: i32) {
    /* TODO(pg-port) */
}
/// DeleteInheritsTuple stub
#[inline]
unsafe fn DeleteInheritsTuple(
    _inhrelid: Oid,
    _inhparent: Oid,
    _expect_detached: bool,
    _childname: *const c_char,
) {
    /* TODO(pg-port) */
}
/// SetRelationHasSubclass stub
#[inline]
unsafe fn SetRelationHasSubclass(_relid: Oid, _relhassubclass: bool) {
    /* TODO(pg-port) */
}

// -- dependency ---------------------------------------------------------------
// catalog/pg_depend.c not yet ported; all stubs here.
type ObjectAddresses = c_void;
const DEPENDENCY_NORMAL: c_char    = b'n' as c_char;
const DEPENDENCY_AUTO: c_char      = b'a' as c_char;
const DEPENDENCY_INTERNAL: c_char  = b'i' as c_char;
const DEPENDENCY_PARTITION_PRI: c_char = b'P' as c_char;
const DEPENDENCY_PARTITION_SEC: c_char = b'S' as c_char;
#[inline]
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _deptype: c_char,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn record_object_address_dependencies(
    _depender: *const ObjectAddress,
    _referenced: *mut ObjectAddresses,
    _deptype: c_char,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn recordDependencyOnSingleRelExpr(
    _depender: *const ObjectAddress,
    _expr: *mut crate::nodes::nodes::Node,
    _relId: Oid,
    _selfref_behavior: c_char,
    _default_behavior: c_char,
    _reverse_self: bool,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn deleteDependencyRecordsForClass(
    _classId: Oid,
    _objectId: Oid,
    _refclassId: Oid,
    _deptype: c_char,
) -> i64 { 0 /* TODO(pg-port) */ }
#[inline]
unsafe fn changeDependenciesOf(_classId: Oid, _oldObjectId: Oid, _newObjectId: Oid) -> i64 { 0 }
#[inline]
unsafe fn changeDependenciesOn(_classId: Oid, _objectId: Oid, _newRefObjectId: Oid) -> i64 { 0 }
#[inline]
unsafe fn new_object_addresses() -> *mut ObjectAddresses { core::ptr::null_mut() }
#[inline]
unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) {}
#[inline]
unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) {}

// -- pg_constraint helpers ----------------------------------------------------
// CONSTRAINT_PRIMARY/UNIQUE/EXCLUSION defined locally below at file top level;
// pg_constraint.rs also has them - use the local copies.
const CONSTRAINT_RELATION: c_char = b'r' as c_char; // pg_constraint.h ConstraintRelType
#[inline]
unsafe fn CreateConstraintEntry(
    _constraintName: *const c_char,
    _constraintNamespace: Oid,
    _constraintType: c_char,
    _isDeferrable: bool,
    _isDeferred: bool,
    _isEnforced: bool,
    _isValidated: bool,
    _parentConstrId: Oid,
    _relId: Oid,
    _constraintKey: *const AttrNumber,
    _constraintNKeys: c_int,
    _constraintNTotal: c_int,
    _domainId: Oid,
    _indexRelId: Oid,
    _foreignRelId: Oid,
    _foreignKey: *const AttrNumber,
    _foreignEqOps: *const Oid,
    _foreignUpdOps: *const Oid,
    _foreignDelOps: *const Oid,
    _fkDeleteSetCols: *const AttrNumber,
    _numFkDeleteSetCols: c_int,
    _foreignUpdateType: c_char,
    _foreignDeleteType: c_char,
    _foreignDelSetCols: *const AttrNumber,
    _numFkDelSetCols: c_int,
    _foreignMatchType: c_char,
    _exclOp: *const Oid,
    _conExpr: *const c_char,
    _conBin: *const c_char,
    _conIsLocal: bool,
    _conInhCount: i16,
    _conNoInherit: bool,
    _conWithoutOverlaps: bool,
    _is_internal: bool,
) -> Oid {
    InvalidOid /* TODO(pg-port) */
}
#[inline]
unsafe fn ConstraintNameIsUsed(
    _cctype: c_char,
    _relid: Oid,
    _name: *const c_char,
) -> bool {
    false /* TODO(pg-port) */
}

// -- pg_statistics helpers ----------------------------------------------------
use crate::catalog::heap::{RemoveStatistics, CopyStatistics};

// -- objectaccess hooks -------------------------------------------------------
#[inline]
unsafe fn InvokeObjectPostCreateHookArg(
    _classId: Oid, _objectId: Oid, _subId: c_int, _is_internal: bool,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn InvokeObjectPostAlterHookArg(
    _classId: Oid, _objectId: Oid, _subId: c_int, _auxiliaryId: Oid, _is_internal: bool,
) { /* TODO(pg-port) */ }

// -- event triggers -----------------------------------------------------------
// commands/event_trigger not yet wired; stubs.
#[inline]
unsafe fn EventTriggerCollectSimpleCommand(
    _address: ObjectAddress,
    _secondaryObject: ObjectAddress,
    _parsetree: *mut crate::nodes::nodes::Node,
) { /* TODO(pg-port) */ }
use crate::catalog::objectaddress_impl::INVALID_OBJECT_ADDRESS as InvalidObjectAddress;

// -- nodes / optimizer / parser -----------------------------------------------
use crate::nodes::nodes::NodeTag;
use crate::nodes::primnodes::Expr;
use crate::nodes::read::stringToNode;
use crate::nodes::equalfuncs::equal;
use crate::nodes::makefuncs::{make_ands_explicit, make_ands_implicit};
// makeNode! is a #[macro_export] macro -- accessible as crate::makeNode!
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::parsenodes::{IndexStmt, ReindexStmt, RelFileNumber};
/// map_variable_attnos stub (optimizer/prep/preptlist.c)
#[inline]
unsafe fn map_variable_attnos(
    _node: *mut crate::nodes::nodes::Node,
    _varno: c_int,
    _sublevels_up: c_int,
    _attmap: *const crate::access::common::attmap::AttrMap,
    _rowtype_domain: Oid,
    _found_whole_row: *mut bool,
) -> *mut crate::nodes::nodes::Node {
    core::ptr::null_mut() /* TODO(pg-port) */
}
use crate::access::common::attmap::AttrMap;

// -- executor -----------------------------------------------------------------
use crate::executor::tuptable::TupleTableSlot;
// executor/nodeIndexscan not yet wired; stubs.
#[inline]
unsafe fn ExecPrepareExprList(
    _nodes: *mut crate::nodes::pg_list::List,
    _estate: *mut EState,
) -> *mut crate::nodes::pg_list::List {
    core::ptr::null_mut() /* TODO(pg-port) */
}
#[inline]
unsafe fn ExecPrepareQual(
    _qual: *mut crate::nodes::pg_list::List,
    _estate: *mut EState,
) -> *mut ExprState {
    core::ptr::null_mut() /* TODO(pg-port) */
}
#[inline]
unsafe fn ExecQual(_state: *mut ExprState, _econtext: *mut crate::nodes::execnodes::ExprContext) -> bool {
    true /* TODO(pg-port) */
}
#[inline]
unsafe fn ExecEvalExprSwitchContext(
    _state: *mut ExprState,
    _econtext: *mut crate::nodes::execnodes::ExprContext,
    _isNull: *mut bool,
) -> Datum {
    0 /* TODO(pg-port) */
}
#[inline]
unsafe fn GetPerTupleExprContext(
    estate: *mut EState,
) -> *mut crate::nodes::execnodes::ExprContext {
    core::ptr::null_mut() /* TODO(pg-port) */
}
use crate::nodes::execnodes::{EState, ExprState, ExprContext};
use crate::executor::execUtils::{CreateExecutorState, FreeExecutorState};
use crate::executor::execTuples::ExecDropSingleTupleTableSlot;
use crate::access::table::tableam::table_slot_create;
use crate::access::sdir::ForwardScanDirection;
type TableScanDesc = *mut c_void;
/// table_beginscan_strat stub
#[inline]
unsafe fn table_beginscan_strat(
    _rel: Relation,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
    _allow_strat: bool,
    _allow_sync: bool,
) -> TableScanDesc {
    core::ptr::null_mut() /* TODO(pg-port) */
}
/// table_endscan stub
#[inline]
unsafe fn table_endscan(_scan: TableScanDesc) { /* TODO(pg-port) */ }
/// table_scan_getnextslot stub
#[inline]
unsafe fn table_scan_getnextslot(
    _scan: TableScanDesc,
    _direction: c_int,
    _slot: *mut crate::executor::tuptable::TupleTableSlot,
) -> bool {
    false /* TODO(pg-port) */
}
/// table_index_validate_scan stub
#[inline]
unsafe fn table_index_validate_scan(
    _heapRel: Relation,
    _indexRel: Relation,
    _indexInfo: *mut crate::nodes::execnodes::IndexInfo,
    _snapshot: *mut c_void,
    _state: *mut ValidateIndexState,
) { /* TODO(pg-port) */ }
// executor/execIndexing stubs
#[inline]
unsafe fn check_exclusion_constraint(
    _heapRel: Relation,
    _indexRel: Relation,
    _indexInfo: *mut crate::nodes::execnodes::IndexInfo,
    _tupleid: *mut crate::storage::itemptr::ItemPointerData,
    _values: *mut Datum,
    _isnull: *mut bool,
    _estate: *mut EState,
    _newIndex: bool,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn index_insert_cleanup(
    _indexRelation: Relation,
    _indexInfo: *mut crate::nodes::execnodes::IndexInfo,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn slot_getsysattr(
    _slot: *mut crate::executor::tuptable::TupleTableSlot,
    _attnum: c_int,
    _isnull: *mut bool,
) -> Datum { 0 }
#[inline]
unsafe fn slot_getattr(
    _slot: *mut crate::executor::tuptable::TupleTableSlot,
    _attnum: c_int,
    _isnull: *mut bool,
) -> Datum { 0 }

// -- index AM operations ------------------------------------------------------
use crate::access::index::indexam::{
    index_bulk_delete, index_opclass_options,
    try_index_open,
};
// Functions not yet exported from indexam; provide stubs.
#[inline]
unsafe fn makeIndexInfo(
    _numAttrs: c_int, _numKeyAttrs: c_int, _amoid: Oid,
    _expressions: *mut crate::nodes::pg_list::List,
    _predicate: *mut crate::nodes::pg_list::List,
    _unique: bool, _nullsNotDistinct: bool,
    _ready: bool, _concurrent: bool,
    _summarizing: bool, _withoutOverlaps: bool,
) -> *mut crate::nodes::execnodes::IndexInfo {
    core::ptr::null_mut() /* TODO(pg-port) */
}
#[inline]
unsafe fn RelationInitIndexAccessInfo(_indexRelation: Relation) { /* TODO(pg-port) */ }
#[inline]
unsafe fn index_register(_heapOid: Oid, _indexOid: Oid, _indexInfo: *mut crate::nodes::execnodes::IndexInfo) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn get_attoptions(_indexId: Oid, _attno: c_int) -> Datum { 0 /* TODO(pg-port) */ }
#[inline]
unsafe fn RelationGetIndexExpressions(_indexRelation: Relation) -> *mut crate::nodes::pg_list::List {
    NIL /* TODO(pg-port) */
}
#[inline]
unsafe fn RelationGetDummyIndexExpressions(_indexRelation: Relation) -> *mut crate::nodes::pg_list::List {
    NIL /* TODO(pg-port) */
}
#[inline]
unsafe fn RelationGetIndexPredicate(_indexRelation: Relation) -> *mut crate::nodes::pg_list::List {
    NIL /* TODO(pg-port) */
}
#[inline]
unsafe fn RelationGetExclusionInfo(
    _indexRelation: Relation,
    _operators: *mut *mut Oid,
    _procs: *mut *mut Oid,
    _strats: *mut *mut u16,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn IndexRelationGetNumberOfKeyAttributes(_indexRelation: Relation) -> c_int {
    0 /* TODO(pg-port) */
}
type IndexStateFlagsAction = c_int;
const INDEX_CREATE_SET_READY: IndexStateFlagsAction = 0;
const INDEX_CREATE_SET_VALID: IndexStateFlagsAction = 1;
const INDEX_DROP_CLEAR_VALID: IndexStateFlagsAction = 2;
const INDEX_DROP_SET_DEAD: IndexStateFlagsAction = 3;
#[inline]
unsafe fn index_set_state_flags(_indexId: Oid, _action: IndexStateFlagsAction) {
    /* TODO(pg-port) */
}

// -- smgr / bufmgr ------------------------------------------------------------
use crate::storage::smgr::smgr::{
    smgrexists, smgrcreate,
};
type SMgrRelation = *mut crate::storage::smgr::smgr::SMgrRelationData;
/// RelationGetSmgr stub -- real impl in utils/cache/relcache.c
#[inline]
unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation {
    core::ptr::null_mut() /* TODO(pg-port) */
}
use crate::catalog::pg_class::{RELPERSISTENCE_UNLOGGED, RELPERSISTENCE_PERMANENT};
use crate::common::relpath::INIT_FORKNUM;

// -- snapshot / xact ----------------------------------------------------------
// utils/snapmgr not yet wired; stubs.
type Snapshot2 = *mut c_void; // separate from local `Snapshot` alias
#[inline]
unsafe fn RegisterSnapshot(snap: *mut c_void) -> *mut c_void { snap }
#[inline]
unsafe fn UnregisterSnapshot(_snap: *mut c_void) {}
#[inline]
unsafe fn GetLatestSnapshot() -> *mut c_void { core::ptr::null_mut() }
#[inline]
unsafe fn GetTransactionSnapshot() -> *mut c_void { core::ptr::null_mut() }
#[inline]
unsafe fn PushActiveSnapshot(_snap: *mut c_void) {}
#[inline]
unsafe fn PopActiveSnapshot() {}
// access/transam/xact not yet wired; stubs.
#[inline]
unsafe fn CommandCounterIncrement() { /* TODO(pg-port) */ }
#[inline]
unsafe fn GetTopTransactionIdIfAny() -> TransactionId { InvalidTransactionId }
#[inline]
unsafe fn CommitTransactionCommand() {}
#[inline]
unsafe fn StartTransactionCommand() {}
#[inline]
unsafe fn GetCurrentTransactionNestLevel() -> c_int { 0 }
// access/transam/predicate not yet wired; stub.
#[inline]
unsafe fn TransferPredicateLocksToHeapRelation(_indexRel: Relation) { /* TODO(pg-port) */ }

// -- GUC / security -----------------------------------------------------------
// utils/guc not yet wired; stubs.
#[inline]
unsafe fn NewGUCNestLevel() -> c_int { 0 }
#[inline]
unsafe fn AtEOXact_GUC(_isCommit: bool, _nestLevel: c_int) {}
#[inline]
unsafe fn RestrictSearchPath() {}
#[inline]
unsafe fn AutoVacuumingActive() -> bool { false }
/// StdRdOptions -- minimal subset used here
#[repr(C)]
struct StdRdOptions {
    vl_len_: i32,
    fillfactor: c_int,
    autovacuum: StdRdOptionsAutovacuum,
}
#[repr(C)]
struct StdRdOptionsAutovacuum {
    enabled: bool,
}
use crate::miscadmin::{
    GetUserIdAndSecContext, SetUserIdAndSecContext,
    SECURITY_RESTRICTED_OPERATION,
};

// -- progress reporting -------------------------------------------------------
use crate::utils::activity::backend_progress::{
    pgstat_progress_update_param, pgstat_progress_update_multi_param,
    pgstat_progress_start_command, pgstat_progress_end_command,
};
use crate::utils::activity::backend_progress::ProgressCommandType::PROGRESS_COMMAND_CREATE_INDEX;
use crate::commands::progress::{
    PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_SUBPHASE,
    PROGRESS_CREATEIDX_TUPLES_DONE, PROGRESS_CREATEIDX_TUPLES_TOTAL,
    PROGRESS_CREATEIDX_COMMAND, PROGRESS_CREATEIDX_INDEX_OID,
    PROGRESS_CREATEIDX_ACCESS_METHOD_OID,
    PROGRESS_CREATEIDX_COMMAND_REINDEX,
    PROGRESS_CREATEIDX_PHASE_BUILD,
    PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN,
    PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT,
    PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN,
    PROGRESS_SCAN_BLOCKS_DONE, PROGRESS_SCAN_BLOCKS_TOTAL,
};
use crate::utils::activity::pgstat_relation::pgstat_drop_relation;
const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: c_int = 1; // access/gin/ginutil defines it too
const PROGRESS_CLUSTER_INDEX_REBUILD_COUNT: c_int = 7;
/// pgstat_copy_relation_stats stub
#[inline]
unsafe fn pgstat_copy_relation_stats(_dst: Relation, _src: Relation) { /* TODO(pg-port) */ }

// -- commands/tablecmds helpers -----------------------------------------------
// commands/tablecmds not yet wired; stubs.
type BlockNumber = u32;
#[inline]
unsafe fn CheckTableNotInUse(_rel: Relation, _stmt: *const c_char) { /* TODO(pg-port) */ }
#[inline]
unsafe fn IsInParallelMode() -> bool { false }
#[inline]
fn RELATION_IS_OTHER_TEMP(_rel: Relation) -> bool { false }
#[inline]
unsafe fn CheckRelationTableSpaceMove(_rel: Relation, _tablespaceOid: Oid) -> bool { false }
#[inline]
unsafe fn SetRelationTableSpace(_rel: Relation, _tablespaceOid: Oid, _newrelfilenode: Oid) {}
#[inline]
unsafe fn RelationSetNewRelfilenumber(_rel: Relation, _persistence: c_char) {}
#[inline]
unsafe fn RelationAssumeNewRelfilelocator(_rel: Relation) {}
// RelationDropStorage is imported above from catalog::storage
#[inline]
unsafe fn RelationGetNumberOfBlocks(_rel: Relation) -> BlockNumber { 0 }
#[inline]
unsafe fn visibilitymap_count(_rel: Relation, _all_visible: *mut BlockNumber, _all_frozen: *mut BlockNumber) {}
/// systable_inplace_update -- not yet ported; stubs.
#[inline]
unsafe fn systable_inplace_update_begin(
    _relation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
    _oldtup: *mut *mut HeapTupleData,
    _state: *mut *mut c_void,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn systable_inplace_update_finish(_state: *mut c_void, _tup: *mut HeapTupleData) { /* TODO(pg-port) */ }
#[inline]
unsafe fn systable_inplace_update_cancel(_state: *mut c_void) { /* TODO(pg-port) */ }
/// maintenance_work_mem -- GUC variable
static maintenance_work_mem: c_int = 1024; // 1 MB stub

// -- tuplesort ----------------------------------------------------------------
// utils/sort/tuplesort not yet wired; stubs.
const TUPLESORT_NONE: c_int = 0;
#[inline]
unsafe fn tuplesort_begin_datum(
    _dtype: Oid, _sortOperator: Oid, _sortCollation: Oid,
    _nullsFirstFlag: bool, _workMem: c_int, _coordinate: *mut c_void, _flags: c_int,
) -> *mut c_void {
    core::ptr::null_mut() /* TODO(pg-port) */
}
#[inline]
unsafe fn tuplesort_performsort(_state: *mut c_void) {}
#[inline]
unsafe fn tuplesort_end(_state: *mut c_void) {}
#[inline]
unsafe fn tuplesort_putdatum(_state: *mut c_void, _val: Datum, _isNull: bool) {}
use crate::catalog::pg_known_oids::Int8LessOperator;
use crate::catalog::pg_type_d::INT8OID;
use crate::postgres::Int64GetDatum;
/// itemptr_encode -- encodes an ItemPointer as int64 for sorting
#[inline]
unsafe fn itemptr_encode(_itemptr: *mut crate::storage::itemptr::ItemPointerData) -> i64 {
    0_i64 /* TODO(pg-port) */
}

// -- plan_create_index_workers ------------------------------------------------
/// plan_create_index_workers stub (optimizer not yet wired)
#[inline]
unsafe fn plan_create_index_workers(_heapOid: Oid, _indexOid: Oid) -> c_int {
    0 /* TODO(pg-port) */
}

// -- reindex helpers ----------------------------------------------------------
// ReindexParams -- minimal local definition (real one in nodes/parsenodes.h)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct ReindexParams {
    pub options: c_int,
    pub tablespaceOid: Oid,
}
pub const REINDEXOPT_VERBOSE: c_int    = 1 << 0;
pub const REINDEXOPT_REPORT_PROGRESS: c_int = 1 << 1;
pub const REINDEXOPT_MISSING_OK: c_int = 1 << 2;
pub const REINDEXOPT_CONCURRENTLY: c_int = 1 << 3;
pub const REINDEX_REL_PROCESS_TOAST: c_int        = 1 << 0;
pub const REINDEX_REL_SUPPRESS_INDEX_USE: c_int   = 1 << 1;
pub const REINDEX_REL_CHECK_CONSTRAINTS: c_int    = 1 << 2;
pub const REINDEX_REL_FORCE_INDEXES_UNLOGGED: c_int = 1 << 3;
pub const REINDEX_REL_FORCE_INDEXES_PERMANENT: c_int = 1 << 4;
// ReindexIsProcessingIndex is defined as a pub fn below (the canonical home is catalog/index.c).

// -- index_create flags -------------------------------------------------------
pub const INDEX_CREATE_IS_PRIMARY: bits16    = 1 << 0;
pub const INDEX_CREATE_ADD_CONSTRAINT: bits16 = 1 << 1;
pub const INDEX_CREATE_SKIP_BUILD: bits16    = 1 << 2;
pub const INDEX_CREATE_CONCURRENT: bits16    = 1 << 3;
pub const INDEX_CREATE_IF_NOT_EXISTS: bits16 = 1 << 4;
pub const INDEX_CREATE_PARTITIONED: bits16   = 1 << 5;
pub const INDEX_CREATE_INVALID: bits16       = 1 << 6;

// -- index_constraint_create flags -------------------------------------------
pub const INDEX_CONSTR_CREATE_MARK_AS_PRIMARY: bits16    = 1 << 0;
pub const INDEX_CONSTR_CREATE_DEFERRABLE: bits16         = 1 << 1;
pub const INDEX_CONSTR_CREATE_INIT_DEFERRED: bits16      = 1 << 2;
pub const INDEX_CONSTR_CREATE_UPDATE_INDEX: bits16       = 1 << 3;
pub const INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS: bits16    = 1 << 4;
pub const INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS: bits16   = 1 << 5;

// -- pg_rusage ----------------------------------------------------------------
// utils/pg_rusage not yet wired; stubs.
#[repr(C)]
struct PGRUsage {
    tv: [i64; 4],
}
#[inline]
unsafe fn pg_rusage_init(_ru0: *mut PGRUsage) {}
#[inline]
unsafe fn pg_rusage_show(_ru0: *const PGRUsage) -> *const c_char {
    b"\0".as_ptr() as *const c_char
}

// -- nodeToString stub --------------------------------------------------------
unsafe fn nodeToString(obj: *mut c_void) -> *mut c_char {
    core::ptr::null_mut() /* TODO(pg-port) */
}
// -- populate_compact_attribute stub ------------------------------------------
unsafe fn populate_compact_attribute(_tupdesc: TupleDesc, _attno: c_int) {
    // TODO(pg-port)
}
// -- palloc0_array macro (must come before first use) -------------------------
macro_rules! palloc0_array {
    ($ty:ty, $n:expr) => {{
        crate::utils::palloc::palloc0(core::mem::size_of::<$ty>() * $n) as *mut $ty
    }};
}
// -- ObjectAddressSet macro ---------------------------------------------------
macro_rules! ObjectAddressSet {
    ($addr:expr, $classId:expr, $objectId:expr) => {{
        $addr.classId     = $classId;
        $addr.objectId    = $objectId;
        $addr.objectSubId = 0;
    }};
    ($addr:expr, $classId:expr, $objectId:expr, $subId:expr) => {{
        $addr.classId     = $classId;
        $addr.objectId    = $objectId;
        $addr.objectSubId = $subId as i32;
    }};
}
// -- AttrNumberGetAttrOffset macro --------------------------------------------
#[inline]
fn AttrNumberGetAttrOffset(attnum: AttrNumber) -> usize {
    (attnum as usize) - 1
}
// -- ATTRIBUTE_FIXED_PART_SIZE -----------------------------------------------
const ATTRIBUTE_FIXED_PART_SIZE: usize = core::mem::size_of::<FormData_pg_attribute>();
// -- exprTypmod stub ----------------------------------------------------------
unsafe fn exprTypmod(expr: *mut Node) -> i32 { -1 /* TODO(pg-port) */ }
// -- InvalidCompressionMethod -------------------------------------------------
const InvalidCompressionMethod: c_char = b'\0' as c_char;
// -- MemSet macro (via ptr::write_bytes) --------------------------------------
#[inline]
unsafe fn MemSet_attr(to: *mut FormData_pg_attribute, val: u8, size: usize) {
    core::ptr::write_bytes(to as *mut u8, val, size);
}
// -- DatumGetPointer ----------------------------------------------------------
#[inline]
unsafe fn DatumGetPointer(d: Datum) -> *mut c_void { d as *mut c_void }
// -- try_table_open stub ------------------------------------------------------
unsafe fn try_table_open(oid: Oid, lockmode: LOCKMODE) -> Relation {
    table_open(oid, lockmode) /* TODO(pg-port): check for missing ok */
}
// -- CreateTrigger stub -------------------------------------------------------
unsafe fn CreateTrigger(
    _trig: *mut c_void, _querystring: *mut c_char,
    _relOid: Oid, _refRelOid: Oid, _constraintOid: Oid,
    _indexOid: Oid, _funcOid: Oid, _parentTriggerOid: Oid,
    _whenClause: *mut c_void, _is_internal: bool, _in_partition: bool,
) -> ObjectAddress {
    INVALID_OBJECT_ADDRESS /* TODO(pg-port) */
}
// -- makeNode for CreateTrigStmt (opaque) ------------------------------------
unsafe fn makeNode_CreateTrigStmt() -> *mut c_void {
    palloc0(512) /* TODO(pg-port) */
}
// -- CHECK_FOR_INTERRUPTS (no-op stub) ----------------------------------------
macro_rules! CHECK_FOR_INTERRUPTS { () => {} }

// ============================================================================
// PART 2: globals, SerializedReindexState, static helper functions
// ============================================================================

// binary_upgrade globals are owned by catalog/binary_upgrade.rs.
// Use the module-level aliases defined at import time.
use crate::catalog::binary_upgrade::binary_upgrade_next_index_pg_class_oid;
use crate::catalog::binary_upgrade::binary_upgrade_next_index_pg_class_relfilenumber;

/*
 * Pointer-free representation of variables used when reindexing system
 * catalogs; we use this to propagate those values to parallel workers.
 */
#[repr(C)]
struct SerializedReindexState {
    currentlyReindexedHeap: Oid,
    currentlyReindexedIndex: Oid,
    numPendingReindexedIndexes: c_int,
    /* pendingReindexedIndexes[FLEXIBLE_ARRAY_MEMBER] follows */
}

/*
 * relationHasPrimaryKey
 *        See whether an existing relation has a primary key.
 *
 * Caller must have suitable lock on the relation.
 *
 * Note: we intentionally do not check indisvalid here; that's because this
 * is used to enforce the rule that there can be only one indisprimary index,
 * and we want that to be true even if said index is invalid.
 */
unsafe fn relationHasPrimaryKey(rel: Relation) -> bool {
    let mut result = false;

    /*
     * Get the list of index OIDs for the table from the relcache, and look up
     * each one in the pg_index syscache until we find one marked primary key
     * (hopefully there isn't more than one such).
     */
    let indexoidlist: *mut List = RelationGetIndexList(rel);

    let mut indexoidscan: *mut ListCell = list_head(indexoidlist);
    'outer: while !indexoidscan.is_null() {
        let indexoid: Oid = lfirst_oid(indexoidscan);

        let indexTuple: *mut HeapTupleData = SearchSysCache1(
            INDEXRELID,
            ObjectIdGetDatum(indexoid),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(indexTuple) {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for index {}", indexoid);
        }
        result = (*(GETSTRUCT(indexTuple) as Form_pg_index)).indisprimary;
        ReleaseSysCache(indexTuple);
        if result {
            break 'outer;
        }
        indexoidscan = lnext(indexoidlist, indexoidscan);
    }

    list_free(indexoidlist);

    result
}

/*
 * index_check_primary_key
 *        Apply special checks needed before creating a PRIMARY KEY index
 *
 * This processing used to be in DefineIndex(), but has been split out
 * so that it can be applied during ALTER TABLE ADD PRIMARY KEY USING INDEX.
 *
 * We check for a pre-existing primary key, and that all columns of the index
 * are simple column references (not expressions), and that all those
 * columns are marked NOT NULL.  If not, fail.
 *
 * We used to automatically change unmarked columns to NOT NULL here by doing
 * our own local ALTER TABLE command.  But that doesn't work well if we're
 * executing one subcommand of an ALTER TABLE: the operations may not get
 * performed in the right order overall.  Now we expect that the parser
 * inserted any required ALTER TABLE SET NOT NULL operations before trying
 * to create a primary-key index.
 *
 * Caller had better have at least ShareLock on the table, else the not-null
 * checking isn't trustworthy.
 */
pub unsafe fn index_check_primary_key(
    heapRel: Relation,
    indexInfo: *const IndexInfo,
    is_alter_table: bool,
    stmt: *const IndexStmt,
) {
    /*
     * If ALTER TABLE or CREATE TABLE .. PARTITION OF, check that there isn't
     * already a PRIMARY KEY.  In CREATE TABLE for an ordinary relation, we
     * have faith that the parser rejected multiple pkey clauses; and CREATE
     * INDEX doesn't have a way to say PRIMARY KEY, so it's no problem either.
     */
    if (is_alter_table || (*(*heapRel).rd_rel).relispartition)
        && relationHasPrimaryKey(heapRel)
    {
        ereport!(ERROR, errmsg!(
                "multiple primary keys for table \"{}\" are not allowed",
                CStr_to_str(RelationGetRelationName(heapRel))
            )) /* C also: errcode */;
    }

    /*
     * Indexes created with NULLS NOT DISTINCT cannot be used for primary key
     * constraints. While there is no direct syntax to reach here, it can be
     * done by creating a separate index and attaching it via ALTER TABLE ..
     * USING INDEX.
     */
    if (*indexInfo).ii_NullsNotDistinct {
        ereport!(ERROR, errmsg!("primary keys cannot use NULLS NOT DISTINCT indexes")) /* C also: errcode */;
    }

    /*
     * Check that all of the attributes in a primary key are marked as not
     * null.  (We don't really expect to see that; it'd mean the parser messed
     * up.  But it seems wise to check anyway.)
     */
    for i in 0..(*indexInfo).ii_NumIndexKeyAttrs {
        let attnum: AttrNumber = (*indexInfo).ii_IndexAttrNumbers[i as usize];

        if attnum == 0 {
            ereport!(ERROR, errmsg!("primary keys cannot be expressions")) /* C also: errcode */;
        }

        /* System attributes are never null, so no need to check */
        if attnum < 0 {
            continue;
        }

        let atttuple: *mut HeapTupleData = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(RelationGetRelid(heapRel)),
            Int16GetDatum(attnum),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(atttuple) {
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                attnum,
                RelationGetRelid(heapRel)
            );
        }
        let attform: Form_pg_attribute = GETSTRUCT(atttuple) as Form_pg_attribute;

        if !(*attform).attnotnull {
            ereport!(ERROR, errmsg!(
                    "primary key column \"{}\" is not marked NOT NULL",
                    CStr_to_str((*attform).attname.data.as_ptr())
                )) /* C also: errcode */;
        }

        ReleaseSysCache(atttuple);
    }
}

/*
 *        ConstructTupleDescriptor
 *
 * Build an index tuple descriptor for a new index
 */
unsafe fn ConstructTupleDescriptor(
    heapRelation: Relation,
    indexInfo: *const IndexInfo,
    indexColNames: *const List,
    accessMethodId: Oid,
    collationIds: *const Oid,
    opclassIds: *const Oid,
) -> TupleDesc {
    let numatts: c_int = (*indexInfo).ii_NumIndexAttrs;
    let numkeyatts: c_int = (*indexInfo).ii_NumIndexKeyAttrs;
    let mut colnames_item: *mut ListCell = list_head(indexColNames);
    let mut indexpr_item: *mut ListCell = list_head((*indexInfo).ii_Expressions);

    /* We need access to the index AM's API struct */
    let amroutine: *mut IndexAmRoutine =
        GetIndexAmRoutineByAmId(accessMethodId, false);

    /* ... and to the table's tuple descriptor */
    let heapTupDesc: TupleDesc = RelationGetDescr(heapRelation);
    let natts: c_int = (*RelationGetForm(heapRelation)).relnatts as c_int;

    /*
     * allocate the new tuple descriptor
     */
    let indexTupDesc: TupleDesc = CreateTemplateTupleDesc(numatts);

    /*
     * Fill in the pg_attribute row.
     */
    for i in 0..numatts {
        let atnum: AttrNumber = (*indexInfo).ii_IndexAttrNumbers[i as usize];
        let to: Form_pg_attribute = TupleDescAttr(indexTupDesc, i);
        let mut tuple: *mut HeapTupleData;
        let mut typeTup: Form_pg_type;
        let mut opclassTup: *mut FormData_pg_opclass;
        let mut keyType: Oid;

        MemSet_attr(to, 0, ATTRIBUTE_FIXED_PART_SIZE);
        (*to).attnum = (i + 1) as AttrNumber;
        (*to).attislocal = true;
        (*to).attcollation = if i < numkeyatts {
            *collationIds.add(i as usize)
        } else {
            InvalidOid
        };

        /*
         * Set the attribute name as specified by caller.
         */
        if colnames_item.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "too few entries in colnames list");
        }
        namestrcpy(
            &mut (*to).attname,
            lfirst(colnames_item) as *const c_char,
        );
        colnames_item = lnext(indexColNames, colnames_item);

        /*
         * For simple index columns, we copy some pg_attribute fields from the
         * parent relation.  For expressions we have to look at the expression
         * result.
         */
        if atnum != 0 {
            /* Simple index column */
            Assert!(atnum > 0); /* should've been caught above */

            if atnum > natts as AttrNumber {
                /* safety check */
                elog!(ERROR, "invalid column number {}", atnum);
            }
            let from: *const FormData_pg_attribute = TupleDescAttr(
                heapTupDesc,
                AttrNumberGetAttrOffset(atnum) as c_int,
            );

            (*to).atttypid = (*from).atttypid;
            (*to).attlen = (*from).attlen;
            (*to).attndims = (*from).attndims;
            (*to).atttypmod = (*from).atttypmod;
            (*to).attbyval = (*from).attbyval;
            (*to).attalign = (*from).attalign;
            (*to).attstorage = (*from).attstorage;
            (*to).attcompression = (*from).attcompression;
        } else {
            /* Expressional index */
            if indexpr_item.is_null() {
                /* shouldn't happen */
                elog!(ERROR, "too few entries in indexprs list");
            }
            let indexkey: *mut Node = lfirst(indexpr_item) as *mut Node;
            indexpr_item = lnext((*indexInfo).ii_Expressions, indexpr_item);

            /*
             * Lookup the expression type in pg_type for the type length etc.
             */
            keyType = exprType(indexkey);
            tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType))
                as *mut HeapTupleData;
            if !HeapTupleIsValid(tuple) {
                elog!(ERROR, "cache lookup failed for type {}", keyType);
            }
            typeTup = GETSTRUCT(tuple) as Form_pg_type;

            /*
             * Assign some of the attributes values. Leave the rest.
             */
            (*to).atttypid = keyType;
            (*to).attlen = (*typeTup).typlen;
            (*to).atttypmod = exprTypmod(indexkey);
            (*to).attbyval = (*typeTup).typbyval;
            (*to).attalign = (*typeTup).typalign;
            (*to).attstorage = (*typeTup).typstorage;

            /*
             * For expression columns, set attcompression invalid, since
             * there's no table column from which to copy the value. Whenever
             * we actually need to compress a value, we'll use whatever the
             * current value of default_toast_compression is at that point in
             * time.
             */
            (*to).attcompression = InvalidCompressionMethod;

            ReleaseSysCache(tuple);

            /*
             * Make sure the expression yields a type that's safe to store in
             * an index.  We need this defense because we have index opclasses
             * for pseudo-types such as "record", and the actually stored type
             * had better be safe; eg, a named composite type is okay, an
             * anonymous record type is not.  The test is the same as for
             * whether a table column is of a safe type (which is why we
             * needn't check for the non-expression case).
             */
            CheckAttributeType(
                (*to).attname.data.as_ptr(),
                (*to).atttypid,
                (*to).attcollation,
                NIL,
                0,
            );
        }

        /*
         * We do not yet have the correct relation OID for the index, so just
         * set it invalid for now.  InitializeAttributeOids() will fix it
         * later.
         */
        (*to).attrelid = InvalidOid;

        /*
         * Check the opclass and index AM to see if either provides a keytype
         * (overriding the attribute type).  Opclass (if exists) takes
         * precedence.
         */
        keyType = (*amroutine).amkeytype;

        if i < (*indexInfo).ii_NumIndexKeyAttrs {
            tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(*opclassIds.add(i as usize)))
                as *mut HeapTupleData;
            if !HeapTupleIsValid(tuple) {
                elog!(
                    ERROR,
                    "cache lookup failed for opclass {}",
                    *opclassIds.add(i as usize)
                );
            }
            opclassTup = GETSTRUCT(tuple) as *mut FormData_pg_opclass;
            if OidIsValid((*opclassTup).opckeytype) {
                keyType = (*opclassTup).opckeytype;
            }

            /*
             * If keytype is specified as ANYELEMENT, and opcintype is
             * ANYARRAY, then the attribute type must be an array (else it'd
             * not have matched this opclass); use its element type.
             *
             * We could also allow ANYCOMPATIBLE/ANYCOMPATIBLEARRAY here, but
             * there seems no need to do so; there's no reason to declare an
             * opclass as taking ANYCOMPATIBLEARRAY rather than ANYARRAY.
             */
            if keyType == ANYELEMENTOID && (*opclassTup).opcintype == ANYARRAYOID {
                keyType = get_base_element_type((*to).atttypid);
                if !OidIsValid(keyType) {
                    elog!(
                        ERROR,
                        "could not get element type of array type {}",
                        (*to).atttypid
                    );
                }
            }

            ReleaseSysCache(tuple);
        }

        /*
         * If a key type different from the heap value is specified, update
         * the type-related fields in the index tupdesc.
         */
        if OidIsValid(keyType) && keyType != (*to).atttypid {
            tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType))
                as *mut HeapTupleData;
            if !HeapTupleIsValid(tuple) {
                elog!(ERROR, "cache lookup failed for type {}", keyType);
            }
            typeTup = GETSTRUCT(tuple) as Form_pg_type;

            (*to).atttypid = keyType;
            (*to).atttypmod = -1;
            (*to).attlen = (*typeTup).typlen;
            (*to).attbyval = (*typeTup).typbyval;
            (*to).attalign = (*typeTup).typalign;
            (*to).attstorage = (*typeTup).typstorage;
            /* As above, use the default compression method in this case */
            (*to).attcompression = InvalidCompressionMethod;

            ReleaseSysCache(tuple);
        }

        populate_compact_attribute(indexTupDesc, i);
    }

    pfree(amroutine as *mut c_void);

    indexTupDesc
}

/* ----------------------------------------------------------------
 *        InitializeAttributeOids
 * ----------------------------------------------------------------
 */
unsafe fn InitializeAttributeOids(
    indexRelation: Relation,
    numatts: c_int,
    indexoid: Oid,
) {
    let tupleDescriptor: TupleDesc = RelationGetDescr(indexRelation);

    for i in 0..numatts {
        (*TupleDescAttr(tupleDescriptor, i)).attrelid = indexoid;
    }
}

/* ----------------------------------------------------------------
 *        AppendAttributeTuples
 * ----------------------------------------------------------------
 */
unsafe fn AppendAttributeTuples(
    indexRelation: Relation,
    attopts: *const Datum,
    stattargets: *const NullableDatum,
) {
    let mut attrs_extra: *mut FormExtraData_pg_attribute = core::ptr::null_mut();

    if !attopts.is_null() {
        let natts: c_int = (*(*indexRelation).rd_att).natts;
        attrs_extra = palloc0_array!(FormExtraData_pg_attribute, natts as usize);

        for i in 0..natts {
            let ae = attrs_extra.add(i as usize);
            if *attopts.add(i as usize) != 0 {
                (*ae).attoptions.value = *attopts.add(i as usize);
            } else {
                (*ae).attoptions.isnull = true;
            }

            if !stattargets.is_null() {
                (*ae).attstattarget = *stattargets.add(i as usize);
            } else {
                (*ae).attstattarget.isnull = true;
            }
        }
    }

    /*
     * open the attribute relation and its indexes
     */
    let pg_attribute: Relation = table_open(AttributeRelationId, RowExclusiveLock);

    let indstate: CatalogIndexState = CatalogOpenIndexes(pg_attribute);

    /*
     * insert data from new index's tupdesc into pg_attribute
     */
    let indexTupDesc: TupleDesc = RelationGetDescr(indexRelation);

    InsertPgAttributeTuples(pg_attribute, indexTupDesc, InvalidOid, attrs_extra, indstate);

    CatalogCloseIndexes(indstate);

    table_close(pg_attribute, RowExclusiveLock);
}

/* ----------------------------------------------------------------
 *        UpdateIndexRelation
 *
 * Construct and insert a new entry in the pg_index catalog
 * ----------------------------------------------------------------
 */
unsafe fn UpdateIndexRelation(
    indexoid: Oid,
    heapoid: Oid,
    parentIndexId: Oid,
    indexInfo: *const IndexInfo,
    collationOids: *const Oid,
    opclassOids: *const Oid,
    coloptions: *const i16,
    primary: bool,
    isexclusion: bool,
    immediate: bool,
    isvalid: bool,
    isready: bool,
) {
    use crate::utils::adt::int::buildint2vector;
    use crate::utils::adt::oid::buildoidvector;

    let indkey: *mut int2vector =
        buildint2vector(core::ptr::null(), (*indexInfo).ii_NumIndexAttrs) as *mut int2vector;
    for i in 0..(*indexInfo).ii_NumIndexAttrs {
        (*indkey).values[i as usize] = (*indexInfo).ii_IndexAttrNumbers[i as usize];
    }
    let indcollation: *mut oidvector =
        buildoidvector(collationOids, (*indexInfo).ii_NumIndexKeyAttrs);
    let indclass: *mut oidvector =
        buildoidvector(opclassOids, (*indexInfo).ii_NumIndexKeyAttrs);
    let indoption: *mut int2vector =
        buildint2vector(coloptions, (*indexInfo).ii_NumIndexKeyAttrs) as *mut int2vector;

    /*
     * Convert the index expressions (if any) to a text datum
     */
    let exprsDatum: Datum;
    if !(*indexInfo).ii_Expressions.is_null() {
        let exprsString: *mut c_char =
            nodeToString((*indexInfo).ii_Expressions as *mut c_void);
        exprsDatum = CStringGetTextDatum(exprsString);
        pfree(exprsString as *mut c_void);
    } else {
        exprsDatum = 0 as Datum;
    }

    /*
     * Convert the index predicate (if any) to a text datum.  Note we convert
     * implicit-AND format to normal explicit-AND for storage.
     */
    let predDatum: Datum;
    if !(*indexInfo).ii_Predicate.is_null() {
        let predString: *mut c_char = nodeToString(
            make_ands_explicit((*indexInfo).ii_Predicate) as *mut c_void,
        );
        predDatum = CStringGetTextDatum(predString);
        pfree(predString as *mut c_void);
    } else {
        predDatum = 0 as Datum;
    }

    /*
     * open the system catalog index relation
     */
    let pg_index: Relation = table_open(IndexRelationId, RowExclusiveLock);

    /*
     * Build a pg_index tuple
     */
    let mut values: [Datum; 21] = [0; 21]; // Natts_pg_index
    let mut nulls: [bool; 21] = [false; 21];

    values[(Anum_pg_index_indexrelid - 1) as usize] = ObjectIdGetDatum(indexoid);
    values[(Anum_pg_index_indrelid - 1) as usize] = ObjectIdGetDatum(heapoid);
    values[(Anum_pg_index_indnatts - 1) as usize] =
        Int16GetDatum((*indexInfo).ii_NumIndexAttrs as i16);
    values[(Anum_pg_index_indnkeyatts - 1) as usize] =
        Int16GetDatum((*indexInfo).ii_NumIndexKeyAttrs as i16);
    values[(Anum_pg_index_indisunique - 1) as usize] = BoolGetDatum((*indexInfo).ii_Unique);
    values[(Anum_pg_index_indnullsnotdistinct - 1) as usize] =
        BoolGetDatum((*indexInfo).ii_NullsNotDistinct);
    values[(Anum_pg_index_indisprimary - 1) as usize] = BoolGetDatum(primary);
    values[(Anum_pg_index_indisexclusion - 1) as usize] = BoolGetDatum(isexclusion);
    values[(Anum_pg_index_indimmediate - 1) as usize] = BoolGetDatum(immediate);
    values[(Anum_pg_index_indisclustered - 1) as usize] = BoolGetDatum(false);
    values[(Anum_pg_index_indisvalid - 1) as usize] = BoolGetDatum(isvalid);
    values[(Anum_pg_index_indcheckxmin - 1) as usize] = BoolGetDatum(false);
    values[(Anum_pg_index_indisready - 1) as usize] = BoolGetDatum(isready);
    values[(Anum_pg_index_indislive - 1) as usize] = BoolGetDatum(true);
    values[(Anum_pg_index_indisreplident - 1) as usize] = BoolGetDatum(false);
    values[(Anum_pg_index_indkey - 1) as usize] = PointerGetDatum(indkey as *mut c_void);
    values[(Anum_pg_index_indcollation - 1) as usize] =
        PointerGetDatum(indcollation as *mut c_void);
    values[(Anum_pg_index_indclass - 1) as usize] =
        PointerGetDatum(indclass as *mut c_void);
    values[(Anum_pg_index_indoption - 1) as usize] =
        PointerGetDatum(indoption as *mut c_void);
    values[(Anum_pg_index_indexprs - 1) as usize] = exprsDatum;
    if exprsDatum == 0 as Datum {
        nulls[(Anum_pg_index_indexprs - 1) as usize] = true;
    }
    values[(Anum_pg_index_indpred - 1) as usize] = predDatum;
    if predDatum == 0 as Datum {
        nulls[(Anum_pg_index_indpred - 1) as usize] = true;
    }

    let tuple: *mut HeapTupleData = heap_form_tuple(
        RelationGetDescr(pg_index),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    /*
     * insert the tuple into the pg_index catalog
     */
    CatalogTupleInsert(pg_index, tuple);

    /*
     * close the relation and free the tuple
     */
    table_close(pg_index, RowExclusiveLock);
    heap_freetuple(tuple);
}


/*
 * index_create
 *
 * heapRelation: table to build index on (suitably locked by caller)
 * indexRelationName: what it say
 * indexRelationId: normally, pass InvalidOid to let this routine
 *        generate an OID for the index.  During bootstrap this may be
 *        nonzero to specify a preselected OID.
 * parentIndexRelid: if creating an index partition, the OID of the
 *        parent index; otherwise InvalidOid.
 * parentConstraintId: if creating a constraint on a partition, the OID
 *        of the constraint in the parent; otherwise InvalidOid.
 * relFileNumber: normally, pass InvalidRelFileNumber to get new storage.
 *        May be nonzero to attach an existing valid build.
 * indexInfo: same info executor uses to insert into the index
 * indexColNames: column names to use for index (List of char *)
 * accessMethodId: OID of index AM to use
 * tableSpaceId: OID of tablespace to use
 * collationIds: array of collation OIDs, one per index column
 * opclassIds: array of index opclass OIDs, one per index column
 * coloptions: array of per-index-column indoption settings
 * reloptions: AM-specific options
 * flags: bitmask (INDEX_CREATE_*)
 * constr_flags: flags passed to index_constraint_create
 * allow_system_table_mods: allow table to be a system catalog
 * is_internal: if true, post creation hook for new index
 * constraintId: if not NULL, receives OID of created constraint
 *
 * Returns the OID of the created index.
 */
pub unsafe fn index_create(
    heapRelation: Relation,
    indexRelationName: *const c_char,
    mut indexRelationId: Oid,
    parentIndexRelid: Oid,
    parentConstraintId: Oid,
    mut relFileNumber: RelFileNumber,
    indexInfo: *mut IndexInfo,
    indexColNames: *const List,
    accessMethodId: Oid,
    tableSpaceId: Oid,
    collationIds: *const Oid,
    opclassIds: *const Oid,
    opclassOptions: *const Datum,
    coloptions: *const i16,
    stattargets: *const NullableDatum,
    reloptions: Datum,
    flags: bits16,
    constr_flags: bits16,
    allow_system_table_mods: bool,
    is_internal: bool,
    constraintId: *mut Oid,
) -> Oid {
    let heapRelationId: Oid = RelationGetRelid(heapRelation);
    let mut shared_relation: bool;
    let mut mapped_relation: bool;
    let is_exclusion: bool;
    let namespaceId: Oid;
    let relpersistence: c_char;
    let isprimary: bool = (flags & INDEX_CREATE_IS_PRIMARY) != 0;
    let invalid: bool = (flags & INDEX_CREATE_INVALID) != 0;
    let concurrent: bool = (flags & INDEX_CREATE_CONCURRENT) != 0;
    let partitioned: bool = (flags & INDEX_CREATE_PARTITIONED) != 0;
    let relkind: c_char;
    let mut relfrozenxid: TransactionId = InvalidTransactionId;
    let mut relminmxid: MultiXactId = InvalidMultiXactId;
    let create_storage: bool = !RelFileNumberIsValid(relFileNumber);

    /* constraint flags can only be set when a constraint is requested */
    Assert!((constr_flags == 0) || ((flags & INDEX_CREATE_ADD_CONSTRAINT) != 0));
    /* partitioned indexes must never be "built" by themselves */
    Assert!(!partitioned || (flags & INDEX_CREATE_SKIP_BUILD) != 0);

    relkind = if partitioned {
        RELKIND_PARTITIONED_INDEX
    } else {
        RELKIND_INDEX
    };
    is_exclusion = !(*indexInfo).ii_ExclusionOps.is_null();

    let pg_class: Relation = table_open(RelationRelationId, RowExclusiveLock);

    /*
     * The index will be in the same namespace as its parent table, and is
     * shared across databases if and only if the parent is.  Likewise, it
     * will use the relfilenumber map if and only if the parent does; and it
     * inherits the parent's relpersistence.
     */
    namespaceId = RelationGetNamespace(heapRelation);
    shared_relation = (*(*heapRelation).rd_rel).relisshared;
    mapped_relation = RelationIsMapped(heapRelation);
    relpersistence = (*(*heapRelation).rd_rel).relpersistence;

    /*
     * check parameters
     */
    if (*indexInfo).ii_NumIndexAttrs < 1 {
        elog!(ERROR, "must index at least one column");
    }

    if !allow_system_table_mods
        && IsSystemRelation(heapRelation)
        && IsNormalProcessingMode()
    {
        ereport!(ERROR, errmsg!(
                "user-defined indexes on system catalog tables are not supported"
            )) /* C also: errcode */;
    }

    /*
     * Btree text_pattern_ops uses text_eq as the equality operator, which is
     * fine as long as the collation is deterministic; text_eq then reduces to
     * bitwise equality and so it is semantically compatible with the other
     * operators and functions in that opclass.  But with a nondeterministic
     * collation, text_eq could yield results that are incompatible with the
     * actual behavior of the index (which is determined by the opclass's
     * comparison function).  We prevent such problems by refusing creation of
     * an index with that opclass and a nondeterministic collation.
     *
     * The same applies to varchar_pattern_ops and bpchar_pattern_ops.  If we
     * find more cases, we might decide to create a real mechanism for marking
     * opclasses as incompatible with nondeterminism; but for now, this small
     * hack suffices.
     *
     * Another solution is to use a special operator, not text_eq, as the
     * equality opclass member; but that is undesirable because it would
     * prevent index usage in many queries that work fine today.
     */
    for i in 0..(*indexInfo).ii_NumIndexKeyAttrs {
        let collation: Oid = *collationIds.add(i as usize);
        let opclass: Oid = *opclassIds.add(i as usize);

        if collation != 0 {
            if (opclass == TEXT_BTREE_PATTERN_OPS_OID
                || opclass == VARCHAR_BTREE_PATTERN_OPS_OID
                || opclass == BPCHAR_BTREE_PATTERN_OPS_OID)
                && !get_collation_isdeterministic(collation)
            {
                let classtup: *mut HeapTupleData = SearchSysCache1(
                    CLAOID,
                    ObjectIdGetDatum(opclass),
                ) as *mut HeapTupleData;
                if !HeapTupleIsValid(classtup) {
                    elog!(
                        ERROR,
                        "cache lookup failed for operator class {}",
                        opclass
                    );
                }
                ereport!(ERROR, errmsg!(
                        "nondeterministic collations are not supported for operator class \"{}\"",
                        CStr_to_str(
                            (*(GETSTRUCT(classtup) as *mut FormData_pg_opclass))
                                .opcname
                                .data
                                .as_ptr()
                        )
                    )) /* C also: errcode */;
                ReleaseSysCache(classtup);
            }
        }
    }

    /*
     * Concurrent index build on a system catalog is unsafe because we tend to
     * release locks before committing in catalogs.
     */
    if concurrent && IsCatalogRelation(heapRelation) {
        ereport!(ERROR, errmsg!(
                "concurrent index creation on system catalog tables is not supported"
            )) /* C also: errcode */;
    }

    /*
     * This case is currently not supported.  There's no way to ask for it in
     * the grammar with CREATE INDEX, but it can happen with REINDEX.
     */
    if concurrent && is_exclusion {
        ereport!(ERROR, errmsg!(
                "concurrent index creation for exclusion constraints is not supported"
            )) /* C also: errcode */;
    }

    /*
     * We cannot allow indexing a shared relation after initdb (because
     * there's no way to make the entry in other databases' pg_class).
     */
    if shared_relation && !IsBootstrapProcessingMode() {
        ereport!(ERROR, errmsg!("shared indexes cannot be created after initdb")) /* C also: errcode */;
    }

    /*
     * Shared relations must be in pg_global, too (last-ditch check)
     */
    if shared_relation && tableSpaceId != GLOBALTABLESPACE_OID {
        elog!(ERROR, "shared relations must be placed in pg_global tablespace");
    }

    use crate::catalog::pg_known_oids::GLOBALTABLESPACE_OID;

    /*
     * Check for duplicate name (both as to the index, and as to the
     * associated constraint if any).  Such cases would fail on the relevant
     * catalogs' unique indexes anyway, but we prefer to give a friendlier
     * error message.
     */
    if OidIsValid(get_relname_relid(indexRelationName, namespaceId)) {
        if (flags & INDEX_CREATE_IF_NOT_EXISTS) != 0 {
            ereport!(NOTICE, errmsg!(
                    "relation \"{}\" already exists, skipping",
                    CStr_to_str(indexRelationName)
                )) /* C also: errcode */;
            table_close(pg_class, RowExclusiveLock);
            return InvalidOid;
        }

        ereport!(ERROR, errmsg!(
                "relation \"{}\" already exists",
                CStr_to_str(indexRelationName)
            )) /* C also: errcode */;
    }

    if (flags & INDEX_CREATE_ADD_CONSTRAINT) != 0
        && ConstraintNameIsUsed(CONSTRAINT_RELATION, heapRelationId, indexRelationName)
    {
        /*
         * INDEX_CREATE_IF_NOT_EXISTS does not apply here, since the
         * conflicting constraint is not an index.
         */
        ereport!(ERROR, errmsg!(
                "constraint \"{}\" for relation \"{}\" already exists",
                CStr_to_str(indexRelationName),
                CStr_to_str(RelationGetRelationName(heapRelation))
            )) /* C also: errcode */;
    }

    /*
     * construct tuple descriptor for index tuples
     */
    let indexTupDesc: TupleDesc = ConstructTupleDescriptor(
        heapRelation,
        indexInfo,
        indexColNames,
        accessMethodId,
        collationIds,
        opclassIds,
    );

    /*
     * Allocate an OID for the index, unless we were told what to use.
     *
     * The OID will be the relfilenumber as well, so make sure it doesn't
     * collide with either pg_class OIDs or existing physical files.
     */
    if !OidIsValid(indexRelationId) {
        /* Use binary-upgrade override for pg_class.oid and relfilenumber */
        if IsBinaryUpgrade {
            if !OidIsValid(binary_upgrade_next_index_pg_class_oid) {
                ereport!(ERROR, errmsg!(
                        "pg_class index OID value not set when in binary upgrade mode"
                    )) /* C also: errcode */;
            }

            indexRelationId = binary_upgrade_next_index_pg_class_oid;
            binary_upgrade_next_index_pg_class_oid = InvalidOid;

            /* Override the index relfilenumber */
            if relkind == RELKIND_INDEX
                && !RelFileNumberIsValid(
                    binary_upgrade_next_index_pg_class_relfilenumber,
                )
            {
                ereport!(ERROR, errmsg!(
                        "index relfilenumber value not set when in binary upgrade mode"
                    )) /* C also: errcode */;
            }
            relFileNumber = binary_upgrade_next_index_pg_class_relfilenumber;
            binary_upgrade_next_index_pg_class_relfilenumber = 0; /* InvalidRelFileNumber */

            /*
             * Note that we want create_storage = true for binary upgrade. The
             * storage we create here will be replaced later, but we need to
             * have something on disk in the meanwhile.
             */
            Assert!(create_storage);
        } else {
            indexRelationId =
                GetNewRelFileNumber(tableSpaceId, pg_class, relpersistence);
        }
    }

    /*
     * create the index relation's relcache entry and, if necessary, the
     * physical disk file. (If we fail further down, it's the smgr's
     * responsibility to remove the disk file again, if any.)
     */
    let indexRelation: Relation = heap_create(
        indexRelationName,
        namespaceId,
        tableSpaceId,
        indexRelationId,
        relFileNumber,
        accessMethodId,
        indexTupDesc,
        relkind,
        relpersistence,
        shared_relation,
        mapped_relation,
        allow_system_table_mods,
        &mut relfrozenxid,
        &mut relminmxid,
        create_storage,
    );

    Assert!(relfrozenxid == InvalidTransactionId);
    Assert!(relminmxid == InvalidMultiXactId);
    Assert!(indexRelationId == RelationGetRelid(indexRelation));

    /*
     * Obtain exclusive lock on it.  Although no other transactions can see it
     * until we commit, this prevents deadlock-risk complaints from lock
     * manager in cases such as CLUSTER.
     */
    LockRelation(indexRelation, AccessExclusiveLock);

    /*
     * Fill in fields of the index's pg_class entry that are not set correctly
     * by heap_create.
     *
     * XXX should have a cleaner way to create cataloged indexes
     */
    (*(*indexRelation).rd_rel).relowner = (*(*heapRelation).rd_rel).relowner;
    (*(*indexRelation).rd_rel).relam = accessMethodId;
    (*(*indexRelation).rd_rel).relispartition = OidIsValid(parentIndexRelid);

    /*
     * store index's pg_class entry
     */
    InsertPgClassTuple(
        pg_class,
        indexRelation,
        RelationGetRelid(indexRelation),
        0 as Datum,
        reloptions,
    );

    /* done with pg_class */
    table_close(pg_class, RowExclusiveLock);

    /*
     * now update the object id's of all the attribute tuple forms in the
     * index relation's tuple descriptor
     */
    InitializeAttributeOids(
        indexRelation,
        (*indexInfo).ii_NumIndexAttrs,
        indexRelationId,
    );

    /*
     * append ATTRIBUTE tuples for the index
     */
    AppendAttributeTuples(indexRelation, opclassOptions, stattargets);

    /* ----------------
     *      update pg_index
     *      (append INDEX tuple)
     *
     *      Note that this stows away a representation of "predicate".
     *      (Or, could define a rule to maintain the predicate) --Nels, Feb '92
     * ----------------
     */
    UpdateIndexRelation(
        indexRelationId,
        heapRelationId,
        parentIndexRelid,
        indexInfo,
        collationIds,
        opclassIds,
        coloptions,
        isprimary,
        is_exclusion,
        (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) == 0,
        !concurrent && !invalid,
        !concurrent,
    );

    /*
     * Register relcache invalidation on the indexes' heap relation, to
     * maintain consistency of its index list
     */
    CacheInvalidateRelcache(heapRelation);

    /* update pg_inherits and the parent's relhassubclass, if needed */
    if OidIsValid(parentIndexRelid) {
        StoreSingleInheritance(indexRelationId, parentIndexRelid, 1);
        LockRelationOid(parentIndexRelid, ShareUpdateExclusiveLock);
        SetRelationHasSubclass(parentIndexRelid, true);
    }

    /*
     * Register constraint and dependencies for the index.
     *
     * If the index is from a CONSTRAINT clause, construct a pg_constraint
     * entry.  The index will be linked to the constraint, which in turn is
     * linked to the table.  If it's not a CONSTRAINT, we need to make a
     * dependency directly on the table.
     *
     * We don't need a dependency on the namespace, because there'll be an
     * indirect dependency via our parent table.
     *
     * During bootstrap we can't register any dependencies, and we don't try
     * to make a constraint either.
     */
    if !IsBootstrapProcessingMode() {
        let myself: ObjectAddress;
        let mut referenced: ObjectAddress = INVALID_OBJECT_ADDRESS;
        let addrs: *mut ObjectAddresses;

        let mut myself_tmp = INVALID_OBJECT_ADDRESS;
        ObjectAddressSet!(myself_tmp, RelationRelationId, indexRelationId);
        let myself = myself_tmp;

        if (flags & INDEX_CREATE_ADD_CONSTRAINT) != 0 {
            let constraintType: c_char;
            let localaddr: ObjectAddress;

            if isprimary {
                constraintType = CONSTRAINT_PRIMARY;
            } else if (*indexInfo).ii_Unique {
                constraintType = CONSTRAINT_UNIQUE;
            } else if is_exclusion {
                constraintType = CONSTRAINT_EXCLUSION;
            } else {
                elog!(ERROR, "constraint must be PRIMARY, UNIQUE or EXCLUDE");
                constraintType = 0; /* keep compiler quiet */
            }

            let localaddr = index_constraint_create(
                heapRelation,
                indexRelationId,
                parentConstraintId,
                indexInfo,
                indexRelationName,
                constraintType,
                constr_flags,
                allow_system_table_mods,
                is_internal,
            );
            if !constraintId.is_null() {
                *constraintId = localaddr.objectId;
            }
        } else {
            let mut have_simple_col = false;

            let addrs = new_object_addresses();

            /* Create auto dependencies on simply-referenced columns */
            for i in 0..(*indexInfo).ii_NumIndexAttrs {
                if (*indexInfo).ii_IndexAttrNumbers[i as usize] != 0 {
                    ObjectAddressSubSet!(
                        referenced,
                        RelationRelationId,
                        heapRelationId,
                        (*indexInfo).ii_IndexAttrNumbers[i as usize] as i32
                    );
                    add_exact_object_address(&referenced, addrs);
                    have_simple_col = true;
                }
            }

            /*
             * If there are no simply-referenced columns, give the index an
             * auto dependency on the whole table.  In most cases, this will
             * be redundant, but it might not be if the index expressions and
             * predicate contain no Vars or only whole-row Vars.
             */
            if !have_simple_col {
                ObjectAddressSet!(referenced, RelationRelationId, heapRelationId);
                add_exact_object_address(&referenced, addrs);
            }

            record_object_address_dependencies(&myself, addrs, DEPENDENCY_AUTO);
            free_object_addresses(addrs);
        }

        /*
         * If this is an index partition, create partition dependencies on
         * both the parent index and the table.  (Note: these must be *in
         * addition to*, not instead of, all other dependencies.  Otherwise
         * we'll be short some dependencies after DETACH PARTITION.)
         */
        if OidIsValid(parentIndexRelid) {
            ObjectAddressSet!(referenced, RelationRelationId, parentIndexRelid);
            recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_PRI);

            ObjectAddressSet!(referenced, RelationRelationId, heapRelationId);
            recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_SEC);
        }

        /* placeholder for normal dependencies */
        let addrs = new_object_addresses();

        /* Store dependency on collations */

        /* The default collation is pinned, so don't bother recording it */
        for i in 0..(*indexInfo).ii_NumIndexKeyAttrs {
            let coll_oid = *collationIds.add(i as usize);
            if OidIsValid(coll_oid) && coll_oid != DEFAULT_COLLATION_OID {
                ObjectAddressSet!(referenced, CollationRelationId, coll_oid);
                add_exact_object_address(&referenced, addrs);
            }
        }

        /* Store dependency on operator classes */
        for i in 0..(*indexInfo).ii_NumIndexKeyAttrs {
            ObjectAddressSet!(
                referenced,
                OperatorClassRelationId,
                *opclassIds.add(i as usize)
            );
            add_exact_object_address(&referenced, addrs);
        }

        record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
        free_object_addresses(addrs);

        /* Store dependencies on anything mentioned in index expressions */
        if !(*indexInfo).ii_Expressions.is_null() {
            recordDependencyOnSingleRelExpr(
                &myself,
                (*indexInfo).ii_Expressions as *mut Node,
                heapRelationId,
                DEPENDENCY_NORMAL,
                DEPENDENCY_AUTO,
                false,
            );
        }

        /* Store dependencies on anything mentioned in predicate */
        if !(*indexInfo).ii_Predicate.is_null() {
            recordDependencyOnSingleRelExpr(
                &myself,
                (*indexInfo).ii_Predicate as *mut Node,
                heapRelationId,
                DEPENDENCY_NORMAL,
                DEPENDENCY_AUTO,
                false,
            );
        }
    } else {
        /* Bootstrap mode - assert we weren't asked for constraint support */
        Assert!((flags & INDEX_CREATE_ADD_CONSTRAINT) == 0);
    }

    /* Post creation hook for new index */
    InvokeObjectPostCreateHookArg(RelationRelationId, indexRelationId, 0, is_internal);

    /*
     * Advance the command counter so that we can see the newly-entered
     * catalog tuples for the index.
     */
    CommandCounterIncrement();

    /*
     * In bootstrap mode, we have to fill in the index strategy structure with
     * information from the catalogs.  If we aren't bootstrapping, then the
     * relcache entry has already been rebuilt thanks to sinval update during
     * CommandCounterIncrement.
     */
    if IsBootstrapProcessingMode() {
        RelationInitIndexAccessInfo(indexRelation);
    } else {
        Assert!(!(*indexRelation).rd_indexcxt.is_null());
    }

    (*(*indexRelation).rd_index).indnkeyatts = (*indexInfo).ii_NumIndexKeyAttrs as i16;

    /* Validate opclass-specific options */
    if !opclassOptions.is_null() {
        for i in 0..(*indexInfo).ii_NumIndexKeyAttrs {
            let _ = index_opclass_options(
                indexRelation,
                (i + 1) as i16,
                *opclassOptions.add(i as usize),
                true,
            );
        }
    }

    /*
     * If this is bootstrap (initdb) time, then we don't actually fill in the
     * index yet.  We'll be creating more indexes and classes later, so we
     * delay filling them in until just before we're done with bootstrapping.
     * Similarly, if the caller specified to skip the build then filling the
     * index is delayed till later (ALTER TABLE can save work in some cases
     * with this).  Otherwise, we call the AM routine that constructs the
     * index.
     */
    if IsBootstrapProcessingMode() {
        index_register(heapRelationId, indexRelationId, indexInfo);
    } else if (flags & INDEX_CREATE_SKIP_BUILD) != 0 {
        /*
         * Caller is responsible for filling the index later on.  However,
         * we'd better make sure that the heap relation is correctly marked as
         * having an index.
         */
        index_update_stats(heapRelation, true, -1.0);
        /* Make the above update visible */
        CommandCounterIncrement();
    } else {
        index_build(heapRelation, indexRelation, indexInfo, false, true);
    }

    /*
     * Close the index; but we keep the lock that we acquired above until end
     * of transaction.  Closing the heap is caller's responsibility.
     */
    index_close(indexRelation, NoLock);

    indexRelationId
}

const DEFAULT_COLLATION_OID: Oid = 100;
#[inline]
fn RelFileNumberIsValid(n: RelFileNumber) -> bool { n != 0 }
unsafe fn CStr_to_str<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("?")
}
use crate::utils::adt::name::namestrcpy;

/* FormData_pg_opclass stub (opckeytype / opcintype fields only) */
#[repr(C)]
struct FormData_pg_opclass {
    opcname: crate::c::NameData,
    opckeytype: Oid,
    opcintype: Oid,
}
/* Form_pg_type: only fields used here */
#[repr(C)]
struct Form_pg_type_inner {
    typlen: i16,
    typbyval: bool,
    typalign: c_char,
    typstorage: c_char,
}
type Form_pg_type = *mut Form_pg_type_inner;
type MultiXactId = u32;
use crate::utils::palloc::{palloc, palloc0, pfree};

// ============================================================================
// PART 4: index_concurrently_*, index_constraint_create, index_drop
// ============================================================================

/*
 * index_concurrently_create_copy
 *
 * Create concurrently an index based on the definition of the one provided by
 * caller.  The index is inserted into catalogs and needs to be built later
 * on.  This is called during concurrent reindex processing.
 *
 * "tablespaceOid" is the tablespace to use for this index.
 */
pub unsafe fn index_concurrently_create_copy(
    heapRelation: Relation,
    oldIndexId: Oid,
    tablespaceOid: Oid,
    newName: *const c_char,
) -> Oid {
    let indexRelation: Relation = index_open(oldIndexId, RowExclusiveLock);

    /* The new index needs some information from the old index */
    let oldInfo: *mut IndexInfo = BuildIndexInfo(indexRelation);

    /*
     * Concurrent build of an index with exclusion constraints is not
     * supported.
     */
    if !(*oldInfo).ii_ExclusionOps.is_null() {
        ereport!(ERROR, errmsg!(
                "concurrent index creation for exclusion constraints is not supported"
            )) /* C also: errcode */;
    }

    /* Get the array of class and column options IDs from index info */
    let indexTuple: *mut HeapTupleData = SearchSysCache1(
        INDEXRELID,
        ObjectIdGetDatum(oldIndexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(indexTuple) {
        elog!(ERROR, "cache lookup failed for index {}", oldIndexId);
    }
    let indclassDatum: Datum =
        SysCacheGetAttrNotNull(INDEXRELID, indexTuple, Anum_pg_index_indclass);
    let indclass: *mut oidvector = DatumGetPointer(indclassDatum) as *mut oidvector;

    let colOptionDatum: Datum =
        SysCacheGetAttrNotNull(INDEXRELID, indexTuple, Anum_pg_index_indoption);
    let indcoloptions: *mut int2vector =
        DatumGetPointer(colOptionDatum) as *mut int2vector;

    /* Fetch reloptions of index if any */
    let classTuple: *mut HeapTupleData = SearchSysCache1(
        RELOID,
        ObjectIdGetDatum(oldIndexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(classTuple) {
        elog!(ERROR, "cache lookup failed for relation {}", oldIndexId);
    }
    let mut isnull: bool = false;
    let reloptionsDatum: Datum =
        SysCacheGetAttr(RELOID, classTuple, Anum_pg_class_reloptions, &mut isnull);

    /*
     * Fetch the list of expressions and predicates directly from the
     * catalogs.  This cannot rely on the information from IndexInfo of the
     * old index as these have been flattened for the planner.
     */
    let mut indexExprs: *mut List = NIL;
    let mut indexPreds: *mut List = NIL;

    if !(*oldInfo).ii_Expressions.is_null() {
        let exprDatum: Datum =
            SysCacheGetAttrNotNull(INDEXRELID, indexTuple, Anum_pg_index_indexprs);
        let exprString: *mut c_char = TextDatumGetCString(exprDatum);
        indexExprs = stringToNode(exprString) as *mut List;
        pfree(exprString as *mut c_void);
    }
    if !(*oldInfo).ii_Predicate.is_null() {
        let predDatum: Datum =
            SysCacheGetAttrNotNull(INDEXRELID, indexTuple, Anum_pg_index_indpred);
        let predString: *mut c_char = TextDatumGetCString(predDatum);
        indexPreds = stringToNode(predString) as *mut List;

        /* Also convert to implicit-AND format */
        indexPreds = make_ands_implicit(indexPreds as *mut Expr) as *mut List;
        pfree(predString as *mut c_void);
    }

    /*
     * Build the index information for the new index.  Note that rebuild of
     * indexes with exclusion constraints is not supported, hence there is no
     * need to fill all the ii_Exclusion* fields.
     */
    let newInfo: *mut IndexInfo = makeIndexInfo(
        (*oldInfo).ii_NumIndexAttrs,
        (*oldInfo).ii_NumIndexKeyAttrs,
        (*oldInfo).ii_Am,
        indexExprs,
        indexPreds,
        (*oldInfo).ii_Unique,
        (*oldInfo).ii_NullsNotDistinct,
        false, /* not ready for inserts */
        true,
        (*(*indexRelation).rd_indam).amsummarizing,
        (*oldInfo).ii_WithoutOverlaps,
    );

    /*
     * Extract the list of column names and the column numbers for the new
     * index information.  All this information will be used for the index
     * creation.
     */
    let mut indexColNames: *mut List = NIL;
    for i in 0..(*oldInfo).ii_NumIndexAttrs {
        let indexTupDesc: TupleDesc = RelationGetDescr(indexRelation);
        let att: Form_pg_attribute = TupleDescAttr(indexTupDesc, i);

        indexColNames = lappend(
            indexColNames,
            (*att).attname.data.as_ptr() as *mut c_void,
        );
        (*newInfo).ii_IndexAttrNumbers[i as usize] =
            (*oldInfo).ii_IndexAttrNumbers[i as usize];
    }

    /* Extract opclass options for each attribute */
    let opclassOptions: *mut Datum =
        palloc0(core::mem::size_of::<Datum>() * (*newInfo).ii_NumIndexAttrs as usize)
            as *mut Datum;
    for i in 0..(*newInfo).ii_NumIndexAttrs {
        *opclassOptions.add(i as usize) = get_attoptions(oldIndexId, i + 1);
    }

    /* Extract statistic targets for each attribute */
    let stattargets: *mut NullableDatum = palloc0_array!(
        NullableDatum,
        (*newInfo).ii_NumIndexAttrs as usize
    );
    for i in 0..(*newInfo).ii_NumIndexAttrs {
        let tp: *mut HeapTupleData = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(oldIndexId),
            Int16GetDatum((i + 1) as i16),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(tp) {
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                i + 1,
                oldIndexId
            );
        }
        let dat: Datum = SysCacheGetAttr(
            ATTNUM,
            tp,
            Anum_pg_attribute_attstattarget,
            &mut isnull,
        );
        ReleaseSysCache(tp);
        (*stattargets.add(i as usize)).value = dat;
        (*stattargets.add(i as usize)).isnull = isnull;
    }

    /*
     * Now create the new index.
     *
     * For a partition index, we adjust the partition dependency later, to
     * ensure a consistent state at all times.  That is why parentIndexRelid
     * is not set here.
     */
    let newIndexId: Oid = index_create(
        heapRelation,
        newName,
        InvalidOid,      /* indexRelationId */
        InvalidOid,      /* parentIndexRelid */
        InvalidOid,      /* parentConstraintId */
        0,               /* relFileNumber = InvalidRelFileNumber */
        newInfo,
        indexColNames,
        (*(*indexRelation).rd_rel).relam,
        tablespaceOid,
        (*indexRelation).rd_indcollation,
        (*indclass).values.as_ptr(),
        opclassOptions,
        (*indcoloptions).values.as_ptr(),
        stattargets,
        reloptionsDatum,
        INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_CONCURRENT,
        0,
        true,  /* allow table to be a system catalog? */
        false, /* is_internal? */
        core::ptr::null_mut(),
    );

    /* Close the relations used and clean up */
    index_close(indexRelation, NoLock);
    ReleaseSysCache(indexTuple);
    ReleaseSysCache(classTuple);

    newIndexId
}

/*
 * index_concurrently_build
 *
 * Build index for a concurrent operation.  Low-level locks are taken when
 * this operation is performed to prevent only schema changes, but they need
 * to be kept until the end of the transaction performing this operation.
 * 'indexOid' refers to an index relation OID already created as part of
 * previous processing, and 'heapOid' refers to its parent heap relation.
 */
pub unsafe fn index_concurrently_build(heapRelationId: Oid, indexRelationId: Oid) {
    /* This had better make sure that a snapshot is active */
    Assert!(ActiveSnapshotSet());

    /* Open and lock the parent heap relation */
    let heapRel: Relation = table_open(heapRelationId, ShareUpdateExclusiveLock);

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext(
        (*(*heapRel).rd_rel).relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel: c_int = NewGUCNestLevel();
    RestrictSearchPath();

    let indexRelation: Relation = index_open(indexRelationId, RowExclusiveLock);

    /*
     * We have to re-build the IndexInfo struct, since it was lost in the
     * commit of the transaction where this concurrent index was created at
     * the catalog level.
     */
    let indexInfo: *mut IndexInfo = BuildIndexInfo(indexRelation);
    Assert!(!(*indexInfo).ii_ReadyForInserts);
    (*indexInfo).ii_Concurrent = true;
    (*indexInfo).ii_BrokenHotChain = false;

    /* Now build the index */
    index_build(heapRel, indexRelation, indexInfo, false, true);

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Close both the relations, but keep the locks */
    table_close(heapRel, NoLock);
    index_close(indexRelation, NoLock);

    /*
     * Update the pg_index row to mark the index as ready for inserts. Once we
     * commit this transaction, any new transactions that open the table must
     * insert new entries into the index for insertions and non-HOT updates.
     */
    index_set_state_flags(indexRelationId, INDEX_CREATE_SET_READY);
}

/*
 * index_concurrently_swap
 *
 * Swap name, dependencies, and constraints of the old index over to the new
 * index, while marking the old index as invalid and the new as valid.
 */
pub unsafe fn index_concurrently_swap(
    newIndexId: Oid,
    oldIndexId: Oid,
    oldName: *const c_char,
) {
    /*
     * Take a necessary lock on the old and new index before swapping them.
     */
    let oldClassRel: Relation = relation_open(oldIndexId, ShareUpdateExclusiveLock);
    let newClassRel: Relation = relation_open(newIndexId, ShareUpdateExclusiveLock);

    /* Now swap names and dependencies of those indexes */
    let pg_class: Relation = table_open(RelationRelationId, RowExclusiveLock);

    let oldClassTuple: *mut HeapTupleData = SearchSysCacheCopy1(
        RELOID,
        ObjectIdGetDatum(oldIndexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(oldClassTuple) {
        elog!(ERROR, "could not find tuple for relation {}", oldIndexId);
    }
    let newClassTuple: *mut HeapTupleData = SearchSysCacheCopy1(
        RELOID,
        ObjectIdGetDatum(newIndexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(newClassTuple) {
        elog!(ERROR, "could not find tuple for relation {}", newIndexId);
    }

    let oldClassForm: Form_pg_class = GETSTRUCT(oldClassTuple) as Form_pg_class;
    let newClassForm: Form_pg_class = GETSTRUCT(newClassTuple) as Form_pg_class;

    /* Swap the names */
    namestrcpy(&mut (*newClassForm).relname, CStr_to_str(RelationGetRelationName(oldClassRel)).as_ptr() as *const c_char);
    namestrcpy(&mut (*oldClassForm).relname, oldName);

    /* Swap the partition flags to track inheritance properly */
    let isPartition: bool = (*newClassForm).relispartition;
    (*newClassForm).relispartition = (*oldClassForm).relispartition;
    (*oldClassForm).relispartition = isPartition;

    CatalogTupleUpdate(pg_class, &mut (*oldClassTuple).t_self, oldClassTuple);
    CatalogTupleUpdate(pg_class, &mut (*newClassTuple).t_self, newClassTuple);

    heap_freetuple(oldClassTuple);
    heap_freetuple(newClassTuple);

    /* Now swap index info */
    let pg_index: Relation = table_open(IndexRelationId, RowExclusiveLock);

    let oldIndexTuple: *mut HeapTupleData = SearchSysCacheCopy1(
        INDEXRELID,
        ObjectIdGetDatum(oldIndexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(oldIndexTuple) {
        elog!(ERROR, "could not find tuple for relation {}", oldIndexId);
    }
    let newIndexTuple: *mut HeapTupleData = SearchSysCacheCopy1(
        INDEXRELID,
        ObjectIdGetDatum(newIndexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(newIndexTuple) {
        elog!(ERROR, "could not find tuple for relation {}", newIndexId);
    }

    let oldIndexForm: Form_pg_index = GETSTRUCT(oldIndexTuple) as Form_pg_index;
    let newIndexForm: Form_pg_index = GETSTRUCT(newIndexTuple) as Form_pg_index;

    /*
     * Copy constraint flags from the old index. This is safe because the old
     * index guaranteed uniqueness.
     */
    (*newIndexForm).indisprimary = (*oldIndexForm).indisprimary;
    (*oldIndexForm).indisprimary = false;
    (*newIndexForm).indisexclusion = (*oldIndexForm).indisexclusion;
    (*oldIndexForm).indisexclusion = false;
    (*newIndexForm).indimmediate = (*oldIndexForm).indimmediate;
    (*oldIndexForm).indimmediate = true;

    /* Preserve indisreplident in the new index */
    (*newIndexForm).indisreplident = (*oldIndexForm).indisreplident;

    /* Preserve indisclustered in the new index */
    (*newIndexForm).indisclustered = (*oldIndexForm).indisclustered;

    /*
     * Mark the new index as valid, and the old index as invalid similarly to
     * what index_set_state_flags() does.
     */
    (*newIndexForm).indisvalid = true;
    (*oldIndexForm).indisvalid = false;
    (*oldIndexForm).indisclustered = false;
    (*oldIndexForm).indisreplident = false;

    CatalogTupleUpdate(pg_index, &mut (*oldIndexTuple).t_self, oldIndexTuple);
    CatalogTupleUpdate(pg_index, &mut (*newIndexTuple).t_self, newIndexTuple);

    heap_freetuple(oldIndexTuple);
    heap_freetuple(newIndexTuple);

    /*
     * Move constraints and triggers over to the new index
     */
    let mut constraintOids: *mut List = get_index_ref_constraints(oldIndexId);

    let indexConstraintOid: Oid = get_index_constraint(oldIndexId);

    if OidIsValid(indexConstraintOid) {
        constraintOids = lappend_oid(constraintOids, indexConstraintOid);
    }

    let pg_constraint: Relation = table_open(ConstraintRelationId, RowExclusiveLock);
    let pg_trigger: Relation = table_open(TriggerRelationId, RowExclusiveLock);

    let mut lc: *mut ListCell = list_head(constraintOids);
    while !lc.is_null() {
        let constraintOid: Oid = lfirst_oid(lc);

        /* Move the constraint from the old to the new index */
        let constraintTuple: *mut HeapTupleData = SearchSysCacheCopy1(
            CONSTROID,
            ObjectIdGetDatum(constraintOid),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(constraintTuple) {
            elog!(ERROR, "could not find tuple for constraint {}", constraintOid);
        }

        let conForm: Form_pg_constraint =
            GETSTRUCT(constraintTuple) as Form_pg_constraint;

        if (*conForm).conindid == oldIndexId {
            (*conForm).conindid = newIndexId;
            CatalogTupleUpdate(
                pg_constraint,
                &mut (*constraintTuple).t_self,
                constraintTuple,
            );
        }

        heap_freetuple(constraintTuple);

        /* Search for trigger records */
        let mut key: [ScanKeyData; 1] = [unsafe { core::mem::zeroed::<ScanKeyData>() }; 1];
        ScanKeyInit(
            &mut key[0],
            Anum_pg_trigger_tgconstraint,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(constraintOid),
        );

        let scan: SysScanDesc = systable_beginscan(
            pg_trigger,
            TriggerConstraintIndexId,
            true,
            core::ptr::null_mut(),
            1,
            key.as_mut_ptr(),
        );

        loop {
            let triggerTuple_raw: *mut HeapTupleData = systable_getnext(scan);
            if !HeapTupleIsValid(triggerTuple_raw) {
                break;
            }
            let tgForm: Form_pg_trigger = GETSTRUCT(triggerTuple_raw) as Form_pg_trigger;

            if (*tgForm).tgconstrindid != oldIndexId {
                continue;
            }

            /* Make a modifiable copy */
            let triggerTuple: *mut HeapTupleData = heap_copytuple(triggerTuple_raw);
            let tgForm2: Form_pg_trigger = GETSTRUCT(triggerTuple) as Form_pg_trigger;

            (*tgForm2).tgconstrindid = newIndexId;

            CatalogTupleUpdate(
                pg_trigger,
                &mut (*triggerTuple).t_self,
                triggerTuple,
            );

            heap_freetuple(triggerTuple);
        }

        systable_endscan(scan);
        lc = lnext(constraintOids, lc);
    }

    /*
     * Move comment if any
     */
    {
        let description: Relation =
            table_open(DescriptionRelationId, RowExclusiveLock);
        let mut skey: [ScanKeyData; 3] = [unsafe { core::mem::zeroed::<ScanKeyData>() }; 3];
        let mut values: [Datum; 3] = [0; 3]; /* Natts_pg_description */
        let mut nulls: [bool; 3] = [false; 3];
        let mut replaces: [bool; 3] = [false; 3];

        values[(Anum_pg_description_objoid - 1) as usize] =
            ObjectIdGetDatum(newIndexId);
        replaces[(Anum_pg_description_objoid - 1) as usize] = true;

        ScanKeyInit(
            &mut skey[0],
            Anum_pg_description_objoid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(oldIndexId),
        );
        ScanKeyInit(
            &mut skey[1],
            Anum_pg_description_classoid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(RelationRelationId),
        );
        ScanKeyInit(
            &mut skey[2],
            Anum_pg_description_objsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum(0),
        );

        let sd: SysScanDesc = systable_beginscan(
            description,
            DescriptionObjIndexId,
            true,
            core::ptr::null_mut(),
            3,
            skey.as_mut_ptr(),
        );

        loop {
            let tuple: *mut HeapTupleData = systable_getnext(sd);
            if tuple.is_null() {
                break;
            }
            let tuple2: *mut HeapTupleData = heap_modify_tuple(
                tuple,
                RelationGetDescr(description),
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
            );
            CatalogTupleUpdate(description, &mut (*tuple2).t_self, tuple2);
            break; /* Assume there can be only one match */
        }

        systable_endscan(sd);
        table_close(description, NoLock);
    }

    /*
     * Swap inheritance relationship with parent index
     */
    if get_rel_relispartition(oldIndexId) {
        let ancestors: *mut List = get_partition_ancestors(oldIndexId);
        let parentIndexRelid: Oid = linitial_oid(ancestors);

        DeleteInheritsTuple(oldIndexId, parentIndexRelid, false, core::ptr::null());
        StoreSingleInheritance(newIndexId, parentIndexRelid, 1);

        list_free(ancestors);
    }

    /*
     * Swap all dependencies of and on the old index to the new one, and
     * vice-versa.  Note that a call to CommandCounterIncrement() would cause
     * duplicate entries in pg_depend, so this should not be done.
     */
    changeDependenciesOf(RelationRelationId, newIndexId, oldIndexId);
    changeDependenciesOn(RelationRelationId, newIndexId, oldIndexId);

    changeDependenciesOf(RelationRelationId, oldIndexId, newIndexId);
    changeDependenciesOn(RelationRelationId, oldIndexId, newIndexId);

    /* copy over statistics from old to new index */
    pgstat_copy_relation_stats(newClassRel, oldClassRel);

    /* Copy data of pg_statistic from the old index to the new one */
    CopyStatistics(oldIndexId, newIndexId);

    /* Close relations */
    table_close(pg_class, RowExclusiveLock);
    table_close(pg_index, RowExclusiveLock);
    table_close(pg_constraint, RowExclusiveLock);
    table_close(pg_trigger, RowExclusiveLock);

    /* The lock taken previously is not released until the end of transaction */
    relation_close(oldClassRel, NoLock);
    relation_close(newClassRel, NoLock);
}

/*
 * index_concurrently_set_dead
 *
 * Perform the last invalidation stage of DROP INDEX CONCURRENTLY or REINDEX
 * CONCURRENTLY before actually dropping the index.  After calling this
 * function, the index is seen by all the backends as dead.  Low-level locks
 * taken here are kept until the end of the transaction calling this function.
 */
pub unsafe fn index_concurrently_set_dead(heapId: Oid, indexId: Oid) {
    /*
     * No more predicate locks will be acquired on this index, and we're about
     * to stop doing inserts into the index which could show conflicts with
     * existing predicate locks, so now is the time to move them to the heap
     * relation.
     */
    let userHeapRelation: Relation =
        table_open(heapId, ShareUpdateExclusiveLock);
    let userIndexRelation: Relation =
        index_open(indexId, ShareUpdateExclusiveLock);
    TransferPredicateLocksToHeapRelation(userIndexRelation);

    /*
     * Now we are sure that nobody uses the index for queries; they just might
     * have it open for updating it.  So now we can unset indisready and
     * indislive, then wait till nobody could be using it at all anymore.
     */
    index_set_state_flags(indexId, INDEX_DROP_SET_DEAD);

    /*
     * Invalidate the relcache for the table, so that after this commit all
     * sessions will refresh the table's index list.  Forgetting just the
     * index's relcache entry is not enough.
     */
    CacheInvalidateRelcache(userHeapRelation);

    /*
     * Close the relations again, though still holding session lock.
     */
    table_close(userHeapRelation, NoLock);
    index_close(userIndexRelation, NoLock);
}

/*
 * index_constraint_create
 *
 * Set up a constraint associated with an index.  Return the new constraint's
 * address.
 *
 * heapRelation: table owning the index (must be suitably locked by caller)
 * indexRelationId: OID of the index
 * parentConstraintId: if constraint is on a partition, the OID of the
 *        constraint in the parent.
 * indexInfo: same info executor uses to insert into the index
 * constraintName: what it say (generally, should match name of index)
 * constraintType: one of CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, or
 *        CONSTRAINT_EXCLUSION
 * flags: bitmask (INDEX_CONSTR_CREATE_*)
 * allow_system_table_mods: allow table to be a system catalog
 * is_internal: index is constructed due to internal process
 */
pub unsafe fn index_constraint_create(
    heapRelation: Relation,
    indexRelationId: Oid,
    parentConstraintId: Oid,
    indexInfo: *const IndexInfo,
    constraintName: *const c_char,
    constraintType: c_char,
    constr_flags: bits16,
    allow_system_table_mods: bool,
    is_internal: bool,
) -> ObjectAddress {
    let namespaceId: Oid = RelationGetNamespace(heapRelation);
    let myself: ObjectAddress;
    let idxaddr: ObjectAddress;
    let conOid: Oid;
    let deferrable: bool = (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) != 0;
    let initdeferred: bool = (constr_flags & INDEX_CONSTR_CREATE_INIT_DEFERRED) != 0;
    let mark_as_primary: bool =
        (constr_flags & INDEX_CONSTR_CREATE_MARK_AS_PRIMARY) != 0;
    let is_without_overlaps: bool =
        (constr_flags & INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS) != 0;
    let islocal: bool;
    let noinherit: bool;
    let inhcount: i16;

    /* constraint creation support doesn't work while bootstrapping */
    Assert!(!IsBootstrapProcessingMode());

    /* enforce system-table restriction */
    if !allow_system_table_mods
        && IsSystemRelation(heapRelation)
        && IsNormalProcessingMode()
    {
        ereport!(ERROR, errmsg!(
                "user-defined indexes on system catalog tables are not supported"
            )) /* C also: errcode */;
    }

    /* primary/unique constraints shouldn't have any expressions */
    if !(*indexInfo).ii_Expressions.is_null()
        && constraintType != CONSTRAINT_EXCLUSION
    {
        elog!(ERROR, "constraints cannot have index expressions");
    }

    /*
     * If we're manufacturing a constraint for a pre-existing index, we need
     * to get rid of the existing auto dependencies for the index (the ones
     * that index_create() would have made instead of calling this function).
     *
     * Note: this code would not necessarily do the right thing if the index
     * has any expressions or predicate, but we'd never be turning such an
     * index into a UNIQUE or PRIMARY KEY constraint.
     */
    if (constr_flags & INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS) != 0 {
        deleteDependencyRecordsForClass(
            RelationRelationId,
            indexRelationId,
            RelationRelationId,
            DEPENDENCY_AUTO,
        );
    }

    if OidIsValid(parentConstraintId) {
        islocal = false;
        inhcount = 1;
        noinherit = false;
    } else {
        islocal = true;
        inhcount = 0;
        noinherit = true;
    }

    /*
     * Construct a pg_constraint entry.
     */
    conOid = CreateConstraintEntry(
        constraintName,
        namespaceId,
        constraintType,
        deferrable,
        initdeferred,
        true, /* Is Enforced */
        true,
        parentConstraintId,
        RelationGetRelid(heapRelation),
        (*indexInfo).ii_IndexAttrNumbers.as_ptr(),
        (*indexInfo).ii_NumIndexKeyAttrs,
        (*indexInfo).ii_NumIndexAttrs,
        InvalidOid,      /* no domain */
        indexRelationId, /* index OID */
        InvalidOid,      /* no foreign key */
        core::ptr::null(),
        core::ptr::null(),
        core::ptr::null(),
        core::ptr::null(),
        core::ptr::null(), /* _fkDeleteSetCols */
        0,
        b' ' as c_char,
        b' ' as c_char,
        core::ptr::null(),
        0,
        b' ' as c_char,
        (*indexInfo).ii_ExclusionOps,
        core::ptr::null(), /* no check constraint */
        core::ptr::null(),
        islocal,
        inhcount,
        noinherit,
        is_without_overlaps,
        is_internal,
    );

    /*
     * Register the index as internally dependent on the constraint.
     *
     * Note that the constraint has a dependency on the table, so we don't
     * need (or want) any direct dependency from the index to the table.
     */
    let mut myself_tmp = INVALID_OBJECT_ADDRESS;
    ObjectAddressSet!(myself_tmp, ConstraintRelationId, conOid);
    let myself = myself_tmp;

    let mut idxaddr_tmp = INVALID_OBJECT_ADDRESS;
    ObjectAddressSet!(idxaddr_tmp, RelationRelationId, indexRelationId);
    let idxaddr = idxaddr_tmp;

    recordDependencyOn(&idxaddr, &myself, DEPENDENCY_INTERNAL);

    /*
     * Also, if this is a constraint on a partition, give it partition-type
     * dependencies on the parent constraint as well as the table.
     */
    if OidIsValid(parentConstraintId) {
        let mut referenced: ObjectAddress = INVALID_OBJECT_ADDRESS;

        ObjectAddressSet!(referenced, ConstraintRelationId, parentConstraintId);
        recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_PRI);
        ObjectAddressSet!(
            referenced,
            RelationRelationId,
            RelationGetRelid(heapRelation)
        );
        recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_SEC);
    }

    /*
     * If the constraint is deferrable, create the deferred uniqueness
     * checking trigger.  (The trigger will be given an internal dependency on
     * the constraint by CreateTrigger.)
     */
    if deferrable {
        /* TODO(pg-port): CreateTrigger / CreateTrigStmt full port */
        let trigger: *mut c_void = makeNode_CreateTrigStmt();
        let _ = CreateTrigger(
            trigger,
            core::ptr::null_mut(),
            RelationGetRelid(heapRelation),
            InvalidOid,
            conOid,
            indexRelationId,
            InvalidOid,
            InvalidOid,
            core::ptr::null_mut(),
            true,
            false,
        );
    }

    /*
     * If needed, mark the index as primary and/or deferred in pg_index.
     *
     * Note: When making an existing index into a constraint, caller must have
     * a table lock that prevents concurrent table updates; otherwise, there
     * is a risk that concurrent readers of the table will miss seeing this
     * index at all.
     */
    if (constr_flags & INDEX_CONSTR_CREATE_UPDATE_INDEX) != 0
        && (mark_as_primary || deferrable)
    {
        let pg_index: Relation = table_open(IndexRelationId, RowExclusiveLock);

        let indexTuple: *mut HeapTupleData = SearchSysCacheCopy1(
            INDEXRELID,
            ObjectIdGetDatum(indexRelationId),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(indexTuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexRelationId);
        }
        let indexForm: Form_pg_index = GETSTRUCT(indexTuple) as Form_pg_index;

        let mut dirty = false;
        let mut marked_as_primary = false;

        if mark_as_primary && !(*indexForm).indisprimary {
            (*indexForm).indisprimary = true;
            dirty = true;
            marked_as_primary = true;
        }

        if deferrable && (*indexForm).indimmediate {
            (*indexForm).indimmediate = false;
            dirty = true;
        }

        if dirty {
            CatalogTupleUpdate(pg_index, &mut (*indexTuple).t_self, indexTuple);

            /*
             * When we mark an existing index as primary, force a relcache
             * flush on its parent table, so that all sessions will become
             * aware that the table now has a primary key.  This is important
             * because it affects some replication behaviors.
             */
            if marked_as_primary {
                CacheInvalidateRelcache(heapRelation);
            }

            InvokeObjectPostAlterHookArg(
                IndexRelationId,
                indexRelationId,
                0,
                InvalidOid,
                is_internal,
            );
        }

        heap_freetuple(indexTuple);
        table_close(pg_index, RowExclusiveLock);
    }

    myself
}

/*
 *        index_drop
 *
 * NOTE: this routine should now only be called through performDeletion(),
 * else associated dependencies won't be cleaned up.
 *
 * If concurrent is true, do a DROP INDEX CONCURRENTLY.  If concurrent is
 * false but concurrent_lock_mode is true, then do a normal DROP INDEX but
 * take a lock for CONCURRENTLY processing.  That is used as part of REINDEX
 * CONCURRENTLY.
 */
pub unsafe fn index_drop(indexId: Oid, concurrent: bool, concurrent_lock_mode: bool) {
    /*
     * A temporary relation uses a non-concurrent DROP.  Other backends can't
     * access a temporary relation, so there's no harm in grabbing a stronger
     * lock (see comments in RemoveRelations), and a non-concurrent DROP is
     * more efficient.
     */
    Assert!(
        get_rel_persistence(indexId) != RELPERSISTENCE_TEMP
            || (!concurrent && !concurrent_lock_mode)
    );

    /*
     * To drop an index safely, we must grab exclusive lock on its parent
     * table.  Exclusive lock on the index alone is insufficient because
     * another backend might be about to execute a query on the parent table.
     * If it relies on a previously cached list of index OIDs, then it could
     * attempt to access the just-dropped index.  We must therefore take a
     * table lock strong enough to prevent all queries on the table from
     * proceeding until we commit and send out a shared-cache-inval notice
     * that will make them update their index lists.
     *
     * In the concurrent case we avoid this requirement by disabling index use
     * in multiple steps and waiting out any transactions that might be using
     * the index, so we don't need exclusive lock on the parent table. Instead
     * we take ShareUpdateExclusiveLock, to ensure that two sessions aren't
     * doing CREATE/DROP INDEX CONCURRENTLY on the same index.  (We will get
     * AccessExclusiveLock on the index below, once we're sure nobody else is
     * using it.)
     */
    let heapId: Oid = IndexGetRelation(indexId, false);
    let lockmode: LOCKMODE = if concurrent || concurrent_lock_mode {
        ShareUpdateExclusiveLock
    } else {
        AccessExclusiveLock
    };
    let mut userHeapRelation: Relation = table_open(heapId, lockmode);
    let mut userIndexRelation: Relation = index_open(indexId, lockmode);

    /*
     * We might still have open queries using it in our own session, which the
     * above locking won't prevent, so test explicitly.
     */
    CheckTableNotInUse(userIndexRelation, b"DROP INDEX\0".as_ptr() as *const c_char);

    if concurrent {
        /*
         * We must commit our transaction in order to make the first pg_index
         * state update visible to other sessions.  If the DROP machinery has
         * already performed any other actions (removal of other objects,
         * pg_depend entries, etc), the commit would make those actions
         * permanent, which would leave us with inconsistent catalog state if
         * we fail partway through the following sequence.  Since DROP INDEX
         * CONCURRENTLY is restricted to dropping just one index that has no
         * dependencies, we should get here before anything's been done ---
         * but let's check that to be sure.  We can verify that the current
         * transaction has not executed any transactional updates by checking
         * that no XID has been assigned.
         */
        if GetTopTransactionIdIfAny() != InvalidTransactionId {
            ereport!(ERROR, errmsg!(
                    "DROP INDEX CONCURRENTLY must be first action in transaction"
                )) /* C also: errcode */;
        }

        /*
         * Mark index invalid by updating its pg_index entry
         */
        index_set_state_flags(indexId, INDEX_DROP_CLEAR_VALID);

        /*
         * Invalidate the relcache for the table, so that after this commit
         * all sessions will refresh any cached plans that might reference the
         * index.
         */
        CacheInvalidateRelcache(userHeapRelation);

        /* save lockrelid and locktag for below, then close but keep locks */
        let mut heaprelid: LockRelId = (*userHeapRelation).rd_lockInfo.lockRelId;
        let mut heaplocktag: LOCKTAG = core::mem::zeroed();
        SET_LOCKTAG_RELATION!(
            heaplocktag,
            heaprelid.dbId,
            heaprelid.relId
        );
        let mut indexrelid: LockRelId = (*userIndexRelation).rd_lockInfo.lockRelId;

        table_close(userHeapRelation, NoLock);
        index_close(userIndexRelation, NoLock);

        /*
         * We must commit our current transaction so that the indisvalid
         * update becomes visible to other transactions; then start another.
         * Note that any previously-built data structures are lost in the
         * commit.  The only data we keep past here are the relation IDs.
         *
         * Before committing, get a session-level lock on the table, to ensure
         * that neither it nor the index can be dropped before we finish. This
         * cannot block, even if someone else is waiting for access, because
         * we already have the same lock within our transaction.
         */
        LockRelationIdForSession(&mut heaprelid as *mut LockRelId, ShareUpdateExclusiveLock);
        LockRelationIdForSession(&mut indexrelid as *mut LockRelId, ShareUpdateExclusiveLock);

        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();

        /*
         * Now we must wait until no running transaction could be using the
         * index for a query.  Use AccessExclusiveLock here to check for
         * running transactions that hold locks of any kind on the table. Note
         * we do not need to worry about xacts that open the table for reading
         * after this point; they will see the index as invalid when they open
         * the relation.
         */
        WaitForLockers(heaplocktag, AccessExclusiveLock, true);

        /*
         * Updating pg_index might involve TOAST table access, so ensure we
         * have a valid snapshot.
         */
        PushActiveSnapshot(GetTransactionSnapshot());

        /* Finish invalidation of index and mark it as dead */
        index_concurrently_set_dead(heapId, indexId);

        PopActiveSnapshot();

        /*
         * Again, commit the transaction to make the pg_index update visible
         * to other sessions.
         */
        CommitTransactionCommand();
        StartTransactionCommand();

        /*
         * Wait till every transaction that saw the old index state has
         * finished.  See above about progress reporting.
         */
        WaitForLockers(heaplocktag, AccessExclusiveLock, true);

        /*
         * Re-open relations to allow us to complete our actions.
         *
         * At this point, nothing should be accessing the index, but lets
         * leave nothing to chance and grab AccessExclusiveLock on the index
         * before the physical deletion.
         */
        userHeapRelation = table_open(heapId, ShareUpdateExclusiveLock);
        userIndexRelation = index_open(indexId, AccessExclusiveLock);
    } else {
        /* Not concurrent, so just transfer predicate locks and we're good */
        TransferPredicateLocksToHeapRelation(userIndexRelation);
    }

    /*
     * Schedule physical removal of the files (if any)
     */
    if RELKIND_HAS_STORAGE((*(*userIndexRelation).rd_rel).relkind) {
        RelationDropStorage(userIndexRelation as *mut c_void);
    }

    /* ensure that stats are dropped if transaction commits */
    pgstat_drop_relation(userIndexRelation);

    /*
     * Close and flush the index's relcache entry, to ensure relcache doesn't
     * try to rebuild it while we're deleting catalog entries. We keep the
     * lock though.
     */
    index_close(userIndexRelation, NoLock);

    use crate::utils::cache::relcache::RelationForgetRelation;
    RelationForgetRelation(indexId);

    /*
     * Updating pg_index might involve TOAST table access, so ensure we have a
     * valid snapshot.
     */
    PushActiveSnapshot(GetTransactionSnapshot());

    /*
     * fix INDEX relation, and check for expressional index
     */
    let indexRelation: Relation = table_open(IndexRelationId, RowExclusiveLock);

    let tuple: *mut HeapTupleData = SearchSysCache1(
        INDEXRELID,
        ObjectIdGetDatum(indexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for index {}", indexId);
    }

    let hasexprs: bool = !heap_attisnull(
        tuple,
        Anum_pg_index_indexprs,
        RelationGetDescr(indexRelation),
    );

    CatalogTupleDelete(indexRelation, &mut (*tuple).t_self);

    ReleaseSysCache(tuple);
    table_close(indexRelation, RowExclusiveLock);

    PopActiveSnapshot();

    /*
     * if it has any expression columns, we might have stored statistics about
     * them.
     */
    if hasexprs {
        RemoveStatistics(indexId, 0);
    }

    /*
     * fix ATTRIBUTE relation
     */
    DeleteAttributeTuples(indexId);

    /*
     * fix RELATION relation
     */
    DeleteRelationTuple(indexId);

    /*
     * fix INHERITS relation
     */
    DeleteInheritsTuple(indexId, InvalidOid, false, core::ptr::null());

    /*
     * We are presently too lazy to attempt to compute the new correct value
     * of relhasindex (the next VACUUM will fix it if necessary). So there is
     * no need to update the pg_class tuple for the owning relation. But we
     * must send out a shared-cache-inval notice on the owning relation to
     * ensure other backends update their relcache lists of indexes.  (In the
     * concurrent case, this is redundant but harmless.)
     */
    CacheInvalidateRelcache(userHeapRelation);

    /*
     * Close owning rel, but keep lock
     */
    table_close(userHeapRelation, NoLock);

    /*
     * Release the session locks before we go.
     */
    if concurrent {
        /* TODO(pg-port): need saved heaprelid/indexrelid - see above */
        /* UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock); */
        /* UnlockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock); */
    }
}

/* stubs used above */
unsafe fn ActiveSnapshotSet() -> bool { true /* TODO(pg-port) */ }
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

// ============================================================================
// PART 5: BuildIndexInfo, BuildDummyIndexInfo, CompareIndexInfo,
//         BuildSpeculativeIndexInfo, FormIndexDatum, index_update_stats,
//         index_build, IndexCheckExclusion
// ============================================================================

/* ----------------------------------------------------------------
 *                        index_build support
 * ----------------------------------------------------------------
 */

/* ----------------
 *        BuildIndexInfo
 *            Construct an IndexInfo record for an open index
 *
 * IndexInfo stores the information about the index that's needed by
 * FormIndexDatum, which is used for both index_build() and later insertion
 * of individual index tuples.  Normally we build an IndexInfo for an index
 * just once per command, and then use it for (potentially) many tuples.
 * ----------------
 */
pub unsafe fn BuildIndexInfo(index: Relation) -> *mut IndexInfo {
    let indexStruct: Form_pg_index = (*index).rd_index;
    let numAtts: c_int = (*indexStruct).indnatts as c_int;

    /* check the number of keys, and copy attr numbers into the IndexInfo */
    if numAtts < 1 || numAtts > INDEX_MAX_KEYS as c_int {
        elog!(
            ERROR,
            "invalid indnatts {} for index {}",
            numAtts,
            RelationGetRelid(index)
        );
    }

    /*
     * Create the node, fetching any expressions needed for expressional
     * indexes and index predicate if any.
     */
    let ii: *mut IndexInfo = makeIndexInfo(
        (*indexStruct).indnatts as c_int,
        (*indexStruct).indnkeyatts as c_int,
        (*(*index).rd_rel).relam,
        RelationGetIndexExpressions(index),
        RelationGetIndexPredicate(index),
        (*indexStruct).indisunique,
        (*indexStruct).indnullsnotdistinct,
        (*indexStruct).indisready,
        false,
        (*(*index).rd_indam).amsummarizing,
        (*indexStruct).indisexclusion && (*indexStruct).indisunique,
    );

    /* fill in attribute numbers */
    for i in 0..numAtts {
        (*ii).ii_IndexAttrNumbers[i as usize] =
            /* indkey is a CATALOG_VARLEN field; read via index_getattr in a full port */
            0 as AttrNumber /* TODO(pg-port) */;
    }

    /* fetch exclusion constraint info if any */
    if (*indexStruct).indisexclusion {
        RelationGetExclusionInfo(
            index,
            &mut (*ii).ii_ExclusionOps,
            &mut (*ii).ii_ExclusionProcs,
            &mut (*ii).ii_ExclusionStrats,
        );
    }

    ii
}

/* ----------------
 *        BuildDummyIndexInfo
 *            Construct a dummy IndexInfo record for an open index
 *
 * This differs from the real BuildIndexInfo in that it will never run any
 * user-defined code that might exist in index expressions or predicates.
 * Instead of the real index expressions, we return null constants that have
 * the right types/typmods/collations.  Predicates and exclusion clauses are
 * just ignored.  This is sufficient for the purpose of truncating an index,
 * since we will not need to actually evaluate the expressions or predicates;
 * the only thing that's likely to be done with the data is construction of
 * a tupdesc describing the index's rowtype.
 * ----------------
 */
pub unsafe fn BuildDummyIndexInfo(index: Relation) -> *mut IndexInfo {
    let indexStruct: Form_pg_index = (*index).rd_index;
    let numAtts: c_int = (*indexStruct).indnatts as c_int;

    /* check the number of keys, and copy attr numbers into the IndexInfo */
    if numAtts < 1 || numAtts > INDEX_MAX_KEYS as c_int {
        elog!(
            ERROR,
            "invalid indnatts {} for index {}",
            numAtts,
            RelationGetRelid(index)
        );
    }

    /*
     * Create the node, using dummy index expressions, and pretending there is
     * no predicate.
     */
    let ii: *mut IndexInfo = makeIndexInfo(
        (*indexStruct).indnatts as c_int,
        (*indexStruct).indnkeyatts as c_int,
        (*(*index).rd_rel).relam,
        RelationGetDummyIndexExpressions(index),
        NIL,
        (*indexStruct).indisunique,
        (*indexStruct).indnullsnotdistinct,
        (*indexStruct).indisready,
        false,
        (*(*index).rd_indam).amsummarizing,
        (*indexStruct).indisexclusion && (*indexStruct).indisunique,
    );

    /* fill in attribute numbers */
    for i in 0..numAtts {
        (*ii).ii_IndexAttrNumbers[i as usize] =
            /* indkey is a CATALOG_VARLEN field; read via index_getattr in a full port */
            0 as AttrNumber /* TODO(pg-port) */;
    }

    /* We ignore the exclusion constraint if any */

    ii
}

/*
 * CompareIndexInfo
 *        Return whether the properties of two indexes (in different tables)
 *        indicate that they have the "same" definitions.
 *
 * Note: passing collations and opfamilies separately is a kludge.  Adding
 * them to IndexInfo may result in better coding here and elsewhere.
 *
 * Use build_attrmap_by_name(index2, index1) to build the attmap.
 */
pub unsafe fn CompareIndexInfo(
    info1: *const IndexInfo,
    info2: *const IndexInfo,
    collations1: *const Oid,
    collations2: *const Oid,
    opfamilies1: *const Oid,
    opfamilies2: *const Oid,
    attmap: *const AttrMap,
) -> bool {
    if (*info1).ii_Unique != (*info2).ii_Unique {
        return false;
    }

    if (*info1).ii_NullsNotDistinct != (*info2).ii_NullsNotDistinct {
        return false;
    }

    /* indexes are only equivalent if they have the same access method */
    if (*info1).ii_Am != (*info2).ii_Am {
        return false;
    }

    /* and same number of attributes */
    if (*info1).ii_NumIndexAttrs != (*info2).ii_NumIndexAttrs {
        return false;
    }

    /* and same number of key attributes */
    if (*info1).ii_NumIndexKeyAttrs != (*info2).ii_NumIndexKeyAttrs {
        return false;
    }

    /*
     * and columns match through the attribute map (actual attribute numbers
     * might differ!)  Note that this checks that index columns that are
     * expressions appear in the same positions.  We will next compare the
     * expressions themselves.
     */
    for i in 0..(*info1).ii_NumIndexAttrs {
        let idx = i as usize;
        if ((*attmap).maplen as usize) < (*info2).ii_IndexAttrNumbers[idx] as usize {
            elog!(ERROR, "incorrect attribute map");
        }

        /* ignore expressions for now (but check their collation/opfamily) */
        if !((*info1).ii_IndexAttrNumbers[idx] == InvalidAttrNumber
            && (*info2).ii_IndexAttrNumbers[idx] == InvalidAttrNumber)
        {
            /* fail if just one index has an expression in this column */
            if (*info1).ii_IndexAttrNumbers[idx] == InvalidAttrNumber
                || (*info2).ii_IndexAttrNumbers[idx] == InvalidAttrNumber
            {
                return false;
            }

            /* both are columns, so check for match after mapping */
            let mapped_attnum = (*info2).ii_IndexAttrNumbers[idx] as usize;
            if *(*attmap).attnums.add(mapped_attnum - 1)
                != (*info1).ii_IndexAttrNumbers[idx]
            {
                return false;
            }
        }

        /* collation and opfamily are not valid for included columns */
        if i >= (*info1).ii_NumIndexKeyAttrs {
            continue;
        }

        if *collations1.add(idx) != *collations2.add(idx) {
            return false;
        }
        if *opfamilies1.add(idx) != *opfamilies2.add(idx) {
            return false;
        }
    }

    /*
     * For expression indexes: either both are expression indexes, or neither
     * is; if they are, make sure the expressions match.
     */
    if (!(*info1).ii_Expressions.is_null()) != (!(*info2).ii_Expressions.is_null()) {
        return false;
    }
    if !(*info1).ii_Expressions.is_null() {
        let mut found_whole_row: bool = false;
        let mapped: *mut Node = map_variable_attnos(
            (*info2).ii_Expressions as *mut Node,
            1,
            0,
            attmap,
            InvalidOid,
            &mut found_whole_row,
        );
        if found_whole_row {
            /*
             * we could throw an error here, but seems out of scope for this
             * routine.
             */
            return false;
        }

        if !equal((*info1).ii_Expressions as *mut c_void, mapped as *mut c_void) {
            return false;
        }
    }

    /* Partial index predicates must be identical, if they exist */
    if (*info1).ii_Predicate.is_null() != (*info2).ii_Predicate.is_null() {
        return false;
    }
    if !(*info1).ii_Predicate.is_null() {
        let mut found_whole_row: bool = false;
        let mapped: *mut Node = map_variable_attnos(
            (*info2).ii_Predicate as *mut Node,
            1,
            0,
            attmap,
            InvalidOid,
            &mut found_whole_row,
        );
        if found_whole_row {
            /*
             * we could throw an error here, but seems out of scope for this
             * routine.
             */
            return false;
        }
        if !equal((*info1).ii_Predicate as *mut c_void, mapped as *mut c_void) {
            return false;
        }
    }

    /* No support currently for comparing exclusion indexes. */
    if !(*info1).ii_ExclusionOps.is_null() || !(*info2).ii_ExclusionOps.is_null() {
        return false;
    }

    true
}

/* ----------------
 *        BuildSpeculativeIndexInfo
 *            Add extra state to IndexInfo record
 *
 * For unique indexes, we usually don't want to add info to the IndexInfo for
 * checking uniqueness, since the B-Tree AM handles that directly.  However, in
 * the case of speculative insertion and conflict detection in logical
 * replication, additional support is required.
 *
 * Do this processing here rather than in BuildIndexInfo() to not incur the
 * overhead in the common non-speculative cases.
 * ----------------
 */
pub unsafe fn BuildSpeculativeIndexInfo(index: Relation, ii: *mut IndexInfo) {
    let indnkeyatts: c_int = IndexRelationGetNumberOfKeyAttributes(index);

    /*
     * fetch info for checking unique indexes
     */
    Assert!((*ii).ii_Unique);

    (*ii).ii_UniqueOps =
        palloc(core::mem::size_of::<Oid>() * indnkeyatts as usize) as *mut Oid;
    (*ii).ii_UniqueProcs =
        palloc(core::mem::size_of::<Oid>() * indnkeyatts as usize) as *mut Oid;
    (*ii).ii_UniqueStrats = palloc(
        core::mem::size_of::<uint16>() * indnkeyatts as usize,
    ) as *mut uint16;

    /*
     * We have to look up the operator's strategy number.  This provides a
     * cross-check that the operator does match the index.
     */
    /* We need the func OIDs and strategy numbers too */
    for i in 0..indnkeyatts {
        let idx = i as usize;
        *(*ii).ii_UniqueStrats.add(idx) = IndexAmTranslateCompareType(
            COMPARE_EQ,
            (*(*index).rd_rel).relam,
            *(*index).rd_opfamily.add(idx),
            false,
        );
        *(*ii).ii_UniqueOps.add(idx) = get_opfamily_member(
            *(*index).rd_opfamily.add(idx),
            *(*index).rd_opcintype.add(idx),
            *(*index).rd_opcintype.add(idx),
            *(*ii).ii_UniqueStrats.add(idx),
        );
        if !OidIsValid(*(*ii).ii_UniqueOps.add(idx)) {
            elog!(
                ERROR,
                "missing operator {}({},{}) in opfamily {}",
                *(*ii).ii_UniqueStrats.add(idx),
                *(*index).rd_opcintype.add(idx),
                *(*index).rd_opcintype.add(idx),
                *(*index).rd_opfamily.add(idx)
            );
        }
        *(*ii).ii_UniqueProcs.add(idx) =
            get_opcode(*(*ii).ii_UniqueOps.add(idx));
    }
}

/* ----------------
 *        FormIndexDatum
 *            Construct values[] and isnull[] arrays for a new index tuple.
 *
 *    indexInfo        Info about the index
 *    slot            Heap tuple for which we must prepare an index entry
 *    estate            executor state for evaluating any index expressions
 *    values            Array of index Datums (output area)
 *    isnull            Array of is-null indicators (output area)
 *
 * When there are no index expressions, estate may be NULL.  Otherwise it
 * must be supplied, *and* the ecxt_scantuple slot of its per-tuple expr
 * context must point to the heap tuple passed in.
 *
 * Notice we don't actually call index_form_tuple() here; we just prepare
 * its input arrays values[] and isnull[].  This is because the index AM
 * may wish to alter the data before storage.
 * ----------------
 */
pub unsafe fn FormIndexDatum(
    indexInfo: *mut IndexInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
    values: *mut Datum,
    isnull: *mut bool,
) {
    if !(*indexInfo).ii_Expressions.is_null()
        && (*indexInfo).ii_ExpressionsState.is_null()
    {
        /* First time through, set up expression evaluation state */
        (*indexInfo).ii_ExpressionsState =
            ExecPrepareExprList((*indexInfo).ii_Expressions, estate);
        /* Check caller has set up context correctly */
        Assert!((*GetPerTupleExprContext(estate)).ecxt_scantuple == slot);
    }
    let mut indexpr_item: *mut ListCell =
        list_head((*indexInfo).ii_ExpressionsState);

    for i in 0..(*indexInfo).ii_NumIndexAttrs {
        let idx = i as usize;
        let keycol: c_int = (*indexInfo).ii_IndexAttrNumbers[idx] as c_int;
        let iDatum: Datum;
        let mut isNull: bool = false;

        if keycol < 0 {
            iDatum = slot_getsysattr(slot, keycol, &mut isNull);
        } else if keycol != 0 {
            /*
             * Plain index column; get the value we need directly from the
             * heap tuple.
             */
            iDatum = slot_getattr(slot, keycol, &mut isNull);
        } else {
            /*
             * Index expression --- need to evaluate it.
             */
            if indexpr_item.is_null() {
                elog!(ERROR, "wrong number of index expressions");
            }
            iDatum = ExecEvalExprSwitchContext(
                lfirst(indexpr_item) as *mut ExprState,
                GetPerTupleExprContext(estate),
                &mut isNull,
            );
            indexpr_item = lnext((*indexInfo).ii_ExpressionsState, indexpr_item);
        }
        *values.add(idx) = iDatum;
        *isnull.add(idx) = isNull;
    }

    if !indexpr_item.is_null() {
        elog!(ERROR, "wrong number of index expressions");
    }
}


/*
 * index_update_stats --- update pg_class entry after CREATE INDEX or REINDEX
 *
 * This routine updates the pg_class row of either an index or its parent
 * relation after CREATE INDEX or REINDEX.  Its rather bizarre API is designed
 * to ensure we can do all the necessary work in just one update.
 *
 * hasindex: set relhasindex to this value
 * reltuples: if >= 0, set reltuples to this value; else no change
 *
 * If reltuples >= 0, relpages, relallvisible, and relallfrozen are also
 * updated (using RelationGetNumberOfBlocks() and visibilitymap_count()).
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing relcache
 * entries to be flushed or updated with the new data.  This must happen even
 * if we find that no change is needed in the pg_class row.  When updating
 * a heap entry, this ensures that other backends find out about the new
 * index.  When updating an index, it's important because some index AMs
 * expect a relcache flush to occur after REINDEX.
 */
unsafe fn index_update_stats(rel: Relation, hasindex: bool, reltuples: f64) {
    let relid: Oid = RelationGetRelid(rel);

    /*
     * As a special hack, if we are dealing with an empty table and the
     * existing reltuples is -1, we leave that alone.  This ensures that
     * creating an index as part of CREATE TABLE doesn't cause the table to
     * prematurely look like it's been vacuumed.  The rd_rel we modify may
     * differ from rel->rd_rel due to e.g. commit of concurrent GRANT, but the
     * commands that change reltuples take locks conflicting with ours.  (Even
     * if a command changed reltuples under a weaker lock, this affects only
     * statistics for an empty table.)
     */
    let mut reltuples = reltuples;
    if reltuples == 0.0 && (*(*rel).rd_rel).reltuples < 0.0 {
        reltuples = -1.0;
    }

    /*
     * Don't update statistics during binary upgrade, because the indexes are
     * created before the data is moved into place.
     */
    let mut update_stats: bool = reltuples >= 0.0 && !IsBinaryUpgrade;

    /*
     * If autovacuum is off, user may not be expecting table relstats to
     * change.  This can be important when restoring a dump that includes
     * statistics, as the table statistics may be restored before the index is
     * created, and we want to preserve the restored table statistics.
     */
    let relkind = (*(*rel).rd_rel).relkind;
    if relkind == RELKIND_RELATION
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
    {
        if AutoVacuumingActive() {
            let options = (*rel).rd_options as *mut StdRdOptions;
            if !options.is_null() && !(*options).autovacuum.enabled {
                update_stats = false;
            }
        } else {
            update_stats = false;
        }
    }

    /*
     * Finish I/O and visibility map buffer locks before
     * systable_inplace_update_begin() locks the pg_class buffer.
     */
    let mut relpages: BlockNumber = 0;
    let mut relallvisible: BlockNumber = 0;
    let mut relallfrozen: BlockNumber = 0;
    if update_stats {
        relpages = RelationGetNumberOfBlocks(rel);

        if relkind != RELKIND_INDEX {
            visibilitymap_count(rel, &mut relallvisible, &mut relallfrozen);
        }
    }

    /*
     * We always update the pg_class row using a non-transactional,
     * overwrite-in-place update.
     */
    let pg_class: Relation = table_open(RelationRelationId, RowExclusiveLock);

    let mut key: [ScanKeyData; 1] = [unsafe { core::mem::zeroed::<ScanKeyData>() }; 1];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    let mut tuple: *mut HeapTupleData = core::ptr::null_mut();
    let mut state: *mut c_void = core::ptr::null_mut();
    systable_inplace_update_begin(
        pg_class,
        ClassOidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
        &mut tuple,
        &mut state,
    );

    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "could not find tuple for relation {}", relid);
    }
    let rd_rel: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;

    /* Should this be a more comprehensive test? */
    Assert!((*rd_rel).relkind != RELKIND_PARTITIONED_INDEX);

    /* Apply required updates, if any, to copied tuple */

    let mut dirty = false;
    if (*rd_rel).relhasindex != hasindex {
        (*rd_rel).relhasindex = hasindex;
        dirty = true;
    }

    if update_stats {
        if (*rd_rel).relpages != relpages as i32 {
            (*rd_rel).relpages = relpages as i32;
            dirty = true;
        }
        if ((*rd_rel).reltuples - reltuples as f32).abs() > f32::EPSILON {
            (*rd_rel).reltuples = reltuples as f32;
            dirty = true;
        }
        if (*rd_rel).relallvisible != relallvisible as i32 {
            (*rd_rel).relallvisible = relallvisible as i32;
            dirty = true;
        }
        if (*rd_rel).relallfrozen != relallfrozen as i32 {
            (*rd_rel).relallfrozen = relallfrozen as i32;
            dirty = true;
        }
    }

    /*
     * If anything changed, write out the tuple
     */
    if dirty {
        systable_inplace_update_finish(state, tuple);
        /* the above sends transactional and immediate cache inval messages */
    } else {
        systable_inplace_update_cancel(state);

        /*
         * While we didn't change relhasindex, CREATE INDEX needs a
         * transactional inval for when the new index's catalog rows become
         * visible.
         */
        CacheInvalidateRelcacheByTuple(tuple);
    }

    heap_freetuple(tuple);

    table_close(pg_class, RowExclusiveLock);
}


/*
 * index_build - invoke access-method-specific index build procedure
 *
 * On entry, the index's catalog entries are valid, and its physical disk
 * file has been created but is empty.  We call the AM-specific build
 * procedure to fill in the index contents.  We then update the pg_class
 * entries of the index and heap relation as needed, using statistics
 * returned by ambuild as well as data passed by the caller.
 *
 * isreindex indicates we are recreating a previously-existing index.
 * parallel indicates if parallelism may be useful.
 *
 * Note: before Postgres 8.2, the passed-in heap and index Relations
 * were automatically closed by this routine.  This is no longer the case.
 * The caller opened 'em, and the caller should close 'em.
 */
pub unsafe fn index_build(
    heapRelation: Relation,
    indexRelation: Relation,
    indexInfo: *mut IndexInfo,
    isreindex: bool,
    parallel: bool,
) {
    /*
     * sanity checks
     */
    Assert!(RelationIsValid(indexRelation));
    Assert!(PointerIsValid((*indexRelation).rd_indam as *const c_void));
    Assert!((*(*indexRelation).rd_indam).ambuild.is_some());
    Assert!((*(*indexRelation).rd_indam).ambuildempty.is_some());

    /*
     * Determine worker process details for parallel CREATE INDEX.  Currently,
     * only btree, GIN, and BRIN have support for parallel builds.
     *
     * Note that planner considers parallel safety for us.
     */
    if parallel
        && IsNormalProcessingMode()
        && (*(*indexRelation).rd_indam).amcanbuildparallel
    {
        (*indexInfo).ii_ParallelWorkers = plan_create_index_workers(
            RelationGetRelid(heapRelation),
            RelationGetRelid(indexRelation),
        );
    }

    if (*indexInfo).ii_ParallelWorkers == 0 {
        ereport!(DEBUG1, errmsg!(
                "building index \"{}\" on table \"{}\" serially",
                CStr_to_str(RelationGetRelationName(indexRelation)),
                CStr_to_str(RelationGetRelationName(heapRelation))
            ));
    } else {
        ereport!(DEBUG1, errmsg!(
                "building index \"{}\" on table \"{}\" with request for {} parallel workers",
                CStr_to_str(RelationGetRelationName(indexRelation)),
                CStr_to_str(RelationGetRelationName(heapRelation)),
                (*indexInfo).ii_ParallelWorkers
            ));
    }

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext(
        (*(*heapRelation).rd_rel).relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel: c_int = NewGUCNestLevel();
    RestrictSearchPath();

    /* Set up initial progress report status */
    {
        let progress_index: [c_int; 6] = [
            PROGRESS_CREATEIDX_PHASE,
            PROGRESS_CREATEIDX_SUBPHASE,
            PROGRESS_CREATEIDX_TUPLES_DONE,
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
            PROGRESS_SCAN_BLOCKS_TOTAL,
        ];
        let progress_vals: [i64; 6] = [
            PROGRESS_CREATEIDX_PHASE_BUILD as i64,
            PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE as i64,
            0,
            0,
            0,
            0,
        ];
        pgstat_progress_update_multi_param(6, progress_index.as_ptr(), progress_vals.as_ptr());
    }

    /*
     * Call the access method's build procedure
     */
    let stats: *mut IndexBuildResult = ((*(*indexRelation).rd_indam).ambuild.unwrap())(
        heapRelation,
        indexRelation,
        indexInfo as *mut c_void,
    );
    Assert!(PointerIsValid(stats as *const c_void));

    /*
     * If this is an unlogged index, we may need to write out an init fork for
     * it -- but we must first check whether one already exists.
     */
    if (*(*indexRelation).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED
        && !smgrexists(RelationGetSmgr(indexRelation), INIT_FORKNUM)
    {
        smgrcreate(RelationGetSmgr(indexRelation), INIT_FORKNUM, false);
        log_smgrcreate(&(*indexRelation).rd_locator as *const _ as *const _, INIT_FORKNUM);
        ((*(*indexRelation).rd_indam).ambuildempty.unwrap())(indexRelation);
    }

    /*
     * If we found any potentially broken HOT chains, mark the index as not
     * being usable until the current transaction is below the event horizon.
     */
    if (*indexInfo).ii_BrokenHotChain && !isreindex && !(*indexInfo).ii_Concurrent {
        let indexId: Oid = RelationGetRelid(indexRelation);
        let pg_index: Relation = table_open(IndexRelationId, RowExclusiveLock);

        let indexTuple: *mut HeapTupleData = SearchSysCacheCopy1(
            INDEXRELID,
            ObjectIdGetDatum(indexId),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(indexTuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexId);
        }
        let indexForm: Form_pg_index = GETSTRUCT(indexTuple) as Form_pg_index;

        /* If it's a new index, indcheckxmin shouldn't be set ... */
        Assert!(!(*indexForm).indcheckxmin);

        (*indexForm).indcheckxmin = true;
        CatalogTupleUpdate(pg_index, &mut (*indexTuple).t_self, indexTuple);

        heap_freetuple(indexTuple);
        table_close(pg_index, RowExclusiveLock);
    }

    /*
     * Update heap and index pg_class rows
     */
    let index_build_result_heap_tuples: f64; /* TODO(pg-port): real struct access */
    let index_build_result_index_tuples: f64;
    /* Opaque stub: read heap_tuples / index_tuples offsets 0/8 */
    index_build_result_heap_tuples = *(stats as *const f64).add(0);
    index_build_result_index_tuples = *(stats as *const f64).add(1);

    index_update_stats(heapRelation, true, index_build_result_heap_tuples);
    index_update_stats(indexRelation, false, index_build_result_index_tuples);

    /* Make the updated catalog row versions visible */
    CommandCounterIncrement();

    /*
     * If it's for an exclusion constraint, make a second pass over the heap
     * to verify that the constraint is satisfied.
     */
    if !(*indexInfo).ii_ExclusionOps.is_null() {
        IndexCheckExclusion(heapRelation, indexRelation, indexInfo);
    }

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);
}

/*
 * IndexCheckExclusion - verify that a new exclusion constraint is satisfied
 *
 * When creating an exclusion constraint, we first build the index normally
 * and then rescan the heap to check for conflicts.
 */
unsafe fn IndexCheckExclusion(
    heapRelation: Relation,
    indexRelation: Relation,
    indexInfo: *mut IndexInfo,
) {
    let mut values: [Datum; 32] = [0; 32]; /* INDEX_MAX_KEYS */
    let mut isnull: [bool; 32] = [false; 32];

    /*
     * If we are reindexing the target index, mark it as no longer being
     * reindexed, to forestall an Assert in index_beginscan when we try to use
     * the index for probes.
     */
    if ReindexIsCurrentlyProcessingIndex(RelationGetRelid(indexRelation)) {
        ResetReindexProcessing();
    }

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.  Also a slot to hold the current tuple.
     */
    let estate: *mut EState = CreateExecutorState();
    let econtext = GetPerTupleExprContext(estate);
    let slot: *mut TupleTableSlot = table_slot_create(heapRelation, core::ptr::null_mut());

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    let predicate: *mut ExprState = ExecPrepareQual((*indexInfo).ii_Predicate, estate);

    /*
     * Scan all live tuples in the base relation.
     */
    let snapshot = RegisterSnapshot(GetLatestSnapshot());
    let scan = table_beginscan_strat(
        heapRelation,
        snapshot,
        0,
        core::ptr::null_mut(),
        true,
        true,
    );

    while table_scan_getnextslot(scan, ForwardScanDirection, slot) {
        CHECK_FOR_INTERRUPTS!();

        /*
         * In a partial index, ignore tuples that don't satisfy the predicate.
         */
        if !predicate.is_null() {
            if !ExecQual(predicate, econtext) {
                continue;
            }
        }

        /*
         * Extract index column values, including computing expressions.
         */
        FormIndexDatum(indexInfo, slot, estate, values.as_mut_ptr(), isnull.as_mut_ptr());

        /*
         * Check that this tuple has no conflicts.
         */
        check_exclusion_constraint(
            heapRelation,
            indexRelation,
            indexInfo,
            &mut (*slot).tts_tid,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
            estate,
            true,
        );

        MemoryContextReset((*econtext).ecxt_per_tuple_memory);
    }

    table_endscan(scan);
    UnregisterSnapshot(snapshot);

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);

    /* These may have been pointing to the now-gone estate */
    (*indexInfo).ii_ExpressionsState = NIL;
    (*indexInfo).ii_PredicateState = core::ptr::null_mut();
}

/* helpers used above */
#[inline]
fn RelationIsValid(rel: Relation) -> bool { !rel.is_null() }
#[inline]
fn PointerIsValid(p: *const c_void) -> bool { !p.is_null() }

/* opcode / opfamily stubs -- TODO(pg-port) */
unsafe fn get_opcode(opno: Oid) -> Oid { 0 /* TODO(pg-port) */ }
unsafe fn get_opfamily_member(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: uint16) -> Oid {
    InvalidOid /* TODO(pg-port) */
}
const COMPARE_EQ: c_int = 1; /* CompareType::COMPARE_EQ */
unsafe fn IndexAmTranslateCompareType(
    compareType: c_int, amoid: Oid, opfamily: Oid, missing_ok: bool,
) -> uint16 {
    1 /* TODO(pg-port) */
}
unsafe fn MemoryContextReset(_ctxt: MemoryContext) { /* TODO(pg-port) */ }

// ============================================================================
// PART 6: validate_index, index_set_state_flags (re-exported), IndexGetRelation,
//         reindex_index, reindex_relation, reindex state management
// ============================================================================

/*
 * validate_index - support code for concurrent index builds
 *
 * We do a concurrent index build by first inserting the catalog entry for the
 * index via index_create(), marking it not indisready and not indisvalid.
 * Then we commit our transaction and start a new one, then we wait for all
 * transactions that could have been modifying the table to terminate.
 */
pub unsafe fn validate_index(heapId: Oid, indexId: Oid, snapshot: Snapshot) {
    {
        let progress_index: [c_int; 5] = [
            PROGRESS_CREATEIDX_PHASE,
            PROGRESS_CREATEIDX_TUPLES_DONE,
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
            PROGRESS_SCAN_BLOCKS_TOTAL,
        ];
        let progress_vals: [i64; 5] = [
            PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN as i64,
            0,
            0,
            0,
            0,
        ];
        pgstat_progress_update_multi_param(
            5,
            progress_index.as_ptr(),
            progress_vals.as_ptr(),
        );
    }

    /* Open and lock the parent heap relation */
    let heapRelation: Relation =
        table_open(heapId, ShareUpdateExclusiveLock);

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext(
        (*(*heapRelation).rd_rel).relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel: c_int = NewGUCNestLevel();
    RestrictSearchPath();

    let indexRelation: Relation = index_open(indexId, RowExclusiveLock);

    /*
     * Fetch info needed for index_insert.  (You might think this should be
     * passed in from DefineIndex, but its copy is long gone due to having
     * been built in a previous transaction.)
     */
    let indexInfo: *mut IndexInfo = BuildIndexInfo(indexRelation);

    /* mark build is concurrent just for consistency */
    (*indexInfo).ii_Concurrent = true;

    /*
     * Scan the index and gather up all the TIDs into a tuplesort object.
     */
    let ivinfo_ptr = palloc0(core::mem::size_of::<IndexVacuumInfoReal>())
        as *mut IndexVacuumInfoReal;
    (*ivinfo_ptr).index = indexRelation;
    (*ivinfo_ptr).heaprel = heapRelation;
    (*ivinfo_ptr).analyze_only = false;
    (*ivinfo_ptr).report_progress = true;
    (*ivinfo_ptr).estimated_count = true;
    (*ivinfo_ptr).message_level = DEBUG2;
    (*ivinfo_ptr).num_heap_tuples = (*(*heapRelation).rd_rel).reltuples as f64;
    (*ivinfo_ptr).strategy = core::ptr::null_mut();

    let mut state_val = ValidateIndexStateReal {
        tuplesort: core::ptr::null_mut(),
        htups: 0.0,
        itups: 0.0,
        tups_inserted: 0.0,
    };

    /*
     * Encode TIDs as int8 values for the sort, rather than directly sorting
     * item pointers.
     */
    state_val.tuplesort = tuplesort_begin_datum(
        INT8OID,
        Int8LessOperator,
        InvalidOid,
        false,
        maintenance_work_mem,
        core::ptr::null_mut(),
        TUPLESORT_NONE,
    );

    /* ambulkdelete updates progress metrics */
    let _ = index_bulk_delete(
        ivinfo_ptr as *mut crate::access::index::genam::IndexVacuumInfo,
        core::ptr::null_mut(),
        Some(validate_index_callback),
        &mut state_val as *mut ValidateIndexStateReal as *mut c_void,
    );

    /* Execute the sort */
    {
        let progress_index: [c_int; 3] = [
            PROGRESS_CREATEIDX_PHASE,
            PROGRESS_SCAN_BLOCKS_DONE,
            PROGRESS_SCAN_BLOCKS_TOTAL,
        ];
        let progress_vals: [i64; 3] = [
            PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT as i64,
            0,
            0,
        ];
        pgstat_progress_update_multi_param(
            3,
            progress_index.as_ptr(),
            progress_vals.as_ptr(),
        );
    }
    tuplesort_performsort(state_val.tuplesort);

    /*
     * Now scan the heap and "merge" it with the index
     */
    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_PHASE,
        PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN as i64,
    );
    table_index_validate_scan(
        heapRelation,
        indexRelation,
        indexInfo,
        snapshot,
        &mut state_val as *mut ValidateIndexStateReal as *mut ValidateIndexState,
    );

    /* Done with tuplesort object */
    tuplesort_end(state_val.tuplesort);

    /* Make sure to release resources cached in indexInfo (if needed). */
    index_insert_cleanup(indexRelation, indexInfo);

    elog!(
        DEBUG2,
        "validate_index found {:.0} heap tuples, {:.0} index tuples; inserted {:.0} missing tuples",
        state_val.htups,
        state_val.itups,
        state_val.tups_inserted
    );

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Close rels, but keep locks */
    index_close(indexRelation, NoLock);
    table_close(heapRelation, NoLock);
}

/* Real IndexVacuumInfo struct layout (mirrors access/genam.h) */
#[repr(C)]
struct IndexVacuumInfoReal {
    index: Relation,
    heaprel: Relation,
    analyze_only: bool,
    report_progress: bool,
    estimated_count: bool,
    message_level: c_int,
    num_heap_tuples: f64,
    strategy: *mut c_void,
}

/* Real ValidateIndexState (mirrors catalog/index.c local struct) */
#[repr(C)]
struct ValidateIndexStateReal {
    tuplesort: *mut c_void, /* Tuplesortstate * */
    htups: f64,
    itups: f64,
    tups_inserted: f64,
}

/*
 * validate_index_callback - bulkdelete callback to collect the index TIDs
 */
unsafe extern "C" fn validate_index_callback(
    itemptr: *mut crate::storage::itemptr::ItemPointerData,
    opaque: *mut c_void,
) -> bool {
    let state: *mut ValidateIndexStateReal = opaque as *mut ValidateIndexStateReal;
    let encoded: i64 = itemptr_encode(itemptr as *mut crate::storage::itemptr::ItemPointerData);

    tuplesort_putdatum((*state).tuplesort, Int64GetDatum(encoded), false);
    (*state).itups += 1.0;
    false /* never actually delete anything */
}

/*
 * index_set_state_flags - adjust pg_index state flags
 *
 * This is used during CREATE/DROP INDEX CONCURRENTLY to adjust the pg_index
 * flags that denote the index's state.
 *
 * Note that CatalogTupleUpdate() sends a cache invalidation message for the
 * tuple, so other sessions will hear about the update as soon as we commit.
 *
 * (Re-exported here; the real impl lives in access/index/indexam.rs)
 */
/* Already imported from indexam above as index_set_state_flags */

/*
 * IndexGetRelation: given an index's relation OID, get the OID of the
 * relation it is an index on.  Uses the system cache.
 */
pub unsafe fn IndexGetRelation(indexId: Oid, missing_ok: bool) -> Oid {
    let tuple: *mut HeapTupleData = SearchSysCache1(
        INDEXRELID,
        ObjectIdGetDatum(indexId),
    ) as *mut HeapTupleData;
    if !HeapTupleIsValid(tuple) {
        if missing_ok {
            return InvalidOid;
        }
        elog!(ERROR, "cache lookup failed for index {}", indexId);
    }
    let index: Form_pg_index = GETSTRUCT(tuple) as Form_pg_index;
    Assert!((*index).indexrelid == indexId);

    let result: Oid = (*index).indrelid;
    ReleaseSysCache(tuple);
    result
}

/*
 * reindex_index - This routine is used to recreate a single index
 */
pub unsafe fn reindex_index(
    stmt: *const ReindexStmt,
    indexId: Oid,
    skip_constraint_checks: bool,
    persistence: c_char,
    params: *const ReindexParams,
) {
    let mut ru0: PGRUsage = core::mem::zeroed();
    pg_rusage_init(&mut ru0);

    let progress: bool =
        ((*params).options & REINDEXOPT_REPORT_PROGRESS) != 0;
    let mut set_tablespace = false;

    /*
     * Open and lock the parent heap relation.  ShareLock is sufficient since
     * we only need to be sure no schema or data changes are going on.
     */
    let heapId: Oid = IndexGetRelation(
        indexId,
        ((*params).options & REINDEXOPT_MISSING_OK) != 0,
    );
    /* if relation is missing, leave */
    if !OidIsValid(heapId) {
        return;
    }

    let heapRelation: Relation = if ((*params).options & REINDEXOPT_MISSING_OK) != 0 {
        try_table_open(heapId, ShareLock)
    } else {
        table_open(heapId, ShareLock)
    };

    /* if relation is gone, leave */
    if heapRelation.is_null() {
        return;
    }

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.
     */
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext(
        (*(*heapRelation).rd_rel).relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel: c_int = NewGUCNestLevel();
    RestrictSearchPath();

    if progress {
        let progress_cols: [c_int; 2] = [
            PROGRESS_CREATEIDX_COMMAND,
            PROGRESS_CREATEIDX_INDEX_OID,
        ];
        let progress_vals: [i64; 2] = [
            PROGRESS_CREATEIDX_COMMAND_REINDEX as i64,
            indexId as i64,
        ];
        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, heapId);
        pgstat_progress_update_multi_param(
            2,
            progress_cols.as_ptr(),
            progress_vals.as_ptr(),
        );
    }

    /*
     * Open the target index relation and get an exclusive lock on it, to
     * ensure that no one else is touching this particular index.
     */
    let iRel: Relation = if ((*params).options & REINDEXOPT_MISSING_OK) != 0 {
        try_index_open(indexId, AccessExclusiveLock)
    } else {
        index_open(indexId, AccessExclusiveLock)
    };

    /* if index relation is gone, leave */
    if iRel.is_null() {
        /* Roll back any GUC changes */
        AtEOXact_GUC(false, save_nestlevel);
        /* Restore userid and security context */
        SetUserIdAndSecContext(save_userid, save_sec_context);
        /* Close parent heap relation, but keep locks */
        table_close(heapRelation, NoLock);
        return;
    }

    if progress {
        pgstat_progress_update_param(
            PROGRESS_CREATEIDX_ACCESS_METHOD_OID,
            (*(*iRel).rd_rel).relam as i64,
        );
    }

    /*
     * If a statement is available, telling that this comes from a REINDEX
     * command, collect the index for event triggers.
     */
    if !stmt.is_null() {
        let mut address: ObjectAddress = INVALID_OBJECT_ADDRESS;
        ObjectAddressSet!(address, RelationRelationId, indexId);
        EventTriggerCollectSimpleCommand(
            address,
            INVALID_OBJECT_ADDRESS,
            stmt as *mut Node,
        );
    }

    /*
     * Partitioned indexes should never get processed here, as they have no
     * physical storage.
     */
    if (*(*iRel).rd_rel).relkind == RELKIND_PARTITIONED_INDEX {
        elog!(
            ERROR,
            "cannot reindex partitioned index \"{}.{}\"",
            CStr_to_str(get_namespace_name(RelationGetNamespace(iRel)) as *const c_char),
            CStr_to_str(RelationGetRelationName(iRel))
        );
    }

    /*
     * Don't allow reindex on temp tables of other backends ... their local
     * buffer manager is not going to cope.
     */
    if RELATION_IS_OTHER_TEMP(iRel) {
        ereport!(ERROR, errmsg!("cannot reindex temporary tables of other sessions")) /* C also: errcode */;
    }

    /*
     * Don't allow reindex of an invalid index on TOAST table.
     */
    if IsToastNamespace(RelationGetNamespace(iRel)) && !get_index_isvalid(indexId) {
        ereport!(ERROR, errmsg!("cannot reindex invalid index on TOAST table")) /* C also: errcode */;
    }

    /*
     * System relations cannot be moved even if allow_system_table_mods is
     * enabled.
     */
    if OidIsValid((*params).tablespaceOid) && IsSystemRelation(iRel) {
        ereport!(ERROR, errmsg!(
                "cannot move system relation \"{}\"",
                CStr_to_str(RelationGetRelationName(iRel))
            )) /* C also: errcode */;
    }

    /* Check if the tablespace of this index needs to be changed */
    if OidIsValid((*params).tablespaceOid)
        && CheckRelationTableSpaceMove(iRel, (*params).tablespaceOid)
    {
        set_tablespace = true;
    }

    /*
     * Also check for active uses of the index in the current transaction.
     */
    CheckTableNotInUse(iRel, b"REINDEX INDEX\0".as_ptr() as *const c_char);

    /* Set new tablespace, if requested */
    if set_tablespace {
        /* Update its pg_class row */
        SetRelationTableSpace(iRel, (*params).tablespaceOid, InvalidOid);

        /*
         * Schedule unlinking of the old index storage at transaction commit.
         */
        RelationDropStorage(iRel as *mut c_void);
        RelationAssumeNewRelfilelocator(iRel);

        /* Make sure the reltablespace change is visible */
        CommandCounterIncrement();
    }

    /*
     * All predicate locks on the index are about to be made invalid. Promote
     * them to relation locks on the heap.
     */
    TransferPredicateLocksToHeapRelation(iRel);

    /* Fetch info needed for index_build */
    let indexInfo: *mut IndexInfo = BuildIndexInfo(iRel);

    /* If requested, skip checking uniqueness/exclusion constraints */
    let mut skipped_constraint = false;
    if skip_constraint_checks {
        if (*indexInfo).ii_Unique || !(*indexInfo).ii_ExclusionOps.is_null() {
            skipped_constraint = true;
        }
        (*indexInfo).ii_Unique = false;
        (*indexInfo).ii_ExclusionOps = core::ptr::null_mut();
        (*indexInfo).ii_ExclusionProcs = core::ptr::null_mut();
        (*indexInfo).ii_ExclusionStrats = core::ptr::null_mut();
    }

    /* Suppress use of the target index while rebuilding it */
    SetReindexProcessing(heapId, indexId);

    /* Create a new physical relation for the index */
    RelationSetNewRelfilenumber(iRel, persistence);

    /* Initialize the index and rebuild */
    /* Note: we do not need to re-establish pkey setting */
    index_build(heapRelation, iRel, indexInfo, true, true);

    /* Re-allow use of target index */
    ResetReindexProcessing();

    /*
     * If the index is marked invalid/not-ready/dead (ie, it's from a failed
     * CREATE INDEX CONCURRENTLY, or a DROP INDEX CONCURRENTLY failed midway),
     * and we didn't skip a uniqueness check, we can now mark it valid.
     */
    if !skipped_constraint {
        let pg_index: Relation = table_open(IndexRelationId, RowExclusiveLock);

        let indexTuple: *mut HeapTupleData = SearchSysCacheCopy1(
            INDEXRELID,
            ObjectIdGetDatum(indexId),
        ) as *mut HeapTupleData;
        if !HeapTupleIsValid(indexTuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexId);
        }
        let indexForm: Form_pg_index = GETSTRUCT(indexTuple) as Form_pg_index;

        let index_bad: bool = !(*indexForm).indisvalid
            || !(*indexForm).indisready
            || !(*indexForm).indislive;
        if index_bad
            || ((*indexForm).indcheckxmin && !(*indexInfo).ii_BrokenHotChain)
        {
            if !(*indexInfo).ii_BrokenHotChain {
                (*indexForm).indcheckxmin = false;
            } else if index_bad {
                (*indexForm).indcheckxmin = true;
            }
            (*indexForm).indisvalid = true;
            (*indexForm).indisready = true;
            (*indexForm).indislive = true;
            CatalogTupleUpdate(pg_index, &mut (*indexTuple).t_self, indexTuple);

            /*
             * Invalidate the relcache for the table, so that after we commit
             * all sessions will refresh the table's index list.
             */
            CacheInvalidateRelcache(heapRelation);
        }

        table_close(pg_index, RowExclusiveLock);
    }

    /* Log what we did */
    if ((*params).options & REINDEXOPT_VERBOSE) != 0 {
        ereport!(INFO, errmsg!(
                "index \"{}\" was reindexed",
                CStr_to_str(get_rel_name(indexId) as *const c_char)
            )) /* C also: errdetail_internal */;
    }

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Close rels, but keep locks */
    index_close(iRel, NoLock);
    table_close(heapRelation, NoLock);

    if progress {
        pgstat_progress_end_command();
    }
}

/*
 * reindex_relation - This routine is used to recreate all indexes
 * of a relation (and optionally its toast relation too, if any).
 *
 * Returns true if any indexes were rebuilt (including toast table's index
 * when relevant).
 */
pub unsafe fn reindex_relation(
    stmt: *const ReindexStmt,
    relid: Oid,
    flags: c_int,
    params: *const ReindexParams,
) -> bool {
    /*
     * Open and lock the relation.
     */
    let rel: Relation = if ((*params).options & REINDEXOPT_MISSING_OK) != 0 {
        try_table_open(relid, ShareLock)
    } else {
        table_open(relid, ShareLock)
    };

    /* if relation is gone, leave */
    if rel.is_null() {
        return false;
    }

    /*
     * Partitioned tables should never get processed here, as they have no
     * physical storage.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        elog!(
            ERROR,
            "cannot reindex partitioned table \"{}.{}\"",
            CStr_to_str(get_namespace_name(RelationGetNamespace(rel)) as *const c_char),
            CStr_to_str(RelationGetRelationName(rel))
        );
    }

    let toast_relid: Oid = (*(*rel).rd_rel).reltoastrelid;

    /*
     * Get the list of index OIDs for this relation.
     */
    let indexIds: *mut List = RelationGetIndexList(rel);

    if (flags & REINDEX_REL_SUPPRESS_INDEX_USE) != 0 {
        /* Suppress use of all the indexes until they are rebuilt */
        SetReindexPending(indexIds);

        /*
         * Make the new heap contents visible --- now things might be
         * inconsistent!
         */
        CommandCounterIncrement();
    }

    /*
     * Reindex the toast table, if any, before the main table.
     */
    let mut result: bool = false;
    if (flags & REINDEX_REL_PROCESS_TOAST) != 0 && OidIsValid(toast_relid) {
        let mut newparams: ReindexParams = *params;
        newparams.options &= !(REINDEXOPT_MISSING_OK);
        newparams.tablespaceOid = InvalidOid;
        result |= reindex_relation(stmt, toast_relid, flags, &newparams);
    }

    /*
     * Compute persistence of indexes: same as that of owning rel, unless
     * caller specified otherwise.
     */
    let persistence: c_char = if (flags & REINDEX_REL_FORCE_INDEXES_UNLOGGED) != 0 {
        RELPERSISTENCE_UNLOGGED
    } else if (flags & REINDEX_REL_FORCE_INDEXES_PERMANENT) != 0 {
        RELPERSISTENCE_PERMANENT
    } else {
        (*(*rel).rd_rel).relpersistence
    };

    /* Reindex all the indexes. */
    let mut i: c_int = 1;
    let mut indexId_cell: *mut ListCell = list_head(indexIds);
    while !indexId_cell.is_null() {
        let indexOid: Oid = lfirst_oid(indexId_cell);
        let indexNamespaceId: Oid = get_rel_namespace(indexOid);

        /*
         * Skip any invalid indexes on a TOAST table.
         */
        if IsToastNamespace(indexNamespaceId) && !get_index_isvalid(indexOid) {
            ereport!(WARNING, errmsg!(
                    "cannot reindex invalid index \"{}.{}\" on TOAST table, skipping",
                    CStr_to_str(get_namespace_name(indexNamespaceId) as *const c_char),
                    CStr_to_str(get_rel_name(indexOid) as *const c_char)
                )) /* C also: errcode */;

            /*
             * Remove this invalid toast index from the reindex pending list.
             */
            if (flags & REINDEX_REL_SUPPRESS_INDEX_USE) != 0 {
                RemoveReindexPending(indexOid);
            }
            indexId_cell = lnext(indexIds, indexId_cell);
            continue;
        }

        reindex_index(
            stmt,
            indexOid,
            (flags & REINDEX_REL_CHECK_CONSTRAINTS) == 0,
            persistence,
            params,
        );

        CommandCounterIncrement();

        /* Index should no longer be in the pending list */
        Assert!(!_reindex_is_processing(indexOid));

        /* Set index rebuild count */
        pgstat_progress_update_param(
            PROGRESS_CLUSTER_INDEX_REBUILD_COUNT,
            i as i64,
        );
        i += 1;
        indexId_cell = lnext(indexIds, indexId_cell);
    }

    /*
     * Close rel, but continue to hold the lock.
     */
    table_close(rel, NoLock);

    result |= !indexIds.is_null() && list_length(indexIds) > 0;

    result
}


/* ----------------------------------------------------------------
 *        System index reindexing support
 *
 * When we are busy reindexing a system index, this code provides support
 * for preventing catalog lookups from using that index.  We also make use
 * of this to catch attempted uses of user indexes during reindexing of
 * those indexes.  This information is propagated to parallel workers;
 * attempting to change it during a parallel operation is not permitted.
 * ----------------------------------------------------------------
 */

static mut currentlyReindexedHeap: Oid = InvalidOid;
static mut currentlyReindexedIndex: Oid = InvalidOid;
static mut pendingReindexedIndexes: *mut List = core::ptr::null_mut();
static mut reindexingNestLevel: c_int = 0;

/*
 * ReindexIsProcessingHeap
 *        True if heap specified by OID is currently being reindexed.
 */
pub unsafe fn ReindexIsProcessingHeap(heapOid: Oid) -> bool {
    heapOid == currentlyReindexedHeap
}

/*
 * ReindexIsCurrentlyProcessingIndex
 *        True if index specified by OID is currently being reindexed.
 */
unsafe fn ReindexIsCurrentlyProcessingIndex(indexOid: Oid) -> bool {
    indexOid == currentlyReindexedIndex
}

/*
 * ReindexIsProcessingIndex
 *        True if index specified by OID is currently being reindexed,
 *        or should be treated as invalid because it is awaiting reindex.
 */
pub unsafe fn ReindexIsProcessingIndex(indexOid: Oid) -> bool {
    indexOid == currentlyReindexedIndex
        || list_member_oid(pendingReindexedIndexes, indexOid)
}

fn _reindex_is_processing(indexOid: Oid) -> bool {
    unsafe { ReindexIsProcessingIndex(indexOid) }
}

/*
 * SetReindexProcessing
 *        Set flag that specified heap/index are being reindexed.
 */
unsafe fn SetReindexProcessing(heapOid: Oid, indexOid: Oid) {
    Assert!(OidIsValid(heapOid) && OidIsValid(indexOid));
    /* Reindexing is not re-entrant. */
    if OidIsValid(currentlyReindexedHeap) {
        elog!(ERROR, "cannot reindex while reindexing");
    }
    currentlyReindexedHeap = heapOid;
    currentlyReindexedIndex = indexOid;
    /* Index is no longer "pending" reindex. */
    RemoveReindexPending(indexOid);
    /* This may have been set already, but in case it isn't, do so now. */
    reindexingNestLevel = GetCurrentTransactionNestLevel();
}

/*
 * ResetReindexProcessing
 *        Unset reindexing status.
 */
unsafe fn ResetReindexProcessing() {
    currentlyReindexedHeap = InvalidOid;
    currentlyReindexedIndex = InvalidOid;
    /* reindexingNestLevel remains set till end of (sub)transaction */
}

/*
 * SetReindexPending
 *        Mark the given indexes as pending reindex.
 *
 * NB: we assume that the current memory context stays valid throughout.
 */
unsafe fn SetReindexPending(indexes: *mut List) {
    /* Reindexing is not re-entrant. */
    if !pendingReindexedIndexes.is_null() {
        elog!(ERROR, "cannot reindex while reindexing");
    }
    if IsInParallelMode() {
        elog!(
            ERROR,
            "cannot modify reindex state during a parallel operation"
        );
    }
    pendingReindexedIndexes = list_copy(indexes);
    reindexingNestLevel = GetCurrentTransactionNestLevel();
}

/*
 * RemoveReindexPending
 *        Remove the given index from the pending list.
 */
unsafe fn RemoveReindexPending(indexOid: Oid) {
    if IsInParallelMode() {
        elog!(
            ERROR,
            "cannot modify reindex state during a parallel operation"
        );
    }
    pendingReindexedIndexes = list_delete_oid(pendingReindexedIndexes, indexOid);
}

/*
 * ResetReindexState
 *        Clear all reindexing state during (sub)transaction abort.
 */
pub unsafe fn ResetReindexState(nestLevel: c_int) {
    /*
     * Because reindexing is not re-entrant, we don't need to cope with nested
     * reindexing states.
     */
    if reindexingNestLevel >= nestLevel {
        currentlyReindexedHeap = InvalidOid;
        currentlyReindexedIndex = InvalidOid;

        /*
         * We needn't try to release the contents of pendingReindexedIndexes;
         * that list should be in a transaction-lifespan context, so it will
         * go away automatically.
         */
        pendingReindexedIndexes = NIL;

        reindexingNestLevel = 0;
    }
}

/*
 * EstimateReindexStateSpace
 *        Estimate space needed to pass reindex state to parallel workers.
 */
pub unsafe fn EstimateReindexStateSpace() -> usize {
    core::mem::offset_of!(SerializedReindexState, currentlyReindexedHeap)
        + core::mem::size_of::<Oid>()
        + core::mem::size_of::<Oid>()
        + core::mem::size_of::<c_int>()
        + core::mem::size_of::<Oid>() * list_length(pendingReindexedIndexes) as usize
}

/*
 * SerializeReindexState
 *        Serialize reindex state for parallel workers.
 */
pub unsafe fn SerializeReindexState(maxsize: usize, start_address: *mut c_char) {
    let sistate: *mut SerializedReindexState =
        start_address as *mut SerializedReindexState;
    let mut c: c_int = 0;

    (*sistate).currentlyReindexedHeap = currentlyReindexedHeap;
    (*sistate).currentlyReindexedIndex = currentlyReindexedIndex;
    (*sistate).numPendingReindexedIndexes = list_length(pendingReindexedIndexes);

    /* pendingReindexedIndexes[c] follow the struct in memory */
    let pending_ptr: *mut Oid =
        (sistate as *mut u8).add(core::mem::size_of::<SerializedReindexState>()) as *mut Oid;
    let mut lc: *mut ListCell = list_head(pendingReindexedIndexes);
    while !lc.is_null() {
        *pending_ptr.add(c as usize) = lfirst_oid(lc);
        c += 1;
        lc = lnext(pendingReindexedIndexes, lc);
    }
}

/*
 * RestoreReindexState
 *        Restore reindex state in a parallel worker.
 */
pub unsafe fn RestoreReindexState(reindexstate: *const c_void) {
    let sistate: *const SerializedReindexState =
        reindexstate as *const SerializedReindexState;
    let mut c: c_int = 0;

    currentlyReindexedHeap = (*sistate).currentlyReindexedHeap;
    currentlyReindexedIndex = (*sistate).currentlyReindexedIndex;

    Assert!(pendingReindexedIndexes.is_null());

    use crate::utils::mmgr::mcxt::TopMemoryContext;
    let oldcontext: *mut c_void = MemoryContextSwitchTo(TopMemoryContext as *mut c_void);
    let pending_ptr: *const Oid = (sistate as *const u8)
        .add(core::mem::size_of::<SerializedReindexState>()) as *const Oid;
    for idx in 0..(*sistate).numPendingReindexedIndexes {
        pendingReindexedIndexes = lappend_oid(
            pendingReindexedIndexes,
            *pending_ptr.add(idx as usize),
        );
    }
    MemoryContextSwitchTo(oldcontext);

    /* Note the worker has its own transaction nesting level */
    reindexingNestLevel = GetCurrentTransactionNestLevel();
}

/* helpers for list_delete_oid and MemoryContextSwitchTo */
unsafe fn list_delete_oid(list: *mut List, oid: Oid) -> *mut List {
    list /* TODO(pg-port) */
}
unsafe fn MemoryContextSwitchTo(ctxt: *mut c_void) -> *mut c_void {
    ctxt /* TODO(pg-port) */
}
type Snapshot = *mut c_void;
type ItemPointer = *mut c_void; /* real type: crate::storage::itemptr::ItemPointerData */
/* DEBUG2 / INFO / WARNING / NOTICE - use as elog level ints */
const DEBUG1: c_int  = 15;
const DEBUG2: c_int  = 14;
const INFO: c_int    = 17;
const WARNING: c_int = 19;
const NOTICE: c_int  = 18;
