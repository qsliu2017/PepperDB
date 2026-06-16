/*-------------------------------------------------------------------------
 *
 * tablecmds.c
 *    Commands for creating and altering table structures and settings
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/commands/tablecmds.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(
    non_camel_case_types,
    non_snake_case,
    unused_variables,
    dead_code,
    unused_imports,
    clippy::all
)]

use std::ffi::{c_char, c_int, c_uint, CStr};
use std::ptr;

use crate::postgres_ext::Oid;
use crate::postgres::{Datum, InvalidOid};
use crate::nodes::parsenodes::{
    AlterTableStmt, AlterTableCmd, AlterTableType, AlterDomainStmt, CreateStmt, DropStmt,
    RangeVar, ColumnDef, Constraint, ConstrType, DropBehavior, ObjectType, IndexStmt,
    CreateStatsStmt, PartitionCmd, PartitionSpec, PartitionBoundSpec, PartitionStrategy,
    ReplicaIdentityStmt, RenameStmt, AlterObjectSchemaStmt, TypeName, CommentStmt,
    AlterTableMoveAllStmt, TruncateStmt,
};
use crate::nodes::pg_list::{List, ListCell};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::ResultRelInfo;
use crate::nodes::execnodes::{EState, ExprState, ExprContext, TupleTableSlot};
use crate::nodes::value::ATAlterConstraint;
use crate::catalog::objectaccess::{ObjectAddress, ObjectAddresses};
use crate::access::transam::SubTransactionId;
use crate::access::common::tupdesc::{TupleDesc, TupleConstr};
use crate::access::htup_details::HeapTupleData;
use crate::storage::lockdefs::LOCKMODE;
use crate::utils::rel::Relation;
use crate::utils::fmgr::Expr;

/* TODO(pg-port): stubs for types not yet in crate */
type HeapTuple = *mut HeapTupleData;
type AttrNumber = i16;
type AttrMap = crate::access::common::attmap::AttrMap;
type RelFileNumber = u32;
type BulkInsertState = *mut std::ffi::c_void;
type TableScanDesc = *mut std::ffi::c_void;
type IndexInfo = *mut std::ffi::c_void;
type ForeignKeyCacheInfo = *mut std::ffi::c_void;
type Snapshot = *mut std::ffi::c_void;
type ParseState = *mut std::ffi::c_void;
type ParseNamespaceItem = *mut std::ffi::c_void;
type Bitmapset = *mut std::ffi::c_void;
type AlterTableUtilityContext = crate::tcop::utility::AlterTableUtilityContext;

/* TODO(pg-port): catalog/scan stubs */
type SysScanDesc = *mut std::ffi::c_void;
#[repr(C)] struct ScanKeyData { _opaque: [u8; 48] }
use crate::catalog::pg_depend::FormData_pg_depend;
use crate::catalog::pg_class::FormData_pg_class;
use crate::catalog::pg_attribute::FormData_pg_attribute;
use crate::catalog::pg_type::FormData_pg_type;
use crate::catalog::pg_constraint::{
    FormData_pg_constraint,
    CONSTRAINT_CHECK, CONSTRAINT_NOTNULL,
    CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, CONSTRAINT_EXCLUSION, CONSTRAINT_FOREIGN,
};
use crate::catalog::heap::CookedConstraint;
use crate::c::NameData;

const AccessShareLock: LOCKMODE = 1;
const RowExclusiveLock: LOCKMODE = 3;
const NoLock: LOCKMODE = 0;
const ShareUpdateExclusiveLock: LOCKMODE = 4;
const ShareRowExclusiveLock: LOCKMODE = 6;
const AccessExclusiveLock: LOCKMODE = 8;
const RowShareLock: LOCKMODE = 2;
const BTEqualStrategyNumber: u16 = 3;
const F_OIDEQ: u32 = 184;
const RELKIND_RELATION: u8 = b'r';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_INDEX: u8 = b'i';
const RELKIND_VIEW: u8 = b'v';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_SEQUENCE: u8 = b'S';
const RELKIND_PARTITIONED_INDEX: u8 = b'I';
const TYPTYPE_COMPOSITE: u8 = b'c';
const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;
const DependRelationId: Oid = 2608;
const DependReferenceIndexId: Oid = 2457;
const TypeRelationId: Oid = 1247;
const RelationRelationId: Oid = 1259;
const InheritsRelationId: Oid = 2611;
const ConstraintRelationId: Oid = 2606;
const AttributeRelationId: Oid = 1249;
const Anum_pg_depend_refclassid: u16 = 5;
const Anum_pg_depend_refobjid: u16 = 6;
const Anum_pg_class_reloftype: u16 = 5;
const ERROR: i32 = 20;
const NOTICE: i32 = 18;
const ForwardScanDirection: i32 = 1;
const InvalidAttrNumber: AttrNumber = 0;
const InvalidOid: Oid = crate::postgres::InvalidOid;

/* ObjectAddress helpers */
const InvalidObjectAddress: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
macro_rules! ObjectAddressSet {
    ($addr:expr, $classId:expr, $objectId:expr) => {
        $addr.classId = $classId; $addr.objectId = $objectId; $addr.objectSubId = 0;
    }
}
macro_rules! ObjectAddressSubSet {
    ($addr:expr, $classId:expr, $objectId:expr, $subId:expr) => {
        $addr.classId = $classId; $addr.objectId = $objectId; $addr.objectSubId = $subId;
    }
}

unsafe fn RELKIND_HAS_STORAGE(k: c_char) -> bool {
    let k = k as u8;
    matches!(k, b'r' | b'm' | b'i' | b't')
}
unsafe fn RELKIND_HAS_PARTITIONS(k: c_char) -> bool { k as u8 == RELKIND_PARTITIONED_TABLE }
unsafe fn OidIsValid(oid: Oid) -> bool { oid != 0 }
unsafe fn HeapTupleIsValid(t: HeapTuple) -> bool { !t.is_null() }
unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum { oid as Datum }
unsafe fn RelationGetRelid(rel: Relation) -> Oid { (*(*rel).rd_rel).oid }
unsafe fn RelationGetRelationName(rel: Relation) -> *mut c_char {
    crate::utils::rel::RelationGetRelationName(rel)
}
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc { (*rel).rd_att }
unsafe fn TupleDescAttr(tupdesc: TupleDesc, n: usize) -> *mut FormData_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc, n as c_int)
}
unsafe fn NameStr_ref(n: &NameData) -> *const c_char { n.data.as_ptr() }
unsafe fn check_stack_depth() { /* TODO(pg-port): stub */ }
unsafe fn format_type_be(oid: Oid) -> *const c_char { ptr::null() /* TODO(pg-port): stub */ }
unsafe fn ScanKeyInit(entry: *mut ScanKeyData, attributeNumber: u16, strategy: u16, procedure: u32, argument: Datum) {
    /* TODO(pg-port): stub */
}
unsafe fn systable_beginscan(heapRelation: Relation, indexId: Oid, indexOk: bool, snapshot: Snapshot, nkeys: i32, key: *mut ScanKeyData) -> SysScanDesc { ptr::null_mut() }
unsafe fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple { ptr::null_mut() }
unsafe fn systable_endscan(sysscan: SysScanDesc) {}
unsafe fn table_beginscan_catalog(heapRelation: Relation, nkeys: i32, key: *mut ScanKeyData) -> TableScanDesc { ptr::null_mut() }
unsafe fn heap_getnext(scan: TableScanDesc, direction: i32) -> HeapTuple { ptr::null_mut() }
unsafe fn table_endscan(scan: TableScanDesc) {}
unsafe fn lappend_oid(list: *mut List, datum: Oid) -> *mut List { crate::nodes::pg_list::lappend_oid(list, datum) }
unsafe fn list_free(list: *mut List) { crate::nodes::pg_list::list_free(list) }
unsafe fn lfirst_oid(lc: *const ListCell) -> Oid { crate::nodes::pg_list::lfirst_oid(lc) }
unsafe fn relation_open(relationId: Oid, lockmode: LOCKMODE) -> Relation { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn relation_close(relation: Relation, lockmode: LOCKMODE) {}
unsafe fn find_all_inheritors(parentrelId: Oid, lockmode: LOCKMODE, numparents: *mut i32) -> *mut List { crate::nodes::pg_list::NIL }
unsafe fn find_inheritance_children(parentrelId: Oid, lockmode: LOCKMODE) -> *mut List { crate::nodes::pg_list::NIL }
unsafe fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn table_close(relation: Relation, lockmode: LOCKMODE) {}

/* TODO(pg-port): dependency stubs for functions called from translated bodies */
unsafe fn RangeVarGetRelidExtended(rv: *const RangeVar, lockmode: LOCKMODE, flags: u32, callback: unsafe fn(*const RangeVar, Oid, Oid, *mut std::ffi::c_void), callback_arg: *mut std::ffi::c_void) -> Oid { 0 }
unsafe fn AcceptInvalidationMessages() {}
unsafe fn new_object_addresses() -> *mut ObjectAddresses { ptr::null_mut() }
unsafe fn makeRangeVarFromNameList(names: *mut List) -> *mut RangeVar { ptr::null_mut() }
unsafe fn add_exact_object_address(obj: *const ObjectAddress, addrs: *mut ObjectAddresses) {}
unsafe fn performMultipleDeletions(addrs: *mut ObjectAddresses, behavior: DropBehavior, flags: c_int) {}
unsafe fn free_object_addresses(addrs: *mut ObjectAddresses) {}
unsafe fn UnlockRelationOid(relOid: Oid, lockmode: LOCKMODE) {}
unsafe fn LockRelationOid(relOid: Oid, lockmode: LOCKMODE) {}
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple { ptr::null_mut() }
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple { ptr::null_mut() }
unsafe fn SearchSysCacheLockedCopy1(cacheId: c_int, key1: Datum) -> HeapTuple { ptr::null_mut() }
unsafe fn ReleaseSysCache(tuple: HeapTuple) {}
unsafe fn GETSTRUCT(tuple: HeapTuple) -> *mut std::ffi::c_void { (*tuple).t_data as *mut std::ffi::c_void }
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *const std::ffi::c_void, tup: HeapTuple) {}
unsafe fn UnlockTuple(heapRel: Relation, tid: *const std::ffi::c_void, lockmode: LOCKMODE) {}
unsafe fn CacheInvalidateRelcacheByTuple(tuple: HeapTuple) {}
unsafe fn CacheInvalidateRelcache(rel: Relation) {}
unsafe fn heap_freetuple(tuple: HeapTuple) {}
unsafe fn RelationGetForm(rel: Relation) -> *mut FormData_pg_class { (*rel).rd_rel }
unsafe fn RelationGetNamespace(rel: Relation) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn RelationGetNotNullConstraints(relid: Oid, include_noinherit: bool, include_invalid: bool) -> *mut List { crate::nodes::pg_list::NIL }
unsafe fn RelationIsLogicallyLogged(rel: Relation) -> bool { false }
unsafe fn RELATION_IS_OTHER_TEMP(rel: Relation) -> bool { false }
unsafe fn IsSystemRelation(rel: Relation) -> bool { false }
unsafe fn IsSystemClass(relid: Oid, reltuple: *mut FormData_pg_class) -> bool { false }
unsafe fn IndexGetRelation(indexOid: Oid, missing_ok: bool) -> Oid { 0 }
unsafe fn get_partition_parent(relOid: Oid, missing_ok: bool) -> Oid { 0 }
unsafe fn PartitionHasPendingDetach(relid: Oid) -> bool { false }
unsafe fn object_ownercheck(classId: Oid, objectId: Oid, userId: Oid) -> bool { true }
unsafe fn aclcheck_error(result: c_int, objtype: c_int, objname: *const c_char) {}
unsafe fn get_relkind_objtype(relkind: c_char) -> c_int { 0 }
unsafe fn GetUserId() -> Oid { 0 }
unsafe fn allowSystemTableMods() -> bool { false /* TODO(pg-port): global var stub */ }
unsafe fn get_domain_constraint_oid(typid: Oid, conname: *const c_char, missing_ok: bool) -> Oid { 0 }
unsafe fn get_relation_constraint_oid(relid: Oid, conname: *const c_char, missing_ok: bool) -> Oid { 0 }
unsafe fn RenameTypeInternal(typeOid: Oid, newTypeName: *const c_char, namespaceId: Oid) {}
unsafe fn get_index_constraint(indexOid: Oid) -> Oid { 0 }
unsafe fn RenameConstraintById(constraintOid: Oid, newname: *const c_char) {}
unsafe fn RenameRelationInternal_catalog(myrelid: Oid, newrelname: *const c_char, is_internal: bool, is_index: bool) {} /* forward decl stub */
unsafe fn EventTriggerAlterTableRelid(relid: Oid) {}
unsafe fn AfterTriggerPendingOnRel(relid: Oid) -> bool { false }
unsafe fn AlterTableGetRelOptionsLockLevel(defList: *mut List) -> LOCKMODE { AccessExclusiveLock }
unsafe fn copyObject_cmd(cmd: *mut AlterTableCmd) -> *mut AlterTableCmd { cmd /* TODO(pg-port): stub */ }
unsafe fn ATPostAlterTypeCleanup(wqueue: *mut *mut List, tab: *mut AlteredTableInfo, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn AlterTableCreateToastTable(relid: Oid, reloptions: Datum, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ProcessUtilityForAlterTable(stmt: *mut Node, context: *mut AlterTableUtilityContext) { /* TODO(pg-port): stub */ }
unsafe fn EventTriggerCollectAlterTableSubcmd(cmd: *mut Node, address: ObjectAddress) { /* TODO(pg-port): stub */ }
unsafe fn CommandCounterIncrement() { /* TODO(pg-port): stub */ }
unsafe fn palloc0(size: usize) -> *mut std::ffi::c_void { /* TODO(pg-port): use palloc */ ptr::null_mut() }
unsafe fn CreateTupleDescCopyConstr(tupdesc: TupleDesc) -> TupleDesc { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn lappend(list: *mut List, datum: *mut std::ffi::c_void) -> *mut List { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn lappend_ptr(list: *mut List, datum: *mut std::ffi::c_void) -> *mut List { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn list_difference_ptr(list1: *mut List, list2: *mut List) -> *mut List { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn list_copy(list: *const List) -> *mut List { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn list_length(list: *const List) -> c_int { 0 /* TODO(pg-port): stub */ }
unsafe fn lfirst_node_AlterTableCmd(lc: *mut ListCell) -> *mut AlterTableCmd { (*lc).ptr_value as *mut AlterTableCmd }
unsafe fn check_for_column_name_collision(rel: Relation, colname: *const c_char, if_not_exists: bool) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn namestrcpy(name: *mut crate::c::NameData, str_: *const c_char) { /* TODO(pg-port): stub */ }
unsafe fn get_relname_relid(relname: *const c_char, namespaceId: Oid) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn typenameTypeId(pstate: ParseState, typeName: *mut TypeName) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn makeTypeNameFromNameList(names: *mut List) -> *mut TypeName { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn checkDomainOwner(tup: HeapTuple) { /* TODO(pg-port): stub */ }
unsafe fn get_rel_relkind(relid: Oid) -> c_char { b'r' as c_char /* TODO(pg-port): stub */ }
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) { /* TODO(pg-port): stub */ }
unsafe fn InvokeObjectPostAlterHookArg(classId: Oid, objectId: Oid, subId: c_int, auxiliaryId: Oid, is_internal: bool) { /* TODO(pg-port): stub */ }
unsafe fn InvokeObjectPostAlterHookArgArg(classId: Oid, objectId: Oid, subId: c_int, auxiliaryId: Oid, is_internal: bool) { /* TODO(pg-port): stub */ }
unsafe fn StoreSingleInheritance(relationId: Oid, parentOid: Oid, seqNumber: i32) { /* TODO(pg-port): stub */ }
unsafe fn recordDependencyOn(depender: *const ObjectAddress, referenced: *const ObjectAddress, deptype: c_int) { /* TODO(pg-port): stub */ }
unsafe fn InvokeObjectPostAlterHookArg2(classId: Oid, objectId: Oid, subId: c_int, auxiliaryId: Oid, is_internal: bool) { /* TODO(pg-port): stub */ }
unsafe fn changeDependencyOnTablespace(classId: Oid, objectId: Oid, newTableSpaceId: Oid) { /* TODO(pg-port): stub */ }
unsafe fn RelFileNumberIsValid(rfn: RelFileNumber) -> bool { rfn != 0 }
unsafe fn SearchSysCacheCopyAttName(relid: Oid, attname: *const c_char) -> HeapTuple { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ATPrepAlterColumnType(wqueue: *mut *mut List, tab: *mut AlteredTableInfo, rel: Relation, recurse: bool, recursing: bool, cmd: *mut AlterTableCmd, lockmode: LOCKMODE, context: *mut AlterTableUtilityContext) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepDropExpression(rel: Relation, cmd: *mut AlterTableCmd, recurse: bool, recursing: bool, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepDropColumn(wqueue: *mut *mut List, rel: Relation, recurse: bool, recursing: bool, cmd: *mut AlterTableCmd, lockmode: LOCKMODE, context: *mut AlterTableUtilityContext) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepAddPrimaryKey(wqueue: *mut *mut List, rel: Relation, cmd: *mut AlterTableCmd, recurse: bool, lockmode: LOCKMODE, context: *mut AlterTableUtilityContext) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepSetAccessMethod(tab: *mut AlteredTableInfo, rel: Relation, amname: *const c_char) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepChangePersistence(tab: *mut AlteredTableInfo, rel: Relation, toLogged: bool) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepSetTableSpace(tab: *mut AlteredTableInfo, rel: Relation, tablespacename: *const c_char, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATPrepAddInherit(child_rel: Relation) { /* TODO(pg-port): stub */ }
unsafe fn transformAlterTableStmt(relid: Oid, stmt: *mut AlterTableStmt, queryString: *const c_char, beforeStmts: *mut *mut List, afterStmts: *mut *mut List) -> *mut AlterTableStmt { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn make_new_heap(tab_relid: Oid, newTableSpace: Oid, accessMethod: Oid, persistence: c_char, lockmode: LOCKMODE) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn finish_heap_swap(OIDOldHeap: Oid, OIDNewHeap: Oid, is_system_catalog: bool, swap_toast_by_content: bool, check_constraints: bool, is_internal: bool, frozenXid: u32, cutoffMulti: u32, newrelpersistence: c_char) { /* TODO(pg-port): stub */ }
unsafe fn InvokeObjectPostAlterHook_rel(classId: Oid, relid: Oid, subId: c_int) { /* TODO(pg-port): stub */ }
unsafe fn EventTriggerTableRewrite(parsetree: *mut Node, tableOid: Oid, reason: c_int) { /* TODO(pg-port): stub */ }
unsafe fn SequenceChangePersistence(relid: Oid, newrelpersistence: c_char) { /* TODO(pg-port): stub */ }
unsafe fn ATExecSetTableSpace(tableOid: Oid, newTableSpace: Oid, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn getOwnedSequences(relid: Oid) -> *mut List { crate::nodes::pg_list::NIL /* TODO(pg-port): stub */ }
unsafe fn validateForeignKeyConstraint(conname: *const c_char, rel: Relation, pkrel: Relation, pkindOid: Oid, constraintOid: Oid, hasperiod: bool) { /* TODO(pg-port): stub */ }
unsafe fn RELKIND_HAS_TABLE_AM(k: c_char) -> bool { let k = k as u8; matches!(k, b'r' | b'm') }
unsafe fn lfirst_int(lc: *const ListCell) -> c_int { 0 /* TODO(pg-port): stub */ }
unsafe fn list_member_oid(list: *mut List, datum: Oid) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn RangeVarCallbackForTruncate(rel: *const RangeVar, relId: Oid, oldRelId: Oid, arg: *mut std::ffi::c_void) { /* TODO(pg-port): stub */ }
unsafe fn heap_truncate_find_FKs(relids: *mut List) -> *mut List { crate::nodes::pg_list::NIL /* TODO(pg-port): stub */ }
unsafe fn heap_truncate_check_FKs(rels: *mut List, tempTables: bool) { /* TODO(pg-port): stub */ }
unsafe fn AfterTriggerBeginQuery() { /* TODO(pg-port): stub */ }
unsafe fn AfterTriggerEndQuery(estate: *mut std::ffi::c_void /* EState */) { /* TODO(pg-port): stub */ }
unsafe fn CreateExecutorState() -> *mut std::ffi::c_void /* EState */ { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn FreeExecutorState(estate: *mut std::ffi::c_void /* EState */) { /* TODO(pg-port): stub */ }
unsafe fn palloc(size: usize) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn InitResultRelInfo(resultRelInfo: *mut ResultRelInfo, resultRelationDesc: Relation, resultRelationIndex: c_int, instrument: *mut std::ffi::c_void, instrument_options: c_int) { /* TODO(pg-port): stub */ }
unsafe fn ExecBSTruncateTriggers(estate: *mut std::ffi::c_void, resultRelInfo: *mut ResultRelInfo) { /* TODO(pg-port): stub */ }
unsafe fn ExecASTruncateTriggers(estate: *mut std::ffi::c_void, resultRelInfo: *mut ResultRelInfo) { /* TODO(pg-port): stub */ }
unsafe fn GetCurrentSubTransactionId() -> crate::access::transam::SubTransactionId { 0 }
unsafe fn heap_truncate_one_rel(rel: Relation) { /* TODO(pg-port): stub */ }
unsafe fn CheckTableForSerializableConflictIn(rel: Relation) { /* TODO(pg-port): stub */ }
unsafe fn RelationSetNewRelfilenumber(rel: Relation, persistence: c_char) { /* TODO(pg-port): stub */ }
unsafe fn reindex_relation(progress: *mut std::ffi::c_void, relid: Oid, flags: c_int, params: *mut std::ffi::c_void) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn pgstat_count_truncate(rel: Relation) { /* TODO(pg-port): stub */ }
unsafe fn GetForeignServerIdByRelId(relid: Oid) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn GetFdwRoutineByServerId(serverid: Oid) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ResetSequence(seq_relid: Oid) { /* TODO(pg-port): stub */ }
unsafe fn GetBulkInsertState() -> BulkInsertState { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn FreeBulkInsertState(bistate: BulkInsertState) { /* TODO(pg-port): stub */ }
unsafe fn object_aclcheck(classId: Oid, objectId: Oid, userId: Oid, mode: u32) -> c_int { 0 /* TODO(pg-port): stub */ }
unsafe fn pg_class_aclcheck(relid: Oid, userId: Oid, mode: u32) -> c_int { 0 /* TODO(pg-port): stub */ }
unsafe fn InvokeObjectTruncateHook(relid: Oid) { /* TODO(pg-port): stub */ }
unsafe fn getOwnedSequences_withOwner(relid: Oid) -> *mut List { crate::nodes::pg_list::NIL /* TODO(pg-port): stub */ }
unsafe fn relation_open_nolock(relid: Oid) -> Relation { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn get_relation_constraint_oid_locked(relid: Oid, conname: *const c_char, missing_ok: bool) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn CheckRelationOidLockedByMe(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool { true /* TODO(pg-port): stub */ }
unsafe fn lappend_int(list: *mut List, datum: c_int) -> *mut List { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn SwitchToUntrustedUser(userId: Oid, ucxt: *mut std::ffi::c_void) { /* TODO(pg-port): stub */ }
unsafe fn RestoreUserContext(ucxt: *mut std::ffi::c_void) { /* TODO(pg-port): stub */ }
unsafe fn RelationIsMapped(rel: Relation) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn MyDatabaseTableSpace_get() -> Oid { 0 /* TODO(pg-port): global var stub */ }
unsafe fn ATExecColumnDefault(rel: Relation, colName: *const c_char, newDefault: *mut Node, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecCookedColumnDefault(rel: Relation, attnum: AttrNumber, newDefault: *mut Node) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAddIdentity(rel: Relation, colName: *const c_char, def: *mut Node, lockmode: LOCKMODE, recurse: bool, recursing: bool) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetIdentity(rel: Relation, colName: *const c_char, def: *mut Node, lockmode: LOCKMODE, recurse: bool, recursing: bool) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropIdentity(rel: Relation, colName: *const c_char, missing_ok: bool, lockmode: LOCKMODE, recurse: bool, recursing: bool) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropNotNull(rel: Relation, colName: *const c_char, recurse: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetNotNull(wqueue: *mut *mut List, rel: Relation, constrname: *const c_char, colName: *const c_char, recurse: bool, recursing: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetExpression(tab: *mut AlteredTableInfo, rel: Relation, colName: *const c_char, newExpr: *mut Node, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropExpression(rel: Relation, colName: *const c_char, missing_ok: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetStatistics(rel: Relation, colName: *const c_char, colNum: AttrNumber, newValue: *mut Node, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetOptions(rel: Relation, colName: *const c_char, options: *mut Node, isReset: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetStorage(rel: Relation, colName: *const c_char, newValue: *mut Node, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecSetCompression(rel: Relation, colName: *const c_char, newValue: *mut Node, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropColumn(wqueue: *mut *mut List, rel: Relation, colName: *const c_char, behavior: DropBehavior, recurse: bool, recursing: bool, missing_ok: bool, lockmode: LOCKMODE, dropped_column_name: *mut *mut c_char) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAddIndex(tab: *mut AlteredTableInfo, rel: Relation, stmt: *mut IndexStmt, is_rebuild: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAddStatistics(tab: *mut AlteredTableInfo, rel: Relation, stmt: *mut CreateStatsStmt, is_rebuild: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAddConstraint(wqueue: *mut *mut List, tab: *mut AlteredTableInfo, rel: Relation, newConstraint: *mut Constraint, recurse: bool, is_readd: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn AlterDomainAddConstraint(typeName: *mut List, newConstraint: *mut Node, node: *mut std::ffi::c_void) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn CommentObject(stmt: *mut CommentStmt) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAddIndexConstraint(tab: *mut AlteredTableInfo, rel: Relation, stmt: *mut IndexStmt, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAlterConstraint(wqueue: *mut *mut List, rel: Relation, con: *mut ATAlterConstraint, recurse: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecValidateConstraint(wqueue: *mut *mut List, rel: Relation, constrName: *const c_char, recurse: bool, recursing: bool, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropConstraint(rel: Relation, constrName: *const c_char, behavior: DropBehavior, recurse: bool, missing_ok: bool, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecAlterColumnType(tab: *mut AlteredTableInfo, rel: Relation, cmd: *mut AlterTableCmd, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAlterColumnGenericOptions(rel: Relation, colName: *const c_char, options: *mut List, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecChangeOwner(relationOid: Oid, newOwnerId: Oid, recursing: bool, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn get_rolespec_oid(spec: *mut Node, missing_ok: bool) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn ATExecClusterOn(rel: Relation, indexName: *const c_char, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropCluster(rel: Relation, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecSetAccessMethodNoStorage(rel: Relation, newAccessMethod: Oid) { /* TODO(pg-port): stub */ }
unsafe fn ATExecSetTableSpaceNoStorage(rel: Relation, newTableSpace: Oid) { /* TODO(pg-port): stub */ }
unsafe fn ATExecSetRelOptions(rel: Relation, defList: *mut List, operation: AlterTableType, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecEnableDisableTrigger(rel: Relation, trigname: *const c_char, fires_when: c_char, skip_system: bool, recurse: bool, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecEnableDisableRule(rel: Relation, rulename: *const c_char, fires_when: c_char, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecAddInherit(rel: Relation, parent: *mut RangeVar, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropInherit(rel: Relation, parent: *mut RangeVar, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAddOf(rel: Relation, ofTypename: *mut TypeName, lockmode: LOCKMODE) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDropOf(rel: Relation, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecReplicaIdentity(rel: Relation, stmt: *mut ReplicaIdentityStmt, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn ATExecSetRowSecurity(rel: Relation, enabled: bool) { /* TODO(pg-port): stub */ }
unsafe fn ATExecForceNoForceRowSecurity(rel: Relation, setsecforce: bool) { /* TODO(pg-port): stub */ }
unsafe fn ATExecGenericOptions(rel: Relation, options: *mut List) { /* TODO(pg-port): stub */ }
unsafe fn ATExecAttachPartition(wqueue: *mut *mut List, rel: Relation, cmd: *mut PartitionCmd, context: *mut AlterTableUtilityContext) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecAttachPartitionIdx(wqueue: *mut *mut List, rel: Relation, name: *mut RangeVar) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDetachPartition(wqueue: *mut *mut List, tab: *mut AlteredTableInfo, rel: Relation, name: *mut RangeVar, concurrent: bool) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn ATExecDetachPartitionFinalize(rel: Relation, name: *mut RangeVar) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn expand_generated_columns_in_expr(expr: *mut Node, rel: Relation, rt_index: c_int) -> *mut Node { expr /* TODO(pg-port): stub */ }
unsafe fn ExecPrepareExpr(expr: *mut Expr, estate: *mut std::ffi::c_void) -> *mut ExprState { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ExecInitExpr(node: *mut Expr, parent: *mut std::ffi::c_void) -> *mut ExprState { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn TupleDescCompactAttr(tupdesc: TupleDesc, i: usize) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn TransferPredicateLocksToHeapRelation(rel: Relation) { /* TODO(pg-port): stub */ }
unsafe fn GetPerTupleExprContext(estate: *mut std::ffi::c_void) -> *mut ExprContext { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn GetPerTupleMemoryContext(estate: *mut std::ffi::c_void) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn MakeSingleTupleTableSlot(tupdesc: TupleDesc, callbacks: *const std::ffi::c_void) -> *mut TupleTableSlot { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn table_slot_callbacks(rel: Relation) -> *const std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ExecStoreAllNullTuple(slot: *mut TupleTableSlot) { /* TODO(pg-port): stub */ }
unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn GetLatestSnapshot() -> Snapshot { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn table_beginscan(rel: Relation, snapshot: Snapshot, nkeys: c_int, key: *mut std::ffi::c_void) -> TableScanDesc { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn MemoryContextSwitchTo(cxt: *mut std::ffi::c_void) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn table_scan_getnextslot(sscan: TableScanDesc, direction: i32, slot: *mut TupleTableSlot) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn slot_getallattrs(slot: *mut TupleTableSlot) { /* TODO(pg-port): stub */ }
unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ExecStoreVirtualTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn slot_attisnull(slot: *mut TupleTableSlot, attnum: c_int) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn ExecEvalExpr(expr: *mut ExprState, econtext: *mut ExprContext, isnull: *mut bool) -> Datum { 0 /* TODO(pg-port): stub */ }
unsafe fn ExecCheck(expr: *mut ExprState, econtext: *mut ExprContext) -> bool { true /* TODO(pg-port): stub */ }
unsafe fn ExecRelGenVirtualNotNull(rInfo: *mut ResultRelInfo, slot: *mut TupleTableSlot, estate: *mut std::ffi::c_void, notnull_virtual_attrs: *mut List) -> AttrNumber { InvalidAttrNumber /* TODO(pg-port): stub */ }
unsafe fn table_tuple_insert(rel: Relation, slot: *mut TupleTableSlot, cid: u32, options: c_int, bistate: BulkInsertState) { /* TODO(pg-port): stub */ }
unsafe fn GetCurrentCommandId(used: bool) -> u32 { 0 /* TODO(pg-port): stub */ }
unsafe fn ResetExprContext(econtext: *mut ExprContext) { /* TODO(pg-port): stub */ }
unsafe fn CHECK_FOR_INTERRUPTS() { /* TODO(pg-port): stub */ }
unsafe fn UnregisterSnapshot(snapshot: Snapshot) { /* TODO(pg-port): stub */ }
unsafe fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot) { /* TODO(pg-port): stub */ }
unsafe fn table_finish_bulk_insert(rel: Relation, options: c_int) { /* TODO(pg-port): stub */ }
unsafe fn RecentXmin() -> u32 { 0 /* TODO(pg-port): global var stub */ }
unsafe fn ReadNextMultiXactId() -> u32 { 0 /* TODO(pg-port): stub */ }
unsafe fn RelationIsUsedAsCatalogTable(rel: Relation) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn list_concat(list1: *mut List, list2: *mut List) -> *mut List { list1 /* TODO(pg-port): stub */ }
unsafe fn makeRangeVar(schemaname: *const c_char, relname: *const c_char, location: c_int) -> *mut RangeVar { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn pstrdup(str_: *const c_char) -> *mut c_char { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn list_make1(x: *mut std::ffi::c_void) -> *mut List { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn equal(a: *mut Node, b: *mut Node) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn list_nth_node_ColumnDef(list: *mut List, n: c_int) -> *mut ColumnDef { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn typenameTypeIdAndMod(pstate: ParseState, typeName: *const TypeName, typeOid: *mut Oid, typeMod: *mut i32) { /* TODO(pg-port): stub */ }
unsafe fn GetColumnDefCollation(pstate: ParseState, coldef: *const ColumnDef, typeOid: Oid) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn format_type_with_typemod(typeOid: Oid, typemod: i32) -> *mut c_char { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn get_collation_name(collOid: Oid) -> *mut c_char { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn aclcheck_error_type(result: c_int, typeOid: Oid) { /* TODO(pg-port): stub */ }
unsafe fn CreateTemplateTupleDesc(natts: c_int) -> TupleDesc { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn TupleDescInitEntry(desc: TupleDesc, attributeNumber: AttrNumber, attname: *const c_char, oidtypeid: Oid, typmod: i32, attdim: c_int) { /* TODO(pg-port): stub */ }
unsafe fn TupleDescInitEntryCollation(desc: TupleDesc, attributeNumber: AttrNumber, collationid: Oid) { /* TODO(pg-port): stub */ }
unsafe fn GetAttributeCompression(atttypid: Oid, compression: *const c_char) -> c_char { 0 /* TODO(pg-port): stub */ }
unsafe fn GetAttributeStorage(atttypid: Oid, storage_name: *const c_char) -> c_char { 0 /* TODO(pg-port): stub */ }
unsafe fn populate_compact_attribute(desc: TupleDesc, attnum: usize) { /* TODO(pg-port): stub */ }
unsafe fn ACL_USAGE() -> u32 { 0x0008 /* TODO(pg-port): stub */ }
unsafe fn ACL_CREATE() -> u32 { 0x0004 /* TODO(pg-port): stub */ }
/* DefineRelation and MergeAttributes dependency stubs */
unsafe fn RangeVarGetAndCheckCreationNamespace(relation: *mut RangeVar, lockmode: LOCKMODE, existing_relid: *mut Oid) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn InSecurityRestrictedOperation() -> bool { false /* TODO(pg-port): stub */ }
unsafe fn get_tablespace_oid(tablespacename: *const c_char, missing_ok: bool) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn get_rel_tablespace(relid: Oid) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn linitial_oid(list: *const List) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn GetDefaultTablespace(relpersistence: c_char, partitioned: bool) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn get_tablespace_name(tablespaceOid: Oid) -> *const c_char { ptr::null() /* TODO(pg-port): stub */ }
unsafe fn transformRelOptions(oldOptions: Datum, defList: *mut List, namspace: *const c_char, validnsps: *const *const c_char, ignoreOids: bool, isReset: bool) -> Datum { 0 /* TODO(pg-port): stub */ }
unsafe fn view_reloptions(reloptions: Datum, validate: bool) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn partitioned_table_reloptions(reloptions: Datum, validate: bool) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn heap_reloptions(relkind: c_char, reloptions: Datum, validate: bool) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn RangeVarGetRelid(relation: *const RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn get_rel_name(relid: Oid) -> *const c_char { ptr::null() /* TODO(pg-port): stub */ }
unsafe fn get_table_am_oid(amname: *const c_char, missing_ok: bool) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn get_rel_relam(relid: Oid) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn default_table_access_method() -> *const c_char { b"heap\0".as_ptr() as *const c_char /* TODO(pg-port): global var stub */ }
unsafe fn heap_create_with_catalog(relname: *const c_char, relnamespace: Oid, reltablespace: Oid, relid: Oid, reltypeid: Oid, reloftypeid: Oid, ownerid: Oid, accessmtd: Oid, tupdesc: TupleDesc, cooked_constraints: *mut List, relkind: c_char, relpersistence: c_char, shared_relation: bool, mapped_relation: bool, oncommit: OnCommitAction, reloptions: Datum, use_user_acl: bool, allow_system_table_mods: bool, is_internal: bool, relrewrite: Oid, typaddress: *mut ObjectAddress) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn AddRelationNewConstraints(rel: Relation, newColDefaults: *mut List, newConstraints: *mut List, allow_merge: bool, is_local: bool, is_internal: bool, queryString: *const c_char) -> *mut List { crate::nodes::pg_list::NIL /* TODO(pg-port): stub */ }
unsafe fn AddRelationNotNullConstraints(rel: Relation, nnconstraints: *mut List, old_notnulls: *mut List, connames: *mut List) -> *mut List { crate::nodes::pg_list::NIL /* TODO(pg-port): stub */ }
unsafe fn StorePartitionKey(rel: Relation, strategy: c_char, partnatts: c_int, partattrs: *const i16, partexprs: *mut List, partopclass: *const Oid, partcollation: *const Oid) { /* TODO(pg-port): stub */ }
unsafe fn StorePartitionBound(rel: Relation, parent: Relation, bound: *mut PartitionBoundSpec) { /* TODO(pg-port): stub */ }
unsafe fn set_attnotnull(wqueue: *mut *mut List, rel: Relation, attnum: c_int, recurse: bool, recursing: bool) { /* TODO(pg-port): stub */ }
unsafe fn make_parsestate(parentParseState: ParseState) -> ParseState { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn transformPartitionBound(pstate: ParseState, parent: Relation, spec: *mut PartitionBoundSpec) -> *mut PartitionBoundSpec { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn check_new_partition_bound(relname: *const c_char, parent: Relation, bound: *mut PartitionBoundSpec, pstate: ParseState) { /* TODO(pg-port): stub */ }
unsafe fn check_default_partition_contents(parent: Relation, defaultRel: Relation, bound: *mut PartitionBoundSpec) { /* TODO(pg-port): stub */ }
unsafe fn get_default_oid_from_partdesc(partdesc: *mut std::ffi::c_void) -> Oid { 0 /* TODO(pg-port): stub */ }
unsafe fn RelationGetPartitionDesc(rel: Relation, include_detached: bool) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn addRangeTableEntryForRelation(pstate: ParseState, rel: Relation, lockmode: LOCKMODE, alias: *mut std::ffi::c_void, inh: bool, inFromCl: bool) -> ParseNamespaceItem { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn addNSItemToQuery(pstate: ParseState, nsitem: ParseNamespaceItem, addToJoinList: bool, addToRelNameSpace: bool, addToVarNameSpace: bool) { /* TODO(pg-port): stub */ }
unsafe fn transformPartitionSpec(rel: Relation, partspec: *mut PartitionSpec) -> *mut PartitionSpec { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ComputePartitionAttrs(pstate: ParseState, rel: Relation, partParams: *mut List, partattrs: *mut i16, partexprs: *mut *mut List, partopclass: *mut Oid, partcollation: *mut Oid, strategy: c_char) { /* TODO(pg-port): stub */ }
unsafe fn RelationGetIndexList(rel: Relation) -> *mut List { crate::nodes::pg_list::NIL /* TODO(pg-port): stub */ }
unsafe fn build_attrmap_by_name(indesc: TupleDesc, outdesc: TupleDesc, missing_ok: bool) -> *mut AttrMap { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn generateClonedIndexStmt(heapRel: *mut RangeVar, source_idx: Relation, attmap: *mut AttrMap, constraintOid: *mut Oid) -> *mut IndexStmt { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn DefineIndex(relationId: Oid, stmt: *mut IndexStmt, indexRelationId: Oid, parentIndexId: Oid, parentConstraintId: Oid, total_parts: c_int, is_alter_table: bool, check_rights: bool, check_not_in_use: bool, skip_build: bool, quiet: bool) -> ObjectAddress { InvalidObjectAddress /* TODO(pg-port): stub */ }
unsafe fn index_open(indexOid: Oid, lockmode: LOCKMODE) -> Relation { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn index_close(rel: Relation, lockmode: LOCKMODE) { /* TODO(pg-port): stub */ }
unsafe fn CloneRowTriggersToPartition(parent: Relation, partition: Relation) { /* TODO(pg-port): stub */ }
unsafe fn CloneForeignKeyConstraints(wqueue: *mut *mut List, parent: Relation, partition: Relation) { /* TODO(pg-port): stub */ }
/* MergeAttributes dependency stubs */
unsafe fn make_attrmap(maplen: c_int) -> *mut AttrMap { crate::access::common::attmap::make_attrmap(maplen) }
unsafe fn free_attrmap(map: *mut AttrMap) { crate::access::common::attmap::free_attrmap(map) }
unsafe fn bms_add_member(a: Bitmapset, x: c_int) -> Bitmapset { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn bms_is_member(x: c_int, a: Bitmapset) -> bool { false /* TODO(pg-port): stub */ }
unsafe fn bms_free(a: Bitmapset) { /* TODO(pg-port): stub */ }
unsafe fn makeColumnDef(colname: *const c_char, typeOid: Oid, typmod: i32, collOid: Oid) -> *mut ColumnDef { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn CompressionMethodIsValid(method: c_char) -> bool { method != 0 /* TODO(pg-port): stub */ }
unsafe fn GetCompressionMethodName(method: c_char) -> *const c_char { ptr::null() /* TODO(pg-port): stub */ }
unsafe fn TupleDescGetDefault(tupdesc: TupleDesc, attnum: i16) -> *mut Node { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn map_variable_attnos(expr: *mut Node, varno: c_int, sublevels_up: c_int, map: *mut AttrMap, rowtype: Oid, found_whole_row: *mut bool) -> *mut Node { expr /* TODO(pg-port): stub */ }
unsafe fn stringToNode(str_: *const c_char) -> *mut Node { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn list_delete_nth_cell(list: *mut List, n: c_int) -> *mut List { list /* TODO(pg-port): stub */ }
unsafe fn list_nth(list: *const List, n: c_int) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn linitial(list: *const List) -> *mut std::ffi::c_void { ptr::null_mut() /* TODO(pg-port): stub */ }
unsafe fn ACLCHECK_NOT_OWNER() -> c_int { 2 /* TODO(pg-port): stub */ }
unsafe fn OBJECT_TABLESPACE() -> c_int { 0 /* TODO(pg-port): stub */ }
unsafe fn MyDatabaseTableSpace() -> Oid { 0 /* TODO(pg-port): global var stub */ }
unsafe fn TableSpaceRelationId_const() -> Oid { 1213 /* TODO(pg-port): stub */ }
unsafe fn ONCOMMIT_NOOP() -> c_int { 0 /* TODO(pg-port): stub */ }
const MaxHeapAttributeNumber: c_int = 1600;
const PARTITION_MAX_KEYS: c_int = 64;
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let sa = CStr::from_ptr(a);
    let sb = CStr::from_ptr(b);
    match sa.cmp(sb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/* Local copy of RawColumnDefault for DefineRelation */
#[repr(C)]
struct RawColumnDefault {
    attnum: AttrNumber,
    raw_default: *mut Node,
    generated: c_char,
}

/* ----- file-level types ----- */

/*
 * ON COMMIT action list
 */
#[repr(C)]
pub struct OnCommitItem {
    pub relid: Oid,               /* relid of relation */
    pub oncommit: OnCommitAction, /* what to do at end of xact */
    /*
     * If this entry was created during the current transaction,
     * creating_subid is the ID of the creating subxact; if created in a prior
     * transaction, creating_subid is zero.  If deleted during the current
     * transaction, deleting_subid is the ID of the deleting subxact; if no
     * deletion request is pending, deleting_subid is zero.
     */
    pub creating_subid: SubTransactionId,
    pub deleting_subid: SubTransactionId,
}

/* TODO(pg-port): OnCommitAction from nodes/parsenodes.h */
pub use crate::nodes::parsenodes::OnCommitAction;

static mut on_commits: *mut List = ptr::null_mut();

/*
 * State information for ALTER TABLE
 *
 * The pending-work queue for an ALTER TABLE is a List of AlteredTableInfo
 * structs, one for each table modified by the operation (the named table
 * plus any child tables that are affected).  We save lists of subcommands
 * to apply to this table (possibly modified by parse transformation steps);
 * these lists will be executed in Phase 2.  If a Phase 3 step is needed,
 * necessary information is stored in the constraints and newvals lists.
 *
 * Phase 2 is divided into multiple passes; subcommands are executed in
 * a pass determined by subcommand type.
 */

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum AlterTablePass {
    AT_PASS_UNSET = -1,          /* UNSET will cause ERROR */
    AT_PASS_DROP = 0,            /* DROP (all flavors) */
    AT_PASS_ALTER_TYPE = 1,      /* ALTER COLUMN TYPE */
    AT_PASS_ADD_COL = 2,         /* ADD COLUMN */
    AT_PASS_SET_EXPRESSION = 3,  /* ALTER SET EXPRESSION */
    AT_PASS_OLD_INDEX = 4,       /* re-add existing indexes */
    AT_PASS_OLD_CONSTR = 5,      /* re-add existing constraints */
    /* We could support a RENAME COLUMN pass here, but not currently used */
    AT_PASS_ADD_CONSTR = 6,      /* ADD constraints (initial examination) */
    AT_PASS_COL_ATTRS = 7,       /* set column attributes, eg NOT NULL */
    AT_PASS_ADD_INDEXCONSTR = 8, /* ADD index-based constraints */
    AT_PASS_ADD_INDEX = 9,       /* ADD indexes */
    AT_PASS_ADD_OTHERCONSTR = 10, /* ADD other constraints, defaults */
    AT_PASS_MISC = 11,           /* other stuff */
}

const AT_NUM_PASSES: usize = 12; /* AT_PASS_MISC + 1 */

#[repr(C)]
pub struct AlteredTableInfo {
    /* Information saved before any work commences: */
    pub relid: Oid,              /* Relation to work on */
    pub relkind: c_char,         /* Its relkind */
    pub oldDesc: TupleDesc,      /* Pre-modification tuple descriptor */

    /*
     * Transiently set during Phase 2, normally set to NULL.
     *
     * ATRewriteCatalogs sets this when it starts, and closes when ATExecCmd
     * returns control.  This can be exploited by ATExecCmd subroutines to
     * close/reopen across transaction boundaries.
     */
    pub rel: Relation,

    /* Information saved by Phase 1 for Phase 2: */
    pub subcmds: [*mut List; AT_NUM_PASSES], /* Lists of AlterTableCmd */
    /* Information saved by Phases 1/2 for Phase 3: */
    pub constraints: *mut List,   /* List of NewConstraint */
    pub newvals: *mut List,       /* List of NewColumnValue */
    pub afterStmts: *mut List,    /* List of utility command parsetrees */
    pub verify_new_notnull: bool, /* T if we should recheck NOT NULL */
    pub rewrite: c_int,           /* Reason for forced rewrite, if any */
    pub chgAccessMethod: bool,    /* T if SET ACCESS METHOD is used */
    pub newAccessMethod: Oid,     /* new access method; 0 means no change,
                                   * if above is true */
    pub newTableSpace: Oid,       /* new tablespace; 0 means no change */
    pub chgPersistence: bool,     /* T if SET LOGGED/UNLOGGED is used */
    pub newrelpersistence: c_char, /* if above is true */
    pub partition_constraint: *mut Expr, /* for attach partition validation */
    /* true, if validating default due to some other attach/detach */
    pub validate_default: bool,
    /* Objects to rebuild after completing ALTER TYPE operations */
    pub changedConstraintOids: *mut List, /* OIDs of constraints to rebuild */
    pub changedConstraintDefs: *mut List, /* string definitions of same */
    pub changedIndexOids: *mut List,      /* OIDs of indexes to rebuild */
    pub changedIndexDefs: *mut List,      /* string definitions of same */
    pub replicaIdentityIndex: *mut c_char, /* index to reset as REPLICA IDENTITY */
    pub clusterOnIndex: *mut c_char,      /* index to use for CLUSTER */
    pub changedStatisticsOids: *mut List, /* OIDs of statistics to rebuild */
    pub changedStatisticsDefs: *mut List, /* string definitions of same */
}

/* Struct describing one new constraint to check in Phase 3 scan */
/* Note: new not-null constraints are handled elsewhere */
#[repr(C)]
pub struct NewConstraint {
    pub name: *mut c_char,     /* Constraint name, or NULL if none */
    pub contype: ConstrType,   /* CHECK or FOREIGN */
    pub refrelid: Oid,         /* PK rel, if FOREIGN */
    pub refindid: Oid,         /* OID of PK's index, if FOREIGN */
    pub conwithperiod: bool,   /* Whether the new FOREIGN KEY uses PERIOD */
    pub conid: Oid,            /* OID of pg_constraint entry, if FOREIGN */
    pub qual: *mut Node,       /* Check expr or CONSTR_FOREIGN Constraint */
    pub qualstate: *mut ExprState, /* Execution state for CHECK expr */
}

/*
 * Struct describing one new column value that needs to be computed during
 * Phase 3 copy (this could be either a new column with a non-null default, or
 * a column that we're changing the type of).  Columns without such an entry
 * are just copied from the old table during ATRewriteTable.  Note that the
 * expr is an expression over *old* table values, except when is_generated
 * is true; then it is an expression over columns of the *new* tuple.
 */
#[repr(C)]
pub struct NewColumnValue {
    pub attnum: AttrNumber,        /* which column */
    pub expr: *mut Expr,           /* expression to compute */
    pub exprstate: *mut ExprState, /* execution state */
    pub is_generated: bool,        /* is it a GENERATED expression? */
}

/*
 * Error-reporting support for RemoveRelations
 */
#[repr(C)]
struct dropmsgstrings {
    kind: c_char,
    nonexistent_code: c_int,
    nonexistent_msg: *const c_char,
    skipping_msg: *const c_char,
    nota_msg: *const c_char,
    drophint_msg: *const c_char,
}

/* TODO(pg-port): RELKIND_* constants */
/* TODO(pg-port): ERRCODE_* constants */

static dropmsgstringarray: [dropmsgstrings; 0] = [];

/* communication between RemoveRelations and RangeVarCallbackForDropRelation */
#[repr(C)]
struct DropRelationCallbackState {
    /* These fields are set by RemoveRelations: */
    expected_relkind: c_char,
    heap_lockmode: LOCKMODE,
    /* These fields are state to track which subsidiary locks are held: */
    heapOid: Oid,
    partParentOid: Oid,
    /* These fields are passed back by RangeVarCallbackForDropRelation: */
    actual_relkind: c_char,
    actual_relpersistence: c_char,
}

/* Alter table target-type flags for ATSimplePermissions */
const ATT_TABLE: c_int            = 0x0001;
const ATT_VIEW: c_int             = 0x0002;
const ATT_MATVIEW: c_int          = 0x0004;
const ATT_INDEX: c_int            = 0x0008;
const ATT_COMPOSITE_TYPE: c_int   = 0x0010;
const ATT_FOREIGN_TABLE: c_int    = 0x0020;
const ATT_PARTITIONED_INDEX: c_int = 0x0040;
const ATT_SEQUENCE: c_int         = 0x0080;
const ATT_PARTITIONED_TABLE: c_int = 0x0100;

/*
 * ForeignTruncateInfo
 *
 * Information related to truncation of foreign tables.  This is used for
 * the elements in a hash table. It uses the server OID as lookup key,
 * and includes a per-server list of all foreign tables involved in the
 * truncation.
 */
#[repr(C)]
pub struct ForeignTruncateInfo {
    pub serverid: Oid,
    pub rels: *mut List,
}

/* Partial or complete FK creation in addFkConstraint() */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum addFkConstraintSides {
    addFkReferencedSide,
    addFkReferencingSide,
    addFkBothSides,
}

/*
 * Partition tables are expected to be dropped when the parent partitioned
 * table gets dropped. Hence for partitioning we use AUTO dependency.
 * Otherwise, for regular inheritance use NORMAL dependency.
 */
/* child_dependency_type(child_is_partition) */
/* => if child_is_partition { DEPENDENCY_AUTO } else { DEPENDENCY_NORMAL } */

/* ----------------------------------------------------------------
 *		DefineRelation
 *				Creates a new relation.
 *
 * stmt carries parsetree information from an ordinary CREATE TABLE statement.
 * The other arguments are used to extend the behavior for other cases:
 * relkind: relkind to assign to the new relation
 * ownerId: if not InvalidOid, use this as the new relation's owner.
 * typaddress: if not null, it's set to the pg_type entry's address.
 * queryString: for error reporting
 *
 * Note that permissions checks are done against current user regardless of
 * ownerId.  A nonzero ownerId is used when someone is creating a relation
 * "on behalf of" someone else, so we still want to see that the current user
 * has permissions to do it.
 *
 * If successful, returns the address of the new relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn DefineRelation(
    stmt: *mut CreateStmt,
    mut relkind: c_char,
    mut ownerId: Oid,
    typaddress: *mut ObjectAddress,
    queryString: *const c_char,
) -> ObjectAddress {
    let mut relname = [0u8; 64]; /* NAMEDATALEN */
    let mut namespaceId: Oid;
    let mut relationId: Oid;
    let mut tablespaceId: Oid;
    let rel: Relation;
    let descriptor: TupleDesc;
    let mut inheritOids: *mut List;
    let mut old_constraints: *mut List = crate::nodes::pg_list::NIL;
    let mut old_notnulls: *mut List = crate::nodes::pg_list::NIL;
    let mut rawDefaults: *mut List;
    let mut cookedDefaults: *mut List;
    let nncols: *mut List;
    let mut connames: *mut List = crate::nodes::pg_list::NIL;
    let mut reloptions: Datum;
    let mut attnum: AttrNumber;
    let partitioned: bool;
    let mut accessMethodId: Oid = InvalidOid;
    let mut address: ObjectAddress = InvalidObjectAddress;

    /*
     * Truncate relname to appropriate length.
     */
    {
        let src = CStr::from_ptr((*(*stmt).relation).relname);
        let bytes = src.to_bytes();
        let len = bytes.len().min(63);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), relname.as_mut_ptr(), len);
        relname[len] = 0;
    }
    let relname_ptr = relname.as_ptr() as *const c_char;

    /*
     * Check consistency of arguments.
     */
    if (*stmt).oncommit != crate::nodes::primnodes::OnCommitAction::ONCOMMIT_NOOP
        && (*(*stmt).relation).relpersistence != RELPERSISTENCE_TEMP
    {
        ereport!(ERROR, errmsg!("ON COMMIT can only be used on temporary tables")
            /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
        );
    }

    if !(*stmt).partspec.is_null() {
        /* only plain tables can be declared partitioned */
        relkind = RELKIND_PARTITIONED_TABLE as c_char;
        partitioned = true;
    } else {
        partitioned = false;
    }

    if relkind == RELKIND_PARTITIONED_TABLE as c_char
        && (*(*stmt).relation).relpersistence == b'u' as c_char /* RELPERSISTENCE_UNLOGGED */
    {
        ereport!(ERROR, errmsg!("partitioned tables cannot be unlogged")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /*
     * Look up namespace for the relation.
     */
    namespaceId = RangeVarGetAndCheckCreationNamespace((*stmt).relation, NoLock, ptr::null_mut());

    /*
     * Security check: disallow creating temp tables in security-restricted code.
     */
    if (*(*stmt).relation).relpersistence == RELPERSISTENCE_TEMP
        && InSecurityRestrictedOperation()
    {
        ereport!(ERROR, errmsg!("cannot create temporary table within security-restricted operation")
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    /*
     * Lockmode for scanning parents.
     */
    let parentLockmode = if !(*stmt).partbound.is_null() {
        AccessExclusiveLock
    } else {
        ShareUpdateExclusiveLock
    };

    /* Determine the list of OIDs of the parents. */
    inheritOids = crate::nodes::pg_list::NIL;
    foreach!(listptr, (*stmt).inhRelations, {
        let rv = crate::nodes::pg_list::lfirst(current_cell!(listptr)) as *const RangeVar;
        let parentOid = RangeVarGetRelid(rv, parentLockmode, false);

        /* Reject duplications */
        if list_member_oid(inheritOids, parentOid) {
            ereport!(ERROR,
                errmsg!("relation \"{}\" would be inherited from more than once",
                    CStr::from_ptr(get_rel_name(parentOid)).to_string_lossy())
                /* C also: errcode(ERRCODE_DUPLICATE_TABLE) */
            );
        }
        inheritOids = lappend_oid(inheritOids, parentOid);
    });

    /*
     * Select tablespace to use.
     */
    if !(*stmt).tablespacename.is_null() {
        tablespaceId = get_tablespace_oid((*stmt).tablespacename, false);
        if partitioned && tablespaceId == MyDatabaseTableSpace() {
            ereport!(ERROR, errmsg!("cannot specify default tablespace for partitioned relations")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            );
        }
    } else if !(*stmt).partbound.is_null() {
        tablespaceId = get_rel_tablespace(linitial_oid(inheritOids));
    } else {
        tablespaceId = InvalidOid;
    }

    /* still nothing? use the default */
    if !OidIsValid(tablespaceId) {
        tablespaceId = GetDefaultTablespace((*(*stmt).relation).relpersistence, partitioned);
    }

    /* Check permissions except when using database's default */
    if OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace() {
        let aclresult = object_aclcheck(TableSpaceRelationId_const(), tablespaceId, GetUserId(), ACL_CREATE());
        if aclresult != 0 /* ACLCHECK_OK */ {
            aclcheck_error(aclresult, OBJECT_TABLESPACE(), get_tablespace_name(tablespaceId));
        }
    }

    /* Disallow placing user relations in pg_global */
    {
        const GLOBALTABLESPACE_OID: Oid = 1664;
        if tablespaceId == GLOBALTABLESPACE_OID {
            ereport!(ERROR, errmsg!("only shared relations can be placed in pg_global tablespace")
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            );
        }
    }

    /* Identify user ID that will own the table */
    if !OidIsValid(ownerId) {
        ownerId = GetUserId();
    }

    /*
     * Parse and validate reloptions, if any.
     */
    const validnsps: [*const c_char; 1] = [ptr::null()]; /* HEAP_RELOPT_NAMESPACES placeholder */
    reloptions = transformRelOptions(0 as Datum, (*stmt).options, ptr::null(), validnsps.as_ptr(), true, false);

    if relkind == RELKIND_VIEW as c_char {
        view_reloptions(reloptions, true);
    } else if relkind == RELKIND_PARTITIONED_TABLE as c_char {
        partitioned_table_reloptions(reloptions, true);
    } else {
        heap_reloptions(relkind, reloptions, true);
    }

    let ofTypeId: Oid;
    if !(*stmt).ofTypename.is_null() {
        let aclresult = object_aclcheck(TypeRelationId, typenameTypeId(ptr::null_mut(), (*stmt).ofTypename), GetUserId(), ACL_USAGE());
        ofTypeId = typenameTypeId(ptr::null_mut(), (*stmt).ofTypename);
        if aclresult != 0 /* ACLCHECK_OK */ {
            aclcheck_error_type(aclresult, ofTypeId);
        }
    } else {
        ofTypeId = InvalidOid;
    }

    /*
     * Look up inheritance ancestors and generate relation schema.
     * (Note that stmt->tableElts is destructively modified by MergeAttributes.)
     */
    (*stmt).tableElts = MergeAttributes((*stmt).tableElts, inheritOids,
                                        (*(*stmt).relation).relpersistence,
                                        !(*stmt).partbound.is_null(),
                                        &mut old_constraints, &mut old_notnulls);

    /*
     * Create a tuple descriptor from the relation schema.
     */
    descriptor = BuildDescForRelation((*stmt).tableElts);

    /*
     * Find columns with default values and prepare for insertion.
     */
    rawDefaults = crate::nodes::pg_list::NIL;
    cookedDefaults = crate::nodes::pg_list::NIL;
    attnum = 0;

    foreach!(listptr2, (*stmt).tableElts, {
        let colDef = crate::nodes::pg_list::lfirst(current_cell!(listptr2)) as *mut ColumnDef;
        attnum += 1;
        if !(*colDef).raw_default.is_null() {
            let rawEnt = palloc(std::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
            (*rawEnt).attnum = attnum;
            (*rawEnt).raw_default = (*colDef).raw_default;
            (*rawEnt).generated = (*colDef).generated;
            rawDefaults = lappend(rawDefaults as *mut std::ffi::c_void, rawEnt as *mut std::ffi::c_void) as *mut List;
        } else if !(*colDef).cooked_default.is_null() {
            let cooked = palloc(std::mem::size_of::<CookedConstraint>()) as *mut CookedConstraint;
            (*cooked).contype = crate::nodes::parsenodes::ConstrType::CONSTR_DEFAULT;
            (*cooked).conoid = InvalidOid;
            (*cooked).name = ptr::null_mut();
            (*cooked).attnum = attnum;
            (*cooked).expr = (*colDef).cooked_default;
            (*cooked).is_enforced = true;
            (*cooked).skip_validation = false;
            (*cooked).is_local = true;
            (*cooked).inhcount = 0;
            (*cooked).is_no_inherit = false;
            cookedDefaults = lappend(cookedDefaults as *mut std::ffi::c_void, cooked as *mut std::ffi::c_void) as *mut List;
        }
    });

    /*
     * For relations with table AM and partitioned tables, select access method.
     */
    if !(*stmt).accessMethod.is_null() {
        accessMethodId = get_table_am_oid((*stmt).accessMethod, false);
    } else if RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_PARTITIONED_TABLE as c_char {
        if !(*stmt).partbound.is_null() {
            accessMethodId = get_rel_relam(linitial_oid(inheritOids));
        }
        if RELKIND_HAS_TABLE_AM(relkind) && !OidIsValid(accessMethodId) {
            accessMethodId = get_table_am_oid(default_table_access_method(), false);
        }
    }

    /*
     * Create the relation.  Inherited defaults and CHECK constraints are
     * passed in for immediate handling.
     */
    relationId = heap_create_with_catalog(
        relname_ptr,
        namespaceId,
        tablespaceId,
        InvalidOid,
        InvalidOid,
        ofTypeId,
        ownerId,
        accessMethodId,
        descriptor,
        list_concat(cookedDefaults, old_constraints),
        relkind,
        (*(*stmt).relation).relpersistence,
        false,
        false,
        (*stmt).oncommit,
        reloptions,
        true,
        allowSystemTableMods(),
        false,
        InvalidOid,
        typaddress,
    );

    /*
     * Bump command counter to make the newly-created relation tuple visible.
     */
    CommandCounterIncrement();

    /*
     * Open the new relation and acquire exclusive lock on it.
     */
    let rel = relation_open(relationId, AccessExclusiveLock);

    /*
     * Add newly specified column default and generation expressions.
     */
    if !rawDefaults.is_null() {
        AddRelationNewConstraints(rel, rawDefaults, crate::nodes::pg_list::NIL,
                                  true, true, false, queryString);
    }

    /*
     * Make column generation expressions visible for use by partitioning.
     */
    CommandCounterIncrement();

    /* Process and store partition bound, if any. */
    if !(*stmt).partbound.is_null() {
        let parentId: Oid = linitial_oid(inheritOids);
        let mut defaultRel: Relation = ptr::null_mut();

        /* Already have strong enough lock on the parent */
        let parent = table_open(parentId, NoLock);

        if (*(*parent).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as c_char {
            ereport!(ERROR,
                errmsg!("\"{}\" is not partitioned",
                    CStr::from_ptr(RelationGetRelationName(parent)).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            );
        }

        let defaultPartOid = get_default_oid_from_partdesc(
            RelationGetPartitionDesc(parent, true));
        if OidIsValid(defaultPartOid) {
            defaultRel = table_open(defaultPartOid, AccessExclusiveLock);
        }

        /* Transform the bound values */
        let pstate = make_parsestate(ptr::null_mut());
        /* pstate->p_sourcetext = queryString; -- opaque type */
        let nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
                                                   ptr::null_mut(), false, false);
        addNSItemToQuery(pstate, nsitem, false, true, true);

        let bound = transformPartitionBound(pstate, parent, (*stmt).partbound);

        check_new_partition_bound(RelationGetRelationName(rel), parent, bound, pstate);

        if OidIsValid(defaultPartOid) {
            check_default_partition_contents(parent, defaultRel, bound);
            /* Keep the lock until commit. */
            table_close(defaultRel, NoLock);
        }

        /* Update the pg_class entry. */
        StorePartitionBound(rel, parent, bound);

        table_close(parent, NoLock);
    }

    /* Store inheritance information for new rel. */
    StoreCatalogInheritance(relationId, inheritOids, !(*stmt).partbound.is_null());

    /*
     * Process the partitioning specification (if any) and store partition key.
     */
    if partitioned {
        let pstate = make_parsestate(ptr::null_mut());
        /* pstate->p_sourcetext = queryString; -- opaque */
        let partnatts = list_length((*(*stmt).partspec).partParams);

        if partnatts > PARTITION_MAX_KEYS {
            ereport!(ERROR, errmsg!("cannot partition using more than {} columns", PARTITION_MAX_KEYS)
                /* C also: errcode(ERRCODE_TOO_MANY_COLUMNS) */
            );
        }

        (*stmt).partspec = transformPartitionSpec(rel, (*stmt).partspec);

        let mut partattrs = [0i16; 64]; /* PARTITION_MAX_KEYS */
        let mut partopclass = [0u32; 64];
        let mut partcollation = [0u32; 64];
        let mut partexprs: *mut List = crate::nodes::pg_list::NIL;

        ComputePartitionAttrs(pstate, rel, (*(*stmt).partspec).partParams,
                              partattrs.as_mut_ptr(), &mut partexprs,
                              partopclass.as_mut_ptr(), partcollation.as_mut_ptr(),
                              (*(*stmt).partspec).strategy as isize as c_char);

        StorePartitionKey(rel, (*(*stmt).partspec).strategy as isize as c_char, partnatts,
                          partattrs.as_ptr(), partexprs,
                          partopclass.as_ptr(), partcollation.as_ptr());

        CommandCounterIncrement();
    }

    /*
     * If we're creating a partition, create indexes, triggers, FKs defined in parent.
     */
    if !(*stmt).partbound.is_null() {
        let parentId: Oid = linitial_oid(inheritOids);

        /* Already have strong enough lock on the parent */
        let parent = table_open(parentId, NoLock);
        let idxlist = RelationGetIndexList(parent);

        /* For each index in the parent, create one in the partition */
        foreach!(cell, idxlist, {
            let idxoid = lfirst_oid(current_cell!(cell));
            let idxRel = index_open(idxoid, AccessShareLock);
            let mut constraintOid: Oid = InvalidOid;

            if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as c_char {
                if (*(*idxRel).rd_index).indisunique {
                    ereport!(ERROR,
                        errmsg!("cannot create foreign partition of partitioned table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(parent)).to_string_lossy())
                        /* C also: errcode, errdetail */
                    );
                } else {
                    index_close(idxRel, AccessShareLock);
                    /* continue -- skip this index */
                }
            } else {
                let attmap = build_attrmap_by_name(RelationGetDescr(rel),
                                                   RelationGetDescr(parent), false);
                let idxstmt = generateClonedIndexStmt(ptr::null_mut(), idxRel,
                                                      attmap, &mut constraintOid);
                DefineIndex(RelationGetRelid(rel), idxstmt, InvalidOid,
                            RelationGetRelid(idxRel), constraintOid,
                            -1, false, false, false, false, false);
                index_close(idxRel, AccessShareLock);
            }
        });

        list_free(idxlist);

        /* Clone row-level triggers, if any */
        if !(*parent).trigdesc.is_null() {
            CloneRowTriggersToPartition(parent, rel);
        }

        /* Clone foreign keys */
        CloneForeignKeyConstraints(ptr::null_mut(), parent, rel);

        table_close(parent, NoLock);
    }

    /*
     * Now add any newly specified CHECK constraints.
     */
    if !(*stmt).constraints.is_null() {
        let conlist = AddRelationNewConstraints(rel, crate::nodes::pg_list::NIL, (*stmt).constraints,
                                               true, true, false, queryString);
        foreach!(lconstr, conlist, {
            let cons = crate::nodes::pg_list::lfirst(current_cell!(lconstr)) as *mut CookedConstraint;
            if !(*cons).name.is_null() {
                connames = lappend(connames as *mut std::ffi::c_void, (*cons).name as *mut std::ffi::c_void) as *mut List;
            }
        });
    }

    /*
     * Merge not-null constraints, create them, and set attnotnull flag.
     */
    let nncols = AddRelationNotNullConstraints(rel, (*stmt).nnconstraints,
                                               old_notnulls, connames);
    /* foreach_int(attrnum, nncols) set_attnotnull(NULL, rel, attrnum, true, false);
     * TODO(pg-port): foreach_int not yet implemented; skipping set_attnotnull loop */

    ObjectAddressSet!(address, RelationRelationId, relationId);

    /*
     * Clean up.  Keep lock on new relation (not visible to others anyway).
     */
    relation_close(rel, NoLock);

    address
}

/*
 * BuildDescForRelation
 *
 * Given a list of ColumnDef nodes, build a TupleDesc.
 *
 * Note: This is only for the limited purpose of table and view creation.  Not
 * everything is filled in.  A real tuple descriptor should be obtained from
 * the relcache.
 */
pub unsafe fn BuildDescForRelation(columns: *const List) -> TupleDesc {
    /*
     * allocate a new tuple descriptor
     */
    let natts = list_length(columns as *const List);
    let desc = CreateTemplateTupleDesc(natts);

    let mut attnum: AttrNumber = 0;

    foreach!(l, columns as *mut List, {
        let entry = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut ColumnDef;

        /*
         * for each entry in the list, get the name and type information from
         * the list and have TupleDescInitEntry fill in the attribute
         * information we need.
         */
        attnum += 1;

        let attname = (*entry).colname;
        let mut atttypid: Oid = 0;
        let mut atttypmod: i32 = 0;
        typenameTypeIdAndMod(ptr::null_mut(), (*entry).typeName, &mut atttypid, &mut atttypmod);

        let aclresult = object_aclcheck(TypeRelationId, atttypid, GetUserId(), ACL_USAGE());
        if aclresult != 0 /* ACLCHECK_OK */ {
            aclcheck_error_type(aclresult, atttypid);
        }

        let attcollation = GetColumnDefCollation(ptr::null_mut(), entry, atttypid);
        let attdim = list_length((*(*entry).typeName).arrayBounds);
        if attdim > i16::MAX as c_int {
            ereport!(ERROR, errmsg!("too many array dimensions")
                /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
            );
        }

        if (*(*entry).typeName).setof {
            ereport!(ERROR,
                errmsg!("column \"{}\" cannot be declared SETOF",
                    CStr::from_ptr(attname).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }

        TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, attdim);
        let att = TupleDescAttr(desc, (attnum - 1) as usize);

        /* Override TupleDescInitEntry's settings as requested */
        TupleDescInitEntryCollation(desc, attnum, attcollation);

        /* Fill in additional stuff not handled by TupleDescInitEntry */
        (*att).attnotnull = (*entry).is_not_null;
        (*att).attislocal = (*entry).is_local;
        (*att).attinhcount = (*entry).inhcount as i16;
        (*att).attidentity = (*entry).identity;
        (*att).attgenerated = (*entry).generated;
        (*att).attcompression = GetAttributeCompression((*att).atttypid, (*entry).compression);
        if (*entry).storage != 0 {
            (*att).attstorage = (*entry).storage;
        } else if !(*entry).storage_name.is_null() {
            (*att).attstorage = GetAttributeStorage((*att).atttypid, (*entry).storage_name);
        }

        populate_compact_attribute(desc, (attnum - 1) as usize);
    });

    desc
}

/*
 * Emit the right error or warning message for a "DROP" command issued on a
 * non-existent relation
 */
unsafe fn DropErrorMsgNonExistent(rel: *mut RangeVar, rightkind: c_char, missing_ok: bool) {
    if !(*rel).schemaname.is_null() {
        /* TODO(pg-port): LookupNamespaceNoError stub -- treat as valid */
        if !missing_ok {
            ereport!(
                ERROR,
                errmsg!("schema \"{}\" does not exist",
                    CStr::from_ptr((*rel).schemaname).to_string_lossy())
                /* C also: errcode(ERRCODE_UNDEFINED_SCHEMA) */
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!("schema \"{}\" does not exist, skipping",
                    CStr::from_ptr((*rel).schemaname).to_string_lossy())
            );
        }
        return;
    }

    for rentry in dropmsgstringarray.iter() {
        if rentry.kind == b'\0' as c_char {
            break;
        }
        if rentry.kind == rightkind {
            if !missing_ok {
                ereport!(
                    ERROR,
                    errmsg!("\"{}\" does not exist",
                        CStr::from_ptr((*rel).relname).to_string_lossy())
                    /* C also: errcode(rentry.nonexistent_code) */
                );
            } else {
                ereport!(
                    NOTICE,
                    errmsg!("\"{}\" does not exist, skipping",
                        CStr::from_ptr((*rel).relname).to_string_lossy())
                );
            }
            return;
        }
    }
    /* Should be impossible */
}

/*
 * Emit the right error message for a "DROP" command issued on a
 * relation of the wrong type
 */
unsafe fn DropErrorMsgWrongType(relname: *const c_char, wrongkind: c_char, rightkind: c_char) {
    let mut rentry: *const dropmsgstrings = dropmsgstringarray.as_ptr();
    while (*rentry).kind != b'\0' as c_char {
        if (*rentry).kind == rightkind { break; }
        rentry = rentry.add(1);
    }

    let mut wentry: *const dropmsgstrings = dropmsgstringarray.as_ptr();
    while (*wentry).kind != b'\0' as c_char {
        if (*wentry).kind == wrongkind { break; }
        wentry = wentry.add(1);
    }
    /* wrongkind could be something we don't have in our table... */

    ereport!(
        ERROR,
        errmsg!("\"{}\" is not the right object type",
            CStr::from_ptr(relname).to_string_lossy())
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
           errmsg(rentry->nota_msg, relname),
           errhint if wentry is valid */
    );
}

/*
 * RemoveRelations
 *		Implements DROP TABLE, DROP INDEX, DROP SEQUENCE, DROP VIEW,
 *		DROP MATERIALIZED VIEW, DROP FOREIGN TABLE
 */
pub unsafe fn RemoveRelations(drop: *mut DropStmt) {
    let objects: *mut ObjectAddresses;
    let relkind: c_char;
    let mut flags: c_int = 0;
    let mut lockmode: LOCKMODE = AccessExclusiveLock;

    /* DROP CONCURRENTLY uses a weaker lock, and has some restrictions */
    if (*drop).concurrent {
        /*
         * Note that for temporary relations this lock may get upgraded later
         * on, but as no other session can access a temporary relation, this
         * is actually fine.
         */
        lockmode = ShareUpdateExclusiveLock;
        /* Assert(drop->removeType == OBJECT_INDEX) */
        if list_length((*drop).objects) != 1 {
            ereport!(
                ERROR,
                errmsg!("DROP INDEX CONCURRENTLY does not support dropping multiple objects")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            );
        }
        if (*drop).behavior == DropBehavior::DROP_CASCADE {
            ereport!(
                ERROR,
                errmsg!("DROP INDEX CONCURRENTLY does not support CASCADE")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            );
        }
    }

    /*
     * First we identify all the relations, then we delete them in a single
     * performMultipleDeletions() call.  This is to avoid unwanted DROP
     * RESTRICT errors if one of the relations depends on another.
     */

    /* Determine required relkind */
    relkind = match (*drop).removeType {
        ObjectType::OBJECT_TABLE => RELKIND_RELATION as c_char,
        ObjectType::OBJECT_INDEX => RELKIND_INDEX as c_char,
        ObjectType::OBJECT_SEQUENCE => RELKIND_SEQUENCE as c_char,
        ObjectType::OBJECT_VIEW => RELKIND_VIEW as c_char,
        ObjectType::OBJECT_MATVIEW => RELKIND_MATVIEW as c_char,
        ObjectType::OBJECT_FOREIGN_TABLE => RELKIND_FOREIGN_TABLE as c_char,
        _ => {
            ereport!(ERROR, errmsg!("unrecognized drop object type"));
            0
        }
    };

    /* Lock and validate each relation; build a list of object addresses */
    objects = new_object_addresses();

    foreach!(cell, (*drop).objects, {
        let rel = makeRangeVarFromNameList(crate::nodes::pg_list::lfirst(current_cell!(cell)) as *mut List);
        let relOid: Oid;
        let mut obj = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
        let mut state = DropRelationCallbackState {
            expected_relkind: relkind,
            heap_lockmode: if (*drop).concurrent { ShareUpdateExclusiveLock } else { AccessExclusiveLock },
            heapOid: InvalidOid,
            partParentOid: InvalidOid,
            actual_relkind: 0,
            actual_relpersistence: 0,
        };

        /*
         * Check for shared-cache-inval messages before trying to access the
         * relation.  This is needed to cover the case where the name
         * identifies a rel that has been dropped and recreated since the
         * start of our transaction: if we don't flush the old syscache entry,
         * then we'll latch onto that entry and suffer an error later.
         */
        AcceptInvalidationMessages();

        relOid = RangeVarGetRelidExtended(rel, lockmode, 0x01 /* RVR_MISSING_OK */,
                                           RangeVarCallbackForDropRelation,
                                           &mut state as *mut DropRelationCallbackState as *mut std::ffi::c_void);

        /* Not there? */
        if !OidIsValid(relOid) {
            DropErrorMsgNonExistent(rel, relkind, (*drop).missing_ok);
            continue;
        }

        /*
         * Decide if concurrent mode needs to be used here or not.  The
         * callback retrieved the rel's persistence for us.
         */
        if (*drop).concurrent && state.actual_relpersistence != RELPERSISTENCE_TEMP {
            /* Assert(list_length(drop->objects) == 1 && drop->removeType == OBJECT_INDEX) */
            flags |= 0x1; /* PERFORM_DELETION_CONCURRENTLY */
        }

        /*
         * Concurrent index drop cannot be used with partitioned indexes, either.
         */
        if (flags & 0x1) != 0 && state.actual_relkind == RELKIND_PARTITIONED_INDEX as c_char {
            ereport!(
                ERROR,
                errmsg!("cannot drop partitioned index \"{}\" concurrently",
                    CStr::from_ptr((*rel).relname).to_string_lossy())
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            );
        }

        /*
         * If we're told to drop a partitioned index, we must acquire lock on
         * all the children of its parent partitioned table before proceeding.
         * Otherwise we'd try to lock the child index partitions before their
         * tables, leading to potential deadlock against other sessions that
         * will lock those objects in the other order.
         */
        if state.actual_relkind == RELKIND_PARTITIONED_INDEX as c_char {
            let _ = find_all_inheritors(state.heapOid, state.heap_lockmode, ptr::null_mut());
        }

        /* OK, we're ready to delete this one */
        obj.classId = RelationRelationId;
        obj.objectId = relOid;
        obj.objectSubId = 0;

        add_exact_object_address(&obj, objects);
    });

    performMultipleDeletions(objects, (*drop).behavior, flags);

    free_object_addresses(objects);
}

/*
 * Before acquiring a table lock, check whether we have sufficient rights.
 * In the case of DROP INDEX, also try to lock the table before the index.
 * Also, if the table to be dropped is a partition, we try to lock the parent
 * first.
 */
unsafe fn RangeVarCallbackForDropRelation(
    rel: *const RangeVar,
    relOid: Oid,
    oldRelOid: Oid,
    arg: *mut std::ffi::c_void,
) {
    let state = arg as *mut DropRelationCallbackState;
    let heap_lockmode = (*state).heap_lockmode;
    let mut invalid_system_index = false;

    /*
     * If we previously locked some other index's heap, and the name we're
     * looking up no longer refers to that relation, release the now-useless
     * lock.
     */
    if relOid != oldRelOid && OidIsValid((*state).heapOid) {
        UnlockRelationOid((*state).heapOid, heap_lockmode);
        (*state).heapOid = InvalidOid;
    }

    /*
     * Similarly, if we previously locked some other partition's heap, and the
     * name we're looking up no longer refers to that relation, release the
     * now-useless lock.
     */
    if relOid != oldRelOid && OidIsValid((*state).partParentOid) {
        UnlockRelationOid((*state).partParentOid, AccessExclusiveLock);
        (*state).partParentOid = InvalidOid;
    }

    /* Didn't find a relation, so no need for locking or permission checks. */
    if !OidIsValid(relOid) {
        return;
    }

    let tuple = SearchSysCache1(0 /* RELOID */, ObjectIdGetDatum(relOid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped, so nothing to do */
    }
    let classform = GETSTRUCT(tuple) as *mut FormData_pg_class;
    let is_partition = (*classform).relispartition;

    /* Pass back some data to save lookups in RemoveRelations */
    (*state).actual_relkind = (*classform).relkind;
    (*state).actual_relpersistence = (*classform).relpersistence;

    /*
     * Both RELKIND_RELATION and RELKIND_PARTITIONED_TABLE are OBJECT_TABLE,
     * but RemoveRelations() can only pass one relkind for a given relation.
     * It chooses RELKIND_RELATION for both regular and partitioned tables.
     * That means we must be careful before giving the wrong type error when
     * the relation is RELKIND_PARTITIONED_TABLE.  An equivalent problem
     * exists with indexes.
     */
    let expected_relkind: c_char = if (*classform).relkind == RELKIND_PARTITIONED_TABLE as c_char {
        RELKIND_RELATION as c_char
    } else if (*classform).relkind == RELKIND_PARTITIONED_INDEX as c_char {
        RELKIND_INDEX as c_char
    } else {
        (*classform).relkind
    };

    if (*state).expected_relkind != expected_relkind {
        DropErrorMsgWrongType((*rel).relname, (*classform).relkind, (*state).expected_relkind);
    }

    /* Allow DROP to either table owner or schema owner */
    if !object_ownercheck(RelationRelationId, relOid, GetUserId())
        && !object_ownercheck(0 /* NamespaceRelationId */, (*classform).relnamespace, GetUserId())
    {
        aclcheck_error(0 /* ACLCHECK_NOT_OWNER */,
                       get_relkind_objtype((*classform).relkind),
                       (*rel).relname);
    }

    /*
     * Check the case of a system index that might have been invalidated by a
     * failed concurrent process and allow its drop.
     */
    if IsSystemClass(relOid, classform) && (*classform).relkind == RELKIND_INDEX as c_char {
        let locTuple = SearchSysCache1(0 /* INDEXRELID */, ObjectIdGetDatum(relOid));
        if !HeapTupleIsValid(locTuple) {
            ReleaseSysCache(tuple);
            return;
        }
        let indexform = GETSTRUCT(locTuple) as *mut crate::catalog::pg_index::FormData_pg_index;
        let indisvalid = (*indexform).indisvalid;
        ReleaseSysCache(locTuple);

        /* Mark object as being an invalid index of system catalogs */
        if !indisvalid {
            invalid_system_index = true;
        }
    }

    /* In the case of an invalid index, it is fine to bypass this check */
    if !invalid_system_index && !allowSystemTableMods() && IsSystemClass(relOid, classform) {
        ereport!(
            ERROR,
            errmsg!("permission denied: \"{}\" is a system catalog",
                CStr::from_ptr((*rel).relname).to_string_lossy())
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    ReleaseSysCache(tuple);

    /*
     * In DROP INDEX, attempt to acquire lock on the parent table before
     * locking the index.  index_drop() will need this anyway, and since
     * regular queries lock tables before their indexes, we risk deadlock if
     * we do it the other way around.  No error if we don't find a pg_index
     * entry, though --- the relation may have been dropped.
     */
    if expected_relkind == RELKIND_INDEX as c_char && relOid != oldRelOid {
        (*state).heapOid = IndexGetRelation(relOid, true);
        if OidIsValid((*state).heapOid) {
            LockRelationOid((*state).heapOid, heap_lockmode);
        }
    }

    /*
     * Similarly, if the relation is a partition, we must acquire lock on its
     * parent before locking the partition.  That's because queries lock the
     * parent before its partitions, so we risk deadlock if we do it the other
     * way around.
     */
    if is_partition && relOid != oldRelOid {
        (*state).partParentOid = get_partition_parent(relOid, true);
        if OidIsValid((*state).partParentOid) {
            LockRelationOid((*state).partParentOid, AccessExclusiveLock);
        }
    }
}

/*
 * ExecuteTruncate
 *		Executes a TRUNCATE command.
 *
 * This is a multi-relation truncate.  We first open and grab exclusive
 * lock on all relations involved, checking permissions and otherwise
 * verifying that the relation is OK for truncation.  Note that if relations
 * are foreign tables, at this stage, we have not yet checked that their
 * foreign data in external data sources are OK for truncation.  These are
 * checked when foreign data are actually truncated later.  In CASCADE mode,
 * relations having FK references to the targeted relations are automatically
 * added to the group; in RESTRICT mode, we check that all FK references are
 * internal to the group that's being truncated.  Finally all the relations
 * are truncated and reindexed.
 */
pub unsafe fn ExecuteTruncate(stmt: *mut TruncateStmt) {
    let mut rels: *mut List = crate::nodes::pg_list::NIL;
    let mut relids: *mut List = crate::nodes::pg_list::NIL;
    let mut relids_logged: *mut List = crate::nodes::pg_list::NIL;

    /*
     * Open, exclusive-lock, and check all the explicitly-specified relations
     */
    foreach!(cell, (*stmt).relations, {
        let rv = crate::nodes::pg_list::lfirst(current_cell!(cell)) as *mut RangeVar;
        let recurse = (*rv).inh;
        let lockmode: LOCKMODE = AccessExclusiveLock;

        let myrelid = RangeVarGetRelidExtended(rv, lockmode, 0,
                                               RangeVarCallbackForTruncate,
                                               ptr::null_mut());

        /* don't throw error for "TRUNCATE foo, foo" */
        if list_member_oid(relids, myrelid) {
            continue;
        }

        /* open the relation, we already hold a lock on it */
        let rel = table_open(myrelid, NoLock);

        /*
         * RangeVarGetRelidExtended() has done most checks with its callback,
         * but other checks with the now-opened Relation remain.
         */
        truncate_check_activity(rel);

        rels = lappend_ptr(rels, rel as *mut std::ffi::c_void);
        relids = lappend_oid(relids, myrelid);

        /* Log this relation only if needed for logical decoding */
        if RelationIsLogicallyLogged(rel) {
            relids_logged = lappend_oid(relids_logged, myrelid);
        }

        if recurse {
            let children = find_all_inheritors(myrelid, lockmode, ptr::null_mut());

            foreach!(child, children, {
                let childrelid = lfirst_oid(current_cell!(child));

                if list_member_oid(relids, childrelid) {
                    continue;
                }

                /* find_all_inheritors already got lock */
                let rel = table_open(childrelid, NoLock);

                /*
                 * It is possible that the parent table has children that are
                 * temp tables of other backends.  We cannot safely access
                 * such tables (because of buffering issues), and the best
                 * thing to do is to silently ignore them.
                 */
                if RELATION_IS_OTHER_TEMP(rel) {
                    table_close(rel, lockmode);
                    continue;
                }

                /*
                 * Inherited TRUNCATE commands perform access permission
                 * checks on the parent table only.
                 */
                truncate_check_rel(RelationGetRelid(rel), (*rel).rd_rel);
                truncate_check_activity(rel);

                rels = lappend_ptr(rels, rel as *mut std::ffi::c_void);
                relids = lappend_oid(relids, childrelid);

                /* Log this relation only if needed for logical decoding */
                if RelationIsLogicallyLogged(rel) {
                    relids_logged = lappend_oid(relids_logged, childrelid);
                }
            });
        } else if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char {
            ereport!(
                ERROR,
                errmsg!("cannot truncate only a partitioned table")
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly.") */
            );
        }
    });

    ExecuteTruncateGuts(rels, relids, relids_logged,
                        (*stmt).behavior, (*stmt).restart_seqs, false);

    /* And close the rels */
    foreach!(cell, rels, {
        let rel = crate::nodes::pg_list::lfirst(current_cell!(cell)) as Relation;
        table_close(rel, NoLock);
    });
}

/*
 * ExecuteTruncateGuts
 *
 * Internal implementation of TRUNCATE.  This is called by the actual TRUNCATE
 * command (see above) as well as replication subscribers that execute a
 * replicated TRUNCATE action.
 *
 * explicit_rels is the list of Relations to truncate that the command
 * specified.  relids is the list of Oids corresponding to explicit_rels.
 * relids_logged is the list of Oids (a subset of relids) that require
 * WAL-logging.  This is all a bit redundant, but the existing callers have
 * this information handy in this form.
 */
pub unsafe fn ExecuteTruncateGuts(
    explicit_rels: *mut List,
    relids: *mut List,
    relids_logged: *mut List,
    behavior: DropBehavior,
    restart_seqs: bool,
    run_as_table_owner: bool,
) {
    let mut rels: *mut List;
    let mut seq_relids: *mut List = crate::nodes::pg_list::NIL;
    let estate: *mut std::ffi::c_void /* EState */;
    let resultRelInfos: *mut ResultRelInfo;
    let mut resultRelInfo: *mut ResultRelInfo;
    let mySubid: crate::access::transam::SubTransactionId;

    /*
     * Check the explicitly-specified relations.
     *
     * In CASCADE mode, suck in all referencing relations as well.
     */
    rels = list_copy(explicit_rels);
    let mut relids = relids; /* local mutable copy */
    let mut relids_logged = relids_logged;
    if behavior == DropBehavior::DROP_CASCADE {
        loop {
            let newrelids = heap_truncate_find_FKs(relids);
            if newrelids.is_null() || list_length(newrelids) == 0 {
                break; /* nothing else to add */
            }

            foreach!(cell, newrelids, {
                let relid = lfirst_oid(current_cell!(cell));
                let rel = table_open(relid, AccessExclusiveLock);
                ereport!(
                    NOTICE,
                    errmsg!("truncate cascades to table \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
                );
                truncate_check_rel(relid, (*rel).rd_rel);
                truncate_check_perms(relid, (*rel).rd_rel);
                truncate_check_activity(rel);
                rels = lappend_ptr(rels, rel as *mut std::ffi::c_void);
                relids = lappend_oid(relids, relid);

                /* Log this relation only if needed for logical decoding */
                if RelationIsLogicallyLogged(rel) {
                    relids_logged = lappend_oid(relids_logged, relid);
                }
            });
        }
    }

    /*
     * Check foreign key references.
     */
    if behavior == DropBehavior::DROP_RESTRICT {
        heap_truncate_check_FKs(rels, false);
    }

    /*
     * If we are asked to restart sequences, find all the sequences.
     */
    if restart_seqs {
        foreach!(cell, rels, {
            let rel = crate::nodes::pg_list::lfirst(current_cell!(cell)) as Relation;
            let seqlist = getOwnedSequences(RelationGetRelid(rel));

            foreach!(seqcell, seqlist, {
                let seq_relid = lfirst_oid(current_cell!(seqcell));
                let seq_rel = relation_open(seq_relid, AccessExclusiveLock);

                /* This check must match AlterSequence! */
                if !object_ownercheck(RelationRelationId, seq_relid, GetUserId()) {
                    aclcheck_error(0 /* ACLCHECK_NOT_OWNER */, 0 /* OBJECT_SEQUENCE */,
                                   RelationGetRelationName(seq_rel));
                }

                seq_relids = lappend_oid(seq_relids, seq_relid);

                relation_close(seq_rel, NoLock);
            });
        });
    }

    /* Prepare to catch AFTER triggers. */
    AfterTriggerBeginQuery();

    /*
     * To fire triggers, we'll need an EState as well as a ResultRelInfo for
     * each relation.  We don't need to call ExecOpenIndices, though.
     */
    estate = CreateExecutorState();
    let nrels = list_length(rels) as usize;
    resultRelInfos = palloc(nrels * std::mem::size_of::<ResultRelInfo>()) as *mut ResultRelInfo;
    resultRelInfo = resultRelInfos;
    foreach!(cell, rels, {
        let rel = crate::nodes::pg_list::lfirst(current_cell!(cell)) as Relation;
        InitResultRelInfo(resultRelInfo, rel, 0 /* dummy rangetable index */,
                          ptr::null_mut(), 0);
        /* estate->es_opened_result_relations = lappend(..., resultRelInfo) */
        resultRelInfo = resultRelInfo.add(1);
    });

    /*
     * Process all BEFORE STATEMENT TRUNCATE triggers.
     */
    resultRelInfo = resultRelInfos;
    foreach!(cell, rels, {
        let mut ucxt: *mut std::ffi::c_void = ptr::null_mut();
        if run_as_table_owner {
            SwitchToUntrustedUser((*(*resultRelInfo).ri_RelationDesc).rd_rel as Oid /* relowner */, &mut ucxt);
        }
        ExecBSTruncateTriggers(estate, resultRelInfo);
        if run_as_table_owner {
            RestoreUserContext(ucxt);
        }
        resultRelInfo = resultRelInfo.add(1);
    });

    /*
     * OK, truncate each table.
     */
    mySubid = GetCurrentSubTransactionId();

    foreach!(cell, rels, {
        let rel = crate::nodes::pg_list::lfirst(current_cell!(cell)) as Relation;

        /* Skip partitioned tables as there is nothing to do */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char {
            continue;
        }

        /*
         * Build the lists of foreign tables belonging to each foreign server
         * and pass each list to the foreign data wrapper's callback function.
         * TODO(pg-port): hash table for FDW foreign tables not yet ported
         */
        if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as c_char {
            /* TODO(pg-port): FDW truncation via ForeignTruncateInfo hash */
            continue;
        }

        /*
         * Normally, we need a transaction-safe truncation here.  However, if
         * the table was either created in the current (sub)transaction or has
         * a new relfilenumber in the current (sub)transaction, then we can
         * just truncate it in-place.
         */
        if (*rel).rd_createSubid == mySubid || (*rel).rd_newRelfilelocatorSubid == mySubid {
            /* Immediate, non-rollbackable truncation is OK */
            heap_truncate_one_rel(rel);
        } else {
            let heap_relid: Oid;
            let toast_relid: Oid;

            /*
             * This effectively deletes all rows in the table, and may be done
             * in a serializable transaction.
             */
            CheckTableForSerializableConflictIn(rel);

            /*
             * Need the full transaction-safe pushups.
             * Create a new empty storage file for the relation.
             */
            RelationSetNewRelfilenumber(rel, (*(*rel).rd_rel).relpersistence);

            heap_relid = RelationGetRelid(rel);

            /* The same for the toast table, if any. */
            toast_relid = (*(*rel).rd_rel).reltoastrelid;
            if OidIsValid(toast_relid) {
                let toastrel = relation_open(toast_relid, AccessExclusiveLock);
                RelationSetNewRelfilenumber(toastrel, (*(*toastrel).rd_rel).relpersistence);
                table_close(toastrel, NoLock);
            }

            /* Reconstruct the indexes to match, and we're done. */
            reindex_relation(ptr::null_mut(), heap_relid, 0x0001 /* REINDEX_REL_PROCESS_TOAST */, ptr::null_mut());
        }

        pgstat_count_truncate(rel);
    });

    /* TODO(pg-port): process ft_htab (FDW truncation) */

    /*
     * Restart owned sequences if we were asked to.
     */
    foreach!(cell, seq_relids, {
        let seq_relid = lfirst_oid(current_cell!(cell));
        ResetSequence(seq_relid);
    });

    /* TODO(pg-port): WAL record for logical decoding (xl_heap_truncate) */

    /*
     * Process all AFTER STATEMENT TRUNCATE triggers.
     */
    resultRelInfo = resultRelInfos;
    foreach!(cell, rels, {
        let mut ucxt: *mut std::ffi::c_void = ptr::null_mut();
        if run_as_table_owner {
            SwitchToUntrustedUser((*(*resultRelInfo).ri_RelationDesc).rd_rel as Oid, &mut ucxt);
        }
        ExecASTruncateTriggers(estate, resultRelInfo);
        if run_as_table_owner {
            RestoreUserContext(ucxt);
        }
        resultRelInfo = resultRelInfo.add(1);
    });

    /* Handle queued AFTER triggers */
    AfterTriggerEndQuery(estate);

    /* We can clean up the EState now */
    FreeExecutorState(estate);

    /*
     * Close any rels opened by CASCADE
     */
    let extra_rels = list_difference_ptr(rels, explicit_rels);
    foreach!(cell, extra_rels, {
        let rel = crate::nodes::pg_list::lfirst(current_cell!(cell)) as Relation;
        table_close(rel, NoLock);
    });
}

/*
 * Check that a given relation is safe to truncate.  Subroutine for
 * ExecuteTruncate() and RangeVarCallbackForTruncate().
 */
unsafe fn truncate_check_rel(relid: Oid, reltuple: *mut FormData_pg_class) {
    let relname = NameStr_ref(&(*reltuple).relname);

    /*
     * Only allow truncate on regular tables, foreign tables using foreign
     * data wrappers supporting TRUNCATE and partitioned tables.
     */
    if (*reltuple).relkind == RELKIND_FOREIGN_TABLE as c_char {
        /* TODO(pg-port): FDW routine check for ExecForeignTruncate */
        ereport!(
            ERROR,
            errmsg!("cannot truncate foreign table \"{}\"",
                CStr::from_ptr(relname).to_string_lossy())
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    } else if (*reltuple).relkind != RELKIND_RELATION as c_char
        && (*reltuple).relkind != RELKIND_PARTITIONED_TABLE as c_char
    {
        ereport!(
            ERROR,
            errmsg!("\"{}\" is not a table",
                CStr::from_ptr(relname).to_string_lossy())
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * Most system catalogs can't be truncated at all.
     */
    if !allowSystemTableMods() && IsSystemClass(relid, reltuple) {
        ereport!(
            ERROR,
            errmsg!("permission denied: \"{}\" is a system catalog",
                CStr::from_ptr(relname).to_string_lossy())
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    InvokeObjectTruncateHook(relid);
}

/*
 * Check that current user has the permission to truncate given relation.
 */
unsafe fn truncate_check_perms(relid: Oid, reltuple: *mut FormData_pg_class) {
    let relname = NameStr_ref(&(*reltuple).relname);
    /* Permissions checks */
    let aclresult = pg_class_aclcheck(relid, GetUserId(), 0x0004 /* ACL_TRUNCATE */);
    if aclresult != 0 /* ACLCHECK_OK */ {
        aclcheck_error(aclresult, get_relkind_objtype((*reltuple).relkind), relname);
    }
}

/*
 * Set of extra sanity checks to check if a given relation is safe to
 * truncate.  This is split with truncate_check_rel() as
 * RangeVarCallbackForTruncate() cannot open a Relation yet.
 */
unsafe fn truncate_check_activity(rel: Relation) {
    /*
     * Don't allow truncate on temp tables of other backends ... their local
     * buffer manager is not going to cope.
     */
    if RELATION_IS_OTHER_TEMP(rel) {
        ereport!(
            ERROR,
            errmsg!("cannot truncate temporary tables of other sessions")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /*
     * Also check for active uses of the relation in the current transaction,
     * including open scans and pending AFTER trigger events.
     */
    CheckTableNotInUse(rel, b"TRUNCATE\0".as_ptr() as *const c_char);
}

/*
 * storage_name
 *	  returns the name corresponding to a typstorage/attstorage enum value
 */
unsafe fn storage_name(c: c_char) -> *const c_char {
    match c {
        x if x == crate::catalog::pg_type::TYPSTORAGE_PLAIN => b"PLAIN\0".as_ptr() as *const c_char,
        x if x == crate::catalog::pg_type::TYPSTORAGE_EXTERNAL => b"EXTERNAL\0".as_ptr() as *const c_char,
        x if x == crate::catalog::pg_type::TYPSTORAGE_EXTENDED => b"EXTENDED\0".as_ptr() as *const c_char,
        x if x == crate::catalog::pg_type::TYPSTORAGE_MAIN => b"MAIN\0".as_ptr() as *const c_char,
        _ => b"???\0".as_ptr() as *const c_char,
    }
}

/*----------
 * MergeAttributes
 *		Returns new schema given initial schema and superclasses.
 *
 * Input arguments:
 * 'columns' is the column/attribute definition for the table. (It's a list
 *		of ColumnDef's.) It is destructively changed.
 * 'supers' is a list of OIDs of parent relations, already locked by caller.
 * 'relpersistence' is the persistence type of the table.
 * 'is_partition' tells if the table is a partition.
 *
 * Output arguments:
 * 'supconstr' receives a list of CookedConstraint representing
 *		CHECK constraints belonging to parent relations, updated as
 *		necessary to be valid for the child.
 * 'supnotnulls' receives a list of CookedConstraint representing
 *		not-null constraints based on those from parent relations.
 *
 * Return value:
 * Completed schema list.
 *
 * Notes:
 *	  The order in which the attributes are inherited is very important.
 *	  Intuitively, the inherited attributes should come first. If a table
 *	  inherits from multiple parents, the order of those attributes are
 *	  according to the order of the parents specified in CREATE TABLE.
 *
 *	  Here's an example:
 *
 *		create table person (name text, age int4, location point);
 *		create table emp (salary int4, manager text) inherits(person);
 *		create table student (gpa float8) inherits (person);
 *		create table stud_emp (percent int4) inherits (emp, student);
 *
 *	  The order of the attributes of stud_emp is:
 *
 *							person {1:name, 2:age, 3:location}
 *							/	 \
 *			   {6:gpa}	student   emp {4:salary, 5:manager}
 *							\	 /
 *						   stud_emp {7:percent}
 *
 *	   If the same attribute name appears multiple times, then it appears
 *	   in the result table in the proper location for its first appearance.
 *
 *	   Constraints (including not-null constraints) for the child table
 *	   are the union of all relevant constraints, from both the child schema
 *	   and parent tables.  In addition, in legacy inheritance, each column that
 *	   appears in a primary key in any of the parents also gets a NOT NULL
 *	   constraint (partitioning doesn't need this, because the PK itself gets
 *	   inherited.)
 *
 *	   The default value for a child column is defined as:
 *		(1) If the child schema specifies a default, that value is used.
 *		(2) If neither the child nor any parent specifies a default, then
 *			the column will not have a default.
 *		(3) If conflicting defaults are inherited from different parents
 *			(and not overridden by the child), an error is raised.
 *		(4) Otherwise the inherited default is used.
 *
 *		Note that the default-value infrastructure is used for generated
 *		columns' expressions too, so most of the preceding paragraph applies
 *		to generation expressions too.  We insist that a child column be
 *		generated if and only if its parent(s) are, but it need not have
 *		the same generation expression.
 *----------
 */
unsafe fn MergeAttributes(
    mut columns: *mut List,
    supers: *const List,
    relpersistence: c_char,
    is_partition: bool,
    supconstr: *mut *mut List,
    supnotnulls: *mut *mut List,
) -> *mut List {
    let mut inh_columns: *mut List = crate::nodes::pg_list::NIL;
    let mut constraints: *mut List = crate::nodes::pg_list::NIL;
    let mut nnconstraints: *mut List = crate::nodes::pg_list::NIL;
    let mut have_bogus_defaults = false;
    let mut child_attno: c_int = 0;
    /* bogus_marker: a sentinel Node to flag conflicting defaults */
    let mut bogus_marker_node: crate::nodes::nodes::Node = crate::nodes::nodes::Node { r#type: 0 };
    let bogus_marker: *mut Node = &mut bogus_marker_node as *mut _;
    let mut saved_columns: *mut List = crate::nodes::pg_list::NIL;

    /*
     * Check for and reject tables with too many columns.
     */
    if list_length(columns) > MaxHeapAttributeNumber {
        ereport!(ERROR, errmsg!("tables can have at most {} columns", MaxHeapAttributeNumber)
            /* C also: errcode(ERRCODE_TOO_MANY_COLUMNS) */
        );
    }

    /*
     * Check for duplicate names in the explicit list of attributes.
     * We loop over list indexes to allow deletions.
     */
    let mut coldefpos = 0;
    loop {
        if coldefpos >= list_length(columns) { break; }
        let coldef = list_nth(columns, coldefpos) as *mut ColumnDef;

        if !is_partition && (*coldef).typeName.is_null() {
            ereport!(ERROR,
                errmsg!("column \"{}\" does not exist",
                    CStr::from_ptr((*coldef).colname).to_string_lossy())
                /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
            );
        }

        /* scan all entries beyond coldef */
        let mut restpos = coldefpos + 1;
        loop {
            if restpos >= list_length(columns) { break; }
            let restdef = list_nth(columns, restpos) as *mut ColumnDef;
            if libc_strcmp((*coldef).colname, (*restdef).colname) == 0 {
                if (*coldef).is_from_type {
                    /* merge column options into column from the type */
                    (*coldef).is_not_null = (*restdef).is_not_null;
                    (*coldef).raw_default = (*restdef).raw_default;
                    (*coldef).cooked_default = (*restdef).cooked_default;
                    (*coldef).constraints = (*restdef).constraints;
                    (*coldef).is_from_type = false;
                    columns = list_delete_nth_cell(columns, restpos);
                    /* don't increment restpos; list got shorter */
                } else {
                    ereport!(ERROR,
                        errmsg!("column \"{}\" specified more than once",
                            CStr::from_ptr((*coldef).colname).to_string_lossy())
                        /* C also: errcode(ERRCODE_DUPLICATE_COLUMN) */
                    );
                }
            } else {
                restpos += 1;
            }
        }
        coldefpos += 1;
    }

    /*
     * For a partition, set aside explicit column defs; we process them later.
     */
    if is_partition {
        saved_columns = columns;
        columns = crate::nodes::pg_list::NIL;
    }

    /*
     * Scan the parents left-to-right, merging their attributes.
     */
    foreach!(lc, supers as *mut List, {
        let parent: Oid = lfirst_oid(current_cell!(lc));

        /* caller already got lock */
        let relation = table_open(parent, NoLock);

        /*
         * Check for active uses of parent partitioned table.
         */
        if is_partition {
            CheckTableNotInUse(relation, b"CREATE TABLE .. PARTITION OF\0".as_ptr() as *const c_char);
        }

        /*
         * We do not allow partitioned tables and partitions to participate
         * in regular inheritance.
         */
        if (*(*relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char && !is_partition {
            ereport!(ERROR,
                errmsg!("cannot inherit from partitioned table \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            );
        }
        if (*(*relation).rd_rel).relispartition && !is_partition {
            ereport!(ERROR,
                errmsg!("cannot inherit from partition \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            );
        }

        let relkind = (*(*relation).rd_rel).relkind;
        if relkind != RELKIND_RELATION as c_char
            && relkind != RELKIND_FOREIGN_TABLE as c_char
            && relkind != RELKIND_PARTITIONED_TABLE as c_char
        {
            ereport!(ERROR,
                errmsg!("inherited relation \"{}\" is not a table or foreign table",
                    CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            );
        }

        /* partition cannot be temporary if parent is permanent */
        if is_partition
            && (*(*relation).rd_rel).relpersistence != RELPERSISTENCE_TEMP
            && relpersistence == RELPERSISTENCE_TEMP
        {
            ereport!(ERROR,
                errmsg!("cannot create a temporary relation as partition of permanent relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            );
        }

        /* permanent rel cannot inherit from temp */
        if relpersistence != RELPERSISTENCE_TEMP
            && (*(*relation).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        {
            if !is_partition {
                ereport!(ERROR,
                    errmsg!("cannot inherit from temporary relation \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                );
            } else {
                ereport!(ERROR,
                    errmsg!("cannot create a permanent relation as partition of temporary relation \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                );
            }
        }

        /* temp rel must belong to this session */
        if (*(*relation).rd_rel).relpersistence == RELPERSISTENCE_TEMP
            && !(*relation).rd_islocaltemp
        {
            if !is_partition {
                ereport!(ERROR,
                    errmsg!("cannot inherit from temporary relation of another session")
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                );
            } else {
                ereport!(ERROR,
                    errmsg!("cannot create as partition of temporary relation of another session")
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                );
            }
        }

        if !object_ownercheck(RelationRelationId, RelationGetRelid(relation), GetUserId()) {
            aclcheck_error(ACLCHECK_NOT_OWNER(), get_relkind_objtype((*(*relation).rd_rel).relkind),
                           RelationGetRelationName(relation));
        }

        let tupleDesc = RelationGetDescr(relation);
        let constr = (*tupleDesc).constr;

        /*
         * newattmap->attnums[] will contain child-table attribute numbers
         * for attributes of this parent table.
         */
        let newattmap = make_attrmap((*tupleDesc).natts);

        /* we can't process inherited defaults until newattmap is complete */
        let mut inherited_defaults: *mut List = crate::nodes::pg_list::NIL;
        let mut cols_with_defaults: *mut List = crate::nodes::pg_list::NIL;

        /*
         * Request attnotnull on columns with not-null constraint.
         */
        let nnconstrs = RelationGetNotNullConstraints(RelationGetRelid(relation), true, false);
        let mut nncols: Bitmapset = ptr::null_mut();
        foreach!(lnn, nnconstrs, {
            let cc = crate::nodes::pg_list::lfirst(current_cell!(lnn)) as *mut CookedConstraint;
            nncols = bms_add_member(nncols, (*cc).attnum as c_int);
        });

        let mut parent_attno: i16 = 1;
        while parent_attno <= (*tupleDesc).natts as i16 {
            let attribute = TupleDescAttr(tupleDesc, (parent_attno - 1) as usize);
            let attributeName = NameStr_ref(&(*attribute).attname);

            /* ignore dropped columns */
            if (*attribute).attisdropped {
                parent_attno += 1;
                continue;
            }

            /* create new column definition */
            let newdef = makeColumnDef(attributeName, (*attribute).atttypid,
                                       (*attribute).atttypmod, (*attribute).attcollation);
            (*newdef).storage = (*attribute).attstorage;
            (*newdef).generated = (*attribute).attgenerated;
            if CompressionMethodIsValid((*attribute).attcompression) {
                (*newdef).compression = pstrdup(GetCompressionMethodName((*attribute).attcompression));
            }

            /* partitions inherit identity column */
            if is_partition {
                (*newdef).identity = (*attribute).attidentity;
            }

            /* does it match some previously considered column from another parent? */
            let exist_attno = findAttrByName(attributeName, inh_columns);
            let mergeddef: *mut ColumnDef;
            if exist_attno > 0 {
                /* merge */
                mergeddef = MergeInheritedAttribute(inh_columns, exist_attno, newdef);
                (*newattmap).attnums[(parent_attno - 1) as usize] = exist_attno as i16;
                /* partitions have only one parent, conflict can't occur */
            } else {
                /* new inherited column */
                (*newdef).inhcount = 1;
                (*newdef).is_local = false;
                inh_columns = lappend(inh_columns as *mut std::ffi::c_void, newdef as *mut std::ffi::c_void) as *mut List;
                child_attno += 1;
                (*newattmap).attnums[(parent_attno - 1) as usize] = child_attno as i16;
                mergeddef = newdef;
            }

            /* mark attnotnull if parent has it */
            if bms_is_member(parent_attno as c_int, nncols) {
                (*mergeddef).is_not_null = true;
            }

            /* locate default/generation expression if any */
            if (*attribute).atthasdef {
                let this_default = TupleDescGetDefault(tupleDesc, parent_attno);
                if this_default.is_null() {
                    ereport!(ERROR,
                        errmsg!("default expression not found for attribute {} of relation \"{}\"",
                            parent_attno,
                            CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy())
                    );
                }
                inherited_defaults = lappend(inherited_defaults as *mut std::ffi::c_void, this_default as *mut std::ffi::c_void) as *mut List;
                cols_with_defaults = lappend(cols_with_defaults as *mut std::ffi::c_void, mergeddef as *mut std::ffi::c_void) as *mut List;
            }

            parent_attno += 1;
        }

        /*
         * Process inherited default expressions, adjusting attnos.
         */
        crate::forboth!(lc1, inherited_defaults, lc2, cols_with_defaults, {
            let this_default = crate::nodes::pg_list::lfirst(current_cell!(lc1)) as *mut Node;
            let def = crate::nodes::pg_list::lfirst(current_cell!(lc2)) as *mut ColumnDef;
            let mut found_whole_row: bool = false;

            let this_default = map_variable_attnos(this_default, 1, 0, newattmap,
                                                   InvalidOid, &mut found_whole_row);
            if found_whole_row {
                ereport!(ERROR,
                    errmsg!("cannot convert whole-row table reference")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errdetail */
                );
            }

            /* if already had a default, check if same */
            if (*def).cooked_default.is_null() {
                (*def).cooked_default = this_default;
            } else if !equal((*def).cooked_default, this_default) {
                (*def).cooked_default = bogus_marker;
                have_bogus_defaults = true;
            }
        });

        /*
         * Copy CHECK constraints of this parent, adjusting attnos.
         */
        if !constr.is_null() && (*constr).num_check > 0 {
            let check = (*constr).check;
            let mut i = 0;
            while i < (*constr).num_check as usize {
                let name = (*check.add(i)).ccname;
                /* ignore non-inheritable */
                if (*check.add(i)).ccnoinherit {
                    i += 1;
                    continue;
                }
                let mut found_whole_row: bool = false;
                let expr = map_variable_attnos(
                    stringToNode((*check.add(i)).ccbin),
                    1, 0, newattmap, InvalidOid, &mut found_whole_row);
                if found_whole_row {
                    ereport!(ERROR,
                        errmsg!("cannot convert whole-row table reference")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errdetail */
                    );
                }
                constraints = MergeCheckConstraint(constraints, name, expr,
                                                   (*check.add(i)).ccenforced);
                i += 1;
            }
        }

        /*
         * Copy not-null constraints from this parent.
         */
        foreach!(lnn2, nnconstrs, {
            let nn = crate::nodes::pg_list::lfirst(current_cell!(lnn2)) as *mut CookedConstraint;
            (*nn).attnum = (*newattmap).attnums[((*nn).attnum - 1) as usize];
            nnconstraints = lappend(nnconstraints as *mut std::ffi::c_void, nn as *mut std::ffi::c_void) as *mut List;
        });

        free_attrmap(newattmap);
        bms_free(nncols);

        /*
         * Close parent rel, keep lock until xact commit.
         */
        table_close(relation, NoLock);
    });

    /*
     * If we had inherited attributes, merge declared columns into them.
     */
    if !inh_columns.is_null() && list_length(inh_columns) > 0 {
        let mut newcol_attno: c_int = 0;

        foreach!(lc2, columns, {
            let newdef = crate::nodes::pg_list::lfirst(current_cell!(lc2)) as *mut ColumnDef;
            let attributeName = (*newdef).colname;

            newcol_attno += 1;

            let exist_attno = findAttrByName(attributeName, inh_columns);
            if exist_attno > 0 {
                /* merge */
                MergeChildAttribute(inh_columns, exist_attno, newcol_attno, newdef);
            } else {
                /* new column, attach unchanged */
                inh_columns = lappend(inh_columns as *mut std::ffi::c_void, newdef as *mut std::ffi::c_void) as *mut List;
            }
        });

        columns = inh_columns;

        if list_length(columns) > MaxHeapAttributeNumber {
            ereport!(ERROR, errmsg!("tables can have at most {} columns", MaxHeapAttributeNumber)
                /* C also: errcode(ERRCODE_TOO_MANY_COLUMNS) */
            );
        }
    }

    /*
     * For partitions, check that saved column constraints reference existing columns.
     */
    if is_partition {
        foreach!(lc3, saved_columns, {
            let restdef = crate::nodes::pg_list::lfirst(current_cell!(lc3)) as *mut ColumnDef;
            let mut found = false;

            foreach!(linner, columns, {
                let coldef = crate::nodes::pg_list::lfirst(current_cell!(linner)) as *mut ColumnDef;
                if libc_strcmp((*coldef).colname, (*restdef).colname) == 0 {
                    found = true;

                    /* check generated column conflicts */
                    if (*coldef).generated != 0 {
                        if !(*restdef).raw_default.is_null() && (*restdef).generated == 0 {
                            ereport!(ERROR,
                                errmsg!("column \"{}\" inherits from generated column but specifies default",
                                    CStr::from_ptr((*restdef).colname).to_string_lossy())
                                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION) */
                            );
                        }
                        if (*restdef).identity != 0 {
                            ereport!(ERROR,
                                errmsg!("column \"{}\" inherits from generated column but specifies identity",
                                    CStr::from_ptr((*restdef).colname).to_string_lossy())
                                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION) */
                            );
                        }
                    } else if (*restdef).generated != 0 {
                        ereport!(ERROR,
                            errmsg!("child column \"{}\" specifies generation expression",
                                CStr::from_ptr((*restdef).colname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errhint */
                        );
                    }

                    if (*coldef).generated != 0 && (*restdef).generated != 0
                        && (*restdef).generated != (*coldef).generated
                    {
                        ereport!(ERROR,
                            errmsg!("column \"{}\" inherits from generated column of different kind",
                                CStr::from_ptr((*restdef).colname).to_string_lossy())
                            /* C also: errcode, errdetail */
                        );
                    }

                    /* override parent default with partition local definition */
                    if !(*restdef).raw_default.is_null() {
                        (*coldef).raw_default = (*restdef).raw_default;
                        (*coldef).cooked_default = ptr::null_mut();
                    }
                }
            });

            if !found {
                ereport!(ERROR,
                    errmsg!("column \"{}\" does not exist",
                        CStr::from_ptr((*restdef).colname).to_string_lossy())
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
                );
            }
        });
    }

    /*
     * Check for conflicting parent default values not overridden by child.
     */
    if have_bogus_defaults {
        foreach!(lc4, columns, {
            let def = crate::nodes::pg_list::lfirst(current_cell!(lc4)) as *mut ColumnDef;
            if (*def).cooked_default == bogus_marker {
                if (*def).generated != 0 {
                    ereport!(ERROR,
                        errmsg!("column \"{}\" inherits conflicting generation expressions",
                            CStr::from_ptr((*def).colname).to_string_lossy())
                        /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errhint */
                    );
                } else {
                    ereport!(ERROR,
                        errmsg!("column \"{}\" inherits conflicting default values",
                            CStr::from_ptr((*def).colname).to_string_lossy())
                        /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errhint */
                    );
                }
            }
        });
    }

    *supconstr = constraints;
    *supnotnulls = nnconstraints;

    columns
}

/*
 * MergeCheckConstraint
 *		Try to merge an inherited CHECK constraint with previous ones
 *
 * If we inherit identically-named constraints from multiple parents, we must
 * merge them, or throw an error if they don't have identical definitions.
 *
 * constraints is a list of CookedConstraint structs for previous constraints.
 *
 * If the new constraint matches an existing one, then the existing
 * constraint's inheritance count is updated.  If there is a conflict (same
 * name but different expression), throw an error.  If the constraint neither
 * matches nor conflicts with an existing one, a new constraint is appended to
 * the list.
 */
unsafe fn MergeCheckConstraint(
    constraints: *mut List,
    name: *const c_char,
    expr: *mut Node,
    is_enforced: bool,
) -> *mut List {
    foreach!(lc, constraints, {
        let ccon = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut CookedConstraint;

        /* Assert(ccon->contype == CONSTR_CHECK) */

        /* Non-matching names never conflict */
        if libc_strcmp((*ccon).name, name) != 0 {
            continue;
        }

        if equal(expr as *mut Node, (*ccon).expr as *mut Node) {
            /* OK to merge constraint with existing */
            /* check for overflow; CookedConstraint.inhcount is c_int */
            if (*ccon).inhcount == i32::MAX {
                ereport!(ERROR, errmsg!("too many inheritance parents")
                    /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
                );
            }
            (*ccon).inhcount += 1;

            /*
             * When enforceability differs, the merged constraint should be
             * marked as ENFORCED because one of the parents is ENFORCED.
             */
            if !(*ccon).is_enforced && is_enforced {
                (*ccon).is_enforced = true;
                (*ccon).skip_validation = false;
            }

            return constraints;
        }

        ereport!(ERROR,
            errmsg!("check constraint name \"{}\" appears multiple times but with different expressions",
                CStr::from_ptr(name).to_string_lossy())
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        );
    });

    /*
     * Constraint couldn't be merged with an existing one and also didn't
     * conflict with an existing one, so add it as a new one to the list.
     */
    let newcon = palloc0(std::mem::size_of::<CookedConstraint>()) as *mut CookedConstraint;
    (*newcon).contype = ConstrType::CONSTR_CHECK;
    (*newcon).name = pstrdup(name);
    (*newcon).expr = expr as *mut std::ffi::c_void as *mut Node;
    (*newcon).inhcount = 1;
    (*newcon).is_enforced = is_enforced;
    (*newcon).skip_validation = !is_enforced;
    lappend(constraints as *mut std::ffi::c_void, newcon as *mut std::ffi::c_void) as *mut List
}

/*
 * MergeChildAttribute
 *		Merge given child attribute definition into given inherited attribute.
 *
 * Input arguments:
 * 'inh_columns' is the list of inherited ColumnDefs.
 * 'exist_attno' is the number of the inherited attribute in inh_columns
 * 'newcol_attno' is the attribute number in child table's schema definition
 * 'newdef' is the column/attribute definition from the child table.
 *
 * The ColumnDef in 'inh_columns' list is modified.  The child attribute's
 * ColumnDef remains unchanged.
 *
 * Notes:
 * - The attribute is merged according to the rules laid out in the prologue
 *   of MergeAttributes().
 * - If matching inherited attribute exists but the child attribute can not be
 *   merged into it, the function throws respective errors.
 * - A partition can not have its own column definitions. Hence this function
 *   is applicable only to a regular inheritance child.
 */
unsafe fn MergeChildAttribute(
    inh_columns: *mut List,
    exist_attno: c_int,
    newcol_attno: c_int,
    newdef: *const ColumnDef,
) {
    let attributeName = (*newdef).colname;

    if exist_attno == newcol_attno {
        ereport!(NOTICE,
            errmsg!("merging column \"{}\" with inherited definition",
                CStr::from_ptr(attributeName).to_string_lossy())
        );
    } else {
        ereport!(NOTICE,
            errmsg!("moving and merging column \"{}\" with inherited definition",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errdetail */
        );
    }

    let inhdef = list_nth_node_ColumnDef(inh_columns, exist_attno - 1);

    /*
     * Must have the same type and typmod
     */
    let mut inhtypeid: Oid = 0;
    let mut inhtypmod: i32 = 0;
    let mut newtypeid: Oid = 0;
    let mut newtypmod: i32 = 0;
    typenameTypeIdAndMod(ptr::null_mut(), (*inhdef).typeName, &mut inhtypeid, &mut inhtypmod);
    typenameTypeIdAndMod(ptr::null_mut(), (*newdef).typeName, &mut newtypeid, &mut newtypmod);
    if inhtypeid != newtypeid || inhtypmod != newtypmod {
        ereport!(ERROR,
            errmsg!("column \"{}\" has a type conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail */
        );
    }

    /*
     * Must have the same collation
     */
    let inhcollid = GetColumnDefCollation(ptr::null_mut(), inhdef, inhtypeid);
    let newcollid = GetColumnDefCollation(ptr::null_mut(), newdef, newtypeid);
    if inhcollid != newcollid {
        ereport!(ERROR,
            errmsg!("column \"{}\" has a collation conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_COLLATION_MISMATCH), errdetail */
        );
    }

    /*
     * Identity is never inherited by a regular inheritance child. Pick
     * child's identity definition if there's one.
     */
    (*inhdef).identity = (*newdef).identity;

    /*
     * Copy storage parameter
     */
    if (*inhdef).storage == 0 {
        (*inhdef).storage = (*newdef).storage;
    } else if (*newdef).storage != 0 && (*inhdef).storage != (*newdef).storage {
        ereport!(ERROR,
            errmsg!("column \"{}\" has a storage parameter conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail */
        );
    }

    /*
     * Copy compression parameter
     */
    if (*inhdef).compression.is_null() {
        (*inhdef).compression = (*newdef).compression;
    } else if !(*newdef).compression.is_null() {
        if libc_strcmp((*inhdef).compression, (*newdef).compression) != 0 {
            ereport!(ERROR,
                errmsg!("column \"{}\" has a compression method conflict",
                    CStr::from_ptr(attributeName).to_string_lossy())
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail */
            );
        }
    }

    /*
     * Merge of not-null constraints = OR 'em together
     */
    (*inhdef).is_not_null |= (*newdef).is_not_null;

    /*
     * Check for conflicts related to generated columns.
     */
    if (*inhdef).generated != 0 {
        if !(*newdef).raw_default.is_null() && (*newdef).generated == 0 {
            ereport!(ERROR,
                errmsg!("column \"{}\" inherits from generated column but specifies default",
                    CStr::from_ptr((*inhdef).colname).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION) */
            );
        }
        if (*newdef).identity != 0 {
            ereport!(ERROR,
                errmsg!("column \"{}\" inherits from generated column but specifies identity",
                    CStr::from_ptr((*inhdef).colname).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION) */
            );
        }
    } else {
        if (*newdef).generated != 0 {
            ereport!(ERROR,
                errmsg!("child column \"{}\" specifies generation expression",
                    CStr::from_ptr((*inhdef).colname).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errhint */
            );
        }
    }

    if (*inhdef).generated != 0 && (*newdef).generated != 0 && (*newdef).generated != (*inhdef).generated {
        ereport!(ERROR,
            errmsg!("column \"{}\" inherits from generated column of different kind",
                CStr::from_ptr((*inhdef).colname).to_string_lossy())
            /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errdetail */
        );
    }

    /*
     * If new def has a default, override previous default
     */
    if !(*newdef).raw_default.is_null() {
        (*inhdef).raw_default = (*newdef).raw_default;
        (*inhdef).cooked_default = (*newdef).cooked_default;
    }

    /* Mark the column as locally defined */
    (*inhdef).is_local = true;
}

/*
 * MergeInheritedAttribute
 *		Merge given parent attribute definition into specified attribute
 *		inherited from the previous parents.
 *
 * Input arguments:
 * 'inh_columns' is the list of previously inherited ColumnDefs.
 * 'exist_attno' is the number the existing matching attribute in inh_columns.
 * 'newdef' is the new parent column/attribute definition to be merged.
 *
 * The matching ColumnDef in 'inh_columns' list is modified and returned.
 *
 * Notes:
 * - The attribute is merged according to the rules laid out in the prologue
 *   of MergeAttributes().
 * - If matching inherited attribute exists but the new attribute can not be
 *   merged into it, the function throws respective errors.
 * - A partition inherits from only a single parent. Hence this function is
 *   applicable only to a regular inheritance.
 */
unsafe fn MergeInheritedAttribute(
    inh_columns: *mut List,
    exist_attno: c_int,
    newdef: *const ColumnDef,
) -> *mut ColumnDef {
    let attributeName = (*newdef).colname;

    ereport!(NOTICE,
        errmsg!("merging multiple inherited definitions of column \"{}\"",
            CStr::from_ptr(attributeName).to_string_lossy())
    );
    let prevdef = list_nth_node_ColumnDef(inh_columns, exist_attno - 1);

    /*
     * Must have the same type and typmod
     */
    let mut prevtypeid: Oid = 0;
    let mut prevtypmod: i32 = 0;
    let mut newtypeid: Oid = 0;
    let mut newtypmod: i32 = 0;
    typenameTypeIdAndMod(ptr::null_mut(), (*prevdef).typeName, &mut prevtypeid, &mut prevtypmod);
    typenameTypeIdAndMod(ptr::null_mut(), (*newdef).typeName, &mut newtypeid, &mut newtypmod);
    if prevtypeid != newtypeid || prevtypmod != newtypmod {
        ereport!(ERROR,
            errmsg!("inherited column \"{}\" has a type conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail */
        );
    }

    /*
     * Must have the same collation
     */
    let prevcollid = GetColumnDefCollation(ptr::null_mut(), prevdef, prevtypeid);
    let newcollid = GetColumnDefCollation(ptr::null_mut(), newdef, newtypeid);
    if prevcollid != newcollid {
        ereport!(ERROR,
            errmsg!("inherited column \"{}\" has a collation conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_COLLATION_MISMATCH), errdetail */
        );
    }

    /*
     * Copy/check storage parameter
     */
    if (*prevdef).storage == 0 {
        (*prevdef).storage = (*newdef).storage;
    } else if (*prevdef).storage != (*newdef).storage {
        ereport!(ERROR,
            errmsg!("inherited column \"{}\" has a storage parameter conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail */
        );
    }

    /*
     * Copy/check compression parameter
     */
    if (*prevdef).compression.is_null() {
        (*prevdef).compression = (*newdef).compression;
    } else if !(*newdef).compression.is_null() {
        if libc_strcmp((*prevdef).compression, (*newdef).compression) != 0 {
            ereport!(ERROR,
                errmsg!("column \"{}\" has a compression method conflict",
                    CStr::from_ptr(attributeName).to_string_lossy())
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail */
            );
        }
    }

    /*
     * Check for GENERATED conflicts
     */
    if (*prevdef).generated != (*newdef).generated {
        ereport!(ERROR,
            errmsg!("inherited column \"{}\" has a generation conflict",
                CStr::from_ptr(attributeName).to_string_lossy())
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        );
    }

    /*
     * Default and other constraints are handled by the caller.
     */

    /* check for overflow; ColumnDef.inhcount is int16 */
    if (*prevdef).inhcount == i16::MAX {
        ereport!(ERROR, errmsg!("too many inheritance parents")
            /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        );
    }
    (*prevdef).inhcount += 1;

    prevdef
}

/*
 * StoreCatalogInheritance
 *		Updates the system catalogs with proper inheritance information.
 *
 * supers is a list of the OIDs of the new relation's direct ancestors.
 */
unsafe fn StoreCatalogInheritance(
    relationId: Oid,
    supers: *mut List,
    child_is_partition: bool,
) {
    /* sanity checks */
    /* Assert(OidIsValid(relationId)) */

    if supers.is_null() || list_length(supers) == 0 {
        return;
    }

    /*
     * Store INHERITS information in pg_inherits using direct ancestors only.
     * Also enter dependencies on the direct ancestors, and make sure they are
     * marked with relhassubclass = true.
     */
    let relation = table_open(InheritsRelationId, RowExclusiveLock);

    let mut seqNumber: i32 = 1;
    foreach!(entry, supers, {
        let parentOid = lfirst_oid(current_cell!(entry));
        StoreCatalogInheritance1(relationId, parentOid, seqNumber, relation, child_is_partition);
        seqNumber += 1;
    });

    table_close(relation, RowExclusiveLock);
}

/*
 * Make catalog entries showing relationId as being an inheritance child
 * of parentOid.  inhRelation is the already-opened pg_inherits catalog.
 */
unsafe fn StoreCatalogInheritance1(
    relationId: Oid,
    parentOid: Oid,
    seqNumber: i32,
    inhRelation: Relation,
    child_is_partition: bool,
) {
    let mut childobject = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut parentobject = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

    /* store the pg_inherits row */
    StoreSingleInheritance(relationId, parentOid, seqNumber);

    /* Store a dependency too */
    parentobject.classId = RelationRelationId;
    parentobject.objectId = parentOid;
    parentobject.objectSubId = 0;
    childobject.classId = RelationRelationId;
    childobject.objectId = relationId;
    childobject.objectSubId = 0;

    /* child_dependency_type(child_is_partition) => DEPENDENCY_AUTO or DEPENDENCY_NORMAL */
    let deptype: c_int = if child_is_partition { 2 /* DEPENDENCY_AUTO */ } else { 1 /* DEPENDENCY_NORMAL */ };
    recordDependencyOn(&childobject, &parentobject, deptype);

    /*
     * Post creation hook of this inheritance.
     */
    InvokeObjectPostAlterHookArg(InheritsRelationId, relationId, 0, parentOid, false);

    /*
     * Mark the parent as having subclasses.
     */
    SetRelationHasSubclass(parentOid, true);
}

/*
 * Look for an existing column entry with the given name.
 *
 * Returns the index (starting with 1) if attribute already exists in columns,
 * 0 if it doesn't.
 */
unsafe fn findAttrByName(attributeName: *const c_char, columns: *const List) -> c_int {
    let mut i: c_int = 1;
    foreach!(lc, columns as *mut List, {
        let coldef = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *const ColumnDef;
        if libc_strcmp(attributeName, (*coldef).colname) == 0 {
            return i;
        }
        i += 1;
    });
    0
}

/*
 * SetRelationHasSubclass
 *		Set the value of the relation's relhassubclass field in pg_class.
 *
 * It's always safe to set this field to true, because all SQL commands are
 * ready to see true and then find no children.  On the other hand, commands
 * generally assume zero children if this is false.
 *
 * Caller must hold any self-exclusive lock until end of transaction.  If the
 * new value is false, caller must have acquired that lock before reading the
 * evidence that justified the false value.  That way, it properly waits if
 * another backend is simultaneously concluding no need to change the tuple
 * (new and old values are true).
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing plans
 * referencing the relation to be rebuilt with the new list of children.
 * This must happen even if we find that no change is needed in the pg_class
 * row.
 */
pub unsafe fn SetRelationHasSubclass(relationId: Oid, relhassubclass: bool) {
    /* Assert(CheckRelationOidLockedByMe(relationId, ShareUpdateExclusiveLock, false) || ...ShareRowExclusiveLock...) */

    /*
     * Fetch a modifiable copy of the tuple, modify it, update pg_class.
     */
    let relationRelation = table_open(RelationRelationId, RowExclusiveLock);
    let tuple = SearchSysCacheCopy1(0 /* RELOID */, ObjectIdGetDatum(relationId));
    if !HeapTupleIsValid(tuple) {
        /* elog(ERROR, "cache lookup failed for relation %u", relationId) */
        ereport!(ERROR, errmsg!("cache lookup failed for relation {}", relationId));
    }
    let classtuple = GETSTRUCT(tuple) as *mut FormData_pg_class;

    if (*classtuple).relhassubclass != relhassubclass {
        (*classtuple).relhassubclass = relhassubclass;
        CatalogTupleUpdate(relationRelation, ptr::null() /* &tuple->t_self */, tuple);
    } else {
        /* no need to change tuple, but force relcache rebuild anyway */
        CacheInvalidateRelcacheByTuple(tuple);
    }

    heap_freetuple(tuple);
    table_close(relationRelation, RowExclusiveLock);
}

/*
 * CheckRelationTableSpaceMove
 *		Check if relation can be moved to new tablespace.
 *
 * NOTE: The caller must hold AccessExclusiveLock on the relation.
 *
 * Returns true if the relation can be moved to the new tablespace; raises
 * an error if it is not possible to do the move; returns false if the move
 * would have no effect.
 */
pub unsafe fn CheckRelationTableSpaceMove(rel: Relation, newTableSpaceId: Oid) -> bool {
    /*
     * No work if no change in tablespace.  Note that MyDatabaseTableSpace is
     * stored as 0.
     */
    let oldTableSpaceId = (*(*rel).rd_rel).reltablespace;
    let MyDatabaseTableSpace: Oid = 0; /* TODO(pg-port): global variable stub */
    if newTableSpaceId == oldTableSpaceId
        || (newTableSpaceId == MyDatabaseTableSpace && oldTableSpaceId == 0)
    {
        return false;
    }

    /*
     * We cannot support moving mapped relations into different tablespaces.
     * (In particular this eliminates all shared catalogs.)
     */
    if RelationIsMapped(rel) {
        ereport!(
            ERROR,
            errmsg!("cannot move system relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Cannot move a non-shared relation into pg_global */
    let GLOBALTABLESPACE_OID: Oid = 1664; /* TODO(pg-port): constant */
    if newTableSpaceId == GLOBALTABLESPACE_OID {
        ereport!(
            ERROR,
            errmsg!("only shared relations can be placed in pg_global tablespace")
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    /*
     * Do not allow moving temp tables of other backends.
     */
    if RELATION_IS_OTHER_TEMP(rel) {
        ereport!(
            ERROR,
            errmsg!("cannot move temporary tables of other sessions")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    true
}

/*
 * SetRelationTableSpace
 *		Set new reltablespace and relfilenumber in pg_class entry.
 *
 * newTableSpaceId is the new tablespace for the relation, and
 * newRelFilenumber its new filenumber.  If newRelFilenumber is
 * InvalidRelFileNumber, this field is not updated.
 *
 * NOTE: The caller must hold AccessExclusiveLock on the relation.
 *
 * The caller of this routine had better check if a relation can be
 * moved to this new tablespace by calling CheckRelationTableSpaceMove()
 * first, and is responsible for making the change visible with
 * CommandCounterIncrement().
 */
pub unsafe fn SetRelationTableSpace(
    rel: Relation,
    newTableSpaceId: Oid,
    newRelFilenumber: RelFileNumber,
) {
    /* Assert(CheckRelationTableSpaceMove(rel, newTableSpaceId)) */

    let reloid = RelationGetRelid(rel);

    /* Get a modifiable copy of the relation's pg_class row. */
    let pg_class = table_open(RelationRelationId, RowExclusiveLock);

    let tuple = SearchSysCacheLockedCopy1(0 /* RELOID */, ObjectIdGetDatum(reloid));
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!("cache lookup failed for relation {}", reloid));
    }
    let otid = (*tuple).t_self; /* ItemPointerData */
    let rd_rel = GETSTRUCT(tuple) as *mut FormData_pg_class;

    let MyDatabaseTableSpace: Oid = MyDatabaseTableSpace_get();

    /* Update the pg_class row. */
    (*rd_rel).reltablespace = if newTableSpaceId == MyDatabaseTableSpace {
        InvalidOid
    } else {
        newTableSpaceId
    };
    if RelFileNumberIsValid(newRelFilenumber) {
        (*rd_rel).relfilenode = newRelFilenumber;
    }
    CatalogTupleUpdate(pg_class, &otid as *const _ as *const std::ffi::c_void, tuple);
    UnlockTuple(pg_class, &otid as *const _ as *const std::ffi::c_void, 0 /* InplaceUpdateTupleLock */);

    /*
     * Record dependency on tablespace.  This is only required for relations
     * that have no physical storage.
     */
    if !RELKIND_HAS_STORAGE((*(*rel).rd_rel).relkind) {
        changeDependencyOnTablespace(RelationRelationId, reloid, (*rd_rel).reltablespace);
    }

    heap_freetuple(tuple);
    table_close(pg_class, RowExclusiveLock);
}

/*
 *		renameatt_check			- basic sanity checks before attribute rename
 */
unsafe fn renameatt_check(
    myrelid: Oid,
    classform: *mut FormData_pg_class,
    recursing: bool,
) {
    let relkind = (*classform).relkind;

    if (*classform).reloftype != 0 && !recursing {
        ereport!(ERROR, errmsg!("cannot rename column of typed table") /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */);
    }

    /*
     * Renaming the columns of sequences or toast tables doesn't actually
     * break anything from the system's point of view, since internal
     * references are by attnum.  But it doesn't seem right to allow users to
     * change names that are hardcoded into the system, hence the following
     * restriction.
     */
    if relkind != RELKIND_RELATION as c_char
        && relkind != RELKIND_VIEW as c_char
        && relkind != RELKIND_MATVIEW as c_char
        && relkind != RELKIND_COMPOSITE_TYPE as c_char
        && relkind != RELKIND_INDEX as c_char
        && relkind != RELKIND_PARTITIONED_INDEX as c_char
        && relkind != RELKIND_FOREIGN_TABLE as c_char
        && relkind != RELKIND_PARTITIONED_TABLE as c_char
    {
        ereport!(ERROR,
            errmsg!("cannot rename columns of relation \"{}\"",
                CStr::from_ptr(NameStr_ref(&(*classform).relname)).to_string_lossy())
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errdetail_relkind_not_supported */
        );
    }

    /*
     * permissions checking.  only the owner of a class can change its schema.
     */
    if !object_ownercheck(RelationRelationId, myrelid, GetUserId()) {
        aclcheck_error(0 /* ACLCHECK_NOT_OWNER */, get_relkind_objtype(get_rel_relkind(myrelid)),
            NameStr_ref(&(*classform).relname));
    }
    if !allowSystemTableMods() && IsSystemClass(myrelid, classform) {
        ereport!(ERROR,
            errmsg!("permission denied: \"{}\" is a system catalog",
                CStr::from_ptr(NameStr_ref(&(*classform).relname)).to_string_lossy())
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }
}

/*
 *		renameatt_internal		- workhorse for renameatt
 *
 * Return value is the attribute number in the 'myrelid' relation.
 */
unsafe fn renameatt_internal(
    myrelid: Oid,
    oldattname: *const c_char,
    newattname: *const c_char,
    recurse: bool,
    recursing: bool,
    expected_parents: c_int,
    behavior: DropBehavior,
) -> AttrNumber {
    /*
     * Grab an exclusive lock on the target table, which we will NOT release
     * until end of transaction.
     */
    let targetrelation = relation_open(myrelid, AccessExclusiveLock);
    renameatt_check(myrelid, RelationGetForm(targetrelation), recursing);

    /*
     * if the 'recurse' flag is set then we are supposed to rename this
     * attribute in all classes that inherit from 'relname' (as well as in
     * 'relname').
     *
     * any permissions or problems with duplicate attributes will cause the
     * whole transaction to abort, which is what we want -- all or nothing.
     */
    if recurse {
        let mut child_numparents: *mut i32 = ptr::null_mut();
        let child_oids = find_all_inheritors(myrelid, AccessExclusiveLock, &mut child_numparents as *mut *mut i32 as *mut i32);

        /*
         * find_all_inheritors does the recursive search of the inheritance
         * hierarchy, so all we have to do is process all of the relids in the
         * list that it returns.
         */
        crate::forboth!(lo, child_oids, li, child_numparents, {
            let childrelid = lfirst_oid(lo);
            let numparents = lfirst_int(li);

            if childrelid == myrelid {
                /* skip the parent, it's handled below */
            } else {
                /* note we need not recurse again */
                renameatt_internal(childrelid, oldattname, newattname, false, true, numparents, behavior);
            }
        });
    } else {
        /*
         * If we are told not to recurse, there had better not be any child
         * tables; else the rename would put them out of step.
         *
         * expected_parents will only be 0 if we are not already recursing.
         */
        if expected_parents == 0
            && !find_inheritance_children(myrelid, NoLock).is_null()
        {
            ereport!(ERROR,
                errmsg!("inherited column \"{}\" must be renamed in child tables too",
                    CStr::from_ptr(oldattname).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }
    }

    /* rename attributes in typed tables of composite type */
    if (*(*targetrelation).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as c_char {
        let child_oids = find_typed_table_dependencies(
            (*(*targetrelation).rd_rel).reltype,
            RelationGetRelationName(targetrelation),
            behavior,
        );
        foreach!(lo, child_oids, {
            renameatt_internal(lfirst_oid(current_cell!(lo)), oldattname, newattname, true, true, 0, behavior);
        });
    }

    let attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    let atttup = SearchSysCacheCopyAttName(myrelid, oldattname);
    if !HeapTupleIsValid(atttup) {
        ereport!(ERROR,
            errmsg!("column \"{}\" does not exist",
                CStr::from_ptr(oldattname).to_string_lossy())
            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }
    let attform = GETSTRUCT(atttup) as *mut FormData_pg_attribute;

    let attnum = (*attform).attnum;
    if attnum <= 0 {
        ereport!(ERROR,
            errmsg!("cannot rename system column \"{}\"",
                CStr::from_ptr(oldattname).to_string_lossy())
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /*
     * if the attribute is inherited, forbid the renaming.  if this is a
     * top-level call to renameatt(), then expected_parents will be 0, so the
     * effect of this code will be to prohibit the renaming if the attribute
     * is inherited at all.  if this is a recursive call to renameatt(),
     * expected_parents will be the number of parents the current relation has
     * within the inheritance hierarchy being processed, so we'll prohibit the
     * renaming only if there are additional parents from elsewhere.
     */
    if ((*attform).attinhcount as c_int) > expected_parents {
        ereport!(ERROR,
            errmsg!("cannot rename inherited column \"{}\"",
                CStr::from_ptr(oldattname).to_string_lossy())
            /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
        );
    }

    /* new name should not already exist */
    let _ = check_for_column_name_collision(targetrelation, newattname, false);

    /* apply the update */
    namestrcpy(&mut (*attform).attname as *mut NameData, newattname);

    CatalogTupleUpdate(attrelation, &(*atttup).t_self as *const _ as *const std::ffi::c_void, atttup);

    InvokeObjectPostAlterHook(RelationRelationId, myrelid, attnum as c_int);

    heap_freetuple(atttup);

    table_close(attrelation, RowExclusiveLock);

    relation_close(targetrelation, NoLock); /* close rel but keep lock */

    attnum
}

/*
 * Perform permissions and integrity checks before acquiring a relation lock.
 */
unsafe fn RangeVarCallbackForRenameAttribute(
    rv: *const RangeVar,
    relid: Oid,
    oldrelid: Oid,
    arg: *mut std::ffi::c_void,
) {
    let tuple = SearchSysCache1(0 /* RELOID */, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped */
    }
    let form = GETSTRUCT(tuple) as *mut FormData_pg_class;
    renameatt_check(relid, form, false);
    ReleaseSysCache(tuple);
}

/*
 *		renameatt		- changes the name of an attribute in a relation
 *
 * The returned ObjectAddress is that of the renamed column.
 */
pub unsafe fn renameatt(stmt: *mut RenameStmt) -> ObjectAddress {
    let mut address: ObjectAddress = InvalidObjectAddress;

    /* lock level taken here should match renameatt_internal */
    let relid = RangeVarGetRelidExtended(
        (*stmt).relation,
        AccessExclusiveLock,
        if (*stmt).missing_ok { 0x01 /* RVR_MISSING_OK */ } else { 0 },
        RangeVarCallbackForRenameAttribute,
        ptr::null_mut(),
    );

    if !OidIsValid(relid) {
        ereport!(NOTICE,
            errmsg!("relation \"{}\" does not exist, skipping",
                CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy())
        );
        return InvalidObjectAddress;
    }

    let attnum = renameatt_internal(
        relid,
        (*stmt).subname,   /* old att name */
        (*stmt).newname,   /* new att name */
        (*(*stmt).relation).inh != 0, /* recursive? */
        false,             /* recursing? */
        0,                 /* expected inhcount */
        (*stmt).behavior,
    );

    ObjectAddressSubSet!(address, RelationRelationId, relid, attnum as i32);

    address
}

/*
 * same logic as renameatt_internal
 */
unsafe fn rename_constraint_internal(
    myrelid: Oid,
    mytypid: Oid,
    oldconname: *const c_char,
    newconname: *const c_char,
    recurse: bool,
    recursing: bool,
    expected_parents: c_int,
) -> ObjectAddress {
    /* Assert(!myrelid || !mytypid) */
    let mut targetrelation: Relation = ptr::null_mut();
    let constraintOid: Oid;
    let mut address: ObjectAddress = InvalidObjectAddress;

    if OidIsValid(mytypid) {
        constraintOid = get_domain_constraint_oid(mytypid, oldconname, false);
    } else {
        targetrelation = relation_open(myrelid, AccessExclusiveLock);

        /*
         * don't tell it whether we're recursing; we allow changing typed
         * tables here
         */
        renameatt_check(myrelid, RelationGetForm(targetrelation), false);

        constraintOid = get_relation_constraint_oid(myrelid, oldconname, false);
    }

    let tuple = SearchSysCache1(0 /* CONSTROID */, ObjectIdGetDatum(constraintOid));
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!("cache lookup failed for constraint {}", constraintOid));
    }
    let con = GETSTRUCT(tuple) as *mut FormData_pg_constraint;

    if OidIsValid(myrelid)
        && ((*con).contype == CONSTRAINT_CHECK || (*con).contype == CONSTRAINT_NOTNULL)
        && !(*con).connoinherit
    {
        if recurse {
            let mut child_numparents: *mut i32 = ptr::null_mut();
            let child_oids = find_all_inheritors(myrelid, AccessExclusiveLock, &mut child_numparents as *mut *mut i32 as *mut i32);

            crate::forboth!(lo, child_oids, li, child_numparents, {
                let childrelid = lfirst_oid(lo);
                let numparents = lfirst_int(li);

                if childrelid != myrelid {
                    rename_constraint_internal(childrelid, InvalidOid, oldconname, newconname, false, true, numparents);
                }
            });
        } else {
            if expected_parents == 0
                && !find_inheritance_children(myrelid, NoLock).is_null()
            {
                ereport!(ERROR,
                    errmsg!("inherited constraint \"{}\" must be renamed in child tables too",
                        CStr::from_ptr(oldconname).to_string_lossy())
                    /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
                );
            }
        }

        if ((*con).coninhcount as c_int) > expected_parents {
            ereport!(ERROR,
                errmsg!("cannot rename inherited constraint \"{}\"",
                    CStr::from_ptr(oldconname).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }
    }

    if OidIsValid((*con).conindid)
        && ((*con).contype == CONSTRAINT_PRIMARY
            || (*con).contype == CONSTRAINT_UNIQUE
            || (*con).contype == CONSTRAINT_EXCLUSION)
    {
        /* rename the index; this renames the constraint as well */
        RenameRelationInternal((*con).conindid, newconname, false, true);
    } else {
        RenameConstraintById(constraintOid, newconname);
    }

    ObjectAddressSet!(address, ConstraintRelationId, constraintOid);

    ReleaseSysCache(tuple);

    if !targetrelation.is_null() {
        /*
         * Invalidate relcache so as others can see the new constraint name.
         */
        CacheInvalidateRelcache(targetrelation);

        relation_close(targetrelation, NoLock); /* close rel but keep lock */
    }

    address
}

pub unsafe fn RenameConstraint(stmt: *mut RenameStmt) -> ObjectAddress {
    let mut relid: Oid = InvalidOid;
    let mut typid: Oid = InvalidOid;

    if (*stmt).renameType == crate::nodes::parsenodes::ObjectType::OBJECT_DOMCONSTRAINT {
        typid = typenameTypeId(ptr::null_mut(), makeTypeNameFromNameList((*stmt).object as *mut List));
        let rel = table_open(TypeRelationId, RowExclusiveLock);
        let tup = SearchSysCache1(0 /* TYPEOID */, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid(tup) {
            ereport!(ERROR, errmsg!("cache lookup failed for type {}", typid));
        }
        checkDomainOwner(tup);
        ReleaseSysCache(tup);
        table_close(rel, NoLock);
    } else {
        /* lock level taken here should match rename_constraint_internal */
        relid = RangeVarGetRelidExtended(
            (*stmt).relation,
            AccessExclusiveLock,
            if (*stmt).missing_ok { 0x01 /* RVR_MISSING_OK */ } else { 0 },
            RangeVarCallbackForRenameAttribute,
            ptr::null_mut(),
        );
        if !OidIsValid(relid) {
            ereport!(NOTICE,
                errmsg!("relation \"{}\" does not exist, skipping",
                    CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy())
            );
            return InvalidObjectAddress;
        }
    }

    rename_constraint_internal(
        relid,
        typid,
        (*stmt).subname,
        (*stmt).newname,
        !(*stmt).relation.is_null() && (*(*stmt).relation).inh != 0, /* recursive? */
        false,   /* recursing? */
        0,       /* expected inhcount */
    )
}

/*
 * Execute ALTER TABLE/INDEX/SEQUENCE/VIEW/MATERIALIZED VIEW/FOREIGN TABLE
 * RENAME
 */
pub unsafe fn RenameRelation(stmt: *mut RenameStmt) -> ObjectAddress {
    let mut is_index_stmt = (*stmt).renameType == crate::nodes::parsenodes::ObjectType::OBJECT_INDEX;
    let relid: Oid;
    let mut address: ObjectAddress = InvalidObjectAddress;

    /*
     * Grab an exclusive lock on the target table, index, sequence, view,
     * materialized view, or foreign table, which we will NOT release until
     * end of transaction.
     *
     * Lock level used here should match RenameRelationInternal, to avoid lock
     * escalation.  However, because ALTER INDEX can be used with any relation
     * type, we mustn't believe without verification.
     */
    loop {
        let lockmode: LOCKMODE = if is_index_stmt { ShareUpdateExclusiveLock } else { AccessExclusiveLock };

        let r = RangeVarGetRelidExtended(
            (*stmt).relation,
            lockmode,
            if (*stmt).missing_ok { 0x01 /* RVR_MISSING_OK */ } else { 0 },
            RangeVarCallbackForAlterRelation,
            stmt as *mut std::ffi::c_void,
        );

        if !OidIsValid(r) {
            ereport!(NOTICE,
                errmsg!("relation \"{}\" does not exist, skipping",
                    CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy())
            );
            return InvalidObjectAddress;
        }

        /*
         * We allow mismatched statement and object types (e.g., ALTER INDEX
         * to rename a table), but we might've used the wrong lock level.  If
         * that happens, retry with the correct lock level.  We don't bother
         * if we already acquired AccessExclusiveLock with an index, however.
         */
        let relkind = get_rel_relkind(r);
        let obj_is_index = relkind == RELKIND_INDEX as c_char
            || relkind == RELKIND_PARTITIONED_INDEX as c_char;
        if obj_is_index || is_index_stmt == obj_is_index {
            relid = r;
            break;
        }

        UnlockRelationOid(r, lockmode);
        is_index_stmt = obj_is_index;
        /* retry with new lockmode */
        let _ = r; // avoid unused-variable warning in loop
    }

    /* Do the work */
    RenameRelationInternal(relid, (*stmt).newname, false, is_index_stmt);

    ObjectAddressSet!(address, RelationRelationId, relid);

    address
}

/*
 *		RenameRelationInternal - change the name of a relation
 */
pub unsafe fn RenameRelationInternal(
    myrelid: Oid,
    newrelname: *const c_char,
    is_internal: bool,
    is_index: bool,
) {
    /*
     * Grab a lock on the target relation, which we will NOT release until end
     * of transaction.  We need at least a self-exclusive lock so that
     * concurrent DDL doesn't overwrite the rename if they start updating
     * while still seeing the old version.  The lock also guards against
     * triggering relcache reloads in concurrent sessions, which might not
     * handle this information changing under them.  For indexes, we can use a
     * reduced lock level because RelationReloadIndexInfo() handles indexes
     * specially.
     */
    let targetrelation = relation_open(myrelid, if is_index { ShareUpdateExclusiveLock } else { AccessExclusiveLock });
    let namespaceId = RelationGetNamespace(targetrelation);

    /*
     * Find relation's pg_class tuple, and make sure newrelname isn't in use.
     */
    let relrelation = table_open(RelationRelationId, RowExclusiveLock);

    let reltup = SearchSysCacheLockedCopy1(0 /* RELOID */, ObjectIdGetDatum(myrelid));
    if !HeapTupleIsValid(reltup) { /* shouldn't happen */
        ereport!(ERROR, errmsg!("cache lookup failed for relation {}", myrelid));
    }
    let otid = (*reltup).t_self;
    let relform = GETSTRUCT(reltup) as *mut FormData_pg_class;

    if get_relname_relid(newrelname, namespaceId) != InvalidOid {
        ereport!(ERROR,
            errmsg!("relation \"{}\" already exists",
                CStr::from_ptr(newrelname).to_string_lossy())
            /* C also: errcode(ERRCODE_DUPLICATE_TABLE) */
        );
    }

    /*
     * Update pg_class tuple with new relname.  (Scribbling on reltup is OK
     * because it's a copy...)
     */
    namestrcpy(&mut (*relform).relname as *mut NameData, newrelname);

    CatalogTupleUpdate(relrelation, &otid as *const _ as *const std::ffi::c_void, reltup);
    UnlockTuple(relrelation, &otid as *const _ as *const std::ffi::c_void, 0 /* InplaceUpdateTupleLock */);

    InvokeObjectPostAlterHookArg(RelationRelationId, myrelid, 0, InvalidOid, is_internal);

    heap_freetuple(reltup);
    table_close(relrelation, RowExclusiveLock);

    /*
     * Also rename the associated type, if any.
     */
    if OidIsValid((*(*targetrelation).rd_rel).reltype) {
        RenameTypeInternal((*(*targetrelation).rd_rel).reltype, newrelname, namespaceId);
    }

    /*
     * Also rename the associated constraint, if any.
     */
    if (*(*targetrelation).rd_rel).relkind == RELKIND_INDEX as c_char
        || (*(*targetrelation).rd_rel).relkind == RELKIND_PARTITIONED_INDEX as c_char
    {
        let constraintId = get_index_constraint(myrelid);
        if OidIsValid(constraintId) {
            RenameConstraintById(constraintId, newrelname);
        }
    }

    /*
     * Close rel, but keep lock!
     */
    relation_close(targetrelation, NoLock);
}

/*
 *		ResetRelRewrite - reset relrewrite
 */
pub unsafe fn ResetRelRewrite(myrelid: Oid) {
    /*
     * Find relation's pg_class tuple.
     */
    let relrelation = table_open(RelationRelationId, RowExclusiveLock);

    let reltup = SearchSysCacheCopy1(0 /* RELOID */, ObjectIdGetDatum(myrelid));
    if !HeapTupleIsValid(reltup) { /* shouldn't happen */
        ereport!(ERROR, errmsg!("cache lookup failed for relation {}", myrelid));
    }
    let relform = GETSTRUCT(reltup) as *mut FormData_pg_class;

    /*
     * Update pg_class tuple.
     */
    (*relform).relrewrite = InvalidOid;

    CatalogTupleUpdate(relrelation, &(*reltup).t_self as *const _ as *const std::ffi::c_void, reltup);

    heap_freetuple(reltup);
    table_close(relrelation, RowExclusiveLock);
}

/*
 * Disallow ALTER TABLE (and similar commands) when the current backend has
 * any open reference to the target table besides the one just acquired by
 * the calling command; this implies there's an open cursor or active plan.
 * We need this check because our lock doesn't protect us against stomping
 * on our own foot, only other people's feet!
 *
 * For ALTER TABLE, the only case known to cause serious trouble is ALTER
 * COLUMN TYPE, and some changes are obviously pretty benign, so this could
 * possibly be relaxed to only error out for certain types of alterations.
 * But the use-case for allowing any of these things is not obvious, so we
 * won't work hard at it for now.
 *
 * We also reject these commands if there are any pending AFTER trigger events
 * for the rel.  This is certainly necessary for the rewriting variants of
 * ALTER TABLE, because they don't preserve tuple TIDs and so the pending
 * events would try to fetch the wrong tuples.  It might be overly cautious
 * in other cases, but again it seems better to err on the side of paranoia.
 *
 * REINDEX calls this with "rel" referencing the index to be rebuilt; here
 * we are worried about active indexscans on the index.  The trigger-event
 * check can be skipped, since we are doing no damage to the parent table.
 *
 * The statement name (eg, "ALTER TABLE") is passed for use in error messages.
 */
pub unsafe fn CheckTableNotInUse(rel: Relation, stmt: *const c_char) {
    let expected_refcnt = if (*rel).rd_isnailed { 2 } else { 1 };
    if (*rel).rd_refcnt != expected_refcnt {
        ereport!(ERROR,
            errmsg!("cannot {} \"{}\" because it is being used by active queries in this session",
                CStr::from_ptr(stmt).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            /* C also: errcode(ERRCODE_OBJECT_IN_USE) */
        );
    }

    if (*(*rel).rd_rel).relkind != RELKIND_INDEX as c_char
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_INDEX as c_char
        && AfterTriggerPendingOnRel(RelationGetRelid(rel))
    {
        ereport!(ERROR,
            errmsg!("cannot {} \"{}\" because it has pending trigger events",
                CStr::from_ptr(stmt).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            /* C also: errcode(ERRCODE_OBJECT_IN_USE) */
        );
    }
}

/*
 * CheckAlterTableIsSafe
 *		Verify that it's safe to allow ALTER TABLE on this relation.
 *
 * This consists of CheckTableNotInUse() plus a check that the relation
 * isn't another session's temp table.  We must split out the temp-table
 * check because there are callers of CheckTableNotInUse() that don't want
 * that, notably DROP TABLE.  (We must allow DROP or we couldn't clean out
 * an orphaned temp schema.)  Compare truncate_check_activity().
 */
unsafe fn CheckAlterTableIsSafe(rel: Relation) {
    /*
     * Don't allow ALTER on temp tables of other backends.  Their local buffer
     * manager is not going to cope if we need to change the table's contents.
     * Even if we don't, there may be optimizations that assume temp tables
     * aren't subject to such interference.
     */
    if RELATION_IS_OTHER_TEMP(rel) {
        ereport!(ERROR,
            errmsg!("cannot alter temporary tables of other sessions")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /*
     * Also check for active uses of the relation in the current transaction,
     * including open scans and pending AFTER trigger events.
     */
    CheckTableNotInUse(rel, b"ALTER TABLE\0".as_ptr() as *const c_char);
}

/*
 * AlterTableLookupRelation
 *		Look up, and lock, the OID for the relation named by an alter table
 *		statement.
 */
pub unsafe fn AlterTableLookupRelation(
    stmt: *mut AlterTableStmt,
    lockmode: LOCKMODE,
) -> Oid {
    RangeVarGetRelidExtended(
        (*stmt).relation,
        lockmode,
        if (*stmt).missing_ok { 0x01 /* RVR_MISSING_OK */ } else { 0 },
        RangeVarCallbackForAlterRelation,
        stmt as *mut std::ffi::c_void,
    )
}

/*
 * AlterTable
 *		Execute ALTER TABLE, which can be a list of subcommands
 *
 * ALTER TABLE is performed in three phases:
 *		1. Examine subcommands and perform pre-transformation checking.
 *		2. Validate and transform subcommands, and update system catalogs.
 *		3. Scan table(s) to check new constraints, and optionally recopy
 *		   the data into new table(s).
 * Phase 3 is not performed unless one or more of the subcommands requires
 * it.  The intention of this design is to allow multiple independent
 * updates of the table schema to be performed with only one pass over the
 * data.
 *
 * ATPrepCmd performs phase 1.  A "work queue" entry is created for
 * each table to be affected (there may be multiple affected tables if the
 * commands traverse a table inheritance hierarchy).  Also we do preliminary
 * validation of the subcommands.  Because earlier subcommands may change
 * the catalog state seen by later commands, there are limits to what can
 * be done in this phase.  Generally, this phase acquires table locks,
 * checks permissions and relkind, and recurses to find child tables.
 *
 * ATRewriteCatalogs performs phase 2 for each affected table.
 * Certain subcommands need to be performed before others to avoid
 * unnecessary conflicts; for example, DROP COLUMN should come before
 * ADD COLUMN.  Therefore phase 1 divides the subcommands into multiple
 * lists, one for each logical "pass" of phase 2.
 *
 * ATRewriteTables performs phase 3 for those tables that need it.
 *
 * For most subcommand types, phases 2 and 3 do no explicit recursion,
 * since phase 1 already does it.  However, for certain subcommand types
 * it is only possible to determine how to recurse at phase 2 time; for
 * those cases, phase 1 sets the cmd->recurse flag.
 *
 * Thanks to the magic of MVCC, an error anywhere along the way rolls back
 * the whole operation; we don't have to do anything special to clean up.
 *
 * The caller must lock the relation, with an appropriate lock level
 * for the subcommands requested, using AlterTableGetLockLevel(stmt->cmds)
 * or higher. We pass the lock level down
 * so that we can apply it recursively to inherited tables. Note that the
 * lock level we want as we recurse might well be higher than required for
 * that specific subcommand. So we pass down the overall lock requirement,
 * rather than reassess it at lower levels.
 *
 * The caller also provides a "context" which is to be passed back to
 * utility.c when we need to execute a subcommand such as CREATE INDEX.
 * Some of the fields therein, such as the relid, are used here as well.
 */
pub unsafe fn AlterTable(
    stmt: *mut AlterTableStmt,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    /* Caller is required to provide an adequate lock. */
    let rel = relation_open((*context).relid, NoLock);

    CheckAlterTableIsSafe(rel);

    ATController(stmt, rel, (*stmt).cmds, (*(*stmt).relation).inh != 0, lockmode, context);
}

/*
 * AlterTableInternal
 *
 * ALTER TABLE with target specified by OID
 *
 * We do not reject if the relation is already open, because it's quite
 * likely that one or more layers of caller have it open.  That means it
 * is unsafe to use this entry point for alterations that could break
 * existing query plans.  On the assumption it's not used for such, we
 * don't have to reject pending AFTER triggers, either.
 *
 * Also, since we don't have an AlterTableUtilityContext, this cannot be
 * used for any subcommand types that require parse transformation or
 * could generate subcommands that have to be passed to ProcessUtility.
 */
pub unsafe fn AlterTableInternal(relid: Oid, cmds: *mut List, recurse: bool) {
    let lockmode: LOCKMODE = AlterTableGetLockLevel(cmds);

    let rel = relation_open(relid, lockmode);

    EventTriggerAlterTableRelid(relid);

    ATController(ptr::null_mut(), rel, cmds, recurse, lockmode, ptr::null_mut());
}

/*
 * AlterTableGetLockLevel
 *
 * Sets the overall lock level required for the supplied list of subcommands.
 * Policy for doing this set according to needs of AlterTable(), see
 * comments there for overall explanation.
 *
 * Function is called before and after parsing, so it must give same
 * answer each time it is called. Some subcommands are transformed
 * into other subcommand types, so the transform must never be made to a
 * lower lock level than previously assigned. All transforms are noted below.
 *
 * Since this is called before we lock the table we cannot use table metadata
 * to influence the type of lock we acquire.
 *
 * There should be no lockmodes hardcoded into the subcommand functions. All
 * lockmode decisions for ALTER TABLE are made here only. The one exception is
 * ALTER TABLE RENAME which is treated as a different statement type T_RenameStmt
 * and does not travel through this section of code and cannot be combined with
 * any of the subcommands given here.
 *
 * Note that Hot Standby only knows about AccessExclusiveLocks on the primary
 * so any changes that might affect SELECTs running on standbys need to use
 * AccessExclusiveLocks even if you think a lesser lock would do, unless you
 * have a solution for that also.
 *
 * Also note that pg_dump uses only an AccessShareLock, meaning that anything
 * that takes a lock less than AccessExclusiveLock can change object definitions
 * while pg_dump is running. Be careful to check that the appropriate data is
 * derived by pg_dump using an MVCC snapshot, rather than syscache lookups,
 * otherwise we might end up with an inconsistent dump that can't restore.
 */
pub unsafe fn AlterTableGetLockLevel(cmds: *mut List) -> LOCKMODE {
    /*
     * This only works if we read catalog tables using MVCC snapshots.
     */
    let mut lockmode: LOCKMODE = ShareUpdateExclusiveLock;

    foreach!(lcmd, cmds, {
        let cmd = crate::nodes::pg_list::lfirst(current_cell!(lcmd)) as *mut AlterTableCmd;
        let cmd_lockmode: LOCKMODE = match (*cmd).subtype {
            /*
             * These subcommands rewrite the heap, so require full locks.
             */
            AlterTableType::AT_AddColumn       /* may rewrite heap, in some cases and visible to SELECT */
            | AlterTableType::AT_SetAccessMethod  /* must rewrite heap */
            | AlterTableType::AT_SetTableSpace    /* must rewrite heap */
            | AlterTableType::AT_AlterColumnType  /* must rewrite heap */
            => AccessExclusiveLock,

            /*
             * These subcommands may require addition of toast tables. If
             * we add a toast table to a table currently being scanned, we
             * might miss data added to the new toast table by concurrent
             * insert transactions.
             */
            AlterTableType::AT_SetStorage /* may add toast tables, see ATRewriteCatalogs() */
            => AccessExclusiveLock,

            /*
             * Removing constraints can affect SELECTs that have been
             * optimized assuming the constraint holds true. See also
             * CloneFkReferenced.
             */
            AlterTableType::AT_DropConstraint /* as DROP INDEX */
            | AlterTableType::AT_DropNotNull  /* may change some SQL plans */
            => AccessExclusiveLock,

            /*
             * Subcommands that may be visible to concurrent SELECTs
             */
            AlterTableType::AT_DropColumn       /* change visible to SELECT */
            | AlterTableType::AT_AddColumnToView /* CREATE VIEW */
            | AlterTableType::AT_DropOids        /* used to equiv to DropColumn */
            | AlterTableType::AT_EnableAlwaysRule  /* may change SELECT rules */
            | AlterTableType::AT_EnableReplicaRule /* may change SELECT rules */
            | AlterTableType::AT_EnableRule        /* may change SELECT rules */
            | AlterTableType::AT_DisableRule       /* may change SELECT rules */
            => AccessExclusiveLock,

            /*
             * Changing owner may remove implicit SELECT privileges
             */
            AlterTableType::AT_ChangeOwner /* change visible to SELECT */
            => AccessExclusiveLock,

            /*
             * Changing foreign table options may affect optimization.
             */
            AlterTableType::AT_GenericOptions
            | AlterTableType::AT_AlterColumnGenericOptions
            => AccessExclusiveLock,

            /*
             * These subcommands affect write operations only.
             */
            AlterTableType::AT_EnableTrig
            | AlterTableType::AT_EnableAlwaysTrig
            | AlterTableType::AT_EnableReplicaTrig
            | AlterTableType::AT_EnableTrigAll
            | AlterTableType::AT_EnableTrigUser
            | AlterTableType::AT_DisableTrig
            | AlterTableType::AT_DisableTrigAll
            | AlterTableType::AT_DisableTrigUser
            => ShareRowExclusiveLock,

            /*
             * These subcommands affect write operations only. XXX
             * Theoretically, these could be ShareRowExclusiveLock.
             */
            AlterTableType::AT_ColumnDefault
            | AlterTableType::AT_CookedColumnDefault
            | AlterTableType::AT_AlterConstraint
            | AlterTableType::AT_AddIndex          /* from ADD CONSTRAINT */
            | AlterTableType::AT_AddIndexConstraint
            | AlterTableType::AT_ReplicaIdentity
            | AlterTableType::AT_SetNotNull
            | AlterTableType::AT_EnableRowSecurity
            | AlterTableType::AT_DisableRowSecurity
            | AlterTableType::AT_ForceRowSecurity
            | AlterTableType::AT_NoForceRowSecurity
            | AlterTableType::AT_AddIdentity
            | AlterTableType::AT_DropIdentity
            | AlterTableType::AT_SetIdentity
            | AlterTableType::AT_SetExpression
            | AlterTableType::AT_DropExpression
            | AlterTableType::AT_SetCompression
            => AccessExclusiveLock,

            AlterTableType::AT_AddConstraint
            | AlterTableType::AT_ReAddConstraint      /* becomes AT_AddConstraint */
            | AlterTableType::AT_ReAddDomainConstraint /* becomes AT_AddConstraint */
            => {
                if IsA!((*cmd).def as *mut crate::nodes::nodes::Node, T_Constraint) {
                    let con = (*cmd).def as *mut Constraint;
                    match (*con).contype {
                        ConstrType::CONSTR_EXCLUSION
                        | ConstrType::CONSTR_PRIMARY
                        | ConstrType::CONSTR_UNIQUE =>
                            AccessExclusiveLock,
                        ConstrType::CONSTR_FOREIGN =>
                            /* We add triggers to both tables, so at least CREATE TRIGGER level */
                            ShareRowExclusiveLock,
                        _ =>
                            AccessExclusiveLock,
                    }
                } else {
                    AccessExclusiveLock
                }
            }

            /*
             * These subcommands affect inheritance behaviour. Queries
             * started before us will continue to see the old inheritance
             * behaviour, while queries started after we commit will see
             * new behaviour. No need to prevent reads or writes to the
             * subtable while we hook it up though. Changing the TupDesc
             * may be a problem, so keep highest lock.
             */
            AlterTableType::AT_AddInherit
            | AlterTableType::AT_DropInherit
            => AccessExclusiveLock,

            /*
             * These subcommands affect implicit row type conversion. They
             * have affects similar to CREATE/DROP CAST on queries. don't
             * provide for invalidating parse trees as a result of such
             * changes, so we keep these at AccessExclusiveLock.
             */
            AlterTableType::AT_AddOf
            | AlterTableType::AT_DropOf
            => AccessExclusiveLock,

            /*
             * Only used by CREATE OR REPLACE VIEW which must conflict
             * with an SELECTs currently using the view.
             */
            AlterTableType::AT_ReplaceRelOptions
            => AccessExclusiveLock,

            /*
             * These subcommands affect general strategies for performance
             * and maintenance, though don't change the semantic results
             * from normal data reads and writes. Delaying an ALTER TABLE
             * behind currently active writes only delays the point where
             * the new strategy begins to take effect, so there is no
             * benefit in waiting. In this case the minimum restriction
             * applies: we don't currently allow concurrent catalog
             * updates.
             */
            AlterTableType::AT_SetStatistics  /* Uses MVCC in getTableAttrs() */
            | AlterTableType::AT_ClusterOn    /* Uses MVCC in getIndexes() */
            | AlterTableType::AT_DropCluster  /* Uses MVCC in getIndexes() */
            | AlterTableType::AT_SetOptions   /* Uses MVCC in getTableAttrs() */
            | AlterTableType::AT_ResetOptions /* Uses MVCC in getTableAttrs() */
            => ShareUpdateExclusiveLock,

            AlterTableType::AT_SetLogged
            | AlterTableType::AT_SetUnLogged
            => AccessExclusiveLock,

            AlterTableType::AT_ValidateConstraint /* Uses MVCC in getConstraints() */
            => ShareUpdateExclusiveLock,

            /*
             * Rel options are more complex than first appears. Options
             * are set here for tables, views and indexes; for historical
             * reasons these can all be used with ALTER TABLE, so we can't
             * decide between them using the basic grammar.
             */
            AlterTableType::AT_SetRelOptions    /* Uses MVCC in getIndexes() and getTables() */
            | AlterTableType::AT_ResetRelOptions /* Uses MVCC in getIndexes() and getTables() */
            => AlterTableGetRelOptionsLockLevel((*cmd).def as *mut List),

            AlterTableType::AT_AttachPartition
            => ShareUpdateExclusiveLock,

            AlterTableType::AT_DetachPartition
            => {
                if (*((*cmd).def as *mut PartitionCmd)).concurrent {
                    ShareUpdateExclusiveLock
                } else {
                    AccessExclusiveLock
                }
            }

            AlterTableType::AT_DetachPartitionFinalize
            => ShareUpdateExclusiveLock,

            _ => {
                ereport!(ERROR, errmsg!("unrecognized alter table type: {}", (*cmd).subtype as i32));
                AccessExclusiveLock /* keep compiler happy */
            }
        };

        /*
         * Take the greatest lockmode from any subcommand
         */
        if cmd_lockmode > lockmode {
            lockmode = cmd_lockmode;
        }
    });

    lockmode
}

/*
 * ATController provides top level control over the phases.
 *
 * parsetree is passed in to allow it to be passed to event triggers
 * when requested.
 */
unsafe fn ATController(
    parsetree: *mut AlterTableStmt,
    rel: Relation,
    cmds: *mut List,
    recurse: bool,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    let mut wqueue: *mut List = crate::nodes::pg_list::NIL;

    /* Phase 1: preliminary examination of commands, create work queue */
    foreach!(lcmd, cmds, {
        let cmd = crate::nodes::pg_list::lfirst(current_cell!(lcmd)) as *mut AlterTableCmd;
        ATPrepCmd(&mut wqueue as *mut *mut List, rel, cmd, recurse, false, lockmode, context);
    });

    /* Close the relation, but keep lock until commit */
    relation_close(rel, NoLock);

    /* Phase 2: update system catalogs */
    ATRewriteCatalogs(&mut wqueue as *mut *mut List, lockmode, context);

    /* Phase 3: scan/rewrite tables as needed, and run afterStmts */
    ATRewriteTables(parsetree, &mut wqueue as *mut *mut List, lockmode, context);
}

/*
 * ATPrepCmd
 *
 * Traffic cop for ALTER TABLE Phase 1 operations, including simple
 * recursion and permission checks.
 *
 * Caller must have acquired appropriate lock type on relation already.
 * This lock should be held until commit.
 */
unsafe fn ATPrepCmd(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    /* Find or create work queue entry for this table */
    let tab = ATGetQueueEntry(wqueue, rel);

    /*
     * Disallow any ALTER TABLE other than ALTER TABLE DETACH FINALIZE on
     * partitions that are pending detach.
     */
    if (*(*rel).rd_rel).relispartition
        && (*cmd).subtype != AlterTableType::AT_DetachPartitionFinalize
        && PartitionHasPendingDetach(RelationGetRelid(rel))
    {
        ereport!(ERROR,
            errmsg!("cannot alter partition \"{}\" with an incomplete detach",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            /* C also: errcode, errhint */
        );
    }

    /*
     * Copy the original subcommand for each table, so we can scribble on it.
     */
    let mut cmd = copyObject_cmd(cmd);

    let pass: AlterTablePass;

    /*
     * Do permissions and relkind checking, recursion to child tables if
     * needed, and any additional phase-1 processing needed.
     */
    match (*cmd).subtype {
        AlterTableType::AT_AddColumn => {
            /* ADD COLUMN */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
            ATPrepAddColumn(wqueue, rel, recurse, recursing, false, cmd, lockmode, context);
            /* Recursion occurs during execution phase */
            pass = AlterTablePass::AT_PASS_ADD_COL;
        }
        AlterTableType::AT_AddColumnToView => {
            /* add column via CREATE OR REPLACE VIEW */
            ATSimplePermissions((*cmd).subtype, rel, ATT_VIEW);
            ATPrepAddColumn(wqueue, rel, recurse, recursing, true, cmd, lockmode, context);
            /* Recursion occurs during execution phase */
            pass = AlterTablePass::AT_PASS_ADD_COL;
        }
        AlterTableType::AT_ColumnDefault => {
            /* ALTER COLUMN DEFAULT */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
            ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
            /* No command-specific prep needed */
            pass = if !(*cmd).def.is_null() { AlterTablePass::AT_PASS_ADD_OTHERCONSTR } else { AlterTablePass::AT_PASS_DROP };
        }
        AlterTableType::AT_CookedColumnDefault => {
            /* add a pre-cooked default */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_ADD_OTHERCONSTR;
        }
        AlterTableType::AT_AddIdentity => {
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
            /* Set up recursion for phase 2; no other prep needed */
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_ADD_OTHERCONSTR;
        }
        AlterTableType::AT_SetIdentity => {
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
            if recurse { (*cmd).recurse = true; }
            /* This should run after AddIdentity, so do it in MISC pass */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_DropIdentity => {
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE);
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_DROP;
        }
        AlterTableType::AT_DropNotNull => {
            /* ALTER COLUMN DROP NOT NULL */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            /* Set up recursion for phase 2; no other prep needed */
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_DROP;
        }
        AlterTableType::AT_SetNotNull => {
            /* ALTER COLUMN SET NOT NULL */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_COL_ATTRS;
        }
        AlterTableType::AT_SetExpression => {
            /* ALTER COLUMN SET EXPRESSION */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
            pass = AlterTablePass::AT_PASS_SET_EXPRESSION;
        }
        AlterTableType::AT_DropExpression => {
            /* ALTER COLUMN DROP EXPRESSION */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
            ATPrepDropExpression(rel, cmd, recurse, recursing, lockmode);
            pass = AlterTablePass::AT_PASS_DROP;
        }
        AlterTableType::AT_SetStatistics => {
            /* ALTER COLUMN SET STATISTICS */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_PARTITIONED_INDEX | ATT_FOREIGN_TABLE);
            ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
            /* No command-specific prep needed */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_SetOptions | AlterTableType::AT_ResetOptions => {
            /* ALTER COLUMN SET/RESET ( options ) */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_SetStorage => {
            /* ALTER COLUMN SET STORAGE */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE);
            ATSimpleRecursion(wqueue, rel, cmd, recurse, lockmode, context);
            /* No command-specific prep needed */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_SetCompression => {
            /* ALTER COLUMN SET COMPRESSION */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_DropColumn => {
            /* DROP COLUMN */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
            ATPrepDropColumn(wqueue, rel, recurse, recursing, cmd, lockmode, context);
            /* Recursion occurs during execution phase */
            pass = AlterTablePass::AT_PASS_DROP;
        }
        AlterTableType::AT_AddIndex => {
            /* ADD INDEX */
            ATSimplePermissions((*cmd).subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_ADD_INDEX;
        }
        AlterTableType::AT_AddConstraint => {
            /* ADD CONSTRAINT */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            ATPrepAddPrimaryKey(wqueue, rel, cmd, recurse, lockmode, context);
            if recurse {
                /* recurses at exec time; lock descendants and set flag */
                let _ = find_all_inheritors(RelationGetRelid(rel), lockmode, ptr::null_mut());
                (*cmd).recurse = true;
            }
            pass = AlterTablePass::AT_PASS_ADD_CONSTR;
        }
        AlterTableType::AT_AddIndexConstraint => {
            /* ADD CONSTRAINT USING INDEX */
            ATSimplePermissions((*cmd).subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_ADD_INDEXCONSTR;
        }
        AlterTableType::AT_DropConstraint => {
            /* DROP CONSTRAINT */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            ATCheckPartitionsNotInUse(rel, lockmode);
            /* Other recursion occurs during execution phase */
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_DROP;
        }
        AlterTableType::AT_AlterColumnType => {
            /* ALTER COLUMN TYPE */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE);
            /* See comments for ATPrepAlterColumnType */
            cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, recurse, lockmode,
                AlterTablePass::AT_PASS_UNSET, context);
            /* Assert(cmd != NULL) */
            /* Performs own recursion */
            ATPrepAlterColumnType(wqueue, tab, rel, recurse, recursing, cmd, lockmode, context);
            pass = AlterTablePass::AT_PASS_ALTER_TYPE;
        }
        AlterTableType::AT_AlterColumnGenericOptions => {
            ATSimplePermissions((*cmd).subtype, rel, ATT_FOREIGN_TABLE);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_ChangeOwner => {
            /* ALTER OWNER */
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_ClusterOn | AlterTableType::AT_DropCluster => {
            /* CLUSTER ON / SET WITHOUT CLUSTER */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW);
            /* These commands never recurse */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_SetLogged | AlterTableType::AT_SetUnLogged => {
            /* SET LOGGED / SET UNLOGGED */
            ATSimplePermissions((*cmd).subtype, rel, ATT_TABLE | ATT_SEQUENCE);
            if (*tab).chgPersistence {
                ereport!(ERROR, errmsg!("cannot change persistence setting twice")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }
            ATPrepChangePersistence(tab, rel, (*cmd).subtype == AlterTableType::AT_SetLogged);
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_DropOids => {
            /* SET WITHOUT OIDS */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            pass = AlterTablePass::AT_PASS_DROP;
        }
        AlterTableType::AT_SetAccessMethod => {
            /* SET ACCESS METHOD */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW);
            /* check if another access method change was already requested */
            if (*tab).chgAccessMethod {
                ereport!(ERROR, errmsg!("cannot have multiple SET ACCESS METHOD subcommands")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }
            ATPrepSetAccessMethod(tab, rel, (*cmd).name);
            pass = AlterTablePass::AT_PASS_MISC; /* does not matter; no work in Phase 2 */
        }
        AlterTableType::AT_SetTableSpace => {
            /* SET TABLESPACE */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_INDEX | ATT_PARTITIONED_INDEX);
            /* This command never recurses */
            ATPrepSetTableSpace(tab, rel, (*cmd).name, lockmode);
            pass = AlterTablePass::AT_PASS_MISC; /* doesn't actually matter */
        }
        AlterTableType::AT_SetRelOptions
        | AlterTableType::AT_ResetRelOptions
        | AlterTableType::AT_ReplaceRelOptions => {
            /* SET (...) / RESET (...) */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_MATVIEW | ATT_INDEX);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_AddInherit => {
            /* INHERIT */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            /* This command never recurses */
            ATPrepAddInherit(rel);
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_DropInherit => {
            /* NO INHERIT */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            /* This command never recurses */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_AlterConstraint => {
            /* ALTER CONSTRAINT */
            ATSimplePermissions((*cmd).subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE);
            /* Recursion occurs during execution phase */
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_ValidateConstraint => {
            /* VALIDATE CONSTRAINT */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            /* Recursion occurs during execution phase */
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_ReplicaIdentity => {
            /* REPLICA IDENTITY ... */
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW);
            pass = AlterTablePass::AT_PASS_MISC;
            /* This command never recurses */
        }
        AlterTableType::AT_EnableTrig
        | AlterTableType::AT_EnableAlwaysTrig
        | AlterTableType::AT_EnableReplicaTrig
        | AlterTableType::AT_EnableTrigAll
        | AlterTableType::AT_EnableTrigUser
        | AlterTableType::AT_DisableTrig
        | AlterTableType::AT_DisableTrigAll
        | AlterTableType::AT_DisableTrigUser => {
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE);
            /* Set up recursion for phase 2; no other prep needed */
            if recurse { (*cmd).recurse = true; }
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_EnableRule
        | AlterTableType::AT_EnableAlwaysRule
        | AlterTableType::AT_EnableReplicaRule
        | AlterTableType::AT_DisableRule
        | AlterTableType::AT_AddOf
        | AlterTableType::AT_DropOf
        | AlterTableType::AT_EnableRowSecurity
        | AlterTableType::AT_DisableRowSecurity
        | AlterTableType::AT_ForceRowSecurity
        | AlterTableType::AT_NoForceRowSecurity => {
            ATSimplePermissions((*cmd).subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE);
            /* These commands never recurse */
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_GenericOptions => {
            ATSimplePermissions((*cmd).subtype, rel, ATT_FOREIGN_TABLE);
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_AttachPartition => {
            ATSimplePermissions((*cmd).subtype, rel,
                ATT_PARTITIONED_TABLE | ATT_PARTITIONED_INDEX);
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_DetachPartition => {
            ATSimplePermissions((*cmd).subtype, rel, ATT_PARTITIONED_TABLE);
            pass = AlterTablePass::AT_PASS_MISC;
        }
        AlterTableType::AT_DetachPartitionFinalize => {
            ATSimplePermissions((*cmd).subtype, rel, ATT_PARTITIONED_TABLE);
            pass = AlterTablePass::AT_PASS_MISC;
        }
        _ => {
            ereport!(ERROR, errmsg!("unrecognized alter table type: {}", (*cmd).subtype as i32));
            pass = AlterTablePass::AT_PASS_UNSET; /* keep compiler quiet */
        }
    }
    /* Assert(pass > AT_PASS_UNSET) */

    /* Add the subcommand to the appropriate list for phase 2 */
    (*tab).subcmds[pass as usize] = lappend((*tab).subcmds[pass as usize] as *mut std::ffi::c_void, cmd as *mut std::ffi::c_void) as *mut List;
}

/*
 * ATRewriteCatalogs
 *
 * Traffic cop for ALTER TABLE Phase 2 operations.  Subcommands are
 * dispatched in a "safe" execution order (designed to avoid unnecessary
 * conflicts).
 */
unsafe fn ATRewriteCatalogs(
    wqueue: *mut *mut List,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    /*
     * We process all the tables "in parallel", one pass at a time.  This is
     * needed because we may have to propagate work from one table to another
     * (specifically, ALTER TYPE on a foreign key's PK has to dispatch the
     * re-adding of the foreign key constraint to the other table).  Work can
     * only be propagated into later passes, however.
     */
    let mut pass = AlterTablePass::AT_PASS_DROP as usize;
    while pass < AT_NUM_PASSES {
        /* Go through each table that needs to be processed */
        foreach!(ltab, *wqueue, {
            let tab = crate::nodes::pg_list::lfirst(current_cell!(ltab)) as *mut AlteredTableInfo;
            let subcmds = (*tab).subcmds[pass];

            if subcmds.is_null() {
                /* subcmds == NIL, skip */
            } else {
                /*
                 * Open the relation and store it in tab.  This allows subroutines
                 * close and reopen, if necessary.  Appropriate lock was obtained
                 * by phase 1, needn't get it again.
                 */
                (*tab).rel = relation_open((*tab).relid, NoLock);

                foreach!(lcmd, subcmds, {
                    let cmd = crate::nodes::pg_list::lfirst(current_cell!(lcmd)) as *mut AlterTableCmd;
                    ATExecCmd(wqueue, tab, cmd, lockmode,
                        /* pass as AlterTablePass: */ std::mem::transmute::<i32, AlterTablePass>(pass as i32),
                        context);
                });

                /*
                 * After the ALTER TYPE or SET EXPRESSION pass, do cleanup work
                 * (this is not done in ATExecAlterColumnType since it should be
                 * done only once if multiple columns of a table are altered).
                 */
                if pass == AlterTablePass::AT_PASS_ALTER_TYPE as usize
                    || pass == AlterTablePass::AT_PASS_SET_EXPRESSION as usize
                {
                    ATPostAlterTypeCleanup(wqueue, tab, lockmode);
                }

                if !(*tab).rel.is_null() {
                    relation_close((*tab).rel, NoLock);
                    (*tab).rel = ptr::null_mut();
                }
            }
        });
        pass += 1;
    }

    /* Check to see if a toast table must be added. */
    foreach!(ltab, *wqueue, {
        let tab = crate::nodes::pg_list::lfirst(current_cell!(ltab)) as *mut AlteredTableInfo;

        /*
         * If the table is source table of ATTACH PARTITION command, we did
         * not modify anything about it that will change its toasting
         * requirement, so no need to check.
         */
        if ((*tab).relkind == RELKIND_RELATION as c_char
            || (*tab).relkind == RELKIND_PARTITIONED_TABLE as c_char)
            && (*tab).partition_constraint.is_null()
            || (*tab).relkind == RELKIND_MATVIEW as c_char
        {
            AlterTableCreateToastTable((*tab).relid, 0 as Datum, lockmode);
        }
    });
}

/*
 * ATExecCmd: dispatch a subcommand to appropriate execution routine
 */
unsafe fn ATExecCmd(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: *mut AlterTableUtilityContext,
) {
    let mut address: ObjectAddress = InvalidObjectAddress;
    let rel = (*tab).rel;
    let mut cmd = cmd; /* may be replaced by ATParseTransformCmd result */

    match (*cmd).subtype {
        AlterTableType::AT_AddColumn | AlterTableType::AT_AddColumnToView => {
            /* ADD COLUMN */
            address = ATExecAddColumn(wqueue, tab, rel, &mut cmd as *mut *mut AlterTableCmd,
                (*cmd).recurse, false, lockmode, cur_pass, context);
        }
        AlterTableType::AT_ColumnDefault => {
            /* ALTER COLUMN DEFAULT */
            address = ATExecColumnDefault(rel, (*cmd).name, (*cmd).def as *mut Node, lockmode);
        }
        AlterTableType::AT_CookedColumnDefault => {
            /* add a pre-cooked default */
            address = ATExecCookedColumnDefault(rel, (*cmd).num, (*cmd).def as *mut Node);
        }
        AlterTableType::AT_AddIdentity => {
            cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode, cur_pass, context);
            /* Assert(cmd != NULL) */
            address = ATExecAddIdentity(rel, (*cmd).name, (*cmd).def as *mut Node, lockmode, (*cmd).recurse, false);
        }
        AlterTableType::AT_SetIdentity => {
            cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode, cur_pass, context);
            /* Assert(cmd != NULL) */
            address = ATExecSetIdentity(rel, (*cmd).name, (*cmd).def as *mut Node, lockmode, (*cmd).recurse, false);
        }
        AlterTableType::AT_DropIdentity => {
            address = ATExecDropIdentity(rel, (*cmd).name, (*cmd).missing_ok, lockmode, (*cmd).recurse, false);
        }
        AlterTableType::AT_DropNotNull => {
            /* ALTER COLUMN DROP NOT NULL */
            address = ATExecDropNotNull(rel, (*cmd).name, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_SetNotNull => {
            /* ALTER COLUMN SET NOT NULL */
            address = ATExecSetNotNull(wqueue, rel, ptr::null(), (*cmd).name,
                (*cmd).recurse, false, lockmode);
        }
        AlterTableType::AT_SetExpression => {
            address = ATExecSetExpression(tab, rel, (*cmd).name, (*cmd).def as *mut Node, lockmode);
        }
        AlterTableType::AT_DropExpression => {
            address = ATExecDropExpression(rel, (*cmd).name, (*cmd).missing_ok, lockmode);
        }
        AlterTableType::AT_SetStatistics => {
            /* ALTER COLUMN SET STATISTICS */
            address = ATExecSetStatistics(rel, (*cmd).name, (*cmd).num, (*cmd).def as *mut Node, lockmode);
        }
        AlterTableType::AT_SetOptions => {
            /* ALTER COLUMN SET ( options ) */
            address = ATExecSetOptions(rel, (*cmd).name, (*cmd).def as *mut Node, false, lockmode);
        }
        AlterTableType::AT_ResetOptions => {
            /* ALTER COLUMN RESET ( options ) */
            address = ATExecSetOptions(rel, (*cmd).name, (*cmd).def as *mut Node, true, lockmode);
        }
        AlterTableType::AT_SetStorage => {
            /* ALTER COLUMN SET STORAGE */
            address = ATExecSetStorage(rel, (*cmd).name, (*cmd).def as *mut Node, lockmode);
        }
        AlterTableType::AT_SetCompression => {
            /* ALTER COLUMN SET COMPRESSION */
            address = ATExecSetCompression(rel, (*cmd).name, (*cmd).def as *mut Node, lockmode);
        }
        AlterTableType::AT_DropColumn => {
            /* DROP COLUMN */
            address = ATExecDropColumn(wqueue, rel, (*cmd).name,
                (*cmd).behavior, (*cmd).recurse, false,
                (*cmd).missing_ok, lockmode,
                ptr::null_mut());
        }
        AlterTableType::AT_AddIndex => {
            /* ADD INDEX */
            address = ATExecAddIndex(tab, rel, (*cmd).def as *mut IndexStmt, false, lockmode);
        }
        AlterTableType::AT_ReAddIndex => {
            /* ADD INDEX (readd) */
            address = ATExecAddIndex(tab, rel, (*cmd).def as *mut IndexStmt, true, lockmode);
        }
        AlterTableType::AT_ReAddStatistics => {
            /* ADD STATISTICS */
            address = ATExecAddStatistics(tab, rel, (*cmd).def as *mut CreateStatsStmt, true, lockmode);
        }
        AlterTableType::AT_AddConstraint => {
            /* ADD CONSTRAINT */
            /* Transform the command only during initial examination */
            if cur_pass == AlterTablePass::AT_PASS_ADD_CONSTR {
                cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, (*cmd).recurse, lockmode, cur_pass, context);
            }
            /* Depending on constraint type, might be no more work to do now */
            if !cmd.is_null() {
                address = ATExecAddConstraint(wqueue, tab, rel,
                    (*cmd).def as *mut Constraint,
                    (*cmd).recurse, false, lockmode);
            }
        }
        AlterTableType::AT_ReAddConstraint => {
            /* Re-add pre-existing check constraint */
            address = ATExecAddConstraint(wqueue, tab, rel, (*cmd).def as *mut Constraint,
                true, true, lockmode);
        }
        AlterTableType::AT_ReAddDomainConstraint => {
            /* Re-add pre-existing domain check constraint */
            let atstmt = (*cmd).def as *mut AlterDomainStmt;
            address = AlterDomainAddConstraint((*atstmt).typeName, (*atstmt).def as *mut Node, ptr::null_mut());
        }
        AlterTableType::AT_ReAddComment => {
            /* Re-add existing comment */
            address = CommentObject((*cmd).def as *mut CommentStmt);
        }
        AlterTableType::AT_AddIndexConstraint => {
            /* ADD CONSTRAINT USING INDEX */
            address = ATExecAddIndexConstraint(tab, rel, (*cmd).def as *mut IndexStmt, lockmode);
        }
        AlterTableType::AT_AlterConstraint => {
            /* ALTER CONSTRAINT */
            address = ATExecAlterConstraint(wqueue, rel,
                castNode!(ATAlterConstraint, T_ATAlterConstraint, (*cmd).def as *mut Node),
                (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_ValidateConstraint => {
            /* VALIDATE CONSTRAINT */
            address = ATExecValidateConstraint(wqueue, rel, (*cmd).name, (*cmd).recurse,
                false, lockmode);
        }
        AlterTableType::AT_DropConstraint => {
            /* DROP CONSTRAINT */
            ATExecDropConstraint(rel, (*cmd).name, (*cmd).behavior,
                (*cmd).recurse,
                (*cmd).missing_ok, lockmode);
        }
        AlterTableType::AT_AlterColumnType => {
            /* ALTER COLUMN TYPE */
            /* parse transformation was done earlier */
            address = ATExecAlterColumnType(tab, rel, cmd, lockmode);
        }
        AlterTableType::AT_AlterColumnGenericOptions => {
            /* ALTER COLUMN OPTIONS */
            address = ATExecAlterColumnGenericOptions(rel, (*cmd).name,
                (*cmd).def as *mut List, lockmode);
        }
        AlterTableType::AT_ChangeOwner => {
            /* ALTER OWNER */
            ATExecChangeOwner(RelationGetRelid(rel),
                get_rolespec_oid((*cmd).newowner as *mut Node, false),
                false, lockmode);
        }
        AlterTableType::AT_ClusterOn => {
            /* CLUSTER ON */
            address = ATExecClusterOn(rel, (*cmd).name, lockmode);
        }
        AlterTableType::AT_DropCluster => {
            /* SET WITHOUT CLUSTER */
            ATExecDropCluster(rel, lockmode);
        }
        AlterTableType::AT_SetLogged | AlterTableType::AT_SetUnLogged => {
            /* nothing to do in phase 2; handled in phase 3 */
        }
        AlterTableType::AT_DropOids => {
            /* SET WITHOUT OIDS */
            /* nothing to do here, oid columns don't exist anymore */
        }
        AlterTableType::AT_SetAccessMethod => {
            /* SET ACCESS METHOD */
            /*
             * Only do this for partitioned tables, for which this is just a
             * catalog change.  Tables with storage are handled by Phase 3.
             */
            if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char
                && (*tab).chgAccessMethod
            {
                ATExecSetAccessMethodNoStorage(rel, (*tab).newAccessMethod);
            }
        }
        AlterTableType::AT_SetTableSpace => {
            /* SET TABLESPACE */
            /*
             * Only do this for partitioned tables and indexes, for which this
             * is just a catalog change.  Other relation types which have
             * storage are handled by Phase 3.
             */
            if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char
                || (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_INDEX as c_char
            {
                ATExecSetTableSpaceNoStorage(rel, (*tab).newTableSpace);
            }
        }
        AlterTableType::AT_SetRelOptions
        | AlterTableType::AT_ResetRelOptions
        | AlterTableType::AT_ReplaceRelOptions => {
            /* SET/RESET/REPLACE (...) */
            ATExecSetRelOptions(rel, (*cmd).def as *mut List, (*cmd).subtype, lockmode);
        }
        AlterTableType::AT_EnableTrig => {
            ATExecEnableDisableTrigger(rel, (*cmd).name,
                b'O' as c_char /* TRIGGER_FIRES_ON_ORIGIN */, false, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_EnableAlwaysTrig => {
            ATExecEnableDisableTrigger(rel, (*cmd).name,
                b'A' as c_char /* TRIGGER_FIRES_ALWAYS */, false, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_EnableReplicaTrig => {
            ATExecEnableDisableTrigger(rel, (*cmd).name,
                b'R' as c_char /* TRIGGER_FIRES_ON_REPLICA */, false, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_DisableTrig => {
            ATExecEnableDisableTrigger(rel, (*cmd).name,
                b'D' as c_char /* TRIGGER_DISABLED */, false, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_EnableTrigAll => {
            ATExecEnableDisableTrigger(rel, ptr::null(),
                b'O' as c_char /* TRIGGER_FIRES_ON_ORIGIN */, false, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_DisableTrigAll => {
            ATExecEnableDisableTrigger(rel, ptr::null(),
                b'D' as c_char /* TRIGGER_DISABLED */, false, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_EnableTrigUser => {
            ATExecEnableDisableTrigger(rel, ptr::null(),
                b'O' as c_char /* TRIGGER_FIRES_ON_ORIGIN */, true, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_DisableTrigUser => {
            ATExecEnableDisableTrigger(rel, ptr::null(),
                b'D' as c_char /* TRIGGER_DISABLED */, true, (*cmd).recurse, lockmode);
        }
        AlterTableType::AT_EnableRule => {
            ATExecEnableDisableRule(rel, (*cmd).name,
                b'O' as c_char /* RULE_FIRES_ON_ORIGIN */, lockmode);
        }
        AlterTableType::AT_EnableAlwaysRule => {
            ATExecEnableDisableRule(rel, (*cmd).name,
                b'A' as c_char /* RULE_FIRES_ALWAYS */, lockmode);
        }
        AlterTableType::AT_EnableReplicaRule => {
            ATExecEnableDisableRule(rel, (*cmd).name,
                b'R' as c_char /* RULE_FIRES_ON_REPLICA */, lockmode);
        }
        AlterTableType::AT_DisableRule => {
            ATExecEnableDisableRule(rel, (*cmd).name,
                b'D' as c_char /* RULE_DISABLED */, lockmode);
        }
        AlterTableType::AT_AddInherit => {
            address = ATExecAddInherit(rel, (*cmd).def as *mut RangeVar, lockmode);
        }
        AlterTableType::AT_DropInherit => {
            address = ATExecDropInherit(rel, (*cmd).def as *mut RangeVar, lockmode);
        }
        AlterTableType::AT_AddOf => {
            address = ATExecAddOf(rel, (*cmd).def as *mut TypeName, lockmode);
        }
        AlterTableType::AT_DropOf => {
            ATExecDropOf(rel, lockmode);
        }
        AlterTableType::AT_ReplicaIdentity => {
            ATExecReplicaIdentity(rel, (*cmd).def as *mut ReplicaIdentityStmt, lockmode);
        }
        AlterTableType::AT_EnableRowSecurity => {
            ATExecSetRowSecurity(rel, true);
        }
        AlterTableType::AT_DisableRowSecurity => {
            ATExecSetRowSecurity(rel, false);
        }
        AlterTableType::AT_ForceRowSecurity => {
            ATExecForceNoForceRowSecurity(rel, true);
        }
        AlterTableType::AT_NoForceRowSecurity => {
            ATExecForceNoForceRowSecurity(rel, false);
        }
        AlterTableType::AT_GenericOptions => {
            ATExecGenericOptions(rel, (*cmd).def as *mut List);
        }
        AlterTableType::AT_AttachPartition => {
            cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode, cur_pass, context);
            /* Assert(cmd != NULL) */
            if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as c_char {
                address = ATExecAttachPartition(wqueue, rel, (*cmd).def as *mut PartitionCmd, context);
            } else {
                address = ATExecAttachPartitionIdx(wqueue, rel,
                    (*((*cmd).def as *mut PartitionCmd)).name);
            }
        }
        AlterTableType::AT_DetachPartition => {
            cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode, cur_pass, context);
            /* Assert(cmd != NULL) */
            /* ATPrepCmd ensures it must be a table */
            /* Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) */
            address = ATExecDetachPartition(wqueue, tab, rel,
                (*((*cmd).def as *mut PartitionCmd)).name,
                (*((*cmd).def as *mut PartitionCmd)).concurrent);
        }
        AlterTableType::AT_DetachPartitionFinalize => {
            address = ATExecDetachPartitionFinalize(rel, (*((*cmd).def as *mut PartitionCmd)).name);
        }
        _ => {
            ereport!(ERROR, errmsg!("unrecognized alter table type: {}", (*cmd).subtype as i32));
        }
    }

    /*
     * Report the subcommand to interested event triggers.
     */
    if !cmd.is_null() {
        EventTriggerCollectAlterTableSubcmd(cmd as *mut Node, address);
    }

    /*
     * Bump the command counter to ensure the next subcommand in the sequence
     * can see the changes so far
     */
    CommandCounterIncrement();
}

/*
 * ATParseTransformCmd: perform parse transformation for one subcommand
 *
 * Returns the transformed subcommand tree, if there is one, else NULL.
 *
 * The parser may hand back additional AlterTableCmd(s) and/or other
 * utility statements, either before or after the original subcommand.
 * Other AlterTableCmds are scheduled into the appropriate slot of the
 * AlteredTableInfo (they had better be for later passes than the current one).
 * Utility statements that are supposed to happen before the AlterTableCmd
 * are executed immediately.  Those that are supposed to happen afterwards
 * are added to the tab->afterStmts list to be done at the very end.
 */
unsafe fn ATParseTransformCmd(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: *mut AlterTableUtilityContext,
) -> *mut AlterTableCmd {
    let mut newcmd: *mut AlterTableCmd = ptr::null_mut();
    let mut beforeStmts: *mut List = ptr::null_mut();
    let mut afterStmts: *mut List = ptr::null_mut();

    /* Gin up an AlterTableStmt with just this subcommand and this table */
    let mut atstmt = makeNode!(AlterTableStmt, T_AlterTableStmt) as *mut AlterTableStmt;
    (*atstmt).relation = makeRangeVar(
        get_namespace_name(RelationGetNamespace(rel)),
        pstrdup(RelationGetRelationName(rel)),
        -1,
    );
    (*(*atstmt).relation).inh = recurse as i8;
    (*atstmt).cmds = list_make1(cmd as *mut std::ffi::c_void);
    (*atstmt).objtype = ObjectType::OBJECT_TABLE; /* needn't be picky here */
    (*atstmt).missing_ok = false;

    /* Transform the AlterTableStmt */
    atstmt = transformAlterTableStmt(
        RelationGetRelid(rel),
        atstmt,
        (*context).queryString,
        &mut beforeStmts as *mut *mut List,
        &mut afterStmts as *mut *mut List,
    );

    /* Execute any statements that should happen before these subcommand(s) */
    foreach!(lc, beforeStmts, {
        let stmt = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut Node;
        ProcessUtilityForAlterTable(stmt, context);
        CommandCounterIncrement();
    });

    /* Examine the transformed subcommands and schedule them appropriately */
    foreach!(lc, (*atstmt).cmds, {
        let cmd2 = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut AlterTableCmd;
        let pass: AlterTablePass;

        /*
         * This switch need only cover the subcommand types that can be added
         * by parse_utilcmd.c; otherwise, we'll use the default strategy of
         * executing the subcommand immediately, as a substitute for the
         * original subcommand.
         */
        match (*cmd2).subtype {
            AlterTableType::AT_AddIndex => {
                pass = AlterTablePass::AT_PASS_ADD_INDEX;
            }
            AlterTableType::AT_AddIndexConstraint => {
                pass = AlterTablePass::AT_PASS_ADD_INDEXCONSTR;
            }
            AlterTableType::AT_AddConstraint => {
                /* Recursion occurs during execution phase */
                if recurse { (*cmd2).recurse = true; }
                let con = castNode!(Constraint, T_Constraint, (*cmd2).def as *mut Node);
                pass = match (*con).contype {
                    ConstrType::CONSTR_NOTNULL => AlterTablePass::AT_PASS_COL_ATTRS,
                    ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE | ConstrType::CONSTR_EXCLUSION
                        => AlterTablePass::AT_PASS_ADD_INDEXCONSTR,
                    _ => AlterTablePass::AT_PASS_ADD_OTHERCONSTR,
                };
            }
            AlterTableType::AT_AlterColumnGenericOptions => {
                /* This command never recurses */
                pass = AlterTablePass::AT_PASS_MISC;
            }
            _ => {
                pass = cur_pass;
            }
        }

        if (pass as i32) < cur_pass as i32 {
            /* Cannot schedule into a pass we already finished */
            ereport!(ERROR, errmsg!("ALTER TABLE scheduling failure: too late for pass {}", pass as i32));
        } else if (pass as i32) > cur_pass as i32 {
            /* OK, queue it up for later */
            (*tab).subcmds[pass as usize] = lappend((*tab).subcmds[pass as usize] as *mut std::ffi::c_void, cmd2 as *mut std::ffi::c_void) as *mut List;
        } else {
            /*
             * We should see at most one subcommand for the current pass,
             * which is the transformed version of the original subcommand.
             */
            if newcmd.is_null() && (*cmd).subtype == (*cmd2).subtype {
                /* Found the transformed version of our subcommand */
                newcmd = cmd2;
            } else {
                ereport!(ERROR, errmsg!("ALTER TABLE scheduling failure: bogus item for pass {}", pass as i32));
            }
        }
    });

    /* Queue up any after-statements to happen at the end */
    (*tab).afterStmts = list_concat((*tab).afterStmts, afterStmts);

    newcmd
}

/*
 * ATRewriteTables: ALTER TABLE phase 3
 */
unsafe fn ATRewriteTables(
    parsetree: *mut AlterTableStmt,
    wqueue: *mut *mut List,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    /* Go through each table that needs to be checked or rewritten */
    foreach!(ltab, *wqueue, {
        let tab = crate::nodes::pg_list::lfirst(current_cell!(ltab)) as *mut AlteredTableInfo;

        /* Relations without storage may be ignored here */
        if !RELKIND_HAS_STORAGE((*tab).relkind) {
            /* continue -- foreach! doesn't support continue, fall through */
        } else {

        /*
         * If we change column data types, the operation has to be propagated
         * to tables that use this table's rowtype as a column type.
         * tab->newvals will also be non-NULL in the case where we're adding a
         * column with a default.
         */
        if !(*tab).newvals.is_null() || (*tab).rewrite > 0 {
            let rel = table_open((*tab).relid, NoLock);
            find_composite_type_dependencies((*(*rel).rd_rel).reltype, rel, ptr::null_mut());
            table_close(rel, NoLock);
        }

        /*
         * We only need to rewrite the table if at least one column needs to
         * be recomputed, or we are changing its persistence or access method.
         */
        if (*tab).rewrite > 0 && (*tab).relkind != RELKIND_SEQUENCE as c_char {
            /* Build a temporary relation and copy data */
            let OldHeap = table_open((*tab).relid, NoLock);

            /*
             * We don't support rewriting of system catalogs.
             */
            if IsSystemRelation(OldHeap) {
                ereport!(ERROR,
                    errmsg!("cannot rewrite system relation \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy())
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }

            if RelationIsUsedAsCatalogTable(OldHeap) {
                ereport!(ERROR,
                    errmsg!("cannot rewrite table \"{}\" used as a catalog table",
                        CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy())
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }

            /*
             * Don't allow rewrite on temp tables of other backends.
             */
            if RELATION_IS_OTHER_TEMP(OldHeap) {
                ereport!(ERROR,
                    errmsg!("cannot rewrite temporary tables of other sessions")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }

            /*
             * Select destination tablespace (same as original unless user
             * requested a change)
             */
            let NewTableSpace = if (*tab).newTableSpace != 0 {
                (*tab).newTableSpace
            } else {
                (*(*OldHeap).rd_rel).reltablespace
            };

            /*
             * Select destination access method (same as original unless user
             * requested a change)
             */
            let NewAccessMethod = if (*tab).chgAccessMethod {
                (*tab).newAccessMethod
            } else {
                (*(*OldHeap).rd_rel).relam
            };

            /*
             * Select persistence of transient table (same as original unless
             * user requested a change)
             */
            let persistence = if (*tab).chgPersistence {
                (*tab).newrelpersistence
            } else {
                (*(*OldHeap).rd_rel).relpersistence
            };

            table_close(OldHeap, NoLock);

            /*
             * Fire off an Event Trigger now, before actually rewriting the
             * table.
             *
             * We don't support Event Trigger for nested commands anywhere,
             * here included, and parsetree is given NULL when coming from
             * AlterTableInternal.
             *
             * And fire it only once.
             */
            if !parsetree.is_null() {
                EventTriggerTableRewrite(parsetree as *mut Node, (*tab).relid, (*tab).rewrite);
            }

            /*
             * Create transient table that will receive the modified data.
             */
            let OIDNewHeap = make_new_heap((*tab).relid, NewTableSpace, NewAccessMethod,
                persistence, lockmode);

            /*
             * Copy the heap data into the new table with the desired
             * modifications, and test the current data within the table
             * against new constraints generated by ALTER TABLE commands.
             */
            ATRewriteTable(tab, OIDNewHeap);

            /*
             * Swap the physical files of the old and new heaps, then rebuild
             * indexes and discard the old heap.
             */
            finish_heap_swap((*tab).relid, OIDNewHeap,
                false, false, true,
                !OidIsValid((*tab).newTableSpace),
                RecentXmin(),
                ReadNextMultiXactId(),
                persistence);

            InvokeObjectPostAlterHook(RelationRelationId, (*tab).relid, 0);
        } else if (*tab).rewrite > 0 && (*tab).relkind == RELKIND_SEQUENCE as c_char {
            if (*tab).chgPersistence {
                SequenceChangePersistence((*tab).relid, (*tab).newrelpersistence);
            }
        } else {
            /*
             * If required, test the current data within the table against new
             * constraints generated by ALTER TABLE commands, but don't
             * rebuild data.
             */
            if !(*tab).constraints.is_null() || (*tab).verify_new_notnull
                || !(*tab).partition_constraint.is_null()
            {
                ATRewriteTable(tab, InvalidOid);
            }

            /*
             * If we had SET TABLESPACE but no reason to reconstruct tuples,
             * just do a block-by-block copy.
             */
            if (*tab).newTableSpace != 0 {
                ATExecSetTableSpace((*tab).relid, (*tab).newTableSpace, lockmode);
            }
        }

        /*
         * Also change persistence of owned sequences, so that it matches the
         * table persistence.
         */
        if (*tab).chgPersistence {
            let seqlist = getOwnedSequences((*tab).relid);
            foreach!(lc, seqlist, {
                let seq_relid = lfirst_oid(current_cell!(lc));
                SequenceChangePersistence(seq_relid, (*tab).newrelpersistence);
            });
        }

        } /* end RELKIND_HAS_STORAGE check */
    });

    /*
     * Foreign key constraints are checked in a final pass.
     */
    foreach!(ltab, *wqueue, {
        let tab = crate::nodes::pg_list::lfirst(current_cell!(ltab)) as *mut AlteredTableInfo;
        let mut rel: Relation = ptr::null_mut();

        /* Relations without storage may be ignored here too */
        if !RELKIND_HAS_STORAGE((*tab).relkind) {
            /* skip */
        } else {
            foreach!(lcon, (*tab).constraints, {
                let con = crate::nodes::pg_list::lfirst(current_cell!(lcon)) as *mut NewConstraint;

                if (*con).contype == ConstrType::CONSTR_FOREIGN {
                    let fkconstraint = (*con).qual as *mut Constraint;

                    if rel.is_null() {
                        /* Long since locked, no need for another */
                        rel = table_open((*tab).relid, NoLock);
                    }

                    let refrel = table_open((*con).refrelid, RowShareLock);

                    validateForeignKeyConstraint((*fkconstraint).conname, rel, refrel,
                        (*con).refindid,
                        (*con).conid,
                        (*con).conwithperiod);

                    /*
                     * No need to mark the constraint row as validated, we did
                     * that when we inserted the row earlier.
                     */

                    table_close(refrel, NoLock);
                }
            });

            if !rel.is_null() {
                table_close(rel, NoLock);
            }
        }
    });

    /* Finally, run any afterStmts that were queued up */
    foreach!(ltab, *wqueue, {
        let tab = crate::nodes::pg_list::lfirst(current_cell!(ltab)) as *mut AlteredTableInfo;

        foreach!(lc, (*tab).afterStmts, {
            let stmt = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut Node;
            ProcessUtilityForAlterTable(stmt, context);
            CommandCounterIncrement();
        });
    });
}

/*
 * ATRewriteTable: scan or rewrite one table
 *
 * A rewrite is requested by passing a valid OIDNewHeap; in that case, caller
 * must already hold AccessExclusiveLock on it.
 */
unsafe fn ATRewriteTable(tab: *mut AlteredTableInfo, OIDNewHeap: Oid) {
    /*
     * Open the relation(s).  We have surely already locked the existing
     * table.
     */
    let oldrel = table_open((*tab).relid, NoLock);
    let oldTupDesc = (*tab).oldDesc;
    let newTupDesc = RelationGetDescr(oldrel); /* includes all mods */

    let newrel: Relation;
    if OidIsValid(OIDNewHeap) {
        /* Assert(CheckRelationOidLockedByMe(OIDNewHeap, AccessExclusiveLock, false)) */
        newrel = table_open(OIDNewHeap, NoLock);
    } else {
        newrel = ptr::null_mut();
    }

    /*
     * Prepare a BulkInsertState and options for table_tuple_insert.
     * The FSM is empty, so don't bother using it.
     */
    let mycid: u32;
    let bistate: BulkInsertState;
    let ti_options: c_int;
    if !newrel.is_null() {
        mycid = GetCurrentCommandId(true);
        bistate = GetBulkInsertState();
        ti_options = 0x0002; /* TABLE_INSERT_SKIP_FSM */
    } else {
        /* keep compiler quiet about using these uninitialized */
        mycid = 0;
        bistate = ptr::null_mut();
        ti_options = 0;
    }

    /*
     * Generate the constraint and default execution states
     */

    let estate = CreateExecutorState();

    /* Build the needed expression execution states */
    let mut needscan = false;

    foreach!(l, (*tab).constraints, {
        let con = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut NewConstraint;

        match (*con).contype {
            ConstrType::CONSTR_CHECK => {
                needscan = true;
                (*con).qualstate = ExecPrepareExpr(
                    expand_generated_columns_in_expr((*con).qual as *mut Node, oldrel, 1) as *mut Expr,
                    estate);
            }
            ConstrType::CONSTR_FOREIGN => {
                /* Nothing to do here */
            }
            _ => {
                ereport!(ERROR, errmsg!("unrecognized constraint type: {}", (*con).contype as i32));
            }
        }
    });

    /* Build expression execution states for partition check quals */
    let mut partqualstate: *mut ExprState = ptr::null_mut();
    if !(*tab).partition_constraint.is_null() {
        needscan = true;
        partqualstate = ExecPrepareExpr((*tab).partition_constraint, estate);
    }

    foreach!(l, (*tab).newvals, {
        let ex = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut NewColumnValue;
        /* expr already planned */
        (*ex).exprstate = ExecInitExpr((*ex).expr as *mut Expr, ptr::null_mut());
    });

    let mut notnull_attrs: *mut List = crate::nodes::pg_list::NIL;
    let mut notnull_virtual_attrs: *mut List = crate::nodes::pg_list::NIL;
    if !newrel.is_null() || (*tab).verify_new_notnull {
        /*
         * If we are rebuilding the tuples OR if we added any new but not
         * verified not-null constraints, check all *valid* not-null
         * constraints.
         */
        let mut i = 0;
        while i < (*newTupDesc).natts {
            let wholeatt = TupleDescAttr(newTupDesc, i as usize);
            /* TODO(pg-port): attnullability check via CompactAttribute */
            let _ = wholeatt;
            i += 1;
        }
        if !notnull_attrs.is_null() || !notnull_virtual_attrs.is_null() {
            needscan = true;
        }
    }

    if !newrel.is_null() || needscan {
        let econtext = GetPerTupleExprContext(estate);

        /* Create necessary tuple slots. */
        let oldslot: *mut TupleTableSlot;
        let newslot: *mut TupleTableSlot;
        if (*tab).rewrite != 0 {
            /* Assert(newrel != NULL) */
            oldslot = MakeSingleTupleTableSlot(oldTupDesc, table_slot_callbacks(oldrel));
            newslot = MakeSingleTupleTableSlot(newTupDesc, table_slot_callbacks(newrel));
            ExecStoreAllNullTuple(newslot);
        } else {
            oldslot = MakeSingleTupleTableSlot(newTupDesc, table_slot_callbacks(oldrel));
            newslot = ptr::null_mut();
        }

        /*
         * Any attributes that are dropped according to the new tuple
         * descriptor can be set to NULL.
         */
        let mut dropped_attrs: *mut List = crate::nodes::pg_list::NIL;
        let mut i = 0;
        while i < (*newTupDesc).natts {
            if (*TupleDescAttr(newTupDesc, i as usize)).attisdropped {
                dropped_attrs = lappend_int(dropped_attrs, i);
            }
            i += 1;
        }

        /*
         * Scan through the rows, generating a new row if needed and then
         * checking all the constraints.
         */
        let snapshot = RegisterSnapshot(GetLatestSnapshot());
        let scan = table_beginscan(oldrel, snapshot, 0, ptr::null_mut());

        /*
         * Switch to per-tuple memory context and reset it for each tuple
         * produced, so we don't leak memory.
         */
        let oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

        while table_scan_getnextslot(scan, ForwardScanDirection, oldslot) {
            let insertslot: *mut TupleTableSlot;

            if (*tab).rewrite > 0 {
                /* Extract data from old tuple */
                slot_getallattrs(oldslot);
                ExecClearTuple(newslot);

                /* copy attributes */
                let nvalid = (*oldslot).tts_nvalid as usize;
                std::ptr::copy_nonoverlapping(
                    (*oldslot).tts_values,
                    (*newslot).tts_values,
                    nvalid,
                );
                std::ptr::copy_nonoverlapping(
                    (*oldslot).tts_isnull,
                    (*newslot).tts_isnull,
                    nvalid,
                );

                /* Set dropped attributes to null in new tuple */
                foreach!(lc, dropped_attrs, {
                    let idx = lfirst_int(current_cell!(lc)) as usize;
                    *(*newslot).tts_isnull.add(idx) = true;
                });

                /*
                 * Constraints and GENERATED expressions might reference the
                 * tableoid column.
                 */
                (*newslot).tts_tableOid = RelationGetRelid(oldrel);

                /*
                 * Process supplied expressions to replace selected columns.
                 *
                 * First, evaluate expressions whose inputs come from the old tuple.
                 */
                (*econtext).ecxt_scantuple = oldslot;

                foreach!(l, (*tab).newvals, {
                    let ex = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut NewColumnValue;
                    if (*ex).is_generated { /* continue */ } else {
                        let attidx = ((*ex).attnum - 1) as usize;
                        *(*newslot).tts_values.add(attidx) = ExecEvalExpr(
                            (*ex).exprstate, econtext,
                            (*newslot).tts_isnull.add(attidx));
                    }
                });

                ExecStoreVirtualTuple(newslot);

                /*
                 * Now, evaluate any expressions whose inputs come from the
                 * new tuple.
                 */
                (*econtext).ecxt_scantuple = newslot;

                foreach!(l, (*tab).newvals, {
                    let ex = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut NewColumnValue;
                    if !(*ex).is_generated { /* continue */ } else {
                        let attidx = ((*ex).attnum - 1) as usize;
                        *(*newslot).tts_values.add(attidx) = ExecEvalExpr(
                            (*ex).exprstate, econtext,
                            (*newslot).tts_isnull.add(attidx));
                    }
                });

                insertslot = newslot;
            } else {
                /*
                 * If there's no rewrite, old and new table are guaranteed to
                 * have the same AM, so we can just use the old slot.
                 */
                insertslot = oldslot;
            }

            /* Now check any constraints on the possibly-changed tuple */
            (*econtext).ecxt_scantuple = insertslot;

            /* notnull_attrs check -- TODO(pg-port): foreach_int when available */

            foreach!(l, (*tab).constraints, {
                let con = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut NewConstraint;

                match (*con).contype {
                    ConstrType::CONSTR_CHECK => {
                        if !ExecCheck((*con).qualstate, econtext) {
                            ereport!(ERROR,
                                errmsg!("check constraint \"{}\" of relation \"{}\" is violated by some row",
                                    CStr::from_ptr((*con).name).to_string_lossy(),
                                    CStr::from_ptr(RelationGetRelationName(oldrel)).to_string_lossy())
                                /* C also: errcode(ERRCODE_CHECK_VIOLATION) */
                            );
                        }
                    }
                    ConstrType::CONSTR_NOTNULL | ConstrType::CONSTR_FOREIGN => {
                        /* Nothing to do here */
                    }
                    _ => {
                        ereport!(ERROR, errmsg!("unrecognized constraint type: {}", (*con).contype as i32));
                    }
                }
            });

            if !partqualstate.is_null() && !ExecCheck(partqualstate, econtext) {
                if (*tab).validate_default {
                    ereport!(ERROR,
                        errmsg!("updated partition constraint for default partition \"{}\" would be violated by some row",
                            CStr::from_ptr(RelationGetRelationName(oldrel)).to_string_lossy())
                        /* C also: errcode(ERRCODE_CHECK_VIOLATION) */
                    );
                } else {
                    ereport!(ERROR,
                        errmsg!("partition constraint of relation \"{}\" is violated by some row",
                            CStr::from_ptr(RelationGetRelationName(oldrel)).to_string_lossy())
                        /* C also: errcode(ERRCODE_CHECK_VIOLATION) */
                    );
                }
            }

            /* Write the tuple out to the new relation */
            if !newrel.is_null() {
                table_tuple_insert(newrel, insertslot, mycid, ti_options, bistate);
            }

            ResetExprContext(econtext);

            CHECK_FOR_INTERRUPTS();
        }

        MemoryContextSwitchTo(oldCxt);
        table_endscan(scan);
        UnregisterSnapshot(snapshot);

        ExecDropSingleTupleTableSlot(oldslot);
        if !newslot.is_null() {
            ExecDropSingleTupleTableSlot(newslot);
        }
    }

    FreeExecutorState(estate);

    table_close(oldrel, NoLock);
    if !newrel.is_null() {
        FreeBulkInsertState(bistate);

        table_finish_bulk_insert(newrel, ti_options);

        table_close(newrel, NoLock);
    }
}

/*
 * ATGetQueueEntry: find or create an entry in the ALTER TABLE work queue
 */
unsafe fn ATGetQueueEntry(wqueue: *mut *mut List, rel: Relation) -> *mut AlteredTableInfo {
    let relid = RelationGetRelid(rel);

    foreach!(ltab, *wqueue, {
        let tab = crate::nodes::pg_list::lfirst(current_cell!(ltab)) as *mut AlteredTableInfo;
        if (*tab).relid == relid {
            return tab;
        }
    });

    /*
     * Not there, so add it.  Note that we make a copy of the relation's
     * existing descriptor before anything interesting can happen to it.
     */
    let tab = palloc0(std::mem::size_of::<AlteredTableInfo>()) as *mut AlteredTableInfo;
    (*tab).relid = relid;
    (*tab).rel = ptr::null_mut();            /* set later */
    (*tab).relkind = (*(*rel).rd_rel).relkind;
    (*tab).oldDesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
    (*tab).newAccessMethod = InvalidOid;
    (*tab).chgAccessMethod = false;
    (*tab).newTableSpace = InvalidOid;
    (*tab).newrelpersistence = RELPERSISTENCE_PERMANENT;
    (*tab).chgPersistence = false;

    *wqueue = lappend(*wqueue as *mut std::ffi::c_void, tab as *mut std::ffi::c_void) as *mut List;

    tab
}

unsafe fn alter_table_type_to_string(cmdtype: AlterTableType) -> *const c_char {
    match cmdtype {
        AlterTableType::AT_AddColumn
        | AlterTableType::AT_AddColumnToView => b"ADD COLUMN\0".as_ptr() as *const c_char,
        AlterTableType::AT_ColumnDefault
        | AlterTableType::AT_CookedColumnDefault => b"ALTER COLUMN ... SET DEFAULT\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropNotNull => b"ALTER COLUMN ... DROP NOT NULL\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetNotNull => b"ALTER COLUMN ... SET NOT NULL\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetExpression => b"ALTER COLUMN ... SET EXPRESSION\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropExpression => b"ALTER COLUMN ... DROP EXPRESSION\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetStatistics => b"ALTER COLUMN ... SET STATISTICS\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetOptions => b"ALTER COLUMN ... SET\0".as_ptr() as *const c_char,
        AlterTableType::AT_ResetOptions => b"ALTER COLUMN ... RESET\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetStorage => b"ALTER COLUMN ... SET STORAGE\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetCompression => b"ALTER COLUMN ... SET COMPRESSION\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropColumn => b"DROP COLUMN\0".as_ptr() as *const c_char,
        AlterTableType::AT_AddIndex
        | AlterTableType::AT_ReAddIndex => ptr::null(), /* not real grammar */
        AlterTableType::AT_AddConstraint
        | AlterTableType::AT_ReAddConstraint
        | AlterTableType::AT_ReAddDomainConstraint
        | AlterTableType::AT_AddIndexConstraint => b"ADD CONSTRAINT\0".as_ptr() as *const c_char,
        AlterTableType::AT_AlterConstraint => b"ALTER CONSTRAINT\0".as_ptr() as *const c_char,
        AlterTableType::AT_ValidateConstraint => b"VALIDATE CONSTRAINT\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropConstraint => b"DROP CONSTRAINT\0".as_ptr() as *const c_char,
        AlterTableType::AT_ReAddComment => ptr::null(), /* not real grammar */
        AlterTableType::AT_AlterColumnType => b"ALTER COLUMN ... SET DATA TYPE\0".as_ptr() as *const c_char,
        AlterTableType::AT_AlterColumnGenericOptions => b"ALTER COLUMN ... OPTIONS\0".as_ptr() as *const c_char,
        AlterTableType::AT_ChangeOwner => b"OWNER TO\0".as_ptr() as *const c_char,
        AlterTableType::AT_ClusterOn => b"CLUSTER ON\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropCluster => b"SET WITHOUT CLUSTER\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetAccessMethod => b"SET ACCESS METHOD\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetLogged => b"SET LOGGED\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetUnLogged => b"SET UNLOGGED\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropOids => b"SET WITHOUT OIDS\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetTableSpace => b"SET TABLESPACE\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetRelOptions => b"SET\0".as_ptr() as *const c_char,
        AlterTableType::AT_ResetRelOptions => b"RESET\0".as_ptr() as *const c_char,
        AlterTableType::AT_ReplaceRelOptions => ptr::null(), /* not real grammar */
        AlterTableType::AT_EnableTrig => b"ENABLE TRIGGER\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableAlwaysTrig => b"ENABLE ALWAYS TRIGGER\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableReplicaTrig => b"ENABLE REPLICA TRIGGER\0".as_ptr() as *const c_char,
        AlterTableType::AT_DisableTrig => b"DISABLE TRIGGER\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableTrigAll => b"ENABLE TRIGGER ALL\0".as_ptr() as *const c_char,
        AlterTableType::AT_DisableTrigAll => b"DISABLE TRIGGER ALL\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableTrigUser => b"ENABLE TRIGGER USER\0".as_ptr() as *const c_char,
        AlterTableType::AT_DisableTrigUser => b"DISABLE TRIGGER USER\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableRule => b"ENABLE RULE\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableAlwaysRule => b"ENABLE ALWAYS RULE\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableReplicaRule => b"ENABLE REPLICA RULE\0".as_ptr() as *const c_char,
        AlterTableType::AT_DisableRule => b"DISABLE RULE\0".as_ptr() as *const c_char,
        AlterTableType::AT_AddInherit => b"INHERIT\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropInherit => b"NO INHERIT\0".as_ptr() as *const c_char,
        AlterTableType::AT_AddOf => b"OF\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropOf => b"NOT OF\0".as_ptr() as *const c_char,
        AlterTableType::AT_ReplicaIdentity => b"REPLICA IDENTITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_EnableRowSecurity => b"ENABLE ROW SECURITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_DisableRowSecurity => b"DISABLE ROW SECURITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_ForceRowSecurity => b"FORCE ROW SECURITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_NoForceRowSecurity => b"NO FORCE ROW SECURITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_GenericOptions => b"OPTIONS\0".as_ptr() as *const c_char,
        AlterTableType::AT_AttachPartition => b"ATTACH PARTITION\0".as_ptr() as *const c_char,
        AlterTableType::AT_DetachPartition => b"DETACH PARTITION\0".as_ptr() as *const c_char,
        AlterTableType::AT_DetachPartitionFinalize => b"DETACH PARTITION ... FINALIZE\0".as_ptr() as *const c_char,
        AlterTableType::AT_AddIdentity => b"ALTER COLUMN ... ADD IDENTITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_SetIdentity => b"ALTER COLUMN ... SET\0".as_ptr() as *const c_char,
        AlterTableType::AT_DropIdentity => b"ALTER COLUMN ... DROP IDENTITY\0".as_ptr() as *const c_char,
        AlterTableType::AT_ReAddStatistics => ptr::null(), /* not real grammar */
        #[allow(unreachable_patterns)]
        _ => ptr::null(),
    }
}

/*
 * ATSimplePermissions
 *
 * - Ensure that it is a relation (or possibly a view)
 * - Ensure this user is the owner
 * - Ensure that it is not a system table
 */
unsafe fn ATSimplePermissions(
    cmdtype: AlterTableType,
    rel: Relation,
    allowed_targets: c_int,
) {
    let actual_target: c_int = match (*(*rel).rd_rel).relkind as u8 {
        b'r' /* RELKIND_RELATION */          => ATT_TABLE,
        b'p' /* RELKIND_PARTITIONED_TABLE */ => ATT_PARTITIONED_TABLE,
        b'v' /* RELKIND_VIEW */              => ATT_VIEW,
        b'm' /* RELKIND_MATVIEW */           => ATT_MATVIEW,
        b'i' /* RELKIND_INDEX */             => ATT_INDEX,
        b'I' /* RELKIND_PARTITIONED_INDEX */ => ATT_PARTITIONED_INDEX,
        b'c' /* RELKIND_COMPOSITE_TYPE */    => ATT_COMPOSITE_TYPE,
        b'f' /* RELKIND_FOREIGN_TABLE */     => ATT_FOREIGN_TABLE,
        b'S' /* RELKIND_SEQUENCE */          => ATT_SEQUENCE,
        _ => 0,
    };

    /* Wrong target type? */
    if (actual_target & allowed_targets) == 0 {
        let action_str = alter_table_type_to_string(cmdtype);
        if !action_str.is_null() {
            ereport!(ERROR,
                errmsg!("ALTER action {} cannot be performed on relation \"{}\"",
                    CStr::from_ptr(action_str).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errdetail_relkind_not_supported */
            );
        } else {
            /* internal error? */
            ereport!(ERROR,
                errmsg!("invalid ALTER action attempted on relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        }
    }

    /* Permissions checks */
    if !object_ownercheck(RelationRelationId, RelationGetRelid(rel), GetUserId()) {
        aclcheck_error(0 /* ACLCHECK_NOT_OWNER */, get_relkind_objtype((*(*rel).rd_rel).relkind),
            RelationGetRelationName(rel));
    }

    if !allowSystemTableMods() && IsSystemRelation(rel) {
        ereport!(ERROR,
            errmsg!("permission denied: \"{}\" is a system catalog",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }
}

/*
 * ATSimpleRecursion
 *
 * Simple table recursion sufficient for most ALTER TABLE operations.
 * All direct and indirect children are processed in an unspecified order.
 * Note that if a child inherits from the original table via multiple
 * inheritance paths, it will be visited just once.
 */
unsafe fn ATSimpleRecursion(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    /*
     * Propagate to children, if desired and if there are (or might be) any
     * children.
     */
    if recurse && (*(*rel).rd_rel).relhassubclass {
        let relid = RelationGetRelid(rel);
        let children = find_all_inheritors(relid, lockmode, ptr::null_mut());
        /*
         * find_all_inheritors does the recursive search of the inheritance
         * hierarchy, so all we have to do is process all of the relids in the
         * list that it returns.
         */
        foreach!(child, children, {
            let childrelid = lfirst_oid(current_cell!(child));
            if childrelid == relid {
                continue;
            }
            /* find_all_inheritors already got lock */
            let childrel = relation_open(childrelid, NoLock);
            CheckAlterTableIsSafe(childrel);
            ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
            relation_close(childrel, NoLock);
        });
    }
}

/*
 * Obtain list of partitions of the given table, locking them all at the given
 * lockmode and ensuring that they all pass CheckAlterTableIsSafe.
 *
 * This function is a no-op if the given relation is not a partitioned table;
 * in particular, nothing is done if it's a legacy inheritance parent.
 */
unsafe fn ATCheckPartitionsNotInUse(rel: Relation, lockmode: LOCKMODE) {
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        let inh = find_all_inheritors(RelationGetRelid(rel), lockmode, ptr::null_mut());
        /* first element is the parent rel; must ignore it */
        for_each_from!(cell, inh, 1, {
            /* find_all_inheritors already got lock */
            let childrel = table_open(lfirst_oid(current_cell!(cell)), NoLock);
            CheckAlterTableIsSafe(childrel);
            table_close(childrel, NoLock);
        });
        list_free(inh);
    }
}

/*
 * ATTypedTableRecursion
 *
 * Propagate ALTER TYPE operations to the typed tables of that type.
 * Also check the RESTRICT/CASCADE behavior.  Given CASCADE, also permit
 * recursion to inheritance children of the typed tables.
 */
unsafe fn ATTypedTableRecursion(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    /* Assert(rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE) */
    let children = find_typed_table_dependencies(
        (*(*rel).rd_rel).reltype,
        RelationGetRelationName(rel),
        (*cmd).behavior,
    );
    foreach!(child, children, {
        let childrelid = lfirst_oid(current_cell!(child));
        let childrel = relation_open(childrelid, lockmode);
        CheckAlterTableIsSafe(childrel);
        ATPrepCmd(wqueue, childrel, cmd, true, true, lockmode, context);
        relation_close(childrel, NoLock);
    });
}

/*
 * find_composite_type_dependencies
 *
 * Check to see if the type "typeOid" is being used as a column in some table
 * (possibly nested several levels deep in composite types, arrays, etc!).
 * Eventually, we'd like to propagate the check or rewrite operation
 * into such tables, but for now, just error out if we find any.
 *
 * Caller should provide either the associated relation of a rowtype,
 * or a type name (not both) for use in the error message, if any.
 *
 * Note that "typeOid" is not necessarily a composite type; it could also be
 * another container type such as an array or range, or a domain over one of
 * these things.  The name of this function is therefore somewhat historical,
 * but it's not worth changing.
 *
 * We assume that functions and views depending on the type are not reasons
 * to reject the ALTER.  (How safe is this really?)
 */
pub unsafe fn find_composite_type_dependencies(
    typeOid: Oid,
    origRelation: Relation,
    origTypeName: *const c_char,
) {
    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /*
     * We scan pg_depend to find those things that depend on the given type.
     * (We assume we can ignore refobjsubid for a type.)
     */
    let depRel = table_open(DependRelationId, AccessShareLock);

    let mut key: [ScanKeyData; 2] = std::mem::zeroed();
    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(TypeRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(typeOid),
    );

    let depScan = systable_beginscan(depRel, DependReferenceIndexId, true, ptr::null_mut(), 2, key.as_mut_ptr());

    loop {
        let depTup = systable_getnext(depScan);
        if !HeapTupleIsValid(depTup) {
            break;
        }
        let pg_depend = (*depTup).t_data as *mut FormData_pg_depend;

        /* Check for directly dependent types */
        if (*pg_depend).classid == TypeRelationId {
            /*
             * This must be an array, domain, or range containing the given
             * type, so recursively check for uses of this type.  Note that
             * any error message will mention the original type not the
             * container; this is intentional.
             */
            find_composite_type_dependencies((*pg_depend).objid, origRelation, origTypeName);
            continue;
        }

        /* Else, ignore dependees that aren't relations */
        if (*pg_depend).classid != RelationRelationId {
            continue;
        }

        let rel = relation_open((*pg_depend).objid, AccessShareLock);
        let tupleDesc = RelationGetDescr(rel);

        /*
         * If objsubid identifies a specific column, refer to that in error
         * messages.  Otherwise, search to see if there's a user column of the
         * type.  (We assume system columns are never of interesting types.)
         */
        let mut att: *mut FormData_pg_attribute = ptr::null_mut();
        if (*pg_depend).objsubid > 0 && (*pg_depend).objsubid <= (*tupleDesc).natts {
            att = TupleDescAttr(tupleDesc, ((*pg_depend).objsubid - 1) as usize);
        } else {
            let mut attno = 1i32;
            while attno <= (*tupleDesc).natts {
                let candidate = TupleDescAttr(tupleDesc, (attno - 1) as usize);
                if (*candidate).atttypid == typeOid && !(*candidate).attisdropped {
                    att = candidate;
                    break;
                }
                attno += 1;
            }
            if att.is_null() {
                /* No such column, so assume OK */
                relation_close(rel, AccessShareLock);
                continue;
            }
        }

        /*
         * We definitely should reject if the relation has storage.  If it's
         * partitioned, then perhaps we don't have to reject: if there are
         * partitions then we'll fail when we find one, else there is no
         * stored data to worry about.  However, it's possible that the type
         * change would affect conclusions about whether the type is sortable
         * or hashable and thus (if it's a partitioning column) break the
         * partitioning rule.  For now, reject for partitioned rels too.
         */
        if RELKIND_HAS_STORAGE((*(*rel).rd_rel).relkind)
            || RELKIND_HAS_PARTITIONS((*(*rel).rd_rel).relkind)
        {
            if !origTypeName.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot alter type \"{}\" because column \"{}.{}\" uses it",
                        CStr::from_ptr(origTypeName).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        CStr::from_ptr(NameStr_ref(&(*att).attname)).to_string_lossy()
                    )
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            } else if (*(*origRelation).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as i8 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot alter type \"{}\" because column \"{}.{}\" uses it",
                        CStr::from_ptr(RelationGetRelationName(origRelation)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        CStr::from_ptr(NameStr_ref(&(*att).attname)).to_string_lossy()
                    )
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            } else if (*(*origRelation).rd_rel).relkind == RELKIND_FOREIGN_TABLE as i8 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot alter foreign table \"{}\" because column \"{}.{}\" uses its row type",
                        CStr::from_ptr(RelationGetRelationName(origRelation)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        CStr::from_ptr(NameStr_ref(&(*att).attname)).to_string_lossy()
                    )
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot alter table \"{}\" because column \"{}.{}\" uses its row type",
                        CStr::from_ptr(RelationGetRelationName(origRelation)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        CStr::from_ptr(NameStr_ref(&(*att).attname)).to_string_lossy()
                    )
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }
        } else if OidIsValid((*(*rel).rd_rel).reltype) {
            /*
             * A view or composite type itself isn't a problem, but we must
             * recursively check for indirect dependencies via its rowtype.
             */
            find_composite_type_dependencies((*(*rel).rd_rel).reltype, origRelation, origTypeName);
        }

        relation_close(rel, AccessShareLock);
    }

    systable_endscan(depScan);
    relation_close(depRel, AccessShareLock);
}

/*
 * find_typed_table_dependencies
 *
 * Check to see if a composite type is being used as the type of a
 * typed table.  Abort if any are found and behavior is RESTRICT.
 * Else return the list of tables.
 */
unsafe fn find_typed_table_dependencies(
    typeOid: Oid,
    typeName: *const c_char,
    behavior: DropBehavior,
) -> *mut List {
    use crate::nodes::pg_list::NIL;
    let classRel = table_open(RelationRelationId, AccessShareLock);

    let mut key: [ScanKeyData; 1] = std::mem::zeroed();
    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_reloftype,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(typeOid),
    );

    let scan = table_beginscan_catalog(classRel, 1, key.as_mut_ptr());

    let mut result: *mut List = NIL;
    loop {
        let tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let classform = (*tuple).t_data as *mut FormData_pg_class;

        if behavior == DropBehavior::DROP_RESTRICT {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot alter type \"{}\" because it is the type of a typed table",
                    CStr::from_ptr(typeName).to_string_lossy()
                )
                /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                   errhint("Use ALTER ... CASCADE to alter the typed tables too.") */
            );
        } else {
            result = lappend_oid(result, (*classform).oid);
        }
    }

    table_endscan(scan);
    table_close(classRel, AccessShareLock);

    result
}

/*
 * check_of_type
 *
 * Check whether a type is suitable for CREATE TABLE OF/ALTER TABLE OF.  If it
 * isn't suitable, throw an error.  Currently, we require that the type
 * originated with CREATE TYPE AS.  We could support any row type, but doing so
 * would require handling a number of extra corner cases in the DDL commands.
 * (Also, allowing domain-over-composite would open up a can of worms about
 * whether and how the domain's constraints should apply to derived tables.)
 */
pub unsafe fn check_of_type(typetuple: HeapTuple) {
    let typ = (*typetuple).t_data as *mut FormData_pg_type;
    let mut typeOk = false;

    if (*typ).typtype == TYPTYPE_COMPOSITE as i8 {
        /* Assert(OidIsValid(typ->typrelid)) */
        let typeRelation = relation_open((*typ).typrelid, AccessShareLock);
        typeOk = (*(*typeRelation).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as i8;
        /*
         * Close the parent rel, but keep our AccessShareLock on it until xact
         * commit.  That will prevent someone else from deleting or ALTERing
         * the type before the typed table creation/conversion commits.
         */
        relation_close(typeRelation, NoLock);

        if !typeOk {
            ereport!(
                ERROR,
                errmsg!(
                    "type {} is the row type of another table",
                    CStr::from_ptr(format_type_be((*typ).oid)).to_string_lossy()
                )
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errdetail("A typed table must use a stand-alone composite type created with CREATE TYPE.") */
            );
        }
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "type {} is not a composite type",
                CStr::from_ptr(format_type_be((*typ).oid)).to_string_lossy()
            )
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
}

/*
 * ALTER TABLE ADD COLUMN
 *
 * Adds an additional attribute to a relation making the assumption that
 * CHECK, NOT NULL, and FOREIGN KEY constraints will be removed from the
 * AT_AddColumn AlterTableCmd by parse_utilcmd.c and added as independent
 * AlterTableCmd's.
 *
 * ADD COLUMN cannot use the normal ALTER TABLE recursion mechanism, because we
 * have to decide at runtime whether to recurse or not depending on whether we
 * actually add a column or merely merge with an existing column.  (We can't
 * check this in a static pre-pass because it won't handle multiple inheritance
 * situations correctly.)
 */
unsafe fn ATPrepAddColumn(
    wqueue: *mut *mut List,
    rel: Relation,
    recurse: bool,
    recursing: bool,
    is_view: bool,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    if (*(*rel).rd_rel).reloftype != InvalidOid && !recursing {
        ereport!(
            ERROR,
            errmsg!("cannot add column to typed table")
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as i8 {
        ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
    }

    if recurse && !is_view {
        (*cmd).recurse = true;
    }
}

/*
 * Add a column to a table.  The return value is the address of the
 * new column in the parent relation.
 *
 * cmd is pass-by-ref so that we can replace it with the parse-transformed
 * copy (but that happens only after we check for IF NOT EXISTS).
 */

// section: tablecmds_mid  (C lines 7217-14726)

// ---------------------------------------------------------------------------
// ATExecAddColumn  (continued from head section -- function starts at 7217
// which is the opening of the function body; signature is in head)
// ---------------------------------------------------------------------------

unsafe fn ATExecAddColumn(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    cmd: *mut *mut AlterTableCmd,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: *mut AlterTableUtilityContext,
) -> ObjectAddress {
    let myrelid = RelationGetRelid(rel);
    let col_def = castNode!(ColumnDef, T_ColumnDef, (*(*cmd)).def);
    let if_not_exists = (*(*cmd)).missing_ok;
    let pgclass: Relation;
    let attrdesc: Relation;
    let reltup: HeapTuple;
    let relform: Form_pg_class;
    let attribute: Form_pg_attribute;
    let newattnum: i32;
    let relkind: i8;
    let mut defval: *mut Expr = std::ptr::null_mut();
    let children: *mut List;
    let address: ObjectAddress;
    let mut tupdesc: TupleDesc = std::ptr::null_mut();

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            (*(*cmd)).subtype,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("cannot add column to a partition")
        );
    }

    attrdesc = table_open(AttributeRelationId, RowExclusiveLock);

    /*
     * Are we adding the column to a recursion child?  If so, check whether to
     * merge with an existing definition for the column.  If we do merge, we
     * must not recurse.  Children will already have the column, and recursing
     * into them would mess up attinhcount.
     */
    if (*col_def).inhcount > 0 {
        let tuple: HeapTuple;
        /* Does child already have a column by this name? */
        tuple = SearchSysCacheCopyAttName(myrelid, (*col_def).colname);
        if HeapTupleIsValid(tuple) {
            let childatt = GETSTRUCT(tuple) as Form_pg_attribute;
            let mut ctypeid: Oid = InvalidOid;
            let mut ctypmod: i32 = 0;
            let ccollid: Oid;

            /* Child column must match on type, typmod, and collation */
            typenameTypeIdAndMod(
                std::ptr::null_mut(),
                (*col_def).typeName,
                &mut ctypeid,
                &mut ctypmod,
            );
            if ctypeid != (*childatt).atttypid || ctypmod != (*childatt).atttypmod {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg!(
                        "child table \"{}\" has different type for column \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*col_def).colname).to_string_lossy()
                    )
                );
            }
            ccollid = GetColumnDefCollation(std::ptr::null_mut(), col_def, ctypeid);
            if ccollid != (*childatt).attcollation {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_COLLATION_MISMATCH),
                    errmsg!(
                        "child table \"{}\" has different collation for column \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*col_def).colname).to_string_lossy()
                    )
                    /* errdetail: "%s" versus "%s" */
                );
            }

            /* Bump the existing child att's inhcount */
            if pg_add_s16_overflow(
                (*childatt).attinhcount,
                1,
                &mut (*childatt).attinhcount,
            ) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg!("too many inheritance parents")
                );
            }
            CatalogTupleUpdate(attrdesc, &mut (*tuple).t_self, tuple);
            heap_freetuple(tuple);

            /* Inform the user about the merge */
            ereport!(
                NOTICE,
                errmsg!(
                    "merging definition of column \"{}\" for child \"{}\"",
                    std::ffi::CStr::from_ptr((*col_def).colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );

            table_close(attrdesc, RowExclusiveLock);
            /* Make the child column change visible */
            CommandCounterIncrement();
            return InvalidObjectAddress;
        }
    }

    /* skip if the name already exists and if_not_exists is true */
    if !check_for_column_name_collision(rel, (*col_def).colname, if_not_exists) {
        table_close(attrdesc, RowExclusiveLock);
        return InvalidObjectAddress;
    }

    /*
     * Okay, we need to add the column, so go ahead and do parse transformation.
     * When recursing, the command was already transformed.
     */
    if !context.is_null() && !recursing {
        *cmd = ATParseTransformCmd(
            wqueue, tab, rel, *cmd, recurse, lockmode, cur_pass, context,
        );
        Assert!(!(*cmd).is_null());
        // col_def re-cast after transform
        let _ = castNode!(ColumnDef, T_ColumnDef, (**cmd).def);
    }

    /*
     * Regular inheritance children are independent enough not to inherit the
     * identity column from parent hence cannot recursively add identity column
     * if the table has inheritance children.
     */
    if !(*col_def).identity.is_null()
        && (*col_def).identity != 0 as _
        && recurse
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
        && !find_inheritance_children(myrelid, NoLock).is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!(
                "cannot recursively add identity column to table that has child tables"
            )
        );
    }

    pgclass = table_open(RelationRelationId, RowExclusiveLock);
    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(myrelid));
    if !HeapTupleIsValid(reltup) {
        elog!(ERROR, "cache lookup failed for relation {}", myrelid);
    }
    relform = GETSTRUCT(reltup) as Form_pg_class;
    relkind = (*relform).relkind;

    /* Determine the new attribute's number */
    newattnum = (*relform).relnatts as i32 + 1;
    if newattnum > MaxHeapAttributeNumber as i32 {
        ereport!(
            ERROR,
            errcode(ERRCODE_TOO_MANY_COLUMNS),
            errmsg!(
                "tables can have at most {} columns",
                MaxHeapAttributeNumber
            )
        );
    }

    /* Construct new attribute's pg_attribute entry. */
    tupdesc = BuildDescForRelation(list_make1(col_def as *mut _));
    attribute = TupleDescAttr(tupdesc, 0);

    /* Fix up attribute number */
    (*attribute).attnum = newattnum as AttrNumber;

    /* make sure datatype is legal for a column */
    CheckAttributeType(
        NameStr!((*attribute).attname),
        (*attribute).atttypid,
        (*attribute).attcollation,
        list_make1_oid((*(*rel).rd_rel).reltype),
        if (*attribute).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            CHKATYPE_IS_VIRTUAL
        } else {
            0
        },
    );

    InsertPgAttributeTuples(attrdesc, tupdesc, myrelid, std::ptr::null_mut(), std::ptr::null_mut());
    table_close(attrdesc, RowExclusiveLock);

    /* Update pg_class tuple as appropriate */
    (*relform).relnatts = newattnum as i16;
    CatalogTupleUpdate(pgclass, &mut (*reltup).t_self, reltup);
    heap_freetuple(reltup);

    /* Post creation hook for new attribute */
    InvokeObjectPostCreateHook(RelationRelationId, myrelid, newattnum);
    table_close(pgclass, RowExclusiveLock);

    /* Make the attribute's catalog entry visible */
    CommandCounterIncrement();

    /* Store the DEFAULT, if any, in the catalogs */
    if !(*col_def).raw_default.is_null() {
        let raw_ent = palloc(core::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
        (*raw_ent).attnum = (*attribute).attnum;
        (*raw_ent).raw_default = copyObject((*col_def).raw_default as *mut _) as *mut _;
        (*raw_ent).generated = (*col_def).generated;

        /*
         * This function is intended for CREATE TABLE, so it processes a
         * _list_ of defaults, but we just do one.
         */
        AddRelationNewConstraints(
            rel,
            list_make1(raw_ent as *mut _),
            std::ptr::null_mut(),
            false,
            true,
            false,
            std::ptr::null_mut(),
        );
        /* Make the additional catalog changes visible */
        CommandCounterIncrement();
    }

    /*
     * Tell Phase 3 to fill in the default expression, if there is one.
     *
     * An exception occurs when the new column is of a domain type.
     */
    if RELKIND_HAS_STORAGE(relkind) {
        let has_domain_constraints: bool;
        let mut has_missing = false;

        /*
         * For an identity column, we can't use build_column_default(),
         * because the sequence ownership isn't set yet.
         */
        if (*col_def).identity != 0 as _ {
            let nve = makeNode!(NextValueExpr, T_NextValueExpr) as *mut NextValueExpr;
            (*nve).seqid =
                RangeVarGetRelid((*col_def).identitySequence, NoLock, false);
            (*nve).typeId = (*attribute).atttypid;
            defval = nve as *mut Expr;
        } else {
            defval = build_column_default(rel, (*attribute).attnum) as *mut Expr;
        }

        /* Build CoerceToDomain(NULL) expression if needed */
        has_domain_constraints = DomainHasConstraints((*attribute).atttypid);
        if defval.is_null() && has_domain_constraints {
            let mut base_type_mod = (*attribute).atttypmod;
            let base_type_id =
                getBaseTypeAndTypmod((*attribute).atttypid, &mut base_type_mod);
            let base_type_coll = get_typcollation(base_type_id);
            defval =
                makeNullConst(base_type_id, base_type_mod, base_type_coll) as *mut Expr;
            defval = coerce_to_target_type(
                std::ptr::null_mut(),
                defval as *mut Node,
                base_type_id,
                (*attribute).atttypid,
                (*attribute).atttypmod,
                COERCION_ASSIGNMENT,
                COERCE_IMPLICIT_CAST,
                -1,
            ) as *mut Expr;
            if defval.is_null() {
                /* should not happen */
                elog!(ERROR, "failed to coerce base type to domain");
            }
        }

        if !defval.is_null() {
            let newval =
                palloc0(core::mem::size_of::<NewColumnValue>()) as *mut NewColumnValue;

            /* Prepare defval for execution, either here or in Phase 3 */
            defval = expression_planner(defval);

            /* Add the new default to the newvals list */
            (*newval).attnum = (*attribute).attnum;
            (*newval).expr = defval;
            (*newval).is_generated = (*col_def).generated != 0 as _;

            (*tab).newvals = lappend((*tab).newvals, newval as *mut _);

            /*
             * Attempt to skip a complete table rewrite by storing the
             * specified DEFAULT value outside of the heap.
             */
            if (*(*rel).rd_rel).relkind == RELKIND_RELATION as i8
                && (*col_def).generated == 0 as _
                && !has_domain_constraints
                && !contain_volatile_functions(defval as *mut Node)
            {
                let estate = CreateExecutorState();
                let expr_state = ExecPrepareExpr(defval, estate);
                let mut missing_is_null = false;
                let missingval = ExecEvalExpr(
                    expr_state,
                    GetPerTupleExprContext(estate),
                    &mut missing_is_null,
                );
                /* If it turns out NULL, nothing to do; else store it */
                if !missing_is_null {
                    StoreAttrMissingVal(rel, (*attribute).attnum, missingval);
                    /* Make the additional catalog change visible */
                    CommandCounterIncrement();
                    has_missing = true;
                }
                FreeExecutorState(estate);
            } else {
                /*
                 * Failed to use missing mode.  We have to do a table rewrite
                 * to install the value --- unless it's a virtual generated column.
                 */
                if (*col_def).generated != ATTRIBUTE_GENERATED_VIRTUAL as i8 {
                    (*tab).rewrite |= AT_REWRITE_DEFAULT_VAL;
                }
            }
        }

        if !has_missing {
            /*
             * If the new column is NOT NULL, and there is no missing value,
             * tell Phase 3 it needs to check for NULLs.
             */
            (*tab).verify_new_notnull |= (*col_def).is_not_null;
        }
    }

    /* Add needed dependency entries for the new column. */
    add_column_datatype_dependency(myrelid, newattnum, (*attribute).atttypid);
    add_column_collation_dependency(myrelid, newattnum, (*attribute).attcollation);

    /*
     * Propagate to children as appropriate.
     */
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    /*
     * If we are told not to recurse, there had better not be any child tables.
     */
    if !children.is_null() && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("column must be added to child tables too")
        );
    }

    /* Children should see column as singly inherited */
    let childcmd: *mut AlterTableCmd;
    if !recursing {
        childcmd = copyObject(*cmd as *mut _) as *mut AlterTableCmd;
        let child_coldef = castNode!(ColumnDef, T_ColumnDef, (*childcmd).def);
        (*child_coldef).inhcount = 1;
        (*child_coldef).is_local = false;
    } else {
        childcmd = *cmd; /* no need to copy again */
    }

    let mut lc = list_head(children);
    while !lc.is_null() {
        let childrelid = lfirst_oid(lc);
        let childrel: Relation;
        let childtab: *mut AlteredTableInfo;

        /* find_inheritance_children already got lock */
        childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        /* Find or create work queue entry for this table */
        childtab = ATGetQueueEntry(wqueue, childrel);

        /* Recurse to child; return value is ignored */
        ATExecAddColumn(
            wqueue, childtab, childrel, &mut (childcmd as *mut AlterTableCmd),
            recurse, true, lockmode, cur_pass, context,
        );

        table_close(childrel, NoLock);
        lc = lnext(children, lc);
    }

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    ObjectAddressSubSet!(address, RelationRelationId, myrelid, newattnum);
    address
}

/*
 * If a new or renamed column will collide with the name of an existing
 * column and if_not_exists is false then error out, else do nothing.
 */
unsafe fn check_for_column_name_collision(
    rel: Relation,
    colname: *const i8,
    if_not_exists: bool,
) -> bool {
    let att_tuple: HeapTuple;
    let attnum: i32;

    /*
     * this test is deliberately not attisdropped-aware, since if one tries to
     * add a column matching a dropped column name, it's gonna fail anyway.
     */
    att_tuple = SearchSysCache2(
        ATTNAME,
        ObjectIdGetDatum(RelationGetRelid(rel)),
        PointerGetDatum(colname as *mut _),
    );
    if !HeapTupleIsValid(att_tuple) {
        return true;
    }

    attnum = (*(GETSTRUCT(att_tuple) as Form_pg_attribute)).attnum as i32;
    ReleaseSysCache(att_tuple);

    /*
     * We throw a different error message for conflicts with system column names.
     */
    if attnum <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_DUPLICATE_COLUMN),
            errmsg!(
                "column name \"{}\" conflicts with a system column name",
                std::ffi::CStr::from_ptr(colname).to_string_lossy()
            )
        );
    } else {
        if if_not_exists {
            ereport!(
                NOTICE,
                errcode(ERRCODE_DUPLICATE_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" already exists, skipping",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            return false;
        }
        ereport!(
            ERROR,
            errcode(ERRCODE_DUPLICATE_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" already exists",
                std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    true
}

/* Install a column's dependency on its datatype. */
unsafe fn add_column_datatype_dependency(relid: Oid, attnum: i32, typid: Oid) {
    let mut myself = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut referenced = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    myself.classId = RelationRelationId;
    myself.objectId = relid;
    myself.objectSubId = attnum;
    referenced.classId = TypeRelationId;
    referenced.objectId = typid;
    referenced.objectSubId = 0;
    recordDependencyOn(&mut myself, &mut referenced, DEPENDENCY_NORMAL);
}

/* Install a column's dependency on its collation. */
unsafe fn add_column_collation_dependency(relid: Oid, attnum: i32, collid: Oid) {
    let mut myself = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut referenced = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    /* We know the default collation is pinned, so don't bother recording it */
    if OidIsValid(collid) && collid != DEFAULT_COLLATION_OID {
        myself.classId = RelationRelationId;
        myself.objectId = relid;
        myself.objectSubId = attnum;
        referenced.classId = CollationRelationId;
        referenced.objectId = collid;
        referenced.objectSubId = 0;
        recordDependencyOn(&mut myself, &mut referenced, DEPENDENCY_NORMAL);
    }
}

/*
 * ALTER TABLE ALTER COLUMN DROP NOT NULL
 *
 * Return the address of the modified column.  If the column was already
 * nullable, InvalidObjectAddress is returned.
 */
unsafe fn ATExecDropNotNull(
    rel: Relation,
    col_name: *const i8,
    recurse: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let con_tup: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attr_rel: Relation;
    let mut address = InvalidObjectAddress;

    /* lookup the attribute */
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);

    /* If the column is already nullable there's nothing to do. */
    if !(*att_tup).attnotnull {
        table_close(attr_rel, RowExclusiveLock);
        return InvalidObjectAddress;
    }

    /* Prevent them from altering a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*att_tup).attidentity != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_SYNTAX_ERROR),
            errmsg!(
                "column \"{}\" of relation \"{}\" is an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * If rel is partition, shouldn't drop NOT NULL if parent has the same.
     */
    if (*(*rel).rd_rel).relispartition {
        let parent_id = get_partition_parent(RelationGetRelid(rel), false);
        let parent = table_open(parent_id, AccessShareLock);
        let tup_desc = RelationGetDescr(parent);
        let parent_attnum = get_attnum(parent_id, col_name);
        if (*TupleDescAttr(tup_desc, (parent_attnum as i32 - 1) as usize)).attnotnull {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!(
                    "column \"{}\" is marked NOT NULL in parent table",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy()
                )
            );
        }
        table_close(parent, AccessShareLock);
    }

    /*
     * Find the constraint that makes this column NOT NULL, and drop it.
     * dropconstraint_internal() resets attnotnull.
     */
    con_tup = findNotNullConstraintAttnum(RelationGetRelid(rel), attnum);
    if con_tup.is_null() {
        elog!(
            ERROR,
            "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
            std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /* The normal case: we have a pg_constraint row, remove it */
    dropconstraint_internal(
        rel, con_tup, DROP_RESTRICT, recurse, false, false, lockmode,
    );
    heap_freetuple(con_tup);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    table_close(attr_rel, RowExclusiveLock);

    address
}

/*
 * set_attnotnull
 *   Helper to update/validate the pg_attribute status of a not-null constraint
 */
unsafe fn set_attnotnull(
    wqueue: *mut *mut List,
    rel: Relation,
    attnum: AttrNumber,
    is_valid: bool,
    queue_validation: bool,
) {
    let attr: Form_pg_attribute;
    let thisatt: *mut CompactAttribute;

    Assert!(!queue_validation || !wqueue.is_null());

    CheckAlterTableIsSafe(rel);

    /*
     * Exit quickly by testing attnotnull from the tupledesc's copy of the attribute.
     */
    attr = TupleDescAttr(RelationGetDescr(rel), (attnum as i32 - 1) as usize);
    if (*attr).attisdropped {
        return;
    }

    if !(*attr).attnotnull {
        let attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
        let tuple = SearchSysCacheCopyAttNum(RelationGetRelid(rel), attnum);
        if !HeapTupleIsValid(tuple) {
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                attnum,
                RelationGetRelid(rel)
            );
        }

        thisatt = TupleDescCompactAttr(RelationGetDescr(rel), (attnum as i32 - 1) as usize);
        (*thisatt).attnullability = ATTNULLABLE_VALID as u8;

        let attr_form = GETSTRUCT(tuple) as Form_pg_attribute;
        (*attr_form).attnotnull = true;
        CatalogTupleUpdate(attr_rel, &mut (*tuple).t_self, tuple);

        /*
         * If the nullness isn't already proven by validated constraints, have
         * ALTER TABLE phase 3 test for it.
         */
        if queue_validation && !wqueue.is_null()
            && !NotNullImpliedByRelConstraints(rel, attr_form)
        {
            let tab = ATGetQueueEntry(wqueue, rel);
            (*tab).verify_new_notnull = true;
        }

        CommandCounterIncrement();
        table_close(attr_rel, RowExclusiveLock);
        heap_freetuple(tuple);
    } else {
        CacheInvalidateRelcache(rel);
    }
}

/*
 * ALTER TABLE ALTER COLUMN SET NOT NULL
 *
 * Add a not-null constraint to a single table and its children.
 */
unsafe fn ATExecSetNotNull(
    wqueue: *mut *mut List,
    rel: Relation,
    con_name: *mut i8,
    col_name: *mut i8,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;
    let constraint: *mut Constraint;
    let ccon: *mut CookedConstraint;
    let cooked: *mut List;
    let mut is_no_inherit = false;

    /* Guard against stack overflow due to overly deep inheritance tree. */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_AddConstraint,
            rel,
            ATT_PARTITIONED_TABLE | ATT_TABLE | ATT_FOREIGN_TABLE,
        );
        Assert!(!con_name.is_null());
    }

    attnum = get_attnum(RelationGetRelid(rel), col_name);
    if attnum == InvalidAttrNumber {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* Prevent them from altering a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /* See if there's already a constraint */
    tuple = findNotNullConstraintAttnum(RelationGetRelid(rel), attnum);
    if HeapTupleIsValid(tuple) {
        let con_form = GETSTRUCT(tuple) as Form_pg_constraint;
        let mut changed = false;

        /*
         * Don't let a NO INHERIT constraint be changed into inherit.
         */
        if (*con_form).connoinherit && recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*con_form).conname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }

        /*
         * If we find an appropriate constraint, increment coninhcount if recursing,
         * set conislocal if not, or validate if not already validated.
         */
        if recursing {
            if pg_add_s16_overflow(
                (*con_form).coninhcount,
                1,
                &mut (*con_form).coninhcount,
            ) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg!("too many inheritance parents")
                );
            }
            changed = true;
        } else if !(*con_form).conislocal {
            (*con_form).conislocal = true;
            changed = true;
        } else if !(*con_form).convalidated {
            /*
             * Flip attnotnull and convalidated, and also validate the constraint.
             */
            return ATExecValidateConstraint(
                wqueue,
                rel,
                NameStr!((*con_form).conname) as *mut i8,
                recurse,
                recursing,
                lockmode,
            );
        }

        if changed {
            let constr_rel = table_open(ConstraintRelationId, RowExclusiveLock);
            CatalogTupleUpdate(constr_rel, &mut (*tuple).t_self, tuple);
            ObjectAddressSet!(address, ConstraintRelationId, (*con_form).oid);
            table_close(constr_rel, RowExclusiveLock);
        }

        if changed {
            return address;
        } else {
            return InvalidObjectAddress;
        }
    }

    /*
     * If we're asked not to recurse, and children exist, raise an error for
     * partitioned tables.
     */
    if !recurse
        && !find_inheritance_children(RelationGetRelid(rel), NoLock).is_null()
    {
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("constraint must be added to child tables too")
                /* errhint: "Do not specify the ONLY keyword." */
            );
        } else {
            is_no_inherit = true;
        }
    }

    /*
     * No constraint exists; we must add one.  Determine a name to use.
     */
    let con_name_used: *mut i8;
    if !recursing {
        Assert!(con_name.is_null());
        con_name_used = ChooseConstraintName(
            RelationGetRelationName(rel),
            col_name,
            b"not_null\0".as_ptr() as *const i8,
            RelationGetNamespace(rel),
            std::ptr::null_mut(),
        );
    } else {
        con_name_used = con_name;
    }

    constraint = makeNotNullConstraint(makeString(col_name));
    (*constraint).is_no_inherit = is_no_inherit;
    (*constraint).conname = con_name_used;

    /* and do it */
    cooked = AddRelationNewConstraints(
        rel,
        std::ptr::null_mut(),
        list_make1(constraint as *mut _),
        false,
        !recursing,
        false,
        std::ptr::null_mut(),
    );
    ccon = linitial(cooked) as *mut CookedConstraint;
    ObjectAddressSet!(address, ConstraintRelationId, (*ccon).conoid);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    /* Mark pg_attribute.attnotnull for the column and queue validation */
    set_attnotnull(wqueue, rel, attnum, true, true);

    /* Recurse to propagate the constraint to children that don't have one. */
    if recurse {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childoid = lfirst_oid(lc);
            let childrel = table_open(childoid, NoLock);
            CommandCounterIncrement();
            ATExecSetNotNull(wqueue, childrel, con_name_used, col_name, recurse, true, lockmode);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    address
}

/*
 * NotNullImpliedByRelConstraints
 *   Does rel's existing constraints imply NOT NULL for the given attribute?
 */
unsafe fn NotNullImpliedByRelConstraints(
    rel: Relation,
    attr: Form_pg_attribute,
) -> bool {
    let nnulltest = makeNode!(NullTest, T_NullTest) as *mut NullTest;

    (*nnulltest).arg = makeVar(
        1,
        (*attr).attnum,
        (*attr).atttypid,
        (*attr).atttypmod,
        (*attr).attcollation,
        0,
    ) as *mut Expr;
    (*nnulltest).nulltesttype = IS_NOT_NULL;

    /*
     * argisrow = false is correct even for a composite column.
     */
    (*nnulltest).argisrow = false;
    (*nnulltest).location = -1;

    if ConstraintImpliedByRelConstraint(
        rel,
        list_make1(nnulltest as *mut _),
        std::ptr::null_mut(),
    ) {
        ereport!(
            DEBUG1,
            errmsg_internal!(
                "existing constraints on column \"{}.{}\" are sufficient to prove that it does not contain nulls",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                std::ffi::CStr::from_ptr(NameStr!((*attr).attname) as *const i8).to_string_lossy()
            )
        );
        return true;
    }

    false
}

/*
 * ALTER TABLE ALTER COLUMN SET/DROP DEFAULT
 *
 * Return the address of the affected column.
 */
unsafe fn ATExecColumnDefault(
    rel: Relation,
    col_name: *const i8,
    new_default: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tupdesc = RelationGetDescr(rel);
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;

    /* get the number of the attribute */
    attnum = get_attnum(RelationGetRelid(rel), col_name);
    if attnum == InvalidAttrNumber {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* Prevent them from altering a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*TupleDescAttr(tupdesc, (attnum as i32 - 1) as usize)).attidentity != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_SYNTAX_ERROR),
            errmsg!(
                "column \"{}\" of relation \"{}\" is an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errhint depends on new_default */
        );
    }

    if (*TupleDescAttr(tupdesc, (attnum as i32 - 1) as usize)).attgenerated != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_SYNTAX_ERROR),
            errmsg!(
                "column \"{}\" of relation \"{}\" is a generated column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * Remove any old default for the column.
     */
    RemoveAttrDefault(
        RelationGetRelid(rel),
        attnum,
        DROP_RESTRICT,
        false,
        !new_default.is_null(),
    );

    if !new_default.is_null() {
        /* SET DEFAULT */
        let raw_ent =
            palloc(core::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
        (*raw_ent).attnum = attnum;
        (*raw_ent).raw_default = new_default;
        (*raw_ent).generated = 0 as _;

        AddRelationNewConstraints(
            rel,
            list_make1(raw_ent as *mut _),
            std::ptr::null_mut(),
            false,
            true,
            false,
            std::ptr::null_mut(),
        );
    }

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * Add a pre-cooked default expression.
 *
 * Return the address of the affected column.
 */
unsafe fn ATExecCookedColumnDefault(
    rel: Relation,
    attnum: AttrNumber,
    new_default: *mut Node,
) -> ObjectAddress {
    let mut address = InvalidObjectAddress;

    /* We assume no checking is required */

    /*
     * Remove any old default for the column.
     */
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, true);

    StoreAttrDefault(rel, attnum, new_default, true);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE ALTER COLUMN ADD IDENTITY
 *
 * Return the address of the affected column.
 */
unsafe fn ATExecAddIdentity(
    rel: Relation,
    col_name: *const i8,
    def: *mut Node,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> ObjectAddress {
    let attrelation: Relation;
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;
    let cdef = castNode!(ColumnDef, T_ColumnDef, def);
    let ispartitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8;

    if ispartitioned && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot add identity to a column of only the partitioned table")
            /* errhint: "Do not specify the ONLY keyword." */
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot add identity to a column of a partition")
        );
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    /* Can't alter a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Creating a column as identity implies NOT NULL, so adding the identity
     * to an existing column that is not NOT NULL would create a state that
     * cannot be reproduced without contortions.
     */
    if !(*att_tup).attnotnull {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" must be declared NOT NULL before identity can be added",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* If a not-null constraint exists, verify it's compatible. */
    if (*att_tup).attnotnull {
        let contup = findNotNullConstraintAttnum(RelationGetRelid(rel), attnum);
        if !HeapTupleIsValid(contup) {
            elog!(
                ERROR,
                "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        }
        let con_form = GETSTRUCT(contup) as Form_pg_constraint;
        if !(*con_form).convalidated {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*con_form).conname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errhint: "You might need to validate it using ..." */
            );
        }
    }

    if (*att_tup).attidentity != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" is already an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    if (*att_tup).atthasdef {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" already has a default value",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    (*att_tup).attidentity = (*cdef).identity;
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*att_tup).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Recurse to propagate the identity column to partitions.
     * Identity is not inherited in regular inheritance children.
     */
    if recurse && ispartitioned {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrel = table_open(lfirst_oid(lc), NoLock);
            ATExecAddIdentity(childrel, col_name, def, lockmode, recurse, true);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    address
}

/*
 * ALTER TABLE ALTER COLUMN SET { GENERATED or sequence options }
 *
 * Return the address of the affected column.
 */
unsafe fn ATExecSetIdentity(
    rel: Relation,
    col_name: *const i8,
    def: *mut Node,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> ObjectAddress {
    let mut generated_el: *mut DefElem = std::ptr::null_mut();
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attrelation: Relation;
    let mut address = InvalidObjectAddress;
    let ispartitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8;

    if ispartitioned && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot change identity column of only the partitioned table")
            /* errhint: "Do not specify the ONLY keyword." */
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot change identity column of a partition")
        );
    }

    {
        let option_list = castNode!(List, T_List, def);
        let mut lc = list_head(option_list);
        while !lc.is_null() {
            let defel = lfirst_node!(DefElem, T_DefElem, current_cell!(lc));
            if libc::strcmp((*defel).defname, b"generated\0".as_ptr() as *const i8) == 0 {
                if !generated_el.is_null() {
                    ereport!(
                        ERROR,
                        errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg!("conflicting or redundant options")
                    );
                }
                generated_el = defel;
            } else {
                elog!(
                    ERROR,
                    "option \"{}\" not recognized",
                    std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()
                );
            }
            lc = lnext(option_list, lc);
        }
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if !(*att_tup).attidentity != false {
        // attidentity == 0
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" is not an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    if !generated_el.is_null() {
        (*att_tup).attidentity = defGetInt32(generated_el) as i8;
        CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

        InvokeObjectPostAlterHook(
            RelationRelationId,
            RelationGetRelid(rel),
            (*att_tup).attnum as i32,
        );
        ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    } else {
        address = InvalidObjectAddress;
    }

    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Recurse to propagate the identity change to partitions.
     * Identity is not inherited in regular inheritance children.
     */
    if !generated_el.is_null() && recurse && ispartitioned {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrel = table_open(lfirst_oid(lc), NoLock);
            ATExecSetIdentity(childrel, col_name, def, lockmode, recurse, true);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    address
}

/*
 * ALTER TABLE ALTER COLUMN DROP IDENTITY
 *
 * Return the address of the affected column.
 */
unsafe fn ATExecDropIdentity(
    rel: Relation,
    col_name: *const i8,
    missing_ok: bool,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attrelation: Relation;
    let mut address = InvalidObjectAddress;
    let seqid: Oid;
    let mut seqaddress: ObjectAddress = InvalidObjectAddress;
    let ispartitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8;

    if ispartitioned && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot drop identity from a column of only the partitioned table")
            /* errhint */
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot drop identity from a column of a partition")
        );
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*att_tup).attidentity == 0 as _ {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not an identity column",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not an identity column, skipping",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            heap_freetuple(tuple);
            table_close(attrelation, RowExclusiveLock);
            return InvalidObjectAddress;
        }
    }

    (*att_tup).attidentity = 0 as _;
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*att_tup).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Recurse to drop the identity from column in partitions.
     * Identity is not inherited in regular inheritance children.
     */
    if recurse && ispartitioned {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrel = table_open(lfirst_oid(lc), NoLock);
            ATExecDropIdentity(childrel, col_name, false, lockmode, recurse, true);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    if !recursing {
        /* drop the internal sequence */
        seqid = getIdentitySequence(rel, attnum, false);
        deleteDependencyRecordsForClass(
            RelationRelationId,
            seqid,
            RelationRelationId,
            DEPENDENCY_INTERNAL,
        );
        CommandCounterIncrement();
        seqaddress.classId = RelationRelationId;
        seqaddress.objectId = seqid;
        seqaddress.objectSubId = 0;
        performDeletion(&mut seqaddress, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }

    address
}

/*
 * ALTER TABLE ALTER COLUMN SET EXPRESSION
 *
 * Return the address of the affected column.
 */
unsafe fn ATExecSetExpression(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    col_name: *const i8,
    new_expr: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attgenerated: i8;
    let rewrite: bool;
    let attrdefoid: Oid;
    let mut address = InvalidObjectAddress;
    let defval: *mut Expr;
    let newval: *mut NewColumnValue;
    let raw_ent: *mut RawColumnDefault;

    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    attgenerated = (*att_tup).attgenerated;
    if attgenerated == 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" is not a generated column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * TODO: This could be done, just need to recheck any constraints afterwards.
     */
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8
        && !(*(*rel).rd_att).constr.is_null()
        && (*(*(*rel).rd_att).constr).num_check > 0
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in tables with check constraints"
            )
            /* errdetail */
        );
    }

    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 && (*att_tup).attnotnull {
        (*tab).verify_new_notnull = true;
    }

    /*
     * We need to prevent this because a change of expression could affect a row filter.
     */
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8
        && !GetRelationPublications(RelationGetRelid(rel)).is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in tables that are part of a publication"
            )
        );
    }

    rewrite = attgenerated == ATTRIBUTE_GENERATED_STORED as i8;

    ReleaseSysCache(tuple);

    if rewrite {
        /*
         * Clear all the missing values if we're rewriting the table.
         */
        RelationClearMissing(rel);
        /* make sure we don't conflict with later attribute modifications */
        CommandCounterIncrement();

        /*
         * Find everything that depends on the column and record enough information
         * to let us recreate the objects after rewrite.
         */
        RememberAllDependentForRebuilding(tab, AT_SetExpression, rel, attnum, col_name);
    }

    /*
     * Drop the dependency records of the GENERATED expression.
     */
    attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
    if !OidIsValid(attrdefoid) {
        elog!(
            ERROR,
            "could not find attrdef tuple for relation {} attnum {}",
            RelationGetRelid(rel),
            attnum
        );
    }
    deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);

    /* Make above changes visible */
    CommandCounterIncrement();

    /*
     * Get rid of the GENERATED expression itself.
     */
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

    /* Prepare to store the new expression, in the catalogs */
    raw_ent = palloc(core::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
    (*raw_ent).attnum = attnum;
    (*raw_ent).raw_default = new_expr;
    (*raw_ent).generated = attgenerated;

    /* Store the generated expression */
    AddRelationNewConstraints(
        rel,
        list_make1(raw_ent as *mut _),
        std::ptr::null_mut(),
        false,
        true,
        false,
        std::ptr::null_mut(),
    );

    /* Make above new expression visible */
    CommandCounterIncrement();

    if rewrite {
        /* Prepare for table rewrite */
        defval = build_column_default(rel, attnum) as *mut Expr;
        newval = palloc0(core::mem::size_of::<NewColumnValue>()) as *mut NewColumnValue;
        (*newval).attnum = attnum;
        (*newval).expr = expression_planner(defval);
        (*newval).is_generated = true;

        (*tab).newvals = lappend((*tab).newvals, newval as *mut _);
        (*tab).rewrite |= AT_REWRITE_DEFAULT_VAL;
    }

    /* Drop any pg_statistic entry for the column */
    RemoveStatistics(RelationGetRelid(rel), attnum);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE ALTER COLUMN DROP EXPRESSION
 */
unsafe fn ATPrepDropExpression(
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    /*
     * Reject ONLY if there are child tables.
     */
    if !recurse && !find_inheritance_children(RelationGetRelid(rel), lockmode).is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!("ALTER TABLE / DROP EXPRESSION must be applied to child tables too")
        );
    }

    /*
     * Cannot drop generation expression from inherited columns.
     */
    if !recursing {
        let tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), (*cmd).name);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr((*cmd).name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }
        let att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
        if (*att_tup).attinhcount > 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("cannot drop generation expression from inherited column")
            );
        }
    }
}

/* Return the address of the affected column. */
unsafe fn ATExecDropExpression(
    rel: Relation,
    col_name: *const i8,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attrelation: Relation;
    let attrdefoid: Oid;
    let mut address = InvalidObjectAddress;

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * TODO: This could be done, but it would need a table rewrite to materialize the generated values.
     */
    if (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns"
            )
            /* errdetail */
        );
    }

    if (*att_tup).attgenerated == 0 as _ {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not a generated column",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not a generated column, skipping",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            heap_freetuple(tuple);
            table_close(attrelation, RowExclusiveLock);
            return InvalidObjectAddress;
        }
    }

    /*
     * Mark the column as no longer generated.
     */
    (*att_tup).attgenerated = 0 as _;
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Drop the dependency records of the GENERATED expression.
     */
    attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
    if !OidIsValid(attrdefoid) {
        elog!(
            ERROR,
            "could not find attrdef tuple for relation {} attnum {}",
            RelationGetRelid(rel),
            attnum
        );
    }
    deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);

    /* Make above changes visible */
    CommandCounterIncrement();

    /*
     * Get rid of the GENERATED expression itself.
     */
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE ALTER COLUMN SET STATISTICS
 *
 * Return value is the address of the modified column
 */
unsafe fn ATExecSetStatistics(
    rel: Relation,
    col_name: *const i8,
    col_num: i16,
    new_value: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let mut newtarget: i32 = 0;
    let newtarget_default: bool;
    let attrelation: Relation;
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let attrtuple: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;
    let mut repl_val = [Datum::from(0usize); Natts_pg_attribute as usize];
    let mut repl_null = [false; Natts_pg_attribute as usize];
    let mut repl_repl = [false; Natts_pg_attribute as usize];

    /*
     * We allow referencing columns by numbers only for indexes.
     */
    if (*(*rel).rd_rel).relkind != RELKIND_INDEX as i8
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_INDEX as i8
        && col_name.is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!("cannot refer to non-index column by number")
        );
    }

    /* -1 was used in previous versions for the default setting */
    if !new_value.is_null() && intVal(new_value) != -1 {
        newtarget = intVal(new_value);
        newtarget_default = false;
    } else {
        newtarget_default = true;
    }

    if !newtarget_default {
        if newtarget < 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg!("statistics target {} is too low", newtarget)
            );
        } else if newtarget > MAX_STATISTICS_TARGET as i32 {
            newtarget = MAX_STATISTICS_TARGET as i32;
            ereport!(
                WARNING,
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg!("lowering statistics target to {}", newtarget)
            );
        }
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    if !col_name.is_null() {
        tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }
    } else {
        tuple = SearchSysCacheAttNum(RelationGetRelid(rel), col_num);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column number {} of relation \"{}\" does not exist",
                    col_num,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }
    }

    attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*attrtuple).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Prevent this as long as the ANALYZE code skips virtual generated columns.
     */
    if (*attrtuple).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter statistics on virtual generated column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_INDEX as i8
        || (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_INDEX as i8
    {
        if (attnum as i32) > (*(*rel).rd_index).indnkeyatts as i32 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot alter statistics on included column \"{}\" of index \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*attrtuple).attname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else if (*(*rel).rd_index).indkey.values[(attnum as usize) - 1] != 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot alter statistics on non-expression column \"{}\" of index \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*attrtuple).attname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errhint: "Alter statistics on table column instead." */
            );
        }
    }

    /* Build new tuple. */
    libc::memset(repl_null.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_repl));
    if !newtarget_default {
        repl_val[(Anum_pg_attribute_attstattarget - 1) as usize] = newtarget as Datum;
    } else {
        repl_null[(Anum_pg_attribute_attstattarget - 1) as usize] = true;
    }
    repl_repl[(Anum_pg_attribute_attstattarget - 1) as usize] = true;
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attrelation),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, newtuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);

    heap_freetuple(newtuple);
    ReleaseSysCache(tuple);
    table_close(attrelation, RowExclusiveLock);

    address
}

/* Return value is the address of the modified column */
unsafe fn ATExecSetOptions(
    rel: Relation,
    col_name: *const i8,
    options: *mut Node,
    is_reset: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let attrelation: Relation;
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let attrtuple: Form_pg_attribute;
    let attnum: AttrNumber;
    let datum: Datum;
    let new_options: Datum;
    let mut isnull = false;
    let mut address = InvalidObjectAddress;
    let mut repl_val = [Datum::from(0usize); Natts_pg_attribute as usize];
    let mut repl_null = [false; Natts_pg_attribute as usize];
    let mut repl_repl = [false; Natts_pg_attribute as usize];

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);

    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*attrtuple).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /* Generate new proposed attoptions (text array) */
    datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions, &mut isnull);
    new_options = transformRelOptions(
        if isnull { 0 as Datum } else { datum },
        castNode!(List, T_List, options),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        false,
        is_reset,
    );
    /* Validate new options */
    attribute_reloptions(new_options, true);

    /* Build new tuple. */
    libc::memset(repl_null.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_repl));
    if new_options != 0 as Datum {
        repl_val[(Anum_pg_attribute_attoptions - 1) as usize] = new_options;
    } else {
        repl_null[(Anum_pg_attribute_attoptions - 1) as usize] = true;
    }
    repl_repl[(Anum_pg_attribute_attoptions - 1) as usize] = true;
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attrelation),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    /* Update system catalog. */
    CatalogTupleUpdate(attrelation, &mut (*newtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);

    heap_freetuple(newtuple);
    ReleaseSysCache(tuple);
    table_close(attrelation, RowExclusiveLock);

    address
}

/*
 * Helper function for ATExecSetStorage and ATExecSetCompression
 *
 * Set the attstorage and/or attcompression fields for index columns
 * associated with the specified table column.
 */
unsafe fn SetIndexStorageProperties(
    rel: Relation,
    attrelation: Relation,
    attnum: AttrNumber,
    setstorage: bool,
    newstorage: i8,
    setcompression: bool,
    newcompression: i8,
    lockmode: LOCKMODE,
) {
    let index_list = RelationGetIndexList(rel);
    let mut lc = list_head(index_list);
    while !lc.is_null() {
        let indexoid = lfirst_oid(lc);
        let indrel = index_open(indexoid, lockmode);
        let mut indattnum: AttrNumber = 0;

        let nk = (*(*indrel).rd_index).indnatts as usize;
        for i in 0..nk {
            if (*(*indrel).rd_index).indkey.values[i] == attnum as i16 {
                indattnum = (i + 1) as AttrNumber;
                break;
            }
        }

        if indattnum == 0 {
            index_close(indrel, lockmode);
            lc = lnext(index_list, lc);
            continue;
        }

        let tuple = SearchSysCacheCopyAttNum(RelationGetRelid(indrel), indattnum);
        if HeapTupleIsValid(tuple) {
            let attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;

            if setstorage {
                (*attrtuple).attstorage = newstorage;
            }
            if setcompression {
                (*attrtuple).attcompression = newcompression;
            }

            CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);
            InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);
            heap_freetuple(tuple);
        }

        index_close(indrel, lockmode);
        lc = lnext(index_list, lc);
    }
}

/*
 * ALTER TABLE ALTER COLUMN SET STORAGE
 *
 * Return value is the address of the modified column
 */
unsafe fn ATExecSetStorage(
    rel: Relation,
    col_name: *const i8,
    new_value: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let attrelation: Relation;
    let tuple: HeapTuple;
    let attrtuple: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);

    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*attrtuple).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    (*attrtuple).attstorage = GetAttributeStorage((*attrtuple).atttypid, strVal(new_value));

    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);

    /*
     * Apply the change to indexes as well (only for simple index columns).
     */
    SetIndexStorageProperties(
        rel, attrelation, attnum,
        true, (*attrtuple).attstorage,
        false, 0,
        lockmode,
    );

    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE DROP COLUMN
 *
 * DROP COLUMN cannot use the normal ALTER TABLE recursion mechanism.
 */
unsafe fn ATPrepDropColumn(
    wqueue: *mut *mut List,
    rel: Relation,
    recurse: bool,
    recursing: bool,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    if (*(*rel).rd_rel).reloftype && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("cannot drop column from typed table")
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as i8 {
        ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
    }

    if recurse {
        (*cmd).recurse = true;
    }
}

/*
 * Drops column 'colName' from relation 'rel' and returns the address of the
 * dropped column.
 */
unsafe fn ATExecDropColumn(
    wqueue: *mut *mut List,
    rel: Relation,
    col_name: *const i8,
    behavior: DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
    addrs: *mut ObjectAddresses,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let targetatt: Form_pg_attribute;
    let attnum: AttrNumber;
    let children: *mut List;
    let mut object = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let is_expr: bool;
    // mut addrs - we may reassign from param
    let mut addrs = addrs;

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_DropColumn,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    /* Initialize addrs on the first invocation */
    Assert!(!recursing || !addrs.is_null());

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if !recursing {
        addrs = new_object_addresses();
    }

    /* get the number of the attribute */
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist, skipping",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            return InvalidObjectAddress;
        }
    }
    targetatt = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*targetatt).attnum;

    /* Can't drop a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot drop system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Don't drop inherited columns, unless recursing.
     */
    if (*targetatt).attinhcount > 0 && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!(
                "cannot drop inherited column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Don't drop columns used in the partition key.
     */
    let _ = &mut is_expr; // used by C macro
    if has_partition_attrs(
        rel,
        bms_make_singleton((attnum as i32) - FirstLowInvalidHeapAttributeNumber),
        &mut (false as bool),
    ) {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!(
                "cannot drop column \"{}\" because it is part of the partition key of relation \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    ReleaseSysCache(tuple);

    /*
     * Propagate to children as appropriate.
     */
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    if !children.is_null() {
        let attr_rel: Relation;

        /*
         * In case of a partitioned table, the column must be dropped from the
         * partitions as well.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 && !recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!(
                    "cannot drop column from only the partitioned table when partitions exist"
                )
                /* errhint: "Do not specify the ONLY keyword." */
            );
        }

        attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrelid = lfirst_oid(lc);
            let childrel = table_open(childrelid, NoLock);
            CheckAlterTableIsSafe(childrel);

            let child_tuple = SearchSysCacheCopyAttName(childrelid, col_name);
            if !HeapTupleIsValid(child_tuple) {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "cache lookup failed for attribute \"{}\" of relation {}",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    childrelid
                );
            }
            let childatt = GETSTRUCT(child_tuple) as Form_pg_attribute;

            if (*childatt).attinhcount <= 0 {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "relation {} has non-inherited attribute \"{}\"",
                    childrelid,
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy()
                );
            }

            if recurse {
                /*
                 * If the child column has other definition sources, just decrement its
                 * inheritance count; if not, recurse to delete it.
                 */
                if (*childatt).attinhcount == 1 && !(*childatt).attislocal {
                    /* Time to delete this child column, too */
                    ATExecDropColumn(
                        wqueue, childrel, col_name, behavior, true, true, false, lockmode, addrs,
                    );
                } else {
                    /* Child column must survive my deletion */
                    (*childatt).attinhcount -= 1;
                    CatalogTupleUpdate(attr_rel, &mut (*child_tuple).t_self, child_tuple);
                    /* Make update visible */
                    CommandCounterIncrement();
                }
            } else {
                /*
                 * If we were told to drop ONLY in this table (no recursion),
                 * mark the inheritors' attributes as locally defined.
                 */
                (*childatt).attinhcount -= 1;
                (*childatt).attislocal = true;
                CatalogTupleUpdate(attr_rel, &mut (*child_tuple).t_self, child_tuple);
                /* Make update visible */
                CommandCounterIncrement();
            }

            heap_freetuple(child_tuple);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
        table_close(attr_rel, RowExclusiveLock);
    }

    /* Add object to delete */
    object.classId = RelationRelationId;
    object.objectId = RelationGetRelid(rel);
    object.objectSubId = attnum as i32;
    add_exact_object_address(&mut object, addrs);

    if !recursing {
        /* Recursion has ended, drop everything that was collected */
        performMultipleDeletions(addrs, behavior, 0);
        free_object_addresses(addrs);
    }

    object
}

/*
 * Prepare to add a primary key on a table, by adding not-null constraints
 * on all columns.
 */
unsafe fn ATPrepAddPrimaryKey(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    let pkconstr = castNode!(Constraint, T_Constraint, (*cmd).def);
    if (*pkconstr).contype != CONSTR_PRIMARY {
        return;
    }

    let mut children: *mut List = std::ptr::null_mut();
    let mut got_children = false;

    /* Verify that columns are not-null, or request that they be made so */
    let mut lc = list_head((*pkconstr).keys);
    while !lc.is_null() {
        let column = lfirst(lc) as *mut String;
        let col_str = strVal(column as *mut Node);

        /*
         * First check if a suitable constraint exists.  If it does, we don't
         * need to request another one.
         */
        let tuple = findNotNullConstraint(RelationGetRelid(rel), col_str);
        if !tuple.is_null() {
            verifyNotNullPKCompatible(tuple, col_str);
            /* All good with this one; don't request another */
            heap_freetuple(tuple);
            lc = lnext((*pkconstr).keys, lc);
            continue;
        } else if !recurse {
            /*
             * No constraint on this column.  Asked not to recurse, we won't
             * create one here, but verify that all children have one.
             */
            if !got_children {
                children = find_inheritance_children(RelationGetRelid(rel), lockmode);
                got_children = true;
            }

            let mut clc = list_head(children);
            while !clc.is_null() {
                let childrelid = lfirst_oid(clc);
                let tup = findNotNullConstraint(childrelid, col_str);
                if tup.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" of table \"{}\" is not marked NOT NULL",
                            std::ffi::CStr::from_ptr(col_str).to_string_lossy(),
                            std::ffi::CStr::from_ptr(get_rel_name(childrelid)).to_string_lossy()
                        )
                    );
                }
                /* verify it's good enough */
                verifyNotNullPKCompatible(tup, col_str);
                clc = lnext(children, clc);
            }
        }

        /* This column is not already not-null, so add it to the queue */
        let nnconstr = makeNotNullConstraint(column as *mut Node);
        let newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd) as *mut AlterTableCmd;
        (*newcmd).subtype = AT_AddConstraint;
        /* note we force recurse=true here; see above */
        (*newcmd).recurse = true;
        (*newcmd).def = nnconstr as *mut Node;

        ATPrepCmd(wqueue, rel, newcmd, true, false, lockmode, context);

        lc = lnext((*pkconstr).keys, lc);
    }
}

/*
 * Verify whether the given not-null constraint is compatible with a primary key.
 */
unsafe fn verifyNotNullPKCompatible(tuple: HeapTuple, colname: *const i8) {
    let con_form = GETSTRUCT(tuple) as Form_pg_constraint;

    if (*con_form).contype != CONSTRAINT_NOTNULL as i8 {
        elog!(ERROR, "constraint {} is not a not-null constraint", (*con_form).oid);
    }

    /* a NO INHERIT constraint is no good */
    if (*con_form).connoinherit {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot create primary key on column \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy()
            )
            /* errdetail, errhint */
        );
    }

    /* an unvalidated constraint is no good */
    if !(*con_form).convalidated {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot create primary key on column \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy()
            )
            /* errdetail, errhint */
        );
    }
}

/*
 * ALTER TABLE ADD INDEX
 *
 * Return value is the address of the new index.
 */
unsafe fn ATExecAddIndex(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    stmt: *mut IndexStmt,
    is_rebuild: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let check_rights: bool;
    let skip_build: bool;
    let quiet: bool;

    Assert!(IsA!(stmt, T_IndexStmt));
    Assert!(!(*stmt).concurrent);
    /* The IndexStmt has already been through transformIndexStmt */
    Assert!((*stmt).transformed);

    /* suppress schema rights check when rebuilding existing index */
    check_rights = !is_rebuild;
    /* skip index build if phase 3 will do it or we're reusing an old one */
    skip_build = (*tab).rewrite > 0 || RelFileNumberIsValid((*stmt).oldNumber);
    /* suppress notices when rebuilding existing index */
    quiet = is_rebuild;

    let address = DefineIndex(
        RelationGetRelid(rel),
        stmt,
        InvalidOid,  /* no predefined OID */
        InvalidOid,  /* no parent index */
        InvalidOid,  /* no parent constraint */
        -1,          /* total_parts unknown */
        true,        /* is_alter_table */
        check_rights,
        false,       /* check_not_in_use - we did it already */
        skip_build,
        quiet,
    );

    /*
     * If TryReuseIndex() stashed a relfilenumber for us, we used it for the
     * new index instead of building from scratch.
     */
    if RelFileNumberIsValid((*stmt).oldNumber) {
        let irel = index_open(address.objectId, NoLock);
        (*irel).rd_createSubid = (*stmt).oldCreateSubid;
        (*irel).rd_firstRelfilelocatorSubid = (*stmt).oldFirstRelfilelocatorSubid;
        RelationPreserveStorage((*irel).rd_locator, true);
        index_close(irel, NoLock);
    }

    address
}

/*
 * ALTER TABLE ADD STATISTICS
 */
unsafe fn ATExecAddStatistics(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    stmt: *mut CreateStatsStmt,
    is_rebuild: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    Assert!(IsA!(stmt, T_CreateStatsStmt));
    Assert!((*stmt).transformed);

    let address = CreateStatistics(stmt, !is_rebuild);
    address
}

/*
 * ALTER TABLE ADD CONSTRAINT USING INDEX
 *
 * Returns the address of the new constraint.
 */
unsafe fn ATExecAddIndexConstraint(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    stmt: *mut IndexStmt,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let index_oid = (*stmt).indexOid;
    let index_rel: Relation;
    let index_name: *mut i8;
    let index_info: *mut IndexInfo;
    let constraint_name: *mut i8;
    let constraint_type: i8;
    let mut flags: bits16;

    Assert!(IsA!(stmt, T_IndexStmt));
    Assert!(OidIsValid(index_oid));
    Assert!((*stmt).isconstraint);

    /*
     * Doing this on partitioned tables is not a simple feature to implement.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables"
            )
        );
    }

    index_rel = index_open(index_oid, AccessShareLock);
    index_name = pstrdup(RelationGetRelationName(index_rel));
    index_info = BuildIndexInfo(index_rel);

    /* this should have been checked at parse time */
    if !(*index_info).ii_Unique {
        elog!(ERROR, "index \"{}\" is not unique", std::ffi::CStr::from_ptr(index_name).to_string_lossy());
    }

    /*
     * Determine name to assign to constraint.
     */
    constraint_name = (*stmt).idxname;
    let constraint_name = if constraint_name.is_null() {
        index_name
    } else if libc::strcmp(constraint_name, index_name) != 0 {
        ereport!(
            NOTICE,
            errmsg!(
                "ALTER TABLE / ADD CONSTRAINT USING INDEX will rename index \"{}\" to \"{}\"",
                std::ffi::CStr::from_ptr(index_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(constraint_name).to_string_lossy()
            )
        );
        RenameRelationInternal(index_oid, constraint_name, false, true);
        constraint_name
    } else {
        constraint_name
    };

    /* Extra checks needed if making primary key */
    if (*stmt).primary {
        index_check_primary_key(rel, index_info, true, stmt);
    }

    /* Note we currently don't support EXCLUSION constraints here */
    if (*stmt).primary {
        constraint_type = CONSTRAINT_PRIMARY as i8;
    } else {
        constraint_type = CONSTRAINT_UNIQUE as i8;
    }

    /* Create the catalog entries for the constraint */
    flags = INDEX_CONSTR_CREATE_UPDATE_INDEX | INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS;
    if (*stmt).initdeferred { flags |= INDEX_CONSTR_CREATE_INIT_DEFERRED; }
    if (*stmt).deferrable   { flags |= INDEX_CONSTR_CREATE_DEFERRABLE; }
    if (*stmt).primary       { flags |= INDEX_CONSTR_CREATE_MARK_AS_PRIMARY; }

    let address = index_constraint_create(
        rel,
        index_oid,
        InvalidOid,
        index_info,
        constraint_name,
        constraint_type,
        flags,
        allowSystemTableMods,
        false, /* is_internal */
    );

    index_close(index_rel, NoLock);

    address
}

/*
 * ALTER TABLE ADD CONSTRAINT
 *
 * Return value is the address of the new constraint; if no constraint was
 * added, InvalidObjectAddress is returned.
 */
unsafe fn ATExecAddConstraint(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    new_constraint: *mut Constraint,
    recurse: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let mut address = InvalidObjectAddress;

    Assert!(IsA!(new_constraint, T_Constraint));

    /*
     * Currently, we only expect to see CONSTR_CHECK, CONSTR_NOTNULL and
     * CONSTR_FOREIGN nodes arriving here.
     */
    match (*new_constraint).contype {
        CONSTR_CHECK | CONSTR_NOTNULL => {
            address = ATAddCheckNNConstraint(
                wqueue, tab, rel, new_constraint, recurse, false, is_readd, lockmode,
            );
        }
        CONSTR_FOREIGN => {
            /*
             * Assign or validate constraint name
             */
            if !(*new_constraint).conname.is_null() {
                if ConstraintNameIsUsed(
                    CONSTRAINT_RELATION,
                    RelationGetRelid(rel),
                    (*new_constraint).conname,
                ) {
                    ereport!(
                        ERROR,
                        errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg!(
                            "constraint \"{}\" for relation \"{}\" already exists",
                            std::ffi::CStr::from_ptr((*new_constraint).conname).to_string_lossy(),
                            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        )
                    );
                }
            } else {
                (*new_constraint).conname = ChooseConstraintName(
                    RelationGetRelationName(rel),
                    ChooseForeignKeyConstraintNameAddition((*new_constraint).fk_attrs),
                    b"fkey\0".as_ptr() as *const i8,
                    RelationGetNamespace(rel),
                    std::ptr::null_mut(),
                );
            }

            address = ATAddForeignKeyConstraint(
                wqueue, tab, rel, new_constraint, recurse, false, lockmode,
            );
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized constraint type: {}",
                (*new_constraint).contype as i32
            );
        }
    }

    address
}

/*
 * Generate the column-name portion of the constraint name for a new foreign
 * key given the list of column names.
 */
unsafe fn ChooseForeignKeyConstraintNameAddition(colnames: *mut List) -> *mut i8 {
    let mut buf = [0i8; NAMEDATALEN * 2];
    let mut buflen: usize = 0;

    buf[0] = 0;
    let mut lc = list_head(colnames);
    while !lc.is_null() {
        let name = strVal(lfirst(lc) as *mut Node);
        if buflen > 0 {
            buf[buflen] = b'_' as i8;
            buflen += 1;
        }

        /*
         * At this point we have buflen <= NAMEDATALEN.
         */
        libc::strncpy(
            buf.as_mut_ptr().add(buflen),
            name,
            NAMEDATALEN as usize,
        );
        buflen += libc::strlen(buf.as_ptr().add(buflen));
        if buflen >= NAMEDATALEN as usize {
            break;
        }
        lc = lnext(colnames, lc);
    }
    pstrdup(buf.as_ptr())
}

/*
 * Add a check or not-null constraint to a single table and its children.
 * Returns the address of the constraint added to the parent relation.
 */
unsafe fn ATAddCheckNNConstraint(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    constr: *mut Constraint,
    recurse: bool,
    recursing: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let newcons: *mut List;
    let children: *mut List;
    let mut address = InvalidObjectAddress;

    /* Guard against stack overflow due to overly deep inheritance tree. */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_AddConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    /*
     * Call AddRelationNewConstraints to do the work.
     */
    newcons = AddRelationNewConstraints(
        rel,
        std::ptr::null_mut(),
        list_make1(copyObject(constr as *mut _) as *mut _),
        recursing || is_readd, /* allow_merge */
        !recursing,            /* is_local */
        is_readd,              /* is_internal */
        std::ptr::null_mut(),  /* queryString not available here */
    );

    /* we don't expect more than one constraint here */
    Assert!(list_length(newcons) <= 1);

    /* Add each to-be-validated constraint to Phase 3's queue */
    let mut lcon = list_head(newcons);
    while !lcon.is_null() {
        let ccon = lfirst(lcon) as *mut CookedConstraint;

        if !(*ccon).skip_validation && (*ccon).contype != CONSTR_NOTNULL {
            let newcon =
                palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
            (*newcon).name = (*ccon).name;
            (*newcon).contype = (*ccon).contype;
            (*newcon).qual = (*ccon).expr;

            (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
        }

        /* Save the actually assigned name if it was defaulted */
        if (*constr).conname.is_null() {
            (*constr).conname = (*ccon).name;
        }

        /*
         * If adding a valid not-null constraint, set the pg_attribute flag
         * and tell phase 3 to verify existing rows, if needed.
         */
        if (*constr).contype == CONSTR_NOTNULL {
            set_attnotnull(
                wqueue,
                rel,
                (*ccon).attnum,
                !(*constr).skip_validation,
                !(*constr).skip_validation,
            );
        }

        ObjectAddressSet!(address, ConstraintRelationId, (*ccon).conoid);
        lcon = lnext(newcons, lcon);
    }

    /* At this point we must have a locked-down name to use */
    Assert!(newcons.is_null() || !(*constr).conname.is_null());

    /* Advance command counter in case same table is visited multiple times */
    CommandCounterIncrement();

    /*
     * If the constraint got merged with an existing constraint, we're done.
     */
    if newcons.is_null() {
        return address;
    }

    /* If adding a NO INHERIT constraint, no need to find our children. */
    if (*constr).is_no_inherit {
        return address;
    }

    /*
     * Propagate to children as appropriate.
     */
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    /*
     * Check if ONLY was specified with ALTER TABLE.
     */
    if !recurse && !children.is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("constraint must be added to child tables too")
        );
    }

    /* Recurse to create the constraint on each child. */
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childrelid = lfirst_oid(child_lc);
        let childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        /* Find or create work queue entry for this table */
        let childtab = ATGetQueueEntry(wqueue, childrel);

        /* Recurse to this child */
        ATAddCheckNNConstraint(
            wqueue, childtab, childrel, constr, recurse, true, is_readd, lockmode,
        );

        table_close(childrel, NoLock);
        child_lc = lnext(children, child_lc);
    }

    address
}

/*
 * Add a foreign-key constraint to a single table; return the new constraint's address.
 */
unsafe fn ATAddForeignKeyConstraint(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    fkconstraint: *mut Constraint,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let pkrel: Relation;
    let mut pkattnum = [0i16; INDEX_MAX_KEYS];
    let mut fkattnum = [0i16; INDEX_MAX_KEYS];
    let mut pktypoid = [InvalidOid; INDEX_MAX_KEYS];
    let mut fktypoid = [InvalidOid; INDEX_MAX_KEYS];
    let mut pkcolloid = [InvalidOid; INDEX_MAX_KEYS];
    let mut fkcolloid = [InvalidOid; INDEX_MAX_KEYS];
    let mut opclasses = [InvalidOid; INDEX_MAX_KEYS];
    let mut pfeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut ppeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut ffeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut fkdelsetcols = [0i16; INDEX_MAX_KEYS];
    let with_period: bool;
    let mut pk_has_without_overlaps = false;
    let mut numfks: i32;
    let numpks: i32;
    let numfkdelsetcols: i32;
    let mut index_oid: Oid = InvalidOid;
    let mut old_check_ok: bool;
    let old_pfeqop_item: *mut ListCell;

    /*
     * Grab ShareRowExclusiveLock on the pk table.
     */
    if OidIsValid((*fkconstraint).old_pktable_oid) {
        pkrel = table_open((*fkconstraint).old_pktable_oid, ShareRowExclusiveLock);
    } else {
        pkrel = table_openrv((*fkconstraint).pktable, ShareRowExclusiveLock);
    }

    /* Validity checks */
    if !recurse && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "cannot use ONLY for foreign key on partitioned table \"{}\" referencing relation \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    if (*(*pkrel).rd_rel).relkind != RELKIND_RELATION as i8
        && (*(*pkrel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "referenced relation \"{}\" is not a table",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    if !allowSystemTableMods && IsSystemRelation(pkrel) {
        ereport!(
            ERROR,
            errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    /*
     * References from permanent or unlogged tables to temp tables, and from
     * permanent tables to unlogged tables, are disallowed.
     */
    match (*(*rel).rd_rel).relpersistence {
        p if p == RELPERSISTENCE_PERMANENT as i8 => {
            if !RelationIsPermanent(pkrel) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on permanent tables may reference only permanent tables"
                    )
                );
            }
        }
        p if p == RELPERSISTENCE_UNLOGGED as i8 => {
            if !RelationIsPermanent(pkrel)
                && (*(*pkrel).rd_rel).relpersistence != RELPERSISTENCE_UNLOGGED as i8
            {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on unlogged tables may reference only permanent or unlogged tables"
                    )
                );
            }
        }
        p if p == RELPERSISTENCE_TEMP as i8 => {
            if (*(*pkrel).rd_rel).relpersistence != RELPERSISTENCE_TEMP as i8 {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on temporary tables may reference only temporary tables"
                    )
                );
            }
            if !(*pkrel).rd_islocaltemp || !(*rel).rd_islocaltemp {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on temporary tables must involve temporary tables of this session"
                    )
                );
            }
        }
        _ => {}
    }

    /*
     * Look up the referencing attributes.
     */
    numfks = transformColumnNameList(
        RelationGetRelid(rel),
        (*fkconstraint).fk_attrs,
        fkattnum.as_mut_ptr(),
        fktypoid.as_mut_ptr(),
        fkcolloid.as_mut_ptr(),
    );
    with_period = (*fkconstraint).fk_with_period || (*fkconstraint).pk_with_period;
    if with_period && !(*fkconstraint).fk_with_period {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_FOREIGN_KEY),
            errmsg!(
                "foreign key uses PERIOD on the referenced table but not the referencing table"
            )
        );
    }

    let num_fk_del_set_cols_raw = transformColumnNameList(
        RelationGetRelid(rel),
        (*fkconstraint).fk_del_set_cols,
        fkdelsetcols.as_mut_ptr(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    numfkdelsetcols = validateFkOnDeleteSetColumns(
        numfks,
        fkattnum.as_ptr(),
        num_fk_del_set_cols_raw,
        fkdelsetcols.as_mut_ptr(),
        (*fkconstraint).fk_del_set_cols,
    );

    /*
     * If the attribute list for the referenced table was omitted, lookup the
     * definition of the primary key.
     */
    if (*fkconstraint).pk_attrs.is_null() {
        numpks = transformFkeyGetPrimaryKey(
            pkrel,
            &mut index_oid,
            &mut (*fkconstraint).pk_attrs,
            pkattnum.as_mut_ptr(),
            pktypoid.as_mut_ptr(),
            pkcolloid.as_mut_ptr(),
            opclasses.as_mut_ptr(),
            &mut pk_has_without_overlaps,
        );

        /* If the primary key uses WITHOUT OVERLAPS, the fk must use PERIOD */
        if pk_has_without_overlaps && !(*fkconstraint).fk_with_period {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg!(
                    "foreign key uses PERIOD on the referenced table but not the referencing table"
                )
            );
        }
    } else {
        numpks = transformColumnNameList(
            RelationGetRelid(pkrel),
            (*fkconstraint).pk_attrs,
            pkattnum.as_mut_ptr(),
            pktypoid.as_mut_ptr(),
            pkcolloid.as_mut_ptr(),
        );

        /* Since we got pk_attrs, one should be a period. */
        if with_period && !(*fkconstraint).pk_with_period {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg!(
                    "foreign key uses PERIOD on the referencing table but not the referenced table"
                )
            );
        }

        /* Look for an index matching the column list */
        index_oid = transformFkeyCheckAttrs(
            pkrel,
            numpks,
            pkattnum.as_mut_ptr(),
            with_period,
            opclasses.as_mut_ptr(),
            &mut pk_has_without_overlaps,
        );
    }

    /*
     * If the referenced primary key has WITHOUT OVERLAPS, the foreign key must use PERIOD.
     */
    if pk_has_without_overlaps && !with_period {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_FOREIGN_KEY),
            errmsg!(
                "foreign key must use PERIOD when referencing a primary key using WITHOUT OVERLAPS"
            )
        );
    }

    /* Now we can check permissions. */
    checkFkeyPermissions(pkrel, pkattnum.as_mut_ptr(), numpks);

    /* Check some things for generated columns. */
    for i in 0..numfks as usize {
        let attgenerated = (*TupleDescAttr(RelationGetDescr(rel), fkattnum[i] as usize - 1)).attgenerated;

        if attgenerated != 0 {
            if (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETNULL as i8
                || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETDEFAULT as i8
                || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_CASCADE as i8
            {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg!(
                        "invalid {} action for foreign key constraint containing generated column",
                        "ON UPDATE"
                    )
                );
            }
            if (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETNULL as i8
                || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETDEFAULT as i8
            {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg!(
                        "invalid {} action for foreign key constraint containing generated column",
                        "ON DELETE"
                    )
                );
            }
        }

        /*
         * FKs on virtual columns are not supported.
         */
        if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "foreign key constraints on virtual generated columns are not supported"
                )
            );
        }
    }

    /*
     * Some actions are currently unsupported for foreign keys using PERIOD.
     */
    if (*fkconstraint).fk_with_period {
        if (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_RESTRICT as i8
            || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_CASCADE as i8
            || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETNULL as i8
            || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETDEFAULT as i8
        {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "unsupported {} action for foreign key constraint using PERIOD",
                    "ON UPDATE"
                )
            );
        }

        if (*fkconstraint).fk_del_action == FKCONSTR_ACTION_RESTRICT as i8
            || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_CASCADE as i8
            || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETNULL as i8
            || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETDEFAULT as i8
        {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "unsupported {} action for foreign key constraint using PERIOD",
                    "ON DELETE"
                )
            );
        }
    }

    if numfks != numpks {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_FOREIGN_KEY),
            errmsg!(
                "number of referencing and referenced columns for foreign key disagree"
            )
        );
    }

    /*
     * On the strength of a previous constraint, we might avoid scanning tables.
     */
    old_check_ok = !(*fkconstraint).old_conpfeqop.is_null();
    Assert!(!old_check_ok || numfks == list_length((*fkconstraint).old_conpfeqop));

    old_pfeqop_item = list_head((*fkconstraint).old_conpfeqop);
    let mut old_pfeqop_item = old_pfeqop_item;

    for i in 0..numpks as usize {
        let pktype = pktypoid[i];
        let fktype = fktypoid[i];
        let pkcoll = pkcolloid[i];
        let fkcoll = fkcolloid[i];
        let cla_ht: HeapTuple;
        let cla_tup: Form_pg_opclass;
        let amid: Oid;
        let opfamily: Oid;
        let opcintype: Oid;
        let for_overlaps: bool;
        let cmptype: CompareType;
        let mut pfeqop: Oid = InvalidOid;
        let mut ppeqop: Oid;
        let mut ffeqop: Oid = InvalidOid;
        let eqstrategy: i16;
        let mut pfeqop_right: Oid = InvalidOid;

        /* We need several fields out of the pg_opclass entry */
        cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclasses[i]));
        if !HeapTupleIsValid(cla_ht) {
            elog!(ERROR, "cache lookup failed for opclass {}", opclasses[i]);
        }
        cla_tup = GETSTRUCT(cla_ht) as Form_pg_opclass;
        amid = (*cla_tup).opcmethod;
        opfamily = (*cla_tup).opcfamily;
        opcintype = (*cla_tup).opcintype;
        ReleaseSysCache(cla_ht);

        for_overlaps = with_period && i == numpks as usize - 1;
        cmptype = if for_overlaps { COMPARE_OVERLAP } else { COMPARE_EQ };
        eqstrategy = IndexAmTranslateCompareType(cmptype, amid, opfamily, true);
        if eqstrategy == InvalidStrategy as i16 {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg!(
                    "{}",
                    if for_overlaps {
                        "could not identify an overlaps operator for foreign key"
                    } else {
                        "could not identify an equality operator for foreign key"
                    }
                )
            );
        }

        /* There had better be a primary equality operator for the index. */
        ppeqop = get_opfamily_member(opfamily, opcintype, opcintype, eqstrategy);
        if !OidIsValid(ppeqop) {
            elog!(
                ERROR,
                "missing operator {}({},{}) in opfamily {}",
                eqstrategy, opcintype, opcintype, opfamily
            );
        }

        /* Are there equality operators that take exactly the FK type? */
        let fktyped = getBaseType(fktype);
        pfeqop = get_opfamily_member(opfamily, opcintype, fktyped, eqstrategy);
        if OidIsValid(pfeqop) {
            pfeqop_right = fktyped;
            ffeqop = get_opfamily_member(opfamily, fktyped, fktyped, eqstrategy);
        }

        if !(OidIsValid(pfeqop) && OidIsValid(ffeqop)) {
            /*
             * Otherwise, look for an implicit cast from the FK type to the opcintype.
             */
            let input_typeids = [pktype, fktype];
            let target_typeids = [opcintype, opcintype];
            if can_coerce_type(
                2,
                input_typeids.as_ptr(),
                target_typeids.as_ptr(),
                COERCION_IMPLICIT,
            ) {
                pfeqop = ppeqop;
                ffeqop = ppeqop;
                pfeqop_right = opcintype;
            }
        }

        if !(OidIsValid(pfeqop) && OidIsValid(ffeqop)) {
            ereport!(
                ERROR,
                errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg!(
                    "foreign key constraint \"{}\" cannot be implemented",
                    std::ffi::CStr::from_ptr((*fkconstraint).conname).to_string_lossy()
                )
                /* errdetail: Key columns ... are of incompatible types */
            );
        }

        /* Collation checks */
        if OidIsValid(pkcoll) && OidIsValid(fkcoll) {
            let pkcolldet = get_collation_isdeterministic(pkcoll);
            let fkcolldet = get_collation_isdeterministic(fkcoll);

            if (!pkcolldet || !fkcolldet) && pkcoll != fkcoll {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_COLLATION_MISMATCH),
                    errmsg!(
                        "foreign key constraint \"{}\" cannot be implemented",
                        std::ffi::CStr::from_ptr((*fkconstraint).conname).to_string_lossy()
                    )
                    /* errdetail */
                );
            }
        }

        if old_check_ok {
            /*
             * When a pfeqop changes, revalidate the constraint.
             */
            let oid_pfeqop = lfirst_oid(old_pfeqop_item);
            old_check_ok = pfeqop == oid_pfeqop;
            old_pfeqop_item = lnext((*fkconstraint).old_conpfeqop, old_pfeqop_item);
        }
        if old_check_ok && !(*tab).oldDesc.is_null() {
            let attr = TupleDescAttr((*tab).oldDesc, fkattnum[i] as usize - 1);
            let old_fktype = (*attr).atttypid;
            let new_fktype = fktype;
            let mut old_castfunc = InvalidOid;
            let mut new_castfunc = InvalidOid;
            let old_pathtype = findFkeyCast(pfeqop_right, old_fktype, &mut old_castfunc);
            let new_pathtype = findFkeyCast(pfeqop_right, new_fktype, &mut new_castfunc);
            let old_fkcoll = (*attr).attcollation;
            let new_fkcoll = fkcoll;

            old_check_ok = new_pathtype == old_pathtype
                && new_castfunc == old_castfunc
                && (!IsPolymorphicType(pfeqop_right) || new_fktype == old_fktype)
                && (new_fkcoll == old_fkcoll
                    || (get_collation_isdeterministic(old_fkcoll)
                        && get_collation_isdeterministic(new_fkcoll)));
        }

        pfeqoperators[i] = pfeqop;
        ppeqoperators[i] = ppeqop;
        ffeqoperators[i] = ffeqop;
    }

    /*
     * For FKs with PERIOD we need additional operators.
     */
    if with_period {
        let mut periodoperoid = InvalidOid;
        let mut aggedperiodoperoid = InvalidOid;
        let mut intersectoperoid = InvalidOid;
        FindFKPeriodOpers(
            opclasses[(numpks as usize) - 1],
            &mut periodoperoid,
            &mut aggedperiodoperoid,
            &mut intersectoperoid,
        );
    }

    /* First, create the constraint catalog entry itself. */
    let address = addFkConstraint(
        addFkBothSides,
        (*fkconstraint).conname,
        fkconstraint,
        rel,
        pkrel,
        index_oid,
        InvalidOid, /* no parent constraint */
        numfks,
        pkattnum.as_mut_ptr(),
        fkattnum.as_mut_ptr(),
        pfeqoperators.as_mut_ptr(),
        ppeqoperators.as_mut_ptr(),
        ffeqoperators.as_mut_ptr(),
        numfkdelsetcols,
        fkdelsetcols.as_mut_ptr(),
        false,
        with_period,
    );

    /* Next process the action triggers at the referenced side and recurse */
    addFkRecurseReferenced(
        fkconstraint,
        rel,
        pkrel,
        index_oid,
        address.objectId,
        numfks,
        pkattnum.as_mut_ptr(),
        fkattnum.as_mut_ptr(),
        pfeqoperators.as_mut_ptr(),
        ppeqoperators.as_mut_ptr(),
        ffeqoperators.as_mut_ptr(),
        numfkdelsetcols,
        fkdelsetcols.as_mut_ptr(),
        old_check_ok,
        InvalidOid,
        InvalidOid,
        with_period,
    );

    /* Lastly create the check triggers at the referencing side and recurse */
    addFkRecurseReferencing(
        wqueue,
        fkconstraint,
        rel,
        pkrel,
        index_oid,
        address.objectId,
        numfks,
        pkattnum.as_mut_ptr(),
        fkattnum.as_mut_ptr(),
        pfeqoperators.as_mut_ptr(),
        ppeqoperators.as_mut_ptr(),
        ffeqoperators.as_mut_ptr(),
        numfkdelsetcols,
        fkdelsetcols.as_mut_ptr(),
        old_check_ok,
        lockmode,
        InvalidOid,
        InvalidOid,
        with_period,
    );

    /* Done. Close pk table, but keep lock until we've committed. */
    table_close(pkrel, NoLock);

    address
}

/*
 * validateFkOnDeleteSetColumns
 *   Verifies that columns used in ON DELETE SET NULL/DEFAULT column lists are valid.
 */
unsafe fn validateFkOnDeleteSetColumns(
    numfks: i32,
    fkattnums: *const i16,
    numfksetcols: i32,
    fksetcolsattnums: *mut i16,
    fksetcols: *mut List,
) -> i32 {
    let mut numcolsout: i32 = 0;

    for i in 0..numfksetcols as usize {
        let setcol_attnum = *fksetcolsattnums.add(i);
        let mut seen = false;

        /* Make sure it's in fkattnums[] */
        for j in 0..numfks as usize {
            if *fkattnums.add(j) == setcol_attnum {
                seen = true;
                break;
            }
        }

        if !seen {
            let col = strVal(list_nth(fksetcols, i as i32) as *mut Node);
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg!(
                    "column \"{}\" referenced in ON DELETE SET action must be part of foreign key",
                    std::ffi::CStr::from_ptr(col).to_string_lossy()
                )
            );
        }

        /* Now check for dups */
        seen = false;
        for j in 0..numcolsout as usize {
            if *fksetcolsattnums.add(j) == setcol_attnum {
                seen = true;
                break;
            }
        }
        if !seen {
            *fksetcolsattnums.add(numcolsout as usize) = setcol_attnum;
            numcolsout += 1;
        }
    }
    numcolsout
}

/*
 * addFkConstraint
 *   Install pg_constraint entries to implement a foreign key constraint.
 */
unsafe fn addFkConstraint(
    fkside: addFkConstraintSides,
    constraintname: *mut i8,
    fkconstraint: *mut Constraint,
    rel: Relation,
    pkrel: Relation,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: i32,
    pkattnum: *mut i16,
    fkattnum: *mut i16,
    pfeqoperators: *mut Oid,
    ppeqoperators: *mut Oid,
    ffeqoperators: *mut Oid,
    numfkdelsetcols: i32,
    fkdelsetcols: *mut i16,
    is_internal: bool,
    with_period: bool,
) -> ObjectAddress {
    let constr_oid: Oid;
    let conname: *mut i8;
    let conislocal: bool;
    let coninhcount: i16;
    let connoinherit: bool;

    /*
     * Verify relkind for each referenced partition.
     */
    if (*(*pkrel).rd_rel).relkind != RELKIND_RELATION as i8
        && (*(*pkrel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "referenced relation \"{}\" is not a table",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    /*
     * Caller supplies us with a constraint name; however, it may be used in
     * this partition, so come up with a different one in that case.
     */
    if ConstraintNameIsUsed(CONSTRAINT_RELATION, RelationGetRelid(rel), constraintname) {
        conname = ChooseConstraintName(
            constraintname,
            std::ptr::null_mut(),
            b"\0".as_ptr() as *const i8,
            RelationGetNamespace(rel),
            std::ptr::null_mut(),
        );
    } else {
        conname = constraintname;
    }

    if (*fkconstraint).conname.is_null() {
        (*fkconstraint).conname = pstrdup(conname);
    }

    if OidIsValid(parent_constr) {
        conislocal = false;
        coninhcount = 1;
        connoinherit = false;
    } else {
        conislocal = true;
        coninhcount = 0;
        /*
         * always inherit for partitioned tables, never for legacy inheritance
         */
        connoinherit = (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8;
    }

    /* Record the FK constraint in pg_constraint. */
    constr_oid = CreateConstraintEntry(
        conname,
        RelationGetNamespace(rel),
        CONSTRAINT_FOREIGN as i8,
        (*fkconstraint).deferrable,
        (*fkconstraint).initdeferred,
        (*fkconstraint).is_enforced,
        (*fkconstraint).initially_valid,
        parent_constr,
        RelationGetRelid(rel),
        fkattnum,
        numfks,
        numfks,
        InvalidOid, /* not a domain constraint */
        index_oid,
        RelationGetRelid(pkrel),
        pkattnum,
        pfeqoperators,
        ppeqoperators,
        ffeqoperators,
        numfks,
        (*fkconstraint).fk_upd_action,
        (*fkconstraint).fk_del_action,
        fkdelsetcols,
        numfkdelsetcols,
        (*fkconstraint).fk_matchtype,
        std::ptr::null_mut(), /* no exclusion constraint */
        std::ptr::null_mut(), /* no check constraint */
        std::ptr::null_mut(),
        conislocal,   /* islocal */
        coninhcount,  /* inhcount */
        connoinherit, /* conNoInherit */
        with_period,  /* conPeriod */
        is_internal,  /* is_internal */
    );

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    ObjectAddressSet!(address, ConstraintRelationId, constr_oid);

    /*
     * In partitioning cases, create the dependency entries for this constraint.
     */
    if OidIsValid(parent_constr) {
        let mut referenced = ObjectAddress {
            classId: InvalidOid,
            objectId: InvalidOid,
            objectSubId: 0,
        };
        ObjectAddressSet!(referenced, ConstraintRelationId, parent_constr);

        Assert!(fkside != addFkBothSides);
        if fkside == addFkReferencedSide {
            recordDependencyOn(&mut address, &mut referenced, DEPENDENCY_INTERNAL);
        } else {
            recordDependencyOn(&mut address, &mut referenced, DEPENDENCY_PARTITION_PRI);
            ObjectAddressSet!(referenced, RelationRelationId, RelationGetRelid(rel));
            recordDependencyOn(&mut address, &mut referenced, DEPENDENCY_PARTITION_SEC);
        }
    }

    /* make new constraint visible, in case we add more */
    CommandCounterIncrement();

    address
}

/*
 * addFkRecurseReferenced
 *   Recursive helper for the referenced side of foreign key creation.
 */
unsafe fn addFkRecurseReferenced(
    fkconstraint: *mut Constraint,
    rel: Relation,
    pkrel: Relation,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: i32,
    pkattnum: *mut i16,
    fkattnum: *mut i16,
    pfeqoperators: *mut Oid,
    ppeqoperators: *mut Oid,
    ffeqoperators: *mut Oid,
    numfkdelsetcols: i32,
    fkdelsetcols: *mut i16,
    old_check_ok: bool,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) {
    let mut delete_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;

    Assert!(CheckRelationLockedByMe(pkrel, ShareRowExclusiveLock, true));
    Assert!(CheckRelationLockedByMe(rel, ShareRowExclusiveLock, true));

    /*
     * Create action triggers to enforce the constraint, or skip if NOT ENFORCED.
     */
    if (*fkconstraint).is_enforced {
        createForeignKeyActionTriggers(
            RelationGetRelid(rel),
            RelationGetRelid(pkrel),
            fkconstraint,
            parent_constr,
            index_oid,
            parent_del_trigger,
            parent_upd_trigger,
            &mut delete_trigger_oid,
            &mut update_trigger_oid,
        );
    }

    /*
     * If the referenced table is partitioned, recurse.
     */
    if (*(*pkrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        let pd = RelationGetPartitionDesc(pkrel, true);

        for i in 0..(*pd).nparts as usize {
            let part_rel = table_open((*pd).oids[i], ShareRowExclusiveLock);
            let map = build_attrmap_by_name_if_req(
                RelationGetDescr(part_rel),
                RelationGetDescr(pkrel),
                false,
            );
            let mapped_pkattnum: *mut AttrNumber;
            let mapped_pkattnum_buf: *mut AttrNumber;

            if !map.is_null() {
                mapped_pkattnum_buf =
                    palloc(core::mem::size_of::<AttrNumber>() * numfks as usize)
                        as *mut AttrNumber;
                for j in 0..numfks as usize {
                    *mapped_pkattnum_buf.add(j) =
                        (*map).attnums[(*pkattnum.add(j) as usize) - 1];
                }
                mapped_pkattnum = mapped_pkattnum_buf;
            } else {
                mapped_pkattnum = pkattnum;
            }

            let part_index_id = index_get_partition(part_rel, index_oid);
            if !OidIsValid(part_index_id) {
                elog!(
                    ERROR,
                    "index for {} not found in partition {}",
                    index_oid,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(part_rel)).to_string_lossy()
                );
            }

            /* Create entry at this level ... */
            let sub_address = addFkConstraint(
                addFkReferencedSide,
                (*fkconstraint).conname,
                fkconstraint,
                rel,
                part_rel,
                part_index_id,
                parent_constr,
                numfks,
                mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                true,
                with_period,
            );
            /* ... and recurse to our children */
            addFkRecurseReferenced(
                fkconstraint,
                rel,
                part_rel,
                part_index_id,
                sub_address.objectId,
                numfks,
                mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                old_check_ok,
                delete_trigger_oid,
                update_trigger_oid,
                with_period,
            );

            /* Done -- clean up (but keep the lock) */
            table_close(part_rel, NoLock);
            if !map.is_null() {
                pfree(mapped_pkattnum as *mut _);
                free_attrmap(map);
            }
        }
    }
}

/*
 * addFkRecurseReferencing
 *   Recursive helper for the referencing side of foreign key creation.
 */
unsafe fn addFkRecurseReferencing(
    wqueue: *mut *mut List,
    fkconstraint: *mut Constraint,
    rel: Relation,
    pkrel: Relation,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: i32,
    pkattnum: *mut i16,
    fkattnum: *mut i16,
    pfeqoperators: *mut Oid,
    ppeqoperators: *mut Oid,
    ffeqoperators: *mut Oid,
    numfkdelsetcols: i32,
    fkdelsetcols: *mut i16,
    old_check_ok: bool,
    lockmode: LOCKMODE,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) {
    let mut insert_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;

    Assert!(OidIsValid(parent_constr));
    Assert!(CheckRelationLockedByMe(rel, ShareRowExclusiveLock, true));
    Assert!(CheckRelationLockedByMe(pkrel, ShareRowExclusiveLock, true));

    if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("foreign key constraints are not supported on foreign tables")
        );
    }

    /*
     * Add check triggers if the constraint is ENFORCED.
     */
    if (*fkconstraint).is_enforced {
        createForeignKeyCheckTriggers(
            RelationGetRelid(rel),
            RelationGetRelid(pkrel),
            fkconstraint,
            parent_constr,
            index_oid,
            parent_ins_trigger,
            parent_upd_trigger,
            &mut insert_trigger_oid,
            &mut update_trigger_oid,
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_RELATION as i8 {
        /*
         * Tell Phase 3 to check that the constraint is satisfied by existing rows.
         */
        if !wqueue.is_null()
            && !old_check_ok
            && !(*fkconstraint).skip_validation
            && (*fkconstraint).is_enforced
        {
            let tab = ATGetQueueEntry(wqueue, rel);
            let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
            (*newcon).name = get_constraint_name(parent_constr);
            (*newcon).contype = CONSTR_FOREIGN;
            (*newcon).refrelid = RelationGetRelid(pkrel);
            (*newcon).refindid = index_oid;
            (*newcon).conid = parent_constr;
            (*newcon).conwithperiod = (*fkconstraint).fk_with_period;
            (*newcon).qual = fkconstraint as *mut Node;

            (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
        }
    } else if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        let pd = RelationGetPartitionDesc(rel, true);
        let trigrel = table_open(TriggerRelationId, RowExclusiveLock);

        /*
         * Recurse to take appropriate action on each partition.
         */
        for i in 0..(*pd).nparts as usize {
            let partition = table_open((*pd).oids[i], lockmode);
            let attmap = build_attrmap_by_name(
                RelationGetDescr(partition),
                RelationGetDescr(rel),
                false,
            );
            let mut mapped_fkattnum = [0 as AttrNumber; INDEX_MAX_KEYS];
            for j in 0..numfks as usize {
                mapped_fkattnum[j] = (*attmap).attnums[(*fkattnum.add(j) as usize) - 1];
            }

            CheckAlterTableIsSafe(partition);

            /* Check whether an existing constraint can be repurposed */
            let part_fks = copyObject(RelationGetFKeyList(partition)) as *mut List;
            let mut attached = false;
            let mut fklc = list_head(part_fks);
            while !fklc.is_null() {
                let fk = lfirst_node!(ForeignKeyCacheInfo, T_ForeignKeyCacheInfo, current_cell!(fklc));
                if tryAttachPartitionForeignKey(
                    wqueue,
                    fk,
                    partition,
                    parent_constr,
                    numfks,
                    mapped_fkattnum.as_mut_ptr(),
                    pkattnum,
                    pfeqoperators,
                    insert_trigger_oid,
                    update_trigger_oid,
                    trigrel,
                ) {
                    attached = true;
                    break;
                }
                fklc = lnext(part_fks, fklc);
            }

            if attached {
                table_close(partition, NoLock);
                continue;
            }

            /*
             * No luck finding a good constraint to reuse; create our own.
             */
            let sub_address = addFkConstraint(
                addFkReferencingSide,
                (*fkconstraint).conname,
                fkconstraint,
                partition,
                pkrel,
                index_oid,
                parent_constr,
                numfks,
                pkattnum,
                mapped_fkattnum.as_mut_ptr(),
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                true,
                with_period,
            );

            addFkRecurseReferencing(
                wqueue,
                fkconstraint,
                partition,
                pkrel,
                index_oid,
                sub_address.objectId,
                numfks,
                pkattnum,
                mapped_fkattnum.as_mut_ptr(),
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                old_check_ok,
                lockmode,
                insert_trigger_oid,
                update_trigger_oid,
                with_period,
            );

            table_close(partition, NoLock);
        }

        table_close(trigrel, RowExclusiveLock);
    }
}

/*
 * CloneForeignKeyConstraints
 *   Clone foreign keys from a partitioned table to a newly acquired partition.
 */
unsafe fn CloneForeignKeyConstraints(
    wqueue: *mut *mut List,
    parent_rel: Relation,
    partition_rel: Relation,
) {
    /* This only works for declarative partitioning */
    Assert!((*(*parent_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8);

    /*
     * First, clone constraints where the parent is on the referencing side.
     */
    CloneFkReferencing(wqueue, parent_rel, partition_rel);

    /*
     * Clone constraints for which the parent is on the referenced side.
     */
    CloneFkReferenced(parent_rel, partition_rel);
}

/*
 * CloneFkReferenced
 *   Find all the FKs that have the parent relation on the referenced side;
 *   clone those constraints to the given partition.
 */
unsafe fn CloneFkReferenced(parent_rel: Relation, partition_rel: Relation) {
    let pg_constraint: Relation;
    let attmap: *mut AttrMap;
    let mut clone: *mut List = std::ptr::null_mut();
    let trigrel: Relation;

    /*
     * Search for any constraints where this partition's parent is in the
     * referenced side. Build the list to clone in two steps to avoid duplicates.
     */
    pg_constraint = table_open(ConstraintRelationId, RowShareLock);

    let mut key = [ScanKeyData::default(); 2];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_confrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent_rel)),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_constraint_contype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(CONSTRAINT_FOREIGN as i8 as i64),
    );
    /* This is a seqscan, as we don't have a usable index ... */
    let scan = systable_beginscan(
        pg_constraint,
        InvalidOid,
        true,
        std::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );
    let mut tuple: HeapTuple;
    loop {
        tuple = systable_getnext(scan);
        if tuple.is_null() { break; }
        let constr_form = GETSTRUCT(tuple) as Form_pg_constraint;
        clone = lappend_oid(clone, (*constr_form).oid);
    }
    systable_endscan(scan);
    table_close(pg_constraint, RowShareLock);

    /*
     * Triggers will be manipulated a bunch of times in the loop below.
     */
    trigrel = table_open(TriggerRelationId, RowExclusiveLock);

    attmap = build_attrmap_by_name(
        RelationGetDescr(partition_rel),
        RelationGetDescr(parent_rel),
        false,
    );

    let mut cell = list_head(clone);
    while !cell.is_null() {
        let constr_oid = lfirst_oid(cell);
        let constr_form: Form_pg_constraint;
        let fk_rel: Relation;
        let index_oid: Oid;
        let part_index_id: Oid;
        let mut numfks: i32 = 0;
        let mut conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut mapped_confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut conpfeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conppeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conffeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut numfkdelsetcols: i32 = 0;
        let mut confdelsetcols = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut delete_trigger_oid = InvalidOid;
        let mut update_trigger_oid = InvalidOid;

        let con_tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constr_oid));
        if !HeapTupleIsValid(con_tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", constr_oid);
        }
        constr_form = GETSTRUCT(con_tuple) as Form_pg_constraint;

        /*
         * As explained above: don't try to clone a constraint for which we're
         * going to clone the parent.
         */
        if list_member_oid(clone, (*constr_form).conparentid) {
            ReleaseSysCache(con_tuple);
            cell = lnext(clone, cell);
            continue;
        }

        /* We need the same lock level that CreateTrigger will acquire */
        fk_rel = table_open((*constr_form).conrelid, ShareRowExclusiveLock);
        index_oid = (*constr_form).conindid;

        DeconstructFkConstraintRow(
            con_tuple,
            &mut numfks,
            conkey.as_mut_ptr(),
            confkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            &mut numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
        );

        for i in 0..numfks as usize {
            mapped_confkey[i] = (*attmap).attnums[(confkey[i] as usize) - 1];
        }

        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*fkconstraint).contype = CONSTRAINT_FOREIGN;
        (*fkconstraint).conname = NameStr!((*constr_form).conname) as *mut i8;
        (*fkconstraint).deferrable = (*constr_form).condeferrable;
        (*fkconstraint).initdeferred = (*constr_form).condeferred;
        (*fkconstraint).location = -1;
        (*fkconstraint).pktable = std::ptr::null_mut();
        (*fkconstraint).pk_attrs = std::ptr::null_mut();
        (*fkconstraint).fk_matchtype = (*constr_form).confmatchtype;
        (*fkconstraint).fk_upd_action = (*constr_form).confupdtype;
        (*fkconstraint).fk_del_action = (*constr_form).confdeltype;
        (*fkconstraint).fk_del_set_cols = std::ptr::null_mut();
        (*fkconstraint).old_conpfeqop = std::ptr::null_mut();
        (*fkconstraint).old_pktable_oid = InvalidOid;
        (*fkconstraint).is_enforced = (*constr_form).conenforced;
        (*fkconstraint).skip_validation = false;
        (*fkconstraint).initially_valid = (*constr_form).convalidated;

        /* set up colnames that are used to generate the constraint name */
        for i in 0..numfks as usize {
            let att = TupleDescAttr(RelationGetDescr(fk_rel), conkey[i] as usize - 1);
            (*fkconstraint).fk_attrs = lappend(
                (*fkconstraint).fk_attrs,
                makeString(NameStr!((*att).attname) as *mut i8) as *mut _,
            );
        }

        /*
         * Add the new foreign key constraint pointing to the new partition.
         */
        part_index_id = index_get_partition(partition_rel, index_oid);
        if !OidIsValid(part_index_id) {
            elog!(
                ERROR,
                "index for {} not found in partition {}",
                index_oid,
                std::ffi::CStr::from_ptr(RelationGetRelationName(partition_rel)).to_string_lossy()
            );
        }

        /*
         * Get the "action" triggers belonging to the constraint.
         */
        if (*constr_form).conenforced {
            GetForeignKeyActionTriggers(
                trigrel,
                constr_oid,
                (*constr_form).confrelid,
                (*constr_form).conrelid,
                &mut delete_trigger_oid,
                &mut update_trigger_oid,
            );
        }

        /* Add this constraint ... */
        let sub_address = addFkConstraint(
            addFkReferencedSide,
            (*fkconstraint).conname,
            fkconstraint,
            fk_rel,
            partition_rel,
            part_index_id,
            constr_oid,
            numfks,
            mapped_confkey.as_mut_ptr(),
            conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            false,
            (*constr_form).conperiod,
        );
        /* ... and recurse */
        addFkRecurseReferenced(
            fkconstraint,
            fk_rel,
            partition_rel,
            part_index_id,
            sub_address.objectId,
            numfks,
            mapped_confkey.as_mut_ptr(),
            conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            true,
            delete_trigger_oid,
            update_trigger_oid,
            (*constr_form).conperiod,
        );

        table_close(fk_rel, NoLock);
        ReleaseSysCache(con_tuple);
        cell = lnext(clone, cell);
    }

    table_close(trigrel, RowExclusiveLock);
}

/*
 * CloneFkReferencing
 *   For each FK constraint of the parent relation, find an equivalent constraint
 *   in its partition relation that can be reparented, or create a new one.
 */
unsafe fn CloneFkReferencing(
    wqueue: *mut *mut List,
    parent_rel: Relation,
    part_rel: Relation,
) {
    let attmap: *mut AttrMap;
    let part_fks: *mut List;
    let mut clone: *mut List = std::ptr::null_mut();
    let trigrel: Relation;

    /* obtain a list of constraints that we need to clone */
    let fk_list = RelationGetFKeyList(parent_rel);
    let mut fk_lc = list_head(fk_list);
    while !fk_lc.is_null() {
        let fk = lfirst(fk_lc) as *mut ForeignKeyCacheInfo;

        /*
         * Refuse to attach a table as partition that this partitioned table
         * already has a foreign key to.
         */
        if (*fk).confrelid == RelationGetRelid(part_rel) {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot attach table \"{}\" as a partition because it is referenced by foreign key \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(part_rel)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(get_constraint_name((*fk).conoid)).to_string_lossy()
                )
            );
        }

        clone = lappend_oid(clone, (*fk).conoid);
        fk_lc = lnext(fk_list, fk_lc);
    }

    /* Silently do nothing if there's nothing to do. */
    if clone.is_null() {
        return;
    }

    if (*(*part_rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("foreign key constraints are not supported on foreign tables")
        );
    }

    trigrel = table_open(TriggerRelationId, RowExclusiveLock);
    attmap = build_attrmap_by_name(
        RelationGetDescr(part_rel),
        RelationGetDescr(parent_rel),
        false,
    );
    part_fks = copyObject(RelationGetFKeyList(part_rel)) as *mut List;

    let mut cell = list_head(clone);
    while !cell.is_null() {
        let parent_constr_oid = lfirst_oid(cell);
        let constr_form: Form_pg_constraint;
        let pkrel: Relation;
        let mut numfks: i32 = 0;
        let mut conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut mapped_conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut conpfeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conppeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conffeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut numfkdelsetcols: i32 = 0;
        let mut confdelsetcols = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut insert_trigger_oid = InvalidOid;
        let mut update_trigger_oid = InvalidOid;

        let tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_constr_oid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", parent_constr_oid);
        }
        constr_form = GETSTRUCT(tuple) as Form_pg_constraint;

        /* Don't clone constraints whose parents are being cloned */
        if list_member_oid(clone, (*constr_form).conparentid) {
            ReleaseSysCache(tuple);
            cell = lnext(clone, cell);
            continue;
        }

        /*
         * Need to prevent concurrent deletions.
         */
        pkrel = table_open((*constr_form).confrelid, ShareRowExclusiveLock);
        if (*(*pkrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
            find_all_inheritors(RelationGetRelid(pkrel), ShareRowExclusiveLock, std::ptr::null_mut());
        }

        DeconstructFkConstraintRow(
            tuple,
            &mut numfks,
            conkey.as_mut_ptr(),
            confkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            &mut numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
        );
        for i in 0..numfks as usize {
            mapped_conkey[i] = (*attmap).attnums[(conkey[i] as usize) - 1];
        }

        /*
         * Get the "check" triggers belonging to the constraint.
         */
        if (*constr_form).conenforced {
            GetForeignKeyCheckTriggers(
                trigrel,
                (*constr_form).oid,
                (*constr_form).confrelid,
                (*constr_form).conrelid,
                &mut insert_trigger_oid,
                &mut update_trigger_oid,
            );
        }

        /*
         * Before creating a new constraint, see whether any existing FKs are fit.
         */
        let mut attached = false;
        let mut fk_lc2 = list_head(part_fks);
        while !fk_lc2.is_null() {
            let fk = lfirst_node!(ForeignKeyCacheInfo, T_ForeignKeyCacheInfo, current_cell!(fk_lc2));
            if tryAttachPartitionForeignKey(
                wqueue,
                fk,
                part_rel,
                parent_constr_oid,
                numfks,
                mapped_conkey.as_mut_ptr(),
                confkey.as_mut_ptr(),
                conpfeqop.as_mut_ptr(),
                insert_trigger_oid,
                update_trigger_oid,
                trigrel,
            ) {
                attached = true;
                table_close(pkrel, NoLock);
                break;
            }
            fk_lc2 = lnext(part_fks, fk_lc2);
        }
        if attached {
            ReleaseSysCache(tuple);
            cell = lnext(clone, cell);
            continue;
        }

        /* No dice.  Set up to create our own constraint */
        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*fkconstraint).contype = CONSTRAINT_FOREIGN;
        (*fkconstraint).deferrable = (*constr_form).condeferrable;
        (*fkconstraint).initdeferred = (*constr_form).condeferred;
        (*fkconstraint).location = -1;
        (*fkconstraint).pktable = std::ptr::null_mut();
        (*fkconstraint).pk_attrs = std::ptr::null_mut();
        (*fkconstraint).fk_matchtype = (*constr_form).confmatchtype;
        (*fkconstraint).fk_upd_action = (*constr_form).confupdtype;
        (*fkconstraint).fk_del_action = (*constr_form).confdeltype;
        (*fkconstraint).fk_del_set_cols = std::ptr::null_mut();
        (*fkconstraint).old_conpfeqop = std::ptr::null_mut();
        (*fkconstraint).old_pktable_oid = InvalidOid;
        (*fkconstraint).is_enforced = (*constr_form).conenforced;
        (*fkconstraint).skip_validation = false;
        (*fkconstraint).initially_valid = (*constr_form).convalidated;
        for i in 0..numfks as usize {
            let att = TupleDescAttr(RelationGetDescr(part_rel), mapped_conkey[i] as usize - 1);
            (*fkconstraint).fk_attrs = lappend(
                (*fkconstraint).fk_attrs,
                makeString(NameStr!((*att).attname) as *mut i8) as *mut _,
            );
        }

        let index_oid = (*constr_form).conindid;
        let with_period = (*constr_form).conperiod;

        /* Create the pg_constraint entry at this level */
        let sub_address = addFkConstraint(
            addFkReferencingSide,
            NameStr!((*constr_form).conname) as *mut i8,
            fkconstraint,
            part_rel,
            pkrel,
            index_oid,
            parent_constr_oid,
            numfks,
            confkey.as_mut_ptr(),
            mapped_conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            false,
            with_period,
        );

        /* Done with the cloned constraint's tuple */
        ReleaseSysCache(tuple);

        /* Create the check triggers, and recurse to partitions, if any */
        addFkRecurseReferencing(
            wqueue,
            fkconstraint,
            part_rel,
            pkrel,
            index_oid,
            sub_address.objectId,
            numfks,
            confkey.as_mut_ptr(),
            mapped_conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            false, /* no old check exists */
            AccessExclusiveLock,
            insert_trigger_oid,
            update_trigger_oid,
            with_period,
        );
        table_close(pkrel, NoLock);
        cell = lnext(clone, cell);
    }

    table_close(trigrel, RowExclusiveLock);
}

/*
 * tryAttachPartitionForeignKey
 *   Examine whether an existing FK constraint on partition can be used
 *   as-is rather than creating a new one.
 */
unsafe fn tryAttachPartitionForeignKey(
    wqueue: *mut *mut List,
    fk: *mut ForeignKeyCacheInfo,
    partition: Relation,
    parent_constr_oid: Oid,
    numfks: i32,
    mapped_conkey: *mut AttrNumber,
    confkey: *mut AttrNumber,
    conpfeqop: *mut Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    trigrel: Relation,
) -> bool {
    let parent_constr_tup: HeapTuple;
    let parent_constr: Form_pg_constraint;
    let partcontup: HeapTuple;
    let part_constr: Form_pg_constraint;

    parent_constr_tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_constr_oid));
    if !HeapTupleIsValid(parent_constr_tup) {
        elog!(ERROR, "cache lookup failed for constraint {}", parent_constr_oid);
    }
    parent_constr = GETSTRUCT(parent_constr_tup) as Form_pg_constraint;

    /* Quick initial checks */
    if (*fk).confrelid != (*parent_constr).confrelid || (*fk).nkeys != numfks {
        ReleaseSysCache(parent_constr_tup);
        return false;
    }
    for i in 0..numfks as usize {
        if (*fk).conkey[i] != *mapped_conkey.add(i)
            || (*fk).confkey[i] != *confkey.add(i)
            || (*fk).conpfeqop[i] != *conpfeqop.add(i)
        {
            ReleaseSysCache(parent_constr_tup);
            return false;
        }
    }

    /* More extensive checks */
    partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum((*fk).conoid));
    if !HeapTupleIsValid(partcontup) {
        elog!(ERROR, "cache lookup failed for constraint {}", (*fk).conoid);
    }
    part_constr = GETSTRUCT(partcontup) as Form_pg_constraint;

    /*
     * An error should be raised if the constraint enforceability is different.
     */
    if (*part_constr).conenforced != (*parent_constr).conenforced {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg!(
                "constraint \"{}\" enforceability conflicts with constraint \"{}\" on relation \"{}\"",
                std::ffi::CStr::from_ptr(NameStr!((*parent_constr).conname) as *mut i8).to_string_lossy(),
                std::ffi::CStr::from_ptr(NameStr!((*part_constr).conname) as *mut i8).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(partition)).to_string_lossy()
            )
        );
    }

    if OidIsValid((*part_constr).conparentid)
        || (*part_constr).condeferrable != (*parent_constr).condeferrable
        || (*part_constr).condeferred != (*parent_constr).condeferred
        || (*part_constr).confupdtype != (*parent_constr).confupdtype
        || (*part_constr).confdeltype != (*parent_constr).confdeltype
        || (*part_constr).confmatchtype != (*parent_constr).confmatchtype
    {
        ReleaseSysCache(parent_constr_tup);
        ReleaseSysCache(partcontup);
        return false;
    }

    ReleaseSysCache(parent_constr_tup);
    ReleaseSysCache(partcontup);

    /* Looks good! Attach this constraint */
    AttachPartitionForeignKey(
        wqueue,
        partition,
        (*fk).conoid,
        parent_constr_oid,
        parent_ins_trigger,
        parent_upd_trigger,
        trigrel,
    );

    true
}

/*
 * AttachPartitionForeignKey
 *   Final tasks of attaching a FK constraint to a partition.
 */
unsafe fn AttachPartitionForeignKey(
    wqueue: *mut *mut List,
    partition: Relation,
    part_constr_oid: Oid,
    parent_constr_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    trigrel: Relation,
) {
    let parent_constr_tup: HeapTuple;
    let parent_constr: Form_pg_constraint;
    let mut partcontup: HeapTuple;
    let part_constr: Form_pg_constraint;
    let queue_validation: bool;
    let part_constr_frelid: Oid;
    let part_constr_relid: Oid;
    let parent_constr_is_enforced: bool;

    parent_constr_tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_constr_oid));
    if !HeapTupleIsValid(parent_constr_tup) {
        elog!(ERROR, "cache lookup failed for constraint {}", parent_constr_oid);
    }
    parent_constr = GETSTRUCT(parent_constr_tup) as Form_pg_constraint;
    parent_constr_is_enforced = (*parent_constr).conenforced;

    partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(part_constr_oid));
    if !HeapTupleIsValid(partcontup) {
        elog!(ERROR, "cache lookup failed for constraint {}", part_constr_oid);
    }
    part_constr = GETSTRUCT(partcontup) as Form_pg_constraint;
    part_constr_frelid = (*part_constr).confrelid;
    part_constr_relid = (*part_constr).conrelid;

    /*
     * If the referenced table is partitioned, remove extra pg_constraint rows
     * and action triggers that are no longer needed.
     */
    if get_rel_relkind(part_constr_frelid) == RELKIND_PARTITIONED_TABLE as i8 {
        let pg_constraint = table_open(ConstraintRelationId, RowShareLock);
        RemoveInheritedConstraint(pg_constraint, trigrel, part_constr_oid, part_constr_relid);
        table_close(pg_constraint, RowShareLock);
    }

    queue_validation = (*parent_constr).convalidated && !(*part_constr).convalidated;

    ReleaseSysCache(partcontup);
    ReleaseSysCache(parent_constr_tup);

    /*
     * The action triggers in the new partition become redundant -- remove them.
     */
    DropForeignKeyConstraintTriggers(trigrel, part_constr_oid, part_constr_frelid, part_constr_relid);

    ConstraintSetParentConstraint(part_constr_oid, parent_constr_oid, RelationGetRelid(partition));

    /*
     * Like the constraint, attach partition's "check" triggers to the
     * corresponding parent triggers if the constraint is ENFORCED.
     */
    if parent_constr_is_enforced {
        let mut insert_trigger_oid = InvalidOid;
        let mut update_trigger_oid = InvalidOid;

        GetForeignKeyCheckTriggers(
            trigrel,
            part_constr_oid,
            part_constr_frelid,
            part_constr_relid,
            &mut insert_trigger_oid,
            &mut update_trigger_oid,
        );
        Assert!(OidIsValid(insert_trigger_oid) && OidIsValid(parent_ins_trigger));
        TriggerSetParentTrigger(trigrel, insert_trigger_oid, parent_ins_trigger, RelationGetRelid(partition));
        Assert!(OidIsValid(update_trigger_oid) && OidIsValid(parent_upd_trigger));
        TriggerSetParentTrigger(trigrel, update_trigger_oid, parent_upd_trigger, RelationGetRelid(partition));
    }

    CommandCounterIncrement();

    if queue_validation {
        let conrel = table_open(ConstraintRelationId, RowExclusiveLock);
        partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(part_constr_oid));
        if !HeapTupleIsValid(partcontup) {
            elog!(ERROR, "cache lookup failed for constraint {}", part_constr_oid);
        }
        let confrelid = (*(GETSTRUCT(partcontup) as Form_pg_constraint)).confrelid;
        /* Use the same lock as for AT_ValidateConstraint */
        QueueFKConstraintValidation(
            wqueue,
            conrel,
            partition,
            confrelid,
            partcontup,
            ShareUpdateExclusiveLock,
        );
        ReleaseSysCache(partcontup);
        table_close(conrel, RowExclusiveLock);
    }
}

/*
 * RemoveInheritedConstraint
 *   Remove the constraint and its associated triggers from the given relation,
 *   which inherited the given constraint.
 */
unsafe fn RemoveInheritedConstraint(
    conrel: Relation,
    trigrel: Relation,
    conoid: Oid,
    conrelid: Oid,
) {
    let objs: *mut ObjectAddresses;
    let mut consttup: HeapTuple;
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    ScanKeyInit(
        &mut key,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conrelid),
    );
    scan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        std::ptr::null_mut(),
        1,
        &mut key,
    );
    objs = new_object_addresses();
    loop {
        consttup = systable_getnext(scan);
        if consttup.is_null() { break; }
        let conform = GETSTRUCT(consttup) as Form_pg_constraint;

        if (*conform).conparentid != conoid {
            continue;
        } else {
            let mut addr = ObjectAddress::default();
            let scan2: SysScanDesc;
            let mut key2 = ScanKeyData::default();

            ObjectAddressSet!(addr, ConstraintRelationId, (*conform).oid);
            add_exact_object_address(&mut addr, objs);

            /*
             * Delete the dependency record binding the two constraint records.
             */
            /* n = */ deleteDependencyRecordsForSpecific(
                ConstraintRelationId,
                (*conform).oid,
                DEPENDENCY_INTERNAL,
                ConstraintRelationId,
                conoid,
            );
            /* Assert n == 1 */

            /*
             * Now search for the triggers and set them up for deletion.
             */
            ScanKeyInit(
                &mut key2,
                Anum_pg_trigger_tgconstraint,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum((*conform).oid),
            );
            scan2 = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key2);
            loop {
                trigtup = systable_getnext(scan2);
                if trigtup.is_null() { break; }
                ObjectAddressSet!(addr, TriggerRelationId, (*(GETSTRUCT(trigtup) as Form_pg_trigger)).oid);
                add_exact_object_address(&mut addr, objs);
            }
            systable_endscan(scan2);
        }
    }
    /* make the dependency deletions visible */
    CommandCounterIncrement();
    performMultipleDeletions(objs, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    systable_endscan(scan);
}

/*
 * DropForeignKeyConstraintTriggers
 *   Delete action triggers for the given FK constraint.
 */
unsafe fn DropForeignKeyConstraintTriggers(
    trigrel: Relation,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) {
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key);
    loop {
        trigtup = systable_getnext(scan);
        if trigtup.is_null() { break; }
        let trgform = GETSTRUCT(trigtup) as Form_pg_trigger;
        let mut trigger_addr = ObjectAddress::default();

        /* Invalid if trigger is not for a referential integrity constraint */
        if !OidIsValid((*trgform).tgconstrrelid) {
            continue;
        }
        if OidIsValid(conrelid) && (*trgform).tgconstrrelid != conrelid {
            continue;
        }
        if OidIsValid(confrelid) && (*trgform).tgrelid != confrelid {
            continue;
        }

        /* We should be dropping trigger related to foreign key constraint */
        Assert!(
            (*trgform).tgfoid == F_RI_FKEY_CHECK_INS
                || (*trgform).tgfoid == F_RI_FKEY_CHECK_UPD
                || (*trgform).tgfoid == F_RI_FKEY_CASCADE_DEL
                || (*trgform).tgfoid == F_RI_FKEY_CASCADE_UPD
                || (*trgform).tgfoid == F_RI_FKEY_RESTRICT_DEL
                || (*trgform).tgfoid == F_RI_FKEY_RESTRICT_UPD
                || (*trgform).tgfoid == F_RI_FKEY_SETNULL_DEL
                || (*trgform).tgfoid == F_RI_FKEY_SETNULL_UPD
                || (*trgform).tgfoid == F_RI_FKEY_SETDEFAULT_DEL
                || (*trgform).tgfoid == F_RI_FKEY_SETDEFAULT_UPD
                || (*trgform).tgfoid == F_RI_FKEY_NOACTION_DEL
                || (*trgform).tgfoid == F_RI_FKEY_NOACTION_UPD
        );

        /*
         * Remove the dependency link so we can drop the trigger while
         * keeping the constraint intact.
         */
        deleteDependencyRecordsFor(TriggerRelationId, (*trgform).oid, false);
        /* make dependency deletion visible to performDeletion */
        CommandCounterIncrement();
        ObjectAddressSet!(trigger_addr, TriggerRelationId, (*trgform).oid);
        performDeletion(&trigger_addr, DROP_RESTRICT, 0);
        /* make trigger drop visible, in case the loop iterates */
        CommandCounterIncrement();
    }

    systable_endscan(scan);
}

/*
 * GetForeignKeyActionTriggers
 *   Returns delete and update "action" triggers of the given relation
 *   belonging to the given constraint.
 */
unsafe fn GetForeignKeyActionTriggers(
    trigrel: Relation,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
    delete_trigger_oid: *mut Oid,
    update_trigger_oid: *mut Oid,
) {
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    *delete_trigger_oid = InvalidOid;
    *update_trigger_oid = InvalidOid;
    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );

    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key);
    loop {
        trigtup = systable_getnext(scan);
        if trigtup.is_null() { break; }
        let trgform = GETSTRUCT(trigtup) as Form_pg_trigger;

        if (*trgform).tgconstrrelid != conrelid {
            continue;
        }
        if (*trgform).tgrelid != confrelid {
            continue;
        }
        /* Only ever look at "action" triggers on the PK side. */
        if RI_FKey_trigger_type((*trgform).tgfoid) != RI_TRIGGER_PK {
            continue;
        }
        if TRIGGER_FOR_DELETE((*trgform).tgtype) {
            Assert!(*delete_trigger_oid == InvalidOid);
            *delete_trigger_oid = (*trgform).oid;
        } else if TRIGGER_FOR_UPDATE((*trgform).tgtype) {
            Assert!(*update_trigger_oid == InvalidOid);
            *update_trigger_oid = (*trgform).oid;
        }
        /* In an assert-enabled build, continue looking to find duplicates */
        #[cfg(not(debug_assertions))]
        if OidIsValid(*delete_trigger_oid) && OidIsValid(*update_trigger_oid) {
            break;
        }
    }

    if !OidIsValid(*delete_trigger_oid) {
        elog!(ERROR, "could not find ON DELETE action trigger of foreign key constraint {}", conoid);
    }
    if !OidIsValid(*update_trigger_oid) {
        elog!(ERROR, "could not find ON UPDATE action trigger of foreign key constraint {}", conoid);
    }

    systable_endscan(scan);
}

/*
 * GetForeignKeyCheckTriggers
 *   Returns insert and update "check" triggers of the given relation
 *   belonging to the given constraint.
 */
unsafe fn GetForeignKeyCheckTriggers(
    trigrel: Relation,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
    insert_trigger_oid: *mut Oid,
    update_trigger_oid: *mut Oid,
) {
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    *insert_trigger_oid = InvalidOid;
    *update_trigger_oid = InvalidOid;
    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );

    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key);
    loop {
        trigtup = systable_getnext(scan);
        if trigtup.is_null() { break; }
        let trgform = GETSTRUCT(trigtup) as Form_pg_trigger;

        if (*trgform).tgconstrrelid != confrelid {
            continue;
        }
        if (*trgform).tgrelid != conrelid {
            continue;
        }
        /* Only ever look at "check" triggers on the FK side. */
        if RI_FKey_trigger_type((*trgform).tgfoid) != RI_TRIGGER_FK {
            continue;
        }
        if TRIGGER_FOR_INSERT((*trgform).tgtype) {
            Assert!(*insert_trigger_oid == InvalidOid);
            *insert_trigger_oid = (*trgform).oid;
        } else if TRIGGER_FOR_UPDATE((*trgform).tgtype) {
            Assert!(*update_trigger_oid == InvalidOid);
            *update_trigger_oid = (*trgform).oid;
        }
        /* In an assert-enabled build, continue looking to find duplicates. */
        #[cfg(not(debug_assertions))]
        if OidIsValid(*insert_trigger_oid) && OidIsValid(*update_trigger_oid) {
            break;
        }
    }

    if !OidIsValid(*insert_trigger_oid) {
        elog!(ERROR, "could not find ON INSERT check triggers of foreign key constraint {}", conoid);
    }
    if !OidIsValid(*update_trigger_oid) {
        elog!(ERROR, "could not find ON UPDATE check triggers of foreign key constraint {}", conoid);
    }

    systable_endscan(scan);
}

/*
 * ATExecAlterConstraint
 *   ALTER TABLE ALTER CONSTRAINT -- update attributes of a constraint.
 *   Currently only works for Foreign Key and not-null constraints.
 */
unsafe fn ATExecAlterConstraint(
    wqueue: *mut *mut List,
    rel: Relation,
    cmdcon: *mut ATAlterConstraint,
    recurse: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let conrel: Relation;
    let tgrel: Relation;
    let scan: SysScanDesc;
    let mut skey = [ScanKeyData::default(); 3];
    let mut contuple: HeapTuple;
    let currcon: Form_pg_constraint;
    let mut address = InvalidObjectAddress;

    /*
     * Disallow altering ONLY a partitioned table, as it would make no sense.
     * This is okay for legacy inheritance.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("constraint must be altered in child tables too")
            /* errhint: Do not specify the ONLY keyword. */
        );
    }

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);

    /* Find and check the target constraint */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum((*cmdcon).conname),
    );
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, std::ptr::null_mut(), 3, skey.as_mut_ptr());

    /* There can be at most one matching row */
    contuple = systable_getnext(scan);
    if !HeapTupleIsValid(contuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    if (*cmdcon).alterDeferrability && (*currcon).contype != CONSTRAINT_FOREIGN as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" is not a foreign key constraint",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    if (*cmdcon).alterEnforceability && (*currcon).contype != CONSTRAINT_FOREIGN as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "cannot alter enforceability of constraint \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    if (*cmdcon).alterInheritability && (*currcon).contype != CONSTRAINT_NOTNULL as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" is not a not-null constraint",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* Refuse to modify inheritability of inherited constraints */
    if (*cmdcon).alterInheritability && (*cmdcon).noinherit && (*currcon).coninhcount > 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot alter inherited constraint \"{}\" on relation \"{}\"",
                std::ffi::CStr::from_ptr(NameStr!((*currcon).conname) as *mut i8).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * If it's not the topmost constraint, raise an error.
     */
    if OidIsValid((*currcon).conparentid) {
        let mut parent = (*currcon).conparentid;
        let mut ancestor_name: *mut i8 = std::ptr::null_mut();
        let mut ancestor_table: *mut i8 = std::ptr::null_mut();

        /* Loop to find the topmost constraint */
        loop {
            let tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent));
            if !HeapTupleIsValid(tp) { break; }
            let contup = GETSTRUCT(tp) as Form_pg_constraint;
            if !OidIsValid((*contup).conparentid) {
                ancestor_name = pstrdup(NameStr!((*contup).conname) as *mut i8);
                ancestor_table = get_rel_name((*contup).conrelid);
                ReleaseSysCache(tp);
                break;
            }
            parent = (*contup).conparentid;
            ReleaseSysCache(tp);
        }

        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot alter constraint \"{}\" on relation \"{}\"",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errdetail and errhint omitted - see C source */
        );
    }

    /*
     * Do the actual catalog work, and recurse if necessary.
     */
    if ATExecAlterConstraintInternal(wqueue, cmdcon, conrel, tgrel, rel, contuple, recurse, lockmode) {
        ObjectAddressSet!(address, ConstraintRelationId, (*currcon).oid);
    }

    systable_endscan(scan);
    table_close(tgrel, RowExclusiveLock);
    table_close(conrel, RowExclusiveLock);

    address
}

/*
 * A subroutine of ATExecAlterConstraint that calls the respective routines for
 * altering constraint's enforceability, deferrability or inheritability.
 */
unsafe fn ATExecAlterConstraintInternal(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    lockmode: LOCKMODE,
) -> bool {
    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let mut changed = false;
    let mut otherrelids: *mut List = std::ptr::null_mut();

    /*
     * Note that even if deferrability is requested to be altered along with
     * enforceability, we don't need to explicitly update multiple entries in
     * pg_trigger related to deferrability.
     */
    if (*cmdcon).alterEnforceability
        && ATExecAlterConstrEnforceability(
            wqueue,
            cmdcon,
            conrel,
            tgrel,
            (*currcon).conrelid,
            (*currcon).confrelid,
            contuple,
            lockmode,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
        )
    {
        changed = true;
    } else if (*cmdcon).alterDeferrability
        && ATExecAlterConstrDeferrability(
            wqueue,
            cmdcon,
            conrel,
            tgrel,
            rel,
            contuple,
            recurse,
            &mut otherrelids,
            lockmode,
        )
    {
        /*
         * AlterConstrUpdateConstraintEntry already invalidated relcache for
         * the relations having the constraint itself; here we also invalidate
         * for relations that have any triggers that are part of the constraint.
         */
        let mut lc = list_head(otherrelids);
        while !lc.is_null() {
            CacheInvalidateRelcacheByRelid(lfirst_oid(lc));
            lc = lnext(otherrelids, lc);
        }
        changed = true;
    }

    /* Do the catalog work for the inheritability change. */
    if (*cmdcon).alterInheritability
        && ATExecAlterConstrInheritability(wqueue, cmdcon, conrel, rel, contuple, lockmode)
    {
        changed = true;
    }

    changed
}

/*
 * Returns true if the constraint's enforceability is altered.
 */
unsafe fn ATExecAlterConstrEnforceability(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    fkrelid: Oid,
    pkrelid: Oid,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) -> bool {
    check_stack_depth();
    Assert!((*cmdcon).alterEnforceability);

    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let conoid = (*currcon).oid;
    Assert!((*currcon).contype == CONSTRAINT_FOREIGN as i8);

    let rel = table_open((*currcon).conrelid, lockmode);
    let mut changed = false;

    if (*currcon).conenforced != (*cmdcon).is_enforced {
        AlterConstrUpdateConstraintEntry(cmdcon, conrel, contuple);
        changed = true;
    }

    /* Drop triggers */
    if !(*cmdcon).is_enforced {
        /*
         * When setting a constraint to NOT ENFORCED, process child relations first,
         * then the parent.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
            || get_rel_relkind((*currcon).confrelid) == RELKIND_PARTITIONED_TABLE as i8
        {
            AlterConstrEnforceabilityRecurse(
                wqueue,
                cmdcon,
                conrel,
                tgrel,
                fkrelid,
                pkrelid,
                contuple,
                lockmode,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
            );
        }
        /* Drop all the triggers */
        DropForeignKeyConstraintTriggers(tgrel, conoid, InvalidOid, InvalidOid);
    } else if changed {
        /* Create triggers */
        let mut referenced_del_trigger_oid = InvalidOid;
        let mut referenced_upd_trigger_oid = InvalidOid;
        let mut referencing_ins_trigger_oid = InvalidOid;
        let mut referencing_upd_trigger_oid = InvalidOid;

        /* Prepare the minimal information required for trigger creation. */
        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*fkconstraint).conname = pstrdup(NameStr!((*currcon).conname) as *mut i8);
        (*fkconstraint).fk_matchtype = (*currcon).confmatchtype;
        (*fkconstraint).fk_upd_action = (*currcon).confupdtype;
        (*fkconstraint).fk_del_action = (*currcon).confdeltype;

        /* Create referenced triggers */
        if (*currcon).conrelid == fkrelid {
            createForeignKeyActionTriggers(
                (*currcon).conrelid,
                (*currcon).confrelid,
                fkconstraint,
                conoid,
                (*currcon).conindid,
                referenced_parent_del_trigger,
                referenced_parent_upd_trigger,
                &mut referenced_del_trigger_oid,
                &mut referenced_upd_trigger_oid,
            );
        }

        /* Create referencing triggers */
        if (*currcon).confrelid == pkrelid {
            createForeignKeyCheckTriggers(
                (*currcon).conrelid,
                pkrelid,
                fkconstraint,
                conoid,
                (*currcon).conindid,
                referencing_parent_ins_trigger,
                referencing_parent_upd_trigger,
                &mut referencing_ins_trigger_oid,
                &mut referencing_upd_trigger_oid,
            );
        }

        /*
         * Tell Phase 3 to check that the constraint is satisfied by existing rows.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_RELATION as i8 && (*currcon).confrelid == pkrelid {
            let tab = ATGetQueueEntry(wqueue, rel);
            let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
            (*newcon).name = (*fkconstraint).conname;
            (*newcon).contype = CONSTR_FOREIGN;
            (*newcon).refrelid = (*currcon).confrelid;
            (*newcon).refindid = (*currcon).conindid;
            (*newcon).conid = (*currcon).oid;
            (*newcon).qual = fkconstraint as *mut Node;
            (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
        }

        /*
         * If the table at either end of the constraint is partitioned, we need to
         * recurse and create triggers for each constraint that is a child.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
            || get_rel_relkind((*currcon).confrelid) == RELKIND_PARTITIONED_TABLE as i8
        {
            AlterConstrEnforceabilityRecurse(
                wqueue,
                cmdcon,
                conrel,
                tgrel,
                fkrelid,
                pkrelid,
                contuple,
                lockmode,
                referenced_del_trigger_oid,
                referenced_upd_trigger_oid,
                referencing_ins_trigger_oid,
                referencing_upd_trigger_oid,
            );
        }
    }

    table_close(rel, NoLock);
    changed
}

/*
 * Returns true if the constraint's deferrability is altered.
 */
unsafe fn ATExecAlterConstrDeferrability(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    otherrelids: *mut *mut List,
    lockmode: LOCKMODE,
) -> bool {
    check_stack_depth();
    Assert!((*cmdcon).alterDeferrability);

    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let refrelid = (*currcon).confrelid;
    let mut changed = false;

    /* Should be foreign key constraint */
    Assert!((*currcon).contype == CONSTRAINT_FOREIGN as i8);

    if (*currcon).condeferrable != (*cmdcon).deferrable
        || (*currcon).condeferred != (*cmdcon).initdeferred
    {
        AlterConstrUpdateConstraintEntry(cmdcon, conrel, contuple);
        changed = true;

        /* Update the triggers that implement the constraint */
        AlterConstrTriggerDeferrability(
            (*currcon).oid,
            tgrel,
            rel,
            (*cmdcon).deferrable,
            (*cmdcon).initdeferred,
            otherrelids,
        );
    }

    /*
     * If the table at either end of the constraint is partitioned, handle
     * every constraint that is a child of this one.
     */
    if recurse
        && changed
        && ((*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
            || get_rel_relkind(refrelid) == RELKIND_PARTITIONED_TABLE as i8)
    {
        AlterConstrDeferrabilityRecurse(wqueue, cmdcon, conrel, tgrel, rel, contuple, recurse, otherrelids, lockmode);
    }

    changed
}

/*
 * Returns true if the constraint's inheritability is altered.
 */
unsafe fn ATExecAlterConstrInheritability(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
) -> bool {
    Assert!((*cmdcon).alterInheritability);

    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;

    /* The current implementation only works for NOT NULL constraints */
    Assert!((*currcon).contype == CONSTRAINT_NOTNULL as i8);

    /* If already in desired state, silently do nothing. */
    if (*cmdcon).noinherit == (*currcon).connoinherit {
        return false;
    }

    AlterConstrUpdateConstraintEntry(cmdcon, conrel, contuple);
    CommandCounterIncrement();

    /* Fetch the column number and name */
    let col_num = extractNotNullColumn(contuple);
    let col_name = get_attname((*currcon).conrelid, col_num, false);

    /* Propagate the change to children. */
    let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childoid = lfirst_oid(child_lc);

        if (*cmdcon).noinherit {
            let child_tup = findNotNullConstraint(childoid, col_name);
            if child_tup.is_null() {
                elog!(
                    ERROR,
                    "cache lookup failed for not-null constraint on column \"{}\" of relation {}",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    childoid
                );
            }
            let childcon = GETSTRUCT(child_tup) as Form_pg_constraint;
            Assert!((*childcon).coninhcount > 0);
            (*childcon).coninhcount -= 1;
            (*childcon).conislocal = true;
            CatalogTupleUpdate(conrel, &mut (*child_tup).t_self, child_tup);
            heap_freetuple(child_tup);
        } else {
            let childrel = table_open(childoid, NoLock);
            let addr = ATExecSetNotNull(
                wqueue,
                childrel,
                NameStr!((*currcon).conname) as *mut i8,
                col_name,
                true,
                true,
                lockmode,
            );
            if OidIsValid(addr.objectId) {
                CommandCounterIncrement();
            }
            table_close(childrel, NoLock);
        }
        child_lc = lnext(children, child_lc);
    }

    true
}

/*
 * AlterConstrTriggerDeferrability
 *   Update constraint trigger deferrability for the given constraint.
 */
unsafe fn AlterConstrTriggerDeferrability(
    conoid: Oid,
    tgrel: Relation,
    rel: Relation,
    deferrable: bool,
    initdeferred: bool,
    otherrelids: *mut *mut List,
) {
    let mut tgtuple: HeapTuple;
    let mut tgkey = ScanKeyData::default();
    let tgscan: SysScanDesc;

    ScanKeyInit(
        &mut tgkey,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut tgkey);
    loop {
        tgtuple = systable_getnext(tgscan);
        if !HeapTupleIsValid(tgtuple) { break; }
        let tgform = GETSTRUCT(tgtuple) as Form_pg_trigger;

        /*
         * Remember OIDs of other relation(s) involved in FK constraint.
         */
        if (*tgform).tgrelid != RelationGetRelid(rel) {
            *otherrelids = list_append_unique_oid(*otherrelids, (*tgform).tgrelid);
        }

        /*
         * Update enable status and deferrability of RI_FKey_noaction_del,
         * RI_FKey_noaction_upd, RI_FKey_check_ins and RI_FKey_check_upd
         * triggers, but not others.
         */
        if (*tgform).tgfoid != F_RI_FKEY_NOACTION_DEL
            && (*tgform).tgfoid != F_RI_FKEY_NOACTION_UPD
            && (*tgform).tgfoid != F_RI_FKEY_CHECK_INS
            && (*tgform).tgfoid != F_RI_FKEY_CHECK_UPD
        {
            continue;
        }

        let tg_copy_tuple = heap_copytuple(tgtuple);
        let copy_tg = GETSTRUCT(tg_copy_tuple) as Form_pg_trigger;
        (*copy_tg).tgdeferrable = deferrable;
        (*copy_tg).tginitdeferred = initdeferred;
        CatalogTupleUpdate(tgrel, &mut (*tg_copy_tuple).t_self, tg_copy_tuple);
        InvokeObjectPostAlterHook(TriggerRelationId, (*tgform).oid, 0);
        heap_freetuple(tg_copy_tuple);
    }

    systable_endscan(tgscan);
}

/*
 * Invokes ATExecAlterConstrEnforceability for each child constraint.
 */
unsafe fn AlterConstrEnforceabilityRecurse(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    fkrelid: Oid,
    pkrelid: Oid,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) {
    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let conoid = (*currcon).oid;
    let mut pkey = ScanKeyData::default();
    let pscan: SysScanDesc;
    let mut childtup: HeapTuple;

    ScanKeyInit(
        &mut pkey,
        Anum_pg_constraint_conparentid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, std::ptr::null_mut(), 1, &mut pkey);
    loop {
        childtup = systable_getnext(pscan);
        if !HeapTupleIsValid(childtup) { break; }
        ATExecAlterConstrEnforceability(
            wqueue,
            cmdcon,
            conrel,
            tgrel,
            fkrelid,
            pkrelid,
            childtup,
            lockmode,
            referenced_parent_del_trigger,
            referenced_parent_upd_trigger,
            referencing_parent_ins_trigger,
            referencing_parent_upd_trigger,
        );
    }
    systable_endscan(pscan);
}

/*
 * Invokes ATExecAlterConstrDeferrability for each child constraint.
 */
unsafe fn AlterConstrDeferrabilityRecurse(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    otherrelids: *mut *mut List,
    lockmode: LOCKMODE,
) {
    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let conoid = (*currcon).oid;
    let mut pkey = ScanKeyData::default();
    let pscan: SysScanDesc;
    let mut childtup: HeapTuple;

    ScanKeyInit(
        &mut pkey,
        Anum_pg_constraint_conparentid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, std::ptr::null_mut(), 1, &mut pkey);
    loop {
        childtup = systable_getnext(pscan);
        if !HeapTupleIsValid(childtup) { break; }
        let childcon = GETSTRUCT(childtup) as Form_pg_constraint;
        let childrel = table_open((*childcon).conrelid, lockmode);
        ATExecAlterConstrDeferrability(
            wqueue, cmdcon, conrel, tgrel, childrel, childtup, recurse, otherrelids, lockmode,
        );
        table_close(childrel, NoLock);
    }
    systable_endscan(pscan);
}

/*
 * Update the constraint entry for the given ATAlterConstraint command.
 */
unsafe fn AlterConstrUpdateConstraintEntry(
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    contuple: HeapTuple,
) {
    Assert!((*cmdcon).alterEnforceability || (*cmdcon).alterDeferrability || (*cmdcon).alterInheritability);

    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;

    if (*cmdcon).alterEnforceability {
        (*copy_con).conenforced = (*cmdcon).is_enforced;
        (*copy_con).convalidated = (*cmdcon).is_enforced;
    }
    if (*cmdcon).alterDeferrability {
        (*copy_con).condeferrable = (*cmdcon).deferrable;
        (*copy_con).condeferred = (*cmdcon).initdeferred;
    }
    if (*cmdcon).alterInheritability {
        (*copy_con).connoinherit = (*cmdcon).noinherit;
    }

    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*copy_con).oid, 0);

    /* Make new constraint flags visible to others */
    CacheInvalidateRelcacheByRelid((*copy_con).conrelid);

    heap_freetuple(copy_tuple);
}

/*
 * ATExecValidateConstraint
 *   ALTER TABLE VALIDATE CONSTRAINT
 *   Return value is the address of the validated constraint.
 *   If the constraint was already validated, InvalidObjectAddress is returned.
 */
unsafe fn ATExecValidateConstraint(
    wqueue: *mut *mut List,
    rel: Relation,
    constr_name: *mut i8,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let conrel: Relation;
    let scan: SysScanDesc;
    let mut skey = [ScanKeyData::default(); 3];
    let mut tuple: HeapTuple;
    let con: Form_pg_constraint;
    let mut address = InvalidObjectAddress;

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Find and check the target constraint */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(constr_name),
    );
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, std::ptr::null_mut(), 3, skey.as_mut_ptr());

    /* There can be at most one matching row */
    tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(constr_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    con = GETSTRUCT(tuple) as Form_pg_constraint;
    if (*con).contype != CONSTRAINT_FOREIGN as i8
        && (*con).contype != CONSTRAINT_CHECK as i8
        && (*con).contype != CONSTRAINT_NOTNULL as i8
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "cannot validate constraint \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr(constr_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errdetail: This operation is not supported for this type of constraint. */
        );
    }

    if !(*con).conenforced {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!("cannot validate NOT ENFORCED constraint")
        );
    }

    if !(*con).convalidated {
        if (*con).contype == CONSTRAINT_FOREIGN as i8 {
            QueueFKConstraintValidation(wqueue, conrel, rel, (*con).confrelid, tuple, lockmode);
        } else if (*con).contype == CONSTRAINT_CHECK as i8 {
            QueueCheckConstraintValidation(
                wqueue, conrel, rel, constr_name, tuple, recurse, recursing, lockmode,
            );
        } else if (*con).contype == CONSTRAINT_NOTNULL as i8 {
            QueueNNConstraintValidation(wqueue, conrel, rel, tuple, recurse, recursing, lockmode);
        }

        ObjectAddressSet!(address, ConstraintRelationId, (*con).oid);
    } else {
        address = InvalidObjectAddress; /* already validated */
    }

    systable_endscan(scan);
    table_close(conrel, RowExclusiveLock);

    address
}

/*
 * QueueFKConstraintValidation
 *   Add an entry to wqueue to validate the given FK constraint in Phase 3.
 */
unsafe fn QueueFKConstraintValidation(
    wqueue: *mut *mut List,
    conrel: Relation,
    fkrel: Relation,
    pkrelid: Oid,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
) {
    let con = GETSTRUCT(contuple) as Form_pg_constraint;
    Assert!((*con).contype == CONSTRAINT_FOREIGN as i8);
    Assert!(!(*con).convalidated);

    /*
     * Add the validation to phase 3's queue; not needed for partitioned
     * tables themselves, only for their partitions.
     */
    if (*(*fkrel).rd_rel).relkind == RELKIND_RELATION as i8 && (*con).confrelid == pkrelid {
        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        /* for now this is all we need */
        (*fkconstraint).conname = pstrdup(NameStr!((*con).conname) as *mut i8);

        let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
        (*newcon).name = (*fkconstraint).conname;
        (*newcon).contype = CONSTR_FOREIGN;
        (*newcon).refrelid = (*con).confrelid;
        (*newcon).refindid = (*con).conindid;
        (*newcon).conid = (*con).oid;
        (*newcon).qual = fkconstraint as *mut Node;

        let tab = ATGetQueueEntry(wqueue, fkrel);
        (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
    }

    /*
     * If the table at either end of the constraint is partitioned, recurse
     * to handle every unvalidated constraint that is a child.
     */
    if (*(*fkrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
        || get_rel_relkind((*con).confrelid) == RELKIND_PARTITIONED_TABLE as i8
    {
        let mut pkey = ScanKeyData::default();
        let pscan: SysScanDesc;
        let mut childtup: HeapTuple;

        ScanKeyInit(
            &mut pkey,
            Anum_pg_constraint_conparentid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*con).oid),
        );
        pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, std::ptr::null_mut(), 1, &mut pkey);
        loop {
            childtup = systable_getnext(pscan);
            if !HeapTupleIsValid(childtup) { break; }
            let childcon = GETSTRUCT(childtup) as Form_pg_constraint;

            /* If the child constraint has already been validated, skip it. */
            if (*childcon).convalidated { continue; }

            let childrel = table_open((*childcon).conrelid, lockmode);
            /*
             * pkrelid should be passed as-is during recursion to identify the root referenced table.
             */
            QueueFKConstraintValidation(wqueue, conrel, childrel, pkrelid, childtup, lockmode);
            table_close(childrel, NoLock);
        }
        systable_endscan(pscan);
    }

    /*
     * Now mark the pg_constraint row as validated.
     */
    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;
    (*copy_con).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*con).oid, 0);
    heap_freetuple(copy_tuple);
}

/*
 * QueueCheckConstraintValidation
 *   Add an entry to wqueue to validate the given check constraint in Phase 3.
 */
unsafe fn QueueCheckConstraintValidation(
    wqueue: *mut *mut List,
    conrel: Relation,
    rel: Relation,
    constr_name: *mut i8,
    contuple: HeapTuple,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    let con = GETSTRUCT(contuple) as Form_pg_constraint;
    Assert!((*con).contype == CONSTRAINT_CHECK as i8);

    let mut children: *mut List = std::ptr::null_mut();

    /*
     * If we're recursing, the parent has already done this.
     */
    if !recursing && !(*con).connoinherit {
        children = find_all_inheritors(RelationGetRelid(rel), lockmode, std::ptr::null_mut());
    }

    /*
     * We recurse before validating on the parent, to reduce risk of deadlocks.
     */
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childoid = lfirst_oid(child_lc);
        if childoid == RelationGetRelid(rel) {
            child_lc = lnext(children, child_lc);
            continue;
        }

        if !recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("constraint must be validated on child tables too")
            );
        }

        /* find_all_inheritors already got lock */
        let childrel = table_open(childoid, NoLock);
        ATExecValidateConstraint(wqueue, childrel, constr_name, false, true, lockmode);
        table_close(childrel, NoLock);
        child_lc = lnext(children, child_lc);
    }

    /* Queue validation for phase 3 */
    let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
    (*newcon).name = constr_name;
    (*newcon).contype = CONSTR_CHECK;
    (*newcon).refrelid = InvalidOid;
    (*newcon).refindid = InvalidOid;
    (*newcon).conid = (*con).oid;

    let val = SysCacheGetAttrNotNull(CONSTROID, contuple, Anum_pg_constraint_conbin);
    let conbin = TextDatumGetCString(val);
    (*newcon).qual = expand_generated_columns_in_expr(stringToNode(conbin), rel, 1);

    let tab = ATGetQueueEntry(wqueue, rel);
    (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);

    /* Invalidate relcache */
    CacheInvalidateRelcache(rel);

    /* Update catalog */
    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;
    (*copy_con).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*con).oid, 0);
    heap_freetuple(copy_tuple);
}

/*
 * QueueNNConstraintValidation
 *   Add an entry to wqueue to validate the given not-null constraint in Phase 3.
 */
unsafe fn QueueNNConstraintValidation(
    wqueue: *mut *mut List,
    conrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    let con = GETSTRUCT(contuple) as Form_pg_constraint;
    Assert!((*con).contype == CONSTRAINT_NOTNULL as i8);

    let attnum = extractNotNullColumn(contuple);
    let mut children: *mut List = std::ptr::null_mut();

    if !recursing && !(*con).connoinherit {
        children = find_all_inheritors(RelationGetRelid(rel), lockmode, std::ptr::null_mut());
    }

    let colname = get_attname(RelationGetRelid(rel), attnum, false);
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childoid = lfirst_oid(child_lc);
        if childoid == RelationGetRelid(rel) {
            child_lc = lnext(children, child_lc);
            continue;
        }

        if !recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("constraint must be validated on child tables too")
            );
        }

        /* The column on child might have a different attnum, search by column name. */
        let contup = findNotNullConstraint(childoid, colname);
        if contup.is_null() {
            elog!(
                ERROR,
                "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                std::ffi::CStr::from_ptr(get_rel_name(childoid)).to_string_lossy()
            );
        }
        let childcon = GETSTRUCT(contup) as Form_pg_constraint;
        if (*childcon).convalidated {
            child_lc = lnext(children, child_lc);
            continue;
        }

        /* find_all_inheritors already got lock */
        let childrel = table_open(childoid, NoLock);
        let conname = pstrdup(NameStr!((*childcon).conname) as *mut i8);
        /* XXX improve ATExecValidateConstraint API to avoid double search */
        ATExecValidateConstraint(wqueue, childrel, conname, false, true, lockmode);
        table_close(childrel, NoLock);
        child_lc = lnext(children, child_lc);
    }

    /* Set attnotnull appropriately without queueing another validation */
    set_attnotnull(std::ptr::null_mut(), rel, attnum, true, false);

    let tab = ATGetQueueEntry(wqueue, rel);
    (*tab).verify_new_notnull = true;

    /* Invalidate relcache */
    CacheInvalidateRelcache(rel);

    /* Update catalogs */
    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;
    (*copy_con).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*con).oid, 0);
    heap_freetuple(copy_tuple);
}

/*
 * transformColumnNameList - transform list of column names
 *   Lookup each name and return its attnum and, optionally, type and collation OIDs.
 */
unsafe fn transformColumnNameList(
    rel_id: Oid,
    col_list: *mut List,
    attnums: *mut i16,
    atttypids: *mut Oid,
    attcollids: *mut Oid,
) -> i32 {
    let mut attnum: i32 = 0;
    let mut lc = list_head(col_list);
    while !lc.is_null() {
        let attname = strVal(lfirst(lc)) as *mut i8;
        let atttuple = SearchSysCacheAttName(rel_id, attname);
        if !HeapTupleIsValid(atttuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" referenced in foreign key constraint does not exist",
                    std::ffi::CStr::from_ptr(attname).to_string_lossy()
                )
            );
        }
        let attform = GETSTRUCT(atttuple) as Form_pg_attribute;
        if (*attform).attnum < 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!("system columns cannot be used in foreign keys")
            );
        }
        if attnum >= INDEX_MAX_KEYS as i32 {
            ereport!(
                ERROR,
                errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg!("cannot have more than {} keys in a foreign key", INDEX_MAX_KEYS)
            );
        }
        *attnums.add(attnum as usize) = (*attform).attnum;
        if !atttypids.is_null() {
            *atttypids.add(attnum as usize) = (*attform).atttypid;
        }
        if !attcollids.is_null() {
            *attcollids.add(attnum as usize) = (*attform).attcollation;
        }
        ReleaseSysCache(atttuple);
        attnum += 1;
        lc = lnext(col_list, lc);
    }
    attnum
}

/*
 * transformFkeyGetPrimaryKey -
 *   Look up the names, attnums, types, and collations of the primary key
 *   attributes for the pkrel.
 */
unsafe fn transformFkeyGetPrimaryKey(
    pkrel: Relation,
    index_oid: *mut Oid,
    attnamelist: *mut *mut List,
    attnums: *mut i16,
    atttypids: *mut Oid,
    attcollids: *mut Oid,
    opclasses: *mut Oid,
    pk_has_without_overlaps: *mut bool,
) -> i32 {
    let mut index_tuple: HeapTuple = std::ptr::null_mut();
    let mut index_struct: Form_pg_index = std::ptr::null_mut();

    *index_oid = InvalidOid;

    let indexoidlist = RelationGetIndexList(pkrel);
    let mut scan_lc = list_head(indexoidlist);
    while !scan_lc.is_null() {
        let indexoid = lfirst_oid(scan_lc);
        index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if !HeapTupleIsValid(index_tuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexoid);
        }
        index_struct = GETSTRUCT(index_tuple) as Form_pg_index;
        if (*index_struct).indisprimary && (*index_struct).indisvalid {
            if !(*index_struct).indimmediate {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg!(
                        "cannot use a deferrable primary key for referenced table \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
                    )
                );
            }
            *index_oid = indexoid;
            break;
        }
        ReleaseSysCache(index_tuple);
        index_tuple = std::ptr::null_mut();
        scan_lc = lnext(indexoidlist, scan_lc);
    }

    list_free(indexoidlist);

    if !OidIsValid(*index_oid) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg!(
                "there is no primary key for referenced table \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    /* Must get indclass the hard way */
    let indclass_datum = SysCacheGetAttrNotNull(INDEXRELID, index_tuple, Anum_pg_index_indclass);
    let indclass = DatumGetPointer(indclass_datum) as *mut oidvector;

    /* Build the list of PK attributes from indkey */
    *attnamelist = std::ptr::null_mut();
    let mut i = 0;
    while i < (*index_struct).indnkeyatts as usize {
        let pkattno = (*index_struct).indkey.values[i];
        *attnums.add(i) = pkattno as i16;
        *atttypids.add(i) = attnumTypeId(pkrel, pkattno as i32);
        *attcollids.add(i) = attnumCollationId(pkrel, pkattno as i32);
        *opclasses.add(i) = (*indclass).values[i];
        *attnamelist = lappend(
            *attnamelist,
            makeString(pstrdup(NameStr!(*attnumAttName(pkrel, pkattno as i32)) as *mut i8)) as *mut _,
        );
        i += 1;
    }

    *pk_has_without_overlaps = (*index_struct).indisexclusion;
    ReleaseSysCache(index_tuple);

    i as i32
}

/*
 * transformFkeyCheckAttrs -
 *   Validate that the 'attnums' columns in the 'pkrel' relation are valid to
 *   reference as part of a foreign key constraint.
 */
unsafe fn transformFkeyCheckAttrs(
    pkrel: Relation,
    numattrs: i32,
    attnums: *mut i16,
    with_period: bool,
    opclasses: *mut Oid,
    pk_has_without_overlaps: *mut bool,
) -> Oid {
    let mut indexoid = InvalidOid;
    let mut found = false;
    let mut found_deferrable = false;

    /* Reject duplicate appearances of columns */
    for i in 0..numattrs as usize {
        for j in (i + 1)..numattrs as usize {
            if *attnums.add(i) == *attnums.add(j) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_FOREIGN_KEY),
                    errmsg!("foreign key referenced-columns list must not contain duplicates")
                );
            }
        }
    }

    let indexoidlist = RelationGetIndexList(pkrel);
    let mut scan_lc = list_head(indexoidlist);
    while !scan_lc.is_null() {
        let index_tuple: HeapTuple;
        let index_struct: Form_pg_index;

        indexoid = lfirst_oid(scan_lc);
        index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if !HeapTupleIsValid(index_tuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexoid);
        }
        index_struct = GETSTRUCT(index_tuple) as Form_pg_index;

        /*
         * Must have the right number of columns; must be unique (or exclusion for temporal)
         * and not a partial index; forget it if there are any expressions.
         */
        if (*index_struct).indnkeyatts == numattrs as i16
            && (if with_period { (*index_struct).indisexclusion } else { (*index_struct).indisunique })
            && (*index_struct).indisvalid
            && heap_attisnull(index_tuple, Anum_pg_index_indpred, std::ptr::null_mut())
            && heap_attisnull(index_tuple, Anum_pg_index_indexprs, std::ptr::null_mut())
        {
            let indclass_datum = SysCacheGetAttrNotNull(INDEXRELID, index_tuple, Anum_pg_index_indclass);
            let indclass = DatumGetPointer(indclass_datum) as *mut oidvector;

            /* Check for a match (columns may appear in different order) */
            'outer: {
                for i in 0..numattrs as usize {
                    found = false;
                    for j in 0..numattrs as usize {
                        if *attnums.add(i) == (*index_struct).indkey.values[j] as i16 {
                            *opclasses.add(i) = (*indclass).values[j];
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        break 'outer;
                    }
                }
                /* The last attribute must be the PERIOD FK part for temporal FKs */
                if found && with_period {
                    let period_attnum = *attnums.add(numattrs as usize - 1);
                    found = period_attnum == (*index_struct).indkey.values[numattrs as usize - 1] as i16;
                }
                /* Refuse deferrable unique/primary key */
                if found && !(*index_struct).indimmediate {
                    found_deferrable = true;
                    found = false;
                }
                /* Record whether index has WITHOUT OVERLAPS */
                if found {
                    *pk_has_without_overlaps = (*index_struct).indisexclusion;
                }
            }
        }
        ReleaseSysCache(index_tuple);
        if found { break; }
        scan_lc = lnext(indexoidlist, scan_lc);
    }

    if !found {
        if found_deferrable {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "cannot use a deferrable unique constraint for referenced table \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg!(
                    "there is no unique constraint matching given keys for referenced table \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
                )
            );
        }
    }

    list_free(indexoidlist);
    indexoid
}

/*
 * findFkeyCast -
 *   Wrapper around find_coercion_pathway() for ATAddForeignKeyConstraint().
 */
unsafe fn findFkeyCast(target_type_id: Oid, source_type_id: Oid, funcid: *mut Oid) -> CoercionPathType {
    let ret: CoercionPathType;
    if target_type_id == source_type_id {
        ret = COERCION_PATH_RELABELTYPE;
        *funcid = InvalidOid;
    } else {
        ret = find_coercion_pathway(target_type_id, source_type_id, COERCION_IMPLICIT, funcid);
        if ret == COERCION_PATH_NONE {
            /* A previously-relied-upon cast is now gone. */
            elog!(ERROR, "could not find cast from {} to {}", source_type_id, target_type_id);
        }
    }
    ret
}

/*
 * checkFkeyPermissions
 *   Permissions checks on the referenced table for ADD FOREIGN KEY.
 */
unsafe fn checkFkeyPermissions(rel: Relation, attnums: *mut i16, natts: i32) {
    let roleid = GetUserId();
    let aclresult = pg_class_aclcheck(RelationGetRelid(rel), roleid, ACL_REFERENCES);
    if aclresult == ACLCHECK_OK {
        return;
    }
    /* Else we must have REFERENCES on each column */
    for i in 0..natts as usize {
        let aclresult = pg_attribute_aclcheck(RelationGetRelid(rel), *attnums.add(i), roleid, ACL_REFERENCES);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, get_relkind_objtype((*(*rel).rd_rel).relkind), RelationGetRelationName(rel));
        }
    }
}

/*
 * validateForeignKeyConstraint
 *   Scan the existing rows in a table to verify they meet a proposed FK constraint.
 */
unsafe fn validateForeignKeyConstraint(
    conname: *mut i8,
    rel: Relation,
    pkrel: Relation,
    pkind_oid: Oid,
    constraint_oid: Oid,
    hasperiod: bool,
) {
    let mut slot: *mut TupleTableSlot;
    let scan: TableScanDesc;
    let mut trig: Trigger = core::mem::zeroed();
    let snapshot: Snapshot;
    let oldcxt: MemoryContext;
    let per_tup_cxt: MemoryContext;

    ereport!(
        DEBUG1,
        errmsg_internal!("validating foreign key constraint \"{}\"", std::ffi::CStr::from_ptr(conname).to_string_lossy())
    );

    /* Build a trigger call structure */
    trig.tgoid = InvalidOid;
    trig.tgname = conname;
    trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
    trig.tgisinternal = true;
    trig.tgconstrrelid = RelationGetRelid(pkrel);
    trig.tgconstrindid = pkind_oid;
    trig.tgconstraint = constraint_oid;
    trig.tgdeferrable = false;
    trig.tginitdeferred = false;
    /* we needn't fill in remaining fields */

    /*
     * See if we can do it with a single LEFT JOIN query.
     */
    if !hasperiod && RI_Initial_Check(&mut trig, rel, pkrel) {
        return;
    }

    /*
     * Scan through each tuple, calling RI_FKey_check_ins as if it had just been inserted.
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());
    slot = table_slot_create(rel, std::ptr::null_mut());
    scan = table_beginscan(rel, snapshot, 0, std::ptr::null_mut());

    per_tup_cxt = AllocSetContextCreate(
        CurrentMemoryContext,
        b"validateForeignKeyConstraint\0".as_ptr() as *const i8,
        ALLOCSET_SMALL_SIZES,
    );
    oldcxt = MemoryContextSwitchTo(per_tup_cxt);

    while table_scan_getnextslot(scan, ForwardScanDirection, slot) {
        let fcinfo = LOCAL_FCINFO!(0);
        let mut trigdata: TriggerData = core::mem::zeroed();

        CHECK_FOR_INTERRUPTS!();

        /* Make a call to the trigger function. No parameters are passed. */
        core::ptr::write_bytes(fcinfo, 0, SizeForFunctionCallInfo(0));

        /* We assume RI_FKey_check_ins won't look at flinfo... */
        trigdata.r#type = T_TriggerData;
        trigdata.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW;
        trigdata.tg_relation = rel;
        trigdata.tg_trigtuple = ExecFetchSlotHeapTuple(slot, false, std::ptr::null_mut());
        trigdata.tg_trigslot = slot;
        trigdata.tg_trigger = &mut trig;

        (*fcinfo).context = &mut trigdata as *mut TriggerData as *mut Node;

        RI_FKey_check_ins(fcinfo);

        MemoryContextReset(per_tup_cxt);
    }

    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(per_tup_cxt);
    table_endscan(scan);
    UnregisterSnapshot(snapshot);
    ExecDropSingleTupleTableSlot(slot);
}

/*
 * CreateFKCheckTrigger
 *   Creates the insert (on_insert=true) or update "check" trigger that
 *   implements a given foreign key. Returns the OID of the created trigger.
 */
unsafe fn CreateFKCheckTrigger(
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: *mut Constraint,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_trig_oid: Oid,
    on_insert: bool,
) -> Oid {
    let trig_address: ObjectAddress;
    let fk_trigger = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;

    /*
     * Note: for a self-referential FK, action triggers fire before check triggers,
     * using names RI_ConstraintTrigger_a_NNNN and RI_ConstraintTrigger_c_NNNN.
     */
    (*fk_trigger).replace = false;
    (*fk_trigger).isconstraint = true;
    (*fk_trigger).trigname = b"RI_ConstraintTrigger_c\0".as_ptr() as *mut i8;
    (*fk_trigger).relation = std::ptr::null_mut();

    /* Either ON INSERT or ON UPDATE */
    if on_insert {
        (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_check_ins\0".as_ptr() as *mut i8);
        (*fk_trigger).events = TRIGGER_TYPE_INSERT;
    } else {
        (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_check_upd\0".as_ptr() as *mut i8);
        (*fk_trigger).events = TRIGGER_TYPE_UPDATE;
    }

    (*fk_trigger).args = std::ptr::null_mut();
    (*fk_trigger).row = true;
    (*fk_trigger).timing = TRIGGER_TYPE_AFTER;
    (*fk_trigger).columns = std::ptr::null_mut();
    (*fk_trigger).whenClause = std::ptr::null_mut();
    (*fk_trigger).transitionRels = std::ptr::null_mut();
    (*fk_trigger).deferrable = (*fkconstraint).deferrable;
    (*fk_trigger).initdeferred = (*fkconstraint).initdeferred;
    (*fk_trigger).constrrel = std::ptr::null_mut();

    trig_address = CreateTrigger(
        fk_trigger,
        std::ptr::null_mut(),
        my_rel_oid,
        ref_rel_oid,
        constraint_oid,
        index_oid,
        InvalidOid,
        parent_trig_oid,
        std::ptr::null_mut(),
        true,
        false,
    );

    /* Make changes-so-far visible */
    CommandCounterIncrement();

    trig_address.objectId
}

/*
 * createForeignKeyActionTriggers
 *   Create the referenced-side "action" triggers that implement a foreign key.
 *   Returns OIDs in *deleteTrigOid and *updateTrigOid.
 */
unsafe fn createForeignKeyActionTriggers(
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: *mut Constraint,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    delete_trig_oid: *mut Oid,
    update_trig_oid: *mut Oid,
) {
    let fk_trigger: *mut CreateTrigStmt;
    let trig_address: ObjectAddress;

    /* Build and execute CREATE CONSTRAINT TRIGGER for ON DELETE action */
    fk_trigger = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;
    (*fk_trigger).replace = false;
    (*fk_trigger).isconstraint = true;
    (*fk_trigger).trigname = b"RI_ConstraintTrigger_a\0".as_ptr() as *mut i8;
    (*fk_trigger).relation = std::ptr::null_mut();
    (*fk_trigger).args = std::ptr::null_mut();
    (*fk_trigger).row = true;
    (*fk_trigger).timing = TRIGGER_TYPE_AFTER;
    (*fk_trigger).events = TRIGGER_TYPE_DELETE;
    (*fk_trigger).columns = std::ptr::null_mut();
    (*fk_trigger).whenClause = std::ptr::null_mut();
    (*fk_trigger).transitionRels = std::ptr::null_mut();
    (*fk_trigger).constrrel = std::ptr::null_mut();

    match (*fkconstraint).fk_del_action as i32 {
        FKCONSTR_ACTION_NOACTION => {
            (*fk_trigger).deferrable = (*fkconstraint).deferrable;
            (*fk_trigger).initdeferred = (*fkconstraint).initdeferred;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_noaction_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_RESTRICT => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_restrict_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_CASCADE => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_cascade_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETNULL => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_setnull_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETDEFAULT => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_setdefault_del\0".as_ptr() as *mut i8);
        }
        _ => {
            elog!(ERROR, "unrecognized FK action type: {}", (*fkconstraint).fk_del_action as i32);
        }
    }

    trig_address = CreateTrigger(
        fk_trigger, std::ptr::null_mut(), ref_rel_oid, my_rel_oid,
        constraint_oid, index_oid, InvalidOid,
        parent_del_trigger, std::ptr::null_mut(), true, false,
    );
    if !delete_trig_oid.is_null() {
        *delete_trig_oid = trig_address.objectId;
    }

    /* Make changes-so-far visible */
    CommandCounterIncrement();

    /* Build and execute CREATE CONSTRAINT TRIGGER for ON UPDATE action */
    let fk_trigger2 = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;
    (*fk_trigger2).replace = false;
    (*fk_trigger2).isconstraint = true;
    (*fk_trigger2).trigname = b"RI_ConstraintTrigger_a\0".as_ptr() as *mut i8;
    (*fk_trigger2).relation = std::ptr::null_mut();
    (*fk_trigger2).args = std::ptr::null_mut();
    (*fk_trigger2).row = true;
    (*fk_trigger2).timing = TRIGGER_TYPE_AFTER;
    (*fk_trigger2).events = TRIGGER_TYPE_UPDATE;
    (*fk_trigger2).columns = std::ptr::null_mut();
    (*fk_trigger2).whenClause = std::ptr::null_mut();
    (*fk_trigger2).transitionRels = std::ptr::null_mut();
    (*fk_trigger2).constrrel = std::ptr::null_mut();

    match (*fkconstraint).fk_upd_action as i32 {
        FKCONSTR_ACTION_NOACTION => {
            (*fk_trigger2).deferrable = (*fkconstraint).deferrable;
            (*fk_trigger2).initdeferred = (*fkconstraint).initdeferred;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_noaction_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_RESTRICT => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_restrict_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_CASCADE => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_cascade_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETNULL => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_setnull_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETDEFAULT => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_setdefault_upd\0".as_ptr() as *mut i8);
        }
        _ => {
            elog!(ERROR, "unrecognized FK action type: {}", (*fkconstraint).fk_upd_action as i32);
        }
    }

    let trig_address2 = CreateTrigger(
        fk_trigger2, std::ptr::null_mut(), ref_rel_oid, my_rel_oid,
        constraint_oid, index_oid, InvalidOid,
        parent_upd_trigger, std::ptr::null_mut(), true, false,
    );
    if !update_trig_oid.is_null() {
        *update_trig_oid = trig_address2.objectId;
    }
}

/*
 * createForeignKeyCheckTriggers
 *   Create the referencing-side "check" triggers that implement a foreign key.
 */
unsafe fn createForeignKeyCheckTriggers(
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: *mut Constraint,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    insert_trig_oid: *mut Oid,
    update_trig_oid: *mut Oid,
) {
    *insert_trig_oid = CreateFKCheckTrigger(
        my_rel_oid, ref_rel_oid, fkconstraint, constraint_oid, index_oid, parent_ins_trigger, true,
    );
    *update_trig_oid = CreateFKCheckTrigger(
        my_rel_oid, ref_rel_oid, fkconstraint, constraint_oid, index_oid, parent_upd_trigger, false,
    );
}

/*
 * ALTER TABLE DROP CONSTRAINT
 *
 * Like DROP COLUMN, we can't use the normal ALTER TABLE recursion mechanism.
 */
unsafe fn ATExecDropConstraint(
    rel: Relation,
    constr_name: *const i8,
    behavior: DropBehavior,
    recurse: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) {
    let conrel: Relation;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = std::mem::zeroed();
    let tuple: *mut HeapTupleData;
    let mut found = false;

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Find and drop the target constraint */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname as i16,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(constr_name as *mut i8),
    );
    scan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        std::ptr::null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    tuple = systable_getnext(scan);
    if HeapTupleIsValid(tuple) {
        dropconstraint_internal(rel, tuple, behavior, recurse, false, missing_ok, lockmode);
        found = true;
    }

    systable_endscan(scan);

    if !found {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("constraint \"{}\" of relation \"{}\" does not exist", /* C also: constrName, RelationGetRelationName(rel) */
                    CStr::from_ptr(constr_name).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        } else {
            ereport!(
                NOTICE,
                errmsg("constraint \"{}\" of relation \"{}\" does not exist, skipping",
                    CStr::from_ptr(constr_name).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        }
    }

    table_close(conrel, RowExclusiveLock);
}

/*
 * Remove a constraint, using its pg_constraint tuple
 *
 * Implementation for ALTER TABLE DROP CONSTRAINT and ALTER TABLE ALTER COLUMN
 * DROP NOT NULL.
 *
 * Returns the address of the constraint being removed.
 */
unsafe fn dropconstraint_internal(
    rel: Relation,
    constraint_tup: *mut HeapTupleData,
    behavior: DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let conrel: Relation;
    let con: Form_pg_constraint;
    let mut conobj: ObjectAddress = std::mem::zeroed();
    let children: *mut List;
    let mut is_no_inherit_constraint = false;
    let constr_name: *mut i8;
    let mut colname: *mut i8 = std::ptr::null_mut();

    /* Guard against stack overflow due to overly deep inheritance tree. */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_DropConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    con = GETSTRUCT(constraint_tup) as Form_pg_constraint;
    constr_name = NameStr((*con).conname) as *mut i8;

    /* Don't allow drop of inherited constraints */
    if (*con).coninhcount > 0 && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("cannot drop inherited constraint \"{}\" of relation \"{}\"",
                CStr::from_ptr(constr_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
        );
    }

    /*
     * Reset pg_constraint.attnotnull, if this is a not-null constraint.
     *
     * While doing that, we're in a good position to disallow dropping a not-
     * null constraint underneath a primary key, a replica identity index, or
     * a generated identity column.
     */
    if (*con).contype == CONSTRAINT_NOTNULL as i8 {
        let attrel: Relation = table_open(AttributeRelationId, RowExclusiveLock);
        let attnum: AttrNumber = extractNotNullColumn(constraint_tup);
        let mut pkattrs: *mut Bitmapset;
        let irattrs: *mut Bitmapset;
        let atttup: *mut HeapTupleData;
        let att_form: Form_pg_attribute;

        /* save column name for recursion step */
        colname = get_attname(RelationGetRelid(rel), attnum, false);

        /*
         * Disallow if it's in the primary key.  For partitioned tables we
         * cannot rely solely on RelationGetIndexAttrBitmap, because it'll
         * return NULL if the primary key is invalid; but we still need to
         * protect not-null constraints under such a constraint, so check the
         * slow way.
         */
        pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

        if pkattrs.is_null() && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
            let pkindex: Oid = RelationGetPrimaryKeyIndex(rel, true);
            if OidIsValid(pkindex) {
                let pk: Relation = relation_open(pkindex, AccessShareLock);
                pkattrs = std::ptr::null_mut();
                for i in 0..(*(*pk).rd_index).indnkeyatts as usize {
                    pkattrs = bms_add_member(
                        pkattrs,
                        (*(*pk).rd_index).indkey.values[i] - FirstLowInvalidHeapAttributeNumber,
                    );
                }
                relation_close(pk, AccessShareLock);
            }
        }

        if !pkattrs.is_null()
            && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs)
        {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("column \"{}\" is in a primary key",
                    CStr::from_ptr(get_attname(RelationGetRelid(rel), attnum, false)).to_string_lossy())
            );
        }

        /* Disallow if it's in the replica identity */
        irattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
        if bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, irattrs) {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("column \"{}\" is in index used as replica identity",
                    CStr::from_ptr(get_attname(RelationGetRelid(rel), attnum, false)).to_string_lossy())
            );
        }

        /* Disallow if it's a GENERATED AS IDENTITY column */
        atttup = SearchSysCacheCopyAttNum(RelationGetRelid(rel), attnum);
        if !HeapTupleIsValid(atttup) {
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                attnum,
                RelationGetRelid(rel)
            );
        }
        att_form = GETSTRUCT(atttup) as Form_pg_attribute;
        if (*att_form).attidentity != 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("column \"{}\" of relation \"{}\" is an identity column",
                    CStr::from_ptr(get_attname(RelationGetRelid(rel), attnum, false)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        }

        /* All good -- reset attnotnull if needed */
        if (*att_form).attnotnull {
            (*att_form).attnotnull = false;
            CatalogTupleUpdate(attrel, &mut (*atttup).t_self, atttup);
        }

        table_close(attrel, RowExclusiveLock);
    }

    is_no_inherit_constraint = (*con).connoinherit;

    /*
     * If it's a foreign-key constraint, we'd better lock the referenced table
     * and check that that's not in use, just as we've already done for the
     * constrained table (else we might, eg, be dropping a trigger that has
     * unfired events).  But we can/must skip that in the self-referential case.
     */
    if (*con).contype == CONSTRAINT_FOREIGN as i8
        && (*con).confrelid != RelationGetRelid(rel)
    {
        let frel: Relation;
        /* Must match lock taken by RemoveTriggerById: */
        frel = table_open((*con).confrelid, AccessExclusiveLock);
        CheckAlterTableIsSafe(frel);
        table_close(frel, NoLock);
    }

    /* Perform the actual constraint deletion */
    ObjectAddressSet(&mut conobj, ConstraintRelationId, (*con).oid);
    performDeletion(&conobj, behavior, 0);

    /*
     * For partitioned tables, non-CHECK, non-NOT-NULL inherited constraints
     * are dropped via the dependency mechanism, so we're done here.
     */
    if (*con).contype != CONSTRAINT_CHECK as i8
        && (*con).contype != CONSTRAINT_NOTNULL as i8
        && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
    {
        table_close(conrel, RowExclusiveLock);
        return conobj;
    }

    /*
     * Propagate to children as appropriate.  Unlike most other ALTER
     * routines, we have to do this one level of recursion at a time; we can't
     * use find_all_inheritors to do it in one pass.
     */
    if !is_no_inherit_constraint {
        children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    } else {
        children = NIL;
    }

    foreach_oid!(childrelid, children, {
        let childrel: Relation;
        let tuple: *mut HeapTupleData;
        let childcon: Form_pg_constraint;

        /* find_inheritance_children already got lock */
        childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        /*
         * We search for not-null constraints by column name, and others by
         * constraint name.
         */
        if (*con).contype == CONSTRAINT_NOTNULL as i8 {
            tuple = findNotNullConstraint(childrelid, colname);
            if !HeapTupleIsValid(tuple) {
                elog!(
                    ERROR,
                    "cache lookup failed for not-null constraint on column \"{}\" of relation {}",
                    CStr::from_ptr(colname).to_string_lossy(),
                    RelationGetRelid(childrel)
                );
            }
        } else {
            let scan: SysScanDesc;
            let mut skey: [ScanKeyData; 3] = std::mem::zeroed();

            ScanKeyInit(
                &mut skey[0],
                Anum_pg_constraint_conrelid as i16,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(childrelid),
            );
            ScanKeyInit(
                &mut skey[1],
                Anum_pg_constraint_contypid as i16,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(InvalidOid),
            );
            ScanKeyInit(
                &mut skey[2],
                Anum_pg_constraint_conname as i16,
                BTEqualStrategyNumber,
                F_NAMEEQ,
                CStringGetDatum(constr_name),
            );
            scan = systable_beginscan(
                conrel,
                ConstraintRelidTypidNameIndexId,
                true,
                std::ptr::null_mut(),
                3,
                skey.as_mut_ptr(),
            );
            /* There can only be one, so no need to loop */
            tuple = systable_getnext(scan);
            if !HeapTupleIsValid(tuple) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("constraint \"{}\" of relation \"{}\" does not exist",
                        CStr::from_ptr(constr_name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(childrel)).to_string_lossy())
                );
            }
            let tuple = heap_copytuple(tuple);
            systable_endscan(scan);
            // use heap_copytuple result
            let _ = tuple;
        }

        childcon = GETSTRUCT(tuple) as Form_pg_constraint;

        /* Right now only CHECK and not-null constraints can be inherited */
        if (*childcon).contype != CONSTRAINT_CHECK as i8
            && (*childcon).contype != CONSTRAINT_NOTNULL as i8
        {
            elog!(ERROR, "inherited constraint is not a CHECK or not-null constraint");
        }

        if (*childcon).coninhcount <= 0 {
            /* shouldn't happen */
            elog!(
                ERROR,
                "relation {} has non-inherited constraint \"{}\"",
                childrelid,
                CStr::from_ptr(NameStr((*childcon).conname) as *const i8).to_string_lossy()
            );
        }

        if recurse {
            /*
             * If the child constraint has other definition sources, just
             * decrement its inheritance count; if not, recurse to delete it.
             */
            if (*childcon).coninhcount == 1 && !(*childcon).conislocal {
                /* Time to delete this child constraint, too */
                dropconstraint_internal(
                    childrel, tuple, behavior, recurse, true, missing_ok, lockmode,
                );
            } else {
                /* Child constraint must survive my deletion */
                (*childcon).coninhcount -= 1;
                CatalogTupleUpdate(conrel, &mut (*tuple).t_self, tuple);
                /* Make update visible */
                CommandCounterIncrement();
            }
        } else {
            /*
             * If we were told to drop ONLY in this table (no recursion) and
             * there are no further parents for this constraint, we need to
             * mark the inheritors' constraints as locally defined rather than
             * inherited.
             */
            (*childcon).coninhcount -= 1;
            if (*childcon).coninhcount == 0 {
                (*childcon).conislocal = true;
            }
            CatalogTupleUpdate(conrel, &mut (*tuple).t_self, tuple);
            /* Make update visible */
            CommandCounterIncrement();
        }

        heap_freetuple(tuple);

        table_close(childrel, NoLock);
    });

    table_close(conrel, RowExclusiveLock);

    conobj
}

/*
 * ALTER COLUMN TYPE
 *
 * Unlike other subcommand types, we do parse transformation for ALTER COLUMN
 * TYPE during phase 1 --- the AlterTableCmd passed in here is already
 * transformed (and must be, because we rely on some transformed fields).
 *
 * The point of this is that the execution of all ALTER COLUMN TYPEs for a
 * table will be done "in parallel" during phase 3, so all the USING
 * expressions should be parsed assuming the original column types.  Also,
 * this allows a USING expression to refer to a field that will be dropped.
 *
 * To make this work safely, AT_PASS_DROP then AT_PASS_ALTER_TYPE must be
 * the first two execution steps in phase 2; they must not see the effects
 * of any other subcommand types, since the USING expressions are parsed
 * against the unmodified table's state.
 */
unsafe fn ATPrepAlterColumnType(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    recurse: bool,
    recursing: bool,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    let col_name: *mut i8 = (*cmd).name;
    let def: *mut ColumnDef = (*cmd).def as *mut ColumnDef;
    let type_name: *mut TypeName = (*def).typeName;
    let mut transform: *mut Node = (*def).cooked_default as *mut Node;
    let tuple: *mut HeapTupleData;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut targettype: Oid = InvalidOid;
    let mut targettypmod: i32 = 0;
    let targetcollid: Oid;
    let newval: *mut NewColumnValue;
    let pstate: *mut ParseState = make_parsestate(std::ptr::null_mut());
    let aclresult: AclResult;
    let mut is_expr: bool = false;

    (*pstate).p_sourcetext = (*context).queryString;

    if (*(*rel).rd_rel).reloftype && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("cannot alter column type of typed table"),
            parser_errposition(pstate, (*def).location)
        );
    }

    /* lookup the attribute so we can check inheritance status */
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg("column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }
    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    /* Can't alter a system attribute */
    if attnum <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot alter system column \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /*
     * Cannot specify USING when altering type of a generated column, because
     * that would violate the generation expression.
     */
    if (*att_tup).attgenerated != 0 && !(*def).cooked_default.is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
            errmsg("cannot specify USING when altering type of generated column"),
            errdetail("Column \"{}\" is a generated column.",
                CStr::from_ptr(col_name).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /*
     * Don't alter inherited columns.  At outer level, there had better not be
     * any inherited definition; when recursing, we assume this was checked at
     * the parent level (see below).
     */
    if (*att_tup).attinhcount > 0 && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("cannot alter inherited column \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /* Don't alter columns used in the partition key */
    if has_partition_attrs(
        rel,
        bms_make_singleton(attnum as i32 - FirstLowInvalidHeapAttributeNumber),
        &mut is_expr,
    ) {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("cannot alter column \"{}\" because it is part of the partition key of relation \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /* Look up the target type */
    typenameTypeIdAndMod(pstate, type_name, &mut targettype, &mut targettypmod);

    aclresult = object_aclcheck(TypeRelationId, targettype, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, targettype);
    }

    /* And the collation */
    targetcollid = GetColumnDefCollation(pstate, def, targettype);

    /* make sure datatype is legal for a column */
    CheckAttributeType(
        col_name,
        targettype,
        targetcollid,
        list_make1_oid((*(*rel).rd_rel).reltype),
        if (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            CHKATYPE_IS_VIRTUAL
        } else {
            0
        },
    );

    if (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
        /* do nothing */
    } else if (*tab).relkind == RELKIND_RELATION as i8
        || (*tab).relkind == RELKIND_PARTITIONED_TABLE as i8
    {
        /*
         * Set up an expression to transform the old data value to the new
         * type. If a USING option was given, use the expression as
         * transformed by transformAlterTableStmt, else just take the old
         * value and try to coerce it.  We do this first so that type
         * incompatibility can be detected before we waste effort, and because
         * we need the expression to be parsed against the original table row
         * type.
         */
        if transform.is_null() {
            transform = makeVar(
                1,
                attnum,
                (*att_tup).atttypid,
                (*att_tup).atttypmod,
                (*att_tup).attcollation,
                0,
            ) as *mut Node;
        }

        transform = coerce_to_target_type(
            pstate,
            transform,
            exprType(transform),
            targettype,
            targettypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if transform.is_null() {
            /* error text depends on whether USING was specified or not */
            if !(*def).cooked_default.is_null() {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("result of USING clause for column \"{}\" cannot be cast automatically to type {}",
                        /* C also: colName, format_type_be(targettype) */
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()),
                    errhint("You might need to add an explicit cast.")
                );
            } else {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("column \"{}\" cannot be cast automatically to type {}",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()),
                    // translator: USING is SQL, don't translate it
                    if (*att_tup).attgenerated == 0 {
                        errhint("You might need to specify \"USING {}::{}\".",
                            CStr::from_ptr(quote_identifier(col_name)).to_string_lossy(),
                            CStr::from_ptr(format_type_with_typemod(targettype, targettypmod)).to_string_lossy())
                    } else { 0 }
                );
            }
        }

        /* Fix collations after all else */
        assign_expr_collations(pstate, transform);

        /* Expand virtual generated columns in the expr. */
        transform = expand_generated_columns_in_expr(transform, rel, 1);

        /* Plan the expr now so we can accurately assess the need to rewrite. */
        transform = expression_planner(transform as *mut Expr) as *mut Node;

        /*
         * Add a work queue item to make ATRewriteTable update the column
         * contents.
         */
        newval = palloc0(std::mem::size_of::<NewColumnValue>()) as *mut NewColumnValue;
        (*newval).attnum = attnum;
        (*newval).expr = transform as *mut Expr;
        (*newval).is_generated = false;

        (*tab).newvals = lappend((*tab).newvals, newval as *mut std::ffi::c_void);
        if ATColumnChangeRequiresRewrite(transform, attnum) {
            (*tab).rewrite |= AT_REWRITE_COLUMN_REWRITE;
        }
    } else if !transform.is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("\"{}\" is not a table",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
        );
    }

    if !RELKIND_HAS_STORAGE((*tab).relkind)
        || (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8
    {
        /*
         * For relations or columns without storage, do this check now.
         * Regular tables will check it later when the table is being rewritten.
         */
        find_composite_type_dependencies((*(*rel).rd_rel).reltype, rel, std::ptr::null_mut());
    }

    ReleaseSysCache(tuple);

    /*
     * Recurse manually by queueing a new command for each child, if
     * necessary. We cannot apply ATSimpleRecursion here because we need to
     * remap attribute numbers in the USING expression, if any.
     *
     * If we are told not to recurse, there had better not be any child
     * tables; else the alter would put them out of step.
     */
    if recurse {
        let relid: Oid = RelationGetRelid(rel);
        let child_oids: *mut List;
        let child_numparents: *mut List;

        child_oids = find_all_inheritors(relid, lockmode, &mut child_numparents);

        /*
         * find_all_inheritors does the recursive search of the inheritance
         * hierarchy, so all we have to do is process all of the relids in the
         * list that it returns.
         */
        let mut lo: *mut ListCell = list_head(child_oids);
        let mut li: *mut ListCell = list_head(child_numparents);
        while !lo.is_null() {
            let childrelid: Oid = lfirst_oid(lo);
            let numparents: i32 = lfirst_int(li);
            let childrel: Relation;
            let childtuple: *mut HeapTupleData;
            let childatt_tup: Form_pg_attribute;
            let mut cmd = cmd; // rebind for possible copy

            if childrelid == relid {
                lo = lnext(child_oids, lo);
                li = lnext(child_numparents, li);
                continue;
            }

            /* find_all_inheritors already got lock */
            childrel = relation_open(childrelid, NoLock);
            CheckAlterTableIsSafe(childrel);

            /*
             * Verify that the child doesn't have any inherited definitions of
             * this column that came from outside this inheritance hierarchy.
             * (renameatt makes a similar test, though in a different way
             * because of its different recursion mechanism.)
             */
            childtuple = SearchSysCacheAttName(RelationGetRelid(childrel), col_name);
            if !HeapTupleIsValid(childtuple) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"{}\" of relation \"{}\" does not exist",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(childrel)).to_string_lossy())
                );
            }
            childatt_tup = GETSTRUCT(childtuple) as Form_pg_attribute;

            if (*childatt_tup).attinhcount > numparents {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("cannot alter inherited column \"{}\" of relation \"{}\"",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(childrel)).to_string_lossy())
                );
            }

            ReleaseSysCache(childtuple);

            /*
             * Remap the attribute numbers.  If no USING expression was
             * specified, there is no need for this step.
             */
            if !(*def).cooked_default.is_null() {
                let attmap: *mut AttrMap;
                let mut found_whole_row: bool = false;

                /* create a copy to scribble on */
                cmd = copyObject(cmd as *mut std::ffi::c_void) as *mut AlterTableCmd;

                attmap = build_attrmap_by_name(
                    RelationGetDescr(childrel),
                    RelationGetDescr(rel),
                    false,
                );
                (*((*cmd).def as *mut ColumnDef)).cooked_default = map_variable_attnos(
                    (*def).cooked_default,
                    1,
                    0,
                    attmap,
                    InvalidOid,
                    &mut found_whole_row,
                );
                if found_whole_row {
                    ereport!(
                        ERROR,
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot convert whole-row table reference"),
                        errdetail("USING expression contains a whole-row table reference.")
                    );
                }
                pfree(attmap as *mut std::ffi::c_void);
            }
            ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
            relation_close(childrel, NoLock);

            lo = lnext(child_oids, lo);
            li = lnext(child_numparents, li);
        }
    } else if !recursing
        && !find_inheritance_children(RelationGetRelid(rel), NoLock).is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("type of inherited column \"{}\" must be changed in child tables too",
                CStr::from_ptr(col_name).to_string_lossy())
        );
    }

    if (*tab).relkind == RELKIND_COMPOSITE_TYPE as i8 {
        ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
    }
}

/*
 * When the data type of a column is changed, a rewrite might not be required
 * if the new type is sufficiently identical to the old one, and the USING
 * clause isn't trying to insert some other value.  It's safe to skip the
 * rewrite in these cases:
 *
 * - the old type is binary coercible to the new type
 * - the new type is an unconstrained domain over the old type
 * - {NEW,OLD} or {OLD,NEW} is {timestamptz,timestamp} and the timezone is UTC
 *
 * In the case of a constrained domain, we could get by with scanning the
 * table and checking the constraint rather than actually rewriting it, but we
 * don't currently try to do that.
 */
unsafe fn ATColumnChangeRequiresRewrite(expr: *mut Node, varattno: AttrNumber) -> bool {
    assert!(!expr.is_null());

    let mut expr = expr;
    loop {
        /* only one varno, so no need to check that */
        if IsA(expr, T_Var) && (*(expr as *mut Var)).varattno == varattno {
            return false;
        } else if IsA(expr, T_RelabelType) {
            expr = (*(expr as *mut RelabelType)).arg as *mut Node;
        } else if IsA(expr, T_CoerceToDomain) {
            let d = expr as *mut CoerceToDomain;
            if DomainHasConstraints((*d).resulttype) {
                return true;
            }
            expr = (*d).arg as *mut Node;
        } else if IsA(expr, T_FuncExpr) {
            let f = expr as *mut FuncExpr;
            match (*f).funcid {
                F_TIMESTAMPTZ_TIMESTAMP | F_TIMESTAMP_TIMESTAMPTZ => {
                    if TimestampTimestampTzRequiresRewrite() {
                        return true;
                    } else {
                        expr = linitial((*f).args) as *mut Node;
                    }
                }
                _ => {
                    return true;
                }
            }
        } else {
            return true;
        }
    }
}
// section: tablecmds_tail -- C lines 14726-22113 (ATExecAlterColumnType ... GetAttributeStorage)

// ---------------------------------------------------------------------------
// ATExecAlterColumnType
// ---------------------------------------------------------------------------

pub unsafe fn ATExecAlterColumnType(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let col_name: *mut libc::c_char = (*cmd).name;
    let def: *mut ColumnDef = (*cmd).def as *mut ColumnDef;
    let type_name: *mut TypeName = (*def).typeName;
    let mut heap_tup: HeapTuple;
    let att_tup: Form_pg_attribute;
    let att_old_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let type_tuple: HeapTuple;
    let tform: Form_pg_type;
    let targettype: Oid;
    let mut targettypmod: i32 = 0;
    let targetcollid: Oid;
    let defaultexpr: *mut Node;
    let attrelation: Relation;
    let dep_rel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut dep_tup: HeapTuple;
    let address: ObjectAddress;

    /*
     * Clear all the missing values if we're rewriting the table, since this
     * renders them pointless.
     */
    if (*tab).rewrite != 0 {
        let newrel: Relation = table_open(RelationGetRelid(rel), NoLock);
        RelationClearMissing(newrel);
        relation_close(newrel, NoLock);
        /* make sure we don't conflict with later attribute modifications */
        CommandCounterIncrement();
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    /* Look up the target column */
    heap_tup = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(heap_tup) {
        /* shouldn't happen */
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }
    att_tup = GETSTRUCT(heap_tup) as Form_pg_attribute;
    attnum = (*att_tup).attnum;
    att_old_tup = TupleDescAttr((*tab).oldDesc, (attnum - 1) as usize) as Form_pg_attribute;

    /* Check for multiple ALTER TYPE on same column --- can't cope */
    if (*att_tup).atttypid != (*att_old_tup).atttypid
        || (*att_tup).atttypmod != (*att_old_tup).atttypmod
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot alter type of column \"{}\" twice",
                CStr::from_ptr(col_name).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Look up the target type (should not fail, since prep found it) */
    type_tuple = typenameType(core::ptr::null_mut(), type_name, &mut targettypmod);
    tform = GETSTRUCT(type_tuple) as Form_pg_type;
    targettype = (*tform).oid;
    /* And the collation */
    targetcollid = GetColumnDefCollation(core::ptr::null_mut(), def, targettype);

    /*
     * If there is a default expression for the column, get it and ensure we
     * can coerce it to the new datatype.  (We must do this before changing
     * the column type, because build_column_default itself will try to
     * coerce, and will not issue the error message we want if it fails.)
     *
     * We remove any implicit coercion steps at the top level of the old
     * default expression; this has been agreed to satisfy the principle of
     * least surprise.
     */
    if (*att_tup).atthasdef {
        let mut dexpr: *mut Node = build_column_default(rel, attnum);
        Assert!(!dexpr.is_null());
        dexpr = strip_implicit_coercions(dexpr);
        dexpr = coerce_to_target_type(
            core::ptr::null_mut(), /* no UNKNOWN params */
            dexpr,
            exprType(dexpr),
            targettype,
            targettypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if dexpr.is_null() {
            if (*att_tup).attgenerated != 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "generation expression for column \"{}\" cannot be cast automatically to type {}",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "default for column \"{}\" cannot be cast automatically to type {}",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
        }
        defaultexpr = dexpr;
    } else {
        defaultexpr = core::ptr::null_mut();
    }

    /*
     * Find everything that depends on the column (constraints, indexes, etc),
     * and record enough information to let us recreate the objects.
     */
    RememberAllDependentForRebuilding(tab, AT_AlterColumnType, rel, attnum, col_name);

    /*
     * Now scan for dependencies of this column on other things. The only
     * things we should find are the dependency on the column datatype and
     * possibly a collation dependency. Those can be removed.
     */
    dep_rel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(attnum as i32),
    );

    scan = systable_beginscan(dep_rel, DependDependerIndexId, true, core::ptr::null_mut(), 3, key.as_mut_ptr());

    loop {
        dep_tup = systable_getnext(scan);
        if !HeapTupleIsValid(dep_tup) {
            break;
        }
        let found_dep: Form_pg_depend = GETSTRUCT(dep_tup) as Form_pg_depend;
        let mut found_object: ObjectAddress = core::mem::zeroed();

        found_object.classId = (*found_dep).refclassid;
        found_object.objectId = (*found_dep).refobjid;
        found_object.objectSubId = (*found_dep).refobjsubid;

        if (*found_dep).deptype != DEPENDENCY_NORMAL as libc::c_char {
            elog!(ERROR, "found unexpected dependency type '{}'", (*found_dep).deptype as u8 as char);
        }
        if !((*found_dep).refclassid == TypeRelationId
            && (*found_dep).refobjid == (*att_tup).atttypid)
            && !((*found_dep).refclassid == CollationRelationId
                && (*found_dep).refobjid == (*att_tup).attcollation)
        {
            elog!(
                ERROR,
                "found unexpected dependency for column: {}",
                CStr::from_ptr(getObjectDescription(&found_object, false)).to_string_lossy()
            );
        }

        CatalogTupleDelete(dep_rel, &(*dep_tup).t_self);
    }

    systable_endscan(scan);
    table_close(dep_rel, RowExclusiveLock);

    /*
     * Here we go --- change the recorded column type and collation.
     * First fix up the missing value if any.
     */
    if (*att_tup).atthasmissing {
        let mut missing_val: Datum;
        let mut missing_null: bool = false;

        /* if rewrite is true the missing value should already be cleared */
        Assert!((*tab).rewrite == 0);

        /* Get the missing value datum */
        missing_val = heap_getattr(
            heap_tup,
            Anum_pg_attribute_attmissingval,
            (*attrelation).rd_att,
            &mut missing_null,
        );

        /* if it's a null array there is nothing to do */
        if !missing_null {
            /*
             * Get the datum out of the array and repack it in a new array
             * built with the new type data.
             */
            let one: i32 = 1;
            let mut is_null: bool = false;
            let mut values_att: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
            let mut nulls_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
            let mut replaces_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
            let new_tup: HeapTuple;

            missing_val = array_get_element(
                missing_val,
                1,
                &one,
                0,
                (*att_tup).attlen,
                (*att_tup).attbyval,
                (*att_tup).attalign,
                &mut is_null,
            );
            missing_val = PointerGetDatum(construct_array(
                &mut missing_val,
                1,
                targettype,
                (*tform).typlen,
                (*tform).typbyval,
                (*tform).typalign,
            ));

            values_att[Anum_pg_attribute_attmissingval - 1] = missing_val;
            replaces_att[Anum_pg_attribute_attmissingval - 1] = true;
            nulls_att[Anum_pg_attribute_attmissingval - 1] = false;

            new_tup = heap_modify_tuple(
                heap_tup,
                RelationGetDescr(attrelation),
                values_att.as_mut_ptr(),
                nulls_att.as_mut_ptr(),
                replaces_att.as_mut_ptr(),
            );
            heap_freetuple(heap_tup);
            heap_tup = new_tup;
            // re-fetch att_tup after tuple replacement
            let att_tup = GETSTRUCT(heap_tup) as Form_pg_attribute;
            let _ = att_tup; // used below via heap_tup
        }
    }

    // re-borrow att_tup for mutation
    let att_tup_mut = GETSTRUCT(heap_tup) as Form_pg_attribute;
    (*att_tup_mut).atttypid = targettype;
    (*att_tup_mut).atttypmod = targettypmod;
    (*att_tup_mut).attcollation = targetcollid;
    if list_length((*type_name).arrayBounds) > libc::INT16_MAX as i32 {
        ereport!(
            ERROR,
            errmsg!("too many array dimensions") /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        );
    }
    (*att_tup_mut).attndims = list_length((*type_name).arrayBounds) as i16;
    (*att_tup_mut).attlen = (*tform).typlen;
    (*att_tup_mut).attbyval = (*tform).typbyval;
    (*att_tup_mut).attalign = (*tform).typalign;
    (*att_tup_mut).attstorage = (*tform).typstorage;
    (*att_tup_mut).attcompression = InvalidCompressionMethod;

    ReleaseSysCache(type_tuple);

    CatalogTupleUpdate(attrelation, &(*heap_tup).t_self, heap_tup);

    table_close(attrelation, RowExclusiveLock);

    /* Install dependencies on new datatype and collation */
    add_column_datatype_dependency(RelationGetRelid(rel), attnum, targettype);
    add_column_collation_dependency(RelationGetRelid(rel), attnum, targetcollid);

    /*
     * Drop any pg_statistic entry for the column, since it's now wrong type
     */
    RemoveStatistics(RelationGetRelid(rel), attnum);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    /*
     * Update the default, if present, by brute force --- remove and re-add
     * the default.
     */
    if !defaultexpr.is_null() {
        /*
         * If it's a GENERATED default, drop its dependency records, in
         * particular its INTERNAL dependency on the column, which would
         * otherwise cause dependency.c to refuse to perform the deletion.
         */
        let att_tup_cur = GETSTRUCT(heap_tup) as Form_pg_attribute;
        if (*att_tup_cur).attgenerated != 0 {
            let attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
            if !OidIsValid(attrdefoid) {
                elog!(
                    ERROR,
                    "could not find attrdef tuple for relation {} attnum {}",
                    RelationGetRelid(rel),
                    attnum
                );
            }
            let _ = deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);
        }

        /*
         * Make updates-so-far visible, particularly the new pg_attribute row
         * which will be updated again.
         */
        CommandCounterIncrement();

        /*
         * We use RESTRICT here for safety, but at present we do not expect
         * anything to depend on the default.
         */
        RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, true, true);

        let _ = StoreAttrDefault(rel, attnum, defaultexpr, true);
    }

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum);

    /* Cleanup */
    heap_freetuple(heap_tup);

    address
}

// ---------------------------------------------------------------------------
// RememberAllDependentForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType and ATExecSetExpression: Find everything
/// that depends on the column (constraints, indexes, etc), and record enough
/// information to let us recreate the objects.
unsafe fn RememberAllDependentForRebuilding(
    tab: *mut AlteredTableInfo,
    subtype: AlterTableType,
    rel: Relation,
    attnum: AttrNumber,
    col_name: *const libc::c_char,
) {
    let dep_rel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut dep_tup: HeapTuple;

    Assert!(subtype == AT_AlterColumnType || subtype == AT_SetExpression);

    dep_rel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_refobjsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(attnum as i32),
    );

    scan = systable_beginscan(dep_rel, DependReferenceIndexId, true, core::ptr::null_mut(), 3, key.as_mut_ptr());

    loop {
        dep_tup = systable_getnext(scan);
        if !HeapTupleIsValid(dep_tup) {
            break;
        }
        let found_dep: Form_pg_depend = GETSTRUCT(dep_tup) as Form_pg_depend;
        let mut found_object: ObjectAddress = core::mem::zeroed();

        found_object.classId = (*found_dep).classid;
        found_object.objectId = (*found_dep).objid;
        found_object.objectSubId = (*found_dep).objsubid;

        match found_object.classId {
            RelationRelationId => {
                let rel_kind: libc::c_char = get_rel_relkind(found_object.objectId);
                if rel_kind == RELKIND_INDEX as libc::c_char
                    || rel_kind == RELKIND_PARTITIONED_INDEX as libc::c_char
                {
                    Assert!(found_object.objectSubId == 0);
                    RememberIndexForRebuilding(found_object.objectId, tab);
                } else if rel_kind == RELKIND_SEQUENCE as libc::c_char {
                    /*
                     * This must be a SERIAL column's sequence. We need
                     * not do anything to it.
                     */
                    Assert!(found_object.objectSubId == 0);
                } else {
                    /* Not expecting any other direct dependencies... */
                    elog!(
                        ERROR,
                        "unexpected object depending on column: {}",
                        CStr::from_ptr(getObjectDescription(&found_object, false))
                            .to_string_lossy()
                    );
                }
            }
            ConstraintRelationId => {
                Assert!(found_object.objectSubId == 0);
                RememberConstraintForRebuilding(found_object.objectId, tab);
            }
            ProcedureRelationId => {
                /*
                 * A new-style SQL function can depend on a column, if that
                 * column is referenced in the parsed function body. FIXME someday.
                 *
                 * This is only a problem for AT_AlterColumnType, not AT_SetExpression.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!("cannot alter type of a column used by a function or procedure")
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errdetail("%s depends on column \"%s\"", ...) */
                    );
                }
            }
            RewriteRelationId => {
                /*
                 * View/rule bodies have pretty much the same issues as
                 * function bodies. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!("cannot alter type of a column used by a view or rule")
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            TriggerRelationId => {
                /*
                 * A trigger can depend on a column because the column is
                 * specified as an update target, or because the column is
                 * used in the trigger's WHEN condition. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot alter type of a column used in a trigger definition"
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            PolicyRelationId => {
                /*
                 * A policy can depend on a column because the column is
                 * specified in the policy's USING or WITH CHECK qual
                 * expressions. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot alter type of a column used in a policy definition"
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            AttrDefaultRelationId => {
                let col: ObjectAddress = GetAttrDefaultColumnAddress(found_object.objectId);
                if col.objectId == RelationGetRelid(rel)
                    && col.objectSubId == attnum as i32
                {
                    /*
                     * Ignore the column's own default expression. The
                     * caller deals with it.
                     */
                } else {
                    /*
                     * This must be a reference from the expression of a
                     * generated column elsewhere in the same table.
                     * Changing the type/generated expression of a column
                     * that is used by a generated column is not allowed
                     * by SQL standard, so just punt for now.
                     */
                    if subtype == AT_AlterColumnType {
                        ereport!(
                            ERROR,
                            errmsg!("cannot alter type of a column used by a generated column")
                            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errdetail("Column \"%s\" is used by generated column \"%s\".", ...) */
                        );
                    }
                }
            }
            StatisticExtRelationId => {
                /*
                 * Give the extended-stats machinery a chance to fix anything
                 * that this column type change would break.
                 */
                RememberStatisticsForRebuilding(found_object.objectId, tab);
            }
            PublicationRelRelationId => {
                /*
                 * Column reference in a PUBLICATION ... FOR TABLE ... WHERE
                 * clause. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot alter type of a column used by a publication WHERE clause"
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            _ => {
                /*
                 * We don't expect any other sorts of objects to depend on a
                 * column.
                 */
                elog!(
                    ERROR,
                    "unexpected object depending on column: {}",
                    CStr::from_ptr(getObjectDescription(&found_object, false)).to_string_lossy()
                );
            }
        }
    }

    systable_endscan(scan);
    table_close(dep_rel, NoLock);
}

// ---------------------------------------------------------------------------
// RememberReplicaIdentityForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that a replica identity
/// needs to be reset.
unsafe fn RememberReplicaIdentityForRebuilding(indoid: Oid, tab: *mut AlteredTableInfo) {
    if !get_index_isreplident(indoid) {
        return;
    }
    if !(*tab).replicaIdentityIndex.is_null() {
        elog!(
            ERROR,
            "relation {} has multiple indexes marked as replica identity",
            (*tab).relid
        );
    }
    (*tab).replicaIdentityIndex = get_rel_name(indoid);
}

// ---------------------------------------------------------------------------
// RememberClusterOnForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember any clustered index.
unsafe fn RememberClusterOnForRebuilding(indoid: Oid, tab: *mut AlteredTableInfo) {
    if !get_index_isclustered(indoid) {
        return;
    }
    if !(*tab).clusterOnIndex.is_null() {
        elog!(ERROR, "relation {} has multiple clustered indexes", (*tab).relid);
    }
    (*tab).clusterOnIndex = get_rel_name(indoid);
}

// ---------------------------------------------------------------------------
// RememberConstraintForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that a constraint needs
/// to be rebuilt (which we might already know).
unsafe fn RememberConstraintForRebuilding(conoid: Oid, tab: *mut AlteredTableInfo) {
    /*
     * This de-duplication check is critical for two independent reasons: we
     * mustn't try to recreate the same constraint twice, and if a constraint
     * depends on more than one column whose type is to be altered, we must
     * capture its definition string before applying any of the column type
     * changes. ruleutils.c will get confused if we ask again later.
     */
    if !list_member_oid((*tab).changedConstraintOids, conoid) {
        /* OK, capture the constraint's existing definition string */
        let defstring: *mut libc::c_char = pg_get_constraintdef_command(conoid);
        let indoid: Oid;

        /*
         * It is critical to create not-null constraints ahead of primary key
         * indexes; otherwise, the not-null constraint would be created by the
         * primary key, and the constraint name would be wrong.
         */
        if get_constraint_type(conoid) == CONSTRAINT_NOTNULL as libc::c_char {
            (*tab).changedConstraintOids =
                lcons_oid(conoid, (*tab).changedConstraintOids);
            (*tab).changedConstraintDefs =
                lcons(defstring as *mut libc::c_void, (*tab).changedConstraintDefs);
        } else {
            (*tab).changedConstraintOids =
                lappend_oid((*tab).changedConstraintOids, conoid);
            (*tab).changedConstraintDefs =
                lappend((*tab).changedConstraintDefs, defstring as *mut libc::c_void);
        }

        /*
         * For the index of a constraint, if any, remember if it is used for
         * the table's replica identity or if it is a clustered index.
         */
        indoid = get_constraint_index(conoid);
        if OidIsValid(indoid) {
            RememberReplicaIdentityForRebuilding(indoid, tab);
            RememberClusterOnForRebuilding(indoid, tab);
        }
    }
}

// ---------------------------------------------------------------------------
// RememberIndexForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that an index needs
/// to be rebuilt (which we might already know).
unsafe fn RememberIndexForRebuilding(indoid: Oid, tab: *mut AlteredTableInfo) {
    /*
     * This de-duplication check is critical for two independent reasons: we
     * mustn't try to recreate the same index twice, and if an index depends
     * on more than one column whose type is to be altered, we must capture
     * its definition string before applying any of the column type changes.
     * ruleutils.c will get confused if we ask again later.
     */
    if !list_member_oid((*tab).changedIndexOids, indoid) {
        /*
         * Before adding it as an index-to-rebuild, we'd better see if it
         * belongs to a constraint, and if so rebuild the constraint instead.
         */
        let conoid: Oid = get_index_constraint(indoid);
        if OidIsValid(conoid) {
            RememberConstraintForRebuilding(conoid, tab);
        } else {
            /* OK, capture the index's existing definition string */
            let defstring: *mut libc::c_char = pg_get_indexdef_string(indoid);

            (*tab).changedIndexOids = lappend_oid((*tab).changedIndexOids, indoid);
            (*tab).changedIndexDefs =
                lappend((*tab).changedIndexDefs, defstring as *mut libc::c_void);

            /*
             * Remember if this index is used for the table's replica identity
             * or if it is a clustered index.
             */
            RememberReplicaIdentityForRebuilding(indoid, tab);
            RememberClusterOnForRebuilding(indoid, tab);
        }
    }
}

// ---------------------------------------------------------------------------
// RememberStatisticsForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that a statistics object
/// needs to be rebuilt (which we might already know).
unsafe fn RememberStatisticsForRebuilding(stxoid: Oid, tab: *mut AlteredTableInfo) {
    /*
     * This de-duplication check is critical for two independent reasons: we
     * mustn't try to recreate the same statistics object twice, and if the
     * statistics object depends on more than one column whose type is to be
     * altered, we must capture its definition string before applying any of
     * the type changes. ruleutils.c will get confused if we ask again later.
     */
    if !list_member_oid((*tab).changedStatisticsOids, stxoid) {
        /* OK, capture the statistics object's existing definition string */
        let defstring: *mut libc::c_char = pg_get_statisticsobjdef_string(stxoid);

        (*tab).changedStatisticsOids =
            lappend_oid((*tab).changedStatisticsOids, stxoid);
        (*tab).changedStatisticsDefs =
            lappend((*tab).changedStatisticsDefs, defstring as *mut libc::c_void);
    }
}

// ---------------------------------------------------------------------------
// ATPostAlterTypeCleanup
// ---------------------------------------------------------------------------

/// Cleanup after we've finished all the ALTER TYPE or SET EXPRESSION
/// operations for a particular relation. We have to drop and recreate all the
/// indexes and constraints that depend on the altered columns.
unsafe fn ATPostAlterTypeCleanup(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    lockmode: LOCKMODE,
) {
    let mut obj: ObjectAddress = core::mem::zeroed();
    let objects: *mut ObjectAddresses = new_object_addresses();
    let mut def_item: *mut ListCell;
    let mut oid_item: *mut ListCell;

    /*
     * Collect all the constraints and indexes to drop so we can process them
     * in a single call. That way we don't have to worry about dependencies
     * among them.
     */

    /*
     * Re-parse the index and constraint definitions, and attach them to the
     * appropriate work queue entries.
     */
    // forboth over changedConstraintOids / changedConstraintDefs
    oid_item = list_head((*tab).changedConstraintOids);
    def_item = list_head((*tab).changedConstraintDefs);
    while !oid_item.is_null() {
        let old_id: Oid = lfirst_oid(oid_item);
        let tup: HeapTuple;
        let con: Form_pg_constraint;
        let relid: Oid;
        let confrelid: Oid;
        let conislocal: bool;

        tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(old_id));
        if !HeapTupleIsValid(tup) {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for constraint {}", old_id);
        }
        con = GETSTRUCT(tup) as Form_pg_constraint;
        if OidIsValid((*con).conrelid) {
            relid = (*con).conrelid;
        } else {
            /* must be a domain constraint */
            relid = get_typ_typrelid(getBaseType((*con).contypid));
            if !OidIsValid(relid) {
                elog!(
                    ERROR,
                    "could not identify relation associated with constraint {}",
                    old_id
                );
            }
        }
        confrelid = (*con).confrelid;
        conislocal = (*con).conislocal;
        ReleaseSysCache(tup);

        ObjectAddressSet!(obj, ConstraintRelationId, old_id);
        add_exact_object_address(&obj, objects);

        /*
         * If the constraint is inherited (only), we don't want to inject a
         * new definition here; it'll get recreated when
         * ATAddCheckNNConstraint recurses from adding the parent table's
         * constraint. But we had to carry the info this far so that we can
         * drop the constraint below.
         */
        if !conislocal {
            oid_item = lnext((*tab).changedConstraintOids, oid_item);
            def_item = lnext((*tab).changedConstraintDefs, def_item);
            continue;
        }

        /*
         * When rebuilding another table's constraint that references the
         * table we're modifying, we might not yet have any lock on the other
         * table, so get one now.
         */
        if relid != (*tab).relid {
            LockRelationOid(relid, AccessExclusiveLock);
        }

        ATPostAlterTypeParse(
            old_id,
            relid,
            confrelid,
            lfirst(def_item) as *mut libc::c_char,
            wqueue,
            lockmode,
            (*tab).rewrite != 0,
        );

        oid_item = lnext((*tab).changedConstraintOids, oid_item);
        def_item = lnext((*tab).changedConstraintDefs, def_item);
    }

    oid_item = list_head((*tab).changedIndexOids);
    def_item = list_head((*tab).changedIndexDefs);
    while !oid_item.is_null() {
        let old_id: Oid = lfirst_oid(oid_item);
        let relid: Oid = IndexGetRelation(old_id, false);

        /*
         * As above, make sure we have lock on the index's table if it's not
         * the same table.
         */
        if relid != (*tab).relid {
            LockRelationOid(relid, AccessExclusiveLock);
        }

        ATPostAlterTypeParse(
            old_id,
            relid,
            InvalidOid,
            lfirst(def_item) as *mut libc::c_char,
            wqueue,
            lockmode,
            (*tab).rewrite != 0,
        );

        ObjectAddressSet!(obj, RelationRelationId, old_id);
        add_exact_object_address(&obj, objects);

        oid_item = lnext((*tab).changedIndexOids, oid_item);
        def_item = lnext((*tab).changedIndexDefs, def_item);
    }

    /* add dependencies for new statistics */
    oid_item = list_head((*tab).changedStatisticsOids);
    def_item = list_head((*tab).changedStatisticsDefs);
    while !oid_item.is_null() {
        let old_id: Oid = lfirst_oid(oid_item);
        let relid: Oid = StatisticsGetRelation(old_id, false);

        /*
         * As above, make sure we have lock on the statistics object's table
         * if it's not the same table. However, we take
         * ShareUpdateExclusiveLock here.
         *
         * CAUTION: this should be done after all cases that grab
         * AccessExclusiveLock.
         */
        if relid != (*tab).relid {
            LockRelationOid(relid, ShareUpdateExclusiveLock);
        }

        ATPostAlterTypeParse(
            old_id,
            relid,
            InvalidOid,
            lfirst(def_item) as *mut libc::c_char,
            wqueue,
            lockmode,
            (*tab).rewrite != 0,
        );

        ObjectAddressSet!(obj, StatisticExtRelationId, old_id);
        add_exact_object_address(&obj, objects);

        oid_item = lnext((*tab).changedStatisticsOids, oid_item);
        def_item = lnext((*tab).changedStatisticsDefs, def_item);
    }

    /*
     * Queue up command to restore replica identity index marking
     */
    if !(*tab).replicaIdentityIndex.is_null() {
        let cmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        let subcmd: *mut ReplicaIdentityStmt = makeNode!(ReplicaIdentityStmt, T_ReplicaIdentityStmt);

        (*subcmd).identity_type = REPLICA_IDENTITY_INDEX;
        (*subcmd).name = (*tab).replicaIdentityIndex;
        (*cmd).subtype = AT_ReplicaIdentity;
        (*cmd).def = subcmd as *mut Node;

        /* do it after indexes and constraints */
        (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] =
            lappend((*tab).subcmds[AT_PASS_OLD_CONSTR as usize], cmd as *mut libc::c_void);
    }

    /*
     * Queue up command to restore marking of index used for cluster.
     */
    if !(*tab).clusterOnIndex.is_null() {
        let cmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

        (*cmd).subtype = AT_ClusterOn;
        (*cmd).name = (*tab).clusterOnIndex;

        /* do it after indexes and constraints */
        (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] =
            lappend((*tab).subcmds[AT_PASS_OLD_CONSTR as usize], cmd as *mut libc::c_void);
    }

    /*
     * It should be okay to use DROP_RESTRICT here, since nothing else should
     * be depending on these objects.
     */
    performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    free_object_addresses(objects);

    /*
     * The objects will get recreated during subsequent passes over the work
     * queue.
     */
}

// ---------------------------------------------------------------------------
// ATPostAlterTypeParse
// ---------------------------------------------------------------------------

/// Parse the previously-saved definition string for a constraint, index or
/// statistics object against the newly-established column data type(s), and
/// queue up the resulting command parsetrees for execution.
unsafe fn ATPostAlterTypeParse(
    old_id: Oid,
    old_rel_id: Oid,
    ref_rel_id: Oid,
    cmd: *mut libc::c_char,
    wqueue: *mut *mut List,
    lockmode: LOCKMODE,
    rewrite: bool,
) {
    let raw_parsetree_list: *mut List;
    let mut querytree_list: *mut List = NIL;
    let mut list_item: *mut ListCell;
    let rel: Relation;

    /*
     * We expect that we will get only ALTER TABLE and CREATE INDEX
     * statements. Hence, there is no need to pass them through
     * parse_analyze_*() or the rewriter, but instead we need to pass them
     * through parse_utilcmd.c to make them ready for execution.
     */
    raw_parsetree_list = raw_parser(cmd, RAW_PARSE_DEFAULT);
    querytree_list = NIL;
    list_item = list_head(raw_parsetree_list);
    while !list_item.is_null() {
        let rs: *mut RawStmt = lfirst_node!(RawStmt, T_RawStmt, list_item);
        let stmt: *mut Node = (*rs).stmt;

        if IsA!(stmt, T_IndexStmt) {
            querytree_list = lappend(
                querytree_list,
                transformIndexStmt(old_rel_id, stmt as *mut IndexStmt, cmd) as *mut libc::c_void,
            );
        } else if IsA!(stmt, T_AlterTableStmt) {
            let mut before_stmts: *mut List = core::ptr::null_mut();
            let mut after_stmts: *mut List = core::ptr::null_mut();

            let transformed = transformAlterTableStmt(
                old_rel_id,
                stmt as *mut AlterTableStmt,
                cmd,
                &mut before_stmts,
                &mut after_stmts,
            ) as *mut Node;
            querytree_list = list_concat(querytree_list, before_stmts);
            querytree_list = lappend(querytree_list, transformed as *mut libc::c_void);
            querytree_list = list_concat(querytree_list, after_stmts);
        } else if IsA!(stmt, T_CreateStatsStmt) {
            querytree_list = lappend(
                querytree_list,
                transformStatsStmt(old_rel_id, stmt as *mut CreateStatsStmt, cmd)
                    as *mut libc::c_void,
            );
        } else {
            querytree_list = lappend(querytree_list, stmt as *mut libc::c_void);
        }

        list_item = lnext(raw_parsetree_list, list_item);
    }

    /* Caller should already have acquired whatever lock we need. */
    rel = relation_open(old_rel_id, NoLock);

    /*
     * Attach each generated command to the proper place in the work queue.
     * Note this could result in creation of entirely new work-queue entries.
     *
     * Also note that we have to tweak the command subtypes, because it turns
     * out that re-creation of indexes and constraints has to act a bit
     * differently from initial creation.
     */
    list_item = list_head(querytree_list);
    while !list_item.is_null() {
        let stm: *mut Node = lfirst(list_item) as *mut Node;
        let tab: *mut AlteredTableInfo = ATGetQueueEntry(wqueue, rel);

        if IsA!(stm, T_IndexStmt) {
            let stmt: *mut IndexStmt = stm as *mut IndexStmt;
            let newcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

            if !rewrite {
                TryReuseIndex(old_id, stmt);
            }
            (*stmt).reset_default_tblspc = true;
            /* keep the index's comment */
            (*stmt).idxcomment = GetComment(old_id, RelationRelationId, 0);

            (*newcmd).subtype = AT_ReAddIndex;
            (*newcmd).def = stmt as *mut Node;
            (*tab).subcmds[AT_PASS_OLD_INDEX as usize] = lappend(
                (*tab).subcmds[AT_PASS_OLD_INDEX as usize],
                newcmd as *mut libc::c_void,
            );
        } else if IsA!(stm, T_AlterTableStmt) {
            let stmt: *mut AlterTableStmt = stm as *mut AlterTableStmt;
            let mut lcmd: *mut ListCell = list_head((*stmt).cmds);
            while !lcmd.is_null() {
                let acmd: *mut AlterTableCmd =
                    lfirst_node!(AlterTableCmd, T_AlterTableCmd, lcmd);

                if (*acmd).subtype == AT_AddIndex {
                    let indstmt: *mut IndexStmt =
                        castNode!(IndexStmt, T_IndexStmt, (*acmd).def);
                    let indoid: Oid = get_constraint_index(old_id);

                    if !rewrite {
                        TryReuseIndex(indoid, indstmt);
                    }
                    /* keep any comment on the index */
                    (*indstmt).idxcomment = GetComment(indoid, RelationRelationId, 0);
                    (*indstmt).reset_default_tblspc = true;

                    (*acmd).subtype = AT_ReAddIndex;
                    (*tab).subcmds[AT_PASS_OLD_INDEX as usize] = lappend(
                        (*tab).subcmds[AT_PASS_OLD_INDEX as usize],
                        acmd as *mut libc::c_void,
                    );

                    /* recreate any comment on the constraint */
                    RebuildConstraintComment(
                        tab,
                        AT_PASS_OLD_INDEX,
                        old_id,
                        rel,
                        NIL,
                        (*indstmt).idxname,
                    );
                } else if (*acmd).subtype == AT_AddConstraint {
                    let con: *mut Constraint =
                        castNode!(Constraint, T_Constraint, (*acmd).def);

                    (*con).old_pktable_oid = ref_rel_id;
                    /* rewriting neither side of a FK */
                    if (*con).contype == CONSTR_FOREIGN
                        && !rewrite
                        && (*tab).rewrite == 0
                    {
                        TryReuseForeignKey(old_id, con);
                    }
                    (*con).reset_default_tblspc = true;
                    (*acmd).subtype = AT_ReAddConstraint;
                    (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] = lappend(
                        (*tab).subcmds[AT_PASS_OLD_CONSTR as usize],
                        acmd as *mut libc::c_void,
                    );

                    /*
                     * Recreate any comment on the constraint. If we have
                     * recreated a primary key, then transformTableConstraint
                     * has added an unnamed not-null constraint here; skip
                     * this in that case.
                     */
                    if !(*con).conname.is_null() {
                        RebuildConstraintComment(
                            tab,
                            AT_PASS_OLD_CONSTR,
                            old_id,
                            rel,
                            NIL,
                            (*con).conname,
                        );
                    } else {
                        Assert!((*con).contype == CONSTR_NOTNULL);
                    }
                } else {
                    elog!(
                        ERROR,
                        "unexpected statement subtype: {}",
                        (*acmd).subtype as i32
                    );
                }

                lcmd = lnext((*stmt).cmds, lcmd);
            }
        } else if IsA!(stm, T_AlterDomainStmt) {
            let stmt: *mut AlterDomainStmt = stm as *mut AlterDomainStmt;

            if (*stmt).subtype == b'C' as libc::c_char {
                /* ADD CONSTRAINT */
                let con: *mut Constraint =
                    castNode!(Constraint, T_Constraint, (*stmt).def);
                let newcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

                (*newcmd).subtype = AT_ReAddDomainConstraint;
                (*newcmd).def = stmt as *mut Node;
                (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] = lappend(
                    (*tab).subcmds[AT_PASS_OLD_CONSTR as usize],
                    newcmd as *mut libc::c_void,
                );

                /* recreate any comment on the constraint */
                RebuildConstraintComment(
                    tab,
                    AT_PASS_OLD_CONSTR,
                    old_id,
                    core::ptr::null_mut(),
                    (*stmt).typeName,
                    (*con).conname,
                );
            } else {
                elog!(ERROR, "unexpected statement subtype: {}", (*stmt).subtype as i32);
            }
        } else if IsA!(stm, T_CreateStatsStmt) {
            let stmt: *mut CreateStatsStmt = stm as *mut CreateStatsStmt;
            let newcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

            /* keep the statistics object's comment */
            (*stmt).stxcomment = GetComment(old_id, StatisticExtRelationId, 0);

            (*newcmd).subtype = AT_ReAddStatistics;
            (*newcmd).def = stmt as *mut Node;
            (*tab).subcmds[AT_PASS_MISC as usize] = lappend(
                (*tab).subcmds[AT_PASS_MISC as usize],
                newcmd as *mut libc::c_void,
            );
        } else {
            elog!(ERROR, "unexpected statement type: {}", nodeTag(stm) as i32);
        }

        list_item = lnext(querytree_list, list_item);
    }

    relation_close(rel, NoLock);
}

// ---------------------------------------------------------------------------
// RebuildConstraintComment
// ---------------------------------------------------------------------------

/// Subroutine for ATPostAlterTypeParse() to recreate any existing comment
/// for a table or domain constraint that is being rebuilt.
///
/// objid is the OID of the constraint.
/// Pass "rel" for a table constraint, or "domname" (domain's qualified name
/// as a string list) for a domain constraint.
unsafe fn RebuildConstraintComment(
    tab: *mut AlteredTableInfo,
    pass: AlterTablePass,
    objid: Oid,
    rel: Relation,
    domname: *mut List,
    conname: *const libc::c_char,
) {
    let cmd: *mut CommentStmt;
    let comment_str: *mut libc::c_char;
    let newcmd: *mut AlterTableCmd;

    /* Look for comment for object wanted, and leave if none */
    comment_str = GetComment(objid, ConstraintRelationId, 0);
    if comment_str.is_null() {
        return;
    }

    /* Build CommentStmt node, copying all input data for safety */
    cmd = makeNode!(CommentStmt, T_CommentStmt);
    if !rel.is_null() {
        (*cmd).objtype = OBJECT_TABCONSTRAINT;
        (*cmd).object = list_make3(
            makeString(get_namespace_name(RelationGetNamespace(rel))),
            makeString(pstrdup(RelationGetRelationName(rel))),
            makeString(pstrdup(conname)),
        ) as *mut Node;
    } else {
        (*cmd).objtype = OBJECT_DOMCONSTRAINT;
        (*cmd).object = list_make2(
            makeTypeNameFromNameList(copyObject(domname)),
            makeString(pstrdup(conname)),
        ) as *mut Node;
    }
    (*cmd).comment = comment_str;

    /* Append it to list of commands */
    newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
    (*newcmd).subtype = AT_ReAddComment;
    (*newcmd).def = cmd as *mut Node;
    (*tab).subcmds[pass as usize] =
        lappend((*tab).subcmds[pass as usize], newcmd as *mut libc::c_void);
}

// ---------------------------------------------------------------------------
// TryReuseIndex
// ---------------------------------------------------------------------------

/// Subroutine for ATPostAlterTypeParse(). Calls out to CheckIndexCompatible()
/// for the real analysis, then mutates the IndexStmt based on that verdict.
unsafe fn TryReuseIndex(old_id: Oid, stmt: *mut IndexStmt) {
    if CheckIndexCompatible(
        old_id,
        (*stmt).accessMethod,
        (*stmt).indexParams,
        (*stmt).excludeOpNames,
        (*stmt).iswithoutoverlaps,
    ) {
        let irel: Relation = index_open(old_id, NoLock);
        /* If it's a partitioned index, there is no storage to share. */
        if (*(*irel).rd_rel).relkind != RELKIND_PARTITIONED_INDEX as libc::c_char {
            (*stmt).oldNumber = (*irel).rd_locator.relNumber;
            (*stmt).oldCreateSubid = (*irel).rd_createSubid;
            (*stmt).oldFirstRelfilelocatorSubid = (*irel).rd_firstRelfilelocatorSubid;
        }
        index_close(irel, NoLock);
    }
}

// ---------------------------------------------------------------------------
// TryReuseForeignKey
// ---------------------------------------------------------------------------

/// Subroutine for ATPostAlterTypeParse().
///
/// Stash the old P-F equality operator into the Constraint node, for possible
/// use by ATAddForeignKeyConstraint() in determining whether revalidation of
/// this constraint can be skipped.
unsafe fn TryReuseForeignKey(old_id: Oid, con: *mut Constraint) {
    let tup: HeapTuple;
    let adatum: Datum;
    let arr: *mut ArrayType;
    let rawarr: *mut Oid;
    let numkeys: i32;

    Assert!((*con).contype == CONSTR_FOREIGN);
    Assert!((*con).old_conpfeqop == NIL); /* already prepared this node */

    tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(old_id));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for constraint {}", old_id);
    }

    adatum = SysCacheGetAttrNotNull(CONSTROID, tup, Anum_pg_constraint_conpfeqop);
    arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
    numkeys = ARR_DIMS(arr)[0];
    /* test follows the one in ri_FetchConstraintInfo() */
    if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID {
        elog!(ERROR, "conpfeqop is not a 1-D Oid array");
    }
    rawarr = ARR_DATA_PTR(arr) as *mut Oid;

    /* stash a List of the operator Oids in our Constraint node */
    for i in 0..numkeys as usize {
        (*con).old_conpfeqop = lappend_oid((*con).old_conpfeqop, *rawarr.add(i));
    }

    ReleaseSysCache(tup);
}

// ---------------------------------------------------------------------------
// ATExecAlterColumnGenericOptions
// ---------------------------------------------------------------------------

/// ALTER COLUMN .. OPTIONS ( ... )
///
/// Returns the address of the modified column
unsafe fn ATExecAlterColumnGenericOptions(
    rel: Relation,
    col_name: *const libc::c_char,
    options: *mut List,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let ftrel: Relation;
    let attrel: Relation;
    let server: *mut ForeignServer;
    let fdw: *mut ForeignDataWrapper;
    let mut tuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut isnull: bool = false;
    let mut repl_val: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
    let mut repl_null: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut repl_repl: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut datum: Datum;
    let fttableform: Form_pg_foreign_table;
    let atttableform: Form_pg_attribute;
    let attnum: AttrNumber;
    let address: ObjectAddress;

    if options == NIL {
        return InvalidObjectAddress;
    }

    /* First, determine FDW validator associated to the foreign table. */
    ftrel = table_open(ForeignTableRelationId, AccessShareLock);
    tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum((*rel).rd_id));
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "foreign table \"{}\" does not exist",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }
    fttableform = GETSTRUCT(tuple) as Form_pg_foreign_table;
    server = GetForeignServer((*fttableform).ftserver);
    fdw = GetForeignDataWrapper((*server).fdwid);

    table_close(ftrel, AccessShareLock);
    ReleaseSysCache(tuple);

    attrel = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }

    /* Prevent them from altering a system attribute */
    atttableform = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*atttableform).attnum;
    if attnum <= 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot alter system column \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Initialize buffers for new tuple values */
    libc::memset(repl_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_val));
    libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

    /* Extract the current options */
    datum = SysCacheGetAttr(
        ATTNAME,
        tuple,
        Anum_pg_attribute_attfdwoptions,
        &mut isnull,
    );
    if isnull {
        datum = PointerGetDatum(core::ptr::null::<libc::c_void>() as *mut libc::c_void);
    }

    /* Transform the options */
    datum = transformGenericOptions(
        AttributeRelationId,
        datum,
        options,
        (*fdw).fdwvalidator,
    );

    if PointerIsValid(DatumGetPointer(datum)) {
        repl_val[Anum_pg_attribute_attfdwoptions - 1] = datum;
    } else {
        repl_null[Anum_pg_attribute_attfdwoptions - 1] = true;
    }

    repl_repl[Anum_pg_attribute_attfdwoptions - 1] = true;

    /* Everything looks good - update the tuple */
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attrel),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(attrel, &(*newtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(
        RelationRelationId,
        RelationGetRelid(rel),
        (*atttableform).attnum as i32,
    );
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum);

    ReleaseSysCache(tuple);

    table_close(attrel, RowExclusiveLock);

    heap_freetuple(newtuple);

    address
}

// ---------------------------------------------------------------------------
// ATExecChangeOwner
// ---------------------------------------------------------------------------

/// ALTER TABLE OWNER
///
/// recursing is true if we are recursing from a table to its indexes,
/// sequences, or toast table. We don't allow the ownership of those things to
/// be changed separately from the parent table.
pub unsafe fn ATExecChangeOwner(
    relation_oid: Oid,
    new_owner_id: Oid,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    let target_rel: Relation;
    let class_rel: Relation;
    let mut tuple: HeapTuple;
    let tuple_class: Form_pg_class;

    /*
     * Get exclusive lock till end of transaction on the target table. Use
     * relation_open so that we can work on indexes and sequences.
     */
    target_rel = relation_open(relation_oid, lockmode);

    /* Get its pg_class tuple, too */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relation_oid);
    }
    tuple_class = GETSTRUCT(tuple) as Form_pg_class;

    /* Can we change the ownership of this tuple? */
    let mut new_owner_id = new_owner_id;
    match (*tuple_class).relkind as u8 {
        RELKIND_RELATION
        | RELKIND_VIEW
        | RELKIND_MATVIEW
        | RELKIND_FOREIGN_TABLE
        | RELKIND_PARTITIONED_TABLE => {
            /* ok to change owner */
        }
        RELKIND_INDEX => {
            if !recursing {
                /*
                 * Because ALTER INDEX OWNER used to be allowed, and in fact
                 * is generated by old versions of pg_dump, we give a warning
                 * and do nothing rather than erroring out.
                 */
                if (*tuple_class).relowner != new_owner_id {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "cannot change owner of index \"{}\"",
                            CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errhint("Change the ownership of the index's table instead.") */
                    );
                }
                /* quick hack to exit via the no-op path */
                new_owner_id = (*tuple_class).relowner;
            }
        }
        RELKIND_PARTITIONED_INDEX => {
            if recursing {
                /* ok */
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot change owner of index \"{}\"",
                        CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errhint("Change the ownership of the index's table instead.") */
                );
            }
        }
        RELKIND_SEQUENCE => {
            if !recursing && (*tuple_class).relowner != new_owner_id {
                /* if it's an owned sequence, disallow changing it by itself */
                let mut table_id: Oid = InvalidOid;
                let mut col_id: i32 = 0;

                if sequenceIsOwned(relation_oid, DEPENDENCY_AUTO, &mut table_id, &mut col_id)
                    || sequenceIsOwned(relation_oid, DEPENDENCY_INTERNAL, &mut table_id, &mut col_id)
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot change owner of sequence \"{}\"",
                            CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errdetail("Sequence \"%s\" is linked to table \"%s\".", ...) */
                    );
                }
            }
        }
        RELKIND_COMPOSITE_TYPE => {
            if recursing {
                /* ok */
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "\"{}\" is a composite type",
                        CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errhint("Use %s instead.", "ALTER TYPE") */
                );
            }
        }
        RELKIND_TOASTVALUE => {
            if !recursing {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot change owner of relation \"{}\"",
                        CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errdetail_relkind_not_supported(tuple_class->relkind) */
                );
            }
            /* else: fall through - same as default for recursing toast */
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change owner of relation \"{}\"",
                    CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errdetail_relkind_not_supported(tuple_class->relkind) */
            );
        }
    }

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded. This is for dump restoration purposes.
     */
    if (*tuple_class).relowner != new_owner_id {
        let mut repl_val: [Datum; Natts_pg_class] = [0; Natts_pg_class];
        let mut repl_null: [bool; Natts_pg_class] = [false; Natts_pg_class];
        let mut repl_repl: [bool; Natts_pg_class] = [false; Natts_pg_class];
        let new_acl: *mut Acl;
        let mut acl_datum: Datum;
        let mut is_null: bool = false;
        let newtuple: HeapTuple;

        /* skip permission checks when recursing to index or toast table */
        if !recursing {
            /* Superusers can always do it */
            if !superuser() {
                let namespace_oid: Oid = (*tuple_class).relnamespace;
                let aclresult: AclResult;

                /* Otherwise, must be owner of the existing object */
                if !object_ownercheck(RelationRelationId, relation_oid, GetUserId()) {
                    aclcheck_error(
                        ACLCHECK_NOT_OWNER,
                        get_relkind_objtype(get_rel_relkind(relation_oid)),
                        RelationGetRelationName(target_rel),
                    );
                }

                /* Must be able to become new owner */
                check_can_set_role(GetUserId(), new_owner_id);

                /* New owner must have CREATE privilege on namespace */
                aclresult = object_aclcheck(
                    NamespaceRelationId,
                    namespace_oid,
                    new_owner_id,
                    ACL_CREATE,
                );
                if aclresult != ACLCHECK_OK {
                    aclcheck_error(
                        aclresult,
                        OBJECT_SCHEMA,
                        get_namespace_name(namespace_oid),
                    );
                }
            }
        }

        libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
        libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

        repl_repl[Anum_pg_class_relowner - 1] = true;
        repl_val[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(new_owner_id);

        /*
         * Determine the modified ACL for the new owner. This is only
         * necessary when the ACL is non-null.
         */
        acl_datum = SysCacheGetAttr(
            RELOID,
            tuple,
            Anum_pg_class_relacl,
            &mut is_null,
        );
        if !is_null {
            new_acl = aclnewowner(
                DatumGetAclP(acl_datum),
                (*tuple_class).relowner,
                new_owner_id,
            );
            repl_repl[Anum_pg_class_relacl - 1] = true;
            repl_val[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);
        }

        newtuple = heap_modify_tuple(
            tuple,
            RelationGetDescr(class_rel),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(class_rel, &(*newtuple).t_self, newtuple);

        heap_freetuple(newtuple);

        /*
         * We must similarly update any per-column ACLs to reflect the new
         * owner; for neatness reasons that's split out as a subroutine.
         */
        change_owner_fix_column_acls(relation_oid, (*tuple_class).relowner, new_owner_id);

        /*
         * Update owner dependency reference, if any.
         */
        if (*tuple_class).relkind as u8 != RELKIND_COMPOSITE_TYPE
            && (*tuple_class).relkind as u8 != RELKIND_INDEX
            && (*tuple_class).relkind as u8 != RELKIND_PARTITIONED_INDEX
            && (*tuple_class).relkind as u8 != RELKIND_TOASTVALUE
        {
            changeDependencyOnOwner(RelationRelationId, relation_oid, new_owner_id);
        }

        /*
         * Also change the ownership of the table's row type, if it has one
         */
        if OidIsValid((*tuple_class).reltype) {
            AlterTypeOwnerInternal((*tuple_class).reltype, new_owner_id);
        }

        /*
         * If we are operating on a table or materialized view, also change
         * the ownership of any indexes and sequences that belong to the
         * relation, as well as its toast table (if it has one).
         */
        if (*tuple_class).relkind as u8 == RELKIND_RELATION
            || (*tuple_class).relkind as u8 == RELKIND_PARTITIONED_TABLE
            || (*tuple_class).relkind as u8 == RELKIND_MATVIEW
            || (*tuple_class).relkind as u8 == RELKIND_TOASTVALUE
        {
            let index_oid_list: *mut List = RelationGetIndexList(target_rel);
            let mut i: *mut ListCell = list_head(index_oid_list);
            while !i.is_null() {
                ATExecChangeOwner(lfirst_oid(i), new_owner_id, true, lockmode);
                i = lnext(index_oid_list, i);
            }
            list_free(index_oid_list);
        }

        /* If it has a toast table, recurse to change its ownership */
        if (*tuple_class).reltoastrelid != InvalidOid {
            ATExecChangeOwner((*tuple_class).reltoastrelid, new_owner_id, true, lockmode);
        }

        /* If it has dependent sequences, recurse to change them too */
        change_owner_recurse_to_sequences(relation_oid, new_owner_id, lockmode);
    }

    InvokeObjectPostAlterHook(RelationRelationId, relation_oid, 0);

    ReleaseSysCache(tuple);
    table_close(class_rel, RowExclusiveLock);
    relation_close(target_rel, NoLock);
}

// ---------------------------------------------------------------------------
// change_owner_fix_column_acls
// ---------------------------------------------------------------------------

/// Helper function for ATExecChangeOwner. Scan the columns of the table
/// and fix any non-null column ACLs to reflect the new owner.
unsafe fn change_owner_fix_column_acls(
    relation_oid: Oid,
    old_owner_id: Oid,
    new_owner_id: Oid,
) {
    let att_relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let mut attribute_tuple: HeapTuple;

    att_relation = table_open(AttributeRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relation_oid),
    );
    scan = systable_beginscan(
        att_relation,
        AttributeRelidNumIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );
    loop {
        attribute_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(attribute_tuple) {
            break;
        }
        let att = GETSTRUCT(attribute_tuple) as Form_pg_attribute;
        let mut repl_val: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
        let mut repl_null: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
        let mut repl_repl: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
        let new_acl: *mut Acl;
        let acl_datum: Datum;
        let mut is_null: bool = false;
        let newtuple: HeapTuple;

        /* Ignore dropped columns */
        if (*att).attisdropped {
            continue;
        }

        acl_datum = heap_getattr(
            attribute_tuple,
            Anum_pg_attribute_attacl,
            RelationGetDescr(att_relation),
            &mut is_null,
        );
        /* Null ACLs do not require changes */
        if is_null {
            continue;
        }

        libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
        libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

        new_acl = aclnewowner(DatumGetAclP(acl_datum), old_owner_id, new_owner_id);
        repl_repl[Anum_pg_attribute_attacl - 1] = true;
        repl_val[Anum_pg_attribute_attacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(
            attribute_tuple,
            RelationGetDescr(att_relation),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(att_relation, &(*newtuple).t_self, newtuple);

        heap_freetuple(newtuple);
    }
    systable_endscan(scan);
    table_close(att_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// change_owner_recurse_to_sequences
// ---------------------------------------------------------------------------

/// Helper function for ATExecChangeOwner. Examines pg_depend searching
/// for sequences that are dependent on serial columns, and changes their
/// ownership.
unsafe fn change_owner_recurse_to_sequences(
    relation_oid: Oid,
    new_owner_id: Oid,
    lockmode: LOCKMODE,
) {
    let dep_rel: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;

    /*
     * SERIAL sequences are those having an auto dependency on one of the
     * table's columns (we don't care *which* column, exactly).
     */
    dep_rel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relation_oid),
    );
    /* we leave refobjsubid unspecified */

    scan = systable_beginscan(dep_rel, DependReferenceIndexId, true, core::ptr::null_mut(), 2, key.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let dep_form: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;
        let seq_rel: Relation;

        /* skip dependencies other than auto dependencies on columns */
        if (*dep_form).refobjsubid == 0
            || (*dep_form).classid != RelationRelationId
            || (*dep_form).objsubid != 0
            || !((*dep_form).deptype == DEPENDENCY_AUTO as libc::c_char
                || (*dep_form).deptype == DEPENDENCY_INTERNAL as libc::c_char)
        {
            continue;
        }

        /* Use relation_open just in case it's an index */
        seq_rel = relation_open((*dep_form).objid, lockmode);

        /* skip non-sequence relations */
        if (*RelationGetForm(seq_rel)).relkind as u8 != RELKIND_SEQUENCE {
            /* No need to keep the lock */
            relation_close(seq_rel, lockmode);
            continue;
        }

        /* We don't need to close the sequence while we alter it. */
        ATExecChangeOwner((*dep_form).objid, new_owner_id, true, lockmode);

        /* Now we can close it. Keep the lock till end of transaction. */
        relation_close(seq_rel, NoLock);
    }

    systable_endscan(scan);

    relation_close(dep_rel, AccessShareLock);
}

// ---------------------------------------------------------------------------
// ATExecClusterOn
// ---------------------------------------------------------------------------

/// ALTER TABLE CLUSTER ON
///
/// The only thing we have to do is to change the indisclustered bits.
/// Return the address of the new clustering index.
unsafe fn ATExecClusterOn(
    rel: Relation,
    index_name: *const libc::c_char,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let index_oid: Oid;
    let address: ObjectAddress;

    index_oid = get_relname_relid(index_name, (*(*rel).rd_rel).relnamespace);

    if !OidIsValid(index_oid) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" for table \"{}\" does not exist",
                CStr::from_ptr(index_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    /* Check index is valid to cluster on */
    check_index_is_clusterable(rel, index_oid, lockmode);

    /* And do the work */
    mark_index_clustered(rel, index_oid, false);

    ObjectAddressSet!(address, RelationRelationId, index_oid);

    address
}

// ---------------------------------------------------------------------------
// ATExecDropCluster
// ---------------------------------------------------------------------------

/// ALTER TABLE SET WITHOUT CLUSTER
///
/// We have to find any indexes on the table that have indisclustered bit
/// set and turn it off.
unsafe fn ATExecDropCluster(rel: Relation, lockmode: LOCKMODE) {
    mark_index_clustered(rel, InvalidOid, false);
}

// ---------------------------------------------------------------------------
// ATPrepSetAccessMethod
// ---------------------------------------------------------------------------

/// Preparation phase for SET ACCESS METHOD
///
/// Check that the access method exists and determine whether a change is
/// actually needed.
unsafe fn ATPrepSetAccessMethod(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    amname: *const libc::c_char,
) {
    let amoid: Oid;

    /*
     * Look up the access method name and check that it differs from the
     * table's current AM. If DEFAULT was specified for a partitioned table
     * (amname is NULL), set it to InvalidOid to reset the catalogued AM.
     */
    if !amname.is_null() {
        amoid = get_table_am_oid(amname, false);
    } else if (*(*rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        amoid = InvalidOid;
    } else {
        amoid = get_table_am_oid(default_table_access_method, false);
    }

    /* if it's a match, phase 3 doesn't need to do anything */
    if (*(*rel).rd_rel).relam == amoid {
        return;
    }

    /* Save info for Phase 3 to do the real work */
    (*tab).rewrite |= AT_REWRITE_ACCESS_METHOD;
    (*tab).newAccessMethod = amoid;
    (*tab).chgAccessMethod = true;
}

// ---------------------------------------------------------------------------
// ATExecSetAccessMethodNoStorage
// ---------------------------------------------------------------------------

/// Special handling of ALTER TABLE SET ACCESS METHOD for relations with no
/// storage that have an interest in preserving AM.
///
/// Since these have no storage, setting the access method is a catalog only
/// operation.
unsafe fn ATExecSetAccessMethodNoStorage(rel: Relation, new_access_method_id: Oid) {
    let pg_class: Relation;
    let old_access_method_id: Oid;
    let tuple: HeapTuple;
    let rd_rel: Form_pg_class;
    let reloid: Oid = RelationGetRelid(rel);

    /*
     * Shouldn't be called on relations having storage; these are processed in
     * phase 3.
     */
    Assert!(!RELKIND_HAS_STORAGE!((*(*rel).rd_rel).relkind as u8));

    /* Get a modifiable copy of the relation's pg_class row. */
    pg_class = table_open(RelationRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(reloid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", reloid);
    }
    rd_rel = GETSTRUCT(tuple) as Form_pg_class;

    /* Update the pg_class row. */
    old_access_method_id = (*rd_rel).relam;
    (*rd_rel).relam = new_access_method_id;

    /* Leave if no update required */
    if (*rd_rel).relam == old_access_method_id {
        heap_freetuple(tuple);
        table_close(pg_class, RowExclusiveLock);
        return;
    }

    CatalogTupleUpdate(pg_class, &(*tuple).t_self, tuple);

    /*
     * Update the dependency on the new access method. No dependency is added
     * if the new access method is InvalidOid (default case).
     */
    if !OidIsValid(old_access_method_id) && OidIsValid((*rd_rel).relam) {
        let mut relobj: ObjectAddress = core::mem::zeroed();
        let mut referenced: ObjectAddress = core::mem::zeroed();

        /*
         * New access method is defined and there was no dependency
         * previously, so record a new one.
         */
        ObjectAddressSet!(relobj, RelationRelationId, reloid);
        ObjectAddressSet!(referenced, AccessMethodRelationId, (*rd_rel).relam);
        recordDependencyOn(&relobj, &referenced, DEPENDENCY_NORMAL);
    } else if OidIsValid(old_access_method_id) && !OidIsValid((*rd_rel).relam) {
        /*
         * There was an access method defined, and no new one, so just remove
         * the existing dependency.
         */
        deleteDependencyRecordsForClass(
            RelationRelationId,
            reloid,
            AccessMethodRelationId,
            DEPENDENCY_NORMAL,
        );
    } else {
        Assert!(OidIsValid(old_access_method_id) && OidIsValid((*rd_rel).relam));

        /* Both are valid, so update the dependency */
        changeDependencyFor(
            RelationRelationId,
            reloid,
            AccessMethodRelationId,
            old_access_method_id,
            (*rd_rel).relam,
        );
    }

    /* make the relam and dependency changes visible */
    CommandCounterIncrement();

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    heap_freetuple(tuple);
    table_close(pg_class, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATPrepSetTableSpace
// ---------------------------------------------------------------------------

/// ALTER TABLE SET TABLESPACE
unsafe fn ATPrepSetTableSpace(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    tablespacename: *const libc::c_char,
    lockmode: LOCKMODE,
) {
    let tablespace_id: Oid;

    /* Check that the tablespace exists */
    tablespace_id = get_tablespace_oid(tablespacename, false);

    /* Check permissions except when moving to database's default */
    if OidIsValid(tablespace_id) && tablespace_id != MyDatabaseTableSpace {
        let aclresult: AclResult = object_aclcheck(
            TableSpaceRelationId,
            tablespace_id,
            GetUserId(),
            ACL_CREATE,
        );
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TABLESPACE, tablespacename);
        }
    }

    /* Save info for Phase 3 to do the real work */
    if OidIsValid((*tab).newTableSpace) {
        ereport!(
            ERROR,
            errmsg!("cannot have multiple SET TABLESPACE subcommands")
            /* errcode(ERRCODE_SYNTAX_ERROR) */
        );
    }

    (*tab).newTableSpace = tablespace_id;
}

// ---------------------------------------------------------------------------
// ATExecSetRelOptions
// ---------------------------------------------------------------------------

/// Set, reset, or replace reloptions.
unsafe fn ATExecSetRelOptions(
    rel: Relation,
    def_list: *mut List,
    operation: AlterTableType,
    lockmode: LOCKMODE,
) {
    let relid: Oid;
    let pgclass: Relation;
    let mut tuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut datum: Datum;
    let new_options: Datum;
    let mut repl_val: [Datum; Natts_pg_class] = [0; Natts_pg_class];
    let mut repl_null: [bool; Natts_pg_class] = [false; Natts_pg_class];
    let mut repl_repl: [bool; Natts_pg_class] = [false; Natts_pg_class];
    let valid_nsps: &[*const libc::c_char] = HEAP_RELOPT_NAMESPACES;

    if def_list == NIL && operation != AT_ReplaceRelOptions {
        return; /* nothing to do */
    }

    pgclass = table_open(RelationRelationId, RowExclusiveLock);

    /* Fetch heap tuple */
    relid = RelationGetRelid(rel);
    tuple = SearchSysCache1Locked(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }

    if operation == AT_ReplaceRelOptions {
        /*
         * If we're supposed to replace the reloptions list, we just pretend
         * there were none before.
         */
        datum = 0 as Datum;
    } else {
        let mut isnull: bool = false;
        /* Get the old reloptions */
        datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &mut isnull);
        if isnull {
            datum = 0 as Datum;
        }
    }

    /* Generate new proposed reloptions (text array) */
    let new_options = transformRelOptions(
        datum,
        def_list,
        core::ptr::null_mut(),
        valid_nsps.as_ptr() as *mut *const libc::c_char,
        false,
        operation == AT_ResetRelOptions,
    );

    /* Validate */
    match (*(*rel).rd_rel).relkind as u8 {
        RELKIND_RELATION | RELKIND_MATVIEW => {
            let _ = heap_reloptions((*(*rel).rd_rel).relkind, new_options, true);
        }
        RELKIND_PARTITIONED_TABLE => {
            let _ = partitioned_table_reloptions(new_options, true);
        }
        RELKIND_VIEW => {
            let _ = view_reloptions(new_options, true);
        }
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => {
            let _ = index_reloptions((*(*rel).rd_indam).amoptions, new_options, true);
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot set options for relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errdetail_relkind_not_supported(rel->rd_rel->relkind) */
            );
        }
    }

    /* Special-case validation of view options */
    if (*(*rel).rd_rel).relkind as u8 == RELKIND_VIEW {
        let view_query: *mut Query = get_view_query(rel);
        let view_options: *mut List = untransformRelOptions(new_options);
        let mut check_option: bool = false;
        let mut cell: *mut ListCell = list_head(view_options);
        while !cell.is_null() {
            let defel: *mut DefElem = lfirst(cell) as *mut DefElem;
            if libc::strcmp((*defel).defname, cstr!("check_option")) == 0 {
                check_option = true;
            }
            cell = lnext(view_options, cell);
        }

        /*
         * If the check option is specified, look to see if the view is
         * actually auto-updatable or not.
         */
        if check_option {
            let view_updatable_error: *const libc::c_char =
                view_query_is_auto_updatable(view_query, true);
            if !view_updatable_error.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("WITH CHECK OPTION is supported only on automatically updatable views")
                    /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errhint("%s", _(view_updatable_error)) */
                );
            }
        }
    }

    /*
     * All we need do here is update the pg_class row; the new options will be
     * propagated into relcaches during post-commit cache inval.
     */
    libc::memset(repl_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_val));
    libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

    if new_options != 0 as Datum {
        repl_val[Anum_pg_class_reloptions - 1] = new_options;
    } else {
        repl_null[Anum_pg_class_reloptions - 1] = true;
    }

    repl_repl[Anum_pg_class_reloptions - 1] = true;

    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(pgclass),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(pgclass, &(*newtuple).t_self, newtuple);
    UnlockTuple(pgclass, &(*tuple).t_self, InplaceUpdateTupleLock);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    heap_freetuple(newtuple);

    ReleaseSysCache(tuple);

    /* repeat the whole exercise for the toast table, if there's one */
    if OidIsValid((*(*rel).rd_rel).reltoastrelid) {
        let toastrel: Relation;
        let toastid: Oid = (*(*rel).rd_rel).reltoastrelid;

        toastrel = table_open(toastid, lockmode);

        /* Fetch heap tuple */
        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", toastid);
        }

        if operation == AT_ReplaceRelOptions {
            datum = 0 as Datum;
        } else {
            let mut isnull: bool = false;
            datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &mut isnull);
            if isnull {
                datum = 0 as Datum;
            }
        }

        let new_options = transformRelOptions(
            datum,
            def_list,
            cstr!("toast"),
            valid_nsps.as_ptr() as *mut *const libc::c_char,
            false,
            operation == AT_ResetRelOptions,
        );

        let _ = heap_reloptions(RELKIND_TOASTVALUE as libc::c_char, new_options, true);

        libc::memset(repl_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_val));
        libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
        libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

        if new_options != 0 as Datum {
            repl_val[Anum_pg_class_reloptions - 1] = new_options;
        } else {
            repl_null[Anum_pg_class_reloptions - 1] = true;
        }

        repl_repl[Anum_pg_class_reloptions - 1] = true;

        let newtuple = heap_modify_tuple(
            tuple,
            RelationGetDescr(pgclass),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(pgclass, &(*newtuple).t_self, newtuple);

        InvokeObjectPostAlterHookArg(
            RelationRelationId,
            RelationGetRelid(toastrel),
            0,
            InvalidOid,
            true,
        );

        heap_freetuple(newtuple);

        ReleaseSysCache(tuple);

        table_close(toastrel, NoLock);
    }

    table_close(pgclass, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecSetTableSpace
// ---------------------------------------------------------------------------

/// Execute ALTER TABLE SET TABLESPACE for cases where there is no tuple
/// rewriting to be done, so we just want to copy the data as fast as possible.
unsafe fn ATExecSetTableSpace(
    table_oid: Oid,
    new_table_space: Oid,
    lockmode: LOCKMODE,
) {
    let rel: Relation;
    let reltoastrelid: Oid;
    let newrelfilenumber: RelFileNumber;
    let mut newrlocator: RelFileLocator;
    let mut reltoastidxids: *mut List = NIL;
    let mut lc: *mut ListCell;

    /*
     * Need lock here in case we are recursing to toast table or index
     */
    rel = relation_open(table_oid, lockmode);

    /* Check first if relation can be moved to new tablespace */
    if !CheckRelationTableSpaceMove(rel, new_table_space) {
        InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
        relation_close(rel, NoLock);
        return;
    }

    reltoastrelid = (*(*rel).rd_rel).reltoastrelid;
    /* Fetch the list of indexes on toast relation if necessary */
    if OidIsValid(reltoastrelid) {
        let toast_rel: Relation = relation_open(reltoastrelid, lockmode);
        reltoastidxids = RelationGetIndexList(toast_rel);
        relation_close(toast_rel, lockmode);
    }

    /*
     * Relfilenumbers are not unique in databases across tablespaces, so we
     * need to allocate a new one in the new tablespace.
     */
    newrelfilenumber = GetNewRelFileNumber(
        new_table_space,
        core::ptr::null_mut(),
        (*(*rel).rd_rel).relpersistence,
    );

    /* Open old and new relation */
    newrlocator = (*rel).rd_locator;
    newrlocator.relNumber = newrelfilenumber;
    newrlocator.spcOid = new_table_space;

    /* hand off to AM to actually create new rel storage and copy the data */
    if (*(*rel).rd_rel).relkind as u8 == RELKIND_INDEX {
        index_copy_data(rel, newrlocator);
    } else {
        Assert!(RELKIND_HAS_TABLE_AM!((*(*rel).rd_rel).relkind as u8));
        table_relation_copy_data(rel, &newrlocator);
    }

    /*
     * Update the pg_class row.
     *
     * NB: This wouldn't work if ATExecSetTableSpace() were allowed to be
     * executed on pg_class or its indexes, but that's forbidden with
     * CheckRelationTableSpaceMove().
     */
    SetRelationTableSpace(rel, new_table_space, newrelfilenumber);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    RelationAssumeNewRelfilelocator(rel);

    relation_close(rel, NoLock);

    /* Make sure the reltablespace change is visible */
    CommandCounterIncrement();

    /* Move associated toast relation and/or indexes, too */
    if OidIsValid(reltoastrelid) {
        ATExecSetTableSpace(reltoastrelid, new_table_space, lockmode);
    }
    lc = list_head(reltoastidxids);
    while !lc.is_null() {
        ATExecSetTableSpace(lfirst_oid(lc), new_table_space, lockmode);
        lc = lnext(reltoastidxids, lc);
    }

    /* Clean up */
    list_free(reltoastidxids);
}

// ---------------------------------------------------------------------------
// ATExecSetTableSpaceNoStorage
// ---------------------------------------------------------------------------

/// Special handling of ALTER TABLE SET TABLESPACE for relations with no
/// storage that have an interest in preserving tablespace.
///
/// Since these have no storage the tablespace can be updated with a simple
/// metadata only operation to update the tablespace.
unsafe fn ATExecSetTableSpaceNoStorage(rel: Relation, new_table_space: Oid) {
    /*
     * Shouldn't be called on relations having storage; these are processed in
     * phase 3.
     */
    Assert!(!RELKIND_HAS_STORAGE!((*(*rel).rd_rel).relkind as u8));

    /* check if relation can be moved to its new tablespace */
    if !CheckRelationTableSpaceMove(rel, new_table_space) {
        InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
        return;
    }

    /* Update can be done, so change reltablespace */
    SetRelationTableSpace(rel, new_table_space, InvalidOid);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    /* Make sure the reltablespace change is visible */
    CommandCounterIncrement();
}

// ---------------------------------------------------------------------------
// AlterTableMoveAll
// ---------------------------------------------------------------------------

/// Alter Table ALL ... SET TABLESPACE
///
/// Allows a user to move all objects of some type in a given tablespace in the
/// current database to another tablespace.
pub unsafe fn AlterTableMoveAll(stmt: *mut AlterTableMoveAllStmt) -> Oid {
    let mut relations: *mut List = NIL;
    let mut l: *mut ListCell;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let rel: Relation;
    let scan: TableScanDesc;
    let mut tuple: HeapTuple;
    let orig_tablespaceoid: Oid;
    let new_tablespaceoid: Oid;
    let role_oids: *mut List = roleSpecsToIds((*stmt).roles);

    /* Ensure we were not asked to move something we can't */
    if (*stmt).objtype != OBJECT_TABLE
        && (*stmt).objtype != OBJECT_INDEX
        && (*stmt).objtype != OBJECT_MATVIEW
    {
        ereport!(
            ERROR,
            errmsg!("only tables, indexes, and materialized views exist in tablespaces")
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    /* Get the orig and new tablespace OIDs */
    orig_tablespaceoid = get_tablespace_oid((*stmt).orig_tablespacename, false);
    let mut new_tablespaceoid = get_tablespace_oid((*stmt).new_tablespacename, false);

    /* Can't move shared relations in to or out of pg_global */
    if orig_tablespaceoid == GLOBALTABLESPACE_OID || new_tablespaceoid == GLOBALTABLESPACE_OID {
        ereport!(
            ERROR,
            errmsg!("cannot move relations in to or out of pg_global tablespace")
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    /*
     * Must have CREATE rights on the new tablespace, unless it is the
     * database default tablespace.
     */
    if OidIsValid(new_tablespaceoid) && new_tablespaceoid != MyDatabaseTableSpace {
        let aclresult: AclResult = object_aclcheck(
            TableSpaceRelationId,
            new_tablespaceoid,
            GetUserId(),
            ACL_CREATE,
        );
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TABLESPACE, get_tablespace_name(new_tablespaceoid));
        }
    }

    /*
     * Now that the checks are done, check if we should set either to
     * InvalidOid because it is our database's default tablespace.
     */
    let mut orig_tablespaceoid = orig_tablespaceoid;
    if orig_tablespaceoid == MyDatabaseTableSpace {
        orig_tablespaceoid = InvalidOid;
    }
    if new_tablespaceoid == MyDatabaseTableSpace {
        new_tablespaceoid = InvalidOid;
    }

    /* no-op */
    if orig_tablespaceoid == new_tablespaceoid {
        return new_tablespaceoid;
    }

    /*
     * Walk the list of objects in the tablespace and move them. This will
     * only find objects in our database, of course.
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_reltablespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(orig_tablespaceoid),
    );

    rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 1, key.as_mut_ptr());
    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let rel_form: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        let rel_oid: Oid = (*rel_form).oid;

        /*
         * Do not move objects in pg_catalog as part of this.
         * Also, explicitly avoid any shared tables, temp tables, or TOAST.
         */
        if IsCatalogNamespace((*rel_form).relnamespace)
            || (*rel_form).relisshared
            || isAnyTempNamespace((*rel_form).relnamespace)
            || IsToastNamespace((*rel_form).relnamespace)
        {
            continue;
        }

        /* Only move the object type requested */
        if ((*stmt).objtype == OBJECT_TABLE
            && (*rel_form).relkind as u8 != RELKIND_RELATION
            && (*rel_form).relkind as u8 != RELKIND_PARTITIONED_TABLE)
            || ((*stmt).objtype == OBJECT_INDEX
                && (*rel_form).relkind as u8 != RELKIND_INDEX
                && (*rel_form).relkind as u8 != RELKIND_PARTITIONED_INDEX)
            || ((*stmt).objtype == OBJECT_MATVIEW
                && (*rel_form).relkind as u8 != RELKIND_MATVIEW)
        {
            continue;
        }

        /* Check if we are only moving objects owned by certain roles */
        if role_oids != NIL && !list_member_oid(role_oids, (*rel_form).relowner) {
            continue;
        }

        /*
         * Handle permissions-checking here since we are locking the tables
         * and also to avoid doing a bunch of work only to fail part-way.
         */
        if !object_ownercheck(RelationRelationId, rel_oid, GetUserId()) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                get_relkind_objtype(get_rel_relkind(rel_oid)),
                NameStr!((*rel_form).relname),
            );
        }

        if (*stmt).nowait && !ConditionalLockRelationOid(rel_oid, AccessExclusiveLock) {
            ereport!(
                ERROR,
                errmsg!(
                    "aborting because lock on relation \"{}.{}\" is not available",
                    CStr::from_ptr(get_namespace_name((*rel_form).relnamespace)).to_string_lossy(),
                    CStr::from_ptr(NameStr!((*rel_form).relname)).to_string_lossy()
                ) /* errcode(ERRCODE_OBJECT_IN_USE) */
            );
        } else {
            LockRelationOid(rel_oid, AccessExclusiveLock);
        }

        /* Add to our list of objects to move */
        relations = lappend_oid(relations, rel_oid);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    if relations == NIL {
        ereport!(
            NOTICE,
            errmsg!(
                "no matching relations in tablespace \"{}\" found",
                if orig_tablespaceoid == InvalidOid {
                    "(database default)"
                } else {
                    CStr::from_ptr(get_tablespace_name(orig_tablespaceoid))
                        .to_str()
                        .unwrap_or("?")
                }
            ) /* errcode(ERRCODE_NO_DATA_FOUND) */
        );
    }

    /* Everything is locked, loop through and move all of the relations. */
    l = list_head(relations);
    while !l.is_null() {
        let mut cmds: *mut List = NIL;
        let cmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

        (*cmd).subtype = AT_SetTableSpace;
        (*cmd).name = (*stmt).new_tablespacename;

        cmds = lappend(cmds, cmd as *mut libc::c_void);

        EventTriggerAlterTableStart(stmt as *mut Node);
        /* OID is set by AlterTableInternal */
        AlterTableInternal(lfirst_oid(l), cmds, false);
        EventTriggerAlterTableEnd();

        l = lnext(relations, l);
    }

    new_tablespaceoid
}

// ---------------------------------------------------------------------------
// index_copy_data
// ---------------------------------------------------------------------------

unsafe fn index_copy_data(rel: Relation, newrlocator: RelFileLocator) {
    let dstrel: SMgrRelation;

    /*
     * Since we copy the file directly without looking at the shared buffers,
     * we'd better first flush out any pages of the source relation that are
     * in shared buffers.
     */
    FlushRelationBuffers(rel);

    /*
     * Create and copy all forks of the relation, and schedule unlinking of
     * old physical files.
     *
     * NOTE: any conflict in relfilenumber value will be caught in
     * RelationCreateStorage().
     */
    dstrel = RelationCreateStorage(newrlocator, (*(*rel).rd_rel).relpersistence, true);

    /* copy main fork */
    RelationCopyStorage(
        RelationGetSmgr(rel),
        dstrel,
        MAIN_FORKNUM,
        (*(*rel).rd_rel).relpersistence,
    );

    /* copy those extra forks that exist */
    let mut fork_num: ForkNumber = MAIN_FORKNUM + 1;
    while fork_num <= MAX_FORKNUM {
        if smgrexists(RelationGetSmgr(rel), fork_num) {
            smgrcreate(dstrel, fork_num, false);

            /*
             * WAL log creation if the relation is persistent, or this is the
             * init fork of an unlogged relation.
             */
            if RelationIsPermanent(rel)
                || ((*(*rel).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED
                    && fork_num == INIT_FORKNUM)
            {
                log_smgrcreate(&newrlocator, fork_num);
            }
            RelationCopyStorage(
                RelationGetSmgr(rel),
                dstrel,
                fork_num,
                (*(*rel).rd_rel).relpersistence,
            );
        }
        fork_num += 1;
    }

    /* drop old relation, and close new one */
    RelationDropStorage(rel);
    smgrclose(dstrel);
}

// ---------------------------------------------------------------------------
// ATExecEnableDisableTrigger
// ---------------------------------------------------------------------------

/// ALTER TABLE ENABLE/DISABLE TRIGGER
///
/// We just pass this off to trigger.c.
unsafe fn ATExecEnableDisableTrigger(
    rel: Relation,
    trigname: *const libc::c_char,
    fires_when: libc::c_char,
    skip_system: bool,
    recurse: bool,
    lockmode: LOCKMODE,
) {
    EnableDisableTrigger(
        rel,
        trigname,
        InvalidOid,
        fires_when,
        skip_system,
        recurse,
        lockmode,
    );

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
}

// ---------------------------------------------------------------------------
// ATExecEnableDisableRule
// ---------------------------------------------------------------------------

/// ALTER TABLE ENABLE/DISABLE RULE
///
/// We just pass this off to rewriteDefine.c.
unsafe fn ATExecEnableDisableRule(
    rel: Relation,
    rulename: *const libc::c_char,
    fires_when: libc::c_char,
    lockmode: LOCKMODE,
) {
    EnableDisableRule(rel, rulename, fires_when);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
}

// ---------------------------------------------------------------------------
// ATPrepAddInherit
// ---------------------------------------------------------------------------

/// ALTER TABLE INHERIT
///
/// Add a parent to the child's parents.
unsafe fn ATPrepAddInherit(child_rel: Relation) {
    if (*(*child_rel).rd_rel).reloftype != InvalidOid {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of typed table")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*child_rel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of a partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*child_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of partitioned table")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
}

// ---------------------------------------------------------------------------
// ATExecAddInherit
// ---------------------------------------------------------------------------

/// Return the address of the new parent relation.
unsafe fn ATExecAddInherit(
    child_rel: Relation,
    parent: *mut RangeVar,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let parent_rel: Relation;
    let children: *mut List;
    let address: ObjectAddress;
    let trigger_name: *const libc::c_char;

    /*
     * A self-exclusive lock is needed here. See the similar case in
     * MergeAttributes() for a full explanation.
     */
    parent_rel = table_openrv(parent, ShareUpdateExclusiveLock);

    /*
     * Must be owner of both parent and child -- child was checked by
     * ATSimplePermissions call in ATPrepCmd
     */
    ATSimplePermissions(
        AT_AddInherit,
        parent_rel,
        ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
    );

    /* Permanent rels cannot inherit from temporary ones */
    if (*(*parent_rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        && (*(*child_rel).rd_rel).relpersistence != RELPERSISTENCE_TEMP
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot inherit from temporary relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* If parent rel is temp, it must belong to this session */
    if (*(*parent_rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        && !(*parent_rel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot inherit from temporary relation of another session")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Ditto for the child */
    if (*(*child_rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        && !(*child_rel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot inherit to temporary relation of another session")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Prevent partitioned tables from becoming inheritance parents */
    if (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        ereport!(
            ERROR,
            errmsg!(
                "cannot inherit from partitioned table \"{}\"",
                CStr::from_ptr((*parent).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Likewise for partitions */
    if (*(*parent_rel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!("cannot inherit from a partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * Prevent circularity by seeing if proposed parent inherits from child.
     * (In particular, this disallows making a rel inherit from itself.)
     *
     * We use weakest lock we can on child's children, namely AccessShareLock.
     */
    children = find_all_inheritors(RelationGetRelid(child_rel), AccessShareLock, core::ptr::null_mut());

    if list_member_oid(children, RelationGetRelid(parent_rel)) {
        ereport!(
            ERROR,
            errmsg!("circular inheritance not allowed")
            /* errcode(ERRCODE_DUPLICATE_TABLE),
               errdetail("\"%s\" is already a child of \"%s\".", ...) */
        );
    }

    /*
     * If child_rel has row-level triggers with transition tables, we
     * currently don't allow it to become an inheritance child.
     */
    trigger_name = FindTriggerIncompatibleWithInheritance((*child_rel).trigdesc);
    if !trigger_name.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "trigger \"{}\" prevents table \"{}\" from becoming an inheritance child",
                CStr::from_ptr(trigger_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
            )
            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("ROW triggers with transition tables are not supported in inheritance hierarchies.") */
        );
    }

    /* OK to create inheritance */
    CreateInheritance(child_rel, parent_rel, false);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(parent_rel));

    /* keep our lock on the parent relation until commit */
    table_close(parent_rel, NoLock);

    address
}

// ---------------------------------------------------------------------------
// CreateInheritance
// ---------------------------------------------------------------------------

/// Catalog manipulation portion of creating inheritance between a child
/// table and a parent table.
///
/// Common to ATExecAddInherit() and ATExecAttachPartition().
unsafe fn CreateInheritance(child_rel: Relation, parent_rel: Relation, ispartition: bool) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut inherits_tuple: HeapTuple;
    let mut inhseqno: i32;

    /* Note: get RowExclusiveLock because we will write pg_inherits below. */
    catalog_relation = table_open(InheritsRelationId, RowExclusiveLock);

    /*
     * Check for duplicates in the list of parents, and determine the highest
     * inhseqno already present; we'll use the next one for the new parent.
     * Also, if proposed child is a partition, it cannot already be inheriting.
     *
     * Note: we do not reject the case where the child already inherits from
     * the parent indirectly; CREATE TABLE doesn't reject comparable cases.
     */
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(child_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    /* inhseqno sequences start at 1 */
    inhseqno = 0;
    loop {
        inherits_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(inherits_tuple) {
            break;
        }
        let inh: Form_pg_inherits = GETSTRUCT(inherits_tuple) as Form_pg_inherits;

        if (*inh).inhparent == RelationGetRelid(parent_rel) {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" would be inherited from more than once",
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
                ) /* errcode(ERRCODE_DUPLICATE_TABLE) */
            );
        }

        if (*inh).inhseqno > inhseqno {
            inhseqno = (*inh).inhseqno;
        }
    }
    systable_endscan(scan);

    /* Match up the columns and bump attinhcount as needed */
    MergeAttributesIntoExisting(child_rel, parent_rel, ispartition);

    /* Match up the constraints and bump coninhcount as needed */
    MergeConstraintsIntoExisting(child_rel, parent_rel);

    /*
     * OK, it looks valid. Make the catalog entries that show inheritance.
     */
    StoreCatalogInheritance1(
        RelationGetRelid(child_rel),
        RelationGetRelid(parent_rel),
        inhseqno + 1,
        catalog_relation,
        (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE,
    );

    /* Now we're done with pg_inherits */
    table_close(catalog_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// decompile_conbin
// ---------------------------------------------------------------------------

/// Obtain the source-text form of the constraint expression for a check
/// constraint, given its pg_constraint tuple
unsafe fn decompile_conbin(contup: HeapTuple, tupdesc: TupleDesc) -> *mut libc::c_char {
    let con: Form_pg_constraint;
    let mut isnull: bool = false;
    let attr: Datum;
    let expr: Datum;

    con = GETSTRUCT(contup) as Form_pg_constraint;
    attr = heap_getattr(contup, Anum_pg_constraint_conbin, tupdesc, &mut isnull);
    if isnull {
        elog!(ERROR, "null conbin for constraint {}", (*con).oid);
    }

    expr = DirectFunctionCall2(pg_get_expr, attr, ObjectIdGetDatum((*con).conrelid));
    TextDatumGetCString(expr)
}

// ---------------------------------------------------------------------------
// constraints_equivalent
// ---------------------------------------------------------------------------

/// Determine whether two check constraints are functionally equivalent
///
/// The test we apply is to see whether they reverse-compile to the same
/// source string.
///
/// Note that we ignore enforceability as there are cases where constraints
/// with differing enforceability are allowed.
unsafe fn constraints_equivalent(
    a: HeapTuple,
    b: HeapTuple,
    tuple_desc: TupleDesc,
) -> bool {
    let acon: Form_pg_constraint = GETSTRUCT(a) as Form_pg_constraint;
    let bcon: Form_pg_constraint = GETSTRUCT(b) as Form_pg_constraint;

    if (*acon).condeferrable != (*bcon).condeferrable
        || (*acon).condeferred != (*bcon).condeferred
        || libc::strcmp(
            decompile_conbin(a, tuple_desc),
            decompile_conbin(b, tuple_desc),
        ) != 0
    {
        false
    } else {
        true
    }
}

// ---------------------------------------------------------------------------
// MergeAttributesIntoExisting
// ---------------------------------------------------------------------------

/// Check columns in child table match up with columns in parent, and increment
/// their attinhcount.
///
/// Called by CreateInheritance
unsafe fn MergeAttributesIntoExisting(
    child_rel: Relation,
    parent_rel: Relation,
    ispartition: bool,
) {
    let attrrel: Relation;
    let parent_desc: TupleDesc;

    attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    parent_desc = RelationGetDescr(parent_rel);

    let mut parent_attno: AttrNumber = 1;
    while parent_attno <= (*parent_desc).natts as AttrNumber {
        let parent_att: Form_pg_attribute =
            TupleDescAttr(parent_desc, (parent_attno - 1) as usize) as Form_pg_attribute;
        let parent_attname: *const libc::c_char = NameStr!((*parent_att).attname);
        let tuple: HeapTuple;

        /* Ignore dropped columns in the parent. */
        if (*parent_att).attisdropped {
            parent_attno += 1;
            continue;
        }

        /* Find same column in child (matching on column name). */
        tuple = SearchSysCacheCopyAttName(RelationGetRelid(child_rel), parent_attname);
        if HeapTupleIsValid(tuple) {
            let child_att: Form_pg_attribute = GETSTRUCT(tuple) as Form_pg_attribute;

            if (*parent_att).atttypid != (*child_att).atttypid
                || (*parent_att).atttypmod != (*child_att).atttypmod
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "child table \"{}\" has different type for column \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            if (*parent_att).attcollation != (*child_att).attcollation {
                ereport!(
                    ERROR,
                    errmsg!(
                        "child table \"{}\" has different collation for column \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_COLLATION_MISMATCH) */
                );
            }

            /*
             * If the parent has a not-null constraint that's not NO INHERIT,
             * make sure the child has one too.
             */
            if (*parent_att).attnotnull && !(*child_att).attnotnull {
                let contup: HeapTuple = findNotNullConstraintAttnum(
                    RelationGetRelid(parent_rel),
                    (*parent_att).attnum,
                );
                if HeapTupleIsValid(contup)
                    && !(*(GETSTRUCT(contup) as Form_pg_constraint)).connoinherit
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                            CStr::from_ptr(parent_attname).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                        ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                    );
                }
            }

            /*
             * Child column must be generated if and only if parent column is.
             */
            if (*parent_att).attgenerated != 0 && (*child_att).attgenerated == 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" in child table must be a generated column",
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
            if (*child_att).attgenerated != 0 && (*parent_att).attgenerated == 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" in child table must not be a generated column",
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            if (*parent_att).attgenerated != 0
                && (*child_att).attgenerated != 0
                && (*child_att).attgenerated != (*parent_att).attgenerated
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" inherits from generated column of different kind",
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    )
                    /* errcode(ERRCODE_DATATYPE_MISMATCH),
                       errdetail("Parent column is %s, child column is %s.", ...) */
                );
            }

            /*
             * Regular inheritance children are independent enough not to
             * inherit identity columns. But partitions are integral part of
             * a partitioned table and inherit identity column.
             */
            if ispartition {
                (*child_att).attidentity = (*parent_att).attidentity;
            }

            /*
             * OK, bump the child column's inheritance count.
             */
            let mut new_inhcount: i16 = 0;
            if pg_add_s16_overflow((*child_att).attinhcount, 1, &mut new_inhcount) {
                ereport!(
                    ERROR,
                    errmsg!("too many inheritance parents")
                    /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
                );
            }
            (*child_att).attinhcount = new_inhcount;

            /*
             * In case of partitions, we must enforce that value of attislocal
             * is same in all partitions.
             */
            if (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
                Assert!((*child_att).attinhcount == 1);
                (*child_att).attislocal = false;
            }

            CatalogTupleUpdate(attrrel, &(*tuple).t_self, tuple);
            heap_freetuple(tuple);
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "child table is missing column \"{}\"",
                    CStr::from_ptr(parent_attname).to_string_lossy()
                ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }

        parent_attno += 1;
    }

    table_close(attrrel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// MergeConstraintsIntoExisting
// ---------------------------------------------------------------------------

/// Check constraints in child table match up with constraints in parent,
/// and increment their coninhcount.
///
/// Constraints that are marked ONLY in the parent are ignored.
///
/// Called by CreateInheritance
unsafe fn MergeConstraintsIntoExisting(child_rel: Relation, parent_rel: Relation) {
    let constraintrel: Relation;
    let parent_scan: SysScanDesc;
    let mut parent_key: ScanKeyData = core::mem::zeroed();
    let mut parent_tuple: HeapTuple;
    let parent_relid: Oid = RelationGetRelid(parent_rel);
    let attmap: *mut AttrMap;

    constraintrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Outer loop scans through the parent's constraint definitions */
    ScanKeyInit(
        &mut parent_key,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(parent_relid),
    );
    parent_scan = systable_beginscan(
        constraintrel,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut parent_key,
    );

    attmap = build_attrmap_by_name(
        RelationGetDescr(parent_rel),
        RelationGetDescr(child_rel),
        true,
    );

    loop {
        parent_tuple = systable_getnext(parent_scan);
        if !HeapTupleIsValid(parent_tuple) {
            break;
        }
        let parent_con: Form_pg_constraint = GETSTRUCT(parent_tuple) as Form_pg_constraint;
        let child_scan: SysScanDesc;
        let mut child_key: ScanKeyData = core::mem::zeroed();
        let mut child_tuple: HeapTuple;
        let parent_attno: AttrNumber;
        let mut found: bool = false;

        if (*parent_con).contype != CONSTRAINT_CHECK as libc::c_char
            && (*parent_con).contype != CONSTRAINT_NOTNULL as libc::c_char
        {
            continue;
        }

        /* if the parent's constraint is marked NO INHERIT, it's not inherited */
        if (*parent_con).connoinherit {
            continue;
        }

        if (*parent_con).contype == CONSTRAINT_NOTNULL as libc::c_char {
            parent_attno = extractNotNullColumn(parent_tuple);
        } else {
            parent_attno = InvalidAttrNumber;
        }

        /* Search for a child constraint matching this one */
        ScanKeyInit(
            &mut child_key,
            Anum_pg_constraint_conrelid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(RelationGetRelid(child_rel)),
        );
        child_scan = systable_beginscan(
            constraintrel,
            ConstraintRelidTypidNameIndexId,
            true,
            core::ptr::null_mut(),
            1,
            &mut child_key,
        );

        loop {
            child_tuple = systable_getnext(child_scan);
            if !HeapTupleIsValid(child_tuple) {
                break;
            }
            let child_con: Form_pg_constraint = GETSTRUCT(child_tuple) as Form_pg_constraint;
            let child_copy: HeapTuple;

            if (*child_con).contype != (*parent_con).contype {
                continue;
            }

            /*
             * CHECK constraints are matched by constraint name, NOT NULL ones
             * by attribute number.
             */
            if (*child_con).contype == CONSTRAINT_CHECK as libc::c_char {
                if libc::strcmp(
                    NameStr!((*parent_con).conname),
                    NameStr!((*child_con).conname),
                ) != 0
                {
                    continue;
                }
            } else if (*child_con).contype == CONSTRAINT_NOTNULL as libc::c_char {
                let parent_attr: Form_pg_attribute =
                    TupleDescAttr((*parent_rel).rd_att, (parent_attno - 1) as usize)
                        as Form_pg_attribute;
                let child_attno: AttrNumber = extractNotNullColumn(child_tuple);
                if parent_attno != (*attmap).attnums[(child_attno - 1) as usize] {
                    continue;
                }

                let child_attr: Form_pg_attribute =
                    TupleDescAttr((*child_rel).rd_att, (child_attno - 1) as usize)
                        as Form_pg_attribute;
                /* there shouldn't be constraints on dropped columns */
                if (*parent_attr).attisdropped || (*child_attr).attisdropped {
                    elog!(ERROR, "found not-null constraint on dropped columns");
                }
            }

            if (*child_con).contype == CONSTRAINT_CHECK as libc::c_char
                && !constraints_equivalent(
                    parent_tuple,
                    child_tuple,
                    RelationGetDescr(constraintrel),
                )
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "child table \"{}\" has different definition for check constraint \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                        CStr::from_ptr(NameStr!((*parent_con).conname)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            /*
             * If the child constraint is "no inherit" then cannot merge
             */
            if (*child_con).connoinherit {
                ereport!(
                    ERROR,
                    errmsg!(
                        "constraint \"{}\" conflicts with non-inherited constraint on child table \"{}\"",
                        CStr::from_ptr(NameStr!((*child_con).conname)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * If the child constraint is "not valid" then cannot merge with a
             * valid parent constraint
             */
            if (*parent_con).convalidated
                && (*child_con).conenforced
                && !(*child_con).convalidated
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "constraint \"{}\" conflicts with NOT VALID constraint on child table \"{}\"",
                        CStr::from_ptr(NameStr!((*child_con).conname)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * A NOT ENFORCED child constraint cannot be merged with an
             * ENFORCED parent constraint.
             */
            if (*parent_con).conenforced && !(*child_con).conenforced {
                ereport!(
                    ERROR,
                    errmsg!(
                        "constraint \"{}\" conflicts with NOT ENFORCED constraint on child table \"{}\"",
                        CStr::from_ptr(NameStr!((*child_con).conname)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * OK, bump the child constraint's inheritance count.
             */
            child_copy = heap_copytuple(child_tuple);
            let child_con_copy: Form_pg_constraint =
                GETSTRUCT(child_copy) as Form_pg_constraint;

            let mut new_inhcount: i16 = 0;
            if pg_add_s16_overflow((*child_con_copy).coninhcount, 1, &mut new_inhcount) {
                ereport!(
                    ERROR,
                    errmsg!("too many inheritance parents")
                    /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
                );
            }
            (*child_con_copy).coninhcount = new_inhcount;

            /*
             * In case of partitions, an inherited constraint must be
             * inherited only once since it cannot have multiple parents and
             * it is never considered local.
             */
            if (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
                Assert!((*child_con_copy).coninhcount == 1);
                (*child_con_copy).conislocal = false;
            }

            CatalogTupleUpdate(constraintrel, &(*child_copy).t_self, child_copy);
            heap_freetuple(child_copy);

            found = true;
            break;
        }

        systable_endscan(child_scan);

        if !found {
            if (*parent_con).contype == CONSTRAINT_NOTNULL as libc::c_char {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                        CStr::from_ptr(get_attname(
                            parent_relid,
                            extractNotNullColumn(parent_tuple),
                            false
                        ))
                        .to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            ereport!(
                ERROR,
                errmsg!(
                    "child table is missing constraint \"{}\"",
                    CStr::from_ptr(NameStr!((*parent_con).conname)).to_string_lossy()
                ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }
    }

    systable_endscan(parent_scan);
    table_close(constraintrel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecDropInherit
// ---------------------------------------------------------------------------

/// ALTER TABLE NO INHERIT
///
/// Return value is the address of the relation that is no longer parent.
unsafe fn ATExecDropInherit(
    rel: Relation,
    parent: *mut RangeVar,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let address: ObjectAddress;
    let parent_rel: Relation;

    if (*(*rel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of a partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * AccessShareLock on the parent is probably enough, seeing that DROP
     * TABLE doesn't lock parent tables at all.
     */
    parent_rel = table_openrv(parent, AccessShareLock);

    /*
     * We don't bother to check ownership of the parent table --- ownership of
     * the child is presumed enough rights.
     */

    /* Off to RemoveInheritance() where most of the work happens */
    RemoveInheritance(rel, parent_rel, false);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(parent_rel));

    /* keep our lock on the parent relation until commit */
    table_close(parent_rel, NoLock);

    address
}

// ---------------------------------------------------------------------------
// MarkInheritDetached
// ---------------------------------------------------------------------------

/// Set inhdetachpending for a partition, for ATExecDetachPartition
/// in concurrent mode.
unsafe fn MarkInheritDetached(child_rel: Relation, parent_rel: Relation) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut inherits_tuple: HeapTuple;
    let mut found: bool = false;

    Assert!((*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE);

    /*
     * Find pg_inherits entries by inhparent. We need to scan them all in
     * order to verify that no other partition is pending detach.
     */
    catalog_relation = table_open(InheritsRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        InheritsParentIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    loop {
        inherits_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(inherits_tuple) {
            break;
        }
        let inh_form: Form_pg_inherits = GETSTRUCT(inherits_tuple) as Form_pg_inherits;
        if (*inh_form).inhdetachpending {
            ereport!(
                ERROR,
                errmsg!(
                    "partition \"{}\" already pending detach in partitioned table \"{}.{}\"",
                    CStr::from_ptr(get_rel_name((*inh_form).inhrelid)).to_string_lossy(),
                    CStr::from_ptr(get_namespace_name((*(*parent_rel).rd_rel).relnamespace))
                        .to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   errhint("Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation.") */
            );
        }

        if (*inh_form).inhrelid == RelationGetRelid(child_rel) {
            let newtup: HeapTuple = heap_copytuple(inherits_tuple);
            (*(GETSTRUCT(newtup) as Form_pg_inherits)).inhdetachpending = true;

            CatalogTupleUpdate(catalog_relation, &(*inherits_tuple).t_self, newtup);
            found = true;
            heap_freetuple(newtup);
            /* keep looking, to ensure we catch others pending detach */
        }
    }

    /* Done */
    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);

    if !found {
        ereport!(
            ERROR,
            errmsg!(
                "relation \"{}\" is not a partition of relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_TABLE) */
        );
    }
}

// ---------------------------------------------------------------------------
// RemoveInheritance
// ---------------------------------------------------------------------------

/// RemoveInheritance
///
/// Drop a parent from the child's parents. This just adjusts the attinhcount
/// and attislocal of the columns and removes the pg_inherit and pg_depend
/// entries.
///
/// Common to ATExecDropInherit() and ATExecDetachPartition().
unsafe fn RemoveInheritance(child_rel: Relation, parent_rel: Relation, expect_detached: bool) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let mut attribute_tuple: HeapTuple;
    let mut constraint_tuple: HeapTuple;
    let attmap: *mut AttrMap;
    let mut connames: *mut List = NIL;
    let mut nncolumns: *mut List = NIL;
    let mut found: bool;
    let is_partitioning: bool;

    is_partitioning =
        (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE;

    found = DeleteInheritsTuple(
        RelationGetRelid(child_rel),
        RelationGetRelid(parent_rel),
        expect_detached,
        RelationGetRelationName(child_rel),
    );
    if !found {
        if is_partitioning {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" is not a partition of relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
                ) /* errcode(ERRCODE_UNDEFINED_TABLE) */
            );
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" is not a parent of relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                ) /* errcode(ERRCODE_UNDEFINED_TABLE) */
            );
        }
    }

    /*
     * Search through child columns looking for ones matching parent rel
     */
    catalog_relation = table_open(AttributeRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(child_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        AttributeRelidNumIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );
    loop {
        attribute_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(attribute_tuple) {
            break;
        }
        let att: Form_pg_attribute = GETSTRUCT(attribute_tuple) as Form_pg_attribute;

        /* Ignore if dropped or not inherited */
        if (*att).attisdropped {
            continue;
        }
        if (*att).attinhcount <= 0 {
            continue;
        }

        if SearchSysCacheExistsAttName(RelationGetRelid(parent_rel), NameStr!((*att).attname)) {
            /* Decrement inhcount and possibly set islocal to true */
            let copy_tuple: HeapTuple = heap_copytuple(attribute_tuple);
            let copy_att: Form_pg_attribute = GETSTRUCT(copy_tuple) as Form_pg_attribute;

            (*copy_att).attinhcount -= 1;
            if (*copy_att).attinhcount == 0 {
                (*copy_att).attislocal = true;
            }

            CatalogTupleUpdate(catalog_relation, &(*copy_tuple).t_self, copy_tuple);
            heap_freetuple(copy_tuple);
        }
    }
    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);

    /*
     * Likewise, find inherited check and not-null constraints and disinherit
     * them. First need a list of the names of the parent's check constraints.
     * For NOT NULL columns, we store column numbers to match.
     */
    attmap = build_attrmap_by_name(
        RelationGetDescr(child_rel),
        RelationGetDescr(parent_rel),
        false,
    );

    catalog_relation = table_open(ConstraintRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    loop {
        constraint_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(constraint_tuple) {
            break;
        }
        let con: Form_pg_constraint = GETSTRUCT(constraint_tuple) as Form_pg_constraint;

        if (*con).connoinherit {
            continue;
        }

        if (*con).contype == CONSTRAINT_CHECK as libc::c_char {
            connames = lappend(connames, pstrdup(NameStr!((*con).conname)) as *mut libc::c_void);
        }
        if (*con).contype == CONSTRAINT_NOTNULL as libc::c_char {
            let parent_attno: AttrNumber = extractNotNullColumn(constraint_tuple);
            nncolumns = lappend_int(nncolumns, (*attmap).attnums[(parent_attno - 1) as usize] as i32);
        }
    }

    systable_endscan(scan);

    /* Now scan the child's constraints to find matches */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(child_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    loop {
        constraint_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(constraint_tuple) {
            break;
        }
        let con: Form_pg_constraint = GETSTRUCT(constraint_tuple) as Form_pg_constraint;
        let mut match_found: bool = false;

        /*
         * Match CHECK constraints by name, not-null constraints by column
         * number, and ignore all others.
         */
        if (*con).contype == CONSTRAINT_CHECK as libc::c_char {
            let mut lc: *mut ListCell = list_head(connames);
            while !lc.is_null() {
                let chkname: *const libc::c_char = lfirst(lc) as *const libc::c_char;
                if libc::strcmp(NameStr!((*con).conname), chkname) == 0 {
                    match_found = true;
                    connames = list_delete_cell(connames, lc);
                    break;
                }
                lc = lnext(connames, lc);
            }
        } else if (*con).contype == CONSTRAINT_NOTNULL as libc::c_char {
            let child_attno: AttrNumber = extractNotNullColumn(constraint_tuple);
            let mut lc: *mut ListCell = list_head(nncolumns);
            while !lc.is_null() {
                let prevattno: i32 = lfirst_int(lc);
                if prevattno == child_attno as i32 {
                    match_found = true;
                    nncolumns = list_delete_cell(nncolumns, lc);
                    break;
                }
                lc = lnext(nncolumns, lc);
            }
        } else {
            continue;
        }

        if match_found {
            /* Decrement inhcount and possibly set islocal to true */
            let copy_tuple: HeapTuple = heap_copytuple(constraint_tuple);
            let copy_con: Form_pg_constraint = GETSTRUCT(copy_tuple) as Form_pg_constraint;

            if (*copy_con).coninhcount <= 0 {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "relation {} has non-inherited constraint \"{}\"",
                    RelationGetRelid(child_rel),
                    CStr::from_ptr(NameStr!((*copy_con).conname)).to_string_lossy()
                );
            }

            (*copy_con).coninhcount -= 1;
            if (*copy_con).coninhcount == 0 {
                (*copy_con).conislocal = true;
            }

            CatalogTupleUpdate(catalog_relation, &(*copy_tuple).t_self, copy_tuple);
            heap_freetuple(copy_tuple);
        }
    }

    /* We should have matched all constraints */
    if connames != NIL || nncolumns != NIL {
        elog!(
            ERROR,
            "{} unmatched constraints while removing inheritance from \"{}\" to \"{}\"",
            list_length(connames) + list_length(nncolumns),
            CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
            CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
        );
    }

    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);

    drop_parent_dependency(
        RelationGetRelid(child_rel),
        RelationRelationId,
        RelationGetRelid(parent_rel),
        child_dependency_type(is_partitioning),
    );

    /*
     * Post alter hook of this inherits. Since object_access_hook doesn't take
     * multiple object identifiers, we relay oid of parent relation using
     * auxiliary_id argument.
     */
    InvokeObjectPostAlterHookArg(
        InheritsRelationId,
        RelationGetRelid(child_rel),
        0,
        RelationGetRelid(parent_rel),
        false,
    );
}

// ---------------------------------------------------------------------------
// drop_parent_dependency
// ---------------------------------------------------------------------------

/// Drop the dependency created by StoreCatalogInheritance1 or
/// heap_create_with_catalog.
unsafe fn drop_parent_dependency(
    relid: Oid,
    refclassid: Oid,
    refobjid: Oid,
    deptype: DependencyType,
) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let mut dep_tuple: HeapTuple;

    catalog_relation = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(0),
    );

    scan = systable_beginscan(
        catalog_relation,
        DependDependerIndexId,
        true,
        core::ptr::null_mut(),
        3,
        key.as_mut_ptr(),
    );

    loop {
        dep_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(dep_tuple) {
            break;
        }
        let dep: Form_pg_depend = GETSTRUCT(dep_tuple) as Form_pg_depend;

        if (*dep).refclassid == refclassid
            && (*dep).refobjid == refobjid
            && (*dep).refobjsubid == 0
            && (*dep).deptype == deptype as libc::c_char
        {
            CatalogTupleDelete(catalog_relation, &(*dep_tuple).t_self);
        }
    }

    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecAddOf
// ---------------------------------------------------------------------------

unsafe fn ATExecAddOf(
    rel: Relation,
    of_typename: *const TypeName,
    _lockmode: LOCKMODE,
) -> ObjectAddress {
    let relid: Oid = RelationGetRelid(rel);
    let typetuple: Type;
    let typeform: Form_pg_type;
    let typeid: Oid;
    let inherits_relation: Relation;
    let relation_relation: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let tableobj: ObjectAddress;
    let typeobj: ObjectAddress;
    let classtuple: HeapTuple;

    /* Validate the type. */
    typetuple = typenameType(core::ptr::null_mut(), of_typename, core::ptr::null_mut());
    check_of_type(typetuple);
    typeform = GETSTRUCT(typetuple) as Form_pg_type;
    typeid = (*typeform).oid;

    /* Fail if the table has any inheritance parents. */
    inherits_relation = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    scan = systable_beginscan(
        inherits_relation,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );
    if HeapTupleIsValid(systable_getnext(scan)) {
        ereport!(
            ERROR,
            errmsg!("typed tables cannot inherit")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    systable_endscan(scan);
    table_close(inherits_relation, AccessShareLock);

    /*
     * Check the tuple descriptors for compatibility. Unlike inheritance, we
     * require that the order also match. However, attnotnull need not match.
     */
    let type_tuple_desc: TupleDesc = lookup_rowtype_tupdesc(typeid, -1);
    let table_tuple_desc: TupleDesc = RelationGetDescr(rel);
    let mut table_attno: AttrNumber = 1;
    let mut type_attno: AttrNumber = 1;
    while type_attno <= (*type_tuple_desc).natts as AttrNumber {
        let type_attr: Form_pg_attribute =
            TupleDescAttr(type_tuple_desc, (type_attno - 1) as usize) as Form_pg_attribute;
        type_attno += 1;
        if (*type_attr).attisdropped {
            continue;
        }
        let type_attname: *const libc::c_char = NameStr!((*type_attr).attname);

        /* Get the next non-dropped table attribute. */
        loop {
            if table_attno > (*table_tuple_desc).natts as AttrNumber {
                ereport!(
                    ERROR,
                    errmsg!(
                        "table is missing column \"{}\"",
                        CStr::from_ptr(type_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
            let table_attr: Form_pg_attribute =
                TupleDescAttr(table_tuple_desc, (table_attno - 1) as usize) as Form_pg_attribute;
            table_attno += 1;
            if !(*table_attr).attisdropped {
                let table_attname: *const libc::c_char = NameStr!((*table_attr).attname);
                /* Compare name. */
                if libc::strncmp(table_attname, type_attname, NAMEDATALEN) != 0 {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "table has column \"{}\" where type requires \"{}\"",
                            CStr::from_ptr(table_attname).to_string_lossy(),
                            CStr::from_ptr(type_attname).to_string_lossy()
                        ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                    );
                }
                /* Compare type. */
                if (*table_attr).atttypid != (*type_attr).atttypid
                    || (*table_attr).atttypmod != (*type_attr).atttypmod
                    || (*table_attr).attcollation != (*type_attr).attcollation
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "table \"{}\" has different type for column \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                            CStr::from_ptr(type_attname).to_string_lossy()
                        ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                    );
                }
                break;
            }
        }
    }
    ReleaseTupleDesc(type_tuple_desc);

    /* Any remaining columns at the end of the table had better be dropped. */
    while table_attno <= (*table_tuple_desc).natts as AttrNumber {
        let table_attr: Form_pg_attribute =
            TupleDescAttr(table_tuple_desc, (table_attno - 1) as usize) as Form_pg_attribute;
        table_attno += 1;
        if !(*table_attr).attisdropped {
            ereport!(
                ERROR,
                errmsg!(
                    "table has extra column \"{}\"",
                    CStr::from_ptr(NameStr!((*table_attr).attname)).to_string_lossy()
                ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }
    }

    /* If the table was already typed, drop the existing dependency. */
    if (*(*rel).rd_rel).reloftype != InvalidOid {
        drop_parent_dependency(
            relid,
            TypeRelationId,
            (*(*rel).rd_rel).reloftype,
            DEPENDENCY_NORMAL,
        );
    }

    /* Record a dependency on the new type. */
    let mut tableobj_local: ObjectAddress = core::mem::zeroed();
    let mut typeobj_local: ObjectAddress = core::mem::zeroed();
    tableobj_local.classId = RelationRelationId;
    tableobj_local.objectId = relid;
    tableobj_local.objectSubId = 0;
    typeobj_local.classId = TypeRelationId;
    typeobj_local.objectId = typeid;
    typeobj_local.objectSubId = 0;
    recordDependencyOn(&tableobj_local, &typeobj_local, DEPENDENCY_NORMAL);

    /* Update pg_class.reloftype */
    relation_relation = table_open(RelationRelationId, RowExclusiveLock);
    classtuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(classtuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(classtuple) as Form_pg_class)).reloftype = typeid;
    CatalogTupleUpdate(relation_relation, &(*classtuple).t_self, classtuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

    heap_freetuple(classtuple);
    table_close(relation_relation, RowExclusiveLock);

    ReleaseSysCache(typetuple);

    let _ = tableobj;
    let _ = typeobj;
    typeobj_local
}

// ---------------------------------------------------------------------------
// ATExecDropOf
// ---------------------------------------------------------------------------

unsafe fn ATExecDropOf(rel: Relation, _lockmode: LOCKMODE) {
    let relid: Oid = RelationGetRelid(rel);
    let relation_relation: Relation;
    let tuple: HeapTuple;

    if !OidIsValid((*(*rel).rd_rel).reloftype) {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a typed table",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * We don't bother to check ownership of the type --- ownership of the
     * table is presumed enough rights. No lock required on the type, either.
     */

    drop_parent_dependency(
        relid,
        TypeRelationId,
        (*(*rel).rd_rel).reloftype,
        DEPENDENCY_NORMAL,
    );

    /* Clear pg_class.reloftype */
    relation_relation = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(tuple) as Form_pg_class)).reloftype = InvalidOid;
    CatalogTupleUpdate(relation_relation, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

    heap_freetuple(tuple);
    table_close(relation_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// relation_mark_replica_identity
// ---------------------------------------------------------------------------

unsafe fn relation_mark_replica_identity(
    rel: Relation,
    ri_type: libc::c_char,
    index_oid: Oid,
    is_internal: bool,
) {
    let pg_index: Relation;
    let pg_class: Relation;
    let pg_class_tuple: HeapTuple;
    let pg_class_form: Form_pg_class;
    let index_list: *mut List;

    /*
     * Check whether relreplident has changed, and update it if so.
     */
    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    pg_class_tuple =
        SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if !HeapTupleIsValid(pg_class_tuple) {
        elog!(
            ERROR,
            "cache lookup failed for relation \"{}\"",
            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }
    pg_class_form = GETSTRUCT(pg_class_tuple) as Form_pg_class;
    if (*pg_class_form).relreplident != ri_type {
        (*pg_class_form).relreplident = ri_type;
        CatalogTupleUpdate(pg_class, &(*pg_class_tuple).t_self, pg_class_tuple);
    }
    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(pg_class_tuple);

    /*
     * Update the per-index indisreplident flags correctly.
     */
    pg_index = table_open(IndexRelationId, RowExclusiveLock);
    index_list = RelationGetIndexList(rel);
    let mut lc: *mut ListCell = list_head(index_list);
    while !lc.is_null() {
        let this_index_oid: Oid = lfirst_oid(lc);
        let mut dirty: bool = false;
        let pg_index_tuple: HeapTuple;
        let pg_index_form: Form_pg_index;

        pg_index_tuple =
            SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(this_index_oid));
        if !HeapTupleIsValid(pg_index_tuple) {
            elog!(ERROR, "cache lookup failed for index {}", this_index_oid);
        }
        pg_index_form = GETSTRUCT(pg_index_tuple) as Form_pg_index;

        if this_index_oid == index_oid {
            /* Set the bit if not already set. */
            if !(*pg_index_form).indisreplident {
                dirty = true;
                (*pg_index_form).indisreplident = true;
            }
        } else {
            /* Unset the bit if set. */
            if (*pg_index_form).indisreplident {
                dirty = true;
                (*pg_index_form).indisreplident = false;
            }
        }

        if dirty {
            CatalogTupleUpdate(pg_index, &(*pg_index_tuple).t_self, pg_index_tuple);
            InvokeObjectPostAlterHookArg(
                IndexRelationId,
                this_index_oid,
                0,
                InvalidOid,
                is_internal,
            );

            /*
             * Invalidate the relcache for the table, so that after we commit
             * all sessions will refresh the table's replica identity index
             * before attempting any UPDATE or DELETE on the table.
             */
            CacheInvalidateRelcache(rel);
        }
        heap_freetuple(pg_index_tuple);

        lc = lnext(index_list, lc);
    }

    table_close(pg_index, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecReplicaIdentity
// ---------------------------------------------------------------------------

unsafe fn ATExecReplicaIdentity(rel: Relation, stmt: *mut ReplicaIdentityStmt, _lockmode: LOCKMODE) {
    let index_oid: Oid;
    let index_rel: Relation;

    if (*stmt).identity_type == REPLICA_IDENTITY_DEFAULT as libc::c_char {
        relation_mark_replica_identity(rel, (*stmt).identity_type, InvalidOid, true);
        return;
    } else if (*stmt).identity_type == REPLICA_IDENTITY_FULL as libc::c_char {
        relation_mark_replica_identity(rel, (*stmt).identity_type, InvalidOid, true);
        return;
    } else if (*stmt).identity_type == REPLICA_IDENTITY_NOTHING as libc::c_char {
        relation_mark_replica_identity(rel, (*stmt).identity_type, InvalidOid, true);
        return;
    } else if (*stmt).identity_type == REPLICA_IDENTITY_INDEX as libc::c_char {
        /* fallthrough */
    } else {
        elog!(ERROR, "unexpected identity type {}", (*stmt).identity_type);
    }

    /* Check that the index exists */
    index_oid = get_relname_relid((*stmt).name, (*(*rel).rd_rel).relnamespace);
    if !OidIsValid(index_oid) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" for table \"{}\" does not exist",
                CStr::from_ptr((*stmt).name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    index_rel = index_open(index_oid, ShareLock);

    /* Check that the index is on the relation we're altering. */
    if (*index_rel).rd_index.is_null()
        || (*(*index_rel).rd_index).indrelid != RelationGetRelid(rel)
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not an index for table \"{}\"",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * The AM must support uniqueness, and the index must in fact be unique.
     * If we have a WITHOUT OVERLAPS constraint (identified by uniqueness +
     * exclusion), we can use that too.
     */
    if (!(*(*index_rel).rd_indam).amcanunique
        || !(*(*index_rel).rd_index).indisunique)
        && !((*(*index_rel).rd_index).indisunique
            && (*(*index_rel).rd_index).indisexclusion)
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use non-unique index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    /* Deferred indexes are not guaranteed to be always unique. */
    if !(*(*index_rel).rd_index).indimmediate {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use non-immediate index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }
    /* Expression indexes aren't supported. */
    if RelationGetIndexExpressions(index_rel) != NIL {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use expression index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }
    /* Predicate indexes aren't supported. */
    if RelationGetIndexPredicate(index_rel) != NIL {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use partial index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Check index for nullable columns. */
    let nkeys: i32 = IndexRelationGetNumberOfKeyAttributes(index_rel);
    for key in 0..nkeys {
        let attno: i16 = (*(*index_rel).rd_index).indkey.values[key as usize];
        let attr: Form_pg_attribute;

        if attno <= 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "index \"{}\" cannot be used as replica identity because column {} is a system column",
                    CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy(),
                    attno
                ) /* errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
            );
        }

        attr = TupleDescAttr((*rel).rd_att, (attno - 1) as usize) as Form_pg_attribute;
        if !(*attr).attnotnull {
            ereport!(
                ERROR,
                errmsg!(
                    "index \"{}\" cannot be used as replica identity because column \"{}\" is nullable",
                    CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy(),
                    CStr::from_ptr(NameStr!((*attr).attname)).to_string_lossy()
                ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            );
        }
    }

    /* This index is suitable for use as a replica identity. Mark it. */
    relation_mark_replica_identity(rel, (*stmt).identity_type, index_oid, true);

    index_close(index_rel, NoLock);
}

// ---------------------------------------------------------------------------
// ATExecSetRowSecurity
// ---------------------------------------------------------------------------

unsafe fn ATExecSetRowSecurity(rel: Relation, rls: bool) {
    let pg_class: Relation;
    let relid: Oid;
    let tuple: HeapTuple;

    relid = RelationGetRelid(rel);

    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(tuple) as Form_pg_class)).relrowsecurity = rls;
    CatalogTupleUpdate(pg_class, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(tuple);
}

// ---------------------------------------------------------------------------
// ATExecForceNoForceRowSecurity
// ---------------------------------------------------------------------------

unsafe fn ATExecForceNoForceRowSecurity(rel: Relation, force_rls: bool) {
    let pg_class: Relation;
    let relid: Oid;
    let tuple: HeapTuple;

    relid = RelationGetRelid(rel);

    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(tuple) as Form_pg_class)).relforcerowsecurity = force_rls;
    CatalogTupleUpdate(pg_class, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(tuple);
}

// ---------------------------------------------------------------------------
// ATExecGenericOptions
// ---------------------------------------------------------------------------

unsafe fn ATExecGenericOptions(rel: Relation, options: *mut List) {
    let ftrel: Relation;
    let server: *mut ForeignServer;
    let fdw: *mut ForeignDataWrapper;
    let mut tuple: HeapTuple;
    let mut isnull: bool = false;
    let mut repl_val: [Datum; Natts_pg_foreign_table as usize] = core::mem::zeroed();
    let mut repl_null: [bool; Natts_pg_foreign_table as usize] = core::mem::zeroed();
    let mut repl_repl: [bool; Natts_pg_foreign_table as usize] = core::mem::zeroed();
    let mut datum: Datum;
    let tableform: Form_pg_foreign_table;

    if options == NIL {
        return;
    }

    ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(FOREIGNTABLEREL, ObjectIdGetDatum((*rel).rd_id));
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "foreign table \"{}\" does not exist",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }
    tableform = GETSTRUCT(tuple) as Form_pg_foreign_table;
    server = GetForeignServer((*tableform).ftserver);
    fdw = GetForeignDataWrapper((*server).fdwid);

    libc::memset(
        repl_val.as_mut_ptr() as *mut libc::c_void,
        0,
        core::mem::size_of_val(&repl_val),
    );
    libc::memset(
        repl_null.as_mut_ptr() as *mut libc::c_void,
        0,
        core::mem::size_of_val(&repl_null),
    );
    libc::memset(
        repl_repl.as_mut_ptr() as *mut libc::c_void,
        0,
        core::mem::size_of_val(&repl_repl),
    );

    /* Extract the current options */
    datum = SysCacheGetAttr(
        FOREIGNTABLEREL,
        tuple,
        Anum_pg_foreign_table_ftoptions,
        &mut isnull,
    );
    if isnull {
        datum = PointerGetDatum(core::ptr::null_mut());
    }

    /* Transform the options */
    datum = transformGenericOptions(
        ForeignTableRelationId,
        datum,
        options,
        (*fdw).fdwvalidator,
    );

    if PointerIsValid(DatumGetPointer(datum)) {
        repl_val[Anum_pg_foreign_table_ftoptions as usize - 1] = datum;
    } else {
        repl_null[Anum_pg_foreign_table_ftoptions as usize - 1] = true;
    }

    repl_repl[Anum_pg_foreign_table_ftoptions as usize - 1] = true;

    tuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(ftrel),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(ftrel, &(*tuple).t_self, tuple);

    CacheInvalidateRelcache(rel);

    InvokeObjectPostAlterHook(ForeignTableRelationId, RelationGetRelid(rel), 0);

    table_close(ftrel, RowExclusiveLock);
    heap_freetuple(tuple);
}

// ---------------------------------------------------------------------------
// ATExecSetCompression
// ---------------------------------------------------------------------------

unsafe fn ATExecSetCompression(
    rel: Relation,
    column: *const libc::c_char,
    new_value: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let attrel: Relation;
    let tuple: HeapTuple;
    let atttableform: Form_pg_attribute;
    let attnum: AttrNumber;
    let compression: *mut libc::c_char;
    let cmethod: libc::c_char;
    let address: ObjectAddress;

    compression = strVal(new_value);

    attrel = table_open(AttributeRelationId, RowExclusiveLock);

    /* copy the cache entry so we can scribble on it below */
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), column);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(column).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }

    atttableform = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*atttableform).attnum;
    if attnum <= 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot alter system column \"{}\"",
                CStr::from_ptr(column).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* get the attribute compression method code */
    cmethod = GetAttributeCompression((*atttableform).atttypid, compression);

    /* update pg_attribute entry */
    (*atttableform).attcompression = cmethod;
    CatalogTupleUpdate(attrel, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);

    /*
     * Apply the change to indexes as well (only for simple index columns).
     */
    SetIndexStorageProperties(
        rel,
        attrel,
        attnum,
        false,
        0,
        true,
        cmethod,
        lockmode,
    );

    heap_freetuple(tuple);
    table_close(attrel, RowExclusiveLock);

    /* make changes visible */
    CommandCounterIncrement();

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum);
    address
}

// ---------------------------------------------------------------------------
// ATPrepChangePersistence
// ---------------------------------------------------------------------------

unsafe fn ATPrepChangePersistence(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    to_logged: bool,
) {
    let pg_constraint: Relation;
    let mut tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();

    /*
     * Disallow changing status for a temp table.  Also verify whether we can
     * get away with doing nothing.
     */
    match (*(*rel).rd_rel).relpersistence as u8 {
        RELPERSISTENCE_TEMP => {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change logged status of table \"{}\" because it is temporary",
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_TABLE_DEFINITION), errtable(rel) */
            );
        }
        RELPERSISTENCE_PERMANENT => {
            if to_logged {
                return;
            }
        }
        RELPERSISTENCE_UNLOGGED => {
            if !to_logged {
                return;
            }
        }
        _ => {}
    }

    /*
     * Check that the table is not part of any publication when changing to
     * UNLOGGED, as UNLOGGED tables can't be published.
     */
    if !to_logged && GetRelationPublications(RelationGetRelid(rel)) != NIL {
        ereport!(
            ERROR,
            errmsg!(
                "cannot change table \"{}\" to unlogged because it is part of a publication",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
               errdetail("Unlogged relations cannot be replicated.") */
        );
    }

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        if to_logged {
            Anum_pg_constraint_conrelid
        } else {
            Anum_pg_constraint_confrelid
        },
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    scan = systable_beginscan(
        pg_constraint,
        if to_logged {
            ConstraintRelidTypidNameIndexId
        } else {
            InvalidOid
        },
        true,
        core::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let con: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

        if (*con).contype == CONSTRAINT_FOREIGN as libc::c_char {
            let foreign_relid: Oid;
            let foreign_rel: Relation;

            /* the opposite end of what we used as scankey */
            foreign_relid = if to_logged { (*con).confrelid } else { (*con).conrelid };

            /* ignore if self-referencing */
            if RelationGetRelid(rel) == foreign_relid {
                continue;
            }

            foreign_rel = relation_open(foreign_relid, AccessShareLock);

            if to_logged {
                if !RelationIsPermanent(foreign_rel) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not change table \"{}\" to logged because it references unlogged table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(foreign_rel)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errtableconstraint(rel, NameStr(con->conname)) */
                    );
                }
            } else {
                if RelationIsPermanent(foreign_rel) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not change table \"{}\" to unlogged because it references logged table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(foreign_rel)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errtableconstraint(rel, NameStr(con->conname)) */
                    );
                }
            }

            relation_close(foreign_rel, AccessShareLock);
        }
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    /* force rewrite if necessary; see comment in ATRewriteTables */
    (*tab).rewrite |= AT_REWRITE_ALTER_PERSISTENCE as i32;
    if to_logged {
        (*tab).newrelpersistence = RELPERSISTENCE_PERMANENT as libc::c_char;
    } else {
        (*tab).newrelpersistence = RELPERSISTENCE_UNLOGGED as libc::c_char;
    }
    (*tab).chgPersistence = true;
}

// ---------------------------------------------------------------------------
// AlterTableNamespace
// ---------------------------------------------------------------------------

pub unsafe fn AlterTableNamespace(
    stmt: *mut AlterObjectSchemaStmt,
    oldschema: *mut Oid,
) -> ObjectAddress {
    let rel: Relation;
    let relid: Oid;
    let old_nsp_oid: Oid;
    let nsp_oid: Oid;
    let newrv: *mut RangeVar;
    let objs_moved: *mut ObjectAddresses;
    let myself: ObjectAddress;

    relid = RangeVarGetRelidExtended(
        (*stmt).relation,
        AccessExclusiveLock,
        if (*stmt).missing_ok { RVR_MISSING_OK } else { 0 },
        Some(RangeVarCallbackForAlterRelation),
        stmt as *mut libc::c_void,
    );

    if !OidIsValid(relid) {
        ereport!(
            NOTICE,
            errmsg!(
                "relation \"{}\" does not exist, skipping",
                CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()
            )
        );
        return InvalidObjectAddress;
    }

    rel = relation_open(relid, NoLock);
    old_nsp_oid = RelationGetNamespace(rel);

    /* If it's an owned sequence, disallow moving it by itself. */
    if (*(*rel).rd_rel).relkind as u8 == RELKIND_SEQUENCE {
        let mut table_id: Oid = InvalidOid;
        let mut col_id: i32 = 0;

        if sequenceIsOwned(relid, DEPENDENCY_AUTO, &mut table_id, &mut col_id)
            || sequenceIsOwned(relid, DEPENDENCY_INTERNAL, &mut table_id, &mut col_id)
        {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot move an owned sequence into another schema"
                )
                /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errdetail("Sequence ... is linked to table ...") */
            );
        }
    }

    /* Get and lock schema OID and check its permissions. */
    newrv = makeRangeVar(
        (*stmt).newschema,
        RelationGetRelationName(rel) as *mut libc::c_char,
        -1,
    );
    nsp_oid = RangeVarGetAndCheckCreationNamespace(newrv, NoLock, core::ptr::null_mut());

    /* common checks on switching namespaces */
    CheckSetNamespace(old_nsp_oid, nsp_oid);

    objs_moved = new_object_addresses();
    AlterTableNamespaceInternal(rel, old_nsp_oid, nsp_oid, objs_moved);
    free_object_addresses(objs_moved);

    ObjectAddressSet!(myself, RelationRelationId, relid);

    if !oldschema.is_null() {
        *oldschema = old_nsp_oid;
    }

    /* close rel, but keep lock until commit */
    relation_close(rel, NoLock);

    myself
}

// ---------------------------------------------------------------------------
// AlterTableNamespaceInternal
// ---------------------------------------------------------------------------

pub unsafe fn AlterTableNamespaceInternal(
    rel: Relation,
    old_nsp_oid: Oid,
    nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
) {
    let class_rel: Relation;

    Assert!(!objs_moved.is_null());

    /* OK, modify the pg_class row and pg_depend entry */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);

    AlterRelationNamespaceInternal(
        class_rel,
        RelationGetRelid(rel),
        old_nsp_oid,
        nsp_oid,
        true,
        objs_moved,
    );

    /* Fix the table's row type too, if it has one */
    if OidIsValid((*(*rel).rd_rel).reltype) {
        AlterTypeNamespaceInternal(
            (*(*rel).rd_rel).reltype,
            nsp_oid,
            false, /* isImplicitArray */
            false, /* ignoreDependent */
            false, /* errorOnTableType */
            objs_moved,
        );
    }

    /* Fix other dependent stuff */
    AlterIndexNamespaces(class_rel, rel, old_nsp_oid, nsp_oid, objs_moved);
    AlterSeqNamespaces(
        class_rel,
        rel,
        old_nsp_oid,
        nsp_oid,
        objs_moved,
        AccessExclusiveLock,
    );
    AlterConstraintNamespaces(RelationGetRelid(rel), old_nsp_oid, nsp_oid, false, objs_moved);

    table_close(class_rel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// AlterRelationNamespaceInternal
// ---------------------------------------------------------------------------

pub unsafe fn AlterRelationNamespaceInternal(
    class_rel: Relation,
    rel_oid: Oid,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    has_depend_entry: bool,
    objs_moved: *mut ObjectAddresses,
) {
    let class_tup: HeapTuple;
    let class_form: Form_pg_class;
    let mut thisobj: ObjectAddress = core::mem::zeroed();
    let already_done: bool;

    /* no rel lock for relkind=c so use LOCKTAG_TUPLE */
    class_tup = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(rel_oid));
    if !HeapTupleIsValid(class_tup) {
        elog!(ERROR, "cache lookup failed for relation {}", rel_oid);
    }
    class_form = GETSTRUCT(class_tup) as Form_pg_class;

    Assert!((*class_form).relnamespace == old_nsp_oid);

    thisobj.classId = RelationRelationId;
    thisobj.objectId = rel_oid;
    thisobj.objectSubId = 0;

    /*
     * If the object has already been moved, don't move it again.
     */
    already_done = object_address_present(&thisobj, objs_moved);
    if !already_done && old_nsp_oid != new_nsp_oid {
        let otid: ItemPointerData = (*class_tup).t_self;

        /* check for duplicate name */
        if get_relname_relid(NameStr!((*class_form).relname), new_nsp_oid) != InvalidOid {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" already exists in schema \"{}\"",
                    CStr::from_ptr(NameStr!((*class_form).relname)).to_string_lossy(),
                    CStr::from_ptr(get_namespace_name(new_nsp_oid)).to_string_lossy()
                ) /* errcode(ERRCODE_DUPLICATE_TABLE) */
            );
        }

        /* classTup is a copy, so OK to scribble on */
        (*class_form).relnamespace = new_nsp_oid;

        CatalogTupleUpdate(class_rel, &otid, class_tup);
        UnlockTuple(class_rel, &otid, InplaceUpdateTupleLock);

        /* Update dependency on schema if caller said so */
        if has_depend_entry
            && changeDependencyFor(
                RelationRelationId,
                rel_oid,
                NamespaceRelationId,
                old_nsp_oid,
                new_nsp_oid,
            ) != 1
        {
            elog!(
                ERROR,
                "could not change schema dependency for relation \"{}\"",
                CStr::from_ptr(NameStr!((*class_form).relname)).to_string_lossy()
            );
        }
    } else {
        UnlockTuple(class_rel, &(*class_tup).t_self, InplaceUpdateTupleLock);
    }

    if !already_done {
        add_exact_object_address(&thisobj, objs_moved);
        InvokeObjectPostAlterHook(RelationRelationId, rel_oid, 0);
    }

    heap_freetuple(class_tup);
}

// ---------------------------------------------------------------------------
// AlterIndexNamespaces (static)
// ---------------------------------------------------------------------------

unsafe fn AlterIndexNamespaces(
    class_rel: Relation,
    rel: Relation,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
) {
    let index_list: *mut List = RelationGetIndexList(rel);
    let mut lc: *mut ListCell = list_head(index_list);
    while !lc.is_null() {
        let index_oid: Oid = lfirst_oid(lc);
        let mut thisobj: ObjectAddress = core::mem::zeroed();

        thisobj.classId = RelationRelationId;
        thisobj.objectId = index_oid;
        thisobj.objectSubId = 0;

        if !object_address_present(&thisobj, objs_moved) {
            AlterRelationNamespaceInternal(
                class_rel,
                index_oid,
                old_nsp_oid,
                new_nsp_oid,
                false,
                objs_moved,
            );
            add_exact_object_address(&thisobj, objs_moved);
        }

        lc = lnext(index_list, lc);
    }

    list_free(index_list);
}

// ---------------------------------------------------------------------------
// AlterSeqNamespaces (static)
// ---------------------------------------------------------------------------

unsafe fn AlterSeqNamespaces(
    class_rel: Relation,
    rel: Relation,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
    lockmode: LOCKMODE,
) {
    let dep_rel: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;

    dep_rel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );

    scan = systable_beginscan(
        dep_rel,
        DependReferenceIndexId,
        true,
        core::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let dep_form: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;
        let seq_rel: Relation;

        /* skip dependencies other than auto dependencies on columns */
        if (*dep_form).refobjsubid == 0
            || (*dep_form).classid != RelationRelationId
            || (*dep_form).objsubid != 0
            || !((*dep_form).deptype == DEPENDENCY_AUTO as libc::c_char
                || (*dep_form).deptype == DEPENDENCY_INTERNAL as libc::c_char)
        {
            continue;
        }

        /* Use relation_open just in case it's an index */
        seq_rel = relation_open((*dep_form).objid, lockmode);

        /* skip non-sequence relations */
        if (*RelationGetForm(seq_rel)).relkind as u8 != RELKIND_SEQUENCE {
            relation_close(seq_rel, lockmode);
            continue;
        }

        /* Fix the pg_class and pg_depend entries */
        AlterRelationNamespaceInternal(
            class_rel,
            (*dep_form).objid,
            old_nsp_oid,
            new_nsp_oid,
            true,
            objs_moved,
        );

        Assert!((*RelationGetForm(seq_rel)).reltype == InvalidOid);

        /* Now we can close it. Keep the lock till end of transaction. */
        relation_close(seq_rel, NoLock);
    }

    systable_endscan(scan);
    relation_close(dep_rel, AccessShareLock);
}

// ---------------------------------------------------------------------------
// register_on_commit_action
// ---------------------------------------------------------------------------

pub unsafe fn register_on_commit_action(relid: Oid, action: OnCommitAction) {
    let oc: *mut OnCommitItem;
    let oldcxt: MemoryContext;

    if action == ONCOMMIT_NOOP || action == ONCOMMIT_PRESERVE_ROWS {
        return;
    }

    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    oc = palloc(core::mem::size_of::<OnCommitItem>()) as *mut OnCommitItem;
    (*oc).relid = relid;
    (*oc).oncommit = action;
    (*oc).creating_subid = GetCurrentSubTransactionId();
    (*oc).deleting_subid = InvalidSubTransactionId;

    on_commits = lcons(oc as *mut libc::c_void, on_commits);

    MemoryContextSwitchTo(oldcxt);
}

// ---------------------------------------------------------------------------
// remove_on_commit_action
// ---------------------------------------------------------------------------

pub unsafe fn remove_on_commit_action(relid: Oid) {
    let mut lc: *mut ListCell = list_head(on_commits);
    while !lc.is_null() {
        let oc: *mut OnCommitItem = lfirst(lc) as *mut OnCommitItem;
        if (*oc).relid == relid {
            (*oc).deleting_subid = GetCurrentSubTransactionId();
            break;
        }
        lc = lnext(on_commits, lc);
    }
}

// ---------------------------------------------------------------------------
// PreCommit_on_commit_actions
// ---------------------------------------------------------------------------

pub unsafe fn PreCommit_on_commit_actions() {
    let mut oids_to_truncate: *mut List = NIL;
    let mut oids_to_drop: *mut List = NIL;

    let mut lc: *mut ListCell = list_head(on_commits);
    while !lc.is_null() {
        let oc: *mut OnCommitItem = lfirst(lc) as *mut OnCommitItem;
        lc = lnext(on_commits, lc);

        /* Ignore entry if already dropped in this xact */
        if (*oc).deleting_subid != InvalidSubTransactionId {
            continue;
        }

        match (*oc).oncommit {
            ONCOMMIT_NOOP | ONCOMMIT_PRESERVE_ROWS => {
                /* Do nothing */
            }
            ONCOMMIT_DELETE_ROWS => {
                if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0 {
                    oids_to_truncate = lappend_oid(oids_to_truncate, (*oc).relid);
                }
            }
            ONCOMMIT_DROP => {
                oids_to_drop = lappend_oid(oids_to_drop, (*oc).relid);
            }
            _ => {}
        }
    }

    if oids_to_truncate != NIL {
        heap_truncate(oids_to_truncate);
    }

    if oids_to_drop != NIL {
        let target_objects: *mut ObjectAddresses = new_object_addresses();

        let mut lc2: *mut ListCell = list_head(oids_to_drop);
        while !lc2.is_null() {
            let mut object: ObjectAddress = core::mem::zeroed();
            object.classId = RelationRelationId;
            object.objectId = lfirst_oid(lc2);
            object.objectSubId = 0;

            Assert!(!object_address_present(&object, target_objects));
            add_exact_object_address(&object, target_objects);

            lc2 = lnext(oids_to_drop, lc2);
        }

        PushActiveSnapshot(GetTransactionSnapshot());
        performMultipleDeletions(
            target_objects,
            DROP_CASCADE,
            PERFORM_DELETION_INTERNAL | PERFORM_DELETION_QUIETLY,
        );
        PopActiveSnapshot();

        /* Assert that all ON COMMIT DROP entries were deleted */
        #[cfg(debug_assertions)]
        {
            let mut lc3: *mut ListCell = list_head(on_commits);
            while !lc3.is_null() {
                let oc: *mut OnCommitItem = lfirst(lc3) as *mut OnCommitItem;
                lc3 = lnext(on_commits, lc3);
                if (*oc).oncommit != ONCOMMIT_DROP {
                    continue;
                }
                Assert!((*oc).deleting_subid != InvalidSubTransactionId);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AtEOXact_on_commit_actions
// ---------------------------------------------------------------------------

pub unsafe fn AtEOXact_on_commit_actions(is_commit: bool) {
    let mut cur_item: *mut ListCell = list_head(on_commits);
    while !cur_item.is_null() {
        let oc: *mut OnCommitItem = lfirst(cur_item) as *mut OnCommitItem;
        let next_item: *mut ListCell = lnext(on_commits, cur_item);

        let should_remove = if is_commit {
            (*oc).deleting_subid != InvalidSubTransactionId
        } else {
            (*oc).creating_subid != InvalidSubTransactionId
        };

        if should_remove {
            on_commits = list_delete_cell(on_commits, cur_item);
            pfree(oc as *mut libc::c_void);
        } else {
            (*oc).creating_subid = InvalidSubTransactionId;
            (*oc).deleting_subid = InvalidSubTransactionId;
        }

        cur_item = next_item;
    }
}

// ---------------------------------------------------------------------------
// AtEOSubXact_on_commit_actions
// ---------------------------------------------------------------------------

pub unsafe fn AtEOSubXact_on_commit_actions(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    let mut cur_item: *mut ListCell = list_head(on_commits);
    while !cur_item.is_null() {
        let oc: *mut OnCommitItem = lfirst(cur_item) as *mut OnCommitItem;
        let next_item: *mut ListCell = lnext(on_commits, cur_item);

        if !is_commit && (*oc).creating_subid == my_subid {
            on_commits = list_delete_cell(on_commits, cur_item);
            pfree(oc as *mut libc::c_void);
        } else {
            if (*oc).creating_subid == my_subid {
                (*oc).creating_subid = parent_subid;
            }
            if (*oc).deleting_subid == my_subid {
                (*oc).deleting_subid = if is_commit {
                    parent_subid
                } else {
                    InvalidSubTransactionId
                };
            }
        }

        cur_item = next_item;
    }
}

// ---------------------------------------------------------------------------
// RangeVarCallbackMaintainsTable
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn RangeVarCallbackMaintainsTable(
    relation: *const RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
    _arg: *mut libc::c_void,
) {
    let relkind: libc::c_char;
    let acl_result: AclResult;

    if !OidIsValid(rel_id) {
        return;
    }

    relkind = get_rel_relkind(rel_id);
    if relkind == 0 {
        return;
    }
    if relkind as u8 != RELKIND_RELATION
        && relkind as u8 != RELKIND_TOASTVALUE
        && relkind as u8 != RELKIND_MATVIEW
        && relkind as u8 != RELKIND_PARTITIONED_TABLE
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a table or materialized view",
                CStr::from_ptr((*relation).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    acl_result = pg_class_aclcheck(rel_id, GetUserId(), ACL_MAINTAIN);
    if acl_result != ACLCHECK_OK {
        aclcheck_error(
            acl_result,
            get_relkind_objtype(get_rel_relkind(rel_id)),
            (*relation).relname,
        );
    }
}

// ---------------------------------------------------------------------------
// RangeVarCallbackForTruncate (static)
// ---------------------------------------------------------------------------

unsafe extern "C" fn RangeVarCallbackForTruncate(
    relation: *const RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
    _arg: *mut libc::c_void,
) {
    let tuple: HeapTuple;

    if !OidIsValid(rel_id) {
        return;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", rel_id);
    }

    truncate_check_rel(rel_id, GETSTRUCT(tuple) as Form_pg_class);
    truncate_check_perms(rel_id, GETSTRUCT(tuple) as Form_pg_class);

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// RangeVarCallbackOwnsRelation
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn RangeVarCallbackOwnsRelation(
    relation: *const RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
    _arg: *mut libc::c_void,
) {
    let tuple: HeapTuple;

    if !OidIsValid(rel_id) {
        return;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", rel_id);
    }

    if !object_ownercheck(RelationRelationId, rel_id, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(rel_id)),
            (*relation).relname,
        );
    }

    if !allowSystemTableMods
        && IsSystemClass(rel_id, GETSTRUCT(tuple) as Form_pg_class)
    {
        ereport!(
            ERROR,
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                CStr::from_ptr((*relation).relname).to_string_lossy()
            ) /* errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// RangeVarCallbackForAlterRelation (static)
// ---------------------------------------------------------------------------

unsafe extern "C" fn RangeVarCallbackForAlterRelation(
    rv: *const RangeVar,
    relid: Oid,
    _oldrelid: Oid,
    arg: *mut libc::c_void,
) {
    let stmt: *mut Node = arg as *mut Node;
    let reltype: ObjectType;
    let tuple: HeapTuple;
    let classform: Form_pg_class;
    let acl_result: AclResult;
    let relkind: libc::c_char;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped */
    }
    classform = GETSTRUCT(tuple) as Form_pg_class;
    relkind = (*classform).relkind;

    /* Must own relation. */
    if !object_ownercheck(RelationRelationId, relid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(relid)),
            (*rv).relname,
        );
    }

    /* No system table modifications unless explicitly allowed. */
    if !allowSystemTableMods && IsSystemClass(relid, classform) {
        ereport!(
            ERROR,
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    if IsA!(stmt, T_RenameStmt) {
        acl_result = object_aclcheck(
            NamespaceRelationId,
            (*classform).relnamespace,
            GetUserId(),
            ACL_CREATE,
        );
        if acl_result != ACLCHECK_OK {
            aclcheck_error(acl_result, OBJECT_SCHEMA,
                           get_namespace_name((*classform).relnamespace));
        }
        reltype = (*(castNode!(RenameStmt, T_RenameStmt, stmt))).renameType;
    } else if IsA!(stmt, T_AlterObjectSchemaStmt) {
        reltype = (*(castNode!(AlterObjectSchemaStmt, T_AlterObjectSchemaStmt, stmt))).objectType;
    } else if IsA!(stmt, T_AlterTableStmt) {
        reltype = (*(castNode!(AlterTableStmt, T_AlterTableStmt, stmt))).objtype;
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(stmt) as u32);
        reltype = OBJECT_TABLE; /* placate compiler */
    }

    if reltype == OBJECT_SEQUENCE && relkind as u8 != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_VIEW && relkind as u8 != RELKIND_VIEW {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a view",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_MATVIEW && relkind as u8 != RELKIND_MATVIEW {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a materialized view",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_FOREIGN_TABLE && relkind as u8 != RELKIND_FOREIGN_TABLE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a foreign table",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_TYPE && relkind as u8 != RELKIND_COMPOSITE_TYPE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a composite type",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_INDEX
        && relkind as u8 != RELKIND_INDEX
        && relkind as u8 != RELKIND_PARTITIONED_INDEX
        && !IsA!(stmt, T_RenameStmt)
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not an index",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype != OBJECT_TYPE && relkind as u8 == RELKIND_COMPOSITE_TYPE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is a composite type",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            )
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
               errhint("Use ALTER TYPE instead.") */
        );
    }

    if IsA!(stmt, T_AlterObjectSchemaStmt) {
        if relkind as u8 == RELKIND_INDEX || relkind as u8 == RELKIND_PARTITIONED_INDEX {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change schema of index \"{}\"",
                    CStr::from_ptr((*rv).relname).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Change the schema of the table instead.") */
            );
        } else if relkind as u8 == RELKIND_COMPOSITE_TYPE {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change schema of composite type \"{}\"",
                    CStr::from_ptr((*rv).relname).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Use ALTER TYPE instead.") */
            );
        } else if relkind as u8 == RELKIND_TOASTVALUE {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change schema of TOAST table \"{}\"",
                    CStr::from_ptr((*rv).relname).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Change the schema of the table instead.") */
            );
        }
    }

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// transformPartitionSpec (static)
// ---------------------------------------------------------------------------

unsafe fn transformPartitionSpec(
    rel: Relation,
    partspec: *mut PartitionSpec,
) -> *mut PartitionSpec {
    let newspec: *mut PartitionSpec;
    let pstate: *mut ParseState;
    let nsitem: *mut ParseNamespaceItem;

    newspec = makeNode!(PartitionSpec, T_PartitionSpec) as *mut PartitionSpec;

    (*newspec).strategy = (*partspec).strategy;
    (*newspec).partParams = NIL;
    (*newspec).location = (*partspec).location;

    /* Check valid number of columns for strategy */
    if (*partspec).strategy == PARTITION_STRATEGY_LIST
        && list_length((*partspec).partParams) != 1
    {
        ereport!(
            ERROR,
            errmsg!("cannot use \"list\" partition strategy with more than one column")
            /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        );
    }

    pstate = make_parsestate(core::ptr::null_mut());
    nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
                                           core::ptr::null_mut(), false, true);
    addNSItemToQuery(pstate, nsitem, true, true, true);

    /* take care of any partition expressions */
    let mut lc: *mut ListCell = list_head((*partspec).partParams);
    while !lc.is_null() {
        let mut pelem: *mut PartitionElem =
            lfirst_node!(PartitionElem, T_PartitionElem, lc) as *mut PartitionElem;
        lc = lnext((*partspec).partParams, lc);

        if !(*pelem).expr.is_null() {
            /* Copy, to avoid scribbling on the input */
            pelem = copyObject(pelem as *mut libc::c_void) as *mut PartitionElem;

            /* Now do parse transformation of the expression */
            (*pelem).expr = transformExpr(pstate, (*pelem).expr,
                                          EXPR_KIND_PARTITION_EXPRESSION);

            /* we have to fix its collations too */
            assign_expr_collations(pstate, (*pelem).expr);
        }

        (*newspec).partParams = lappend((*newspec).partParams, pelem as *mut libc::c_void);
    }

    newspec
}

// ---------------------------------------------------------------------------
// ComputePartitionAttrs (static)
// ---------------------------------------------------------------------------

unsafe fn ComputePartitionAttrs(
    pstate: *mut ParseState,
    rel: Relation,
    part_params: *mut List,
    partattrs: *mut AttrNumber,
    partexprs: *mut *mut List,
    partopclass: *mut Oid,
    partcollation: *mut Oid,
    strategy: PartitionStrategy,
) {
    let mut attn: i32 = 0;
    let am_oid: Oid;

    let mut lc: *mut ListCell = list_head(part_params);
    while !lc.is_null() {
        let pelem: *mut PartitionElem =
            lfirst_node!(PartitionElem, T_PartitionElem, lc) as *mut PartitionElem;
        lc = lnext(part_params, lc);
        let atttype: Oid;
        let mut attcollation: Oid;

        if !(*pelem).name.is_null() {
            /* Simple attribute reference */
            let atttuple: HeapTuple;
            let attform: Form_pg_attribute;

            atttuple = SearchSysCacheAttName(RelationGetRelid(rel), (*pelem).name);
            if !HeapTupleIsValid(atttuple) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" named in partition key does not exist",
                        CStr::from_ptr((*pelem).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_UNDEFINED_COLUMN),
                       parser_errposition(pstate, pelem->location) */
                );
            }
            attform = GETSTRUCT(atttuple) as Form_pg_attribute;

            if (*attform).attnum <= 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot use system column \"{}\" in partition key",
                        CStr::from_ptr((*pelem).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            if (*attform).attgenerated != 0 {
                ereport!(
                    ERROR,
                    errmsg!("cannot use generated column in partition key")
                    /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errdetail("Column ... is a generated column.") */
                );
            }

            *partattrs.add(attn as usize) = (*attform).attnum;
            atttype = (*attform).atttypid;
            attcollation = (*attform).attcollation;
            ReleaseSysCache(atttuple);
        } else {
            /* Expression */
            let mut expr: *mut Node = (*pelem).expr;
            let mut partattname: [libc::c_char; 16] = core::mem::zeroed();
            let mut expr_attrs: *mut Bitmapset = core::ptr::null_mut();

            Assert!(!expr.is_null());
            atttype = exprType(expr);
            attcollation = exprCollation(expr);

            libc::snprintf(
                partattname.as_mut_ptr(),
                partattname.len(),
                c"%d".as_ptr(),
                attn + 1,
            );
            CheckAttributeType(
                partattname.as_ptr(),
                atttype,
                attcollation,
                NIL,
                CHKATYPE_IS_PARTKEY as i32,
            );

            /* Strip any top-level COLLATE clause. */
            while IsA!(expr, T_CollateExpr) {
                expr = (*(expr as *mut CollateExpr)).arg as *mut Node;
            }

            pull_varattnos(expr, 1, &mut expr_attrs);
            if bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, expr_attrs) {
                expr_attrs = bms_add_range(
                    expr_attrs,
                    1 - FirstLowInvalidHeapAttributeNumber,
                    RelationGetNumberOfAttributes(rel) - FirstLowInvalidHeapAttributeNumber,
                );
                expr_attrs = bms_del_member(
                    expr_attrs,
                    0 - FirstLowInvalidHeapAttributeNumber,
                );
            }

            let mut i: i32 = -1;
            loop {
                i = bms_next_member(expr_attrs, i);
                if i < 0 {
                    break;
                }
                let attno: AttrNumber = (i + FirstLowInvalidHeapAttributeNumber) as AttrNumber;
                Assert!(attno != 0);

                if attno < 0 {
                    ereport!(
                        ERROR,
                        errmsg!("partition key expressions cannot contain system column references")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }

                if (*(TupleDescAttr(RelationGetDescr(rel), (attno - 1) as usize)
                    as Form_pg_attribute))
                    .attgenerated
                    != 0
                {
                    ereport!(
                        ERROR,
                        errmsg!("cannot use generated column in partition key")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }
            }

            if IsA!(expr, T_Var) && (*(expr as *mut Var)).varattno > 0 {
                *partattrs.add(attn as usize) = (*(expr as *mut Var)).varattno;
            } else {
                *partattrs.add(attn as usize) = 0;
                *partexprs = lappend(*partexprs, expr as *mut libc::c_void);

                expr = expression_planner(expr as *mut Expr) as *mut Node;

                if contain_mutable_functions(expr) {
                    ereport!(
                        ERROR,
                        errmsg!("functions in partition key expression must be marked IMMUTABLE")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }

                if IsA!(expr, T_Const) {
                    ereport!(
                        ERROR,
                        errmsg!("cannot use constant expression as partition key")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }
            }
        }

        /* Apply collation override if any */
        if !(*pelem).collation.is_null() {
            attcollation = get_collation_oid((*pelem).collation, false);
        }

        if type_is_collatable(atttype) {
            if !OidIsValid(attcollation) {
                ereport!(
                    ERROR,
                    errmsg!("could not determine which collation to use for partition expression")
                    /* errcode(ERRCODE_INDETERMINATE_COLLATION),
                       errhint("Use the COLLATE clause to set the collation explicitly.") */
                );
            }
        } else {
            if OidIsValid(attcollation) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "collations are not supported by type {}",
                        CStr::from_ptr(format_type_be(atttype)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
        }

        *partcollation.add(attn as usize) = attcollation;

        if strategy == PARTITION_STRATEGY_HASH {
            am_oid = HASH_AM_OID;
        } else {
            am_oid = BTREE_AM_OID;
        }

        if (*pelem).opclass.is_null() {
            *partopclass.add(attn as usize) = GetDefaultOpClass(atttype, am_oid);

            if !OidIsValid(*partopclass.add(attn as usize)) {
                if strategy == PARTITION_STRATEGY_HASH {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "data type {} has no default operator class for access method \"hash\"",
                            CStr::from_ptr(format_type_be(atttype)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_UNDEFINED_OBJECT) */
                    );
                } else {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "data type {} has no default operator class for access method \"btree\"",
                            CStr::from_ptr(format_type_be(atttype)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_UNDEFINED_OBJECT) */
                    );
                }
            }
        } else {
            *partopclass.add(attn as usize) = ResolveOpClass(
                (*pelem).opclass,
                atttype,
                if am_oid == HASH_AM_OID {
                    c"hash".as_ptr()
                } else {
                    c"btree".as_ptr()
                },
                am_oid,
            );
        }

        attn += 1;
    }
}

// ---------------------------------------------------------------------------
// PartConstraintImpliedByRelConstraint
// ---------------------------------------------------------------------------

pub unsafe fn PartConstraintImpliedByRelConstraint(
    scanrel: Relation,
    part_constraint: *mut List,
) -> bool {
    let mut exist_constraint: *mut List = NIL;
    let constr: *mut TupleConstr = (*RelationGetDescr(scanrel)).constr;

    if !constr.is_null() && (*constr).has_not_null {
        let natts: i32 = (*(*scanrel).rd_att).natts as i32;

        for i in 1..=natts {
            let att: *mut CompactAttribute =
                TupleDescCompactAttr((*scanrel).rd_att, (i - 1) as usize);

            /* invalid not-null constraint must be ignored here */
            if (*att).attnullability == ATTNULLABLE_VALID && !(*att).attisdropped {
                let whole_att: Form_pg_attribute =
                    TupleDescAttr((*scanrel).rd_att, (i - 1) as usize) as Form_pg_attribute;
                let ntest: *mut NullTest = makeNode!(NullTest, T_NullTest) as *mut NullTest;

                (*ntest).arg = makeVar(1, i as AttrNumber,
                                       (*whole_att).atttypid,
                                       (*whole_att).atttypmod,
                                       (*whole_att).attcollation,
                                       0) as *mut Expr;
                (*ntest).nulltesttype = IS_NOT_NULL;
                (*ntest).argisrow = false;
                (*ntest).location = -1;
                exist_constraint = lappend(exist_constraint, ntest as *mut libc::c_void);
            }
        }
    }

    ConstraintImpliedByRelConstraint(scanrel, part_constraint, exist_constraint)
}

// ---------------------------------------------------------------------------
// ConstraintImpliedByRelConstraint
// ---------------------------------------------------------------------------

pub unsafe fn ConstraintImpliedByRelConstraint(
    scanrel: Relation,
    test_constraint: *mut List,
    proven_constraint: *mut List,
) -> bool {
    let mut exist_constraint: *mut List = list_copy(proven_constraint);
    let constr: *mut TupleConstr = (*RelationGetDescr(scanrel)).constr;
    let num_check: i32 = if !constr.is_null() { (*constr).num_check as i32 } else { 0 };

    for i in 0..num_check {
        let mut cexpr: *mut Node;

        if !(*(*constr).check.add(i as usize)).ccvalid {
            continue;
        }

        Assert!((*(*constr).check.add(i as usize)).ccenforced);

        cexpr = stringToNode((*(*constr).check.add(i as usize)).ccbin) as *mut Node;

        cexpr = eval_const_expressions(core::ptr::null_mut(), cexpr);
        cexpr = canonicalize_qual(cexpr as *mut Expr, true) as *mut Node;

        exist_constraint = list_concat(
            exist_constraint,
            make_ands_implicit(cexpr as *mut Expr),
        );
    }

    predicate_implied_by(test_constraint, exist_constraint, true)
}

// ---------------------------------------------------------------------------
// QueuePartitionConstraintValidation (static)
// ---------------------------------------------------------------------------

unsafe fn QueuePartitionConstraintValidation(
    wqueue: *mut *mut List,
    scanrel: Relation,
    part_constraint: *mut List,
    validate_default: bool,
) {
    if PartConstraintImpliedByRelConstraint(scanrel, part_constraint) {
        if !validate_default {
            ereport!(
                DEBUG1,
                errmsg_internal!(
                    "partition constraint for table \"{}\" is implied by existing constraints",
                    CStr::from_ptr(RelationGetRelationName(scanrel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                DEBUG1,
                errmsg_internal!(
                    "updated partition constraint for default partition \"{}\" is implied by existing constraints",
                    CStr::from_ptr(RelationGetRelationName(scanrel)).to_string_lossy()
                )
            );
        }
        return;
    }

    if (*(*scanrel).rd_rel).relkind as u8 == RELKIND_RELATION {
        let tab: *mut AlteredTableInfo;

        tab = ATGetQueueEntry(wqueue, scanrel);
        Assert!((*tab).partition_constraint.is_null());
        (*tab).partition_constraint =
            linitial(part_constraint) as *mut Expr;
        (*tab).validate_default = validate_default;
    } else if (*(*scanrel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        let partdesc: PartitionDesc = RelationGetPartitionDesc(scanrel, true);

        for i in 0..(*partdesc).nparts {
            let part_rel: Relation;
            let this_part_constraint: *mut List;

            part_rel = table_open(*(*partdesc).oids.add(i as usize), AccessExclusiveLock);

            this_part_constraint =
                map_partition_varattnos(part_constraint, 1, part_rel, scanrel);

            QueuePartitionConstraintValidation(
                wqueue,
                part_rel,
                this_part_constraint,
                validate_default,
            );
            table_close(part_rel, NoLock);
        }
    }
}

// ---------------------------------------------------------------------------
// ATExecAttachPartition (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecAttachPartition(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut PartitionCmd,
    context: *mut AlterTableUtilityContext,
) -> ObjectAddress {
    let attachrel: Relation;
    let catalog: Relation;
    let attachrel_children: *mut List;
    let mut part_constraint: *mut List;
    let scan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let address: ObjectAddress;
    let trigger_name: *const libc::c_char;
    let default_part_oid: Oid;
    let part_bound_constraint: *mut List;
    let pstate: *mut ParseState = make_parsestate(core::ptr::null_mut());

    (*pstate).p_sourcetext = (*context).queryString;

    /*
     * We must lock the default partition if one exists, because attaching a
     * new partition will change its partition constraint.
     */
    default_part_oid =
        get_default_oid_from_partdesc(RelationGetPartitionDesc(rel, true));
    if OidIsValid(default_part_oid) {
        LockRelationOid(default_part_oid, AccessExclusiveLock);
    }

    attachrel = table_openrv((*cmd).name, AccessExclusiveLock);

    ATSimplePermissions(
        AT_AttachPartition,
        attachrel,
        ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
    );

    /* A partition can only have one parent */
    if (*(*attachrel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is already a partition",
                CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if OidIsValid((*(*attachrel).rd_rel).reloftype) {
        ereport!(
            ERROR,
            errmsg!("cannot attach a typed table as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Table being attached should not already be part of inheritance: child */
    catalog = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut skey,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(attachrel)),
    );
    scan = systable_beginscan(
        catalog,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey,
    );
    if HeapTupleIsValid(systable_getnext(scan)) {
        ereport!(
            ERROR,
            errmsg!("cannot attach inheritance child as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    systable_endscan(scan);

    /* ...or as a parent table (except when it is partitioned) */
    ScanKeyInit(
        &mut skey,
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(attachrel)),
    );
    scan = systable_beginscan(
        catalog,
        InheritsParentIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey,
    );
    if HeapTupleIsValid(systable_getnext(scan))
        && (*(*attachrel).rd_rel).relkind as u8 == RELKIND_RELATION
    {
        ereport!(
            ERROR,
            errmsg!("cannot attach inheritance parent as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    systable_endscan(scan);
    table_close(catalog, AccessShareLock);

    attachrel_children = find_all_inheritors(
        RelationGetRelid(attachrel),
        AccessExclusiveLock,
        core::ptr::null_mut(),
    );
    if list_member_oid(attachrel_children, RelationGetRelid(rel)) {
        ereport!(
            ERROR,
            errmsg!("circular inheritance not allowed")
            /* errcode(ERRCODE_DUPLICATE_TABLE),
               errdetail("... is already a child of ...") */
        );
    }

    if (*(*rel).rd_rel).relpersistence != RELPERSISTENCE_TEMP as libc::c_char
        && (*(*attachrel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot attach a temporary relation as partition of permanent relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
        && (*(*attachrel).rd_rel).relpersistence != RELPERSISTENCE_TEMP as libc::c_char
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot attach a permanent relation as partition of temporary relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
        && !(*rel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot attach as partition of temporary relation of another session")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*attachrel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
        && !(*attachrel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot attach temporary relation of another session as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Check for identity columns or columns not in parent */
    let tuple_desc: TupleDesc = RelationGetDescr(attachrel);
    let natts: i32 = (*tuple_desc).natts as i32;
    for attno in 1..=natts {
        let attribute: Form_pg_attribute =
            TupleDescAttr(tuple_desc, (attno - 1) as usize) as Form_pg_attribute;
        let attribute_name: *const libc::c_char = NameStr!((*attribute).attname);

        if (*attribute).attisdropped {
            continue;
        }

        if (*attribute).attidentity != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "table \"{}\" being attached contains an identity column \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy(),
                    CStr::from_ptr(attribute_name).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   errdetail("The new partition may not contain an identity column.") */
            );
        }

        if !SearchSysCacheExists2(
            ATTNAME,
            ObjectIdGetDatum(RelationGetRelid(rel)),
            CStringGetDatum(attribute_name),
        ) {
            ereport!(
                ERROR,
                errmsg!(
                    "table \"{}\" contains column \"{}\" not found in parent \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy(),
                    CStr::from_ptr(attribute_name).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_DATATYPE_MISMATCH),
                   errdetail("The new partition may contain only the columns present in parent.") */
            );
        }
    }

    trigger_name = FindTriggerIncompatibleWithInheritance((*attachrel).trigdesc);
    if !trigger_name.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "trigger \"{}\" prevents table \"{}\" from becoming a partition",
                CStr::from_ptr(trigger_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy()
            )
            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("ROW triggers with transition tables are not supported on partitions.") */
        );
    }

    check_new_partition_bound(
        RelationGetRelationName(attachrel) as *mut libc::c_char,
        rel,
        (*cmd).bound,
        pstate,
    );

    /* OK to create inheritance. Rest of the checks performed there */
    CreateInheritance(attachrel, rel, true);

    /* Update the pg_class entry. */
    StorePartitionBound(attachrel, rel, (*cmd).bound);

    /* Ensure there exists a correct set of indexes in the partition. */
    AttachPartitionEnsureIndexes(wqueue, rel, attachrel);

    /* and triggers */
    CloneRowTriggersToPartition(rel, attachrel);

    /* Clone foreign key constraints. */
    CloneForeignKeyConstraints(wqueue, rel, attachrel);

    part_bound_constraint = get_qual_from_partbound(rel, (*cmd).bound);
    part_constraint = list_concat_copy(part_bound_constraint, RelationGetPartitionQual(rel));

    if !part_constraint.is_null() {
        part_constraint =
            eval_const_expressions(core::ptr::null_mut(), part_constraint as *mut Node)
                as *mut List;
        part_constraint = list_make1(make_ands_explicit(part_constraint) as *mut libc::c_void);
        part_constraint =
            map_partition_varattnos(part_constraint, 1, attachrel, rel);

        QueuePartitionConstraintValidation(wqueue, attachrel, part_constraint, false);
    }

    if OidIsValid(default_part_oid) {
        let default_rel: Relation;
        let def_part_constraint: *mut List;

        Assert!(!(*(*cmd).bound).is_default);

        default_rel = table_open(default_part_oid, NoLock);
        def_part_constraint = get_proposed_default_constraint(part_bound_constraint);
        let def_part_constraint = map_partition_varattnos(
            def_part_constraint,
            1,
            default_rel,
            rel,
        );
        QueuePartitionConstraintValidation(wqueue, default_rel, def_part_constraint, true);

        table_close(default_rel, NoLock);
    }

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(attachrel));

    if (*(*attachrel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        let mut lc: *mut ListCell = list_head(attachrel_children);
        while !lc.is_null() {
            CacheInvalidateRelcacheByRelid(lfirst_oid(lc));
            lc = lnext(attachrel_children, lc);
        }
    }

    table_close(attachrel, NoLock);
    address
}

// ---------------------------------------------------------------------------
// AttachPartitionEnsureIndexes (static)
// ---------------------------------------------------------------------------

unsafe fn AttachPartitionEnsureIndexes(
    wqueue: *mut *mut List,
    rel: Relation,
    attachrel: Relation,
) {
    let idxes: *mut List;
    let attach_rel_idxs: *mut List;
    let attach_rel_idx_rels: *mut Relation;
    let attach_infos: *mut *mut IndexInfo;
    let cxt: MemoryContext;
    let oldcxt: MemoryContext;

    cxt = AllocSetContextCreate(
        CurrentMemoryContext,
        c"AttachPartitionEnsureIndexes".as_ptr(),
        ALLOCSET_DEFAULT_SIZES!(),
    );
    oldcxt = MemoryContextSwitchTo(cxt);

    idxes = RelationGetIndexList(rel);
    attach_rel_idxs = RelationGetIndexList(attachrel);
    let n_attach_idxs = list_length(attach_rel_idxs) as usize;
    attach_rel_idx_rels =
        palloc(core::mem::size_of::<Relation>() * n_attach_idxs) as *mut Relation;
    attach_infos =
        palloc(core::mem::size_of::<*mut IndexInfo>() * n_attach_idxs) as *mut *mut IndexInfo;

    /* Build arrays of all existing indexes and their IndexInfos */
    {
        let mut i: usize = 0;
        let mut lc: *mut ListCell = list_head(attach_rel_idxs);
        while !lc.is_null() {
            let cld_idx_id: Oid = lfirst_oid(lc);
            *attach_rel_idx_rels.add(i) = index_open(cld_idx_id, AccessShareLock);
            *attach_infos.add(i) = BuildIndexInfo(*attach_rel_idx_rels.add(i));
            i += 1;
            lc = lnext(attach_rel_idxs, lc);
        }
    }

    /* goto out target -- use a labeled block */
    'out: {
        /*
         * If attaching a foreign table, fail if any constraint index exists.
         */
        if (*(*attachrel).rd_rel).relkind as u8 == RELKIND_FOREIGN_TABLE {
            let mut cell: *mut ListCell = list_head(idxes);
            while !cell.is_null() {
                let idx: Oid = lfirst_oid(cell);
                let idx_rel: Relation = index_open(idx, AccessShareLock);

                if (*(*idx_rel).rd_index).indisunique || (*(*idx_rel).rd_index).indisprimary {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot attach foreign table \"{}\" as partition of partitioned table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errdetail("Partitioned table ... contains unique indexes.") */
                    );
                }
                index_close(idx_rel, AccessShareLock);
                cell = lnext(idxes, cell);
            }

            break 'out;
        }

        /* For each index on partitioned table, find or create matching one. */
        let mut cell: *mut ListCell = list_head(idxes);
        while !cell.is_null() {
            let idx: Oid = lfirst_oid(cell);
            let idx_rel: Relation = index_open(idx, AccessShareLock);
            let info: *mut IndexInfo;
            let attmap: *mut AttrMap;
            let mut found: bool = false;
            let constraint_oid: Oid;

            /* Ignore non-partitioned indexes in the partitioned table */
            if (*(*idx_rel).rd_rel).relkind as u8 != RELKIND_PARTITIONED_INDEX {
                index_close(idx_rel, AccessShareLock);
                cell = lnext(idxes, cell);
                continue;
            }

            info = BuildIndexInfo(idx_rel);
            attmap = build_attrmap_by_name(
                RelationGetDescr(attachrel),
                RelationGetDescr(rel),
                false,
            );
            constraint_oid =
                get_relation_idx_constraint_oid(RelationGetRelid(rel), idx);

            for i in 0..n_attach_idxs {
                let cld_idx_id: Oid = RelationGetRelid(*attach_rel_idx_rels.add(i));
                let mut cld_constr_oid: Oid = InvalidOid;

                /* does this index have a parent?  if so, can't use it */
                if (*(*attach_rel_idx_rels.add(i)).rd_rel).relispartition {
                    continue;
                }

                /* If this index is invalid, can't use it */
                if !(*(*(*attach_rel_idx_rels.add(i)).rd_index)).indisvalid {
                    continue;
                }

                if CompareIndexInfo(
                    *attach_infos.add(i),
                    info,
                    (*(*attach_rel_idx_rels.add(i))).rd_indcollation,
                    (*idx_rel).rd_indcollation,
                    (*(*attach_rel_idx_rels.add(i))).rd_opfamily,
                    (*idx_rel).rd_opfamily,
                    attmap,
                ) {
                    if OidIsValid(constraint_oid) {
                        cld_constr_oid = get_relation_idx_constraint_oid(
                            RelationGetRelid(attachrel),
                            cld_idx_id,
                        );
                        if !OidIsValid(cld_constr_oid) {
                            continue;
                        }

                        if get_constraint_type(constraint_oid)
                            != get_constraint_type(cld_constr_oid)
                        {
                            continue;
                        }
                    }

                    /* bingo. */
                    IndexSetParentIndex(*attach_rel_idx_rels.add(i), idx);
                    if OidIsValid(constraint_oid) {
                        ConstraintSetParentConstraint(
                            cld_constr_oid,
                            constraint_oid,
                            RelationGetRelid(attachrel),
                        );
                    }
                    found = true;
                    CommandCounterIncrement();
                    break;
                }
            }

            if !found {
                let stmt: *mut IndexStmt;
                let con_oid: Oid;

                stmt = generateClonedIndexStmt(
                    core::ptr::null_mut(),
                    idx_rel,
                    attmap,
                    &mut (con_oid as Oid) as *mut Oid,
                );
                DefineIndex(
                    RelationGetRelid(attachrel),
                    stmt,
                    InvalidOid,
                    RelationGetRelid(idx_rel),
                    con_oid,
                    -1,
                    true,
                    false,
                    false,
                    false,
                    false,
                );
            }

            index_close(idx_rel, AccessShareLock);
            cell = lnext(idxes, cell);
        }
    } // 'out

    /* Clean up. */
    for i in 0..n_attach_idxs {
        index_close(*attach_rel_idx_rels.add(i), AccessShareLock);
    }
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(cxt);

    let _ = wqueue;
}

// ---------------------------------------------------------------------------
// CloneRowTriggersToPartition (static)
// ---------------------------------------------------------------------------

unsafe fn CloneRowTriggersToPartition(parent: Relation, partition: Relation) {
    let pg_trigger: Relation;
    let mut key: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tuple: HeapTuple;
    let per_tup_cxt: MemoryContext;

    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent)),
    );
    pg_trigger = table_open(TriggerRelationId, RowExclusiveLock);
    scan = systable_beginscan(
        pg_trigger,
        TriggerRelidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    per_tup_cxt = AllocSetContextCreate(
        CurrentMemoryContext,
        c"clone trig".as_ptr(),
        ALLOCSET_SMALL_SIZES!(),
    );

    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let trig_form: Form_pg_trigger = GETSTRUCT(tuple) as Form_pg_trigger;
        let trig_stmt: *mut CreateTrigStmt;
        let mut qual: *mut Node = core::ptr::null_mut();
        let mut value: Datum;
        let mut isnull: bool = false;
        let mut cols: *mut List = NIL;
        let mut trigargs: *mut List = NIL;
        let oldcxt: MemoryContext;

        /* Ignore statement-level triggers; those are not cloned. */
        if !TRIGGER_FOR_ROW!((*trig_form).tgtype as u32) {
            continue;
        }

        /* Don't clone internal triggers */
        if (*trig_form).tgisinternal {
            continue;
        }

        /* Complain if we find an unexpected trigger type. */
        if !TRIGGER_FOR_BEFORE!((*trig_form).tgtype as u32)
            && !TRIGGER_FOR_AFTER!((*trig_form).tgtype as u32)
        {
            elog!(
                ERROR,
                "unexpected trigger \"{}\" found",
                CStr::from_ptr(NameStr!((*trig_form).tgname)).to_string_lossy()
            );
        }

        oldcxt = MemoryContextSwitchTo(per_tup_cxt);

        /* If there is a WHEN clause, generate a 'cooked' version of it. */
        value = heap_getattr(
            tuple,
            Anum_pg_trigger_tgqual,
            RelationGetDescr(pg_trigger),
            &mut isnull,
        );
        if !isnull {
            qual = stringToNode(TextDatumGetCString(value)) as *mut Node;
            qual = map_partition_varattnos(
                qual as *mut List,
                PRS2_OLD_VARNO as i32,
                partition,
                parent,
            ) as *mut Node;
            qual = map_partition_varattnos(
                qual as *mut List,
                PRS2_NEW_VARNO as i32,
                partition,
                parent,
            ) as *mut Node;
        }

        /* If there is a column list, transform it. */
        if (*trig_form).tgattr.dim1 > 0 {
            for i in 0..(*trig_form).tgattr.dim1 {
                let col: Form_pg_attribute = TupleDescAttr(
                    (*parent).rd_att,
                    (*trig_form).tgattr.values[i as usize] as usize - 1,
                ) as Form_pg_attribute;
                cols = lappend(
                    cols,
                    makeString(pstrdup(NameStr!((*col).attname)) as *mut libc::c_char)
                        as *mut libc::c_void,
                );
            }
        }

        /* Reconstruct trigger arguments list. */
        if (*trig_form).tgnargs > 0 {
            let mut p: *mut libc::c_char;

            value = heap_getattr(
                tuple,
                Anum_pg_trigger_tgargs,
                RelationGetDescr(pg_trigger),
                &mut isnull,
            );
            if isnull {
                elog!(
                    ERROR,
                    "tgargs is null for trigger \"{}\" in partition \"{}\"",
                    CStr::from_ptr(NameStr!((*trig_form).tgname)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(partition)).to_string_lossy()
                );
            }

            p = VARDATA_ANY!(DatumGetByteaPP(value)) as *mut libc::c_char;

            for _ in 0..(*trig_form).tgnargs {
                trigargs = lappend(
                    trigargs,
                    makeString(pstrdup(p) as *mut libc::c_char) as *mut libc::c_void,
                );
                p = p.add(libc::strlen(p) + 1);
            }
        }

        trig_stmt = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;
        (*trig_stmt).replace = false;
        (*trig_stmt).isconstraint = OidIsValid((*trig_form).tgconstraint);
        (*trig_stmt).trigname = NameStr!((*trig_form).tgname) as *mut libc::c_char;
        (*trig_stmt).relation = core::ptr::null_mut();
        (*trig_stmt).funcname = core::ptr::null_mut(); /* passed separately */
        (*trig_stmt).args = trigargs;
        (*trig_stmt).row = true;
        (*trig_stmt).timing =
            ((*trig_form).tgtype & TRIGGER_TYPE_TIMING_MASK as i16) as i16;
        (*trig_stmt).events =
            ((*trig_form).tgtype & TRIGGER_TYPE_EVENT_MASK as i16) as i16;
        (*trig_stmt).columns = cols;
        (*trig_stmt).whenClause = core::ptr::null_mut(); /* passed separately */
        (*trig_stmt).transitionRels = NIL;
        (*trig_stmt).deferrable = (*trig_form).tgdeferrable;
        (*trig_stmt).initdeferred = (*trig_form).tginitdeferred;
        (*trig_stmt).constrrel = core::ptr::null_mut();

        CreateTriggerFiringOn(
            trig_stmt,
            core::ptr::null_mut(),
            RelationGetRelid(partition),
            (*trig_form).tgconstrrelid,
            InvalidOid,
            InvalidOid,
            (*trig_form).tgfoid,
            (*trig_form).oid,
            qual,
            false,
            true,
            (*trig_form).tgenabled,
        );

        MemoryContextSwitchTo(oldcxt);
        MemoryContextReset(per_tup_cxt);
    }

    MemoryContextDelete(per_tup_cxt);
    systable_endscan(scan);
    table_close(pg_trigger, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecDetachPartition (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecDetachPartition(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    name: *mut RangeVar,
    concurrent: bool,
) -> ObjectAddress {
    let mut part_rel: Relation;
    let address: ObjectAddress;
    let default_part_oid: Oid;
    let partdesc: PartitionDesc;

    partdesc = RelationGetPartitionDesc(rel, true);
    default_part_oid = get_default_oid_from_partdesc(partdesc);
    if OidIsValid(default_part_oid) {
        if concurrent {
            ereport!(
                ERROR,
                errmsg!("cannot detach partitions concurrently when a default partition exists")
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }
        LockRelationOid(default_part_oid, AccessExclusiveLock);
    }

    part_rel = table_openrv(
        name,
        if concurrent {
            ShareUpdateExclusiveLock
        } else {
            AccessExclusiveLock
        },
    );

    if !concurrent {
        RemoveInheritance(part_rel, rel, false);
    } else {
        MarkInheritDetached(part_rel, rel);
    }

    ATDetachCheckNoForeignKeyRefs(part_rel);

    if concurrent {
        let part_relid: Oid = RelationGetRelid(part_rel);
        let parent_relid: Oid = RelationGetRelid(rel);
        let mut tag: LOCKTAG = core::mem::zeroed();
        let parent_relname: *mut libc::c_char = MemoryContextStrdup(
            PortalContext,
            RelationGetRelationName(rel),
        );
        let part_relname: *mut libc::c_char = MemoryContextStrdup(
            PortalContext,
            RelationGetRelationName(part_rel),
        );

        if (*partdesc).boundinfo != core::ptr::null_mut()
            && (*(*partdesc).boundinfo).strategy != PARTITION_STRATEGY_HASH as libc::c_char
        {
            DetachAddConstraintIfNeeded(wqueue, part_rel);
        }

        CacheInvalidateRelcache(rel);

        table_close(part_rel, NoLock);
        table_close(rel, NoLock);
        (*tab).rel = core::ptr::null_mut();

        PopActiveSnapshot();
        CommitTransactionCommand();

        StartTransactionCommand();

        SET_LOCKTAG_RELATION!(tag, MyDatabaseId, parent_relid);
        let tag_list: *mut List = list_make1(&mut tag as *mut LOCKTAG as *mut libc::c_void);
        WaitForLockersMultiple(tag_list, AccessExclusiveLock, false);

        let rel_new = try_relation_open(parent_relid, ShareUpdateExclusiveLock);
        part_rel = try_relation_open(part_relid, AccessExclusiveLock);

        if rel_new.is_null() {
            if !part_rel.is_null() {
                elog!(
                    WARNING,
                    "dangling partition \"{}\" remains, can't fix",
                    CStr::from_ptr(part_relname).to_string_lossy()
                );
            }
            ereport!(
                ERROR,
                errmsg!(
                    "partitioned table \"{}\" was removed concurrently",
                    CStr::from_ptr(parent_relname).to_string_lossy()
                ) /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }
        if part_rel.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "partition \"{}\" was removed concurrently",
                    CStr::from_ptr(part_relname).to_string_lossy()
                ) /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }

        (*tab).rel = rel_new;
        // re-bind rel to rel_new for remaining use (we must use rel_new going forward)
        let rel = rel_new;
        let _ = rel;
    }

    PushActiveSnapshot(GetTransactionSnapshot());
    DetachPartitionFinalize(rel, part_rel, concurrent, default_part_oid);
    PopActiveSnapshot();

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(part_rel));
    table_close(part_rel, NoLock);
    address
}

// ---------------------------------------------------------------------------
// DetachPartitionFinalize (static)
// ---------------------------------------------------------------------------

unsafe fn DetachPartitionFinalize(
    rel: Relation,
    part_rel: Relation,
    concurrent: bool,
    default_part_oid: Oid,
) {
    let class_rel: Relation;
    let fks: *mut List;
    let mut cell: *mut ListCell;
    let indexes: *mut List;
    let mut new_val: [Datum; Natts_pg_class as usize] = core::mem::zeroed();
    let mut new_null: [bool; Natts_pg_class as usize] = core::mem::zeroed();
    let mut new_repl: [bool; Natts_pg_class as usize] = core::mem::zeroed();
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut trigrel: Relation = core::ptr::null_mut();
    let mut fkoids: *mut List = NIL;

    if concurrent {
        RemoveInheritance(part_rel, rel, true);
    }

    /* Drop any triggers that were cloned on creation/attach. */
    DropClonedTriggersFromPartition(RelationGetRelid(part_rel));

    /* Detach any foreign keys that are inherited. */
    fks = copyObject(RelationGetFKeyList(part_rel)) as *mut List;
    if fks != NIL {
        trigrel = table_open(TriggerRelationId, RowExclusiveLock);
    }

    /* Collect all FK OIDs first, to detect parent/child relationships */
    cell = list_head(fks);
    while !cell.is_null() {
        let fk: *mut ForeignKeyCacheInfo = lfirst(cell) as *mut ForeignKeyCacheInfo;
        fkoids = lappend_oid(fkoids, (*fk).conoid);
        cell = lnext(fks, cell);
    }

    cell = list_head(fks);
    while !cell.is_null() {
        let fk: *mut ForeignKeyCacheInfo = lfirst(cell) as *mut ForeignKeyCacheInfo;
        cell = lnext(fks, cell);
        let contup: HeapTuple;
        let conform: Form_pg_constraint;

        contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum((*fk).conoid));
        if !HeapTupleIsValid(contup) {
            elog!(ERROR, "cache lookup failed for constraint {}", (*fk).conoid);
        }
        conform = GETSTRUCT(contup) as Form_pg_constraint;

        /* Consider only inherited foreign keys, and only if parent not in list */
        if (*conform).contype != CONSTRAINT_FOREIGN as libc::c_char
            || !OidIsValid((*conform).conparentid)
            || list_member_oid(fkoids, (*conform).conparentid)
        {
            ReleaseSysCache(contup);
            continue;
        }

        ConstraintSetParentConstraint((*fk).conoid, InvalidOid, InvalidOid);

        if (*fk).conenforced {
            let mut insert_trigger_oid: Oid = InvalidOid;
            let mut update_trigger_oid: Oid = InvalidOid;

            GetForeignKeyCheckTriggers(
                trigrel,
                (*fk).conoid,
                (*fk).confrelid,
                (*fk).conrelid,
                &mut insert_trigger_oid,
                &mut update_trigger_oid,
            );
            Assert!(OidIsValid(insert_trigger_oid));
            TriggerSetParentTrigger(
                trigrel,
                insert_trigger_oid,
                InvalidOid,
                RelationGetRelid(part_rel),
            );
            Assert!(OidIsValid(update_trigger_oid));
            TriggerSetParentTrigger(
                trigrel,
                update_trigger_oid,
                InvalidOid,
                RelationGetRelid(part_rel),
            );
        }

        {
            let fkconstraint: *mut Constraint =
                makeNode!(Constraint, T_Constraint) as *mut Constraint;
            let mut numfks: i32 = 0;
            let mut conkey: [AttrNumber; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut confkey: [AttrNumber; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut conpfeqop: [Oid; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut conppeqop: [Oid; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut conffeqop: [Oid; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut numfkdelsetcols: i32 = 0;
            let mut confdelsetcols: [AttrNumber; INDEX_MAX_KEYS] = core::mem::zeroed();
            let refd_rel: Relation;

            DeconstructFkConstraintRow(
                contup,
                &mut numfks,
                conkey.as_mut_ptr(),
                confkey.as_mut_ptr(),
                conpfeqop.as_mut_ptr(),
                conppeqop.as_mut_ptr(),
                conffeqop.as_mut_ptr(),
                &mut numfkdelsetcols,
                confdelsetcols.as_mut_ptr(),
            );

            (*fkconstraint).contype = CONSTRAINT_FOREIGN;
            (*fkconstraint).conname = pstrdup(NameStr!((*conform).conname));
            (*fkconstraint).deferrable = (*conform).condeferrable;
            (*fkconstraint).initdeferred = (*conform).condeferred;
            (*fkconstraint).is_enforced = (*conform).conenforced;
            (*fkconstraint).skip_validation = true;
            (*fkconstraint).initially_valid = (*conform).convalidated;
            (*fkconstraint).pktable = core::ptr::null_mut();
            (*fkconstraint).fk_attrs = NIL;
            (*fkconstraint).pk_attrs = NIL;
            (*fkconstraint).fk_matchtype = (*conform).confmatchtype;
            (*fkconstraint).fk_upd_action = (*conform).confupdtype;
            (*fkconstraint).fk_del_action = (*conform).confdeltype;
            (*fkconstraint).fk_del_set_cols = NIL;
            (*fkconstraint).old_conpfeqop = NIL;
            (*fkconstraint).old_pktable_oid = InvalidOid;
            (*fkconstraint).location = -1;

            for i in 0..numfks as usize {
                let att: Form_pg_attribute = TupleDescAttr(
                    RelationGetDescr(part_rel),
                    conkey[i] as usize - 1,
                ) as Form_pg_attribute;
                (*fkconstraint).fk_attrs = lappend(
                    (*fkconstraint).fk_attrs,
                    makeString(NameStr!((*att).attname) as *mut libc::c_char) as *mut libc::c_void,
                );
            }

            refd_rel = table_open((*fk).confrelid, ShareRowExclusiveLock);

            addFkRecurseReferenced(
                fkconstraint,
                part_rel,
                refd_rel,
                (*conform).conindid,
                (*fk).conoid,
                numfks,
                confkey.as_mut_ptr(),
                conkey.as_mut_ptr(),
                conpfeqop.as_mut_ptr(),
                conppeqop.as_mut_ptr(),
                conffeqop.as_mut_ptr(),
                numfkdelsetcols,
                confdelsetcols.as_mut_ptr(),
                true,
                InvalidOid,
                InvalidOid,
                (*conform).conperiod,
            );
            table_close(refd_rel, NoLock);
        }

        ReleaseSysCache(contup);
    }
    list_free_deep(fks);
    if !trigrel.is_null() {
        table_close(trigrel, RowExclusiveLock);
    }

    /* Remove sub-constraints that are in the referenced-side of a larger constraint */
    let parent_fk_refs: *mut List = GetParentedForeignKeyRefs(part_rel);
    cell = list_head(parent_fk_refs);
    while !cell.is_null() {
        let constr_oid: Oid = lfirst_oid(cell);
        let mut constraint: ObjectAddress = core::mem::zeroed();
        cell = lnext(parent_fk_refs, cell);

        ConstraintSetParentConstraint(constr_oid, InvalidOid, InvalidOid);
        deleteDependencyRecordsForClass(
            ConstraintRelationId,
            constr_oid,
            ConstraintRelationId,
            DEPENDENCY_INTERNAL,
        );
        CommandCounterIncrement();

        ObjectAddressSet!(constraint, ConstraintRelationId, constr_oid);
        performDeletion(&constraint, DROP_RESTRICT, 0);
    }

    /* Now we can detach indexes */
    indexes = RelationGetIndexList(part_rel);
    cell = list_head(indexes);
    while !cell.is_null() {
        let idxid: Oid = lfirst_oid(cell);
        cell = lnext(indexes, cell);
        let parent_idx: Oid;
        let idx: Relation;
        let constr_oid: Oid;
        let parent_constr_oid: Oid;

        if !has_superclass(idxid) {
            continue;
        }

        parent_idx = get_partition_parent(idxid, false);
        Assert!(IndexGetRelation(parent_idx, false) == RelationGetRelid(rel));

        idx = index_open(idxid, AccessExclusiveLock);
        IndexSetParentIndex(idx, InvalidOid);

        constr_oid =
            get_relation_idx_constraint_oid(RelationGetRelid(part_rel), idxid);
        parent_constr_oid =
            get_relation_idx_constraint_oid(RelationGetRelid(rel), parent_idx);
        if OidIsValid(parent_constr_oid) && OidIsValid(constr_oid) {
            ConstraintSetParentConstraint(constr_oid, InvalidOid, InvalidOid);
        }

        index_close(idx, NoLock);
    }

    /* Update pg_class tuple */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(part_rel)));
    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for relation {}",
            RelationGetRelid(part_rel)
        );
    }
    Assert!((*(GETSTRUCT(tuple) as Form_pg_class)).relispartition);

    libc::memset(new_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&new_val));
    libc::memset(new_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&new_null));
    libc::memset(new_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&new_repl));
    new_val[Anum_pg_class_relpartbound as usize - 1] = 0;
    new_null[Anum_pg_class_relpartbound as usize - 1] = true;
    new_repl[Anum_pg_class_relpartbound as usize - 1] = true;
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(class_rel),
        new_val.as_mut_ptr(),
        new_null.as_mut_ptr(),
        new_repl.as_mut_ptr(),
    );

    (*(GETSTRUCT(newtuple) as Form_pg_class)).relispartition = false;
    CatalogTupleUpdate(class_rel, &(*newtuple).t_self, newtuple);
    heap_freetuple(newtuple);
    table_close(class_rel, RowExclusiveLock);

    /* Drop identity property from all identity columns of partition. */
    for attno in 0..RelationGetNumberOfAttributes(part_rel) {
        let attr: Form_pg_attribute =
            TupleDescAttr((*part_rel).rd_att, attno as usize) as Form_pg_attribute;
        if !(*attr).attisdropped && (*attr).attidentity != 0 {
            ATExecDropIdentity(
                part_rel,
                NameStr!((*attr).attname),
                false,
                AccessExclusiveLock,
                true,
                true,
            );
        }
    }

    if OidIsValid(default_part_oid) {
        if RelationGetRelid(part_rel) == default_part_oid {
            update_default_partition_oid(RelationGetRelid(rel), InvalidOid);
        } else {
            CacheInvalidateRelcacheByRelid(default_part_oid);
        }
    }

    CacheInvalidateRelcache(rel);

    if (*(*part_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        let children: *mut List = find_all_inheritors(
            RelationGetRelid(part_rel),
            AccessExclusiveLock,
            core::ptr::null_mut(),
        );
        cell = list_head(children);
        while !cell.is_null() {
            CacheInvalidateRelcacheByRelid(lfirst_oid(cell));
            cell = lnext(children, cell);
        }
    }
}

// ---------------------------------------------------------------------------
// ATExecDetachPartitionFinalize (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecDetachPartitionFinalize(rel: Relation, name: *mut RangeVar) -> ObjectAddress {
    let part_rel: Relation;
    let address: ObjectAddress;
    let snap: Snapshot = GetActiveSnapshot();

    part_rel = table_openrv(name, AccessExclusiveLock);

    WaitForOlderSnapshots((*snap).xmin, false);

    DetachPartitionFinalize(rel, part_rel, true, InvalidOid);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(part_rel));
    table_close(part_rel, NoLock);
    address
}

// ---------------------------------------------------------------------------
// DetachAddConstraintIfNeeded (static)
// ---------------------------------------------------------------------------

unsafe fn DetachAddConstraintIfNeeded(wqueue: *mut *mut List, part_rel: Relation) {
    let mut constraint_expr: *mut List;

    constraint_expr = RelationGetPartitionQual(part_rel);
    constraint_expr =
        eval_const_expressions(core::ptr::null_mut(), constraint_expr as *mut Node)
            as *mut List;

    if !PartConstraintImpliedByRelConstraint(part_rel, constraint_expr) {
        let tab: *mut AlteredTableInfo;
        let n: *mut Constraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;

        tab = ATGetQueueEntry(wqueue, part_rel);

        (*n).contype = CONSTR_CHECK;
        (*n).conname = core::ptr::null_mut();
        (*n).location = -1;
        (*n).is_no_inherit = false;
        (*n).raw_expr = core::ptr::null_mut();
        (*n).cooked_expr = nodeToString(make_ands_explicit(constraint_expr) as *mut libc::c_void);
        (*n).is_enforced = true;
        (*n).initially_valid = true;
        (*n).skip_validation = true;

        ATAddCheckNNConstraint(
            wqueue,
            tab,
            part_rel,
            n,
            true,
            false,
            true,
            ShareUpdateExclusiveLock,
        );
    }
}

// ---------------------------------------------------------------------------
// DropClonedTriggersFromPartition (static)
// ---------------------------------------------------------------------------

unsafe fn DropClonedTriggersFromPartition(partition_id: Oid) {
    let mut skey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;
    let tgrel: Relation;
    let objects: *mut ObjectAddresses;

    objects = new_object_addresses();

    ScanKeyInit(
        &mut skey,
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(partition_id),
    );
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    scan = systable_beginscan(
        tgrel,
        TriggerRelidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey,
    );

    loop {
        trigtup = systable_getnext(scan);
        if !HeapTupleIsValid(trigtup) {
            break;
        }
        let pg_trigger: Form_pg_trigger = GETSTRUCT(trigtup) as Form_pg_trigger;
        let mut trig: ObjectAddress = core::mem::zeroed();

        /* Ignore triggers that weren't cloned */
        if !OidIsValid((*pg_trigger).tgparentid) {
            continue;
        }

        /*
         * Ignore internal triggers that are implementation objects of foreign
         * keys.
         */
        if OidIsValid((*pg_trigger).tgconstrrelid) {
            continue;
        }

        deleteDependencyRecordsForClass(
            TriggerRelationId,
            (*pg_trigger).oid,
            TriggerRelationId,
            DEPENDENCY_PARTITION_PRI,
        );
        deleteDependencyRecordsForClass(
            TriggerRelationId,
            (*pg_trigger).oid,
            RelationRelationId,
            DEPENDENCY_PARTITION_SEC,
        );

        ObjectAddressSet!(trig, TriggerRelationId, (*pg_trigger).oid);
        add_exact_object_address(&trig, objects);
    }

    CommandCounterIncrement();
    performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    free_object_addresses(objects);
    systable_endscan(scan);
    table_close(tgrel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// AttachIndexCallbackState (struct) and RangeVarCallbackForAttachIndex
// ---------------------------------------------------------------------------

#[repr(C)]
struct AttachIndexCallbackState {
    partition_oid: Oid,
    parent_tbl_oid: Oid,
    locked_parent_tbl: bool,
}

unsafe extern "C" fn RangeVarCallbackForAttachIndex(
    rv: *const RangeVar,
    rel_oid: Oid,
    old_rel_oid: Oid,
    arg: *mut libc::c_void,
) {
    let state: *mut AttachIndexCallbackState = arg as *mut AttachIndexCallbackState;
    let classform: Form_pg_class;
    let tuple: HeapTuple;

    if !(*state).locked_parent_tbl {
        LockRelationOid((*state).parent_tbl_oid, AccessShareLock);
        (*state).locked_parent_tbl = true;
    }

    if rel_oid != old_rel_oid && OidIsValid((*state).partition_oid) {
        UnlockRelationOid((*state).partition_oid, AccessShareLock);
        (*state).partition_oid = InvalidOid;
    }

    if !OidIsValid(rel_oid) {
        return;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped */
    }
    classform = GETSTRUCT(tuple) as Form_pg_class;
    if (*classform).relkind as u8 != RELKIND_PARTITIONED_INDEX
        && (*classform).relkind as u8 != RELKIND_INDEX
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not an index",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        );
    }
    ReleaseSysCache(tuple);

    (*state).partition_oid = IndexGetRelation(rel_oid, false);
    LockRelationOid((*state).partition_oid, AccessShareLock);
}

// ---------------------------------------------------------------------------
// ATExecAttachPartitionIdx (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecAttachPartitionIdx(
    wqueue: *mut *mut List,
    parent_idx: Relation,
    name: *mut RangeVar,
) -> ObjectAddress {
    let part_idx: Relation;
    let part_tbl: Relation;
    let parent_tbl: Relation;
    let address: ObjectAddress;
    let part_idx_id: Oid;
    let curr_parent: Oid;
    let mut state: AttachIndexCallbackState = AttachIndexCallbackState {
        partition_oid: InvalidOid,
        parent_tbl_oid: (*(*parent_idx).rd_index).indrelid,
        locked_parent_tbl: false,
    };

    part_idx_id = RangeVarGetRelidExtended(
        name,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForAttachIndex),
        &mut state as *mut AttachIndexCallbackState as *mut libc::c_void,
    );

    if !OidIsValid(part_idx_id) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" does not exist",
                CStr::from_ptr((*name).relname).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    part_idx = relation_open(part_idx_id, AccessExclusiveLock);
    parent_tbl = relation_open((*(*parent_idx).rd_index).indrelid, AccessShareLock);
    part_tbl = relation_open((*(*part_idx).rd_index).indrelid, NoLock);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(part_idx));

    /* Silently do nothing if already in the right state */
    curr_parent = if (*(*part_idx).rd_rel).relispartition {
        get_partition_parent(part_idx_id, false)
    } else {
        InvalidOid
    };

    if curr_parent != RelationGetRelid(parent_idx) {
        let child_info: *mut IndexInfo;
        let parent_info: *mut IndexInfo;
        let attmap: *mut AttrMap;
        let mut found: bool;
        let part_desc: PartitionDesc;
        let constraint_oid: Oid;
        let mut cld_constr_id: Oid = InvalidOid;

        refuseDupeIndexAttach(parent_idx, part_idx, part_tbl);

        if OidIsValid(curr_parent) {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   errdetail("Index ... is already attached to another index.") */
            );
        }

        /* Make sure it indexes a partition of the other index's table */
        part_desc = RelationGetPartitionDesc(parent_tbl, true);
        found = false;
        for i in 0..(*part_desc).nparts {
            if *(*part_desc).oids.add(i as usize) == state.partition_oid {
                found = true;
                break;
            }
        }
        if !found {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                )
                /* errdetail("Index ... is not an index on any partition of table ...") */
            );
        }

        /* Ensure the indexes are compatible */
        child_info = BuildIndexInfo(part_idx);
        parent_info = BuildIndexInfo(parent_idx);
        attmap = build_attrmap_by_name(
            RelationGetDescr(part_tbl),
            RelationGetDescr(parent_tbl),
            false,
        );
        if !CompareIndexInfo(
            child_info,
            parent_info,
            (*part_idx).rd_indcollation,
            (*parent_idx).rd_indcollation,
            (*part_idx).rd_opfamily,
            (*parent_idx).rd_opfamily,
            attmap,
        ) {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                )
                /* errdetail("The index definitions do not match.") */
            );
        }

        constraint_oid = get_relation_idx_constraint_oid(
            RelationGetRelid(parent_tbl),
            RelationGetRelid(parent_idx),
        );

        if OidIsValid(constraint_oid) {
            cld_constr_id =
                get_relation_idx_constraint_oid(RelationGetRelid(part_tbl), part_idx_id);
            if !OidIsValid(cld_constr_id) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot attach index \"{}\" as a partition of index \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                    )
                    /* errdetail("The index ... belongs to a constraint...") */
                );
            }
        }

        if (*(*parent_idx).rd_index).indisprimary {
            verifyPartitionIndexNotNull(child_info, part_tbl);
        }

        IndexSetParentIndex(part_idx, RelationGetRelid(parent_idx));
        if OidIsValid(constraint_oid) {
            ConstraintSetParentConstraint(
                cld_constr_id,
                constraint_oid,
                RelationGetRelid(part_tbl),
            );
        }

        free_attrmap(attmap);
        validatePartitionedIndex(parent_idx, parent_tbl);
    }

    relation_close(parent_tbl, AccessShareLock);
    relation_close(part_tbl, NoLock);
    relation_close(part_idx, NoLock);

    let _ = wqueue;
    address
}

// ---------------------------------------------------------------------------
// refuseDupeIndexAttach (static)
// ---------------------------------------------------------------------------

unsafe fn refuseDupeIndexAttach(
    parent_idx: Relation,
    part_idx: Relation,
    partition_tbl: Relation,
) {
    let existing_idx: Oid;

    existing_idx = index_get_partition(partition_tbl, RelationGetRelid(parent_idx));
    if OidIsValid(existing_idx) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot attach index \"{}\" as a partition of index \"{}\"",
                CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
               errdetail("Another index is already attached for partition ...") */
        );
    }
}

// ---------------------------------------------------------------------------
// validatePartitionedIndex (static)
// ---------------------------------------------------------------------------

unsafe fn validatePartitionedIndex(parted_idx: Relation, parted_tbl: Relation) {
    let inherits_rel: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut tuples: i32 = 0;
    let mut inh_tup: HeapTuple;
    let mut updated: bool = false;

    Assert!((*(*parted_idx).rd_rel).relkind as u8 == RELKIND_PARTITIONED_INDEX);

    inherits_rel = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parted_idx)),
    );
    scan = systable_beginscan(
        inherits_rel,
        InheritsParentIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    loop {
        inh_tup = systable_getnext(scan);
        if inh_tup.is_null() {
            break;
        }
        let inh_form: Form_pg_inherits = GETSTRUCT(inh_tup) as Form_pg_inherits;
        let ind_tup: HeapTuple;
        let index_form: Form_pg_index;

        ind_tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum((*inh_form).inhrelid));
        if !HeapTupleIsValid(ind_tup) {
            elog!(ERROR, "cache lookup failed for index {}", (*inh_form).inhrelid);
        }
        index_form = GETSTRUCT(ind_tup) as Form_pg_index;
        if (*index_form).indisvalid {
            tuples += 1;
        }
        ReleaseSysCache(ind_tup);
    }

    systable_endscan(scan);
    table_close(inherits_rel, AccessShareLock);

    if tuples == (*RelationGetPartitionDesc(parted_tbl, true)).nparts {
        let idx_rel: Relation;
        let ind_tup: HeapTuple;
        let index_form: Form_pg_index;

        idx_rel = table_open(IndexRelationId, RowExclusiveLock);
        ind_tup = SearchSysCacheCopy1(
            INDEXRELID,
            ObjectIdGetDatum(RelationGetRelid(parted_idx)),
        );
        if !HeapTupleIsValid(ind_tup) {
            elog!(
                ERROR,
                "cache lookup failed for index {}",
                RelationGetRelid(parted_idx)
            );
        }
        index_form = GETSTRUCT(ind_tup) as Form_pg_index;
        (*index_form).indisvalid = true;
        updated = true;
        CatalogTupleUpdate(idx_rel, &(*ind_tup).t_self, ind_tup);
        table_close(idx_rel, RowExclusiveLock);
        heap_freetuple(ind_tup);
    }

    if updated && (*(*parted_idx).rd_rel).relispartition {
        let parent_idx_id: Oid;
        let parent_tbl_id: Oid;
        let parent_idx: Relation;
        let parent_tbl: Relation;

        CommandCounterIncrement();

        parent_idx_id = get_partition_parent(RelationGetRelid(parted_idx), false);
        parent_tbl_id = get_partition_parent(RelationGetRelid(parted_tbl), false);
        parent_idx = relation_open(parent_idx_id, AccessExclusiveLock);
        parent_tbl = relation_open(parent_tbl_id, AccessExclusiveLock);
        Assert!(!(*(*parent_idx).rd_index).indisvalid);

        validatePartitionedIndex(parent_idx, parent_tbl);

        relation_close(parent_idx, AccessExclusiveLock);
        relation_close(parent_tbl, AccessExclusiveLock);
    }
}

// ---------------------------------------------------------------------------
// verifyPartitionIndexNotNull (static)
// ---------------------------------------------------------------------------

unsafe fn verifyPartitionIndexNotNull(iinfo: *mut IndexInfo, partition: Relation) {
    for i in 0..(*iinfo).ii_NumIndexKeyAttrs as usize {
        let att: Form_pg_attribute = TupleDescAttr(
            RelationGetDescr(partition),
            (*iinfo).ii_IndexAttrNumbers[i] as usize - 1,
        ) as Form_pg_attribute;

        if !(*att).attnotnull {
            ereport!(
                ERROR,
                errmsg!("invalid primary key definition")
                /* errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                   errdetail("Column ... of relation ... is not marked NOT NULL.") */
            );
        }
    }
}

// ---------------------------------------------------------------------------
// GetParentedForeignKeyRefs (static)
// ---------------------------------------------------------------------------

unsafe fn GetParentedForeignKeyRefs(partition: Relation) -> *mut List {
    let pg_constraint: Relation;
    let mut tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let mut constraints: *mut List = NIL;

    if RelationGetIndexList(partition) == NIL
        || bms_is_empty(RelationGetIndexAttrBitmap(partition, INDEX_ATTR_BITMAP_KEY))
    {
        return NIL;
    }

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_confrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(partition)),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_constraint_contype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(CONSTRAINT_FOREIGN as libc::c_char as Datum),
    );

    scan = systable_beginscan(
        pg_constraint,
        InvalidOid,
        true,
        core::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );
    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let constr_form: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

        if !OidIsValid((*constr_form).conparentid) {
            continue;
        }

        constraints = lappend_oid(constraints, (*constr_form).oid);
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    constraints
}

// ---------------------------------------------------------------------------
// ATDetachCheckNoForeignKeyRefs (static)
// ---------------------------------------------------------------------------

unsafe fn ATDetachCheckNoForeignKeyRefs(partition: Relation) {
    let constraints: *mut List;
    let mut cell: *mut ListCell;

    constraints = GetParentedForeignKeyRefs(partition);

    cell = list_head(constraints);
    while !cell.is_null() {
        let constr_oid: Oid = lfirst_oid(cell);
        cell = lnext(constraints, cell);
        let tuple: HeapTuple;
        let constr_form: Form_pg_constraint;
        let rel: Relation;
        let mut trig: Trigger = core::mem::zeroed();

        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constr_oid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", constr_oid);
        }
        constr_form = GETSTRUCT(tuple) as Form_pg_constraint;

        Assert!(OidIsValid((*constr_form).conparentid));
        Assert!((*constr_form).confrelid == RelationGetRelid(partition));

        rel = table_open((*constr_form).conrelid, ShareLock);

        trig.tgoid = InvalidOid;
        trig.tgname = NameStr!((*constr_form).conname) as *mut libc::c_char;
        trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
        trig.tgisinternal = true;
        trig.tgconstrrelid = RelationGetRelid(partition);
        trig.tgconstrindid = (*constr_form).conindid;
        trig.tgconstraint = (*constr_form).oid;
        trig.tgdeferrable = false;
        trig.tginitdeferred = false;

        RI_PartitionRemove_Check(&trig, rel, partition);

        ReleaseSysCache(tuple);
        table_close(rel, NoLock);
    }
}

// ---------------------------------------------------------------------------
// GetAttributeCompression
// ---------------------------------------------------------------------------

unsafe fn GetAttributeCompression(
    atttypid: Oid,
    compression: *const libc::c_char,
) -> libc::c_char {
    let cmethod: libc::c_char;

    if compression.is_null()
        || libc::strcmp(compression, c"default".as_ptr()) == 0
    {
        return InvalidCompressionMethod as libc::c_char;
    }

    if !TypeIsToastable(atttypid) {
        ereport!(
            ERROR,
            errmsg!(
                "column data type {} does not support compression",
                CStr::from_ptr(format_type_be(atttypid)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    cmethod = CompressionNameToMethod(compression);
    if !CompressionMethodIsValid(cmethod) {
        ereport!(
            ERROR,
            errmsg!(
                "invalid compression method \"{}\"",
                CStr::from_ptr(compression).to_string_lossy()
            ) /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    cmethod
}

// ---------------------------------------------------------------------------
// GetAttributeStorage
// ---------------------------------------------------------------------------

unsafe fn GetAttributeStorage(
    atttypid: Oid,
    storagemode: *const libc::c_char,
) -> libc::c_char {
    let cstorage: u8;

    if pg_strcasecmp(storagemode, c"plain".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_PLAIN;
    } else if pg_strcasecmp(storagemode, c"external".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_EXTERNAL;
    } else if pg_strcasecmp(storagemode, c"extended".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_EXTENDED;
    } else if pg_strcasecmp(storagemode, c"main".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_MAIN;
    } else if pg_strcasecmp(storagemode, c"default".as_ptr()) == 0 {
        cstorage = get_typstorage(atttypid);
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "invalid storage type \"{}\"",
                CStr::from_ptr(storagemode).to_string_lossy()
            ) /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
        unreachable!();
    }

    if !(cstorage == TYPSTORAGE_PLAIN || TypeIsToastable(atttypid)) {
        ereport!(
            ERROR,
            errmsg!(
                "column data type {} can only have storage PLAIN",
                CStr::from_ptr(format_type_be(atttypid)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    cstorage as libc::c_char
}

// ---------------------------------------------------------------------------
// TODO(pg-port) stubs for new dependencies not in head/earlier sections
// ---------------------------------------------------------------------------

// TODO(pg-port): StoreCatalogInheritance1 -- adds a single pg_inherits row
// TODO(pg-port): child_dependency_type -- returns DEPENDENCY_AUTO for partitioned, else DEPENDENCY_NORMAL
// TODO(pg-port): DeleteInheritsTuple -- removes pg_inherits row for (child, parent)
// TODO(pg-port): extractNotNullColumn -- returns attnum from a not-null constraint tuple
// TODO(pg-port): findNotNullConstraintAttnum -- finds not-null constraint tuple by attnum
// TODO(pg-port): build_attrmap_by_name -- builds AttrMap mapping attrs by name
// TODO(pg-port): pg_add_s16_overflow -- wrapping addition with overflow check
// TODO(pg-port): check_new_partition_bound -- validates new partition bound
// TODO(pg-port): StorePartitionBound -- stores partition bound in pg_class
// TODO(pg-port): get_qual_from_partbound -- generates partition constraint from bound
// TODO(pg-port): RelationGetPartitionQual -- gets partition constraint qual list
// TODO(pg-port): map_partition_varattnos -- remaps varattnos for partition
// TODO(pg-port): get_proposed_default_constraint -- gets default partition constraint
// TODO(pg-port): list_concat_copy -- concatenates two lists (copy)
// TODO(pg-port): make_ands_explicit -- converts implicit-AND list to explicit AND node
// TODO(pg-port): make_ands_implicit -- converts AND expr to implicit-AND list
// TODO(pg-port): eval_const_expressions -- simplifies constant expressions
// TODO(pg-port): canonicalize_qual -- canonicalizes qual expression
// TODO(pg-port): predicate_implied_by -- tests if constraints imply a predicate
// TODO(pg-port): stringToNode -- deserializes a node from its string representation
// TODO(pg-port): nodeToString -- serializes a node to string representation
// TODO(pg-port): list_make1 -- creates a one-element list
// TODO(pg-port): list_copy -- shallow-copies a list
// TODO(pg-port): WaitForOlderSnapshots -- waits for snapshots older than given xmin
// TODO(pg-port): WaitForLockersMultiple -- waits for all lockers of given lock tags
// TODO(pg-port): SET_LOCKTAG_RELATION macro -- initializes a LOCKTAG for a relation
// TODO(pg-port): StartTransactionCommand / CommitTransactionCommand -- xact boundaries
// TODO(pg-port): PushActiveSnapshot / PopActiveSnapshot -- snapshot stack
// TODO(pg-port): GetTransactionSnapshot -- returns current transaction snapshot
// TODO(pg-port): GetCurrentSubTransactionId -- returns current subtransaction ID
// TODO(pg-port): InvalidSubTransactionId -- sentinel for no subtransaction
// TODO(pg-port): MyXactFlags / XACT_FLAGS_ACCESSEDTEMPNAMESPACE -- xact flags
// TODO(pg-port): PortalContext -- memory context for portal
// TODO(pg-port): CacheMemoryContext -- memory context for caches
// TODO(pg-port): lcons -- prepend element to list
// TODO(pg-port): heap_truncate -- truncates given relations
// TODO(pg-port): performMultipleDeletions -- performs cascaded object deletions
// TODO(pg-port): PERFORM_DELETION_INTERNAL / PERFORM_DELETION_QUIETLY -- flags
// TODO(pg-port): new_object_addresses / free_object_addresses / add_exact_object_address -- object-address sets
// TODO(pg-port): object_address_present -- checks if address is in set
// TODO(pg-port): CloneForeignKeyConstraints -- clones FK constraints to partition
// TODO(pg-port): addFkRecurseReferenced -- adds FK referenced-side triggers
// TODO(pg-port): DeconstructFkConstraintRow -- deconstructs FK constraint row
// TODO(pg-port): GetForeignKeyCheckTriggers -- finds FK check triggers by constraint
// TODO(pg-port): TriggerSetParentTrigger -- sets parent trigger on a trigger
// TODO(pg-port): ConstraintSetParentConstraint -- sets parent constraint
// TODO(pg-port): IndexSetParentIndex -- sets parent index on partition index
// TODO(pg-port): CompareIndexInfo -- compares two IndexInfo structures
// TODO(pg-port): BuildIndexInfo -- builds IndexInfo for an index relation
// TODO(pg-port): generateClonedIndexStmt -- generates IndexStmt clone for partition
// TODO(pg-port): DefineIndex -- creates an index
// TODO(pg-port): index_get_partition -- finds partition index for a given parent index
// TODO(pg-port): get_partition_parent -- gets parent of a partition
// TODO(pg-port): has_superclass -- checks if relation has a superclass
// TODO(pg-port): IndexGetRelation -- gets relation OID for an index
// TODO(pg-port): ATAddCheckNNConstraint -- adds CHECK/NOT NULL constraint
// TODO(pg-port): RI_PartitionRemove_Check -- validates RI when removing partition
// TODO(pg-port): deleteDependencyRecordsForClass -- deletes dependency records
// TODO(pg-port): changeDependencyFor -- changes a dependency entry
// TODO(pg-port): update_default_partition_oid -- updates default partition in pg_partitioned_table
// TODO(pg-port): AlterTypeNamespaceInternal -- moves type to new namespace
// TODO(pg-port): AlterConstraintNamespaces -- moves constraints to new namespace
// TODO(pg-port): CheckSetNamespace -- validates namespace change
// TODO(pg-port): RangeVarGetAndCheckCreationNamespace -- gets/validates namespace OID
// TODO(pg-port): sequenceIsOwned -- checks if sequence is owned by a column
// TODO(pg-port): GetRelationPublications -- gets publications for a relation
// TODO(pg-port): RelationIsPermanent -- checks if relation is permanent
// TODO(pg-port): typenameType -- looks up type by TypeName
// TODO(pg-port): check_of_type -- validates type for OF TABLE
// TODO(pg-port): lookup_rowtype_tupdesc -- gets TupleDesc for a row type
// TODO(pg-port): recordDependencyOn -- records a dependency
// TODO(pg-port): GetForeignServer / GetForeignDataWrapper -- FDW metadata access
// TODO(pg-port): transformGenericOptions -- transforms generic FDW options
// TODO(pg-port): CompressionNameToMethod / CompressionMethodIsValid -- compression utilities
// TODO(pg-port): InvalidCompressionMethod -- sentinel compression method value
// TODO(pg-port): TypeIsToastable -- checks if type can be toasted
// TODO(pg-port): get_typstorage -- gets default storage for a type
// TODO(pg-port): pg_strcasecmp -- case-insensitive strcmp
// TODO(pg-port): SetIndexStorageProperties -- applies storage properties to index columns
// TODO(pg-port): FindTriggerIncompatibleWithInheritance -- finds incompatible triggers
// TODO(pg-port): CreateTriggerFiringOn -- creates trigger with given firing conditions
// TODO(pg-port): GetActiveSnapshot -- returns active snapshot
// TODO(pg-port): on_commits static -- list of OnCommitItem entries
// TODO(pg-port): ATGetQueueEntry -- gets/creates AlteredTableInfo entry in work queue
// TODO(pg-port): RelationGetFKeyList -- returns foreign key list for relation
// TODO(pg-port): list_free_deep -- frees list and all elements
// TODO(pg-port): IndexRelationGetNumberOfKeyAttributes -- returns number of key attrs
// TODO(pg-port): RelationGetIndexExpressions / RelationGetIndexPredicate -- index metadata
// TODO(pg-port): CacheInvalidateRelcache / CacheInvalidateRelcacheByRelid -- cache invalidation
