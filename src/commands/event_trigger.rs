/*-------------------------------------------------------------------------
 *
 * event_trigger.rs
 *   PostgreSQL EVENT TRIGGER support code.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/backend/commands/event_trigger.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::c_int;

use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::htup_details::HeapTupleData;
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::relscan::SysScanDescData;
use crate::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::access::table::table::{table_open, table_close};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::indexing::{CatalogTupleInsert, CatalogTupleUpdate};
use crate::lib::ilist::{slist_head, slist_node, slist_iter, slist_init, slist_is_empty, slist_push_head};
use crate::nodes::nodes::Node;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::pg_list::{List, NIL, lappend_oid, list_free};
use crate::nodes::parsenodes::ObjectType;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::{AccessShareLock, RowExclusiveLock, AccessExclusiveLock, ExclusiveLock};
use crate::tcop::deparse_utility::{
    CollectedCommand, CollectedATSubcmd, CollectedCommandType,
    CollectedCommand_d, CollectedCommand_simple, CollectedCommand_alterTable,
    CollectedCommand_grant, CollectedCommand_opfam, CollectedCommand_createopc,
    CollectedCommand_atscfg, CollectedCommand_defprivs,
    SCT_Simple, SCT_AlterTable, SCT_Grant, SCT_AlterOpFamily,
    SCT_AlterDefaultPrivileges, SCT_CreateOpClass, SCT_AlterTSConfig,
};
use crate::tcop::cmdtag::CommandTag;
use crate::utils::aclchk_internal::InternalGrant;
use crate::utils::rel::Relation;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};

// HeapTuple = *mut HeapTupleData (project convention)
type HeapTuple = *mut HeapTupleData;

// SysScanDesc
type SysScanDesc = *mut SysScanDescData;

/* ----------------------------------------------------------------
 * Local stub types for unported dependencies
 * ---------------------------------------------------------------- */

// Form_pg_event_trigger  TODO(pg-port)
#[repr(C)]
pub struct FormData_pg_event_trigger {
    pub oid: Oid,
    pub evtname: NameData,
    pub evtevent: NameData,
    pub evtowner: Oid,
    pub evtfoid: Oid,
    pub evtenabled: c_char,
    /* evttags: nullable text[] -- not a fixed-size field */
}
type Form_pg_event_trigger = *mut FormData_pg_event_trigger;

// Form_pg_database  TODO(pg-port)
#[repr(C)]
pub struct FormData_pg_database {
    _opaque: [u8; 0],
    pub dathasloginevt: bool,
}
type Form_pg_database = *mut FormData_pg_database;

// Form_pg_trigger  TODO(pg-port)
#[repr(C)]
pub struct FormData_pg_trigger {
    pub tgrelid: Oid,
    _opaque: [u8; 0],
}
type Form_pg_trigger = *mut FormData_pg_trigger;

// Form_pg_policy  TODO(pg-port)
#[repr(C)]
pub struct FormData_pg_policy {
    pub polrelid: Oid,
    _opaque: [u8; 0],
}
type Form_pg_policy = *mut FormData_pg_policy;

// NameData  TODO(pg-port): from postgres.h / c.h
#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}

// EventTriggerEvent enum  TODO(pg-port): from commands/event_trigger.h
pub type EventTriggerEvent = c_int;
pub const EVT_DDLCommandStart: EventTriggerEvent = 0;
pub const EVT_DDLCommandEnd: EventTriggerEvent   = 1;
pub const EVT_SQLDrop: EventTriggerEvent         = 2;
pub const EVT_TableRewrite: EventTriggerEvent    = 3;
pub const EVT_Login: EventTriggerEvent           = 4;

// EventTriggerData  TODO(pg-port): from commands/event_trigger.h
#[repr(C)]
pub struct EventTriggerData {
    pub type_: crate::nodes::nodes::NodeTag,
    pub event: *const c_char,
    pub parsetree: *mut Node,
    pub tag: CommandTag,
}

// EventTriggerCacheItem  TODO(pg-port): from utils/evtcache.h
#[repr(C)]
pub struct EventTriggerCacheItem {
    pub fnoid: Oid,
    pub enabled: c_char,
    pub tagset: *mut Bitmapset,
}

// ReturnSetInfo / Tuplestorestate  TODO(pg-port)
use crate::nodes::execnodes::{ReturnSetInfo, Tuplestorestate};

// PgStat_FunctionCallUsage  TODO(pg-port)
#[repr(C)]
pub struct PgStat_FunctionCallUsage {
    _opaque: [u8; 0],
}

// ArrayType  TODO(pg-port)
#[repr(C)]
pub struct ArrayType {
    _opaque: [u8; 0],
}

/* ----------------------------------------------------------------
 * Stub functions for unported dependencies
 * ---------------------------------------------------------------- */

// access/heapam.h
unsafe fn heap_form_tuple(
    _tupdesc: *mut crate::access::common::tupdesc::TupleDescData,
    _values: *mut Datum,
    _nulls: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/heap/heapam.c
}
unsafe fn heap_freetuple(_htup: HeapTuple) {
    unimplemented!() // TODO(pg-port): access/heap/heapam.c
}
unsafe fn heap_getattr(
    _tuple: HeapTuple,
    _attnum: AttrNumber,
    _tupdesc: *mut crate::access::common::tupdesc::TupleDescData,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}

// catalog/catalog.h
unsafe fn GetNewOidWithIndex(
    _relation: Relation,
    _indexId: Oid,
    _oidcolumn: AttrNumber,
) -> Oid {
    unimplemented!() // TODO(pg-port): catalog/catalog.c
}

// catalog/dependency.h
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_int,
) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
pub const DEPENDENCY_NORMAL: c_int = 0;

unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _ownerId: Oid) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn changeDependencyOnOwner(_classId: Oid, _objectId: Oid, _newOwnerId: Oid) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _replace: bool) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}

// catalog/objectaccess.h
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    unimplemented!() // TODO(pg-port): catalog/objectaccess.c
}
unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    unimplemented!() // TODO(pg-port): catalog/objectaccess.c
}

// catalog/pg_event_trigger.h catalog constants  TODO(pg-port)
pub const EventTriggerRelationId: Oid = 3466;
pub const EventTriggerOidIndexId: Oid = 3467;
pub const Anum_pg_event_trigger_oid: AttrNumber = 1;
pub const Anum_pg_event_trigger_evtname: AttrNumber = 2;
pub const Anum_pg_event_trigger_evtevent: AttrNumber = 3;
pub const Anum_pg_event_trigger_evtowner: AttrNumber = 4;
pub const Anum_pg_event_trigger_evtfoid: AttrNumber = 5;
pub const Anum_pg_event_trigger_evtenabled: AttrNumber = 6;
pub const Anum_pg_event_trigger_evttags: AttrNumber = 7;
pub const Natts_pg_event_trigger: usize = 7;

// catalog/pg_database.h  TODO(pg-port)
pub const DatabaseRelationId: Oid = 1262;
pub const DatabaseOidIndexId: Oid = 2672;
pub const Anum_pg_database_oid: AttrNumber = 1;

// catalog/pg_namespace.h  TODO(pg-port)
pub const NamespaceRelationId: Oid = 2615;

// catalog/pg_authid.h  TODO(pg-port)
pub const AuthIdRelationId: Oid = 1260;

// catalog/pg_auth_members.h  TODO(pg-port)
pub const AuthMemRelationId: Oid = 1261;

// catalog/pg_parameter_acl.h  TODO(pg-port)
pub const ParameterAclRelationId: Oid = 6243;

// catalog/pg_attrdef.h  TODO(pg-port)
pub const AttrDefaultRelationId: Oid = 2604;

// catalog/pg_trigger.h  TODO(pg-port)
pub const TriggerRelationId: Oid = 2620;
pub const TriggerOidIndexId: Oid = 2696;
pub const Anum_pg_trigger_oid: AttrNumber = 1;

// catalog/pg_policy.h  TODO(pg-port)
pub const PolicyRelationId: Oid = 3256;
pub const PolicyOidIndexId: Oid = 3257;
pub const Anum_pg_policy_oid: AttrNumber = 1;

// catalog/pg_proc.h  TODO(pg-port)
pub const ProcedureRelationId: Oid = 1255;

// catalog/pg_tablespace.h  TODO(pg-port)
pub const TableSpaceRelationId: Oid = 1213;

// catalog/pg_opclass.h  TODO(pg-port)
pub const OperatorClassRelationId: Oid = 2616;

// catalog/pg_opfamily.h  TODO(pg-port)
pub const OperatorFamilyRelationId: Oid = 2753;

// catalog/pg_ts_config.h  TODO(pg-port)
pub const TSConfigRelationId: Oid = 3602;

// catalog/pg_type.h  TODO(pg-port)
pub const RelationRelationId: Oid = 1259;
pub const EVENT_TRIGGEROID: Oid = 3838;
pub const TEXTOID: Oid = 25;

// sys cache IDs  TODO(pg-port)
pub const EVENTTRIGGERNAME: c_int = 38;
pub const EVENTTRIGGEROID: c_int  = 39;
pub const DATABASEOID: c_int      = 18;

// BTEqualStrategyNumber  TODO(pg-port)
pub const BTEqualStrategyNumber: crate::access::stratnum::StrategyNumber = 3;
pub const F_OIDEQ: crate::postgres_ext::RegProcedure = 184;

// utils/syscache.h  TODO(pg-port)
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn SearchSysCacheLockedCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn GetSysCacheOid1(_cacheId: c_int, _anum: AttrNumber, _key1: Datum) -> Oid {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn HeapTupleIsValid(_tuple: HeapTuple) -> bool {
    unimplemented!() // TODO(pg-port): access/htup.h macro
}
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/htup.h macro
}
unsafe fn RelationGetDescr(
    _rel: Relation,
) -> *mut crate::access::common::tupdesc::TupleDescData {
    unimplemented!() // TODO(pg-port): utils/rel.h macro
}

// utils/acl.h  TODO(pg-port)
unsafe fn GetUserId() -> Oid {
    unimplemented!() // TODO(pg-port): utils/adt/acl.c
}
unsafe fn superuser() -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/acl.c
}
unsafe fn superuser_arg(_userId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/acl.c
}
unsafe fn object_ownercheck(_classId: Oid, _objectId: Oid, _userId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/acl.c
}

pub type AclResult = c_int;
pub const ACLCHECK_NOT_OWNER: AclResult = 2;

unsafe fn aclcheck_error(
    _aclerr: AclResult,
    _objtype: ObjectType,
    _objname: *const c_char,
) {
    unimplemented!() // TODO(pg-port): utils/adt/acl.c
}

// miscadmin.h  TODO(pg-port)
unsafe fn check_stack_depth() {
    unimplemented!() // TODO(pg-port): tcop/postgres.c
}
pub static mut IsUnderPostmaster: bool = false;
pub static mut MyDatabaseId: Oid = 0;
pub static mut MyDatabaseHasLoginEventTriggers: bool = false;
pub const OBJECT_EVENT_TRIGGER: ObjectType = ObjectType::OBJECT_EVENT_TRIGGER;

// utils/evtcache.h  TODO(pg-port)
unsafe fn EventCacheLookup(_event: EventTriggerEvent) -> *mut List {
    unimplemented!() // TODO(pg-port): utils/cache/evtcache.c
}

// access/xact.h  TODO(pg-port)
unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO(pg-port): access/transam/xact.c
}
unsafe fn StartTransactionCommand() {
    unimplemented!() // TODO(pg-port): access/transam/xact.c
}
unsafe fn CommitTransactionCommand() {
    unimplemented!() // TODO(pg-port): access/transam/xact.c
}

// utils/snapmgr.h  TODO(pg-port)
unsafe fn GetTransactionSnapshot() -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/time/snapmgr.c
}
unsafe fn PushActiveSnapshot(_snap: *mut c_void) {
    unimplemented!() // TODO(pg-port): utils/time/snapmgr.c
}
unsafe fn PopActiveSnapshot() {
    unimplemented!() // TODO(pg-port): utils/time/snapmgr.c
}

// storage/lmgr.h  TODO(pg-port)
unsafe fn LockSharedObject(_classId: Oid, _objectId: Oid, _objsubid: u16, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c
}
unsafe fn ConditionalLockSharedObject(
    _classId: Oid,
    _objectId: Oid,
    _objsubid: u16,
    _lockmode: c_int,
) -> bool {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c
}
unsafe fn UnlockTuple(_rel: Relation, _tid: *const crate::storage::itemptr::ItemPointerData, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c
}
pub const InplaceUpdateTupleLock: c_int = ExclusiveLock;

// storage/itemptr.h  TODO(pg-port)
use crate::storage::itemptr::ItemPointerData;

// utils/fmgr.h  TODO(pg-port)
unsafe fn fmgr_info(_fnOid: Oid, _finfo: *mut FmgrInfo) {
    unimplemented!() // TODO(pg-port): utils/fmgr/fmgr.c
}

// funcapi.h  TODO(pg-port)
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: *mut crate::access::common::tupdesc::TupleDescData,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port): utils/sort/tuplestore.c
}

// pgstat.h  TODO(pg-port)
unsafe fn pgstat_init_function_usage(
    _fcinfo: FunctionCallInfo,
    _fcu: *mut PgStat_FunctionCallUsage,
) {
    unimplemented!() // TODO(pg-port): pgstat.c
}
unsafe fn pgstat_end_function_usage(_fcu: *mut PgStat_FunctionCallUsage, _finalize: bool) {
    unimplemented!() // TODO(pg-port): pgstat.c
}

// FunctionCallInvoke  TODO(pg-port)
unsafe fn FunctionCallInvoke(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr.h macro
}

// InitFunctionCallInfoData  TODO(pg-port)
unsafe fn InitFunctionCallInfoData(
    _fcinfo: FunctionCallInfo,
    _flinfo: *mut FmgrInfo,
    _nargs: c_int,
    _collation: Oid,
    _context: *mut Node,
    _resultinfo: *mut c_void,
) {
    unimplemented!() // TODO(pg-port): utils/fmgr.h macro
}

// LOCAL_FCINFO placeholder  TODO(pg-port)
// Used inline as a raw pointer in EventTriggerInvoke.

// tcop/cmdtag.h  TODO(pg-port)
unsafe fn GetCommandTagEnum(_commandName: *const c_char) -> CommandTag {
    unimplemented!() // TODO(pg-port): tcop/cmdtag.c
}
unsafe fn GetCommandTagName(_tag: CommandTag) -> *const c_char {
    unimplemented!() // TODO(pg-port): tcop/cmdtag.c
}
unsafe fn command_tag_event_trigger_ok(_tag: CommandTag) -> bool {
    unimplemented!() // TODO(pg-port): tcop/cmdtag.c
}
unsafe fn command_tag_table_rewrite_ok(_tag: CommandTag) -> bool {
    unimplemented!() // TODO(pg-port): tcop/cmdtag.c
}
unsafe fn CreateCommandTag(_parsetree: *mut Node) -> CommandTag {
    unimplemented!() // TODO(pg-port): tcop/utility.c
}
unsafe fn CreateCommandName(_parsetree: *mut Node) -> *const c_char {
    unimplemented!() // TODO(pg-port): tcop/utility.c
}

// CMDTAG_LOGIN  TODO(pg-port)
use crate::tcop::cmdtag::CommandTag::CMDTAG_LOGIN;

// parser/parse_func.h  TODO(pg-port)
unsafe fn LookupFuncName(
    _funcname: *mut List,
    _nargs: c_int,
    _argtypes: *const Oid,
    _noError: bool,
) -> Oid {
    unimplemented!() // TODO(pg-port): parser/parse_func.c
}

// utils/lsyscache.h  TODO(pg-port)
unsafe fn get_func_rettype(_funcid: Oid) -> Oid {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn get_namespace_name(_nsoid: Oid) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn get_namespace_name_or_temp(_nsoid: Oid) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}

// utils/builtins.h  TODO(pg-port)
unsafe fn NameListToString(_names: *mut List) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c
}
unsafe fn NameStr(_name: *const NameData) -> *const c_char {
    unimplemented!() // TODO(pg-port): c.h macro
}
unsafe fn namestrcpy(_name: *mut NameData, _str_: *const c_char) {
    unimplemented!() // TODO(pg-port): utils/adt/name.c
}
unsafe fn namestrcmp(_name: *const NameData, _str_: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): utils/adt/name.c
}
unsafe fn pg_ascii_toupper(_c: u8) -> u8 {
    unimplemented!() // TODO(pg-port): port/pg_ascii_toupper
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c
}
unsafe fn construct_array_builtin(
    _elems: *mut Datum,
    _nelems: c_int,
    _elmtype: Oid,
) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): utils/adt/array_utils.c
}
unsafe fn construct_empty_array(_elmtype: Oid) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): utils/adt/arrayfuncs.c
}
unsafe fn strlist_to_textarray(_list: *mut List) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): utils/builtins.h macro
}

// catalog/objectaddress.h  TODO(pg-port)
unsafe fn ObjectAddressSet(_addr: *mut ObjectAddress, _classId: Oid, _objectId: Oid) {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c macro
}
unsafe fn getObjectIdentityParts(
    _object: *const ObjectAddress,
    _objname: *mut *mut List,
    _objargs: *mut *mut List,
    _missing_ok: bool,
) -> *const c_char {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn getObjectTypeDescription(
    _object: *const ObjectAddress,
    _missing_ok: bool,
) -> *const c_char {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn getObjectIdentity(
    _object: *const ObjectAddress,
    _missing_ok: bool,
) -> *mut c_char {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn is_objectclass_supported(_classId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn get_object_attnum_oid(_classId: Oid) -> AttrNumber {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn get_object_attnum_namespace(_classId: Oid) -> AttrNumber {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn get_object_attnum_name(_classId: Oid) -> AttrNumber {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn get_object_namensp_unique(_classId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn get_catalog_object_by_oid(
    _catalog: Relation,
    _oidattnum: AttrNumber,
    _objectId: Oid,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn GetAttrDefaultColumnAddress(_attrdefoid: Oid) -> ObjectAddress {
    unimplemented!() // TODO(pg-port): catalog/objectaddress.c
}
unsafe fn isTempNamespace(_namespaceId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): catalog/namespace.c
}
unsafe fn isAnyTempNamespace(_namespaceId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): catalog/namespace.c
}

// commands/extension.h  TODO(pg-port)
pub static mut creating_extension: bool = false;

// access/heapam_internal.h  TODO(pg-port)
unsafe fn systable_inplace_update_begin(
    _rel: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snap: *mut c_void,
    _nkeys: c_int,
    _key: *const ScanKeyData,
    _tup: *mut HeapTuple,
    _state: *mut *mut c_void,
) {
    unimplemented!() // TODO(pg-port): access/heapam.c
}
unsafe fn systable_inplace_update_finish(_state: *mut c_void, _tup: HeapTuple) {
    unimplemented!() // TODO(pg-port): access/heapam.c
}
unsafe fn systable_inplace_update_cancel(_state: *mut c_void) {
    unimplemented!() // TODO(pg-port): access/heapam.c
}

// utils/bitmapset.h  TODO(pg-port)
unsafe fn bms_is_empty(_a: *const Bitmapset) -> bool {
    unimplemented!() // TODO(pg-port): nodes/bitmapset.c
}
unsafe fn bms_is_member(_x: CommandTag, _a: *const Bitmapset) -> bool {
    unimplemented!() // TODO(pg-port): nodes/bitmapset.c
}

// utils/builtins.h
unsafe fn copyObject(_from: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO(pg-port): nodes/copyfuncs.c
}

// OidIsValid macro  TODO(pg-port)
unsafe fn OidIsValid(_oid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): c.h macro
}

// DatumGetObjectId / DatumGetName  TODO(pg-port)
unsafe fn DatumGetObjectId(_datum: Datum) -> Oid {
    unimplemented!() // TODO(pg-port): postgres.h macro
}
unsafe fn DatumGetName(_datum: Datum) -> *mut NameData {
    unimplemented!() // TODO(pg-port): postgres.h macro
}

// session replication role  TODO(pg-port)
pub static mut SessionReplicationRole: c_int = 0;
pub const SESSION_REPLICATION_ROLE_REPLICA: c_int = 1;

// trigger firing modes  TODO(pg-port)
pub const TRIGGER_FIRES_ON_ORIGIN:  c_char = b'O' as c_char;
pub const TRIGGER_FIRES_ON_REPLICA: c_char = b'R' as c_char;
pub const TRIGGER_DISABLED:         c_char = b'D' as c_char;

// InvalidOid convenience  TODO(pg-port)
pub const InvalidOid: Oid = 0;

// PG_TRY / PG_FINALLY / PG_END_TRY are expressed as Rust closures below.

// strVal helper  TODO(pg-port): parsenodes.h / value.h macro
unsafe fn strVal(_v: *mut c_void) -> *const c_char {
    unimplemented!() // TODO(pg-port): nodes/value.h macro
}

// list_length  TODO(pg-port)
unsafe fn list_length(_list: *const List) -> c_int {
    unimplemented!() // TODO(pg-port): nodes/pg_list.h macro
}

// lfirst  TODO(pg-port)
unsafe fn lfirst(_cell: *mut crate::nodes::pg_list::ListCell) -> *mut c_void {
    unimplemented!() // TODO(pg-port): nodes/pg_list.h macro
}

// lfirst_oid  TODO(pg-port)
unsafe fn lfirst_oid(_cell: *mut crate::nodes::pg_list::ListCell) -> Oid {
    unimplemented!() // TODO(pg-port): nodes/pg_list.h macro
}

// lappend  TODO(pg-port)
unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List {
    unimplemented!() // TODO(pg-port): nodes/list.c
}

// list_copy  TODO(pg-port)
unsafe fn list_copy(_list: *mut List) -> *mut List {
    unimplemented!() // TODO(pg-port): nodes/list.c
}

// palloc_array  TODO(pg-port)
unsafe fn palloc_array(_size: usize, _nelems: c_int) -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/palloc.h macro
}

// trackDroppedObjectsNeeded forward declared; implemented below

/*
 * Module-level state
 */
#[repr(C)]
struct EventTriggerQueryState {
    /* memory context for this state's objects */
    cxt: MemoryContext,

    /* sql_drop */
    SQLDropList: slist_head,
    in_sql_drop: bool,

    /* table_rewrite */
    table_rewrite_oid: Oid,         /* InvalidOid, or set for table_rewrite event */
    table_rewrite_reason: c_int,    /* AT_REWRITE reason */

    /* Support for command collection */
    commandCollectionInhibited: bool,
    currentCommand: *mut CollectedCommand,
    commandList: *mut List,         /* list of CollectedCommand; see deparse_utility.h */
    previous: *mut EventTriggerQueryState,
}

static mut currentEventTriggerState: *mut EventTriggerQueryState = core::ptr::null_mut();

/* GUC parameter */
pub static mut event_triggers: bool = true;

/* Support for dropped objects */
#[repr(C)]
struct SQLDropObject {
    address: ObjectAddress,
    schemaname: *const c_char,
    objname: *const c_char,
    objidentity: *const c_char,
    objecttype: *const c_char,
    addrnames: *mut List,
    addrargs: *mut List,
    original: bool,
    normal: bool,
    istemp: bool,
    next: slist_node,
}

/*
 * Create an event trigger.
 */
pub unsafe fn CreateEventTrigger(stmt: *mut crate::nodes::parsenodes::CreateEventTrigStmt) -> Oid {
    let mut tuple: HeapTuple;
    let funcoid: Oid;
    let funcrettype: Oid;
    let evtowner: Oid = GetUserId();
    let mut lc: *mut crate::nodes::pg_list::ListCell;
    let mut tags: *mut List = NIL;

    /*
     * It would be nice to allow database owners or even regular users to do
     * this, but there are obvious privilege escalation risks which would have
     * to somehow be plugged first.
     */
    if !superuser() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
            errmsg!(
                "permission denied to create event trigger \"{}\"",
                core::ffi::CStr::from_ptr((*stmt).trigname).to_string_lossy()
            )
            /* C also: errhint("Must be superuser to create an event trigger.") */
        );
    }

    /* Validate event name. */
    if libc_strcmp((*stmt).eventname, c"ddl_command_start".as_ptr()) != 0
        && libc_strcmp((*stmt).eventname, c"ddl_command_end".as_ptr()) != 0
        && libc_strcmp((*stmt).eventname, c"sql_drop".as_ptr()) != 0
        && libc_strcmp((*stmt).eventname, c"login".as_ptr()) != 0
        && libc_strcmp((*stmt).eventname, c"table_rewrite".as_ptr()) != 0
    {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            errmsg!(
                "unrecognized event name \"{}\"",
                core::ffi::CStr::from_ptr((*stmt).eventname).to_string_lossy()
            )
        );
    }

    /* Validate filter conditions. */
    lc = list_head((*stmt).whenclause);
    while !lc.is_null() {
        let def = lfirst(lc) as *mut crate::nodes::parsenodes::DefElem;

        if libc_strcmp((*def).defname, c"tag".as_ptr()) == 0 {
            if !tags.is_null() {
                error_duplicate_filter_variable((*def).defname);
            }
            tags = (*def).arg as *mut List;
        } else {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                errmsg!(
                    "unrecognized filter variable \"{}\"",
                    core::ffi::CStr::from_ptr((*def).defname).to_string_lossy()
                )
            );
        }
        lc = lnext((*stmt).whenclause, lc);
    }

    /* Validate tag list, if any. */
    if (libc_strcmp((*stmt).eventname, c"ddl_command_start".as_ptr()) == 0
        || libc_strcmp((*stmt).eventname, c"ddl_command_end".as_ptr()) == 0
        || libc_strcmp((*stmt).eventname, c"sql_drop".as_ptr()) == 0)
        && !tags.is_null()
    {
        validate_ddl_tags(c"tag".as_ptr(), tags);
    } else if libc_strcmp((*stmt).eventname, c"table_rewrite".as_ptr()) == 0
        && !tags.is_null()
    {
        validate_table_rewrite_tags(c"tag".as_ptr(), tags);
    } else if libc_strcmp((*stmt).eventname, c"login".as_ptr()) == 0 && !tags.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            errmsg!("tag filtering is not supported for login event triggers")
        );
    }

    /*
     * Give user a nice error message if an event trigger of the same name
     * already exists.
     */
    tuple = SearchSysCache1(EVENTTRIGGERNAME, CStringGetDatum((*stmt).trigname));
    if HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            errmsg!(
                "event trigger \"{}\" already exists",
                core::ffi::CStr::from_ptr((*stmt).trigname).to_string_lossy()
            )
        );
    }

    /* Find and validate the trigger function. */
    funcoid = LookupFuncName((*stmt).funcname, 0, core::ptr::null(), false);
    funcrettype = get_func_rettype(funcoid);
    if funcrettype != EVENT_TRIGGEROID {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            errmsg!(
                "function {} must return type {}",
                core::ffi::CStr::from_ptr(NameListToString((*stmt).funcname)).to_string_lossy(),
                "event_trigger"
            )
        );
    }

    /* Insert catalog entries. */
    insert_event_trigger_tuple((*stmt).trigname, (*stmt).eventname, evtowner, funcoid, tags)
}

// helper: libc strcmp wrapper
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" { fn strcmp(a: *const c_char, b: *const c_char) -> c_int; }
    strcmp(a, b)
}

// CStringGetDatum  TODO(pg-port)
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): postgres.h macro
}

// list helpers needed above
unsafe fn list_head(_list: *const List) -> *mut crate::nodes::pg_list::ListCell {
    unimplemented!() // TODO(pg-port): nodes/pg_list.h
}
unsafe fn lnext(_list: *const List, _lc: *mut crate::nodes::pg_list::ListCell)
    -> *mut crate::nodes::pg_list::ListCell
{
    unimplemented!() // TODO(pg-port): nodes/pg_list.h
}

/*
 * Validate DDL command tags.
 */
unsafe fn validate_ddl_tags(filtervar: *const c_char, taglist: *mut List) {
    let mut lc: *mut crate::nodes::pg_list::ListCell = list_head(taglist);
    while !lc.is_null() {
        let tagstr: *const c_char = strVal(lfirst(lc) as *mut c_void);
        let commandTag: CommandTag = GetCommandTagEnum(tagstr);

        if commandTag == crate::tcop::cmdtag::CommandTag::CMDTAG_UNKNOWN {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                errmsg!(
                    "filter value \"{}\" not recognized for filter variable \"{}\"",
                    core::ffi::CStr::from_ptr(tagstr).to_string_lossy(),
                    core::ffi::CStr::from_ptr(filtervar).to_string_lossy()
                )
            );
        }
        if !command_tag_event_trigger_ok(commandTag) {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                /* translator: %s represents an SQL statement name */
                errmsg!(
                    "event triggers are not supported for {}",
                    core::ffi::CStr::from_ptr(tagstr).to_string_lossy()
                )
            );
        }
        lc = lnext(taglist, lc);
    }
}

/*
 * Validate DDL command tags for event table_rewrite.
 */
unsafe fn validate_table_rewrite_tags(filtervar: *const c_char, taglist: *mut List) {
    let mut lc: *mut crate::nodes::pg_list::ListCell = list_head(taglist);
    while !lc.is_null() {
        let tagstr: *const c_char = strVal(lfirst(lc) as *mut c_void);
        let commandTag: CommandTag = GetCommandTagEnum(tagstr);

        if !command_tag_table_rewrite_ok(commandTag) {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                /* translator: %s represents an SQL statement name */
                errmsg!(
                    "event triggers are not supported for {}",
                    core::ffi::CStr::from_ptr(tagstr).to_string_lossy()
                )
            );
        }
        lc = lnext(taglist, lc);
    }
}

/*
 * Complain about a duplicate filter variable.
 */
unsafe fn error_duplicate_filter_variable(defname: *const c_char) {
    ereport!(
        ERROR,
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        errmsg!(
            "filter variable \"{}\" specified more than once",
            core::ffi::CStr::from_ptr(defname).to_string_lossy()
        )
    );
}

/*
 * Insert the new pg_event_trigger row and record dependencies.
 */
unsafe fn insert_event_trigger_tuple(
    trigname: *const c_char,
    eventname: *const c_char,
    evtOwner: Oid,
    funcoid: Oid,
    taglist: *mut List,
) -> Oid {
    let tgrel: Relation;
    let trigoid: Oid;
    let tuple: HeapTuple;
    let mut values: [Datum; Natts_pg_event_trigger] = core::mem::zeroed();
    let mut nulls: [bool; Natts_pg_event_trigger] = [false; Natts_pg_event_trigger];
    let mut evtnamedata: NameData = core::mem::zeroed();
    let mut evteventdata: NameData = core::mem::zeroed();
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    /* Open pg_event_trigger. */
    tgrel = table_open(EventTriggerRelationId, RowExclusiveLock);

    /* Build the new pg_trigger tuple. */
    trigoid = GetNewOidWithIndex(tgrel, EventTriggerOidIndexId, Anum_pg_event_trigger_oid);
    values[Anum_pg_event_trigger_oid as usize - 1] = ObjectIdGetDatum(trigoid);
    namestrcpy(&mut evtnamedata, trigname);
    values[Anum_pg_event_trigger_evtname as usize - 1] = NameGetDatum(&evtnamedata);
    namestrcpy(&mut evteventdata, eventname);
    values[Anum_pg_event_trigger_evtevent as usize - 1] = NameGetDatum(&evteventdata);
    values[Anum_pg_event_trigger_evtowner as usize - 1] = ObjectIdGetDatum(evtOwner);
    values[Anum_pg_event_trigger_evtfoid as usize - 1] = ObjectIdGetDatum(funcoid);
    values[Anum_pg_event_trigger_evtenabled as usize - 1] =
        CharGetDatum(TRIGGER_FIRES_ON_ORIGIN);
    if taglist.is_null() /* NIL */ {
        nulls[Anum_pg_event_trigger_evttags as usize - 1] = true;
    } else {
        values[Anum_pg_event_trigger_evttags as usize - 1] = filter_list_to_array(taglist);
    }

    /* Insert heap tuple. */
    tuple = heap_form_tuple(RelationGetDescr(tgrel), values.as_mut_ptr(), nulls.as_mut_ptr());
    CatalogTupleInsert(tgrel, tuple);
    heap_freetuple(tuple);

    /*
     * Login event triggers have an additional flag in pg_database to enable
     * faster lookups in hot codepaths. Set the flag unless already True.
     */
    if libc_strcmp(eventname, c"login".as_ptr()) == 0 {
        SetDatabaseHasLoginEventTriggers();
    }

    /* Depend on owner. */
    recordDependencyOnOwner(EventTriggerRelationId, trigoid, evtOwner);

    /* Depend on event trigger function. */
    myself.classId = EventTriggerRelationId;
    myself.objectId = trigoid;
    myself.objectSubId = 0;
    referenced.classId = ProcedureRelationId;
    referenced.objectId = funcoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* Depend on extension, if any. */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new event trigger */
    InvokeObjectPostCreateHook(EventTriggerRelationId, trigoid, 0);

    /* Close pg_event_trigger. */
    table_close(tgrel, RowExclusiveLock);

    trigoid
}

// NameGetDatum  TODO(pg-port)
unsafe fn NameGetDatum(_name: *const NameData) -> Datum {
    unimplemented!() // TODO(pg-port): postgres.h macro
}

// CharGetDatum  TODO(pg-port)
unsafe fn CharGetDatum(_c: c_char) -> Datum {
    unimplemented!() // TODO(pg-port): postgres.h macro
}

/*
 * In the parser, a clause like WHEN tag IN ('cmd1', 'cmd2') is represented
 * by a DefElem whose value is a List of String nodes; in the catalog, we
 * store the list of strings as a text array.  This function transforms the
 * former representation into the latter one.
 *
 * For cleanliness, we store command tags in the catalog as text.  It's
 * possible (although not currently anticipated) that we might have
 * a case-sensitive filter variable in the future, in which case this would
 * need some further adjustment.
 */
unsafe fn filter_list_to_array(filterlist: *mut List) -> Datum {
    let mut lc: *mut crate::nodes::pg_list::ListCell = list_head(filterlist);
    let l: c_int = list_length(filterlist);
    let data: *mut Datum = palloc(l as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    let mut i: c_int = 0;

    while !lc.is_null() {
        let value: *const c_char = strVal(lfirst(lc) as *mut c_void);
        let mut result: *mut c_char = pstrdup(value) as *mut c_char;
        let mut p = result;

        while *p != 0 {
            *p = pg_ascii_toupper(*p as u8) as c_char;
            p = p.add(1);
        }
        *data.add(i as usize) = PointerGetDatum(cstring_to_text(result));
        pfree(result as *mut c_void);
        i += 1;
        lc = lnext(filterlist, lc);
    }

    PointerGetDatum(construct_array_builtin(data, l, TEXTOID))
}

/*
 * Set pg_database.dathasloginevt flag for current database indicating that
 * current database has on login event triggers.
 */
pub unsafe fn SetDatabaseHasLoginEventTriggers() {
    /* Set dathasloginevt flag in pg_database */
    let db: Form_pg_database;
    let pg_db: Relation = table_open(DatabaseRelationId, RowExclusiveLock);
    let mut otid: ItemPointerData = core::mem::zeroed();
    let tuple: HeapTuple;

    /*
     * Use shared lock to prevent a conflict with EventTriggerOnLogin() trying
     * to reset pg_database.dathasloginevt flag.  Note, this lock doesn't
     * effectively blocks database or other objection.  It's just custom lock
     * tag used to prevent multiple backends changing
     * pg_database.dathasloginevt flag.
     */
    LockSharedObject(DatabaseRelationId, MyDatabaseId, 0, AccessExclusiveLock);

    tuple = SearchSysCacheLockedCopy1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for database {}", MyDatabaseId);
    }
    otid = (*tuple).t_self;
    db = GETSTRUCT(tuple) as Form_pg_database;
    if !(*db).dathasloginevt {
        (*db).dathasloginevt = true;
        CatalogTupleUpdate(pg_db, &otid, tuple);
        CommandCounterIncrement();
    }
    UnlockTuple(pg_db, &otid, InplaceUpdateTupleLock);
    table_close(pg_db, RowExclusiveLock);
    heap_freetuple(tuple);
}

/*
 * ALTER EVENT TRIGGER foo ENABLE|DISABLE|ENABLE ALWAYS|REPLICA
 */
pub unsafe fn AlterEventTrigger(
    stmt: *mut crate::nodes::parsenodes::AlterEventTrigStmt,
) -> Oid {
    let tgrel: Relation;
    let tup: HeapTuple;
    let trigoid: Oid;
    let evtForm: Form_pg_event_trigger;
    let tgenabled: c_char = (*stmt).tgenabled;

    tgrel = table_open(EventTriggerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(EVENTTRIGGERNAME, CStringGetDatum((*stmt).trigname));
    if !HeapTupleIsValid(tup) {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!(
                "event trigger \"{}\" does not exist",
                core::ffi::CStr::from_ptr((*stmt).trigname).to_string_lossy()
            )
        );
    }

    evtForm = GETSTRUCT(tup) as Form_pg_event_trigger;
    trigoid = (*evtForm).oid;

    if !object_ownercheck(EventTriggerRelationId, trigoid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_EVENT_TRIGGER, (*stmt).trigname);
    }

    /* tuple is a copy, so we can modify it below */
    (*evtForm).evtenabled = tgenabled;

    CatalogTupleUpdate(tgrel, &(*tup).t_self, tup);

    /*
     * Login event triggers have an additional flag in pg_database to enable
     * faster lookups in hot codepaths. Set the flag unless already True.
     */
    if namestrcmp(&(*evtForm).evtevent, c"login".as_ptr()) == 0
        && tgenabled != TRIGGER_DISABLED
    {
        SetDatabaseHasLoginEventTriggers();
    }

    InvokeObjectPostAlterHook(EventTriggerRelationId, trigoid, 0);

    /* clean up */
    heap_freetuple(tup);
    table_close(tgrel, RowExclusiveLock);

    trigoid
}

/*
 * Change event trigger's owner -- by name
 */
pub unsafe fn AlterEventTriggerOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let evtOid: Oid;
    let tup: HeapTuple;
    let evtForm: Form_pg_event_trigger;
    let rel: Relation;
    let mut address: ObjectAddress = core::mem::zeroed();

    rel = table_open(EventTriggerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(EVENTTRIGGERNAME, CStringGetDatum(name));

    if !HeapTupleIsValid(tup) {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!(
                "event trigger \"{}\" does not exist",
                core::ffi::CStr::from_ptr(name).to_string_lossy()
            )
        );
    }

    evtForm = GETSTRUCT(tup) as Form_pg_event_trigger;
    evtOid = (*evtForm).oid;

    AlterEventTriggerOwner_internal(rel, tup, newOwnerId);

    ObjectAddressSet(&mut address, EventTriggerRelationId, evtOid);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);

    address
}

/*
 * Change event trigger owner, by OID
 */
pub unsafe fn AlterEventTriggerOwner_oid(trigOid: Oid, newOwnerId: Oid) {
    let tup: HeapTuple;
    let rel: Relation;

    rel = table_open(EventTriggerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(EVENTTRIGGEROID, ObjectIdGetDatum(trigOid));

    if !HeapTupleIsValid(tup) {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!("event trigger with OID {} does not exist", trigOid)
        );
    }

    AlterEventTriggerOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Internal workhorse for changing an event trigger's owner
 */
unsafe fn AlterEventTriggerOwner_internal(rel: Relation, tup: HeapTuple, newOwnerId: Oid) {
    let form: Form_pg_event_trigger;

    form = GETSTRUCT(tup) as Form_pg_event_trigger;

    if (*form).evtowner == newOwnerId {
        return;
    }

    if !object_ownercheck(EventTriggerRelationId, (*form).oid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_EVENT_TRIGGER,
            NameStr(&(*form).evtname),
        );
    }

    /* New owner must be a superuser */
    if !superuser_arg(newOwnerId) {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
            errmsg!(
                "permission denied to change owner of event trigger \"{}\"",
                core::ffi::CStr::from_ptr(NameStr(&(*form).evtname)).to_string_lossy()
            )
            /* C also: errhint("The owner of an event trigger must be a superuser.") */
        );
    }

    (*form).evtowner = newOwnerId;
    CatalogTupleUpdate(rel, &(*tup).t_self, tup);

    /* Update owner dependency reference */
    changeDependencyOnOwner(EventTriggerRelationId, (*form).oid, newOwnerId);

    InvokeObjectPostAlterHook(EventTriggerRelationId, (*form).oid, 0);
}

/*
 * get_event_trigger_oid - Look up an event trigger by name to find its OID.
 *
 * If missing_ok is false, throw an error if trigger not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_event_trigger_oid(trigname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid1(
        EVENTTRIGGERNAME,
        Anum_pg_event_trigger_oid,
        CStringGetDatum(trigname),
    );
    if !OidIsValid(oid) && !missing_ok {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!(
                "event trigger \"{}\" does not exist",
                core::ffi::CStr::from_ptr(trigname).to_string_lossy()
            )
        );
    }
    oid
}

/*
 * Return true when we want to fire given Event Trigger and false otherwise,
 * filtering on the session replication role and the event trigger registered
 * tags matching.
 */
unsafe fn filter_event_trigger(tag: CommandTag, item: *const EventTriggerCacheItem) -> bool {
    /*
     * Filter by session replication role, knowing that we never see disabled
     * items down here.
     */
    if SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA {
        if (*item).enabled == TRIGGER_FIRES_ON_ORIGIN {
            return false;
        }
    } else {
        if (*item).enabled == TRIGGER_FIRES_ON_REPLICA {
            return false;
        }
    }

    /* Filter by tags, if any were specified. */
    if !bms_is_empty((*item).tagset) && !bms_is_member(tag, (*item).tagset) {
        return false;
    }

    /* if we reach that point, we're not filtering out this item */
    true
}

unsafe fn EventTriggerGetTag(parsetree: *mut Node, event: EventTriggerEvent) -> CommandTag {
    if event == EVT_Login {
        CMDTAG_LOGIN
    } else {
        CreateCommandTag(parsetree)
    }
}

/*
 * Setup for running triggers for the given event.  Return value is an OID list
 * of functions to run; if there are any, trigdata is filled with an
 * appropriate EventTriggerData for them to receive.
 */
unsafe fn EventTriggerCommonSetup(
    parsetree: *mut Node,
    event: EventTriggerEvent,
    eventstr: *const c_char,
    trigdata: *mut EventTriggerData,
    unfiltered: bool,
) -> *mut List {
    let tag: CommandTag;
    let cachelist: *mut List;
    let mut lc: *mut crate::nodes::pg_list::ListCell;
    let mut runlist: *mut List = NIL;

    /*
     * We want the list of command tags for which this procedure is actually
     * invoked to match up exactly with the list that CREATE EVENT TRIGGER
     * accepts.  This debugging cross-check will throw an error if this
     * function is invoked for a command tag that CREATE EVENT TRIGGER won't
     * accept.  (Unfortunately, there doesn't seem to be any simple, automated
     * way to verify that CREATE EVENT TRIGGER doesn't accept extra stuff that
     * never reaches this control point.)
     *
     * If this cross-check fails for you, you probably need to either adjust
     * standard_ProcessUtility() not to invoke event triggers for the command
     * type in question, or you need to adjust event_trigger_ok to accept the
     * relevant command tag.
     */
    #[cfg(debug_assertions)]
    {
        let dbgtag: CommandTag = EventTriggerGetTag(parsetree, event);

        if event == EVT_DDLCommandStart
            || event == EVT_DDLCommandEnd
            || event == EVT_SQLDrop
            || event == EVT_Login
        {
            if !command_tag_event_trigger_ok(dbgtag) {
                elog!(
                    ERROR,
                    "unexpected command tag \"{}\"",
                    core::ffi::CStr::from_ptr(GetCommandTagName(dbgtag)).to_string_lossy()
                );
            }
        } else if event == EVT_TableRewrite {
            if !command_tag_table_rewrite_ok(dbgtag) {
                elog!(
                    ERROR,
                    "unexpected command tag \"{}\"",
                    core::ffi::CStr::from_ptr(GetCommandTagName(dbgtag)).to_string_lossy()
                );
            }
        }
    }

    /* Use cache to find triggers for this event; fast exit if none. */
    cachelist = EventCacheLookup(event);
    if cachelist.is_null() /* NIL */ {
        return NIL;
    }

    /* Get the command tag. */
    tag = EventTriggerGetTag(parsetree, event);

    /*
     * Filter list of event triggers by command tag, and copy them into our
     * memory context.  Once we start running the command triggers, or indeed
     * once we do anything at all that touches the catalogs, an invalidation
     * might leave cachelist pointing at garbage, so we must do this before we
     * can do much else.
     */
    lc = list_head(cachelist);
    while !lc.is_null() {
        let item: *const EventTriggerCacheItem = lfirst(lc) as *const EventTriggerCacheItem;

        if unfiltered || filter_event_trigger(tag, item) {
            /* We must plan to fire this trigger. */
            runlist = lappend_oid(runlist, (*item).fnoid);
        }
        lc = lnext(cachelist, lc);
    }

    /* Don't spend any more time on this if no functions to run */
    if runlist.is_null() /* NIL */ {
        return NIL;
    }

    (*trigdata).type_ = crate::nodes::nodes::NodeTag::T_EventTriggerData;
    (*trigdata).event = eventstr;
    (*trigdata).parsetree = parsetree;
    (*trigdata).tag = tag;

    runlist
}

/*
 * Fire ddl_command_start triggers.
 */
pub unsafe fn EventTriggerDDLCommandStart(parsetree: *mut Node) {
    let runlist: *mut List;
    let mut trigdata: EventTriggerData = core::mem::zeroed();

    /*
     * Event Triggers are completely disabled in standalone mode.  There are
     * (at least) two reasons for this:
     *
     * 1. A sufficiently broken event trigger might not only render the
     * database unusable, but prevent disabling itself to fix the situation.
     * In this scenario, restarting in standalone mode provides an escape
     * hatch.
     *
     * 2. BuildEventTriggerCache relies on systable_beginscan_ordered, and
     * therefore will malfunction if pg_event_trigger's indexes are damaged.
     * To allow recovery from a damaged index, we need some operating mode
     * wherein event triggers are disabled.  (Or we could implement
     * heapscan-and-sort logic for that case, but having disaster recovery
     * scenarios depend on code that's otherwise untested isn't appetizing.)
     *
     * Additionally, event triggers can be disabled with a superuser-only GUC
     * to make fixing database easier as per 1 above.
     */
    if !IsUnderPostmaster || !event_triggers {
        return;
    }

    runlist = EventTriggerCommonSetup(
        parsetree,
        EVT_DDLCommandStart,
        c"ddl_command_start".as_ptr(),
        &mut trigdata,
        false,
    );
    if runlist.is_null() /* NIL */ {
        return;
    }

    /* Run the triggers. */
    EventTriggerInvoke(runlist, &mut trigdata);

    /* Cleanup. */
    list_free(runlist);

    /*
     * Make sure anything the event triggers did will be visible to the main
     * command.
     */
    CommandCounterIncrement();
}

/*
 * Fire ddl_command_end triggers.
 */
pub unsafe fn EventTriggerDDLCommandEnd(parsetree: *mut Node) {
    let runlist: *mut List;
    let mut trigdata: EventTriggerData = core::mem::zeroed();

    /*
     * See EventTriggerDDLCommandStart for a discussion about why event
     * triggers are disabled in single user mode or via GUC.
     */
    if !IsUnderPostmaster || !event_triggers {
        return;
    }

    /*
     * Also do nothing if our state isn't set up, which it won't be if there
     * weren't any relevant event triggers at the start of the current DDL
     * command.  This test might therefore seem optional, but it's important
     * because EventTriggerCommonSetup might find triggers that didn't exist
     * at the time the command started.  Although this function itself
     * wouldn't crash, the event trigger functions would presumably call
     * pg_event_trigger_ddl_commands which would fail.  Better to do nothing
     * until the next command.
     */
    if currentEventTriggerState.is_null() {
        return;
    }

    runlist = EventTriggerCommonSetup(
        parsetree,
        EVT_DDLCommandEnd,
        c"ddl_command_end".as_ptr(),
        &mut trigdata,
        false,
    );
    if runlist.is_null() /* NIL */ {
        return;
    }

    /*
     * Make sure anything the main command did will be visible to the event
     * triggers.
     */
    CommandCounterIncrement();

    /* Run the triggers. */
    EventTriggerInvoke(runlist, &mut trigdata);

    /* Cleanup. */
    list_free(runlist);
}

/*
 * Fire sql_drop triggers.
 */
pub unsafe fn EventTriggerSQLDrop(parsetree: *mut Node) {
    let runlist: *mut List;
    let mut trigdata: EventTriggerData = core::mem::zeroed();

    /*
     * See EventTriggerDDLCommandStart for a discussion about why event
     * triggers are disabled in single user mode or via a GUC.
     */
    if !IsUnderPostmaster || !event_triggers {
        return;
    }

    /*
     * Use current state to determine whether this event fires at all.  If
     * there are no triggers for the sql_drop event, then we don't have
     * anything to do here.  Note that dropped object collection is disabled
     * if this is the case, so even if we were to try to run, the list would
     * be empty.
     */
    if currentEventTriggerState.is_null()
        || slist_is_empty(&(*currentEventTriggerState).SQLDropList)
    {
        return;
    }

    runlist = EventTriggerCommonSetup(
        parsetree,
        EVT_SQLDrop,
        c"sql_drop".as_ptr(),
        &mut trigdata,
        false,
    );

    /*
     * Nothing to do if run list is empty.  Note this typically can't happen,
     * because if there are no sql_drop events, then objects-to-drop wouldn't
     * have been collected in the first place and we would have quit above.
     * But it could occur if event triggers were dropped partway through.
     */
    if runlist.is_null() /* NIL */ {
        return;
    }

    /*
     * Make sure anything the main command did will be visible to the event
     * triggers.
     */
    CommandCounterIncrement();

    /*
     * Make sure pg_event_trigger_dropped_objects only works when running
     * these triggers.  Use PG_TRY to ensure in_sql_drop is reset even when
     * one trigger fails.  (This is perhaps not necessary, as the currentState
     * variable will be removed shortly by our caller, but it seems better to
     * play safe.)
     */
    (*currentEventTriggerState).in_sql_drop = true;

    /* Run the triggers. */
    // PG_TRY/PG_FINALLY expressed as a closure-based pattern
    {
        // PG_TRY
        EventTriggerInvoke(runlist, &mut trigdata);
    }
    // PG_FINALLY (always runs)
    (*currentEventTriggerState).in_sql_drop = false;
    // PG_END_TRY

    /* Cleanup. */
    list_free(runlist);
}

/*
 * Fire login event triggers if any are present.  The dathasloginevt
 * pg_database flag is left unchanged when an event trigger is dropped to avoid
 * complicating the codepath in the case of multiple event triggers.  This
 * function will instead unset the flag if no trigger is defined.
 */
pub unsafe fn EventTriggerOnLogin() {
    let mut runlist: *mut List;
    let mut trigdata: EventTriggerData = core::mem::zeroed();

    /*
     * See EventTriggerDDLCommandStart for a discussion about why event
     * triggers are disabled in single user mode or via a GUC.  We also need a
     * database connection (some background workers don't have it).
     */
    if !IsUnderPostmaster
        || !event_triggers
        || !OidIsValid(MyDatabaseId)
        || !MyDatabaseHasLoginEventTriggers
    {
        return;
    }

    StartTransactionCommand();
    runlist = EventTriggerCommonSetup(
        core::ptr::null_mut(),
        EVT_Login,
        c"login".as_ptr(),
        &mut trigdata,
        false,
    );

    if !runlist.is_null() /* != NIL */ {
        /*
         * Event trigger execution may require an active snapshot.
         */
        PushActiveSnapshot(GetTransactionSnapshot());

        /* Run the triggers. */
        EventTriggerInvoke(runlist, &mut trigdata);

        /* Cleanup. */
        list_free(runlist);

        PopActiveSnapshot();
    }
    /*
     * There is no active login event trigger, but our
     * pg_database.dathasloginevt is set. Try to unset this flag.  We use the
     * lock to prevent concurrent SetDatabaseHasLoginEventTriggers(), but we
     * don't want to hang the connection waiting on the lock.  Thus, we are
     * just trying to acquire the lock conditionally.
     */
    else if ConditionalLockSharedObject(
        DatabaseRelationId,
        MyDatabaseId,
        0,
        AccessExclusiveLock,
    ) {
        /*
         * The lock is held.  Now we need to recheck that login event triggers
         * list is still empty.  Once the list is empty, we know that even if
         * there is a backend which concurrently inserts/enables a login event
         * trigger, it will update pg_database.dathasloginevt *afterwards*.
         */
        runlist = EventTriggerCommonSetup(
            core::ptr::null_mut(),
            EVT_Login,
            c"login".as_ptr(),
            &mut trigdata,
            true,
        );

        if runlist.is_null() /* NIL */ {
            let pg_db: Relation = table_open(DatabaseRelationId, RowExclusiveLock);
            let mut tuple: HeapTuple = core::ptr::null_mut();
            let mut state: *mut c_void = core::ptr::null_mut();
            let db: Form_pg_database;
            let mut key: [ScanKeyData; 1] = core::mem::zeroed();

            /* Fetch a copy of the tuple to scribble on */
            ScanKeyInit(
                &mut key[0],
                Anum_pg_database_oid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(MyDatabaseId),
            );

            systable_inplace_update_begin(
                pg_db,
                DatabaseOidIndexId,
                true,
                core::ptr::null_mut(),
                1,
                key.as_ptr(),
                &mut tuple,
                &mut state,
            );

            if !HeapTupleIsValid(tuple) {
                elog!(ERROR, "could not find tuple for database {}", MyDatabaseId);
            }

            db = GETSTRUCT(tuple) as Form_pg_database;
            if (*db).dathasloginevt {
                (*db).dathasloginevt = false;

                /*
                 * Do an "in place" update of the pg_database tuple.  Doing
                 * this instead of regular updates serves two purposes. First,
                 * that avoids possible waiting on the row-level lock. Second,
                 * that avoids dealing with TOAST.
                 */
                systable_inplace_update_finish(state, tuple);
            } else {
                systable_inplace_update_cancel(state);
            }
            table_close(pg_db, RowExclusiveLock);
            heap_freetuple(tuple);
        } else {
            list_free(runlist);
        }
    }
    CommitTransactionCommand();
}


/*
 * Fire table_rewrite triggers.
 */
pub unsafe fn EventTriggerTableRewrite(parsetree: *mut Node, tableOid: Oid, reason: c_int) {
    let runlist: *mut List;
    let mut trigdata: EventTriggerData = core::mem::zeroed();

    /*
     * See EventTriggerDDLCommandStart for a discussion about why event
     * triggers are disabled in single user mode or via a GUC.
     */
    if !IsUnderPostmaster || !event_triggers {
        return;
    }

    /*
     * Also do nothing if our state isn't set up, which it won't be if there
     * weren't any relevant event triggers at the start of the current DDL
     * command.  This test might therefore seem optional, but it's
     * *necessary*, because EventTriggerCommonSetup might find triggers that
     * didn't exist at the time the command started.
     */
    if currentEventTriggerState.is_null() {
        return;
    }

    runlist = EventTriggerCommonSetup(
        parsetree,
        EVT_TableRewrite,
        c"table_rewrite".as_ptr(),
        &mut trigdata,
        false,
    );
    if runlist.is_null() /* NIL */ {
        return;
    }

    /*
     * Make sure pg_event_trigger_table_rewrite_oid only works when running
     * these triggers. Use PG_TRY to ensure table_rewrite_oid is reset even
     * when one trigger fails. (This is perhaps not necessary, as the
     * currentState variable will be removed shortly by our caller, but it
     * seems better to play safe.)
     */
    (*currentEventTriggerState).table_rewrite_oid = tableOid;
    (*currentEventTriggerState).table_rewrite_reason = reason;

    /* Run the triggers. */
    // PG_TRY/PG_FINALLY
    {
        EventTriggerInvoke(runlist, &mut trigdata);
    }
    // PG_FINALLY (always runs)
    (*currentEventTriggerState).table_rewrite_oid = InvalidOid;
    (*currentEventTriggerState).table_rewrite_reason = 0;
    // PG_END_TRY

    /* Cleanup. */
    list_free(runlist);

    /*
     * Make sure anything the event triggers did will be visible to the main
     * command.
     */
    CommandCounterIncrement();
}

/*
 * Invoke each event trigger in a list of event triggers.
 */
unsafe fn EventTriggerInvoke(fn_oid_list: *mut List, trigdata: *mut EventTriggerData) {
    let context: MemoryContext;
    let oldcontext: MemoryContext;
    let mut lc: *mut crate::nodes::pg_list::ListCell;
    let mut first: bool = true;

    /* Guard against stack overflow due to recursive event trigger */
    check_stack_depth();

    /*
     * Let's evaluate event triggers in their own memory context, so that any
     * leaks get cleaned up promptly.
     */
    context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"event trigger context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    oldcontext = MemoryContextSwitchTo(context);

    /* Call each event trigger. */
    lc = list_head(fn_oid_list);
    while !lc.is_null() {
        let fnoid: Oid = lfirst_oid(lc);
        let mut flinfo: FmgrInfo = core::mem::zeroed();
        let mut fcusage: PgStat_FunctionCallUsage = core::mem::zeroed();
        // Allocate fcinfo on the stack; 0 args.
        // LOCAL_FCINFO(fcinfo, 0) -> a FunctionCallInfoBaseData with 0 args
        let fcinfo_storage: *mut crate::utils::fmgr::FunctionCallInfoBaseData =
            palloc0(core::mem::size_of::<crate::utils::fmgr::FunctionCallInfoBaseData>())
                as *mut crate::utils::fmgr::FunctionCallInfoBaseData;
        let fcinfo: FunctionCallInfo = fcinfo_storage;

        elog!(crate::utils::elog::DEBUG1, "EventTriggerInvoke {}", fnoid);

        /*
         * We want each event trigger to be able to see the results of the
         * previous event trigger's action.  Caller is responsible for any
         * command-counter increment that is needed between the event trigger
         * and anything else in the transaction.
         */
        if first {
            first = false;
        } else {
            CommandCounterIncrement();
        }

        /* Look up the function */
        fmgr_info(fnoid, &mut flinfo);

        /* Call the function, passing no arguments but setting a context. */
        InitFunctionCallInfoData(
            fcinfo,
            &mut flinfo,
            0,
            InvalidOid,
            trigdata as *mut Node,
            core::ptr::null_mut(),
        );
        pgstat_init_function_usage(fcinfo, &mut fcusage);
        FunctionCallInvoke(fcinfo);
        pgstat_end_function_usage(&mut fcusage, true);

        /* Reclaim memory. */
        MemoryContextReset(context);

        lc = lnext(fn_oid_list, lc);
    }

    /* Restore old memory context and delete the temporary one. */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(context);
}

/*
 * Do event triggers support this object type?
 *
 * See also event trigger documentation in event-trigger.sgml.
 */
pub unsafe fn EventTriggerSupportsObjectType(obtype: ObjectType) -> bool {
    match obtype {
        ObjectType::OBJECT_DATABASE
        | ObjectType::OBJECT_TABLESPACE
        | ObjectType::OBJECT_ROLE
        | ObjectType::OBJECT_PARAMETER_ACL =>
            /* no support for global objects (except subscriptions) */
            false,
        ObjectType::OBJECT_EVENT_TRIGGER =>
            /* no support for event triggers on event triggers */
            false,
        _ => true,
    }
}

/*
 * Do event triggers support this object class?
 *
 * See also event trigger documentation in event-trigger.sgml.
 */
pub unsafe fn EventTriggerSupportsObject(object: *const ObjectAddress) -> bool {
    match (*object).classId {
        DatabaseRelationId
        | TableSpaceRelationId
        | AuthIdRelationId
        | AuthMemRelationId
        | ParameterAclRelationId =>
            /* no support for global objects (except subscriptions) */
            false,
        EventTriggerRelationId =>
            /* no support for event triggers on event triggers */
            false,
        _ => true,
    }
}

/*
 * Prepare event trigger state for a new complete query to run, if necessary;
 * returns whether this was done.  If it was, EventTriggerEndCompleteQuery must
 * be called when the query is done, regardless of whether it succeeds or fails
 * -- so use of a PG_TRY block is mandatory.
 */
pub unsafe fn EventTriggerBeginCompleteQuery() -> bool {
    let state: *mut EventTriggerQueryState;
    let cxt: MemoryContext;

    /*
     * Currently, sql_drop, table_rewrite, ddl_command_end events are the only
     * reason to have event trigger state at all; so if there are none, don't
     * install one.
     */
    if !trackDroppedObjectsNeeded() {
        return false;
    }

    cxt = AllocSetContextCreate!(
        TopMemoryContext,
        c"event trigger state".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    state = MemoryContextAlloc(cxt, core::mem::size_of::<EventTriggerQueryState>())
        as *mut EventTriggerQueryState;
    (*state).cxt = cxt;
    slist_init(&mut (*state).SQLDropList);
    (*state).in_sql_drop = false;
    (*state).table_rewrite_oid = InvalidOid;

    (*state).commandCollectionInhibited = if !currentEventTriggerState.is_null() {
        (*currentEventTriggerState).commandCollectionInhibited
    } else {
        false
    };
    (*state).currentCommand = core::ptr::null_mut();
    (*state).commandList = NIL;
    (*state).previous = currentEventTriggerState;
    currentEventTriggerState = state;

    true
}

/*
 * Query completed (or errored out) -- clean up local state, return to previous
 * one.
 *
 * Note: it's an error to call this routine if EventTriggerBeginCompleteQuery
 * returned false previously.
 *
 * Note: this might be called in the PG_CATCH block of a failing transaction,
 * so be wary of running anything unnecessary.  (In particular, it's probably
 * unwise to try to allocate memory.)
 */
pub unsafe fn EventTriggerEndCompleteQuery() {
    let prevstate: *mut EventTriggerQueryState;

    prevstate = (*currentEventTriggerState).previous;

    /* this avoids the need for retail pfree of SQLDropList items: */
    MemoryContextDelete((*currentEventTriggerState).cxt);

    currentEventTriggerState = prevstate;
}

/*
 * Do we need to keep close track of objects being dropped?
 *
 * This is useful because there is a cost to running with them enabled.
 */
pub unsafe fn trackDroppedObjectsNeeded() -> bool {
    /*
     * true if any sql_drop, table_rewrite, ddl_command_end event trigger
     * exists
     */
    (!EventCacheLookup(EVT_SQLDrop).is_null())
        || (!EventCacheLookup(EVT_TableRewrite).is_null())
        || (!EventCacheLookup(EVT_DDLCommandEnd).is_null())
}

// string.h  TODO(pg-port)
unsafe fn memcpy(_dst: *mut c_void, _src: *const c_void, _n: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): libc memcpy
}

/*
 * EventTriggerSQLDropAddObject
 *		Add an object to the list of dropped objects for the current event
 *		trigger query.
 */
pub unsafe fn EventTriggerSQLDropAddObject(
    object: *const ObjectAddress,
    original: bool,
    normal: bool,
) {
    let obj: *mut SQLDropObject;
    let oldcxt: MemoryContext;

    if currentEventTriggerState.is_null() {
        return;
    }

    Assert!(EventTriggerSupportsObject(object));

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    obj = palloc0(core::mem::size_of::<SQLDropObject>()) as *mut SQLDropObject;
    (*obj).address = *object;
    (*obj).original = original;
    (*obj).normal = normal;

    if (*object).classId == NamespaceRelationId {
        /* Special handling is needed for temp namespaces */
        if isTempNamespace((*object).objectId) {
            (*obj).istemp = true;
        } else if isAnyTempNamespace((*object).objectId) {
            /* don't report temp schemas except my own */
            pfree(obj as *mut c_void);
            MemoryContextSwitchTo(oldcxt);
            return;
        }
        (*obj).objname = get_namespace_name((*object).objectId);
    } else if (*object).classId == AttrDefaultRelationId {
        /* We treat a column default as temp if its table is temp */
        let colobject: ObjectAddress;

        colobject = GetAttrDefaultColumnAddress((*object).objectId);
        if OidIsValid(colobject.objectId) {
            if !obtain_object_name_namespace(&colobject, obj) {
                pfree(obj as *mut c_void);
                MemoryContextSwitchTo(oldcxt);
                return;
            }
        }
    } else if (*object).classId == TriggerRelationId {
        /* Similarly, a trigger is temp if its table is temp */
        /* Sadly, there's no lsyscache.c support for trigger objects */
        let pg_trigger_rel: Relation;
        let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
        let sscan: SysScanDesc;
        let tuple: HeapTuple;
        let relid: Oid;

        /* Fetch the trigger's table OID the hard way */
        pg_trigger_rel = table_open(TriggerRelationId, AccessShareLock);
        ScanKeyInit(
            &mut skey[0],
            Anum_pg_trigger_oid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*object).objectId),
        );
        sscan = systable_beginscan(
            pg_trigger_rel,
            TriggerOidIndexId,
            true,
            core::ptr::null_mut(),
            1,
            skey.as_mut_ptr(),
        );
        tuple = systable_getnext(sscan);
        if HeapTupleIsValid(tuple) {
            relid = (*(GETSTRUCT(tuple) as Form_pg_trigger)).tgrelid;
        } else {
            relid = InvalidOid; /* shouldn't happen */
        }
        systable_endscan(sscan);
        table_close(pg_trigger_rel, AccessShareLock);
        /* Do nothing if we didn't find the trigger */
        if OidIsValid(relid) {
            let mut relobject: ObjectAddress = core::mem::zeroed();

            relobject.classId = RelationRelationId;
            relobject.objectId = relid;
            /* Arbitrarily set objectSubId nonzero so as not to fill objname */
            relobject.objectSubId = 1;
            if !obtain_object_name_namespace(&relobject, obj) {
                pfree(obj as *mut c_void);
                MemoryContextSwitchTo(oldcxt);
                return;
            }
        }
    } else if (*object).classId == PolicyRelationId {
        /* Similarly, a policy is temp if its table is temp */
        /* Sadly, there's no lsyscache.c support for policy objects */
        let pg_policy_rel: Relation;
        let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
        let sscan: SysScanDesc;
        let tuple: HeapTuple;
        let relid: Oid;

        /* Fetch the policy's table OID the hard way */
        pg_policy_rel = table_open(PolicyRelationId, AccessShareLock);
        ScanKeyInit(
            &mut skey[0],
            Anum_pg_policy_oid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*object).objectId),
        );
        sscan = systable_beginscan(
            pg_policy_rel,
            PolicyOidIndexId,
            true,
            core::ptr::null_mut(),
            1,
            skey.as_mut_ptr(),
        );
        tuple = systable_getnext(sscan);
        if HeapTupleIsValid(tuple) {
            relid = (*(GETSTRUCT(tuple) as Form_pg_policy)).polrelid;
        } else {
            relid = InvalidOid; /* shouldn't happen */
        }
        systable_endscan(sscan);
        table_close(pg_policy_rel, AccessShareLock);
        /* Do nothing if we didn't find the policy */
        if OidIsValid(relid) {
            let mut relobject: ObjectAddress = core::mem::zeroed();

            relobject.classId = RelationRelationId;
            relobject.objectId = relid;
            /* Arbitrarily set objectSubId nonzero so as not to fill objname */
            relobject.objectSubId = 1;
            if !obtain_object_name_namespace(&relobject, obj) {
                pfree(obj as *mut c_void);
                MemoryContextSwitchTo(oldcxt);
                return;
            }
        }
    } else {
        /* Generic handling for all other object classes */
        if !obtain_object_name_namespace(object, obj) {
            /* don't report temp objects except my own */
            pfree(obj as *mut c_void);
            MemoryContextSwitchTo(oldcxt);
            return;
        }
    }

    /* object identity, objname and objargs */
    (*obj).objidentity = getObjectIdentityParts(
        &(*obj).address,
        &mut (*obj).addrnames,
        &mut (*obj).addrargs,
        false,
    );

    /* object type */
    (*obj).objecttype = getObjectTypeDescription(&(*obj).address, false);

    slist_push_head(&mut (*currentEventTriggerState).SQLDropList, &mut (*obj).next);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * Fill obj->objname, obj->schemaname, and obj->istemp based on object.
 *
 * Returns true if this object should be reported, false if it should
 * be ignored because it is a temporary object of another session.
 */
unsafe fn obtain_object_name_namespace(
    object: *const ObjectAddress,
    obj: *mut SQLDropObject,
) -> bool {
    /*
     * Obtain schema names from the object's catalog tuple, if one exists;
     * this lets us skip objects in temp schemas.  We trust that
     * ObjectProperty contains all object classes that can be
     * schema-qualified.
     *
     * Currently, this function does nothing for object classes that are not
     * in ObjectProperty, but we might sometime add special cases for that.
     */
    if is_objectclass_supported((*object).classId) {
        let catalog: Relation;
        let tuple: HeapTuple;

        catalog = table_open((*object).classId, AccessShareLock);
        tuple = get_catalog_object_by_oid(
            catalog,
            get_object_attnum_oid((*object).classId),
            (*object).objectId,
        );

        if !tuple.is_null() {
            let mut attnum: AttrNumber;
            let mut datum: Datum;
            let mut isnull: bool = false;

            attnum = get_object_attnum_namespace((*object).classId);
            if attnum != InvalidAttrNumber {
                datum = heap_getattr(tuple, attnum, RelationGetDescr(catalog), &mut isnull);
                if !isnull {
                    let namespaceId: Oid;

                    namespaceId = DatumGetObjectId(datum);
                    /* temp objects are only reported if they are my own */
                    if isTempNamespace(namespaceId) {
                        (*obj).schemaname = c"pg_temp".as_ptr();
                        (*obj).istemp = true;
                    } else if isAnyTempNamespace(namespaceId) {
                        /* no need to fill any fields of *obj */
                        table_close(catalog, AccessShareLock);
                        return false;
                    } else {
                        (*obj).schemaname = get_namespace_name(namespaceId);
                        (*obj).istemp = false;
                    }
                }
            }

            if get_object_namensp_unique((*object).classId) && (*object).objectSubId == 0 {
                attnum = get_object_attnum_name((*object).classId);
                if attnum != InvalidAttrNumber {
                    datum = heap_getattr(tuple, attnum, RelationGetDescr(catalog), &mut isnull);
                    if !isnull {
                        (*obj).objname = pstrdup(NameStr(DatumGetName(datum)));
                    }
                }
            }
        }

        table_close(catalog, AccessShareLock);
    }

    true
}

/*
 * pg_event_trigger_dropped_objects
 *
 * Make the list of dropped objects available to the user function run by the
 * Event Trigger.
 */
pub unsafe fn pg_event_trigger_dropped_objects(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut iter: slist_iter = core::mem::zeroed();

    /*
     * Protect this function from being called out of context
     */
    if currentEventTriggerState.is_null() || !(*currentEventTriggerState).in_sql_drop {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED) */
            errmsg!(
                "{} can only be called in a sql_drop event trigger function",
                "pg_event_trigger_dropped_objects()"
            )
        );
    }

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    crate::slist_foreach!(iter, &(*currentEventTriggerState).SQLDropList, {
        let obj: *mut SQLDropObject;
        let mut i: c_int = 0;
        let mut values: [Datum; 12] = [0; 12];
        let mut nulls: [bool; 12] = [false; 12];

        obj = crate::slist_container!(SQLDropObject, next, iter.cur);

        /* classid */
        values[i as usize] = ObjectIdGetDatum((*obj).address.classId);
        i += 1;

        /* objid */
        values[i as usize] = ObjectIdGetDatum((*obj).address.objectId);
        i += 1;

        /* objsubid */
        values[i as usize] = Int32GetDatum((*obj).address.objectSubId);
        i += 1;

        /* original */
        values[i as usize] = BoolGetDatum((*obj).original);
        i += 1;

        /* normal */
        values[i as usize] = BoolGetDatum((*obj).normal);
        i += 1;

        /* is_temporary */
        values[i as usize] = BoolGetDatum((*obj).istemp);
        i += 1;

        /* object_type */
        values[i as usize] = CStringGetTextDatum((*obj).objecttype);
        i += 1;

        /* schema_name */
        if !(*obj).schemaname.is_null() {
            values[i as usize] = CStringGetTextDatum((*obj).schemaname);
            i += 1;
        } else {
            nulls[i as usize] = true;
            i += 1;
        }

        /* object_name */
        if !(*obj).objname.is_null() {
            values[i as usize] = CStringGetTextDatum((*obj).objname);
            i += 1;
        } else {
            nulls[i as usize] = true;
            i += 1;
        }

        /* object_identity */
        if !(*obj).objidentity.is_null() {
            values[i as usize] = CStringGetTextDatum((*obj).objidentity);
            i += 1;
        } else {
            nulls[i as usize] = true;
            i += 1;
        }

        /* address_names and address_args */
        if !(*obj).addrnames.is_null() {
            values[i as usize] = PointerGetDatum(strlist_to_textarray((*obj).addrnames) as *const c_void);
            i += 1;

            if !(*obj).addrargs.is_null() {
                values[i as usize] = PointerGetDatum(strlist_to_textarray((*obj).addrargs) as *const c_void);
                i += 1;
            } else {
                values[i as usize] = PointerGetDatum(construct_empty_array(TEXTOID) as *const c_void);
                i += 1;
            }
        } else {
            nulls[i as usize] = true;
            i += 1;
            nulls[i as usize] = true;
            i += 1;
        }

        let _ = i;

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    });

    0 as Datum /* (Datum) 0 */
}

/*
 * pg_event_trigger_table_rewrite_oid
 *
 * Make the Oid of the table going to be rewritten available to the user
 * function run by the Event Trigger.
 */
pub unsafe fn pg_event_trigger_table_rewrite_oid(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    /*
     * Protect this function from being called out of context
     */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).table_rewrite_oid == InvalidOid
    {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED) */
            errmsg!(
                "{} can only be called in a table_rewrite event trigger function",
                "pg_event_trigger_table_rewrite_oid()"
            )
        );
    }

    crate::PG_RETURN_OID!((*currentEventTriggerState).table_rewrite_oid);
}

/*
 * pg_event_trigger_table_rewrite_reason
 *
 * Make the rewrite reason available to the user.
 */
pub unsafe fn pg_event_trigger_table_rewrite_reason(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    /*
     * Protect this function from being called out of context
     */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).table_rewrite_reason == 0
    {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED) */
            errmsg!(
                "{} can only be called in a table_rewrite event trigger function",
                "pg_event_trigger_table_rewrite_reason()"
            )
        );
    }

    crate::PG_RETURN_INT32!((*currentEventTriggerState).table_rewrite_reason);
}

/*-------------------------------------------------------------------------
 * Support for DDL command deparsing
 *
 * The routines below enable an event trigger function to obtain a list of
 * DDL commands as they are executed.  There are three main pieces to this
 * feature:
 *
 * 1) Within ProcessUtilitySlow, or some sub-routine thereof, each DDL command
 * adds a struct CollectedCommand representation of itself to the command list,
 * using the routines below.
 *
 * 2) Some time after that, ddl_command_end fires and the command list is made
 * available to the event trigger function via pg_event_trigger_ddl_commands();
 * the complete command details are exposed as a column of type pg_ddl_command.
 *
 * 3) An extension can install a function capable of taking a value of type
 * pg_ddl_command and transform it into some external, user-visible and/or
 * -modifiable representation.
 *-------------------------------------------------------------------------
 */

/*
 * Inhibit DDL command collection.
 */
pub unsafe fn EventTriggerInhibitCommandCollection() {
    if currentEventTriggerState.is_null() {
        return;
    }

    (*currentEventTriggerState).commandCollectionInhibited = true;
}

/*
 * Re-establish DDL command collection.
 */
pub unsafe fn EventTriggerUndoInhibitCommandCollection() {
    if currentEventTriggerState.is_null() {
        return;
    }

    (*currentEventTriggerState).commandCollectionInhibited = false;
}

/*
 * EventTriggerCollectSimpleCommand
 *		Save data about a simple DDL command that was just executed
 *
 * address identifies the object being operated on.  secondaryObject is an
 * object address that was related in some way to the executed command; its
 * meaning is command-specific.
 *
 * For instance, for an ALTER obj SET SCHEMA command, objtype is the type of
 * object being moved, objectId is its OID, and secondaryOid is the OID of the
 * old schema.  (The destination schema OID can be obtained by catalog lookup
 * of the object.)
 */
pub unsafe fn EventTriggerCollectSimpleCommand(
    address: ObjectAddress,
    secondaryObject: ObjectAddress,
    parsetree: *mut Node,
) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    command = palloc(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;

    (*command).type_ = SCT_Simple;
    (*command).in_extension = creating_extension;

    (*command).d.simple.address = address;
    (*command).d.simple.secondaryObject = secondaryObject;
    (*command).parsetree = copyObject(parsetree as *mut c_void) as *mut Node;

    (*currentEventTriggerState).commandList =
        lappend((*currentEventTriggerState).commandList, command as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerAlterTableStart
 *		Prepare to receive data on an ALTER TABLE command about to be executed
 *
 * Note we don't collect the command immediately; instead we keep it in
 * currentCommand, and only when we're done processing the subcommands we will
 * add it to the command list.
 */
pub unsafe fn EventTriggerAlterTableStart(parsetree: *mut Node) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    command = palloc(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;

    (*command).type_ = SCT_AlterTable;
    (*command).in_extension = creating_extension;

    (*command).d.alterTable.classId = RelationRelationId;
    (*command).d.alterTable.objectId = InvalidOid;
    (*command).d.alterTable.subcmds = NIL;
    (*command).parsetree = copyObject(parsetree as *mut c_void) as *mut Node;

    (*command).parent = (*currentEventTriggerState).currentCommand;
    (*currentEventTriggerState).currentCommand = command;

    MemoryContextSwitchTo(oldcxt);
}

/*
 * Remember the OID of the object being affected by an ALTER TABLE.
 *
 * This is needed because in some cases we don't know the OID until later.
 */
pub unsafe fn EventTriggerAlterTableRelid(objectId: Oid) {
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    (*(*currentEventTriggerState).currentCommand).d.alterTable.objectId = objectId;
}

/*
 * EventTriggerCollectAlterTableSubcmd
 *		Save data about a single part of an ALTER TABLE.
 *
 * Several different commands go through this path, but apart from ALTER TABLE
 * itself, they are all concerned with AlterTableCmd nodes that are generated
 * internally, so that's all that this code needs to handle at the moment.
 */
pub unsafe fn EventTriggerCollectAlterTableSubcmd(subcmd: *mut Node, address: ObjectAddress) {
    let oldcxt: MemoryContext;
    let newsub: *mut CollectedATSubcmd;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    Assert!(crate::IsA!(subcmd, T_AlterTableCmd));
    Assert!(!(*currentEventTriggerState).currentCommand.is_null());
    Assert!(OidIsValid(
        (*(*currentEventTriggerState).currentCommand).d.alterTable.objectId
    ));

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    newsub = palloc(core::mem::size_of::<CollectedATSubcmd>()) as *mut CollectedATSubcmd;
    (*newsub).address = address;
    (*newsub).parsetree = copyObject(subcmd as *mut c_void) as *mut Node;

    (*(*currentEventTriggerState).currentCommand).d.alterTable.subcmds = lappend(
        (*(*currentEventTriggerState).currentCommand).d.alterTable.subcmds,
        newsub as *mut c_void,
    );

    MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerAlterTableEnd
 *		Finish up saving an ALTER TABLE command, and add it to command list.
 *
 * FIXME this API isn't considering the possibility that an xact/subxact is
 * aborted partway through.  Probably it's best to add an
 * AtEOSubXact_EventTriggers() to fix this.
 */
pub unsafe fn EventTriggerAlterTableEnd() {
    let parent: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    parent = (*(*currentEventTriggerState).currentCommand).parent;

    /* If no subcommands, don't collect */
    if (*(*currentEventTriggerState).currentCommand).d.alterTable.subcmds != NIL {
        let oldcxt: MemoryContext;

        oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

        (*currentEventTriggerState).commandList = lappend(
            (*currentEventTriggerState).commandList,
            (*currentEventTriggerState).currentCommand as *mut c_void,
        );

        MemoryContextSwitchTo(oldcxt);
    } else {
        pfree((*currentEventTriggerState).currentCommand as *mut c_void);
    }

    (*currentEventTriggerState).currentCommand = parent;
}

/*
 * EventTriggerCollectGrant
 *		Save data about a GRANT/REVOKE command being executed
 *
 * This function creates a copy of the InternalGrant, as the original might
 * not have the right lifetime.
 */
pub unsafe fn EventTriggerCollectGrant(istmt: *mut InternalGrant) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;
    let icopy: *mut InternalGrant;
    let mut cell: *mut crate::nodes::pg_list::ListCell;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    /*
     * This is tedious, but necessary.
     */
    icopy = palloc(core::mem::size_of::<InternalGrant>()) as *mut InternalGrant;
    memcpy(
        icopy as *mut c_void,
        istmt as *const c_void,
        core::mem::size_of::<InternalGrant>(),
    );
    (*icopy).objects = list_copy((*istmt).objects);
    (*icopy).grantees = list_copy((*istmt).grantees);
    (*icopy).col_privs = NIL;
    cell = list_head((*istmt).col_privs);
    while !cell.is_null() {
        (*icopy).col_privs = lappend((*icopy).col_privs, copyObject(lfirst(cell)));
        cell = lnext((*istmt).col_privs, cell);
    }

    /* Now collect it, using the copied InternalGrant */
    command = palloc(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;
    (*command).type_ = SCT_Grant;
    (*command).in_extension = creating_extension;
    (*command).d.grant.istmt = icopy;
    (*command).parsetree = core::ptr::null_mut();

    (*currentEventTriggerState).commandList =
        lappend((*currentEventTriggerState).commandList, command as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerCollectAlterOpFam
 *		Save data about an ALTER OPERATOR FAMILY ADD/DROP command being
 *		executed
 */
pub unsafe fn EventTriggerCollectAlterOpFam(
    stmt: *mut crate::nodes::parsenodes::AlterOpFamilyStmt,
    opfamoid: Oid,
    operators: *mut List,
    procedures: *mut List,
) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    command = palloc(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;
    (*command).type_ = SCT_AlterOpFamily;
    (*command).in_extension = creating_extension;
    ObjectAddressSet(
        &mut (*command).d.opfam.address,
        OperatorFamilyRelationId,
        opfamoid,
    );
    (*command).d.opfam.operators = operators;
    (*command).d.opfam.procedures = procedures;
    (*command).parsetree = copyObject(stmt as *mut c_void) as *mut Node;

    (*currentEventTriggerState).commandList =
        lappend((*currentEventTriggerState).commandList, command as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerCollectCreateOpClass
 *		Save data about a CREATE OPERATOR CLASS command being executed
 */
pub unsafe fn EventTriggerCollectCreateOpClass(
    stmt: *mut crate::nodes::parsenodes::CreateOpClassStmt,
    opcoid: Oid,
    operators: *mut List,
    procedures: *mut List,
) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    command = palloc0(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;
    (*command).type_ = SCT_CreateOpClass;
    (*command).in_extension = creating_extension;
    ObjectAddressSet(
        &mut (*command).d.createopc.address,
        OperatorClassRelationId,
        opcoid,
    );
    (*command).d.createopc.operators = operators;
    (*command).d.createopc.procedures = procedures;
    (*command).parsetree = copyObject(stmt as *mut c_void) as *mut Node;

    (*currentEventTriggerState).commandList =
        lappend((*currentEventTriggerState).commandList, command as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerCollectAlterTSConfig
 *		Save data about an ALTER TEXT SEARCH CONFIGURATION command being
 *		executed
 */
pub unsafe fn EventTriggerCollectAlterTSConfig(
    stmt: *mut crate::nodes::parsenodes::AlterTSConfigurationStmt,
    cfgId: Oid,
    dictIds: *mut Oid,
    ndicts: c_int,
) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    command = palloc0(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;
    (*command).type_ = SCT_AlterTSConfig;
    (*command).in_extension = creating_extension;
    ObjectAddressSet(&mut (*command).d.atscfg.address, TSConfigRelationId, cfgId);
    if ndicts > 0 {
        (*command).d.atscfg.dictIds =
            palloc_array(core::mem::size_of::<Oid>(), ndicts) as *mut Oid;
        memcpy(
            (*command).d.atscfg.dictIds as *mut c_void,
            dictIds as *const c_void,
            core::mem::size_of::<Oid>() * ndicts as usize,
        );
    }
    (*command).d.atscfg.ndicts = ndicts;
    (*command).parsetree = copyObject(stmt as *mut c_void) as *mut Node;

    (*currentEventTriggerState).commandList =
        lappend((*currentEventTriggerState).commandList, command as *mut c_void);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerCollectAlterDefPrivs
 *		Save data about an ALTER DEFAULT PRIVILEGES command being
 *		executed
 */
pub unsafe fn EventTriggerCollectAlterDefPrivs(
    stmt: *mut crate::nodes::parsenodes::AlterDefaultPrivilegesStmt,
) {
    let oldcxt: MemoryContext;
    let command: *mut CollectedCommand;

    /* ignore if event trigger context not set, or collection disabled */
    if currentEventTriggerState.is_null()
        || (*currentEventTriggerState).commandCollectionInhibited
    {
        return;
    }

    oldcxt = MemoryContextSwitchTo((*currentEventTriggerState).cxt);

    command = palloc0(core::mem::size_of::<CollectedCommand>()) as *mut CollectedCommand;
    (*command).type_ = SCT_AlterDefaultPrivileges;
    (*command).d.defprivs.objtype = (*(*stmt).action).objtype;
    (*command).in_extension = creating_extension;
    (*command).parsetree = copyObject(stmt as *mut c_void) as *mut Node;

    (*currentEventTriggerState).commandList =
        lappend((*currentEventTriggerState).commandList, command as *mut c_void);
    MemoryContextSwitchTo(oldcxt);
}

/*
 * In a ddl_command_end event trigger, this function reports the DDL commands
 * being run.
 */
pub unsafe fn pg_event_trigger_ddl_commands(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut lc: *mut crate::nodes::pg_list::ListCell;

    /*
     * Protect this function from being called out of context
     */
    if currentEventTriggerState.is_null() {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED) */
            errmsg!(
                "{} can only be called in an event trigger function",
                "pg_event_trigger_ddl_commands()"
            )
        );
    }

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    lc = list_head((*currentEventTriggerState).commandList);
    while !lc.is_null() {
        let cmd: *mut CollectedCommand = lfirst(lc) as *mut CollectedCommand;
        let mut values: [Datum; 9] = [0; 9];
        let mut nulls: [bool; 9] = [false; 9];
        let mut addr: ObjectAddress = core::mem::zeroed();
        let mut i: c_int = 0;

        /*
         * For IF NOT EXISTS commands that attempt to create an existing
         * object, the returned OID is Invalid.  Don't return anything.
         *
         * One might think that a viable alternative would be to look up the
         * Oid of the existing object and run the deparse with that.  But
         * since the parse tree might be different from the one that created
         * the object in the first place, we might not end up in a consistent
         * state anyway.
         */
        if (*cmd).type_ == SCT_Simple && !OidIsValid((*cmd).d.simple.address.objectId) {
            lc = lnext((*currentEventTriggerState).commandList, lc);
            continue;
        }

        match (*cmd).type_ {
            SCT_Simple | SCT_AlterTable | SCT_AlterOpFamily | SCT_CreateOpClass
            | SCT_AlterTSConfig => {
                let identity: *mut c_char;
                let r#type: *const c_char;
                let mut schema: *mut c_char = core::ptr::null_mut();

                if (*cmd).type_ == SCT_Simple {
                    addr = (*cmd).d.simple.address;
                } else if (*cmd).type_ == SCT_AlterTable {
                    ObjectAddressSet(
                        &mut addr,
                        (*cmd).d.alterTable.classId,
                        (*cmd).d.alterTable.objectId,
                    );
                } else if (*cmd).type_ == SCT_AlterOpFamily {
                    addr = (*cmd).d.opfam.address;
                } else if (*cmd).type_ == SCT_CreateOpClass {
                    addr = (*cmd).d.createopc.address;
                } else if (*cmd).type_ == SCT_AlterTSConfig {
                    addr = (*cmd).d.atscfg.address;
                }

                /*
                 * If an object was dropped in the same command we may end
                 * up in a situation where we generated a message but can
                 * no longer look for the object information, so skip it
                 * rather than failing.  This can happen for example with
                 * some subcommand combinations of ALTER TABLE.
                 */
                identity = getObjectIdentity(&addr, true);
                if identity.is_null() {
                    lc = lnext((*currentEventTriggerState).commandList, lc);
                    continue;
                }

                /* The type can never be NULL. */
                r#type = getObjectTypeDescription(&addr, true);

                /*
                 * Obtain schema name, if any ("pg_temp" if a temp
                 * object). If the object class is not in the supported
                 * list here, we assume it's a schema-less object type,
                 * and thus "schema" remains set to NULL.
                 */
                if is_objectclass_supported(addr.classId) {
                    let nspAttnum: AttrNumber;

                    nspAttnum = get_object_attnum_namespace(addr.classId);
                    if nspAttnum != InvalidAttrNumber {
                        let catalog: Relation;
                        let objtup: HeapTuple;
                        let schema_oid: Oid;
                        let mut isnull: bool = false;

                        catalog = table_open(addr.classId, AccessShareLock);
                        objtup = get_catalog_object_by_oid(
                            catalog,
                            get_object_attnum_oid(addr.classId),
                            addr.objectId,
                        );
                        if !HeapTupleIsValid(objtup) {
                            elog!(
                                ERROR,
                                "cache lookup failed for object {}/{}",
                                addr.classId,
                                addr.objectId
                            );
                        }
                        schema_oid = heap_getattr(
                            objtup,
                            nspAttnum,
                            RelationGetDescr(catalog),
                            &mut isnull,
                        ) as Oid;
                        if isnull {
                            elog!(
                                ERROR,
                                "invalid null namespace in object {}/{}/{}",
                                addr.classId,
                                addr.objectId,
                                addr.objectSubId
                            );
                        }
                        schema = get_namespace_name_or_temp(schema_oid);

                        table_close(catalog, AccessShareLock);
                    }
                }

                /* classid */
                values[i as usize] = ObjectIdGetDatum(addr.classId);
                i += 1;
                /* objid */
                values[i as usize] = ObjectIdGetDatum(addr.objectId);
                i += 1;
                /* objsubid */
                values[i as usize] = Int32GetDatum(addr.objectSubId);
                i += 1;
                /* command tag */
                values[i as usize] = CStringGetTextDatum(CreateCommandName((*cmd).parsetree));
                i += 1;
                /* object_type */
                values[i as usize] = CStringGetTextDatum(r#type);
                i += 1;
                /* schema */
                if schema.is_null() {
                    nulls[i as usize] = true;
                    i += 1;
                } else {
                    values[i as usize] = CStringGetTextDatum(schema);
                    i += 1;
                }
                /* identity */
                values[i as usize] = CStringGetTextDatum(identity);
                i += 1;
                /* in_extension */
                values[i as usize] = BoolGetDatum((*cmd).in_extension);
                i += 1;
                /* command */
                values[i as usize] = PointerGetDatum(cmd as *const c_void);
                i += 1;
            }

            SCT_AlterDefaultPrivileges => {
                /* classid */
                nulls[i as usize] = true;
                i += 1;
                /* objid */
                nulls[i as usize] = true;
                i += 1;
                /* objsubid */
                nulls[i as usize] = true;
                i += 1;
                /* command tag */
                values[i as usize] = CStringGetTextDatum(CreateCommandName((*cmd).parsetree));
                i += 1;
                /* object_type */
                values[i as usize] =
                    CStringGetTextDatum(stringify_adefprivs_objtype((*cmd).d.defprivs.objtype));
                i += 1;
                /* schema */
                nulls[i as usize] = true;
                i += 1;
                /* identity */
                nulls[i as usize] = true;
                i += 1;
                /* in_extension */
                values[i as usize] = BoolGetDatum((*cmd).in_extension);
                i += 1;
                /* command */
                values[i as usize] = PointerGetDatum(cmd as *const c_void);
                i += 1;
            }

            SCT_Grant => {
                /* classid */
                nulls[i as usize] = true;
                i += 1;
                /* objid */
                nulls[i as usize] = true;
                i += 1;
                /* objsubid */
                nulls[i as usize] = true;
                i += 1;
                /* command tag */
                values[i as usize] = CStringGetTextDatum(if (*(*cmd).d.grant.istmt).is_grant {
                    c"GRANT".as_ptr()
                } else {
                    c"REVOKE".as_ptr()
                });
                i += 1;
                /* object_type */
                values[i as usize] =
                    CStringGetTextDatum(stringify_grant_objtype((*(*cmd).d.grant.istmt).objtype));
                i += 1;
                /* schema */
                nulls[i as usize] = true;
                i += 1;
                /* identity */
                nulls[i as usize] = true;
                i += 1;
                /* in_extension */
                values[i as usize] = BoolGetDatum((*cmd).in_extension);
                i += 1;
                /* command */
                values[i as usize] = PointerGetDatum(cmd as *const c_void);
                i += 1;
            }

            _ => {}
        }

        let _ = i;

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        lc = lnext((*currentEventTriggerState).commandList, lc);
    }

    crate::PG_RETURN_VOID!();
}

/*
 * Return the ObjectType as a string, as it would appear in GRANT and
 * REVOKE commands.
 */
unsafe fn stringify_grant_objtype(objtype: ObjectType) -> *const c_char {
    match objtype {
        ObjectType::OBJECT_COLUMN => return c"COLUMN".as_ptr(),
        ObjectType::OBJECT_TABLE => return c"TABLE".as_ptr(),
        ObjectType::OBJECT_SEQUENCE => return c"SEQUENCE".as_ptr(),
        ObjectType::OBJECT_DATABASE => return c"DATABASE".as_ptr(),
        ObjectType::OBJECT_DOMAIN => return c"DOMAIN".as_ptr(),
        ObjectType::OBJECT_FDW => return c"FOREIGN DATA WRAPPER".as_ptr(),
        ObjectType::OBJECT_FOREIGN_SERVER => return c"FOREIGN SERVER".as_ptr(),
        ObjectType::OBJECT_FUNCTION => return c"FUNCTION".as_ptr(),
        ObjectType::OBJECT_LANGUAGE => return c"LANGUAGE".as_ptr(),
        ObjectType::OBJECT_LARGEOBJECT => return c"LARGE OBJECT".as_ptr(),
        ObjectType::OBJECT_SCHEMA => return c"SCHEMA".as_ptr(),
        ObjectType::OBJECT_PARAMETER_ACL => return c"PARAMETER".as_ptr(),
        ObjectType::OBJECT_PROCEDURE => return c"PROCEDURE".as_ptr(),
        ObjectType::OBJECT_ROUTINE => return c"ROUTINE".as_ptr(),
        ObjectType::OBJECT_TABLESPACE => return c"TABLESPACE".as_ptr(),
        ObjectType::OBJECT_TYPE => return c"TYPE".as_ptr(),
        /* these currently aren't used */
        ObjectType::OBJECT_ACCESS_METHOD
        | ObjectType::OBJECT_AGGREGATE
        | ObjectType::OBJECT_AMOP
        | ObjectType::OBJECT_AMPROC
        | ObjectType::OBJECT_ATTRIBUTE
        | ObjectType::OBJECT_CAST
        | ObjectType::OBJECT_COLLATION
        | ObjectType::OBJECT_CONVERSION
        | ObjectType::OBJECT_DEFAULT
        | ObjectType::OBJECT_DEFACL
        | ObjectType::OBJECT_DOMCONSTRAINT
        | ObjectType::OBJECT_EVENT_TRIGGER
        | ObjectType::OBJECT_EXTENSION
        | ObjectType::OBJECT_FOREIGN_TABLE
        | ObjectType::OBJECT_INDEX
        | ObjectType::OBJECT_MATVIEW
        | ObjectType::OBJECT_OPCLASS
        | ObjectType::OBJECT_OPERATOR
        | ObjectType::OBJECT_OPFAMILY
        | ObjectType::OBJECT_POLICY
        | ObjectType::OBJECT_PUBLICATION
        | ObjectType::OBJECT_PUBLICATION_NAMESPACE
        | ObjectType::OBJECT_PUBLICATION_REL
        | ObjectType::OBJECT_ROLE
        | ObjectType::OBJECT_RULE
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_SUBSCRIPTION
        | ObjectType::OBJECT_TABCONSTRAINT
        | ObjectType::OBJECT_TRANSFORM
        | ObjectType::OBJECT_TRIGGER
        | ObjectType::OBJECT_TSCONFIGURATION
        | ObjectType::OBJECT_TSDICTIONARY
        | ObjectType::OBJECT_TSPARSER
        | ObjectType::OBJECT_TSTEMPLATE
        | ObjectType::OBJECT_USER_MAPPING
        | ObjectType::OBJECT_VIEW => {
            elog!(ERROR, "unsupported object type: {}", objtype as c_int);
        }
        #[allow(unreachable_patterns)]
        _ => {}
    }

    c"???".as_ptr() /* keep compiler quiet */
}

/*
 * Return the ObjectType as a string; as above, but use the spelling
 * in ALTER DEFAULT PRIVILEGES commands instead.  Generally this is just
 * the plural.
 */
unsafe fn stringify_adefprivs_objtype(objtype: ObjectType) -> *const c_char {
    match objtype {
        ObjectType::OBJECT_COLUMN => return c"COLUMNS".as_ptr(),
        ObjectType::OBJECT_TABLE => return c"TABLES".as_ptr(),
        ObjectType::OBJECT_SEQUENCE => return c"SEQUENCES".as_ptr(),
        ObjectType::OBJECT_DATABASE => return c"DATABASES".as_ptr(),
        ObjectType::OBJECT_DOMAIN => return c"DOMAINS".as_ptr(),
        ObjectType::OBJECT_FDW => return c"FOREIGN DATA WRAPPERS".as_ptr(),
        ObjectType::OBJECT_FOREIGN_SERVER => return c"FOREIGN SERVERS".as_ptr(),
        ObjectType::OBJECT_FUNCTION => return c"FUNCTIONS".as_ptr(),
        ObjectType::OBJECT_LANGUAGE => return c"LANGUAGES".as_ptr(),
        ObjectType::OBJECT_LARGEOBJECT => return c"LARGE OBJECTS".as_ptr(),
        ObjectType::OBJECT_SCHEMA => return c"SCHEMAS".as_ptr(),
        ObjectType::OBJECT_PROCEDURE => return c"PROCEDURES".as_ptr(),
        ObjectType::OBJECT_ROUTINE => return c"ROUTINES".as_ptr(),
        ObjectType::OBJECT_TABLESPACE => return c"TABLESPACES".as_ptr(),
        ObjectType::OBJECT_TYPE => return c"TYPES".as_ptr(),
        /* these currently aren't used */
        ObjectType::OBJECT_ACCESS_METHOD
        | ObjectType::OBJECT_AGGREGATE
        | ObjectType::OBJECT_AMOP
        | ObjectType::OBJECT_AMPROC
        | ObjectType::OBJECT_ATTRIBUTE
        | ObjectType::OBJECT_CAST
        | ObjectType::OBJECT_COLLATION
        | ObjectType::OBJECT_CONVERSION
        | ObjectType::OBJECT_DEFAULT
        | ObjectType::OBJECT_DEFACL
        | ObjectType::OBJECT_DOMCONSTRAINT
        | ObjectType::OBJECT_EVENT_TRIGGER
        | ObjectType::OBJECT_EXTENSION
        | ObjectType::OBJECT_FOREIGN_TABLE
        | ObjectType::OBJECT_INDEX
        | ObjectType::OBJECT_MATVIEW
        | ObjectType::OBJECT_OPCLASS
        | ObjectType::OBJECT_OPERATOR
        | ObjectType::OBJECT_OPFAMILY
        | ObjectType::OBJECT_PARAMETER_ACL
        | ObjectType::OBJECT_POLICY
        | ObjectType::OBJECT_PUBLICATION
        | ObjectType::OBJECT_PUBLICATION_NAMESPACE
        | ObjectType::OBJECT_PUBLICATION_REL
        | ObjectType::OBJECT_ROLE
        | ObjectType::OBJECT_RULE
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_SUBSCRIPTION
        | ObjectType::OBJECT_TABCONSTRAINT
        | ObjectType::OBJECT_TRANSFORM
        | ObjectType::OBJECT_TRIGGER
        | ObjectType::OBJECT_TSCONFIGURATION
        | ObjectType::OBJECT_TSDICTIONARY
        | ObjectType::OBJECT_TSPARSER
        | ObjectType::OBJECT_TSTEMPLATE
        | ObjectType::OBJECT_USER_MAPPING
        | ObjectType::OBJECT_VIEW => {
            elog!(ERROR, "unsupported object type: {}", objtype as c_int);
        }
        #[allow(unreachable_patterns)]
        _ => {}
    }

    c"???".as_ptr() /* keep compiler quiet */
}
