/*-------------------------------------------------------------------------
 *
 * typecmds.rs
 *   Routines for SQL commands that manipulate types (and domains).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/backend/commands/typecmds.c
 *
 * DESCRIPTION
 *   The "DefineFoo" routines take the parse tree and pick out the
 *   appropriate arguments/flags, passing the results to the
 *   corresponding "FooCreate" routines (in src/backend/catalog) that do
 *   the actual catalog-munging.  These routines also verify permission
 *   of the user to execute the command.
 *
 * NOTES
 *   These things must be defined and committed in the following order:
 *     "create function":
 *             input/output, recv/send functions
 *     "create type":
 *             type
 *     "create operator":
 *             operators
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{foreach, current_cell, lfirst_node, IsA, makeNode};

use std::ffi::{c_char, c_int};

use crate::nodes::pg_list::{List, ListCell};
use crate::nodes::parsenodes::{
    AlterEnumStmt, AlterTypeStmt, CreateDomainStmt, CreateEnumStmt, CreateRangeStmt,
    Constraint, ColumnRef, CreateStmt, DefElem, DropBehavior, ObjectType,
    RenameStmt, TypeName, ConstrType, ConstrType::*,
};
use crate::nodes::primnodes::RangeVar;
use crate::catalog::objectaccess::ObjectAddress;
use crate::parser::parse_node::ParseState;

/* --------------------------------------------------------------------------
 * Local type stubs for unported dependencies
 * -------------------------------------------------------------------------- */

// HeapTuple is a single pointer (HeapTupleData*).
use crate::access::htup_details::HeapTupleData;
type HeapTuple = *mut HeapTupleData;

// Relation pointer
type RelationData = crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// SysScanDesc / ScanKeyData stubs  TODO(pg-port)
#[repr(C)] pub struct SysScanDescData { _opaque: [u8; 0] }
type SysScanDesc = *mut SysScanDescData;
#[repr(C)] pub struct ScanKeyDataStruct { _opaque: [u8; 64] }
type ScanKeyData = ScanKeyDataStruct;

// TupleDesc
use crate::access::common::tupdesc::{TupleDescData, TupleDesc, TupleDescAttr as TupleDescAttr_real};

// Form_pg_type / Form_pg_constraint / Form_pg_attribute / Form_pg_depend  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_type { _opaque: [u8; 0] }
type Form_pg_type = *mut FormData_pg_type;
#[repr(C)] pub struct FormData_pg_constraint { _opaque: [u8; 0] }
type Form_pg_constraint = *mut FormData_pg_constraint;
#[repr(C)] pub struct FormData_pg_attribute { _opaque: [u8; 0] }
type Form_pg_attribute = *mut FormData_pg_attribute;
#[repr(C)] pub struct FormData_pg_depend { _opaque: [u8; 0] }
type Form_pg_depend = *mut FormData_pg_depend;

// EState / ExprContext / ExprState / Expr / TupleTableSlot / TableScanDesc  TODO(pg-port)
#[repr(C)] pub struct EState { _opaque: [u8; 0] }
use crate::nodes::execnodes::ExprContext;
#[repr(C)] pub struct ExprState { _opaque: [u8; 0] }
#[repr(C)] pub struct ExprData { _opaque: [u8; 0] }
type Expr = ExprData;
#[repr(C)] pub struct TupleTableSlotData { _opaque: [u8; 0] }
type TupleTableSlot = *mut TupleTableSlotData;
#[repr(C)] pub struct TableScanDescData { _opaque: [u8; 0] }
type TableScanDesc = *mut TableScanDescData;

// Node / Snapshot  TODO(pg-port)
use crate::nodes::nodes::Node;
#[repr(C)] pub struct SnapshotData { _opaque: [u8; 0] }
type Snapshot = *mut SnapshotData;

// CoerceToDomainValue  TODO(pg-port)
#[repr(C)] pub struct CoerceToDomainValue {
    pub r#type: i32,
    pub typeId: Oid,
    pub typeMod: int32,
    pub collation: Oid,
    pub location: c_int,
}

// ObjectAddresses  TODO(pg-port)
#[repr(C)] pub struct ObjectAddresses { _opaque: [u8; 0] }

// AclResult  TODO(pg-port)
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;

// regproc = Oid in this simplified port
type regproc = Oid;

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const NoLock: LOCKMODE = 0;
const ShareLock: LOCKMODE = 1;
const RowExclusiveLock: LOCKMODE = 3;
const AccessShareLock: LOCKMODE = 1;
const AccessExclusiveLock: LOCKMODE = 8;

// Acl  TODO(pg-port)
#[repr(C)] pub struct AclData { _opaque: [u8; 0] }
type Acl = AclData;

// oidvector  TODO(pg-port)
use crate::c::oidvector;
use crate::utils::array::ArrayType;

// CONSTR_* variants are imported via `use crate::nodes::parsenodes::ConstrType::*` above

// pg_type typtype constants  TODO(pg-port)
const TYPTYPE_BASE: c_char = b'b' as c_char;
const TYPTYPE_COMPOSITE: c_char = b'c' as c_char;
const TYPTYPE_DOMAIN: c_char = b'd' as c_char;
const TYPTYPE_ENUM: c_char = b'e' as c_char;
const TYPTYPE_PSEUDO: c_char = b'p' as c_char;
const TYPTYPE_RANGE: c_char = b'r' as c_char;
const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char;

// typcategory constants  TODO(pg-port)
const TYPCATEGORY_USER: c_char = b'U' as c_char;
const TYPCATEGORY_ARRAY: c_char = b'A' as c_char;
const TYPCATEGORY_ENUM: c_char = b'E' as c_char;
const TYPCATEGORY_RANGE: c_char = b'R' as c_char;

// typalign constants  TODO(pg-port)
const TYPALIGN_CHAR: c_char = b'c' as c_char;
const TYPALIGN_SHORT: c_char = b's' as c_char;
const TYPALIGN_INT: c_char = b'i' as c_char;
const TYPALIGN_DOUBLE: c_char = b'd' as c_char;

// typstorage constants  TODO(pg-port)
const TYPSTORAGE_PLAIN: c_char = b'p' as c_char;
const TYPSTORAGE_EXTERNAL: c_char = b'e' as c_char;
const TYPSTORAGE_EXTENDED: c_char = b'x' as c_char;
const TYPSTORAGE_MAIN: c_char = b'm' as c_char;

// catalog OIDs  TODO(pg-port)
const TypeRelationId: Oid = 1247;
const NamespaceRelationId: Oid = 2615;
const ProcedureRelationId: Oid = 1255;
const ConstraintRelationId: Oid = 2606;
const DependRelationId: Oid = 2608;
const RelationRelationId: Oid = 1259;

// OID-index constants  TODO(pg-port)
const TypeOidIndexId: Oid = 2703;

// Built-in function OIDs  TODO(pg-port)
const F_ARRAY_IN: Oid = 750;
const F_ARRAY_OUT: Oid = 751;
const F_ARRAY_RECV: Oid = 2400;
const F_ARRAY_SEND: Oid = 2401;
const F_ARRAY_TYPANALYZE: Oid = 2767;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6204;
const F_RAW_ARRAY_SUBSCRIPT_HANDLER: Oid = 6205;
const F_ENUM_IN: Oid = 3504;
const F_ENUM_OUT: Oid = 3505;
const F_ENUM_RECV: Oid = 3506;
const F_ENUM_SEND: Oid = 3507;
const F_RANGE_IN: Oid = 3834;
const F_RANGE_OUT: Oid = 3835;
const F_RANGE_RECV: Oid = 3836;
const F_RANGE_SEND: Oid = 3837;
const F_RANGE_TYPANALYZE: Oid = 3908;
const F_MULTIRANGE_IN: Oid = 4074;
const F_MULTIRANGE_OUT: Oid = 4075;
const F_MULTIRANGE_RECV: Oid = 4076;
const F_MULTIRANGE_SEND: Oid = 4077;
const F_MULTIRANGE_TYPANALYZE: Oid = 4078;
const F_DOMAIN_IN: Oid = 2116;
const F_DOMAIN_RECV: Oid = 2117;
const F_FMGR_INTERNAL_VALIDATOR: Oid = 2246;

// misc constants  TODO(pg-port)
const BTREE_AM_OID: Oid = 403;
const DEFAULT_TYPDELIM: c_char = b',' as c_char;
const DEFAULT_COLLATION_OID: Oid = 100;
const OIDOID: Oid = 26;
const INT4OID: Oid = 23;
const CSTRINGOID: Oid = 2275;
const INTERNALOID: Oid = 2281;
const BYTEAOID: Oid = 17;
const BOOLOID: Oid = 16;
const FLOAT8OID: Oid = 701;
const TEXTOID: Oid = 25;
const CHAROID: Oid = 18;
const CSTRINGARRAYOID: Oid = 1263;

// Anum_pg_type_* column numbers  TODO(pg-port)
const Anum_pg_type_oid: c_int = 1;
const Anum_pg_type_typdefault: c_int = 42;
const Anum_pg_type_typdefaultbin: c_int = 43;
const Anum_pg_type_typowner: c_int = 3;
const Anum_pg_type_typacl: c_int = 44;
const Anum_pg_type_typstorage: c_int = 17;
const Anum_pg_type_typreceive: c_int = 20;
const Anum_pg_type_typsend: c_int = 21;
const Anum_pg_type_typmodin: c_int = 22;
const Anum_pg_type_typmodout: c_int = 23;
const Anum_pg_type_typanalyze: c_int = 24;
const Anum_pg_type_typsubscript: c_int = 31;
const Anum_pg_type_typbasetype: c_int = 34;
const Natts_pg_type: usize = 44;

// Anum_pg_constraint_*  TODO(pg-port)
const Anum_pg_constraint_conrelid: c_int = 2;
const Anum_pg_constraint_contypid: c_int = 3;
const Anum_pg_constraint_conname: c_int = 1;
const Anum_pg_constraint_conbin: c_int = 12;

// Anum_pg_depend_*  TODO(pg-port)
const Anum_pg_depend_refclassid: c_int = 4;
const Anum_pg_depend_refobjid: c_int = 5;

// ScanStrategy  TODO(pg-port)
const BTEqualStrategyNumber: c_int = 3;

// Index OIDs  TODO(pg-port)
const ConstraintRelidTypidNameIndexId: Oid = 2664;
const DependReferenceIndexId: Oid = 2643;

// prokind  TODO(pg-port)
const PROKIND_FUNCTION: c_char = b'f' as c_char;
const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;
const PROVOLATILE_VOLATILE: c_char = b'v' as c_char;
const PROPARALLEL_SAFE: c_char = b's' as c_char;

// relkind  TODO(pg-port)
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_MATVIEW: c_char = b'm' as c_char;
const RELKIND_COMPOSITE_TYPE: c_char = b'c' as c_char;

// DEPENDENCY_INTERNAL  TODO(pg-port)
const DEPENDENCY_INTERNAL: c_char = b'i' as c_char;

// COERCION  TODO(pg-port)
const COERCION_CODE_EXPLICIT: c_char = b'e' as c_char;
const COERCION_METHOD_FUNCTION: c_char = b'f' as c_char;

// FUNC_PARAM_VARIADIC  TODO(pg-port)
const FUNC_PARAM_VARIADIC: c_char = b'v' as c_char;

// CONSTRAINT_* pg_constraint contype values  TODO(pg-port)
const CONSTRAINT_CHECK: c_char = b'c' as c_char;
const CONSTRAINT_NOTNULL: c_char = b'n' as c_char;
const CONSTRAINT_DOMAIN: c_char = b'd' as c_char; /* actually 'n' in older PG, but for ConstraintNameIsUsed */

use crate::nodes::primnodes::OnCommitAction;
use crate::nodes::primnodes::OnCommitAction::ONCOMMIT_NOOP;

// language  TODO(pg-port)
const INTERNALlanguageId: Oid = 12;
const BOOTSTRAP_SUPERUSERID: Oid = 10;

// ForwardScanDirection  TODO(pg-port)
const ForwardScanDirection: c_int = 1;

// OBJECT_*  TODO(pg-port)
const OBJECT_SCHEMA: c_int = 37;
const OBJECT_FUNCTION: c_int = 18;
const OBJECT_DOMAIN: c_int = 12;

// EXPR_KIND_DOMAIN_CHECK  TODO(pg-port)
const EXPR_KIND_DOMAIN_CHECK: c_int = 17;

// ACL permissions  TODO(pg-port)
const ACL_CREATE: c_uint = 1 << 3;
const ACL_USAGE: c_uint = 1 << 10;
const ACL_EXECUTE: c_uint = 1 << 5;

// Drop behavior  TODO(pg-port)
const DROP_RESTRICT: c_int = 0;

/* Potentially set by pg_upgrade_support functions */
pub static mut binary_upgrade_next_array_pg_type_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_mrng_pg_type_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_mrng_array_pg_type_oid: Oid = InvalidOid;

/* --------------------------------------------------------------------------
 * result structure for get_rels_with_domain()
 * -------------------------------------------------------------------------- */
struct RelToCheck {
    rel: Relation,   /* opened and locked relation */
    natts: c_int,    /* number of attributes of interest */
    atts: *mut c_int, /* attribute numbers */
    /* atts[] is of allocated length RelationGetNumberOfAttributes(rel) */
}

/* --------------------------------------------------------------------------
 * parameter structure for AlterTypeRecurse()
 * -------------------------------------------------------------------------- */
struct AlterTypeRecurseParams {
    /* Flags indicating which type attributes to update */
    updateStorage: bool,
    updateReceive: bool,
    updateSend: bool,
    updateTypmodin: bool,
    updateTypmodout: bool,
    updateAnalyze: bool,
    updateSubscript: bool,
    /* New values for relevant attributes */
    storage: c_char,
    receiveOid: Oid,
    sendOid: Oid,
    typmodinOid: Oid,
    typmodoutOid: Oid,
    analyzeOid: Oid,
    subscriptOid: Oid,
}

/* --------------------------------------------------------------------------
 * Stub implementations for unported dependencies  TODO(pg-port)
 * -------------------------------------------------------------------------- */

unsafe fn QualifiedNameGetCreationNamespace(
    names: *mut List, typeName: *mut *mut c_char,
) -> Oid { crate::catalog::namespace::QualifiedNameGetCreationNamespace(names as _, typeName as _) as _ }

unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn superuser() -> bool { crate::utils::misc::superuser::superuser() }
unsafe fn IsBinaryUpgrade() -> bool { crate::utils::init::globals::IsBinaryUpgrade }

unsafe fn object_aclcheck(
    classId: Oid, objectId: Oid, userId: Oid, acl: c_uint,
) -> AclResult { crate::catalog::aclchk::object_aclcheck(classId as _, objectId as _, userId as _, acl as _) as _ }
unsafe fn aclcheck_error(res: AclResult, objtype: c_int, name: *const c_char) {
    crate::catalog::aclchk::aclcheck_error(core::mem::transmute(res as i32), core::mem::transmute(objtype), name as _)
}
unsafe fn aclcheck_error_type(res: AclResult, typeOid: Oid) {
    crate::catalog::aclchk::aclcheck_error_type(core::mem::transmute(res as i32), typeOid as _)
}

unsafe fn object_ownercheck(classId: Oid, objectId: Oid, userId: Oid) -> bool {
    crate::catalog::aclchk::object_ownercheck(classId as _, objectId as _, userId as _)
}
unsafe fn check_can_set_role(member: Oid, role: Oid) {
    crate::utils::adt::acl::check_can_set_role(member as _, role as _)
}

unsafe fn GetSysCacheOid2(cacheId: c_int, oidAttNum: c_int, key1: Datum, key2: Datum) -> Oid {
    crate::utils::cache::lsyscache::GetSysCacheOid2(cacheId, oidAttNum as _, key1, key2) as _
}
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(cacheId, key1) as _
}
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCacheCopy(cacheId, key1, 0, 0, 0) as _
}
unsafe fn SysCacheGetAttr(
    cacheId: c_int, tup: HeapTuple, attNum: c_int, isnull: *mut bool,
) -> Datum { crate::utils::cache::syscache::SysCacheGetAttr(cacheId, tup as _, attNum as _, isnull) }
unsafe fn SysCacheGetAttrNotNull(cacheId: c_int, tup: HeapTuple, attNum: c_int) -> Datum {
    crate::utils::cache::syscache::SysCacheGetAttrNotNull(cacheId, tup as _, attNum as _)
}
unsafe fn SearchSysCacheExists2(cacheId: c_int, key1: Datum, key2: Datum) -> bool {
    crate::utils::cache::syscache::SearchSysCacheExists(cacheId, key1, key2, 0, 0)
}
unsafe fn ReleaseSysCache(tup: HeapTuple) { crate::utils::cache::syscache::ReleaseSysCache(tup as _) }

unsafe fn table_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    crate::access::table::table::table_open(relid as _, lockmode as _) as _
}
unsafe fn table_close(rel: Relation, lockmode: LOCKMODE) {
    crate::access::table::table::table_close(rel as _, lockmode as _)
}
unsafe fn relation_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    crate::access::common::relation::relation_open(relid as _, lockmode as _) as _
}
unsafe fn relation_close(rel: Relation, lockmode: LOCKMODE) {
    crate::access::common::relation::relation_close(rel as _, lockmode as _)
}
unsafe fn table_beginscan(rel: Relation, snap: Snapshot, nkeys: c_int, keys: *const ScanKeyData)
    -> TableScanDesc {
    crate::access::table::tableam::table_beginscan_strat(rel as _, snap as _, nkeys, keys as _, true, true) as _
}
unsafe fn table_endscan(scan: TableScanDesc) {
    crate::access::table::tableam::table_endscan(scan as _)
}
unsafe fn table_scan_getnextslot(scan: TableScanDesc, dir: c_int, slot: TupleTableSlot) -> bool {
    crate::access::table::tableam::table_scan_getnextslot(scan as _, core::mem::transmute(dir), slot as _)
}
unsafe fn table_slot_create(rel: Relation, slots: *mut *mut c_void) -> TupleTableSlot {
    crate::access::table::tableam::table_slot_create(rel as _, slots as _) as _
}
unsafe fn ExecDropSingleTupleTableSlot(slot: TupleTableSlot) {
    crate::executor::execTuples::ExecDropSingleTupleTableSlot(slot as _)
}

unsafe fn CatalogTupleUpdate(rel: Relation, otid: *mut ItemPointerData, tup: HeapTuple) {
    crate::catalog::indexing::CatalogTupleUpdate(rel as _, otid as _, tup as _)
}
unsafe fn CatalogTupleDelete(rel: Relation, otid: *mut ItemPointerData) {
    crate::catalog::indexing::CatalogTupleDelete(rel as _, otid as _)
}
unsafe fn heap_modify_tuple(
    tup: HeapTuple, desc: TupleDesc,
    values: *const Datum, nulls: *const bool, replaces: *const bool,
) -> HeapTuple {
    crate::access::common::heaptuple::heap_modify_tuple(tup as _, desc as _, values, nulls, replaces) as _
}
unsafe fn heap_copytuple(tup: HeapTuple) -> HeapTuple {
    crate::access::common::heaptuple::heap_copytuple(tup as _) as _
}
unsafe fn heap_freetuple(tup: HeapTuple) {
    crate::access::common::heaptuple::heap_freetuple(tup as _)
}
unsafe fn heap_getattr(tup: HeapTuple, attnum: c_int, desc: TupleDesc, isnull: *mut bool) -> Datum {
    crate::access::htup_details::heap_getattr(tup as _, attnum, desc as _, isnull)
}

// ItemPointerData from storage::itemptr
use crate::storage::itemptr::ItemPointerData;

unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool { !tup.is_null() }

unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void { crate::access::htup_details::GETSTRUCT(tup as _) }
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc { crate::utils::rel::RelationGetDescr(rel as _) as _ }
unsafe fn RelationGetRelid(rel: Relation) -> Oid { crate::utils::rel::RelationGetRelid(rel as _) as _ }
unsafe fn RelationGetNumberOfAttributes(rel: Relation) -> c_int { crate::utils::rel::RelationGetNumberOfAttributes(rel as _) }
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char { crate::utils::rel::RelationGetRelationName(rel as _) as _ }
unsafe fn TupleDescAttr(desc: TupleDesc, attno: c_int) -> Form_pg_attribute { TupleDescAttr_real(desc, attno) as *mut _ }

unsafe fn TypeCreate(
    newTypeOid: Oid, typeName: *const c_char, typeNamespace: Oid,
    relationOid: Oid, relationKind: c_int, ownerId: Oid, internalSize: int16,
    typeType: c_char, typeCategory: c_char, typePreferred: bool, typDelim: c_char,
    inputProcedure: Oid, outputProcedure: Oid, receiveProcedure: Oid, sendProcedure: Oid,
    typmodinProcedure: Oid, typmodoutProcedure: Oid, analyzeProcedure: Oid,
    subscriptProcedure: Oid, elementType: Oid, isImplicitArray: bool, arrayType: Oid,
    baseType: Oid, defaultTypeValue: *const c_char, defaultTypeBin: *const c_char,
    passedByValue: bool, alignment: c_char, storage: c_char, typeMod: int32,
    typNDims: int32, typeNotNull: bool, typeCollation: Oid,
) -> ObjectAddress {
    core::mem::transmute(crate::catalog::pg_type::TypeCreate(
        newTypeOid as _, typeName as _, typeNamespace as _, relationOid as _, relationKind as _,
        ownerId as _, internalSize, typeType, typeCategory, typePreferred, typDelim,
        inputProcedure as _, outputProcedure as _, receiveProcedure as _, sendProcedure as _,
        typmodinProcedure as _, typmodoutProcedure as _, analyzeProcedure as _, subscriptProcedure as _,
        elementType as _, isImplicitArray, arrayType as _, baseType as _, defaultTypeValue as _,
        defaultTypeBin as _, passedByValue, alignment, storage, typeMod, typNDims, typeNotNull,
        typeCollation as _,
    ))
}

unsafe fn TypeShellMake(typeName: *const c_char, typeNamespace: Oid, ownerId: Oid)
    -> ObjectAddress { core::mem::transmute(crate::catalog::pg_type::TypeShellMake(typeName as _, typeNamespace as _, ownerId as _)) }

unsafe fn moveArrayTypeName(typeOid: Oid, typeName: *const c_char, typeNamespace: Oid) -> bool {
    crate::catalog::pg_type::moveArrayTypeName(typeOid as _, typeName as _, typeNamespace as _)
}
unsafe fn makeArrayTypeName(typeName: *const c_char, typeNamespace: Oid) -> *mut c_char {
    crate::catalog::pg_type::makeArrayTypeName(typeName as _, typeNamespace as _) as _
}
unsafe fn makeMultirangeTypeName(rangeTypeName: *const c_char, typeNamespace: Oid)
    -> *mut c_char { crate::catalog::pg_type::makeMultirangeTypeName(rangeTypeName as _, typeNamespace as _) as _ }

unsafe fn get_typisdefined(typid: Oid) -> bool { crate::utils::cache::lsyscache::get_typisdefined(typid as _) }
unsafe fn get_typtype(typid: Oid) -> c_char { crate::utils::cache::lsyscache::get_typtype(typid as _) }
unsafe fn get_typlen(typid: Oid) -> int16 { crate::utils::cache::lsyscache::get_typlen(typid as _) as _ }
unsafe fn get_typcollation(typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_typcollation(typid as _) as _ }
unsafe fn type_is_collatable(typid: Oid) -> bool { crate::utils::cache::lsyscache::type_is_collatable(typid as _) }
unsafe fn get_typlenbyvalalign(typid: Oid, len: *mut int16, byval: *mut bool, align: *mut c_char) {
    crate::utils::cache::lsyscache::get_typlenbyvalalign(typid as _, len as _, byval, align)
}
unsafe fn format_type_be(typid: Oid) -> *const c_char { crate::utils::adt::format_type::format_type_be(typid as _) as _ }
unsafe fn get_func_rettype(funcid: Oid) -> Oid { crate::utils::cache::lsyscache::get_func_rettype(funcid as _) as _ }
unsafe fn func_volatile(funcid: Oid) -> c_char { crate::utils::cache::lsyscache::func_volatile(funcid as _) }
unsafe fn get_func_name(funcid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_func_name(funcid as _) as _ }
unsafe fn get_namespace_name(nspid: Oid) -> *const c_char { crate::utils::cache::lsyscache::get_namespace_name(nspid as _) as _ }
unsafe fn get_rel_relkind(relid: Oid) -> c_char { crate::utils::cache::lsyscache::get_rel_relkind(relid as _) }
unsafe fn get_element_type(typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_element_type(typid as _) as _ }
unsafe fn get_array_type(typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_array_type(typid as _) as _ }
unsafe fn get_collation_oid(collname: *const List, missing_ok: bool) -> Oid {
    crate::catalog::namespace::get_collation_oid(collname as _, missing_ok) as _
}
unsafe fn get_opclass_oid(amid: Oid, opcname: *const List, missing_ok: bool) -> Oid {
    crate::catalog::objectaddress_impl::get_opclass_oid(amid as _, opcname as _, missing_ok) as _
}
unsafe fn get_opclass_input_type(opcid: Oid) -> Oid { crate::utils::cache::lsyscache::get_opclass_input_type(opcid as _) as _ }
unsafe fn GetDefaultOpClass(typid: Oid, amid: Oid) -> Oid { crate::commands::indexcmds::GetDefaultOpClass_full(typid as _, amid as _) as _ }
unsafe fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> bool { crate::parser::parse_coerce::IsBinaryCoercible(srctype as _, targettype as _) }
unsafe fn IsTrueArrayType(typTup: Form_pg_type) -> bool { crate::utils::cache::lsyscache::IsTrueArrayType(typTup as _) }
unsafe fn get_range_multirange(rangeOid: Oid) -> Oid { crate::utils::cache::lsyscache::get_range_multirange(rangeOid as _) as _ }
unsafe fn get_multirange_range(multirangeOid: Oid) -> Oid { crate::utils::cache::lsyscache::get_multirange_range(multirangeOid as _) as _ }

unsafe fn LookupFuncName(
    funcname: *const List, nargs: c_int, argtypes: *const Oid, noError: bool,
) -> Oid { crate::parser::parse_func::LookupFuncName(funcname as _, nargs, argtypes as _, noError) as _ }
unsafe fn func_signature_string(
    funcname: *const List, nargs: c_int, argnames: *const List, argtypes: *const Oid,
) -> *const c_char { crate::parser::parse_func::func_signature_string(funcname as _, nargs, argnames as _, argtypes as _) as _ }
unsafe fn NameListToString(lst: *const List) -> *const c_char { crate::catalog::namespace::NameListToString(lst as _) as _ }
unsafe fn LookupCreationNamespace(newschema: *const c_char) -> Oid { crate::catalog::namespace::LookupCreationNamespace(newschema as _) as _ }
unsafe fn RangeVarGetAndCheckCreationNamespace(
    rv: *mut RangeVar, lockmode: LOCKMODE, existing_relation_id: *mut Oid,
) -> Oid { crate::catalog::namespace::RangeVarGetAndCheckCreationNamespace(rv as _, lockmode as _, existing_relation_id as _) as _ }
unsafe fn RangeVarAdjustRelationPersistence(rv: *mut RangeVar, nspid: Oid) {
    crate::catalog::namespace::RangeVarAdjustRelationPersistence(rv as _, nspid as _)
}

unsafe fn defGetQualifiedName(defel: *const DefElem) -> *mut List { crate::commands::define::defGetQualifiedName(defel as _) as _ }
unsafe fn defGetString(defel: *const DefElem) -> *mut c_char { crate::commands::define::defGetString(defel as _) as _ }
unsafe fn defGetTypeName(defel: *const DefElem) -> *mut TypeName { crate::commands::define::defGetTypeName(defel as _) as _ }
unsafe fn defGetBoolean(defel: *const DefElem) -> bool { crate::commands::define::defGetBoolean(defel as _) }
unsafe fn defGetTypeLength(defel: *const DefElem) -> int16 { crate::commands::define::defGetTypeLength(defel as _) as _ }
unsafe fn errorConflictingDefElem(defel: *const DefElem, pstate: *mut ParseState) { crate::commands::define::errorConflictingDefElem(defel as _, pstate as _) }
unsafe fn pg_strcasecmp(a: *const c_char, b: *const c_char) -> c_int { crate::port::pgstrcasecmp::pg_strcasecmp(a as _, b as _) }
unsafe fn pg_strcasecmp_lit(a: *const c_char, b: &std::ffi::CStr) -> c_int {
    pg_strcasecmp(a, b.as_ptr())
}
unsafe fn strcmp_lit(a: *const c_char, b: &std::ffi::CStr) -> c_int {
    libc_strcmp(a, b.as_ptr())
}
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int { libc::strcmp(a, b) }

unsafe fn typenameType(
    pstate: *mut ParseState, typeName: *const TypeName, typmod_p: *mut int32,
) -> HeapTuple { crate::parser::parse_type::typenameType(pstate as _, typeName as _, typmod_p) as _ }
unsafe fn typenameTypeId(pstate: *mut ParseState, typeName: *const TypeName) -> Oid {
    crate::parser::parse_type::typenameTypeId(pstate as _, typeName as _) as _
}
unsafe fn typeTypeId(tup: HeapTuple) -> Oid { crate::parser::parse_type::typeTypeId(tup as _) as _ }
unsafe fn TypeNameToString(typeName: *const TypeName) -> *const c_char { crate::parser::parse_type::TypeNameToString(typeName as _) as _ }
unsafe fn makeTypeNameFromNameList(names: *const List) -> *mut TypeName { crate::nodes::makefuncs::makeTypeNameFromNameList(names as _) as _ }
unsafe fn LookupTypeName(
    pstate: *mut ParseState, typeName: *const TypeName, typmod_p: *mut int32, missing_ok: bool,
) -> HeapTuple { crate::parser::parse_type::LookupTypeName(pstate as _, typeName as _, typmod_p, missing_ok) as _ }

unsafe fn EnumValuesCreate(enumTypeOid: Oid, vals: *const List) { crate::catalog::pg_enum::EnumValuesCreate(enumTypeOid as _, vals as _) }
unsafe fn EnumValuesDelete(enumTypeOid: Oid) { crate::catalog::pg_enum::EnumValuesDelete(enumTypeOid as _) }
unsafe fn AddEnumLabel(
    enumTypeOid: Oid, newVal: *const c_char, neighbor: *const c_char,
    newValIsAfter: bool, skipIfExists: bool,
) { crate::catalog::pg_enum::AddEnumLabel(enumTypeOid as _, newVal as _, neighbor as _, newValIsAfter, skipIfExists) }
unsafe fn RenameEnumLabel(enumTypeOid: Oid, oldVal: *const c_char, newVal: *const c_char) {
    crate::catalog::pg_enum::RenameEnumLabel(enumTypeOid as _, oldVal as _, newVal as _)
}
unsafe fn RangeCreate(
    rangeTypeOid: Oid, rangeSubType: Oid, rangeCollation: Oid, rangeSubOpclass: Oid,
    rangeCanonical: regproc, rangeSubtypeDiff: regproc, multirangeTypeOid: Oid,
) { crate::catalog::pg_range::RangeCreate(rangeTypeOid as _, rangeSubType as _, rangeCollation as _, rangeSubOpclass as _, rangeCanonical as _, rangeSubtypeDiff as _, multirangeTypeOid as _) }
unsafe fn RangeDelete(rangeTypeOid: Oid) { crate::catalog::pg_range::RangeDelete(rangeTypeOid as _) }

unsafe fn GenerateTypeDependencies(
    newtup: HeapTuple, rel: Relation, defaultExpr: *mut Node,
    typacl: *mut Acl, relationKind: c_int, isImplicitArray: bool,
    isDependentType: bool, rebuildDeps: bool, /* C also: bool addExtension */
    rebuild: bool,
) { crate::catalog::pg_type::GenerateTypeDependencies(newtup as _, rel as _, defaultExpr as _, typacl as _, relationKind as _, isImplicitArray, isDependentType, rebuildDeps, rebuild) }
unsafe fn recordDependencyOn(
    myself: *const ObjectAddress, referenced: *const ObjectAddress, deptype: c_char,
) { crate::catalog::pg_depend::recordDependencyOn(myself as _, referenced as _, deptype as _) }
unsafe fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwnerId: Oid) { crate::catalog::pg_shdepend::changeDependencyOnOwner(classId as _, objectId as _, newOwnerId as _) }
unsafe fn changeDependencyFor(
    classId: Oid, objectId: Oid, refClassId: Oid, oldRefId: Oid, newRefId: Oid,
) -> c_int { crate::catalog::pg_depend::changeDependencyFor(classId as _, objectId as _, refClassId as _, oldRefId as _, newRefId as _) as _ }
unsafe fn performDeletion(object: *const ObjectAddress, behavior: c_int, flags: c_int) {
    crate::catalog::dependency::performDeletion(object as _, core::mem::transmute(behavior), flags)
}

unsafe fn ProcedureCreate(
    procedureName: *const c_char, procNamespace: Oid, replace: bool, returnsSet: bool,
    returnType: Oid, proOwner: Oid, languageObjectId: Oid, languageValidator: Oid,
    prosrc: *const c_char, probin: *const c_char, prosqlbody: *const Node,
    prokind: c_char, security_definer: bool, isLeakProof: bool, isStrict: bool,
    volatility: c_char, parallel: c_char, parameterTypes: *const oidvector,
    allParameterTypes: Datum, parameterModes: Datum, parameterNames: Datum,
    parameterDefaults: *const List, trftypes: Datum, trfoids: *const List,
    proconfig: Datum, prosupport: Oid, procost: f64, prorows: f64,
) -> ObjectAddress {
    core::mem::transmute(crate::catalog::pg_proc::ProcedureCreate(
        procedureName as _, procNamespace as _, replace, returnsSet, returnType as _, proOwner as _,
        languageObjectId as _, languageValidator as _, prosrc as _, probin as _, prosqlbody as _,
        prokind, security_definer, isLeakProof, isStrict, volatility, parallel, parameterTypes as _,
        allParameterTypes, parameterModes, parameterNames, parameterDefaults as _, trftypes,
        trfoids as _, proconfig, prosupport as _, procost as f32, prorows as f32,
    ))
}

unsafe fn CastCreate(
    sourcetypeid: Oid, targettypeid: Oid, funcid: Oid, inoutcast: Oid, trfcast: Oid,
    castcontext: c_char, castmethod: c_char, deptype: c_char,
) { crate::catalog::pg_cast::CastCreate(sourcetypeid as _, targettypeid as _, funcid as _, inoutcast as _, trfcast as _, castcontext, castmethod, deptype as _); }

unsafe fn ConstraintNameIsUsed(
    ctype: c_char, typid: Oid, conname: *const c_char,
) -> bool {
    let cat: crate::catalog::pg_constraint::ConstraintCategory =
        if ctype == CONSTRAINT_DOMAIN { crate::catalog::pg_constraint::CONSTRAINT_DOMAIN }
        else { crate::catalog::pg_constraint::CONSTRAINT_RELATION };
    crate::catalog::pg_constraint::ConstraintNameIsUsed(cat, typid as _, conname as _)
}
unsafe fn ChooseConstraintName(
    name1: *const c_char, name2: *const c_char, label: *const c_char,
    namespaceid: Oid, others: *const List,
) -> *mut c_char { crate::catalog::pg_constraint::ChooseConstraintName(name1 as _, name2 as _, label as _, namespaceid as _, others as _) as _ }
unsafe fn CreateConstraintEntry(
    constraintName: *const c_char, constraintNamespace: Oid, constraintType: c_char,
    isDeferrable: bool, isDeferred: bool, isEnforced: bool, isValidated: bool,
    parentConstrId: Oid, conRelid: Oid, conKey: *const c_int, conNKeys: c_int,
    conKeyTotal: c_int, conTypid: Oid, conIndid: Oid, conFrelid: Oid,
    conFKey: *const c_int, pfeqop: *const Oid, ppeqop: *const Oid,
    ffeqop: *const Oid, conNFKeys: c_int, fkDelAction: c_char, fkUpdAction: c_char,
    fkDelSetCols: *const c_int, conNFkDelSetCols: c_int, fkMatchType: c_char,
    exclOp: *const c_int, conExpr: *const Node, conBin: *const c_char,
    conIsLocal: bool, coninhcount: c_int, connoinherit: bool, conperiod: bool,
    is_internal: bool,
) -> Oid {
    crate::catalog::pg_constraint::CreateConstraintEntry(
        constraintName as _, constraintNamespace as _, constraintType, isDeferrable, isDeferred,
        isEnforced, isValidated, parentConstrId as _, conRelid as _, conKey as _, conNKeys,
        conKeyTotal, conTypid as _, conIndid as _, conFrelid as _, conFKey as _, pfeqop as _,
        ppeqop as _, ffeqop as _, conNFKeys, fkDelAction, fkUpdAction, fkDelSetCols as _,
        conNFkDelSetCols, fkMatchType, exclOp as _, conExpr as _, conBin as _, conIsLocal,
        coninhcount as _, connoinherit, conperiod, is_internal,
    ) as _
}
unsafe fn findDomainNotNullConstraint(domainoid: Oid) -> HeapTuple { crate::catalog::pg_constraint::findDomainNotNullConstraint(domainoid as _) as _ }

unsafe fn make_parsestate(parent: *mut ParseState) -> *mut ParseState { crate::parser::parse_node::make_parsestate(parent as _) as _ }
unsafe fn cookDefault(
    pstate: *mut ParseState, raw_expr: *mut Node, basetypeid: Oid, basetypmod: int32,
    domainName: *const c_char, sortGroupRef: c_int,
) -> *mut Node { crate::catalog::heap::cookDefault(pstate as _, raw_expr as _, basetypeid as _, basetypmod, domainName as _, sortGroupRef as _) as _ }
unsafe fn transformExpr(pstate: *mut ParseState, expr: *mut Node, exprKind: c_int) -> *mut Node {
    crate::parser::parse_expr::transformExpr(pstate as _, expr as _, core::mem::transmute(exprKind)) as _
}
unsafe fn coerce_to_boolean(pstate: *mut ParseState, node: *mut Node, constructName: *const c_char)
    -> *mut Node { crate::parser::parse_coerce::coerce_to_boolean(pstate as _, node as _, constructName as _) as _ }
unsafe fn assign_expr_collations(pstate: *mut ParseState, expr: *mut Node) { crate::parser::parse_collate::assign_expr_collations(pstate as _, expr as _) }
unsafe fn contain_var_clause(node: *const Node) -> bool { crate::optimizer::util::var::contain_var_clause(node as _) }
unsafe fn deparse_expression(
    _expr: *const Node, _rtable: *const List, _forceprefix: bool, _showimplicit: bool,
) -> *mut c_char { unimplemented!() /* real impl in unwired utils::adt::ruleutils */ }
unsafe fn nodeToString(_node: *const Node) -> *mut c_char { crate::nodes::outfuncs::nodeToString(_node as _) as _ }
unsafe fn nodeTag(node: *const Node) -> c_int { crate::nodes::nodes::nodeTag(node) as _ }
unsafe fn stringToNode(str_: *const c_char) -> *mut Node { crate::nodes::read::stringToNode(str_ as _) as _ }
unsafe fn copyObject(obj: *const c_void) -> *mut c_void { crate::nodes::copyfuncs::copyObjectImpl(obj as _) as _ }

unsafe fn CreateExecutorState() -> *mut EState { crate::executor::execUtils::CreateExecutorState() as _ }
unsafe fn GetPerTupleExprContext(estate: *mut EState) -> *mut ExprContext { crate::executor::execUtils::GetPerTupleExprContext(estate as _) as _ }
unsafe fn ExecPrepareExpr(node: *mut Expr, estate: *mut EState) -> *mut ExprState { crate::executor::execExpr::ExecPrepareExpr(node as _, estate as _) as _ }
unsafe fn ExecEvalExprSwitchContext(
    state: *mut ExprState, econtext: *mut ExprContext, isnull: *mut bool,
) -> Datum { crate::executor::executor::ExecEvalExprSwitchContext(state as _, econtext as _, isnull) }
unsafe fn ResetExprContext(econtext: *mut ExprContext) { crate::executor::execUtils::ResetExprContext(econtext as _) }
unsafe fn FreeExecutorState(estate: *mut EState) { crate::executor::execUtils::FreeExecutorState(estate as _) }
unsafe fn slot_getattr(slot: TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum {
    crate::executor::tuptable::slot_getattr(slot as _, attnum, isnull)
}
unsafe fn slot_attisnull(slot: TupleTableSlot, attnum: c_int) -> bool { crate::executor::tuptable::slot_attisnull(slot as _, attnum) }
unsafe fn DatumGetBool(d: Datum) -> bool { crate::postgres::DatumGetBool(d) }
unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char { crate::utils::builtins::TextDatumGetCString(d) as _ }
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum { crate::utils::builtins::CStringGetTextDatum(s as _) }
unsafe fn CStringGetDatum(s: *const c_char) -> Datum { crate::postgres::CStringGetDatum(s as _) }
unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum { crate::postgres::ObjectIdGetDatum(oid as _) }
unsafe fn NameGetDatum(name: *const c_void) -> Datum { crate::postgres::NameGetDatum(name as _) }
unsafe fn PointerGetDatum(p: *const c_void) -> Datum { crate::postgres::PointerGetDatum(p as _) }
unsafe fn CharGetDatum(c: c_char) -> Datum { crate::postgres::CharGetDatum(c) }

unsafe fn GetNewOidWithIndex(rel: Relation, indexId: Oid, oidcolno: c_int) -> Oid {
    crate::catalog::catalog::GetNewOidWithIndex(rel as _, indexId as _, oidcolno as _) as _
}
unsafe fn AssignTypeArrayOid() -> Oid { AssignTypeArrayOid_impl() }
unsafe fn AssignTypeMultirangeOid() -> Oid { AssignTypeMultirangeOid_impl() }
unsafe fn AssignTypeMultirangeArrayOid() -> Oid { AssignTypeMultirangeArrayOid_impl() }
unsafe fn buildoidvector(oids: *const Oid, n: c_int) -> *mut oidvector { crate::utils::builtins::buildoidvector(oids as _, n) as _ }
unsafe fn construct_array_builtin(elems: *const Datum, n: c_int, elmtype: Oid) -> *mut ArrayType {
    crate::utils::adt::arrayfuncs::construct_array_builtin(elems as _, n, elmtype as _) as _
}

unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) { /* no-op unless object access hooks installed */ }
unsafe fn ObjectAddressSet(obj: *mut ObjectAddress, classId: Oid, objectId: Oid) {
    crate::catalog::objectaddress_impl::ObjectAddressSet(&mut *(obj as *mut _), classId as _, objectId as _)
}
unsafe fn object_address_present(object: *const ObjectAddress, addrs: *const ObjectAddresses) -> bool {
    crate::catalog::dependency::object_address_present(object as _, addrs as _)
}
unsafe fn add_exact_object_address(object: *const ObjectAddress, addrs: *mut ObjectAddresses) {
    crate::catalog::dependency::add_exact_object_address(object as _, addrs as _)
}
unsafe fn new_object_addresses() -> *mut ObjectAddresses { crate::catalog::dependency::new_object_addresses() as _ }
unsafe fn free_object_addresses(addrs: *mut ObjectAddresses) { crate::catalog::dependency::free_object_addresses(addrs as _) }
/* AlterTypeNamespace_oid and AlterTypeNamespaceInternal are defined below */
unsafe fn AlterRelationNamespaceInternal(
    classRel: Relation, relOid: Oid, oldNspOid: Oid, newNspOid: Oid,
    hasDependEntry: bool, objsMoved: *mut ObjectAddresses,
) { crate::commands::tablecmds::AlterRelationNamespaceInternal(classRel as _, relOid as _, oldNspOid as _, newNspOid as _, hasDependEntry, objsMoved as _) }
unsafe fn AlterConstraintNamespaces(
    ownerId: Oid, oldNspId: Oid, newNspId: Oid, isType: bool,
    objsMoved: *mut ObjectAddresses,
) { crate::catalog::pg_constraint::AlterConstraintNamespaces(ownerId as _, oldNspId as _, newNspId as _, isType, objsMoved as _) }
unsafe fn CheckSetNamespace(oldNspOid: Oid, newNspOid: Oid) { crate::catalog::namespace::CheckSetNamespace(oldNspOid as _, newNspOid as _) }

unsafe fn RenameRelationInternal(
    relid: Oid, newRelName: *const c_char, lock_is_implicit: bool, is_index: bool,
) { crate::commands::tablecmds::RenameRelationInternal(relid as _, newRelName as _, lock_is_implicit, is_index) }
unsafe fn RenameTypeInternal(typeOid: Oid, newTypeName: *const c_char, nspOid: Oid) {
    crate::catalog::pg_type::RenameTypeInternal(typeOid as _, newTypeName as _, nspOid as _)
}
unsafe fn ATExecChangeOwner(
    relOid: Oid, newOwnerId: Oid, recursing: bool, lockmode: LOCKMODE,
) { crate::commands::tablecmds::ATExecChangeOwner(relOid as _, newOwnerId as _, recursing, lockmode as _) }
/* AlterTypeOwnerInternal is defined below */

unsafe fn DefineRelation(
    stmt: *mut CreateStmt, relkind: c_char, ownerId: Oid,
    address: *mut ObjectAddress, queryString: *const c_char,
) -> ObjectAddress { core::mem::transmute(crate::commands::tablecmds::DefineRelation(stmt as _, relkind, ownerId as _, address as _, queryString as _)) }

unsafe fn find_composite_type_dependencies(
    typeOid: Oid, origRelation: Relation, origTypeName: *const c_char,
) { crate::commands::tablecmds::find_composite_type_dependencies(typeOid as _, origRelation as _, origTypeName as _) }

unsafe fn systable_beginscan(
    rel: Relation, indexId: Oid, indexOK: bool, snap: Snapshot,
    nkeys: c_int, key: *const ScanKeyData,
) -> SysScanDesc { crate::access::index::genam::systable_beginscan(rel as _, indexId as _, indexOK, snap as _, nkeys, key as _) as _ }
unsafe fn systable_getnext(scan: SysScanDesc) -> HeapTuple { crate::access::index::genam::systable_getnext(scan as _) as _ }
unsafe fn systable_endscan(scan: SysScanDesc) { crate::access::index::genam::systable_endscan(scan as _) }
unsafe fn ScanKeyInit(
    entry: *mut ScanKeyData, attNum: c_int, strategy: c_int,
    procedure: Oid, argument: Datum,
) { crate::access::common::scankey::ScanKeyInit(entry as _, attNum as _, strategy as _, procedure as _, argument) }

unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List { crate::nodes::list::lappend(list as _, datum) as _ }
unsafe fn list_concat(list1: *mut List, list2: *mut List) -> *mut List { crate::nodes::list::list_concat(list1 as _, list2 as _) as _ }
unsafe fn list_length(list: *const List) -> c_int { crate::nodes::pg_list::list_length(list as _) }
unsafe fn linitial(list: *const List) -> *mut c_void { crate::nodes::pg_list::linitial(list as _) }
unsafe fn lsecond(list: *const List) -> *mut c_void { crate::nodes::pg_list::lsecond(list as _) }
unsafe fn lfirst(lc: *const ListCell) -> *mut c_void { crate::nodes::pg_list::lfirst(lc as _) }
unsafe fn strVal(node: *const Node) -> *mut c_char { crate::strVal!(node) as _ }
unsafe fn intVal(node: *const Node) -> c_int { crate::intVal!(node) }
unsafe fn linitial_node_List(list: *const List) -> *mut List { crate::nodes::pg_list::linitial(list as _) as _ }

unsafe fn palloc(size: usize) -> *mut c_void { crate::utils::palloc::palloc(size as _) }
unsafe fn pfree(ptr: *mut c_void) { crate::utils::palloc::pfree(ptr) }

unsafe fn check_stack_depth() { crate::miscadmin::check_stack_depth() }
unsafe fn CommandCounterIncrement() { crate::access::transam::xact::CommandCounterIncrement() }
unsafe fn CacheInvalidateHeapTuple(rel: Relation, tup: HeapTuple, newtup: HeapTuple) {
    crate::utils::cache::inval::CacheInvalidateHeapTuple(rel as _, tup as _, newtup as _)
}

unsafe fn aclnewowner(old_acl: *const Acl, oldOwnerId: Oid, newOwnerId: Oid) -> *mut Acl {
    crate::utils::adt::acl::aclnewowner(old_acl as _, oldOwnerId as _, newOwnerId as _) as _
}
unsafe fn DatumGetAclP(d: Datum) -> *mut Acl { crate::PG_DETOAST_DATUM!(d) as _ }

unsafe fn GetLatestSnapshot() -> Snapshot { crate::utils::time::snapmgr::GetLatestSnapshot() as _ }
unsafe fn RegisterSnapshot(snap: Snapshot) -> Snapshot { crate::utils::time::snapmgr::RegisterSnapshot(snap as _) as _ }
unsafe fn UnregisterSnapshot(snap: Snapshot) { crate::utils::time::snapmgr::UnregisterSnapshot(snap as _) }

unsafe fn NameStr(name: *const c_void) -> *const c_char { crate::c::NameStr(&*(name as *const crate::c::NameData)) as _ }

// ScanKey function OID constants
const F_OIDEQ: Oid = 184;
const F_NAMEEQ: Oid = 93;

// libitial helper aliases
unsafe fn linitial_node_ColumnRef(list: *const List) -> *mut ColumnRef { crate::nodes::pg_list::linitial(list as _) as _ }

/*
 * domainAddCheckConstraint - code shared between CREATE and ALTER DOMAIN
 */
unsafe fn domainAddCheckConstraint(
    domainOid: Oid, domainNamespace: Oid, baseTypeOid: Oid, typMod: c_int,
    constr: *mut Constraint, domainName: *const c_char, constrAddr: *mut ObjectAddress,
) -> *mut c_char {
    let expr: *mut Node;
    let ccbin: *mut c_char;
    let pstate: *mut ParseState;
    let domVal: *mut CoerceToDomainValue;
    let ccoid: Oid;

    /* Assert(constr->contype == CONSTR_CHECK); */

    /*
     * Assign or validate constraint name
     */
    if !(*constr).conname.is_null() {
        if ConstraintNameIsUsed(CONSTRAINT_DOMAIN,
                                domainOid,
                                (*constr).conname) {
            ereport!(ERROR,
                     errmsg!("constraint \"{}\" for domain \"{}\" already exists",
                             std::ffi::CStr::from_ptr((*constr).conname).to_string_lossy(),
                             std::ffi::CStr::from_ptr(domainName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    } else {
        (*constr).conname = ChooseConstraintName(domainName,
                                                 core::ptr::null(),
                                                 c"check".as_ptr(),
                                                 domainNamespace,
                                                 core::ptr::null());
    }

    /*
     * Convert the A_EXPR in raw_expr into an EXPR
     */
    pstate = make_parsestate(core::ptr::null_mut());

    /*
     * Set up a CoerceToDomainValue to represent the occurrence of VALUE in
     * the expression.  Note that it will appear to have the type of the base
     * type, not the domain.  This seems correct since within the check
     * expression, we should not assume the input value can be considered a
     * member of the domain.
     */
    domVal = makeNode!(CoerceToDomainValue, T_CoerceToDomainValue) as *mut CoerceToDomainValue;
    (*domVal).typeId = baseTypeOid;
    (*domVal).typeMod = typMod;
    (*domVal).collation = get_typcollation(baseTypeOid);
    (*domVal).location = -1; /* will be set when/if used */

    (*pstate).p_pre_columnref_hook = Some(replace_domain_constraint_value);
    (*pstate).p_ref_hook_state = domVal as *mut c_void;

    expr = transformExpr(pstate, (*constr).raw_expr, EXPR_KIND_DOMAIN_CHECK);

    /*
     * Make sure it yields a boolean result.
     */
    let expr = coerce_to_boolean(pstate, expr, c"CHECK".as_ptr());

    /*
     * Fix up collation information.
     */
    assign_expr_collations(pstate, expr);

    /*
     * Domains don't allow variables (this is probably dead code now that
     * add_missing_from is history, but let's be sure).
     */
    if !(*pstate).p_rtable.is_null() ||
        contain_var_clause(expr) {
        ereport!(ERROR,
                 errmsg!("cannot use table references in domain check constraint"));
        /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
    }

    /*
     * Convert to string form for storage.
     */
    ccbin = nodeToString(expr);

    /*
     * Store the constraint in pg_constraint
     */
    ccoid =
        CreateConstraintEntry((*constr).conname, /* Constraint Name */
                              domainNamespace,   /* namespace */
                              CONSTRAINT_CHECK,  /* Constraint Type */
                              false,             /* Is Deferrable */
                              false,             /* Is Deferred */
                              true,              /* Is Enforced */
                              !(*constr).skip_validation, /* Is Validated */
                              InvalidOid,        /* no parent constraint */
                              InvalidOid,        /* not a relation constraint */
                              core::ptr::null(),
                              0,
                              0,
                              domainOid,         /* domain constraint */
                              InvalidOid,        /* no associated index */
                              InvalidOid,        /* Foreign key fields */
                              core::ptr::null(),
                              core::ptr::null(),
                              core::ptr::null(),
                              core::ptr::null(),
                              0,
                              b' ' as c_char,
                              b' ' as c_char,
                              core::ptr::null(),
                              0,
                              b' ' as c_char,
                              core::ptr::null(), /* not an exclusion constraint */
                              expr,              /* Tree form of check constraint */
                              ccbin,             /* Binary form of check constraint */
                              true,              /* is local */
                              0,                 /* inhcount */
                              false,             /* connoinherit */
                              false,             /* conperiod */
                              false);            /* is_internal */
    if !constrAddr.is_null() {
        ObjectAddressSet(constrAddr, ConstraintRelationId, ccoid);
    }

    /*
     * Return the compiled constraint expression so the calling routine can
     * perform any additional required tests.
     */
    ccbin
}

/* Parser pre_columnref_hook for domain CHECK constraint parsing */
unsafe fn replace_domain_constraint_value(pstate: *mut ParseState, cref: *mut c_void) -> *mut Node {
    let cref = cref as *mut ColumnRef;
    /*
     * Check for a reference to "value", and if that's what it is, replace
     * with a CoerceToDomainValue as prepared for us by
     * domainAddCheckConstraint. (We handle VALUE as a name, not a keyword, to
     * avoid breaking a lot of applications that have used VALUE as a column
     * name in the past.)
     */
    if list_length((*cref).fields) == 1 {
        let field1: *mut Node = linitial((*cref).fields) as *mut Node;
        let colname: *mut c_char;

        colname = strVal(field1);
        if strcmp_lit(colname, c"value") == 0 {
            let domVal: *mut CoerceToDomainValue =
                copyObject((*pstate).p_ref_hook_state) as *mut CoerceToDomainValue;

            /* Propagate location knowledge, if any */
            (*domVal).location = (*cref).location;
            return domVal as *mut Node;
        }
    }
    core::ptr::null_mut()
}

/*
 * domainAddNotNullConstraint - code shared between CREATE and ALTER DOMAIN
 */
unsafe fn domainAddNotNullConstraint(
    domainOid: Oid, domainNamespace: Oid, _baseTypeOid: Oid, _typMod: c_int,
    constr: *mut Constraint, domainName: *const c_char, constrAddr: *mut ObjectAddress,
) {
    let ccoid: Oid;

    /* Assert(constr->contype == CONSTR_NOTNULL); */

    /*
     * Assign or validate constraint name
     */
    if !(*constr).conname.is_null() {
        if ConstraintNameIsUsed(CONSTRAINT_DOMAIN,
                                domainOid,
                                (*constr).conname) {
            ereport!(ERROR,
                     errmsg!("constraint \"{}\" for domain \"{}\" already exists",
                             std::ffi::CStr::from_ptr((*constr).conname).to_string_lossy(),
                             std::ffi::CStr::from_ptr(domainName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    } else {
        (*constr).conname = ChooseConstraintName(domainName,
                                                 core::ptr::null(),
                                                 c"not_null".as_ptr(),
                                                 domainNamespace,
                                                 core::ptr::null());
    }

    /*
     * Store the constraint in pg_constraint
     */
    ccoid =
        CreateConstraintEntry((*constr).conname, /* Constraint Name */
                              domainNamespace,    /* namespace */
                              CONSTRAINT_NOTNULL, /* Constraint Type */
                              false,              /* Is Deferrable */
                              false,              /* Is Deferred */
                              true,               /* Is Enforced */
                              !(*constr).skip_validation, /* Is Validated */
                              InvalidOid,         /* no parent constraint */
                              InvalidOid,         /* not a relation constraint */
                              core::ptr::null(),
                              0,
                              0,
                              domainOid,          /* domain constraint */
                              InvalidOid,         /* no associated index */
                              InvalidOid,         /* Foreign key fields */
                              core::ptr::null(),
                              core::ptr::null(),
                              core::ptr::null(),
                              core::ptr::null(),
                              0,
                              b' ' as c_char,
                              b' ' as c_char,
                              core::ptr::null(),
                              0,
                              b' ' as c_char,
                              core::ptr::null(), /* not an exclusion constraint */
                              core::ptr::null(),
                              core::ptr::null(),
                              true,              /* is local */
                              0,                 /* inhcount */
                              false,             /* connoinherit */
                              false,             /* conperiod */
                              false);            /* is_internal */

    if !constrAddr.is_null() {
        ObjectAddressSet(constrAddr, ConstraintRelationId, ccoid);
    }
}

/* =========================================================================
 * Part 2: DefineType, RemoveTypeById
 * ========================================================================= */

/*
 * DefineType
 *     Registers a new base type.
 */
pub unsafe fn DefineType(
    pstate: *mut ParseState,
    names: *mut List,
    parameters: *mut List,
) -> ObjectAddress {
    let typeName: *mut c_char;
    let typeNamespace: Oid;
    let mut internalLength: int16 = -1; /* default: variable-length */
    let mut inputName: *mut List = std::ptr::null_mut(); /* NIL */
    let mut outputName: *mut List = std::ptr::null_mut();
    let mut receiveName: *mut List = std::ptr::null_mut();
    let mut sendName: *mut List = std::ptr::null_mut();
    let mut typmodinName: *mut List = std::ptr::null_mut();
    let mut typmodoutName: *mut List = std::ptr::null_mut();
    let mut analyzeName: *mut List = std::ptr::null_mut();
    let mut subscriptName: *mut List = std::ptr::null_mut();
    let mut category: c_char = TYPCATEGORY_USER;
    let mut preferred: bool = false;
    let mut delimiter: c_char = DEFAULT_TYPDELIM;
    let mut elemType: Oid = InvalidOid;
    let mut defaultValue: *mut c_char = std::ptr::null_mut();
    let mut byValue: bool = false;
    let mut alignment: c_char = TYPALIGN_INT; /* default alignment */
    let mut storage: c_char = TYPSTORAGE_PLAIN; /* default TOAST storage method */
    let mut collation: Oid = InvalidOid;
    let mut likeTypeEl: *mut DefElem = std::ptr::null_mut();
    let mut internalLengthEl: *mut DefElem = std::ptr::null_mut();
    let mut inputNameEl: *mut DefElem = std::ptr::null_mut();
    let mut outputNameEl: *mut DefElem = std::ptr::null_mut();
    let mut receiveNameEl: *mut DefElem = std::ptr::null_mut();
    let mut sendNameEl: *mut DefElem = std::ptr::null_mut();
    let mut typmodinNameEl: *mut DefElem = std::ptr::null_mut();
    let mut typmodoutNameEl: *mut DefElem = std::ptr::null_mut();
    let mut analyzeNameEl: *mut DefElem = std::ptr::null_mut();
    let mut subscriptNameEl: *mut DefElem = std::ptr::null_mut();
    let mut categoryEl: *mut DefElem = std::ptr::null_mut();
    let mut preferredEl: *mut DefElem = std::ptr::null_mut();
    let mut delimiterEl: *mut DefElem = std::ptr::null_mut();
    let mut elemTypeEl: *mut DefElem = std::ptr::null_mut();
    let mut defaultValueEl: *mut DefElem = std::ptr::null_mut();
    let mut byValueEl: *mut DefElem = std::ptr::null_mut();
    let mut alignmentEl: *mut DefElem = std::ptr::null_mut();
    let mut storageEl: *mut DefElem = std::ptr::null_mut();
    let mut collatableEl: *mut DefElem = std::ptr::null_mut();
    let inputOid: Oid;
    let outputOid: Oid;
    let mut receiveOid: Oid = InvalidOid;
    let mut sendOid: Oid = InvalidOid;
    let mut typmodinOid: Oid = InvalidOid;
    let mut typmodoutOid: Oid = InvalidOid;
    let mut analyzeOid: Oid = InvalidOid;
    let mut subscriptOid: Oid = InvalidOid;
    let array_type: *mut c_char;
    let array_oid: Oid;
    let mut typoid: Oid;
    let address: ObjectAddress;

    /*
     * As of Postgres 8.4, we require superuser privilege to create a base
     * type.  This is simple paranoia: there are too many ways to mess up the
     * system with an incorrect type definition (for instance, representation
     * parameters that don't match what the C code expects).  In practice it
     * takes superuser privilege to create the I/O functions, and so the
     * former requirement that you own the I/O functions pretty much forced
     * superuserness anyway.  We're just making doubly sure here.
     *
     * XXX re-enable NOT_USED code sections below if you remove this test.
     */
    if !superuser() {
        ereport!(ERROR, errmsg!("must be superuser to create a base type"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /* Convert list of names to a name and namespace */
    let mut typeName_out: *mut c_char = std::ptr::null_mut();
    typeNamespace = QualifiedNameGetCreationNamespace(names, &mut typeName_out);
    typeName = typeName_out;

    /* #ifdef NOT_USED: check ACL_CREATE on namespace - omitted per superuser check */

    /*
     * Look to see if type already exists.
     */
    typoid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum(typeName),
        ObjectIdGetDatum(typeNamespace),
    );

    /*
     * If it's not a shell, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if OidIsValid(typoid) && get_typisdefined(typoid) {
        if moveArrayTypeName(typoid, typeName, typeNamespace) {
            typoid = InvalidOid;
        } else {
            ereport!(ERROR, errmsg!("type \"{}\" already exists",
                std::ffi::CStr::from_ptr(typeName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /*
     * If this command is a parameterless CREATE TYPE, then we're just here to
     * make a shell type, so do that (or fail if there already is a shell).
     */
    if parameters.is_null() {
        if OidIsValid(typoid) {
            ereport!(ERROR, errmsg!("type \"{}\" already exists",
                std::ffi::CStr::from_ptr(typeName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }

        let address = TypeShellMake(typeName, typeNamespace, GetUserId());
        return address;
    }

    /*
     * Otherwise, we must already have a shell type, since there is no other
     * way that the I/O functions could have been created.
     */
    if !OidIsValid(typoid) {
        ereport!(ERROR, errmsg!("type \"{}\" does not exist",
            std::ffi::CStr::from_ptr(typeName).to_string_lossy()));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT),
         * errhint("Create the type as a shell type, then create its I/O functions, then do a full CREATE TYPE.") */
    }

    /* Extract the parameters from the parameter list */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst(crate::current_cell!(pl)) as *mut DefElem;
        let defelp: *mut *mut DefElem;

        if strcmp_lit((*defel).defname, c"like") == 0 {
            defelp = &mut likeTypeEl;
        } else if strcmp_lit((*defel).defname, c"internallength") == 0 {
            defelp = &mut internalLengthEl;
        } else if strcmp_lit((*defel).defname, c"input") == 0 {
            defelp = &mut inputNameEl;
        } else if strcmp_lit((*defel).defname, c"output") == 0 {
            defelp = &mut outputNameEl;
        } else if strcmp_lit((*defel).defname, c"receive") == 0 {
            defelp = &mut receiveNameEl;
        } else if strcmp_lit((*defel).defname, c"send") == 0 {
            defelp = &mut sendNameEl;
        } else if strcmp_lit((*defel).defname, c"typmod_in") == 0 {
            defelp = &mut typmodinNameEl;
        } else if strcmp_lit((*defel).defname, c"typmod_out") == 0 {
            defelp = &mut typmodoutNameEl;
        } else if strcmp_lit((*defel).defname, c"analyze") == 0
               || strcmp_lit((*defel).defname, c"analyse") == 0 {
            defelp = &mut analyzeNameEl;
        } else if strcmp_lit((*defel).defname, c"subscript") == 0 {
            defelp = &mut subscriptNameEl;
        } else if strcmp_lit((*defel).defname, c"category") == 0 {
            defelp = &mut categoryEl;
        } else if strcmp_lit((*defel).defname, c"preferred") == 0 {
            defelp = &mut preferredEl;
        } else if strcmp_lit((*defel).defname, c"delimiter") == 0 {
            defelp = &mut delimiterEl;
        } else if strcmp_lit((*defel).defname, c"element") == 0 {
            defelp = &mut elemTypeEl;
        } else if strcmp_lit((*defel).defname, c"default") == 0 {
            defelp = &mut defaultValueEl;
        } else if strcmp_lit((*defel).defname, c"passedbyvalue") == 0 {
            defelp = &mut byValueEl;
        } else if strcmp_lit((*defel).defname, c"alignment") == 0 {
            defelp = &mut alignmentEl;
        } else if strcmp_lit((*defel).defname, c"storage") == 0 {
            defelp = &mut storageEl;
        } else if strcmp_lit((*defel).defname, c"collatable") == 0 {
            defelp = &mut collatableEl;
        } else {
            /* WARNING, not ERROR, for historical backwards-compatibility */
            ereport!(WARNING, errmsg!("type attribute \"{}\" not recognized",
                std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition(pstate, (*defel).location) */
            continue;
        }
        if !(*defelp).is_null() {
            errorConflictingDefElem(defel, pstate);
        }
        *defelp = defel;
    });

    /*
     * Now interpret the options; we do this separately so that LIKE can be
     * overridden by other options regardless of the ordering in the parameter
     * list.
     */
    if !likeTypeEl.is_null() {
        let likeType: HeapTuple = typenameType(pstate, defGetTypeName(likeTypeEl) as *const TypeName, std::ptr::null_mut());
        let likeForm: Form_pg_type = GETSTRUCT(likeType) as Form_pg_type;
        internalLength = (*(likeForm as *mut FormData_pg_type_fields)).typlen;
        byValue = (*(likeForm as *mut FormData_pg_type_fields)).typbyval;
        alignment = (*(likeForm as *mut FormData_pg_type_fields)).typalign;
        storage = (*(likeForm as *mut FormData_pg_type_fields)).typstorage;
        ReleaseSysCache(likeType);
    }
    if !internalLengthEl.is_null() {
        internalLength = defGetTypeLength(internalLengthEl);
    }
    if !inputNameEl.is_null() {
        inputName = defGetQualifiedName(inputNameEl);
    }
    if !outputNameEl.is_null() {
        outputName = defGetQualifiedName(outputNameEl);
    }
    if !receiveNameEl.is_null() {
        receiveName = defGetQualifiedName(receiveNameEl);
    }
    if !sendNameEl.is_null() {
        sendName = defGetQualifiedName(sendNameEl);
    }
    if !typmodinNameEl.is_null() {
        typmodinName = defGetQualifiedName(typmodinNameEl);
    }
    if !typmodoutNameEl.is_null() {
        typmodoutName = defGetQualifiedName(typmodoutNameEl);
    }
    if !analyzeNameEl.is_null() {
        analyzeName = defGetQualifiedName(analyzeNameEl);
    }
    if !subscriptNameEl.is_null() {
        subscriptName = defGetQualifiedName(subscriptNameEl);
    }
    if !categoryEl.is_null() {
        let p: *mut c_char = defGetString(categoryEl);
        category = *p;
        /* restrict to non-control ASCII */
        if (category as u8) < 32 || (category as u8) > 126 {
            ereport!(ERROR, errmsg!("invalid type category \"{}\": must be simple ASCII",
                std::ffi::CStr::from_ptr(p).to_string_lossy()));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !preferredEl.is_null() {
        preferred = defGetBoolean(preferredEl);
    }
    if !delimiterEl.is_null() {
        let p: *mut c_char = defGetString(delimiterEl);
        delimiter = *p;
        /* XXX shouldn't we restrict the delimiter? */
    }
    if !elemTypeEl.is_null() {
        elemType = typenameTypeId(std::ptr::null_mut(), defGetTypeName(elemTypeEl));
        /* disallow arrays of pseudotypes */
        if get_typtype(elemType) == TYPTYPE_PSEUDO {
            ereport!(ERROR, errmsg!("array element type cannot be {}",
                std::ffi::CStr::from_ptr(format_type_be(elemType)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        }
    }
    if !defaultValueEl.is_null() {
        defaultValue = defGetString(defaultValueEl);
    }
    if !byValueEl.is_null() {
        byValue = defGetBoolean(byValueEl);
    }
    if !alignmentEl.is_null() {
        let a: *mut c_char = defGetString(alignmentEl);

        /*
         * Note: if argument was an unquoted identifier, parser will have
         * applied translations to it, so be prepared to recognize translated
         * type names as well as the nominal form.
         */
        if pg_strcasecmp_lit(a, c"double") == 0
            || pg_strcasecmp_lit(a, c"float8") == 0
            || pg_strcasecmp_lit(a, c"pg_catalog.float8") == 0
        {
            alignment = TYPALIGN_DOUBLE;
        } else if pg_strcasecmp_lit(a, c"int4") == 0
               || pg_strcasecmp_lit(a, c"pg_catalog.int4") == 0
        {
            alignment = TYPALIGN_INT;
        } else if pg_strcasecmp_lit(a, c"int2") == 0
               || pg_strcasecmp_lit(a, c"pg_catalog.int2") == 0
        {
            alignment = TYPALIGN_SHORT;
        } else if pg_strcasecmp_lit(a, c"char") == 0
               || pg_strcasecmp_lit(a, c"pg_catalog.bpchar") == 0
        {
            alignment = TYPALIGN_CHAR;
        } else {
            ereport!(ERROR, errmsg!("alignment \"{}\" not recognized",
                std::ffi::CStr::from_ptr(a).to_string_lossy()));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !storageEl.is_null() {
        let a: *mut c_char = defGetString(storageEl);

        if pg_strcasecmp_lit(a, c"plain") == 0 {
            storage = TYPSTORAGE_PLAIN;
        } else if pg_strcasecmp_lit(a, c"external") == 0 {
            storage = TYPSTORAGE_EXTERNAL;
        } else if pg_strcasecmp_lit(a, c"extended") == 0 {
            storage = TYPSTORAGE_EXTENDED;
        } else if pg_strcasecmp_lit(a, c"main") == 0 {
            storage = TYPSTORAGE_MAIN;
        } else {
            ereport!(ERROR, errmsg!("storage \"{}\" not recognized",
                std::ffi::CStr::from_ptr(a).to_string_lossy()));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !collatableEl.is_null() {
        collation = if defGetBoolean(collatableEl) { DEFAULT_COLLATION_OID } else { InvalidOid };
    }

    /*
     * make sure we have our required definitions
     */
    if inputName.is_null() {
        ereport!(ERROR, errmsg!("type input function must be specified"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }
    if outputName.is_null() {
        ereport!(ERROR, errmsg!("type output function must be specified"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if typmodinName.is_null() && !typmodoutName.is_null() {
        ereport!(ERROR, errmsg!("type modifier output function is useless without a type modifier input function"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * Convert I/O proc names to OIDs
     */
    inputOid = findTypeInputFunction(inputName, typoid);
    outputOid = findTypeOutputFunction(outputName, typoid);
    if !receiveName.is_null() {
        receiveOid = findTypeReceiveFunction(receiveName, typoid);
    }
    if !sendName.is_null() {
        sendOid = findTypeSendFunction(sendName, typoid);
    }

    /*
     * Convert typmodin/out function proc names to OIDs.
     */
    if !typmodinName.is_null() {
        typmodinOid = findTypeTypmodinFunction(typmodinName);
    }
    if !typmodoutName.is_null() {
        typmodoutOid = findTypeTypmodoutFunction(typmodoutName);
    }

    /*
     * Convert analysis function proc name to an OID. If no analysis function
     * is specified, we'll use zero to select the built-in default algorithm.
     */
    if !analyzeName.is_null() {
        analyzeOid = findTypeAnalyzeFunction(analyzeName, typoid);
    }

    /*
     * Likewise look up the subscripting function if any.  If it is not
     * specified, but a typelem is specified, allow that if
     * raw_array_subscript_handler can be used.  (This is for backwards
     * compatibility; maybe someday we should throw an error instead.)
     */
    if !subscriptName.is_null() {
        subscriptOid = findTypeSubscriptingFunction(subscriptName, typoid);
    } else if OidIsValid(elemType) {
        if internalLength > 0 && !byValue && get_typlen(elemType) > 0 {
            subscriptOid = F_RAW_ARRAY_SUBSCRIPT_HANDLER;
        } else {
            ereport!(ERROR, errmsg!("element type cannot be specified without a subscripting function"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }

    /* #ifdef NOT_USED: ownership checks on I/O functions */

    /*
     * OK, we're done checking, time to make the type.  We must assign the
     * array type OID ahead of calling TypeCreate, since the base type and
     * array type each refer to the other.
     */
    array_oid = AssignTypeArrayOid();

    /*
     * now have TypeCreate do all the real work.
     *
     * Note: the pg_type.oid is stored in user tables as array elements (base
     * types) in ArrayType and in composite types in DatumTupleFields.  This
     * oid must be preserved by binary upgrades.
     */
    let address = TypeCreate(
        InvalidOid,      /* no predetermined type OID */
        typeName,        /* type name */
        typeNamespace,   /* namespace */
        InvalidOid,      /* relation oid (n/a here) */
        0,               /* relation kind (ditto) */
        GetUserId(),     /* owner's ID */
        internalLength,  /* internal size */
        TYPTYPE_BASE,    /* type-type (base type) */
        category,        /* type-category */
        preferred,       /* is it a preferred type? */
        delimiter,       /* array element delimiter */
        inputOid,        /* input procedure */
        outputOid,       /* output procedure */
        receiveOid,      /* receive procedure */
        sendOid,         /* send procedure */
        typmodinOid,     /* typmodin procedure */
        typmodoutOid,    /* typmodout procedure */
        analyzeOid,      /* analyze procedure */
        subscriptOid,    /* subscript procedure */
        elemType,        /* element type ID */
        false,           /* this is not an implicit array type */
        array_oid,       /* array type we are about to create */
        InvalidOid,      /* base type ID (only for domains) */
        defaultValue,    /* default type value */
        std::ptr::null_mut(), /* no binary form available */
        byValue,         /* passed by value */
        alignment,       /* required alignment */
        storage,         /* TOAST strategy */
        -1,              /* typMod (Domains only) */
        0,               /* Array Dimensions of typbasetype */
        false,           /* Type NOT NULL */
        collation,       /* type's collation */
    );
    /* Assert(typoid == address.objectId); */

    /*
     * Create the array type that goes with it.
     */
    array_type = makeArrayTypeName(typeName, typeNamespace);

    /* alignment must be TYPALIGN_INT or TYPALIGN_DOUBLE for arrays */
    alignment = if alignment == TYPALIGN_DOUBLE { TYPALIGN_DOUBLE } else { TYPALIGN_INT };

    TypeCreate(
        array_oid,           /* force assignment of this type OID */
        array_type,          /* type name */
        typeNamespace,       /* namespace */
        InvalidOid,          /* relation oid (n/a here) */
        0,                   /* relation kind (ditto) */
        GetUserId(),         /* owner's ID */
        -1,                  /* internal size (always varlena) */
        TYPTYPE_BASE,        /* type-type (base type) */
        TYPCATEGORY_ARRAY,   /* type-category (array) */
        false,               /* array types are never preferred */
        delimiter,           /* array element delimiter */
        F_ARRAY_IN,          /* input procedure */
        F_ARRAY_OUT,         /* output procedure */
        F_ARRAY_RECV,        /* receive procedure */
        F_ARRAY_SEND,        /* send procedure */
        typmodinOid,         /* typmodin procedure */
        typmodoutOid,        /* typmodout procedure */
        F_ARRAY_TYPANALYZE,  /* analyze procedure */
        F_ARRAY_SUBSCRIPT_HANDLER, /* array subscript procedure */
        typoid,              /* element type ID */
        true,                /* yes this is an array type */
        InvalidOid,          /* no further array type */
        InvalidOid,          /* base type ID */
        std::ptr::null_mut(), /* never a default type value */
        std::ptr::null_mut(), /* binary default isn't sent either */
        false,               /* never passed by value */
        alignment,           /* see above */
        TYPSTORAGE_EXTENDED, /* ARRAY is always toastable */
        -1,                  /* typMod (Domains only) */
        0,                   /* Array dimensions of typbasetype */
        false,               /* Type NOT NULL */
        collation,           /* type's collation */
    );

    pfree(array_type as *mut c_void);

    address
}

/*
 * Guts of type deletion.
 */
pub unsafe fn RemoveTypeById(typeOid: Oid) {
    let relation: Relation;
    let tup: HeapTuple;

    relation = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", typeOid));
    }

    CatalogTupleDelete(relation, &mut (*(tup as *mut HeapTupleDataFull)).t_self as *mut ItemPointerData);

    /*
     * If it is an enum, delete the pg_enum entries too; we don't bother with
     * making dependency entries for those, so it has to be done "by hand"
     * here.
     */
    if (*(GETSTRUCT(tup) as Form_pg_type as *mut FormData_pg_type_typtype)).typtype == TYPTYPE_ENUM {
        EnumValuesDelete(typeOid);
    }

    /*
     * If it is a range type, delete the pg_range entry too; we don't bother
     * with making a dependency entry for that, so it has to be done "by hand"
     * here.
     */
    if (*(GETSTRUCT(tup) as Form_pg_type as *mut FormData_pg_type_typtype)).typtype == TYPTYPE_RANGE {
        RangeDelete(typeOid);
    }

    ReleaseSysCache(tup);

    table_close(relation, RowExclusiveLock);
}

/* Helper: opaque struct for t_self access */
#[repr(C)]
struct HeapTupleDataFull {
    t_len: u32,
    t_self: ItemPointerData,
    t_tableOid: Oid,
    t_data: *mut c_void,
}

/* Helper: opaque struct for typtype access from GETSTRUCT */
#[repr(C)]
struct FormData_pg_type_typtype {
    /* We only need offset to typtype; use a placeholder approach */
    _pad: [u8; 48],
    typtype: c_char,
}

/* Helper for LIKE clause field access */
#[repr(C)]
struct FormData_pg_type_fields {
    _pad_oid: Oid,
    _pad_name: [u8; 64],
    _pad_ns: Oid,
    typlen: int16,
    typbyval: bool,
    _pad2: [u8; 4],
    typtype: c_char,
    typcategory: c_char,
    typispreferred: bool,
    typisdefined: bool,
    typdelim: c_char,
    typrelid: Oid,
    typsubscript: Oid,
    typelem: Oid,
    typarray: Oid,
    typinput: Oid,
    typoutput: Oid,
    typreceive: Oid,
    typsend: Oid,
    typmodin: Oid,
    typmodout: Oid,
    typanalyze: Oid,
    typalign: c_char,
    typstorage: c_char,
    typnotnull: bool,
    typbasetype: Oid,
    typtypmod: int32,
    typndims: int32,
    typcollation: Oid,
    typowner: Oid,
    oid: Oid,
    typnamespace: Oid,
    typname: [u8; 64],
    typoutput2: Oid,
}

/* SysCache IDs -- TODO(pg-port) */
const TYPENAMENSP: c_int = 81;
const TYPEOID: c_int = 82;
const CONSTROID: c_int = 19;

unsafe fn OidIsValid(oid: Oid) -> bool { oid != InvalidOid }

/* =========================================================================
 * Part 3: DefineDomain, DefineEnum, AlterEnum, checkEnumOwner
 * ========================================================================= */

/*
 * DefineDomain
 *     Registers a new domain.
 */
pub unsafe fn DefineDomain(pstate: *mut ParseState, stmt: *mut CreateDomainStmt) -> ObjectAddress {
    let domainName: *mut c_char;
    let domainArrayName: *mut c_char;
    let domainNamespace: Oid;
    let mut aclresult: AclResult = ACLCHECK_OK;
    let internalLength: int16;
    let inputProcedure: Oid;
    let outputProcedure: Oid;
    let receiveProcedure: Oid;
    let sendProcedure: Oid;
    let analyzeProcedure: Oid;
    let byValue: bool;
    let category: c_char;
    let delimiter: c_char;
    let mut alignment: c_char;
    let storage: c_char;
    let typtype: c_char;
    let mut datum: Datum = 0;
    let mut isnull: bool = false;
    let mut defaultValue: *mut c_char = std::ptr::null_mut();
    let mut defaultValueBin: *mut c_char = std::ptr::null_mut();
    let mut saw_default: bool = false;
    let mut typNotNull: bool = false;
    let mut nullDefined: bool = false;
    let typNDims: int32 = list_length((*(*stmt).typeName).arrayBounds);
    let typeTup: HeapTuple;
    let schema: *mut List = (*stmt).constraints;
    let basetypeoid: Oid;
    let old_type_oid: Oid;
    let domaincoll: Oid;
    let domainArrayOid: Oid;
    let baseType: Form_pg_type;
    let mut basetypeMod: int32 = 0;
    let baseColl: Oid;
    let address: ObjectAddress;

    /* Convert list of names to a name and namespace */
    let mut domainName_out: *mut c_char = std::ptr::null_mut();
    domainNamespace = QualifiedNameGetCreationNamespace((*stmt).domainname, &mut domainName_out);
    domainName = domainName_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, domainNamespace, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(domainNamespace));
    }

    /*
     * Check for collision with an existing type name.  If there is one and
     * it's an autogenerated array, we can rename it out of the way.
     */
    old_type_oid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum(domainName),
        ObjectIdGetDatum(domainNamespace),
    );
    if OidIsValid(old_type_oid) {
        if !moveArrayTypeName(old_type_oid, domainName, domainNamespace) {
            ereport!(ERROR, errmsg!("type \"{}\" already exists",
                std::ffi::CStr::from_ptr(domainName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /*
     * Look up the base type.
     */
    typeTup = typenameType(pstate, (*stmt).typeName, &mut basetypeMod);
    baseType = GETSTRUCT(typeTup) as Form_pg_type;
    basetypeoid = (*(baseType as *mut FormData_pg_type_fields)).oid;

    /*
     * Base type must be a plain base type, a composite type, another domain,
     * an enum or a range type.  Domains over pseudotypes would create a
     * security hole.  (It would be shorter to code this to just check for
     * pseudotypes; but it seems safer to call out the specific typtypes that
     * are supported, rather than assume that all future typtypes would be
     * automatically supported.)
     */
    typtype = (*(baseType as *mut FormData_pg_type_fields)).typtype;
    if typtype != TYPTYPE_BASE
        && typtype != TYPTYPE_COMPOSITE
        && typtype != TYPTYPE_DOMAIN
        && typtype != TYPTYPE_ENUM
        && typtype != TYPTYPE_RANGE
        && typtype != TYPTYPE_MULTIRANGE
    {
        ereport!(ERROR, errmsg!("\"{}\" is not a valid base type for a domain",
            std::ffi::CStr::from_ptr(TypeNameToString((*stmt).typeName)).to_string_lossy()));
        /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition */
    }

    aclresult = object_aclcheck(TypeRelationId, basetypeoid, GetUserId(), ACL_USAGE as c_uint);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, basetypeoid);
    }

    /*
     * Collect the properties of the new domain.  Some are inherited from the
     * base type, some are not.  If you change any of this inheritance
     * behavior, be sure to update AlterTypeRecurse() to match!
     */

    /* Identify the collation if any */
    baseColl = (*(baseType as *mut FormData_pg_type_fields)).typcollation;
    if !(*stmt).collClause.is_null() {
        domaincoll = get_collation_oid((*(*stmt).collClause).collname as *const List, false);
    } else {
        domaincoll = baseColl;
    }

    /* Complain if COLLATE is applied to an uncollatable type */
    if OidIsValid(domaincoll) && !OidIsValid(baseColl) {
        ereport!(ERROR, errmsg!("collations are not supported by type {}",
            std::ffi::CStr::from_ptr(format_type_be(basetypeoid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition */
    }

    /* passed by value */
    byValue = (*(baseType as *mut FormData_pg_type_fields)).typbyval;

    /* Required Alignment */
    alignment = (*(baseType as *mut FormData_pg_type_fields)).typalign;

    /* TOAST Strategy */
    storage = (*(baseType as *mut FormData_pg_type_fields)).typstorage;

    /* Storage Length */
    internalLength = (*(baseType as *mut FormData_pg_type_fields)).typlen;

    /* Type Category */
    category = (*(baseType as *mut FormData_pg_type_fields)).typcategory;

    /* Array element Delimiter */
    delimiter = (*(baseType as *mut FormData_pg_type_fields)).typdelim;

    /* I/O Functions */
    inputProcedure = F_DOMAIN_IN;
    outputProcedure = (*(baseType as *mut FormData_pg_type_fields)).typoutput;
    receiveProcedure = F_DOMAIN_RECV;
    sendProcedure = (*(baseType as *mut FormData_pg_type_fields)).typsend;

    /* Domains never accept typmods, so no typmodin/typmodout needed */

    /* Analysis function */
    analyzeProcedure = (*(baseType as *mut FormData_pg_type_fields)).typanalyze;

    /*
     * Domains don't need a subscript function, since they are not
     * subscriptable on their own.  If the base type is subscriptable, the
     * parser will reduce the type to the base type before subscripting.
     */

    /* Inherited default value */
    datum = SysCacheGetAttr(TYPEOID, typeTup, Anum_pg_type_typdefault, &mut isnull);
    if !isnull {
        defaultValue = TextDatumGetCString(datum);
    }

    /* Inherited default binary value */
    datum = SysCacheGetAttr(TYPEOID, typeTup, Anum_pg_type_typdefaultbin, &mut isnull);
    if !isnull {
        defaultValueBin = TextDatumGetCString(datum);
    }

    /*
     * Run through constraints manually to avoid the additional processing
     * conducted by DefineRelation() and friends.
     */
    foreach!(listptr, schema, {
        let constr: *mut Constraint = lfirst(crate::current_cell!(listptr)) as *mut Constraint;

        if !IsA!(constr, T_Constraint) {
            ereport!(ERROR, errmsg!("unrecognized node type: {}", nodeTag(constr as *const Node)));
        }
        match (*constr).contype {
            CONSTR_DEFAULT => {
                /*
                 * The inherited default value may be overridden by the user
                 * with the DEFAULT <expr> clause ... but only once.
                 */
                if saw_default {
                    ereport!(ERROR, errmsg!("multiple default expressions"));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                }
                saw_default = true;

                if !(*constr).raw_expr.is_null() {
                    let defaultExpr: *mut Node;

                    /*
                     * Cook the constr->raw_expr into an expression. Note:
                     * name is strictly for error message
                     */
                    defaultExpr = cookDefault(pstate, (*constr).raw_expr,
                                              basetypeoid, basetypeMod, domainName, 0);

                    /*
                     * If the expression is just a NULL constant, we treat it
                     * like not having a default.
                     *
                     * Note that if the basetype is another domain, we'll see
                     * a CoerceToDomain expr here and not discard the default.
                     * This is critical because the domain default needs to be
                     * retained to override any default that the base domain
                     * might have.
                     */
                    if defaultExpr.is_null()
                        || (IsA!(defaultExpr, T_Const)
                            && (*(defaultExpr as *const ConstNode)).constisnull)
                    {
                        defaultValue = std::ptr::null_mut();
                        defaultValueBin = std::ptr::null_mut();
                    } else {
                        /*
                         * Expression must be stored as a nodeToString result,
                         * but we also require a valid textual representation
                         * (mainly to make life easier for pg_dump).
                         */
                        defaultValue = deparse_expression(defaultExpr, std::ptr::null(), false, false);
                        defaultValueBin = nodeToString(defaultExpr);
                    }
                } else {
                    /* No default (can this still happen?) */
                    defaultValue = std::ptr::null_mut();
                    defaultValueBin = std::ptr::null_mut();
                }
            }
            CONSTR_NOTNULL => {
                if nullDefined {
                    if !typNotNull {
                        ereport!(ERROR, errmsg!("conflicting NULL/NOT NULL constraints"));
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    }
                    ereport!(ERROR, errmsg!("redundant NOT NULL constraint definition"));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION), parser_errposition */
                }
                if (*constr).is_no_inherit {
                    ereport!(ERROR, errmsg!("not-null constraints for domains cannot be marked NO INHERIT"));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION), parser_errposition */
                }
                typNotNull = true;
                nullDefined = true;
            }
            CONSTR_NULL => {
                if nullDefined && typNotNull {
                    ereport!(ERROR, errmsg!("conflicting NULL/NOT NULL constraints"));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                }
                typNotNull = false;
                nullDefined = true;
            }
            CONSTR_CHECK => {
                /*
                 * Check constraints are handled after domain creation, as
                 * they require the Oid of the domain; at this point we can
                 * only check that they're not marked NO INHERIT, because that
                 * would be bogus.
                 */
                if (*constr).is_no_inherit {
                    ereport!(ERROR, errmsg!("check constraints for domains cannot be marked NO INHERIT"));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION), parser_errposition */
                }
            }
            CONSTR_UNIQUE => {
                ereport!(ERROR, errmsg!("unique constraints not possible for domains"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            }
            CONSTR_PRIMARY => {
                ereport!(ERROR, errmsg!("primary key constraints not possible for domains"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            }
            CONSTR_EXCLUSION => {
                ereport!(ERROR, errmsg!("exclusion constraints not possible for domains"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            }
            CONSTR_FOREIGN => {
                ereport!(ERROR, errmsg!("foreign key constraints not possible for domains"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            }
            CONSTR_ATTR_DEFERRABLE | CONSTR_ATTR_NOT_DEFERRABLE
            | CONSTR_ATTR_DEFERRED | CONSTR_ATTR_IMMEDIATE => {
                ereport!(ERROR, errmsg!("specifying constraint deferrability not supported for domains"));
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            }
            CONSTR_GENERATED | CONSTR_IDENTITY => {
                ereport!(ERROR, errmsg!("specifying GENERATED not supported for domains"));
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            }
            CONSTR_ATTR_ENFORCED | CONSTR_ATTR_NOT_ENFORCED => {
                ereport!(ERROR, errmsg!("specifying constraint enforceability not supported for domains"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION), parser_errposition */
            }
        }
    });

    /* Allocate OID for array type */
    domainArrayOid = AssignTypeArrayOid();

    /*
     * Have TypeCreate do all the real work.
     */
    let address = TypeCreate(
        InvalidOid,          /* no predetermined type OID */
        domainName,          /* type name */
        domainNamespace,     /* namespace */
        InvalidOid,          /* relation oid (n/a here) */
        0,                   /* relation kind (ditto) */
        GetUserId(),         /* owner's ID */
        internalLength,      /* internal size */
        TYPTYPE_DOMAIN,      /* type-type (domain type) */
        category,            /* type-category */
        false,               /* domain types are never preferred */
        delimiter,           /* array element delimiter */
        inputProcedure,      /* input procedure */
        outputProcedure,     /* output procedure */
        receiveProcedure,    /* receive procedure */
        sendProcedure,       /* send procedure */
        InvalidOid,          /* typmodin procedure - none */
        InvalidOid,          /* typmodout procedure - none */
        analyzeProcedure,    /* analyze procedure */
        InvalidOid,          /* subscript procedure - none */
        InvalidOid,          /* no array element type */
        false,               /* this isn't an array */
        domainArrayOid,      /* array type we are about to create */
        basetypeoid,         /* base type ID */
        defaultValue,        /* default type value (text) */
        defaultValueBin,     /* default type value (binary) */
        byValue,             /* passed by value */
        alignment,           /* required alignment */
        storage,             /* TOAST strategy */
        basetypeMod,         /* typeMod value */
        typNDims,            /* Array dimensions for base type */
        typNotNull,          /* Type NOT NULL */
        domaincoll,          /* type's collation */
    );

    /*
     * Create the array type that goes with it.
     */
    domainArrayName = makeArrayTypeName(domainName, domainNamespace);

    /* alignment must be TYPALIGN_INT or TYPALIGN_DOUBLE for arrays */
    alignment = if alignment == TYPALIGN_DOUBLE { TYPALIGN_DOUBLE } else { TYPALIGN_INT };

    TypeCreate(
        domainArrayOid,      /* force assignment of this type OID */
        domainArrayName,     /* type name */
        domainNamespace,     /* namespace */
        InvalidOid,          /* relation oid (n/a here) */
        0,                   /* relation kind (ditto) */
        GetUserId(),         /* owner's ID */
        -1,                  /* internal size (always varlena) */
        TYPTYPE_BASE,        /* type-type (base type) */
        TYPCATEGORY_ARRAY,   /* type-category (array) */
        false,               /* array types are never preferred */
        delimiter,           /* array element delimiter */
        F_ARRAY_IN,          /* input procedure */
        F_ARRAY_OUT,         /* output procedure */
        F_ARRAY_RECV,        /* receive procedure */
        F_ARRAY_SEND,        /* send procedure */
        InvalidOid,          /* typmodin procedure - none */
        InvalidOid,          /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,  /* analyze procedure */
        F_ARRAY_SUBSCRIPT_HANDLER, /* array subscript procedure */
        address.objectId,    /* element type ID */
        true,                /* yes this is an array type */
        InvalidOid,          /* no further array type */
        InvalidOid,          /* base type ID */
        std::ptr::null_mut(), /* never a default type value */
        std::ptr::null_mut(), /* binary default isn't sent either */
        false,               /* never passed by value */
        alignment,           /* see above */
        TYPSTORAGE_EXTENDED, /* ARRAY is always toastable */
        -1,                  /* typMod (Domains only) */
        0,                   /* Array dimensions of typbasetype */
        false,               /* Type NOT NULL */
        domaincoll,          /* type's collation */
    );

    pfree(domainArrayName as *mut c_void);

    /*
     * Process constraints which refer to the domain ID returned by TypeCreate
     */
    foreach!(listptr, schema, {
        let constr: *mut Constraint = lfirst(crate::current_cell!(listptr)) as *mut Constraint;

        /* it must be a Constraint, per check above */

        match (*constr).contype {
            CONSTR_CHECK => {
                domainAddCheckConstraint(address.objectId, domainNamespace,
                                         basetypeoid, basetypeMod,
                                         constr, domainName, std::ptr::null_mut());
            }
            CONSTR_NOTNULL => {
                domainAddNotNullConstraint(address.objectId, domainNamespace,
                                           basetypeoid, basetypeMod,
                                           constr, domainName, std::ptr::null_mut());
            }
            /* Other constraint types were fully processed above */
            _ => {}
        }

        /* CCI so we can detect duplicate constraint names */
        CommandCounterIncrement();
    });

    /*
     * Now we can clean up.
     */
    ReleaseSysCache(typeTup);

    address
}

/* Helper stubs for DefineDomain node casts */
#[repr(C)] struct ConstNode { _pad: [u8; 8], constisnull: bool }

/*
 * DefineEnum
 *     Registers a new enum.
 */
pub unsafe fn DefineEnum(stmt: *mut CreateEnumStmt) -> ObjectAddress {
    let enumName: *mut c_char;
    let enumArrayName: *mut c_char;
    let enumNamespace: Oid;
    let aclresult: AclResult;
    let old_type_oid: Oid;
    let enumArrayOid: Oid;
    let enumTypeAddr: ObjectAddress;

    /* Convert list of names to a name and namespace */
    let mut enumName_out: *mut c_char = std::ptr::null_mut();
    enumNamespace = QualifiedNameGetCreationNamespace((*stmt).typeName, &mut enumName_out);
    enumName = enumName_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, enumNamespace, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(enumNamespace));
    }

    /*
     * Check for collision with an existing type name.  If there is one and
     * it's an autogenerated array, we can rename it out of the way.
     */
    old_type_oid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum(enumName),
        ObjectIdGetDatum(enumNamespace),
    );
    if OidIsValid(old_type_oid) {
        if !moveArrayTypeName(old_type_oid, enumName, enumNamespace) {
            ereport!(ERROR, errmsg!("type \"{}\" already exists",
                std::ffi::CStr::from_ptr(enumName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /* Allocate OID for array type */
    enumArrayOid = AssignTypeArrayOid();

    /* Create the pg_type entry */
    let enumTypeAddr = TypeCreate(
        InvalidOid,             /* no predetermined type OID */
        enumName,               /* type name */
        enumNamespace,          /* namespace */
        InvalidOid,             /* relation oid (n/a here) */
        0,                      /* relation kind (ditto) */
        GetUserId(),            /* owner's ID */
        core::mem::size_of::<Oid>() as int16, /* internal size */
        TYPTYPE_ENUM,           /* type-type (enum type) */
        TYPCATEGORY_ENUM,       /* type-category (enum type) */
        false,                  /* enum types are never preferred */
        DEFAULT_TYPDELIM,       /* array element delimiter */
        F_ENUM_IN,              /* input procedure */
        F_ENUM_OUT,             /* output procedure */
        F_ENUM_RECV,            /* receive procedure */
        F_ENUM_SEND,            /* send procedure */
        InvalidOid,             /* typmodin procedure - none */
        InvalidOid,             /* typmodout procedure - none */
        InvalidOid,             /* analyze procedure - default */
        InvalidOid,             /* subscript procedure - none */
        InvalidOid,             /* element type ID */
        false,                  /* this is not an array type */
        enumArrayOid,           /* array type we are about to create */
        InvalidOid,             /* base type ID (only for domains) */
        std::ptr::null_mut(),   /* never a default type value */
        std::ptr::null_mut(),   /* binary default isn't sent either */
        true,                   /* always passed by value */
        TYPALIGN_INT,           /* int alignment */
        TYPSTORAGE_PLAIN,       /* TOAST strategy always plain */
        -1,                     /* typMod (Domains only) */
        0,                      /* Array dimensions of typbasetype */
        false,                  /* Type NOT NULL */
        InvalidOid,             /* type's collation */
    );

    /* Enter the enum's values into pg_enum */
    EnumValuesCreate(enumTypeAddr.objectId, (*stmt).vals);

    /*
     * Create the array type that goes with it.
     */
    enumArrayName = makeArrayTypeName(enumName, enumNamespace);

    TypeCreate(
        enumArrayOid,           /* force assignment of this type OID */
        enumArrayName,          /* type name */
        enumNamespace,          /* namespace */
        InvalidOid,             /* relation oid (n/a here) */
        0,                      /* relation kind (ditto) */
        GetUserId(),            /* owner's ID */
        -1,                     /* internal size (always varlena) */
        TYPTYPE_BASE,           /* type-type (base type) */
        TYPCATEGORY_ARRAY,      /* type-category (array) */
        false,                  /* array types are never preferred */
        DEFAULT_TYPDELIM,       /* array element delimiter */
        F_ARRAY_IN,             /* input procedure */
        F_ARRAY_OUT,            /* output procedure */
        F_ARRAY_RECV,           /* receive procedure */
        F_ARRAY_SEND,           /* send procedure */
        InvalidOid,             /* typmodin procedure - none */
        InvalidOid,             /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,     /* analyze procedure */
        F_ARRAY_SUBSCRIPT_HANDLER, /* array subscript procedure */
        enumTypeAddr.objectId,  /* element type ID */
        true,                   /* yes this is an array type */
        InvalidOid,             /* no further array type */
        InvalidOid,             /* base type ID */
        std::ptr::null_mut(),   /* never a default type value */
        std::ptr::null_mut(),   /* binary default isn't sent either */
        false,                  /* never passed by value */
        TYPALIGN_INT,           /* enums have int align, so do their arrays */
        TYPSTORAGE_EXTENDED,    /* ARRAY is always toastable */
        -1,                     /* typMod (Domains only) */
        0,                      /* Array dimensions of typbasetype */
        false,                  /* Type NOT NULL */
        InvalidOid,             /* type's collation */
    );

    pfree(enumArrayName as *mut c_void);

    enumTypeAddr
}

/*
 * AlterEnum
 *     Adds a new label to an existing enum.
 */
pub unsafe fn AlterEnum(stmt: *mut AlterEnumStmt) -> ObjectAddress {
    let enum_type_oid: Oid;
    let typename: *mut TypeName;
    let tup: HeapTuple;
    let address: ObjectAddress = std::mem::zeroed();

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList((*stmt).typeName);
    enum_type_oid = typenameTypeId(std::ptr::null_mut(), typename);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(enum_type_oid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", enum_type_oid));
    }

    /* Check it's an enum and check user has permission to ALTER the enum */
    checkEnumOwner(tup);

    ReleaseSysCache(tup);

    if !(*stmt).oldVal.is_null() {
        /* Rename an existing label */
        RenameEnumLabel(enum_type_oid, (*stmt).oldVal, (*stmt).newVal);
    } else {
        /* Add a new label */
        AddEnumLabel(enum_type_oid, (*stmt).newVal,
                     (*stmt).newValNeighbor, (*stmt).newValIsAfter,
                     (*stmt).skipIfNewValExists);
    }

    InvokeObjectPostAlterHook(TypeRelationId, enum_type_oid, 0);

    let mut address: ObjectAddress = std::mem::zeroed();
    ObjectAddressSet(&mut address, TypeRelationId, enum_type_oid);

    address
}

/*
 * checkEnumOwner
 *
 * Check that the type is actually an enum and that the current user
 * has permission to do ALTER TYPE on it.  Throw an error if not.
 */
unsafe fn checkEnumOwner(tup: HeapTuple) {
    let typTup: Form_pg_type = GETSTRUCT(tup) as Form_pg_type;

    /* Check that this is actually an enum */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype != TYPTYPE_ENUM {
        ereport!(ERROR, errmsg!("{} is not an enum",
            std::ffi::CStr::from_ptr(format_type_be(
                (*(typTup as *mut FormData_pg_type_fields)).oid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* Permission check: must own type */
    if !object_ownercheck(TypeRelationId, (*(typTup as *mut FormData_pg_type_fields)).oid, GetUserId()) {
        aclcheck_error_type(ACLCHECK_NOT_OWNER, (*(typTup as *mut FormData_pg_type_fields)).oid);
    }
}

/* =========================================================================
 * Part 4: DefineRange, makeRangeConstructors, makeMultirangeConstructors,
 *         findType* functions, findRange* functions,
 *         AssignTypeArrayOid, AssignTypeMultirangeOid, AssignTypeMultirangeArrayOid
 * ========================================================================= */

/*
 * DefineRange
 *     Registers a new range type.
 *
 * Perhaps it might be worthwhile to set pg_type.typelem to the base type,
 * and likewise on multiranges to set it to the range type. But having a
 * non-zero typelem is treated elsewhere as a synonym for being an array,
 * and users might have queries with that same assumption.
 */
pub unsafe fn DefineRange(pstate: *mut ParseState, stmt: *mut CreateRangeStmt) -> ObjectAddress {
    let typeName: *mut c_char;
    let typeNamespace: Oid;
    let mut typoid: Oid;
    let rangeArrayName: *mut c_char;
    let mut multirangeTypeName: *mut c_char = std::ptr::null_mut();
    let multirangeArrayName: *mut c_char;
    let mut multirangeNamespace: Oid = InvalidOid;
    let rangeArrayOid: Oid;
    let multirangeOid: Oid;
    let multirangeArrayOid: Oid;
    let mut rangeSubtype: Oid = InvalidOid;
    let mut rangeSubOpclassName: *mut List = std::ptr::null_mut(); /* NIL */
    let mut rangeCollationName: *mut List = std::ptr::null_mut(); /* NIL */
    let mut rangeCanonicalName: *mut List = std::ptr::null_mut(); /* NIL */
    let mut rangeSubtypeDiffName: *mut List = std::ptr::null_mut(); /* NIL */
    let rangeSubOpclass: Oid;
    let rangeCollation: Oid;
    let rangeCanonical: regproc;
    let rangeSubtypeDiff: regproc;
    let mut subtyplen: int16 = 0;
    let mut subtypbyval: bool = false;
    let mut subtypalign: c_char = 0;
    let alignment: c_char;
    let aclresult: AclResult;
    let address: ObjectAddress;
    let castFuncOid: Oid;

    /* Convert list of names to a name and namespace */
    let mut typeName_out: *mut c_char = std::ptr::null_mut();
    typeNamespace = QualifiedNameGetCreationNamespace((*stmt).typeName, &mut typeName_out);
    typeName = typeName_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, typeNamespace, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(typeNamespace));
    }

    /*
     * Look to see if type already exists.
     */
    typoid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum(typeName),
        ObjectIdGetDatum(typeNamespace),
    );

    /*
     * If it's not a shell, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if OidIsValid(typoid) && get_typisdefined(typoid) {
        if moveArrayTypeName(typoid, typeName, typeNamespace) {
            typoid = InvalidOid;
        } else {
            ereport!(ERROR, errmsg!("type \"{}\" already exists",
                std::ffi::CStr::from_ptr(typeName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /*
     * Unlike DefineType(), we don't insist on a shell type existing first, as
     * it's only needed if the user wants to specify a canonical function.
     */

    /* Extract the parameters from the parameter list */
    foreach!(lc, (*stmt).params, {
        let defel: *mut DefElem = lfirst(crate::current_cell!(lc)) as *mut DefElem;

        if strcmp_lit((*defel).defname, c"subtype") == 0 {
            if OidIsValid(rangeSubtype) {
                errorConflictingDefElem(defel, pstate);
            }
            /* we can look up the subtype name immediately */
            rangeSubtype = typenameTypeId(std::ptr::null_mut(), defGetTypeName(defel));
        } else if strcmp_lit((*defel).defname, c"subtype_opclass") == 0 {
            if !rangeSubOpclassName.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            rangeSubOpclassName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"collation") == 0 {
            if !rangeCollationName.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            rangeCollationName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"canonical") == 0 {
            if !rangeCanonicalName.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            rangeCanonicalName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"subtype_diff") == 0 {
            if !rangeSubtypeDiffName.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            rangeSubtypeDiffName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"multirange_type_name") == 0 {
            if !multirangeTypeName.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            /* we can look up the subtype name immediately */
            let mut mtn_out: *mut c_char = std::ptr::null_mut();
            multirangeNamespace = QualifiedNameGetCreationNamespace(
                defGetQualifiedName(defel), &mut mtn_out);
            multirangeTypeName = mtn_out;
        } else {
            ereport!(ERROR, errmsg!("type attribute \"{}\" not recognized",
                std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }
    });

    /* Must have a subtype */
    if !OidIsValid(rangeSubtype) {
        ereport!(ERROR, errmsg!("type attribute \"subtype\" is required"));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
    }
    /* disallow ranges of pseudotypes */
    if get_typtype(rangeSubtype) == TYPTYPE_PSEUDO {
        ereport!(ERROR, errmsg!("range subtype cannot be {}",
            std::ffi::CStr::from_ptr(format_type_be(rangeSubtype)).to_string_lossy()));
        /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
    }

    /* Identify subopclass */
    rangeSubOpclass = findRangeSubOpclass(rangeSubOpclassName, rangeSubtype);

    /* Identify collation to use, if any */
    if type_is_collatable(rangeSubtype) {
        if !rangeCollationName.is_null() {
            rangeCollation = get_collation_oid(rangeCollationName, false);
        } else {
            rangeCollation = get_typcollation(rangeSubtype);
        }
    } else {
        if !rangeCollationName.is_null() {
            ereport!(ERROR, errmsg!("range collation specified but subtype does not support collation"));
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        }
        rangeCollation = InvalidOid;
    }

    /* Identify support functions, if provided */
    if !rangeCanonicalName.is_null() {
        if !OidIsValid(typoid) {
            ereport!(ERROR, errmsg!("cannot specify a canonical function without a pre-created shell type"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
             * errhint("Create the type as a shell type, then create its canonicalization function, then do a full CREATE TYPE.") */
        }
        rangeCanonical = findRangeCanonicalFunction(rangeCanonicalName, typoid);
    } else {
        rangeCanonical = InvalidOid;
    }

    if !rangeSubtypeDiffName.is_null() {
        rangeSubtypeDiff = findRangeSubtypeDiffFunction(rangeSubtypeDiffName, rangeSubtype);
    } else {
        rangeSubtypeDiff = InvalidOid;
    }

    get_typlenbyvalalign(rangeSubtype, &mut subtyplen, &mut subtypbyval, &mut subtypalign);

    /* alignment must be TYPALIGN_INT or TYPALIGN_DOUBLE for ranges */
    alignment = if subtypalign == TYPALIGN_DOUBLE { TYPALIGN_DOUBLE } else { TYPALIGN_INT };

    /* Allocate OID for array type, its multirange, and its multirange array */
    rangeArrayOid = AssignTypeArrayOid();
    multirangeOid = AssignTypeMultirangeOid();
    multirangeArrayOid = AssignTypeMultirangeArrayOid();

    /* Create the pg_type entry */
    let address = TypeCreate(
        InvalidOid,          /* no predetermined type OID */
        typeName,            /* type name */
        typeNamespace,       /* namespace */
        InvalidOid,          /* relation oid (n/a here) */
        0,                   /* relation kind (ditto) */
        GetUserId(),         /* owner's ID */
        -1,                  /* internal size (always varlena) */
        TYPTYPE_RANGE,       /* type-type (range type) */
        TYPCATEGORY_RANGE,   /* type-category (range type) */
        false,               /* range types are never preferred */
        DEFAULT_TYPDELIM,    /* array element delimiter */
        F_RANGE_IN,          /* input procedure */
        F_RANGE_OUT,         /* output procedure */
        F_RANGE_RECV,        /* receive procedure */
        F_RANGE_SEND,        /* send procedure */
        InvalidOid,          /* typmodin procedure - none */
        InvalidOid,          /* typmodout procedure - none */
        F_RANGE_TYPANALYZE,  /* analyze procedure */
        InvalidOid,          /* subscript procedure - none */
        InvalidOid,          /* element type ID - none */
        false,               /* this is not an array type */
        rangeArrayOid,       /* array type we are about to create */
        InvalidOid,          /* base type ID (only for domains) */
        std::ptr::null_mut(), /* never a default type value */
        std::ptr::null_mut(), /* no binary form available either */
        false,               /* never passed by value */
        alignment,           /* alignment */
        TYPSTORAGE_EXTENDED, /* TOAST strategy (always extended) */
        -1,                  /* typMod (Domains only) */
        0,                   /* Array dimensions of typbasetype */
        false,               /* Type NOT NULL */
        InvalidOid,          /* type's collation (ranges never have one) */
    );
    /* Assert(typoid == InvalidOid || typoid == address.objectId); */
    typoid = address.objectId;

    /* Create the multirange that goes with it */
    if !multirangeTypeName.is_null() {
        let old_typoid: Oid;

        /*
         * Look to see if multirange type already exists.
         */
        old_typoid = GetSysCacheOid2(
            TYPENAMENSP,
            Anum_pg_type_oid,
            CStringGetDatum(multirangeTypeName),
            ObjectIdGetDatum(multirangeNamespace),
        );

        /*
         * If it's not a shell, see if it's an autogenerated array type, and
         * if so rename it out of the way.
         */
        if OidIsValid(old_typoid) && get_typisdefined(old_typoid) {
            if !moveArrayTypeName(old_typoid, multirangeTypeName, multirangeNamespace) {
                ereport!(ERROR, errmsg!("type \"{}\" already exists",
                    std::ffi::CStr::from_ptr(multirangeTypeName).to_string_lossy()));
                /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            }
        }
    } else {
        /* Generate multirange name automatically */
        multirangeNamespace = typeNamespace;
        multirangeTypeName = makeMultirangeTypeName(typeName, multirangeNamespace);
    }

    let mltrngaddress = TypeCreate(
        multirangeOid,          /* force assignment of this type OID */
        multirangeTypeName,     /* type name */
        multirangeNamespace,    /* namespace */
        InvalidOid,             /* relation oid (n/a here) */
        0,                      /* relation kind (ditto) */
        GetUserId(),            /* owner's ID */
        -1,                     /* internal size (always varlena) */
        TYPTYPE_MULTIRANGE,     /* type-type (multirange type) */
        TYPCATEGORY_RANGE,      /* type-category (range type) */
        false,                  /* multirange types are never preferred */
        DEFAULT_TYPDELIM,       /* array element delimiter */
        F_MULTIRANGE_IN,        /* input procedure */
        F_MULTIRANGE_OUT,       /* output procedure */
        F_MULTIRANGE_RECV,      /* receive procedure */
        F_MULTIRANGE_SEND,      /* send procedure */
        InvalidOid,             /* typmodin procedure - none */
        InvalidOid,             /* typmodout procedure - none */
        F_MULTIRANGE_TYPANALYZE, /* analyze procedure */
        InvalidOid,             /* subscript procedure - none */
        InvalidOid,             /* element type ID - none */
        false,                  /* this is not an array type */
        multirangeArrayOid,     /* array type we are about to create */
        InvalidOid,             /* base type ID (only for domains) */
        std::ptr::null_mut(),   /* never a default type value */
        std::ptr::null_mut(),   /* no binary form available either */
        false,                  /* never passed by value */
        alignment,              /* alignment */
        b'x' as c_char,        /* TOAST strategy (always extended) */
        -1,                     /* typMod (Domains only) */
        0,                      /* Array dimensions of typbasetype */
        false,                  /* Type NOT NULL */
        InvalidOid,             /* type's collation (ranges never have one) */
    );
    /* Assert(multirangeOid == mltrngaddress.objectId); */

    /* Create the entry in pg_range */
    RangeCreate(typoid, rangeSubtype, rangeCollation, rangeSubOpclass,
                rangeCanonical, rangeSubtypeDiff, multirangeOid);

    /*
     * Create the array type that goes with it.
     */
    rangeArrayName = makeArrayTypeName(typeName, typeNamespace);

    TypeCreate(
        rangeArrayOid,       /* force assignment of this type OID */
        rangeArrayName,      /* type name */
        typeNamespace,       /* namespace */
        InvalidOid,          /* relation oid (n/a here) */
        0,                   /* relation kind (ditto) */
        GetUserId(),         /* owner's ID */
        -1,                  /* internal size (always varlena) */
        TYPTYPE_BASE,        /* type-type (base type) */
        TYPCATEGORY_ARRAY,   /* type-category (array) */
        false,               /* array types are never preferred */
        DEFAULT_TYPDELIM,    /* array element delimiter */
        F_ARRAY_IN,          /* input procedure */
        F_ARRAY_OUT,         /* output procedure */
        F_ARRAY_RECV,        /* receive procedure */
        F_ARRAY_SEND,        /* send procedure */
        InvalidOid,          /* typmodin procedure - none */
        InvalidOid,          /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,  /* analyze procedure */
        F_ARRAY_SUBSCRIPT_HANDLER, /* array subscript procedure */
        typoid,              /* element type ID */
        true,                /* yes this is an array type */
        InvalidOid,          /* no further array type */
        InvalidOid,          /* base type ID */
        std::ptr::null_mut(), /* never a default type value */
        std::ptr::null_mut(), /* binary default isn't sent either */
        false,               /* never passed by value */
        alignment,           /* alignment - same as range's */
        TYPSTORAGE_EXTENDED, /* ARRAY is always toastable */
        -1,                  /* typMod (Domains only) */
        0,                   /* Array dimensions of typbasetype */
        false,               /* Type NOT NULL */
        InvalidOid,          /* typcollation */
    );

    pfree(rangeArrayName as *mut c_void);

    /* Create the multirange's array type */
    multirangeArrayName = makeArrayTypeName(multirangeTypeName, typeNamespace);

    TypeCreate(
        multirangeArrayOid,      /* force assignment of this type OID */
        multirangeArrayName,     /* type name */
        multirangeNamespace,     /* namespace */
        InvalidOid,              /* relation oid (n/a here) */
        0,                       /* relation kind (ditto) */
        GetUserId(),             /* owner's ID */
        -1,                      /* internal size (always varlena) */
        TYPTYPE_BASE,            /* type-type (base type) */
        TYPCATEGORY_ARRAY,       /* type-category (array) */
        false,                   /* array types are never preferred */
        DEFAULT_TYPDELIM,        /* array element delimiter */
        F_ARRAY_IN,              /* input procedure */
        F_ARRAY_OUT,             /* output procedure */
        F_ARRAY_RECV,            /* receive procedure */
        F_ARRAY_SEND,            /* send procedure */
        InvalidOid,              /* typmodin procedure - none */
        InvalidOid,              /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,      /* analyze procedure */
        F_ARRAY_SUBSCRIPT_HANDLER, /* array subscript procedure */
        multirangeOid,           /* element type ID */
        true,                    /* yes this is an array type */
        InvalidOid,              /* no further array type */
        InvalidOid,              /* base type ID */
        std::ptr::null_mut(),    /* never a default type value */
        std::ptr::null_mut(),    /* binary default isn't sent either */
        false,                   /* never passed by value */
        alignment,               /* alignment - same as range's */
        b'x' as c_char,         /* ARRAY is always toastable */
        -1,                      /* typMod (Domains only) */
        0,                       /* Array dimensions of typbasetype */
        false,                   /* Type NOT NULL */
        InvalidOid,              /* typcollation */
    );

    /* And create the constructor functions for this range type */
    makeRangeConstructors(typeName, typeNamespace, typoid, rangeSubtype);
    let mut castFuncOid: Oid = InvalidOid;
    makeMultirangeConstructors(multirangeTypeName, typeNamespace,
                               multirangeOid, typoid, rangeArrayOid,
                               &mut castFuncOid);

    /* Create cast from the range type to its multirange type */
    CastCreate(typoid, multirangeOid, castFuncOid, InvalidOid, InvalidOid,
               COERCION_CODE_EXPLICIT, COERCION_METHOD_FUNCTION,
               DEPENDENCY_INTERNAL);

    pfree(multirangeArrayName as *mut c_void);

    address
}

/*
 * Because there may exist several range types over the same subtype, the
 * range type can't be uniquely determined from the subtype.  So it's
 * impossible to define a polymorphic constructor; we have to generate new
 * constructor functions explicitly for each range type.
 *
 * We actually define 4 functions, with 0 through 3 arguments.  This is just
 * to offer more convenience for the user.
 */
unsafe fn makeRangeConstructors(name: *const c_char, namespace: Oid,
                                 rangeOid: Oid, subtype: Oid) {
    static PROSRC: [&std::ffi::CStr; 2] = [
        c"range_constructor2",
        c"range_constructor3",
    ];
    static PRONARGS: [c_int; 2] = [2, 3];

    let mut constructorArgTypes: [Oid; 3] = [0; 3];
    let myself: ObjectAddress;
    let referenced: ObjectAddress;

    constructorArgTypes[0] = subtype;
    constructorArgTypes[1] = subtype;
    constructorArgTypes[2] = TEXTOID;

    let mut referenced: ObjectAddress = std::mem::zeroed();
    referenced.classId = TypeRelationId;
    referenced.objectId = rangeOid;
    referenced.objectSubId = 0;

    for i in 0..2usize {
        let constructorArgTypesVector: *mut oidvector;

        constructorArgTypesVector = buildoidvector(constructorArgTypes.as_ptr(), PRONARGS[i]);

        let myself = ProcedureCreate(
            name,                        /* name: same as range type */
            namespace,                   /* namespace */
            false,                       /* replace */
            false,                       /* returns set */
            rangeOid,                    /* return type */
            BOOTSTRAP_SUPERUSERID,       /* proowner */
            INTERNALlanguageId,          /* language */
            F_FMGR_INTERNAL_VALIDATOR,   /* language validator */
            PROSRC[i].as_ptr(),          /* prosrc */
            std::ptr::null(),            /* probin */
            std::ptr::null(),            /* prosqlbody */
            PROKIND_FUNCTION,
            false,                       /* security_definer */
            false,                       /* leakproof */
            false,                       /* isStrict */
            PROVOLATILE_IMMUTABLE,       /* volatility */
            PROPARALLEL_SAFE,            /* parallel safety */
            constructorArgTypesVector,   /* parameterTypes */
            PointerGetDatum(std::ptr::null()), /* allParameterTypes */
            PointerGetDatum(std::ptr::null()), /* parameterModes */
            PointerGetDatum(std::ptr::null()), /* parameterNames */
            std::ptr::null_mut(),        /* parameterDefaults */
            PointerGetDatum(std::ptr::null()), /* trftypes */
            std::ptr::null_mut(),        /* trfoids */
            PointerGetDatum(std::ptr::null()), /* proconfig */
            InvalidOid,                  /* prosupport */
            1.0f64,                      /* procost */
            0.0f64,                      /* prorows */
        );

        /*
         * Make the constructors internally-dependent on the range type so
         * that they go away silently when the type is dropped.  Note that
         * pg_dump depends on this choice to avoid dumping the constructors.
         */
        recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    }
}

/*
 * We make a separate multirange constructor for each range type
 * so its name can include the base type, like range constructors do.
 * If we had an anyrangearray polymorphic type we could use it here,
 * but since each type has its own constructor name there's no need.
 *
 * Sets castFuncOid to the oid of the new constructor that can be used
 * to cast from a range to a multirange.
 */
unsafe fn makeMultirangeConstructors(
    name: *const c_char, namespace: Oid,
    multirangeOid: Oid, rangeOid: Oid, rangeArrayOid: Oid,
    castFuncOid: *mut Oid,
) {
    let mut argtypes: *mut oidvector;
    let allParamTypes: Datum;
    let allParameterTypes: *mut ArrayType;
    let paramModes: Datum;
    let parameterModes: *mut ArrayType;

    let mut referenced: ObjectAddress = std::mem::zeroed();
    referenced.classId = TypeRelationId;
    referenced.objectId = multirangeOid;
    referenced.objectSubId = 0;

    /* 0-arg constructor - for empty multiranges */
    argtypes = buildoidvector(std::ptr::null(), 0);
    let myself = ProcedureCreate(
        name,                        /* name: same as multirange type */
        namespace,
        false,                       /* replace */
        false,                       /* returns set */
        multirangeOid,               /* return type */
        BOOTSTRAP_SUPERUSERID,       /* proowner */
        INTERNALlanguageId,          /* language */
        F_FMGR_INTERNAL_VALIDATOR,
        c"multirange_constructor0".as_ptr(), /* prosrc */
        std::ptr::null(),            /* probin */
        std::ptr::null(),            /* prosqlbody */
        PROKIND_FUNCTION,
        false,                       /* security_definer */
        false,                       /* leakproof */
        true,                        /* isStrict */
        PROVOLATILE_IMMUTABLE,       /* volatility */
        PROPARALLEL_SAFE,            /* parallel safety */
        argtypes,                    /* parameterTypes */
        PointerGetDatum(std::ptr::null()), /* allParameterTypes */
        PointerGetDatum(std::ptr::null()), /* parameterModes */
        PointerGetDatum(std::ptr::null()), /* parameterNames */
        std::ptr::null_mut(),        /* parameterDefaults */
        PointerGetDatum(std::ptr::null()), /* trftypes */
        std::ptr::null_mut(),        /* trfoids */
        PointerGetDatum(std::ptr::null()), /* proconfig */
        InvalidOid,                  /* prosupport */
        1.0f64,                      /* procost */
        0.0f64,                      /* prorows */
    );

    /*
     * Make the constructor internally-dependent on the multirange type so
     * that they go away silently when the type is dropped.  Note that pg_dump
     * depends on this choice to avoid dumping the constructors.
     */
    recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    pfree(argtypes as *mut c_void);

    /*
     * 1-arg constructor - for casts
     *
     * In theory we shouldn't need both this and the vararg (n-arg)
     * constructor, but having a separate 1-arg function lets us define casts
     * against it.
     */
    argtypes = buildoidvector(&rangeOid, 1);
    let myself = ProcedureCreate(
        name,                        /* name: same as multirange type */
        namespace,
        false,                       /* replace */
        false,                       /* returns set */
        multirangeOid,               /* return type */
        BOOTSTRAP_SUPERUSERID,       /* proowner */
        INTERNALlanguageId,          /* language */
        F_FMGR_INTERNAL_VALIDATOR,
        c"multirange_constructor1".as_ptr(), /* prosrc */
        std::ptr::null(),            /* probin */
        std::ptr::null(),            /* prosqlbody */
        PROKIND_FUNCTION,
        false,                       /* security_definer */
        false,                       /* leakproof */
        true,                        /* isStrict */
        PROVOLATILE_IMMUTABLE,       /* volatility */
        PROPARALLEL_SAFE,            /* parallel safety */
        argtypes,                    /* parameterTypes */
        PointerGetDatum(std::ptr::null()), /* allParameterTypes */
        PointerGetDatum(std::ptr::null()), /* parameterModes */
        PointerGetDatum(std::ptr::null()), /* parameterNames */
        std::ptr::null_mut(),        /* parameterDefaults */
        PointerGetDatum(std::ptr::null()), /* trftypes */
        std::ptr::null_mut(),        /* trfoids */
        PointerGetDatum(std::ptr::null()), /* proconfig */
        InvalidOid,                  /* prosupport */
        1.0f64,                      /* procost */
        0.0f64,                      /* prorows */
    );
    /* ditto */
    recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    pfree(argtypes as *mut c_void);
    *castFuncOid = myself.objectId;

    /* n-arg constructor - vararg */
    argtypes = buildoidvector(&rangeArrayOid, 1);
    allParamTypes = ObjectIdGetDatum(rangeArrayOid);
    allParameterTypes = construct_array_builtin(&allParamTypes, 1, OIDOID);
    paramModes = CharGetDatum(FUNC_PARAM_VARIADIC);
    parameterModes = construct_array_builtin(&paramModes, 1, CHAROID);
    let myself = ProcedureCreate(
        name,                        /* name: same as multirange type */
        namespace,
        false,                       /* replace */
        false,                       /* returns set */
        multirangeOid,               /* return type */
        BOOTSTRAP_SUPERUSERID,       /* proowner */
        INTERNALlanguageId,          /* language */
        F_FMGR_INTERNAL_VALIDATOR,
        c"multirange_constructor2".as_ptr(), /* prosrc */
        std::ptr::null(),            /* probin */
        std::ptr::null(),            /* prosqlbody */
        PROKIND_FUNCTION,
        false,                       /* security_definer */
        false,                       /* leakproof */
        true,                        /* isStrict */
        PROVOLATILE_IMMUTABLE,       /* volatility */
        PROPARALLEL_SAFE,            /* parallel safety */
        argtypes,                    /* parameterTypes */
        PointerGetDatum(allParameterTypes as *const c_void), /* allParameterTypes */
        PointerGetDatum(parameterModes as *const c_void),    /* parameterModes */
        PointerGetDatum(std::ptr::null()), /* parameterNames */
        std::ptr::null_mut(),        /* parameterDefaults */
        PointerGetDatum(std::ptr::null()), /* trftypes */
        std::ptr::null_mut(),        /* trfoids */
        PointerGetDatum(std::ptr::null()), /* proconfig */
        InvalidOid,                  /* prosupport */
        1.0f64,                      /* procost */
        0.0f64,                      /* prorows */
    );
    /* ditto */
    recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    pfree(argtypes as *mut c_void);
    pfree(allParameterTypes as *mut c_void);
    pfree(parameterModes as *mut c_void);
}

/*
 * Find suitable I/O and other support functions for a type.
 *
 * typeOid is the type's OID (which will already exist, if only as a shell
 * type).
 */

unsafe fn findTypeInputFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 3] = [0; 3];
    let procOid: Oid;
    let procOid2: Oid;

    /*
     * Input functions can take a single argument of type CSTRING, or three
     * arguments (string, typioparam OID, typmod).  Whine about ambiguity if
     * both forms exist.
     */
    argList[0] = CSTRINGOID;
    argList[1] = OIDOID;
    argList[2] = INT4OID;

    let mut procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    procOid2 = LookupFuncName(procname, 3, argList.as_ptr(), true);
    if OidIsValid(procOid) {
        if OidIsValid(procOid2) {
            ereport!(ERROR, errmsg!("type input function {} has multiple matches",
                std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
            /* C also: errcode(ERRCODE_AMBIGUOUS_FUNCTION) */
        }
    } else {
        procOid = procOid2;
        /* If not found, reference the 1-argument signature in error msg */
        if !OidIsValid(procOid) {
            ereport!(ERROR, errmsg!("function {} does not exist",
                std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
        }
    }

    /* Input functions must return the target type. */
    if get_func_rettype(procOid) != typeOid {
        ereport!(ERROR, errmsg!("type input function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * Print warnings if any of the type's I/O functions are marked volatile.
     * There is a general assumption that I/O functions are stable or
     * immutable; this allows us for example to mark record_in/record_out
     * stable rather than volatile.  Ideally we would throw errors not just
     * warnings here; but since this check is new as of 9.5, and since the
     * volatility marking might be just an error-of-omission and not a true
     * indication of how the function behaves, we'll let it pass as a warning
     * for now.
     */
    if func_volatile(procOid) == PROVOLATILE_VOLATILE {
        ereport!(WARNING, errmsg!("type input function {} should not be volatile",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeOutputFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];
    let procOid: Oid;

    /*
     * Output functions always take a single argument of the type and return
     * cstring.
     */
    argList[0] = typeOid;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != CSTRINGOID {
        ereport!(ERROR, errmsg!("type output function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(), "cstring"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Just a warning for now, per comments in findTypeInputFunction */
    if func_volatile(procOid) == PROVOLATILE_VOLATILE {
        ereport!(WARNING, errmsg!("type output function {} should not be volatile",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeReceiveFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 3] = [0; 3];
    let procOid: Oid;
    let procOid2: Oid;

    /*
     * Receive functions can take a single argument of type INTERNAL, or three
     * arguments (internal, typioparam OID, typmod).  Whine about ambiguity if
     * both forms exist.
     */
    argList[0] = INTERNALOID;
    argList[1] = OIDOID;
    argList[2] = INT4OID;

    let mut procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    let procOid2 = LookupFuncName(procname, 3, argList.as_ptr(), true);
    if OidIsValid(procOid) {
        if OidIsValid(procOid2) {
            ereport!(ERROR, errmsg!("type receive function {} has multiple matches",
                std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
            /* C also: errcode(ERRCODE_AMBIGUOUS_FUNCTION) */
        }
    } else {
        procOid = procOid2;
        /* If not found, reference the 1-argument signature in error msg */
        if !OidIsValid(procOid) {
            ereport!(ERROR, errmsg!("function {} does not exist",
                std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
        }
    }

    /* Receive functions must return the target type. */
    if get_func_rettype(procOid) != typeOid {
        ereport!(ERROR, errmsg!("type receive function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Just a warning for now, per comments in findTypeInputFunction */
    if func_volatile(procOid) == PROVOLATILE_VOLATILE {
        ereport!(WARNING, errmsg!("type receive function {} should not be volatile",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeSendFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];

    /*
     * Send functions always take a single argument of the type and return
     * bytea.
     */
    argList[0] = typeOid;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != BYTEAOID {
        ereport!(ERROR, errmsg!("type send function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(), "bytea"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Just a warning for now, per comments in findTypeInputFunction */
    if func_volatile(procOid) == PROVOLATILE_VOLATILE {
        ereport!(WARNING, errmsg!("type send function {} should not be volatile",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeTypmodinFunction(procname: *mut List) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];

    /*
     * typmodin functions always take one cstring[] argument and return int4.
     */
    argList[0] = CSTRINGARRAYOID;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != INT4OID {
        ereport!(ERROR, errmsg!("typmod_in function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(), "integer"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Just a warning for now, per comments in findTypeInputFunction */
    if func_volatile(procOid) == PROVOLATILE_VOLATILE {
        ereport!(WARNING, errmsg!("type modifier input function {} should not be volatile",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeTypmodoutFunction(procname: *mut List) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];

    /*
     * typmodout functions always take one int4 argument and return cstring.
     */
    argList[0] = INT4OID;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != CSTRINGOID {
        ereport!(ERROR, errmsg!("typmod_out function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(), "cstring"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Just a warning for now, per comments in findTypeInputFunction */
    if func_volatile(procOid) == PROVOLATILE_VOLATILE {
        ereport!(WARNING, errmsg!("type modifier output function {} should not be volatile",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeAnalyzeFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];

    /*
     * Analyze functions always take one INTERNAL argument and return bool.
     */
    argList[0] = INTERNALOID;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != BOOLOID {
        ereport!(ERROR, errmsg!("type analyze function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(), "boolean"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

unsafe fn findTypeSubscriptingFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];

    /*
     * Subscripting support functions always take one INTERNAL argument and
     * return INTERNAL.  (The argument is not used, but we must have it to
     * maintain type safety.)
     */
    argList[0] = INTERNALOID;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != INTERNALOID {
        ereport!(ERROR, errmsg!("type subscripting function {} must return type {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy(), "internal"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * We disallow array_subscript_handler() from being selected explicitly,
     * since that must only be applied to autogenerated array types.
     */
    if procOid == F_ARRAY_SUBSCRIPT_HANDLER {
        ereport!(ERROR, errmsg!("user-defined types cannot use subscripting function {}",
            std::ffi::CStr::from_ptr(NameListToString(procname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    procOid
}

/*
 * Find suitable support functions and opclasses for a range type.
 */

/*
 * Find named btree opclass for subtype, or default btree opclass if
 * opcname is NIL.
 */
unsafe fn findRangeSubOpclass(opcname: *mut List, subtype: Oid) -> Oid {
    let opcid: Oid;
    let opInputType: Oid;

    if !opcname.is_null() {
        opcid = get_opclass_oid(BTREE_AM_OID, opcname, false);

        /*
         * Verify that the operator class accepts this datatype. Note we will
         * accept binary compatibility.
         */
        opInputType = get_opclass_input_type(opcid);
        if !IsBinaryCoercible(subtype, opInputType) {
            ereport!(ERROR, errmsg!("operator class \"{}\" does not accept data type {}",
                std::ffi::CStr::from_ptr(NameListToString(opcname)).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(subtype)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        }
    } else {
        opcid = GetDefaultOpClass(subtype, BTREE_AM_OID);
        if !OidIsValid(opcid) {
            /* We spell the error message identically to ResolveOpClass */
            ereport!(ERROR, errmsg!("data type {} has no default operator class for access method \"{}\"",
                std::ffi::CStr::from_ptr(format_type_be(subtype)).to_string_lossy(), "btree"));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT),
             * errhint("You must specify an operator class for the range type or define a default operator class for the subtype.") */
        }
    }

    opcid
}

unsafe fn findRangeCanonicalFunction(procname: *mut List, typeOid: Oid) -> Oid {
    let mut argList: [Oid; 1] = [0; 1];
    let aclresult: AclResult;

    /*
     * Range canonical functions must take and return the range type, and must
     * be immutable.
     */
    argList[0] = typeOid;

    let procOid = LookupFuncName(procname, 1, argList.as_ptr(), true);

    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != typeOid {
        ereport!(ERROR, errmsg!("range canonical function {} must return range type",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if func_volatile(procOid) != PROVOLATILE_IMMUTABLE {
        ereport!(ERROR, errmsg!("range canonical function {} must be immutable",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 1, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Also, range type's creator must have permission to call function */
    aclresult = object_aclcheck(ProcedureRelationId, procOid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(procOid));
    }

    procOid
}

unsafe fn findRangeSubtypeDiffFunction(procname: *mut List, subtype: Oid) -> Oid {
    let mut argList: [Oid; 2] = [0; 2];
    let aclresult: AclResult;

    /*
     * Range subtype diff functions must take two arguments of the subtype,
     * must return float8, and must be immutable.
     */
    argList[0] = subtype;
    argList[1] = subtype;

    let procOid = LookupFuncName(procname, 2, argList.as_ptr(), true);

    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 2, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != FLOAT8OID {
        ereport!(ERROR, errmsg!("range subtype diff function {} must return type {}",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 2, std::ptr::null(), argList.as_ptr())).to_string_lossy(),
            "double precision"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if func_volatile(procOid) != PROVOLATILE_IMMUTABLE {
        ereport!(ERROR, errmsg!("range subtype diff function {} must be immutable",
            std::ffi::CStr::from_ptr(func_signature_string(procname, 2, std::ptr::null(), argList.as_ptr())).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* Also, range type's creator must have permission to call function */
    aclresult = object_aclcheck(ProcedureRelationId, procOid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(procOid));
    }

    procOid
}

/*
 * AssignTypeArrayOid
 *
 * Pre-assign the type's array OID for use in pg_type.typarray
 */
pub unsafe fn AssignTypeArrayOid_impl() -> Oid {
    let type_array_oid: Oid;

    /* Use binary-upgrade override for pg_type.typarray? */
    if IsBinaryUpgrade() {
        if !OidIsValid(binary_upgrade_next_array_pg_type_oid) {
            ereport!(ERROR, errmsg!("pg_type array OID value not set when in binary upgrade mode"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        type_array_oid = binary_upgrade_next_array_pg_type_oid;
        binary_upgrade_next_array_pg_type_oid = InvalidOid;
    } else {
        let pg_type = table_open(TypeRelationId, AccessShareLock);

        type_array_oid = GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid);
        table_close(pg_type, AccessShareLock);
    }

    type_array_oid
}

/*
 * AssignTypeMultirangeOid
 *
 * Pre-assign the range type's multirange OID for use in pg_type.oid
 */
pub unsafe fn AssignTypeMultirangeOid_impl() -> Oid {
    let type_multirange_oid: Oid;

    /* Use binary-upgrade override for pg_type.oid? */
    if IsBinaryUpgrade() {
        if !OidIsValid(binary_upgrade_next_mrng_pg_type_oid) {
            ereport!(ERROR, errmsg!("pg_type multirange OID value not set when in binary upgrade mode"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        type_multirange_oid = binary_upgrade_next_mrng_pg_type_oid;
        binary_upgrade_next_mrng_pg_type_oid = InvalidOid;
    } else {
        let pg_type = table_open(TypeRelationId, AccessShareLock);

        type_multirange_oid = GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid);
        table_close(pg_type, AccessShareLock);
    }

    type_multirange_oid
}

/*
 * AssignTypeMultirangeArrayOid
 *
 * Pre-assign the range type's multirange array OID for use in pg_type.typarray
 */
pub unsafe fn AssignTypeMultirangeArrayOid_impl() -> Oid {
    let type_multirange_array_oid: Oid;

    /* Use binary-upgrade override for pg_type.oid? */
    if IsBinaryUpgrade() {
        if !OidIsValid(binary_upgrade_next_mrng_array_pg_type_oid) {
            ereport!(ERROR, errmsg!("pg_type multirange array OID value not set when in binary upgrade mode"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        type_multirange_array_oid = binary_upgrade_next_mrng_array_pg_type_oid;
        binary_upgrade_next_mrng_array_pg_type_oid = InvalidOid;
    } else {
        let pg_type = table_open(TypeRelationId, AccessShareLock);

        type_multirange_array_oid = GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid);
        table_close(pg_type, AccessShareLock);
    }

    type_multirange_array_oid
}

/* =========================================================================
 * Part 5: DefineCompositeType, AlterDomainDefault, AlterDomainNotNull,
 *         AlterDomainDropConstraint, AlterDomainAddConstraint,
 *         AlterDomainValidateConstraint, validateDomainNotNullConstraint,
 *         validateDomainCheckConstraint, get_rels_with_domain, checkDomainOwner
 * ========================================================================= */

/*-------------------------------------------------------------------
 * DefineCompositeType
 *
 * Create a Composite Type relation.
 * `DefineRelation' does all the work, we just provide the correct
 * arguments!
 *
 * If the relation already exists, then 'DefineRelation' will abort
 * the xact...
 *
 * Return type is the new type's object address.
 *-------------------------------------------------------------------
 */
pub unsafe fn DefineCompositeType(typevar: *mut RangeVar, coldeflist: *mut List) -> ObjectAddress {
    let createStmt: *mut CreateStmt = makeNode!(CreateStmt, T_CreateStmt) as *mut CreateStmt;
    let old_type_oid: Oid;
    let typeNamespace: Oid;
    let address: ObjectAddress;

    /*
     * now set the parameters for keys/inheritance etc. All of these are
     * uninteresting for composite types...
     */
    (*createStmt).relation = typevar;
    (*createStmt).tableElts = coldeflist;
    (*createStmt).inhRelations = std::ptr::null_mut(); /* NIL */
    (*createStmt).constraints = std::ptr::null_mut(); /* NIL */
    (*createStmt).options = std::ptr::null_mut(); /* NIL */
    (*createStmt).oncommit = ONCOMMIT_NOOP;
    (*createStmt).tablespacename = std::ptr::null_mut();
    (*createStmt).if_not_exists = false;

    /*
     * Check for collision with an existing type name. If there is one and
     * it's an autogenerated array, we can rename it out of the way.  This
     * check is here mainly to get a better error message about a "type"
     * instead of below about a "relation".
     */
    typeNamespace = RangeVarGetAndCheckCreationNamespace((*createStmt).relation,
                                                          NoLock, std::ptr::null_mut());
    RangeVarAdjustRelationPersistence((*createStmt).relation, typeNamespace);
    old_type_oid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum((*(*createStmt).relation).relname),
        ObjectIdGetDatum(typeNamespace),
    );
    if OidIsValid(old_type_oid) {
        if !moveArrayTypeName(old_type_oid, (*(*createStmt).relation).relname, typeNamespace) {
            ereport!(ERROR, errmsg!("type \"{}\" already exists",
                std::ffi::CStr::from_ptr((*(*createStmt).relation).relname).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /*
     * Finally create the relation.  This also creates the type.
     */
    let mut address: ObjectAddress = std::mem::zeroed();
    DefineRelation(createStmt, RELKIND_COMPOSITE_TYPE, InvalidOid, &mut address,
                   std::ptr::null());

    address
}

/*
 * AlterDomainDefault
 *
 * Routine implementing ALTER DOMAIN SET/DROP DEFAULT statements.
 *
 * Returns ObjectAddress of the modified domain.
 */
pub unsafe fn AlterDomainDefault(names: *mut List, defaultRaw: *mut Node) -> ObjectAddress {
    let typename: *mut TypeName;
    let domainoid: Oid;
    let tup: HeapTuple;
    let pstate: *mut ParseState;
    let rel: Relation;
    let mut defaultValue: *mut c_char = std::ptr::null_mut();
    let mut defaultExpr: *mut Node = std::ptr::null_mut(); /* NULL if no default specified */
    let mut new_record: [Datum; Natts_pg_type] = [0; Natts_pg_type];
    let mut new_record_nulls: [bool; Natts_pg_type] = [false; Natts_pg_type];
    let mut new_record_repl: [bool; Natts_pg_type] = [false; Natts_pg_type];
    let newtuple: HeapTuple;
    let typTup: Form_pg_type;

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Look up the domain in the type table */
    rel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", domainoid));
    }
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /* Setup new tuple */

    /* Store the new default into the tuple */
    if !defaultRaw.is_null() {
        /* Create a dummy ParseState for transformExpr */
        pstate = make_parsestate(std::ptr::null_mut());

        /*
         * Cook the colDef->raw_expr into an expression. Note: Name is
         * strictly for error message
         */
        defaultExpr = cookDefault(pstate, defaultRaw,
                                  (*(typTup as *mut FormData_pg_type_fields)).typbasetype,
                                  (*(typTup as *mut FormData_pg_type_fields)).typtypmod,
                                  NameStr(std::ptr::addr_of!((*(typTup as *mut FormData_pg_type_fields)).typname) as *const c_void),
                                  0);

        /*
         * If the expression is just a NULL constant, we treat the command
         * like ALTER ... DROP DEFAULT.  (But see note for same test in
         * DefineDomain.)
         */
        if defaultExpr.is_null()
            || (IsA!(defaultExpr, T_Const)
                && (*(defaultExpr as *const ConstNode)).constisnull)
        {
            /* Default is NULL, drop it */
            defaultExpr = std::ptr::null_mut();
            new_record_nulls[Anum_pg_type_typdefaultbin as usize - 1] = true;
            new_record_repl[Anum_pg_type_typdefaultbin as usize - 1] = true;
            new_record_nulls[Anum_pg_type_typdefault as usize - 1] = true;
            new_record_repl[Anum_pg_type_typdefault as usize - 1] = true;
        } else {
            /*
             * Expression must be stored as a nodeToString result, but we also
             * require a valid textual representation (mainly to make life
             * easier for pg_dump).
             */
            defaultValue = deparse_expression(defaultExpr, std::ptr::null(), false, false);

            /*
             * Form an updated tuple with the new default and write it back.
             */
            new_record[Anum_pg_type_typdefaultbin as usize - 1] =
                CStringGetTextDatum(nodeToString(defaultExpr));

            new_record_repl[Anum_pg_type_typdefaultbin as usize - 1] = true;
            new_record[Anum_pg_type_typdefault as usize - 1] =
                CStringGetTextDatum(defaultValue);
            new_record_repl[Anum_pg_type_typdefault as usize - 1] = true;
        }
    } else {
        /* ALTER ... DROP DEFAULT */
        new_record_nulls[Anum_pg_type_typdefaultbin as usize - 1] = true;
        new_record_repl[Anum_pg_type_typdefaultbin as usize - 1] = true;
        new_record_nulls[Anum_pg_type_typdefault as usize - 1] = true;
        new_record_repl[Anum_pg_type_typdefault as usize - 1] = true;
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel),
                                  new_record.as_ptr(), new_record_nulls.as_ptr(),
                                  new_record_repl.as_ptr());

    CatalogTupleUpdate(rel, &mut (*(newtuple as *mut HeapTupleDataFull)).t_self as *mut ItemPointerData, newtuple);

    /* Rebuild dependencies */
    GenerateTypeDependencies(newtuple,
                             rel,
                             defaultExpr,
                             std::ptr::null_mut(), /* don't have typacl handy */
                             0,                    /* relation kind is n/a */
                             false,                /* a domain isn't an implicit array */
                             false,                /* nor is it any kind of dependent type */
                             false,                /* don't touch extension membership */
                             true);               /* We do need to rebuild dependencies */

    InvokeObjectPostAlterHook(TypeRelationId, domainoid, 0);

    let mut address: ObjectAddress = std::mem::zeroed();
    ObjectAddressSet(&mut address, TypeRelationId, domainoid);

    /* Clean up */
    table_close(rel, RowExclusiveLock);
    heap_freetuple(newtuple);

    address
}

/*
 * AlterDomainNotNull
 *
 * Routine implementing ALTER DOMAIN SET/DROP NOT NULL statements.
 *
 * Returns ObjectAddress of the modified domain.
 */
pub unsafe fn AlterDomainNotNull(names: *mut List, notNull: bool) -> ObjectAddress {
    let typename: *mut TypeName;
    let domainoid: Oid;
    let typrel: Relation;
    let tup: HeapTuple;
    let typTup: Form_pg_type;
    let mut address: ObjectAddress = std::mem::zeroed(); /* = InvalidObjectAddress */

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Look up the domain in the type table */
    typrel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", domainoid));
    }
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /* Is the domain already set to the desired constraint? */
    if (*(typTup as *mut FormData_pg_type_fields)).typnotnull == notNull {
        table_close(typrel, RowExclusiveLock);
        return address;
    }

    if notNull {
        let constr: *mut Constraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*constr).contype = CONSTR_NOTNULL;
        (*constr).initially_valid = true;
        (*constr).location = -1;

        domainAddNotNullConstraint(domainoid, (*(typTup as *mut FormData_pg_type_fields)).typnamespace,
                                   (*(typTup as *mut FormData_pg_type_fields)).typbasetype,
                                   (*(typTup as *mut FormData_pg_type_fields)).typtypmod,
                                   constr,
                                   NameStr(std::ptr::addr_of!((*(typTup as *mut FormData_pg_type_fields)).typname) as *const c_void),
                                   std::ptr::null_mut());

        validateDomainNotNullConstraint(domainoid);
    } else {
        let conTup: HeapTuple;
        let mut conobj: ObjectAddress = std::mem::zeroed();

        conTup = findDomainNotNullConstraint(domainoid);
        if conTup.is_null() {
            ereport!(ERROR, errmsg!("could not find not-null constraint on domain \"{}\"",
                std::ffi::CStr::from_ptr(NameStr(std::ptr::addr_of!((*(typTup as *mut FormData_pg_type_fields)).typname) as *const c_void)).to_string_lossy()));
        }

        ObjectAddressSet(&mut conobj, ConstraintRelationId,
                         (*(GETSTRUCT(conTup) as Form_pg_constraint as *mut FormData_pg_constraint_oid)).oid);
        performDeletion(&conobj, DROP_RESTRICT, 0);
    }

    /*
     * Okay to update pg_type row.  We can scribble on typTup because it's a
     * copy.
     */
    (*(typTup as *mut FormData_pg_type_fields)).typnotnull = notNull;

    CatalogTupleUpdate(typrel, &mut (*(tup as *mut HeapTupleDataFull)).t_self as *mut ItemPointerData, tup);

    InvokeObjectPostAlterHook(TypeRelationId, domainoid, 0);

    ObjectAddressSet(&mut address, TypeRelationId, domainoid);

    /* Clean up */
    heap_freetuple(tup);
    table_close(typrel, RowExclusiveLock);

    address
}

/* Helper stub for pg_constraint oid field */
#[repr(C)] struct FormData_pg_constraint_oid { _pad: [u8; 0], oid: Oid }

/*
 * AlterDomainDropConstraint
 *
 * Implements the ALTER DOMAIN DROP CONSTRAINT statement
 *
 * Returns ObjectAddress of the modified domain.
 */
pub unsafe fn AlterDomainDropConstraint(
    names: *mut List,
    constrName: *const c_char,
    behavior: c_int,
    missing_ok: bool,
) -> ObjectAddress {
    let typename: *mut TypeName;
    let domainoid: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let conrel: Relation;
    let conscan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = unsafe { std::mem::zeroed() };
    let contup: HeapTuple;
    let mut found: bool = false;

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Look up the domain in the type table */
    rel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", domainoid));
    }

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /* Grab an appropriate lock on the pg_constraint relation */
    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Find and remove the target constraint */
    ScanKeyInit(&mut skey[0],
                Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&mut skey[1],
                Anum_pg_constraint_contypid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(domainoid));
    ScanKeyInit(&mut skey[2],
                Anum_pg_constraint_conname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(constrName));

    conscan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true,
                                  std::ptr::null_mut(), 3, skey.as_ptr());

    /* There can be at most one matching row */
    let contup = systable_getnext(conscan);
    if !contup.is_null() {
        let construct: Form_pg_constraint = GETSTRUCT(contup) as Form_pg_constraint;
        let mut conobj: ObjectAddress = std::mem::zeroed();

        if (*(construct as *mut FormData_pg_constraint_type)).contype == CONSTRAINT_NOTNULL {
            (*(GETSTRUCT(tup) as Form_pg_type as *mut FormData_pg_type_fields)).typnotnull = false;
            CatalogTupleUpdate(rel, &mut (*(tup as *mut HeapTupleDataFull)).t_self as *mut ItemPointerData, tup);
        }

        conobj.classId = ConstraintRelationId;
        conobj.objectId = (*(construct as *mut FormData_pg_constraint_oid)).oid;
        conobj.objectSubId = 0;

        performDeletion(&conobj, behavior, 0);
        found = true;
    }

    /* Clean up after the scan */
    systable_endscan(conscan);
    table_close(conrel, RowExclusiveLock);

    if !found {
        if !missing_ok {
            ereport!(ERROR, errmsg!("constraint \"{}\" of domain \"{}\" does not exist",
                std::ffi::CStr::from_ptr(constrName).to_string_lossy(),
                std::ffi::CStr::from_ptr(TypeNameToString(typename)).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        } else {
            ereport!(NOTICE, errmsg!("constraint \"{}\" of domain \"{}\" does not exist, skipping",
                std::ffi::CStr::from_ptr(constrName).to_string_lossy(),
                std::ffi::CStr::from_ptr(TypeNameToString(typename)).to_string_lossy()));
        }
    }

    /*
     * We must send out an sinval message for the domain, to ensure that any
     * dependent plans get rebuilt.  Since this command doesn't change the
     * domain's pg_type row, that won't happen automatically; do it manually.
     */
    CacheInvalidateHeapTuple(rel, tup, std::ptr::null_mut());

    let mut address: ObjectAddress = std::mem::zeroed();
    ObjectAddressSet(&mut address, TypeRelationId, domainoid);

    /* Clean up */
    table_close(rel, RowExclusiveLock);

    address
}

/* Helper stub for contype access */
#[repr(C)] struct FormData_pg_constraint_type { _pad: [u8; 8], contype: c_char }

/*
 * AlterDomainAddConstraint
 *
 * Implements the ALTER DOMAIN .. ADD CONSTRAINT statement.
 */
pub unsafe fn AlterDomainAddConstraint(
    names: *mut List,
    newConstraint: *mut Node,
    constrAddr: *mut ObjectAddress,
) -> ObjectAddress {
    let typename: *mut TypeName;
    let domainoid: Oid;
    let typrel: Relation;
    let tup: HeapTuple;
    let typTup: Form_pg_type;
    let constr: *mut Constraint;
    let ccbin: *mut c_char;
    let mut address: ObjectAddress = std::mem::zeroed(); /* = InvalidObjectAddress */

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Look up the domain in the type table */
    typrel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", domainoid));
    }
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    if !IsA!(newConstraint, T_Constraint) {
        ereport!(ERROR, errmsg!("unrecognized node type: {}", nodeTag(newConstraint)));
    }

    constr = newConstraint as *mut Constraint;

    /* enforced by parser */
    /* Assert(constr->contype == CONSTR_CHECK || constr->contype == CONSTR_NOTNULL); */

    if (*constr).contype == CONSTR_CHECK {
        /*
         * First, process the constraint expression and add an entry to
         * pg_constraint.
         */

        ccbin = domainAddCheckConstraint(domainoid,
                                          (*(typTup as *mut FormData_pg_type_fields)).typnamespace,
                                          (*(typTup as *mut FormData_pg_type_fields)).typbasetype,
                                          (*(typTup as *mut FormData_pg_type_fields)).typtypmod,
                                          constr,
                                          NameStr(std::ptr::addr_of!((*(typTup as *mut FormData_pg_type_fields)).typname) as *const c_void),
                                          constrAddr);

        /*
         * If requested to validate the constraint, test all values stored in
         * the attributes based on the domain the constraint is being added
         * to.
         */
        if !(*constr).skip_validation {
            validateDomainCheckConstraint(domainoid, ccbin);
        }

        /*
         * We must send out an sinval message for the domain, to ensure that
         * any dependent plans get rebuilt.  Since this command doesn't change
         * the domain's pg_type row, that won't happen automatically; do it
         * manually.
         */
        CacheInvalidateHeapTuple(typrel, tup, std::ptr::null_mut());
    } else if (*constr).contype == CONSTR_NOTNULL {
        /* Is the domain already set NOT NULL? */
        if (*(typTup as *mut FormData_pg_type_fields)).typnotnull {
            table_close(typrel, RowExclusiveLock);
            return address;
        }
        domainAddNotNullConstraint(domainoid,
                                   (*(typTup as *mut FormData_pg_type_fields)).typnamespace,
                                   (*(typTup as *mut FormData_pg_type_fields)).typbasetype,
                                   (*(typTup as *mut FormData_pg_type_fields)).typtypmod,
                                   constr,
                                   NameStr(std::ptr::addr_of!((*(typTup as *mut FormData_pg_type_fields)).typname) as *const c_void),
                                   constrAddr);

        if !(*constr).skip_validation {
            validateDomainNotNullConstraint(domainoid);
        }

        (*(typTup as *mut FormData_pg_type_fields)).typnotnull = true;
        CatalogTupleUpdate(typrel, &mut (*(tup as *mut HeapTupleDataFull)).t_self as *mut ItemPointerData, tup);
    }

    ObjectAddressSet(&mut address, TypeRelationId, domainoid);

    /* Clean up */
    table_close(typrel, RowExclusiveLock);

    address
}

/*
 * AlterDomainValidateConstraint
 *
 * Implements the ALTER DOMAIN .. VALIDATE CONSTRAINT statement.
 */
pub unsafe fn AlterDomainValidateConstraint(names: *mut List, constrName: *const c_char)
    -> ObjectAddress
{
    let typename: *mut TypeName;
    let domainoid: Oid;
    let typrel: Relation;
    let conrel: Relation;
    let tup: HeapTuple;
    let con: Form_pg_constraint;
    let copy_con: Form_pg_constraint;
    let conbin: *mut c_char;
    let scan: SysScanDesc;
    let val: Datum;
    let tuple: HeapTuple;
    let copyTuple: HeapTuple;
    let mut skey: [ScanKeyData; 3] = unsafe { std::mem::zeroed() };

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Look up the domain in the type table */
    typrel = table_open(TypeRelationId, AccessShareLock);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(domainoid));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("cache lookup failed for type {}", domainoid));
    }

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /*
     * Find and check the target constraint
     */
    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    ScanKeyInit(&mut skey[0],
                Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&mut skey[1],
                Anum_pg_constraint_contypid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(domainoid));
    ScanKeyInit(&mut skey[2],
                Anum_pg_constraint_conname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(constrName));

    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true,
                               std::ptr::null_mut(), 3, skey.as_ptr());

    /* There can be at most one matching row */
    let tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!("constraint \"{}\" of domain \"{}\" does not exist",
            std::ffi::CStr::from_ptr(constrName).to_string_lossy(),
            std::ffi::CStr::from_ptr(TypeNameToString(typename)).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    let con = GETSTRUCT(tuple) as Form_pg_constraint;
    if (*(con as *mut FormData_pg_constraint_type)).contype != CONSTRAINT_CHECK {
        ereport!(ERROR, errmsg!("constraint \"{}\" of domain \"{}\" is not a check constraint",
            std::ffi::CStr::from_ptr(constrName).to_string_lossy(),
            std::ffi::CStr::from_ptr(TypeNameToString(typename)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    val = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conbin);
    conbin = TextDatumGetCString(val);

    validateDomainCheckConstraint(domainoid, conbin);

    /*
     * Now update the catalog, while we have the door open.
     */
    copyTuple = heap_copytuple(tuple);
    let copy_con = GETSTRUCT(copyTuple) as Form_pg_constraint;
    (*(copy_con as *mut FormData_pg_constraint_validated)).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*(copyTuple as *mut HeapTupleDataFull)).t_self as *mut ItemPointerData, copyTuple);

    InvokeObjectPostAlterHook(ConstraintRelationId,
        (*(con as *mut FormData_pg_constraint_oid)).oid, 0);

    let mut address: ObjectAddress = std::mem::zeroed();
    ObjectAddressSet(&mut address, TypeRelationId, domainoid);

    heap_freetuple(copyTuple);

    systable_endscan(scan);

    table_close(typrel, AccessShareLock);
    table_close(conrel, RowExclusiveLock);

    ReleaseSysCache(tup);

    address
}

/* Helper stub for convalidated field */
#[repr(C)] struct FormData_pg_constraint_validated { _pad: [u8; 16], convalidated: bool }

/*
 * Verify that all columns currently using the domain are not null.
 */
unsafe fn validateDomainNotNullConstraint(domainoid: Oid) {
    let rels: *mut List;

    /* Fetch relation list with attributes based on this domain */
    /* ShareLock is sufficient to prevent concurrent data changes */

    rels = get_rels_with_domain(domainoid, ShareLock);

    foreach!(rt, rels, {
        let rtc: *mut RelToCheck = lfirst(crate::current_cell!(rt)) as *mut RelToCheck;
        let testrel: Relation = (*rtc).rel;
        let tupdesc: TupleDesc = RelationGetDescr(testrel);
        let slot: TupleTableSlot;
        let scan: TableScanDesc;
        let snapshot: Snapshot;

        /* Scan all tuples in this relation */
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        scan = table_beginscan(testrel, snapshot, 0, std::ptr::null());
        slot = table_slot_create(testrel, std::ptr::null_mut());
        while table_scan_getnextslot(scan, ForwardScanDirection, slot) {
            /* Test attributes that are of the domain */
            for i in 0..(*rtc).natts {
                let attnum: c_int = *(*rtc).atts.add(i as usize);
                let attr: Form_pg_attribute = TupleDescAttr(tupdesc, attnum - 1);

                if slot_attisnull(slot, attnum) {
                    /*
                     * In principle the auxiliary information for this error
                     * should be errdatatype(), but errtablecol() seems
                     * considerably more useful in practice.  Since this code
                     * only executes in an ALTER DOMAIN command, the client
                     * should already know which domain is in question.
                     */
                    ereport!(ERROR, errmsg!("column \"{}\" of table \"{}\" contains null values",
                        std::ffi::CStr::from_ptr(NameStr(std::ptr::addr_of!((*(attr as *mut FormData_pg_attribute_name)).attname) as *const c_void)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(RelationGetRelationName(testrel)).to_string_lossy()));
                    /* C also: errcode(ERRCODE_NOT_NULL_VIOLATION), errtablecol(testrel, attnum) */
                }
            }
        }
        ExecDropSingleTupleTableSlot(slot);
        table_endscan(scan);
        UnregisterSnapshot(snapshot);

        /* Close each rel after processing, but keep lock */
        table_close(testrel, NoLock);
    });
}

/* Helper stub for attname field */
#[repr(C)] struct FormData_pg_attribute_name { _pad: [u8; 0], attname: [u8; 64] }

/*
 * Verify that all columns currently using the domain satisfy the given check
 * constraint expression.
 */
unsafe fn validateDomainCheckConstraint(domainoid: Oid, ccbin: *const c_char) {
    let expr: *mut Expr = stringToNode(ccbin) as *mut Expr;
    let rels: *mut List;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let exprstate: *mut ExprState;

    /* Need an EState to run ExecEvalExpr */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);

    /* build execution state for expr */
    exprstate = ExecPrepareExpr(expr, estate);

    /* Fetch relation list with attributes based on this domain */
    /* ShareLock is sufficient to prevent concurrent data changes */

    rels = get_rels_with_domain(domainoid, ShareLock);

    foreach!(rt, rels, {
        let rtc: *mut RelToCheck = lfirst(crate::current_cell!(rt)) as *mut RelToCheck;
        let testrel: Relation = (*rtc).rel;
        let tupdesc: TupleDesc = RelationGetDescr(testrel);
        let slot: TupleTableSlot;
        let scan: TableScanDesc;
        let snapshot: Snapshot;

        /* Scan all tuples in this relation */
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        scan = table_beginscan(testrel, snapshot, 0, std::ptr::null());
        slot = table_slot_create(testrel, std::ptr::null_mut());
        while table_scan_getnextslot(scan, ForwardScanDirection, slot) {
            /* Test attributes that are of the domain */
            for i in 0..(*rtc).natts {
                let attnum: c_int = *(*rtc).atts.add(i as usize);
                let d: Datum;
                let mut isNull: bool = false;
                let conResult: Datum;
                let attr: Form_pg_attribute = TupleDescAttr(tupdesc, attnum - 1);

                d = slot_getattr(slot, attnum, &mut isNull);

                (*econtext).domainValue_datum = d;
                (*econtext).domainValue_isNull = isNull;

                conResult = ExecEvalExprSwitchContext(exprstate, econtext, &mut isNull);

                if !isNull && !DatumGetBool(conResult) {
                    /*
                     * In principle the auxiliary information for this error
                     * should be errdomainconstraint(), but errtablecol()
                     * seems considerably more useful in practice.  Since this
                     * code only executes in an ALTER DOMAIN command, the
                     * client should already know which domain is in question,
                     * and which constraint too.
                     */
                    ereport!(ERROR, errmsg!("column \"{}\" of table \"{}\" contains values that violate the new constraint",
                        std::ffi::CStr::from_ptr(NameStr(std::ptr::addr_of!((*(attr as *mut FormData_pg_attribute_name)).attname) as *const c_void)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(RelationGetRelationName(testrel)).to_string_lossy()));
                    /* C also: errcode(ERRCODE_CHECK_VIOLATION), errtablecol(testrel, attnum) */
                }
            }

            ResetExprContext(econtext);
        }
        ExecDropSingleTupleTableSlot(slot);
        table_endscan(scan);
        UnregisterSnapshot(snapshot);

        /* Hold relation lock till commit (XXX bad for concurrency) */
        table_close(testrel, NoLock);
    });

    FreeExecutorState(estate);
}

/*
 * get_rels_with_domain
 *
 * Fetch all relations / attributes which are using the domain
 *
 * The result is a list of RelToCheck structs, one for each distinct
 * relation, each containing one or more attribute numbers that are of
 * the domain type.  We have opened each rel and acquired the specified lock
 * type on it.
 *
 * We support nested domains by including attributes that are of derived
 * domain types.  Current callers do not need to distinguish between attributes
 * that are of exactly the given domain and those that are of derived domains.
 *
 * XXX this is completely broken because there is no way to lock the domain
 * to prevent columns from being added or dropped while our command runs.
 * We can partially protect against column drops by locking relations as we
 * come across them, but there is still a race condition (the window between
 * seeing a pg_depend entry and acquiring lock on the relation it references).
 * Also, holding locks on all these relations simultaneously creates a non-
 * trivial risk of deadlock.  We can minimize but not eliminate the deadlock
 * risk by using the weakest suitable lock (ShareLock for most callers).
 *
 * XXX the API for this is not sufficient to support checking domain values
 * that are inside container types, such as composite types, arrays, or
 * ranges.  Currently we just error out if a container type containing the
 * target domain is stored anywhere.
 *
 * Generally used for retrieving a list of tests when adding
 * new constraints to a domain.
 */
unsafe fn get_rels_with_domain(domainOid: Oid, lockmode: LOCKMODE) -> *mut List {
    let mut result: *mut List = std::ptr::null_mut(); /* NIL */
    let domainTypeName: *const c_char = format_type_be(domainOid);
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = std::mem::zeroed();
    let depScan: SysScanDesc;
    let depTup: HeapTuple;

    /* Assert(lockmode != NoLock); */

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /*
     * We scan pg_depend to find those things that depend on the domain. (We
     * assume we can ignore refobjsubid for a domain.)
     */
    depRel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(&mut key[0],
                Anum_pg_depend_refclassid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(TypeRelationId));
    ScanKeyInit(&mut key[1],
                Anum_pg_depend_refobjid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(domainOid));

    depScan = systable_beginscan(depRel, DependReferenceIndexId, true,
                                  std::ptr::null_mut(), 2, key.as_ptr());

    loop {
        let depTup = systable_getnext(depScan);
        if depTup.is_null() { break; }

        let pg_depend: Form_pg_depend = GETSTRUCT(depTup) as Form_pg_depend;
        let mut rtc: *mut RelToCheck = std::ptr::null_mut();
        let pg_att: Form_pg_attribute;

        /* Check for directly dependent types */
        if (*(pg_depend as *mut FormData_pg_depend_fields)).classid == TypeRelationId {
            if get_typtype((*(pg_depend as *mut FormData_pg_depend_fields)).objid) == TYPTYPE_DOMAIN {
                /*
                 * This is a sub-domain, so recursively add dependent columns
                 * to the output list.  This is a bit inefficient since we may
                 * fail to combine RelToCheck entries when attributes of the
                 * same rel have different derived domain types, but it's
                 * probably not worth improving.
                 */
                result = list_concat(result,
                                     get_rels_with_domain(
                                         (*(pg_depend as *mut FormData_pg_depend_fields)).objid,
                                         lockmode));
            } else {
                /*
                 * Otherwise, it is some container type using the domain, so
                 * fail if there are any columns of this type.
                 */
                find_composite_type_dependencies(
                    (*(pg_depend as *mut FormData_pg_depend_fields)).objid,
                    std::ptr::null_mut(),
                    domainTypeName);
            }
            continue;
        }

        /* Else, ignore dependees that aren't user columns of relations */
        /* (we assume system columns are never of domain types) */
        if (*(pg_depend as *mut FormData_pg_depend_fields)).classid != RelationRelationId
            || (*(pg_depend as *mut FormData_pg_depend_fields)).objsubid <= 0
        {
            continue;
        }

        /* See if we already have an entry for this relation */
        foreach!(rellist, result, {
            let rt: *mut RelToCheck = lfirst(crate::current_cell!(rellist)) as *mut RelToCheck;

            if RelationGetRelid((*rt).rel) == (*(pg_depend as *mut FormData_pg_depend_fields)).objid {
                rtc = rt;
                break;
            }
        });

        if rtc.is_null() {
            /* First attribute found for this relation */
            let rel: Relation;

            /* Acquire requested lock on relation */
            rel = relation_open((*(pg_depend as *mut FormData_pg_depend_fields)).objid, lockmode);

            /*
             * Check to see if rowtype is stored anyplace as a composite-type
             * column; if so we have to fail, for now anyway.
             */
            if OidIsValid((*((*rel).rd_rel as *mut FormData_pg_type_fields)).typtype as Oid) {
                /* rd_rel->reltype */
                find_composite_type_dependencies(
                    (*((*rel).rd_rel as *mut FormData_pg_class_reltype)).reltype,
                    std::ptr::null_mut(),
                    domainTypeName);
            }

            /*
             * Otherwise, we can ignore relations except those with both
             * storage and user-chosen column types.
             *
             * XXX If an index-only scan could satisfy "col::some_domain" from
             * a suitable expression index, this should also check expression
             * index columns.
             */
            if (*((*rel).rd_rel as *mut FormData_pg_class_relkind)).relkind != RELKIND_RELATION
                && (*((*rel).rd_rel as *mut FormData_pg_class_relkind)).relkind != RELKIND_MATVIEW
            {
                relation_close(rel, lockmode);
                continue;
            }

            /* Build the RelToCheck entry with enough space for all atts */
            rtc = palloc(core::mem::size_of::<RelToCheck>()) as *mut RelToCheck;
            (*rtc).rel = rel;
            (*rtc).natts = 0;
            (*rtc).atts = palloc(core::mem::size_of::<c_int>() *
                RelationGetNumberOfAttributes(rel) as usize) as *mut c_int;
            result = lappend(result, rtc as *mut c_void);
        }

        /*
         * Confirm column has not been dropped, and is of the expected type.
         * This defends against an ALTER DROP COLUMN occurring just before we
         * acquired lock ... but if the whole table were dropped, we'd still
         * have a problem.
         */
        if (*(pg_depend as *mut FormData_pg_depend_fields)).objsubid > RelationGetNumberOfAttributes((*rtc).rel) {
            continue;
        }
        pg_att = TupleDescAttr((*(*rtc).rel).rd_att, (*(pg_depend as *mut FormData_pg_depend_fields)).objsubid - 1);
        if (*(pg_att as *mut FormData_pg_attribute_drop)).attisdropped
            || (*(pg_att as *mut FormData_pg_attribute_drop)).atttypid != domainOid
        {
            continue;
        }

        /*
         * Okay, add column to result.  We store the columns in column-number
         * order; this is just a hack to improve predictability of regression
         * test output ...
         */
        /* Assert(rtc->natts < RelationGetNumberOfAttributes(rtc->rel)); */

        let mut ptr = (*rtc).natts;
        (*rtc).natts += 1;
        while ptr > 0 && *(*rtc).atts.add(ptr as usize - 1) > (*(pg_depend as *mut FormData_pg_depend_fields)).objsubid {
            *(*rtc).atts.add(ptr as usize) = *(*rtc).atts.add(ptr as usize - 1);
            ptr -= 1;
        }
        *(*rtc).atts.add(ptr as usize) = (*(pg_depend as *mut FormData_pg_depend_fields)).objsubid;
    }

    systable_endscan(depScan);

    relation_close(depRel, AccessShareLock);

    result
}

/* Helper stubs for field access */
#[repr(C)] struct FormData_pg_depend_fields {
    _pad: [u8; 0],
    classid: Oid,
    objid: Oid,
    objsubid: c_int,
}
#[repr(C)] struct FormData_pg_class_reltype { _pad: [u8; 0], reltype: Oid }
#[repr(C)] struct FormData_pg_class_relkind { _pad: [u8; 0], relkind: c_char }
/* RelationData is defined via type alias at top of file */
#[repr(C)] struct FormData_pg_attribute_drop {
    _pad: [u8; 0],
    attisdropped: bool,
    atttypid: Oid,
}

/*
 * checkDomainOwner
 *
 * Check that the type is actually a domain and that the current user
 * has permission to do ALTER DOMAIN on it.  Throw an error if not.
 */
pub unsafe fn checkDomainOwner(tup: HeapTuple) {
    let typTup: Form_pg_type = GETSTRUCT(tup) as Form_pg_type;

    /* Check that this is actually a domain */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype != TYPTYPE_DOMAIN {
        ereport!(ERROR, errmsg!("{} is not a domain",
            std::ffi::CStr::from_ptr(format_type_be(
                (*(typTup as *mut FormData_pg_type_fields)).oid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* Permission check: must own type */
    if !object_ownercheck(TypeRelationId, (*(typTup as *mut FormData_pg_type_fields)).oid, GetUserId()) {
        aclcheck_error_type(ACLCHECK_NOT_OWNER, (*(typTup as *mut FormData_pg_type_fields)).oid);
    }
}

/*
 * Execute ALTER TYPE RENAME
 */
pub unsafe fn RenameType(stmt: *mut RenameStmt) -> ObjectAddress {
    let names: *mut List = (*stmt).object as *mut List; /* castNode(List, stmt->object) */
    let newTypeName: *const c_char = (*stmt).newname;
    let typename: *mut TypeName;
    let typeOid: Oid;
    let rel: Relation;
    let tup: HeapTuple;
    let typTup: Form_pg_type;
    let mut address: ObjectAddress = unsafe { core::mem::zeroed() };

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    typeOid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Look up the type in the type table */
    rel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", typeOid);
    }
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /* check permissions on type */
    if !object_ownercheck(TypeRelationId, typeOid, GetUserId()) {
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typeOid);
    }

    /* ALTER DOMAIN used on a non-domain? */
    if (*stmt).renameType == ObjectType::OBJECT_DOMAIN && (*(typTup as *mut FormData_pg_type_fields)).typtype != TYPTYPE_DOMAIN {
        ereport!(ERROR, errmsg!("{} is not a domain",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /*
     * If it's a composite type, we need to check that it really is a
     * free-standing composite type, and not a table's rowtype. We want people
     * to use ALTER TABLE not ALTER TYPE for that case.
     */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype == TYPTYPE_COMPOSITE
        && get_rel_relkind((*(typTup as *mut FormData_pg_type_fields)).typrelid) != RELKIND_COMPOSITE_TYPE
    {
        ereport!(ERROR, errmsg!("{} is a table's row type",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errhint("Use ALTER TABLE instead.") */
    }

    /* don't allow direct alteration of array types, either */
    if IsTrueArrayType(typTup) {
        ereport!(ERROR, errmsg!("cannot alter array type {}",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errhint about typelem */
    }

    /* we do allow separate renaming of multirange types, though */

    /*
     * If type is composite we need to rename associated pg_class entry too.
     * RenameRelationInternal will call RenameTypeInternal automatically.
     */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype == TYPTYPE_COMPOSITE {
        RenameRelationInternal((*(typTup as *mut FormData_pg_type_fields)).typrelid,
                               newTypeName, false, false);
    } else {
        RenameTypeInternal(typeOid, newTypeName,
                           (*(typTup as *mut FormData_pg_type_fields)).typnamespace);
    }

    ObjectAddressSet(&mut address as *mut ObjectAddress, TypeRelationId, typeOid);
    /* Clean up */
    table_close(rel, RowExclusiveLock);

    address
}

/*
 * Change the owner of a type.
 */
pub unsafe fn AlterTypeOwner(names: *mut List, newOwnerId: Oid, objecttype: ObjectType) -> ObjectAddress {
    let typename: *mut TypeName;
    let typeOid: Oid;
    let rel: Relation;
    let tup: HeapTuple;
    let newtup: HeapTuple;
    let typTup: Form_pg_type;
    let aclresult: AclResult;
    let mut address: ObjectAddress = std::mem::zeroed();

    rel = table_open(TypeRelationId, RowExclusiveLock);

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);

    /* Use LookupTypeName here so that shell types can be processed */
    tup = LookupTypeName(std::ptr::null_mut(), typename, std::ptr::null_mut(), false);
    if tup.is_null() {
        ereport!(ERROR, errmsg!("type \"{}\" does not exist",
            std::ffi::CStr::from_ptr(TypeNameToString(typename)).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }
    typeOid = typeTypeId(tup);

    /* Copy the syscache entry so we can scribble on it below */
    newtup = heap_copytuple(tup);
    ReleaseSysCache(tup);
    let tup = newtup;
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /* Don't allow ALTER DOMAIN on a type */
    if objecttype == ObjectType::OBJECT_DOMAIN && (*(typTup as *mut FormData_pg_type_fields)).typtype != TYPTYPE_DOMAIN {
        ereport!(ERROR, errmsg!("{} is not a domain",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /*
     * If it's a composite type, we need to check that it really is a
     * free-standing composite type, and not a table's rowtype. We want people
     * to use ALTER TABLE not ALTER TYPE for that case.
     */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype == TYPTYPE_COMPOSITE
        && get_rel_relkind((*(typTup as *mut FormData_pg_type_fields)).typrelid) != RELKIND_COMPOSITE_TYPE
    {
        ereport!(ERROR, errmsg!("{} is a table's row type",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errhint("Use ALTER TABLE instead.") */
    }

    /* don't allow direct alteration of array types, either */
    if IsTrueArrayType(typTup) {
        ereport!(ERROR, errmsg!("cannot alter array type {}",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errhint about typelem */
    }

    /* don't allow direct alteration of multirange types, either */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype == TYPTYPE_MULTIRANGE {
        let rangetype: Oid = get_multirange_range(typeOid);
        /* We don't expect get_multirange_range to fail, but cope if so */
        ereport!(ERROR, errmsg!("cannot alter multirange type {}",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: OidIsValid(rangetype) ? errhint(...) : 0 */
        let _ = rangetype;
    }

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (*(typTup as *mut FormData_pg_type_fields)).typowner != newOwnerId {
        /* Superusers can always do it */
        if !superuser() {
            /* Otherwise, must be owner of the existing object */
            if !object_ownercheck(TypeRelationId, (*(typTup as *mut FormData_pg_type_fields)).oid, GetUserId()) {
                aclcheck_error_type(ACLCHECK_NOT_OWNER, (*(typTup as *mut FormData_pg_type_fields)).oid);
            }

            /* Must be able to become new owner */
            check_can_set_role(GetUserId(), newOwnerId);

            /* New owner must have CREATE privilege on namespace */
            aclresult = object_aclcheck(NamespaceRelationId,
                                        (*(typTup as *mut FormData_pg_type_fields)).typnamespace,
                                        newOwnerId, ACL_CREATE);
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_SCHEMA,
                               get_namespace_name((*(typTup as *mut FormData_pg_type_fields)).typnamespace));
            }
        }

        AlterTypeOwner_oid(typeOid, newOwnerId, true);
    }

    ObjectAddressSet(&mut address as *mut ObjectAddress, TypeRelationId, typeOid);

    /* Clean up */
    table_close(rel, RowExclusiveLock);

    address
}

/*
 * AlterTypeOwner_oid - change type owner unconditionally
 *
 * This function recurses to handle dependent types (arrays and multiranges).
 * It invokes any necessary access object hooks.  If hasDependEntry is true,
 * this function modifies the pg_shdepend entry appropriately (this should be
 * passed as false only for table rowtypes and dependent types).
 *
 * This is used by ALTER TABLE/TYPE OWNER commands, as well as by REASSIGN
 * OWNED BY.  It assumes the caller has done all needed checks.
 */
pub unsafe fn AlterTypeOwner_oid(typeOid: Oid, newOwnerId: Oid, hasDependEntry: bool) {
    let rel: Relation;
    let tup: HeapTuple;
    let typTup: Form_pg_type;

    rel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", typeOid);
    }
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /*
     * If it's a composite type, invoke ATExecChangeOwner so that we fix up
     * the pg_class entry properly.  That will call back to
     * AlterTypeOwnerInternal to take care of the pg_type entry(s).
     */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype == TYPTYPE_COMPOSITE {
        ATExecChangeOwner((*(typTup as *mut FormData_pg_type_fields)).typrelid,
                          newOwnerId, true, AccessExclusiveLock);
    } else {
        AlterTypeOwnerInternal(typeOid, newOwnerId);
    }

    /* Update owner dependency reference */
    if hasDependEntry {
        changeDependencyOnOwner(TypeRelationId, typeOid, newOwnerId);
    }

    InvokeObjectPostAlterHook(TypeRelationId, typeOid, 0);

    ReleaseSysCache(tup);
    table_close(rel, RowExclusiveLock);
}

/*
 * AlterTypeOwnerInternal - bare-bones type owner change.
 *
 * This routine simply modifies the owner of a pg_type entry, and recurses
 * to handle any dependent types.
 */
pub unsafe fn AlterTypeOwnerInternal(typeOid: Oid, newOwnerId: Oid) {
    let rel: Relation;
    let mut tup: HeapTuple;
    let typTup: Form_pg_type;
    let mut repl_val: [Datum; 44 /* Natts_pg_type */] = std::mem::zeroed();
    let repl_null: [bool; 44] = [false; 44];
    let mut repl_repl: [bool; 44] = [false; 44];
    let newAcl: *mut Acl;
    let aclDatum: Datum;
    let mut isNull: bool = false;

    rel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", typeOid);
    }
    typTup = GETSTRUCT(tup) as Form_pg_type;

    /* repl_null and repl_repl already zeroed/false by initialization */

    repl_repl[Anum_pg_type_typowner as usize - 1] = true;
    repl_val[Anum_pg_type_typowner as usize - 1] = ObjectIdGetDatum(newOwnerId);

    aclDatum = heap_getattr(tup, Anum_pg_type_typacl,
                            RelationGetDescr(rel), &mut isNull);
    /* Null ACLs do not require changes */
    if !isNull {
        newAcl = aclnewowner(DatumGetAclP(aclDatum),
                             (*(typTup as *mut FormData_pg_type_fields)).typowner,
                             newOwnerId);
        repl_repl[Anum_pg_type_typacl as usize - 1] = true;
        repl_val[Anum_pg_type_typacl as usize - 1] = PointerGetDatum(newAcl as *const c_void);
    }

    tup = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val.as_ptr(),
                            repl_null.as_ptr(), repl_repl.as_ptr());

    CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut ItemPointerData, tup);

    /* If it has an array type, update that too */
    if OidIsValid((*(typTup as *mut FormData_pg_type_fields)).typarray) {
        AlterTypeOwnerInternal((*(typTup as *mut FormData_pg_type_fields)).typarray, newOwnerId);
    }

    /* If it is a range type, update the associated multirange too */
    if (*(typTup as *mut FormData_pg_type_fields)).typtype == TYPTYPE_RANGE {
        let multirange_typeid: Oid = get_range_multirange(typeOid);

        if !OidIsValid(multirange_typeid) {
            ereport!(ERROR, errmsg!("could not find multirange type for data type {}",
                std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }
        AlterTypeOwnerInternal(multirange_typeid, newOwnerId);
    }

    /* Clean up */
    table_close(rel, RowExclusiveLock);
}

/*
 * Execute ALTER TYPE SET SCHEMA
 */
pub unsafe fn AlterTypeNamespace(names: *mut List, newschema: *const c_char,
                                  objecttype: ObjectType, oldschema: *mut Oid) -> ObjectAddress {
    let typename: *mut TypeName;
    let typeOid: Oid;
    let nspOid: Oid;
    let oldNspOid: Oid;
    let objsMoved: *mut ObjectAddresses;
    let mut myself: ObjectAddress = std::mem::zeroed();

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList(names);
    typeOid = typenameTypeId(std::ptr::null_mut(), typename);

    /* Don't allow ALTER DOMAIN on a non-domain type */
    if objecttype == ObjectType::OBJECT_DOMAIN && get_typtype(typeOid) != TYPTYPE_DOMAIN {
        ereport!(ERROR, errmsg!("{} is not a domain",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* get schema OID and check its permissions */
    nspOid = LookupCreationNamespace(newschema);

    objsMoved = new_object_addresses();
    oldNspOid = AlterTypeNamespace_oid(typeOid, nspOid, false, objsMoved);
    free_object_addresses(objsMoved);

    if !oldschema.is_null() {
        *oldschema = oldNspOid;
    }

    ObjectAddressSet(&mut myself as *mut ObjectAddress, TypeRelationId, typeOid);

    myself
}

/*
 * ALTER TYPE SET SCHEMA, where the caller has already looked up the OIDs
 * of the type and the target schema and checked the schema's privileges.
 *
 * If ignoreDependent is true, we silently ignore dependent types
 * (array types and table rowtypes) rather than raising errors.
 *
 * This entry point is exported for use by AlterObjectNamespace_oid,
 * which doesn't want errors when it passes OIDs of dependent types.
 *
 * Returns the type's old namespace OID, or InvalidOid if we did nothing.
 */
pub unsafe fn AlterTypeNamespace_oid(typeOid: Oid, nspOid: Oid, ignoreDependent: bool,
                                      objsMoved: *mut ObjectAddresses) -> Oid {
    let elemOid: Oid;

    /* check permissions on type */
    if !object_ownercheck(TypeRelationId, typeOid, GetUserId()) {
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typeOid);
    }

    /* don't allow direct alteration of array types */
    elemOid = get_element_type(typeOid);
    if OidIsValid(elemOid) && get_array_type(elemOid) == typeOid {
        if ignoreDependent {
            return InvalidOid;
        }
        ereport!(ERROR, errmsg!("cannot alter array type {}",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errhint("You can alter type %s, ...", format_type_be(elemOid)) */
    }

    /* and do the work */
    AlterTypeNamespaceInternal(typeOid, nspOid,
                               false,           /* isImplicitArray */
                               ignoreDependent, /* ignoreDependent */
                               true,            /* errorOnTableType */
                               objsMoved)
}

/*
 * Move specified type to new namespace.
 *
 * Caller must have already checked privileges.
 *
 * The function automatically recurses to process the type's array type,
 * if any.  isImplicitArray should be true only when doing this internal
 * recursion (outside callers must never try to move an array type directly).
 *
 * If ignoreDependent is true, we silently don't process table types.
 *
 * If errorOnTableType is true, the function errors out if the type is
 * a table type.  ALTER TABLE has to be used to move a table to a new
 * namespace.  (This flag is ignored if ignoreDependent is true.)
 *
 * We also do nothing if the type is already listed in *objsMoved.
 * After a successful move, we add the type to *objsMoved.
 *
 * Returns the type's old namespace OID, or InvalidOid if we did nothing.
 */
pub unsafe fn AlterTypeNamespaceInternal(typeOid: Oid, nspOid: Oid,
                                          isImplicitArray: bool,
                                          ignoreDependent: bool,
                                          errorOnTableType: bool,
                                          objsMoved: *mut ObjectAddresses) -> Oid {
    let rel: Relation;
    let tup: HeapTuple;
    let typform: Form_pg_type;
    let oldNspOid: Oid;
    let arrayOid: Oid;
    let isCompositeType: bool;
    let mut thisobj: ObjectAddress = std::mem::zeroed();

    /*
     * Make sure we haven't moved this object previously.
     */
    thisobj.classId = TypeRelationId;
    thisobj.objectId = typeOid;
    thisobj.objectSubId = 0;

    if object_address_present(&thisobj as *const ObjectAddress, objsMoved) {
        return InvalidOid;
    }

    rel = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", typeOid);
    }
    typform = GETSTRUCT(tup) as Form_pg_type;

    oldNspOid = (*(typform as *mut FormData_pg_type_fields)).typnamespace;
    arrayOid = (*(typform as *mut FormData_pg_type_fields)).typarray;

    /* If the type is already there, we can skip these next few checks. */
    if oldNspOid != nspOid {
        /* common checks on switching namespaces */
        CheckSetNamespace(oldNspOid, nspOid);

        /* check for duplicate name (more friendly than unique-index failure) */
        if SearchSysCacheExists2(TYPENAMENSP,
                                  NameGetDatum(std::ptr::addr_of!((*(typform as *mut FormData_pg_type_fields)).typname) as *const c_void),
                                  ObjectIdGetDatum(nspOid))
        {
            ereport!(ERROR, errmsg!("type \"{}\" already exists in schema \"{}\"",
                std::ffi::CStr::from_ptr(NameStr(std::ptr::addr_of!((*(typform as *mut FormData_pg_type_fields)).typname) as *const c_void)).to_string_lossy(),
                std::ffi::CStr::from_ptr(get_namespace_name(nspOid)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /* Detect whether type is a composite type (but not a table rowtype) */
    isCompositeType =
        (*(typform as *mut FormData_pg_type_fields)).typtype == TYPTYPE_COMPOSITE
        && get_rel_relkind((*(typform as *mut FormData_pg_type_fields)).typrelid) == RELKIND_COMPOSITE_TYPE;

    /* Enforce not-table-type if requested */
    if (*(typform as *mut FormData_pg_type_fields)).typtype == TYPTYPE_COMPOSITE && !isCompositeType {
        if ignoreDependent {
            table_close(rel, RowExclusiveLock);
            return InvalidOid;
        }
        if errorOnTableType {
            ereport!(ERROR, errmsg!("{} is a table's row type",
                std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errhint("Use ALTER TABLE instead.") */
        }
    }

    if oldNspOid != nspOid {
        /* OK, modify the pg_type row */

        /* tup is a copy, so we can scribble directly on it */
        (*(typform as *mut FormData_pg_type_fields)).typnamespace = nspOid;

        CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut ItemPointerData, tup);
    }

    /*
     * Composite types have pg_class entries.
     *
     * We need to modify the pg_class tuple as well to reflect the change of
     * schema.
     */
    if isCompositeType {
        let classRel: Relation;

        classRel = table_open(RelationRelationId, RowExclusiveLock);

        AlterRelationNamespaceInternal(classRel,
                                       (*(typform as *mut FormData_pg_type_fields)).typrelid,
                                       oldNspOid, nspOid, false, objsMoved);

        table_close(classRel, RowExclusiveLock);

        /*
         * Check for constraints associated with the composite type (we don't
         * currently support this, but probably will someday).
         */
        AlterConstraintNamespaces((*(typform as *mut FormData_pg_type_fields)).typrelid,
                                   oldNspOid, nspOid, false, objsMoved);
    } else {
        /* If it's a domain, it might have constraints */
        if (*(typform as *mut FormData_pg_type_fields)).typtype == TYPTYPE_DOMAIN {
            AlterConstraintNamespaces(typeOid, oldNspOid, nspOid, true, objsMoved);
        }
    }

    /*
     * Update dependency on schema, if any --- a table rowtype has not got
     * one, and neither does an implicit array.
     */
    if oldNspOid != nspOid
        && (isCompositeType || (*(typform as *mut FormData_pg_type_fields)).typtype != TYPTYPE_COMPOSITE)
        && !isImplicitArray
    {
        if changeDependencyFor(TypeRelationId, typeOid,
                               NamespaceRelationId, oldNspOid, nspOid) != 1 {
            elog!(ERROR, "could not change schema dependency for type \"{}\"",
                std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy());
        }
    }

    InvokeObjectPostAlterHook(TypeRelationId, typeOid, 0);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);

    add_exact_object_address(&thisobj as *const ObjectAddress, objsMoved);

    /* Recursively alter the associated array type, if any */
    if OidIsValid(arrayOid) {
        AlterTypeNamespaceInternal(arrayOid, nspOid,
                                   true,  /* isImplicitArray */
                                   false, /* ignoreDependent */
                                   true,  /* errorOnTableType */
                                   objsMoved);
    }

    oldNspOid
}

/*
 * AlterType
 *     ALTER TYPE <type> SET (option = ...)
 *
 * NOTE: the set of changes that can be allowed here is constrained by many
 * non-obvious implementation restrictions.  Tread carefully when considering
 * adding new flexibility.
 */
pub unsafe fn AlterType(stmt: *mut crate::nodes::parsenodes::AlterTypeStmt) -> ObjectAddress {
    let mut address: ObjectAddress = std::mem::zeroed();
    let catalog: Relation;
    let typename: *mut TypeName;
    let tup: HeapTuple;
    let typeOid: Oid;
    let typForm: Form_pg_type;
    let mut requireSuper: bool = false;
    let mut atparams: AlterTypeRecurseParams = std::mem::zeroed();

    catalog = table_open(TypeRelationId, RowExclusiveLock);

    /* Make a TypeName so we can use standard type lookup machinery */
    typename = makeTypeNameFromNameList((*stmt).typeName);
    tup = typenameType(std::ptr::null_mut(), typename, std::ptr::null_mut());

    typeOid = typeTypeId(tup);
    typForm = GETSTRUCT(tup) as Form_pg_type;

    /* Process options */
    /* atparams already zeroed */
    foreach!(pl, (*stmt).options, {
        let defel: *mut DefElem = lfirst(crate::current_cell!(pl)) as *mut DefElem;
        let defname: *const c_char = (*defel).defname;

        if strcmp_lit(defname, c"storage") == 0 {
            let a: *mut c_char = defGetString(defel);

            if pg_strcasecmp_lit(a, c"plain") == 0 {
                atparams.storage = TYPSTORAGE_PLAIN;
            } else if pg_strcasecmp_lit(a, c"external") == 0 {
                atparams.storage = TYPSTORAGE_EXTERNAL;
            } else if pg_strcasecmp_lit(a, c"extended") == 0 {
                atparams.storage = TYPSTORAGE_EXTENDED;
            } else if pg_strcasecmp_lit(a, c"main") == 0 {
                atparams.storage = TYPSTORAGE_MAIN;
            } else {
                ereport!(ERROR, errmsg!("storage \"{}\" not recognized",
                    std::ffi::CStr::from_ptr(a).to_string_lossy()));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }

            /*
             * Validate the storage request.  If the type isn't varlena, it
             * certainly doesn't support non-PLAIN storage.
             */
            if atparams.storage != TYPSTORAGE_PLAIN
                && (*(typForm as *mut FormData_pg_type_fields)).typlen != -1
            {
                ereport!(ERROR, errmsg!("fixed-size types must have storage PLAIN"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            /*
             * Switching from PLAIN to non-PLAIN is allowed, but it requires
             * superuser, since we can't validate that the type's C functions
             * will support it.  Switching from non-PLAIN to PLAIN is
             * disallowed outright, because it's not practical to ensure that
             * no tables have toasted values of the type.  Switching among
             * different non-PLAIN settings is OK, since it just constitutes a
             * change in the strategy requested for columns created in the
             * future.
             */
            if atparams.storage != TYPSTORAGE_PLAIN
                && (*(typForm as *mut FormData_pg_type_fields)).typstorage == TYPSTORAGE_PLAIN
            {
                requireSuper = true;
            } else if atparams.storage == TYPSTORAGE_PLAIN
                && (*(typForm as *mut FormData_pg_type_fields)).typstorage != TYPSTORAGE_PLAIN
            {
                ereport!(ERROR, errmsg!("cannot change type's storage to PLAIN"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            atparams.updateStorage = true;
        } else if strcmp_lit(defname, c"receive") == 0 {
            if !(*defel).arg.is_null() {
                atparams.receiveOid =
                    findTypeReceiveFunction(defGetQualifiedName(defel), typeOid);
            } else {
                atparams.receiveOid = InvalidOid; /* NONE, remove function */
            }
            atparams.updateReceive = true;
            /* Replacing an I/O function requires superuser. */
            requireSuper = true;
        } else if strcmp_lit(defname, c"send") == 0 {
            if !(*defel).arg.is_null() {
                atparams.sendOid =
                    findTypeSendFunction(defGetQualifiedName(defel), typeOid);
            } else {
                atparams.sendOid = InvalidOid; /* NONE, remove function */
            }
            atparams.updateSend = true;
            /* Replacing an I/O function requires superuser. */
            requireSuper = true;
        } else if strcmp_lit(defname, c"typmod_in") == 0 {
            if !(*defel).arg.is_null() {
                atparams.typmodinOid =
                    findTypeTypmodinFunction(defGetQualifiedName(defel));
            } else {
                atparams.typmodinOid = InvalidOid; /* NONE, remove function */
            }
            atparams.updateTypmodin = true;
            /* Replacing an I/O function requires superuser. */
            requireSuper = true;
        } else if strcmp_lit(defname, c"typmod_out") == 0 {
            if !(*defel).arg.is_null() {
                atparams.typmodoutOid =
                    findTypeTypmodoutFunction(defGetQualifiedName(defel));
            } else {
                atparams.typmodoutOid = InvalidOid; /* NONE, remove function */
            }
            atparams.updateTypmodout = true;
            /* Replacing an I/O function requires superuser. */
            requireSuper = true;
        } else if strcmp_lit(defname, c"analyze") == 0 {
            if !(*defel).arg.is_null() {
                atparams.analyzeOid =
                    findTypeAnalyzeFunction(defGetQualifiedName(defel), typeOid);
            } else {
                atparams.analyzeOid = InvalidOid; /* NONE, remove function */
            }
            atparams.updateAnalyze = true;
            /* Replacing an analyze function requires superuser. */
            requireSuper = true;
        } else if strcmp_lit(defname, c"subscript") == 0 {
            if !(*defel).arg.is_null() {
                atparams.subscriptOid =
                    findTypeSubscriptingFunction(defGetQualifiedName(defel), typeOid);
            } else {
                atparams.subscriptOid = InvalidOid; /* NONE, remove function */
            }
            atparams.updateSubscript = true;
            /* Replacing a subscript function requires superuser. */
            requireSuper = true;
        }
        /*
         * The rest of the options that CREATE accepts cannot be changed.
         * Check for them so that we can give a meaningful error message.
         */
        else if strcmp_lit(defname, c"input") == 0
            || strcmp_lit(defname, c"output") == 0
            || strcmp_lit(defname, c"internallength") == 0
            || strcmp_lit(defname, c"passedbyvalue") == 0
            || strcmp_lit(defname, c"alignment") == 0
            || strcmp_lit(defname, c"like") == 0
            || strcmp_lit(defname, c"category") == 0
            || strcmp_lit(defname, c"preferred") == 0
            || strcmp_lit(defname, c"default") == 0
            || strcmp_lit(defname, c"element") == 0
            || strcmp_lit(defname, c"delimiter") == 0
            || strcmp_lit(defname, c"collatable") == 0
        {
            ereport!(ERROR, errmsg!("type attribute \"{}\" cannot be changed",
                std::ffi::CStr::from_ptr(defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        } else {
            ereport!(ERROR, errmsg!("type attribute \"{}\" not recognized",
                std::ffi::CStr::from_ptr(defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }
    });

    /*
     * Permissions check.  Require superuser if we decided the command
     * requires that, else must own the type.
     */
    if requireSuper {
        if !superuser() {
            ereport!(ERROR, errmsg!("must be superuser to alter a type"));
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        }
    } else {
        if !object_ownercheck(TypeRelationId, typeOid, GetUserId()) {
            aclcheck_error_type(ACLCHECK_NOT_OWNER, typeOid);
        }
    }

    /*
     * We disallow all forms of ALTER TYPE SET on types that aren't plain base
     * types.  It would for example be highly unsafe, not to mention
     * pointless, to change the send/receive functions for a composite type.
     * Moreover, pg_dump has no support for changing these properties on
     * non-base types.  We might weaken this someday, but not now.
     *
     * Note: if you weaken this enough to allow composite types, be sure to
     * adjust the GenerateTypeDependencies call in AlterTypeRecurse.
     */
    if (*(typForm as *mut FormData_pg_type_fields)).typtype != TYPTYPE_BASE {
        ereport!(ERROR, errmsg!("{} is not a base type",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /*
     * For the same reasons, don't allow direct alteration of array types.
     */
    if IsTrueArrayType(typForm) {
        ereport!(ERROR, errmsg!("{} is not a base type",
            std::ffi::CStr::from_ptr(format_type_be(typeOid)).to_string_lossy()));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* OK, recursively update this type and any arrays/domains over it */
    AlterTypeRecurse(typeOid, false, tup, catalog, &mut atparams as *mut AlterTypeRecurseParams);

    /* Clean up */
    ReleaseSysCache(tup);

    table_close(catalog, RowExclusiveLock);

    ObjectAddressSet(&mut address as *mut ObjectAddress, TypeRelationId, typeOid);

    address
}

/*
 * AlterTypeRecurse: one recursion step for AlterType()
 *
 * Apply the changes specified by "atparams" to the type identified by
 * "typeOid", whose existing pg_type tuple is "tup".  If necessary,
 * recursively update its array type as well.  Then search for any domains
 * over this type, and recursively apply (most of) the same changes to those
 * domains.
 *
 * We need this because the system generally assumes that a domain inherits
 * many properties from its base type.  See DefineDomain() above for details
 * of what is inherited.  Arrays inherit a smaller number of properties,
 * but not none.
 *
 * There's a race condition here, in that some other transaction could
 * concurrently add another domain atop this base type; we'd miss updating
 * that one.  Hence, be wary of allowing ALTER TYPE to change properties for
 * which it'd be really fatal for a domain to be out of sync with its base
 * type (typlen, for example).  In practice, races seem unlikely to be an
 * issue for plausible use-cases for ALTER TYPE.  If one does happen, it could
 * be fixed by re-doing the same ALTER TYPE once all prior transactions have
 * committed.
 */
unsafe fn AlterTypeRecurse(typeOid: Oid, isImplicitArray: bool,
                            tup: HeapTuple, catalog: Relation,
                            atparams: *mut AlterTypeRecurseParams) {
    let mut values: [Datum; 44 /* Natts_pg_type */] = std::mem::zeroed();
    let nulls: [bool; 44] = [false; 44];
    let mut replaces: [bool; 44] = [false; 44];
    let newtup: HeapTuple;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = std::mem::zeroed();

    /* Since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* Update the current type's tuple */
    /* values/nulls/replaces already zeroed */

    if (*atparams).updateStorage {
        replaces[Anum_pg_type_typstorage as usize - 1] = true;
        values[Anum_pg_type_typstorage as usize - 1] = CharGetDatum((*atparams).storage);
    }
    if (*atparams).updateReceive {
        replaces[Anum_pg_type_typreceive as usize - 1] = true;
        values[Anum_pg_type_typreceive as usize - 1] = ObjectIdGetDatum((*atparams).receiveOid);
    }
    if (*atparams).updateSend {
        replaces[Anum_pg_type_typsend as usize - 1] = true;
        values[Anum_pg_type_typsend as usize - 1] = ObjectIdGetDatum((*atparams).sendOid);
    }
    if (*atparams).updateTypmodin {
        replaces[Anum_pg_type_typmodin as usize - 1] = true;
        values[Anum_pg_type_typmodin as usize - 1] = ObjectIdGetDatum((*atparams).typmodinOid);
    }
    if (*atparams).updateTypmodout {
        replaces[Anum_pg_type_typmodout as usize - 1] = true;
        values[Anum_pg_type_typmodout as usize - 1] = ObjectIdGetDatum((*atparams).typmodoutOid);
    }
    if (*atparams).updateAnalyze {
        replaces[Anum_pg_type_typanalyze as usize - 1] = true;
        values[Anum_pg_type_typanalyze as usize - 1] = ObjectIdGetDatum((*atparams).analyzeOid);
    }
    if (*atparams).updateSubscript {
        replaces[Anum_pg_type_typsubscript as usize - 1] = true;
        values[Anum_pg_type_typsubscript as usize - 1] = ObjectIdGetDatum((*atparams).subscriptOid);
    }

    newtup = heap_modify_tuple(tup, RelationGetDescr(catalog),
                               values.as_ptr(), nulls.as_ptr(), replaces.as_ptr());

    CatalogTupleUpdate(catalog, &mut (*newtup).t_self as *mut ItemPointerData, newtup);

    /* Rebuild dependencies for this type */
    GenerateTypeDependencies(newtup,
                             catalog,
                             std::ptr::null_mut(), /* don't have defaultExpr handy */
                             std::ptr::null_mut(), /* don't have typacl handy */
                             0, /* we rejected composite types above */
                             isImplicitArray,  /* it might be an array */
                             isImplicitArray,  /* dependent iff it's array */
                             false, /* don't touch extension membership */
                             true);

    InvokeObjectPostAlterHook(TypeRelationId, typeOid, 0);

    /*
     * Arrays inherit their base type's typmodin and typmodout, but none of
     * the other properties we're concerned with here.  Recurse to the array
     * type if needed.
     */
    if !isImplicitArray
        && ((*atparams).updateTypmodin || (*atparams).updateTypmodout)
    {
        let arrtypoid: Oid = (*(GETSTRUCT(newtup) as *mut FormData_pg_type_fields)).typarray;

        if OidIsValid(arrtypoid) {
            let arrtup: HeapTuple;
            let mut arrparams: AlterTypeRecurseParams = std::mem::zeroed();

            arrtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(arrtypoid));
            if !HeapTupleIsValid(arrtup) {
                elog!(ERROR, "cache lookup failed for type {}", arrtypoid);
            }

            /* arrparams already zeroed */
            arrparams.updateTypmodin = (*atparams).updateTypmodin;
            arrparams.updateTypmodout = (*atparams).updateTypmodout;
            arrparams.typmodinOid = (*atparams).typmodinOid;
            arrparams.typmodoutOid = (*atparams).typmodoutOid;

            AlterTypeRecurse(arrtypoid, true, arrtup, catalog, &mut arrparams as *mut AlterTypeRecurseParams);

            ReleaseSysCache(arrtup);
        }
    }

    /*
     * Now we need to recurse to domains.  However, some properties are not
     * inherited by domains, so clear the update flags for those.
     */
    (*atparams).updateReceive = false;    /* domains use F_DOMAIN_RECV */
    (*atparams).updateTypmodin = false;   /* domains don't have typmods */
    (*atparams).updateTypmodout = false;
    (*atparams).updateSubscript = false;  /* domains don't have subscriptors */

    /* Skip the scan if nothing remains to be done */
    if !((*atparams).updateStorage
         || (*atparams).updateSend
         || (*atparams).updateAnalyze)
    {
        return;
    }

    /* Search pg_type for possible domains over this type */
    ScanKeyInit(key.as_mut_ptr(),
                Anum_pg_type_typbasetype,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(typeOid));

    scan = systable_beginscan(catalog, InvalidOid, false,
                              std::ptr::null_mut(), 1, key.as_ptr());

    loop {
        let domainTup = systable_getnext(scan);
        if domainTup.is_null() { break; }

        let domainForm: Form_pg_type = GETSTRUCT(domainTup) as Form_pg_type;

        /*
         * Shouldn't have a nonzero typbasetype in a non-domain, but let's
         * check
         */
        if (*(domainForm as *mut FormData_pg_type_fields)).typtype != TYPTYPE_DOMAIN {
            continue;
        }

        AlterTypeRecurse((*(domainForm as *mut FormData_pg_type_fields)).oid,
                         false, domainTup, catalog, atparams);
    }

    systable_endscan(scan);
}
