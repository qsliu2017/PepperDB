/*-------------------------------------------------------------------------
 *
 * opclasscmds.rs
 *
 *	  Routines for opclass (and opfamily) manipulation commands
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/opclasscmds.c
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
use crate::{foreach, current_cell, lfirst_node, makeNode};
use core::ffi::{c_char, c_int, c_void};

use crate::access::index::amapi::{IndexAmRoutine, OpFamilyMember};
use crate::access::htup_details::HeapTupleData;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{List, ListCell, lappend, lfirst, linitial, lsecond, list_length, NIL};
use crate::nodes::parsenodes::{
    AlterOpFamilyStmt, CreateOpClassStmt, CreateOpClassItem, CreateOpFamilyStmt,
    ObjectWithArgs, ObjectType, ObjectType::*, TypeName,
    OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_FUNCTION, OPCLASS_ITEM_STORAGETYPE,
};
use crate::catalog::objectaccess::ObjectAddress;

/* --------------------------------------------------------------------------
 * Local type stubs for unported dependencies
 * -------------------------------------------------------------------------- */

// HeapTuple is a single pointer (HeapTupleData*).
type HeapTuple = *mut HeapTupleData;

// Relation pointer
type RelationData = crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// Operator is an alias for HeapTuple (a syscache tuple ref) in PostgreSQL.
type Operator = HeapTuple;

// SysScanDesc / ScanKeyData stubs  TODO(pg-port)
#[repr(C)] pub struct SysScanDescData { _opaque: [u8; 0] }
type SysScanDesc = *mut SysScanDescData;
#[repr(C)] pub struct ScanKeyData { _opaque: [u8; 64] }

// Form structs  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_am { _opaque: [u8; 0] }
type Form_pg_am = *mut FormData_pg_am;
#[repr(C)] pub struct FormData_pg_opfamily { _opaque: [u8; 0] }
type Form_pg_opfamily = *mut FormData_pg_opfamily;
#[repr(C)] pub struct FormData_pg_opclass { _opaque: [u8; 0] }
type Form_pg_opclass = *mut FormData_pg_opclass;
use crate::catalog::pg_operator::{FormData_pg_operator, Form_pg_operator};
use crate::catalog::pg_proc::{FormData_pg_proc, Form_pg_proc};

// NameData  TODO(pg-port)
#[repr(C)] pub struct NameData { pub data: [c_char; 64] }

// AclResult  TODO(pg-port)
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const RowExclusiveLock: LOCKMODE = 3;

// DropBehavior  TODO(pg-port)
type DropBehavior = c_int;
const DROP_RESTRICT: DropBehavior = 0;

/* --------------------------------------------------------------------------
 * Constant stubs (catalog OIDs, syscache ids, attribute numbers, codes)
 * -------------------------------------------------------------------------- */

// Access method OIDs  TODO(pg-port): catalog/pg_am_d.h
const BTREE_AM_OID: Oid = 403;

// errcode classification codes  TODO(pg-port): utils/errcodes.h
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_SYNTAX_ERROR: c_int = 0;

// SHRT_MAX (limits.h)
const SHRT_MAX: c_int = 32767;

// syscache ids  TODO(pg-port): utils/syscache.h
const OPFAMILYAMNAMENSP: c_int = 41;
const OPFAMILYOID: c_int = 42;
const CLAAMNAMENSP: c_int = 13;
const CLAOID: c_int = 14;
const AMOID: c_int = 2;
const AMNAME: c_int = 1;
const OPEROID: c_int = 40;
const PROCOID: c_int = 47;
const AMOPSTRATEGY: c_int = 4;
const AMPROCNUM: c_int = 5;

// catalog relation OIDs  TODO(pg-port): catalog/pg_*_d.h
const OperatorFamilyRelationId: Oid = 2753;
const OperatorClassRelationId: Oid = 2616;
const AccessMethodRelationId: Oid = 2601;
const NamespaceRelationId: Oid = 2615;
const TypeRelationId: Oid = 1247;
const OperatorRelationId: Oid = 2617;
const ProcedureRelationId: Oid = 1255;
const AccessMethodOperatorRelationId: Oid = 2602;
const AccessMethodProcedureRelationId: Oid = 2603;

// catalog index OIDs  TODO(pg-port)
const OpfamilyOidIndexId: Oid = 2755;
const OpclassOidIndexId: Oid = 2687;
const OpclassAmNameNspIndexId: Oid = 2686;
const AccessMethodOperatorOidIndexId: Oid = 2756;
const AccessMethodProcedureOidIndexId: Oid = 2757;

// pg_opfamily attribute numbers  TODO(pg-port)
const Natts_pg_opfamily: usize = 5;
const Anum_pg_opfamily_oid: c_int = 1;
const Anum_pg_opfamily_opfmethod: c_int = 2;
const Anum_pg_opfamily_opfname: c_int = 3;
const Anum_pg_opfamily_opfnamespace: c_int = 4;
const Anum_pg_opfamily_opfowner: c_int = 5;

// pg_opclass attribute numbers  TODO(pg-port)
const Natts_pg_opclass: usize = 9;
const Anum_pg_opclass_oid: c_int = 1;
const Anum_pg_opclass_opcmethod: c_int = 2;
const Anum_pg_opclass_opcname: c_int = 3;
const Anum_pg_opclass_opcnamespace: c_int = 4;
const Anum_pg_opclass_opcowner: c_int = 5;
const Anum_pg_opclass_opcfamily: c_int = 6;
const Anum_pg_opclass_opcintype: c_int = 7;
const Anum_pg_opclass_opcdefault: c_int = 8;
const Anum_pg_opclass_opckeytype: c_int = 9;

// pg_amop attribute numbers  TODO(pg-port)
const Natts_pg_amop: usize = 9;
const Anum_pg_amop_oid: c_int = 1;
const Anum_pg_amop_amopfamily: c_int = 2;
const Anum_pg_amop_amoplefttype: c_int = 3;
const Anum_pg_amop_amoprighttype: c_int = 4;
const Anum_pg_amop_amopstrategy: c_int = 5;
const Anum_pg_amop_amoppurpose: c_int = 6;
const Anum_pg_amop_amopopr: c_int = 7;
const Anum_pg_amop_amopmethod: c_int = 8;
const Anum_pg_amop_amopsortfamily: c_int = 9;

// pg_amproc attribute numbers  TODO(pg-port)
const Natts_pg_amproc: usize = 6;
const Anum_pg_amproc_oid: c_int = 1;
const Anum_pg_amproc_amprocfamily: c_int = 2;
const Anum_pg_amproc_amproclefttype: c_int = 3;
const Anum_pg_amproc_amprocrighttype: c_int = 4;
const Anum_pg_amproc_amprocnum: c_int = 5;
const Anum_pg_amproc_amproc: c_int = 6;

// amop purpose codes  TODO(pg-port): catalog/pg_amop.h
const AMOP_SEARCH: c_char = b's' as c_char;
const AMOP_ORDER: c_char = b'o' as c_char;

// dependency types  TODO(pg-port): catalog/dependency.h
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;
const DEPENDENCY_AUTO: c_char = b'a' as c_char;
const DEPENDENCY_INTERNAL: c_char = b'i' as c_char;

// btree strategy/proc numbers  TODO(pg-port): access/nbtree.h
const BTEqualStrategyNumber: c_int = 3;
const BTORDER_PROC: c_int = 1;
const BTSORTSUPPORT_PROC: c_int = 2;
const BTINRANGE_PROC: c_int = 3;
const BTEQUALIMAGE_PROC: c_int = 4;
const BTSKIPSUPPORT_PROC: c_int = 6;

// hash proc numbers  TODO(pg-port): access/hash.h
const HASHSTANDARD_PROC: c_int = 1;
const HASHEXTENDED_PROC: c_int = 2;

// well-known type OIDs  TODO(pg-port): catalog/pg_type_d.h
const BOOLOID: Oid = 16;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const VOIDOID: Oid = 2278;
const INTERNALOID: Oid = 2281;

// fmgr builtin OIDs  TODO(pg-port): utils/fmgroids.h
const F_OIDEQ: Oid = 184;

// access-method ACL  TODO(pg-port): utils/acl.h
const ACL_CREATE: u64 = 1 << 11;

// InvalidObjectAddress  TODO(pg-port): catalog/dependency.h
const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

/* --------------------------------------------------------------------------
 * Function stubs for dependencies defined in other .c files  TODO(pg-port)
 * -------------------------------------------------------------------------- */

unsafe fn DeconstructQualifiedName(names: *mut List, nspname_p: *mut *mut c_char, objname_p: *mut *mut c_char) { crate::catalog::namespace::DeconstructQualifiedName(names as _, nspname_p, objname_p) }
unsafe fn LookupExplicitNamespace(nspname: *const c_char, missing_ok: bool) -> Oid { crate::catalog::namespace::LookupExplicitNamespace(nspname, missing_ok) }
unsafe fn OpfamilynameGetOpfid(amid: Oid, opfname: *const c_char) -> Oid { crate::catalog::namespace::OpfamilynameGetOpfid(amid, opfname) }
unsafe fn OpclassnameGetOpcid(amid: Oid, opcname: *const c_char) -> Oid { crate::catalog::namespace::OpclassnameGetOpcid(amid, opcname) }
unsafe fn QualifiedNameGetCreationNamespace(names: *mut List, objname_p: *mut *mut c_char) -> Oid { crate::catalog::namespace::QualifiedNameGetCreationNamespace(names as _, objname_p) }
unsafe fn NameListToString(names: *mut List) -> *mut c_char { crate::catalog::namespace::NameListToString(names as _) }
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_namespace_name(nspid) }
unsafe fn get_am_name(amOid: Oid) -> *mut c_char { crate::commands::amcmds::get_am_name(amOid) }

unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache1(cacheId, key1) as _ }
unsafe fn SearchSysCache3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache3(cacheId, key1, key2, key3) as _ }
unsafe fn SearchSysCacheExists3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> bool { crate::utils::cache::syscache::SearchSysCacheExists3(cacheId, key1, key2, key3) }
unsafe fn SearchSysCacheExists4(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum, key4: Datum) -> bool { crate::utils::cache::syscache::SearchSysCacheExists(cacheId, key1, key2, key3, key4) }
unsafe fn GetSysCacheOid4(cacheId: c_int, oidcol: c_int, key1: Datum, key2: Datum, key3: Datum, key4: Datum) -> Oid { crate::utils::cache::syscache::GetSysCacheOid(cacheId, oidcol as _, key1, key2, key3, key4) }
unsafe fn ReleaseSysCache(tuple: HeapTuple) { crate::utils::cache::syscache::ReleaseSysCache(tuple as _) }

unsafe fn cstr_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() { std::borrow::Cow::Borrowed("(null)") }
    else { std::ffi::CStr::from_ptr(s).to_string_lossy() }
}

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool { !tuple.is_null() }
unsafe fn OidIsValid(objectId: Oid) -> bool { objectId != InvalidOid }
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void { crate::access::htup_details::GETSTRUCT(tup as _) as _ }
unsafe fn NameStr(name: *const NameData) -> *const c_char { /* TODO(pg-port): c.h */ (*name).data.as_ptr() }
unsafe fn NameGetDatum(name: *const NameData) -> Datum { /* postgres.h: CStringGetDatum(NameStr(*X)) */ CStringGetDatum(NameStr(name)) }

unsafe fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation { crate::access::table::table::table_open(relationId, lockmode as _) as _ }
unsafe fn table_close(relation: Relation, lockmode: LOCKMODE) { crate::access::table::table::table_close(relation as _, lockmode as _) }
unsafe fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: c_int) -> Oid { crate::catalog::catalog::GetNewOidWithIndex(relation as _, indexId, oidcolumn as _) }
unsafe fn heap_form_tuple(tupleDescriptor: crate::access::common::tupdesc::TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple { crate::access::common::heaptuple::heap_form_tuple(tupleDescriptor as _, values, isnull) as _ }
unsafe fn heap_freetuple(htup: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(htup as _) }
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) -> Oid { crate::catalog::indexing::CatalogTupleInsert(heapRel as _, tup as _); InvalidOid }

unsafe fn namestrcpy(name: *mut NameData, str_: *const c_char) -> c_int { crate::utils::adt::name::namestrcpy(name as _, str_); 0 }
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn superuser() -> bool { crate::utils::misc::superuser::superuser() }

unsafe fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: u64) -> AclResult { core::mem::transmute::<crate::utils::adt::acl::AclResult, AclResult>(crate::catalog::aclchk::object_aclcheck(classid, objectid, roleid, mode as _)) }
unsafe fn aclcheck_error(aclerr: AclResult, objtype: ObjectType, objectname: *const c_char) { crate::catalog::aclchk::aclcheck_error(core::mem::transmute::<AclResult, crate::utils::adt::acl::AclResult>(aclerr), objtype, objectname) }

unsafe fn recordDependencyOn(depender: *const ObjectAddress, referenced: *const ObjectAddress, behavior: c_char) { crate::catalog::pg_depend::recordDependencyOn(depender as _, referenced as _, behavior as _) }
unsafe fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) { crate::catalog::pg_shdepend::recordDependencyOnOwner(classId, objectId, owner) }
unsafe fn recordDependencyOnCurrentExtension(object: *const ObjectAddress, isReplace: bool) { crate::catalog::pg_depend::recordDependencyOnCurrentExtension(object as _, isReplace) }
unsafe fn performDeletion(object: *const ObjectAddress, behavior: DropBehavior, flags: c_int) { crate::catalog::dependency::performDeletion(object as _, core::mem::transmute::<i32, crate::nodes::parsenodes::DropBehavior>(behavior), flags) }
unsafe fn IsPinnedObject(classId: Oid, objectId: Oid) -> bool { crate::catalog::catalog::IsPinnedObject(classId, objectId) }

unsafe fn EventTriggerCollectSimpleCommand(address: ObjectAddress, secondaryObject: ObjectAddress, parsetree: *mut Node) { /* DDL no-op (no event triggers in bring-up) */ }
unsafe fn EventTriggerCollectCreateOpClass(stmt: *mut CreateOpClassStmt, opcoid: Oid, operators: *mut List, procedures: *mut List) { /* DDL no-op (no event triggers in bring-up) */ }
unsafe fn EventTriggerCollectAlterOpFam(stmt: *mut AlterOpFamilyStmt, opfamoid: Oid, operators: *mut List, procedures: *mut List) { /* DDL no-op (no event triggers in bring-up) */ }
unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) { crate::parser_link_shims::InvokeObjectPostCreateHook(classId, objectId, subId) }

unsafe fn GetIndexAmRoutineByAmId(amoid: Oid, noerror: bool) -> *mut IndexAmRoutine { crate::access::index::amapi::GetIndexAmRoutineByAmId(amoid, noerror) }
unsafe fn get_index_am_oid(amname: *const c_char, missing_ok: bool) -> Oid { crate::commands::amcmds::get_index_am_oid(amname, missing_ok) }

unsafe fn typenameTypeId(pstate: *mut c_void, typeName: *mut TypeName) -> Oid { crate::parser::parse_type::typenameTypeId(pstate as _, typeName as _) }
unsafe fn TypeNameToString(typeName: *mut TypeName) -> *mut c_char { crate::parser::parse_type::TypeNameToString(typeName as _) }
unsafe fn LookupOperWithArgs(oper: *mut ObjectWithArgs, noError: bool) -> Oid { crate::parser::parse_oper::LookupOperWithArgs(oper as _, noError) }
unsafe fn LookupOperName(pstate: *mut c_void, opername: *mut List, oprleft: Oid, oprright: Oid, noError: bool, location: c_int) -> Oid { crate::parser::parse_oper::LookupOperName(pstate as _, opername as _, oprleft, oprright, noError, location) }
unsafe fn LookupFuncWithArgs(objtype: ObjectType, func: *mut ObjectWithArgs, noError: bool) -> Oid { crate::parser::parse_func::LookupFuncWithArgs(objtype, func as _, noError) }

unsafe fn format_type_be(type_oid: Oid) -> *mut c_char { crate::utils::adt::format_type::format_type_be(type_oid) }
unsafe fn get_func_signature(funcid: Oid, argtypes: *mut *mut Oid, nargs: *mut c_int) -> *mut c_char { unimplemented!("STUB get_func_signature") }
unsafe fn op_input_types(opno: Oid, lefttype: *mut Oid, righttype: *mut Oid) { crate::utils::cache::lsyscache::op_input_types(opno, lefttype, righttype) }

unsafe fn ScanKeyInit(entry: *mut ScanKeyData, attributeNumber: c_int, strategy: c_int, procedure: Oid, argument: Datum) { crate::access::common::scankey::ScanKeyInit(entry as _, attributeNumber as _, strategy as _, procedure, argument) }
unsafe fn systable_beginscan(heapRelation: Relation, indexId: Oid, indexOK: bool, snapshot: *mut c_void, nkeys: c_int, key: *mut ScanKeyData) -> SysScanDesc { crate::access::index::genam::systable_beginscan(heapRelation as _, indexId, indexOK, snapshot as _, nkeys, key as _) as _ }
unsafe fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple { crate::access::index::genam::systable_getnext(sysscan as _) as _ }
unsafe fn systable_endscan(sysscan: SysScanDesc) { crate::access::index::genam::systable_endscan(sysscan as _) }

/* The following helpers convert the typed Form_* GETSTRUCT pointers. */
unsafe fn GETSTRUCT_pg_am(tup: HeapTuple) -> Form_pg_am { GETSTRUCT(tup) as Form_pg_am }
unsafe fn GETSTRUCT_pg_opfamily(tup: HeapTuple) -> Form_pg_opfamily { GETSTRUCT(tup) as Form_pg_opfamily }
unsafe fn GETSTRUCT_pg_opclass(tup: HeapTuple) -> Form_pg_opclass { GETSTRUCT(tup) as Form_pg_opclass }
unsafe fn GETSTRUCT_pg_operator(tup: HeapTuple) -> Form_pg_operator { GETSTRUCT(tup) as Form_pg_operator }
unsafe fn GETSTRUCT_pg_proc(tup: HeapTuple) -> Form_pg_proc { GETSTRUCT(tup) as Form_pg_proc }

/* --------------------------------------------------------------------------
 * Field-access shims for the opaque Form_* structs  TODO(pg-port)
 * (mirrors the typecmds.rs precedent: opaque Form + side struct for fields)
 * -------------------------------------------------------------------------- */
#[repr(C)] pub struct FormData_pg_am_fields { pub oid: Oid, pub amname: NameData }
#[repr(C)] pub struct FormData_pg_opfamily_fields { pub oid: Oid }
#[repr(C)] pub struct FormData_pg_opclass_fields {
    pub oid: Oid,
    pub opcmethod: Oid,
    pub opcname: NameData,
    pub opcnamespace: Oid,
    pub opcowner: Oid,
    pub opcfamily: Oid,
    pub opcintype: Oid,
    pub opcdefault: bool,
    pub opckeytype: Oid,
}

/*
 * OpFamilyCacheLookup
 *		Look up an existing opfamily by name.
 *
 * Returns a syscache tuple reference, or NULL if not found.
 */
unsafe fn OpFamilyCacheLookup(amID: Oid, opfamilyname: *mut List, missing_ok: bool) -> HeapTuple {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut opfname: *mut c_char = core::ptr::null_mut();
    let mut htup: HeapTuple;

    /* deconstruct the name list */
    DeconstructQualifiedName(opfamilyname, &mut schemaname, &mut opfname);

    if !schemaname.is_null() {
        /* Look in specific schema only */
        let namespaceId: Oid;

        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if !OidIsValid(namespaceId) {
            htup = core::ptr::null_mut();
        } else {
            htup = SearchSysCache3(OPFAMILYAMNAMENSP,
                                   ObjectIdGetDatum(amID),
                                   PointerGetDatum(opfname as *mut c_void),
                                   ObjectIdGetDatum(namespaceId));
        }
    } else {
        /* Unqualified opfamily name, so search the search path */
        let opfID: Oid = OpfamilynameGetOpfid(amID, opfname);

        if !OidIsValid(opfID) {
            htup = core::ptr::null_mut();
        } else {
            htup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfID));
        }
    }

    if !HeapTupleIsValid(htup) && !missing_ok {
        let amtup: HeapTuple;

        amtup = SearchSysCache1(AMOID, ObjectIdGetDatum(amID));
        if !HeapTupleIsValid(amtup) {
            elog!(ERROR, "cache lookup failed for access method {}", amID);
        }
        ereport!(ERROR,
                 errmsg!("operator family \"{}\" does not exist for access method \"{}\"",
                         cstr_display(NameListToString(opfamilyname)),
                         cstr_display(NameStr(std::ptr::addr_of!((*(GETSTRUCT_pg_am(amtup) as *mut FormData_pg_am_fields)).amname)))));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    return htup;
}

/*
 * get_opfamily_oid
 *	  find an opfamily OID by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error.
 */
pub unsafe fn get_opfamily_oid(amID: Oid, opfamilyname: *mut List, missing_ok: bool) -> Oid {
    let htup: HeapTuple;
    let opfamform: Form_pg_opfamily;
    let opfID: Oid;

    htup = OpFamilyCacheLookup(amID, opfamilyname, missing_ok);
    if !HeapTupleIsValid(htup) {
        return InvalidOid;
    }
    opfamform = GETSTRUCT_pg_opfamily(htup);
    opfID = (*(opfamform as *mut FormData_pg_opfamily_fields)).oid;
    ReleaseSysCache(htup);

    return opfID;
}

/*
 * OpClassCacheLookup
 *		Look up an existing opclass by name.
 *
 * Returns a syscache tuple reference, or NULL if not found.
 */
unsafe fn OpClassCacheLookup(amID: Oid, opclassname: *mut List, missing_ok: bool) -> HeapTuple {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut opcname: *mut c_char = core::ptr::null_mut();
    let mut htup: HeapTuple;

    /* deconstruct the name list */
    DeconstructQualifiedName(opclassname, &mut schemaname, &mut opcname);

    if !schemaname.is_null() {
        /* Look in specific schema only */
        let namespaceId: Oid;

        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if !OidIsValid(namespaceId) {
            htup = core::ptr::null_mut();
        } else {
            htup = SearchSysCache3(CLAAMNAMENSP,
                                   ObjectIdGetDatum(amID),
                                   PointerGetDatum(opcname as *mut c_void),
                                   ObjectIdGetDatum(namespaceId));
        }
    } else {
        /* Unqualified opclass name, so search the search path */
        let opcID: Oid = OpclassnameGetOpcid(amID, opcname);

        if !OidIsValid(opcID) {
            htup = core::ptr::null_mut();
        } else {
            htup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opcID));
        }
    }

    if !HeapTupleIsValid(htup) && !missing_ok {
        let amtup: HeapTuple;

        amtup = SearchSysCache1(AMOID, ObjectIdGetDatum(amID));
        if !HeapTupleIsValid(amtup) {
            elog!(ERROR, "cache lookup failed for access method {}", amID);
        }
        ereport!(ERROR,
                 errmsg!("operator class \"{}\" does not exist for access method \"{}\"",
                         cstr_display(NameListToString(opclassname)),
                         cstr_display(NameStr(std::ptr::addr_of!((*(GETSTRUCT_pg_am(amtup) as *mut FormData_pg_am_fields)).amname)))));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    return htup;
}

/*
 * get_opclass_oid
 *	  find an opclass OID by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error.
 */
pub unsafe fn get_opclass_oid(amID: Oid, opclassname: *mut List, missing_ok: bool) -> Oid {
    let htup: HeapTuple;
    let opcform: Form_pg_opclass;
    let opcID: Oid;

    htup = OpClassCacheLookup(amID, opclassname, missing_ok);
    if !HeapTupleIsValid(htup) {
        return InvalidOid;
    }
    opcform = GETSTRUCT_pg_opclass(htup);
    opcID = (*(opcform as *mut FormData_pg_opclass_fields)).oid;
    ReleaseSysCache(htup);

    return opcID;
}

/*
 * CreateOpFamily
 *		Internal routine to make the catalog entry for a new operator family.
 *
 * Caller must have done permissions checks etc. already.
 */
unsafe fn CreateOpFamily(stmt: *mut CreateOpFamilyStmt, opfname: *const c_char,
                         namespaceoid: Oid, amoid: Oid) -> ObjectAddress {
    let opfamilyoid: Oid;
    let rel: Relation;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_opfamily] = [0 as Datum; Natts_pg_opfamily];
    let mut nulls: [bool; Natts_pg_opfamily] = [false; Natts_pg_opfamily];
    let mut opfName: NameData = core::mem::zeroed();
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    rel = table_open(OperatorFamilyRelationId, RowExclusiveLock);

    /*
     * Make sure there is no existing opfamily of this name (this is just to
     * give a more friendly error message than "duplicate key").
     */
    if SearchSysCacheExists3(OPFAMILYAMNAMENSP,
                             ObjectIdGetDatum(amoid),
                             CStringGetDatum(opfname),
                             ObjectIdGetDatum(namespaceoid)) {
        ereport!(ERROR,
                 errmsg!("operator family \"{}\" for access method \"{}\" already exists",
                         cstr_display(opfname), cstr_display((*stmt).amname)));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    /*
     * Okay, let's create the pg_opfamily entry.
     */
    /* memset(values, 0, sizeof(values)); memset(nulls, false, sizeof(nulls)); -- done at init */

    opfamilyoid = GetNewOidWithIndex(rel, OpfamilyOidIndexId,
                                     Anum_pg_opfamily_oid);
    values[(Anum_pg_opfamily_oid - 1) as usize] = ObjectIdGetDatum(opfamilyoid);
    values[(Anum_pg_opfamily_opfmethod - 1) as usize] = ObjectIdGetDatum(amoid);
    namestrcpy(&mut opfName, opfname);
    values[(Anum_pg_opfamily_opfname - 1) as usize] = NameGetDatum(&opfName);
    values[(Anum_pg_opfamily_opfnamespace - 1) as usize] = ObjectIdGetDatum(namespaceoid);
    values[(Anum_pg_opfamily_opfowner - 1) as usize] = ObjectIdGetDatum(GetUserId());

    tup = heap_form_tuple((*rel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tup);

    heap_freetuple(tup);

    /*
     * Create dependencies for the opfamily proper.
     */
    myself.classId = OperatorFamilyRelationId;
    myself.objectId = opfamilyoid;
    myself.objectSubId = 0;

    /* dependency on access method */
    referenced.classId = AccessMethodRelationId;
    referenced.objectId = amoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = namespaceoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* dependency on owner */
    recordDependencyOnOwner(OperatorFamilyRelationId, opfamilyoid, GetUserId());

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Report the new operator family to possibly interested event triggers */
    EventTriggerCollectSimpleCommand(myself, InvalidObjectAddress,
                                     stmt as *mut Node);

    /* Post creation hook for new operator family */
    InvokeObjectPostCreateHook(OperatorFamilyRelationId, opfamilyoid, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}

/*
 * DefineOpClass
 *		Define a new index operator class.
 */
pub unsafe fn DefineOpClass(stmt: *mut CreateOpClassStmt) -> ObjectAddress {
    let mut opcname: *mut c_char = core::ptr::null_mut();   /* name of opclass we're creating */
    let amoid: Oid;             /* our AM's oid */
    let typeoid: Oid;           /* indexable datatype oid */
    let mut storageoid: Oid;    /* storage datatype oid, if any */
    let namespaceoid: Oid;      /* namespace to create opclass in */
    let opfamilyoid: Oid;       /* oid of containing opfamily */
    let opclassoid: Oid;        /* oid of opclass we create */
    let mut maxOpNumber: c_int; /* amstrategies value */
    let optsProcNumber: c_int;  /* amoptsprocnum value */
    let maxProcNumber: c_int;   /* amsupport value */
    let amstorage: bool;        /* amstorage flag */
    let mut operators: *mut List; /* OpFamilyMember list for operators */
    let mut procedures: *mut List; /* OpFamilyMember list for support procs */
    let mut rel: Relation;
    let mut tup: HeapTuple;
    let amform: Form_pg_am;
    let amroutine: *mut IndexAmRoutine;
    let mut values: [Datum; Natts_pg_opclass] = [0 as Datum; Natts_pg_opclass];
    let mut nulls: [bool; Natts_pg_opclass] = [false; Natts_pg_opclass];
    let aclresult: AclResult;
    let mut opcName: NameData = core::mem::zeroed();
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    /* Convert list of names to a name and namespace */
    namespaceoid = QualifiedNameGetCreationNamespace((*stmt).opclassname,
                                                     &mut opcname);

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, namespaceoid, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA,
                       get_namespace_name(namespaceoid));
    }

    /* Get necessary info about access method */
    if std::env::var_os("PDB_AM").is_some() { eprintln!("PDB_AM amname={:?} AMNAME_id={}", cstr_display((*stmt).amname), AMNAME); }
    tup = SearchSysCache1(AMNAME, CStringGetDatum((*stmt).amname));
    if std::env::var_os("PDB_AM").is_some() { eprintln!("PDB_AM SearchSysCache1(AMNAME) valid={}", HeapTupleIsValid(tup)); }
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 errmsg!("access method \"{}\" does not exist",
                         cstr_display((*stmt).amname)));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    amform = GETSTRUCT_pg_am(tup);
    amoid = (*(amform as *mut FormData_pg_am_fields)).oid;
    amroutine = GetIndexAmRoutineByAmId(amoid, false);
    ReleaseSysCache(tup);

    maxOpNumber = (*amroutine).amstrategies as c_int;
    /* if amstrategies is zero, just enforce that op numbers fit in int16 */
    if maxOpNumber <= 0 {
        maxOpNumber = SHRT_MAX;
    }
    maxProcNumber = (*amroutine).amsupport as c_int;
    optsProcNumber = (*amroutine).amoptsprocnum as c_int;
    amstorage = (*amroutine).amstorage;

    /* XXX Should we make any privilege check against the AM? */

    /*
     * The question of appropriate permissions for CREATE OPERATOR CLASS is
     * interesting.  Creating an opclass is tantamount to granting public
     * execute access on the functions involved, since the index machinery
     * generally does not check access permission before using the functions.
     * A minimum expectation therefore is that the caller have execute
     * privilege with grant option.  Since we don't have a way to make the
     * opclass go away if the grant option is revoked, we choose instead to
     * require ownership of the functions.  It's also not entirely clear what
     * permissions should be required on the datatype, but ownership seems
     * like a safe choice.
     *
     * Currently, we require superuser privileges to create an opclass. This
     * seems necessary because we have no way to validate that the offered set
     * of operators and functions are consistent with the AM's expectations.
     * It would be nice to provide such a check someday, if it can be done
     * without solving the halting problem :-(
     *
     * XXX re-enable NOT_USED code sections below if you remove this test.
     */
    if !superuser() {
        ereport!(ERROR,
                 errmsg!("must be superuser to create an operator class"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /* Look up the datatype */
    typeoid = typenameTypeId(core::ptr::null_mut(), (*stmt).datatype);

    /* #ifdef NOT_USED: ownership check on datatype omitted (superuser check above) */

    /*
     * Look up the containing operator family, or create one if FAMILY option
     * was omitted and there's not a match already.
     */
    if !(*stmt).opfamilyname.is_null() {
        opfamilyoid = get_opfamily_oid(amoid, (*stmt).opfamilyname, false);
    } else {
        /* Lookup existing family of same name and namespace */
        tup = SearchSysCache3(OPFAMILYAMNAMENSP,
                              ObjectIdGetDatum(amoid),
                              PointerGetDatum(opcname as *mut c_void),
                              ObjectIdGetDatum(namespaceoid));
        if HeapTupleIsValid(tup) {
            opfamilyoid = (*(GETSTRUCT_pg_opfamily(tup) as *mut FormData_pg_opfamily_fields)).oid;

            /*
             * XXX given the superuser check above, there's no need for an
             * ownership check here
             */
            ReleaseSysCache(tup);
        } else {
            let opfstmt: *mut CreateOpFamilyStmt;
            let tmpAddr: ObjectAddress;

            opfstmt = makeNode!(CreateOpFamilyStmt, T_CreateOpFamilyStmt);
            (*opfstmt).opfamilyname = (*stmt).opclassname;
            (*opfstmt).amname = (*stmt).amname;

            /*
             * Create it ... again no need for more permissions ...
             */
            tmpAddr = CreateOpFamily(opfstmt, opcname, namespaceoid, amoid);
            opfamilyoid = tmpAddr.objectId;
        }
    }

    operators = NIL;
    procedures = NIL;

    /* Storage datatype is optional */
    storageoid = InvalidOid;

    /*
     * Scan the "items" list to obtain additional info.
     */
    foreach!(l, (*stmt).items, {
        let item: *mut CreateOpClassItem = lfirst_node!(CreateOpClassItem, T_CreateOpClassItem, current_cell!(l));
        let mut operOid: Oid;
        let mut funcOid: Oid;
        let sortfamilyOid: Oid;
        let member: *mut OpFamilyMember;

        match (*item).itemtype {
            OPCLASS_ITEM_OPERATOR => {
                if (*item).number <= 0 || (*item).number > maxOpNumber {
                    ereport!(ERROR,
                             errmsg!("invalid operator number {}, must be between 1 and {}",
                                     (*item).number, maxOpNumber));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                if (*(*item).name).objargs != NIL {
                    operOid = LookupOperWithArgs((*item).name, false);
                } else {
                    /* Default to binary op on input datatype */
                    operOid = LookupOperName(core::ptr::null_mut(), (*(*item).name).objname,
                                             typeoid, typeoid,
                                             false, -1);
                }

                if !(*item).order_family.is_null() {
                    sortfamilyOid = get_opfamily_oid(BTREE_AM_OID,
                                                     (*item).order_family,
                                                     false);
                } else {
                    sortfamilyOid = InvalidOid;
                }

                /* #ifdef NOT_USED: ownership checks omitted (superuser check above) */

                /* Save the info */
                member = palloc0(core::mem::size_of::<OpFamilyMember>()) as *mut OpFamilyMember;
                (*member).is_func = false;
                (*member).object = operOid;
                (*member).number = (*item).number;
                (*member).sortfamily = sortfamilyOid;
                assignOperTypes(member, amoid, typeoid);
                addFamilyMember(&mut operators, member);
            }
            OPCLASS_ITEM_FUNCTION => {
                if (*item).number <= 0 || (*item).number > maxProcNumber {
                    ereport!(ERROR,
                             errmsg!("invalid function number {}, must be between 1 and {}",
                                     (*item).number, maxProcNumber));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                funcOid = LookupFuncWithArgs(OBJECT_FUNCTION, (*item).name, false);
                /* #ifdef NOT_USED: ownership check omitted (superuser check above) */
                /* Save the info */
                member = palloc0(core::mem::size_of::<OpFamilyMember>()) as *mut OpFamilyMember;
                (*member).is_func = true;
                (*member).object = funcOid;
                (*member).number = (*item).number;

                /* allow overriding of the function's actual arg types */
                if !(*item).class_args.is_null() {
                    processTypesSpec((*item).class_args,
                                     &mut (*member).lefttype, &mut (*member).righttype);
                }

                assignProcTypes(member, amoid, typeoid, optsProcNumber);
                addFamilyMember(&mut procedures, member);
            }
            OPCLASS_ITEM_STORAGETYPE => {
                if OidIsValid(storageoid) {
                    ereport!(ERROR,
                             errmsg!("storage type specified more than once"));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                storageoid = typenameTypeId(core::ptr::null_mut(), (*item).storedtype);

                /* #ifdef NOT_USED: ownership check on datatype omitted (superuser check above) */
            }
            _ => {
                elog!(ERROR, "unrecognized item type: {}", (*item).itemtype);
            }
        }
    });

    /*
     * If storagetype is specified, make sure it's legal.
     */
    if OidIsValid(storageoid) {
        /* Just drop the spec if same as column datatype */
        if storageoid == typeoid {
            storageoid = InvalidOid;
        } else if !amstorage {
            ereport!(ERROR,
                     errmsg!("storage type cannot be different from data type for access method \"{}\"",
                             cstr_display((*stmt).amname)));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
    }

    rel = table_open(OperatorClassRelationId, RowExclusiveLock);

    /*
     * Make sure there is no existing opclass of this name (this is just to
     * give a more friendly error message than "duplicate key").
     */
    if SearchSysCacheExists3(CLAAMNAMENSP,
                             ObjectIdGetDatum(amoid),
                             CStringGetDatum(opcname),
                             ObjectIdGetDatum(namespaceoid)) {
        ereport!(ERROR,
                 errmsg!("operator class \"{}\" for access method \"{}\" already exists",
                         cstr_display(opcname), cstr_display((*stmt).amname)));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    /*
     * If we are creating a default opclass, check there isn't one already.
     * (Note we do not restrict this test to visible opclasses; this ensures
     * that typcache.c can find unique solutions to its questions.)
     */
    if (*stmt).isDefault {
        let mut skey: [ScanKeyData; 1] = [core::mem::zeroed()];
        let scan: SysScanDesc;

        ScanKeyInit(&mut skey[0],
                    Anum_pg_opclass_opcmethod,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(amoid));

        scan = systable_beginscan(rel, OpclassAmNameNspIndexId, true,
                                  core::ptr::null_mut(), 1, skey.as_mut_ptr());

        loop {
            tup = systable_getnext(scan);
            if !HeapTupleIsValid(tup) {
                break;
            }
            let opclass: Form_pg_opclass = GETSTRUCT_pg_opclass(tup);
            let opclassf = opclass as *mut FormData_pg_opclass_fields;

            if (*opclassf).opcintype == typeoid && (*opclassf).opcdefault {
                ereport!(ERROR,
                         errmsg!("could not make operator class \"{}\" be default for type {}",
                                 cstr_display(opcname),
                                 cstr_display(TypeNameToString((*stmt).datatype))));
                /* C also: errcode(ERRCODE_DUPLICATE_OBJECT),
                 * errdetail("Operator class \"%s\" already is the default.", NameStr(opclass->opcname)) */
            }
        }

        systable_endscan(scan);
    }

    /*
     * Okay, let's create the pg_opclass entry.
     */
    /* memset(values, 0, ...); memset(nulls, false, ...); -- done at init */

    opclassoid = GetNewOidWithIndex(rel, OpclassOidIndexId,
                                    Anum_pg_opclass_oid);
    values[(Anum_pg_opclass_oid - 1) as usize] = ObjectIdGetDatum(opclassoid);
    values[(Anum_pg_opclass_opcmethod - 1) as usize] = ObjectIdGetDatum(amoid);
    namestrcpy(&mut opcName, opcname);
    values[(Anum_pg_opclass_opcname - 1) as usize] = NameGetDatum(&opcName);
    values[(Anum_pg_opclass_opcnamespace - 1) as usize] = ObjectIdGetDatum(namespaceoid);
    values[(Anum_pg_opclass_opcowner - 1) as usize] = ObjectIdGetDatum(GetUserId());
    values[(Anum_pg_opclass_opcfamily - 1) as usize] = ObjectIdGetDatum(opfamilyoid);
    values[(Anum_pg_opclass_opcintype - 1) as usize] = ObjectIdGetDatum(typeoid);
    values[(Anum_pg_opclass_opcdefault - 1) as usize] = BoolGetDatum((*stmt).isDefault);
    values[(Anum_pg_opclass_opckeytype - 1) as usize] = ObjectIdGetDatum(storageoid);

    tup = heap_form_tuple((*rel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tup);

    heap_freetuple(tup);

    /*
     * Now that we have the opclass OID, set up default dependency info for
     * the pg_amop and pg_amproc entries.  Historically, CREATE OPERATOR CLASS
     * has created hard dependencies on the opclass, so that's what we use.
     */
    foreach!(l, operators, {
        let op: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;

        (*op).ref_is_hard = true;
        (*op).ref_is_family = false;
        (*op).refobjid = opclassoid;
    });
    foreach!(l, procedures, {
        let proc: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;

        (*proc).ref_is_hard = true;
        (*proc).ref_is_family = false;
        (*proc).refobjid = opclassoid;
    });

    /*
     * Let the index AM editorialize on the dependency choices.  It could also
     * do further validation on the operators and functions, if it likes.
     */
    if let Some(amadjustmembers) = (*amroutine).amadjustmembers {
        amadjustmembers(opfamilyoid,
                        opclassoid,
                        operators,
                        procedures);
    }

    /*
     * Now add tuples to pg_amop and pg_amproc tying in the operators and
     * functions.  Dependencies on them are inserted, too.
     */
    storeOperators((*stmt).opfamilyname, amoid, opfamilyoid,
                   operators, false);
    storeProcedures((*stmt).opfamilyname, amoid, opfamilyoid,
                    procedures, false);

    /* let event triggers know what happened */
    EventTriggerCollectCreateOpClass(stmt, opclassoid, operators, procedures);

    /*
     * Create dependencies for the opclass proper.  Note: we do not need a
     * dependency link to the AM, because that exists through the opfamily.
     */
    myself.classId = OperatorClassRelationId;
    myself.objectId = opclassoid;
    myself.objectSubId = 0;

    /* dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = namespaceoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* dependency on opfamily */
    referenced.classId = OperatorFamilyRelationId;
    referenced.objectId = opfamilyoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* dependency on indexed datatype */
    referenced.classId = TypeRelationId;
    referenced.objectId = typeoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* dependency on storage datatype */
    if OidIsValid(storageoid) {
        referenced.classId = TypeRelationId;
        referenced.objectId = storageoid;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /* dependency on owner */
    recordDependencyOnOwner(OperatorClassRelationId, opclassoid, GetUserId());

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new operator class */
    InvokeObjectPostCreateHook(OperatorClassRelationId, opclassoid, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}


/*
 * DefineOpFamily
 *		Define a new index operator family.
 */
pub unsafe fn DefineOpFamily(stmt: *mut CreateOpFamilyStmt) -> ObjectAddress {
    let mut opfname: *mut c_char = core::ptr::null_mut();   /* name of opfamily we're creating */
    let amoid: Oid;             /* our AM's oid */
    let namespaceoid: Oid;      /* namespace to create opfamily in */
    let aclresult: AclResult;

    /* Convert list of names to a name and namespace */
    namespaceoid = QualifiedNameGetCreationNamespace((*stmt).opfamilyname,
                                                     &mut opfname);

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, namespaceoid, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA,
                       get_namespace_name(namespaceoid));
    }

    /* Get access method OID, throwing an error if it doesn't exist. */
    amoid = get_index_am_oid((*stmt).amname, false);

    /* XXX Should we make any privilege check against the AM? */

    /*
     * Currently, we require superuser privileges to create an opfamily. See
     * comments in DefineOpClass.
     */
    if !superuser() {
        ereport!(ERROR,
                 errmsg!("must be superuser to create an operator family"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /* Insert pg_opfamily catalog entry */
    return CreateOpFamily(stmt, opfname, namespaceoid, amoid);
}


/*
 * AlterOpFamily
 *		Add or remove operators/procedures within an existing operator family.
 *
 * Note: this implements only ALTER OPERATOR FAMILY ... ADD/DROP.  Some
 * other commands called ALTER OPERATOR FAMILY exist, but go through
 * different code paths.
 */
pub unsafe fn AlterOpFamily(stmt: *mut AlterOpFamilyStmt) -> Oid {
    let amoid: Oid;             /* our AM's oid */
    let opfamilyoid: Oid;       /* oid of opfamily */
    let mut maxOpNumber: c_int; /* amstrategies value */
    let optsProcNumber: c_int;  /* amoptsprocnum value */
    let maxProcNumber: c_int;   /* amsupport value */
    let tup: HeapTuple;
    let amform: Form_pg_am;
    let amroutine: *mut IndexAmRoutine;

    /* Get necessary info about access method */
    if std::env::var_os("PDB_AM").is_some() { eprintln!("PDB_AM amname={:?} AMNAME_id={}", cstr_display((*stmt).amname), AMNAME); }
    tup = SearchSysCache1(AMNAME, CStringGetDatum((*stmt).amname));
    if std::env::var_os("PDB_AM").is_some() { eprintln!("PDB_AM SearchSysCache1(AMNAME) valid={}", HeapTupleIsValid(tup)); }
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 errmsg!("access method \"{}\" does not exist",
                         cstr_display((*stmt).amname)));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    amform = GETSTRUCT_pg_am(tup);
    amoid = (*(amform as *mut FormData_pg_am_fields)).oid;
    amroutine = GetIndexAmRoutineByAmId(amoid, false);
    ReleaseSysCache(tup);

    maxOpNumber = (*amroutine).amstrategies as c_int;
    /* if amstrategies is zero, just enforce that op numbers fit in int16 */
    if maxOpNumber <= 0 {
        maxOpNumber = SHRT_MAX;
    }
    maxProcNumber = (*amroutine).amsupport as c_int;
    optsProcNumber = (*amroutine).amoptsprocnum as c_int;

    /* XXX Should we make any privilege check against the AM? */

    /* Look up the opfamily */
    opfamilyoid = get_opfamily_oid(amoid, (*stmt).opfamilyname, false);

    /*
     * Currently, we require superuser privileges to alter an opfamily.
     *
     * XXX re-enable NOT_USED code sections below if you remove this test.
     */
    if !superuser() {
        ereport!(ERROR,
                 errmsg!("must be superuser to alter an operator family"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /*
     * ADD and DROP cases need separate code from here on down.
     */
    if (*stmt).isDrop {
        AlterOpFamilyDrop(stmt, amoid, opfamilyoid,
                          maxOpNumber, maxProcNumber, (*stmt).items);
    } else {
        AlterOpFamilyAdd(stmt, amoid, opfamilyoid,
                         maxOpNumber, maxProcNumber, optsProcNumber,
                         (*stmt).items);
    }

    return opfamilyoid;
}

/*
 * ADD part of ALTER OP FAMILY
 */
unsafe fn AlterOpFamilyAdd(stmt: *mut AlterOpFamilyStmt, amoid: Oid, opfamilyoid: Oid,
                           maxOpNumber: c_int, maxProcNumber: c_int, optsProcNumber: c_int,
                           items: *mut List) {
    let amroutine: *mut IndexAmRoutine = GetIndexAmRoutineByAmId(amoid, false);
    let mut operators: *mut List; /* OpFamilyMember list for operators */
    let mut procedures: *mut List; /* OpFamilyMember list for support procs */

    operators = NIL;
    procedures = NIL;

    /*
     * Scan the "items" list to obtain additional info.
     */
    foreach!(l, items, {
        let item: *mut CreateOpClassItem = lfirst_node!(CreateOpClassItem, T_CreateOpClassItem, current_cell!(l));
        let mut operOid: Oid;
        let mut funcOid: Oid;
        let sortfamilyOid: Oid;
        let member: *mut OpFamilyMember;

        match (*item).itemtype {
            OPCLASS_ITEM_OPERATOR => {
                if (*item).number <= 0 || (*item).number > maxOpNumber {
                    ereport!(ERROR,
                             errmsg!("invalid operator number {}, must be between 1 and {}",
                                     (*item).number, maxOpNumber));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                if (*(*item).name).objargs != NIL {
                    operOid = LookupOperWithArgs((*item).name, false);
                } else {
                    ereport!(ERROR,
                             errmsg!("operator argument types must be specified in ALTER OPERATOR FAMILY"));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                    operOid = InvalidOid;   /* keep compiler quiet */
                }

                if !(*item).order_family.is_null() {
                    sortfamilyOid = get_opfamily_oid(BTREE_AM_OID,
                                                     (*item).order_family,
                                                     false);
                } else {
                    sortfamilyOid = InvalidOid;
                }

                /* #ifdef NOT_USED: ownership checks omitted (superuser check above) */

                /* Save the info */
                member = palloc0(core::mem::size_of::<OpFamilyMember>()) as *mut OpFamilyMember;
                (*member).is_func = false;
                (*member).object = operOid;
                (*member).number = (*item).number;
                (*member).sortfamily = sortfamilyOid;
                /* We can set up dependency fields immediately */
                /* Historically, ALTER ADD has created soft dependencies */
                (*member).ref_is_hard = false;
                (*member).ref_is_family = true;
                (*member).refobjid = opfamilyoid;
                assignOperTypes(member, amoid, InvalidOid);
                addFamilyMember(&mut operators, member);
            }
            OPCLASS_ITEM_FUNCTION => {
                if (*item).number <= 0 || (*item).number > maxProcNumber {
                    ereport!(ERROR,
                             errmsg!("invalid function number {}, must be between 1 and {}",
                                     (*item).number, maxProcNumber));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                funcOid = LookupFuncWithArgs(OBJECT_FUNCTION, (*item).name, false);
                /* #ifdef NOT_USED: ownership check omitted (superuser check above) */

                /* Save the info */
                member = palloc0(core::mem::size_of::<OpFamilyMember>()) as *mut OpFamilyMember;
                (*member).is_func = true;
                (*member).object = funcOid;
                (*member).number = (*item).number;
                /* We can set up dependency fields immediately */
                /* Historically, ALTER ADD has created soft dependencies */
                (*member).ref_is_hard = false;
                (*member).ref_is_family = true;
                (*member).refobjid = opfamilyoid;

                /* allow overriding of the function's actual arg types */
                if !(*item).class_args.is_null() {
                    processTypesSpec((*item).class_args,
                                     &mut (*member).lefttype, &mut (*member).righttype);
                }

                assignProcTypes(member, amoid, InvalidOid, optsProcNumber);
                addFamilyMember(&mut procedures, member);
            }
            OPCLASS_ITEM_STORAGETYPE => {
                ereport!(ERROR,
                         errmsg!("STORAGE cannot be specified in ALTER OPERATOR FAMILY"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            }
            _ => {
                elog!(ERROR, "unrecognized item type: {}", (*item).itemtype);
            }
        }
    });

    /*
     * Let the index AM editorialize on the dependency choices.  It could also
     * do further validation on the operators and functions, if it likes.
     */
    if let Some(amadjustmembers) = (*amroutine).amadjustmembers {
        amadjustmembers(opfamilyoid,
                        InvalidOid,     /* no specific opclass */
                        operators,
                        procedures);
    }

    /*
     * Add tuples to pg_amop and pg_amproc tying in the operators and
     * functions.  Dependencies on them are inserted, too.
     */
    storeOperators((*stmt).opfamilyname, amoid, opfamilyoid,
                   operators, true);
    storeProcedures((*stmt).opfamilyname, amoid, opfamilyoid,
                    procedures, true);

    /* make information available to event triggers */
    EventTriggerCollectAlterOpFam(stmt, opfamilyoid,
                                  operators, procedures);
}

/*
 * DROP part of ALTER OP FAMILY
 */
unsafe fn AlterOpFamilyDrop(stmt: *mut AlterOpFamilyStmt, amoid: Oid, opfamilyoid: Oid,
                            maxOpNumber: c_int, maxProcNumber: c_int, items: *mut List) {
    let mut operators: *mut List; /* OpFamilyMember list for operators */
    let mut procedures: *mut List; /* OpFamilyMember list for support procs */

    operators = NIL;
    procedures = NIL;

    /*
     * Scan the "items" list to obtain additional info.
     */
    foreach!(l, items, {
        let item: *mut CreateOpClassItem = lfirst_node!(CreateOpClassItem, T_CreateOpClassItem, current_cell!(l));
        let mut lefttype: Oid = InvalidOid;
        let mut righttype: Oid = InvalidOid;
        let member: *mut OpFamilyMember;

        match (*item).itemtype {
            OPCLASS_ITEM_OPERATOR => {
                if (*item).number <= 0 || (*item).number > maxOpNumber {
                    ereport!(ERROR,
                             errmsg!("invalid operator number {}, must be between 1 and {}",
                                     (*item).number, maxOpNumber));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                processTypesSpec((*item).class_args, &mut lefttype, &mut righttype);
                /* Save the info */
                member = palloc0(core::mem::size_of::<OpFamilyMember>()) as *mut OpFamilyMember;
                (*member).is_func = false;
                (*member).number = (*item).number;
                (*member).lefttype = lefttype;
                (*member).righttype = righttype;
                addFamilyMember(&mut operators, member);
            }
            OPCLASS_ITEM_FUNCTION => {
                if (*item).number <= 0 || (*item).number > maxProcNumber {
                    ereport!(ERROR,
                             errmsg!("invalid function number {}, must be between 1 and {}",
                                     (*item).number, maxProcNumber));
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                }
                processTypesSpec((*item).class_args, &mut lefttype, &mut righttype);
                /* Save the info */
                member = palloc0(core::mem::size_of::<OpFamilyMember>()) as *mut OpFamilyMember;
                (*member).is_func = true;
                (*member).number = (*item).number;
                (*member).lefttype = lefttype;
                (*member).righttype = righttype;
                addFamilyMember(&mut procedures, member);
            }
            OPCLASS_ITEM_STORAGETYPE => {
                /* grammar prevents this from appearing */
                elog!(ERROR, "unrecognized item type: {}", (*item).itemtype);
            }
            _ => {
                elog!(ERROR, "unrecognized item type: {}", (*item).itemtype);
            }
        }
    });

    /*
     * Remove tuples from pg_amop and pg_amproc.
     */
    dropOperators((*stmt).opfamilyname, amoid, opfamilyoid, operators);
    dropProcedures((*stmt).opfamilyname, amoid, opfamilyoid, procedures);

    /* make information available to event triggers */
    EventTriggerCollectAlterOpFam(stmt, opfamilyoid,
                                  operators, procedures);
}


/*
 * Deal with explicit arg types used in ALTER ADD/DROP
 */
unsafe fn processTypesSpec(args: *mut List, lefttype: *mut Oid, righttype: *mut Oid) {
    let mut typeName: *mut TypeName;

    Assert!(args != NIL);

    typeName = linitial(args) as *mut TypeName;
    *lefttype = typenameTypeId(core::ptr::null_mut(), typeName);

    if list_length(args) > 1 {
        typeName = lsecond(args) as *mut TypeName;
        *righttype = typenameTypeId(core::ptr::null_mut(), typeName);
    } else {
        *righttype = *lefttype;
    }

    if list_length(args) > 2 {
        ereport!(ERROR,
                 errmsg!("one or two argument types must be specified"));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
    }
}


/*
 * Determine the lefttype/righttype to assign to an operator,
 * and do any validity checking we can manage.
 */
unsafe fn assignOperTypes(member: *mut OpFamilyMember, amoid: Oid, typeoid: Oid) {
    let optup: Operator;
    let opform: Form_pg_operator;

    /* Fetch the operator definition */
    optup = SearchSysCache1(OPEROID, ObjectIdGetDatum((*member).object));
    if !HeapTupleIsValid(optup) {
        elog!(ERROR, "cache lookup failed for operator {}", (*member).object);
    }
    opform = GETSTRUCT_pg_operator(optup);

    /*
     * Opfamily operators must be binary.
     */
    if (*opform).oprkind != b'b' as c_char {
        ereport!(ERROR,
                 errmsg!("index operators must be binary"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if OidIsValid((*member).sortfamily) {
        /*
         * Ordering op, check index supports that.  (We could perhaps also
         * check that the operator returns a type supported by the sortfamily,
         * but that seems more trouble than it's worth here.  If it does not,
         * the operator will never be matchable to any ORDER BY clause, but no
         * worse consequences can ensue.  Also, trying to check that would
         * create an ordering hazard during dump/reload: it's possible that
         * the family has been created but not yet populated with the required
         * operators.)
         */
        let amroutine: *mut IndexAmRoutine = GetIndexAmRoutineByAmId(amoid, false);

        if !(*amroutine).amcanorderbyop {
            ereport!(ERROR,
                     errmsg!("access method \"{}\" does not support ordering operators",
                             cstr_display(get_am_name(amoid))));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
    } else {
        /*
         * Search operators must return boolean.
         */
        if (*opform).oprresult != BOOLOID {
            ereport!(ERROR,
                     errmsg!("index search operators must return boolean"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
    }

    /*
     * If lefttype/righttype isn't specified, use the operator's input types
     */
    if !OidIsValid((*member).lefttype) {
        (*member).lefttype = (*opform).oprleft;
    }
    if !OidIsValid((*member).righttype) {
        (*member).righttype = (*opform).oprright;
    }

    ReleaseSysCache(optup);
}

/*
 * Determine the lefttype/righttype to assign to a support procedure,
 * and do any validity checking we can manage.
 */
unsafe fn assignProcTypes(member: *mut OpFamilyMember, amoid: Oid, typeoid: Oid,
                          opclassOptsProcNum: c_int) {
    let proctup: HeapTuple;
    let procform: Form_pg_proc;

    /* Fetch the procedure definition */
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum((*member).object));
    if !HeapTupleIsValid(proctup) {
        elog!(ERROR, "cache lookup failed for function {}", (*member).object);
    }
    procform = GETSTRUCT_pg_proc(proctup);

    /* Check the signature of the opclass options parsing function */
    if (*member).number == opclassOptsProcNum {
        if OidIsValid(typeoid) {
            if (OidIsValid((*member).lefttype) && (*member).lefttype != typeoid) ||
               (OidIsValid((*member).righttype) && (*member).righttype != typeoid) {
                ereport!(ERROR,
                         errmsg!("associated data types for operator class options parsing functions must match opclass input type"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        } else {
            if (*member).lefttype != (*member).righttype {
                ereport!(ERROR,
                         errmsg!("left and right associated data types for operator class options parsing functions must match"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        }

        if (*procform).prorettype != VOIDOID ||
           (*procform).pronargs != 1 ||
           *(*procform).proargtypes.values.as_ptr() != INTERNALOID {
            ereport!(ERROR,
                     errmsg!("invalid operator class options parsing function"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
             * errhint("Valid signature of operator class options parsing function is %s.", "(internal) RETURNS void") */
        }
    }
    /*
     * Ordering comparison procs must be 2-arg procs returning int4.  Ordering
     * sortsupport procs must take internal and return void.  Ordering
     * in_range procs must be 5-arg procs returning bool.  Ordering equalimage
     * procs must take 1 arg and return bool.  Hashing support proc 1 must be
     * a 1-arg proc returning int4, while proc 2 must be a 2-arg proc
     * returning int8. Otherwise we don't know.
     */
    else if (*GetIndexAmRoutineByAmId(amoid, false)).amcanorder {
        if (*member).number == BTORDER_PROC {
            if (*procform).pronargs != 2 {
                ereport!(ERROR,
                         errmsg!("ordering comparison functions must have two arguments"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != INT4OID {
                ereport!(ERROR,
                         errmsg!("ordering comparison functions must return integer"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            /*
             * If lefttype/righttype isn't specified, use the proc's input
             * types
             */
            if !OidIsValid((*member).lefttype) {
                (*member).lefttype = *(*procform).proargtypes.values.as_ptr();
            }
            if !OidIsValid((*member).righttype) {
                (*member).righttype = *(*procform).proargtypes.values.as_ptr().add(1);
            }
        } else if (*member).number == BTSORTSUPPORT_PROC {
            if (*procform).pronargs != 1 ||
               *(*procform).proargtypes.values.as_ptr() != INTERNALOID {
                ereport!(ERROR,
                         errmsg!("ordering sort support functions must accept type \"internal\""));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != VOIDOID {
                ereport!(ERROR,
                         errmsg!("ordering sort support functions must return void"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            /*
             * Can't infer lefttype/righttype from proc, so use default rule
             */
        } else if (*member).number == BTINRANGE_PROC {
            if (*procform).pronargs != 5 {
                ereport!(ERROR,
                         errmsg!("ordering in_range functions must have five arguments"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != BOOLOID {
                ereport!(ERROR,
                         errmsg!("ordering in_range functions must return boolean"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            /*
             * If lefttype/righttype isn't specified, use the proc's input
             * types (we look at the test-value and offset arguments)
             */
            if !OidIsValid((*member).lefttype) {
                (*member).lefttype = *(*procform).proargtypes.values.as_ptr();
            }
            if !OidIsValid((*member).righttype) {
                (*member).righttype = *(*procform).proargtypes.values.as_ptr().add(2);
            }
        } else if (*member).number == BTEQUALIMAGE_PROC {
            if (*procform).pronargs != 1 {
                ereport!(ERROR,
                         errmsg!("ordering equal image functions must have one argument"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != BOOLOID {
                ereport!(ERROR,
                         errmsg!("ordering equal image functions must return boolean"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            /*
             * pg_amproc functions are indexed by (lefttype, righttype), but
             * an equalimage function can only be called at CREATE INDEX time.
             * The same opclass opcintype OID is always used for lefttype and
             * righttype.  Providing a cross-type routine isn't sensible.
             * Reject cross-type ALTER OPERATOR FAMILY ...  ADD FUNCTION 4
             * statements here.
             */
            if (*member).lefttype != (*member).righttype {
                ereport!(ERROR,
                         errmsg!("ordering equal image functions must not be cross-type"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        } else if (*member).number == BTSKIPSUPPORT_PROC {
            if (*procform).pronargs != 1 ||
               *(*procform).proargtypes.values.as_ptr() != INTERNALOID {
                ereport!(ERROR,
                         errmsg!("btree skip support functions must accept type \"internal\""));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != VOIDOID {
                ereport!(ERROR,
                         errmsg!("btree skip support functions must return void"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }

            /*
             * pg_amproc functions are indexed by (lefttype, righttype), but a
             * skip support function doesn't make sense in cross-type
             * scenarios.  The same opclass opcintype OID is always used for
             * lefttype and righttype.  Providing a cross-type routine isn't
             * sensible.  Reject cross-type ALTER OPERATOR FAMILY ...  ADD
             * FUNCTION 6 statements here.
             */
            if (*member).lefttype != (*member).righttype {
                ereport!(ERROR,
                         errmsg!("btree skip support functions must not be cross-type"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        }
    } else if (*GetIndexAmRoutineByAmId(amoid, false)).amcanhash {
        if (*member).number == HASHSTANDARD_PROC {
            if (*procform).pronargs != 1 {
                ereport!(ERROR,
                         errmsg!("hash function 1 must have one argument"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != INT4OID {
                ereport!(ERROR,
                         errmsg!("hash function 1 must return integer"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        } else if (*member).number == HASHEXTENDED_PROC {
            if (*procform).pronargs != 2 {
                ereport!(ERROR,
                         errmsg!("hash function 2 must have two arguments"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
            if (*procform).prorettype != INT8OID {
                ereport!(ERROR,
                         errmsg!("hash function 2 must return bigint"));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        }

        /*
         * If lefttype/righttype isn't specified, use the proc's input type
         */
        if !OidIsValid((*member).lefttype) {
            (*member).lefttype = *(*procform).proargtypes.values.as_ptr();
        }
        if !OidIsValid((*member).righttype) {
            (*member).righttype = *(*procform).proargtypes.values.as_ptr();
        }
    }

    /*
     * The default in CREATE OPERATOR CLASS is to use the class' opcintype as
     * lefttype and righttype.  In CREATE or ALTER OPERATOR FAMILY, opcintype
     * isn't available, so make the user specify the types.
     */
    if !OidIsValid((*member).lefttype) {
        (*member).lefttype = typeoid;
    }
    if !OidIsValid((*member).righttype) {
        (*member).righttype = typeoid;
    }

    if !OidIsValid((*member).lefttype) || !OidIsValid((*member).righttype) {
        ereport!(ERROR,
                 errmsg!("associated data types must be specified for index support function"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    ReleaseSysCache(proctup);
}

/*
 * Add a new family member to the appropriate list, after checking for
 * duplicated strategy or proc number.
 */
unsafe fn addFamilyMember(list: *mut *mut List, member: *mut OpFamilyMember) {
    foreach!(l, *list, {
        let old: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;

        if (*old).number == (*member).number &&
           (*old).lefttype == (*member).lefttype &&
           (*old).righttype == (*member).righttype {
            if (*member).is_func {
                ereport!(ERROR,
                         errmsg!("function number {} for ({},{}) appears more than once",
                                 (*member).number,
                                 cstr_display(format_type_be((*member).lefttype)),
                                 cstr_display(format_type_be((*member).righttype))));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            } else {
                ereport!(ERROR,
                         errmsg!("operator number {} for ({},{}) appears more than once",
                                 (*member).number,
                                 cstr_display(format_type_be((*member).lefttype)),
                                 cstr_display(format_type_be((*member).righttype))));
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        }
    });
    *list = lappend(*list, member as *mut c_void);
}

/*
 * Dump the operators to pg_amop
 *
 * We also make dependency entries in pg_depend for the pg_amop entries.
 */
unsafe fn storeOperators(opfamilyname: *mut List, amoid: Oid, opfamilyoid: Oid,
                         operators: *mut List, isAdd: bool) {
    let rel: Relation;
    let mut values: [Datum; Natts_pg_amop] = [0 as Datum; Natts_pg_amop];
    let mut nulls: [bool; Natts_pg_amop] = [false; Natts_pg_amop];
    let mut tup: HeapTuple;
    let mut entryoid: Oid;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    rel = table_open(AccessMethodOperatorRelationId, RowExclusiveLock);

    foreach!(l, operators, {
        let op: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;
        let oppurpose: c_char;

        /*
         * If adding to an existing family, check for conflict with an
         * existing pg_amop entry (just to give a nicer error message)
         */
        if isAdd &&
           SearchSysCacheExists4(AMOPSTRATEGY,
                                 ObjectIdGetDatum(opfamilyoid),
                                 ObjectIdGetDatum((*op).lefttype),
                                 ObjectIdGetDatum((*op).righttype),
                                 Int16GetDatum((*op).number as int16)) {
            ereport!(ERROR,
                     errmsg!("operator {}({},{}) already exists in operator family \"{}\"",
                             (*op).number,
                             cstr_display(format_type_be((*op).lefttype)),
                             cstr_display(format_type_be((*op).righttype)),
                             cstr_display(NameListToString(opfamilyname))));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }

        oppurpose = if OidIsValid((*op).sortfamily) { AMOP_ORDER } else { AMOP_SEARCH };

        /* Create the pg_amop entry */
        values = [0 as Datum; Natts_pg_amop];
        nulls = [false; Natts_pg_amop];

        entryoid = GetNewOidWithIndex(rel, AccessMethodOperatorOidIndexId,
                                      Anum_pg_amop_oid);
        values[(Anum_pg_amop_oid - 1) as usize] = ObjectIdGetDatum(entryoid);
        values[(Anum_pg_amop_amopfamily - 1) as usize] = ObjectIdGetDatum(opfamilyoid);
        values[(Anum_pg_amop_amoplefttype - 1) as usize] = ObjectIdGetDatum((*op).lefttype);
        values[(Anum_pg_amop_amoprighttype - 1) as usize] = ObjectIdGetDatum((*op).righttype);
        values[(Anum_pg_amop_amopstrategy - 1) as usize] = Int16GetDatum((*op).number as int16);
        values[(Anum_pg_amop_amoppurpose - 1) as usize] = CharGetDatum(oppurpose);
        values[(Anum_pg_amop_amopopr - 1) as usize] = ObjectIdGetDatum((*op).object);
        values[(Anum_pg_amop_amopmethod - 1) as usize] = ObjectIdGetDatum(amoid);
        values[(Anum_pg_amop_amopsortfamily - 1) as usize] = ObjectIdGetDatum((*op).sortfamily);

        tup = heap_form_tuple((*rel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());

        CatalogTupleInsert(rel, tup);

        heap_freetuple(tup);

        /* Make its dependencies */
        myself.classId = AccessMethodOperatorRelationId;
        myself.objectId = entryoid;
        myself.objectSubId = 0;

        referenced.classId = OperatorRelationId;
        referenced.objectId = (*op).object;
        referenced.objectSubId = 0;

        /* see comments in amapi.h about dependency strength */
        recordDependencyOn(&myself, &referenced,
                           if (*op).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });

        referenced.classId = if (*op).ref_is_family { OperatorFamilyRelationId } else { OperatorClassRelationId };
        referenced.objectId = (*op).refobjid;
        referenced.objectSubId = 0;

        recordDependencyOn(&myself, &referenced,
                           if (*op).ref_is_hard { DEPENDENCY_INTERNAL } else { DEPENDENCY_AUTO });

        if typeDepNeeded((*op).lefttype, op) {
            referenced.classId = TypeRelationId;
            referenced.objectId = (*op).lefttype;
            referenced.objectSubId = 0;

            /* see comments in amapi.h about dependency strength */
            recordDependencyOn(&myself, &referenced,
                               if (*op).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });
        }

        if (*op).lefttype != (*op).righttype &&
           typeDepNeeded((*op).righttype, op) {
            referenced.classId = TypeRelationId;
            referenced.objectId = (*op).righttype;
            referenced.objectSubId = 0;

            /* see comments in amapi.h about dependency strength */
            recordDependencyOn(&myself, &referenced,
                               if (*op).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });
        }

        /* A search operator also needs a dep on the referenced opfamily */
        if OidIsValid((*op).sortfamily) {
            referenced.classId = OperatorFamilyRelationId;
            referenced.objectId = (*op).sortfamily;
            referenced.objectSubId = 0;

            recordDependencyOn(&myself, &referenced,
                               if (*op).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });
        }

        /* Post create hook of this access method operator */
        InvokeObjectPostCreateHook(AccessMethodOperatorRelationId,
                                   entryoid, 0);
    });

    table_close(rel, RowExclusiveLock);
}

/*
 * Dump the procedures (support routines) to pg_amproc
 *
 * We also make dependency entries in pg_depend for the pg_amproc entries.
 */
unsafe fn storeProcedures(opfamilyname: *mut List, amoid: Oid, opfamilyoid: Oid,
                          procedures: *mut List, isAdd: bool) {
    let rel: Relation;
    let mut values: [Datum; Natts_pg_amproc] = [0 as Datum; Natts_pg_amproc];
    let mut nulls: [bool; Natts_pg_amproc] = [false; Natts_pg_amproc];
    let mut tup: HeapTuple;
    let mut entryoid: Oid;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    rel = table_open(AccessMethodProcedureRelationId, RowExclusiveLock);

    foreach!(l, procedures, {
        let proc: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;

        /*
         * If adding to an existing family, check for conflict with an
         * existing pg_amproc entry (just to give a nicer error message)
         */
        if isAdd &&
           SearchSysCacheExists4(AMPROCNUM,
                                 ObjectIdGetDatum(opfamilyoid),
                                 ObjectIdGetDatum((*proc).lefttype),
                                 ObjectIdGetDatum((*proc).righttype),
                                 Int16GetDatum((*proc).number as int16)) {
            ereport!(ERROR,
                     errmsg!("function {}({},{}) already exists in operator family \"{}\"",
                             (*proc).number,
                             cstr_display(format_type_be((*proc).lefttype)),
                             cstr_display(format_type_be((*proc).righttype)),
                             cstr_display(NameListToString(opfamilyname))));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }

        /* Create the pg_amproc entry */
        values = [0 as Datum; Natts_pg_amproc];
        nulls = [false; Natts_pg_amproc];

        entryoid = GetNewOidWithIndex(rel, AccessMethodProcedureOidIndexId,
                                      Anum_pg_amproc_oid);
        values[(Anum_pg_amproc_oid - 1) as usize] = ObjectIdGetDatum(entryoid);
        values[(Anum_pg_amproc_amprocfamily - 1) as usize] = ObjectIdGetDatum(opfamilyoid);
        values[(Anum_pg_amproc_amproclefttype - 1) as usize] = ObjectIdGetDatum((*proc).lefttype);
        values[(Anum_pg_amproc_amprocrighttype - 1) as usize] = ObjectIdGetDatum((*proc).righttype);
        values[(Anum_pg_amproc_amprocnum - 1) as usize] = Int16GetDatum((*proc).number as int16);
        values[(Anum_pg_amproc_amproc - 1) as usize] = ObjectIdGetDatum((*proc).object);

        tup = heap_form_tuple((*rel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());

        CatalogTupleInsert(rel, tup);

        heap_freetuple(tup);

        /* Make its dependencies */
        myself.classId = AccessMethodProcedureRelationId;
        myself.objectId = entryoid;
        myself.objectSubId = 0;

        referenced.classId = ProcedureRelationId;
        referenced.objectId = (*proc).object;
        referenced.objectSubId = 0;

        /* see comments in amapi.h about dependency strength */
        recordDependencyOn(&myself, &referenced,
                           if (*proc).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });

        referenced.classId = if (*proc).ref_is_family { OperatorFamilyRelationId } else { OperatorClassRelationId };
        referenced.objectId = (*proc).refobjid;
        referenced.objectSubId = 0;

        recordDependencyOn(&myself, &referenced,
                           if (*proc).ref_is_hard { DEPENDENCY_INTERNAL } else { DEPENDENCY_AUTO });

        if typeDepNeeded((*proc).lefttype, proc) {
            referenced.classId = TypeRelationId;
            referenced.objectId = (*proc).lefttype;
            referenced.objectSubId = 0;

            /* see comments in amapi.h about dependency strength */
            recordDependencyOn(&myself, &referenced,
                               if (*proc).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });
        }

        if (*proc).lefttype != (*proc).righttype &&
           typeDepNeeded((*proc).righttype, proc) {
            referenced.classId = TypeRelationId;
            referenced.objectId = (*proc).righttype;
            referenced.objectSubId = 0;

            /* see comments in amapi.h about dependency strength */
            recordDependencyOn(&myself, &referenced,
                               if (*proc).ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO });
        }

        /* Post create hook of access method procedure */
        InvokeObjectPostCreateHook(AccessMethodProcedureRelationId,
                                   entryoid, 0);
    });

    table_close(rel, RowExclusiveLock);
}

/*
 * Detect whether a pg_amop or pg_amproc entry needs an explicit dependency
 * on its lefttype or righttype.
 *
 * We make such a dependency unless the entry has an indirect dependency
 * via its referenced operator or function.  That's nearly always true
 * for operators, but might well not be true for support functions.
 */
unsafe fn typeDepNeeded(typid: Oid, member: *mut OpFamilyMember) -> bool {
    let mut result: bool = true;

    /*
     * If the type is pinned, we don't need a dependency.  This is a bit of a
     * layering violation perhaps (recordDependencyOn would ignore the request
     * anyway), but it's a cheap test and will frequently save a syscache
     * lookup here.
     */
    if IsPinnedObject(TypeRelationId, typid) {
        return false;
    }

    /* Nope, so check the input types of the function or operator. */
    if (*member).is_func {
        let mut argtypes: *mut Oid = core::ptr::null_mut();
        let mut nargs: c_int = 0;

        let _ = get_func_signature((*member).object, &mut argtypes, &mut nargs);
        let mut i: c_int = 0;
        while i < nargs {
            if typid == *argtypes.add(i as usize) {
                result = false; /* match, no dependency needed */
                break;
            }
            i += 1;
        }
        pfree(argtypes as *mut c_void);
    } else {
        let mut lefttype: Oid = InvalidOid;
        let mut righttype: Oid = InvalidOid;

        op_input_types((*member).object, &mut lefttype, &mut righttype);
        if typid == lefttype || typid == righttype {
            result = false;     /* match, no dependency needed */
        }
    }
    return result;
}


/*
 * Remove operator entries from an opfamily.
 *
 * Note: this is only allowed for "loose" members of an opfamily, hence
 * behavior is always RESTRICT.
 */
unsafe fn dropOperators(opfamilyname: *mut List, amoid: Oid, opfamilyoid: Oid,
                        operators: *mut List) {
    foreach!(l, operators, {
        let op: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;
        let amopid: Oid;
        let mut object: ObjectAddress = core::mem::zeroed();

        amopid = GetSysCacheOid4(AMOPSTRATEGY, Anum_pg_amop_oid,
                                 ObjectIdGetDatum(opfamilyoid),
                                 ObjectIdGetDatum((*op).lefttype),
                                 ObjectIdGetDatum((*op).righttype),
                                 Int16GetDatum((*op).number as int16));
        if !OidIsValid(amopid) {
            ereport!(ERROR,
                     errmsg!("operator {}({},{}) does not exist in operator family \"{}\"",
                             (*op).number,
                             cstr_display(format_type_be((*op).lefttype)),
                             cstr_display(format_type_be((*op).righttype)),
                             cstr_display(NameListToString(opfamilyname))));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }

        object.classId = AccessMethodOperatorRelationId;
        object.objectId = amopid;
        object.objectSubId = 0;

        performDeletion(&object, DROP_RESTRICT, 0);
    });
}

/*
 * Remove procedure entries from an opfamily.
 *
 * Note: this is only allowed for "loose" members of an opfamily, hence
 * behavior is always RESTRICT.
 */
unsafe fn dropProcedures(opfamilyname: *mut List, amoid: Oid, opfamilyoid: Oid,
                         procedures: *mut List) {
    foreach!(l, procedures, {
        let op: *mut OpFamilyMember = lfirst(current_cell!(l)) as *mut OpFamilyMember;
        let amprocid: Oid;
        let mut object: ObjectAddress = core::mem::zeroed();

        amprocid = GetSysCacheOid4(AMPROCNUM, Anum_pg_amproc_oid,
                                   ObjectIdGetDatum(opfamilyoid),
                                   ObjectIdGetDatum((*op).lefttype),
                                   ObjectIdGetDatum((*op).righttype),
                                   Int16GetDatum((*op).number as int16));
        if !OidIsValid(amprocid) {
            ereport!(ERROR,
                     errmsg!("function {}({},{}) does not exist in operator family \"{}\"",
                             (*op).number,
                             cstr_display(format_type_be((*op).lefttype)),
                             cstr_display(format_type_be((*op).righttype)),
                             cstr_display(NameListToString(opfamilyname))));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }

        object.classId = AccessMethodProcedureRelationId;
        object.objectId = amprocid;
        object.objectSubId = 0;

        performDeletion(&object, DROP_RESTRICT, 0);
    });
}

/*
 * Subroutine for ALTER OPERATOR CLASS SET SCHEMA/RENAME
 *
 * Is there an operator class with the given name and signature already
 * in the given namespace?	If so, raise an appropriate error message.
 */
pub unsafe fn IsThereOpClassInNamespace(opcname: *const c_char, opcmethod: Oid,
                                        opcnamespace: Oid) {
    /* make sure the new name doesn't exist */
    if SearchSysCacheExists3(CLAAMNAMENSP,
                             ObjectIdGetDatum(opcmethod),
                             CStringGetDatum(opcname),
                             ObjectIdGetDatum(opcnamespace)) {
        ereport!(ERROR,
                 errmsg!("operator class \"{}\" for access method \"{}\" already exists in schema \"{}\"",
                         cstr_display(opcname),
                         cstr_display(get_am_name(opcmethod)),
                         cstr_display(get_namespace_name(opcnamespace))));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }
}

/*
 * Subroutine for ALTER OPERATOR FAMILY SET SCHEMA/RENAME
 *
 * Is there an operator family with the given name and signature already
 * in the given namespace?	If so, raise an appropriate error message.
 */
pub unsafe fn IsThereOpFamilyInNamespace(opfname: *const c_char, opfmethod: Oid,
                                         opfnamespace: Oid) {
    /* make sure the new name doesn't exist */
    if SearchSysCacheExists3(OPFAMILYAMNAMENSP,
                             ObjectIdGetDatum(opfmethod),
                             CStringGetDatum(opfname),
                             ObjectIdGetDatum(opfnamespace)) {
        ereport!(ERROR,
                 errmsg!("operator family \"{}\" for access method \"{}\" already exists in schema \"{}\"",
                         cstr_display(opfname),
                         cstr_display(get_am_name(opfmethod)),
                         cstr_display(get_namespace_name(opfnamespace))));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }
}
