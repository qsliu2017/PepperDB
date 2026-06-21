//! commands/alter.c - Drivers for generic alter commands.
//!
//! Source: postgres/src/backend/commands/alter.c (PostgreSQL 18.3)
//! Merged header: postgres/src/include/commands/alter.h
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! This is the generic ALTER ... RENAME / SET SCHEMA / OWNER dispatch.  The
//! per-object-type implementations (RenameRelation, AlterTableNamespace, etc.)
//! and the catalog/objectaddress.c machinery (get_object_address, the
//! get_object_attnum_* / get_object_catcache_* accessors) live in modules that
//! have not yet been ported; those are STUBBED locally with
//! `// TODO(pg-port): real X lives in <file>`.

use crate::prelude::*;

use core::mem::size_of;

use crate::{castNode, strVal};

// ----------------------------------------------------------------------------
// Real imports from already-ported modules.
// ----------------------------------------------------------------------------

// access/htup_details.h: HeapTuple handle, validity check, GETSTRUCT, heap_getattr.
use crate::access::htup_details::{
    heap_getattr, HeapTuple, HeapTupleIsValid, GETSTRUCT,
};
// access/common/heaptuple.c: form/free a modified tuple.
use crate::access::common::heaptuple::{heap_freetuple, heap_modify_tuple};
// access/table.h: table_open / table_close (relation_open/close are private
// stubs in that module, so we re-stub the relation_* pair locally below).
use crate::access::table::table::{table_close, table_open};

// catalog/objectaccess.h: ObjectAddress (the canonical home until
// catalog/objectaddress.c is ported).
use crate::catalog::objectaccess::ObjectAddress;

// catalog/pg_*_d.h: catalog relation OIDs.
use crate::catalog::catalog_oids::{
    CollationRelationId, ConversionRelationId, DatabaseRelationId,
    EventTriggerRelationId, ForeignDataWrapperRelationId, ForeignServerRelationId,
    LanguageRelationId, LargeObjectMetadataRelationId, LargeObjectRelationId,
    NamespaceRelationId, OperatorClassRelationId, OperatorFamilyRelationId,
    OperatorRelationId, ProcedureRelationId, PublicationRelationId,
    RelationRelationId, StatisticExtRelationId, SubscriptionRelationId,
    TSConfigRelationId, TSDictionaryRelationId, TSParserRelationId,
    TSTemplateRelationId, TypeRelationId,
};

// catalog Form structs (fixed parts of catalog rows).
use crate::catalog::pg_collation::Form_pg_collation;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_opfamily::Form_pg_opfamily;
use crate::catalog::pg_proc::Form_pg_proc;
use crate::catalog::pg_publication::Form_pg_publication;
use crate::catalog::pg_subscription::Form_pg_subscription;

// access/attnum.h: attribute number type + invalid sentinel.
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};

// nodes/parsenodes.h: the ALTER statement nodes, ObjectType enum, RoleSpec.
use crate::nodes::parsenodes::{
    AlterObjectDependsStmt, AlterObjectSchemaStmt, AlterOwnerStmt, ObjectType, RenameStmt,
    RoleSpec,
};
use crate::nodes::parsenodes::ObjectType::*;
// nodes/pg_list.h, nodes/primnodes.h, nodes/value.h, nodes/nodes.h.
use crate::nodes::pg_list::List;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::RangeVar;

// utils/rel.h: relation accessors.
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetNamespace, RelationGetNumberOfAttributes,
    RelationGetRelationName, RelationGetRelid,
};

// storage/lockdefs.h: lock modes used here.
use crate::storage::lockdefs::{
    AccessExclusiveLock, InplaceUpdateTupleLock, NoLock, RowExclusiveLock, LOCKMODE,
};

// storage/itemptr.h: ItemPointer (for CatalogTupleUpdate / UnlockTuple).
use crate::storage::itemptr::ItemPointerData;

// miscadmin.h: current database OID.
use crate::miscadmin::MyDatabaseId;

// utils/adt/acl.c: real role-privilege checks.
use crate::utils::adt::acl::{has_privs_of_role, GetUserId};
// utils/misc/superuser.c: superuser check.
use crate::utils::misc::superuser::superuser;

// utils/adt/name.c: namestrcpy (copy a CString into a NameData, padded).
use crate::utils::adt::name::namestrcpy;
// c.h: NameStr (address of the data field of a NameData), Name, NameData.
use crate::c::{Name, NameData};
// c.h: oidvector layout (pg_proc.proargtypes is an oidvector beyond the
// fixed FormData_pg_proc; see pg_proc_proargtypes() below).
use crate::c::oidvector;
// pg_config_manual.h: maximum identifier length (incl. trailing NUL).
use crate::pg_config::NAMEDATALEN;

// postgres.h: Datum conversions.
use crate::postgres::{
    CStringGetDatum, DatumGetName, DatumGetObjectId, ObjectIdGetDatum, PointerGetDatum,
};

// commands/defrem.h: friendly duplicate-name check for functions.
use crate::commands::defrem::IsThereFunctionInNamespace;
// commands/collationcmds.c: friendly duplicate-name check for collations.
use crate::commands::collationcmds::IsThereCollationInNamespace;

// ----------------------------------------------------------------------------
// gettext_noop(x): i18n no-op marker - identity (#define gettext_noop(x) (x)).
// ----------------------------------------------------------------------------
fn gettext_noop(s: &'static str) -> &'static str {
    s
}

// ----------------------------------------------------------------------------
// Local stubs / constants for as-yet-unported dependencies.  Each is annotated
// with where the real symbol lives.  Values that are concrete PostgreSQL 18.3
// constants are filled in; everything else is `unimplemented!()`.
// ----------------------------------------------------------------------------

// utils/acl.h
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;
const ACL_CREATE: u64 = 1 << 11; // AclMode bit for CREATE
type Acl = c_void;

// catalog/dependency.h
type ObjectAddresses = c_void;
type DependencyType = c_int;
const DEPENDENCY_AUTO_EXTENSION: DependencyType = b'x' as c_int;

// ESC errcodes (utils/errcodes.h).  The ereport! shim ignores the code, but we
// keep the symbol for fidelity.
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;

/* TODO(pg-port): real get_object_catcache_oid lives in catalog/objectaddress.c */
unsafe fn get_object_catcache_oid(class_id: Oid) -> c_int {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_catcache_name lives in catalog/objectaddress.c */
unsafe fn get_object_catcache_name(class_id: Oid) -> c_int {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_attnum_oid lives in catalog/objectaddress.c */
unsafe fn get_object_attnum_oid(class_id: Oid) -> AttrNumber {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_attnum_name lives in catalog/objectaddress.c */
unsafe fn get_object_attnum_name(class_id: Oid) -> AttrNumber {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_attnum_namespace lives in catalog/objectaddress.c */
unsafe fn get_object_attnum_namespace(class_id: Oid) -> AttrNumber {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_attnum_owner lives in catalog/objectaddress.c */
unsafe fn get_object_attnum_owner(class_id: Oid) -> AttrNumber {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_attnum_acl lives in catalog/objectaddress.c */
unsafe fn get_object_attnum_acl(class_id: Oid) -> AttrNumber {
    let _ = class_id;
    unimplemented!()
}
/* TODO(pg-port): real get_object_type lives in catalog/objectaddress.c */
unsafe fn get_object_type(class_id: Oid, object_id: Oid) -> ObjectType {
    let _ = (class_id, object_id);
    unimplemented!()
}
/* TODO(pg-port): real getObjectDescriptionOids lives in catalog/objectaddress.c */
unsafe fn getObjectDescriptionOids(class_id: Oid, object_id: Oid) -> *mut c_char {
    let _ = (class_id, object_id);
    unimplemented!()
}
/* TODO(pg-port): real get_object_address lives in catalog/objectaddress.c */
unsafe fn get_object_address(
    objtype: ObjectType,
    object: *mut Node,
    relp: *mut Relation,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> ObjectAddress {
    let _ = (objtype, object, relp, lockmode, missing_ok);
    unimplemented!()
}
/* TODO(pg-port): real get_object_address_rv lives in catalog/objectaddress.c */
unsafe fn get_object_address_rv(
    objtype: ObjectType,
    rel: *mut RangeVar,
    object: *mut List,
    relp: *mut Relation,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> ObjectAddress {
    let _ = (objtype, rel, object, relp, lockmode, missing_ok);
    unimplemented!()
}
/* TODO(pg-port): real check_object_ownership lives in catalog/objectaddress.c */
unsafe fn check_object_ownership(
    roleid: Oid,
    objtype: ObjectType,
    address: ObjectAddress,
    object: *mut Node,
    relation: Relation,
) {
    let _ = (roleid, objtype, address, object, relation);
    unimplemented!()
}
/* TODO(pg-port): real get_catalog_object_by_oid_extended lives in catalog/objectaddress.c */
unsafe fn get_catalog_object_by_oid_extended(
    catalog: Relation,
    oid_col: AttrNumber,
    object_id: Oid,
    locktup: bool,
) -> HeapTuple {
    let _ = (catalog, oid_col, object_id, locktup);
    unimplemented!()
}

/* TODO(pg-port): real relation_open lives in access/relation.c */
unsafe fn relation_open(relation_id: Oid, lockmode: LOCKMODE) -> Relation {
    let _ = (relation_id, lockmode);
    unimplemented!()
}
/* TODO(pg-port): real relation_close lives in access/relation.c */
unsafe fn relation_close(relation: Relation, lockmode: LOCKMODE) {
    let _ = (relation, lockmode);
    unimplemented!()
}

/* TODO(pg-port): real SearchSysCache1 lives in utils/cache/syscache.c */
unsafe fn SearchSysCache1(cache_id: c_int, key1: Datum) -> HeapTuple {
    let _ = (cache_id, key1);
    unimplemented!()
}
/* TODO(pg-port): real SearchSysCacheCopy1 lives in utils/cache/syscache.c */
unsafe fn SearchSysCacheCopy1(cache_id: c_int, key1: Datum) -> HeapTuple {
    let _ = (cache_id, key1);
    unimplemented!()
}
/* TODO(pg-port): real SearchSysCacheExists1 lives in utils/cache/syscache.c */
unsafe fn SearchSysCacheExists1(cache_id: c_int, key1: Datum) -> bool {
    let _ = (cache_id, key1);
    unimplemented!()
}
/* TODO(pg-port): real SearchSysCacheExists2 lives in utils/cache/syscache.c */
unsafe fn SearchSysCacheExists2(cache_id: c_int, key1: Datum, key2: Datum) -> bool {
    let _ = (cache_id, key1, key2);
    unimplemented!()
}
/* TODO(pg-port): real ReleaseSysCache lives in utils/cache/syscache.c */
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    let _ = tuple;
    unimplemented!()
}

// utils/cache/syscache.h SysCacheIdentifier value used directly here.
/* TODO(pg-port): real SUBSCRIPTIONNAME enumerator lives in utils/cache/syscache.h */
const SUBSCRIPTIONNAME: c_int = 66;

/* TODO(pg-port): real get_namespace_name lives in utils/cache/lsyscache.c */
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char {
    let _ = nspid;
    unimplemented!()
}
/* TODO(pg-port): real get_database_name lives in commands/dbcommands.c */
unsafe fn get_database_name(dbid: Oid) -> *mut c_char {
    let _ = dbid;
    unimplemented!()
}
/* TODO(pg-port): real get_rolespec_oid lives in utils/adt/acl.c */
unsafe fn get_rolespec_oid(role: *mut RoleSpec, missing_ok: bool) -> Oid {
    let _ = (role, missing_ok);
    unimplemented!()
}
/* TODO(pg-port): real check_can_set_role lives in utils/adt/acl.c */
unsafe fn check_can_set_role(member: Oid, role: Oid) {
    let _ = (member, role);
    unimplemented!()
}
/* TODO(pg-port): real object_aclcheck lives in utils/adt/acl.c */
unsafe fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: u64) -> AclResult {
    let _ = (classid, objectid, roleid, mode);
    unimplemented!()
}
/* TODO(pg-port): real aclcheck_error lives in utils/adt/acl.c */
unsafe fn aclcheck_error(aclerr: AclResult, objtype: ObjectType, objectname: *const c_char) {
    let _ = (aclerr, objtype, objectname);
    unimplemented!()
}
/* TODO(pg-port): real aclnewowner lives in utils/adt/acl.c */
unsafe fn aclnewowner(old_acl: *mut Acl, old_owner_id: Oid, new_owner_id: Oid) -> *mut Acl {
    let _ = (old_acl, old_owner_id, new_owner_id);
    unimplemented!()
}
/* TODO(pg-port): real DatumGetAclP lives in utils/acl.h */
unsafe fn DatumGetAclP(x: Datum) -> *mut Acl {
    let _ = x;
    unimplemented!()
}

/* TODO(pg-port): real CatalogTupleUpdate lives in catalog/indexing.c */
unsafe fn CatalogTupleUpdate(heap_rel: Relation, otid: *mut ItemPointerData, tup: HeapTuple) {
    let _ = (heap_rel, otid, tup);
    unimplemented!()
}

/* TODO(pg-port): real UnlockTuple lives in storage/lmgr/lmgr.c */
unsafe fn UnlockTuple(relation: Relation, tid: *mut ItemPointerData, lockmode: LOCKMODE) {
    let _ = (relation, tid, lockmode);
    unimplemented!()
}

/* TODO(pg-port): real changeDependencyFor lives in catalog/pg_depend.c */
unsafe fn changeDependencyFor(
    class_id: Oid,
    object_id: Oid,
    ref_class_id: Oid,
    old_ref_object_id: Oid,
    new_ref_object_id: Oid,
) -> c_long {
    let _ = (class_id, object_id, ref_class_id, old_ref_object_id, new_ref_object_id);
    unimplemented!()
}
/* TODO(pg-port): real changeDependencyOnOwner lives in catalog/pg_shdepend.c */
unsafe fn changeDependencyOnOwner(class_id: Oid, object_id: Oid, new_owner_id: Oid) {
    let _ = (class_id, object_id, new_owner_id);
    unimplemented!()
}
/* TODO(pg-port): real deleteDependencyRecordsForSpecific lives in catalog/pg_depend.c */
unsafe fn deleteDependencyRecordsForSpecific(
    class_id: Oid,
    object_id: Oid,
    behavior: c_char,
    ref_class_id: Oid,
    ref_object_id: Oid,
) -> c_long {
    let _ = (class_id, object_id, behavior, ref_class_id, ref_object_id);
    unimplemented!()
}
/* TODO(pg-port): real getAutoExtensionsOfObject lives in catalog/pg_depend.c */
unsafe fn getAutoExtensionsOfObject(class_id: Oid, object_id: Oid) -> *mut List {
    let _ = (class_id, object_id);
    unimplemented!()
}
/* TODO(pg-port): real recordDependencyOn lives in catalog/dependency.c */
unsafe fn recordDependencyOn(
    depender: *const ObjectAddress,
    referenced: *const ObjectAddress,
    behavior: DependencyType,
) {
    let _ = (depender, referenced, behavior);
    unimplemented!()
}
/* TODO(pg-port): real list_member_oid lives in nodes/list.c */
unsafe fn list_member_oid(list: *const List, datum: Oid) -> bool {
    let _ = (list, datum);
    unimplemented!()
}

/* TODO(pg-port): real CheckSetNamespace lives in catalog/namespace.c */
unsafe fn CheckSetNamespace(old_nsp_oid: Oid, nsp_oid: Oid) {
    let _ = (old_nsp_oid, nsp_oid);
    unimplemented!()
}
/* TODO(pg-port): real LookupCreationNamespace lives in catalog/namespace.c */
unsafe fn LookupCreationNamespace(nsp_name: *const c_char) -> Oid {
    let _ = nsp_name;
    unimplemented!()
}

/* TODO(pg-port): real IsThereOpClassInNamespace lives in commands/opclasscmds.c */
unsafe fn IsThereOpClassInNamespace(opcname: *const c_char, opcmethod: Oid, opcnamespace: Oid) {
    let _ = (opcname, opcmethod, opcnamespace);
    unimplemented!()
}
/* TODO(pg-port): real IsThereOpFamilyInNamespace lives in commands/opclasscmds.c */
unsafe fn IsThereOpFamilyInNamespace(opfname: *const c_char, opfmethod: Oid, opfnamespace: Oid) {
    let _ = (opfname, opfmethod, opfnamespace);
    unimplemented!()
}

/* TODO(pg-port): real LogicalRepWorkersWakeupAtCommit lives in replication/logical/launcher.c */
unsafe fn LogicalRepWorkersWakeupAtCommit(subid: Oid) {
    let _ = subid;
    unimplemented!()
}
/* TODO(pg-port): real InvalidatePubRelSyncCache lives in commands/publicationcmds.c */
unsafe fn InvalidatePubRelSyncCache(pubid: Oid, puballtables: bool) {
    let _ = (pubid, puballtables);
    unimplemented!()
}
/* TODO(pg-port): real InvokeObjectPostAlterHook lives in catalog/objectaccess.h (inline) */
unsafe fn InvokeObjectPostAlterHook(class_id: Oid, object_id: Oid, sub_id: c_int) {
    let _ = (class_id, object_id, sub_id);
}

// --- per-object-type ALTER drivers (each in its own as-yet-unported file) ---

/* TODO(pg-port): real RenameConstraint lives in commands/tablecmds.c */
unsafe fn RenameConstraint(stmt: *mut RenameStmt) -> ObjectAddress {
    let _ = stmt;
    unimplemented!()
}
/* TODO(pg-port): real RenameDatabase lives in commands/dbcommands.c */
unsafe fn RenameDatabase(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let _ = (oldname, newname);
    unimplemented!()
}
/* TODO(pg-port): real RenameRole lives in commands/user.c */
unsafe fn RenameRole(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let _ = (oldname, newname);
    unimplemented!()
}
/* TODO(pg-port): real RenameSchema lives in commands/schemacmds.c */
unsafe fn RenameSchema(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let _ = (oldname, newname);
    unimplemented!()
}
/* TODO(pg-port): real RenameTableSpace lives in commands/tablespace.c */
unsafe fn RenameTableSpace(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let _ = (oldname, newname);
    unimplemented!()
}
/* TODO(pg-port): real RenameRelation lives in commands/tablecmds.c */
unsafe fn RenameRelation(stmt: *mut RenameStmt) -> ObjectAddress {
    let _ = stmt;
    unimplemented!()
}
/* TODO(pg-port): real renameatt lives in commands/tablecmds.c */
unsafe fn renameatt(stmt: *mut RenameStmt) -> ObjectAddress {
    let _ = stmt;
    unimplemented!()
}
/* TODO(pg-port): real RenameRewriteRule lives in rewrite/rewriteDefine.c */
unsafe fn RenameRewriteRule(
    relation: *mut RangeVar,
    old_name: *const c_char,
    new_name: *const c_char,
) -> ObjectAddress {
    let _ = (relation, old_name, new_name);
    unimplemented!()
}
/* TODO(pg-port): real renametrig lives in commands/trigger.c */
unsafe fn renametrig(stmt: *mut RenameStmt) -> ObjectAddress {
    let _ = stmt;
    unimplemented!()
}
/* TODO(pg-port): real rename_policy lives in commands/policy.c */
unsafe fn rename_policy(stmt: *mut RenameStmt) -> ObjectAddress {
    let _ = stmt;
    unimplemented!()
}
/* TODO(pg-port): real RenameType lives in commands/typecmds.c */
unsafe fn RenameType(stmt: *mut RenameStmt) -> ObjectAddress {
    let _ = stmt;
    unimplemented!()
}

/* TODO(pg-port): real AlterExtensionNamespace lives in commands/extension.c */
unsafe fn AlterExtensionNamespace(
    extension_name: *const c_char,
    new_schema: *const c_char,
    old_schema_oid: *mut Oid,
) -> ObjectAddress {
    let _ = (extension_name, new_schema, old_schema_oid);
    unimplemented!()
}
/* TODO(pg-port): real AlterTableNamespace lives in commands/tablecmds.c */
unsafe fn AlterTableNamespace(
    stmt: *mut AlterObjectSchemaStmt,
    old_schema_oid: *mut Oid,
) -> ObjectAddress {
    let _ = (stmt, old_schema_oid);
    unimplemented!()
}
/* TODO(pg-port): real AlterTableNamespaceInternal lives in commands/tablecmds.c */
unsafe fn AlterTableNamespaceInternal(
    rel: Relation,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
) {
    let _ = (rel, old_nsp_oid, new_nsp_oid, objs_moved);
    unimplemented!()
}
/* TODO(pg-port): real AlterTypeNamespace lives in commands/typecmds.c */
unsafe fn AlterTypeNamespace(
    names: *mut List,
    newschema: *const c_char,
    objecttype: ObjectType,
    old_schema_oid: *mut Oid,
) -> ObjectAddress {
    let _ = (names, newschema, objecttype, old_schema_oid);
    unimplemented!()
}
/* TODO(pg-port): real AlterTypeNamespace_oid lives in commands/typecmds.c */
unsafe fn AlterTypeNamespace_oid(
    typid: Oid,
    nsp_oid: Oid,
    ignore_dependent: bool,
    objs_moved: *mut ObjectAddresses,
) -> Oid {
    let _ = (typid, nsp_oid, ignore_dependent, objs_moved);
    unimplemented!()
}

/* TODO(pg-port): real AlterDatabaseOwner lives in commands/dbcommands.c */
unsafe fn AlterDatabaseOwner(dbname: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (dbname, newowner);
    unimplemented!()
}
/* TODO(pg-port): real AlterSchemaOwner lives in commands/schemacmds.c */
unsafe fn AlterSchemaOwner(name: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (name, newowner);
    unimplemented!()
}
/* TODO(pg-port): real AlterTypeOwner lives in commands/typecmds.c */
unsafe fn AlterTypeOwner(
    names: *mut List,
    newowner: Oid,
    objecttype: ObjectType,
) -> ObjectAddress {
    let _ = (names, newowner, objecttype);
    unimplemented!()
}
/* TODO(pg-port): real AlterForeignDataWrapperOwner lives in commands/foreigncmds.c */
unsafe fn AlterForeignDataWrapperOwner(name: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (name, newowner);
    unimplemented!()
}
/* TODO(pg-port): real AlterForeignServerOwner lives in commands/foreigncmds.c */
unsafe fn AlterForeignServerOwner(name: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (name, newowner);
    unimplemented!()
}
/* TODO(pg-port): real AlterEventTriggerOwner lives in commands/event_trigger.c */
unsafe fn AlterEventTriggerOwner(name: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (name, newowner);
    unimplemented!()
}
/* TODO(pg-port): real AlterPublicationOwner lives in commands/publicationcmds.c */
unsafe fn AlterPublicationOwner(name: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (name, newowner);
    unimplemented!()
}
/* TODO(pg-port): real AlterSubscriptionOwner lives in commands/subscriptioncmds.c */
unsafe fn AlterSubscriptionOwner(name: *const c_char, newowner: Oid) -> ObjectAddress {
    let _ = (name, newowner);
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Helpers / constructors.
// ----------------------------------------------------------------------------

/*
 * InvalidObjectAddress - the all-zero / invalid object address.
 * catalog/objectaddress.h (a static const in C, a constructor here).
 */
fn InvalidObjectAddress() -> ObjectAddress {
    ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    }
}

/*
 * ObjectAddressSet(addr, class, object): convenience builder.
 * catalog/objectaddress.h (a macro in C).
 */
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

/* NameGetDatum(name): address of a NameData as a Datum (postgres.h). */
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    PointerGetDatum(name as *const c_void)
}

/*
 * pg_proc.proargtypes is an oidvector that lives beyond the CATALOG_VARLEN
 * cutoff of FormData_pg_proc, so it is not a fixed field of the ported struct.
 * This helper returns a pointer to where `proc->proargtypes` would begin (just
 * past the fixed prefix), matching the C `&proc->proargtypes`.
 */
unsafe fn pg_proc_proargtypes(proc: Form_pg_proc) -> *mut oidvector {
    (proc as *mut u8).add(size_of::<crate::catalog::pg_proc::FormData_pg_proc>())
        as *mut oidvector
}

/*
 * Raise an error to the effect that an object of the given name is already
 * present in the given namespace.
 */
unsafe fn report_name_conflict(class_id: Oid, name: *const c_char) -> ! {
    let msgfmt: &'static str;

    match class_id {
        x if x == EventTriggerRelationId => {
            msgfmt = gettext_noop("event trigger \"{}\" already exists");
        }
        x if x == ForeignDataWrapperRelationId => {
            msgfmt = gettext_noop("foreign-data wrapper \"{}\" already exists");
        }
        x if x == ForeignServerRelationId => {
            msgfmt = gettext_noop("server \"{}\" already exists");
        }
        x if x == LanguageRelationId => {
            msgfmt = gettext_noop("language \"{}\" already exists");
        }
        x if x == PublicationRelationId => {
            msgfmt = gettext_noop("publication \"{}\" already exists");
        }
        x if x == SubscriptionRelationId => {
            msgfmt = gettext_noop("subscription \"{}\" already exists");
        }
        _ => {
            elog!(ERROR, "unsupported object class: {}", class_id);
            unreachable!();
        }
    }

    let _ = errcode(ERRCODE_DUPLICATE_OBJECT);
    ereport!(
        ERROR,
        errmsg!("{}", msgfmt.replace("{}", &cstr_to_string(name)))
    );
    unreachable!()
}

unsafe fn report_namespace_conflict(class_id: Oid, name: *const c_char, nsp_oid: Oid) -> ! {
    let msgfmt: &'static str;

    Assert!(OidIsValid(nsp_oid));

    match class_id {
        x if x == ConversionRelationId => {
            Assert!(OidIsValid(nsp_oid));
            msgfmt = gettext_noop("conversion \"{}\" already exists in schema \"{}\"");
        }
        x if x == StatisticExtRelationId => {
            Assert!(OidIsValid(nsp_oid));
            msgfmt = gettext_noop("statistics object \"{}\" already exists in schema \"{}\"");
        }
        x if x == TSParserRelationId => {
            Assert!(OidIsValid(nsp_oid));
            msgfmt = gettext_noop("text search parser \"{}\" already exists in schema \"{}\"");
        }
        x if x == TSDictionaryRelationId => {
            Assert!(OidIsValid(nsp_oid));
            msgfmt =
                gettext_noop("text search dictionary \"{}\" already exists in schema \"{}\"");
        }
        x if x == TSTemplateRelationId => {
            Assert!(OidIsValid(nsp_oid));
            msgfmt = gettext_noop("text search template \"{}\" already exists in schema \"{}\"");
        }
        x if x == TSConfigRelationId => {
            Assert!(OidIsValid(nsp_oid));
            msgfmt = gettext_noop(
                "text search configuration \"{}\" already exists in schema \"{}\"",
            );
        }
        _ => {
            elog!(ERROR, "unsupported object class: {}", class_id);
            unreachable!();
        }
    }

    let _ = errcode(ERRCODE_DUPLICATE_OBJECT);
    let msg = msgfmt
        .replacen("{}", &cstr_to_string(name), 1)
        .replacen("{}", &cstr_to_string(get_namespace_name(nsp_oid)), 1);
    ereport!(ERROR, msg);
    unreachable!()
}

/* small helper: render a C string pointer as a Rust String for error messages. */
unsafe fn cstr_to_string(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    core::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

/*
 * AlterObjectRename_internal
 *
 * Generic function to rename the given object, for simple cases (won't
 * work for tables, nor other cases where we need to do more than change
 * the name column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objectId: OID of object to be renamed
 * new_name: CString representation of new name
 */
unsafe fn AlterObjectRename_internal(rel: Relation, object_id: Oid, new_name: *const c_char) {
    let class_id = RelationGetRelid(rel);
    let oid_cache_id = get_object_catcache_oid(class_id);
    let name_cache_id = get_object_catcache_name(class_id);
    let anum_name = get_object_attnum_name(class_id);
    let anum_namespace = get_object_attnum_namespace(class_id);
    let anum_owner = get_object_attnum_owner(class_id);
    let oldtup: HeapTuple;
    let newtup: HeapTuple;
    let mut datum: Datum;
    let mut isnull: bool = false;
    let namespace_id: Oid;
    let owner_id: Oid;
    let old_name: *mut c_char;
    let aclresult: AclResult;
    let values: *mut Datum;
    let nulls: *mut bool;
    let replaces: *mut bool;
    let mut nameattrdata: NameData = std::mem::zeroed();

    oldtup = SearchSysCache1(oid_cache_id, ObjectIdGetDatum(object_id));
    if !HeapTupleIsValid(oldtup) {
        elog!(
            ERROR,
            "cache lookup failed for object {} of catalog \"{}\"",
            object_id,
            cstr_to_string(RelationGetRelationName(rel))
        );
    }

    datum = heap_getattr(oldtup, anum_name as c_int, RelationGetDescr(rel), &mut isnull);
    Assert!(!isnull);
    old_name = NameStr(&*(DatumGetName(datum))) as *mut c_char;

    /* Get OID of namespace */
    if anum_namespace > 0 {
        datum = heap_getattr(
            oldtup,
            anum_namespace as c_int,
            RelationGetDescr(rel),
            &mut isnull,
        );
        Assert!(!isnull);
        namespace_id = DatumGetObjectId(datum);
    } else {
        namespace_id = InvalidOid;
    }

    /* Permission checks ... superusers can always do it */
    if !superuser() {
        /* Fail if object does not have an explicit owner */
        if anum_owner <= 0 {
            let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            ereport!(
                ERROR,
                errmsg!(
                    "must be superuser to rename {}",
                    cstr_to_string(getObjectDescriptionOids(class_id, object_id))
                )
            );
        }

        /* Otherwise, must be owner of the existing object */
        datum = heap_getattr(
            oldtup,
            anum_owner as c_int,
            RelationGetDescr(rel),
            &mut isnull,
        );
        Assert!(!isnull);
        owner_id = DatumGetObjectId(datum);

        if !has_privs_of_role(GetUserId(), owner_id) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                get_object_type(class_id, object_id),
                old_name,
            );
        }

        /* User must have CREATE privilege on the namespace */
        if OidIsValid(namespace_id) {
            aclresult =
                object_aclcheck(NamespaceRelationId, namespace_id, GetUserId(), ACL_CREATE);
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespace_id));
            }
        }

        if class_id == SubscriptionRelationId {
            let form: Form_pg_subscription;

            /* must have CREATE privilege on database */
            let aclresult2 =
                object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
            if aclresult2 != ACLCHECK_OK {
                aclcheck_error(aclresult2, OBJECT_DATABASE, get_database_name(MyDatabaseId));
            }

            /*
             * Don't allow non-superuser modification of a subscription with
             * password_required=false.
             */
            form = GETSTRUCT(oldtup) as Form_pg_subscription;
            if !(*form).subpasswordrequired && !superuser() {
                let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
                ereport!(ERROR, "password_required=false is superuser-only");
            }
        }
    }

    /*
     * Check for duplicate name (more friendly than unique-index failure).
     * Since this is just a friendliness check, we can just skip it in cases
     * where there isn't suitable support.
     */
    if class_id == ProcedureRelationId {
        let proc = GETSTRUCT(oldtup) as Form_pg_proc;

        IsThereFunctionInNamespace(
            new_name,
            (*proc).pronargs as c_int,
            pg_proc_proargtypes(proc),
            (*proc).pronamespace,
        );
    } else if class_id == CollationRelationId {
        let coll = GETSTRUCT(oldtup) as Form_pg_collation;

        IsThereCollationInNamespace(new_name, (*coll).collnamespace);
    } else if class_id == OperatorClassRelationId {
        let opc = GETSTRUCT(oldtup) as Form_pg_opclass;

        IsThereOpClassInNamespace(new_name, (*opc).opcmethod, (*opc).opcnamespace);
    } else if class_id == OperatorFamilyRelationId {
        let opf = GETSTRUCT(oldtup) as Form_pg_opfamily;

        IsThereOpFamilyInNamespace(new_name, (*opf).opfmethod, (*opf).opfnamespace);
    } else if class_id == SubscriptionRelationId {
        if SearchSysCacheExists2(
            SUBSCRIPTIONNAME,
            ObjectIdGetDatum(MyDatabaseId),
            CStringGetDatum(new_name),
        ) {
            report_name_conflict(class_id, new_name);
        }

        /*
         * Also enforce regression testing naming rules, if enabled
         * (ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS - disabled by default).
         */

        /* Wake up related replication workers to handle this change quickly */
        LogicalRepWorkersWakeupAtCommit(object_id);
    } else if name_cache_id >= 0 {
        if OidIsValid(namespace_id) {
            if SearchSysCacheExists2(
                name_cache_id,
                CStringGetDatum(new_name),
                ObjectIdGetDatum(namespace_id),
            ) {
                report_namespace_conflict(class_id, new_name, namespace_id);
            }
        } else if SearchSysCacheExists1(name_cache_id, CStringGetDatum(new_name)) {
            report_name_conflict(class_id, new_name);
        }
    }

    /* Build modified tuple */
    values = palloc0(RelationGetNumberOfAttributes(rel) as usize * size_of::<Datum>())
        as *mut Datum;
    nulls =
        palloc0(RelationGetNumberOfAttributes(rel) as usize * size_of::<bool>()) as *mut bool;
    replaces =
        palloc0(RelationGetNumberOfAttributes(rel) as usize * size_of::<bool>()) as *mut bool;
    namestrcpy(&mut nameattrdata as Name, new_name);
    *values.add((anum_name - 1) as usize) = NameGetDatum(&nameattrdata);
    *replaces.add((anum_name - 1) as usize) = true;
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

    /* Perform actual update */
    CatalogTupleUpdate(rel, &mut (*oldtup).t_self, newtup);

    InvokeObjectPostAlterHook(class_id, object_id, 0);

    /* Do post catalog-update tasks */
    if class_id == PublicationRelationId {
        let pub_: Form_pg_publication = GETSTRUCT(oldtup) as Form_pg_publication;

        /*
         * Invalidate relsynccache entries.
         *
         * Unlike ALTER PUBLICATION ADD/SET/DROP commands, renaming a
         * publication does not impact the publication status of tables. So,
         * we don't need to invalidate relcache to rebuild the rd_pubdesc.
         * Instead, we invalidate only the relsyncache.
         */
        InvalidatePubRelSyncCache((*pub_).oid, (*pub_).puballtables);
    }

    /* Release memory */
    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    pfree(replaces as *mut c_void);
    heap_freetuple(newtup);

    ReleaseSysCache(oldtup);
}

/*
 * Executes an ALTER OBJECT / RENAME TO statement.  Based on the object
 * type, the function appropriate to that type is executed.
 *
 * Return value is the address of the renamed object.
 */
pub unsafe fn ExecRenameStmt(stmt: *mut RenameStmt) -> ObjectAddress {
    match (*stmt).renameType {
        OBJECT_TABCONSTRAINT | OBJECT_DOMCONSTRAINT => RenameConstraint(stmt),

        OBJECT_DATABASE => RenameDatabase((*stmt).subname, (*stmt).newname),

        OBJECT_ROLE => RenameRole((*stmt).subname, (*stmt).newname),

        OBJECT_SCHEMA => RenameSchema((*stmt).subname, (*stmt).newname),

        OBJECT_TABLESPACE => RenameTableSpace((*stmt).subname, (*stmt).newname),

        OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW | OBJECT_INDEX
        | OBJECT_FOREIGN_TABLE => RenameRelation(stmt),

        OBJECT_COLUMN | OBJECT_ATTRIBUTE => renameatt(stmt),

        OBJECT_RULE => RenameRewriteRule((*stmt).relation, (*stmt).subname, (*stmt).newname),

        OBJECT_TRIGGER => renametrig(stmt),

        OBJECT_POLICY => rename_policy(stmt),

        OBJECT_DOMAIN | OBJECT_TYPE => RenameType(stmt),

        OBJECT_AGGREGATE
        | OBJECT_COLLATION
        | OBJECT_CONVERSION
        | OBJECT_EVENT_TRIGGER
        | OBJECT_FDW
        | OBJECT_FOREIGN_SERVER
        | OBJECT_FUNCTION
        | OBJECT_OPCLASS
        | OBJECT_OPFAMILY
        | OBJECT_LANGUAGE
        | OBJECT_PROCEDURE
        | OBJECT_ROUTINE
        | OBJECT_STATISTIC_EXT
        | OBJECT_TSCONFIGURATION
        | OBJECT_TSDICTIONARY
        | OBJECT_TSPARSER
        | OBJECT_TSTEMPLATE
        | OBJECT_PUBLICATION
        | OBJECT_SUBSCRIPTION => {
            let address: ObjectAddress;
            let catalog: Relation;

            address = get_object_address(
                (*stmt).renameType,
                (*stmt).object,
                null_mut(),
                AccessExclusiveLock,
                false,
            );

            catalog = table_open(address.classId, RowExclusiveLock);
            AlterObjectRename_internal(catalog, address.objectId, (*stmt).newname);
            table_close(catalog, RowExclusiveLock);

            address
        }

        _ => {
            elog!(
                ERROR,
                "unrecognized rename stmt type: {}",
                (*stmt).renameType as c_int
            );
            #[allow(unreachable_code)]
            InvalidObjectAddress() /* keep compiler happy */
        }
    }
}

/*
 * Executes an ALTER OBJECT / [NO] DEPENDS ON EXTENSION statement.
 *
 * Return value is the address of the altered object.  refAddress is an output
 * argument which, if not null, receives the address of the object that the
 * altered object now depends on.
 */
pub unsafe fn ExecAlterObjectDependsStmt(
    stmt: *mut AlterObjectDependsStmt,
    ref_address: *mut ObjectAddress,
) -> ObjectAddress {
    let address: ObjectAddress;
    let ref_addr: ObjectAddress;
    let mut rel: Relation = null_mut();

    address = get_object_address_rv(
        (*stmt).objectType,
        (*stmt).relation,
        (*stmt).object as *mut List,
        &mut rel,
        AccessExclusiveLock,
        false,
    );

    /*
     * Verify that the user is entitled to run the command.
     *
     * We don't check any privileges on the extension, because that's not
     * needed.  The object owner is stipulating, by running this command, that
     * the extension owner can drop the object whenever they feel like it,
     * which is not considered a problem.
     */
    check_object_ownership(
        GetUserId(),
        (*stmt).objectType,
        address,
        (*stmt).object,
        rel,
    );

    /*
     * If a relation was involved, it would have been opened and locked. We
     * don't need the relation here, but we'll retain the lock until commit.
     */
    if !rel.is_null() {
        table_close(rel, NoLock);
    }

    ref_addr = get_object_address(
        OBJECT_EXTENSION,
        (*stmt).extname as *mut Node,
        null_mut(),
        AccessExclusiveLock,
        false,
    );
    if !ref_address.is_null() {
        *ref_address = ref_addr;
    }

    if (*stmt).remove {
        deleteDependencyRecordsForSpecific(
            address.classId,
            address.objectId,
            DEPENDENCY_AUTO_EXTENSION as c_char,
            ref_addr.classId,
            ref_addr.objectId,
        );
    } else {
        let currexts: *mut List;

        /* Avoid duplicates */
        currexts = getAutoExtensionsOfObject(address.classId, address.objectId);
        if !list_member_oid(currexts, ref_addr.objectId) {
            recordDependencyOn(&address, &ref_addr, DEPENDENCY_AUTO_EXTENSION);
        }
    }

    address
}

/*
 * Executes an ALTER OBJECT / SET SCHEMA statement.  Based on the object
 * type, the function appropriate to that type is executed.
 *
 * Return value is that of the altered object.
 *
 * oldSchemaAddr is an output argument which, if not NULL, is set to the object
 * address of the original schema.
 */
pub unsafe fn ExecAlterObjectSchemaStmt(
    stmt: *mut AlterObjectSchemaStmt,
    old_schema_addr: *mut ObjectAddress,
) -> ObjectAddress {
    let address: ObjectAddress;
    let mut old_nsp_oid: Oid = InvalidOid;

    match (*stmt).objectType {
        OBJECT_EXTENSION => {
            address = AlterExtensionNamespace(
                strVal!((*stmt).object),
                (*stmt).newschema,
                if !old_schema_addr.is_null() {
                    &mut old_nsp_oid
                } else {
                    null_mut()
                },
            );
        }

        OBJECT_FOREIGN_TABLE | OBJECT_SEQUENCE | OBJECT_TABLE | OBJECT_VIEW | OBJECT_MATVIEW => {
            address = AlterTableNamespace(
                stmt,
                if !old_schema_addr.is_null() {
                    &mut old_nsp_oid
                } else {
                    null_mut()
                },
            );
        }

        OBJECT_DOMAIN | OBJECT_TYPE => {
            address = AlterTypeNamespace(
                castNode!(List, T_List, (*stmt).object),
                (*stmt).newschema,
                (*stmt).objectType,
                if !old_schema_addr.is_null() {
                    &mut old_nsp_oid
                } else {
                    null_mut()
                },
            );
        }

        /* generic code path */
        OBJECT_AGGREGATE
        | OBJECT_COLLATION
        | OBJECT_CONVERSION
        | OBJECT_FUNCTION
        | OBJECT_OPERATOR
        | OBJECT_OPCLASS
        | OBJECT_OPFAMILY
        | OBJECT_PROCEDURE
        | OBJECT_ROUTINE
        | OBJECT_STATISTIC_EXT
        | OBJECT_TSCONFIGURATION
        | OBJECT_TSDICTIONARY
        | OBJECT_TSPARSER
        | OBJECT_TSTEMPLATE => {
            let catalog: Relation;
            let class_id: Oid;
            let nsp_oid: Oid;

            address = get_object_address(
                (*stmt).objectType,
                (*stmt).object,
                null_mut(),
                AccessExclusiveLock,
                false,
            );
            class_id = address.classId;
            catalog = table_open(class_id, RowExclusiveLock);
            nsp_oid = LookupCreationNamespace((*stmt).newschema);

            old_nsp_oid = AlterObjectNamespace_internal(catalog, address.objectId, nsp_oid);
            table_close(catalog, RowExclusiveLock);
        }

        _ => {
            elog!(
                ERROR,
                "unrecognized AlterObjectSchemaStmt type: {}",
                (*stmt).objectType as c_int
            );
            #[allow(unreachable_code)]
            return InvalidObjectAddress(); /* keep compiler happy */
        }
    }

    if !old_schema_addr.is_null() {
        ObjectAddressSet(&mut *old_schema_addr, NamespaceRelationId, old_nsp_oid);
    }

    address
}

/*
 * Change an object's namespace given its classOid and object Oid.
 *
 * Objects that don't have a namespace should be ignored, as should
 * dependent types such as array types.
 *
 * This function is currently used only by ALTER EXTENSION SET SCHEMA,
 * so it only needs to cover object kinds that can be members of an
 * extension, and it can silently ignore dependent types --- we assume
 * those will be moved when their parent object is moved.
 *
 * Returns the OID of the object's previous namespace, or InvalidOid if
 * object doesn't have a schema or was ignored due to being a dependent type.
 */
pub unsafe fn AlterObjectNamespace_oid(
    class_id: Oid,
    objid: Oid,
    nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
) -> Oid {
    let mut old_nsp_oid: Oid = InvalidOid;

    match class_id {
        x if x == RelationRelationId => {
            let rel: Relation;

            rel = relation_open(objid, AccessExclusiveLock);
            old_nsp_oid = RelationGetNamespace(rel);

            AlterTableNamespaceInternal(rel, old_nsp_oid, nsp_oid, objs_moved);

            relation_close(rel, NoLock);
        }

        x if x == TypeRelationId => {
            old_nsp_oid = AlterTypeNamespace_oid(objid, nsp_oid, true, objs_moved);
        }

        x if x == ProcedureRelationId
            || x == CollationRelationId
            || x == ConversionRelationId
            || x == OperatorRelationId
            || x == OperatorClassRelationId
            || x == OperatorFamilyRelationId
            || x == StatisticExtRelationId
            || x == TSParserRelationId
            || x == TSDictionaryRelationId
            || x == TSTemplateRelationId
            || x == TSConfigRelationId =>
        {
            let catalog: Relation;

            catalog = table_open(class_id, RowExclusiveLock);

            old_nsp_oid = AlterObjectNamespace_internal(catalog, objid, nsp_oid);

            table_close(catalog, RowExclusiveLock);
        }

        _ => {
            /* ignore object types that don't have schema-qualified names */
            Assert!(get_object_attnum_namespace(class_id) == InvalidAttrNumber);
        }
    }

    old_nsp_oid
}

/*
 * Generic function to change the namespace of a given object, for simple
 * cases (won't work for tables, nor other cases where we need to do more
 * than change the namespace column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objid: OID of object to change the namespace of
 * nspOid: OID of new namespace
 *
 * Returns the OID of the object's previous namespace.
 */
unsafe fn AlterObjectNamespace_internal(rel: Relation, objid: Oid, nsp_oid: Oid) -> Oid {
    let class_id = RelationGetRelid(rel);
    let oid_cache_id = get_object_catcache_oid(class_id);
    let name_cache_id = get_object_catcache_name(class_id);
    let anum_name = get_object_attnum_name(class_id);
    let anum_namespace = get_object_attnum_namespace(class_id);
    let anum_owner = get_object_attnum_owner(class_id);
    let old_nsp_oid: Oid;
    let name: Datum;
    let namespace: Datum;
    let mut isnull: bool = false;
    let tup: HeapTuple;
    let newtup: HeapTuple;
    let values: *mut Datum;
    let nulls: *mut bool;
    let replaces: *mut bool;

    tup = SearchSysCacheCopy1(oid_cache_id, ObjectIdGetDatum(objid));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(
            ERROR,
            "cache lookup failed for object {} of catalog \"{}\"",
            objid,
            cstr_to_string(RelationGetRelationName(rel))
        );
    }

    name = heap_getattr(tup, anum_name as c_int, RelationGetDescr(rel), &mut isnull);
    Assert!(!isnull);
    namespace = heap_getattr(
        tup,
        anum_namespace as c_int,
        RelationGetDescr(rel),
        &mut isnull,
    );
    Assert!(!isnull);
    old_nsp_oid = DatumGetObjectId(namespace);

    /*
     * If the object is already in the correct namespace, we don't need to do
     * anything except fire the object access hook.
     */
    if old_nsp_oid == nsp_oid {
        InvokeObjectPostAlterHook(class_id, objid, 0);
        return old_nsp_oid;
    }

    /* Check basic namespace related issues */
    CheckSetNamespace(old_nsp_oid, nsp_oid);

    /* Permission checks ... superusers can always do it */
    if !superuser() {
        let owner: Datum;
        let owner_id: Oid;
        let aclresult: AclResult;

        /* Fail if object does not have an explicit owner */
        if anum_owner <= 0 {
            let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            ereport!(
                ERROR,
                errmsg!(
                    "must be superuser to set schema of {}",
                    cstr_to_string(getObjectDescriptionOids(class_id, objid))
                )
            );
        }

        /* Otherwise, must be owner of the existing object */
        owner = heap_getattr(tup, anum_owner as c_int, RelationGetDescr(rel), &mut isnull);
        Assert!(!isnull);
        owner_id = DatumGetObjectId(owner);

        if !has_privs_of_role(GetUserId(), owner_id) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                get_object_type(class_id, objid),
                NameStr(&*(DatumGetName(name))),
            );
        }

        /* User must have CREATE privilege on new namespace */
        aclresult = object_aclcheck(NamespaceRelationId, nsp_oid, GetUserId(), ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(nsp_oid));
        }
    }

    /*
     * Check for duplicate name (more friendly than unique-index failure).
     * Since this is just a friendliness check, we can just skip it in cases
     * where there isn't suitable support.
     */
    if class_id == ProcedureRelationId {
        let proc = GETSTRUCT(tup) as Form_pg_proc;

        IsThereFunctionInNamespace(
            NameStr(&(*proc).proname),
            (*proc).pronargs as c_int,
            pg_proc_proargtypes(proc),
            nsp_oid,
        );
    } else if class_id == CollationRelationId {
        let coll = GETSTRUCT(tup) as Form_pg_collation;

        IsThereCollationInNamespace(NameStr(&(*coll).collname), nsp_oid);
    } else if class_id == OperatorClassRelationId {
        let opc = GETSTRUCT(tup) as Form_pg_opclass;

        IsThereOpClassInNamespace(NameStr(&(*opc).opcname), (*opc).opcmethod, nsp_oid);
    } else if class_id == OperatorFamilyRelationId {
        let opf = GETSTRUCT(tup) as Form_pg_opfamily;

        IsThereOpFamilyInNamespace(NameStr(&(*opf).opfname), (*opf).opfmethod, nsp_oid);
    } else if name_cache_id >= 0
        && SearchSysCacheExists2(name_cache_id, name, ObjectIdGetDatum(nsp_oid))
    {
        report_namespace_conflict(class_id, NameStr(&*(DatumGetName(name))), nsp_oid);
    }

    /* Build modified tuple */
    values = palloc0(RelationGetNumberOfAttributes(rel) as usize * size_of::<Datum>())
        as *mut Datum;
    nulls =
        palloc0(RelationGetNumberOfAttributes(rel) as usize * size_of::<bool>()) as *mut bool;
    replaces =
        palloc0(RelationGetNumberOfAttributes(rel) as usize * size_of::<bool>()) as *mut bool;
    *values.add((anum_namespace - 1) as usize) = ObjectIdGetDatum(nsp_oid);
    *replaces.add((anum_namespace - 1) as usize) = true;
    newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    /* Perform actual update */
    CatalogTupleUpdate(rel, &mut (*tup).t_self, newtup);

    /* Release memory */
    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);
    pfree(replaces as *mut c_void);

    /* update dependency to point to the new schema */
    if changeDependencyFor(class_id, objid, NamespaceRelationId, old_nsp_oid, nsp_oid) != 1 {
        elog!(ERROR, "could not change schema dependency for object {}", objid);
    }

    InvokeObjectPostAlterHook(class_id, objid, 0);

    old_nsp_oid
}

/*
 * Executes an ALTER OBJECT / OWNER TO statement.  Based on the object
 * type, the function appropriate to that type is executed.
 */
pub unsafe fn ExecAlterOwnerStmt(stmt: *mut AlterOwnerStmt) -> ObjectAddress {
    let newowner = get_rolespec_oid((*stmt).newowner, false);

    match (*stmt).objectType {
        OBJECT_DATABASE => AlterDatabaseOwner(strVal!((*stmt).object), newowner),

        OBJECT_SCHEMA => AlterSchemaOwner(strVal!((*stmt).object), newowner),

        OBJECT_TYPE | OBJECT_DOMAIN /* same as TYPE */ => {
            AlterTypeOwner(castNode!(List, T_List, (*stmt).object), newowner, (*stmt).objectType)
        }

        OBJECT_FDW => AlterForeignDataWrapperOwner(strVal!((*stmt).object), newowner),

        OBJECT_FOREIGN_SERVER => AlterForeignServerOwner(strVal!((*stmt).object), newowner),

        OBJECT_EVENT_TRIGGER => AlterEventTriggerOwner(strVal!((*stmt).object), newowner),

        OBJECT_PUBLICATION => AlterPublicationOwner(strVal!((*stmt).object), newowner),

        OBJECT_SUBSCRIPTION => AlterSubscriptionOwner(strVal!((*stmt).object), newowner),

        /* Generic cases */
        OBJECT_AGGREGATE
        | OBJECT_COLLATION
        | OBJECT_CONVERSION
        | OBJECT_FUNCTION
        | OBJECT_LANGUAGE
        | OBJECT_LARGEOBJECT
        | OBJECT_OPERATOR
        | OBJECT_OPCLASS
        | OBJECT_OPFAMILY
        | OBJECT_PROCEDURE
        | OBJECT_ROUTINE
        | OBJECT_STATISTIC_EXT
        | OBJECT_TABLESPACE
        | OBJECT_TSDICTIONARY
        | OBJECT_TSCONFIGURATION => {
            let address: ObjectAddress;

            address = get_object_address(
                (*stmt).objectType,
                (*stmt).object,
                null_mut(),
                AccessExclusiveLock,
                false,
            );

            AlterObjectOwner_internal(address.classId, address.objectId, newowner);

            address
        }

        _ => {
            elog!(
                ERROR,
                "unrecognized AlterOwnerStmt type: {}",
                (*stmt).objectType as c_int
            );
            #[allow(unreachable_code)]
            InvalidObjectAddress() /* keep compiler happy */
        }
    }
}

/*
 * Generic function to change the ownership of a given object, for simple
 * cases (won't work for tables, nor other cases where we need to do more than
 * change the ownership column of a single catalog entry).
 *
 * classId: OID of catalog containing object
 * objectId: OID of object to change the ownership of
 * new_ownerId: OID of new object owner
 *
 * This will work on large objects, but we have to beware of the fact that
 * classId isn't the OID of the catalog to modify in that case.
 */
pub unsafe fn AlterObjectOwner_internal(class_id: Oid, object_id: Oid, new_owner_id: Oid) {
    /* For large objects, the catalog to modify is pg_largeobject_metadata */
    let catalog_id: Oid = if class_id == LargeObjectRelationId {
        LargeObjectMetadataRelationId
    } else {
        class_id
    };
    let anum_oid = get_object_attnum_oid(catalog_id);
    let anum_owner = get_object_attnum_owner(catalog_id);
    let anum_namespace = get_object_attnum_namespace(catalog_id);
    let anum_acl = get_object_attnum_acl(catalog_id);
    let anum_name = get_object_attnum_name(catalog_id);
    let rel: Relation;
    let oldtup: HeapTuple;
    let mut datum: Datum;
    let mut isnull: bool = false;
    let old_owner_id: Oid;
    let mut namespace_id: Oid = InvalidOid;

    rel = table_open(catalog_id, RowExclusiveLock);

    /* Search tuple and lock it. */
    oldtup = get_catalog_object_by_oid_extended(rel, anum_oid, object_id, true);
    if oldtup.is_null() {
        elog!(
            ERROR,
            "cache lookup failed for object {} of catalog \"{}\"",
            object_id,
            cstr_to_string(RelationGetRelationName(rel))
        );
    }

    datum = heap_getattr(oldtup, anum_owner as c_int, RelationGetDescr(rel), &mut isnull);
    Assert!(!isnull);
    old_owner_id = DatumGetObjectId(datum);

    if anum_namespace != InvalidAttrNumber {
        datum = heap_getattr(
            oldtup,
            anum_namespace as c_int,
            RelationGetDescr(rel),
            &mut isnull,
        );
        Assert!(!isnull);
        namespace_id = DatumGetObjectId(datum);
    }

    if old_owner_id != new_owner_id {
        let nattrs: AttrNumber;
        let newtup: HeapTuple;
        let values: *mut Datum;
        let nulls: *mut bool;
        let replaces: *mut bool;

        /* Superusers can bypass permission checks */
        if !superuser() {
            /* must be owner */
            if !has_privs_of_role(GetUserId(), old_owner_id) {
                let objname: *mut c_char;
                let mut namebuf: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

                if anum_name != InvalidAttrNumber {
                    datum = heap_getattr(
                        oldtup,
                        anum_name as c_int,
                        RelationGetDescr(rel),
                        &mut isnull,
                    );
                    Assert!(!isnull);
                    objname = NameStr(&*DatumGetName(datum)) as *mut c_char;
                } else {
                    let s = format!("{}\0", object_id);
                    let bytes = s.as_bytes();
                    let n = core::cmp::min(bytes.len(), NAMEDATALEN);
                    for i in 0..n {
                        namebuf[i] = bytes[i] as c_char;
                    }
                    objname = namebuf.as_mut_ptr();
                }
                aclcheck_error(
                    ACLCHECK_NOT_OWNER,
                    get_object_type(catalog_id, object_id),
                    objname,
                );
            }
            /* Must be able to become new owner */
            check_can_set_role(GetUserId(), new_owner_id);

            /* New owner must have CREATE privilege on namespace */
            if OidIsValid(namespace_id) {
                let aclresult: AclResult;

                aclresult = object_aclcheck(
                    NamespaceRelationId,
                    namespace_id,
                    new_owner_id,
                    ACL_CREATE,
                );
                if aclresult != ACLCHECK_OK {
                    aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespace_id));
                }
            }
        }

        /* Build a modified tuple */
        nattrs = RelationGetNumberOfAttributes(rel) as AttrNumber;
        values = palloc0(nattrs as usize * size_of::<Datum>()) as *mut Datum;
        nulls = palloc0(nattrs as usize * size_of::<bool>()) as *mut bool;
        replaces = palloc0(nattrs as usize * size_of::<bool>()) as *mut bool;
        *values.add((anum_owner - 1) as usize) = ObjectIdGetDatum(new_owner_id);
        *replaces.add((anum_owner - 1) as usize) = true;

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        if anum_acl != InvalidAttrNumber {
            datum = heap_getattr(oldtup, anum_acl as c_int, RelationGetDescr(rel), &mut isnull);
            if !isnull {
                let new_acl: *mut Acl;

                new_acl = aclnewowner(DatumGetAclP(datum), old_owner_id, new_owner_id);
                *values.add((anum_acl - 1) as usize) = PointerGetDatum(new_acl);
                *replaces.add((anum_acl - 1) as usize) = true;
            }
        }

        newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

        /* Perform actual update */
        CatalogTupleUpdate(rel, &mut (*newtup).t_self, newtup);

        UnlockTuple(rel, &mut (*oldtup).t_self, InplaceUpdateTupleLock);

        /* Update owner dependency reference */
        changeDependencyOnOwner(class_id, object_id, new_owner_id);

        /* Release memory */
        pfree(values as *mut c_void);
        pfree(nulls as *mut c_void);
        pfree(replaces as *mut c_void);
    } else {
        UnlockTuple(rel, &mut (*oldtup).t_self, InplaceUpdateTupleLock);
    }

    /* Note the post-alter hook gets classId not catalogId */
    InvokeObjectPostAlterHook(class_id, object_id, 0);

    table_close(rel, RowExclusiveLock);
}
