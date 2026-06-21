/*-------------------------------------------------------------------------
 *
 * aclchk.rs
 *    Routines to check access control permissions.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/catalog/aclchk.c
 *
 * NOTES
 *    See acl.h.
 *
 *    The xxx_aclmask() functions in this file are wrappers around
 *    acl.c's aclmask() function; see that for basic usage information.
 *    The wrapper functions add object-type-specific lookup capability.
 *    Generally, they will throw error if the object doesn't exist.
 *
 *    The xxx_aclmask_ext() functions add the ability to not throw
 *    error if the object doesn't exist.  If their "is_missing" argument
 *    isn't NULL, then when the object isn't found they will set
 *    *is_missing = true and return zero (no privileges) instead of
 *    throwing an error.  Caller must initialize *is_missing = false.
 *
 *    The xxx_aclcheck() functions are simplified wrappers around the
 *    corresponding xxx_aclmask() functions, simply returning ACLCHECK_OK
 *    if any of the privileges specified in "mode" are held, and otherwise
 *    a suitable error code (in practice, always ACLCHECK_NO_PRIV).
 *    Again, they will throw error if the object doesn't exist.
 *
 *    The xxx_aclcheck_ext() functions add the ability to not throw
 *    error if the object doesn't exist.  Their "is_missing" argument
 *    works similarly to the xxx_aclmask_ext() functions.
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
    unreachable_patterns,
    improper_ctypes,
    clippy::all,
)]

use crate::prelude::*;
use crate::postgres_ext::Oid;

// Node / parse types
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{List, ListCell, NIL, lappend_oid, lappend, list_concat, list_head, lnext};
use crate::nodes::parsenodes::{
    ObjectType, ObjectType::*, DropBehavior, DropBehavior::*,
    AclMode, GrantStmt, AlterDefaultPrivilegesStmt, AccessPriv,
    RoleSpec, RoleSpecType::ROLESPEC_PUBLIC, DefElem, TypeName,
    ACL_INSERT, ACL_SELECT, ACL_UPDATE, ACL_DELETE, ACL_TRUNCATE,
    ACL_REFERENCES, ACL_TRIGGER, ACL_EXECUTE, ACL_USAGE, ACL_CREATE,
    ACL_CREATE_TEMP, ACL_CONNECT, ACL_SET, ACL_ALTER_SYSTEM, ACL_MAINTAIN,
    ACL_NO_RIGHTS,
};
use crate::nodes::primnodes::RangeVar;
// ParseState is not yet translated; use c_void stub
type ParseState = c_void;
// list_make1_oid is a #[macro_export] macro accessible as crate::list_make1_oid!
// strVal is a #[macro_export] macro in nodes::value -- define a safe fn wrapper
#[inline]
unsafe fn strVal(v: *mut c_void) -> *const c_char {
    // In PG15+ String node has .sval; for a translated port, sval is at a known offset
    // Stub: access String::sval directly via casting
    if v.is_null() { return core::ptr::null(); }
    let s = v as *mut crate::nodes::value::String;
    (*s).sval
}
// Value is the old pre-PG15 union node type; in PG15+ it's split into String/Integer/Float.
// Use c_void as the type for these casts; strVal fn above handles the extraction.
type Value = c_void;

// Internal grant structure
use crate::utils::aclchk_internal::InternalGrant;

// ACL types and functions
use crate::utils::adt::acl::{
    Acl, AclItem, AclMaskHow, AclMaskHow::*, AclResult, AclResult::*,
    ACL_ID_PUBLIC,
    ACL_MODECHG_ADD, ACL_MODECHG_DEL,
    ACL_ALL_RIGHTS_COLUMN, ACL_ALL_RIGHTS_RELATION, ACL_ALL_RIGHTS_SEQUENCE,
    ACL_ALL_RIGHTS_DATABASE, ACL_ALL_RIGHTS_FDW, ACL_ALL_RIGHTS_FOREIGN_SERVER,
    ACL_ALL_RIGHTS_FUNCTION, ACL_ALL_RIGHTS_LANGUAGE, ACL_ALL_RIGHTS_LARGEOBJECT,
    ACL_ALL_RIGHTS_PARAMETER_ACL, ACL_ALL_RIGHTS_SCHEMA, ACL_ALL_RIGHTS_TABLESPACE,
    ACL_ALL_RIGHTS_TYPE,
    ACLITEM_ALL_PRIV_BITS,
    ACL_GRANT_OPTION_FOR, ACL_OPTION_TO_PRIVS,
    ACLITEM_SET_PRIVS_GOPTIONS,
    ACL_NUM, ACL_DAT,
    BOOTSTRAP_SUPERUSERID,
    make_empty_acl, aclcopy, aclconcat, aclmerge, aclupdate, aclnewowner,
    acldefault, aclmembers, aclmask, aclitemsort, aclequal,
    select_best_grantor,
    DatabaseRelationId, ForeignDataWrapperRelationId, ForeignServerRelationId,
    ProcedureRelationId, LanguageRelationId, NamespaceRelationId,
    TableSpaceRelationId, TypeRelationId,
    GetUserId, superuser_arg,
    MyDatabaseId,
};
// DatumGetAclP is private in acl.rs; define a local wrapper that casts Datum (ptr) to *mut Acl
#[inline]
unsafe fn DatumGetAclP(x: Datum) -> *mut Acl {
    x as *mut Acl
}
// ROLE_PG_* are predefined role OIDs not yet wired; stub as constants
const ROLE_PG_READ_ALL_DATA: Oid  = 0xFFFF_FF01; /* TODO(pg-port) */
const ROLE_PG_WRITE_ALL_DATA: Oid = 0xFFFF_FF02; /* TODO(pg-port) */
const ROLE_PG_MAINTAIN: Oid       = 0xFFFF_FF03; /* TODO(pg-port) */

// Heap / access
use crate::access::htup_details::{
    HeapTuple, HeapTupleIsValid, GETSTRUCT,
    heap_getattr,
};
// heap_form_tuple and heap_modify_tuple are not yet wired; stubs:
use crate::access::common::tupdesc::TupleDescData;
type TupleDesc = *mut TupleDescData;
#[inline]
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple { crate::access::common::heaptuple::heap_form_tuple(_tupleDescriptor as _, _values as _, _isnull as _) }
#[inline]
unsafe fn heap_modify_tuple(
    _tuple: HeapTuple,
    _tupleDesc: TupleDesc,
    _replValues: *mut Datum,
    _replIsnull: *mut bool,
    _doReplace: *mut bool,
) -> HeapTuple { crate::access::common::heaptuple::heap_modify_tuple(_tuple as _, _tupleDesc as _, _replValues as _, _replIsnull as _, _doReplace as _) }
// RelationGetDescr from utils::rel is already wired
use crate::utils::rel::RelationGetDescr;
use crate::access::htup_details::HeapTupleData;
use crate::access::table::table::{table_open, table_close};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::stratnum::StrategyNumber;
use crate::access::sdir::ForwardScanDirection;
use crate::access::table::tableam::table_beginscan_catalog;

// Locking
use crate::storage::lockdefs::{LOCKMODE, AccessShareLock, RowExclusiveLock};

// Catalog OIDs and form types
use crate::catalog::catalog_oids::{
    RelationRelationId, AttributeRelationId, DefaultAclRelationId,
    LargeObjectRelationId, LargeObjectMetadataRelationId,
    InitPrivsRelationId, ParameterAclRelationId,
};
use crate::catalog::catalog::IsSystemClass;
use crate::catalog::pg_class::{
    Form_pg_class,
    RELKIND_RELATION, RELKIND_VIEW, RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE,
    RELKIND_PARTITIONED_TABLE, RELKIND_SEQUENCE, RELKIND_INDEX,
    RELKIND_PARTITIONED_INDEX, RELKIND_COMPOSITE_TYPE,
};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_default_acl::{
    Form_pg_default_acl,
    DEFACLOBJ_RELATION, DEFACLOBJ_SEQUENCE, DEFACLOBJ_FUNCTION,
    DEFACLOBJ_TYPE, DEFACLOBJ_NAMESPACE, DEFACLOBJ_LARGEOBJECT,
};
use crate::catalog::pg_language::Form_pg_language;
use crate::catalog::pg_largeobject_metadata::Form_pg_largeobject_metadata;
use crate::catalog::pg_authid::Form_pg_authid;
use crate::catalog::pg_namespace::Form_pg_namespace;
use crate::catalog::pg_type::{Form_pg_type, TYPTYPE_MULTIRANGE};
use crate::catalog::pg_proc::{Form_pg_proc, PROKIND_PROCEDURE};
use crate::catalog::indexing::{CatalogTupleInsert, CatalogTupleUpdate, CatalogTupleDelete};
use crate::catalog::objectaccess::ObjectAddress;
// InvokeObjectPostCreateHook/AlterHook not yet wired; stubs:
#[inline]
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}
#[inline]
unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}
// creating_extension lives in commands/extension.c; not yet wired
static mut creating_extension: bool = false;

// Scan helpers
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::relscan::SysScanDescData;
use crate::access::index::genam::{
    systable_beginscan, systable_endscan, SysScanDesc,
};
// Wrap systable_getnext to return *mut HeapTupleData matching htup_details::HeapTuple
use crate::access::index::genam::systable_getnext as _systable_getnext_void;
#[inline]
unsafe fn systable_getnext(scan: SysScanDesc) -> HeapTuple {
    _systable_getnext_void(scan) as HeapTuple
}
use crate::utils::rel::Relation;

// Postgres core / misc
use crate::postgres::{Datum, ObjectIdGetDatum, Int16GetDatum, Int32GetDatum,
    PointerGetDatum, CharGetDatum, DatumGetObjectId, DatumGetPointer,
    DatumGetName,
};
// Snapshot not yet wired from transam; use c_void stub
type Snapshot = *mut c_void;
// ParseState not in parsenodes (it's in parser/parse_node.h); stub:
// type ParseState already defined above
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::storage::lockdefs::InplaceUpdateTupleLock;

// --------------------------------
// TODO(pg-port) stubs - real symbols live in the C files noted
// --------------------------------

extern "C" {
    // utils/cache/syscache.c
    fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple;
    fn SearchSysCache3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> HeapTuple;
    fn SearchSysCacheLocked1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn SysCacheGetAttr(
        cacheId: c_int, tup: HeapTuple, attributeNumber: AttrNumber, isNull: *mut bool,
    ) -> Datum;
    fn SysCacheGetAttrNotNull(
        cacheId: c_int, tup: HeapTuple, attributeNumber: AttrNumber,
    ) -> Datum;
    // utils/cache/lsyscache.c
    fn get_attnum(relid: Oid, attname: *const c_char) -> AttrNumber;
    fn get_rel_name(relid: Oid) -> *mut c_char;
    fn get_multirange_range(multirangeOid: Oid) -> Oid;
    fn IsTrueArrayType(form: Form_pg_type) -> bool;
    fn has_privs_of_role(member: Oid, role: Oid) -> bool;
    fn isTempNamespace(namespaceId: Oid) -> bool;
    // catalog/objectaddress.c
    fn get_object_catcache_oid(classid: Oid) -> c_int;
    fn get_object_class_descr(classid: Oid) -> *const c_char;
    fn get_object_attnum_owner(classid: Oid) -> AttrNumber;
    fn get_object_attnum_acl(classid: Oid) -> AttrNumber;
    fn get_object_attnum_name(classid: Oid) -> AttrNumber;
    fn get_object_attnum_oid(classid: Oid) -> AttrNumber;
    fn get_object_oid_index(classid: Oid) -> Oid;
    fn get_object_type(classid: Oid, objectid: Oid) -> ObjectType;
    fn get_object_address(
        objtype: ObjectType, object: *mut Node, relation: *mut *mut crate::utils::rel::RelationData,
        lockmode: LOCKMODE, missing_ok: bool,
    ) -> ObjectAddress;
    // commands/extension.c
    fn get_rolespec_oid(rolespec: *const RoleSpec, missing_ok: bool) -> Oid;
    fn errorConflictingDefElem(defel: *const DefElem, pstate: *const ParseState);
    // catalog/namespace.c
    fn LookupExplicitNamespace(nspname: *const c_char, missing_ok: bool) -> Oid;
    fn RangeVarGetRelid(relation: *const RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> Oid;
    fn makeTypeNameFromNameList(names: *mut List) -> *mut TypeName;
    // commands/event_trigger.c
    fn EventTriggerSupportsObjectType(obtype: ObjectType) -> bool;
    fn EventTriggerCollectGrant(istmt: *mut InternalGrant);
    // catalog/dependency.c
    fn performDeletion(object: *const ObjectAddress, behavior: DropBehavior, flags: c_int);
    fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid);
    fn recordDependencyOn(
        depender: *const ObjectAddress, referenced: *const ObjectAddress,
        deptype: c_char,
    );
    // catalog/pg_shdepend.c
    fn updateAclDependencies(
        classId: Oid, objectId: Oid, objsubId: c_int, ownerId: Oid,
        noldmembers: c_int, oldmembers: *mut Oid,
        nnewmembers: c_int, newmembers: *mut Oid,
    );
    fn updateInitAclDependencies(
        classId: Oid, objectId: Oid, objsubId: c_int,
        noldmembers: c_int, oldmembers: *mut Oid,
        nnewmembers: c_int, newmembers: *mut Oid,
    );
    // catalog/pg_parameter_acl.c
    fn ParameterAclLookup(parameter: *const c_char, missing_ok: bool) -> Oid;
    fn ParameterAclCreate(parameter: *const c_char) -> Oid;
    fn convert_GUC_name_for_parameter_acl(name: *const c_char) -> *mut c_char;
    // access/xact.c
    fn CommandCounterIncrement();
    // utils/adt/varlena.c
    fn cstring_to_text(s: *const c_char) -> *mut crate::c::text;
    fn TextDatumGetCString(d: Datum) -> *mut c_char;
    fn text_to_cstring(t: *const crate::c::text) -> *mut c_char;
    // utils/builtins.h
    fn palloc0(size: usize) -> *mut c_void;
    fn palloc0_array_datum(n: usize) -> *mut Datum;
    fn palloc0_array_bool(n: usize) -> *mut bool;
    // storage/lmgr.c
    fn UnlockTuple(relation: Relation, tid: *const crate::storage::itemptr::ItemPointerData, lockmode: LOCKMODE);
    // access/heapam.c
    fn heap_getnext(scan: crate::access::relscan::TableScanDesc, direction: c_int) -> HeapTuple;
    fn table_endscan(scan: crate::access::relscan::TableScanDesc);
    // utils/misc/guc.c
    fn IsBootstrapProcessingMode() -> bool;
    // utils/cache/lsyscache.c (already in acl.rs but we redeclare for local use)
    fn get_namespace_oid(nspname: *const c_char, missing_ok: bool) -> Oid;
    // snprintf from libc
    fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
}

unsafe fn get_element_type(typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_element_type(typid as _) as _ }
unsafe fn format_type_be(type_oid: Oid) -> *mut c_char { crate::utils::adt::format_type::format_type_be(type_oid as _) as _ }

// Palloc helpers: the C uses palloc0_array(T, n) -- produce *mut T via palloc0.
#[inline]
unsafe fn palloc0_datums(n: usize) -> *mut Datum {
    palloc0(n * core::mem::size_of::<Datum>()) as *mut Datum
}
#[inline]
unsafe fn palloc0_bools(n: usize) -> *mut bool {
    palloc0(n * core::mem::size_of::<bool>()) as *mut bool
}

// --------------------------------
// Catalog index / syscache constants (genbki-generated in C)
// --------------------------------
const RELOID: c_int          = 57;
const ATTNUM: c_int          = 7;
const AUTHOID: c_int         = 11;
const NAMESPACEOID: c_int    = 38;
const TYPEOID: c_int         = 82;
const DEFACLROLENSPOBJ: c_int = crate::utils::cache::syscache_ids_gen::DEFACLROLENSPOBJ;
const PARAMETERACLOID: c_int  = 44;   // TODO(pg-port)
const PARAMETERACLNAME: c_int = 43;   // TODO(pg-port)

const DefaultAclOidIndexId: Oid              = 828;
const LargeObjectMetadataOidIndexId: Oid     = 2996;
const InitPrivsObjIndexId: Oid               = 3395;   // TODO(pg-port)

// Anum_ constants needed in this file
const Anum_pg_default_acl_oid: AttrNumber           = 1;
const Anum_pg_default_acl_defaclrole: AttrNumber    = 2;
const Anum_pg_default_acl_defaclnamespace: AttrNumber = 3;
const Anum_pg_default_acl_defaclobjtype: AttrNumber = 4;
const Anum_pg_default_acl_defaclacl: AttrNumber     = 5;
const Natts_pg_default_acl: usize                   = 5;

const Anum_pg_attribute_attacl: AttrNumber          = 22;
const Natts_pg_attribute: usize                     = 43;

const Anum_pg_class_relacl: AttrNumber              = 31;
const Natts_pg_class: usize                         = 34;

const Anum_pg_largeobject_metadata_oid: AttrNumber      = 1;
const Anum_pg_largeobject_metadata_lomacl: AttrNumber   = 3;
const Natts_pg_largeobject_metadata: usize              = 3;

const Anum_pg_init_privs_objoid: AttrNumber     = 1;
const Anum_pg_init_privs_classoid: AttrNumber   = 2;
const Anum_pg_init_privs_objsubid: AttrNumber   = 3;
const Anum_pg_init_privs_privtype: AttrNumber   = 4;
const Anum_pg_init_privs_initprivs: AttrNumber  = 5;
const Natts_pg_init_privs: usize                = 5;

const Anum_pg_parameter_acl_parname: AttrNumber = 2;
const Anum_pg_parameter_acl_paracl: AttrNumber  = 3;
const Natts_pg_parameter_acl: usize             = 3;

const Anum_pg_namespace_nspacl: AttrNumber  = 4;
const Anum_pg_type_typacl: AttrNumber       = 34;

const Anum_pg_class_relnamespace: AttrNumber = 3;
const Anum_pg_class_relkind: AttrNumber      = 17;
const Anum_pg_proc_pronamespace: AttrNumber  = 3;
const Anum_pg_proc_prokind: AttrNumber       = 14;

// dependency type constant
const DEPENDENCY_AUTO: c_char = b'a' as c_char;
// pg_init_privs privtype
const INITPRIVS_EXTENSION: c_char = b'e' as c_char;

// Scan direction
const FORWARD: c_int = ForwardScanDirection;

// F_ strategy function OIDs (from fmgroids.h)
const F_OIDEQ: u32   = 184;
const F_CHAREQ: u32  = 1048;
const F_CHARNE: u32  = 1053;
const F_INT4EQ: u32  = 65;
const BTEqualStrategyNumber: StrategyNumber = 3;

const InvalidOid: Oid = 0;
const NAMEDATALEN: usize = 64;
// DEPENDENCY_AUTO already defined as const above

// palloc/pfree
extern "C" { fn pfree(pointer: *mut c_void); }
#[inline]
unsafe fn palloc(size: usize) -> *mut c_void {
    palloc0(size)
}

// NameStr macro equivalent
#[inline]
unsafe fn NameStr(n: crate::c::NameData) -> *const c_char {
    n.data.as_ptr() as *const c_char
}

// DatumGetAclPCopy
#[inline]
unsafe fn DatumGetAclPCopy(d: Datum) -> *mut Acl {
    // aclcopy of the toasted-detoasted form
    let acl = DatumGetAclP(d);
    aclcopy(acl)
}

/*
 * Internal format used by ALTER DEFAULT PRIVILEGES.
 */
#[repr(C)]
struct InternalDefaultACL {
    roleid: Oid,        /* owning role */
    nspid: Oid,         /* namespace, or InvalidOid if none */
    /* remaining fields are same as in InternalGrant: */
    is_grant: bool,
    objtype: ObjectType,
    all_privs: bool,
    privileges: AclMode,
    grantees: *mut List,
    grant_option: bool,
    behavior: DropBehavior,
}

/*
 * When performing a binary-upgrade, pg_dump will call a function to set
 * this variable to let us know that we need to populate the pg_init_privs
 * table for the GRANT/REVOKE commands while this variable is set to true.
 */
#[no_mangle]
pub static mut binary_upgrade_record_init_privs: bool = false;

/*
 * If is_grant is true, adds the given privileges for the list of
 * grantees to the existing old_acl.  If is_grant is false, the
 * privileges for the given grantees are removed from old_acl.
 *
 * NB: the original old_acl is pfree'd.
 */
unsafe fn merge_acl_with_grant(
    old_acl: *mut Acl,
    is_grant: bool,
    grant_option: bool,
    behavior: DropBehavior,
    grantees: *mut List,
    privileges: AclMode,
    grantorId: Oid,
    ownerId: Oid,
) -> *mut Acl {
    let modechg: c_int;
    let mut j: *mut ListCell;
    let mut new_acl: *mut Acl;

    modechg = if is_grant { ACL_MODECHG_ADD } else { ACL_MODECHG_DEL };

    new_acl = old_acl;

    j = if grantees.is_null() { core::ptr::null_mut() } else { list_head(grantees) };
    while !j.is_null() {
        let mut aclitem: AclItem = core::mem::zeroed();
        let newer_acl: *mut Acl;

        aclitem.ai_grantee = (* ((*j).ptr_value as *mut Oid) );

        /*
         * Grant options can only be granted to individual roles, not PUBLIC.
         * The reason is that if a user would re-grant a privilege that he
         * held through PUBLIC, and later the user is removed, the situation
         * is impossible to clean up.
         */
        if is_grant && grant_option && aclitem.ai_grantee == ACL_ID_PUBLIC {
            ereport!(ERROR, errmsg!("grant options can only be granted to roles"));
        }

        aclitem.ai_grantor = grantorId;

        /*
         * The asymmetry in the conditions here comes from the spec.  In
         * GRANT, the grant_option flag signals WITH GRANT OPTION, which means
         * to grant both the basic privilege and its grant option. But in
         * REVOKE, plain revoke revokes both the basic privilege and its grant
         * option, while REVOKE GRANT OPTION revokes only the option.
         */
        ACLITEM_SET_PRIVS_GOPTIONS(
            &mut aclitem,
            if is_grant || !grant_option { privileges } else { ACL_NO_RIGHTS },
            if !is_grant || grant_option { privileges } else { ACL_NO_RIGHTS },
        );

        newer_acl = aclupdate(new_acl, &aclitem, modechg, ownerId, behavior);

        /* avoid memory leak when there are many grantees */
        pfree(new_acl as *mut c_void);
        new_acl = newer_acl;

        j = lnext(grantees, j);
    }

    new_acl
}

/*
 * Restrict the privileges to what we can actually grant, and emit
 * the standards-mandated warning and error messages.
 */
unsafe fn restrict_and_check_grant(
    is_grant: bool,
    avail_goptions: AclMode,
    all_privs: bool,
    privileges: AclMode,
    objectId: Oid,
    grantorId: Oid,
    objtype: ObjectType,
    objname: *const c_char,
    att_number: AttrNumber,
    colname: *const c_char,
) -> AclMode {
    let mut this_privileges: AclMode;
    let whole_mask: AclMode;

    match objtype {
        OBJECT_COLUMN =>       { whole_mask = ACL_ALL_RIGHTS_COLUMN; }
        OBJECT_TABLE =>        { whole_mask = ACL_ALL_RIGHTS_RELATION; }
        OBJECT_SEQUENCE =>     { whole_mask = ACL_ALL_RIGHTS_SEQUENCE; }
        OBJECT_DATABASE =>     { whole_mask = ACL_ALL_RIGHTS_DATABASE; }
        OBJECT_FUNCTION =>     { whole_mask = ACL_ALL_RIGHTS_FUNCTION; }
        OBJECT_LANGUAGE =>     { whole_mask = ACL_ALL_RIGHTS_LANGUAGE; }
        OBJECT_LARGEOBJECT =>  { whole_mask = ACL_ALL_RIGHTS_LARGEOBJECT; }
        OBJECT_SCHEMA =>       { whole_mask = ACL_ALL_RIGHTS_SCHEMA; }
        OBJECT_TABLESPACE =>   { whole_mask = ACL_ALL_RIGHTS_TABLESPACE; }
        OBJECT_FDW =>          { whole_mask = ACL_ALL_RIGHTS_FDW; }
        OBJECT_FOREIGN_SERVER => { whole_mask = ACL_ALL_RIGHTS_FOREIGN_SERVER; }
        OBJECT_EVENT_TRIGGER => {
            elog!(ERROR, "grantable rights not supported for event triggers");
            /* not reached, but keep compiler quiet */
            return ACL_NO_RIGHTS;
        }
        OBJECT_TYPE => { whole_mask = ACL_ALL_RIGHTS_TYPE; }
        OBJECT_PARAMETER_ACL => { whole_mask = ACL_ALL_RIGHTS_PARAMETER_ACL; }
        _ => {
            elog!(ERROR, "unrecognized object type: {}", objtype as c_int);
            /* not reached, but keep compiler quiet */
            return ACL_NO_RIGHTS;
        }
    }

    /*
     * If we found no grant options, consider whether to issue a hard error.
     * Per spec, having any privilege at all on the object will get you by
     * here.
     */
    if avail_goptions == ACL_NO_RIGHTS {
        if pg_aclmask(
            objtype, objectId, att_number, grantorId,
            whole_mask | ACL_GRANT_OPTION_FOR(whole_mask),
            ACLMASK_ANY,
        ) == ACL_NO_RIGHTS {
            if objtype == OBJECT_COLUMN && !colname.is_null() {
                aclcheck_error_col(ACLCHECK_NO_PRIV, objtype, objname, colname);
            } else {
                aclcheck_error(ACLCHECK_NO_PRIV, objtype, objname);
            }
        }
    }

    /*
     * Restrict the operation to what we can actually grant or revoke, and
     * issue a warning if appropriate.
     */
    this_privileges = privileges & ACL_OPTION_TO_PRIVS(avail_goptions);
    if is_grant {
        if this_privileges == 0 {
            if objtype == OBJECT_COLUMN && !colname.is_null() {
                ereport!(WARNING, errmsg!("no privileges were granted for column \"{}\" of relation \"{}\"",
                             cstr_to_str(colname), cstr_to_str(objname)));
            } else {
                ereport!(WARNING, errmsg!("no privileges were granted for \"{}\"", cstr_to_str(objname)));
            }
        } else if !all_privs && this_privileges != privileges {
            if objtype == OBJECT_COLUMN && !colname.is_null() {
                ereport!(WARNING, errmsg!("not all privileges were granted for column \"{}\" of relation \"{}\"",
                             cstr_to_str(colname), cstr_to_str(objname)));
            } else {
                ereport!(WARNING, errmsg!("not all privileges were granted for \"{}\"", cstr_to_str(objname)));
            }
        }
    } else {
        if this_privileges == 0 {
            if objtype == OBJECT_COLUMN && !colname.is_null() {
                ereport!(WARNING, errmsg!("no privileges could be revoked for column \"{}\" of relation \"{}\"",
                             cstr_to_str(colname), cstr_to_str(objname)));
            } else {
                ereport!(WARNING, errmsg!("no privileges could be revoked for \"{}\"", cstr_to_str(objname)));
            }
        } else if !all_privs && this_privileges != privileges {
            if objtype == OBJECT_COLUMN && !colname.is_null() {
                ereport!(WARNING, errmsg!("not all privileges could be revoked for column \"{}\" of relation \"{}\"",
                             cstr_to_str(colname), cstr_to_str(objname)));
            } else {
                ereport!(WARNING, errmsg!("not all privileges could be revoked for \"{}\"", cstr_to_str(objname)));
            }
        }
    }

    this_privileges
}

/// Helper: convert a raw C string pointer to a Rust &str for format macros.
/// Produces "<null>" for null pointers.
unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "<null>";
    }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("?")
}

/*
 * Called to execute the utility commands GRANT and REVOKE
 */
#[no_mangle]
pub unsafe fn ExecuteGrantStmt(stmt: *mut GrantStmt) {
    let mut istmt: InternalGrant = core::mem::zeroed();
    let mut cell: *mut ListCell;
    let errormsg: *const c_char;
    let all_privileges: AclMode;

    if !(*stmt).grantor.is_null() {
        let grantor: Oid;

        grantor = get_rolespec_oid((*stmt).grantor, false);

        /*
         * Currently, this clause is only for SQL compatibility, not very
         * interesting otherwise.
         */
        if grantor != GetUserId() {
            ereport!(ERROR, errmsg!("grantor must be current user"));
        }
    }

    /*
     * Turn the regular GrantStmt into the InternalGrant form.
     */
    istmt.is_grant = (*stmt).is_grant;
    istmt.objtype = (*stmt).objtype;

    /* Collect the OIDs of the target objects */
    match (*stmt).targtype {
        ACL_TARGET_OBJECT => {
            istmt.objects = objectNamesToOids((*stmt).objtype, (*stmt).objects, (*stmt).is_grant);
        }
        ACL_TARGET_ALL_IN_SCHEMA => {
            istmt.objects = objectsInSchemaToOids((*stmt).objtype, (*stmt).objects);
        }
        /* ACL_TARGET_DEFAULTS should not be seen here */
        _ => {
            elog!(ERROR, "unrecognized GrantStmt.targtype: {}", (*stmt).targtype as c_int);
        }
    }

    /* all_privs to be filled below */
    /* privileges to be filled below */
    istmt.col_privs = NIL;    /* may get filled below */
    istmt.grantees = NIL;     /* filled below */
    istmt.grant_option = (*stmt).grant_option;
    istmt.behavior = (*stmt).behavior;

    /*
     * Convert the RoleSpec list into an Oid list.
     */
    cell = if (*stmt).grantees.is_null() { core::ptr::null_mut() } else { list_head((*stmt).grantees) };
    while !cell.is_null() {
        let grantee = (*cell).ptr_value as *mut RoleSpec;
        let grantee_uid: Oid;

        match (*grantee).roletype {
            ROLESPEC_PUBLIC => {
                grantee_uid = ACL_ID_PUBLIC;
            }
            _ => {
                grantee_uid = get_rolespec_oid(grantee, false);
            }
        }
        istmt.grantees = lappend_oid(istmt.grantees, grantee_uid);
        cell = lnext((*stmt).grantees, cell);
    }

    /*
     * Convert stmt->privileges, a list of AccessPriv nodes, into an AclMode bitmask.
     */
    match (*stmt).objtype {
        OBJECT_TABLE => {
            /*
             * Because this might be a sequence, we test both relation and
             * sequence bits, and later do a more limited test when we know
             * the object type.
             */
            all_privileges = ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE;
            errormsg = b"invalid privilege type %s for relation\0".as_ptr() as *const c_char;
        }
        OBJECT_SEQUENCE => {
            all_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            errormsg = b"invalid privilege type %s for sequence\0".as_ptr() as *const c_char;
        }
        OBJECT_DATABASE => {
            all_privileges = ACL_ALL_RIGHTS_DATABASE;
            errormsg = b"invalid privilege type %s for database\0".as_ptr() as *const c_char;
        }
        OBJECT_DOMAIN => {
            all_privileges = ACL_ALL_RIGHTS_TYPE;
            errormsg = b"invalid privilege type %s for domain\0".as_ptr() as *const c_char;
        }
        OBJECT_FUNCTION => {
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = b"invalid privilege type %s for function\0".as_ptr() as *const c_char;
        }
        OBJECT_LANGUAGE => {
            all_privileges = ACL_ALL_RIGHTS_LANGUAGE;
            errormsg = b"invalid privilege type %s for language\0".as_ptr() as *const c_char;
        }
        OBJECT_LARGEOBJECT => {
            all_privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
            errormsg = b"invalid privilege type %s for large object\0".as_ptr() as *const c_char;
        }
        OBJECT_SCHEMA => {
            all_privileges = ACL_ALL_RIGHTS_SCHEMA;
            errormsg = b"invalid privilege type %s for schema\0".as_ptr() as *const c_char;
        }
        OBJECT_PROCEDURE => {
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = b"invalid privilege type %s for procedure\0".as_ptr() as *const c_char;
        }
        OBJECT_ROUTINE => {
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = b"invalid privilege type %s for routine\0".as_ptr() as *const c_char;
        }
        OBJECT_TABLESPACE => {
            all_privileges = ACL_ALL_RIGHTS_TABLESPACE;
            errormsg = b"invalid privilege type %s for tablespace\0".as_ptr() as *const c_char;
        }
        OBJECT_TYPE => {
            all_privileges = ACL_ALL_RIGHTS_TYPE;
            errormsg = b"invalid privilege type %s for type\0".as_ptr() as *const c_char;
        }
        OBJECT_FDW => {
            all_privileges = ACL_ALL_RIGHTS_FDW;
            errormsg = b"invalid privilege type %s for foreign-data wrapper\0".as_ptr() as *const c_char;
        }
        OBJECT_FOREIGN_SERVER => {
            all_privileges = ACL_ALL_RIGHTS_FOREIGN_SERVER;
            errormsg = b"invalid privilege type %s for foreign server\0".as_ptr() as *const c_char;
        }
        OBJECT_PARAMETER_ACL => {
            all_privileges = ACL_ALL_RIGHTS_PARAMETER_ACL;
            errormsg = b"invalid privilege type %s for parameter\0".as_ptr() as *const c_char;
        }
        _ => {
            elog!(ERROR, "unrecognized GrantStmt.objtype: {}", (*stmt).objtype as c_int);
            /* keep compiler quiet */
            all_privileges = ACL_NO_RIGHTS;
            errormsg = core::ptr::null();
        }
    }

    if (*stmt).privileges.is_null() {
        istmt.all_privs = true;
        /*
         * will be turned into ACL_ALL_RIGHTS_* by the internal routines
         * depending on the object type
         */
        istmt.privileges = ACL_NO_RIGHTS;
    } else {
        istmt.all_privs = false;
        istmt.privileges = ACL_NO_RIGHTS;

        cell = list_head((*stmt).privileges);
        while !cell.is_null() {
            let privnode = (*cell).ptr_value as *mut AccessPriv;
            let priv_: AclMode;

            /*
             * If it's a column-level specification, we just set it aside in
             * col_privs for the moment; but insist it's for a relation.
             */
            if !(*privnode).cols.is_null() {
                if (*stmt).objtype != OBJECT_TABLE {
                    ereport!(ERROR, errmsg!("column privileges are only valid for relations"));
                }
                istmt.col_privs = lappend(istmt.col_privs, privnode as *mut c_void);
                cell = lnext((*stmt).privileges, cell);
                continue;
            }

            if (*privnode).priv_name.is_null() {    /* parser mistake? */
                elog!(ERROR, "AccessPriv node must specify privilege or columns");
            }
            priv_ = string_to_privilege((*privnode).priv_name);

            if (priv_ as AclMode) & !(all_privileges as AclMode) != 0 {
                ereport!(ERROR, errmsg!("{}", cstr_to_str(errormsg)));
            }

            istmt.privileges |= priv_;
            cell = lnext((*stmt).privileges, cell);
        }
    }

    ExecGrantStmt_oids(&mut istmt);
}

// ACL target type constants come from GrantTargetType enum in parsenodes
use crate::nodes::parsenodes::{GrantTargetType, GrantTargetType::*};

/*
 * ExecGrantStmt_oids
 *
 * Internal entry point for granting and revoking privileges.
 */
unsafe fn ExecGrantStmt_oids(istmt: *mut InternalGrant) {
    match (*istmt).objtype {
        OBJECT_TABLE | OBJECT_SEQUENCE => {
            ExecGrant_Relation(istmt);
        }
        OBJECT_DATABASE => {
            ExecGrant_common(istmt, DatabaseRelationId, ACL_ALL_RIGHTS_DATABASE, None);
        }
        OBJECT_DOMAIN | OBJECT_TYPE => {
            ExecGrant_common(istmt, TypeRelationId, ACL_ALL_RIGHTS_TYPE, Some(ExecGrant_Type_check));
        }
        OBJECT_FDW => {
            ExecGrant_common(istmt, ForeignDataWrapperRelationId, ACL_ALL_RIGHTS_FDW, None);
        }
        OBJECT_FOREIGN_SERVER => {
            ExecGrant_common(istmt, ForeignServerRelationId, ACL_ALL_RIGHTS_FOREIGN_SERVER, None);
        }
        OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE => {
            ExecGrant_common(istmt, ProcedureRelationId, ACL_ALL_RIGHTS_FUNCTION, None);
        }
        OBJECT_LANGUAGE => {
            ExecGrant_common(istmt, LanguageRelationId, ACL_ALL_RIGHTS_LANGUAGE, Some(ExecGrant_Language_check));
        }
        OBJECT_LARGEOBJECT => {
            ExecGrant_Largeobject(istmt);
        }
        OBJECT_SCHEMA => {
            ExecGrant_common(istmt, NamespaceRelationId, ACL_ALL_RIGHTS_SCHEMA, None);
        }
        OBJECT_TABLESPACE => {
            ExecGrant_common(istmt, TableSpaceRelationId, ACL_ALL_RIGHTS_TABLESPACE, None);
        }
        OBJECT_PARAMETER_ACL => {
            ExecGrant_Parameter(istmt);
        }
        _ => {
            elog!(ERROR, "unrecognized GrantStmt.objtype: {}", (*istmt).objtype as c_int);
        }
    }

    /*
     * Pass the info to event triggers about the just-executed GRANT.
     */
    if EventTriggerSupportsObjectType((*istmt).objtype) {
        EventTriggerCollectGrant(istmt);
    }
}

/*
 * objectNamesToOids
 *
 * Turn a list of object names of a given type into an Oid list.
 */
unsafe fn objectNamesToOids(objtype: ObjectType, objnames: *mut List, is_grant: bool) -> *mut List {
    let mut objects: *mut List = NIL;
    let mut cell: *mut ListCell;
    let lockmode: LOCKMODE = AccessShareLock;

    Assert!(!objnames.is_null());

    match objtype {
        OBJECT_TABLE | OBJECT_SEQUENCE => {
            /*
             * Here, we don't use get_object_address().  It requires that the
             * specified object type match the actual type of the object, but
             * in GRANT/REVOKE, all table-like things are addressed as TABLE.
             */
            cell = list_head(objnames);
            while !cell.is_null() {
                let relvar = (*cell).ptr_value as *mut RangeVar;
                let relOid: Oid;
                relOid = RangeVarGetRelid(relvar, lockmode, false);
                objects = lappend_oid(objects, relOid);
                cell = lnext(objnames, cell);
            }
        }
        OBJECT_DOMAIN | OBJECT_TYPE => {
            /*
             * The parse representation of types and domains in privilege
             * targets is different from that expected by get_object_address()
             * (for parse conflict reasons), so we have to do a bit of
             * conversion here.
             */
            cell = list_head(objnames);
            while !cell.is_null() {
                let typname = (*cell).ptr_value as *mut List;
                let tn: *mut TypeName = makeTypeNameFromNameList(typname);
                let mut relation: *mut crate::utils::rel::RelationData = core::ptr::null_mut();
                let address: ObjectAddress;

                address = get_object_address(
                    objtype, tn as *mut Node, &mut relation, lockmode, false,
                );
                Assert!(relation.is_null());
                objects = lappend_oid(objects, address.objectId);
                cell = lnext(objnames, cell);
            }
        }
        OBJECT_PARAMETER_ACL => {
            /*
             * Parameters are handled completely differently.
             */
            cell = list_head(objnames);
            while !cell.is_null() {
                /*
                 * In this code we represent a GUC by the OID of its entry in
                 * pg_parameter_acl, which we have to manufacture here if it
                 * doesn't exist yet.
                 */
                let parameter: *const c_char = strVal((*cell).ptr_value as *mut c_void);
                let mut parameterId: Oid = ParameterAclLookup(parameter, true);

                if !OidIsValid(parameterId) && is_grant {
                    parameterId = ParameterAclCreate(parameter);

                    /*
                     * Prevent error when processing duplicate objects, and
                     * make this new entry visible so that ExecGrant_Parameter
                     * can update it.
                     */
                    CommandCounterIncrement();
                }
                if OidIsValid(parameterId) {
                    objects = lappend_oid(objects, parameterId);
                }
                cell = lnext(objnames, cell);
            }
        }
        _ => {
            /*
             * For most object types, we use get_object_address() directly.
             */
            cell = list_head(objnames);
            while !cell.is_null() {
                let mut relation: *mut crate::utils::rel::RelationData = core::ptr::null_mut();
                let address: ObjectAddress;
                address = get_object_address(
                    objtype, (*cell).ptr_value as *mut Node, &mut relation, lockmode, false,
                );
                objects = lappend_oid(objects, address.objectId);
                cell = lnext(objnames, cell);
            }
        }
    }

    objects
}

/// OidIsValid helper
#[inline]
fn OidIsValid(oid: Oid) -> bool { oid != InvalidOid }

/*
 * objectsInSchemaToOids
 *
 * Find all objects of a given type in specified schemas, and make a list
 * of their Oids.
 */
unsafe fn objectsInSchemaToOids(objtype: ObjectType, nspnames: *mut List) -> *mut List {
    let mut objects: *mut List = NIL;
    let mut cell: *mut ListCell;

    cell = if nspnames.is_null() { core::ptr::null_mut() } else { list_head(nspnames) };
    while !cell.is_null() {
        let nspname: *const c_char = strVal((*cell).ptr_value as *mut c_void);
        let namespaceId: Oid;
        let objs: *mut List;

        namespaceId = LookupExplicitNamespace(nspname, false);

        match objtype {
            OBJECT_TABLE => {
                let mut o: *mut List;
                o = getRelationsInNamespace(namespaceId, RELKIND_RELATION);
                objects = list_concat(objects, o);
                o = getRelationsInNamespace(namespaceId, RELKIND_VIEW);
                objects = list_concat(objects, o);
                o = getRelationsInNamespace(namespaceId, RELKIND_MATVIEW);
                objects = list_concat(objects, o);
                o = getRelationsInNamespace(namespaceId, RELKIND_FOREIGN_TABLE);
                objects = list_concat(objects, o);
                o = getRelationsInNamespace(namespaceId, RELKIND_PARTITIONED_TABLE);
                objects = list_concat(objects, o);
            }
            OBJECT_SEQUENCE => {
                let o = getRelationsInNamespace(namespaceId, RELKIND_SEQUENCE);
                objects = list_concat(objects, o);
            }
            OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE => {
                let mut key = [core::mem::zeroed::<ScanKeyData>(); 2];
                let mut keycount: c_int;
                let rel: Relation;
                let scan: crate::access::relscan::TableScanDesc;
                let mut tuple: HeapTuple;

                keycount = 0;
                ScanKeyInit(
                    &mut key[keycount as usize],
                    Anum_pg_proc_pronamespace,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(namespaceId),
                );
                keycount += 1;

                if objtype == OBJECT_FUNCTION {
                    /* includes aggregates and window functions */
                    ScanKeyInit(
                        &mut key[keycount as usize],
                        Anum_pg_proc_prokind,
                        BTEqualStrategyNumber, F_CHARNE,
                        CharGetDatum(PROKIND_PROCEDURE),
                    );
                    keycount += 1;
                } else if objtype == OBJECT_PROCEDURE {
                    ScanKeyInit(
                        &mut key[keycount as usize],
                        Anum_pg_proc_prokind,
                        BTEqualStrategyNumber, F_CHAREQ,
                        CharGetDatum(PROKIND_PROCEDURE),
                    );
                    keycount += 1;
                }

                rel = table_open(ProcedureRelationId, AccessShareLock);
                scan = table_beginscan_catalog(rel, keycount, key.as_mut_ptr());

                loop {
                    tuple = heap_getnext(scan, ForwardScanDirection);
                    if tuple.is_null() { break; }
                    let oid: Oid = (*(GETSTRUCT(tuple) as Form_pg_proc)).oid;
                    objects = lappend_oid(objects, oid);
                }

                table_endscan(scan);
                table_close(rel, AccessShareLock);
            }
            _ => {
                /* should not happen */
                elog!(ERROR, "unrecognized GrantStmt.objtype: {}", objtype as c_int);
            }
        }

        cell = lnext(nspnames, cell);
    }

    objects
}

/*
 * getRelationsInNamespace
 *
 * Return Oid list of relations in given namespace filtered by relation kind
 */
unsafe fn getRelationsInNamespace(namespaceId: Oid, relkind: c_char) -> *mut List {
    let mut relations: *mut List = NIL;
    let mut key = [core::mem::zeroed::<ScanKeyData>(); 2];
    let rel: Relation;
    let scan: crate::access::relscan::TableScanDesc;
    let mut tuple: HeapTuple;

    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_relnamespace,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_class_relkind,
        BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(relkind),
    );

    rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 2, key.as_mut_ptr());

    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() { break; }
        let oid: Oid = (*(GETSTRUCT(tuple) as Form_pg_class)).oid;
        relations = lappend_oid(relations, oid);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    relations
}


/*
 * ALTER DEFAULT PRIVILEGES statement
 */
#[no_mangle]
pub unsafe fn ExecAlterDefaultPrivilegesStmt(
    pstate: *mut ParseState,
    stmt: *mut AlterDefaultPrivilegesStmt,
) {
    let action: *mut GrantStmt = (*stmt).action;
    let mut iacls: InternalDefaultACL = core::mem::zeroed();
    let mut cell: *mut ListCell;
    let mut rolespecs: *mut List = NIL;
    let mut nspnames: *mut List = NIL;
    let mut drolespecs: *mut DefElem = core::ptr::null_mut();
    let mut dnspnames: *mut DefElem = core::ptr::null_mut();
    let all_privileges: AclMode;
    let errormsg: *const c_char;

    /* Deconstruct the "options" part of the statement */
    cell = if (*stmt).options.is_null() { core::ptr::null_mut() } else { list_head((*stmt).options) };
    while !cell.is_null() {
        let defel = (*cell).ptr_value as *mut DefElem;

        let defname_s = core::ffi::CStr::from_ptr((*defel).defname).to_str().unwrap_or("");
        if defname_s == "schemas" {
            if !dnspnames.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dnspnames = defel;
        } else if defname_s == "roles" {
            if !drolespecs.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            drolespecs = defel;
        } else {
            elog!(ERROR, "option \"{}\" not recognized", defname_s);
        }
        cell = lnext((*stmt).options, cell);
    }

    if !dnspnames.is_null() {
        nspnames = (*dnspnames).arg as *mut List;
    }
    if !drolespecs.is_null() {
        rolespecs = (*drolespecs).arg as *mut List;
    }

    /* Prepare the InternalDefaultACL representation of the statement */
    /* roleid to be filled below */
    /* nspid to be filled in SetDefaultACLsInSchemas */
    iacls.is_grant = (*action).is_grant;
    iacls.objtype = (*action).objtype;
    /* all_privs to be filled below */
    /* privileges to be filled below */
    iacls.grantees = NIL;       /* filled below */
    iacls.grant_option = (*action).grant_option;
    iacls.behavior = (*action).behavior;

    /*
     * Convert the RoleSpec list into an Oid list.
     */
    cell = if (*action).grantees.is_null() { core::ptr::null_mut() } else { list_head((*action).grantees) };
    while !cell.is_null() {
        let grantee = (*cell).ptr_value as *mut RoleSpec;
        let grantee_uid: Oid;
        match (*grantee).roletype {
            ROLESPEC_PUBLIC => { grantee_uid = ACL_ID_PUBLIC; }
            _ => { grantee_uid = get_rolespec_oid(grantee, false); }
        }
        iacls.grantees = lappend_oid(iacls.grantees, grantee_uid);
        cell = lnext((*action).grantees, cell);
    }

    /*
     * Convert action->privileges, a list of privilege strings, into an
     * AclMode bitmask.
     */
    match (*action).objtype {
        OBJECT_TABLE => {
            all_privileges = ACL_ALL_RIGHTS_RELATION;
            errormsg = b"invalid privilege type %s for relation\0".as_ptr() as *const c_char;
        }
        OBJECT_SEQUENCE => {
            all_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            errormsg = b"invalid privilege type %s for sequence\0".as_ptr() as *const c_char;
        }
        OBJECT_FUNCTION => {
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = b"invalid privilege type %s for function\0".as_ptr() as *const c_char;
        }
        OBJECT_PROCEDURE => {
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = b"invalid privilege type %s for procedure\0".as_ptr() as *const c_char;
        }
        OBJECT_ROUTINE => {
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = b"invalid privilege type %s for routine\0".as_ptr() as *const c_char;
        }
        OBJECT_TYPE => {
            all_privileges = ACL_ALL_RIGHTS_TYPE;
            errormsg = b"invalid privilege type %s for type\0".as_ptr() as *const c_char;
        }
        OBJECT_SCHEMA => {
            all_privileges = ACL_ALL_RIGHTS_SCHEMA;
            errormsg = b"invalid privilege type %s for schema\0".as_ptr() as *const c_char;
        }
        OBJECT_LARGEOBJECT => {
            all_privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
            errormsg = b"invalid privilege type %s for large object\0".as_ptr() as *const c_char;
        }
        _ => {
            elog!(ERROR, "unrecognized GrantStmt.objtype: {}", (*action).objtype as c_int);
            /* keep compiler quiet */
            all_privileges = ACL_NO_RIGHTS;
            errormsg = core::ptr::null();
        }
    }

    if (*action).privileges.is_null() {
        iacls.all_privs = true;
        /*
         * will be turned into ACL_ALL_RIGHTS_* by the internal routines
         * depending on the object type
         */
        iacls.privileges = ACL_NO_RIGHTS;
    } else {
        iacls.all_privs = false;
        iacls.privileges = ACL_NO_RIGHTS;

        cell = list_head((*action).privileges);
        while !cell.is_null() {
            let privnode = (*cell).ptr_value as *mut AccessPriv;
            let priv_: AclMode;

            if !(*privnode).cols.is_null() {
                ereport!(ERROR, errmsg!("default privileges cannot be set for columns"));
            }

            if (*privnode).priv_name.is_null() {    /* parser mistake? */
                elog!(ERROR, "AccessPriv node must specify privilege");
            }
            priv_ = string_to_privilege((*privnode).priv_name);

            if (priv_ as AclMode) & !(all_privileges as AclMode) != 0 {
                ereport!(ERROR, errmsg!("{}", cstr_to_str(errormsg)));
            }

            iacls.privileges |= priv_;
            cell = lnext((*action).privileges, cell);
        }
    }

    if rolespecs.is_null() {
        /* Set permissions for myself */
        iacls.roleid = GetUserId();
        SetDefaultACLsInSchemas(&mut iacls, nspnames);
    } else {
        /* Look up the role OIDs and do permissions checks */
        let mut rolecell: *mut ListCell = list_head(rolespecs);
        while !rolecell.is_null() {
            let rolespec = (*rolecell).ptr_value as *mut RoleSpec;
            iacls.roleid = get_rolespec_oid(rolespec, false);

            if !has_privs_of_role(GetUserId(), iacls.roleid) {
                ereport!(ERROR, errmsg!("permission denied to change default privileges"));
            }

            SetDefaultACLsInSchemas(&mut iacls, nspnames);
            rolecell = lnext(rolespecs, rolecell);
        }
    }
}

/*
 * Process ALTER DEFAULT PRIVILEGES for a list of target schemas
 *
 * All fields of *iacls except nspid were filled already
 */
unsafe fn SetDefaultACLsInSchemas(iacls: *mut InternalDefaultACL, nspnames: *mut List) {
    if nspnames.is_null() {
        /* Set database-wide permissions if no schema was specified */
        (*iacls).nspid = InvalidOid;
        SetDefaultACL(iacls);
    } else {
        /* Look up the schema OIDs and set permissions for each one */
        let mut nspcell: *mut ListCell = list_head(nspnames);
        while !nspcell.is_null() {
            let nspname: *const c_char = strVal((*nspcell).ptr_value as *mut c_void);
            (*iacls).nspid = get_namespace_oid(nspname, false);

            /*
             * We used to insist that the target role have CREATE privileges
             * on the schema, since without that it wouldn't be able to create
             * an object for which these default privileges would apply.
             * However, this check proved to be more confusing than helpful,
             * and it also caused certain database states to not be
             * dumpable/restorable, since revoking CREATE doesn't cause
             * default privileges for the schema to go away.  So now, we just
             * allow the ALTER; if the user lacks CREATE he'll find out when
             * he tries to create an object.
             */
            SetDefaultACL(iacls);
            nspcell = lnext(nspnames, nspcell);
        }
    }
}


/*
 * Create or update a pg_default_acl entry
 */
unsafe fn SetDefaultACL(iacls: *mut InternalDefaultACL) {
    let mut this_privileges: AclMode = (*iacls).privileges;
    let objtype: c_char;
    let rel: Relation;
    let mut tuple: HeapTuple;
    let isNew: bool;
    let def_acl: *mut Acl;
    let old_acl: *mut Acl;
    let new_acl: *mut Acl;
    let newtuple: HeapTuple;
    let noldmembers: c_int;
    let mut nnewmembers: c_int = 0;
    let mut oldmembers: *mut Oid = core::ptr::null_mut();
    let mut newmembers: *mut Oid = core::ptr::null_mut();

    rel = table_open(DefaultAclRelationId, RowExclusiveLock);

    /*
     * The default for a global entry is the hard-wired default ACL for the
     * particular object type.  The default for non-global entries is an empty
     * ACL.
     */
    if !OidIsValid((*iacls).nspid) {
        def_acl = acldefault((*iacls).objtype, (*iacls).roleid);
    } else {
        def_acl = make_empty_acl();
    }

    /*
     * Convert ACL object type to pg_default_acl object type and handle
     * all_privs option
     */
    match (*iacls).objtype {
        OBJECT_TABLE => {
            objtype = DEFACLOBJ_RELATION;
            if (*iacls).all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_RELATION;
            }
        }
        OBJECT_SEQUENCE => {
            objtype = DEFACLOBJ_SEQUENCE;
            if (*iacls).all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            }
        }
        OBJECT_FUNCTION => {
            objtype = DEFACLOBJ_FUNCTION;
            if (*iacls).all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_FUNCTION;
            }
        }
        OBJECT_TYPE => {
            objtype = DEFACLOBJ_TYPE;
            if (*iacls).all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_TYPE;
            }
        }
        OBJECT_SCHEMA => {
            if OidIsValid((*iacls).nspid) {
                ereport!(ERROR, errmsg!("cannot use IN SCHEMA clause when using {}", "GRANT/REVOKE ON SCHEMAS"));
            }
            objtype = DEFACLOBJ_NAMESPACE;
            if (*iacls).all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_SCHEMA;
            }
        }
        OBJECT_LARGEOBJECT => {
            if OidIsValid((*iacls).nspid) {
                ereport!(ERROR, errmsg!("cannot use IN SCHEMA clause when using {}", "GRANT/REVOKE ON LARGE OBJECTS"));
            }
            objtype = DEFACLOBJ_LARGEOBJECT;
            if (*iacls).all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
            }
        }
        _ => {
            elog!(ERROR, "unrecognized object type: {}", (*iacls).objtype as c_int);
            objtype = 0;    /* keep compiler quiet */
        }
    }

    /* Search for existing row for this object type in catalog */
    tuple = SearchSysCache3(
        DEFACLROLENSPOBJ,
        ObjectIdGetDatum((*iacls).roleid),
        ObjectIdGetDatum((*iacls).nspid),
        CharGetDatum(objtype),
    );

    let (old_acl_val, noldmembers_val) = if HeapTupleIsValid(tuple) {
        let mut aclDatum: Datum;
        let mut isNull: bool = false;

        aclDatum = SysCacheGetAttr(
            DEFACLROLENSPOBJ, tuple,
            Anum_pg_default_acl_defaclacl,
            &mut isNull,
        );
        isNew = false;
        if !isNull {
            let oa = DatumGetAclPCopy(aclDatum);
            let nm = aclmembers(oa, &mut oldmembers);
            (oa, nm)
        } else {
            (core::ptr::null_mut(), 0)    /* this case shouldn't happen, probably */
        }
    } else {
        isNew = true;
        (core::ptr::null_mut(), 0)
    };
    let old_acl_resolved: *mut Acl;
    let noldmembers_resolved: c_int;
    if old_acl_val.is_null() {
        /* If no or null entry, start with the default ACL value */
        old_acl_resolved = aclcopy(def_acl);
        /* There are no old member roles according to the catalogs */
        noldmembers_resolved = 0;
        oldmembers = core::ptr::null_mut();
    } else {
        old_acl_resolved = old_acl_val;
        noldmembers_resolved = noldmembers_val;
    }

    /*
     * Generate new ACL.  Grantor of rights is always the same as the target
     * role.
     */
    new_acl = merge_acl_with_grant(
        old_acl_resolved,
        (*iacls).is_grant,
        (*iacls).grant_option,
        (*iacls).behavior,
        (*iacls).grantees,
        this_privileges,
        (*iacls).roleid,
        (*iacls).roleid,
    );

    /*
     * If the result is the same as the default value, we do not need an
     * explicit pg_default_acl entry, and should in fact remove the entry if
     * it exists.
     */
    aclitemsort(new_acl);
    aclitemsort(def_acl);
    if aclequal(new_acl, def_acl) {
        /* delete old entry, if indeed there is one */
        if !isNew {
            let mut myself: ObjectAddress = core::mem::zeroed();
            /*
             * The dependency machinery will take care of removing all
             * associated dependency entries.  We use DROP_RESTRICT since
             * there shouldn't be anything depending on this entry.
             */
            myself.classId = DefaultAclRelationId;
            myself.objectId = (*(GETSTRUCT(tuple) as Form_pg_default_acl)).oid;
            myself.objectSubId = 0;
            performDeletion(&myself, DROP_RESTRICT, 0);
        }
    } else {
        let mut values = [0 as Datum; Natts_pg_default_acl];
        let mut nulls = [false; Natts_pg_default_acl];
        let mut replaces = [false; Natts_pg_default_acl];
        let defAclOid: Oid;

        if isNew {
            /* insert new entry */
            defAclOid = GetNewOidWithIndex(
                rel, DefaultAclOidIndexId, Anum_pg_default_acl_oid,
            );
            values[Anum_pg_default_acl_oid as usize - 1] = ObjectIdGetDatum(defAclOid);
            values[Anum_pg_default_acl_defaclrole as usize - 1] = ObjectIdGetDatum((*iacls).roleid);
            values[Anum_pg_default_acl_defaclnamespace as usize - 1] = ObjectIdGetDatum((*iacls).nspid);
            values[Anum_pg_default_acl_defaclobjtype as usize - 1] = CharGetDatum(objtype);
            values[Anum_pg_default_acl_defaclacl as usize - 1] = PointerGetDatum(new_acl as *const c_void);

            newtuple = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());
            CatalogTupleInsert(rel, newtuple);
        } else {
            defAclOid = (*(GETSTRUCT(tuple) as Form_pg_default_acl)).oid;

            /* update existing entry */
            values[Anum_pg_default_acl_defaclacl as usize - 1] = PointerGetDatum(new_acl as *const c_void);
            replaces[Anum_pg_default_acl_defaclacl as usize - 1] = true;

            newtuple = heap_modify_tuple(
                tuple, RelationGetDescr(rel),
                values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
            );
            CatalogTupleUpdate(rel, &mut (*newtuple).t_self, newtuple);
        }

        /* these dependencies don't change in an update */
        if isNew {
            /* dependency on role */
            recordDependencyOnOwner(DefaultAclRelationId, defAclOid, (*iacls).roleid);

            /* dependency on namespace */
            if OidIsValid((*iacls).nspid) {
                let mut myself: ObjectAddress = core::mem::zeroed();
                let mut referenced: ObjectAddress = core::mem::zeroed();

                myself.classId = DefaultAclRelationId;
                myself.objectId = defAclOid;
                myself.objectSubId = 0;

                referenced.classId = NamespaceRelationId;
                referenced.objectId = (*iacls).nspid;
                referenced.objectSubId = 0;

                recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
            }
        }

        /*
         * Update the shared dependency ACL info
         */
        nnewmembers = aclmembers(new_acl, &mut newmembers);

        updateAclDependencies(
            DefaultAclRelationId,
            defAclOid, 0,
            (*iacls).roleid,
            noldmembers_resolved, oldmembers,
            nnewmembers, newmembers,
        );

        if isNew {
            InvokeObjectPostCreateHook(DefaultAclRelationId, defAclOid, 0);
        } else {
            InvokeObjectPostAlterHook(DefaultAclRelationId, defAclOid, 0);
        }
    }

    if HeapTupleIsValid(tuple) {
        ReleaseSysCache(tuple);
    }

    table_close(rel, RowExclusiveLock);

    /* prevent error when processing duplicate objects */
    CommandCounterIncrement();
}


/*
 * RemoveRoleFromObjectACL
 *
 * Used by shdepDropOwned to remove mentions of a role in ACLs.
 */
#[no_mangle]
pub unsafe fn RemoveRoleFromObjectACL(roleid: Oid, classid: Oid, objid: Oid) {
    if classid == DefaultAclRelationId {
        let mut iacls: InternalDefaultACL = core::mem::zeroed();
        let rel: Relation;
        let mut skey = [core::mem::zeroed::<ScanKeyData>(); 1];
        let scan: SysScanDesc;
        let tuple: HeapTuple;

        /* first fetch info needed by SetDefaultACL */
        rel = table_open(DefaultAclRelationId, AccessShareLock);

        ScanKeyInit(
            &mut skey[0],
            Anum_pg_default_acl_oid,
            BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(objid),
        );

        scan = systable_beginscan(rel, DefaultAclOidIndexId, true, core::ptr::null_mut(), 1, skey.as_mut_ptr());

        tuple = systable_getnext(scan);

        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "could not find tuple for default ACL {}", objid);
        }

        let pg_default_acl_tuple = GETSTRUCT(tuple) as Form_pg_default_acl;

        iacls.roleid = (*pg_default_acl_tuple).defaclrole;
        iacls.nspid  = (*pg_default_acl_tuple).defaclnamespace;

        match (*pg_default_acl_tuple).defaclobjtype {
            DEFACLOBJ_RELATION  => { iacls.objtype = OBJECT_TABLE; }
            DEFACLOBJ_SEQUENCE  => { iacls.objtype = OBJECT_SEQUENCE; }
            DEFACLOBJ_FUNCTION  => { iacls.objtype = OBJECT_FUNCTION; }
            DEFACLOBJ_TYPE      => { iacls.objtype = OBJECT_TYPE; }
            DEFACLOBJ_NAMESPACE => { iacls.objtype = OBJECT_SCHEMA; }
            DEFACLOBJ_LARGEOBJECT => { iacls.objtype = OBJECT_LARGEOBJECT; }
            _ => {
                /* Shouldn't get here */
                elog!(ERROR, "unexpected default ACL type: {}", (*pg_default_acl_tuple).defaclobjtype as c_int);
            }
        }

        systable_endscan(scan);
        table_close(rel, AccessShareLock);

        iacls.is_grant = false;
        iacls.all_privs = true;
        iacls.privileges = ACL_NO_RIGHTS;
        iacls.grantees = crate::list_make1_oid!(roleid);
        iacls.grant_option = false;
        iacls.behavior = DROP_CASCADE;

        /* Do it */
        SetDefaultACL(&mut iacls);
    } else {
        let mut istmt: InternalGrant = core::mem::zeroed();

        match classid {
            RelationRelationId => {
                /* it's OK to use TABLE for a sequence */
                istmt.objtype = OBJECT_TABLE;
            }
            DatabaseRelationId => { istmt.objtype = OBJECT_DATABASE; }
            TypeRelationId     => { istmt.objtype = OBJECT_TYPE; }
            ProcedureRelationId => { istmt.objtype = OBJECT_ROUTINE; }
            LanguageRelationId => { istmt.objtype = OBJECT_LANGUAGE; }
            LargeObjectRelationId => { istmt.objtype = OBJECT_LARGEOBJECT; }
            NamespaceRelationId => { istmt.objtype = OBJECT_SCHEMA; }
            TableSpaceRelationId => { istmt.objtype = OBJECT_TABLESPACE; }
            ForeignServerRelationId => { istmt.objtype = OBJECT_FOREIGN_SERVER; }
            ForeignDataWrapperRelationId => { istmt.objtype = OBJECT_FDW; }
            ParameterAclRelationId => { istmt.objtype = OBJECT_PARAMETER_ACL; }
            _ => {
                elog!(ERROR, "unexpected object class {}", classid);
            }
        }
        istmt.is_grant = false;
        istmt.objects = crate::list_make1_oid!(objid);
        istmt.all_privs = true;
        istmt.privileges = ACL_NO_RIGHTS;
        istmt.col_privs = NIL;
        istmt.grantees = crate::list_make1_oid!(roleid);
        istmt.grant_option = false;
        istmt.behavior = DROP_CASCADE;

        ExecGrantStmt_oids(&mut istmt);
    }
}


/*
 * expand_col_privileges
 *
 * OR the specified privilege(s) into per-column array entries for each
 * specified attribute.
 */
unsafe fn expand_col_privileges(
    colnames: *mut List,
    table_oid: Oid,
    this_privileges: AclMode,
    col_privileges: *mut AclMode,
    num_col_privileges: c_int,
) {
    let mut cell: *mut ListCell = if colnames.is_null() { core::ptr::null_mut() } else { list_head(colnames) };
    while !cell.is_null() {
        let colname: *const c_char = strVal((*cell).ptr_value as *mut c_void);
        let mut attnum: AttrNumber;

        attnum = get_attnum(table_oid, colname);
        if attnum == InvalidAttrNumber {
            ereport!(ERROR, errmsg!("column \"{}\" of relation \"{}\" does not exist", cstr_to_str(colname), cstr_to_str(get_rel_name(table_oid))));
        }
        attnum -= crate::access::sysattr::FirstLowInvalidHeapAttributeNumber as AttrNumber;
        if attnum <= 0 || attnum >= num_col_privileges as AttrNumber {
            elog!(ERROR, "column number out of range");   /* safety check */
        }
        *col_privileges.add(attnum as usize) |= this_privileges;
        cell = lnext(colnames, cell);
    }
}

/*
 * expand_all_col_privileges
 *
 * OR the specified privilege(s) into per-column array entries for each valid
 * attribute of a relation.
 */
unsafe fn expand_all_col_privileges(
    table_oid: Oid,
    classForm: Form_pg_class,
    this_privileges: AclMode,
    col_privileges: *mut AclMode,
    num_col_privileges: c_int,
) {
    let mut curr_att: AttrNumber;

    Assert!(
        (*classForm).relnatts as c_int
            - (crate::access::sysattr::FirstLowInvalidHeapAttributeNumber as c_int) < num_col_privileges
    );
    curr_att = crate::access::sysattr::FirstLowInvalidHeapAttributeNumber as AttrNumber + 1;
    while curr_att <= (*classForm).relnatts as AttrNumber {
        let attTuple: HeapTuple;
        let isdropped: bool;

        if curr_att == InvalidAttrNumber as AttrNumber {
            curr_att += 1;
            continue;
        }

        /* Views don't have any system columns at all */
        if (*classForm).relkind == RELKIND_VIEW && curr_att < 0 {
            curr_att += 1;
            continue;
        }

        attTuple = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(table_oid),
            Int16GetDatum(curr_att),
        );
        if !HeapTupleIsValid(attTuple) {
            elog!(ERROR, "cache lookup failed for attribute {} of relation {}",
                  curr_att, table_oid);
        }

        isdropped = (*(GETSTRUCT(attTuple) as Form_pg_attribute)).attisdropped;
        ReleaseSysCache(attTuple);

        /* ignore dropped columns */
        if isdropped {
            curr_att += 1;
            continue;
        }

        let idx = (curr_att - crate::access::sysattr::FirstLowInvalidHeapAttributeNumber as AttrNumber) as usize;
        *col_privileges.add(idx) |= this_privileges;
        curr_att += 1;
    }
}


/*
 *    This processes attributes, but expects to be called from
 *    ExecGrant_Relation, not directly from ExecuteGrantStmt.
 */
unsafe fn ExecGrant_Attribute(
    istmt: *mut InternalGrant,
    relOid: Oid,
    relname: *const c_char,
    attnum: AttrNumber,
    ownerId: Oid,
    col_privileges: AclMode,
    attRelation: Relation,
    old_rel_acl: *const Acl,
) {
    use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
    let attr_tuple: HeapTuple;
    let pg_attribute_tuple: Form_pg_attribute;
    let old_acl: *mut Acl;
    let mut new_acl: *mut Acl;
    let merged_acl: *mut Acl;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let mut grantorId: Oid = InvalidOid;
    let mut avail_goptions: AclMode = ACL_NO_RIGHTS;
    let need_update: bool;
    let newtuple: HeapTuple;
    let mut values = [0 as Datum; Natts_pg_attribute];
    let mut nulls = [false; Natts_pg_attribute];
    let mut replaces = [false; Natts_pg_attribute];
    let noldmembers: c_int;
    let nnewmembers: c_int;
    let mut oldmembers: *mut Oid = core::ptr::null_mut();
    let mut newmembers: *mut Oid = core::ptr::null_mut();
    let mut col_privileges_mut = col_privileges;

    attr_tuple = SearchSysCache2(
        ATTNUM,
        ObjectIdGetDatum(relOid),
        Int16GetDatum(attnum),
    );
    if !HeapTupleIsValid(attr_tuple) {
        elog!(ERROR, "cache lookup failed for attribute {} of relation {}", attnum, relOid);
    }
    pg_attribute_tuple = GETSTRUCT(attr_tuple) as Form_pg_attribute;

    /*
     * Get working copy of existing ACL. If there's no ACL, substitute the
     * proper default.
     */
    aclDatum = SysCacheGetAttr(ATTNUM, attr_tuple, Anum_pg_attribute_attacl, &mut isNull);
    if isNull {
        old_acl = acldefault(OBJECT_COLUMN, ownerId);
        /* There are no old member roles according to the catalogs */
        noldmembers = 0;
        oldmembers = core::ptr::null_mut();
    } else {
        old_acl = DatumGetAclPCopy(aclDatum);
        /* Get the roles mentioned in the existing ACL */
        noldmembers = aclmembers(old_acl, &mut oldmembers);
    }

    /*
     * In select_best_grantor we should consider existing table-level ACL bits
     * as well as the per-column ACL.
     */
    merged_acl = aclconcat(old_rel_acl, old_acl);

    /* Determine ID to do the grant as, and available grant options */
    select_best_grantor(
        GetUserId(), col_privileges_mut,
        merged_acl, ownerId,
        &mut grantorId, &mut avail_goptions,
    );

    pfree(merged_acl as *mut c_void);

    /*
     * Restrict the privileges to what we can actually grant, and emit the
     * standards-mandated warning and error messages.
     */
    col_privileges_mut = restrict_and_check_grant(
        (*istmt).is_grant, avail_goptions,
        col_privileges_mut == ACL_ALL_RIGHTS_COLUMN,
        col_privileges_mut,
        relOid, grantorId, OBJECT_COLUMN,
        relname, attnum,
        NameStr((*pg_attribute_tuple).attname),
    );

    /*
     * Generate new ACL.
     */
    new_acl = merge_acl_with_grant(
        old_acl, (*istmt).is_grant,
        (*istmt).grant_option,
        (*istmt).behavior, (*istmt).grantees,
        col_privileges_mut, grantorId,
        ownerId,
    );

    /*
     * We need the members of both old and new ACLs so we can correct the
     * shared dependency information.
     */
    nnewmembers = aclmembers(new_acl, &mut newmembers);

    /* finished building new ACL value, now insert it */

    /*
     * If the updated ACL is empty, we can set attacl to null, and maybe even
     * avoid an update of the pg_attribute row.
     */
    if ACL_NUM(new_acl) > 0 {
        values[Anum_pg_attribute_attacl as usize - 1] = PointerGetDatum(new_acl as *const c_void);
        need_update = true;
    } else {
        nulls[Anum_pg_attribute_attacl as usize - 1] = true;
        need_update = !isNull;
    }
    replaces[Anum_pg_attribute_attacl as usize - 1] = true;

    if need_update {
        newtuple = heap_modify_tuple(
            attr_tuple, RelationGetDescr(attRelation),
            values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
        );

        CatalogTupleUpdate(attRelation, &mut (*newtuple).t_self, newtuple);

        /* Update initial privileges for extensions */
        recordExtensionInitPriv(
            relOid, RelationRelationId, attnum as c_int,
            if ACL_NUM(new_acl) > 0 { new_acl } else { core::ptr::null_mut() },
        );

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            RelationRelationId, relOid, attnum as c_int,
            ownerId,
            noldmembers, oldmembers,
            nnewmembers, newmembers,
        );
    }

    pfree(new_acl as *mut c_void);

    ReleaseSysCache(attr_tuple);
}

/*
 *    This processes both sequences and non-sequences.
 */
unsafe fn ExecGrant_Relation(istmt: *mut InternalGrant) {
    use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
    let relation: Relation;
    let attRelation: Relation;
    let mut cell: *mut ListCell;

    relation    = table_open(RelationRelationId, RowExclusiveLock);
    attRelation = table_open(AttributeRelationId, RowExclusiveLock);

    cell = if (*istmt).objects.is_null() { core::ptr::null_mut() } else { list_head((*istmt).objects) };
    while !cell.is_null() {
        let relOid: Oid = *((*cell).ptr_value as *mut Oid);
        let mut aclDatum: Datum;
        let pg_class_tuple: Form_pg_class;
        let mut isNull: bool = false;
        let mut this_privileges: AclMode;
        let mut col_privileges: *mut AclMode;
        let num_col_privileges: c_int;
        let mut have_col_privileges: bool;
        let old_acl: *mut Acl;
        let old_rel_acl: *mut Acl;
        let mut noldmembers: c_int;
        let mut oldmembers: *mut Oid = core::ptr::null_mut();
        let ownerId: Oid;
        let tuple: HeapTuple;
        let mut cell_colprivs: *mut ListCell;

        tuple = SearchSysCacheLocked1(RELOID, ObjectIdGetDatum(relOid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", relOid);
        }
        pg_class_tuple = GETSTRUCT(tuple) as Form_pg_class;

        /* Not sensible to grant on an index */
        if (*pg_class_tuple).relkind == RELKIND_INDEX
            || (*pg_class_tuple).relkind == RELKIND_PARTITIONED_INDEX
        {
            ereport!(ERROR, errmsg!("\"{}\" is an index", cstr_to_str(NameStr((*pg_class_tuple).relname))));
        }

        /* Composite types aren't tables either */
        if (*pg_class_tuple).relkind == RELKIND_COMPOSITE_TYPE {
            ereport!(ERROR, errmsg!("\"{}\" is a composite type", cstr_to_str(NameStr((*pg_class_tuple).relname))));
        }

        /* Used GRANT SEQUENCE on a non-sequence? */
        if (*istmt).objtype == OBJECT_SEQUENCE
            && (*pg_class_tuple).relkind != RELKIND_SEQUENCE
        {
            ereport!(ERROR, errmsg!("\"{}\" is not a sequence", cstr_to_str(NameStr((*pg_class_tuple).relname))));
        }

        /* Adjust the default permissions based on object type */
        if (*istmt).all_privs && (*istmt).privileges == ACL_NO_RIGHTS {
            if (*pg_class_tuple).relkind == RELKIND_SEQUENCE {
                this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            } else {
                this_privileges = ACL_ALL_RIGHTS_RELATION;
            }
        } else {
            this_privileges = (*istmt).privileges;
        }

        /*
         * The GRANT TABLE syntax can be used for sequences and non-sequences.
         */
        if (*istmt).objtype == OBJECT_TABLE {
            if (*pg_class_tuple).relkind == RELKIND_SEQUENCE {
                /*
                 * For backward compatibility, just throw a warning for
                 * invalid sequence permissions when using the non-sequence GRANT syntax.
                 */
                if this_privileges & !(ACL_ALL_RIGHTS_SEQUENCE as AclMode) != 0 {
                    ereport!(WARNING, errmsg!("sequence \"{}\" only supports USAGE, SELECT, and UPDATE privileges", cstr_to_str(NameStr((*pg_class_tuple).relname))));
                    this_privileges &= ACL_ALL_RIGHTS_SEQUENCE as AclMode;
                }
            } else {
                if this_privileges & !(ACL_ALL_RIGHTS_RELATION as AclMode) != 0 {
                    /*
                     * USAGE is the only permission supported by sequences but
                     * not by non-sequences.
                     */
                    ereport!(ERROR, errmsg!("invalid privilege type {} for table", "USAGE"));
                }
            }
        }

        /*
         * Set up array in which we'll accumulate any column privilege bits
         * that need modification.
         */
        num_col_privileges = (*pg_class_tuple).relnatts as c_int
            - FirstLowInvalidHeapAttributeNumber as c_int + 1;
        col_privileges = palloc0(num_col_privileges as usize * core::mem::size_of::<AclMode>()) as *mut AclMode;
        have_col_privileges = false;

        /*
         * If we are revoking relation privileges that are also column
         * privileges, we must implicitly revoke them from each column too,
         * per SQL spec.
         */
        if !(*istmt).is_grant && (this_privileges & ACL_ALL_RIGHTS_COLUMN) != 0 {
            expand_all_col_privileges(
                relOid, pg_class_tuple,
                this_privileges & ACL_ALL_RIGHTS_COLUMN,
                col_privileges,
                num_col_privileges,
            );
            have_col_privileges = true;
        }

        /*
         * Get owner ID and working copy of existing ACL.
         */
        ownerId = (*pg_class_tuple).relowner;
        aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &mut isNull);
        if isNull {
            match (*pg_class_tuple).relkind {
                RELKIND_SEQUENCE => {
                    old_acl = acldefault(OBJECT_SEQUENCE, ownerId);
                }
                _ => {
                    old_acl = acldefault(OBJECT_TABLE, ownerId);
                }
            }
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = core::ptr::null_mut();
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &mut oldmembers);
        }

        /* Need an extra copy of original rel ACL for column handling */
        old_rel_acl = aclcopy(old_acl);

        /*
         * Handle relation-level privileges, if any were specified
         */
        if this_privileges != ACL_NO_RIGHTS {
            let mut avail_goptions: AclMode = 0;
            let new_acl: *mut Acl;
            let mut grantorId: Oid = InvalidOid;
            let newtuple: HeapTuple;
            let mut values = [0 as Datum; Natts_pg_class];
            let mut nulls = [false; Natts_pg_class];
            let mut replaces = [false; Natts_pg_class];
            let mut nnewmembers: c_int = 0;
            let mut newmembers: *mut Oid = core::ptr::null_mut();
            let objtype: ObjectType;

            /* Determine ID to do the grant as, and available grant options */
            select_best_grantor(
                GetUserId(), this_privileges,
                old_acl, ownerId,
                &mut grantorId, &mut avail_goptions,
            );

            match (*pg_class_tuple).relkind {
                RELKIND_SEQUENCE => { objtype = OBJECT_SEQUENCE; }
                _                => { objtype = OBJECT_TABLE; }
            }

            /*
             * Restrict the privileges to what we can actually grant.
             */
            this_privileges = restrict_and_check_grant(
                (*istmt).is_grant, avail_goptions,
                (*istmt).all_privs, this_privileges,
                relOid, grantorId, objtype,
                NameStr((*pg_class_tuple).relname),
                0, core::ptr::null(),
            );

            /*
             * Generate new ACL.
             */
            new_acl = merge_acl_with_grant(
                old_acl,
                (*istmt).is_grant,
                (*istmt).grant_option,
                (*istmt).behavior,
                (*istmt).grantees,
                this_privileges,
                grantorId,
                ownerId,
            );

            /*
             * We need the members of both old and new ACLs.
             */
            nnewmembers = aclmembers(new_acl, &mut newmembers);

            /* finished building new ACL value, now insert it */
            replaces[Anum_pg_class_relacl as usize - 1] = true;
            values[Anum_pg_class_relacl as usize - 1] = PointerGetDatum(new_acl as *const c_void);

            newtuple = heap_modify_tuple(
                tuple, RelationGetDescr(relation),
                values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
            );

            CatalogTupleUpdate(relation, &mut (*newtuple).t_self, newtuple);
            UnlockTuple(relation, &(*tuple).t_self, InplaceUpdateTupleLock);

            /* Update initial privileges for extensions */
            recordExtensionInitPriv(relOid, RelationRelationId, 0, new_acl);

            /* Update the shared dependency ACL info */
            updateAclDependencies(
                RelationRelationId, relOid, 0,
                ownerId,
                noldmembers, oldmembers,
                nnewmembers, newmembers,
            );

            pfree(new_acl as *mut c_void);
        } else {
            UnlockTuple(relation, &(*tuple).t_self, InplaceUpdateTupleLock);
        }

        /*
         * Handle column-level privileges.
         */
        cell_colprivs = if (*istmt).col_privs.is_null() { core::ptr::null_mut() } else { list_head((*istmt).col_privs) };
        while !cell_colprivs.is_null() {
            let col_privs_node = (*cell_colprivs).ptr_value as *mut AccessPriv;

            if (*col_privs_node).priv_name.is_null() {
                this_privileges = ACL_ALL_RIGHTS_COLUMN;
            } else {
                this_privileges = string_to_privilege((*col_privs_node).priv_name);
            }

            if this_privileges & !(ACL_ALL_RIGHTS_COLUMN as AclMode) != 0 {
                ereport!(ERROR, errmsg!("invalid privilege type {} for column", cstr_to_str(privilege_to_string(this_privileges))));
            }

            if (*pg_class_tuple).relkind == RELKIND_SEQUENCE
                && this_privileges & !(ACL_SELECT as AclMode) != 0
            {
                /*
                 * The only column privilege allowed on sequences is SELECT.
                 */
                ereport!(WARNING, errmsg!("sequence \"{}\" only supports SELECT column privileges", cstr_to_str(NameStr((*pg_class_tuple).relname))));
                this_privileges &= ACL_SELECT as AclMode;
            }

            expand_col_privileges(
                (*col_privs_node).cols,
                relOid,
                this_privileges,
                col_privileges,
                num_col_privileges,
            );
            have_col_privileges = true;
            cell_colprivs = lnext((*istmt).col_privs, cell_colprivs);
        }

        if have_col_privileges {
            let mut i: AttrNumber = 0;
            while i < num_col_privileges as AttrNumber {
                if *col_privileges.add(i as usize) == ACL_NO_RIGHTS {
                    i += 1;
                    continue;
                }
                ExecGrant_Attribute(
                    istmt,
                    relOid,
                    NameStr((*pg_class_tuple).relname),
                    i + FirstLowInvalidHeapAttributeNumber as AttrNumber,
                    ownerId,
                    *col_privileges.add(i as usize),
                    attRelation,
                    old_rel_acl,
                );
                i += 1;
            }
        }

        pfree(old_rel_acl as *mut c_void);
        pfree(col_privileges as *mut c_void);

        ReleaseSysCache(tuple);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
        cell = lnext((*istmt).objects, cell);
    }

    table_close(attRelation, RowExclusiveLock);
    table_close(relation, RowExclusiveLock);
}

unsafe fn ExecGrant_common(
    istmt: *mut InternalGrant,
    classid: Oid,
    default_privs: AclMode,
    object_check: Option<unsafe fn(*mut InternalGrant, HeapTuple)>,
) {
    let cacheid: c_int;
    let relation: Relation;
    let mut cell: *mut ListCell;

    if (*istmt).all_privs && (*istmt).privileges == ACL_NO_RIGHTS {
        (*istmt).privileges = default_privs;
    }

    cacheid = get_object_catcache_oid(classid);

    relation = table_open(classid, RowExclusiveLock);

    cell = if (*istmt).objects.is_null() { core::ptr::null_mut() } else { list_head((*istmt).objects) };
    while !cell.is_null() {
        let objectid: Oid = *((*cell).ptr_value as *mut Oid);
        let mut aclDatum: Datum;
        let nameDatum: Datum;
        let mut isNull: bool = false;
        let mut avail_goptions: AclMode = 0;
        let this_privileges: AclMode;
        let old_acl: *mut Acl;
        let new_acl: *mut Acl;
        let mut grantorId: Oid = InvalidOid;
        let ownerId: Oid;
        let tuple: HeapTuple;
        let newtuple: HeapTuple;
        let nattrs = (*RelationGetDescr(relation)).natts as usize;
        let values = palloc0(nattrs * core::mem::size_of::<Datum>()) as *mut Datum;
        let nulls  = palloc0(nattrs * core::mem::size_of::<bool>()) as *mut bool;
        let replaces = palloc0(nattrs * core::mem::size_of::<bool>()) as *mut bool;
        let mut noldmembers: c_int = 0;
        let mut nnewmembers: c_int = 0;
        let mut oldmembers: *mut Oid = core::ptr::null_mut();
        let mut newmembers: *mut Oid = core::ptr::null_mut();

        tuple = SearchSysCacheLocked1(cacheid, ObjectIdGetDatum(objectid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for {} {}", cstr_to_str(get_object_class_descr(classid)), objectid);
        }

        /*
         * Additional object-type-specific checks
         */
        if let Some(check_fn) = object_check {
            check_fn(istmt, tuple);
        }

        /*
         * Get owner ID and working copy of existing ACL.
         */
        ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(
            cacheid, tuple, get_object_attnum_owner(classid),
        ));
        aclDatum = SysCacheGetAttr(
            cacheid, tuple, get_object_attnum_acl(classid), &mut isNull,
        );
        if isNull {
            old_acl = acldefault(get_object_type(classid, objectid), ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = core::ptr::null_mut();
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &mut oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(
            GetUserId(), (*istmt).privileges,
            old_acl, ownerId,
            &mut grantorId, &mut avail_goptions,
        );

        nameDatum = SysCacheGetAttrNotNull(
            cacheid, tuple, get_object_attnum_name(classid),
        );

        /*
         * Restrict the privileges to what we can actually grant.
         */
        this_privileges = restrict_and_check_grant(
            (*istmt).is_grant, avail_goptions,
            (*istmt).all_privs, (*istmt).privileges,
            objectid, grantorId, get_object_type(classid, objectid),
            NameStr(*DatumGetName(nameDatum)),
            0, core::ptr::null(),
        );

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(
            old_acl, (*istmt).is_grant,
            (*istmt).grant_option, (*istmt).behavior,
            (*istmt).grantees, this_privileges,
            grantorId, ownerId,
        );

        /*
         * We need the members of both old and new ACLs.
         */
        nnewmembers = aclmembers(new_acl, &mut newmembers);

        /* finished building new ACL value, now insert it */
        *replaces.add(get_object_attnum_acl(classid) as usize - 1) = true;
        *values.add(get_object_attnum_acl(classid) as usize - 1) = PointerGetDatum(new_acl as *const c_void);

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

        CatalogTupleUpdate(relation, &mut (*newtuple).t_self, newtuple);
        UnlockTuple(relation, &(*tuple).t_self, InplaceUpdateTupleLock);

        /* Update initial privileges for extensions */
        recordExtensionInitPriv(objectid, classid, 0, new_acl);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            classid, objectid, 0,
            ownerId,
            noldmembers, oldmembers,
            nnewmembers, newmembers,
        );

        ReleaseSysCache(tuple);
        pfree(new_acl as *mut c_void);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
        cell = lnext((*istmt).objects, cell);
    }

    table_close(relation, RowExclusiveLock);
}

unsafe fn ExecGrant_Language_check(istmt: *mut InternalGrant, tuple: HeapTuple) {
    let pg_language_tuple: Form_pg_language = GETSTRUCT(tuple) as Form_pg_language;

    if !(*pg_language_tuple).lanpltrusted {
        ereport!(ERROR, errmsg!("language \"{}\" is not trusted", cstr_to_str(NameStr((*pg_language_tuple).lanname))));
    }
}


unsafe fn ExecGrant_Largeobject(istmt: *mut InternalGrant) {
    let relation: Relation;
    let mut cell: *mut ListCell;

    if (*istmt).all_privs && (*istmt).privileges == ACL_NO_RIGHTS {
        (*istmt).privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
    }

    relation = table_open(LargeObjectMetadataRelationId, RowExclusiveLock);

    cell = if (*istmt).objects.is_null() { core::ptr::null_mut() } else { list_head((*istmt).objects) };
    while !cell.is_null() {
        let loid: Oid = *((*cell).ptr_value as *mut Oid);
        let form_lo_meta: Form_pg_largeobject_metadata;
        let mut loname = [0i8; NAMEDATALEN];
        let mut aclDatum: Datum;
        let mut isNull: bool = false;
        let mut avail_goptions: AclMode = 0;
        let this_privileges: AclMode;
        let old_acl: *mut Acl;
        let new_acl: *mut Acl;
        let mut grantorId: Oid = InvalidOid;
        let ownerId: Oid;
        let newtuple: HeapTuple;
        let mut values = [0 as Datum; Natts_pg_largeobject_metadata];
        let mut nulls = [false; Natts_pg_largeobject_metadata];
        let mut replaces = [false; Natts_pg_largeobject_metadata];
        let mut noldmembers: c_int = 0;
        let mut nnewmembers: c_int = 0;
        let mut oldmembers: *mut Oid = core::ptr::null_mut();
        let mut newmembers: *mut Oid = core::ptr::null_mut();
        let mut entry = [core::mem::zeroed::<ScanKeyData>(); 1];
        let scan: SysScanDesc;
        let tuple: HeapTuple;

        /* There's no syscache for pg_largeobject_metadata */
        ScanKeyInit(
            &mut entry[0],
            Anum_pg_largeobject_metadata_oid,
            BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(loid),
        );

        scan = systable_beginscan(
            relation, LargeObjectMetadataOidIndexId, true,
            core::ptr::null_mut(), 1, entry.as_mut_ptr(),
        );

        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "could not find tuple for large object {}", loid);
        }

        form_lo_meta = GETSTRUCT(tuple) as Form_pg_largeobject_metadata;

        /*
         * Get owner ID and working copy of existing ACL.
         */
        ownerId = (*form_lo_meta).lomowner;
        aclDatum = heap_getattr(
            tuple, Anum_pg_largeobject_metadata_lomacl as i32,
            RelationGetDescr(relation), &mut isNull,
        );
        if isNull {
            old_acl = acldefault(OBJECT_LARGEOBJECT, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = core::ptr::null_mut();
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &mut oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(
            GetUserId(), (*istmt).privileges,
            old_acl, ownerId,
            &mut grantorId, &mut avail_goptions,
        );

        /*
         * Restrict the privileges to what we can actually grant.
         */
        snprintf(loname.as_mut_ptr(), NAMEDATALEN, b"large object %u\0".as_ptr() as *const c_char, loid);
        let this_privileges = restrict_and_check_grant(
            (*istmt).is_grant, avail_goptions,
            (*istmt).all_privs, (*istmt).privileges,
            loid, grantorId, OBJECT_LARGEOBJECT,
            loname.as_ptr(), 0, core::ptr::null(),
        );

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(
            old_acl, (*istmt).is_grant,
            (*istmt).grant_option, (*istmt).behavior,
            (*istmt).grantees, this_privileges,
            grantorId, ownerId,
        );

        /*
         * We need the members of both old and new ACLs.
         */
        nnewmembers = aclmembers(new_acl, &mut newmembers);

        /* finished building new ACL value, now insert it */
        replaces[Anum_pg_largeobject_metadata_lomacl as usize - 1] = true;
        values[Anum_pg_largeobject_metadata_lomacl as usize - 1] = PointerGetDatum(new_acl as *const c_void);

        newtuple = heap_modify_tuple(
            tuple, RelationGetDescr(relation),
            values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
        );

        CatalogTupleUpdate(relation, &mut (*newtuple).t_self, newtuple);

        /* Update initial privileges for extensions */
        recordExtensionInitPriv(loid, LargeObjectRelationId, 0, new_acl);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            LargeObjectRelationId, (*form_lo_meta).oid, 0,
            ownerId,
            noldmembers, oldmembers,
            nnewmembers, newmembers,
        );

        systable_endscan(scan);
        pfree(new_acl as *mut c_void);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
        cell = lnext((*istmt).objects, cell);
    }

    table_close(relation, RowExclusiveLock);
}

unsafe fn ExecGrant_Type_check(istmt: *mut InternalGrant, tuple: HeapTuple) {
    let pg_type_tuple: Form_pg_type = GETSTRUCT(tuple) as Form_pg_type;

    /* Disallow GRANT on dependent types */
    if IsTrueArrayType(pg_type_tuple) {
        ereport!(ERROR, errmsg!("cannot set privileges of array types"));
    }
    if (*pg_type_tuple).typtype == TYPTYPE_MULTIRANGE {
        ereport!(ERROR, errmsg!("cannot set privileges of multirange types"));
    }
}

unsafe fn ExecGrant_Parameter(istmt: *mut InternalGrant) {
    let relation: Relation;
    let mut cell: *mut ListCell;

    if (*istmt).all_privs && (*istmt).privileges == ACL_NO_RIGHTS {
        (*istmt).privileges = ACL_ALL_RIGHTS_PARAMETER_ACL;
    }

    relation = table_open(ParameterAclRelationId, RowExclusiveLock);

    cell = if (*istmt).objects.is_null() { core::ptr::null_mut() } else { list_head((*istmt).objects) };
    while !cell.is_null() {
        let parameterId: Oid = *((*cell).ptr_value as *mut Oid);
        let nameDatum: Datum;
        let parname: *const c_char;
        let mut aclDatum: Datum;
        let mut isNull: bool = false;
        let mut avail_goptions: AclMode = 0;
        let this_privileges: AclMode;
        let old_acl: *mut Acl;
        let new_acl: *mut Acl;
        let mut grantorId: Oid = InvalidOid;
        let ownerId: Oid;
        let tuple: HeapTuple;
        let mut noldmembers: c_int = 0;
        let mut nnewmembers: c_int = 0;
        let mut oldmembers: *mut Oid = core::ptr::null_mut();
        let mut newmembers: *mut Oid = core::ptr::null_mut();

        tuple = SearchSysCache1(PARAMETERACLOID, ObjectIdGetDatum(parameterId));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for parameter ACL {}", parameterId);
        }

        /* We'll need the GUC's name */
        nameDatum = SysCacheGetAttrNotNull(
            PARAMETERACLOID, tuple, Anum_pg_parameter_acl_parname,
        );
        parname = TextDatumGetCString(nameDatum);

        /* Treat all parameters as belonging to the bootstrap superuser. */
        ownerId = BOOTSTRAP_SUPERUSERID;

        /*
         * Get working copy of existing ACL.
         */
        aclDatum = SysCacheGetAttr(
            PARAMETERACLOID, tuple, Anum_pg_parameter_acl_paracl, &mut isNull,
        );

        if isNull {
            old_acl = acldefault((*istmt).objtype, ownerId);
            /* There are no old member roles according to the catalogs */
            noldmembers = 0;
            oldmembers = core::ptr::null_mut();
        } else {
            old_acl = DatumGetAclPCopy(aclDatum);
            /* Get the roles mentioned in the existing ACL */
            noldmembers = aclmembers(old_acl, &mut oldmembers);
        }

        /* Determine ID to do the grant as, and available grant options */
        select_best_grantor(
            GetUserId(), (*istmt).privileges,
            old_acl, ownerId,
            &mut grantorId, &mut avail_goptions,
        );

        /*
         * Restrict the privileges to what we can actually grant.
         */
        this_privileges = restrict_and_check_grant(
            (*istmt).is_grant, avail_goptions,
            (*istmt).all_privs, (*istmt).privileges,
            parameterId, grantorId,
            OBJECT_PARAMETER_ACL,
            parname,
            0, core::ptr::null(),
        );

        /*
         * Generate new ACL.
         */
        new_acl = merge_acl_with_grant(
            old_acl, (*istmt).is_grant,
            (*istmt).grant_option, (*istmt).behavior,
            (*istmt).grantees, this_privileges,
            grantorId, ownerId,
        );

        /*
         * We need the members of both old and new ACLs.
         */
        nnewmembers = aclmembers(new_acl, &mut newmembers);

        /*
         * If the new ACL is equal to the default, we don't need the catalog
         * entry any longer.
         */
        if aclequal(new_acl, acldefault((*istmt).objtype, ownerId)) {
            CatalogTupleDelete(relation, &mut (*tuple).t_self as *mut _);
        } else {
            /* finished building new ACL value, now insert it */
            let newtuple: HeapTuple;
            let mut values = [0 as Datum; Natts_pg_parameter_acl];
            let mut nulls = [false; Natts_pg_parameter_acl];
            let mut replaces = [false; Natts_pg_parameter_acl];

            replaces[Anum_pg_parameter_acl_paracl as usize - 1] = true;
            values[Anum_pg_parameter_acl_paracl as usize - 1] = PointerGetDatum(new_acl as *const c_void);

            newtuple = heap_modify_tuple(
                tuple, RelationGetDescr(relation),
                values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
            );

            CatalogTupleUpdate(relation, &mut (*newtuple).t_self, newtuple);
        }

        /* Update initial privileges for extensions */
        recordExtensionInitPriv(parameterId, ParameterAclRelationId, 0, new_acl);

        /* Update the shared dependency ACL info */
        updateAclDependencies(
            ParameterAclRelationId, parameterId, 0,
            ownerId,
            noldmembers, oldmembers,
            nnewmembers, newmembers,
        );

        ReleaseSysCache(tuple);
        pfree(new_acl as *mut c_void);

        /* prevent error when processing duplicate objects */
        CommandCounterIncrement();
        cell = lnext((*istmt).objects, cell);
    }

    table_close(relation, RowExclusiveLock);
}


unsafe fn string_to_privilege(privname: *const c_char) -> AclMode {
    let s = core::ffi::CStr::from_ptr(privname).to_str().unwrap_or("");
    match s {
        "insert"       => ACL_INSERT,
        "select"       => ACL_SELECT,
        "update"       => ACL_UPDATE,
        "delete"       => ACL_DELETE,
        "truncate"     => ACL_TRUNCATE,
        "references"   => ACL_REFERENCES,
        "trigger"      => ACL_TRIGGER,
        "execute"      => ACL_EXECUTE,
        "usage"        => ACL_USAGE,
        "create"       => ACL_CREATE,
        "temporary"    => ACL_CREATE_TEMP,
        "temp"         => ACL_CREATE_TEMP,
        "connect"      => ACL_CONNECT,
        "set"          => ACL_SET,
        "alter system" => ACL_ALTER_SYSTEM,
        "maintain"     => ACL_MAINTAIN,
        _ => {
            ereport!(ERROR, errmsg!("unrecognized privilege type \"{}\"", s));
            0   /* appease compiler */
        }
    }
}

unsafe fn privilege_to_string(privilege: AclMode) -> *const c_char {
    match privilege {
        ACL_INSERT      => b"INSERT\0".as_ptr() as *const c_char,
        ACL_SELECT      => b"SELECT\0".as_ptr() as *const c_char,
        ACL_UPDATE      => b"UPDATE\0".as_ptr() as *const c_char,
        ACL_DELETE      => b"DELETE\0".as_ptr() as *const c_char,
        ACL_TRUNCATE    => b"TRUNCATE\0".as_ptr() as *const c_char,
        ACL_REFERENCES  => b"REFERENCES\0".as_ptr() as *const c_char,
        ACL_TRIGGER     => b"TRIGGER\0".as_ptr() as *const c_char,
        ACL_EXECUTE     => b"EXECUTE\0".as_ptr() as *const c_char,
        ACL_USAGE       => b"USAGE\0".as_ptr() as *const c_char,
        ACL_CREATE      => b"CREATE\0".as_ptr() as *const c_char,
        ACL_CREATE_TEMP => b"TEMP\0".as_ptr() as *const c_char,
        ACL_CONNECT     => b"CONNECT\0".as_ptr() as *const c_char,
        ACL_SET         => b"SET\0".as_ptr() as *const c_char,
        ACL_ALTER_SYSTEM => b"ALTER SYSTEM\0".as_ptr() as *const c_char,
        ACL_MAINTAIN    => b"MAINTAIN\0".as_ptr() as *const c_char,
        _ => {
            elog!(ERROR, "unrecognized privilege: {}", privilege as c_int);
            core::ptr::null()   /* appease compiler */
        }
    }
}

/*
 * Standardized reporting of aclcheck permissions failures.
 */
#[no_mangle]
pub unsafe fn aclcheck_error(aclerr: AclResult, objtype: ObjectType, objectname: *const c_char) {
    match aclerr {
        ACLCHECK_OK => {
            /* no error, so return to caller */
        }
        ACLCHECK_NO_PRIV => {
            let msg: *const c_char = match objtype {
                OBJECT_AGGREGATE     => b"permission denied for aggregate %s\0".as_ptr() as *const c_char,
                OBJECT_COLLATION     => b"permission denied for collation %s\0".as_ptr() as *const c_char,
                OBJECT_COLUMN        => b"permission denied for column %s\0".as_ptr() as *const c_char,
                OBJECT_CONVERSION    => b"permission denied for conversion %s\0".as_ptr() as *const c_char,
                OBJECT_DATABASE      => b"permission denied for database %s\0".as_ptr() as *const c_char,
                OBJECT_DOMAIN        => b"permission denied for domain %s\0".as_ptr() as *const c_char,
                OBJECT_EVENT_TRIGGER => b"permission denied for event trigger %s\0".as_ptr() as *const c_char,
                OBJECT_EXTENSION     => b"permission denied for extension %s\0".as_ptr() as *const c_char,
                OBJECT_FDW           => b"permission denied for foreign-data wrapper %s\0".as_ptr() as *const c_char,
                OBJECT_FOREIGN_SERVER => b"permission denied for foreign server %s\0".as_ptr() as *const c_char,
                OBJECT_FOREIGN_TABLE => b"permission denied for foreign table %s\0".as_ptr() as *const c_char,
                OBJECT_FUNCTION      => b"permission denied for function %s\0".as_ptr() as *const c_char,
                OBJECT_INDEX         => b"permission denied for index %s\0".as_ptr() as *const c_char,
                OBJECT_LANGUAGE      => b"permission denied for language %s\0".as_ptr() as *const c_char,
                OBJECT_LARGEOBJECT   => b"permission denied for large object %s\0".as_ptr() as *const c_char,
                OBJECT_MATVIEW       => b"permission denied for materialized view %s\0".as_ptr() as *const c_char,
                OBJECT_OPCLASS       => b"permission denied for operator class %s\0".as_ptr() as *const c_char,
                OBJECT_OPERATOR      => b"permission denied for operator %s\0".as_ptr() as *const c_char,
                OBJECT_OPFAMILY      => b"permission denied for operator family %s\0".as_ptr() as *const c_char,
                OBJECT_PARAMETER_ACL => b"permission denied for parameter %s\0".as_ptr() as *const c_char,
                OBJECT_POLICY        => b"permission denied for policy %s\0".as_ptr() as *const c_char,
                OBJECT_PROCEDURE     => b"permission denied for procedure %s\0".as_ptr() as *const c_char,
                OBJECT_PUBLICATION   => b"permission denied for publication %s\0".as_ptr() as *const c_char,
                OBJECT_ROUTINE       => b"permission denied for routine %s\0".as_ptr() as *const c_char,
                OBJECT_SCHEMA        => b"permission denied for schema %s\0".as_ptr() as *const c_char,
                OBJECT_SEQUENCE      => b"permission denied for sequence %s\0".as_ptr() as *const c_char,
                OBJECT_STATISTIC_EXT => b"permission denied for statistics object %s\0".as_ptr() as *const c_char,
                OBJECT_SUBSCRIPTION  => b"permission denied for subscription %s\0".as_ptr() as *const c_char,
                OBJECT_TABLE         => b"permission denied for table %s\0".as_ptr() as *const c_char,
                OBJECT_TABLESPACE    => b"permission denied for tablespace %s\0".as_ptr() as *const c_char,
                OBJECT_TSCONFIGURATION => b"permission denied for text search configuration %s\0".as_ptr() as *const c_char,
                OBJECT_TSDICTIONARY  => b"permission denied for text search dictionary %s\0".as_ptr() as *const c_char,
                OBJECT_TYPE          => b"permission denied for type %s\0".as_ptr() as *const c_char,
                OBJECT_VIEW          => b"permission denied for view %s\0".as_ptr() as *const c_char,
                /* these currently aren't used */
                OBJECT_ACCESS_METHOD | OBJECT_AMOP | OBJECT_AMPROC | OBJECT_ATTRIBUTE
                | OBJECT_CAST | OBJECT_DEFAULT | OBJECT_DEFACL | OBJECT_DOMCONSTRAINT
                | OBJECT_PUBLICATION_NAMESPACE | OBJECT_PUBLICATION_REL
                | OBJECT_ROLE | OBJECT_RULE | OBJECT_TABCONSTRAINT
                | OBJECT_TRANSFORM | OBJECT_TRIGGER | OBJECT_TSPARSER
                | OBJECT_TSTEMPLATE | OBJECT_USER_MAPPING => {
                    if std::env::var_os("PDB_AUTH").is_some() { eprintln!("PDB_BT unsupported_objtype site bt:
{}", std::backtrace::Backtrace::force_capture()); } elog!(ERROR, "unsupported object type: {}", objtype as c_int);
                    core::ptr::null()
                }
                _ => b"???\0".as_ptr() as *const c_char,
            };
            ereport!(ERROR, errmsg!("permission denied for {} {}", cstr_to_str(msg), cstr_to_str(objectname)));
        }
        ACLCHECK_NOT_OWNER => {
            let msg: *const c_char = match objtype {
                OBJECT_AGGREGATE     => b"must be owner of aggregate %s\0".as_ptr() as *const c_char,
                OBJECT_COLLATION     => b"must be owner of collation %s\0".as_ptr() as *const c_char,
                OBJECT_CONVERSION    => b"must be owner of conversion %s\0".as_ptr() as *const c_char,
                OBJECT_DATABASE      => b"must be owner of database %s\0".as_ptr() as *const c_char,
                OBJECT_DOMAIN        => b"must be owner of domain %s\0".as_ptr() as *const c_char,
                OBJECT_EVENT_TRIGGER => b"must be owner of event trigger %s\0".as_ptr() as *const c_char,
                OBJECT_EXTENSION     => b"must be owner of extension %s\0".as_ptr() as *const c_char,
                OBJECT_FDW           => b"must be owner of foreign-data wrapper %s\0".as_ptr() as *const c_char,
                OBJECT_FOREIGN_SERVER => b"must be owner of foreign server %s\0".as_ptr() as *const c_char,
                OBJECT_FOREIGN_TABLE => b"must be owner of foreign table %s\0".as_ptr() as *const c_char,
                OBJECT_FUNCTION      => b"must be owner of function %s\0".as_ptr() as *const c_char,
                OBJECT_INDEX         => b"must be owner of index %s\0".as_ptr() as *const c_char,
                OBJECT_LANGUAGE      => b"must be owner of language %s\0".as_ptr() as *const c_char,
                OBJECT_LARGEOBJECT   => b"must be owner of large object %s\0".as_ptr() as *const c_char,
                OBJECT_MATVIEW       => b"must be owner of materialized view %s\0".as_ptr() as *const c_char,
                OBJECT_OPCLASS       => b"must be owner of operator class %s\0".as_ptr() as *const c_char,
                OBJECT_OPERATOR      => b"must be owner of operator %s\0".as_ptr() as *const c_char,
                OBJECT_OPFAMILY      => b"must be owner of operator family %s\0".as_ptr() as *const c_char,
                OBJECT_PROCEDURE     => b"must be owner of procedure %s\0".as_ptr() as *const c_char,
                OBJECT_PUBLICATION   => b"must be owner of publication %s\0".as_ptr() as *const c_char,
                OBJECT_ROUTINE       => b"must be owner of routine %s\0".as_ptr() as *const c_char,
                OBJECT_SEQUENCE      => b"must be owner of sequence %s\0".as_ptr() as *const c_char,
                OBJECT_SUBSCRIPTION  => b"must be owner of subscription %s\0".as_ptr() as *const c_char,
                OBJECT_TABLE         => b"must be owner of table %s\0".as_ptr() as *const c_char,
                OBJECT_TYPE          => b"must be owner of type %s\0".as_ptr() as *const c_char,
                OBJECT_VIEW          => b"must be owner of view %s\0".as_ptr() as *const c_char,
                OBJECT_SCHEMA        => b"must be owner of schema %s\0".as_ptr() as *const c_char,
                OBJECT_STATISTIC_EXT => b"must be owner of statistics object %s\0".as_ptr() as *const c_char,
                OBJECT_TABLESPACE    => b"must be owner of tablespace %s\0".as_ptr() as *const c_char,
                OBJECT_TSCONFIGURATION => b"must be owner of text search configuration %s\0".as_ptr() as *const c_char,
                OBJECT_TSDICTIONARY  => b"must be owner of text search dictionary %s\0".as_ptr() as *const c_char,
                /*
                 * Special cases: For these, the error message talks about "relation",
                 * because that's where the ownership is attached.
                 */
                OBJECT_COLUMN | OBJECT_POLICY | OBJECT_RULE
                | OBJECT_TABCONSTRAINT | OBJECT_TRIGGER =>
                    b"must be owner of relation %s\0".as_ptr() as *const c_char,
                /* these currently aren't used */
                OBJECT_ACCESS_METHOD | OBJECT_AMOP | OBJECT_AMPROC | OBJECT_ATTRIBUTE
                | OBJECT_CAST | OBJECT_DEFAULT | OBJECT_DEFACL | OBJECT_DOMCONSTRAINT
                | OBJECT_PARAMETER_ACL | OBJECT_PUBLICATION_NAMESPACE | OBJECT_PUBLICATION_REL
                | OBJECT_ROLE | OBJECT_TRANSFORM | OBJECT_TSPARSER
                | OBJECT_TSTEMPLATE | OBJECT_USER_MAPPING => {
                    if std::env::var_os("PDB_AUTH").is_some() { eprintln!("PDB_BT unsupported_objtype site bt:
{}", std::backtrace::Backtrace::force_capture()); } elog!(ERROR, "unsupported object type: {}", objtype as c_int);
                    core::ptr::null()
                }
                _ => b"???\0".as_ptr() as *const c_char,
            };
            ereport!(ERROR, errmsg!("must be owner of {} {}", cstr_to_str(msg), cstr_to_str(objectname)));
        }
        _ => {
            elog!(ERROR, "unrecognized AclResult: {}", aclerr as c_int);
        }
    }
}


#[no_mangle]
pub unsafe fn aclcheck_error_col(
    aclerr: AclResult,
    objtype: ObjectType,
    objectname: *const c_char,
    colname: *const c_char,
) {
    match aclerr {
        ACLCHECK_OK => {
            /* no error, so return to caller */
        }
        ACLCHECK_NO_PRIV => {
            ereport!(ERROR, errmsg!("permission denied for column \"{}\" of relation \"{}\"",
                         cstr_to_str(colname), cstr_to_str(objectname)));
        }
        ACLCHECK_NOT_OWNER => {
            /* relation msg is OK since columns don't have separate owners */
            aclcheck_error(aclerr, objtype, objectname);
        }
        _ => {
            elog!(ERROR, "unrecognized AclResult: {}", aclerr as c_int);
        }
    }
}


/*
 * Special common handling for types: use element type instead of array type,
 * and format nicely
 */
#[no_mangle]
pub unsafe fn aclcheck_error_type(aclerr: AclResult, typeOid: Oid) {
    let element_type: Oid = get_element_type(typeOid);
    aclcheck_error(
        aclerr, OBJECT_TYPE,
        format_type_be(if element_type != InvalidOid { element_type } else { typeOid }),
    );
}


/*
 * Relay for the various pg_*_mask routines depending on object kind
 */
unsafe fn pg_aclmask(
    objtype: ObjectType,
    object_oid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> AclMode {
    match objtype {
        OBJECT_COLUMN => {
            pg_class_aclmask(object_oid, roleid, mask, how)
                | pg_attribute_aclmask(object_oid, attnum, roleid, mask, how)
        }
        OBJECT_TABLE | OBJECT_SEQUENCE => {
            pg_class_aclmask(object_oid, roleid, mask, how)
        }
        OBJECT_DATABASE => {
            object_aclmask(DatabaseRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_FUNCTION => {
            object_aclmask(ProcedureRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_LANGUAGE => {
            object_aclmask(LanguageRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_LARGEOBJECT => {
            pg_largeobject_aclmask_snapshot(object_oid, roleid, mask, how, core::ptr::null_mut())
        }
        OBJECT_PARAMETER_ACL => {
            pg_parameter_acl_aclmask(object_oid, roleid, mask, how)
        }
        OBJECT_SCHEMA => {
            object_aclmask(NamespaceRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_STATISTIC_EXT => {
            elog!(ERROR, "grantable rights not supported for statistics objects");
            /* not reached, but keep compiler quiet */
            ACL_NO_RIGHTS
        }
        OBJECT_TABLESPACE => {
            object_aclmask(TableSpaceRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_FDW => {
            object_aclmask(ForeignDataWrapperRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_FOREIGN_SERVER => {
            object_aclmask(ForeignServerRelationId, object_oid, roleid, mask, how)
        }
        OBJECT_EVENT_TRIGGER => {
            elog!(ERROR, "grantable rights not supported for event triggers");
            /* not reached, but keep compiler quiet */
            ACL_NO_RIGHTS
        }
        OBJECT_TYPE => {
            object_aclmask(TypeRelationId, object_oid, roleid, mask, how)
        }
        _ => {
            elog!(ERROR, "unrecognized object type: {}", objtype as c_int);
            /* not reached, but keep compiler quiet */
            ACL_NO_RIGHTS
        }
    }
}


/* ****************************************************************
 * Exported routines for examining a user's privileges for various objects
 * ****************************************************************
 */

/*
 * Generic routine for examining a user's privileges for an object
 */
unsafe fn object_aclmask(
    classid: Oid, objectid: Oid, roleid: Oid,
    mask: AclMode, how: AclMaskHow,
) -> AclMode {
    object_aclmask_ext(classid, objectid, roleid, mask, how, core::ptr::null_mut())
}

/*
 * Generic routine for examining a user's privileges for an object,
 * with is_missing
 */
unsafe fn object_aclmask_ext(
    classid: Oid, objectid: Oid, roleid: Oid,
    mask: AclMode, how: AclMaskHow,
    is_missing: *mut bool,
) -> AclMode {
    let cacheid: c_int;
    let result: AclMode;
    let tuple: HeapTuple;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let acl: *mut Acl;
    let ownerId: Oid;

    /* Special cases */
    match classid {
        x if x == NamespaceRelationId => {
            return pg_namespace_aclmask_ext(objectid, roleid, mask, how, is_missing);
        }
        x if x == TypeRelationId => {
            return pg_type_aclmask_ext(objectid, roleid, mask, how, is_missing);
        }
        _ => {}
    }

    /* Even more special cases */
    Assert!(classid != RelationRelationId);           /* should use pg_class_acl* */
    Assert!(classid != LargeObjectMetadataRelationId); /* should use pg_largeobject_acl* */

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return mask;
    }

    /*
     * Get the object's ACL from its catalog
     */
    cacheid = get_object_catcache_oid(classid);

    tuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objectid));
    if !HeapTupleIsValid(tuple) {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return 0;
        } else {
            elog!(ERROR, "cache lookup failed for {} {}", cstr_to_str(get_object_class_descr(classid)), objectid);
        }
    }

    ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(
        cacheid, tuple, get_object_attnum_owner(classid),
    ));

    aclDatum = SysCacheGetAttr(
        cacheid, tuple, get_object_attnum_acl(classid), &mut isNull,
    );
    let acl_val: *mut Acl;
    if isNull {
        /* No ACL, so build default ACL */
        acl_val = acldefault(get_object_type(classid, objectid), ownerId);
        aclDatum = 0;
    } else {
        /* detoast ACL if necessary */
        acl_val = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl_val, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    ReleaseSysCache(tuple);

    result
}

/*
 * Routine for examining a user's privileges for a column
 */
unsafe fn pg_attribute_aclmask(
    table_oid: Oid, attnum: AttrNumber, roleid: Oid,
    mask: AclMode, how: AclMaskHow,
) -> AclMode {
    pg_attribute_aclmask_ext(table_oid, attnum, roleid, mask, how, core::ptr::null_mut())
}

/*
 * Routine for examining a user's privileges for a column, with is_missing
 */
unsafe fn pg_attribute_aclmask_ext(
    table_oid: Oid, attnum: AttrNumber, roleid: Oid,
    mask: AclMode, how: AclMaskHow, is_missing: *mut bool,
) -> AclMode {
    let result: AclMode;
    let classTuple: HeapTuple;
    let attTuple: HeapTuple;
    let classForm: Form_pg_class;
    let attributeForm: Form_pg_attribute;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let acl: *mut Acl;
    let ownerId: Oid;

    /*
     * First, get the column's ACL from its pg_attribute entry
     */
    attTuple = SearchSysCache2(
        ATTNUM,
        ObjectIdGetDatum(table_oid),
        Int16GetDatum(attnum),
    );
    if !HeapTupleIsValid(attTuple) {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("attribute {} of relation with OID {} does not exist",
                         attnum, table_oid));
        }
    }

    attributeForm = GETSTRUCT(attTuple) as Form_pg_attribute;

    /* Check dropped columns, too */
    if (*attributeForm).attisdropped {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            ReleaseSysCache(attTuple);
            return 0;
        } else {
            ereport!(ERROR, errmsg!("attribute {} of relation with OID {} does not exist",
                         attnum, table_oid));
        }
    }

    aclDatum = SysCacheGetAttr(ATTNUM, attTuple, Anum_pg_attribute_attacl, &mut isNull);

    /*
     * Here we hard-wire knowledge that the default ACL for a column grants no
     * privileges, so that we can fall out quickly in the very common case
     * where attacl is null.
     */
    if isNull {
        ReleaseSysCache(attTuple);
        return 0;
    }

    /*
     * Must get the relation's ownerId from pg_class.
     */
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if !HeapTupleIsValid(classTuple) {
        ReleaseSysCache(attTuple);
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("relation with OID {} does not exist", table_oid));
        }
    }
    classForm = GETSTRUCT(classTuple) as Form_pg_class;

    ownerId = (*classForm).relowner;

    ReleaseSysCache(classTuple);

    /* detoast column's ACL if necessary */
    let acl_val = DatumGetAclP(aclDatum);

    result = aclmask(acl_val, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    ReleaseSysCache(attTuple);

    result
}

/*
 * Exported routine for examining a user's privileges for a table
 */
#[no_mangle]
pub unsafe fn pg_class_aclmask(
    table_oid: Oid, roleid: Oid, mask: AclMode, how: AclMaskHow,
) -> AclMode {
    pg_class_aclmask_ext(table_oid, roleid, mask, how, core::ptr::null_mut())
}

/*
 * Routine for examining a user's privileges for a table, with is_missing
 */
unsafe fn pg_class_aclmask_ext(
    table_oid: Oid, roleid: Oid, mask: AclMode,
    how: AclMaskHow, is_missing: *mut bool,
) -> AclMode {
    let mut result: AclMode;
    let tuple: HeapTuple;
    let classForm: Form_pg_class;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let acl: *mut Acl;
    let ownerId: Oid;

    /*
     * Must get the relation's tuple from pg_class
     */
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if !HeapTupleIsValid(tuple) {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("relation with OID {} does not exist", table_oid));
        }
    }

    classForm = GETSTRUCT(tuple) as Form_pg_class;

    /*
     * Deny anyone permission to update a system catalog unless
     * pg_authid.rolsuper is set.
     */
    let mut mask_mut = mask;
    if (mask_mut & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE)) != 0
        && IsSystemClass(table_oid, classForm)
        && (*classForm).relkind != RELKIND_VIEW
        && !superuser_arg(roleid)
    {
        mask_mut &= !(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE);
    }

    /*
     * Otherwise, superusers bypass all permission-checking.
     */
    if std::env::var_os("PDB_AUTH").is_some() { eprintln!("PDB_AUTH aclmask_ext roleid={} mask={} mask_mut={} super={}", roleid, mask, mask_mut, superuser_arg(roleid)); }
    if superuser_arg(roleid) {
        ReleaseSysCache(tuple);
        return mask_mut;
    }

    /*
     * Normal case: get the relation's ACL from pg_class
     */
    ownerId = (*classForm).relowner;

    aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &mut isNull);
    let acl_val: *mut Acl;
    if isNull {
        /* No ACL, so build default ACL */
        match (*classForm).relkind {
            RELKIND_SEQUENCE => {
                acl_val = acldefault(OBJECT_SEQUENCE, ownerId);
            }
            _ => {
                acl_val = acldefault(OBJECT_TABLE, ownerId);
            }
        }
        aclDatum = 0;
    } else {
        /* detoast rel's ACL if necessary */
        acl_val = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl_val, roleid, ownerId, mask_mut, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    ReleaseSysCache(tuple);

    /*
     * Check if ACL_SELECT is being checked and, if so, and not set already as
     * part of the result, then check if the user is a member of the
     * pg_read_all_data role, which allows read access to all relations.
     */
    if mask_mut & ACL_SELECT != 0 && !(result & ACL_SELECT != 0)
        && has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA)
    {
        result |= ACL_SELECT;
    }

    /*
     * Check if ACL_INSERT, ACL_UPDATE, or ACL_DELETE is being checked.
     */
    if mask_mut & (ACL_INSERT | ACL_UPDATE | ACL_DELETE) != 0
        && !(result & (ACL_INSERT | ACL_UPDATE | ACL_DELETE) != 0)
        && has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA)
    {
        result |= mask_mut & (ACL_INSERT | ACL_UPDATE | ACL_DELETE);
    }

    /*
     * Check if ACL_MAINTAIN is being checked.
     */
    if mask_mut & ACL_MAINTAIN != 0
        && !(result & ACL_MAINTAIN != 0)
        && has_privs_of_role(roleid, ROLE_PG_MAINTAIN)
    {
        result |= ACL_MAINTAIN;
    }

    result
}

/*
 * Routine for examining a user's privileges for a configuration
 * parameter (GUC), identified by GUC name.
 */
unsafe fn pg_parameter_aclmask(
    name: *const c_char, roleid: Oid, mask: AclMode, how: AclMaskHow,
) -> AclMode {
    let result: AclMode;
    let parname: *mut c_char;
    let partext: *mut crate::c::text;
    let tuple: HeapTuple;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return mask;
    }

    /* Convert name to the form it should have in pg_parameter_acl... */
    parname = convert_GUC_name_for_parameter_acl(name);
    partext = cstring_to_text(parname);

    /* ... and look it up */
    tuple = SearchSysCache1(PARAMETERACLNAME, PointerGetDatum(partext as *const c_void));

    if !HeapTupleIsValid(tuple) {
        /* If no entry, GUC has no permissions for non-superusers */
        result = ACL_NO_RIGHTS;
    } else {
        let mut aclDatum: Datum;
        let mut isNull: bool = false;

        aclDatum = SysCacheGetAttr(
            PARAMETERACLNAME, tuple, Anum_pg_parameter_acl_paracl, &mut isNull,
        );
        let acl_val: *mut Acl;
        if isNull {
            /* No ACL, so build default ACL */
            acl_val = acldefault(OBJECT_PARAMETER_ACL, BOOTSTRAP_SUPERUSERID);
            aclDatum = 0;
        } else {
            /* detoast ACL if necessary */
            acl_val = DatumGetAclP(aclDatum);
        }

        result = aclmask(acl_val, roleid, BOOTSTRAP_SUPERUSERID, mask, how);

        /* if we have a detoasted copy, free it */
        if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
            pfree(acl_val as *mut c_void);
        }

        ReleaseSysCache(tuple);
    }

    pfree(parname as *mut c_void);
    pfree(partext as *mut c_void);

    result
}

/*
 * Routine for examining a user's privileges for a configuration
 * parameter (GUC), identified by the OID of its pg_parameter_acl entry.
 */
unsafe fn pg_parameter_acl_aclmask(
    acl_oid: Oid, roleid: Oid, mask: AclMode, how: AclMaskHow,
) -> AclMode {
    let result: AclMode;
    let tuple: HeapTuple;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return mask;
    }

    /* Get the ACL from pg_parameter_acl */
    tuple = SearchSysCache1(PARAMETERACLOID, ObjectIdGetDatum(acl_oid));
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!("parameter ACL with OID {} does not exist", acl_oid));
    }

    aclDatum = SysCacheGetAttr(
        PARAMETERACLOID, tuple, Anum_pg_parameter_acl_paracl, &mut isNull,
    );
    let acl_val: *mut Acl;
    if isNull {
        /* No ACL, so build default ACL */
        acl_val = acldefault(OBJECT_PARAMETER_ACL, BOOTSTRAP_SUPERUSERID);
        aclDatum = 0;
    } else {
        /* detoast ACL if necessary */
        acl_val = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl_val, roleid, BOOTSTRAP_SUPERUSERID, mask, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    ReleaseSysCache(tuple);

    result
}

/*
 * Routine for examining a user's privileges for a largeobject
 */
unsafe fn pg_largeobject_aclmask_snapshot(
    lobj_oid: Oid, roleid: Oid,
    mask: AclMode, how: AclMaskHow,
    snapshot: Snapshot,
) -> AclMode {
    let result: AclMode;
    let pg_lo_meta: Relation;
    let mut entry = [core::mem::zeroed::<ScanKeyData>(); 1];
    let scan: SysScanDesc;
    let tuple: HeapTuple;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let ownerId: Oid;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return mask;
    }

    /*
     * Get the largeobject's ACL from pg_largeobject_metadata
     */
    pg_lo_meta = table_open(LargeObjectMetadataRelationId, AccessShareLock);

    ScanKeyInit(
        &mut entry[0],
        Anum_pg_largeobject_metadata_oid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(lobj_oid),
    );

    scan = systable_beginscan(
        pg_lo_meta, LargeObjectMetadataOidIndexId, true,
        snapshot, 1, entry.as_mut_ptr(),
    );

    tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(ERROR, errmsg!("large object {} does not exist", lobj_oid));
    }

    ownerId = (*(GETSTRUCT(tuple) as Form_pg_largeobject_metadata)).lomowner;

    aclDatum = heap_getattr(
        tuple, Anum_pg_largeobject_metadata_lomacl as i32,
        RelationGetDescr(pg_lo_meta), &mut isNull,
    );

    let acl_val: *mut Acl;
    if isNull {
        /* No ACL, so build default ACL */
        acl_val = acldefault(OBJECT_LARGEOBJECT, ownerId);
        aclDatum = 0;
    } else {
        /* detoast ACL if necessary */
        acl_val = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl_val, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    systable_endscan(scan);

    table_close(pg_lo_meta, AccessShareLock);

    result
}

/*
 * Routine for examining a user's privileges for a namespace, with is_missing
 */
unsafe fn pg_namespace_aclmask_ext(
    nsp_oid: Oid, roleid: Oid,
    mask: AclMode, how: AclMaskHow,
    is_missing: *mut bool,
) -> AclMode {
    let mut result: AclMode;
    let tuple: HeapTuple;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let ownerId: Oid;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return mask;
    }

    /*
     * If we have been assigned this namespace as a temp namespace, check to
     * make sure we have CREATE TEMP permission on the database.
     */
    if isTempNamespace(nsp_oid) {
        if object_aclcheck_ext(DatabaseRelationId, MyDatabaseId, roleid,
                                ACL_CREATE_TEMP, is_missing) == ACLCHECK_OK
        {
            return mask & ACL_ALL_RIGHTS_SCHEMA;
        } else {
            return mask & ACL_USAGE;
        }
    }

    /*
     * Get the schema's ACL from pg_namespace
     */
    tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
    if !HeapTupleIsValid(tuple) {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("schema with OID {} does not exist", nsp_oid));
        }
    }

    ownerId = (*(GETSTRUCT(tuple) as Form_pg_namespace)).nspowner;

    aclDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspacl, &mut isNull);
    let acl_val: *mut Acl;
    if isNull {
        /* No ACL, so build default ACL */
        acl_val = acldefault(OBJECT_SCHEMA, ownerId);
        aclDatum = 0;
    } else {
        /* detoast ACL if necessary */
        acl_val = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl_val, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    ReleaseSysCache(tuple);

    /*
     * Check if ACL_USAGE is being checked and, if so, and not set already as
     * part of the result, then check if the user is a member of the
     * pg_read_all_data or pg_write_all_data roles.
     */
    if mask & ACL_USAGE != 0 && !(result & ACL_USAGE != 0)
        && (has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA)
            || has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA))
    {
        result |= ACL_USAGE;
    }
    result
}

/*
 * Routine for examining a user's privileges for a type, with is_missing
 */
unsafe fn pg_type_aclmask_ext(
    type_oid: Oid, roleid: Oid, mask: AclMode, how: AclMaskHow,
    is_missing: *mut bool,
) -> AclMode {
    let result: AclMode;
    let mut tuple: HeapTuple;
    let mut typeForm: Form_pg_type;
    let mut aclDatum: Datum;
    let mut isNull: bool = false;
    let ownerId: Oid;

    /* Bypass permission checks for superusers */
    if superuser_arg(roleid) {
        return mask;
    }

    /*
     * Must get the type's tuple from pg_type
     */
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if !HeapTupleIsValid(tuple) {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("type with OID {} does not exist", type_oid));
        }
    }
    typeForm = GETSTRUCT(tuple) as Form_pg_type;

    /*
     * "True" array types don't manage permissions of their own; consult the
     * element type instead.
     */
    if IsTrueArrayType(typeForm) {
        let elttype_oid: Oid = (*typeForm).typelem;

        ReleaseSysCache(tuple);

        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(elttype_oid));
        if !HeapTupleIsValid(tuple) {
            if !is_missing.is_null() {
                /* return "no privileges" instead of throwing an error */
                *is_missing = true;
                return 0;
            } else {
                ereport!(ERROR, errmsg!("type with OID {} does not exist", elttype_oid));
            }
        }
        typeForm = GETSTRUCT(tuple) as Form_pg_type;
    }

    /*
     * Likewise, multirange types don't manage their own permissions.
     */
    if (*typeForm).typtype == TYPTYPE_MULTIRANGE {
        let rangetype: Oid = get_multirange_range((*typeForm).oid);

        ReleaseSysCache(tuple);

        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(rangetype));
        if !HeapTupleIsValid(tuple) {
            if !is_missing.is_null() {
                /* return "no privileges" instead of throwing an error */
                *is_missing = true;
                return 0;
            } else {
                ereport!(ERROR, errmsg!("type with OID {} does not exist", rangetype));
            }
        }
        typeForm = GETSTRUCT(tuple) as Form_pg_type;
    }

    /*
     * Now get the type's owner and ACL from the tuple
     */
    ownerId = (*typeForm).typowner;

    aclDatum = SysCacheGetAttr(TYPEOID, tuple, Anum_pg_type_typacl, &mut isNull);
    let acl_val: *mut Acl;
    if isNull {
        /* No ACL, so build default ACL */
        acl_val = acldefault(OBJECT_TYPE, ownerId);
        aclDatum = 0;
    } else {
        /* detoast rel's ACL if necessary */
        acl_val = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl_val, roleid, ownerId, mask, how);

    /* if we have a detoasted copy, free it */
    if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
        pfree(acl_val as *mut c_void);
    }

    ReleaseSysCache(tuple);

    result
}

/*
 * Exported generic routine for checking a user's access privileges to an object
 */
#[no_mangle]
pub unsafe fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: AclMode) -> AclResult {
    object_aclcheck_ext(classid, objectid, roleid, mode, core::ptr::null_mut())
}

/*
 * Exported generic routine for checking a user's access privileges to an
 * object, with is_missing
 */
#[no_mangle]
pub unsafe fn object_aclcheck_ext(
    classid: Oid, objectid: Oid, roleid: Oid, mode: AclMode, is_missing: *mut bool,
) -> AclResult {
    if object_aclmask_ext(classid, objectid, roleid, mode, ACLMASK_ANY, is_missing) != 0 {
        ACLCHECK_OK
    } else {
        ACLCHECK_NO_PRIV
    }
}

/*
 * Exported routine for checking a user's access privileges to a column
 */
#[no_mangle]
pub unsafe fn pg_attribute_aclcheck(
    table_oid: Oid, attnum: AttrNumber, roleid: Oid, mode: AclMode,
) -> AclResult {
    pg_attribute_aclcheck_ext(table_oid, attnum, roleid, mode, core::ptr::null_mut())
}


/*
 * Exported routine for checking a user's access privileges to a column,
 * with is_missing
 */
#[no_mangle]
pub unsafe fn pg_attribute_aclcheck_ext(
    table_oid: Oid, attnum: AttrNumber, roleid: Oid, mode: AclMode, is_missing: *mut bool,
) -> AclResult {
    if pg_attribute_aclmask_ext(table_oid, attnum, roleid, mode, ACLMASK_ANY, is_missing) != 0 {
        ACLCHECK_OK
    } else {
        ACLCHECK_NO_PRIV
    }
}

/*
 * Exported routine for checking a user's access privileges to any/all columns
 */
#[no_mangle]
pub unsafe fn pg_attribute_aclcheck_all(
    table_oid: Oid, roleid: Oid, mode: AclMode, how: AclMaskHow,
) -> AclResult {
    pg_attribute_aclcheck_all_ext(table_oid, roleid, mode, how, core::ptr::null_mut())
}

/*
 * Exported routine for checking a user's access privileges to any/all columns,
 * with is_missing
 */
#[no_mangle]
pub unsafe fn pg_attribute_aclcheck_all_ext(
    table_oid: Oid, roleid: Oid,
    mode: AclMode, how: AclMaskHow,
    is_missing: *mut bool,
) -> AclResult {
    let mut result: AclResult;
    let classTuple: HeapTuple;
    let classForm: Form_pg_class;
    let ownerId: Oid;
    let nattrs: AttrNumber;
    let mut curr_att: AttrNumber;

    /*
     * Must fetch pg_class row to get owner ID and number of attributes.
     */
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if !HeapTupleIsValid(classTuple) {
        if !is_missing.is_null() {
            /* return "no privileges" instead of throwing an error */
            *is_missing = true;
            return ACLCHECK_NO_PRIV;
        } else {
            ereport!(ERROR, errmsg!("relation with OID {} does not exist", table_oid));
        }
    }
    classForm = GETSTRUCT(classTuple) as Form_pg_class;

    ownerId = (*classForm).relowner;
    nattrs = (*classForm).relnatts as AttrNumber;

    ReleaseSysCache(classTuple);

    /*
     * Initialize result in case there are no non-dropped columns.
     */
    result = ACLCHECK_NO_PRIV;

    curr_att = 1;
    while curr_att <= nattrs {
        let attTuple: HeapTuple;
        let mut aclDatum: Datum;
        let mut isNull: bool = false;
        let attmask: AclMode;

        attTuple = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(table_oid),
            Int16GetDatum(curr_att),
        );

        /*
         * Lookup failure probably indicates that the table was just dropped.
         */
        if !HeapTupleIsValid(attTuple) {
            curr_att += 1;
            continue;
        }

        /* ignore dropped columns */
        if (*(GETSTRUCT(attTuple) as Form_pg_attribute)).attisdropped {
            ReleaseSysCache(attTuple);
            curr_att += 1;
            continue;
        }

        aclDatum = SysCacheGetAttr(
            ATTNUM, attTuple, Anum_pg_attribute_attacl, &mut isNull,
        );

        /*
         * Here we hard-wire knowledge that the default ACL for a column
         * grants no privileges.
         */
        if isNull {
            attmask = 0;
        } else {
            /* detoast column's ACL if necessary */
            let acl_val = DatumGetAclP(aclDatum);
            attmask = aclmask(acl_val, roleid, ownerId, mode, ACLMASK_ANY);
            /* if we have a detoasted copy, free it */
            if !acl_val.is_null() && (acl_val as *mut c_void) != DatumGetPointer(aclDatum) as *mut c_void {
                pfree(acl_val as *mut c_void);
            }
        }

        ReleaseSysCache(attTuple);

        if attmask != 0 {
            result = ACLCHECK_OK;
            if how == ACLMASK_ANY {
                break;   /* succeed on any success */
            }
        } else {
            result = ACLCHECK_NO_PRIV;
            if how == ACLMASK_ALL {
                break;   /* fail on any failure */
            }
        }
        curr_att += 1;
    }

    result
}

/*
 * Exported routine for checking a user's access privileges to a table
 */
#[no_mangle]
pub unsafe fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: AclMode) -> AclResult {
    pg_class_aclcheck_ext(table_oid, roleid, mode, core::ptr::null_mut())
}

/*
 * Exported routine for checking a user's access privileges to a table,
 * with is_missing
 */
#[no_mangle]
pub unsafe fn pg_class_aclcheck_ext(
    table_oid: Oid, roleid: Oid, mode: AclMode, is_missing: *mut bool,
) -> AclResult {
    if pg_class_aclmask_ext(table_oid, roleid, mode, ACLMASK_ANY, is_missing) != 0 {
        ACLCHECK_OK
    } else {
        ACLCHECK_NO_PRIV
    }
}

/*
 * Exported routine for checking a user's access privileges to a configuration
 * parameter (GUC), identified by GUC name.
 */
#[no_mangle]
pub unsafe fn pg_parameter_aclcheck(name: *const c_char, roleid: Oid, mode: AclMode) -> AclResult {
    if pg_parameter_aclmask(name, roleid, mode, ACLMASK_ANY) != 0 {
        ACLCHECK_OK
    } else {
        ACLCHECK_NO_PRIV
    }
}

/*
 * Exported routine for checking a user's access privileges to a largeobject
 */
#[no_mangle]
pub unsafe fn pg_largeobject_aclcheck_snapshot(
    lobj_oid: Oid, roleid: Oid, mode: AclMode, snapshot: Snapshot,
) -> AclResult {
    if pg_largeobject_aclmask_snapshot(lobj_oid, roleid, mode, ACLMASK_ANY, snapshot) != 0 {
        ACLCHECK_OK
    } else {
        ACLCHECK_NO_PRIV
    }
}

/*
 * Generic ownership check for an object
 */
#[no_mangle]
pub unsafe fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool {
    let cacheid: c_int;
    let ownerId: Oid;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return true;
    }

    /* For large objects, the catalog to consult is pg_largeobject_metadata */
    let classid_resolved = if classid == LargeObjectRelationId {
        LargeObjectMetadataRelationId
    } else {
        classid
    };

    cacheid = get_object_catcache_oid(classid_resolved);
    if cacheid != -1 {
        /* we can get the object's tuple from the syscache */
        let tuple: HeapTuple;

        tuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objectid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for {} {}", cstr_to_str(get_object_class_descr(classid_resolved)), objectid);
        }

        ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(
            cacheid, tuple, get_object_attnum_owner(classid_resolved),
        ));
        ReleaseSysCache(tuple);
    } else {
        /* for catalogs without an appropriate syscache */
        let rel: Relation;
        let mut entry = [core::mem::zeroed::<ScanKeyData>(); 1];
        let scan: SysScanDesc;
        let tuple: HeapTuple;
        let mut isnull: bool = false;

        rel = table_open(classid_resolved, AccessShareLock);

        ScanKeyInit(
            &mut entry[0],
            get_object_attnum_oid(classid_resolved),
            BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(objectid),
        );

        scan = systable_beginscan(
            rel, get_object_oid_index(classid_resolved), true,
            core::ptr::null_mut(), 1, entry.as_mut_ptr(),
        );

        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "could not find tuple for {} {}", cstr_to_str(get_object_class_descr(classid_resolved)), objectid);
        }

        ownerId = DatumGetObjectId(heap_getattr(
            tuple, get_object_attnum_owner(classid_resolved) as i32,
            RelationGetDescr(rel), &mut isnull,
        ));
        Assert!(!isnull);

        systable_endscan(scan);
        table_close(rel, AccessShareLock);
    }

    has_privs_of_role(roleid, ownerId)
}

/*
 * Check whether specified role has CREATEROLE privilege (or is a superuser)
 */
#[no_mangle]
pub unsafe fn has_createrole_privilege(roleid: Oid) -> bool {
    let mut result: bool = false;
    let utup: HeapTuple;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return true;
    }

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if HeapTupleIsValid(utup) {
        result = (*(GETSTRUCT(utup) as Form_pg_authid)).rolcreaterole;
        ReleaseSysCache(utup);
    }
    result
}

#[no_mangle]
pub unsafe fn has_bypassrls_privilege(roleid: Oid) -> bool {
    let mut result: bool = false;
    let utup: HeapTuple;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return true;
    }

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if HeapTupleIsValid(utup) {
        result = (*(GETSTRUCT(utup) as Form_pg_authid)).rolbypassrls;
        ReleaseSysCache(utup);
    }
    result
}

/*
 * Fetch pg_default_acl entry for given role, namespace and object type
 * Returns NULL if no such entry.
 */
unsafe fn get_default_acl_internal(roleId: Oid, nsp_oid: Oid, objtype: c_char) -> *mut Acl {
    let mut result: *mut Acl = core::ptr::null_mut();
    let tuple: HeapTuple;

    tuple = SearchSysCache3(
        DEFACLROLENSPOBJ,
        ObjectIdGetDatum(roleId),
        ObjectIdGetDatum(nsp_oid),
        CharGetDatum(objtype),
    );

    if HeapTupleIsValid(tuple) {
        let mut aclDatum: Datum;
        let mut isNull: bool = false;

        aclDatum = SysCacheGetAttr(
            DEFACLROLENSPOBJ, tuple,
            Anum_pg_default_acl_defaclacl,
            &mut isNull,
        );
        if !isNull {
            result = DatumGetAclPCopy(aclDatum);
        }
        ReleaseSysCache(tuple);
    }

    result
}

/*
 * Get default permissions for newly created object within given schema
 *
 * Returns NULL if built-in system defaults should be used.
 */
#[no_mangle]
pub unsafe fn get_user_default_acl(objtype: ObjectType, ownerId: Oid, nsp_oid: Oid) -> *mut Acl {
    let mut result: *mut Acl;
    let mut glob_acl: *mut Acl;
    let schema_acl: *mut Acl;
    let def_acl: *mut Acl;
    let defaclobjtype: c_char;

    /*
     * Use NULL during bootstrap, since pg_default_acl probably isn't there yet.
     */
    if IsBootstrapProcessingMode() {
        return core::ptr::null_mut();
    }

    /* Check if object type is supported in pg_default_acl */
    match objtype {
        OBJECT_TABLE    => { defaclobjtype = DEFACLOBJ_RELATION; }
        OBJECT_SEQUENCE => { defaclobjtype = DEFACLOBJ_SEQUENCE; }
        OBJECT_FUNCTION => { defaclobjtype = DEFACLOBJ_FUNCTION; }
        OBJECT_TYPE     => { defaclobjtype = DEFACLOBJ_TYPE; }
        OBJECT_SCHEMA   => { defaclobjtype = DEFACLOBJ_NAMESPACE; }
        OBJECT_LARGEOBJECT => { defaclobjtype = DEFACLOBJ_LARGEOBJECT; }
        _ => { return core::ptr::null_mut(); }
    }

    /* Look up the relevant pg_default_acl entries */
    glob_acl   = get_default_acl_internal(ownerId, InvalidOid, defaclobjtype);
    schema_acl = get_default_acl_internal(ownerId, nsp_oid, defaclobjtype);

    /* Quick out if neither entry exists */
    if glob_acl.is_null() && schema_acl.is_null() {
        return core::ptr::null_mut();
    }

    /* We need to know the hard-wired default value, too */
    def_acl = acldefault(objtype, ownerId);

    /* If there's no global entry, substitute the hard-wired default */
    if glob_acl.is_null() {
        glob_acl = def_acl;
    }

    /* Merge in any per-schema privileges */
    result = aclmerge(glob_acl, schema_acl, ownerId);

    /*
     * For efficiency, we want to return NULL if the result equals default.
     */
    aclitemsort(result);
    aclitemsort(def_acl);
    if aclequal(result, def_acl) {
        result = core::ptr::null_mut();
    }

    result
}

/*
 * Record dependencies on roles mentioned in a new object's ACL.
 */
#[no_mangle]
pub unsafe fn recordDependencyOnNewAcl(
    classId: Oid, objectId: Oid, objsubId: i32,
    ownerId: Oid, acl: *mut Acl,
) {
    let nmembers: c_int;
    let mut members: *mut Oid = core::ptr::null_mut();

    /* Nothing to do if ACL is defaulted */
    if acl.is_null() {
        return;
    }

    /* Extract roles mentioned in ACL */
    nmembers = aclmembers(acl, &mut members);

    /* Update the shared dependency ACL info */
    updateAclDependencies(
        classId, objectId, objsubId,
        ownerId,
        0, core::ptr::null_mut(),
        nmembers, members,
    );
}

/*
 * Record initial privileges for the top-level object passed in.
 */
#[no_mangle]
pub unsafe fn recordExtObjInitPriv(objoid: Oid, classoid: Oid) {
    /*
     * pg_class / pg_attribute
     */
    if classoid == RelationRelationId {
        let pg_class_tuple: Form_pg_class;
        let mut aclDatum: Datum;
        let mut isNull: bool = false;
        let tuple: HeapTuple;

        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objoid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", objoid);
        }
        pg_class_tuple = GETSTRUCT(tuple) as Form_pg_class;

        /*
         * Indexes don't have permissions, neither do the pg_class rows for
         * composite types.
         */
        if (*pg_class_tuple).relkind == RELKIND_INDEX
            || (*pg_class_tuple).relkind == RELKIND_PARTITIONED_INDEX
            || (*pg_class_tuple).relkind == RELKIND_COMPOSITE_TYPE
        {
            ReleaseSysCache(tuple);
            return;
        }

        /*
         * If this isn't a sequence then it's possibly going to have
         * column-level ACLs associated with it.
         */
        if (*pg_class_tuple).relkind != RELKIND_SEQUENCE {
            let mut curr_att: AttrNumber;
            let nattrs: AttrNumber = (*pg_class_tuple).relnatts as AttrNumber;

            curr_att = 1;
            while curr_att <= nattrs {
                let attTuple: HeapTuple;
                let mut attaclDatum: Datum;
                let mut isNullAtt: bool = false;

                attTuple = SearchSysCache2(
                    ATTNUM,
                    ObjectIdGetDatum(objoid),
                    Int16GetDatum(curr_att),
                );

                if !HeapTupleIsValid(attTuple) {
                    curr_att += 1;
                    continue;
                }

                /* ignore dropped columns */
                if (*(GETSTRUCT(attTuple) as Form_pg_attribute)).attisdropped {
                    ReleaseSysCache(attTuple);
                    curr_att += 1;
                    continue;
                }

                attaclDatum = SysCacheGetAttr(
                    ATTNUM, attTuple,
                    Anum_pg_attribute_attacl,
                    &mut isNullAtt,
                );

                /* no need to do anything for a NULL ACL */
                if isNullAtt {
                    ReleaseSysCache(attTuple);
                    curr_att += 1;
                    continue;
                }

                recordExtensionInitPrivWorker(
                    objoid, classoid, curr_att as c_int,
                    DatumGetAclP(attaclDatum),
                );

                ReleaseSysCache(attTuple);
                curr_att += 1;
            }
        }

        aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &mut isNull);

        /* Add the record, if any, for the top-level object */
        if !isNull {
            recordExtensionInitPrivWorker(objoid, classoid, 0, DatumGetAclP(aclDatum));
        }

        ReleaseSysCache(tuple);
    } else if classoid == LargeObjectRelationId {
        /* For large objects, we must consult pg_largeobject_metadata */
        let mut aclDatum: Datum;
        let mut isNull: bool = false;
        let tuple: HeapTuple;
        let mut entry = [core::mem::zeroed::<ScanKeyData>(); 1];
        let scan: SysScanDesc;
        let relation: Relation;

        /*
         * Note: this is dead code, given that we don't allow large objects to
         * be made extension members.
         */
        relation = table_open(LargeObjectMetadataRelationId, RowExclusiveLock);

        /* There's no syscache for pg_largeobject_metadata */
        ScanKeyInit(
            &mut entry[0],
            Anum_pg_largeobject_metadata_oid,
            BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(objoid),
        );

        scan = systable_beginscan(
            relation, LargeObjectMetadataOidIndexId, true,
            core::ptr::null_mut(), 1, entry.as_mut_ptr(),
        );

        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "could not find tuple for large object {}", objoid);
        }

        aclDatum = heap_getattr(
            tuple, Anum_pg_largeobject_metadata_lomacl as i32,
            RelationGetDescr(relation), &mut isNull,
        );

        /* Add the record, if any, for the top-level object */
        if !isNull {
            recordExtensionInitPrivWorker(objoid, classoid, 0, DatumGetAclP(aclDatum));
        }

        systable_endscan(scan);
    /* This will error on unsupported classoid. */
    } else if get_object_attnum_acl(classoid) != InvalidAttrNumber {
        let cacheid: c_int;
        let mut aclDatum: Datum;
        let mut isNull: bool = false;
        let tuple: HeapTuple;

        cacheid = get_object_catcache_oid(classoid);
        tuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objoid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for {} {}", cstr_to_str(get_object_class_descr(classoid)), objoid);
        }

        aclDatum = SysCacheGetAttr(
            cacheid, tuple, get_object_attnum_acl(classoid), &mut isNull,
        );

        /* Add the record, if any, for the top-level object */
        if !isNull {
            recordExtensionInitPrivWorker(objoid, classoid, 0, DatumGetAclP(aclDatum));
        }

        ReleaseSysCache(tuple);
    }
}

/*
 * For the object passed in, remove its ACL and the ACLs of any object subIds
 * from pg_init_privs.
 */
#[no_mangle]
pub unsafe fn removeExtObjInitPriv(objoid: Oid, classoid: Oid) {
    /*
     * If this is a relation then we need to see if there are any sub-objects.
     */
    if classoid == RelationRelationId {
        let pg_class_tuple: Form_pg_class;
        let tuple: HeapTuple;

        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objoid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", objoid);
        }
        pg_class_tuple = GETSTRUCT(tuple) as Form_pg_class;

        if (*pg_class_tuple).relkind == RELKIND_INDEX
            || (*pg_class_tuple).relkind == RELKIND_PARTITIONED_INDEX
            || (*pg_class_tuple).relkind == RELKIND_COMPOSITE_TYPE
        {
            ReleaseSysCache(tuple);
            return;
        }

        if (*pg_class_tuple).relkind != RELKIND_SEQUENCE {
            let mut curr_att: AttrNumber;
            let nattrs: AttrNumber = (*pg_class_tuple).relnatts as AttrNumber;

            curr_att = 1;
            while curr_att <= nattrs {
                let attTuple: HeapTuple;
                attTuple = SearchSysCache2(
                    ATTNUM,
                    ObjectIdGetDatum(objoid),
                    Int16GetDatum(curr_att),
                );

                if !HeapTupleIsValid(attTuple) {
                    curr_att += 1;
                    continue;
                }

                /* when removing, remove all entries, even dropped columns */
                recordExtensionInitPrivWorker(objoid, classoid, curr_att as c_int, core::ptr::null_mut());

                ReleaseSysCache(attTuple);
                curr_att += 1;
            }
        }

        ReleaseSysCache(tuple);
    }

    /* Remove the record, if any, for the top-level object */
    recordExtensionInitPrivWorker(objoid, classoid, 0, core::ptr::null_mut());
}

/*
 * Record initial ACL for an extension object
 */
unsafe fn recordExtensionInitPriv(objoid: Oid, classoid: Oid, objsubid: c_int, new_acl: *mut Acl) {
    /*
     * Generally, we only record the initial privileges when an extension is
     * being created.
     */
    if !creating_extension && !binary_upgrade_record_init_privs {
        return;
    }

    recordExtensionInitPrivWorker(objoid, classoid, objsubid, new_acl);
}

/*
 * Record initial ACL for an extension object, worker.
 */
unsafe fn recordExtensionInitPrivWorker(objoid: Oid, classoid: Oid, objsubid: c_int, new_acl: *mut Acl) {
    let relation: Relation;
    let mut key = [core::mem::zeroed::<ScanKeyData>(); 3];
    let scan: SysScanDesc;
    let mut oldtuple: HeapTuple;
    let mut noldmembers: c_int = 0;
    let mut nnewmembers: c_int = 0;
    let mut oldmembers: *mut Oid = core::ptr::null_mut();
    let mut newmembers: *mut Oid = core::ptr::null_mut();

    /* We'll need the role membership of the new ACL. */
    nnewmembers = aclmembers(new_acl, &mut newmembers);

    /* Search pg_init_privs for an existing entry. */
    relation = table_open(InitPrivsRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_init_privs_objoid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(objoid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_init_privs_classoid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(classoid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_init_privs_objsubid,
        BTEqualStrategyNumber, F_INT4EQ,
        Int32GetDatum(objsubid),
    );

    scan = systable_beginscan(
        relation, InitPrivsObjIndexId, true,
        core::ptr::null_mut(), 3, key.as_mut_ptr(),
    );

    /* There should exist only one entry or none. */
    oldtuple = systable_getnext(scan);

    /* If we find an entry, update it with the latest ACL. */
    if HeapTupleIsValid(oldtuple) {
        let mut values = [0 as Datum; Natts_pg_init_privs];
        let mut nulls = [false; Natts_pg_init_privs];
        let mut replace = [false; Natts_pg_init_privs];
        let mut oldAclDatum: Datum;
        let mut isNull: bool = false;
        let old_acl: *mut Acl;

        /* Update pg_shdepend for roles mentioned in the old/new ACLs. */
        oldAclDatum = heap_getattr(
            oldtuple, Anum_pg_init_privs_initprivs as i32,
            RelationGetDescr(relation), &mut isNull,
        );
        Assert!(!isNull);
        old_acl = DatumGetAclP(oldAclDatum);
        noldmembers = aclmembers(old_acl, &mut oldmembers);

        updateInitAclDependencies(
            classoid, objoid, objsubid,
            noldmembers, oldmembers,
            nnewmembers, newmembers,
        );

        /* If we have a new ACL to set, then update the row with it. */
        if !new_acl.is_null() && ACL_NUM(new_acl) != 0 {
            values[Anum_pg_init_privs_initprivs as usize - 1] = PointerGetDatum(new_acl as *const c_void);
            replace[Anum_pg_init_privs_initprivs as usize - 1] = true;

            oldtuple = heap_modify_tuple(
                oldtuple, RelationGetDescr(relation),
                values.as_mut_ptr(), nulls.as_mut_ptr(), replace.as_mut_ptr(),
            );

            CatalogTupleUpdate(relation, &mut (*oldtuple).t_self, oldtuple);
        } else {
            /* new_acl is NULL/empty, so delete the entry we found. */
            CatalogTupleDelete(relation, &mut (*oldtuple).t_self as *mut _);
        }
    } else {
        let mut values = [0 as Datum; Natts_pg_init_privs];
        let mut nulls = [false; Natts_pg_init_privs];

        /*
         * Only add a new entry if the new ACL is non-NULL.
         */
        if !new_acl.is_null() && ACL_NUM(new_acl) != 0 {
            /* No entry found, so add it. */
            values[Anum_pg_init_privs_objoid as usize - 1]   = ObjectIdGetDatum(objoid);
            values[Anum_pg_init_privs_classoid as usize - 1] = ObjectIdGetDatum(classoid);
            values[Anum_pg_init_privs_objsubid as usize - 1] = Int32GetDatum(objsubid);

            /* This function only handles initial privileges of extensions */
            values[Anum_pg_init_privs_privtype as usize - 1] = CharGetDatum(INITPRIVS_EXTENSION);

            values[Anum_pg_init_privs_initprivs as usize - 1] = PointerGetDatum(new_acl as *const c_void);

            let tuple = heap_form_tuple(
                RelationGetDescr(relation), values.as_mut_ptr(), nulls.as_mut_ptr(),
            );

            CatalogTupleInsert(relation, tuple);

            /* Update pg_shdepend, too. */
            noldmembers = 0;
            oldmembers = core::ptr::null_mut();

            updateInitAclDependencies(
                classoid, objoid, objsubid,
                noldmembers, oldmembers,
                nnewmembers, newmembers,
            );
        }
    }

    systable_endscan(scan);

    /* prevent error when processing objects multiple times */
    CommandCounterIncrement();

    table_close(relation, RowExclusiveLock);
}

/*
 * ReplaceRoleInInitPriv
 *
 * Used by shdepReassignOwned to replace mentions of a role in pg_init_privs.
 */
#[no_mangle]
pub unsafe fn ReplaceRoleInInitPriv(
    oldroleid: Oid, newroleid: Oid,
    classid: Oid, objid: Oid, objsubid: i32,
) {
    let rel: Relation;
    let mut key = [core::mem::zeroed::<ScanKeyData>(); 3];
    let scan: SysScanDesc;
    let oldtuple: HeapTuple;
    let mut oldAclDatum: Datum;
    let mut isNull: bool = false;
    let old_acl: *mut Acl;
    let new_acl: *mut Acl;
    let newtuple: HeapTuple;
    let noldmembers: c_int;
    let nnewmembers: c_int;
    let mut oldmembers: *mut Oid = core::ptr::null_mut();
    let mut newmembers: *mut Oid = core::ptr::null_mut();

    /* Search for existing pg_init_privs entry for the target object. */
    rel = table_open(InitPrivsRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_init_privs_objoid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(objid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_init_privs_classoid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(classid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_init_privs_objsubid,
        BTEqualStrategyNumber, F_INT4EQ,
        Int32GetDatum(objsubid),
    );

    scan = systable_beginscan(rel, InitPrivsObjIndexId, true, core::ptr::null_mut(), 3, key.as_mut_ptr());

    /* There should exist only one entry or none. */
    oldtuple = systable_getnext(scan);

    if !HeapTupleIsValid(oldtuple) {
        /*
         * Hmm, why are we here if there's no entry?  But pack up and go away quietly.
         */
        systable_endscan(scan);
        table_close(rel, RowExclusiveLock);
        return;
    }

    /* Get a writable copy of the existing ACL. */
    oldAclDatum = heap_getattr(
        oldtuple, Anum_pg_init_privs_initprivs as i32,
        RelationGetDescr(rel), &mut isNull,
    );
    Assert!(!isNull);
    old_acl = DatumGetAclPCopy(oldAclDatum);

    /*
     * Generate new ACL.
     */
    new_acl = aclnewowner(old_acl, oldroleid, newroleid);

    /*
     * If we end with an empty ACL, delete the pg_init_privs entry.
     */
    if new_acl.is_null() || ACL_NUM(new_acl) == 0 {
        CatalogTupleDelete(rel, &mut (*oldtuple).t_self as *mut _);
    } else {
        let mut values = [0 as Datum; Natts_pg_init_privs];
        let mut nulls = [false; Natts_pg_init_privs];
        let mut replaces = [false; Natts_pg_init_privs];

        /* Update existing entry. */
        values[Anum_pg_init_privs_initprivs as usize - 1] = PointerGetDatum(new_acl as *const c_void);
        replaces[Anum_pg_init_privs_initprivs as usize - 1] = true;

        let newtuple = heap_modify_tuple(
            oldtuple, RelationGetDescr(rel),
            values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
        );
        CatalogTupleUpdate(rel, &mut (*newtuple).t_self, newtuple);
    }

    /*
     * Update the shared dependency ACL info.
     */
    noldmembers = aclmembers(old_acl, &mut oldmembers);
    nnewmembers = aclmembers(new_acl, &mut newmembers);

    updateInitAclDependencies(
        classid, objid, objsubid,
        noldmembers, oldmembers,
        nnewmembers, newmembers,
    );

    systable_endscan(scan);

    /* prevent error when processing objects multiple times */
    CommandCounterIncrement();

    table_close(rel, RowExclusiveLock);
}

/*
 * RemoveRoleFromInitPriv
 *
 * Used by shdepDropOwned to remove mentions of a role in pg_init_privs.
 */
#[no_mangle]
pub unsafe fn RemoveRoleFromInitPriv(roleid: Oid, classid: Oid, objid: Oid, objsubid: i32) {
    let rel: Relation;
    let mut key = [core::mem::zeroed::<ScanKeyData>(); 3];
    let scan: SysScanDesc;
    let oldtuple: HeapTuple;
    let cacheid: c_int;
    let objtuple: HeapTuple;
    let ownerId: Oid;
    let mut oldAclDatum: Datum;
    let mut isNull: bool = false;
    let old_acl: *mut Acl;
    let mut new_acl: *mut Acl;
    let newtuple: HeapTuple;
    let noldmembers: c_int;
    let nnewmembers: c_int;
    let mut oldmembers: *mut Oid = core::ptr::null_mut();
    let mut newmembers: *mut Oid = core::ptr::null_mut();

    /* Search for existing pg_init_privs entry for the target object. */
    rel = table_open(InitPrivsRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_init_privs_objoid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(objid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_init_privs_classoid,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(classid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_init_privs_objsubid,
        BTEqualStrategyNumber, F_INT4EQ,
        Int32GetDatum(objsubid),
    );

    scan = systable_beginscan(rel, InitPrivsObjIndexId, true, core::ptr::null_mut(), 3, key.as_mut_ptr());

    /* There should exist only one entry or none. */
    oldtuple = systable_getnext(scan);

    if !HeapTupleIsValid(oldtuple) {
        /*
         * Hmm, why are we here if there's no entry?  But pack up and go away quietly.
         */
        systable_endscan(scan);
        table_close(rel, RowExclusiveLock);
        return;
    }

    /* Get a writable copy of the existing ACL. */
    oldAclDatum = heap_getattr(
        oldtuple, Anum_pg_init_privs_initprivs as i32,
        RelationGetDescr(rel), &mut isNull,
    );
    Assert!(!isNull);
    old_acl = DatumGetAclPCopy(oldAclDatum);

    /*
     * We need the members of both old and new ACLs.
     */
    noldmembers = aclmembers(old_acl, &mut oldmembers);

    /* Must find out the owner's OID the hard way. */
    cacheid = get_object_catcache_oid(classid);
    objtuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objid));
    if !HeapTupleIsValid(objtuple) {
        elog!(ERROR, "cache lookup failed for {} {}", cstr_to_str(get_object_class_descr(classid)), objid);
    }

    ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(
        cacheid, objtuple, get_object_attnum_owner(classid),
    ));
    ReleaseSysCache(objtuple);

    /*
     * Generate new ACL.  Grantor of rights is always the same as the owner.
     */
    if !old_acl.is_null() {
        new_acl = merge_acl_with_grant(
            old_acl,
            false,           /* is_grant */
            false,           /* grant_option */
            DROP_RESTRICT,
            crate::list_make1_oid!(roleid),
            ACLITEM_ALL_PRIV_BITS,
            ownerId,
            ownerId,
        );
    } else {
        new_acl = core::ptr::null_mut();   /* this case shouldn't happen, probably */
    }

    /* If we end with an empty ACL, delete the pg_init_privs entry. */
    if new_acl.is_null() || ACL_NUM(new_acl) == 0 {
        CatalogTupleDelete(rel, &mut (*oldtuple).t_self as *mut _);
    } else {
        let mut values = [0 as Datum; Natts_pg_init_privs];
        let mut nulls = [false; Natts_pg_init_privs];
        let mut replaces = [false; Natts_pg_init_privs];

        /* Update existing entry. */
        values[Anum_pg_init_privs_initprivs as usize - 1] = PointerGetDatum(new_acl as *const c_void);
        replaces[Anum_pg_init_privs_initprivs as usize - 1] = true;

        let newtuple = heap_modify_tuple(
            oldtuple, RelationGetDescr(rel),
            values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr(),
        );
        CatalogTupleUpdate(rel, &mut (*newtuple).t_self, newtuple);
    }

    /*
     * Update the shared dependency ACL info.
     */
    nnewmembers = aclmembers(new_acl, &mut newmembers);

    updateInitAclDependencies(
        classid, objid, objsubid,
        noldmembers, oldmembers,
        nnewmembers, newmembers,
    );

    systable_endscan(scan);

    /* prevent error when processing objects multiple times */
    CommandCounterIncrement();

    table_close(rel, RowExclusiveLock);
}
