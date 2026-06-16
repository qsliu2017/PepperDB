//! Translation of postgres/src/include/catalog/pg_type.h
//!
//! The `FormData_pg_type` struct: the fixed-layout, guaranteed-not-null part of
//! a pg_type catalog row.  This is exactly the portion of the row that the C
//! struct exposes in memory; the variable-length / nullable trailing fields
//! (typdefaultbin, typdefault, typacl, guarded by CATALOG_VARLEN in the C
//! header) are NOT part of this struct - they live only in a real on-disk
//! pg_type tuple and are reached via heap_getattr.
//!
//! Some of the values in a pg_type instance are copied into pg_attribute
//! instances (typlen/typbyval/typalign/typstorage); see FormData_pg_attribute.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, int32, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

use crate::prelude::*;
use crate::pg_config::NAMEDATALEN;
use crate::Assert;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_type - the fixed part of a pg_type row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_type {
    /* oid */
    pub oid: Oid,
    /* type name */
    pub typname: NameData,
    /* OID of namespace containing this type */
    pub typnamespace: Oid,
    /* type owner */
    pub typowner: Oid,
    /* number of bytes for a fixed-size type; negative for variable-length */
    pub typlen: int16,
    /* pass by value (true) or by reference (false)? */
    pub typbyval: bool,
    /* type kind: base/composite/domain/enum/pseudo/range (TYPTYPE macros) */
    pub typtype: c_char,
    /* arbitrary type classification (TYPCATEGORY macros) */
    pub typcategory: c_char,
    /* is type "preferred" within its category? */
    pub typispreferred: bool,
    /* false if entry is only a placeholder (forward reference) */
    pub typisdefined: bool,
    /* delimiter for arrays of this type */
    pub typdelim: c_char,
    /* associated pg_class OID if a composite type, else 0 */
    pub typrelid: Oid,
    /* type-specific subscripting handler (0 = not subscriptable) */
    pub typsubscript: regproc,
    /* element type yielded by subscripting, else 0 */
    pub typelem: Oid,
    /* the "true" array type having this type as element, else 0 */
    pub typarray: Oid,
    /* text input conversion procedure (required) */
    pub typinput: regproc,
    /* text output conversion procedure (required) */
    pub typoutput: regproc,
    /* binary input conversion procedure (optional) */
    pub typreceive: regproc,
    /* binary output conversion procedure (optional) */
    pub typsend: regproc,
    /* input procedure for optional type modifiers */
    pub typmodin: regproc,
    /* output procedure for optional type modifiers */
    pub typmodout: regproc,
    /* custom ANALYZE procedure (0 selects the default) */
    pub typanalyze: regproc,
    /* alignment requirement when storing a value (TYPALIGN macros) */
    pub typalign: c_char,
    /* toasting preparation and default storage strategy (TYPSTORAGE macros) */
    pub typstorage: c_char,
    /* NOT NULL constraint against this datatype (mainly for domains) */
    pub typnotnull: bool,
    /* base type a domain is based on; 0 if not a domain */
    pub typbasetype: Oid,
    /* typmod to apply to a domain's base type; -1 if not a domain */
    pub typtypmod: int32,
    /* declared number of dimensions for an array domain type, else 0 */
    pub typndims: int32,
    /* collation: 0 if type cannot use collations, else collation OID */
    pub typcollation: Oid,
}

/*
 * Form_pg_type corresponds to a pointer to a row with the format of the pg_type
 * relation.
 */
pub type Form_pg_type = *mut FormData_pg_type;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * macros for values of poor-mans-enumerated-type columns
 * ----------------------------------------------------------------
 */

/* TYPTYPE_* - the typtype column */
pub const TYPTYPE_BASE: c_char = b'b' as c_char; /* base type (ordinary scalar type) */
pub const TYPTYPE_COMPOSITE: c_char = b'c' as c_char; /* composite (e.g., table's rowtype) */
pub const TYPTYPE_DOMAIN: c_char = b'd' as c_char; /* domain over another type */
pub const TYPTYPE_ENUM: c_char = b'e' as c_char; /* enumerated type */
pub const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char; /* multirange type */
pub const TYPTYPE_PSEUDO: c_char = b'p' as c_char; /* pseudo-type */
pub const TYPTYPE_RANGE: c_char = b'r' as c_char; /* range type */

/* TYPCATEGORY_* - the typcategory column */
pub const TYPCATEGORY_INVALID: c_char = b'\0' as c_char; /* not an allowed category */
pub const TYPCATEGORY_ARRAY: c_char = b'A' as c_char;
pub const TYPCATEGORY_BOOLEAN: c_char = b'B' as c_char;
pub const TYPCATEGORY_COMPOSITE: c_char = b'C' as c_char;
pub const TYPCATEGORY_DATETIME: c_char = b'D' as c_char;
pub const TYPCATEGORY_ENUM: c_char = b'E' as c_char;
pub const TYPCATEGORY_GEOMETRIC: c_char = b'G' as c_char;
pub const TYPCATEGORY_NETWORK: c_char = b'I' as c_char; /* think INET */
pub const TYPCATEGORY_NUMERIC: c_char = b'N' as c_char;
pub const TYPCATEGORY_PSEUDOTYPE: c_char = b'P' as c_char;
pub const TYPCATEGORY_RANGE: c_char = b'R' as c_char;
pub const TYPCATEGORY_STRING: c_char = b'S' as c_char;
pub const TYPCATEGORY_TIMESPAN: c_char = b'T' as c_char;
pub const TYPCATEGORY_USER: c_char = b'U' as c_char;
pub const TYPCATEGORY_BITSTRING: c_char = b'V' as c_char; /* er ... "varbit"? */
pub const TYPCATEGORY_UNKNOWN: c_char = b'X' as c_char;
pub const TYPCATEGORY_INTERNAL: c_char = b'Z' as c_char;

/* TYPALIGN_* - the typalign column (canonical home; tupmacs/tupdesc duplicate) */
pub const TYPALIGN_CHAR: c_char = b'c' as c_char; /* char alignment (i.e. unaligned) */
pub const TYPALIGN_SHORT: c_char = b's' as c_char; /* short alignment (typically 2 bytes) */
pub const TYPALIGN_INT: c_char = b'i' as c_char; /* int alignment (typically 4 bytes) */
pub const TYPALIGN_DOUBLE: c_char = b'd' as c_char; /* double alignment (often 8 bytes) */

/* TYPSTORAGE_* - the typstorage column (canonical home; tupmacs/tupdesc duplicate) */
pub const TYPSTORAGE_PLAIN: c_char = b'p' as c_char; /* type not prepared for toasting */
pub const TYPSTORAGE_EXTERNAL: c_char = b'e' as c_char; /* toastable, don't try to compress */
pub const TYPSTORAGE_EXTENDED: c_char = b'x' as c_char; /* fully toastable */
pub const TYPSTORAGE_MAIN: c_char = b'm' as c_char; /* like 'x' but try to store inline */

// ===========================================================================
// pg_type.c - routines to support manipulation of the pg_type relation
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
// ===========================================================================

// ---------------------------------------------------------------------------
// Type aliases (faithful pointers; concrete defs live in their own modules)
// ---------------------------------------------------------------------------
type Relation = *mut crate::utils::rel::RelationData;
type HeapTuple = *mut crate::access::htup_details::HeapTupleData;
type TupleDesc = *mut crate::access::common::tupdesc::TupleDescData;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type Node = crate::nodes::nodes::Node;

// ObjectAddresses  TODO(pg-port): catalog/dependency.c
#[repr(C)]
pub struct ObjectAddresses {
    _opaque: [u8; 0],
}

// Acl  TODO(pg-port): utils/adt/acl.c
#[repr(C)]
pub struct AclData {
    _opaque: [u8; 0],
}
type Acl = AclData;

// LOCKMODE  TODO(pg-port): storage/lockdefs.h
type LOCKMODE = c_int;
const RowExclusiveLock: LOCKMODE = 3;

// AclResult  TODO(pg-port): utils/acl.h
type AclResult = c_int;
const ACLCHECK_NOT_OWNER: AclResult = 2;

// ObjectClass kinds used by aclcheck_error/get_user_default_acl  TODO(pg-port)
const OBJECT_TYPE: c_int = 39;

// DependencyType  TODO(pg-port): catalog/dependency.h
type DependencyType = c_int;
const DEPENDENCY_NORMAL: DependencyType = b'n' as c_int;
const DEPENDENCY_INTERNAL: DependencyType = b'i' as c_int;

// SysCache cache ids  TODO(pg-port): utils/syscache.h
const TYPENAMENSP: c_int = 0;
const TYPEOID: c_int = 0;

// catalog OIDs  TODO(pg-port): catalog/*_d.h
const TypeRelationId: Oid = 1247;
const NamespaceRelationId: Oid = 2615;
const ProcedureRelationId: Oid = 1255;
const RelationRelationId: Oid = 1259;
const CollationRelationId: Oid = 3456;
const TypeOidIndexId: Oid = 2703;

// built-in function OIDs  TODO(pg-port): utils/fmgroids.h
const F_SHELL_IN: Oid = 2398;
const F_SHELL_OUT: Oid = 2399;

// misc constants  TODO(pg-port)
const InvalidOid: Oid = 0;
const DEFAULT_TYPDELIM: c_char = b',' as c_char;
const DEFAULT_COLLATION_OID: Oid = 100;
const RELKIND_COMPOSITE_TYPE: c_char = b'c' as c_char;
const NIL: *mut crate::nodes::pg_list::List = core::ptr::null_mut();

// Anum_pg_type_* column numbers (catalog order; CATALOG_VARLEN trail at 30..32)
const Anum_pg_type_oid: c_int = 1;
const Anum_pg_type_typname: c_int = 2;
const Anum_pg_type_typnamespace: c_int = 3;
const Anum_pg_type_typowner: c_int = 4;
const Anum_pg_type_typlen: c_int = 5;
const Anum_pg_type_typbyval: c_int = 6;
const Anum_pg_type_typtype: c_int = 7;
const Anum_pg_type_typcategory: c_int = 8;
const Anum_pg_type_typispreferred: c_int = 9;
const Anum_pg_type_typisdefined: c_int = 10;
const Anum_pg_type_typdelim: c_int = 11;
const Anum_pg_type_typrelid: c_int = 12;
const Anum_pg_type_typsubscript: c_int = 13;
const Anum_pg_type_typelem: c_int = 14;
const Anum_pg_type_typarray: c_int = 15;
const Anum_pg_type_typinput: c_int = 16;
const Anum_pg_type_typoutput: c_int = 17;
const Anum_pg_type_typreceive: c_int = 18;
const Anum_pg_type_typsend: c_int = 19;
const Anum_pg_type_typmodin: c_int = 20;
const Anum_pg_type_typmodout: c_int = 21;
const Anum_pg_type_typanalyze: c_int = 22;
const Anum_pg_type_typalign: c_int = 23;
const Anum_pg_type_typstorage: c_int = 24;
const Anum_pg_type_typnotnull: c_int = 25;
const Anum_pg_type_typbasetype: c_int = 26;
const Anum_pg_type_typtypmod: c_int = 27;
const Anum_pg_type_typndims: c_int = 28;
const Anum_pg_type_typcollation: c_int = 29;
const Anum_pg_type_typdefaultbin: c_int = 30;
const Anum_pg_type_typdefault: c_int = 31;
const Anum_pg_type_typacl: c_int = 32;
const Natts_pg_type: usize = 32;

/* Potentially set by pg_upgrade_support functions */
pub static mut binary_upgrade_next_pg_type_oid: Oid = InvalidOid;

// ---------------------------------------------------------------------------
// Local stubs for unported helpers  TODO(pg-port)
// ---------------------------------------------------------------------------
unsafe fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO(pg-port): access/table/table.c
}
unsafe fn table_close(relation: Relation, lockmode: LOCKMODE) {
    unimplemented!() // TODO(pg-port): access/table/table.c
}
unsafe fn namestrcpy(name: *mut NameData, s: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): common/string.c
}
unsafe fn heap_form_tuple(
    tupleDescriptor: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn heap_freetuple(htup: HeapTuple) {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn heap_getattr(
    tup: HeapTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) -> Oid {
    unimplemented!() // TODO(pg-port): catalog/indexing.c
}
unsafe fn CatalogTupleUpdate(
    heapRel: Relation,
    otid: *mut crate::storage::itemptr::ItemPointerData,
    tup: HeapTuple,
) {
    unimplemented!() // TODO(pg-port): catalog/indexing.c
}
unsafe fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: c_int) -> Oid {
    unimplemented!() // TODO(pg-port): catalog/catalog.c
}
unsafe fn SearchSysCacheCopy2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn GetSysCacheOid2(cacheId: c_int, oidcol: c_int, key1: Datum, key2: Datum) -> Oid {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn SearchSysCacheExists2(cacheId: c_int, key1: Datum, key2: Datum) -> bool {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/htup_details.h
}
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn stringToNode(s: *mut c_char) -> *mut Node {
    unimplemented!() // TODO(pg-port): nodes/read.c
}
unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    unimplemented!() // TODO(pg-port): builtins.h
}
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): builtins.h
}
unsafe fn DatumGetAclPCopy(d: Datum) -> *mut Acl {
    unimplemented!() // TODO(pg-port): utils/acl.h
}
unsafe fn get_user_default_acl(objtype: c_int, ownerId: Oid, nsp_oid: Oid) -> *mut Acl {
    unimplemented!() // TODO(pg-port): catalog/aclchk.c
}
unsafe fn aclcheck_error(aclerr: AclResult, objtype: c_int, objectname: *const c_char) {
    unimplemented!() // TODO(pg-port): catalog/aclchk.c
}
unsafe fn get_typisdefined(typid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn get_element_type(typid: Oid) -> Oid {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn get_array_type(typid: Oid) -> Oid {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn makeObjectName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO(pg-port): commands/indexcmds.c
}
unsafe fn new_object_addresses() -> *mut ObjectAddresses {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn add_exact_object_address(object: *const ObjectAddress, addrs: *mut ObjectAddresses) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn record_object_address_dependencies(
    depender: *const ObjectAddress,
    referenced: *mut ObjectAddresses,
    behavior: DependencyType,
) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn free_object_addresses(addrs: *mut ObjectAddresses) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn recordDependencyOn(
    depender: *const ObjectAddress,
    referenced: *const ObjectAddress,
    behavior: DependencyType,
) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn recordDependencyOnExpr(
    depender: *const ObjectAddress,
    expr: *mut Node,
    rtable: *mut crate::nodes::pg_list::List,
    behavior: DependencyType,
) {
    unimplemented!() // TODO(pg-port): catalog/dependency.c
}
unsafe fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) {
    unimplemented!() // TODO(pg-port): catalog/pg_shdepend.c
}
unsafe fn recordDependencyOnNewAcl(
    classId: Oid,
    objectId: Oid,
    objsubId: c_int,
    ownerId: Oid,
    acl: *mut Acl,
) {
    unimplemented!() // TODO(pg-port): catalog/pg_shdepend.c
}
unsafe fn recordDependencyOnCurrentExtension(object: *const ObjectAddress, isReplace: bool) {
    unimplemented!() // TODO(pg-port): catalog/pg_depend.c
}
unsafe fn deleteDependencyRecordsFor(classId: Oid, objectId: Oid, skipExtensionDeps: bool) -> c_long {
    unimplemented!() // TODO(pg-port): catalog/pg_depend.c
}
unsafe fn deleteSharedDependencyRecordsFor(classId: Oid, objectId: Oid, objectSubId: c_int) {
    unimplemented!() // TODO(pg-port): catalog/pg_shdepend.c
}
unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO(pg-port): access/transam/xact.c
}
unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) {
    // TODO(pg-port): catalog/objectaccess.h - no-op unless hook installed
}
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {
    // TODO(pg-port): catalog/objectaccess.h - no-op unless hook installed
}
unsafe fn IsBootstrapProcessingMode() -> bool {
    unimplemented!() // TODO(pg-port): miscadmin.h
}
#[allow(non_upper_case_globals)]
static mut IsBinaryUpgrade: bool = false;
unsafe fn pg_mbcliplen(mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): mb/mbutils.c
}
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}
// C library string helpers
extern "C" {
    fn strstr(haystack: *const c_char, needle: *const c_char) -> *const c_char;
    fn strlen(s: *const c_char) -> usize;
}
unsafe fn NameGetDatum(n: *const NameData) -> Datum {
    n as Datum
}
// psprintf is variadic in C; this faithful stub covers the 1-3 string-arg
// call shapes used below.  TODO(pg-port): utils/mmgr/mcxt.c
unsafe fn psprintf(
    fmt: *const c_char,
    a: *const c_char,
    b: *const c_char,
    c: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/mmgr/mcxt.c
}

// ---------------------------------------------------------------------------
//		TypeShellMake
//
//		This procedure inserts a "shell" tuple into the pg_type relation.
//		The type tuple inserted has valid but dummy values, and its
//		"typisdefined" field is false indicating it's not really defined.
// ---------------------------------------------------------------------------
pub unsafe fn TypeShellMake(
    typeName: *const c_char,
    typeNamespace: Oid,
    ownerId: Oid,
) -> ObjectAddress {
    let pg_type_desc: Relation;
    let tupDesc: TupleDesc;
    let mut i: c_int;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_type] = [0 as Datum; Natts_pg_type];
    let mut nulls: [bool; Natts_pg_type] = [false; Natts_pg_type];
    let typoid: Oid;
    let mut name: NameData = core::mem::zeroed();
    let mut address: ObjectAddress = core::mem::zeroed();

    Assert!(PointerIsValid(typeName));

    /*
     * open pg_type
     */
    pg_type_desc = table_open(TypeRelationId, RowExclusiveLock);
    tupDesc = (*pg_type_desc).rd_att;

    /*
     * initialize our *nulls and *values arrays
     */
    i = 0;
    while i < Natts_pg_type as c_int {
        nulls[i as usize] = false;
        values[i as usize] = 0 as Datum; /* redundant, but safe */
        i += 1;
    }

    /*
     * initialize *values with the type name and dummy values
     *
     * The representational details are the same as int4 ... it doesn't really
     * matter what they are so long as they are consistent.  Also note that we
     * give it typtype = TYPTYPE_PSEUDO as extra insurance that it won't be
     * mistaken for a usable type.
     */
    namestrcpy(&mut name, typeName);
    values[(Anum_pg_type_typname - 1) as usize] = NameGetDatum(&name);
    values[(Anum_pg_type_typnamespace - 1) as usize] = ObjectIdGetDatum(typeNamespace);
    values[(Anum_pg_type_typowner - 1) as usize] = ObjectIdGetDatum(ownerId);
    values[(Anum_pg_type_typlen - 1) as usize] =
        Int16GetDatum(core::mem::size_of::<int32>() as int16);
    values[(Anum_pg_type_typbyval - 1) as usize] = BoolGetDatum(true);
    values[(Anum_pg_type_typtype - 1) as usize] = CharGetDatum(TYPTYPE_PSEUDO);
    values[(Anum_pg_type_typcategory - 1) as usize] = CharGetDatum(TYPCATEGORY_PSEUDOTYPE);
    values[(Anum_pg_type_typispreferred - 1) as usize] = BoolGetDatum(false);
    values[(Anum_pg_type_typisdefined - 1) as usize] = BoolGetDatum(false);
    values[(Anum_pg_type_typdelim - 1) as usize] = CharGetDatum(DEFAULT_TYPDELIM);
    values[(Anum_pg_type_typrelid - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typsubscript - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typelem - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typarray - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typinput - 1) as usize] = ObjectIdGetDatum(F_SHELL_IN);
    values[(Anum_pg_type_typoutput - 1) as usize] = ObjectIdGetDatum(F_SHELL_OUT);
    values[(Anum_pg_type_typreceive - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typsend - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typmodin - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typmodout - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typanalyze - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typalign - 1) as usize] = CharGetDatum(TYPALIGN_INT);
    values[(Anum_pg_type_typstorage - 1) as usize] = CharGetDatum(TYPSTORAGE_PLAIN);
    values[(Anum_pg_type_typnotnull - 1) as usize] = BoolGetDatum(false);
    values[(Anum_pg_type_typbasetype - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    values[(Anum_pg_type_typtypmod - 1) as usize] = Int32GetDatum(-1);
    values[(Anum_pg_type_typndims - 1) as usize] = Int32GetDatum(0);
    values[(Anum_pg_type_typcollation - 1) as usize] = ObjectIdGetDatum(InvalidOid);
    nulls[(Anum_pg_type_typdefaultbin - 1) as usize] = true;
    nulls[(Anum_pg_type_typdefault - 1) as usize] = true;
    nulls[(Anum_pg_type_typacl - 1) as usize] = true;

    /* Use binary-upgrade override for pg_type.oid? */
    if IsBinaryUpgrade {
        if !OidIsValid(binary_upgrade_next_pg_type_oid) {
            ereport!(
                ERROR,
                errmsg!("pg_type OID value not set when in binary upgrade mode")
            ); /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        typoid = binary_upgrade_next_pg_type_oid;
        binary_upgrade_next_pg_type_oid = InvalidOid;
    } else {
        typoid = GetNewOidWithIndex(pg_type_desc, TypeOidIndexId, Anum_pg_type_oid);
    }

    values[(Anum_pg_type_oid - 1) as usize] = ObjectIdGetDatum(typoid);

    /*
     * create a new type tuple
     */
    tup = heap_form_tuple(tupDesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    /*
     * insert the tuple in the relation and get the tuple's oid.
     */
    CatalogTupleInsert(pg_type_desc, tup);

    /*
     * Create dependencies.  We can/must skip this in bootstrap mode.
     */
    if !IsBootstrapProcessingMode() {
        GenerateTypeDependencies(
            tup,
            pg_type_desc,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            0,
            false,
            false,
            true, /* make extension dependency */
            false,
        );
    }

    /* Post creation hook for new shell type */
    InvokeObjectPostCreateHook(TypeRelationId, typoid, 0);

    ObjectAddressSet(&mut address, TypeRelationId, typoid);

    /*
     * clean up and return the type-oid
     */
    heap_freetuple(tup);
    table_close(pg_type_desc, RowExclusiveLock);

    address
}

// ---------------------------------------------------------------------------
//		TypeCreate
//
//		This does all the necessary work needed to define a new type.
// ---------------------------------------------------------------------------
pub unsafe fn TypeCreate(
    newTypeOid: Oid,
    typeName: *const c_char,
    typeNamespace: Oid,
    relationOid: Oid,   /* only for relation rowtypes */
    relationKind: c_char, /* ditto */
    ownerId: Oid,
    internalSize: int16,
    typeType: c_char,
    typeCategory: c_char,
    typePreferred: bool,
    typDelim: c_char,
    inputProcedure: Oid,
    outputProcedure: Oid,
    receiveProcedure: Oid,
    sendProcedure: Oid,
    typmodinProcedure: Oid,
    typmodoutProcedure: Oid,
    analyzeProcedure: Oid,
    subscriptProcedure: Oid,
    elementType: Oid,
    isImplicitArray: bool,
    arrayType: Oid,
    baseType: Oid,
    defaultTypeValue: *const c_char, /* human-readable rep */
    defaultTypeBin: *mut c_char,     /* cooked rep */
    passedByValue: bool,
    alignment: c_char,
    storage: c_char,
    typeMod: int32,
    typNDims: int32, /* Array dimensions for baseType */
    typeNotNull: bool,
    typeCollation: Oid,
) -> ObjectAddress {
    let pg_type_desc: Relation;
    let typeObjectId: Oid;
    let isDependentType: bool;
    let mut rebuildDeps: bool = false;
    let mut typacl: *mut Acl;
    let mut tup: HeapTuple;
    let mut nulls: [bool; Natts_pg_type] = [false; Natts_pg_type];
    let mut replaces: [bool; Natts_pg_type] = [false; Natts_pg_type];
    let mut values: [Datum; Natts_pg_type] = [0 as Datum; Natts_pg_type];
    let mut name: NameData = core::mem::zeroed();
    let mut i: c_int;
    let mut address: ObjectAddress = core::mem::zeroed();

    /*
     * We assume that the caller validated the arguments individually, but did
     * not check for bad combinations.
     *
     * Validate size specifications: either positive (fixed-length) or -1
     * (varlena) or -2 (cstring).
     */
    if !(internalSize > 0 || internalSize == -1 || internalSize == -2) {
        ereport!(
            ERROR,
            errmsg!("invalid type internal size {}", internalSize)
        ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if passedByValue {
        /*
         * Pass-by-value types must have a fixed length that is one of the
         * values supported by fetch_att() and store_att_byval(); and the
         * alignment had better agree, too.  All this code must match
         * access/tupmacs.h!
         */
        if internalSize == core::mem::size_of::<c_char>() as int16 {
            if alignment != TYPALIGN_CHAR {
                ereport!(
                    ERROR,
                    errmsg!(
                        "alignment \"{}\" is invalid for passed-by-value type of size {}",
                        alignment as u8 as char,
                        internalSize
                    )
                ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        } else if internalSize == core::mem::size_of::<int16>() as int16 {
            if alignment != TYPALIGN_SHORT {
                ereport!(
                    ERROR,
                    errmsg!(
                        "alignment \"{}\" is invalid for passed-by-value type of size {}",
                        alignment as u8 as char,
                        internalSize
                    )
                ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        } else if internalSize == core::mem::size_of::<int32>() as int16 {
            if alignment != TYPALIGN_INT {
                ereport!(
                    ERROR,
                    errmsg!(
                        "alignment \"{}\" is invalid for passed-by-value type of size {}",
                        alignment as u8 as char,
                        internalSize
                    )
                ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        }
        /* #if SIZEOF_DATUM == 8 */
        else if internalSize == core::mem::size_of::<Datum>() as int16 {
            if alignment != TYPALIGN_DOUBLE {
                ereport!(
                    ERROR,
                    errmsg!(
                        "alignment \"{}\" is invalid for passed-by-value type of size {}",
                        alignment as u8 as char,
                        internalSize
                    )
                ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            }
        }
        /* #endif */
        else {
            ereport!(
                ERROR,
                errmsg!(
                    "internal size {} is invalid for passed-by-value type",
                    internalSize
                )
            ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
    } else {
        /* varlena types must have int align or better */
        if internalSize == -1 && !(alignment == TYPALIGN_INT || alignment == TYPALIGN_DOUBLE) {
            ereport!(
                ERROR,
                errmsg!(
                    "alignment \"{}\" is invalid for variable-length type",
                    alignment as u8 as char
                )
            ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        /* cstring must have char alignment */
        if internalSize == -2 && !(alignment == TYPALIGN_CHAR) {
            ereport!(
                ERROR,
                errmsg!(
                    "alignment \"{}\" is invalid for variable-length type",
                    alignment as u8 as char
                )
            ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
    }

    /* Only varlena types can be toasted */
    if storage != TYPSTORAGE_PLAIN && internalSize != -1 {
        ereport!(
            ERROR,
            errmsg!("fixed-size types must have storage PLAIN")
        ); /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * This is a dependent type if it's an implicitly-created array type or
     * multirange type, or if it's a relation rowtype that's not a composite
     * type.
     */
    isDependentType = isImplicitArray
        || typeType == TYPTYPE_MULTIRANGE
        || (OidIsValid(relationOid) && relationKind != RELKIND_COMPOSITE_TYPE);

    /*
     * initialize arrays needed for heap_form_tuple or heap_modify_tuple
     */
    i = 0;
    while i < Natts_pg_type as c_int {
        nulls[i as usize] = false;
        replaces[i as usize] = true;
        values[i as usize] = 0 as Datum;
        i += 1;
    }

    /*
     * insert data values
     */
    namestrcpy(&mut name, typeName);
    values[(Anum_pg_type_typname - 1) as usize] = NameGetDatum(&name);
    values[(Anum_pg_type_typnamespace - 1) as usize] = ObjectIdGetDatum(typeNamespace);
    values[(Anum_pg_type_typowner - 1) as usize] = ObjectIdGetDatum(ownerId);
    values[(Anum_pg_type_typlen - 1) as usize] = Int16GetDatum(internalSize);
    values[(Anum_pg_type_typbyval - 1) as usize] = BoolGetDatum(passedByValue);
    values[(Anum_pg_type_typtype - 1) as usize] = CharGetDatum(typeType);
    values[(Anum_pg_type_typcategory - 1) as usize] = CharGetDatum(typeCategory);
    values[(Anum_pg_type_typispreferred - 1) as usize] = BoolGetDatum(typePreferred);
    values[(Anum_pg_type_typisdefined - 1) as usize] = BoolGetDatum(true);
    values[(Anum_pg_type_typdelim - 1) as usize] = CharGetDatum(typDelim);
    values[(Anum_pg_type_typrelid - 1) as usize] = ObjectIdGetDatum(relationOid);
    values[(Anum_pg_type_typsubscript - 1) as usize] = ObjectIdGetDatum(subscriptProcedure);
    values[(Anum_pg_type_typelem - 1) as usize] = ObjectIdGetDatum(elementType);
    values[(Anum_pg_type_typarray - 1) as usize] = ObjectIdGetDatum(arrayType);
    values[(Anum_pg_type_typinput - 1) as usize] = ObjectIdGetDatum(inputProcedure);
    values[(Anum_pg_type_typoutput - 1) as usize] = ObjectIdGetDatum(outputProcedure);
    values[(Anum_pg_type_typreceive - 1) as usize] = ObjectIdGetDatum(receiveProcedure);
    values[(Anum_pg_type_typsend - 1) as usize] = ObjectIdGetDatum(sendProcedure);
    values[(Anum_pg_type_typmodin - 1) as usize] = ObjectIdGetDatum(typmodinProcedure);
    values[(Anum_pg_type_typmodout - 1) as usize] = ObjectIdGetDatum(typmodoutProcedure);
    values[(Anum_pg_type_typanalyze - 1) as usize] = ObjectIdGetDatum(analyzeProcedure);
    values[(Anum_pg_type_typalign - 1) as usize] = CharGetDatum(alignment);
    values[(Anum_pg_type_typstorage - 1) as usize] = CharGetDatum(storage);
    values[(Anum_pg_type_typnotnull - 1) as usize] = BoolGetDatum(typeNotNull);
    values[(Anum_pg_type_typbasetype - 1) as usize] = ObjectIdGetDatum(baseType);
    values[(Anum_pg_type_typtypmod - 1) as usize] = Int32GetDatum(typeMod);
    values[(Anum_pg_type_typndims - 1) as usize] = Int32GetDatum(typNDims);
    values[(Anum_pg_type_typcollation - 1) as usize] = ObjectIdGetDatum(typeCollation);

    /*
     * initialize the default binary value for this type.  Check for nulls of
     * course.
     */
    if !defaultTypeBin.is_null() {
        values[(Anum_pg_type_typdefaultbin - 1) as usize] = CStringGetTextDatum(defaultTypeBin);
    } else {
        nulls[(Anum_pg_type_typdefaultbin - 1) as usize] = true;
    }

    /*
     * initialize the default value for this type.
     */
    if !defaultTypeValue.is_null() {
        values[(Anum_pg_type_typdefault - 1) as usize] = CStringGetTextDatum(defaultTypeValue);
    } else {
        nulls[(Anum_pg_type_typdefault - 1) as usize] = true;
    }

    /*
     * Initialize the type's ACL, too.  But dependent types don't get one.
     */
    if isDependentType {
        typacl = core::ptr::null_mut();
    } else {
        typacl = get_user_default_acl(OBJECT_TYPE, ownerId, typeNamespace);
    }
    if !typacl.is_null() {
        values[(Anum_pg_type_typacl - 1) as usize] = PointerGetDatum(typacl as *const c_void);
    } else {
        nulls[(Anum_pg_type_typacl - 1) as usize] = true;
    }

    /*
     * open pg_type and prepare to insert or update a row.
     */
    pg_type_desc = table_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy2(
        TYPENAMENSP,
        CStringGetDatum(typeName),
        ObjectIdGetDatum(typeNamespace),
    );
    if HeapTupleIsValid(tup) {
        let typform: Form_pg_type = GETSTRUCT(tup) as Form_pg_type;

        /*
         * check that the type is not already defined.  It may exist as a
         * shell type, however.
         */
        if (*typform).typisdefined {
            ereport!(
                ERROR,
                errmsg!(
                    "type \"{}\" already exists",
                    std::ffi::CStr::from_ptr(typeName).to_string_lossy()
                )
            ); /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }

        /*
         * shell type must have been created by same owner
         */
        if (*typform).typowner != ownerId {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TYPE, typeName);
        }

        /* trouble if caller wanted to force the OID */
        if OidIsValid(newTypeOid) {
            elog!(ERROR, "cannot assign new OID to existing shell type");
        }

        replaces[(Anum_pg_type_oid - 1) as usize] = false;

        /*
         * Okay to update existing shell type tuple
         */
        tup = heap_modify_tuple(
            tup,
            RelationGetDescr(pg_type_desc),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
        );

        CatalogTupleUpdate(pg_type_desc, &mut (*tup).t_self, tup);

        typeObjectId = (*typform).oid;

        rebuildDeps = true; /* get rid of shell type's dependencies */
    } else {
        /* Force the OID if requested by caller */
        if OidIsValid(newTypeOid) {
            typeObjectId = newTypeOid;
        }
        /* Use binary-upgrade override for pg_type.oid, if supplied. */
        else if IsBinaryUpgrade {
            if !OidIsValid(binary_upgrade_next_pg_type_oid) {
                ereport!(
                    ERROR,
                    errmsg!("pg_type OID value not set when in binary upgrade mode")
                ); /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }

            typeObjectId = binary_upgrade_next_pg_type_oid;
            binary_upgrade_next_pg_type_oid = InvalidOid;
        } else {
            typeObjectId = GetNewOidWithIndex(pg_type_desc, TypeOidIndexId, Anum_pg_type_oid);
        }

        values[(Anum_pg_type_oid - 1) as usize] = ObjectIdGetDatum(typeObjectId);

        tup = heap_form_tuple(
            RelationGetDescr(pg_type_desc),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        CatalogTupleInsert(pg_type_desc, tup);
    }

    /*
     * Create dependencies.  We can/must skip this in bootstrap mode.
     */
    if !IsBootstrapProcessingMode() {
        GenerateTypeDependencies(
            tup,
            pg_type_desc,
            if !defaultTypeBin.is_null() {
                stringToNode(defaultTypeBin)
            } else {
                core::ptr::null_mut()
            },
            typacl as *mut c_void,
            relationKind,
            isImplicitArray,
            isDependentType,
            true, /* make extension dependency */
            rebuildDeps,
        );
    }

    /* Post creation hook for new type */
    InvokeObjectPostCreateHook(TypeRelationId, typeObjectId, 0);

    ObjectAddressSet(&mut address, TypeRelationId, typeObjectId);

    /*
     * finish up
     */
    table_close(pg_type_desc, RowExclusiveLock);

    address
}

// ---------------------------------------------------------------------------
// GenerateTypeDependencies: build the dependencies needed for a type
// ---------------------------------------------------------------------------
pub unsafe fn GenerateTypeDependencies(
    typeTuple: HeapTuple,
    typeCatalog: Relation,
    mut defaultExpr: *mut Node,
    mut typacl: *mut c_void,
    relationKind: c_char, /* only for relation rowtypes */
    isImplicitArray: bool,
    isDependentType: bool,
    makeExtensionDep: bool,
    rebuild: bool,
) {
    let typeForm: Form_pg_type = GETSTRUCT(typeTuple) as Form_pg_type;
    let typeObjectId: Oid = (*typeForm).oid;
    let datum: Datum;
    let mut isNull: bool = false;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let addrs_normal: *mut ObjectAddresses;

    /* Extract defaultExpr if caller didn't pass it */
    if defaultExpr.is_null() {
        let datum2 = heap_getattr(
            typeTuple,
            Anum_pg_type_typdefaultbin,
            RelationGetDescr(typeCatalog),
            &mut isNull,
        );
        if !isNull {
            defaultExpr = stringToNode(TextDatumGetCString(datum2));
        }
    }
    /* Extract typacl if caller didn't pass it */
    if typacl.is_null() {
        datum = heap_getattr(
            typeTuple,
            Anum_pg_type_typacl,
            RelationGetDescr(typeCatalog),
            &mut isNull,
        );
        if !isNull {
            typacl = DatumGetAclPCopy(datum) as *mut c_void;
        }
    }

    /* If rebuild, first flush old dependencies, except extension deps */
    if rebuild {
        deleteDependencyRecordsFor(TypeRelationId, typeObjectId, true);
        deleteSharedDependencyRecordsFor(TypeRelationId, typeObjectId, 0);
    }

    ObjectAddressSet(&mut myself, TypeRelationId, typeObjectId);

    /*
     * Make dependencies on namespace, owner, ACL.
     */

    /* collects normal dependencies for bulk recording */
    addrs_normal = new_object_addresses();

    if !isDependentType || (*typeForm).typtype == TYPTYPE_MULTIRANGE {
        ObjectAddressSet(&mut referenced, NamespaceRelationId, (*typeForm).typnamespace);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if !isDependentType {
        recordDependencyOnOwner(TypeRelationId, typeObjectId, (*typeForm).typowner);

        recordDependencyOnNewAcl(
            TypeRelationId,
            typeObjectId,
            0,
            (*typeForm).typowner,
            typacl as *mut Acl,
        );
    }

    /*
     * Make extension dependency if requested.
     */
    if makeExtensionDep {
        recordDependencyOnCurrentExtension(&myself, rebuild);
    }

    /* Normal dependencies on the I/O and support functions */
    if OidIsValid((*typeForm).typinput) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typinput);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typoutput) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typoutput);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typreceive) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typreceive);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typsend) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typsend);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typmodin) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typmodin);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typmodout) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typmodout);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typanalyze) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typanalyze);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if OidIsValid((*typeForm).typsubscript) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*typeForm).typsubscript);
        add_exact_object_address(&referenced, addrs_normal);
    }

    /* Normal dependency from a domain to its base type. */
    if OidIsValid((*typeForm).typbasetype) {
        ObjectAddressSet(&mut referenced, TypeRelationId, (*typeForm).typbasetype);
        add_exact_object_address(&referenced, addrs_normal);
    }

    /*
     * Normal dependency from a domain to its collation.  We know the default
     * collation is pinned, so don't bother recording it.
     */
    if OidIsValid((*typeForm).typcollation) && (*typeForm).typcollation != DEFAULT_COLLATION_OID {
        ObjectAddressSet(&mut referenced, CollationRelationId, (*typeForm).typcollation);
        add_exact_object_address(&referenced, addrs_normal);
    }

    record_object_address_dependencies(&myself, addrs_normal, DEPENDENCY_NORMAL);
    free_object_addresses(addrs_normal);

    /* Normal dependency on the default expression. */
    if !defaultExpr.is_null() {
        recordDependencyOnExpr(&myself, defaultExpr, NIL, DEPENDENCY_NORMAL);
    }

    /*
     * If the type is a rowtype for a relation, mark it as internally
     * dependent on the relation, *unless* it is a stand-alone composite type
     * relation.
     */
    if OidIsValid((*typeForm).typrelid) {
        ObjectAddressSet(&mut referenced, RelationRelationId, (*typeForm).typrelid);

        if relationKind != RELKIND_COMPOSITE_TYPE {
            recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
        } else {
            recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
        }
    }

    /*
     * If the type is an implicitly-created array type, mark it as internally
     * dependent on the element type.  Otherwise, if it has an element type,
     * the dependency is a normal one.
     */
    if OidIsValid((*typeForm).typelem) {
        ObjectAddressSet(&mut referenced, TypeRelationId, (*typeForm).typelem);
        recordDependencyOn(
            &myself,
            &referenced,
            if isImplicitArray {
                DEPENDENCY_INTERNAL
            } else {
                DEPENDENCY_NORMAL
            },
        );
    }

    /*
     * Note: you might expect that we should record an internal dependency of
     * a multirange on its range type here, by analogy with the cases above.
     * But instead, that is done by RangeCreate().
     */
}

// ---------------------------------------------------------------------------
// RenameTypeInternal
//		This renames a type, as well as any associated array type.
// ---------------------------------------------------------------------------
pub unsafe fn RenameTypeInternal(typeOid: Oid, newTypeName: *const c_char, typeNamespace: Oid) {
    let pg_type_desc: Relation;
    let tuple: HeapTuple;
    let typ: Form_pg_type;
    let arrayOid: Oid;
    let oldTypeOid: Oid;

    pg_type_desc = table_open(TypeRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for type {}", typeOid);
    }
    typ = GETSTRUCT(tuple) as Form_pg_type;

    /* We are not supposed to be changing schemas here */
    Assert!(typeNamespace == (*typ).typnamespace);

    arrayOid = (*typ).typarray;

    /* Check for a conflicting type name. */
    oldTypeOid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum(newTypeName),
        ObjectIdGetDatum(typeNamespace),
    );

    /*
     * If there is one, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if OidIsValid(oldTypeOid) {
        if get_typisdefined(oldTypeOid)
            && moveArrayTypeName(oldTypeOid, newTypeName, typeNamespace)
        {
            /* successfully dodged the problem */
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "type \"{}\" already exists",
                    std::ffi::CStr::from_ptr(newTypeName).to_string_lossy()
                )
            ); /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /* OK, do the rename --- tuple is a copy, so OK to scribble on it */
    namestrcpy(&mut (*typ).typname, newTypeName);

    CatalogTupleUpdate(pg_type_desc, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(TypeRelationId, typeOid, 0);

    heap_freetuple(tuple);
    table_close(pg_type_desc, RowExclusiveLock);

    /*
     * If the type has an array type, recurse to handle that.
     */
    if OidIsValid(arrayOid) && arrayOid != oldTypeOid {
        let arrname: *mut c_char = makeArrayTypeName(newTypeName, typeNamespace);

        RenameTypeInternal(arrayOid, arrname, typeNamespace);
        pfree(arrname as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// makeArrayTypeName
//	  - given a base type name, make an array type name for it
//
// the caller is responsible for pfreeing the result
// ---------------------------------------------------------------------------
pub unsafe fn makeArrayTypeName(typeName: *const c_char, typeNamespace: Oid) -> *mut c_char {
    let mut arr_name: *mut c_char;
    let mut pass: c_int = 0;
    let mut suffix: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

    /*
     * Per ancient Postgres tradition, array type names are made by prepending
     * an underscore to the base type name.
     *
     * The actual name generation can be farmed out to makeObjectName() by
     * giving it an empty first name component.
     */

    /* First, try with no numeric suffix */
    arr_name = makeObjectName(c"".as_ptr(), typeName, core::ptr::null());

    loop {
        if !SearchSysCacheExists2(
            TYPENAMENSP,
            CStringGetDatum(arr_name),
            ObjectIdGetDatum(typeNamespace),
        ) {
            break;
        }

        /* That attempt conflicted.  Prepare a new name with some digits. */
        pfree(arr_name as *mut c_void);
        pass += 1;
        let s = format!("{}\0", pass);
        let bytes = s.as_bytes();
        let n = core::cmp::min(bytes.len(), suffix.len());
        for k in 0..n {
            suffix[k] = bytes[k] as c_char;
        }
        arr_name = makeObjectName(c"".as_ptr(), typeName, suffix.as_ptr());
    }

    arr_name
}

// ---------------------------------------------------------------------------
// moveArrayTypeName
//	  - try to reassign an array type name that the user wants to use.
//
// Returns true if successfully moved the type, false if not.
// ---------------------------------------------------------------------------
pub unsafe fn moveArrayTypeName(
    typeOid: Oid,
    typeName: *const c_char,
    typeNamespace: Oid,
) -> bool {
    let elemOid: Oid;
    let newname: *mut c_char;

    /* We need do nothing if it's a shell type. */
    if !get_typisdefined(typeOid) {
        return true;
    }

    /* Can't change it if it's not an autogenerated array type. */
    elemOid = get_element_type(typeOid);
    if !OidIsValid(elemOid) || get_array_type(elemOid) != typeOid {
        return false;
    }

    /*
     * OK, use makeArrayTypeName to pick an unused modification of the name.
     */
    newname = makeArrayTypeName(typeName, typeNamespace);

    /* Apply the rename */
    RenameTypeInternal(typeOid, newname, typeNamespace);

    /*
     * We must bump the command counter so that any subsequent use of
     * makeArrayTypeName sees what we just did and doesn't pick the same name.
     */
    CommandCounterIncrement();

    pfree(newname as *mut c_void);

    true
}

// ---------------------------------------------------------------------------
// makeMultirangeTypeName
//	  - given a range type name, make a multirange type name for it
//
// caller is responsible for pfreeing the result
// ---------------------------------------------------------------------------
pub unsafe fn makeMultirangeTypeName(
    rangeTypeName: *const c_char,
    typeNamespace: Oid,
) -> *mut c_char {
    let buf: *mut c_char;
    let rangestr: *const c_char;

    /*
     * If the range type name contains "range" then change that to
     * "multirange". Otherwise add "_multirange" to the end.
     */
    rangestr = strstr(rangeTypeName, c"range".as_ptr());
    if !rangestr.is_null() {
        let prefix: *mut c_char =
            pnstrdup(rangeTypeName, rangestr.offset_from(rangeTypeName) as usize);

        buf = psprintf(c"%s%s%s".as_ptr(), prefix, c"multi".as_ptr(), rangestr);
    } else {
        buf = psprintf(
            c"%s_multirange".as_ptr(),
            pnstrdup(rangeTypeName, (NAMEDATALEN - 12) as usize),
            core::ptr::null(),
            core::ptr::null(),
        );
    }

    /* clip it at NAMEDATALEN-1 bytes */
    *buf.add(pg_mbcliplen(buf, strlen(buf) as c_int, (NAMEDATALEN - 1) as c_int) as usize) =
        b'\0' as c_char;

    if SearchSysCacheExists2(
        TYPENAMENSP,
        CStringGetDatum(buf),
        ObjectIdGetDatum(typeNamespace),
    ) {
        ereport!(
            ERROR,
            errmsg!(
                "type \"{}\" already exists",
                std::ffi::CStr::from_ptr(buf).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        /* C also: errdetail("Failed while creating a multirange type for type \"%s\".", rangeTypeName) */
        /* C also: errhint("You can manually specify a multirange type name using the \"multirange_type_name\" attribute.") */
    }

    pstrdup(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // typname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_type, typname), 4);
        // typnamespace follows the NAMEDATALEN-byte typname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_type, typnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_type>()
                >= core::mem::offset_of!(FormData_pg_type, typcollation)
                    + core::mem::size_of::<Oid>()
        );
    }
}
