//! Translation of postgres/src/include/catalog/pg_shdepend.h
//!
//! The `FormData_pg_shdepend` struct: the fixed-layout part of a pg_shdepend
//! catalog row.  The C header has no `#ifdef CATALOG_VARLEN` cutoff, so every
//! declared column is part of this in-memory struct.
//!
//! pg_shdepend records shared (cross-database) dependencies; only dependencies
//! on roles are explicitly stored.  There is no leading `oid` column - the row
//! is identified by the depender (dbid, classid, objid, objsubid).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_upper_case_globals)]
#![allow(unreachable_patterns)]

use crate::c::int32;
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_int, c_ulong, c_void};
use core::ptr;

use crate::catalog::catalog::{IsPinnedObject, IsSharedRelation};
use crate::catalog::catalog_oids::{
    AuthIdRelationId, AuthMemRelationId, CollationRelationId, ConversionRelationId,
    DatabaseRelationId, DefaultAclRelationId, EventTriggerRelationId, ExtensionRelationId,
    ForeignDataWrapperRelationId, ForeignServerRelationId, LanguageRelationId,
    LargeObjectRelationId, NamespaceRelationId, OperatorClassRelationId, OperatorFamilyRelationId,
    OperatorRelationId, ProcedureRelationId, PublicationRelationId, RelationRelationId,
    SharedDependRelationId, StatisticExtRelationId, SubscriptionRelationId, TSConfigRelationId,
    TSDictionaryRelationId, TableSpaceRelationId, TypeRelationId, UserMappingRelationId,
};
use crate::miscadmin::{IsBootstrapProcessingMode, MyDatabaseId};

use crate::catalog::objectaccess::ObjectAddress;
use crate::nodes::pg_list::{List, ListCell, lfirst, lfirst_oid, lappend, list_free_deep, NIL};
use crate::{foreach, current_cell, ereport, elog, errmsg, Assert};
use crate::utils::elog::{ERROR};

// ---------------------------------------------------------------------------
// Local type/constant placeholders for headers not yet ported.
// ---------------------------------------------------------------------------

// catalog/dependency.h - SharedDependencyType codes.
// TODO(pg-port): replace with the catalog/dependency.h SharedDependencyType enum.
pub type SharedDependencyType = c_int;
const SHARED_DEPENDENCY_INVALID: SharedDependencyType = 0;
const SHARED_DEPENDENCY_OWNER: SharedDependencyType = b'o' as c_int;
const SHARED_DEPENDENCY_ACL: SharedDependencyType = b'a' as c_int;
const SHARED_DEPENDENCY_INITACL: SharedDependencyType = b'i' as c_int;
const SHARED_DEPENDENCY_POLICY: SharedDependencyType = b'r' as c_int;
const SHARED_DEPENDENCY_TABLESPACE: SharedDependencyType = b't' as c_int;

// parsenodes.h - DropBehavior.
// TODO(pg-port): replace with the nodes/parsenodes.h DropBehavior enum.
pub type DropBehavior = c_int;

// storage/lock.h - lock modes.
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 3;
const AccessExclusiveLock: c_int = 8;

// access/stratnum.h / utils/fmgroids.h.
const BTEqualStrategyNumber: c_int = 3;
const F_OIDEQ: Oid = 184;
const F_INT4EQ: Oid = 65;

// utils/syscache.h - SysCacheIdentifier AUTHOID.
const AUTHOID: c_int = 11;

// utils/errcodes.h.
const ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;

// catalog/pg_tablespace_d.h.
const DEFAULTTABLESPACE_OID: Oid = 1663;

// Attribute numbers / column count for pg_shdepend (catalog/pg_shdepend_d.h).
const Anum_pg_shdepend_dbid: c_int = 1;
const Anum_pg_shdepend_classid: c_int = 2;
const Anum_pg_shdepend_objid: c_int = 3;
const Anum_pg_shdepend_objsubid: c_int = 4;
const Anum_pg_shdepend_refclassid: c_int = 5;
const Anum_pg_shdepend_refobjid: c_int = 6;
const Anum_pg_shdepend_deptype: c_int = 7;
const Natts_pg_shdepend: usize = 7;

// catalog/catalog.c index OIDs (private in catalog.rs).
const SharedDependDependerIndexId: Oid = 1232;
const SharedDependReferenceIndexId: Oid = 1233;

// utils/memutils.h - context creation flag set.
const ALLOCSET_DEFAULT_SIZES: c_int = 0;

// access/htup.h - threshold for multi-insert batching.
const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

const InvalidOid: Oid = 0;

#[inline]
fn OidIsValid(o: Oid) -> bool {
    o != InvalidOid
}

// ---------------------------------------------------------------------------
// Opaque types and stubs for not-yet-ported subsystems.
// ---------------------------------------------------------------------------

pub type Relation = *mut c_void;
pub type HeapTuple = *mut HeapTupleData;
pub type TupleDesc = *mut c_void;
pub type SysScanDesc = *mut c_void;
pub type SnapshotData = c_void;
pub type CatalogIndexState = *mut c_void;
pub type ObjectAddresses = c_void;

/* executor/tuptable.h - minimal mirror of the fields used here. */
#[repr(C)]
pub struct TupleTableSlot {
    pub tts_values: *mut Datum,
    pub tts_isnull: *mut bool,
    pub tts_tupleDescriptor: *mut TupleDescData,
}
#[repr(C)]
pub struct TupleDescData {
    pub natts: c_int,
}
pub type MemoryContext = *mut c_void;

#[repr(C)]
pub struct ItemPointerData {
    _private: [u8; 6],
}

#[repr(C)]
pub struct HeapTupleData {
    pub t_len: u32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: *mut c_void,
}

pub use crate::access::common::scankey::ScanKeyData;

/* access/genam.h, access/table.h, access/htup.h, catalog/indexing.h */
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::table::table::table_open(relationId as _, lockmode as _) as _
}
unsafe fn table_close(relation: Relation, lockmode: c_int) {
    crate::access::table::table::table_close(relation as _, lockmode as _)
}
unsafe fn ScanKeyInit(
    entry: *mut ScanKeyData,
    attributeNumber: c_int,
    strategy: c_int,
    procedure: Oid,
    argument: Datum,
) {
    crate::access::common::scankey::ScanKeyInit(
        entry as _,
        attributeNumber as _,
        strategy as _,
        procedure as _,
        argument as _,
    )
}
unsafe fn systable_beginscan(
    heapRelation: Relation,
    indexId: Oid,
    indexOK: bool,
    snapshot: *mut SnapshotData,
    nkeys: c_int,
    key: *mut ScanKeyData,
) -> SysScanDesc {
    crate::access::index::genam::systable_beginscan(
        heapRelation as _,
        indexId as _,
        indexOK,
        snapshot as _,
        nkeys as _,
        key as _,
    ) as _
}
unsafe fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple {
    crate::access::index::genam::systable_getnext(sysscan as _) as _
}
unsafe fn systable_endscan(sysscan: SysScanDesc) {
    crate::access::index::genam::systable_endscan(sysscan as _)
}
unsafe fn systable_recheck_tuple(sysscan: SysScanDesc, tup: HeapTuple) -> bool {
    crate::access::index::genam::systable_recheck_tuple(sysscan as _, tup as _)
}
unsafe fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void {
    crate::access::htup_details::GETSTRUCT(tuple as _) as _
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn heap_form_tuple(tupleDescriptor: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple {
    crate::access::common::heaptuple::heap_form_tuple(tupleDescriptor as _, values as _, isnull as _) as _
}
unsafe fn heap_copytuple(tuple: HeapTuple) -> HeapTuple {
    crate::access::common::heaptuple::heap_copytuple(tuple as _) as _
}
unsafe fn heap_freetuple(htup: HeapTuple) {
    crate::access::common::heaptuple::heap_freetuple(htup as _)
}
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    crate::utils::rel::RelationGetDescr(relation as _) as _
}
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) {
    crate::catalog::indexing::CatalogTupleInsert(heapRel as _, tup as _)
}
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *mut ItemPointerData, tup: HeapTuple) {
    crate::catalog::indexing::CatalogTupleUpdate(heapRel as _, otid as _, tup as _)
}
unsafe fn CatalogTupleDelete(heapRel: Relation, tid: *mut ItemPointerData) {
    crate::catalog::indexing::CatalogTupleDelete(heapRel as _, tid as _)
}
unsafe fn CatalogOpenIndexes(heapRel: Relation) -> CatalogIndexState {
    crate::catalog::indexing::CatalogOpenIndexes(heapRel as _) as _
}
unsafe fn CatalogCloseIndexes(indstate: CatalogIndexState) {
    crate::catalog::indexing::CatalogCloseIndexes(indstate as _)
}
unsafe fn CatalogTuplesMultiInsertWithInfo(
    heapRel: Relation,
    slot: *mut *mut TupleTableSlot,
    ntuples: c_int,
    indstate: CatalogIndexState,
) {
    crate::catalog::indexing::CatalogTuplesMultiInsertWithInfo(
        heapRel as _,
        slot as _,
        ntuples as _,
        indstate as _,
    )
}

/* Datum conversion helpers (postgres.h). */
pub type Datum = usize;
#[inline]
unsafe fn ObjectIdGetDatum(o: Oid) -> Datum {
    o as Datum
}
#[inline]
unsafe fn Int32GetDatum(v: int32) -> Datum {
    v as u32 as Datum
}
#[inline]
unsafe fn CharGetDatum(v: c_char) -> Datum {
    v as u8 as Datum
}

/* executor/tuptable.h - slot helpers (executor/execTuples.c). */
unsafe fn MakeSingleTupleTableSlot(tupleDesc: TupleDesc, tts_ops: *const c_void) -> *mut TupleTableSlot {
    crate::executor::execTuples::MakeSingleTupleTableSlot(tupleDesc as _, tts_ops as _) as _
}
unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::tuptable::ExecClearTuple(slot as _) as _
}
unsafe fn ExecStoreVirtualTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::execTuples::ExecStoreVirtualTuple(slot as _) as _
}
unsafe fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot) {
    crate::executor::execTuples::ExecDropSingleTupleTableSlot(slot as _)
}
static TTSOpsHeapTuple: c_int = 0;

/* utils/palloc.h */
unsafe fn palloc(size: usize) -> *mut c_void {
    crate::utils::palloc::palloc(size as _) as _
}
unsafe fn repalloc(pointer: *mut c_void, size: usize) -> *mut c_void {
    crate::utils::palloc::repalloc(pointer as _, size as _) as _
}
unsafe fn pfree(pointer: *mut c_void) {
    crate::utils::palloc::pfree(pointer as _)
}

/* utils/memutils.h */
unsafe fn AllocSetContextCreate(parent: MemoryContext, name: *const c_char, _flags: c_int) -> MemoryContext {
    crate::utils::mmgr::aset::AllocSetContextCreate(
        parent as _,
        name as _,
        crate::utils::memutils::ALLOCSET_DEFAULT_SIZES,
    ) as _
}
unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    crate::utils::mmgr::mcxt::MemoryContextSwitchTo(context as _) as _
}
unsafe fn MemoryContextDelete(context: MemoryContext) {
    crate::utils::mmgr::mcxt::MemoryContextDelete(context as _)
}
static mut CurrentMemoryContext: MemoryContext = ptr::null_mut();

/* access/xact.h */
unsafe fn CommandCounterIncrement() {
    crate::access::transam::xact::CommandCounterIncrement()
}

/* storage/lmgr.h */
unsafe fn LockSharedObject(classId: Oid, objectId: Oid, objsubid: u16, lockmode: c_int) {
    crate::storage::lmgr::lmgr::LockSharedObject(classId as _, objectId as _, objsubid as _, lockmode as _)
}

/* utils/syscache.h */
unsafe fn SearchSysCacheExists1(cacheId: c_int, key1: Datum) -> bool {
    crate::utils::cache::syscache::SearchSysCacheExists(cacheId as _, key1 as _, 0, 0, 0)
}

/* commands/tablespace.h, commands/dbcommands.h */
unsafe fn get_tablespace_name(_spc_oid: Oid) -> *mut c_char { crate::commands::tablespace::get_tablespace_name(_spc_oid as _) }
unsafe fn get_database_name(dbid: Oid) -> *mut c_char {
    crate::commands::dbcommands::get_database_name(dbid as _) as _
}

/* catalog/objectaddress.c */
unsafe fn getObjectDescription(object: *const ObjectAddress, missing_ok: bool) -> *mut c_char {
    crate::catalog::objectaddress_impl::getObjectDescription(object as _, missing_ok) as _
}
unsafe fn new_object_addresses() -> *mut ObjectAddresses {
    crate::catalog::dependency::new_object_addresses() as _
}
unsafe fn add_exact_object_address(object: *const ObjectAddress, addrs: *mut ObjectAddresses) {
    crate::catalog::dependency::add_exact_object_address(object as _, addrs as _)
}
unsafe fn sort_object_addresses(addrs: *mut ObjectAddresses) {
    crate::catalog::dependency::sort_object_addresses(addrs as _)
}
unsafe fn free_object_addresses(addrs: *mut ObjectAddresses) {
    crate::catalog::dependency::free_object_addresses(addrs as _)
}

/* catalog/dependency.c */
unsafe fn AcquireDeletionLock(object: *const ObjectAddress, flags: c_int) {
    crate::catalog::dependency::AcquireDeletionLock(object as _, flags as _)
}
unsafe fn ReleaseDeletionLock(object: *const ObjectAddress) {
    crate::catalog::dependency::ReleaseDeletionLock(object as _)
}
unsafe fn performMultipleDeletions(objects: *const ObjectAddresses, behavior: DropBehavior, flags: c_int) {
    crate::catalog::dependency::performMultipleDeletions(objects as _, core::mem::transmute(behavior), flags as _)
}

/* commands/policy.h, catalog/aclchk.c, catalog/pg_init_privs.c */
unsafe fn RemoveRoleFromObjectPolicy(roleid: Oid, classid: Oid, objid: Oid) -> bool {
    crate::commands::policy::RemoveRoleFromObjectPolicy(roleid as _, classid as _, objid as _)
}
unsafe fn RemoveRoleFromObjectACL(roleid: Oid, classid: Oid, objid: Oid) {
    crate::catalog::aclchk::RemoveRoleFromObjectACL(roleid as _, classid as _, objid as _)
}
unsafe fn RemoveRoleFromInitPriv(roleid: Oid, classid: Oid, objid: Oid, objsubid: int32) {
    crate::catalog::aclchk::RemoveRoleFromInitPriv(roleid as _, classid as _, objid as _, objsubid as _)
}
unsafe fn ReplaceRoleInInitPriv(oldroleid: Oid, newroleid: Oid, classid: Oid, objid: Oid, objsubid: int32) {
    crate::catalog::aclchk::ReplaceRoleInInitPriv(oldroleid as _, newroleid as _, classid as _, objid as _, objsubid as _)
}

/* ALTER OWNER routines (commands/<x>.c). */
unsafe fn AlterTypeOwner_oid(oid: Oid, newOwnerId: Oid, hasDependEntry: bool) {
    crate::commands::typecmds::AlterTypeOwner_oid(oid as _, newOwnerId as _, hasDependEntry)
}
unsafe fn AlterSchemaOwner_oid(oid: Oid, newOwnerId: Oid) {
    crate::commands::schemacmds::AlterSchemaOwner_oid(oid as _, newOwnerId as _)
}
unsafe fn ATExecChangeOwner(relationOid: Oid, newOwnerId: Oid, recursing: bool, lockmode: c_int) {
    crate::commands::tablecmds::ATExecChangeOwner(relationOid as _, newOwnerId as _, recursing, lockmode as _)
}
unsafe fn AlterForeignServerOwner_oid(_oid: Oid, _newOwnerId: Oid) { unimplemented!() }
unsafe fn AlterForeignDataWrapperOwner_oid(_oid: Oid, _newOwnerId: Oid) { unimplemented!() }
unsafe fn AlterEventTriggerOwner_oid(_oid: Oid, _newOwnerId: Oid) { unimplemented!() }
unsafe fn AlterPublicationOwner_oid(_oid: Oid, _newOwnerId: Oid) { unimplemented!() }
unsafe fn AlterSubscriptionOwner_oid(_oid: Oid, _newOwnerId: Oid) { unimplemented!() }
unsafe fn AlterObjectOwner_internal(classId: Oid, objectId: Oid, new_ownerId: Oid) {
    crate::commands::alter::AlterObjectOwner_internal(classId as _, objectId as _, new_ownerId as _)
}

// ---------------------------------------------------------------------------
// StringInfo (lib/stringinfo.h) - minimal local mirror.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
pub type StringInfo = *mut StringInfoData;

unsafe fn initStringInfo(str: *mut StringInfoData) {
    crate::lib::stringinfo::initStringInfo(str as _)
}
unsafe fn appendStringInfoChar(str: *mut StringInfoData, ch: c_char) {
    crate::lib::stringinfo::appendStringInfoChar(str as _, ch as _)
}
// appendStringInfo with a format string is rendered via this helper in C; here
// the message strings are formatted by the caller and appended as-is.
unsafe fn appendStringInfoString(str: *mut StringInfoData, s: *const c_char) {
    crate::lib::stringinfo::appendStringInfoString(str as _, s as _)
}
unsafe extern "C" {
    /* lib/stringinfo.h - the C variadic formatting append. */
    fn appendStringInfo(str: *mut StringInfoData, fmt: *const c_char, ...);
}

/* utils/elog.h NLS helpers - no-ops in this port (return msgid as-is). */
#[inline]
unsafe fn ngettext(msgid: *const c_char, _msgid_plural: *const c_char, n: c_ulong) -> *const c_char {
    if n == 1 { msgid } else { _msgid_plural }
}
#[inline]
unsafe fn gettext_(s: *const c_char) -> *const c_char {
    s
}

/* utils/palloc.h - the libc-style qsort used for the comparator. */
unsafe extern "C" {
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> c_int>,
    );
}

/* access/relation.h - the tuple descriptor stored in the relcache entry. */
unsafe fn rd_att(_relation: Relation) -> TupleDesc {
    RelationGetDescr(_relation)
}

// ---------------------------------------------------------------------------
// SharedDependencyObjectType / ShDependObjectInfo (private to this module).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum SharedDependencyObjectType {
    LOCAL_OBJECT,
    SHARED_OBJECT,
    REMOTE_OBJECT,
}
use SharedDependencyObjectType::*;

#[derive(Clone, Copy)]
#[repr(C)]
struct ShDependObjectInfo {
    object: ObjectAddress,
    deptype: c_char,
    objtype: SharedDependencyObjectType,
}

/*
 * A struct to keep track of dependencies found in other databases.
 */
#[repr(C)]
struct remoteDep {
    dbOid: Oid,
    count: c_int,
}

/*
 * FormData_pg_shdepend - the fixed part of a pg_shdepend row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_shdepend {
    /* OID of database containing object; 0 denotes a shared object */
    pub dbid: Oid,
    /* OID of table (pg_class) containing the dependent object */
    pub classid: Oid,
    /* OID of the dependent object itself */
    pub objid: Oid,
    /* column number, or 0 if not used */
    pub objsubid: int32,
    /* OID of table (pg_class) containing the referenced object */
    pub refclassid: Oid,
    /* OID of the referenced object itself */
    pub refobjid: Oid,
    /* dependency type; see codes in dependency.h (SharedDependencyType) */
    pub deptype: c_char,
}

/*
 * Form_pg_shdepend corresponds to a pointer to a row with the format of the
 * pg_shdepend relation.
 */
pub type Form_pg_shdepend = *mut FormData_pg_shdepend;

// ---------------------------------------------------------------------------
// Forward declarations of static helpers defined in this file.
// ---------------------------------------------------------------------------

/* catalog/objectaddress.h - ObjectAddressSet macro. */
#[inline]
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

const MAX_REPORTED_DEPS: c_int = 100;

/*
 * recordSharedDependencyOn
 *
 * Record a dependency between 2 objects via their respective ObjectAddresses.
 * The first argument is the dependent object, the second the one it
 * references (which must be a shared object).
 *
 * This locks the referenced object and makes sure it still exists.
 * Then it creates an entry in pg_shdepend.  The lock is kept until
 * the end of the transaction.
 *
 * Dependencies on pinned objects are not recorded.
 */
pub unsafe fn recordSharedDependencyOn(
    depender: *mut ObjectAddress,
    referenced: *mut ObjectAddress,
    deptype: SharedDependencyType,
) {
    let sdepRel: Relation;

    /*
     * Objects in pg_shdepend can't have SubIds.
     */
    Assert!((*depender).objectSubId == 0);
    Assert!((*referenced).objectSubId == 0);

    /*
     * During bootstrap, do nothing since pg_shdepend may not exist yet.
     * initdb will fill in appropriate pg_shdepend entries after bootstrap.
     */
    if IsBootstrapProcessingMode() {
        return;
    }

    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    /* If the referenced object is pinned, do nothing. */
    if !IsPinnedObject((*referenced).classId, (*referenced).objectId) {
        shdepAddDependency(
            sdepRel,
            (*depender).classId,
            (*depender).objectId,
            (*depender).objectSubId,
            (*referenced).classId,
            (*referenced).objectId,
            deptype,
        );
    }

    table_close(sdepRel, RowExclusiveLock);
}

/*
 * recordDependencyOnOwner
 *
 * A convenient wrapper of recordSharedDependencyOn -- register the specified
 * user as owner of the given object.
 *
 * Note: it's the caller's responsibility to ensure that there isn't an owner
 * entry for the object already.
 */
pub unsafe fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) {
    let mut myself = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };
    let mut referenced = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };

    myself.classId = classId;
    myself.objectId = objectId;
    myself.objectSubId = 0;

    referenced.classId = AuthIdRelationId;
    referenced.objectId = owner;
    referenced.objectSubId = 0;

    recordSharedDependencyOn(&mut myself, &mut referenced, SHARED_DEPENDENCY_OWNER);
}

/*
 * shdepChangeDep
 *
 * Update shared dependency records to account for an updated referenced
 * object.  This is an internal workhorse for operations such as changing
 * an object's owner.
 *
 * There must be no more than one existing entry for the given dependent
 * object and dependency type!	So in practice this can only be used for
 * updating SHARED_DEPENDENCY_OWNER and SHARED_DEPENDENCY_TABLESPACE
 * entries, which should have that property.
 *
 * If there is no previous entry, we assume it was referencing a PINned
 * object, so we create a new entry.  If the new referenced object is
 * PINned, we don't create an entry (and drop the old one, if any).
 * (For tablespaces, we don't record dependencies in certain cases, so
 * there are other possible reasons for entries to be missing.)
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
unsafe fn shdepChangeDep(
    sdepRel: Relation,
    classid: Oid,
    objid: Oid,
    objsubid: int32,
    refclassid: Oid,
    refobjid: Oid,
    deptype: SharedDependencyType,
) {
    let dbid: Oid = classIdGetDbId(classid);
    let mut oldtup: HeapTuple = ptr::null_mut();
    let mut scantup: HeapTuple;
    let mut key: [ScanKeyData; 4] = [
        core::mem::zeroed(),
        core::mem::zeroed(),
        core::mem::zeroed(),
        core::mem::zeroed(),
    ];
    let scan: SysScanDesc;

    /*
     * Make sure the new referenced object doesn't go away while we record the
     * dependency.
     */
    shdepLockAndCheckObject(refclassid, refobjid);

    /*
     * Look for a previous entry
     */
    ScanKeyInit(&mut key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(dbid));
    ScanKeyInit(&mut key[1], Anum_pg_shdepend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
    ScanKeyInit(&mut key[2], Anum_pg_shdepend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
    ScanKeyInit(&mut key[3], Anum_pg_shdepend_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, ptr::null_mut(), 4, key.as_mut_ptr());

    loop {
        scantup = systable_getnext(scan);
        if scantup.is_null() {
            break;
        }
        /* Ignore if not of the target dependency type */
        if (*(GETSTRUCT(scantup) as Form_pg_shdepend)).deptype as SharedDependencyType != deptype {
            continue;
        }
        /* Caller screwed up if multiple matches */
        if !oldtup.is_null() {
            elog!(
                ERROR,
                "multiple pg_shdepend entries for object {}/{}/{} deptype {}",
                classid,
                objid,
                objsubid,
                deptype as u8 as char
            );
        }
        oldtup = heap_copytuple(scantup);
    }

    systable_endscan(scan);

    if IsPinnedObject(refclassid, refobjid) {
        /* No new entry needed, so just delete existing entry if any */
        if !oldtup.is_null() {
            CatalogTupleDelete(sdepRel, &mut (*oldtup).t_self);
        }
    } else if !oldtup.is_null() {
        /* Need to update existing entry */
        let shForm = GETSTRUCT(oldtup) as Form_pg_shdepend;

        /* Since oldtup is a copy, we can just modify it in-memory */
        (*shForm).refclassid = refclassid;
        (*shForm).refobjid = refobjid;

        CatalogTupleUpdate(sdepRel, &mut (*oldtup).t_self, oldtup);
    } else {
        /* Need to insert new entry */
        let mut values: [Datum; Natts_pg_shdepend] = [0; Natts_pg_shdepend];
        let mut nulls: [bool; Natts_pg_shdepend] = [false; Natts_pg_shdepend];

        values[(Anum_pg_shdepend_dbid - 1) as usize] = ObjectIdGetDatum(dbid);
        values[(Anum_pg_shdepend_classid - 1) as usize] = ObjectIdGetDatum(classid);
        values[(Anum_pg_shdepend_objid - 1) as usize] = ObjectIdGetDatum(objid);
        values[(Anum_pg_shdepend_objsubid - 1) as usize] = Int32GetDatum(objsubid);

        values[(Anum_pg_shdepend_refclassid - 1) as usize] = ObjectIdGetDatum(refclassid);
        values[(Anum_pg_shdepend_refobjid - 1) as usize] = ObjectIdGetDatum(refobjid);
        values[(Anum_pg_shdepend_deptype - 1) as usize] = CharGetDatum(deptype as c_char);

        /*
         * we are reusing oldtup just to avoid declaring a new variable, but
         * it's certainly a new tuple
         */
        oldtup = heap_form_tuple(RelationGetDescr(sdepRel), values.as_mut_ptr(), nulls.as_mut_ptr());
        CatalogTupleInsert(sdepRel, oldtup);
    }

    if !oldtup.is_null() {
        heap_freetuple(oldtup);
    }
}

/*
 * changeDependencyOnOwner
 *
 * Update the shared dependencies to account for the new owner.
 *
 * Note: we don't need an objsubid argument because only whole objects
 * have owners.
 */
pub unsafe fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwnerId: Oid) {
    let sdepRel: Relation;

    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    /* Adjust the SHARED_DEPENDENCY_OWNER entry */
    shdepChangeDep(sdepRel, classId, objectId, 0, AuthIdRelationId, newOwnerId, SHARED_DEPENDENCY_OWNER);

    /*----------
     * There should never be a SHARED_DEPENDENCY_ACL entry for the owner,
     * so get rid of it if there is one.  This can happen if the new owner
     * was previously granted some rights to the object.
     *
     * This step is analogous to aclnewowner's removal of duplicate entries
     * in the ACL.  We have to do it to handle this scenario:
     *		A grants some rights on an object to B
     *		ALTER OWNER changes the object's owner to B
     *		ALTER OWNER changes the object's owner to C
     * The third step would remove all mention of B from the object's ACL,
     * but we'd still have a SHARED_DEPENDENCY_ACL for B if we did not do
     * things this way.
     *
     * The rule against having a SHARED_DEPENDENCY_ACL entry for the owner
     * allows us to fix things up in just this one place, without having
     * to make the various ALTER OWNER routines each know about it.
     *----------
     */
    shdepDropDependency(sdepRel, classId, objectId, 0, true, AuthIdRelationId, newOwnerId, SHARED_DEPENDENCY_ACL);

    /*
     * However, nothing need be done about SHARED_DEPENDENCY_INITACL entries,
     * since those exist whether or not the role is the object's owner, and
     * ALTER OWNER does not modify the underlying pg_init_privs entry.
     */

    table_close(sdepRel, RowExclusiveLock);
}

/*
 * recordDependencyOnTablespace
 *
 * A convenient wrapper of recordSharedDependencyOn -- register the specified
 * tablespace as default for the given object.
 *
 * Note: it's the caller's responsibility to ensure that there isn't a
 * tablespace entry for the object already.
 */
pub unsafe fn recordDependencyOnTablespace(classId: Oid, objectId: Oid, tablespace: Oid) {
    let mut myself = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };
    let mut referenced = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };

    ObjectAddressSet(&mut myself, classId, objectId);
    ObjectAddressSet(&mut referenced, TableSpaceRelationId, tablespace);

    recordSharedDependencyOn(&mut myself, &mut referenced, SHARED_DEPENDENCY_TABLESPACE);
}

/*
 * changeDependencyOnTablespace
 *
 * Update the shared dependencies to account for the new tablespace.
 *
 * Note: we don't need an objsubid argument because only whole objects
 * have tablespaces.
 */
pub unsafe fn changeDependencyOnTablespace(classId: Oid, objectId: Oid, newTablespaceId: Oid) {
    let sdepRel: Relation;

    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    if newTablespaceId != DEFAULTTABLESPACE_OID && newTablespaceId != InvalidOid {
        shdepChangeDep(sdepRel, classId, objectId, 0, TableSpaceRelationId, newTablespaceId, SHARED_DEPENDENCY_TABLESPACE);
    } else {
        shdepDropDependency(sdepRel, classId, objectId, 0, true, InvalidOid, InvalidOid, SHARED_DEPENDENCY_INVALID);
    }

    table_close(sdepRel, RowExclusiveLock);
}

/*
 * getOidListDiff
 *		Helper for updateAclDependencies.
 *
 * Takes two Oid arrays and removes elements that are common to both arrays,
 * leaving just those that are in one input but not the other.
 * We assume both arrays have been sorted and de-duped.
 */
unsafe fn getOidListDiff(list1: *mut Oid, nlist1: *mut c_int, list2: *mut Oid, nlist2: *mut c_int) {
    let mut in1: c_int;
    let mut in2: c_int;
    let mut out1: c_int;
    let mut out2: c_int;

    in1 = 0;
    in2 = 0;
    out1 = 0;
    out2 = 0;
    while in1 < *nlist1 && in2 < *nlist2 {
        if *list1.add(in1 as usize) == *list2.add(in2 as usize) {
            /* skip over duplicates */
            in1 += 1;
            in2 += 1;
        } else if *list1.add(in1 as usize) < *list2.add(in2 as usize) {
            /* list1[in1] is not in list2 */
            *list1.add(out1 as usize) = *list1.add(in1 as usize);
            out1 += 1;
            in1 += 1;
        } else {
            /* list2[in2] is not in list1 */
            *list2.add(out2 as usize) = *list2.add(in2 as usize);
            out2 += 1;
            in2 += 1;
        }
    }

    /* any remaining list1 entries are not in list2 */
    while in1 < *nlist1 {
        *list1.add(out1 as usize) = *list1.add(in1 as usize);
        out1 += 1;
        in1 += 1;
    }

    /* any remaining list2 entries are not in list1 */
    while in2 < *nlist2 {
        *list2.add(out2 as usize) = *list2.add(in2 as usize);
        out2 += 1;
        in2 += 1;
    }

    *nlist1 = out1;
    *nlist2 = out2;
}

/*
 * updateAclDependencies
 *		Update the pg_shdepend info for an object's ACL during GRANT/REVOKE.
 *
 * NOTE: Both input arrays must be sorted and de-duped.  The arrays are pfreed
 * before return.
 */
pub unsafe fn updateAclDependencies(
    classId: Oid,
    objectId: Oid,
    objsubId: int32,
    ownerId: Oid,
    noldmembers: c_int,
    oldmembers: *mut Oid,
    nnewmembers: c_int,
    newmembers: *mut Oid,
) {
    updateAclDependenciesWorker(
        classId,
        objectId,
        objsubId,
        ownerId,
        SHARED_DEPENDENCY_ACL,
        noldmembers,
        oldmembers,
        nnewmembers,
        newmembers,
    );
}

/*
 * updateInitAclDependencies
 *		Update the pg_shdepend info for a pg_init_privs entry.
 */
pub unsafe fn updateInitAclDependencies(
    classId: Oid,
    objectId: Oid,
    objsubId: int32,
    noldmembers: c_int,
    oldmembers: *mut Oid,
    nnewmembers: c_int,
    newmembers: *mut Oid,
) {
    updateAclDependenciesWorker(
        classId,
        objectId,
        objsubId,
        InvalidOid, /* ownerId will not be consulted */
        SHARED_DEPENDENCY_INITACL,
        noldmembers,
        oldmembers,
        nnewmembers,
        newmembers,
    );
}

/* Common code for the above two functions */
unsafe fn updateAclDependenciesWorker(
    classId: Oid,
    objectId: Oid,
    objsubId: int32,
    ownerId: Oid,
    deptype: SharedDependencyType,
    mut noldmembers: c_int,
    oldmembers: *mut Oid,
    mut nnewmembers: c_int,
    newmembers: *mut Oid,
) {
    let sdepRel: Relation;
    let mut i: c_int;

    /*
     * Remove entries that are common to both lists; those represent existing
     * dependencies we don't need to change.
     *
     * OK to overwrite the inputs since we'll pfree them anyway.
     */
    getOidListDiff(oldmembers, &mut noldmembers, newmembers, &mut nnewmembers);

    if noldmembers > 0 || nnewmembers > 0 {
        sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

        /* Add new dependencies that weren't already present */
        i = 0;
        while i < nnewmembers {
            let roleid: Oid = *newmembers.add(i as usize);

            /*
             * For SHARED_DEPENDENCY_ACL entries, skip the owner: she has an
             * OWNER shdep entry instead.  But for INITACL entries, we record
             * the owner too.
             */
            if deptype == SHARED_DEPENDENCY_ACL && roleid == ownerId {
                i += 1;
                continue;
            }

            /* Skip pinned roles; they don't need dependency entries */
            if IsPinnedObject(AuthIdRelationId, roleid) {
                i += 1;
                continue;
            }

            shdepAddDependency(sdepRel, classId, objectId, objsubId, AuthIdRelationId, roleid, deptype);
            i += 1;
        }

        /* Drop no-longer-used old dependencies */
        i = 0;
        while i < noldmembers {
            let roleid: Oid = *oldmembers.add(i as usize);

            /* Skip the owner for ACL entries, same as above */
            if deptype == SHARED_DEPENDENCY_ACL && roleid == ownerId {
                i += 1;
                continue;
            }

            /* Skip pinned roles */
            if IsPinnedObject(AuthIdRelationId, roleid) {
                i += 1;
                continue;
            }

            shdepDropDependency(
                sdepRel,
                classId,
                objectId,
                objsubId,
                false, /* exact match on objsubId */
                AuthIdRelationId,
                roleid,
                deptype,
            );
            i += 1;
        }

        table_close(sdepRel, RowExclusiveLock);
    }

    if !oldmembers.is_null() {
        pfree(oldmembers as *mut c_void);
    }
    if !newmembers.is_null() {
        pfree(newmembers as *mut c_void);
    }
}

/*
 * qsort comparator for ShDependObjectInfo items
 */
unsafe extern "C" fn shared_dependency_comparator(a: *const c_void, b: *const c_void) -> c_int {
    let obja = a as *const ShDependObjectInfo;
    let objb = b as *const ShDependObjectInfo;

    /*
     * Primary sort key is OID ascending.
     */
    if (*obja).object.objectId < (*objb).object.objectId {
        return -1;
    }
    if (*obja).object.objectId > (*objb).object.objectId {
        return 1;
    }

    /*
     * Next sort on catalog ID, in case identical OIDs appear in different
     * catalogs.  Sort direction is pretty arbitrary here.
     */
    if (*obja).object.classId < (*objb).object.classId {
        return -1;
    }
    if (*obja).object.classId > (*objb).object.classId {
        return 1;
    }

    /*
     * Sort on object subId.
     *
     * We sort the subId as an unsigned int so that 0 (the whole object) will
     * come first.
     */
    if ((*obja).object.objectSubId as u32) < ((*objb).object.objectSubId as u32) {
        return -1;
    }
    if ((*obja).object.objectSubId as u32) > ((*objb).object.objectSubId as u32) {
        return 1;
    }

    /*
     * Last, sort on deptype, in case the same object has multiple dependency
     * types.  (Note that there's no need to consider objtype, as that's
     * determined by the catalog OID.)
     */
    if (*obja).deptype < (*objb).deptype {
        return -1;
    }
    if (*obja).deptype > (*objb).deptype {
        return 1;
    }

    0
}

/*
 * checkSharedDependencies
 *
 * Check whether there are shared dependency entries for a given shared
 * object; return true if so.
 *
 * In addition, return a string containing a newline-separated list of object
 * descriptions that depend on the shared object, or NULL if none is found.
 */
#[no_mangle]
pub unsafe fn checkSharedDependencies(
    classId: Oid,
    objectId: Oid,
    detail_msg: *mut *mut c_char,
    detail_log_msg: *mut *mut c_char,
) -> bool {
    let sdepRel: Relation;
    let mut key: [ScanKeyData; 2] = [core::mem::zeroed(), core::mem::zeroed()];
    let scan: SysScanDesc;
    let mut tup: HeapTuple;
    let mut numReportedDeps: c_int = 0;
    let mut numNotReportedDeps: c_int = 0;
    let mut numNotReportedDbs: c_int = 0;
    let mut remDeps: *mut List = NIL;
    let mut cell: *mut ListCell;
    let mut object = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };
    let mut objects: *mut ShDependObjectInfo;
    let mut numobjects: c_int;
    let mut allocedobjects: c_int;
    let mut descs: StringInfoData = StringInfoData { data: ptr::null_mut(), len: 0, maxlen: 0, cursor: 0 };
    let mut alldescs: StringInfoData = StringInfoData { data: ptr::null_mut(), len: 0, maxlen: 0, cursor: 0 };

    /* This case can be dispatched quickly */
    if IsPinnedObject(classId, objectId) {
        object.classId = classId;
        object.objectId = objectId;
        object.objectSubId = 0;
        ereport!(
            ERROR,
            errmsg!(
                "cannot drop {} because it is required by the database system",
                std::ffi::CStr::from_ptr(getObjectDescription(&object, false)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
    }

    /*
     * We limit the number of dependencies reported to the client to
     * MAX_REPORTED_DEPS, since client software may not deal well with
     * enormous error strings.  The server log always gets a full report.
     */
    allocedobjects = 128; /* arbitrary initial array size */
    objects = palloc(allocedobjects as usize * core::mem::size_of::<ShDependObjectInfo>()) as *mut ShDependObjectInfo;
    numobjects = 0;
    initStringInfo(&mut descs);
    initStringInfo(&mut alldescs);

    sdepRel = table_open(SharedDependRelationId, AccessShareLock);

    ScanKeyInit(&mut key[0], Anum_pg_shdepend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&mut key[1], Anum_pg_shdepend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));

    scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, ptr::null_mut(), 2, key.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let sdepForm = GETSTRUCT(tup) as Form_pg_shdepend;

        object.classId = (*sdepForm).classid;
        object.objectId = (*sdepForm).objid;
        object.objectSubId = (*sdepForm).objsubid;

        /*
         * If it's a dependency local to this database or it's a shared
         * object, add it to the objects array.
         *
         * If it's a remote dependency, keep track of it so we can report the
         * number of them later.
         */
        if (*sdepForm).dbid == MyDatabaseId || (*sdepForm).dbid == InvalidOid {
            if numobjects >= allocedobjects {
                allocedobjects *= 2;
                objects = repalloc(
                    objects as *mut c_void,
                    allocedobjects as usize * core::mem::size_of::<ShDependObjectInfo>(),
                ) as *mut ShDependObjectInfo;
            }
            (*objects.add(numobjects as usize)).object = object;
            (*objects.add(numobjects as usize)).deptype = (*sdepForm).deptype;
            (*objects.add(numobjects as usize)).objtype = if (*sdepForm).dbid == MyDatabaseId {
                LOCAL_OBJECT
            } else {
                SHARED_OBJECT
            };
            numobjects += 1;
        } else {
            /* It's not local nor shared, so it must be remote. */
            let mut dep: *mut remoteDep;
            let mut stored: bool = false;

            /*
             * XXX this info is kept on a simple List.  Maybe it's not good
             * for performance, but using a hash table seems needlessly
             * complex.
             */
            foreach!(cell, remDeps, {
                dep = lfirst(current_cell!(cell)) as *mut remoteDep;
                if (*dep).dbOid == (*sdepForm).dbid {
                    (*dep).count += 1;
                    stored = true;
                    break;
                }
            });
            if !stored {
                dep = palloc(core::mem::size_of::<remoteDep>()) as *mut remoteDep;
                (*dep).dbOid = (*sdepForm).dbid;
                (*dep).count = 1;
                remDeps = lappend(remDeps, dep as *mut c_void);
            }
        }
    }

    systable_endscan(scan);

    table_close(sdepRel, AccessShareLock);

    /*
     * Sort and report local and shared objects.
     */
    if numobjects > 1 {
        qsort(
            objects as *mut c_void,
            numobjects as usize,
            core::mem::size_of::<ShDependObjectInfo>(),
            Some(shared_dependency_comparator),
        );
    }

    for i in 0..numobjects {
        if numReportedDeps < MAX_REPORTED_DEPS {
            numReportedDeps += 1;
            storeObjectDescription(
                &mut descs,
                (*objects.add(i as usize)).objtype,
                &mut (*objects.add(i as usize)).object,
                (*objects.add(i as usize)).deptype as SharedDependencyType,
                0,
            );
        } else {
            numNotReportedDeps += 1;
        }
        storeObjectDescription(
            &mut alldescs,
            (*objects.add(i as usize)).objtype,
            &mut (*objects.add(i as usize)).object,
            (*objects.add(i as usize)).deptype as SharedDependencyType,
            0,
        );
    }

    /*
     * Summarize dependencies in remote databases.
     */
    foreach!(cell, remDeps, {
        let dep = lfirst(current_cell!(cell)) as *mut remoteDep;

        object.classId = DatabaseRelationId;
        object.objectId = (*dep).dbOid;
        object.objectSubId = 0;

        if numReportedDeps < MAX_REPORTED_DEPS {
            numReportedDeps += 1;
            storeObjectDescription(&mut descs, REMOTE_OBJECT, &mut object, SHARED_DEPENDENCY_INVALID, (*dep).count);
        } else {
            numNotReportedDbs += 1;
        }
        storeObjectDescription(&mut alldescs, REMOTE_OBJECT, &mut object, SHARED_DEPENDENCY_INVALID, (*dep).count);
    });

    pfree(objects as *mut c_void);
    list_free_deep(remDeps);

    if descs.len == 0 {
        pfree(descs.data as *mut c_void);
        pfree(alldescs.data as *mut c_void);
        *detail_msg = ptr::null_mut();
        *detail_log_msg = ptr::null_mut();
        return false;
    }

    if numNotReportedDeps > 0 {
        appendStringInfo(
            &mut descs,
            ngettext(
                c"\nand %d other object (see server log for list)".as_ptr(),
                c"\nand %d other objects (see server log for list)".as_ptr(),
                numNotReportedDeps as c_ulong,
            ),
            numNotReportedDeps,
        );
    }
    if numNotReportedDbs > 0 {
        appendStringInfo(
            &mut descs,
            ngettext(
                c"\nand objects in %d other database (see server log for list)".as_ptr(),
                c"\nand objects in %d other databases (see server log for list)".as_ptr(),
                numNotReportedDbs as c_ulong,
            ),
            numNotReportedDbs,
        );
    }

    *detail_msg = descs.data;
    *detail_log_msg = alldescs.data;
    true
}

/*
 * copyTemplateDependencies
 *
 * Routine to create the initial shared dependencies of a new database.
 * We simply copy the dependencies from the template database.
 */
pub unsafe fn copyTemplateDependencies(templateDbId: Oid, newDbId: Oid) {
    let sdepRel: Relation;
    let sdepDesc: TupleDesc;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
    let scan: SysScanDesc;
    let mut tup: HeapTuple;
    let indstate: CatalogIndexState;
    let slot: *mut *mut TupleTableSlot;
    let mut max_slots: c_int;
    let mut slot_init_count: c_int;
    let mut slot_stored_count: c_int;

    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);
    sdepDesc = RelationGetDescr(sdepRel);

    /*
     * Allocate the slots to use, but delay costly initialization until we
     * know that they will be used.
     */
    max_slots = (MAX_CATALOG_MULTI_INSERT_BYTES / core::mem::size_of::<FormData_pg_shdepend>()) as c_int;
    slot = palloc(core::mem::size_of::<*mut TupleTableSlot>() * max_slots as usize) as *mut *mut TupleTableSlot;

    indstate = CatalogOpenIndexes(sdepRel);

    /* Scan all entries with dbid = templateDbId */
    ScanKeyInit(&mut key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(templateDbId));

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, ptr::null_mut(), 1, key.as_mut_ptr());

    /* number of slots currently storing tuples */
    slot_stored_count = 0;
    /* number of slots currently initialized */
    slot_init_count = 0;

    /*
     * Copy the entries of the original database, changing the database Id to
     * that of the new database.  Note that because we are not copying rows
     * with dbId == 0 (ie, rows describing dependent shared objects) we won't
     * copy the ownership dependency of the template database itself; this is
     * what we want.
     */
    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let shdep: Form_pg_shdepend;

        if slot_init_count < max_slots {
            *slot.add(slot_stored_count as usize) = MakeSingleTupleTableSlot(sdepDesc, &raw const TTSOpsHeapTuple as *const c_void);
            slot_init_count += 1;
        }

        let cur = *slot.add(slot_stored_count as usize);
        ExecClearTuple(cur);

        ptr::write_bytes(
            (*cur).tts_isnull,
            0,
            (*(*cur).tts_tupleDescriptor).natts as usize,
        );

        shdep = GETSTRUCT(tup) as Form_pg_shdepend;

        *(*cur).tts_values.add((Anum_pg_shdepend_dbid - 1) as usize) = ObjectIdGetDatum(newDbId);
        *(*cur).tts_values.add((Anum_pg_shdepend_classid - 1) as usize) = (*shdep).classid as Datum;
        *(*cur).tts_values.add((Anum_pg_shdepend_objid - 1) as usize) = (*shdep).objid as Datum;
        *(*cur).tts_values.add((Anum_pg_shdepend_objsubid - 1) as usize) = (*shdep).objsubid as u32 as Datum;
        *(*cur).tts_values.add((Anum_pg_shdepend_refclassid - 1) as usize) = (*shdep).refclassid as Datum;
        *(*cur).tts_values.add((Anum_pg_shdepend_refobjid - 1) as usize) = (*shdep).refobjid as Datum;
        *(*cur).tts_values.add((Anum_pg_shdepend_deptype - 1) as usize) = (*shdep).deptype as u8 as Datum;

        ExecStoreVirtualTuple(cur);
        slot_stored_count += 1;

        /* If slots are full, insert a batch of tuples */
        if slot_stored_count == max_slots {
            CatalogTuplesMultiInsertWithInfo(sdepRel, slot, slot_stored_count, indstate);
            slot_stored_count = 0;
        }
    }

    /* Insert any tuples left in the buffer */
    if slot_stored_count > 0 {
        CatalogTuplesMultiInsertWithInfo(sdepRel, slot, slot_stored_count, indstate);
    }

    systable_endscan(scan);

    CatalogCloseIndexes(indstate);
    table_close(sdepRel, RowExclusiveLock);

    /* Drop only the number of slots used */
    for i in 0..slot_init_count {
        ExecDropSingleTupleTableSlot(*slot.add(i as usize));
    }
    pfree(slot as *mut c_void);
}

/*
 * dropDatabaseDependencies
 *
 * Delete pg_shdepend entries corresponding to a database that's being
 * dropped.
 */
#[no_mangle]
pub unsafe fn dropDatabaseDependencies(databaseId: Oid) {
    let sdepRel: Relation;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    /*
     * First, delete all the entries that have the database Oid in the dbid
     * field.
     */
    ScanKeyInit(&mut key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(databaseId));
    /* We leave the other index fields unspecified */

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, ptr::null_mut(), 1, key.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        CatalogTupleDelete(sdepRel, &mut (*tup).t_self);
    }

    systable_endscan(scan);

    /* Now delete all entries corresponding to the database itself */
    shdepDropDependency(sdepRel, DatabaseRelationId, databaseId, 0, true, InvalidOid, InvalidOid, SHARED_DEPENDENCY_INVALID);

    table_close(sdepRel, RowExclusiveLock);
}

/*
 * deleteSharedDependencyRecordsFor
 *
 * Delete all pg_shdepend entries corresponding to an object that's being
 * dropped or modified.  The object is assumed to be either a shared object
 * or local to the current database (the classId tells us which).
 *
 * If objectSubId is zero, we are deleting a whole object, so get rid of
 * pg_shdepend entries for subobjects as well.
 */
#[no_mangle]
pub unsafe fn deleteSharedDependencyRecordsFor(classId: Oid, objectId: Oid, objectSubId: int32) {
    let sdepRel: Relation;

    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    shdepDropDependency(
        sdepRel,
        classId,
        objectId,
        objectSubId,
        objectSubId == 0,
        InvalidOid,
        InvalidOid,
        SHARED_DEPENDENCY_INVALID,
    );

    table_close(sdepRel, RowExclusiveLock);
}

/*
 * shdepAddDependency
 *		Internal workhorse for inserting into pg_shdepend
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
unsafe fn shdepAddDependency(
    sdepRel: Relation,
    classId: Oid,
    objectId: Oid,
    objsubId: int32,
    refclassId: Oid,
    refobjId: Oid,
    deptype: SharedDependencyType,
) {
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_shdepend] = [0; Natts_pg_shdepend];
    let mut nulls: [bool; Natts_pg_shdepend] = [false; Natts_pg_shdepend];

    /*
     * Make sure the object doesn't go away while we record the dependency on
     * it.  DROP routines should lock the object exclusively before they check
     * shared dependencies.
     */
    shdepLockAndCheckObject(refclassId, refobjId);

    /*
     * Form the new tuple and record the dependency.
     */
    values[(Anum_pg_shdepend_dbid - 1) as usize] = ObjectIdGetDatum(classIdGetDbId(classId));
    values[(Anum_pg_shdepend_classid - 1) as usize] = ObjectIdGetDatum(classId);
    values[(Anum_pg_shdepend_objid - 1) as usize] = ObjectIdGetDatum(objectId);
    values[(Anum_pg_shdepend_objsubid - 1) as usize] = Int32GetDatum(objsubId);

    values[(Anum_pg_shdepend_refclassid - 1) as usize] = ObjectIdGetDatum(refclassId);
    values[(Anum_pg_shdepend_refobjid - 1) as usize] = ObjectIdGetDatum(refobjId);
    values[(Anum_pg_shdepend_deptype - 1) as usize] = CharGetDatum(deptype as c_char);

    tup = heap_form_tuple(rd_att(sdepRel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(sdepRel, tup);

    /* clean up */
    heap_freetuple(tup);
}

/*
 * shdepDropDependency
 *		Internal workhorse for deleting entries from pg_shdepend.
 *
 * We drop entries having the following properties:
 *	dependent object is the one identified by classId/objectId/objsubId
 *	if refclassId isn't InvalidOid, it must match the entry's refclassid
 *	if refobjId isn't InvalidOid, it must match the entry's refobjid
 *	if deptype isn't SHARED_DEPENDENCY_INVALID, it must match entry's deptype
 *
 * If drop_subobjects is true, we ignore objsubId and consider all entries
 * matching classId/objectId.
 *
 * sdepRel must be the pg_shdepend relation, already opened and suitably
 * locked.
 */
unsafe fn shdepDropDependency(
    sdepRel: Relation,
    classId: Oid,
    objectId: Oid,
    objsubId: int32,
    drop_subobjects: bool,
    refclassId: Oid,
    refobjId: Oid,
    deptype: SharedDependencyType,
) {
    let mut key: [ScanKeyData; 4] = [
        core::mem::zeroed(),
        core::mem::zeroed(),
        core::mem::zeroed(),
        core::mem::zeroed(),
    ];
    let nkeys: c_int;
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* Scan for entries matching the dependent object */
    ScanKeyInit(&mut key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classIdGetDbId(classId)));
    ScanKeyInit(&mut key[1], Anum_pg_shdepend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&mut key[2], Anum_pg_shdepend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));
    if drop_subobjects {
        nkeys = 3;
    } else {
        ScanKeyInit(&mut key[3], Anum_pg_shdepend_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubId));
        nkeys = 4;
    }

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, ptr::null_mut(), nkeys, key.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let shdepForm = GETSTRUCT(tup) as Form_pg_shdepend;

        /* Filter entries according to additional parameters */
        if OidIsValid(refclassId) && (*shdepForm).refclassid != refclassId {
            continue;
        }
        if OidIsValid(refobjId) && (*shdepForm).refobjid != refobjId {
            continue;
        }
        if deptype != SHARED_DEPENDENCY_INVALID && (*shdepForm).deptype as SharedDependencyType != deptype {
            continue;
        }

        /* OK, delete it */
        CatalogTupleDelete(sdepRel, &mut (*tup).t_self);
    }

    systable_endscan(scan);
}

/*
 * classIdGetDbId
 *
 * Get the database Id that should be used in pg_shdepend, given the OID
 * of the catalog containing the object.  For shared objects, it's 0
 * (InvalidOid); for all other objects, it's the current database Id.
 */
unsafe fn classIdGetDbId(classId: Oid) -> Oid {
    let dbId: Oid;

    if IsSharedRelation(classId) {
        dbId = InvalidOid;
    } else {
        dbId = MyDatabaseId;
    }

    dbId
}

/*
 * shdepLockAndCheckObject
 *
 * Lock the object that we are about to record a dependency on.
 * After it's locked, verify that it hasn't been dropped while we
 * weren't looking.  If the object has been dropped, this function
 * does not return!
 */
pub unsafe fn shdepLockAndCheckObject(classId: Oid, objectId: Oid) {
    /* AccessShareLock should be OK, since we are not modifying the object */
    LockSharedObject(classId, objectId, 0, AccessShareLock);

    match classId {
        c if c == AuthIdRelationId => {
            if !SearchSysCacheExists1(AUTHOID, ObjectIdGetDatum(objectId)) {
                ereport!(ERROR, errmsg!("role {} was concurrently dropped", objectId));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            }
        }

        c if c == TableSpaceRelationId => {
            /* For lack of a syscache on pg_tablespace, do this: */
            let tablespace: *mut c_char = get_tablespace_name(objectId);

            if tablespace.is_null() {
                ereport!(ERROR, errmsg!("tablespace {} was concurrently dropped", objectId));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            }
            pfree(tablespace as *mut c_void);
        }

        c if c == DatabaseRelationId => {
            /* For lack of a syscache on pg_database, do this: */
            let database: *mut c_char = get_database_name(objectId);

            if database.is_null() {
                ereport!(ERROR, errmsg!("database {} was concurrently dropped", objectId));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            }
            pfree(database as *mut c_void);
        }

        _ => {
            elog!(ERROR, "unrecognized shared classId: {}", classId);
        }
    }
}

/*
 * storeObjectDescription
 *		Append the description of a dependent object to "descs"
 */
unsafe fn storeObjectDescription(
    descs: StringInfo,
    r#type: SharedDependencyObjectType,
    object: *mut ObjectAddress,
    deptype: SharedDependencyType,
    count: c_int,
) {
    let objdesc: *mut c_char = getObjectDescription(object, false);

    /*
     * An object being dropped concurrently doesn't need to be reported.
     */
    if objdesc.is_null() {
        return;
    }

    /* separate entries with a newline */
    if (*descs).len != 0 {
        appendStringInfoChar(descs, b'\n' as c_char);
    }

    match r#type {
        LOCAL_OBJECT | SHARED_OBJECT => {
            if deptype == SHARED_DEPENDENCY_OWNER {
                appendStringInfo(descs, c"owner of %s".as_ptr(), objdesc);
            } else if deptype == SHARED_DEPENDENCY_ACL {
                appendStringInfo(descs, c"privileges for %s".as_ptr(), objdesc);
            } else if deptype == SHARED_DEPENDENCY_INITACL {
                appendStringInfo(descs, c"initial privileges for %s".as_ptr(), objdesc);
            } else if deptype == SHARED_DEPENDENCY_POLICY {
                appendStringInfo(descs, c"target of %s".as_ptr(), objdesc);
            } else if deptype == SHARED_DEPENDENCY_TABLESPACE {
                appendStringInfo(descs, c"tablespace for %s".as_ptr(), objdesc);
            } else {
                elog!(ERROR, "unrecognized dependency type: {}", deptype as c_int);
            }
        }

        REMOTE_OBJECT => {
            /* translator: %s will always be "database %s" */
            appendStringInfo(
                descs,
                ngettext(c"%d object in %s".as_ptr(), c"%d objects in %s".as_ptr(), count as c_ulong),
                count,
                objdesc,
            );
        }
    }

    pfree(objdesc as *mut c_void);
}

/*
 * shdepDropOwned
 *
 * Drop the objects owned by any one of the given RoleIds.  If a role has
 * access to an object, the grant will be removed as well (but the object
 * will not, of course).
 */
pub unsafe fn shdepDropOwned(roleids: *mut List, behavior: DropBehavior) {
    let sdepRel: Relation;
    let mut cell: *mut ListCell;
    let deleteobjs: *mut ObjectAddresses;

    deleteobjs = new_object_addresses();

    /*
     * We don't need this strong a lock here, but we'll call routines that
     * acquire RowExclusiveLock.  Better get that right now to avoid potential
     * deadlock failures.
     */
    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    /*
     * For each role, find the dependent objects and drop them using the
     * regular (non-shared) dependency management.
     */
    foreach!(cell, roleids, {
        let roleid: Oid = lfirst_oid(current_cell!(cell));
        let mut key: [ScanKeyData; 2] = [core::mem::zeroed(), core::mem::zeroed()];
        let scan: SysScanDesc;
        let mut tuple: HeapTuple;

        /* Doesn't work for pinned objects */
        if IsPinnedObject(AuthIdRelationId, roleid) {
            let mut obj = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };

            obj.classId = AuthIdRelationId;
            obj.objectId = roleid;
            obj.objectSubId = 0;

            ereport!(
                ERROR,
                errmsg!(
                    "cannot drop objects owned by {} because they are required by the database system",
                    std::ffi::CStr::from_ptr(getObjectDescription(&obj, false)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
        }

        ScanKeyInit(&mut key[0], Anum_pg_shdepend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(AuthIdRelationId));
        ScanKeyInit(&mut key[1], Anum_pg_shdepend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

        scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, ptr::null_mut(), 2, key.as_mut_ptr());

        loop {
            tuple = systable_getnext(scan);
            if tuple.is_null() {
                break;
            }
            let sdepForm = GETSTRUCT(tuple) as Form_pg_shdepend;
            let mut obj = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };

            /*
             * We only operate on shared objects and objects in the current
             * database
             */
            if (*sdepForm).dbid != MyDatabaseId && (*sdepForm).dbid != InvalidOid {
                continue;
            }

            match (*sdepForm).deptype as SharedDependencyType {
                /* Shouldn't happen */
                d if d == SHARED_DEPENDENCY_INVALID => {
                    elog!(ERROR, "unexpected dependency type");
                }
                d if d == SHARED_DEPENDENCY_POLICY => {
                    /*
                     * Try to remove role from policy; if unable to, remove
                     * policy.
                     */
                    if !RemoveRoleFromObjectPolicy(roleid, (*sdepForm).classid, (*sdepForm).objid) {
                        obj.classId = (*sdepForm).classid;
                        obj.objectId = (*sdepForm).objid;
                        obj.objectSubId = (*sdepForm).objsubid;

                        /*
                         * Acquire lock on object, then verify this dependency
                         * is still relevant.
                         */
                        AcquireDeletionLock(&obj, 0);
                        if !systable_recheck_tuple(scan, tuple) {
                            ReleaseDeletionLock(&obj);
                            continue;
                        }
                        add_exact_object_address(&obj, deleteobjs);
                    }
                }
                d if d == SHARED_DEPENDENCY_ACL => {
                    /*
                     * Dependencies on role grants are recorded using
                     * SHARED_DEPENDENCY_ACL, but unlike a regular ACL list
                     * there's a separate catalog row for each grant - so
                     * removing the grant just means removing the entire row.
                     */
                    if (*sdepForm).classid != AuthMemRelationId {
                        RemoveRoleFromObjectACL(roleid, (*sdepForm).classid, (*sdepForm).objid);
                        continue;
                    }
                    /* FALLTHROUGH */
                    /*
                     * Save it for deletion below, if it's a local object or a
                     * role grant.
                     */
                    if (*sdepForm).dbid == MyDatabaseId || (*sdepForm).classid == AuthMemRelationId {
                        obj.classId = (*sdepForm).classid;
                        obj.objectId = (*sdepForm).objid;
                        obj.objectSubId = (*sdepForm).objsubid;
                        /* as above */
                        AcquireDeletionLock(&obj, 0);
                        if !systable_recheck_tuple(scan, tuple) {
                            ReleaseDeletionLock(&obj);
                            continue;
                        }
                        add_exact_object_address(&obj, deleteobjs);
                    }
                }
                d if d == SHARED_DEPENDENCY_OWNER => {
                    /*
                     * Save it for deletion below, if it's a local object or a
                     * role grant. Other shared objects, such as databases,
                     * should not be removed here.
                     */
                    if (*sdepForm).dbid == MyDatabaseId || (*sdepForm).classid == AuthMemRelationId {
                        obj.classId = (*sdepForm).classid;
                        obj.objectId = (*sdepForm).objid;
                        obj.objectSubId = (*sdepForm).objsubid;
                        /* as above */
                        AcquireDeletionLock(&obj, 0);
                        if !systable_recheck_tuple(scan, tuple) {
                            ReleaseDeletionLock(&obj);
                            continue;
                        }
                        add_exact_object_address(&obj, deleteobjs);
                    }
                }
                d if d == SHARED_DEPENDENCY_INITACL => {
                    /*
                     * Any mentions of the role that remain in pg_init_privs
                     * entries are just dropped.
                     */

                    /* Shouldn't see a role grant here */
                    Assert!((*sdepForm).classid != AuthMemRelationId);
                    RemoveRoleFromInitPriv(roleid, (*sdepForm).classid, (*sdepForm).objid, (*sdepForm).objsubid);
                }
                _ => {}
            }
        }

        systable_endscan(scan);
    });

    /*
     * For stability of deletion-report ordering, sort the objects into
     * approximate reverse creation order before deletion.
     */
    sort_object_addresses(deleteobjs);

    /* the dependency mechanism does the actual work */
    performMultipleDeletions(deleteobjs, behavior, 0);

    table_close(sdepRel, RowExclusiveLock);

    free_object_addresses(deleteobjs);
}

/*
 * shdepReassignOwned
 *
 * Change the owner of objects owned by any of the roles in roleids to
 * newrole.  Grants are not touched.
 */
pub unsafe fn shdepReassignOwned(roleids: *mut List, newrole: Oid) {
    let sdepRel: Relation;
    let mut cell: *mut ListCell;

    /*
     * We don't need this strong a lock here, but we'll call routines that
     * acquire RowExclusiveLock.  Better get that right now to avoid potential
     * deadlock problems.
     */
    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    foreach!(cell, roleids, {
        let scan: SysScanDesc;
        let mut key: [ScanKeyData; 2] = [core::mem::zeroed(), core::mem::zeroed()];
        let mut tuple: HeapTuple;
        let roleid: Oid = lfirst_oid(current_cell!(cell));

        /* Refuse to work on pinned roles */
        if IsPinnedObject(AuthIdRelationId, roleid) {
            let mut obj = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };

            obj.classId = AuthIdRelationId;
            obj.objectId = roleid;
            obj.objectSubId = 0;

            ereport!(
                ERROR,
                errmsg!(
                    "cannot reassign ownership of objects owned by {} because they are required by the database system",
                    std::ffi::CStr::from_ptr(getObjectDescription(&obj, false)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */

            /*
             * There's no need to tell the whole truth, which is that we
             * didn't track these dependencies at all ...
             */
        }

        ScanKeyInit(&mut key[0], Anum_pg_shdepend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(AuthIdRelationId));
        ScanKeyInit(&mut key[1], Anum_pg_shdepend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

        scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, ptr::null_mut(), 2, key.as_mut_ptr());

        loop {
            tuple = systable_getnext(scan);
            if tuple.is_null() {
                break;
            }
            let sdepForm = GETSTRUCT(tuple) as Form_pg_shdepend;
            let cxt: MemoryContext;
            let oldcxt: MemoryContext;

            /*
             * We only operate on shared objects and objects in the current
             * database
             */
            if (*sdepForm).dbid != MyDatabaseId && (*sdepForm).dbid != InvalidOid {
                continue;
            }

            /*
             * The various DDL routines called here tend to leak memory in
             * CurrentMemoryContext.  Fix that by running each call in a
             * short-lived context.
             */
            cxt = AllocSetContextCreate(CurrentMemoryContext, c"shdepReassignOwned".as_ptr(), ALLOCSET_DEFAULT_SIZES);
            oldcxt = MemoryContextSwitchTo(cxt);

            /* Perform the appropriate processing */
            match (*sdepForm).deptype as SharedDependencyType {
                d if d == SHARED_DEPENDENCY_OWNER => {
                    shdepReassignOwned_Owner(sdepForm, newrole);
                }
                d if d == SHARED_DEPENDENCY_INITACL => {
                    shdepReassignOwned_InitAcl(sdepForm, roleid, newrole);
                }
                d if d == SHARED_DEPENDENCY_ACL
                    || d == SHARED_DEPENDENCY_POLICY
                    || d == SHARED_DEPENDENCY_TABLESPACE =>
                {
                    /* Nothing to do for these entry types */
                }
                _ => {
                    elog!(ERROR, "unrecognized dependency type: {}", (*sdepForm).deptype as c_int);
                }
            }

            /* Clean up */
            MemoryContextSwitchTo(oldcxt);
            MemoryContextDelete(cxt);

            /* Make sure the next iteration will see my changes */
            CommandCounterIncrement();
        }

        systable_endscan(scan);
    });

    table_close(sdepRel, RowExclusiveLock);
}

/*
 * shdepReassignOwned_Owner
 *
 * shdepReassignOwned's processing of SHARED_DEPENDENCY_OWNER entries
 */
unsafe fn shdepReassignOwned_Owner(sdepForm: Form_pg_shdepend, newrole: Oid) {
    /* Issue the appropriate ALTER OWNER call */
    let classid = (*sdepForm).classid;
    if classid == TypeRelationId {
        AlterTypeOwner_oid((*sdepForm).objid, newrole, true);
    } else if classid == NamespaceRelationId {
        AlterSchemaOwner_oid((*sdepForm).objid, newrole);
    } else if classid == RelationRelationId {
        /*
         * Pass recursing = true so that we don't fail on indexes, owned
         * sequences, etc when we happen to visit them before their parent
         * table.
         */
        ATExecChangeOwner((*sdepForm).objid, newrole, true, AccessExclusiveLock);
    } else if classid == DefaultAclRelationId {
        /*
         * Ignore default ACLs; they should be handled by DROP OWNED, not
         * REASSIGN OWNED.
         */
    } else if classid == UserMappingRelationId {
        /* ditto */
    } else if classid == ForeignServerRelationId {
        AlterForeignServerOwner_oid((*sdepForm).objid, newrole);
    } else if classid == ForeignDataWrapperRelationId {
        AlterForeignDataWrapperOwner_oid((*sdepForm).objid, newrole);
    } else if classid == EventTriggerRelationId {
        AlterEventTriggerOwner_oid((*sdepForm).objid, newrole);
    } else if classid == PublicationRelationId {
        AlterPublicationOwner_oid((*sdepForm).objid, newrole);
    } else if classid == SubscriptionRelationId {
        AlterSubscriptionOwner_oid((*sdepForm).objid, newrole);
    } else if classid == CollationRelationId
        || classid == ConversionRelationId
        || classid == OperatorRelationId
        || classid == ProcedureRelationId
        || classid == LanguageRelationId
        || classid == LargeObjectRelationId
        || classid == OperatorFamilyRelationId
        || classid == OperatorClassRelationId
        || classid == ExtensionRelationId
        || classid == StatisticExtRelationId
        || classid == TableSpaceRelationId
        || classid == DatabaseRelationId
        || classid == TSConfigRelationId
        || classid == TSDictionaryRelationId
    {
        /* Generic alter owner cases */
        AlterObjectOwner_internal((*sdepForm).classid, (*sdepForm).objid, newrole);
    } else {
        elog!(ERROR, "unexpected classid {}", (*sdepForm).classid);
    }
}

/*
 * shdepReassignOwned_InitAcl
 *
 * shdepReassignOwned's processing of SHARED_DEPENDENCY_INITACL entries
 */
unsafe fn shdepReassignOwned_InitAcl(sdepForm: Form_pg_shdepend, oldrole: Oid, newrole: Oid) {
    /*
     * Currently, REASSIGN OWNED replaces mentions of oldrole with newrole in
     * pg_init_privs entries, just as it does in the object's regular ACL.
     */
    ReplaceRoleInInitPriv(oldrole, newrole, (*sdepForm).classid, (*sdepForm).objid, (*sdepForm).objsubid);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classid sits right after the 4-byte dbid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_shdepend, classid), 4);
        // The struct must at least span through its last fixed field, deptype.
        assert!(
            core::mem::size_of::<FormData_pg_shdepend>()
                >= core::mem::offset_of!(FormData_pg_shdepend, deptype)
                    + core::mem::size_of::<c_char>()
        );
    }
}
