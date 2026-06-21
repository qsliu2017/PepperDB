//! Translation of postgres/src/include/catalog/pg_attrdef.h
//!
//! The `FormData_pg_attrdef` struct: the fixed-layout part of a pg_attrdef
//! ("attribute defaults") catalog row.  As in the C header, the struct as
//! compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length field (adbin, the
//! pg_node_tree nodeToString representation of the default, guarded by
//! CATALOG_VARLEN) is NOT part of this in-memory struct - it lives only in a
//! real on-disk pg_attrdef tuple and is reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int16;
use crate::postgres_ext::Oid;

// --- Translation of postgres/src/backend/catalog/pg_attrdef.c ---
use crate::prelude::*;
use core::ffi::{c_char, c_int};
use core::ptr::null_mut;

use crate::utils::rel::{Relation, RelationGetRelid, RelationGetDescr};
use crate::access::attnum::AttrNumber;
use crate::access::stratnum::StrategyNumber;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DropBehavior;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::common::heaptuple::{heap_form_tuple, heap_modify_tuple, heap_freetuple};
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext, SysScanDesc};
use crate::access::table::table::{table_open, table_close};
use crate::access::common::relation::{relation_open, relation_close};
use crate::catalog::catalog::GetNewOidWithIndex;
unsafe fn nodeToString(_obj: *const core::ffi::c_void) -> *mut c_char { crate::nodes::outfuncs::nodeToString(_obj as _) }
use crate::catalog::catalog_oids::{AttrDefaultRelationId, AttributeRelationId, RelationRelationId};
use crate::storage::lockdefs::{RowExclusiveLock, AccessShareLock, AccessExclusiveLock, NoLock};
use crate::catalog::pg_attribute::Form_pg_attribute;

// -- Catalog index OIDs (catalog/pg_attrdef.h indexing) -------------------
const AttrDefaultIndexId: Oid    = 2656; // pg_attrdef_adrelid_adnum_index
const AttrDefaultOidIndexId: Oid = 2657; // pg_attrdef_oid_index

// -- pg_attrdef column numbers (catalog/pg_attrdef.h) ---------------------
const Anum_pg_attrdef_oid: AttrNumber     = 1;
const Anum_pg_attrdef_adrelid: AttrNumber = 2;
const Anum_pg_attrdef_adnum: AttrNumber   = 3;
const Anum_pg_attrdef_adbin: AttrNumber   = 4;
const Natts_pg_attrdef: usize             = 4;

// -- pg_attribute column number (catalog/pg_attribute.h) ------------------
const Natts_pg_attribute: usize = 43;
const Anum_pg_attribute_atthasdef: AttrNumber = 13;

// -- Syscache IDs (utils/syscache.h) --------------------------------------
const ATTNUM: c_int = 7;

// -- B-tree strategy / function OIDs --------------------------------------
const BTEqualStrategyNumber: StrategyNumber = 3;
const F_OIDEQ: Oid  = 184;
const F_INT2EQ: Oid = 63;

// -- Dependency kinds (catalog/dependency.h) ------------------------------
const DEPENDENCY_NORMAL: c_char   = b'n' as c_char;
const DEPENDENCY_AUTO: c_char     = b'a' as c_char;
const DEPENDENCY_INTERNAL: c_char = b'i' as c_char;

// -- performDeletion flags (catalog/dependency.h) -------------------------
const PERFORM_DELETION_INTERNAL: c_int = 0x0001;

/*
 * ObjectAddress identifies a database object by class/object/sub-id.
 * (catalog/objectaddress.h)
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: c_int,
}

const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

/// TODO(pg-port): catalog/indexing.c CatalogTupleInsert
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) { crate::catalog::indexing::CatalogTupleInsert(heapRel as _, tup as _); }

/// TODO(pg-port): catalog/indexing.c CatalogTupleUpdate
unsafe fn CatalogTupleUpdate(
    heapRel: Relation,
    otid: *mut crate::storage::itemptr::ItemPointerData,
    tup: HeapTuple,
) {
    crate::catalog::indexing::CatalogTupleUpdate(heapRel as _, otid as _, tup as _);
}

/// TODO(pg-port): catalog/indexing.c CatalogTupleDelete
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut crate::storage::itemptr::ItemPointerData) { crate::catalog::indexing::CatalogTupleDelete(_heapRel as _, _tid as _) }

/// TODO(pg-port): utils/cache/syscache.c SearchSysCacheCopy2
unsafe fn SearchSysCacheCopy2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple {
    let tuple = crate::utils::cache::syscache::SearchSysCache2(cacheId, key1, key2);
    if !HeapTupleIsValid(tuple) {
        return tuple;
    }
    let newtuple = crate::access::common::heaptuple::heap_copytuple(tuple as _) as HeapTuple;
    crate::utils::cache::syscache::ReleaseSysCache(tuple);
    newtuple
}

/// TODO(pg-port): utils/adt/varlena.c CStringGetTextDatum
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    crate::utils::builtins::CStringGetTextDatum(s)
}

/// TODO(pg-port): catalog/pg_depend.c recordDependencyOn
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_char,
) { crate::catalog::pg_depend::recordDependencyOn(_depender as _, _referenced as _, _behavior as _) }

/// TODO(pg-port): catalog/dependency.c recordDependencyOnSingleRelExpr
unsafe fn recordDependencyOnSingleRelExpr(
    _depender: *const ObjectAddress,
    _expr: *mut Node,
    _relId: Oid,
    _behavior: c_char,
    _self_behavior: c_char,
    _reverse_self: bool,
) { crate::catalog::dependency::recordDependencyOnSingleRelExpr(_depender as _, _expr as _, _relId as _, _behavior as _, _self_behavior as _, _reverse_self as _) }

/// TODO(pg-port): catalog/dependency.c performDeletion
unsafe fn performDeletion(_object: *const ObjectAddress, _behavior: DropBehavior, _flags: c_int) {}

/// TODO(pg-port): catalog/objectaccess.c InvokeObjectPostCreateHookArg
unsafe fn InvokeObjectPostCreateHookArg(
    _classId: Oid,
    _objectId: Oid,
    _subId: c_int,
    _is_internal: bool,
) {
}

/*
 * Store a default expression for column attnum of relation rel.
 *
 * Returns the OID of the new pg_attrdef tuple.
 */
pub unsafe fn StoreAttrDefault(
    rel: Relation,
    attnum: AttrNumber,
    expr: *mut Node,
    is_internal: bool,
) -> Oid {
    let adbin: *mut c_char;
    let adrel: Relation;
    let mut tuple: HeapTuple;
    let mut values: [Datum; Natts_pg_attrdef] = core::mem::zeroed();
    let nulls: [bool; Natts_pg_attrdef] = [false, false, false, false];
    let attrrel: Relation;
    let mut atttup: HeapTuple;
    let attStruct: Form_pg_attribute;
    let mut valuesAtt: [Datum; Natts_pg_attribute] = core::mem::zeroed();
    let nullsAtt: [bool; Natts_pg_attribute] = core::mem::zeroed();
    let mut replacesAtt: [bool; Natts_pg_attribute] = core::mem::zeroed();
    let attgenerated: c_char;
    let attrdefOid: Oid;
    let mut colobject: ObjectAddress = core::mem::zeroed();
    let mut defobject: ObjectAddress = core::mem::zeroed();
    adrel = table_open(AttrDefaultRelationId, RowExclusiveLock);

    /*
     * Flatten expression to string form for storage.
     */
    adbin = nodeToString(expr as *const core::ffi::c_void);

    /*
     * Make the pg_attrdef entry.
     */
    attrdefOid = GetNewOidWithIndex(adrel, AttrDefaultOidIndexId, Anum_pg_attrdef_oid);
    values[Anum_pg_attrdef_oid as usize - 1] = ObjectIdGetDatum(attrdefOid);
    values[Anum_pg_attrdef_adrelid as usize - 1] = ObjectIdGetDatum(RelationGetRelid(rel));
    values[Anum_pg_attrdef_adnum as usize - 1] = Int16GetDatum(attnum);
    values[Anum_pg_attrdef_adbin as usize - 1] = CStringGetTextDatum(adbin);
    tuple = heap_form_tuple((*adrel).rd_att, values.as_ptr(), nulls.as_ptr());
    CatalogTupleInsert(adrel, tuple);

    defobject.classId = AttrDefaultRelationId;
    defobject.objectId = attrdefOid;
    defobject.objectSubId = 0;

    table_close(adrel, RowExclusiveLock);

    /* now can free some of the stuff allocated above */
    pfree(DatumGetPointer(values[Anum_pg_attrdef_adbin as usize - 1]) as *mut core::ffi::c_void);
    heap_freetuple(tuple);
    pfree(adbin as *mut core::ffi::c_void);

    /*
     * Update the pg_attribute entry for the column to show that a default
     * exists.
     */
    attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    atttup = SearchSysCacheCopy2(
        ATTNUM,
        ObjectIdGetDatum(RelationGetRelid(rel)),
        Int16GetDatum(attnum),
    );
    if !HeapTupleIsValid(atttup) {
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            RelationGetRelid(rel)
        );
    }
    attStruct = GETSTRUCT(atttup) as Form_pg_attribute;
    attgenerated = (*attStruct).attgenerated;

    valuesAtt[Anum_pg_attribute_atthasdef as usize - 1] = BoolGetDatum(true);
    replacesAtt[Anum_pg_attribute_atthasdef as usize - 1] = true;

    atttup = heap_modify_tuple(
        atttup,
        RelationGetDescr(attrrel),
        valuesAtt.as_ptr(),
        nullsAtt.as_ptr(),
        replacesAtt.as_ptr(),
    );

    CatalogTupleUpdate(attrrel, &mut (*atttup).t_self, atttup);

    table_close(attrrel, RowExclusiveLock);
    heap_freetuple(atttup);

    /*
     * Make a dependency so that the pg_attrdef entry goes away if the column
     * (or whole table) is deleted.  In the case of a generated column, make
     * it an internal dependency to prevent the default expression from being
     * deleted separately.
     */
    colobject.classId = RelationRelationId;
    colobject.objectId = RelationGetRelid(rel);
    colobject.objectSubId = attnum as c_int;

    recordDependencyOn(
        &defobject,
        &colobject,
        if attgenerated != 0 { DEPENDENCY_INTERNAL } else { DEPENDENCY_AUTO },
    );

    /*
     * Record dependencies on objects used in the expression, too.
     */
    recordDependencyOnSingleRelExpr(
        &defobject,
        expr,
        RelationGetRelid(rel),
        DEPENDENCY_NORMAL,
        DEPENDENCY_NORMAL,
        false,
    );

    /*
     * Post creation hook for attribute defaults.
     *
     * XXX. ALTER TABLE ALTER COLUMN SET/DROP DEFAULT is implemented with a
     * couple of deletion/creation of the attribute's default entry, so the
     * callee should check existence of an older version of this entry if it
     * needs to distinguish.
     */
    InvokeObjectPostCreateHookArg(
        AttrDefaultRelationId,
        RelationGetRelid(rel),
        attnum as c_int,
        is_internal,
    );

    attrdefOid
}

/*
 *		RemoveAttrDefault
 *
 * If the specified relation/attribute has a default, remove it.
 * (If no default, raise error if complain is true, else return quietly.)
 */
pub unsafe fn RemoveAttrDefault(
    relid: Oid,
    attnum: AttrNumber,
    behavior: DropBehavior,
    complain: bool,
    internal: bool,
) {
    let attrdef_rel: Relation;
    let mut scankeys: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tuple: HeapTuple;
    let mut found = false;

    attrdef_rel = table_open(AttrDefaultRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut scankeys[0],
        Anum_pg_attrdef_adrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut scankeys[1],
        Anum_pg_attrdef_adnum,
        BTEqualStrategyNumber,
        F_INT2EQ,
        Int16GetDatum(attnum),
    );

    scan = systable_beginscan(
        attrdef_rel,
        AttrDefaultIndexId,
        true,
        null_mut(),
        2,
        scankeys.as_mut_ptr(),
    );

    /* There should be at most one matching tuple, but we loop anyway */
    loop {
        tuple = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let mut object: ObjectAddress = core::mem::zeroed();
        let attrtuple = GETSTRUCT(tuple) as Form_pg_attrdef;

        object.classId = AttrDefaultRelationId;
        object.objectId = (*attrtuple).oid;
        object.objectSubId = 0;

        performDeletion(
            &object,
            behavior,
            if internal { PERFORM_DELETION_INTERNAL } else { 0 },
        );

        found = true;
    }

    systable_endscan(scan);
    table_close(attrdef_rel, RowExclusiveLock);

    if complain && !found {
        elog!(
            ERROR,
            "could not find attrdef tuple for relation {} attnum {}",
            relid,
            attnum
        );
    }
}

/*
 *		RemoveAttrDefaultById
 *
 * Remove a pg_attrdef entry specified by OID.  This is the guts of
 * attribute-default removal.  Note it should be called via performDeletion,
 * not directly.
 */
pub unsafe fn RemoveAttrDefaultById(attrdefId: Oid) {
    let attrdef_rel: Relation;
    let attr_rel: Relation;
    let myrel: Relation;
    let mut scankeys: [ScanKeyData; 1] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tuple: HeapTuple;
    let myrelid: Oid;
    let myattnum: AttrNumber;

    /* Grab an appropriate lock on the pg_attrdef relation */
    attrdef_rel = table_open(AttrDefaultRelationId, RowExclusiveLock);

    /* Find the pg_attrdef tuple */
    ScanKeyInit(
        &mut scankeys[0],
        Anum_pg_attrdef_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(attrdefId),
    );

    scan = systable_beginscan(
        attrdef_rel,
        AttrDefaultOidIndexId,
        true,
        null_mut(),
        1,
        scankeys.as_mut_ptr(),
    );

    tuple = systable_getnext(scan) as HeapTuple;
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "could not find tuple for attrdef {}", attrdefId);
    }

    myrelid = (*(GETSTRUCT(tuple) as Form_pg_attrdef)).adrelid;
    myattnum = (*(GETSTRUCT(tuple) as Form_pg_attrdef)).adnum;

    /* Get an exclusive lock on the relation owning the attribute */
    myrel = relation_open(myrelid, AccessExclusiveLock);

    /* Now we can delete the pg_attrdef row */
    CatalogTupleDelete(attrdef_rel, &mut (*tuple).t_self);

    systable_endscan(scan);
    table_close(attrdef_rel, RowExclusiveLock);

    /* Fix the pg_attribute row */
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy2(
        ATTNUM,
        ObjectIdGetDatum(myrelid),
        Int16GetDatum(myattnum),
    );
    if !HeapTupleIsValid(tuple) {
        /* shouldn't happen */
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            myattnum,
            myrelid
        );
    }

    (*(GETSTRUCT(tuple) as Form_pg_attribute)).atthasdef = false;

    CatalogTupleUpdate(attr_rel, &mut (*tuple).t_self, tuple);

    /*
     * Our update of the pg_attribute row will force a relcache rebuild, so
     * there's nothing else to do here.
     */
    table_close(attr_rel, RowExclusiveLock);

    /* Keep lock on attribute's rel until end of xact */
    relation_close(myrel, NoLock);
}

/*
 * Get the pg_attrdef OID of the default expression for a column
 * identified by relation OID and column number.
 *
 * Returns InvalidOid if there is no such pg_attrdef entry.
 */
pub unsafe fn GetAttrDefaultOid(relid: Oid, attnum: AttrNumber) -> Oid {
    let mut result: Oid = InvalidOid;
    let attrdef: Relation;
    let mut keys: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let tup: HeapTuple;

    attrdef = table_open(AttrDefaultRelationId, AccessShareLock);
    ScanKeyInit(
        &mut keys[0],
        Anum_pg_attrdef_adrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut keys[1],
        Anum_pg_attrdef_adnum,
        BTEqualStrategyNumber,
        F_INT2EQ,
        Int16GetDatum(attnum),
    );
    scan = systable_beginscan(
        attrdef,
        AttrDefaultIndexId,
        true,
        null_mut(),
        2,
        keys.as_mut_ptr(),
    );

    tup = systable_getnext(scan) as HeapTuple;
    if HeapTupleIsValid(tup) {
        let atdform = GETSTRUCT(tup) as Form_pg_attrdef;

        result = (*atdform).oid;
    }

    systable_endscan(scan);
    table_close(attrdef, AccessShareLock);

    result
}

/*
 * Given a pg_attrdef OID, return the relation OID and column number of
 * the owning column (represented as an ObjectAddress for convenience).
 *
 * Returns InvalidObjectAddress if there is no such pg_attrdef entry.
 */
pub unsafe fn GetAttrDefaultColumnAddress(attrdefoid: Oid) -> ObjectAddress {
    let mut result: ObjectAddress = InvalidObjectAddress;
    let attrdef: Relation;
    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
    let scan: SysScanDesc;
    let tup: HeapTuple;

    attrdef = table_open(AttrDefaultRelationId, AccessShareLock);
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_attrdef_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(attrdefoid),
    );
    scan = systable_beginscan(
        attrdef,
        AttrDefaultOidIndexId,
        true,
        null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    tup = systable_getnext(scan) as HeapTuple;
    if HeapTupleIsValid(tup) {
        let atdform = GETSTRUCT(tup) as Form_pg_attrdef;

        result.classId = RelationRelationId;
        result.objectId = (*atdform).adrelid;
        result.objectSubId = (*atdform).adnum as c_int;
    }

    systable_endscan(scan);
    table_close(attrdef, AccessShareLock);

    result
}

/*
 * FormData_pg_attrdef - the fixed part of a pg_attrdef row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_attrdef {
    /* oid */
    pub oid: Oid,
    /* OID of table containing attribute */
    pub adrelid: Oid,
    /* attnum of attribute */
    pub adnum: int16,
}

/*
 * Form_pg_attrdef corresponds to a pointer to a tuple with the format of the
 * pg_attrdef relation.
 */
pub type Form_pg_attrdef = *mut FormData_pg_attrdef;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // adrelid sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_attrdef, adrelid), 4);
        // adnum follows the 4-byte adrelid Oid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_attrdef, adnum),
            4 + core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_attrdef>()
                >= core::mem::offset_of!(FormData_pg_attrdef, adnum)
                    + core::mem::size_of::<int16>()
        );
    }
}
