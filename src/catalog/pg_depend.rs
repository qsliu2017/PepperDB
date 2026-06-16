//! Translation of postgres/src/include/catalog/pg_depend.h and
//! postgres/src/backend/catalog/pg_depend.c
//!
//! FormData_pg_depend - records dependencies between database objects so that
//! DROP can complain or cascade.  No CATALOG_VARLEN section: all columns are
//! fixed-layout.  (The `deptype` codes live in catalog/dependency.h, ported
//! separately when that header lands.)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_depend {
    /// OID of the system catalog the dependent object is in.
    pub classid: Oid,
    /// OID of the dependent object itself.
    pub objid: Oid,
    /// Column number of the dependent object, or 0 if the whole object.
    pub objsubid: int32,
    /// OID of the system catalog the referenced object is in.
    pub refclassid: Oid,
    /// OID of the referenced object itself.
    pub refobjid: Oid,
    /// Column number of the referenced object, or 0 if the whole object.
    pub refobjsubid: int32,
    /// Dependency type code (see catalog/dependency.h).
    pub deptype: c_char,
}

pub type Form_pg_depend = *mut FormData_pg_depend;

/*-------------------------------------------------------------------------
 *
 * pg_depend.c
 *	  routines to support manipulation of the pg_depend relation
 *
 * Source: postgres/src/backend/catalog/pg_depend.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::htup_details::{GETSTRUCT, HeapTuple, HeapTupleIsValid};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::index::genam::{
    SysScanDesc, systable_beginscan, systable_endscan, systable_getnext,
};
use crate::catalog::objectaddress_impl::{
    get_extension_name, getObjectDescription, table_close, table_open,
};
use crate::catalog::catalog::IsPinnedObject;
use crate::catalog::catalog_oids::{
    ConstraintRelationId, DependRelationId, ExtensionRelationId,
    RelationRelationId, TypeRelationId,
};
use crate::catalog::dependency::{
    DEPENDENCY_AUTO, DEPENDENCY_AUTO_EXTENSION, DEPENDENCY_EXTENSION,
    DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, DependencyType,
};
use crate::catalog::indexing::{
    CatalogCloseIndexes, CatalogIndexState, CatalogOpenIndexes,
    CatalogTuplesMultiInsertWithInfo, MAX_CATALOG_MULTI_INSERT_BYTES,
};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::partition::get_partition_ancestors;
use crate::catalog::pg_type::Form_pg_type;
use crate::commands::extension::{CurrentExtensionObject, creating_extension};
use crate::executor::tuptable::TupleTableSlot;
use crate::miscadmin::IsBootstrapProcessingMode;
use crate::nodes::pg_list::{
    List, lappend_oid, linitial_oid, list_free, list_length, llast_oid,
};
use crate::postgres::{CharGetDatum, Int32GetDatum, ObjectIdGetDatum};
use crate::utils::rel::Relation;
use crate::storage::lockdefs::{AccessShareLock, RowExclusiveLock};
use crate::c::NameData;

use core::ffi::CStr;

// pg_depend column numbers (catalog/pg_depend.h)
// TODO(pg-port): replace with generated pg_depend_d.h constants.
const Anum_pg_depend_classid: AttrNumber = 1;
const Anum_pg_depend_objid: AttrNumber = 2;
const Anum_pg_depend_objsubid: AttrNumber = 3;
const Anum_pg_depend_refclassid: AttrNumber = 4;
const Anum_pg_depend_refobjid: AttrNumber = 5;
const Anum_pg_depend_refobjsubid: AttrNumber = 6;

// pg_depend index OIDs (catalog/indexing.h)
// TODO(pg-port): replace with generated indexing constants.
const DependDependerIndexId: Oid = 2673;
const DependReferenceIndexId: Oid = 2674;

// fmgr OIDs used in ScanKeyInit (utils/fmgroids.h)
// TODO(pg-port): replace with generated fmgroids.h constants.
const F_OIDEQ: RegProcedure = 184;
const F_INT4EQ: RegProcedure = 65;

// pg_class.relkind code (catalog/pg_class.h)
// TODO(pg-port): replace with generated pg_class_d.h constant.
const RELKIND_SEQUENCE: c_char = b'S' as c_char;

// syscache id (utils/syscache.h)
// TODO(pg-port): replace with generated syscache enum.
const TYPEOID: c_int = 0;

/// Test if an object is required for basic database functionality.
///
/// The passed subId, if any, is ignored; we assume that only whole objects
/// are pinned (and that this implies pinning their components).
unsafe fn isObjectPinned(object: *const ObjectAddress) -> bool {
    IsPinnedObject((*object).classId, (*object).objectId)
}

/*
 * Record a dependency between 2 objects via their respective ObjectAddress.
 * The first argument is the dependent object, the second the one it
 * references.
 *
 * This simply creates an entry in pg_depend, without any other processing.
 */
pub unsafe fn recordDependencyOn(
    depender: *const ObjectAddress,
    referenced: *const ObjectAddress,
    behavior: DependencyType,
) {
    recordMultipleDependencies(depender, referenced, 1, behavior);
}

/*
 * Record multiple dependencies (of the same kind) for a single dependent
 * object.  This has a little less overhead than recording each separately.
 */
pub unsafe fn recordMultipleDependencies(
    depender: *const ObjectAddress,
    mut referenced: *const ObjectAddress,
    nreferenced: c_int,
    behavior: DependencyType,
) {
    let dependDesc: Relation;
    let mut indstate: CatalogIndexState;
    let slot: *mut *mut TupleTableSlot;
    let mut i: c_int;
    let max_slots: c_int;
    let mut slot_init_count: c_int;
    let mut slot_stored_count: c_int;

    if nreferenced <= 0 {
        return; /* nothing to do */
    }

    /*
     * During bootstrap, do nothing since pg_depend may not exist yet.
     *
     * Objects created during bootstrap are most likely pinned, and the few
     * that are not do not have dependencies on each other, so that there
     * would be no need to make a pg_depend entry anyway.
     */
    if IsBootstrapProcessingMode() {
        return;
    }

    dependDesc = table_open(DependRelationId, RowExclusiveLock);

    /*
     * Allocate the slots to use, but delay costly initialization until we
     * know that they will be used.
     */
    max_slots = Min(
        nreferenced,
        (MAX_CATALOG_MULTI_INSERT_BYTES as usize
            / core::mem::size_of::<FormData_pg_depend>()) as c_int,
    );
    slot = palloc(core::mem::size_of::<*mut TupleTableSlot>() * max_slots as usize)
        as *mut *mut TupleTableSlot;

    /* Don't open indexes unless we need to make an update */
    indstate = null_mut();

    /* number of slots currently storing tuples */
    slot_stored_count = 0;
    /* number of slots currently initialized */
    slot_init_count = 0;
    i = 0;
    while i < nreferenced {
        /*
         * If the referenced object is pinned by the system, there's no real
         * need to record dependencies on it.  This saves lots of space in
         * pg_depend, so it's worth the time taken to check.
         */
        if !isObjectPinned(referenced) {
            if slot_init_count < max_slots {
                *slot.add(slot_stored_count as usize) = MakeSingleTupleTableSlot(
                    RelationGetDescr(dependDesc),
                    &TTSOpsHeapTuple,
                );
                slot_init_count += 1;
            }

            let cur = *slot.add(slot_stored_count as usize);
            ExecClearTuple(cur);

            /*
             * Record the dependency.  Note we don't bother to check for duplicate
             * dependencies; there's no harm in them.
             */
            *(*cur).tts_values.add((Anum_pg_depend_refclassid - 1) as usize) =
                ObjectIdGetDatum((*referenced).classId);
            *(*cur).tts_values.add((Anum_pg_depend_refobjid - 1) as usize) =
                ObjectIdGetDatum((*referenced).objectId);
            *(*cur).tts_values.add((Anum_pg_depend_refobjsubid - 1) as usize) =
                Int32GetDatum((*referenced).objectSubId);
            *(*cur).tts_values.add((Anum_pg_depend_deptype - 1) as usize) =
                CharGetDatum(behavior as c_char);
            *(*cur).tts_values.add((Anum_pg_depend_classid - 1) as usize) =
                ObjectIdGetDatum((*depender).classId);
            *(*cur).tts_values.add((Anum_pg_depend_objid - 1) as usize) =
                ObjectIdGetDatum((*depender).objectId);
            *(*cur).tts_values.add((Anum_pg_depend_objsubid - 1) as usize) =
                Int32GetDatum((*depender).objectSubId);

            core::ptr::write_bytes(
                (*cur).tts_isnull,
                0,
                (*(*cur).tts_tupleDescriptor).natts as usize,
            );

            ExecStoreVirtualTuple(cur);
            slot_stored_count += 1;

            /* If slots are full, insert a batch of tuples */
            if slot_stored_count == max_slots {
                /* fetch index info only when we know we need it */
                if indstate.is_null() {
                    indstate = CatalogOpenIndexes(dependDesc);
                }

                CatalogTuplesMultiInsertWithInfo(
                    dependDesc,
                    slot,
                    slot_stored_count,
                    indstate,
                );
                slot_stored_count = 0;
            }
        }

        i += 1;
        referenced = referenced.add(1);
    }

    /* Insert any tuples left in the buffer */
    if slot_stored_count > 0 {
        /* fetch index info only when we know we need it */
        if indstate.is_null() {
            indstate = CatalogOpenIndexes(dependDesc);
        }

        CatalogTuplesMultiInsertWithInfo(dependDesc, slot, slot_stored_count, indstate);
    }

    if !indstate.is_null() {
        CatalogCloseIndexes(indstate);
    }

    table_close(dependDesc, RowExclusiveLock);

    /* Drop only the number of slots used */
    i = 0;
    while i < slot_init_count {
        ExecDropSingleTupleTableSlot(*slot.add(i as usize));
        i += 1;
    }
    pfree(slot as *mut c_void);
}

/*
 * If we are executing a CREATE EXTENSION operation, mark the given object
 * as being a member of the extension, or check that it already is one.
 * Otherwise, do nothing.
 *
 * This must be called during creation of any user-definable object type
 * that could be a member of an extension.
 *
 * isReplace must be true if the object already existed, and false if it is
 * newly created.  In the former case we insist that it already be a member
 * of the current extension.  In the latter case we can skip checking whether
 * it is already a member of any extension.
 */
pub unsafe fn recordDependencyOnCurrentExtension(
    object: *const ObjectAddress,
    isReplace: bool,
) {
    /* Only whole objects can be extension members */
    Assert!((*object).objectSubId == 0);

    if creating_extension {
        let mut extension: ObjectAddress = core::mem::zeroed();

        /* Only need to check for existing membership if isReplace */
        if isReplace {
            let oldext: Oid;

            /*
             * Side note: these catalog lookups are safe only because the
             * object is a pre-existing one.  In the not-isReplace case, the
             * caller has most likely not yet done a CommandCounterIncrement
             * that would make the new object visible.
             */
            oldext = getExtensionOfObject((*object).classId, (*object).objectId);
            if OidIsValid(oldext) {
                /* If already a member of this extension, nothing to do */
                if oldext == CurrentExtensionObject {
                    return;
                }
                /* Already a member of some other extension, so reject */
                ereport!(
                    ERROR,
                    errmsg!(
                        "{} is already a member of extension \"{}\"",
                        CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy(),
                        CStr::from_ptr(get_extension_name(oldext)).to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            }
            /* It's a free-standing object, so reject */
            ereport!(
                ERROR,
                errmsg!(
                    "{} is not a member of extension \"{}\"",
                    CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy(),
                    CStr::from_ptr(get_extension_name(CurrentExtensionObject)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            /* C also: errdetail("An extension is not allowed to replace an object that it does not own.") */
        }

        /* OK, record it as a member of CurrentExtensionObject */
        extension.classId = ExtensionRelationId;
        extension.objectId = CurrentExtensionObject;
        extension.objectSubId = 0;

        recordDependencyOn(object, &extension, DEPENDENCY_EXTENSION);
    }
}

/*
 * If we are executing a CREATE EXTENSION operation, check that the given
 * object is a member of the extension, and throw an error if it isn't.
 * Otherwise, do nothing.
 */
pub unsafe fn checkMembershipInCurrentExtension(object: *const ObjectAddress) {
    /*
     * This is actually the same condition tested in
     * recordDependencyOnCurrentExtension; but we want to issue a
     * differently-worded error, and anyway it would be pretty confusing to
     * call recordDependencyOnCurrentExtension in these circumstances.
     */

    /* Only whole objects can be extension members */
    Assert!((*object).objectSubId == 0);

    if creating_extension {
        let oldext: Oid;

        oldext = getExtensionOfObject((*object).classId, (*object).objectId);
        /* If already a member of this extension, OK */
        if oldext == CurrentExtensionObject {
            return;
        }
        /* Else complain */
        ereport!(
            ERROR,
            errmsg!(
                "{} is not a member of extension \"{}\"",
                CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy(),
                CStr::from_ptr(get_extension_name(CurrentExtensionObject)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        /* C also: errdetail("An extension may only use CREATE ... IF NOT EXISTS to skip object creation if the conflicting object is one that it already owns.") */
    }
}

/*
 * deleteDependencyRecordsFor -- delete all records with given depender
 * classId/objectId.  Returns the number of records deleted.
 */
pub unsafe fn deleteDependencyRecordsFor(
    classId: Oid,
    objectId: Oid,
    skipExtensionDeps: bool,
) -> c_long {
    let mut count: c_long = 0;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        if skipExtensionDeps
            && (*(GETSTRUCT(tup) as Form_pg_depend)).deptype == DEPENDENCY_EXTENSION
        {
            continue;
        }

        CatalogTupleDelete(depRel, &mut (*tup).t_self);
        count += 1;
    }

    systable_endscan(scan);

    table_close(depRel, RowExclusiveLock);

    count
}

/*
 * deleteDependencyRecordsForClass -- delete all records with given depender
 * classId/objectId, dependee classId, and deptype.
 * Returns the number of records deleted.
 */
pub unsafe fn deleteDependencyRecordsForClass(
    classId: Oid,
    objectId: Oid,
    refclassId: Oid,
    deptype: c_char,
) -> c_long {
    let mut count: c_long = 0;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).refclassid == refclassId && (*depform).deptype == deptype {
            CatalogTupleDelete(depRel, &mut (*tup).t_self);
            count += 1;
        }
    }

    systable_endscan(scan);

    table_close(depRel, RowExclusiveLock);

    count
}

/*
 * deleteDependencyRecordsForSpecific -- delete all records with given depender
 * classId/objectId, dependee classId/objectId, of the given deptype.
 * Returns the number of records deleted.
 */
pub unsafe fn deleteDependencyRecordsForSpecific(
    classId: Oid,
    objectId: Oid,
    deptype: c_char,
    refclassId: Oid,
    refobjectId: Oid,
) -> c_long {
    let mut count: c_long = 0;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).refclassid == refclassId
            && (*depform).refobjid == refobjectId
            && (*depform).deptype == deptype
        {
            CatalogTupleDelete(depRel, &mut (*tup).t_self);
            count += 1;
        }
    }

    systable_endscan(scan);

    table_close(depRel, RowExclusiveLock);

    count
}

/*
 * Adjust dependency record(s) to point to a different object of the same type
 *
 * classId/objectId specify the referencing object.
 * refClassId/oldRefObjectId specify the old referenced object.
 * newRefObjectId is the new referenced object (must be of class refClassId).
 *
 * Returns the number of records updated -- zero indicates a problem.
 */
pub unsafe fn changeDependencyFor(
    classId: Oid,
    objectId: Oid,
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> c_long {
    let mut count: c_long = 0;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;
    let mut objAddr: ObjectAddress = core::mem::zeroed();
    let mut depAddr: ObjectAddress = core::mem::zeroed();
    let oldIsPinned: bool;
    let newIsPinned: bool;

    /*
     * Check to see if either oldRefObjectId or newRefObjectId is pinned.
     * Pinned objects should not have any dependency entries pointing to them,
     * so in these cases we should add or remove a pg_depend entry, or do
     * nothing at all, rather than update an entry as in the normal case.
     */
    objAddr.classId = refClassId;
    objAddr.objectId = oldRefObjectId;
    objAddr.objectSubId = 0;

    oldIsPinned = isObjectPinned(&objAddr);

    objAddr.objectId = newRefObjectId;

    newIsPinned = isObjectPinned(&objAddr);

    if oldIsPinned {
        /*
         * If both are pinned, we need do nothing.  However, return 1 not 0,
         * else callers will think this is an error case.
         */
        if newIsPinned {
            return 1;
        }

        /*
         * There is no old dependency record, but we should insert a new one.
         * Assume a normal dependency is wanted.
         */
        depAddr.classId = classId;
        depAddr.objectId = objectId;
        depAddr.objectSubId = 0;
        recordDependencyOn(&depAddr, &objAddr, DEPENDENCY_NORMAL);

        return 1;
    }

    depRel = table_open(DependRelationId, RowExclusiveLock);

    /* There should be existing dependency record(s), so search. */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let mut depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).refclassid == refClassId && (*depform).refobjid == oldRefObjectId {
            if newIsPinned {
                CatalogTupleDelete(depRel, &mut (*tup).t_self);
            } else {
                /* make a modifiable copy */
                tup = heap_copytuple(tup);
                depform = GETSTRUCT(tup) as Form_pg_depend;

                (*depform).refobjid = newRefObjectId;

                CatalogTupleUpdate(depRel, &mut (*tup).t_self, tup);

                heap_freetuple(tup);
            }

            count += 1;
        }
    }

    systable_endscan(scan);

    table_close(depRel, RowExclusiveLock);

    count
}

/*
 * Adjust all dependency records to come from a different object of the same type
 *
 * classId/oldObjectId specify the old referencing object.
 * newObjectId is the new referencing object (must be of class classId).
 *
 * Returns the number of records updated.
 */
pub unsafe fn changeDependenciesOf(
    classId: Oid,
    oldObjectId: Oid,
    newObjectId: Oid,
) -> c_long {
    let mut count: c_long = 0;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oldObjectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend;

        /* make a modifiable copy */
        tup = heap_copytuple(tup);
        depform = GETSTRUCT(tup) as Form_pg_depend;

        (*depform).objid = newObjectId;

        CatalogTupleUpdate(depRel, &mut (*tup).t_self, tup);

        heap_freetuple(tup);

        count += 1;
    }

    systable_endscan(scan);

    table_close(depRel, RowExclusiveLock);

    count
}

/*
 * Adjust all dependency records to point to a different object of the same type
 *
 * refClassId/oldRefObjectId specify the old referenced object.
 * newRefObjectId is the new referenced object (must be of class refClassId).
 *
 * Returns the number of records updated.
 */
pub unsafe fn changeDependenciesOn(
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> c_long {
    let mut count: c_long = 0;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;
    let mut objAddr: ObjectAddress = core::mem::zeroed();
    let newIsPinned: bool;

    depRel = table_open(DependRelationId, RowExclusiveLock);

    /*
     * If oldRefObjectId is pinned, there won't be any dependency entries on
     * it --- we can't cope in that case.  (This isn't really worth expending
     * code to fix, in current usage; it just means you can't rename stuff out
     * of pg_catalog, which would likely be a bad move anyway.)
     */
    objAddr.classId = refClassId;
    objAddr.objectId = oldRefObjectId;
    objAddr.objectSubId = 0;

    if isObjectPinned(&objAddr) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot remove dependency on {} because it is a system object",
                CStr::from_ptr(getObjectDescription(&objAddr, false)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /*
     * We can handle adding a dependency on something pinned, though, since
     * that just means deleting the dependency entry.
     */
    objAddr.objectId = newRefObjectId;

    newIsPinned = isObjectPinned(&objAddr);

    /* Now search for dependency records */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(refClassId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oldRefObjectId),
    );

    scan = systable_beginscan(
        depRel,
        DependReferenceIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        if newIsPinned {
            CatalogTupleDelete(depRel, &mut (*tup).t_self);
        } else {
            let depform: Form_pg_depend;

            /* make a modifiable copy */
            tup = heap_copytuple(tup);
            depform = GETSTRUCT(tup) as Form_pg_depend;

            (*depform).refobjid = newRefObjectId;

            CatalogTupleUpdate(depRel, &mut (*tup).t_self, tup);

            heap_freetuple(tup);
        }

        count += 1;
    }

    systable_endscan(scan);

    table_close(depRel, RowExclusiveLock);

    count
}

/*
 * Find the extension containing the specified object, if any
 *
 * Returns the OID of the extension, or InvalidOid if the object does not
 * belong to any extension.
 */
pub unsafe fn getExtensionOfObject(classId: Oid, objectId: Oid) -> Oid {
    let mut result: Oid = InvalidOid;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).refclassid == ExtensionRelationId
            && (*depform).deptype == DEPENDENCY_EXTENSION
        {
            result = (*depform).refobjid;
            break; /* no need to keep scanning */
        }
    }

    systable_endscan(scan);

    table_close(depRel, AccessShareLock);

    result
}

/*
 * Return (possibly NIL) list of extensions that the given object depends on
 * in DEPENDENCY_AUTO_EXTENSION mode.
 */
pub unsafe fn getAutoExtensionsOfObject(classId: Oid, objectId: Oid) -> *mut List {
    let mut result: *mut List = null_mut();
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).refclassid == ExtensionRelationId
            && (*depform).deptype == DEPENDENCY_AUTO_EXTENSION
        {
            result = lappend_oid(result, (*depform).refobjid);
        }
    }

    systable_endscan(scan);

    table_close(depRel, AccessShareLock);

    result
}

/*
 * Look up a type belonging to an extension.
 *
 * Returns the type's OID, or InvalidOid if not found.
 */
pub unsafe fn getExtensionType(extensionOid: Oid, typname: *const c_char) -> Oid {
    let mut result: Oid = InvalidOid;
    let depRel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(ExtensionRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(extensionOid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_refobjsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(0),
    );

    scan = systable_beginscan(
        depRel,
        DependReferenceIndexId,
        true,
        null_mut(),
        3,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).classid == TypeRelationId && (*depform).deptype == DEPENDENCY_EXTENSION {
            let typoid: Oid = (*depform).objid;
            let typtup: HeapTuple;

            typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
            if !HeapTupleIsValid(typtup) {
                continue; /* should we throw an error? */
            }
            if libc_strcmp(
                NameStr(&(*(GETSTRUCT(typtup) as Form_pg_type)).typname),
                typname,
            ) == 0
            {
                result = typoid;
                ReleaseSysCache(typtup);
                break; /* no need to keep searching */
            }
            ReleaseSysCache(typtup);
        }
    }

    systable_endscan(scan);

    table_close(depRel, AccessShareLock);

    result
}

/*
 * Detect whether a sequence is marked as "owned" by a column
 *
 * An ownership marker is an AUTO or INTERNAL dependency from the sequence to the
 * column.  If we find one, store the identity of the owning column
 * into *tableId and *colId and return true; else return false.
 */
pub unsafe fn sequenceIsOwned(
    seqId: Oid,
    deptype: c_char,
    tableId: *mut Oid,
    colId: *mut int32,
) -> bool {
    let mut ret: bool = false;
    let depRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, AccessShareLock);

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
        ObjectIdGetDatum(seqId),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let depform: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        if (*depform).refclassid == RelationRelationId && (*depform).deptype == deptype {
            *tableId = (*depform).refobjid;
            *colId = (*depform).refobjsubid;
            ret = true;
            break; /* no need to keep scanning */
        }
    }

    systable_endscan(scan);

    table_close(depRel, AccessShareLock);

    ret
}

/*
 * Collect a list of OIDs of all sequences owned by the specified relation,
 * and column if specified.  If deptype is not zero, then only find sequences
 * with the specified dependency type.
 */
unsafe fn getOwnedSequences_internal(
    relid: Oid,
    attnum: AttrNumber,
    deptype: c_char,
) -> *mut List {
    let mut result: *mut List = null_mut();
    let depRel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    depRel = table_open(DependRelationId, AccessShareLock);

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
        ObjectIdGetDatum(relid),
    );
    if attnum != 0 {
        ScanKeyInit(
            &mut key[2],
            Anum_pg_depend_refobjsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum(attnum as int32),
        );
    }

    scan = systable_beginscan(
        depRel,
        DependReferenceIndexId,
        true,
        null_mut(),
        if attnum != 0 { 3 } else { 2 },
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let deprec: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        /*
         * We assume any auto or internal dependency of a sequence on a column
         * must be what we are looking for.  (We need the relkind test because
         * indexes can also have auto dependencies on columns.)
         */
        if (*deprec).classid == RelationRelationId
            && (*deprec).objsubid == 0
            && (*deprec).refobjsubid != 0
            && ((*deprec).deptype == DEPENDENCY_AUTO || (*deprec).deptype == DEPENDENCY_INTERNAL)
            && get_rel_relkind((*deprec).objid) == RELKIND_SEQUENCE
        {
            if deptype == 0 || (*deprec).deptype == deptype {
                result = lappend_oid(result, (*deprec).objid);
            }
        }
    }

    systable_endscan(scan);

    table_close(depRel, AccessShareLock);

    result
}

/*
 * Collect a list of OIDs of all sequences owned (identity or serial) by the
 * specified relation.
 */
pub unsafe fn getOwnedSequences(relid: Oid) -> *mut List {
    getOwnedSequences_internal(relid, 0, 0)
}

/*
 * Get owned identity sequence, error if not exactly one.
 */
pub unsafe fn getIdentitySequence(
    rel: Relation,
    mut attnum: AttrNumber,
    missing_ok: bool,
) -> Oid {
    let mut relid: Oid = RelationGetRelid(rel);
    let seqlist: *mut List;

    /*
     * The identity sequence is associated with the topmost partitioned table,
     * which might have column order different than the given partition.
     */
    if (*RelationGetForm(rel)).relispartition {
        let ancestors: *mut List = get_partition_ancestors(relid);
        let attname: *const c_char = get_attname(relid, attnum, false);

        relid = llast_oid(ancestors);
        attnum = get_attnum(relid, attname);
        if attnum == InvalidAttrNumber {
            elog!(
                ERROR,
                "cache lookup failed for attribute \"{}\" of relation {}",
                CStr::from_ptr(attname).to_string_lossy(),
                relid
            );
        }
        list_free(ancestors);
    }

    seqlist = getOwnedSequences_internal(relid, attnum, DEPENDENCY_INTERNAL);
    if list_length(seqlist) > 1 {
        elog!(ERROR, "more than one owned sequence found");
    } else if seqlist.is_null() {
        if missing_ok {
            return InvalidOid;
        } else {
            elog!(ERROR, "no owned sequence found");
        }
    }

    linitial_oid(seqlist)
}

/*
 * get_index_constraint
 *		Given the OID of an index, return the OID of the owning unique,
 *		primary-key, or exclusion constraint, or InvalidOid if there
 *		is no owning constraint.
 */
pub unsafe fn get_index_constraint(indexId: Oid) -> Oid {
    let mut constraintId: Oid = InvalidOid;
    let depRel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* Search the dependency table for the index */
    depRel = table_open(DependRelationId, AccessShareLock);

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
        ObjectIdGetDatum(indexId),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(0),
    );

    scan = systable_beginscan(
        depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        3,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let deprec: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        /*
         * We assume any internal dependency on a constraint must be what we
         * are looking for.
         */
        if (*deprec).refclassid == ConstraintRelationId
            && (*deprec).refobjsubid == 0
            && (*deprec).deptype == DEPENDENCY_INTERNAL
        {
            constraintId = (*deprec).refobjid;
            break;
        }
    }

    systable_endscan(scan);
    table_close(depRel, AccessShareLock);

    constraintId
}

/*
 * get_index_ref_constraints
 *		Given the OID of an index, return the OID of all foreign key
 *		constraints which reference the index.
 */
pub unsafe fn get_index_ref_constraints(indexId: Oid) -> *mut List {
    let mut result: *mut List = null_mut();
    let depRel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* Search the dependency table for the index */
    depRel = table_open(DependRelationId, AccessShareLock);

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
        ObjectIdGetDatum(indexId),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_refobjsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(0),
    );

    scan = systable_beginscan(
        depRel,
        DependReferenceIndexId,
        true,
        null_mut(),
        3,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let deprec: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        /*
         * We assume any normal dependency from a constraint must be what we
         * are looking for.
         */
        if (*deprec).classid == ConstraintRelationId
            && (*deprec).objsubid == 0
            && (*deprec).deptype == DEPENDENCY_NORMAL
        {
            result = lappend_oid(result, (*deprec).objid);
        }
    }

    systable_endscan(scan);
    table_close(depRel, AccessShareLock);

    result
}

// Anum_pg_depend_deptype (catalog/pg_depend.h)
const Anum_pg_depend_deptype: AttrNumber = 7;

// NameStr: extract the C string from a NameData field (c.h)
unsafe fn NameStr(name: *const NameData) -> *const c_char {
    (*name).data.as_ptr()
}

// libc strcmp wrapper used by getExtensionType.
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i = 0isize;
    loop {
        let ca = *a.offset(i);
        let cb = *b.offset(i);
        if ca != cb {
            return (ca as u8 as c_int) - (cb as u8 as c_int);
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

// TODO(pg-port): executor/tuptable.h slot ops vtable; not yet ported.
extern "C" {
    static TTSOpsHeapTuple: c_void;
}

// TODO(pg-port): executor/execTuples.c MakeSingleTupleTableSlot
unsafe fn MakeSingleTupleTableSlot(
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
    _tts_ops: *const c_void,
) -> *mut TupleTableSlot {
    unimplemented!()
}
// TODO(pg-port): executor/execTuples.c ExecDropSingleTupleTableSlot
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {}
// TODO(pg-port): executor/execTuples.c ExecClearTuple
unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    null_mut()
}
// TODO(pg-port): executor/execTuples.c ExecStoreVirtualTuple
unsafe fn ExecStoreVirtualTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    null_mut()
}
// TODO(pg-port): utils/rel.h RelationGetDescr
unsafe fn RelationGetDescr(_relation: Relation) -> crate::access::common::tupdesc::TupleDesc {
    unimplemented!()
}
// TODO(pg-port): utils/rel.h RelationGetRelid
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    unimplemented!()
}
// TODO(pg-port): utils/rel.h RelationGetForm
unsafe fn RelationGetForm(
    _relation: Relation,
) -> *mut crate::catalog::pg_class::FormData_pg_class {
    unimplemented!()
}
// TODO(pg-port): access/htup.h heap_copytuple
unsafe fn heap_copytuple(_tuple: HeapTuple) -> HeapTuple {
    unimplemented!()
}
// TODO(pg-port): access/htup.h heap_freetuple
unsafe fn heap_freetuple(_htup: HeapTuple) {}
// TODO(pg-port): catalog/indexing.c CatalogTupleDelete
unsafe fn CatalogTupleDelete(
    _heapRel: Relation,
    _tid: *mut crate::storage::itemptr::ItemPointerData,
) {
}
// TODO(pg-port): catalog/indexing.c CatalogTupleUpdate
unsafe fn CatalogTupleUpdate(
    _heapRel: Relation,
    _otid: *mut crate::storage::itemptr::ItemPointerData,
    _tup: HeapTuple,
) {
}
// TODO(pg-port): utils/cache/lsyscache.c get_rel_relkind
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    unimplemented!()
}
// TODO(pg-port): utils/cache/lsyscache.c get_attname
unsafe fn get_attname(_relid: Oid, _attnum: AttrNumber, _missing_ok: bool) -> *const c_char {
    unimplemented!()
}
// TODO(pg-port): utils/cache/lsyscache.c get_attnum
unsafe fn get_attnum(_relid: Oid, _attname: *const c_char) -> AttrNumber {
    unimplemented!()
}
// TODO(pg-port): utils/cache/syscache.c SearchSysCache1
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}
// TODO(pg-port): utils/cache/syscache.c ReleaseSysCache
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}
// access/attnum.h
const InvalidAttrNumber: AttrNumber = 0;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(core::mem::offset_of!(FormData_pg_depend, objid), 4);
        assert_eq!(core::mem::offset_of!(FormData_pg_depend, refclassid), 12);
        assert!(
            core::mem::size_of::<FormData_pg_depend>()
                >= core::mem::offset_of!(FormData_pg_depend, deptype) + 1
        );
    }
}
