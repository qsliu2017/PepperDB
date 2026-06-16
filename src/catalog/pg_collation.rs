//! Translation of postgres/src/include/catalog/pg_collation.h
//!
//! The `FormData_pg_collation` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_collation catalog row.  This is exactly the portion of the row
//! that the C struct exposes in memory; the variable-length / nullable trailing
//! fields (collcollate, collctype, colllocale, collicurules, collversion, all
//! `text` and guarded by CATALOG_VARLEN in the C header) are NOT part of this
//! struct - they live only in a real on-disk pg_collation tuple and are reached
//! via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int32, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_collation - the fixed part of a pg_collation row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_collation {
    /* oid */
    pub oid: Oid,
    /* collation name */
    pub collname: NameData,
    /* OID of namespace containing this collation */
    pub collnamespace: Oid,
    /* owner of collation */
    pub collowner: Oid,
    /* see COLLPROVIDER_* constants below */
    pub collprovider: c_char,
    /* if true, collation is deterministic */
    pub collisdeterministic: bool,
    /* encoding for this collation; -1 = "all" */
    pub collencoding: int32,
}

/*
 * Form_pg_collation corresponds to a pointer to a row with the format of the
 * pg_collation relation.
 */
pub type Form_pg_collation = *mut FormData_pg_collation;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * COLLPROVIDER_* - values of the collprovider column.
 * ----------------------------------------------------------------
 */
pub const COLLPROVIDER_DEFAULT: c_char = b'd' as c_char;
pub const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;
pub const COLLPROVIDER_ICU: c_char = b'i' as c_char;
pub const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

/*-------------------------------------------------------------------------
 *
 * pg_collation.c
 *	  routines to support manipulation of the pg_collation relation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_collation.c
 *
 *-------------------------------------------------------------------------
 */

use crate::c::int32 as int32_t;
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Int32GetDatum, NameGetDatum, ObjectIdGetDatum, PointerGetDatum,
};
use core::ffi::{c_int, c_void};
use core::ptr;

type Datum = usize;
type AttrNumber = i16;
type LOCKMODE = c_int;
type Relation = *mut c_void;
type TupleDesc = *mut c_void;
type HeapTuple = *mut c_void;

const InvalidOid: Oid = 0;

/* error levels (utils/elog.h) */
const NOTICE: c_int = 18;
const ERROR: c_int = 21;

/* lock modes (storage/lockdefs.h) */
const NoLock: LOCKMODE = 0;
const ShareRowExclusiveLock: LOCKMODE = 6;

/* relation / index OIDs (catalog/pg_collation.h, catalog/pg_namespace.h) */
const CollationRelationId: Oid = 3456;
const CollationOidIndexId: Oid = 3085;
const NamespaceRelationId: Oid = 2615;

/* syscache id (utils/syscache.h) */
const COLLNAMEENCNSP: c_int = 206;

/* attribute numbers / count (catalog/pg_collation_d.h) */
const Natts_pg_collation: usize = 12;
const Anum_pg_collation_oid: AttrNumber = 1;
const Anum_pg_collation_collname: AttrNumber = 2;
const Anum_pg_collation_collnamespace: AttrNumber = 3;
const Anum_pg_collation_collowner: AttrNumber = 4;
const Anum_pg_collation_collprovider: AttrNumber = 5;
const Anum_pg_collation_collisdeterministic: AttrNumber = 6;
const Anum_pg_collation_collencoding: AttrNumber = 7;
const Anum_pg_collation_collcollate: AttrNumber = 8;
const Anum_pg_collation_collctype: AttrNumber = 9;
const Anum_pg_collation_colllocale: AttrNumber = 10;
const Anum_pg_collation_collicurules: AttrNumber = 11;
const Anum_pg_collation_collversion: AttrNumber = 12;

/* dependency type code (catalog/dependency.h) */
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;

#[repr(C)]
#[derive(Clone, Copy)]
struct ObjectAddress {
    classId: Oid,
    objectId: Oid,
    objectSubId: int32_t,
}

#[inline]
fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

/*
 * CollationCreate
 *
 * Add a new tuple to pg_collation.
 *
 * if_not_exists: if true, don't fail on duplicate name, just print a notice
 * and return InvalidOid.
 * quiet: if true, don't fail on duplicate name, just silently return
 * InvalidOid (overrides if_not_exists).
 */
pub unsafe fn CollationCreate(
    collname: *const c_char,
    collnamespace: Oid,
    collowner: Oid,
    collprovider: c_char,
    collisdeterministic: bool,
    collencoding: int32_t,
    collcollate: *const c_char,
    collctype: *const c_char,
    colllocale: *const c_char,
    collicurules: *const c_char,
    collversion: *const c_char,
    if_not_exists: bool,
    quiet: bool,
) -> Oid {
    let rel: Relation;
    let tupDesc: TupleDesc;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_collation] = [0; Natts_pg_collation];
    let mut nulls: [bool; Natts_pg_collation] = [false; Natts_pg_collation];
    let mut name_name: NameData = core::mem::zeroed();
    let mut oid: Oid;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    assert!(!collname.is_null());
    assert!(collnamespace != InvalidOid);
    assert!(collowner != InvalidOid);
    assert!(
        (collprovider == COLLPROVIDER_LIBC
            && !collcollate.is_null()
            && !collctype.is_null()
            && colllocale.is_null())
            || (collprovider != COLLPROVIDER_LIBC
                && collcollate.is_null()
                && collctype.is_null()
                && !colllocale.is_null())
    );

    /*
     * Make sure there is no existing collation of same name & encoding.
     *
     * This would be caught by the unique index anyway; we're just giving a
     * friendlier error message.  The unique index provides a backstop against
     * race conditions.
     */
    oid = GetSysCacheOid3(
        COLLNAMEENCNSP,
        Anum_pg_collation_oid,
        PointerGetDatum(collname as *const c_void),
        Int32GetDatum(collencoding),
        ObjectIdGetDatum(collnamespace),
    );
    if OidIsValid(oid) {
        if quiet {
            return InvalidOid;
        } else if if_not_exists {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            ObjectAddressSet(&mut myself, CollationRelationId, oid);
            checkMembershipInCurrentExtension(&myself);

            /* OK to skip */
            if collencoding == -1 {
                ereport!(
                    NOTICE,
                    errmsg!(
                        "collation \"{}\" already exists, skipping",
                        std::ffi::CStr::from_ptr(collname).to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            } else {
                ereport!(
                    NOTICE,
                    errmsg!(
                        "collation \"{}\" for encoding \"{}\" already exists, skipping",
                        std::ffi::CStr::from_ptr(collname).to_string_lossy(),
                        std::ffi::CStr::from_ptr(pg_encoding_to_char(collencoding))
                            .to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            }
            return InvalidOid;
        } else {
            if collencoding == -1 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "collation \"{}\" already exists",
                        std::ffi::CStr::from_ptr(collname).to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "collation \"{}\" for encoding \"{}\" already exists",
                        std::ffi::CStr::from_ptr(collname).to_string_lossy(),
                        std::ffi::CStr::from_ptr(pg_encoding_to_char(collencoding))
                            .to_string_lossy()
                    )
                );
                /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            }
        }
    }

    /* open pg_collation; see below about the lock level */
    rel = table_open(CollationRelationId, ShareRowExclusiveLock);

    /*
     * Also forbid a specific-encoding collation shadowing an any-encoding
     * collation, or an any-encoding collation being shadowed (see
     * get_collation_name()).  This test is not backed up by the unique index,
     * so we take a ShareRowExclusiveLock earlier, to protect against
     * concurrent changes fooling this check.
     */
    if collencoding == -1 {
        oid = GetSysCacheOid3(
            COLLNAMEENCNSP,
            Anum_pg_collation_oid,
            PointerGetDatum(collname as *const c_void),
            Int32GetDatum(GetDatabaseEncoding()),
            ObjectIdGetDatum(collnamespace),
        );
    } else {
        oid = GetSysCacheOid3(
            COLLNAMEENCNSP,
            Anum_pg_collation_oid,
            PointerGetDatum(collname as *const c_void),
            Int32GetDatum(-1),
            ObjectIdGetDatum(collnamespace),
        );
    }
    if OidIsValid(oid) {
        if quiet {
            table_close(rel, NoLock);
            return InvalidOid;
        } else if if_not_exists {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            ObjectAddressSet(&mut myself, CollationRelationId, oid);
            checkMembershipInCurrentExtension(&myself);

            /* OK to skip */
            table_close(rel, NoLock);
            ereport!(
                NOTICE,
                errmsg!(
                    "collation \"{}\" already exists, skipping",
                    std::ffi::CStr::from_ptr(collname).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            return InvalidOid;
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "collation \"{}\" already exists",
                    std::ffi::CStr::from_ptr(collname).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    tupDesc = RelationGetDescr(rel);

    /* form a tuple */
    nulls.iter_mut().for_each(|n| *n = false);

    namestrcpy(&mut name_name, collname);
    oid = GetNewOidWithIndex(rel, CollationOidIndexId, Anum_pg_collation_oid);
    values[(Anum_pg_collation_oid - 1) as usize] = ObjectIdGetDatum(oid);
    values[(Anum_pg_collation_collname - 1) as usize] = NameGetDatum(&name_name);
    values[(Anum_pg_collation_collnamespace - 1) as usize] = ObjectIdGetDatum(collnamespace);
    values[(Anum_pg_collation_collowner - 1) as usize] = ObjectIdGetDatum(collowner);
    values[(Anum_pg_collation_collprovider - 1) as usize] = CharGetDatum(collprovider);
    values[(Anum_pg_collation_collisdeterministic - 1) as usize] =
        BoolGetDatum(collisdeterministic);
    values[(Anum_pg_collation_collencoding - 1) as usize] = Int32GetDatum(collencoding);
    if !collcollate.is_null() {
        values[(Anum_pg_collation_collcollate - 1) as usize] = CStringGetTextDatum(collcollate);
    } else {
        nulls[(Anum_pg_collation_collcollate - 1) as usize] = true;
    }
    if !collctype.is_null() {
        values[(Anum_pg_collation_collctype - 1) as usize] = CStringGetTextDatum(collctype);
    } else {
        nulls[(Anum_pg_collation_collctype - 1) as usize] = true;
    }
    if !colllocale.is_null() {
        values[(Anum_pg_collation_colllocale - 1) as usize] = CStringGetTextDatum(colllocale);
    } else {
        nulls[(Anum_pg_collation_colllocale - 1) as usize] = true;
    }
    if !collicurules.is_null() {
        values[(Anum_pg_collation_collicurules - 1) as usize] = CStringGetTextDatum(collicurules);
    } else {
        nulls[(Anum_pg_collation_collicurules - 1) as usize] = true;
    }
    if !collversion.is_null() {
        values[(Anum_pg_collation_collversion - 1) as usize] = CStringGetTextDatum(collversion);
    } else {
        nulls[(Anum_pg_collation_collversion - 1) as usize] = true;
    }

    tup = heap_form_tuple(tupDesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    /* insert a new tuple */
    CatalogTupleInsert(rel, tup);
    assert!(OidIsValid(oid));

    /* set up dependencies for the new collation */
    myself.classId = CollationRelationId;
    myself.objectId = oid;
    myself.objectSubId = 0;

    /* create dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = collnamespace;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* create dependency on owner */
    recordDependencyOnOwner(CollationRelationId, oid, collowner);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new collation */
    InvokeObjectPostCreateHook(CollationRelationId, oid, 0);

    heap_freetuple(tup);
    table_close(rel, NoLock);

    oid
}

/* ---- imported / not-yet-ported dependencies ---- */

extern "C" {
    fn pg_encoding_to_char(encoding: c_int) -> *const c_char;
}

unsafe fn GetSysCacheOid3(
    _cacheId: c_int,
    _oidcol: AttrNumber,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
) -> Oid {
    unimplemented!() // TODO(pg-port): utils/cache/syscache.c
}

unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!() // TODO(pg-port): utils/mb/mbutils.c
}

unsafe fn ObjectAddressSet(addr: *mut ObjectAddress, class_id: Oid, object_id: Oid) {
    (*addr).classId = class_id;
    (*addr).objectId = object_id;
    (*addr).objectSubId = 0;
}

unsafe fn checkMembershipInCurrentExtension(_object: *const ObjectAddress) {
    unimplemented!() // TODO(pg-port): catalog/pg_depend.c
}

unsafe fn table_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO(pg-port): access/table/table.c
}

unsafe fn table_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO(pg-port): access/table/table.c
}

unsafe fn RelationGetDescr(_relation: Relation) -> TupleDesc {
    unimplemented!() // TODO(pg-port): utils/rel.h
}

unsafe fn namestrcpy(_name: *mut NameData, _s: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): utils/adt/name.c
}

unsafe fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: AttrNumber) -> Oid {
    unimplemented!() // TODO(pg-port): catalog/catalog.c
}

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): utils/builtins.h
}

unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}

unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {
    unimplemented!() // TODO(pg-port): catalog/indexing.c
}

unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_char,
) {
    unimplemented!() // TODO(pg-port): catalog/pg_depend.c
}

unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _owner: Oid) {
    unimplemented!() // TODO(pg-port): catalog/pg_shdepend.c
}

unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _isReplace: bool) {
    unimplemented!() // TODO(pg-port): catalog/pg_depend.c
}

unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    // TODO(pg-port): catalog/objectaccess.h (no-op unless hook installed)
}

unsafe fn heap_freetuple(_htup: HeapTuple) {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}

#[allow(unused_imports)]
use crate::{ereport, errmsg};
#[allow(unused_imports)]
use ptr as _;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // collname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_collation, collname), 4);
        // collnamespace follows the NAMEDATALEN-byte collname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_collation, collnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_collation>()
                >= core::mem::offset_of!(FormData_pg_collation, collencoding)
                    + core::mem::size_of::<int32>()
        );
    }
}
