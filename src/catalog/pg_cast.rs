//! Translation of postgres/src/include/catalog/pg_cast.h
//!
//! The `FormData_pg_cast` struct: the fixed-layout part of a pg_cast catalog
//! row, describing the "type casts" system catalog.  As of Postgres 8.0,
//! pg_cast describes not only type coercion functions but also length coercion
//! functions.
//!
//! The pg_cast header has no `#ifdef CATALOG_VARLEN` section, so every declared
//! column is part of this fixed struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_cast - the fixed part of a pg_cast row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_cast {
    /* oid */
    pub oid: Oid,
    /* source datatype for cast */
    pub castsource: Oid,
    /* destination datatype for cast */
    pub casttarget: Oid,
    /* cast function; 0 = binary coercible */
    pub castfunc: Oid,
    /* contexts in which cast can be used (CoercionCodes COERCION_CODE_*) */
    pub castcontext: c_char,
    /* cast method (CoercionMethod COERCION_METHOD_*) */
    pub castmethod: c_char,
}

/*
 * Form_pg_cast corresponds to a pointer to a tuple with the format of the
 * pg_cast relation.
 */
pub type Form_pg_cast = *mut FormData_pg_cast;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * In the C header these are `typedef enum` members; since both columns are
 * stored as a "char", they use ASCII codes for human convenience in reading
 * the table.
 * ----------------------------------------------------------------
 */

/*
 * The allowable values for pg_cast.castcontext (CoercionCodes).  Internally to
 * the backend these are converted to the CoercionContext enum (primnodes.h);
 * the ASCII codes don't have to sort in any special order.
 */
pub const COERCION_CODE_IMPLICIT: c_char = b'i' as c_char; /* coercion in context of expression */
pub const COERCION_CODE_ASSIGNMENT: c_char = b'a' as c_char; /* coercion in context of assignment */
pub const COERCION_CODE_EXPLICIT: c_char = b'e' as c_char; /* explicit cast operation */

/*
 * The allowable values for pg_cast.castmethod (CoercionMethod).  Stored as a
 * "char" using ASCII codes for human convenience in reading the table.
 */
pub const COERCION_METHOD_FUNCTION: c_char = b'f' as c_char; /* use a function */
pub const COERCION_METHOD_BINARY: c_char = b'b' as c_char; /* types are binary-compatible */
pub const COERCION_METHOD_INOUT: c_char = b'i' as c_char; /* use input/output functions */

/* ----------------------------------------------------------------
 * Translation of postgres/src/backend/catalog/pg_cast.c
 *	  routines to support manipulation of the pg_cast relation
 * ----------------------------------------------------------------
 */
use core::ffi::c_int;
use crate::postgres::{Datum, ObjectIdGetDatum, CharGetDatum};
use crate::utils::error::elog::ERROR;
use crate::access::htup_details::HeapTuple;
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::access::table::table::{table_open, table_close, LOCKMODE};
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::catalog::indexing::CatalogTupleInsert;
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::dependency::{
    ObjectAddresses, new_object_addresses, add_exact_object_address,
    record_object_address_dependencies, free_object_addresses, DependencyType,
};
use crate::catalog::pg_depend::recordDependencyOnCurrentExtension;
use crate::utils::cache::syscache::SearchSysCache2;
use crate::utils::builtins::format_type_be;
use crate::utils::rel::RelationGetDescr;
use crate::utils::rel::Relation;
use crate::c::OidIsValid;
use crate::ereport;
use crate::errmsg;

/* catalog relation/index OIDs (catalog/pg_cast.h, pg_type.h, pg_proc.h) */
const CastRelationId: Oid = 2605;
const CastOidIndexId: Oid = 2660;
const TypeRelationId: Oid = 1247;
const ProcedureRelationId: Oid = 1255;

const RowExclusiveLock: LOCKMODE = 4;

/* syscache id (utils/syscache.h) */
const CASTSOURCETARGET: c_int = 0;

/* pg_cast column attribute numbers and total count (catalog/pg_cast.h) */
const Anum_pg_cast_oid: usize = 1;
const Anum_pg_cast_castsource: usize = 2;
const Anum_pg_cast_casttarget: usize = 3;
const Anum_pg_cast_castfunc: usize = 4;
const Anum_pg_cast_castcontext: usize = 5;
const Anum_pg_cast_castmethod: usize = 6;
const Natts_pg_cast: usize = 6;

/* ObjectAddressSet: catalog/objectaddress.h convenience setter. */
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

/* HeapTupleIsValid: access/htup.h. */
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

/* InvokeObjectPostCreateHook: no-op unless object_access_hook is set. */
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}

/*
 * ----------------------------------------------------------------
 *		CastCreate
 *
 * Forms and inserts catalog tuples for a new cast being created.
 * Caller must have already checked privileges, and done consistency
 * checks on the given datatypes and cast function (if applicable).
 *
 * Since we allow binary coercibility of the datatypes to the cast
 * function's input and result, there could be one or two WITHOUT FUNCTION
 * casts that this one depends on.  We don't record that explicitly
 * in pg_cast, but we still need to make dependencies on those casts.
 *
 * 'behavior' indicates the types of the dependencies that the new
 * cast will have on its input and output types, the cast function,
 * and the other casts if any.
 * ----------------------------------------------------------------
 */
pub unsafe fn CastCreate(
    sourcetypeid: Oid,
    targettypeid: Oid,
    funcid: Oid,
    incastid: Oid,
    outcastid: Oid,
    castcontext: c_char,
    castmethod: c_char,
    behavior: DependencyType,
) -> ObjectAddress {
    let relation: Relation;
    let mut tuple: HeapTuple;
    let castid: Oid;
    let mut values: [Datum; Natts_pg_cast] = [0; Natts_pg_cast];
    let nulls: [bool; Natts_pg_cast] = [false; Natts_pg_cast];
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let addrs: *mut ObjectAddresses;

    relation = table_open(CastRelationId, RowExclusiveLock);

    /*
     * Check for duplicate.  This is just to give a friendly error message,
     * the unique index would catch it anyway (so no need to sweat about race
     * conditions).
     */
    tuple = SearchSysCache2(
        CASTSOURCETARGET,
        ObjectIdGetDatum(sourcetypeid),
        ObjectIdGetDatum(targettypeid),
    );
    if HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "cast from type {} to type {} already exists",
                std::ffi::CStr::from_ptr(format_type_be(sourcetypeid)).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(targettypeid)).to_string_lossy()
            )
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        );
    }

    /* ready to go */
    castid = GetNewOidWithIndex(relation, CastOidIndexId, Anum_pg_cast_oid as _);
    values[Anum_pg_cast_oid - 1] = ObjectIdGetDatum(castid);
    values[Anum_pg_cast_castsource - 1] = ObjectIdGetDatum(sourcetypeid);
    values[Anum_pg_cast_casttarget - 1] = ObjectIdGetDatum(targettypeid);
    values[Anum_pg_cast_castfunc - 1] = ObjectIdGetDatum(funcid);
    values[Anum_pg_cast_castcontext - 1] = CharGetDatum(castcontext);
    values[Anum_pg_cast_castmethod - 1] = CharGetDatum(castmethod);

    tuple = heap_form_tuple(RelationGetDescr(relation), values.as_mut_ptr(), nulls.as_ptr() as *mut bool);

    CatalogTupleInsert(relation, tuple);

    addrs = new_object_addresses();

    /* make dependency entries */
    ObjectAddressSet(&mut myself, CastRelationId, castid);

    /* dependency on source type */
    ObjectAddressSet(&mut referenced, TypeRelationId, sourcetypeid);
    add_exact_object_address(&referenced, addrs);

    /* dependency on target type */
    ObjectAddressSet(&mut referenced, TypeRelationId, targettypeid);
    add_exact_object_address(&referenced, addrs);

    /* dependency on function */
    if OidIsValid(funcid) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, funcid);
        add_exact_object_address(&referenced, addrs);
    }

    /* dependencies on casts required for function */
    if OidIsValid(incastid) {
        ObjectAddressSet(&mut referenced, CastRelationId, incastid);
        add_exact_object_address(&referenced, addrs);
    }
    if OidIsValid(outcastid) {
        ObjectAddressSet(&mut referenced, CastRelationId, outcastid);
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, behavior);
    free_object_addresses(addrs);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new cast */
    InvokeObjectPostCreateHook(CastRelationId, castid, 0);

    heap_freetuple(tuple);

    table_close(relation, RowExclusiveLock);

    myself
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // castsource sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_cast, castsource), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_cast>()
                >= core::mem::offset_of!(FormData_pg_cast, castmethod)
                    + core::mem::size_of::<c_char>()
        );
    }
}
