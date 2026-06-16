//! Translation of postgres/src/include/catalog/pg_range.h
//!
//! The `FormData_pg_range` struct: the fixed-layout part of a pg_range catalog
//! row, defining the "range type" system catalog.  This header has NO
//! CATALOG_VARLEN section, so every declared column is part of the fixed C
//! struct and is included here.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_range - the fixed part of a pg_range row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_range {
    /* OID of owning range type */
    pub rngtypid: Oid,
    /* OID of range's element type (subtype) */
    pub rngsubtype: Oid,
    /* OID of the range's multirange type */
    pub rngmultitypid: Oid,
    /* collation for this range type, or 0 */
    pub rngcollation: Oid,
    /* subtype's btree opclass */
    pub rngsubopc: Oid,
    /* canonicalize range, or 0 */
    pub rngcanonical: regproc,
    /* subtype difference as a float8, or 0 */
    pub rngsubdiff: regproc,
}

/*
 * Form_pg_range corresponds to a pointer to a row with the format of the
 * pg_range relation.
 */
pub type Form_pg_range = *mut FormData_pg_range;

/*-------------------------------------------------------------------------
 *
 * pg_range.c
 *	  routines to support manipulation of the pg_range relation
 *
 * Source: postgres/src/backend/catalog/pg_range.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::index::genam::{
    SysScanDesc, systable_beginscan, systable_endscan, systable_getnext,
};
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::catalog::objectaddress_impl::{table_close, table_open};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::catalog_oids::{
    CollationRelationId, OperatorClassRelationId, ProcedureRelationId,
    RangeRelationId, TypeRelationId,
};
use crate::catalog::dependency::{
    DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, ObjectAddresses,
    add_exact_object_address, free_object_addresses, new_object_addresses,
    record_object_address_dependencies,
};
use crate::catalog::pg_depend::recordDependencyOn;
use crate::catalog::indexing::{CatalogTupleDelete, CatalogTupleInsert};
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::storage::lockdefs::RowExclusiveLock;

// pg_range column numbers (catalog/pg_range.h)
// TODO(pg-port): replace with generated pg_range_d.h constants.
const Natts_pg_range: usize = 7;
const Anum_pg_range_rngtypid: AttrNumber = 1;
const Anum_pg_range_rngsubtype: AttrNumber = 2;
const Anum_pg_range_rngmultitypid: AttrNumber = 3;
const Anum_pg_range_rngcollation: AttrNumber = 4;
const Anum_pg_range_rngsubopc: AttrNumber = 5;
const Anum_pg_range_rngcanonical: AttrNumber = 6;
const Anum_pg_range_rngsubdiff: AttrNumber = 7;

// pg_range index OID (catalog/pg_range.h)
// TODO(pg-port): replace with generated indexing constant.
const RangeTypidIndexId: Oid = 3542;

// fmgr OID used in ScanKeyInit (utils/fmgroids.h)
// TODO(pg-port): replace with generated fmgroids.h constant.
const F_OIDEQ: RegProcedure = 184;

/* catalog/dependency.h ObjectAddressSet helper. */
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, classId: Oid, objectId: Oid) {
    addr.classId = classId;
    addr.objectId = objectId;
    addr.objectSubId = 0;
}

/*
 * RangeCreate
 *		Create an entry in pg_range.
 */
pub unsafe fn RangeCreate(
    rangeTypeOid: Oid,
    rangeSubType: Oid,
    rangeCollation: Oid,
    rangeSubOpclass: Oid,
    rangeCanonical: RegProcedure,
    rangeSubDiff: RegProcedure,
    multirangeTypeOid: Oid,
) {
    let pg_range: Relation;
    let mut values: [Datum; Natts_pg_range] = [0; Natts_pg_range];
    let mut nulls: [bool; Natts_pg_range] = [false; Natts_pg_range];
    let tup: HeapTuple;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let mut referencing: ObjectAddress = core::mem::zeroed();
    let addrs: *mut ObjectAddresses;

    pg_range = table_open(RangeRelationId, RowExclusiveLock);

    nulls = [false; Natts_pg_range];

    values[Anum_pg_range_rngtypid as usize - 1] = ObjectIdGetDatum(rangeTypeOid);
    values[Anum_pg_range_rngsubtype as usize - 1] = ObjectIdGetDatum(rangeSubType);
    values[Anum_pg_range_rngcollation as usize - 1] = ObjectIdGetDatum(rangeCollation);
    values[Anum_pg_range_rngsubopc as usize - 1] = ObjectIdGetDatum(rangeSubOpclass);
    values[Anum_pg_range_rngcanonical as usize - 1] = ObjectIdGetDatum(rangeCanonical);
    values[Anum_pg_range_rngsubdiff as usize - 1] = ObjectIdGetDatum(rangeSubDiff);
    values[Anum_pg_range_rngmultitypid as usize - 1] = ObjectIdGetDatum(multirangeTypeOid);

    tup = heap_form_tuple(RelationGetDescr(pg_range), values.as_ptr(), nulls.as_ptr());

    CatalogTupleInsert(pg_range, tup);
    heap_freetuple(tup);

    /* record type's dependencies on range-related items */
    addrs = new_object_addresses();

    ObjectAddressSet(&mut myself, TypeRelationId, rangeTypeOid);

    ObjectAddressSet(&mut referenced, TypeRelationId, rangeSubType);
    add_exact_object_address(&referenced, addrs);

    ObjectAddressSet(&mut referenced, OperatorClassRelationId, rangeSubOpclass);
    add_exact_object_address(&referenced, addrs);

    if OidIsValid(rangeCollation) {
        ObjectAddressSet(&mut referenced, CollationRelationId, rangeCollation);
        add_exact_object_address(&referenced, addrs);
    }

    if OidIsValid(rangeCanonical) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, rangeCanonical);
        add_exact_object_address(&referenced, addrs);
    }

    if OidIsValid(rangeSubDiff) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, rangeSubDiff);
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    /* record multirange type's dependency on the range type */
    referencing.classId = TypeRelationId;
    referencing.objectId = multirangeTypeOid;
    referencing.objectSubId = 0;
    recordDependencyOn(&referencing, &myself, DEPENDENCY_INTERNAL);

    table_close(pg_range, RowExclusiveLock);
}

/*
 * RangeDelete
 *		Remove the pg_range entry for the specified type.
 */
pub unsafe fn RangeDelete(rangeTypeOid: Oid) {
    let pg_range: Relation;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    pg_range = table_open(RangeRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_range_rngtypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(rangeTypeOid),
    );

    scan = systable_beginscan(
        pg_range,
        RangeTypidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }
        CatalogTupleDelete(pg_range, &mut (*tup).t_self);
    }

    systable_endscan(scan);

    table_close(pg_range, RowExclusiveLock);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // rngsubtype sits right after the 4-byte rngtypid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_range, rngsubtype), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_range>()
                >= core::mem::offset_of!(FormData_pg_range, rngsubdiff)
                    + core::mem::size_of::<regproc>()
        );
    }
}
