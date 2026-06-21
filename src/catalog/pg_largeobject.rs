//! Translation of postgres/src/include/catalog/pg_largeobject.h
//!
//! The `FormData_pg_largeobject` struct: the fixed-layout part of a
//! pg_largeobject catalog row.  The C header has no `#ifdef CATALOG_VARLEN`,
//! but the trailing `data bytea` column is a variable-length (varlena) type;
//! like the other CATALOG_VARLEN-style trailing fields it is NOT part of this
//! in-memory fixed struct.  It lives only in a real on-disk pg_largeobject
//! tuple and is reached via direct access (see inv_api.c) / heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! This file also carries the 1:1 translation of
//! postgres/src/backend/catalog/pg_largeobject.c - the routines to support
//! manipulation of the pg_largeobject relation.

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use crate::c::int32;
use crate::postgres_ext::Oid;

use core::ffi::{c_int, c_void};

use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid};
use crate::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};
use crate::c::OidIsValid;
use crate::catalog::aclchk::{get_user_default_acl, recordDependencyOnNewAcl};
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::catalog::catalog_oids::{LargeObjectMetadataRelationId, LargeObjectRelationId};
use crate::catalog::indexing::{CatalogTupleDelete, CatalogTupleInsert};
use crate::miscadmin::GetUserId;
use crate::nodes::parsenodes::ObjectType::OBJECT_LARGEOBJECT;
use crate::postgres::{Datum, ObjectIdGetDatum, PointerGetDatum};
use crate::storage::itemptr::ItemPointer;
use crate::utils::adt::acl::Acl;
use crate::storage::lockdefs::{AccessShareLock, RowExclusiveLock};
use crate::utils::rel::RelationGetDescr;
use crate::utils::snapshot::Snapshot;
use crate::{ereport, errmsg};
use crate::utils::elog::ERROR;

// Opaque relation/scan types not yet centrally ported into this module's path.
type Relation = *mut c_void;
type SysScanDesc = *mut c_void;

// utils/fmgroids.h - OIDEQ procedure oid.
const F_OIDEQ: Oid = 184;

// utils/errcodes.h.
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;

// Attribute numbers / column count for pg_largeobject_metadata
// (catalog/pg_largeobject_metadata_d.h).
const Anum_pg_largeobject_metadata_oid: c_int = 1;
const Anum_pg_largeobject_metadata_lomowner: c_int = 2;
const Anum_pg_largeobject_metadata_lomacl: c_int = 3;
const Natts_pg_largeobject_metadata: usize = 3;

// Attribute number for pg_largeobject (catalog/pg_largeobject_d.h).
const Anum_pg_largeobject_loid: c_int = 1;

// catalog/indexing.h index OIDs (the toast/index oids for largeobject).
// TODO(pg-port): centralize alongside the other DECLARE_UNIQUE_INDEX oids.
const LargeObjectMetadataOidIndexId: Oid = 2996;
const LargeObjectLOidPNIndexId: Oid = 2683;

const InvalidOid: Oid = 0;

/*
 * Create a large object having the given LO identifier.
 *
 * We create a new large object by inserting an entry into
 * pg_largeobject_metadata without any data pages, so that the object
 * will appear to exist with size 0.
 */
pub unsafe fn LargeObjectCreate(loid: Oid) -> Oid {
    let pg_lo_meta: Relation;
    let ntup: HeapTuple;
    let loid_new: Oid;
    let mut values: [Datum; Natts_pg_largeobject_metadata] = [0; Natts_pg_largeobject_metadata];
    let mut nulls: [bool; Natts_pg_largeobject_metadata] = [false; Natts_pg_largeobject_metadata];
    let ownerId: Oid;
    let lomacl: *mut Acl;

    pg_lo_meta = table_open(LargeObjectMetadataRelationId, RowExclusiveLock) as Relation;

    /*
     * Insert metadata of the largeobject
     */
    /* values/nulls already zero-initialized above */

    if OidIsValid(loid) {
        loid_new = loid;
    } else {
        loid_new = GetNewOidWithIndex(
            pg_lo_meta as _,
            LargeObjectMetadataOidIndexId,
            Anum_pg_largeobject_metadata_oid as _,
        );
    }
    ownerId = GetUserId();
    lomacl = get_user_default_acl(OBJECT_LARGEOBJECT, ownerId, InvalidOid);

    values[(Anum_pg_largeobject_metadata_oid - 1) as usize] = ObjectIdGetDatum(loid_new);
    values[(Anum_pg_largeobject_metadata_lomowner - 1) as usize] = ObjectIdGetDatum(ownerId);

    if !lomacl.is_null() {
        values[(Anum_pg_largeobject_metadata_lomacl - 1) as usize] =
            PointerGetDatum(lomacl as *const c_void);
    } else {
        nulls[(Anum_pg_largeobject_metadata_lomacl - 1) as usize] = true;
    }

    ntup = heap_form_tuple(
        RelationGetDescr(pg_lo_meta as _),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    CatalogTupleInsert(pg_lo_meta as _, ntup);

    heap_freetuple(ntup);

    table_close(pg_lo_meta as _, RowExclusiveLock);

    /* dependencies on roles mentioned in default ACL */
    recordDependencyOnNewAcl(LargeObjectRelationId, loid_new, 0, ownerId, lomacl);

    loid_new
}

/*
 * Drop a large object having the given LO identifier.  Both the data pages
 * and metadata must be dropped.
 */
pub unsafe fn LargeObjectDrop(loid: Oid) {
    let pg_lo_meta: Relation;
    let pg_largeobject: Relation;
    let mut skey: [ScanKeyData; 1] = std::mem::zeroed();
    let mut scan: SysScanDesc;
    let mut tuple: HeapTuple;

    pg_lo_meta = table_open(LargeObjectMetadataRelationId, RowExclusiveLock) as Relation;

    pg_largeobject = table_open(LargeObjectRelationId, RowExclusiveLock) as Relation;

    /*
     * Delete an entry from pg_largeobject_metadata
     */
    ScanKeyInit(
        &mut skey[0] as ScanKey,
        Anum_pg_largeobject_metadata_oid as _,
        BTEqualStrategyNumber as _,
        F_OIDEQ as _,
        ObjectIdGetDatum(loid),
    );

    scan = systable_beginscan(
        pg_lo_meta as _,
        LargeObjectMetadataOidIndexId,
        true,
        std::ptr::null_mut(),
        1,
        skey.as_mut_ptr() as ScanKey,
    ) as SysScanDesc;

    tuple = systable_getnext(scan as _) as HeapTuple;
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!("large object {} does not exist", loid)
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
        let _ = ERRCODE_UNDEFINED_OBJECT;
    }

    CatalogTupleDelete(pg_lo_meta as _, &mut (*tuple).t_self as ItemPointer);

    systable_endscan(scan as _);

    /*
     * Delete all the associated entries from pg_largeobject
     */
    ScanKeyInit(
        &mut skey[0] as ScanKey,
        Anum_pg_largeobject_loid as _,
        BTEqualStrategyNumber as _,
        F_OIDEQ as _,
        ObjectIdGetDatum(loid),
    );

    scan = systable_beginscan(
        pg_largeobject as _,
        LargeObjectLOidPNIndexId,
        true,
        std::ptr::null_mut(),
        1,
        skey.as_mut_ptr() as ScanKey,
    ) as SysScanDesc;
    loop {
        tuple = systable_getnext(scan as _) as HeapTuple;
        if !HeapTupleIsValid(tuple) {
            break;
        }
        CatalogTupleDelete(pg_largeobject as _, &mut (*tuple).t_self as ItemPointer);
    }

    systable_endscan(scan as _);

    table_close(pg_largeobject as _, RowExclusiveLock);

    table_close(pg_lo_meta as _, RowExclusiveLock);
}

/*
 * LargeObjectExists
 *
 * We don't use the system cache for large object metadata, for fear of
 * using too much local memory.
 *
 * This function always scans the system catalog using an up-to-date snapshot,
 * so it should not be used when a large object is opened in read-only mode
 * (because large objects opened in read only mode are supposed to be viewed
 * relative to the caller's snapshot, whereas in read-write mode they are
 * relative to a current snapshot).
 */
pub unsafe fn LargeObjectExists(loid: Oid) -> bool {
    LargeObjectExistsWithSnapshot(loid, std::ptr::null_mut())
}

/*
 * Same as LargeObjectExists(), except snapshot to read with can be specified.
 */
#[no_mangle]
pub unsafe fn LargeObjectExistsWithSnapshot(loid: Oid, snapshot: Snapshot) -> bool {
    let pg_lo_meta: Relation;
    let mut skey: [ScanKeyData; 1] = std::mem::zeroed();
    let sd: SysScanDesc;
    let tuple: HeapTuple;
    let mut retval: bool = false;

    ScanKeyInit(
        &mut skey[0] as ScanKey,
        Anum_pg_largeobject_metadata_oid as _,
        BTEqualStrategyNumber as _,
        F_OIDEQ as _,
        ObjectIdGetDatum(loid),
    );

    pg_lo_meta = table_open(LargeObjectMetadataRelationId, AccessShareLock) as Relation;

    sd = systable_beginscan(
        pg_lo_meta as _,
        LargeObjectMetadataOidIndexId,
        true,
        snapshot as _,
        1,
        skey.as_mut_ptr() as ScanKey,
    ) as SysScanDesc;

    tuple = systable_getnext(sd as _) as HeapTuple;
    if HeapTupleIsValid(tuple) {
        retval = true;
    }

    systable_endscan(sd as _);

    table_close(pg_lo_meta as _, AccessShareLock);

    retval
}

/*
 * FormData_pg_largeobject - the fixed part of a pg_largeobject row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_largeobject {
    /* Identifier of large object */
    pub loid: Oid,
    /* Page number (starting from 0) */
    pub pageno: int32,
}

/*
 * Form_pg_largeobject corresponds to a pointer to a tuple with the format of
 * the pg_largeobject relation.
 */
pub type Form_pg_largeobject = *mut FormData_pg_largeobject;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * pg_largeobject.h exposes no EXPOSE_TO_CLIENT_CODE #define constants.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // pageno sits right after the 4-byte loid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_largeobject, pageno), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_largeobject>()
                >= core::mem::offset_of!(FormData_pg_largeobject, pageno)
                    + core::mem::size_of::<int32>()
        );
    }
}
