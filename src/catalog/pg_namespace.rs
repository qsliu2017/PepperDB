//! Translation of postgres/src/include/catalog/pg_namespace.h
//!
//! The `FormData_pg_namespace` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_namespace catalog row.  This is exactly the portion of the row
//! that the C struct exposes in memory; the variable-length / nullable trailing
//! field (nspacl, guarded by CATALOG_VARLEN in the C header) is NOT part of this
//! struct - it lives only in a real on-disk pg_namespace tuple and is reached
//! via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

use crate::prelude::*;
use core::ffi::c_char;

/*
 * FormData_pg_namespace - the fixed part of a pg_namespace row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_namespace {
    /* oid */
    pub oid: Oid,
    /* name of the namespace */
    pub nspname: NameData,
    /* owner (creator) of the namespace */
    pub nspowner: Oid,
}

/*
 * Form_pg_namespace corresponds to a pointer to a tuple with the format of the
 * pg_namespace relation.
 */
pub type Form_pg_namespace = *mut FormData_pg_namespace;

// --- NamespaceCreate (translation of postgres/src/backend/catalog/pg_namespace.c) ---

const NamespaceRelationId: Oid = 2615;
const NamespaceOidIndexId: Oid = 2685;
const Natts_pg_namespace: usize = 4;
const Anum_pg_namespace_oid: i16 = 1;
const Anum_pg_namespace_nspname: i16 = 2;
const Anum_pg_namespace_nspowner: i16 = 3;
const Anum_pg_namespace_nspacl: i16 = 4;

#[repr(C)]
struct ObjectAddress {
    classId: Oid,
    objectId: Oid,
    objectSubId: core::ffi::c_int,
}

/* dependency on extension is a no-op here (matches other ports) */
unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _isReplace: bool) {}

/* post-create hook is a no-op (matches other ports) */
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: core::ffi::c_int) {}

/* ----------------
 * NamespaceCreate
 *
 * Create a namespace (schema) with the given name and owner OID.
 *
 * If isTemp is true, this schema is a per-backend schema for holding
 * temporary tables.
 * ---------------
 */
#[no_mangle]
pub unsafe extern "C" fn NamespaceCreate(nspName: *const c_char, ownerId: Oid, isTemp: bool) -> Oid {
    use crate::utils::adt::acl::Acl;

    /* sanity checks */
    if nspName.is_null() {
        elog!(ERROR, "no namespace name supplied");
    }

    /* make sure there is no existing namespace of same name */
    if crate::utils::cache::syscache::SearchSysCacheExists1(
        crate::utils::cache::syscache_ids_gen::NAMESPACENAME,
        PointerGetDatum(nspName as *const c_void),
    ) {
        ereport!(
            ERROR,
            errmsg!(
                "schema \"{}\" already exists",
                core::ffi::CStr::from_ptr(nspName).to_string_lossy()
            )
        );
    }

    let nspacl: *mut Acl = if !isTemp {
        crate::catalog::aclchk::get_user_default_acl(
            crate::nodes::parsenodes::ObjectType::OBJECT_SCHEMA,
            ownerId,
            crate::postgres_ext::InvalidOid,
        )
    } else {
        core::ptr::null_mut()
    };

    let nspdesc = crate::access::table::table::table_open(
        NamespaceRelationId,
        crate::storage::lockdefs::RowExclusiveLock,
    );
    let tupDesc = crate::utils::rel::RelationGetDescr(nspdesc);

    /* initialize nulls and values */
    let mut nulls: [bool; Natts_pg_namespace] = [false; Natts_pg_namespace];
    let mut values: [Datum; Natts_pg_namespace] = [0 as Datum; Natts_pg_namespace];

    let nspoid = crate::catalog::catalog::GetNewOidWithIndex(
        nspdesc,
        NamespaceOidIndexId,
        Anum_pg_namespace_oid,
    );
    values[Anum_pg_namespace_oid as usize - 1] = ObjectIdGetDatum(nspoid);
    let mut nname: NameData = core::mem::zeroed();
    crate::utils::builtins::namestrcpy(&mut nname as *mut NameData, nspName);
    values[Anum_pg_namespace_nspname as usize - 1] = NameGetDatum(&nname);
    values[Anum_pg_namespace_nspowner as usize - 1] = ObjectIdGetDatum(ownerId);
    if !nspacl.is_null() {
        values[Anum_pg_namespace_nspacl as usize - 1] = PointerGetDatum(nspacl as *const c_void);
    } else {
        nulls[Anum_pg_namespace_nspacl as usize - 1] = true;
    }

    let tup = crate::access::common::heaptuple::heap_form_tuple(
        tupDesc,
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    crate::catalog::indexing::CatalogTupleInsert(nspdesc, tup);
    Assert!(OidIsValid(nspoid));

    crate::access::table::table::table_close(nspdesc, crate::storage::lockdefs::RowExclusiveLock);

    /* Record dependencies */
    let myself = ObjectAddress {
        classId: NamespaceRelationId,
        objectId: nspoid,
        objectSubId: 0,
    };

    /* dependency on owner */
    crate::catalog::pg_shdepend::recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId);

    /* dependencies on roles mentioned in default ACL */
    crate::catalog::aclchk::recordDependencyOnNewAcl(
        NamespaceRelationId,
        nspoid,
        0,
        ownerId,
        nspacl,
    );

    /* dependency on extension ... but not for magic temp schemas */
    if !isTemp {
        recordDependencyOnCurrentExtension(&myself, false);
    }

    /* Post creation hook for new schema */
    InvokeObjectPostCreateHook(NamespaceRelationId, nspoid, 0);

    nspoid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // nspname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_namespace, nspname), 4);
        // nspowner follows the NAMEDATALEN-byte nspname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_namespace, nspowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_namespace>()
                >= core::mem::offset_of!(FormData_pg_namespace, nspowner)
                    + core::mem::size_of::<Oid>()
        );
    }
}
