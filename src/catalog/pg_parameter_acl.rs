//! Translation of postgres/src/include/catalog/pg_parameter_acl.h
//!
//! The `FormData_pg_parameter_acl` struct: the fixed-layout part of a
//! pg_parameter_acl ("configuration parameter ACL") catalog row.  As in the C
//! header, the struct as compiled into the backend stops at the field just
//! before `#ifdef CATALOG_VARLEN`; the trailing variable-length fields (parname
//! text, paracl aclitem[], guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_parameter_acl tuple
//! and are reached via heap_getattr.  Thus the fixed part is just the `oid`
//! column.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_parameter_acl - the fixed part of a pg_parameter_acl row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_parameter_acl {
    /* oid */
    pub oid: Oid,
}

/*
 * Form_pg_parameter_acl corresponds to a pointer to a tuple with the format of
 * the pg_parameter_acl relation.
 */
pub type Form_pg_parameter_acl = *mut FormData_pg_parameter_acl;

// ---------------------------------------------------------------------------
// Translation of postgres/src/backend/catalog/pg_parameter_acl.c
//   routines to support manipulation of the pg_parameter_acl relation
// ---------------------------------------------------------------------------

use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::access::htup_details::HeapTuple;
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::catalog::catalog_oids::ParameterAclRelationId;
use crate::catalog::indexing::CatalogTupleInsert;
use crate::utils::builtins::cstring_to_text;
use crate::utils::cache::lsyscache::GetSysCacheOid1;
use crate::utils::misc::guc::{
    check_GUC_name_for_parameter_acl, convert_GUC_name_for_parameter_acl,
};
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::access::common::tupdesc::TupleDesc;

// catalog/pg_parameter_acl.h column / attribute numbers.
const Anum_pg_parameter_acl_oid: AttrNumber = 1;
const Anum_pg_parameter_acl_parname: AttrNumber = 2;
const Anum_pg_parameter_acl_paracl: AttrNumber = 3;
const Natts_pg_parameter_acl: usize = 3;

// catalog/pg_parameter_acl.h index OID.
const ParameterAclOidIndexId: Oid = 6247;

// utils/syscache.h syscache id (PARAMETERACLNAME).  TODO(pg-port).
const PARAMETERACLNAME: c_int = 110;

const RowExclusiveLock: c_int = 3;
const NoLock: c_int = 0;

// ERRCODE_UNDEFINED_OBJECT (folded into the ereport! as a /* C also: */ note).
const ERRCODE_UNDEFINED_OBJECT: c_int = 0; // TODO(pg-port)

/*
 * ParameterAclLookup - Given a configuration parameter name,
 * look up the associated configuration parameter ACL's OID.
 *
 * If missing_ok is false, throw an error if ACL entry not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn ParameterAclLookup(parameter: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;
    let parname: *mut c_char;

    /* Convert name to the form it should have in pg_parameter_acl... */
    parname = convert_GUC_name_for_parameter_acl(parameter);

    /* ... and look it up */
    oid = GetSysCacheOid1(
        PARAMETERACLNAME,
        Anum_pg_parameter_acl_oid,
        PointerGetDatum(cstring_to_text(parname) as *const c_void),
    );

    if !OidIsValid(oid) && !missing_ok {
        let _ = ERRCODE_UNDEFINED_OBJECT;
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!(
                "parameter ACL \"{}\" does not exist",
                std::ffi::CStr::from_ptr(parameter).to_string_lossy()
            )
        );
    }

    pfree(parname as *mut c_void);

    oid
}

/*
 * ParameterAclCreate
 *
 * Add a new tuple to pg_parameter_acl.
 *
 * parameter: the parameter name to create an entry for.
 * Caller should have verified that there's no such entry already.
 *
 * Returns the new entry's OID.
 */
pub unsafe fn ParameterAclCreate(parameter: *const c_char) -> Oid {
    let parameterId: Oid;
    let parname: *mut c_char;
    let rel: Relation;
    let tupDesc: TupleDesc;
    let tuple: HeapTuple;
    let mut values: [Datum; Natts_pg_parameter_acl] = [0; Natts_pg_parameter_acl];
    let mut nulls: [bool; Natts_pg_parameter_acl] = [false; Natts_pg_parameter_acl];

    /*
     * To prevent cluttering pg_parameter_acl with useless entries, insist
     * that the name be valid.
     */
    check_GUC_name_for_parameter_acl(parameter);

    /* Convert name to the form it should have in pg_parameter_acl. */
    parname = convert_GUC_name_for_parameter_acl(parameter);

    /*
     * Create and insert a new record containing a null ACL.
     *
     * We don't take a strong enough lock to prevent concurrent insertions,
     * relying instead on the unique index.
     */
    rel = table_open(ParameterAclRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);
    parameterId = GetNewOidWithIndex(rel, ParameterAclOidIndexId, Anum_pg_parameter_acl_oid);
    values[Anum_pg_parameter_acl_oid as usize - 1] = ObjectIdGetDatum(parameterId);
    values[Anum_pg_parameter_acl_parname as usize - 1] =
        PointerGetDatum(cstring_to_text(parname) as *const c_void);
    nulls[Anum_pg_parameter_acl_paracl as usize - 1] = true;
    tuple = heap_form_tuple(tupDesc, values.as_ptr(), nulls.as_ptr());
    CatalogTupleInsert(rel, tuple);

    /* Close pg_parameter_acl, but keep lock till commit. */
    heap_freetuple(tuple);
    table_close(rel, NoLock);

    parameterId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // The sole fixed field, oid, sits at offset 0.
        assert_eq!(core::mem::offset_of!(FormData_pg_parameter_acl, oid), 0);
        // The struct must at least span through its last fixed field, oid.
        assert!(
            core::mem::size_of::<FormData_pg_parameter_acl>()
                >= core::mem::offset_of!(FormData_pg_parameter_acl, oid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
