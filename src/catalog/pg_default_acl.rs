//! Translation of postgres/src/include/catalog/pg_default_acl.h
//!
//! The `FormData_pg_default_acl` struct: the fixed-layout part of a
//! pg_default_acl catalog row.  As in the C header, the struct as compiled into
//! the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length field (defaclacl[], an aclitem array guarded by
//! CATALOG_VARLEN) is NOT part of this in-memory struct - it lives only in a
//! real on-disk pg_default_acl tuple and is reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_default_acl - the fixed part of a pg_default_acl row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_default_acl {
    /* oid */
    pub oid: Oid,
    /* OID of role owning this ACL */
    pub defaclrole: Oid,
    /* OID of namespace, or 0 for all */
    pub defaclnamespace: Oid,
    /* see DEFACLOBJ_xxx constants below */
    pub defaclobjtype: c_char,
}

/*
 * Form_pg_default_acl corresponds to a pointer to a tuple with the format of
 * the pg_default_acl relation.
 */
pub type Form_pg_default_acl = *mut FormData_pg_default_acl;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * Types of objects for which the user is allowed to specify default
 * permissions through pg_default_acl.  These codes are used in the
 * defaclobjtype column.
 * ----------------------------------------------------------------
 */

pub const DEFACLOBJ_RELATION: c_char = b'r' as c_char; /* table, view */
pub const DEFACLOBJ_SEQUENCE: c_char = b'S' as c_char; /* sequence */
pub const DEFACLOBJ_FUNCTION: c_char = b'f' as c_char; /* function */
pub const DEFACLOBJ_TYPE: c_char = b'T' as c_char; /* type */
pub const DEFACLOBJ_NAMESPACE: c_char = b'n' as c_char; /* namespace */
pub const DEFACLOBJ_LARGEOBJECT: c_char = b'L' as c_char; /* large object */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // defaclrole sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_default_acl, defaclrole), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_default_acl>()
                >= core::mem::offset_of!(FormData_pg_default_acl, defaclobjtype)
                    + core::mem::size_of::<c_char>()
        );
    }
}
