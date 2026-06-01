//! Translation of postgres/src/include/catalog/pg_operator.h
//!
//! The `FormData_pg_operator` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_operator catalog row.  The C header has NO `#ifdef
//! CATALOG_VARLEN` section, so every field of the catalog definition (from
//! `oid` through `oprjoin`) is part of this in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_operator - the fixed part of a pg_operator row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_operator {
    /* oid */
    pub oid: Oid,
    /* name of operator */
    pub oprname: NameData,
    /* OID of namespace containing this oper */
    pub oprnamespace: Oid,
    /* operator owner */
    pub oprowner: Oid,
    /* 'l' for prefix or 'b' for infix */
    pub oprkind: c_char,
    /* can be used in merge join? */
    pub oprcanmerge: bool,
    /* can be used in hash join? */
    pub oprcanhash: bool,
    /* left arg type, or 0 if prefix operator */
    pub oprleft: Oid,
    /* right arg type */
    pub oprright: Oid,
    /* result datatype; can be 0 in a "shell" operator */
    pub oprresult: Oid,
    /* OID of commutator oper, or 0 if none */
    pub oprcom: Oid,
    /* OID of negator oper, or 0 if none */
    pub oprnegate: Oid,
    /* OID of underlying function; can be 0 in a "shell" operator */
    pub oprcode: regproc,
    /* OID of restriction estimator, or 0 */
    pub oprrest: regproc,
    /* OID of join estimator, or 0 */
    pub oprjoin: regproc,
}

/*
 * Form_pg_operator corresponds to a pointer to a row with the format of the
 * pg_operator relation.
 */
pub type Form_pg_operator = *mut FormData_pg_operator;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * The pg_operator.h header defines no EXPOSE_TO_CLIENT_CODE macros; the
 * oprkind values ('l' prefix, 'b' infix) are documented inline only.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // oprname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_operator, oprname), 4);
        // oprnamespace follows the NAMEDATALEN-byte oprname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_operator, oprnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_operator>()
                >= core::mem::offset_of!(FormData_pg_operator, oprjoin)
                    + core::mem::size_of::<regproc>()
        );
    }
}
