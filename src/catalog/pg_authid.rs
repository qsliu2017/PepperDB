//! Translation of postgres/src/include/catalog/pg_authid.h
//!
//! The `FormData_pg_authid` struct: the fixed-layout, guaranteed-not-null part
//! of a pg_authid catalog row.  As in the C header, the struct as compiled into
//! the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing nullable fields (rolpassword, rolvaliduntil, guarded by
//! CATALOG_VARLEN) are NOT part of this in-memory struct - they live only in a
//! real on-disk pg_authid tuple and are reached via heap_getattr.
//!
//! pg_authid is the "authorization identifier" system catalog (pg_shadow and
//! pg_group are now views on pg_authid).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int32, NameData};
use crate::postgres_ext::Oid;

/*
 * FormData_pg_authid - the fixed part of a pg_authid row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_authid {
    /* oid */
    pub oid: Oid,
    /* name of role */
    pub rolname: NameData,
    /* read this field via superuser() only! */
    pub rolsuper: bool,
    /* inherit privileges from other roles? */
    pub rolinherit: bool,
    /* allowed to create more roles? */
    pub rolcreaterole: bool,
    /* allowed to create databases? */
    pub rolcreatedb: bool,
    /* allowed to log in as session user? */
    pub rolcanlogin: bool,
    /* role used for streaming replication */
    pub rolreplication: bool,
    /* bypasses row-level security? */
    pub rolbypassrls: bool,
    /* max connections allowed (-1=no limit) */
    pub rolconnlimit: int32,
}

/*
 * Form_pg_authid corresponds to a pointer to a tuple with the format of the
 * pg_authid relation.
 */
pub type Form_pg_authid = *mut FormData_pg_authid;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * pg_authid.h exposes no #define constants to client code.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // rolname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_authid, rolname), 4);
        // rolsuper follows the NAMEDATALEN-byte rolname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_authid, rolsuper),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_authid>()
                >= core::mem::offset_of!(FormData_pg_authid, rolconnlimit)
                    + core::mem::size_of::<int32>()
        );
    }
}
