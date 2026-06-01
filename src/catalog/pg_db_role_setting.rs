//! Translation of postgres/src/include/catalog/pg_db_role_setting.h
//!
//! The `FormData_pg_db_role_setting` struct: the fixed-layout part of a
//! pg_db_role_setting catalog row (per-database/per-user GUC settings).  As in
//! the C header, the struct as compiled into the backend stops at the field
//! just before `#ifdef CATALOG_VARLEN`; the trailing variable-length field
//! (setconfig[1], a text[] of GUC settings, guarded by CATALOG_VARLEN) is NOT
//! part of this in-memory struct - it lives only in a real on-disk tuple and is
//! reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_db_role_setting - the fixed part of a pg_db_role_setting row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_db_role_setting {
    /* database, or 0 for a role-specific setting */
    pub setdatabase: Oid,
    /* role, or 0 for a database-specific setting */
    pub setrole: Oid,
}

/*
 * Form_pg_db_role_setting corresponds to a pointer to a tuple with the format
 * of the pg_db_role_setting relation.
 */
pub type Form_pg_db_role_setting = *mut FormData_pg_db_role_setting;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (The pg_db_role_setting header exposes no #define constants.)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // setrole sits right after the 4-byte setdatabase Oid (the first key field).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_db_role_setting, setrole),
            core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_db_role_setting>()
                >= core::mem::offset_of!(FormData_pg_db_role_setting, setrole)
                    + core::mem::size_of::<Oid>()
        );
    }
}
