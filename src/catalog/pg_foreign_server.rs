//! Translation of postgres/src/include/catalog/pg_foreign_server.h
//!
//! The `FormData_pg_foreign_server` struct: the fixed-layout part of a
//! pg_foreign_server catalog row.  As in the C header, the struct as compiled
//! into the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length fields (srvtype, srvversion, srvacl[], srvoptions[],
//! guarded by CATALOG_VARLEN) are NOT part of this in-memory struct - they live
//! only in a real on-disk pg_foreign_server tuple and are reached via
//! heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_foreign_server - the fixed part of a pg_foreign_server row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_foreign_server {
    /* oid */
    pub oid: Oid,
    /* foreign server name */
    pub srvname: NameData,
    /* server owner */
    pub srvowner: Oid,
    /* server FDW */
    pub srvfdw: Oid,
}

/*
 * Form_pg_foreign_server corresponds to a pointer to a tuple with the format of
 * the pg_foreign_server relation.
 */
pub type Form_pg_foreign_server = *mut FormData_pg_foreign_server;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // srvname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_foreign_server, srvname), 4);
        // srvowner follows the NAMEDATALEN-byte srvname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_foreign_server, srvowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_foreign_server>()
                >= core::mem::offset_of!(FormData_pg_foreign_server, srvfdw)
                    + core::mem::size_of::<Oid>()
        );
    }
}
