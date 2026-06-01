//! Translation of postgres/src/include/catalog/pg_ts_template.h
//!
//! The `FormData_pg_ts_template` struct: the fixed-layout part of a
//! pg_ts_template ("text search template") catalog row.  This C header has no
//! `#ifdef CATALOG_VARLEN` section, so every declared column is part of the
//! fixed in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_ts_template - the fixed part of a pg_ts_template row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_ts_template {
    /* oid */
    pub oid: Oid,
    /* template name */
    pub tmplname: NameData,
    /* name space */
    pub tmplnamespace: Oid,
    /* initialization method of dict (may be 0) */
    pub tmplinit: regproc,
    /* base method of dictionary */
    pub tmpllexize: regproc,
}

/*
 * Form_pg_ts_template corresponds to a pointer to a row with the format of the
 * pg_ts_template relation.
 */
pub type Form_pg_ts_template = *mut FormData_pg_ts_template;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // tmplname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_ts_template, tmplname), 4);
        // tmplnamespace follows the NAMEDATALEN-byte tmplname (offset 4 + 64).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_ts_template, tmplnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_ts_template>()
                >= core::mem::offset_of!(FormData_pg_ts_template, tmpllexize)
                    + core::mem::size_of::<regproc>()
        );
    }
}
