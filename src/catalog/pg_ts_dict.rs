//! Translation of postgres/src/include/catalog/pg_ts_dict.h
//!
//! The `FormData_pg_ts_dict` struct: the fixed-layout part of a pg_ts_dict
//! catalog row (the "text search dictionary" system catalog).  As in the C
//! header, the struct as compiled into the backend stops at the field just
//! before `#ifdef CATALOG_VARLEN`; the trailing variable-length field
//! (dictinitoption, a text guarded by CATALOG_VARLEN) is NOT part of this
//! in-memory struct - it lives only in a real on-disk pg_ts_dict tuple and is
//! reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_ts_dict - the fixed part of a pg_ts_dict row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_ts_dict {
    /* oid */
    pub oid: Oid,
    /* dictionary name */
    pub dictname: NameData,
    /* name space */
    pub dictnamespace: Oid,
    /* owner */
    pub dictowner: Oid,
    /* dictionary's template */
    pub dicttemplate: Oid,
}

/*
 * Form_pg_ts_dict corresponds to a pointer to a row with the format of the
 * pg_ts_dict relation.
 */
pub type Form_pg_ts_dict = *mut FormData_pg_ts_dict;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // dictname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_ts_dict, dictname), 4);
        // dictnamespace follows the NAMEDATALEN-byte dictname (offset 4 + 64).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_ts_dict, dictnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_ts_dict>()
                >= core::mem::offset_of!(FormData_pg_ts_dict, dicttemplate)
                    + core::mem::size_of::<Oid>()
        );
    }
}
