//! Translation of postgres/src/include/catalog/pg_attrdef.h
//!
//! The `FormData_pg_attrdef` struct: the fixed-layout part of a pg_attrdef
//! ("attribute defaults") catalog row.  As in the C header, the struct as
//! compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length field (adbin, the
//! pg_node_tree nodeToString representation of the default, guarded by
//! CATALOG_VARLEN) is NOT part of this in-memory struct - it lives only in a
//! real on-disk pg_attrdef tuple and is reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int16;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_attrdef - the fixed part of a pg_attrdef row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_attrdef {
    /* oid */
    pub oid: Oid,
    /* OID of table containing attribute */
    pub adrelid: Oid,
    /* attnum of attribute */
    pub adnum: int16,
}

/*
 * Form_pg_attrdef corresponds to a pointer to a tuple with the format of the
 * pg_attrdef relation.
 */
pub type Form_pg_attrdef = *mut FormData_pg_attrdef;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // adrelid sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_attrdef, adrelid), 4);
        // adnum follows the 4-byte adrelid Oid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_attrdef, adnum),
            4 + core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_attrdef>()
                >= core::mem::offset_of!(FormData_pg_attrdef, adnum)
                    + core::mem::size_of::<int16>()
        );
    }
}
