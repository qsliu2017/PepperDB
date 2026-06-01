//! Translation of postgres/src/include/catalog/pg_attribute.h
//!
//! The `FormData_pg_attribute` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_attribute catalog row.  This is exactly as much of the row as
//! gets copied into a tuple descriptor (see access/tupdesc.rs), so the
//! variable-length trailing fields (attstattarget/attacl/attoptions/
//! attfdwoptions/attmissingval, guarded by CATALOG_VARLEN in the C header) are
//! NOT part of this struct - they live only in a real on-disk pg_attribute
//! tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, int32, NameData};
use crate::postgres::NullableDatum;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_attribute - the fixed part of a pg_attribute row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; this
 * is required because tuple descriptors store an array of these immediately
 * after their CompactAttribute array, and sizeof(FormData_pg_attribute) is used
 * to compute the array stride (see TupleDescSize / TupleDescAttr).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_attribute {
    /* OID of relation containing this attribute */
    pub attrelid: Oid,
    /* name of attribute */
    pub attname: NameData,
    /* OID of the attribute's type (pg_type), or 0 for a dropped column */
    pub atttypid: Oid,
    /* copy of pg_type.typlen */
    pub attlen: int16,
    /* attribute number (1-based for user attrs; <0 for system attrs) */
    pub attnum: int16,
    /* type-specific modifier supplied at table creation (-1 if none) */
    pub atttypmod: int32,
    /* declared number of array dimensions, else 0 */
    pub attndims: int16,
    /* copy of pg_type.typbyval */
    pub attbyval: bool,
    /* copy of pg_type.typalign */
    pub attalign: c_char,
    /* see pg_type.typstorage (TYPSTORAGE macros) */
    pub attstorage: c_char,
    /* current compression method ('\0' = default, 'p' = pglz, 'l' = lz4) */
    pub attcompression: c_char,
    /* whether a (possibly invalid) not-null constraint exists */
    pub attnotnull: bool,
    /* has a DEFAULT value */
    pub atthasdef: bool,
    /* has a missing value */
    pub atthasmissing: bool,
    /* one of the ATTRIBUTE_IDENTITY_* constants, or '\0' */
    pub attidentity: c_char,
    /* one of the ATTRIBUTE_GENERATED_* constants, or '\0' */
    pub attgenerated: c_char,
    /* is dropped (logically invisible) */
    pub attisdropped: bool,
    /* whether the column has ever had a local definition */
    pub attislocal: bool,
    /* number of times inherited from direct parent relation(s) */
    pub attinhcount: int16,
    /* attribute's collation, if any */
    pub attcollation: Oid,
}

/*
 * Form_pg_attribute corresponds to a pointer to a tuple with the format of the
 * pg_attribute relation.
 */
pub type Form_pg_attribute = *mut FormData_pg_attribute;

/*
 * ATTRIBUTE_FIXED_PART_SIZE is the size of the fixed-layout,
 * guaranteed-not-null part of a pg_attribute row.  This is in fact as much of
 * the row as gets copied into tuple descriptors.
 *
 *   #define ATTRIBUTE_FIXED_PART_SIZE \
 *       (offsetof(FormData_pg_attribute,attcollation) + sizeof(Oid))
 */
pub const ATTRIBUTE_FIXED_PART_SIZE: usize =
    core::mem::offset_of!(FormData_pg_attribute, attcollation) + core::mem::size_of::<Oid>();

/*
 * FormExtraData_pg_attribute contains (some of) the fields excluded from
 * FormData_pg_attribute by CATALOG_VARLEN; used by DDL code so that the
 * combination of the two can pass around all the information about an attribute.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormExtraData_pg_attribute {
    pub attstattarget: NullableDatum,
    pub attoptions: NullableDatum,
}

/* ATTRIBUTE_IDENTITY_* (EXPOSE_TO_CLIENT_CODE) */
pub const ATTRIBUTE_IDENTITY_ALWAYS: c_char = b'a' as c_char;
pub const ATTRIBUTE_IDENTITY_BY_DEFAULT: c_char = b'd' as c_char;

/* ATTRIBUTE_GENERATED_* (EXPOSE_TO_CLIENT_CODE) */
pub const ATTRIBUTE_GENERATED_STORED: c_char = b's' as c_char;
pub const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // attcollation is the last fixed field; the fixed part ends right after it.
        assert_eq!(
            ATTRIBUTE_FIXED_PART_SIZE,
            core::mem::offset_of!(FormData_pg_attribute, attcollation) + 4
        );
        // The struct must be at least as large as its fixed part (alignment may
        // add trailing padding, which is fine - the C struct has it too).
        assert!(core::mem::size_of::<FormData_pg_attribute>() >= ATTRIBUTE_FIXED_PART_SIZE);
        // attname sits right after the 4-byte attrelid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_attribute, attname), 4);
    }
}
