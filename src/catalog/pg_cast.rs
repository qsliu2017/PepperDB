//! Translation of postgres/src/include/catalog/pg_cast.h
//!
//! The `FormData_pg_cast` struct: the fixed-layout part of a pg_cast catalog
//! row, describing the "type casts" system catalog.  As of Postgres 8.0,
//! pg_cast describes not only type coercion functions but also length coercion
//! functions.
//!
//! The pg_cast header has no `#ifdef CATALOG_VARLEN` section, so every declared
//! column is part of this fixed struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_cast - the fixed part of a pg_cast row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_cast {
    /* oid */
    pub oid: Oid,
    /* source datatype for cast */
    pub castsource: Oid,
    /* destination datatype for cast */
    pub casttarget: Oid,
    /* cast function; 0 = binary coercible */
    pub castfunc: Oid,
    /* contexts in which cast can be used (CoercionCodes COERCION_CODE_*) */
    pub castcontext: c_char,
    /* cast method (CoercionMethod COERCION_METHOD_*) */
    pub castmethod: c_char,
}

/*
 * Form_pg_cast corresponds to a pointer to a tuple with the format of the
 * pg_cast relation.
 */
pub type Form_pg_cast = *mut FormData_pg_cast;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * In the C header these are `typedef enum` members; since both columns are
 * stored as a "char", they use ASCII codes for human convenience in reading
 * the table.
 * ----------------------------------------------------------------
 */

/*
 * The allowable values for pg_cast.castcontext (CoercionCodes).  Internally to
 * the backend these are converted to the CoercionContext enum (primnodes.h);
 * the ASCII codes don't have to sort in any special order.
 */
pub const COERCION_CODE_IMPLICIT: c_char = b'i' as c_char; /* coercion in context of expression */
pub const COERCION_CODE_ASSIGNMENT: c_char = b'a' as c_char; /* coercion in context of assignment */
pub const COERCION_CODE_EXPLICIT: c_char = b'e' as c_char; /* explicit cast operation */

/*
 * The allowable values for pg_cast.castmethod (CoercionMethod).  Stored as a
 * "char" using ASCII codes for human convenience in reading the table.
 */
pub const COERCION_METHOD_FUNCTION: c_char = b'f' as c_char; /* use a function */
pub const COERCION_METHOD_BINARY: c_char = b'b' as c_char; /* types are binary-compatible */
pub const COERCION_METHOD_INOUT: c_char = b'i' as c_char; /* use input/output functions */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // castsource sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_cast, castsource), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_cast>()
                >= core::mem::offset_of!(FormData_pg_cast, castmethod)
                    + core::mem::size_of::<c_char>()
        );
    }
}
