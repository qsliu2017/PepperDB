//! Translation of postgres/src/include/catalog/pg_type.h
//!
//! The `FormData_pg_type` struct: the fixed-layout, guaranteed-not-null part of
//! a pg_type catalog row.  This is exactly the portion of the row that the C
//! struct exposes in memory; the variable-length / nullable trailing fields
//! (typdefaultbin, typdefault, typacl, guarded by CATALOG_VARLEN in the C
//! header) are NOT part of this struct - they live only in a real on-disk
//! pg_type tuple and are reached via heap_getattr.
//!
//! Some of the values in a pg_type instance are copied into pg_attribute
//! instances (typlen/typbyval/typalign/typstorage); see FormData_pg_attribute.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, int32, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_type - the fixed part of a pg_type row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_type {
    /* oid */
    pub oid: Oid,
    /* type name */
    pub typname: NameData,
    /* OID of namespace containing this type */
    pub typnamespace: Oid,
    /* type owner */
    pub typowner: Oid,
    /* number of bytes for a fixed-size type; negative for variable-length */
    pub typlen: int16,
    /* pass by value (true) or by reference (false)? */
    pub typbyval: bool,
    /* type kind: base/composite/domain/enum/pseudo/range (TYPTYPE macros) */
    pub typtype: c_char,
    /* arbitrary type classification (TYPCATEGORY macros) */
    pub typcategory: c_char,
    /* is type "preferred" within its category? */
    pub typispreferred: bool,
    /* false if entry is only a placeholder (forward reference) */
    pub typisdefined: bool,
    /* delimiter for arrays of this type */
    pub typdelim: c_char,
    /* associated pg_class OID if a composite type, else 0 */
    pub typrelid: Oid,
    /* type-specific subscripting handler (0 = not subscriptable) */
    pub typsubscript: regproc,
    /* element type yielded by subscripting, else 0 */
    pub typelem: Oid,
    /* the "true" array type having this type as element, else 0 */
    pub typarray: Oid,
    /* text input conversion procedure (required) */
    pub typinput: regproc,
    /* text output conversion procedure (required) */
    pub typoutput: regproc,
    /* binary input conversion procedure (optional) */
    pub typreceive: regproc,
    /* binary output conversion procedure (optional) */
    pub typsend: regproc,
    /* input procedure for optional type modifiers */
    pub typmodin: regproc,
    /* output procedure for optional type modifiers */
    pub typmodout: regproc,
    /* custom ANALYZE procedure (0 selects the default) */
    pub typanalyze: regproc,
    /* alignment requirement when storing a value (TYPALIGN macros) */
    pub typalign: c_char,
    /* toasting preparation and default storage strategy (TYPSTORAGE macros) */
    pub typstorage: c_char,
    /* NOT NULL constraint against this datatype (mainly for domains) */
    pub typnotnull: bool,
    /* base type a domain is based on; 0 if not a domain */
    pub typbasetype: Oid,
    /* typmod to apply to a domain's base type; -1 if not a domain */
    pub typtypmod: int32,
    /* declared number of dimensions for an array domain type, else 0 */
    pub typndims: int32,
    /* collation: 0 if type cannot use collations, else collation OID */
    pub typcollation: Oid,
}

/*
 * Form_pg_type corresponds to a pointer to a row with the format of the pg_type
 * relation.
 */
pub type Form_pg_type = *mut FormData_pg_type;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * macros for values of poor-mans-enumerated-type columns
 * ----------------------------------------------------------------
 */

/* TYPTYPE_* - the typtype column */
pub const TYPTYPE_BASE: c_char = b'b' as c_char; /* base type (ordinary scalar type) */
pub const TYPTYPE_COMPOSITE: c_char = b'c' as c_char; /* composite (e.g., table's rowtype) */
pub const TYPTYPE_DOMAIN: c_char = b'd' as c_char; /* domain over another type */
pub const TYPTYPE_ENUM: c_char = b'e' as c_char; /* enumerated type */
pub const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char; /* multirange type */
pub const TYPTYPE_PSEUDO: c_char = b'p' as c_char; /* pseudo-type */
pub const TYPTYPE_RANGE: c_char = b'r' as c_char; /* range type */

/* TYPCATEGORY_* - the typcategory column */
pub const TYPCATEGORY_INVALID: c_char = b'\0' as c_char; /* not an allowed category */
pub const TYPCATEGORY_ARRAY: c_char = b'A' as c_char;
pub const TYPCATEGORY_BOOLEAN: c_char = b'B' as c_char;
pub const TYPCATEGORY_COMPOSITE: c_char = b'C' as c_char;
pub const TYPCATEGORY_DATETIME: c_char = b'D' as c_char;
pub const TYPCATEGORY_ENUM: c_char = b'E' as c_char;
pub const TYPCATEGORY_GEOMETRIC: c_char = b'G' as c_char;
pub const TYPCATEGORY_NETWORK: c_char = b'I' as c_char; /* think INET */
pub const TYPCATEGORY_NUMERIC: c_char = b'N' as c_char;
pub const TYPCATEGORY_PSEUDOTYPE: c_char = b'P' as c_char;
pub const TYPCATEGORY_RANGE: c_char = b'R' as c_char;
pub const TYPCATEGORY_STRING: c_char = b'S' as c_char;
pub const TYPCATEGORY_TIMESPAN: c_char = b'T' as c_char;
pub const TYPCATEGORY_USER: c_char = b'U' as c_char;
pub const TYPCATEGORY_BITSTRING: c_char = b'V' as c_char; /* er ... "varbit"? */
pub const TYPCATEGORY_UNKNOWN: c_char = b'X' as c_char;
pub const TYPCATEGORY_INTERNAL: c_char = b'Z' as c_char;

/* TYPALIGN_* - the typalign column (canonical home; tupmacs/tupdesc duplicate) */
pub const TYPALIGN_CHAR: c_char = b'c' as c_char; /* char alignment (i.e. unaligned) */
pub const TYPALIGN_SHORT: c_char = b's' as c_char; /* short alignment (typically 2 bytes) */
pub const TYPALIGN_INT: c_char = b'i' as c_char; /* int alignment (typically 4 bytes) */
pub const TYPALIGN_DOUBLE: c_char = b'd' as c_char; /* double alignment (often 8 bytes) */

/* TYPSTORAGE_* - the typstorage column (canonical home; tupmacs/tupdesc duplicate) */
pub const TYPSTORAGE_PLAIN: c_char = b'p' as c_char; /* type not prepared for toasting */
pub const TYPSTORAGE_EXTERNAL: c_char = b'e' as c_char; /* toastable, don't try to compress */
pub const TYPSTORAGE_EXTENDED: c_char = b'x' as c_char; /* fully toastable */
pub const TYPSTORAGE_MAIN: c_char = b'm' as c_char; /* like 'x' but try to store inline */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // typname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_type, typname), 4);
        // typnamespace follows the NAMEDATALEN-byte typname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_type, typnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_type>()
                >= core::mem::offset_of!(FormData_pg_type, typcollation)
                    + core::mem::size_of::<Oid>()
        );
    }
}
