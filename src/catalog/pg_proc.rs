//! Translation of postgres/src/include/catalog/pg_proc.h
//!
//! The `FormData_pg_proc` struct: the fixed-layout part of a pg_proc catalog
//! row, i.e. the fields from the opening brace up to the start of the
//! variable-length columns.
//!
//! NOTE on the CATALOG_VARLEN cutoff: in the C header, `prorettype` is the last
//! fixed-width field.  The comment "variable-length fields start here, but we
//! allow direct access to proargtypes" precedes `proargtypes` (an oidvector),
//! which is the FIRST variable-length field - it lives outside the #ifdef
//! CATALOG_VARLEN block only so C code can reference it directly, but it is NOT
//! a fixed-width member and is OMITTED here.  All remaining fields
//! (proallargtypes, proargmodes, proargnames, proargdefaults, protrftypes,
//! prosrc, probin, prosqlbody, proconfig, proacl) are guarded by
//! CATALOG_VARLEN and are likewise not part of this struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{float4, int16, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* regproc is a C typedef for Oid (see postgres_ext.h / c.h usage). */
pub type regproc = Oid;

/*
 * FormData_pg_proc - the fixed part of a pg_proc row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_proc {
    /* oid */
    pub oid: Oid,
    /* procedure name */
    pub proname: NameData,
    /* OID of namespace containing this proc */
    pub pronamespace: Oid,
    /* procedure owner */
    pub proowner: Oid,
    /* OID of pg_language entry */
    pub prolang: Oid,
    /* estimated execution cost */
    pub procost: float4,
    /* estimated # of rows out (if proretset) */
    pub prorows: float4,
    /* element type of variadic array, or 0 if not variadic */
    pub provariadic: Oid,
    /* planner support function for this function, or 0 if none */
    pub prosupport: regproc,
    /* see PROKIND_ categories below */
    pub prokind: c_char,
    /* security definer */
    pub prosecdef: bool,
    /* is it a leakproof function? */
    pub proleakproof: bool,
    /* strict with respect to NULLs? */
    pub proisstrict: bool,
    /* returns a set? */
    pub proretset: bool,
    /* see PROVOLATILE_ categories below */
    pub provolatile: c_char,
    /* see PROPARALLEL_ categories below */
    pub proparallel: c_char,
    /* number of arguments */
    pub pronargs: int16,
    /* number of arguments with defaults */
    pub pronargdefaults: int16,
    /* OID of result type */
    pub prorettype: Oid,
}

/*
 * Form_pg_proc corresponds to a pointer to a tuple with the format of the
 * pg_proc relation.
 */
pub type Form_pg_proc = *mut FormData_pg_proc;

/* Symbolic values for prokind column (EXPOSE_TO_CLIENT_CODE) */
pub const PROKIND_FUNCTION: c_char = b'f' as c_char;
pub const PROKIND_AGGREGATE: c_char = b'a' as c_char;
pub const PROKIND_WINDOW: c_char = b'w' as c_char;
pub const PROKIND_PROCEDURE: c_char = b'p' as c_char;

/* Symbolic values for provolatile column (EXPOSE_TO_CLIENT_CODE) */
pub const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char; /* never changes for given input */
pub const PROVOLATILE_STABLE: c_char = b's' as c_char; /* does not change within a scan */
pub const PROVOLATILE_VOLATILE: c_char = b'v' as c_char; /* can change even within a scan */

/* Symbolic values for proparallel column (EXPOSE_TO_CLIENT_CODE) */
pub const PROPARALLEL_SAFE: c_char = b's' as c_char; /* can run in worker or leader */
pub const PROPARALLEL_RESTRICTED: c_char = b'r' as c_char; /* can run in parallel leader only */
pub const PROPARALLEL_UNSAFE: c_char = b'u' as c_char; /* banned while in parallel mode */

/* Symbolic values for proargmodes column (EXPOSE_TO_CLIENT_CODE) */
pub const PROARGMODE_IN: c_char = b'i' as c_char;
pub const PROARGMODE_OUT: c_char = b'o' as c_char;
pub const PROARGMODE_INOUT: c_char = b'b' as c_char;
pub const PROARGMODE_VARIADIC: c_char = b'v' as c_char;
pub const PROARGMODE_TABLE: c_char = b't' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // proname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_proc, proname), 4);
        // prorettype is the last fixed field; the struct must be at least large
        // enough to hold it (alignment may add trailing padding, as in C).
        assert!(
            core::mem::size_of::<FormData_pg_proc>()
                >= core::mem::offset_of!(FormData_pg_proc, prorettype) + core::mem::size_of::<Oid>()
        );
        // regproc is faithfully an Oid (4 bytes).
        assert_eq!(core::mem::size_of::<regproc>(), core::mem::size_of::<Oid>());
    }
}
