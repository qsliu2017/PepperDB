//! Translation of postgres/src/include/catalog/pg_aggregate.h
//!
//! The `FormData_pg_aggregate` struct: the fixed-layout part of a pg_aggregate
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length fields (agginitval, aggminitval - both `text`, guarded by
//! CATALOG_VARLEN) are NOT part of this in-memory struct - they live only in a
//! real on-disk pg_aggregate tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, int32};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_aggregate - the fixed part of a pg_aggregate row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_aggregate {
    /* pg_proc OID of the aggregate itself */
    pub aggfnoid: regproc,
    /* aggregate kind, see AGGKIND_ categories below */
    pub aggkind: c_char,
    /* number of arguments that are "direct" arguments */
    pub aggnumdirectargs: int16,
    /* transition function */
    pub aggtransfn: regproc,
    /* final function (0 if none) */
    pub aggfinalfn: regproc,
    /* combine function (0 if none) */
    pub aggcombinefn: regproc,
    /* function to convert transtype to bytea (0 if none) */
    pub aggserialfn: regproc,
    /* function to convert bytea to transtype (0 if none) */
    pub aggdeserialfn: regproc,
    /* forward function for moving-aggregate mode (0 if none) */
    pub aggmtransfn: regproc,
    /* inverse function for moving-aggregate mode (0 if none) */
    pub aggminvtransfn: regproc,
    /* final function for moving-aggregate mode (0 if none) */
    pub aggmfinalfn: regproc,
    /* true to pass extra dummy arguments to aggfinalfn */
    pub aggfinalextra: bool,
    /* true to pass extra dummy arguments to aggmfinalfn */
    pub aggmfinalextra: bool,
    /* tells whether aggfinalfn modifies transition state */
    pub aggfinalmodify: c_char,
    /* tells whether aggmfinalfn modifies transition state */
    pub aggmfinalmodify: c_char,
    /* associated sort operator (0 if none) */
    pub aggsortop: Oid,
    /* type of aggregate's transition (state) data */
    pub aggtranstype: Oid,
    /* estimated size of state data (0 for default estimate) */
    pub aggtransspace: int32,
    /* type of moving-aggregate state data (0 if none) */
    pub aggmtranstype: Oid,
    /* estimated size of moving-agg state (0 for default est) */
    pub aggmtransspace: int32,
}

/*
 * Form_pg_aggregate corresponds to a pointer to a tuple with the format of the
 * pg_aggregate relation.
 */
pub type Form_pg_aggregate = *mut FormData_pg_aggregate;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/*
 * Symbolic values for aggkind column.  We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments) and from hypothetical-set aggregates
 * (which are a subclass of ordered-set aggregates in which the last
 * direct arguments have to match up in number and datatypes with the
 * aggregated arguments).
 */
pub const AGGKIND_NORMAL: c_char = b'n' as c_char;
pub const AGGKIND_ORDERED_SET: c_char = b'o' as c_char;
pub const AGGKIND_HYPOTHETICAL: c_char = b'h' as c_char;

/* Use this macro to test for "ordered-set agg including hypothetical case" */
pub fn AGGKIND_IS_ORDERED_SET(kind: c_char) -> bool {
    kind != AGGKIND_NORMAL
}

/*
 * Symbolic values for aggfinalmodify and aggmfinalmodify columns.
 * Preferably, finalfns do not modify the transition state value at all,
 * but in some cases that would cost too much performance.  We distinguish
 * "pure read only" and "trashes it arbitrarily" cases, as well as the
 * intermediate case where multiple finalfn calls are allowed but the
 * transfn cannot be applied anymore after the first finalfn call.
 */
pub const AGGMODIFY_READ_ONLY: c_char = b'r' as c_char;
pub const AGGMODIFY_SHAREABLE: c_char = b's' as c_char;
pub const AGGMODIFY_READ_WRITE: c_char = b'w' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // aggfnoid is the first (key) field, sitting at offset 0.  Note there is
        // no leading oid column in pg_aggregate; aggfnoid serves as the OID key.
        assert_eq!(core::mem::offset_of!(FormData_pg_aggregate, aggfnoid), 0);
        // aggkind follows the 4-byte regproc (Oid) aggfnoid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_aggregate, aggkind),
            core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_aggregate>()
                >= core::mem::offset_of!(FormData_pg_aggregate, aggmtransspace)
                    + core::mem::size_of::<int32>()
        );
    }
}
