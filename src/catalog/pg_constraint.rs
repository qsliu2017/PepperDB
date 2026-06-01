//! Translation of postgres/src/include/catalog/pg_constraint.h
//!
//! The `FormData_pg_constraint` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_constraint catalog row.  This is exactly the portion of the row
//! that the C struct exposes in memory; the variable-length / nullable trailing
//! fields (conkey, confkey, conpfeqop, conppeqop, conffeqop, confdelsetcols,
//! conexclop, conbin, guarded by CATALOG_VARLEN in the C header) are NOT part of
//! this struct - they live only in a real on-disk pg_constraint tuple and are
//! reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_constraint - the fixed part of a pg_constraint row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_constraint {
    /* oid */
    pub oid: Oid,
    /* name of this constraint */
    pub conname: NameData,
    /* OID of namespace containing constraint */
    pub connamespace: Oid,
    /* constraint type; see CONSTRAINT_* codes below */
    pub contype: c_char,
    /* deferrable constraint? */
    pub condeferrable: bool,
    /* deferred by default? */
    pub condeferred: bool,
    /* enforced constraint? */
    pub conenforced: bool,
    /* constraint has been validated? */
    pub convalidated: bool,
    /* relation this constraint constrains; 0 if not relation-specific */
    pub conrelid: Oid,
    /* domain this constraint constrains; 0 if not a domain constraint */
    pub contypid: Oid,
    /* index supporting this constraint, if any; else 0 */
    pub conindid: Oid,
    /* corresponding constraint OID in parent if inherited partition; else 0 */
    pub conparentid: Oid,
    /* relation referenced by foreign key; 0 if not a foreign key */
    pub confrelid: Oid,
    /* foreign key's ON UPDATE action */
    pub confupdtype: c_char,
    /* foreign key's ON DELETE action */
    pub confdeltype: c_char,
    /* foreign key's match type */
    pub confmatchtype: c_char,
    /* has a local definition (do not drop when coninhcount is 0) */
    pub conislocal: bool,
    /* number of times inherited from direct parent relation(s) */
    pub coninhcount: int16,
    /* has a local definition and cannot be inherited */
    pub connoinherit: bool,
    /* last column uses overlaps instead of equals (PK/unique/FK) */
    pub conperiod: bool,
}

/*
 * Form_pg_constraint corresponds to a pointer to a row with the format of the
 * pg_constraint relation.
 */
pub type Form_pg_constraint = *mut FormData_pg_constraint;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/* Valid values for contype */
pub const CONSTRAINT_CHECK: c_char = b'c' as c_char;
pub const CONSTRAINT_FOREIGN: c_char = b'f' as c_char;
pub const CONSTRAINT_NOTNULL: c_char = b'n' as c_char;
pub const CONSTRAINT_PRIMARY: c_char = b'p' as c_char;
pub const CONSTRAINT_UNIQUE: c_char = b'u' as c_char;
pub const CONSTRAINT_TRIGGER: c_char = b't' as c_char;
pub const CONSTRAINT_EXCLUSION: c_char = b'x' as c_char;

/*
 * Valid values for confupdtype and confdeltype are the FKCONSTR_ACTION_xxx
 * constants defined in parsenodes.h.  Valid values for confmatchtype are the
 * FKCONSTR_MATCH_xxx constants defined in parsenodes.h.
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // conname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_constraint, conname), 4);
        // connamespace follows the NAMEDATALEN-byte conname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_constraint, connamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_constraint>()
                >= core::mem::offset_of!(FormData_pg_constraint, conperiod)
                    + core::mem::size_of::<bool>()
        );
    }
}
