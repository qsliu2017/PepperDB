//! Translation of postgres/src/include/catalog/pg_depend.h
//!
//! FormData_pg_depend - records dependencies between database objects so that
//! DROP can complain or cascade.  No CATALOG_VARLEN section: all columns are
//! fixed-layout.  (The `deptype` codes live in catalog/dependency.h, ported
//! separately when that header lands.)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_depend {
    /// OID of the system catalog the dependent object is in.
    pub classid: Oid,
    /// OID of the dependent object itself.
    pub objid: Oid,
    /// Column number of the dependent object, or 0 if the whole object.
    pub objsubid: int32,
    /// OID of the system catalog the referenced object is in.
    pub refclassid: Oid,
    /// OID of the referenced object itself.
    pub refobjid: Oid,
    /// Column number of the referenced object, or 0 if the whole object.
    pub refobjsubid: int32,
    /// Dependency type code (see catalog/dependency.h).
    pub deptype: c_char,
}

pub type Form_pg_depend = *mut FormData_pg_depend;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(core::mem::offset_of!(FormData_pg_depend, objid), 4);
        assert_eq!(core::mem::offset_of!(FormData_pg_depend, refclassid), 12);
        assert!(
            core::mem::size_of::<FormData_pg_depend>()
                >= core::mem::offset_of!(FormData_pg_depend, deptype) + 1
        );
    }
}
