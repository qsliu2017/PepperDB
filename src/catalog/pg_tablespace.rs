//! Translation of postgres/src/include/catalog/pg_tablespace.h
//!
//! FormData_pg_tablespace - the fixed part of a pg_tablespace row.  The trailing
//! spcacl (aclitem[]) and spcoptions (text[]) fields are guarded by
//! CATALOG_VARLEN and are NOT part of the in-memory fixed struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_tablespace {
    /// Row OID.
    pub oid: Oid,
    /// Tablespace name.
    pub spcname: NameData,
    /// Owner of the tablespace.
    pub spcowner: Oid,
}

pub type Form_pg_tablespace = *mut FormData_pg_tablespace;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(core::mem::offset_of!(FormData_pg_tablespace, spcname), 4);
        assert_eq!(
            core::mem::offset_of!(FormData_pg_tablespace, spcowner),
            4 + core::mem::size_of::<NameData>()
        );
        assert!(
            core::mem::size_of::<FormData_pg_tablespace>()
                >= core::mem::offset_of!(FormData_pg_tablespace, spcowner) + 4
        );
    }
}
