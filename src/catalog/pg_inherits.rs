//! Translation of postgres/src/include/catalog/pg_inherits.h
//!
//! FormData_pg_inherits - records table inheritance / partitioning parent links.
//! This header has no CATALOG_VARLEN section, so all columns are fixed-layout.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_inherits {
    /// OID of the child relation.
    pub inhrelid: Oid,
    /// OID of the parent relation.
    pub inhparent: Oid,
    /// 1-based position of this parent among the child's parents.
    pub inhseqno: int32,
    /// True while a concurrent partition detach is in progress.
    pub inhdetachpending: bool,
}

pub type Form_pg_inherits = *mut FormData_pg_inherits;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(core::mem::offset_of!(FormData_pg_inherits, inhparent), 4);
        assert_eq!(core::mem::offset_of!(FormData_pg_inherits, inhseqno), 8);
        assert!(
            core::mem::size_of::<FormData_pg_inherits>()
                >= core::mem::offset_of!(FormData_pg_inherits, inhdetachpending) + 1
        );
    }
}
