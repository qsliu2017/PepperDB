//! Translation of postgres/src/include/catalog/pg_auth_members.h
//!
//! FormData_pg_auth_members - records role membership grants.  No CATALOG_VARLEN
//! section, so all columns are fixed-layout.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_auth_members {
    /// Row OID.
    pub oid: Oid,
    /// ID of a role.
    pub roleid: Oid,
    /// ID of a member of that role.
    pub member: Oid,
    /// Who granted the membership.
    pub grantor: Oid,
    /// Granted with admin option?
    pub admin_option: bool,
    /// Exercise privileges without SET ROLE?
    pub inherit_option: bool,
    /// Use SET ROLE to the target role?
    pub set_option: bool,
}

pub type Form_pg_auth_members = *mut FormData_pg_auth_members;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(core::mem::offset_of!(FormData_pg_auth_members, roleid), 4);
        assert_eq!(core::mem::offset_of!(FormData_pg_auth_members, admin_option), 16);
        assert!(
            core::mem::size_of::<FormData_pg_auth_members>()
                >= core::mem::offset_of!(FormData_pg_auth_members, set_option) + 1
        );
    }
}
