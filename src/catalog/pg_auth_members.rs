//! Translated from PostgreSQL src/include/catalog/pg_auth_members.h

use crate::postgres_ext::Oid;

// BKI_SHARED_RELATION BKI_ROWTYPE_OID(2843,AuthMemRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const AuthMemRelationId: Oid = Oid(1261);
pub const AuthMemRelation_Rowtype_Id: Oid = Oid(2843);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_auth_members {
    pub oid: Oid,
    pub roleid: Oid,  // BKI_LOOKUP(pg_authid)
    pub member: Oid,  // BKI_LOOKUP(pg_authid)
    pub grantor: Oid, // BKI_LOOKUP(pg_authid)
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

pub type Form_pg_auth_members = *mut FormData_pg_auth_members; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_auth_members_oid_index, 6303, AuthMemOidIndexId)
// DECLARE_UNIQUE_INDEX(pg_auth_members_role_member_index, 2694, AuthMemRoleMemIndexId)
// DECLARE_UNIQUE_INDEX(pg_auth_members_member_role_index, 2695, AuthMemMemRoleIndexId)
// DECLARE_INDEX(pg_auth_members_grantor_index, 6302, AuthMemGrantorIndexId)
// MAKE_SYSCACHE(AUTHMEMROLEMEM, pg_auth_members_role_member_index, 8)
// MAKE_SYSCACHE(AUTHMEMMEMROLE, pg_auth_members_member_role_index, 8)

