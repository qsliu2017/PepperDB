//! Translated from PostgreSQL src/include/catalog/pg_default_acl.h

use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

pub const DefaultAclRelationId: Oid = Oid::new(826);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_default_acl {
    pub oid: Oid,
    pub defaclrole: Oid,      // BKI_LOOKUP(pg_authid)
    pub defaclnamespace: Oid, // BKI_LOOKUP_OPT(pg_namespace)
    pub defaclobjtype: i8,
    // CATALOG_VARLEN (not in fixed part):
    pub defaclacl: [AclItem; 1], // aclitem[1]
}

pub type Form_pg_default_acl = *mut FormData_pg_default_acl; // TODO(ptr)

// DECLARE_TOAST(pg_default_acl, 4143, 4144)
// DECLARE_UNIQUE_INDEX(pg_default_acl_role_nsp_obj_index, 827, DefaultAclRoleNspObjIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_default_acl_oid_index, 828, DefaultAclOidIndexId)
// MAKE_SYSCACHE(DEFACLROLENSPOBJ, pg_default_acl_role_nsp_obj_index, 8)

// Object types for defaclobjtype
pub const DEFACLOBJ_RELATION: i8 = b'r' as i8;
pub const DEFACLOBJ_SEQUENCE: i8 = b'S' as i8;
pub const DEFACLOBJ_FUNCTION: i8 = b'f' as i8;
pub const DEFACLOBJ_TYPE: i8 = b'T' as i8;
pub const DEFACLOBJ_NAMESPACE: i8 = b'n' as i8;
pub const DEFACLOBJ_LARGEOBJECT: i8 = b'L' as i8;
