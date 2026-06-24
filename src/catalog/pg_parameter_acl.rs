//! Translated from PostgreSQL src/include/catalog/pg_parameter_acl.h

use crate::c::{text, varlena};
use crate::postgres_ext::Oid;

pub const ParameterAclRelationId: Oid = Oid(6243); // BKI_SHARED_RELATION

#[repr(C)]
pub struct FormData_pg_parameter_acl {
    pub oid: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub parname: text,
    pub paracl: [varlena; 1], // aclitem[1]; varlena array tail
}

pub type Form_pg_parameter_acl = *mut FormData_pg_parameter_acl; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_parameter_acl_oid: i32 = 1;
pub const Anum_pg_parameter_acl_parname: i32 = 2;
pub const Anum_pg_parameter_acl_paracl: i32 = 3;
pub const Natts_pg_parameter_acl: i32 = 3;

// DECLARE_TOAST_WITH_MACRO(pg_parameter_acl, 6244, 6245, ...)
// DECLARE_UNIQUE_INDEX(pg_parameter_acl_parname_index, 6246, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_parameter_acl_oid_index, 6247, ...)
// MAKE_SYSCACHE(PARAMETERACLNAME, ...); MAKE_SYSCACHE(PARAMETERACLOID, ...)

pub fn ParameterAclLookup(_parameter: &str, _missing_ok: bool) -> Oid {
    unimplemented!()
}
pub fn ParameterAclCreate(_parameter: &str) -> Oid {
    unimplemented!()
}
