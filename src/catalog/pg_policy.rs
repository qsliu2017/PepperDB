//! Translated from PostgreSQL src/include/catalog/pg_policy.h

use crate::c::{NameData, varlena};
use crate::postgres_ext::Oid;

pub const PolicyRelationId: Oid = Oid(3256);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_policy {
    pub oid: Oid,
    pub polname: NameData,
    pub polrelid: Oid,
    pub polcmd: i8, // char
    pub polpermissive: bool,
    // CATALOG_VARLEN (not in fixed part)
    pub polroles: [Oid; 1], // Oid[1] array tail
    pub polqual: varlena,   // pg_node_tree
    pub polwithcheck: varlena, // pg_node_tree
}

pub type Form_pg_policy = *mut FormData_pg_policy; // TODO(ptr)

// DECLARE_TOAST(pg_policy, 4167, 4168)
// DECLARE_UNIQUE_INDEX_PKEY(pg_policy_oid_index, 3257, PolicyOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_policy_polrelid_polname_index, 3258, ...)
