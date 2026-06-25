//! Translated from PostgreSQL src/include/catalog/pg_namespace.h

use crate::c::NameData;
use crate::postgres_ext::Oid;

pub const NamespaceRelationId: Oid = Oid(2615);

// aclitem catalog field is varlena; modeled here.
pub type Aclitem = crate::c::text; // TODO(struct-forward)

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_namespace {
    pub oid: Oid,
    pub nspname: NameData,
    pub nspowner: Oid, // BKI_DEFAULT(POSTGRES) BKI_LOOKUP(pg_authid)
    // CATALOG_VARLEN (not in fixed part):
    pub nspacl: [Aclitem; 1], // aclitem[]
}

pub type Form_pg_namespace = *mut FormData_pg_namespace; // TODO(ptr)

// DECLARE_TOAST(pg_namespace, 4163, 4164)
// DECLARE_UNIQUE_INDEX(pg_namespace_nspname_index, 2684, NamespaceNameIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_namespace_oid_index, 2685, NamespaceOidIndexId, ...)
// MAKE_SYSCACHE(NAMESPACENAME, pg_namespace_nspname_index, 4)
// MAKE_SYSCACHE(NAMESPACEOID, pg_namespace_oid_index, 16)

pub fn NamespaceCreate(_nsp_name: &str, _owner_id: Oid, _is_temp: bool) -> Oid {
    unimplemented!()
}
