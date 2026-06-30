//! Translated from PostgreSQL src/include/catalog/pg_namespace.h

use crate::c::NameData;
use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

pub const NamespaceRelationId: Oid = Oid::new(2615);
/// pg_namespace composite rowtype OID (genbki-assigned). Used to nail the
/// descriptor at bootstrap (formrdesc); not load-bearing beyond the rowtype id.
pub const NamespaceRelation_Rowtype_Id: Oid = Oid::new(11632);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_namespace {
    pub oid: Oid,
    pub nspname: NameData,
    pub nspowner: Oid, // BKI_DEFAULT(POSTGRES) BKI_LOOKUP(pg_authid)
    // CATALOG_VARLEN (not in fixed part):
    pub nspacl: [AclItem; 1], // aclitem[]
}

pub type Form_pg_namespace = *mut FormData_pg_namespace; // TODO(ptr)

// DECLARE_TOAST(pg_namespace, 4163, 4164)
// DECLARE_UNIQUE_INDEX(pg_namespace_nspname_index, 2684, NamespaceNameIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_namespace_oid_index, 2685, NamespaceOidIndexId, ...)
// MAKE_SYSCACHE(NAMESPACENAME, pg_namespace_nspname_index, 4)
// MAKE_SYSCACHE(NAMESPACEOID, pg_namespace_oid_index, 16)

/// pg_namespace_nspname_index: unique index on nspname.
pub const NamespaceNameIndexId: Oid = Oid::new(2684);
/// pg_namespace_oid_index: unique index on oid (the pkey).
pub const NamespaceOidIndexId: Oid = Oid::new(2685);

// Well-known namespace OIDs (BKI seed OIDs from pg_namespace.dat).
/// pg_catalog: the system catalog schema.
pub const PG_CATALOG_NAMESPACE: Oid = Oid::new(11);
/// pg_toast: the reserved TOAST schema.
pub const PG_TOAST_NAMESPACE: Oid = Oid::new(99);
/// public: the standard public schema.
pub const PG_PUBLIC_NAMESPACE: Oid = Oid::new(2200);

pub use crate::backend::catalog::pg_namespace::namespace_create as NamespaceCreate;
