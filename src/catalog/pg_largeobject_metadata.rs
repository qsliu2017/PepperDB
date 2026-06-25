//! Translated from PostgreSQL src/include/catalog/pg_largeobject_metadata.h

use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

pub const LargeObjectMetadataRelationId: Oid = Oid(2995);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_largeobject_metadata {
    pub oid: Oid,
    pub lomowner: Oid, // BKI_LOOKUP(pg_authid)
    // CATALOG_VARLEN (not in fixed part):
    pub lomacl: [AclItem; 1], // aclitem[1]
}

pub type Form_pg_largeobject_metadata = *mut FormData_pg_largeobject_metadata; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_largeobject_metadata_oid_index, 2996, LargeObjectMetadataOidIndexId)

