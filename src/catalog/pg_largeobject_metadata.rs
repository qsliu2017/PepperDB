//! Translated from PostgreSQL src/include/catalog/pg_largeobject_metadata.h

use crate::postgres_ext::Oid;

pub const LargeObjectMetadataRelationId: Oid = Oid(2995);

#[repr(C)]
pub struct FormData_pg_largeobject_metadata {
    pub oid: Oid,
    pub lomowner: Oid, // BKI_LOOKUP(pg_authid)
    // CATALOG_VARLEN (not in fixed part):
    pub lomacl: [Aclitem; 1], // aclitem[1]; TODO(struct-forward)
}

// aclitem placeholder; real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

pub type Form_pg_largeobject_metadata = *mut FormData_pg_largeobject_metadata; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_largeobject_metadata_oid_index, 2996, LargeObjectMetadataOidIndexId)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_largeobject_metadata_oid: i32 = 1;
pub const Anum_pg_largeobject_metadata_lomowner: i32 = 2;
pub const Anum_pg_largeobject_metadata_lomacl: i32 = 3;
pub const Natts_pg_largeobject_metadata: i32 = 3;
