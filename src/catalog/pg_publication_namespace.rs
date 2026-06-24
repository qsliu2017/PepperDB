//! Translated from PostgreSQL src/include/catalog/pg_publication_namespace.h

use crate::postgres_ext::Oid;

pub const PublicationNamespaceRelationId: Oid = Oid(6237);

#[repr(C)]
pub struct FormData_pg_publication_namespace {
    pub oid: Oid,
    pub pnpubid: Oid,
    pub pnnspid: Oid,
}

pub type Form_pg_publication_namespace = *mut FormData_pg_publication_namespace; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_publication_namespace_oid: i32 = 1;
pub const Anum_pg_publication_namespace_pnpubid: i32 = 2;
pub const Anum_pg_publication_namespace_pnnspid: i32 = 3;
pub const Natts_pg_publication_namespace: i32 = 3;

// DECLARE_UNIQUE_INDEX_PKEY(pg_publication_namespace_oid_index, 6238, ...)
// DECLARE_UNIQUE_INDEX(pg_publication_namespace_pnnspid_pnpubid_index, 6239, ...)
// MAKE_SYSCACHE(PUBLICATIONNAMESPACE, ...); MAKE_SYSCACHE(PUBLICATIONNAMESPACEMAP, ...)
