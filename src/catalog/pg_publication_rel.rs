//! Translated from PostgreSQL src/include/catalog/pg_publication_rel.h

use crate::c::varlena;
use crate::postgres_ext::Oid;

pub const PublicationRelRelationId: Oid = Oid(6106);

#[repr(C)]
pub struct FormData_pg_publication_rel {
    pub oid: Oid,
    pub prpubid: Oid,
    pub prrelid: Oid,
    // CATALOG_VARLEN (not in fixed part)
    pub prqual: varlena,  // pg_node_tree
    pub prattrs: varlena, // int2vector
}

pub type Form_pg_publication_rel = *mut FormData_pg_publication_rel; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_publication_rel_oid: i32 = 1;
pub const Anum_pg_publication_rel_prpubid: i32 = 2;
pub const Anum_pg_publication_rel_prrelid: i32 = 3;
pub const Anum_pg_publication_rel_prqual: i32 = 4;
pub const Anum_pg_publication_rel_prattrs: i32 = 5;
pub const Natts_pg_publication_rel: i32 = 5;

// DECLARE_TOAST(pg_publication_rel, 6228, 6229)
// DECLARE_UNIQUE_INDEX_PKEY(pg_publication_rel_oid_index, 6112, ...)
// DECLARE_UNIQUE_INDEX(pg_publication_rel_prrelid_prpubid_index, 6113, ...)
// DECLARE_INDEX(pg_publication_rel_prpubid_index, 6116, ...)
// MAKE_SYSCACHE(PUBLICATIONREL, ...); MAKE_SYSCACHE(PUBLICATIONRELMAP, ...)
