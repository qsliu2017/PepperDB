//! Translated from PostgreSQL src/include/catalog/pg_replication_origin.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const ReplicationOriginRelationId: Oid = Oid(6000); // BKI_SHARED_RELATION

#[repr(C)]
pub struct FormData_pg_replication_origin {
    pub roident: Oid,
    pub roname: text, // first varlen field, direct-accessible
}

pub type Form_pg_replication_origin = *mut FormData_pg_replication_origin; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_replication_origin_roident: i32 = 1;
pub const Anum_pg_replication_origin_roname: i32 = 2;
pub const Natts_pg_replication_origin: i32 = 2;

// DECLARE_UNIQUE_INDEX_PKEY(pg_replication_origin_roiident_index, 6001, ...)
// DECLARE_UNIQUE_INDEX(pg_replication_origin_roname_index, 6002, ...)
// MAKE_SYSCACHE(REPLORIGIDENT, ...); MAKE_SYSCACHE(REPLORIGNAME, ...)
