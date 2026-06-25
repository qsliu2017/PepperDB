//! Translated from PostgreSQL src/include/catalog/pg_transform.h

use crate::c::regproc;
use crate::postgres_ext::Oid;

pub const TransformRelationId: Oid = Oid(3576);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_transform {
    pub oid: Oid,
    pub trftype: Oid,
    pub trflang: Oid,
    pub trffromsql: regproc,
    pub trftosql: regproc,
}

pub type Form_pg_transform = *mut FormData_pg_transform; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_transform_oid_index, 3574, TransformOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_transform_type_lang_index, 3575, TransformTypeLangIndexId, ...)
// MAKE_SYSCACHE(TRFOID, ...); MAKE_SYSCACHE(TRFTYPELANG, ...)
