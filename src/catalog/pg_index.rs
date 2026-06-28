//! Translated from PostgreSQL src/include/catalog/pg_index.h

use bitflags::bitflags;

use crate::c::text;
use crate::postgres_ext::Oid;

pub const IndexRelationId: Oid = Oid(2610);

// int2vector / oidvector / pg_node_tree catalog fields are varlena; modeled as text.
pub type Int2vector = text;
pub type Oidvector = text;
pub type PgNodeTree = text;

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_index {
    pub indexrelid: Oid, // BKI_LOOKUP(pg_class)
    pub indrelid: Oid,   // BKI_LOOKUP(pg_class)
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    // variable-length, but direct access allowed:
    pub indkey: Int2vector, // BKI_FORCE_NOT_NULL
    // CATALOG_VARLEN (not in fixed part):
    pub indcollation: Oidvector, // BKI_LOOKUP_OPT(pg_collation)
    pub indclass: Oidvector,     // BKI_LOOKUP(pg_opclass)
    pub indoption: Int2vector,
    pub indexprs: PgNodeTree,
    pub indpred: PgNodeTree,
}

pub type Form_pg_index = *mut FormData_pg_index; // TODO(ptr)

// DECLARE_TOAST_WITH_MACRO(pg_index, 6351, 6352, PgIndexToastTable, PgIndexToastIndex)
// DECLARE_INDEX(pg_index_indrelid_index, 2678, IndexIndrelidIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_index_indexrelid_index, 2679, IndexRelidIndexId)
// MAKE_SYSCACHE(INDEXRELID, pg_index_indexrelid_index, 64)

/// pg_index_indrelid_index: (non-unique) index on indrelid.
pub const IndexIndrelidIndexId: Oid = Oid(2678);
/// pg_index_indexrelid_index: unique index on indexrelid (the pkey).
pub const IndexRelidIndexId: Oid = Oid(2679);
// DECLARE_ARRAY_FOREIGN_KEY_OPT((indrelid, indkey), pg_attribute, (attrelid, attnum))

// per-column indoption bits (packed in the on-disk indoption int2vector)
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IndOption: i16 {
        const DESC = 0x0001;
        const NULLS_FIRST = 0x0002;
    }
}
