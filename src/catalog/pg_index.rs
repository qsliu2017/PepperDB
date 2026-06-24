//! Translated from PostgreSQL src/include/catalog/pg_index.h

use bitflags::bitflags;

use crate::c::text;
use crate::postgres_ext::Oid;

pub const IndexRelationId: Oid = Oid(2610);

// int2vector / oidvector / pg_node_tree catalog fields are varlena; modeled as text.
pub type Int2vector = text; // TODO(struct-forward)
pub type Oidvector = text; // TODO(struct-forward)
pub type PgNodeTree = text; // TODO(struct-forward)

#[repr(C)]
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
// DECLARE_ARRAY_FOREIGN_KEY_OPT((indrelid, indkey), pg_attribute, (attrelid, attnum))

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_index_indexrelid: i32 = 1;
pub const Anum_pg_index_indrelid: i32 = 2;
pub const Anum_pg_index_indnatts: i32 = 3;
pub const Anum_pg_index_indnkeyatts: i32 = 4;
pub const Anum_pg_index_indisunique: i32 = 5;
pub const Anum_pg_index_indnullsnotdistinct: i32 = 6;
pub const Anum_pg_index_indisprimary: i32 = 7;
pub const Anum_pg_index_indisexclusion: i32 = 8;
pub const Anum_pg_index_indimmediate: i32 = 9;
pub const Anum_pg_index_indisclustered: i32 = 10;
pub const Anum_pg_index_indisvalid: i32 = 11;
pub const Anum_pg_index_indcheckxmin: i32 = 12;
pub const Anum_pg_index_indisready: i32 = 13;
pub const Anum_pg_index_indislive: i32 = 14;
pub const Anum_pg_index_indisreplident: i32 = 15;
pub const Anum_pg_index_indkey: i32 = 16;
pub const Anum_pg_index_indcollation: i32 = 17;
pub const Anum_pg_index_indclass: i32 = 18;
pub const Anum_pg_index_indoption: i32 = 19;
pub const Anum_pg_index_indexprs: i32 = 20;
pub const Anum_pg_index_indpred: i32 = 21;
pub const Natts_pg_index: i32 = 21;

// per-column indoption bits (packed in the on-disk indoption int2vector)
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IndOption: i16 {
        const DESC = 0x0001;
        const NULLS_FIRST = 0x0002;
    }
}
