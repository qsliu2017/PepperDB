//! Translated from PostgreSQL src/include/catalog/pg_partitioned_table.h

use crate::c::varlena;
use crate::postgres_ext::Oid;

pub const PartitionedRelationId: Oid = Oid(3350);

#[repr(C)]
pub struct FormData_pg_partitioned_table {
    pub partrelid: Oid,
    pub partstrat: i8, // char
    pub partnatts: i16,
    pub partdefid: Oid,
    pub partattrs: varlena, // int2vector (first varlen field, direct-accessible)
    // CATALOG_VARLEN (not in fixed part)
    pub partclass: varlena,     // oidvector
    pub partcollation: varlena, // oidvector
    pub partexprs: varlena,     // pg_node_tree
}

pub type Form_pg_partitioned_table = *mut FormData_pg_partitioned_table; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_partitioned_table_partrelid: i32 = 1;
pub const Anum_pg_partitioned_table_partstrat: i32 = 2;
pub const Anum_pg_partitioned_table_partnatts: i32 = 3;
pub const Anum_pg_partitioned_table_partdefid: i32 = 4;
pub const Anum_pg_partitioned_table_partattrs: i32 = 5;
pub const Anum_pg_partitioned_table_partclass: i32 = 6;
pub const Anum_pg_partitioned_table_partcollation: i32 = 7;
pub const Anum_pg_partitioned_table_partexprs: i32 = 8;
pub const Natts_pg_partitioned_table: i32 = 8;

// DECLARE_TOAST(pg_partitioned_table, 4165, 4166)
// DECLARE_UNIQUE_INDEX_PKEY(pg_partitioned_table_partrelid_index, 3351, PartitionedRelidIndexId, ...)
// MAKE_SYSCACHE(PARTRELID, ...)
// DECLARE_ARRAY_FOREIGN_KEY_OPT((partrelid, partattrs), pg_attribute, (attrelid, attnum))
