//! Translated from PostgreSQL src/include/catalog/pg_partitioned_table.h

use crate::c::varlena;
use crate::postgres_ext::Oid;

pub const PartitionedRelationId: Oid = Oid::new(3350);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
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

// DECLARE_TOAST(pg_partitioned_table, 4165, 4166)
// DECLARE_UNIQUE_INDEX_PKEY(pg_partitioned_table_partrelid_index, 3351, PartitionedRelidIndexId, ...)
// MAKE_SYSCACHE(PARTRELID, ...)
// DECLARE_ARRAY_FOREIGN_KEY_OPT((partrelid, partattrs), pg_attribute, (attrelid, attnum))
