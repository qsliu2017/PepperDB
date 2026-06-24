//! Translated from PostgreSQL src/include/catalog/pg_foreign_table.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const ForeignTableRelationId: Oid = Oid(3118);

#[repr(C)]
pub struct FormData_pg_foreign_table {
    pub ftrelid: Oid,  // BKI_LOOKUP(pg_class)
    pub ftserver: Oid, // BKI_LOOKUP(pg_foreign_server)
    // CATALOG_VARLEN (not in fixed part):
    pub ftoptions: [text; 1],
}

pub type Form_pg_foreign_table = *mut FormData_pg_foreign_table; // TODO(ptr)

// DECLARE_TOAST(pg_foreign_table, 4153, 4154)
// DECLARE_UNIQUE_INDEX_PKEY(pg_foreign_table_relid_index, 3119, ForeignTableRelidIndexId)
// MAKE_SYSCACHE(FOREIGNTABLEREL, pg_foreign_table_relid_index, 4)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_foreign_table_ftrelid: i32 = 1;
pub const Anum_pg_foreign_table_ftserver: i32 = 2;
pub const Anum_pg_foreign_table_ftoptions: i32 = 3;
pub const Natts_pg_foreign_table: i32 = 3;
