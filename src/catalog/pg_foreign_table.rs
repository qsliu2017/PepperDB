//! Translated from PostgreSQL src/include/catalog/pg_foreign_table.h

use crate::c::text;
use crate::postgres_ext::Oid;

pub const ForeignTableRelationId: Oid = Oid::new(3118);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
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

