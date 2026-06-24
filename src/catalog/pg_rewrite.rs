//! Translated from PostgreSQL src/include/catalog/pg_rewrite.h

use crate::c::{NameData, varlena};
use crate::postgres_ext::Oid;

pub const RewriteRelationId: Oid = Oid(2618);

#[repr(C)]
pub struct FormData_pg_rewrite {
    pub oid: Oid,
    pub rulename: NameData,
    pub ev_class: Oid,
    pub ev_type: i8,    // char
    pub ev_enabled: i8, // char
    pub is_instead: bool,
    // CATALOG_VARLEN (not in fixed part)
    pub ev_qual: varlena,   // pg_node_tree
    pub ev_action: varlena, // pg_node_tree
}

pub type Form_pg_rewrite = *mut FormData_pg_rewrite; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_rewrite_oid: i32 = 1;
pub const Anum_pg_rewrite_rulename: i32 = 2;
pub const Anum_pg_rewrite_ev_class: i32 = 3;
pub const Anum_pg_rewrite_ev_type: i32 = 4;
pub const Anum_pg_rewrite_ev_enabled: i32 = 5;
pub const Anum_pg_rewrite_is_instead: i32 = 6;
pub const Anum_pg_rewrite_ev_qual: i32 = 7;
pub const Anum_pg_rewrite_ev_action: i32 = 8;
pub const Natts_pg_rewrite: i32 = 8;

// DECLARE_TOAST(pg_rewrite, 2838, 2839)
// DECLARE_UNIQUE_INDEX_PKEY(pg_rewrite_oid_index, 2692, RewriteOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_rewrite_rel_rulename_index, 2693, ...)
// MAKE_SYSCACHE(RULERELNAME, ...)
