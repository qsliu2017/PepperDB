//! pg_namespace catalog manipulation. Translated from the M2-reachable parts of
//! `src/backend/catalog/pg_namespace.c`.
//!
//! M2 path: namespace LOOKUP (resolving pg_catalog / public for the search path)
//! is in `namespace.rs`; `NamespaceCreate` (CREATE SCHEMA) is staged -- pg_namespace
//! is seeded by the initdb pass, and the M2 search path uses the well-known
//! namespace OIDs directly.

use crate::postgres_ext::Oid;

/// `NamespaceCreate`: insert a new pg_namespace row. STAGED (rules.md s4): CREATE
/// SCHEMA is not on the M2 path; the built-in namespaces (pg_catalog/public) are
/// seeded by `bootstrap_catalogs`. A faithful create needs the pg_namespace heap
/// open + `CatalogTupleInsert`, which lands with the non-nailed-catalog initdb.
pub fn namespace_create(nsp_name: &str, owner_id: Oid, is_temp: bool) -> Oid {
    let _ = (nsp_name, owner_id, is_temp);
    unimplemented!("NamespaceCreate: CREATE SCHEMA is M10; pg_namespace is seeded by initdb")
}
