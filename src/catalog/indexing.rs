//! Translated from PostgreSQL src/include/catalog/indexing.h
//!
//! The bodies live in `crate::backend::catalog::indexing`; this header re-exports
//! them under their C names (rules.md s3). `CatalogIndexState` is the port's owned
//! open-indexes handle (PG aliases `ResultRelInfo`; here it carries the heap
//! relation + its open indexes directly).

pub use crate::backend::catalog::indexing::{
    catalog_close_indexes as CatalogCloseIndexes, catalog_open_indexes as CatalogOpenIndexes,
    catalog_tuple_insert as CatalogTupleInsert,
    catalog_tuple_insert_with_info as CatalogTupleInsertWithInfo,
    catalog_tuple_update as CatalogTupleUpdate, CatalogIndexState,
    MAX_CATALOG_MULTI_INSERT_BYTES,
};

// The DECLARE_*_INDEX/DECLARE_OID bootstrap macros in catalog/indexing.h are BKI
// metadata emitted by genbki; the per-catalog index OIDs are declared next to each
// catalog's Form struct (e.g. ClassOidIndexId in pg_class).
