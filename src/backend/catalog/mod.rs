//! Catalog manipulation: the `.c` bodies for `src/backend/catalog/`.
//!
//! Step 15 (plan 003): the universal catalog-row insert (`indexing`), OID and
//! relfilenumber assignment + the system/catalog predicates (`catalog`), the
//! table-creation orchestrator (`heap`), the index create/build path (`index`),
//! the rowtype create (`pg_type`), and unqualified-name resolution
//! (`namespace`). Together with the completed `bootstrap_catalogs` (initdb) these
//! let `CREATE TABLE t(a int)` look up `int4` by name and write the new table's
//! pg_class/pg_attribute/pg_type rows + build the catalog unique indexes.

pub mod catalog;
pub mod dependency;
pub mod heap;
pub mod index;
pub mod indexing;
pub mod namespace;
pub mod objectaddress;
pub mod pg_namespace;
pub mod pg_type;

#[cfg(test)]
mod tests;
