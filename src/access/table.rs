//! Translated from PostgreSQL src/include/access/table.h
//!
//! M2 (step 12): the `async` table-open/close bodies live in
//! `crate::backend::access::table::table` (they delegate to the async
//! `relation_open` lock-wait leaf; rules.md s5). They are NOT re-exported under
//! these C names yet: existing sync callers (e.g. autovacuum.rs) still use the
//! pre-async stubs here, and will migrate to the async backend forms as their
//! files are async-converted. New M2 code calls
//! `crate::backend::access::table::table::{table_open, table_openrv, table_close}`
//! directly.

use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use std::sync::Arc;
use crate::utils::rel::RelationData;

pub fn table_open(_relationId: Oid, _lockmode: LockMode) -> Arc<RelationData> {
    unimplemented!()
}
pub fn table_openrv(_relation: &RangeVar, _lockmode: LockMode) -> Arc<RelationData> {
    unimplemented!()
}
/// missing_ok toggles the NULL sentinel into None.
pub fn table_openrv_extended(
    _relation: &RangeVar,
    _lockmode: LockMode,
    _missing_ok: bool,
) -> Option<Arc<RelationData>> {
    unimplemented!()
}
/// Returns None if the relation does not exist (NULL sentinel).
pub fn try_table_open(_relationId: Oid, _lockmode: LockMode) -> Option<Arc<RelationData>> {
    unimplemented!()
}
pub fn table_close(_relation: Arc<RelationData>, _lockmode: LockMode) {
    unimplemented!()
}
