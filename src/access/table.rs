//! Translated from PostgreSQL src/include/access/table.h

use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use crate::utils::relcache::Relation;

pub fn table_open(_relationId: Oid, _lockmode: LockMode) -> Relation {
    unimplemented!()
}
pub fn table_openrv(_relation: &RangeVar, _lockmode: LockMode) -> Relation {
    unimplemented!()
}
/// missing_ok toggles the NULL sentinel into None.
pub fn table_openrv_extended(
    _relation: &RangeVar,
    _lockmode: LockMode,
    _missing_ok: bool,
) -> Option<Relation> {
    unimplemented!()
}
/// Returns None if the relation does not exist (NULL sentinel).
pub fn try_table_open(_relationId: Oid, _lockmode: LockMode) -> Option<Relation> {
    unimplemented!()
}
pub fn table_close(_relation: Relation, _lockmode: LockMode) {
    unimplemented!()
}
