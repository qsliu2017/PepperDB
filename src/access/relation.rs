//! Translated from PostgreSQL src/include/access/relation.h

use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use crate::utils::relcache::Relation;

pub fn relation_open(_relationId: Oid, _lockmode: LockMode) -> Relation {
    unimplemented!()
}
/// Returns None if the relation does not exist (NULL sentinel).
pub fn try_relation_open(_relationId: Oid, _lockmode: LockMode) -> Option<Relation> {
    unimplemented!()
}
pub fn relation_openrv(_relation: &RangeVar, _lockmode: LockMode) -> Relation {
    unimplemented!()
}
/// missing_ok toggles the NULL sentinel into None.
pub fn relation_openrv_extended(
    _relation: &RangeVar,
    _lockmode: LockMode,
    _missing_ok: bool,
) -> Option<Relation> {
    unimplemented!()
}
pub fn relation_close(_relation: Relation, _lockmode: LockMode) {
    unimplemented!()
}
