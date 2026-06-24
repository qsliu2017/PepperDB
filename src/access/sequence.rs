//! Translated from PostgreSQL src/include/access/sequence.h

use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use crate::utils::relcache::Relation;

pub fn sequence_open(_relationId: Oid, _lockmode: LockMode) -> Relation {
    unimplemented!()
}
pub fn sequence_close(_relation: Relation, _lockmode: LockMode) {
    unimplemented!()
}
