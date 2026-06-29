//! Translated from PostgreSQL src/include/access/sequence.h

use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use std::sync::Arc;
use crate::utils::rel::RelationData;

pub fn sequence_open(_relationId: Oid, _lockmode: LockMode) -> Arc<RelationData> {
    unimplemented!()
}
pub fn sequence_close(_relation: &RelationData, _lockmode: LockMode) {
    unimplemented!()
}
