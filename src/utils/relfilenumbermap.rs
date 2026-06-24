//! Translated from PostgreSQL src/include/utils/relfilenumbermap.h

use crate::common::relpath::RelFileNumber;
use crate::postgres_ext::Oid;

/// InvalidOid sentinel -> Option (not-found).
pub fn RelidByRelfilenumber(
    _reltablespace: Oid,
    _relfilenumber: RelFileNumber,
) -> Option<Oid> {
    unimplemented!()
}
