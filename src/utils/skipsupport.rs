//! Translated from PostgreSQL src/include/utils/skipsupport.h
//! Support routines for B-Tree skip scan.

use crate::postgres::Datum;
use crate::utils::relcache::Relation;

/// C: `typedef struct SkipSupportData *SkipSupport;` nullable handle.
pub type SkipSupport<'a> = Option<&'a mut SkipSupportData>;

/// Decrement/increment callback: returns a copy of `existing`, sets overflow
/// when it already matches low_elem/high_elem. C out-param `bool *overflow` is
/// folded into the return as `(Datum, overflow)`.
pub type SkipSupportIncDec = fn(rel: Relation, existing: Datum) -> (Datum, bool);

/// State/callbacks used by skip arrays to procedurally generate elements.
/// A BTSKIPSUPPORT_PROC function must set every field (no optional fields).
pub struct SkipSupportData {
    /// lowest sorting/leftmost non-NULL value
    pub low_elem: Datum,
    /// highest sorting/rightmost non-NULL value
    pub high_elem: Datum,
    pub decrement: SkipSupportIncDec,
    pub increment: SkipSupportIncDec,
}

use crate::postgres_ext::Oid;

pub fn PrepareSkipSupportFromOpclass(
    _opfamily: Oid,
    _opcintype: Oid,
    _reverse: bool,
) -> SkipSupport<'static> {
    unimplemented!()
}
