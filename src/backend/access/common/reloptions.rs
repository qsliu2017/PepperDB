//! Relation/tablespace option parsing. Translated from
//! backend/access/common/reloptions.c.
//!
//! STAGED: only the empty/default path is implemented -- enough for M2's
//! `CREATE TABLE t(a int)` with no (or empty) `WITH` clause. The full relopts
//! framework (the static `relOpts` registry built by `initialize_reloptions`,
//! `parseRelOptionsInternal`'s text[] decode, `allocateReloptStruct` /
//! `fillRelOptions`' bytea-offset writes into `StdRdOptions`, and the `add_*`
//! registration API) is deferred to a later milestone.
//!
//! Faithful empty-case behavior: for a relation created with no storage
//! parameters the reloptions Datum is NULL, and PG's per-relation accessors
//! (`RelationGetFillFactor`, `BrinGetPagesPerRange`, ...) fall back to built-in
//! defaults whenever `rd_options` is NULL. Returning `None` (NULL `bytea`) here
//! is therefore behaviorally identical to a defaults-filled `StdRdOptions` for
//! every consumer, without materializing the struct.

use crate::access::reloptions::relopt_kind;
use crate::c::{varlena, PointerIsValid};
use crate::catalog::pg_class::{
    RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use crate::postgres::{Datum, DatumGetPointer};

/// `transformRelOptions`: merge `def_list` into `old_options`. Only the empty
/// `def_list` path (no `WITH` clause) is implemented: per C, an empty list is a
/// no-op that returns `old_options` unchanged (a NULL Datum for a fresh CREATE).
/// A non-empty `def_list` needs the array-build machinery and is staged.
pub fn transform_rel_options(
    old_options: Datum,
    def_list: &[*mut core::ffi::c_void],
    _namspace: Option<&str>,
    _valid_nsps: &[&str],
    _accept_oids_off: bool,
    _is_reset: bool,
) -> Datum {
    // no change if empty list
    if def_list.is_empty() {
        return old_options;
    }
    unimplemented!("transformRelOptions with a non-empty WITH list is staged")
}

/// `default_reloptions`: parse the standard relation options into a
/// `StdRdOptions` bytea. STAGED to the empty case: a NULL options Datum (no
/// `WITH` clause) yields `None` (the relation uses built-in defaults). A
/// non-NULL Datum needs `parseRelOptions` + `fillRelOptions` and is staged.
pub fn default_reloptions(
    reloptions: Datum,
    _validate: bool,
    _kind: relopt_kind,
) -> Option<*mut varlena> {
    if PointerIsValid(DatumGetPointer(reloptions)) {
        unimplemented!("default_reloptions with non-empty options is staged (StdRdOptions framework)")
    }
    None
}

/// `heap_reloptions`: parse options for heaps, matviews and toast tables.
/// Dispatches on relkind exactly as C; with the empty options Datum every
/// supported relkind returns `None` (defaults), and unsupported relkinds always
/// return `None`.
pub fn heap_reloptions(relkind: u8, reloptions: Datum, validate: bool) -> Option<*mut varlena> {
    let relkind = relkind as i8;
    match relkind {
        RELKIND_TOASTVALUE => default_reloptions(reloptions, validate, relopt_kind::TOAST),
        RELKIND_RELATION | RELKIND_MATVIEW => {
            default_reloptions(reloptions, validate, relopt_kind::HEAP)
        }
        // other relkinds are not supported
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::PointerGetDatum;

    fn null_datum() -> Datum {
        PointerGetDatum(core::ptr::null())
    }

    #[test]
    fn empty_heap_reloptions_returns_none() {
        // CREATE TABLE t(a int) with no WITH: NULL options -> NULL reloptions.
        assert!(heap_reloptions(RELKIND_RELATION as u8, null_datum(), true).is_none());
        assert!(heap_reloptions(RELKIND_MATVIEW as u8, null_datum(), false).is_none());
        assert!(heap_reloptions(RELKIND_TOASTVALUE as u8, null_datum(), true).is_none());
    }

    #[test]
    fn unsupported_relkind_returns_none() {
        assert!(heap_reloptions(b'i', null_datum(), true).is_none());
    }

    #[test]
    fn default_reloptions_empty_is_none() {
        assert!(default_reloptions(null_datum(), true, relopt_kind::HEAP).is_none());
    }

    #[test]
    fn transform_rel_options_empty_list_is_noop() {
        let old = null_datum();
        let out = transform_rel_options(old, &[], None, &[], false, false);
        assert_eq!(DatumGetPointer(out), DatumGetPointer(old));
    }
}
