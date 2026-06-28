//! Convenience catalog lookups over the syscache. Translated from the
//! M2-reachable accessors of `src/backend/utils/cache/lsyscache.c`.
//!
//! These are thin readers: `SearchSysCache1(...)` + `GETSTRUCT` to pull a few
//! columns out of a catalog row. Two flavors:
//!  - SYNC accessors (`get_type_output_info`, `get_typlenbyval`, ...) read an
//!    already-warm syscache entry (a cache HIT). They serve the sync callers
//!    (printtup/makefuncs/tupdesc). `get_type_output_info` additionally falls back
//!    to the builtin int2/4/8 output map when the catalog isn't warmed yet -- this
//!    is the M1 SELECT-1 path, where no pg_type heap exists. (This replaces the
//!    step-02 shim: when the catalog IS warm the value comes from real syscache;
//!    the builtin fallback only covers the catalog-less bootstrap window.)
//!  - ASYNC accessors warm the syscache (scan) then read, for callers in async
//!    context (executor, tests).

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: the catalog caches are PER-BACKEND task-confined state (raw HeapTuple/FmgrInfo pointers); their populate futures never migrate threads mid-await. await_holding_lock/refcell are clean (enforced)."
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "catalog-cache routines take raw Relation/HeapTuple pointers per the C API; the deref is faithful to C (callers pass live handles)"
)]
#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]
use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::backend::utils::cache::syscache::{
    release_sys_cache, search_sys_cache, search_sys_cache_populate,
};
use crate::catalog::pg_type::{Form_pg_type, FormData_pg_type};
use crate::postgres::{Datum, ObjectIdGetDatum};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::syscache::SysCacheIdentifier;

/// Read the `Form_pg_type` out of a held TYPEOID syscache tuple.
///
/// SAFETY: `tuple` is a live, held syscache tuple whose fixed part is a pg_type
/// row.
unsafe fn type_form(tuple: *mut HeapTupleData) -> &'static FormData_pg_type {
    let pt: Form_pg_type = GETSTRUCT(&*tuple).cast::<FormData_pg_type>();
    &*pt
}

// ---------------------------------------------------------------------------
// getTypeOutputInfo (sync, hit-or-builtin) + async variant
// ---------------------------------------------------------------------------

/// `getTypeOutputInfo` (SYNC): return `(typoutput, typIsVarlena)` for `type`.
/// Reads pg_type via a warm TYPEOID syscache HIT; if the catalog is not yet warmed
/// (M1 SELECT 1, catalog-less), falls back to the builtin int2/4/8 output map.
///
/// The step-02 shim is gone: when the syscache is warm this returns the REAL
/// `typoutput` from the pg_type row (so M2 type output works for any seeded type);
/// the builtin fallback covers only the bootstrap window before any pg_type heap
/// exists, which is exactly where M1's printtup runs.
#[must_use]
pub fn get_type_output_info(r#type: Oid) -> (Oid, bool) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(r#type)]) {
        // SAFETY: held syscache tuple.
        let pt = unsafe { type_form(tuple) };
        let out = (pt.typoutput, (!pt.typbyval) && pt.typlen == -1);
        release_sys_cache(tuple);
        return out;
    }
    builtin_type_output(r#type)
}

/// ASYNC `getTypeOutputInfo`: warm the TYPEOID cache then read. Use in async
/// context to guarantee the real catalog value.
pub async fn get_type_output_info_populate(shared: &Arc<SharedState>, r#type: Oid) -> (Oid, bool) {
    if let Some(tuple) =
        search_sys_cache_populate(shared, SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(r#type)]).await
    {
        // SAFETY: held syscache tuple.
        let pt = unsafe { type_form(tuple) };
        let out = (pt.typoutput, (!pt.typbyval) && pt.typlen == -1);
        release_sys_cache(tuple);
        return out;
    }
    builtin_type_output(r#type)
}

/// The builtin int2/4/8 output map (bootstrap-window fallback; the int types are
/// pass-by-value fixed-length, so `typIsVarlena` is false).
fn builtin_type_output(r#type: Oid) -> (Oid, bool) {
    use crate::catalog::genbki::{INT2OID, INT4OID, INT8OID};
    use crate::utils::fmgroids::{F_INT2OUT, F_INT4OUT, F_INT8OUT};
    let typoutput = match r#type {
        t if t == INT4OID => F_INT4OUT,
        t if t == INT2OID => F_INT2OUT,
        t if t == INT8OID => F_INT8OUT,
        _ => cache_lookup_failed(r#type),
    };
    (typoutput, false)
}

/// Raise PG's "cache lookup failed for type" error (used when a sync accessor hits
/// a cold cache for a non-builtin type). `elog!(ERROR, ..)` raises; the trailing
/// `unreachable!` gives the call site a diverging value.
fn cache_lookup_failed(typid: Oid) -> ! {
    crate::elog!(
        crate::utils::elog::ERROR,
        format!("cache lookup failed for type {} (syscache not warm)", typid.0)
    );
    unreachable!("elog!(ERROR) raises")
}

// ---------------------------------------------------------------------------
// get_typlenbyval / get_typlenbyvalalign (sync, hit) + async variants
// ---------------------------------------------------------------------------

/// `get_typlenbyval` (SYNC, hit): `(typlen, typbyval)` from a warm TYPEOID entry.
/// For the builtin int types (catalog not warm) returns the known layout.
#[must_use]
pub fn get_typlenbyval(typid: Oid) -> (i16, bool) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]) {
        // SAFETY: held syscache tuple.
        let pt = unsafe { type_form(tuple) };
        let out = (pt.typlen, pt.typbyval);
        release_sys_cache(tuple);
        return out;
    }
    match builtin_typlenbyvalalign(typid) {
        Some((l, b, _)) => (l, b),
        None => cache_lookup_failed(typid),
    }
}

/// `get_typlenbyvalalign` (SYNC, hit): `(typlen, typbyval, typalign)`.
#[must_use]
pub fn get_typlenbyvalalign(typid: Oid) -> (i16, bool, u8) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]) {
        // SAFETY: held syscache tuple.
        let pt = unsafe { type_form(tuple) };
        let out = (pt.typlen, pt.typbyval, pt.typalign as u8);
        release_sys_cache(tuple);
        return out;
    }
    builtin_typlenbyvalalign(typid).unwrap_or_else(|| cache_lookup_failed(typid))
}

/// ASYNC `get_typlenbyvalalign`: warm + read.
pub async fn get_typlenbyvalalign_populate(shared: &Arc<SharedState>, typid: Oid) -> (i16, bool, u8) {
    if let Some(tuple) =
        search_sys_cache_populate(shared, SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]).await
    {
        // SAFETY: held syscache tuple.
        let pt = unsafe { type_form(tuple) };
        let out = (pt.typlen, pt.typbyval, pt.typalign as u8);
        release_sys_cache(tuple);
        return out;
    }
    builtin_typlenbyvalalign(typid).unwrap_or_else(|| cache_lookup_failed(typid))
}

/// Builtin layout for the int2/4/8 types (bootstrap-window fallback).
fn builtin_typlenbyvalalign(typid: Oid) -> Option<(i16, bool, u8)> {
    use crate::catalog::genbki::{INT2OID, INT4OID, INT8OID};
    use crate::catalog::pg_type::{TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT};
    let r = match typid {
        t if t == INT2OID => (2, true, TYPALIGN_SHORT as u8),
        t if t == INT4OID => (4, true, TYPALIGN_INT as u8),
        t if t == INT8OID => (8, true, TYPALIGN_DOUBLE as u8),
        _ => return None,
    };
    Some(r)
}

// ---------------------------------------------------------------------------
// get_rel_name / get_rel_namespace (sync, via relcache) + async
// ---------------------------------------------------------------------------

/// `get_rel_name` (SYNC): the relation name, from a cached relcache entry.
#[must_use]
pub fn get_rel_name(relid: Oid) -> Option<String> {
    let rel = crate::utils::relcache::RelationIdGetRelation(relid)?;
    // SAFETY: cached relation with a valid rd_rel.
    let name = unsafe {
        let form = (*rel).rd_rel;
        if form.is_null() {
            None
        } else {
            Some(name_to_string(&(*form).relname))
        }
    };
    crate::utils::relcache::RelationClose(rel);
    name
}

/// `get_rel_namespace` (SYNC): the relation's namespace OID, or InvalidOid.
#[must_use]
pub fn get_rel_namespace(relid: Oid) -> Oid {
    crate::utils::relcache::RelationIdGetRelation(relid).map_or(
        crate::postgres_ext::InvalidOid,
        |rel| {
            // SAFETY: cached relation.
            let ns = unsafe {
                let form = (*rel).rd_rel;
                if form.is_null() {
                    crate::postgres_ext::InvalidOid
                } else {
                    (*form).relnamespace
                }
            };
            crate::utils::relcache::RelationClose(rel);
            ns
        },
    )
}

fn name_to_string(name: &crate::c::NameData) -> String {
    let end = name.data.iter().position(|&b| b == 0).unwrap_or(name.data.len());
    String::from_utf8_lossy(&name.data[..end]).into_owned()
}

// Keep Datum referenced (used by the public header re-exports' signatures).
const _: fn(Oid) -> Datum = ObjectIdGetDatum;
