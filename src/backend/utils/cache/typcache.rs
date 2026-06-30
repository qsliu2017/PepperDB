//! Type cache. Translated from the step-39-relevant parts of
//! `src/backend/utils/cache/typcache.c` (disposition: grow).
//!
//! `lookup_type_cache` builds (and caches per task) a [`TypeCacheEntry`] holding the
//! reachable per-type metadata: the physical layout (typlen/byval/align/storage),
//! the type's kind (typtype) + rowtype OID, the composite tuple descriptor for
//! `typtype = 'c'`, and the domain base type/typmod for `'d'`. The composite-type
//! DDL + the executor's composite ops reach this.
//!
//! Async coloring (rules.md s5): populating an entry reads pg_type via the syscache
//! and (for composites) opens the rowtype relation, both of which reach the buffer
//! pool -- so this entry is `async` and threads `&Arc<SharedState>`, unlike the C
//! sync `lookup_type_cache` (which relies on already-warm caches).
//!
//! STAGED (rules.md s4): the operator-class resolution (btree/hash opfamily + the
//! eq/lt/gt/cmp/hash operators -- those fields stay `InvalidOid` until requested via
//! a warmed pg_opclass), range/multirange info, the domain *constraint* set
//! (`domain_data`), enum data, and the cross-task shared cache. The reachable fields
//! are the layout + composite tupdesc + domain base, which is what the DDL needs.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::access::tupdesc::TupleDesc;
use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache_populate};
use crate::utils::syscache::SysCacheIdentifier;
use crate::catalog::pg_type::{FormData_pg_type, Form_pg_type, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN};
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::typcache::TypeCacheFlags;

/// The reachable subset of PG's `TypeCacheEntry`: an owned snapshot of one type's
/// cached metadata. The opfamily/operator/range/enum/constraint fields stage
/// (rules.md s4); the layout + composite tupdesc + domain base are populated.
#[derive(Clone)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typstorage: u8,
    pub typtype: i8,
    pub typrelid: Oid,
    pub typelem: Oid,
    pub typcollation: Oid,
    /// Composite rowtype descriptor (`typtype = 'c'`), else `None`.
    pub tup_desc: Option<TupleDesc>,
    /// Domain base type + typmod (`typtype = 'd'`), else `(InvalidOid, -1)`.
    pub domain_base_type: Oid,
    pub domain_base_typmod: i32,
}

/// PG `lookup_type_cache` (reachable async form): fetch the cached metadata for
/// `type_id`. `flags` requests optional fields (the composite tupdesc, the domain
/// base info); the layout fields are always filled. Per-task cache via the
/// task-local map (mirrors the relcache/catcache per-task pattern); a hit returns a
/// clone of the cached entry.
pub async fn lookup_type_cache(
    shared: &Arc<SharedState>,
    type_id: Oid,
    flags: TypeCacheFlags,
) -> TypeCacheEntry {
    if let Some(entry) = cache_get(type_id, flags) {
        return entry;
    }

    let tuple = search_sys_cache_populate(shared, SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(type_id)])
        .await
        .unwrap_or_else(|| {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("cache lookup failed for type {}", type_id.get())
            );
            unreachable!("elog!(ERROR) raises");
        });

    // Read the fixed pg_type fields out of the held tuple before releasing it.
    let (typlen, typbyval, typalign, typstorage, typtype, typrelid, typelem, typcollation,
         typbasetype, typtypmod) = {
        // SAFETY: held TYPEOID hit -> a pg_type row; borrow ends before release.
        let pt = unsafe { type_form(&*tuple) };
        (pt.typlen, pt.typbyval, pt.typalign as u8, pt.typstorage as u8, pt.typtype,
         pt.typrelid, pt.typelem, pt.typcollation, pt.typbasetype, pt.typtypmod)
    };
    release_sys_cache(tuple);

    // Composite tupdesc: open the rowtype relation and clone its descriptor.
    let tup_desc = if typtype == TYPTYPE_COMPOSITE && typrelid.is_valid() {
        crate::backend::utils::cache::relcache::relation_build_desc(shared, typrelid)
            .await
            .and_then(|rel| rel.rd_att.clone())
    } else {
        None
    };

    let (domain_base_type, domain_base_typmod) = if typtype == TYPTYPE_DOMAIN {
        (typbasetype, typtypmod)
    } else {
        (InvalidOid, -1)
    };

    let entry = TypeCacheEntry {
        type_id,
        typlen,
        typbyval,
        typalign,
        typstorage,
        typtype,
        typrelid,
        typelem,
        typcollation,
        tup_desc,
        domain_base_type,
        domain_base_typmod,
    };
    cache_put(entry.clone());
    let _ = flags; // every reachable field is populated unconditionally on M10.
    entry
}

/// Read the `Form_pg_type` out of a held TYPEOID syscache tuple (borrow rooted in
/// the tuple, rule 10).
///
/// SAFETY: `tuple`'s fixed part is a pg_type row (a held TYPEOID syscache hit).
unsafe fn type_form(tuple: &HeapTupleData) -> &FormData_pg_type {
    let pt: Form_pg_type = GETSTRUCT(tuple).cast::<FormData_pg_type>();
    // SAFETY: `pt` points into `tuple`'s body; the borrow is tied to `tuple`.
    unsafe { &*pt }
}

// ---------------------------------------------------------------------------
// Per-task type cache. Mirrors the relcache/catcache per-task scope: a
// task-local map keyed by type OID, valid for the life of the backend task. The
// cross-task shared registry (PG's process-global TypeCacheHash) stages.
// ---------------------------------------------------------------------------

tokio::task_local! {
    static TYPE_CACHE: std::cell::RefCell<std::collections::HashMap<Oid, TypeCacheEntry>>;
}

/// Run `fut` with a fresh per-task type cache in scope. (Tests + the backend task
/// wrap their work in this; outside a scope, lookups simply always miss.)
pub async fn scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    TYPE_CACHE
        .scope(std::cell::RefCell::new(std::collections::HashMap::new()), fut)
        .await
}

fn cache_get(type_id: Oid, _flags: TypeCacheFlags) -> Option<TypeCacheEntry> {
    TYPE_CACHE.try_with(|c| c.borrow().get(&type_id).cloned()).ok().flatten()
}

fn cache_put(entry: TypeCacheEntry) {
    let _ = TYPE_CACHE.try_with(|c| c.borrow_mut().insert(entry.type_id, entry));
}
