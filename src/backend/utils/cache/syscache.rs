//! The system catalog cache layer over [`catcache`]. Translated from
//! `src/backend/utils/cache/syscache.c`.
//!
//! `syscache.c` is a thin shell over `catcache.c`: a static descriptor table
//! ([`cacheinfo`]) maps each [`SysCacheIdentifier`] to its catalog, index, and key
//! columns, and `SearchSysCacheN` forward to the corresponding catcache.
//!
//! Async coloring mirrors catcache: the public `SearchSysCacheN` are SYNC (a cache
//! HIT only); the async [`search_sys_cache_populate`] warms a cache by scanning.
//! The M2 path warms pg_type/pg_proc/pg_class/pg_attribute before the sync
//! `getTypeOutputInfo`/`TupleDescInitEntry` lookups run.
//!
//! [`catcache`]: crate::backend::utils::cache::catcache

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]
use std::sync::Arc;

use crate::access::attnum::AttrNumber;
use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::backend::access::common::heaptuple::heap_getattr;
use crate::backend::utils::cache::catcache::{
    release_cat_cache, search_cat_cache, search_cat_cache_populate,
};
use crate::postgres::{Datum, DatumGetObjectId};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::syscache::SysCacheIdentifier;

/// One entry of the syscache descriptor table (C `cachedesc`).
#[derive(Clone)]
pub struct CacheDesc {
    /// OID of the cached relation.
    pub reloid: Oid,
    /// OID of the supporting unique index (staged index-scan arm).
    pub indoid: Oid,
    /// Number of keys.
    pub nkeys: usize,
    /// 1-based attribute numbers of the key columns.
    pub key: [i16; 4],
    /// Number of hash buckets.
    pub nbuckets: usize,
}

const fn cd(reloid: Oid, key: [i16; 4], nkeys: usize, nbuckets: usize) -> CacheDesc {
    CacheDesc { reloid, indoid: InvalidOid, nkeys, key, nbuckets }
}

/// The syscache descriptor table (C `cacheinfo[]`), indexed by the
/// [`SysCacheIdentifier`] discriminant. Built lazily on first use.
///
/// Step 14 fully specifies the M2-reachable caches (pg_type/pg_proc/pg_class/
/// pg_attribute/pg_namespace/pg_am/pg_opclass/pg_amproc/pg_operator); the
/// remaining identifiers default to an unreached placeholder (their `reloid` is
/// `InvalidOid`, so a lookup is a guaranteed miss until they are wired). The
/// `indoid` is left invalid because the M2 scan path is the heap scan (the index
/// arm stages in step 13-rest).
#[must_use]
pub fn cacheinfo() -> Vec<CacheDesc> {
    use crate::catalog::pg_am::AccessMethodRelationId;
    use crate::catalog::pg_amproc::AccessMethodProcedureRelationId;
    use crate::catalog::pg_attribute::AttributeRelationId;
    use crate::catalog::pg_cast::CastRelationId;
    use crate::catalog::pg_class::RelationRelationId;
    use crate::catalog::pg_namespace::NamespaceRelationId;
    use crate::catalog::pg_opclass::OperatorClassRelationId;
    use crate::catalog::pg_operator::OperatorRelationId;
    use crate::catalog::pg_proc::ProcedureRelationId;
    use crate::catalog::pg_type::TypeRelationId;

    let mut v = vec![cd(InvalidOid, [0; 4], 0, 0); SYSCACHE_SIZE];
    let mut set = |id: SysCacheIdentifier, desc: CacheDesc| {
        v[id as usize] = desc;
    };

    // pg_type.oid -> the row (Anum_pg_type_oid == 1).
    set(SysCacheIdentifier::TYPEOID, cd(TypeRelationId, [1, 0, 0, 0], 1, 64));
    // pg_proc.oid (Anum_pg_proc_oid == 1).
    set(SysCacheIdentifier::PROCOID, cd(ProcedureRelationId, [1, 0, 0, 0], 1, 128));
    // pg_class.oid (Anum_pg_class_oid == 1).
    set(SysCacheIdentifier::RELOID, cd(RelationRelationId, [1, 0, 0, 0], 1, 128));
    // pg_attribute (attrelid, attnum) and (attrelid, attname).
    set(
        SysCacheIdentifier::ATTNUM,
        cd(AttributeRelationId, [att_relid(), att_num(), 0, 0], 2, 128),
    );
    set(
        SysCacheIdentifier::ATTNAME,
        cd(AttributeRelationId, [att_relid(), att_name(), 0, 0], 2, 32),
    );
    // pg_namespace.oid.
    set(SysCacheIdentifier::NAMESPACEOID, cd(NamespaceRelationId, [1, 0, 0, 0], 1, 16));
    // pg_am.oid.
    set(SysCacheIdentifier::AMOID, cd(AccessMethodRelationId, [1, 0, 0, 0], 1, 4));
    // pg_opclass.oid.
    set(SysCacheIdentifier::CLAOID, cd(OperatorClassRelationId, [1, 0, 0, 0], 1, 8));
    // pg_operator.oid.
    set(SysCacheIdentifier::OPEROID, cd(OperatorRelationId, [1, 0, 0, 0], 1, 32));
    // pg_operator (oprname, oprleft, oprright, oprnamespace) -- the operator
    // resolution key (OpernameGetOprid via SearchSysCache4(OPERNAMENSP)).
    {
        use crate::catalog::pg_operator as o;
        set(
            SysCacheIdentifier::OPERNAMENSP,
            cd(
                OperatorRelationId,
                [
                    o::Anum_pg_operator_oprname as i16,
                    o::Anum_pg_operator_oprleft as i16,
                    o::Anum_pg_operator_oprright as i16,
                    o::Anum_pg_operator_oprnamespace as i16,
                ],
                4,
                256,
            ),
        );
    }
    // pg_proc (proname, proargtypes, pronamespace) -- function resolution key
    // (func_get_detail via SearchSysCache3(PROCNAMEARGSNSP)). proargtypes is an
    // oidvector key (compared by value bytes; see catcache KeyKind::Oidvector).
    {
        use crate::catalog::pg_proc as p;
        set(
            SysCacheIdentifier::PROCNAMEARGSNSP,
            cd(
                ProcedureRelationId,
                [
                    p::Anum_pg_proc_proname as i16,
                    p::Anum_pg_proc_proargtypes as i16,
                    p::Anum_pg_proc_pronamespace as i16,
                    0,
                ],
                3,
                128,
            ),
        );
    }
    // pg_amproc (amprocfamily, amproclefttype, amprocrighttype, amprocnum).
    set(
        SysCacheIdentifier::AMPROCNUM,
        cd(
            AccessMethodProcedureRelationId,
            [amproc_family(), amproc_left(), amproc_right(), amproc_num()],
            4,
            16,
        ),
    );
    // M4 (step 23): pg_cast (castsource, casttarget) -- the cast-resolution key
    // (find_coercion_pathway via SearchSysCache2(CASTSOURCETARGET)).
    {
        use crate::catalog::pg_cast as c;
        set(
            SysCacheIdentifier::CASTSOURCETARGET,
            cd(
                CastRelationId,
                [c::Anum_pg_cast_castsource as i16, c::Anum_pg_cast_casttarget as i16, 0, 0],
                2,
                256,
            ),
        );
    }
    // M5 (step 25B): pg_aggregate (aggfnoid) -- nodeAgg's SearchSysCache1(AGGFNOID).
    set(SysCacheIdentifier::AGGFNOID, agg_fnoid_desc());
    // M4: pg_type (typname, typnamespace) -- the type-name resolution key
    // (typenameTypeId via SearchSysCache2(TYPENAMENSP)), used by the cast TypeName
    // resolution in the sync expression transform.
    {
        use crate::catalog::pg_type as t;
        set(
            SysCacheIdentifier::TYPENAMENSP,
            cd(
                TypeRelationId,
                [t::Anum_pg_type_typname as i16, t::Anum_pg_type_typnamespace as i16, 0, 0],
                2,
                64,
            ),
        );
    }

    v
}

// pg_attribute key attribute numbers (Anum_pg_attribute_*). attrelid is column 1,
// attname column 2, attnum column 6 in the catalog layout.
const fn att_relid() -> i16 {
    crate::catalog::pg_attribute::Anum_pg_attribute_attrelid as i16
}
const fn att_name() -> i16 {
    crate::catalog::pg_attribute::Anum_pg_attribute_attname as i16
}
const fn att_num() -> i16 {
    crate::catalog::pg_attribute::Anum_pg_attribute_attnum as i16
}
/// The AGGFNOID cache descriptor (pg_aggregate keyed by aggfnoid).
fn agg_fnoid_desc() -> CacheDesc {
    use crate::catalog::pg_aggregate::{Anum_pg_aggregate_aggfnoid, AggregateRelationId};
    cd(AggregateRelationId, [Anum_pg_aggregate_aggfnoid as i16, 0, 0, 0], 1, 16)
}
const fn amproc_family() -> i16 {
    crate::catalog::pg_amproc::Anum_pg_amproc_amprocfamily as i16
}
const fn amproc_left() -> i16 {
    crate::catalog::pg_amproc::Anum_pg_amproc_amproclefttype as i16
}
const fn amproc_right() -> i16 {
    crate::catalog::pg_amproc::Anum_pg_amproc_amprocrighttype as i16
}
const fn amproc_num() -> i16 {
    crate::catalog::pg_amproc::Anum_pg_amproc_amprocnum as i16
}

/// C `SysCacheSize`.
pub const SYSCACHE_SIZE: usize = crate::utils::syscache::SYSCACHE_SIZE;

// ---------------------------------------------------------------------------
// Sync search (hit-only) + async warm
// ---------------------------------------------------------------------------

/// SYNC `SearchSysCache1` (hit-only). Returns the held tuple on a HIT, `None` on a
/// negative hit or a cold miss. Warm via [`search_sys_cache1`] first.
#[must_use]
pub fn search_sys_cache(cache_id: SysCacheIdentifier, keys: &[Datum]) -> Option<HeapTuple> {
    search_cat_cache(cache_id as usize, keys)
}

/// ASYNC warm-and-search (C `SearchSysCacheN` whole, including the scan miss).
/// Tries a hit, then populates by scanning. Used by the bootstrap/executor and
/// the lsyscache async accessors.
pub async fn search_sys_cache_populate(
    shared: &Arc<SharedState>,
    cache_id: SysCacheIdentifier,
    keys: &[Datum],
) -> Option<HeapTuple> {
    if let Some(t) = search_cat_cache(cache_id as usize, keys) {
        return Some(t);
    }
    // Box::pin the (deep) scan/populate future to cap async stack-frame growth in
    // debug builds (the populate -> systable -> heap_getnext chain is large).
    Box::pin(search_cat_cache_populate(shared, cache_id as usize, keys)).await
}

/// `ReleaseSysCache`: drop the reference taken by a successful search.
pub fn release_sys_cache(tuple: HeapTuple) {
    release_cat_cache(tuple);
}

/// SYNC `SearchSysCacheExists` (hit-only).
#[must_use]
pub fn search_sys_cache_exists(cache_id: SysCacheIdentifier, keys: &[Datum]) -> bool {
    search_cat_cache(cache_id as usize, keys).is_some_and(|t| {
        release_cat_cache(t);
        true
    })
}

/// ASYNC exists (warm + test).
pub async fn search_sys_cache_exists_populate(
    shared: &Arc<SharedState>,
    cache_id: SysCacheIdentifier,
    keys: &[Datum],
) -> bool {
    search_sys_cache_populate(shared, cache_id, keys)
        .await
        .is_some_and(|t| {
            release_cat_cache(t);
            true
        })
}

/// SYNC `GetSysCacheOid` (hit-only): look up a row and return its `oidcol` value.
#[must_use]
pub fn get_sys_cache_oid(
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    keys: &[Datum],
) -> Option<Oid> {
    let tuple = search_cat_cache(cache_id as usize, keys)?;
    let oid = read_oid_attr(cache_id, tuple, oidcol);
    release_cat_cache(tuple);
    oid
}

/// ASYNC `GetSysCacheOid` (warm + read).
pub async fn get_sys_cache_oid_populate(
    shared: &Arc<SharedState>,
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    keys: &[Datum],
) -> Option<Oid> {
    let tuple = search_sys_cache_populate(shared, cache_id, keys).await?;
    let oid = read_oid_attr(cache_id, tuple, oidcol);
    release_cat_cache(tuple);
    oid
}

/// Read an Oid-typed attribute from a held syscache tuple, via the cache's
/// relation tuple descriptor.
fn read_oid_attr(cache_id: SysCacheIdentifier, tuple: HeapTuple, oidcol: AttrNumber) -> Option<Oid> {
    let reloid = cacheinfo()[cache_id as usize].reloid;
    let rel = crate::utils::relcache::RelationIdGetRelation(reloid)?;
    // RelationIdGetRelation bumps the refcount; balance it with RelationClose
    // after cloning the (Arc) descriptor.
    let td = rel.rd_att.clone();
    crate::utils::relcache::RelationClose(rel);
    let td = td?;
    let tref: &HeapTupleData = unsafe { &*tuple };
    let (v, isnull) = unsafe { heap_getattr(tref, i32::from(oidcol), &td) };
    if isnull {
        return None;
    }
    let oid = DatumGetObjectId(v);
    if oid == InvalidOid {
        None
    } else {
        Some(oid)
    }
}
