//! Namespace + search-path name resolution. Translated from the M2-reachable
//! parts of `src/backend/catalog/namespace.c`.
//!
//! Resolves an unqualified relation/type name against the search path. M2 uses a
//! fixed search path of pg_catalog (11) then public (2200) -- the default with no
//! `search_path` GUC set; an explicit schema qualifier resolves directly. The
//! lookup is a heap scan of pg_class/pg_type filtered by (name, namespace), trying
//! each namespace in path order (PG `RelnameGetRelid` / `RangeVarGetRelid`).
//!
//! Async coloring (rules.md s5): the catalog scans reach the buffer pool, so the
//! resolution routines are `async` and thread `&Arc<SharedState>`.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: holds per-backend raw Relation/HeapTuple handles task-confined for the operation; same contract as relcache/genam"
)]

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::skey::ScanKeyData;
use crate::backend::access::common::heaptuple::{heap_freetuple, heap_getattr};
use crate::backend::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::backend::access::heap::heapam::SendPtr;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_namespace::{PG_CATALOG_NAMESPACE, PG_PUBLIC_NAMESPACE};
use crate::postgres::{Datum, DatumGetObjectId, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::relcache::Relation;

/// The M2 default search path: pg_catalog first, then public. PG computes this
/// from the `search_path` GUC (`fetchSearchPath`); M2 uses the default schemas
/// directly (no temp namespace, no GUC override).
#[must_use]
pub fn default_search_path() -> [Oid; 2] {
    [PG_CATALOG_NAMESPACE, PG_PUBLIC_NAMESPACE]
}

/// `RelnameGetRelid`: resolve an unqualified relation name against the search path,
/// returning its OID (or `None` if not found). Scans pg_class by (relname,
/// relnamespace) for each namespace in path order; the first match wins (PG's
/// search-path precedence).
pub async fn relname_get_relid(shared: &Arc<SharedState>, relname: &str) -> Option<Oid> {
    for nsp in default_search_path() {
        if let Some(oid) = relname_nsp_get_relid(shared, relname, nsp).await {
            return Some(oid);
        }
    }
    None
}

/// Resolve `relname` in a specific `namespace_id`, returning the relation OID.
pub async fn relname_nsp_get_relid(
    shared: &Arc<SharedState>,
    relname: &str,
    namespace_id: Oid,
) -> Option<Oid> {
    use crate::catalog::pg_class::{
        Anum_pg_class_oid, Anum_pg_class_relname, Anum_pg_class_relnamespace, RelationRelationId,
    };
    scan_name_nsp(
        shared,
        RelationRelationId,
        Anum_pg_class_relname,
        Anum_pg_class_relnamespace,
        Anum_pg_class_oid,
        relname,
        namespace_id,
    )
    .await
}

/// `TypenameGetTypid`: resolve an unqualified type name against the search path,
/// returning its OID. Scans pg_type by (typname, typnamespace). This is the lookup
/// `CREATE TABLE t(a int)` uses to turn `int4` into its type OID.
pub async fn typename_get_typid(shared: &Arc<SharedState>, typname: &str) -> Option<Oid> {
    for nsp in default_search_path() {
        if let Some(oid) = typename_nsp_get_typid(shared, typname, nsp).await {
            return Some(oid);
        }
    }
    None
}

/// Resolve `typname` in a specific namespace, returning the type OID.
pub async fn typename_nsp_get_typid(
    shared: &Arc<SharedState>,
    typname: &str,
    namespace_id: Oid,
) -> Option<Oid> {
    use crate::catalog::pg_type::{
        Anum_pg_type_oid, Anum_pg_type_typname, Anum_pg_type_typnamespace, TypeRelationId,
    };
    scan_name_nsp(
        shared,
        TypeRelationId,
        Anum_pg_type_typname,
        Anum_pg_type_typnamespace,
        Anum_pg_type_oid,
        typname,
        namespace_id,
    )
    .await
}

/// `get_namespace_oid` (M2 subset): resolve a schema name to its OID. M2 knows the
/// built-in schemas directly; an unknown name returns `None` (PG scans
/// pg_namespace, seeded by initdb).
#[must_use]
pub fn get_namespace_oid(nspname: &str, _missing_ok: bool) -> Option<Oid> {
    match nspname {
        "pg_catalog" => Some(PG_CATALOG_NAMESPACE),
        "public" => Some(PG_PUBLIC_NAMESPACE),
        "pg_toast" => Some(crate::catalog::pg_namespace::PG_TOAST_NAMESPACE),
        _ => None,
    }
}

/// `LookupExplicitNamespace`: resolve a schema qualifier to its OID.
#[must_use]
pub fn lookup_explicit_namespace(nspname: &str, missing_ok: bool) -> Option<Oid> {
    get_namespace_oid(nspname, missing_ok)
}

/// `RangeVarGetRelid` (M2 form): resolve a `RangeVar` (an optionally schema-
/// qualified relation reference) to its OID. A schema qualifier resolves in that
/// schema; an unqualified name searches the path. `None` if not found.
pub async fn range_var_get_relid(
    shared: &Arc<SharedState>,
    schemaname: Option<&str>,
    relname: &str,
) -> Option<Oid> {
    match schemaname {
        Some(schema) => {
            let nsp = lookup_explicit_namespace(schema, true)?;
            relname_nsp_get_relid(shared, relname, nsp).await
        }
        None => relname_get_relid(shared, relname).await,
    }
}

/// Heap-scan `catalog_relid` for a tuple whose `name_attno` equals `name` and whose
/// `nsp_attno` equals `namespace_id`, returning its `oid_attno` value.
async fn scan_name_nsp(
    shared: &Arc<SharedState>,
    catalog_relid: Oid,
    name_attno: i32,
    nsp_attno: i32,
    oid_attno: i32,
    name: &str,
    namespace_id: Oid,
) -> Option<Oid> {
    let catalog = SendPtr(relation_id_get_relation(catalog_relid)?);
    // SAFETY: live open catalog relation with a descriptor.
    let desc = unsafe { (*catalog.get()).rd_att.clone() }
        .unwrap_or_else(|| unreachable!("catalog has a descriptor"));

    // The M2 systable heap scan applies keys post-fetch; pass the namespace key
    // (by-value oid) so the scan filters on it, then match the name in the loop
    // (name equality on the fixed `name` type).
    let key = [ScanKeyData {
        flags: 0,
        attno: nsp_attno as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: zero_fmgr_info(),
        argument: ObjectIdGetDatum(namespace_id),
    }];
    let mut scan = systable_beginscan(shared, catalog.get(), InvalidOid, false, None, &key);

    let mut result = None;
    while let Some(tup) = systable_getnext(shared, &mut scan).await {
        // SAFETY: tup is a live owned tuple copy held by the scan.
        let tref: &HeapTupleData = unsafe { &*tup };
        if !tuple_name_eq(tref, &desc, name_attno, name) {
            continue;
        }
        // SAFETY: oid_attno is a valid by-value oid column.
        let (oid_d, isnull) = unsafe { heap_getattr(tref, oid_attno, &desc) };
        if !isnull {
            result = Some(DatumGetObjectId(oid_d));
            break;
        }
    }

    systable_endscan(shared, &mut scan);
    relation_close(catalog.get());
    result
}

/// Whether a tuple's `name`-typed column at `attno` equals `name`.
fn tuple_name_eq(
    tup: &HeapTupleData,
    desc: &crate::access::tupdesc::TupleDescData,
    attno: i32,
    name: &str,
) -> bool {
    // SAFETY: attno is a valid 1-based attribute number.
    let (val, isnull): (Datum, bool) = unsafe { heap_getattr(tup, attno, desc) };
    if isnull || val.0 == 0 {
        return false;
    }
    // SAFETY: a name Datum points at a NameData (NAMEDATALEN NUL-padded bytes).
    let nd = unsafe { &*(val.0 as *const crate::c::NameData) };
    let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
    nd.data[..end] == *name.as_bytes()
}

fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: core::ptr::null_mut(),
        expr: core::ptr::null_mut(),
    }
}
