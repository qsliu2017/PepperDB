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


use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::skey::ScanKeyData;
use crate::backend::access::common::heaptuple::{heap_freetuple, heap_getattr};
use crate::backend::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
};
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_namespace::{PG_CATALOG_NAMESPACE, PG_PUBLIC_NAMESPACE};
use crate::postgres::{Datum, DatumGetObjectId, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// The M2 default search path: pg_catalog first, then public. PG computes this
/// from the `search_path` GUC (`fetchSearchPath`); M2 uses the default schemas
/// directly (no GUC override). The backend's temp namespace is prepended by
/// [`active_search_path`].
#[must_use]
pub fn default_search_path() -> [Oid; 2] {
    [PG_CATALOG_NAMESPACE, PG_PUBLIC_NAMESPACE]
}

/// PG `recomputeNamespacePath` (subset): the active search path. The backend's
/// temp namespace -- if one has been created -- is implicitly FIRST, so temp
/// tables shadow permanent ones of the same name; then pg_catalog, then public.
#[must_use]
pub fn active_search_path() -> Vec<Oid> {
    let mut path = Vec::with_capacity(3);
    if let Some(session) = crate::session::try_current() {
        let temp = session.temp_namespace();
        if temp.is_valid() {
            path.push(temp);
        }
    }
    path.extend(default_search_path());
    path
}

/// PG `GetTempTableNamespace` / `AccessTempTableNamespace`: this backend's temp
/// namespace, creating it on first use (`InitTempTableNamespace`). The namespace
/// is named `pg_temp_<proc_pid>` (PG: `pg_temp_<MyProcNumber>`), owned by the
/// bootstrap superuser, and remembered per session. Staged vs namespace.c
/// (rules.md s4): the ACL_CREATE_TEMP permission check, the recovery / parallel-
/// worker guards, the `pg_toast_temp_N` companion (no TOAST yet), leftover
/// clean-out (`RemoveTempRelations` -- synthetic proc pids are unique per server
/// run, so a reused name only arises across restarts), and end-of-xact cleanup.
pub async fn get_temp_table_namespace(shared: &Arc<SharedState>) -> Oid {
    let session = crate::session::current();
    let existing = session.temp_namespace();
    if existing.is_valid() {
        return existing;
    }
    let namespace_name = format!("pg_temp_{}", session.proc_pid());
    let namespace_id = if let Some(oid) = namespace_oid_by_name(shared, &namespace_name).await {
        oid
    } else {
        let oid = crate::backend::catalog::pg_namespace::namespace_create(
            shared,
            &namespace_name,
            crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID,
            true,
        )
        .await;
        // Advance the command counter to make the namespace visible.
        crate::backend::access::transam::xact::CommandCounterIncrement();
        oid
    };
    session.set_temp_namespace(namespace_id);
    namespace_id
}

/// `RelnameGetRelid`: resolve an unqualified relation name against the search path,
/// returning its OID (or `None` if not found). Scans pg_class by (relname,
/// relnamespace) for each namespace in path order; the first match wins (PG's
/// search-path precedence -- the temp namespace shadows).
pub async fn relname_get_relid(shared: &Arc<SharedState>, relname: &str) -> Option<Oid> {
    for nsp in active_search_path() {
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
    for nsp in active_search_path() {
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

/// `get_namespace_oid` (M2 subset): resolve a built-in schema name to its OID by the
/// well-known OIDs. The on-disk pg_namespace scan (which also finds user-created
/// schemas) is [`namespace_oid_by_name`] (async); this sync form is kept for the
/// callers that only need the seeded built-ins (no buffer-pool access).
#[must_use]
pub fn get_namespace_oid(nspname: &str, _missing_ok: bool) -> Option<Oid> {
    match nspname {
        "pg_catalog" => Some(PG_CATALOG_NAMESPACE),
        "public" => Some(PG_PUBLIC_NAMESPACE),
        "pg_toast" => Some(crate::catalog::pg_namespace::PG_TOAST_NAMESPACE),
        _ => None,
    }
}

/// `get_namespace_oid` (on-disk form): resolve a schema name to its OID by scanning
/// pg_namespace (so user-created schemas resolve too). Falls back to the well-known
/// built-ins if the scan finds nothing (the cold-start pre-initdb path).
pub async fn namespace_oid_by_name(shared: &Arc<SharedState>, nspname: &str) -> Option<Oid> {
    use crate::catalog::pg_namespace::{
        Anum_pg_namespace_nspname, Anum_pg_namespace_oid, NamespaceRelationId,
    };
    if let Some(catalog) = relation_id_get_relation(NamespaceRelationId) {
        relation_close(catalog);
        if let Some(oid) = scan_namespace_by_name(
            shared,
            NamespaceRelationId,
            Anum_pg_namespace_nspname,
            Anum_pg_namespace_oid,
            nspname,
        )
        .await
        {
            return Some(oid);
        }
    }
    get_namespace_oid(nspname, true)
}

/// Heap-scan `catalog_relid` for a tuple whose `name_attno` (a `name` column) equals
/// `name`, returning its `oid_attno` value (no namespace key -- pg_namespace's name
/// is globally unique).
async fn scan_namespace_by_name(
    shared: &Arc<SharedState>,
    catalog_relid: Oid,
    name_attno: i32,
    oid_attno: i32,
    name: &str,
) -> Option<Oid> {
    let catalog = relation_id_get_relation(catalog_relid)?;
    let desc = catalog.rd_att.clone()
        .unwrap_or_else(|| unreachable!("catalog has a descriptor"));
    let snap = systable_scan_snapshot(shared, &catalog, None);
    let mut scan = systable_beginscan(shared, &catalog, InvalidOid, false, &snap, &[]);
    let mut result = None;
    while let Some(tref) = systable_getnext(shared, &mut scan).await {
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
    relation_close(catalog);
    result
}

/// `LookupExplicitNamespace`: resolve a schema qualifier to its OID.
#[must_use]
pub fn lookup_explicit_namespace(nspname: &str, missing_ok: bool) -> Option<Oid> {
    get_namespace_oid(nspname, missing_ok)
}

/// `RangeVarGetRelid` (M2 form): resolve a `RangeVar` (an optionally schema-
/// qualified relation reference) to its OID. A schema qualifier resolves in that
/// schema (`pg_temp` aliases this backend's temp namespace); an unqualified name
/// searches the path. `None` if not found.
pub async fn range_var_get_relid(
    shared: &Arc<SharedState>,
    schemaname: Option<&str>,
    relname: &str,
) -> Option<Oid> {
    match schemaname {
        // The pg_temp alias: only this backend's temp namespace qualifies, and
        // only once it exists (RangeVarGetRelidExtended's temp arm).
        Some("pg_temp") => {
            let session = crate::session::try_current()?;
            let temp = session.temp_namespace();
            if !temp.is_valid() {
                return None;
            }
            relname_nsp_get_relid(shared, relname, temp).await
        }
        Some(schema) => {
            // Resolve the schema on-disk so user-created schemas (CREATE SCHEMA)
            // qualify a relation reference, not just the seeded built-ins.
            let nsp = namespace_oid_by_name(shared, schema).await?;
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
    let catalog = relation_id_get_relation(catalog_relid)?;
    let desc = catalog.rd_att.clone()
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
    let snap = systable_scan_snapshot(shared, &catalog, None);
    let mut scan = systable_beginscan(shared, &catalog, InvalidOid, false, &snap, &key);

    let mut result = None;
    while let Some(tref) = systable_getnext(shared, &mut scan).await {
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
    relation_close(catalog);
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
        mcxt: (),
        expr: None,
    }
}
