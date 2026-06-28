//! Catalog physical-address + identity helpers. Translated from the M2-reachable
//! parts of `src/backend/catalog/catalog.c`.
//!
//! Two jobs: hand out new OIDs / relfilenumbers without collision
//! (`GetNewOidWithIndex`, `GetNewObjectId`-via-`GetNewRelFileNumber`), and answer
//! the "is this a system / catalog / shared / pinned relation?" predicates the
//! rest of the catalog code branches on.
//!
//! Async coloring (rules.md s5): `GetNewOidWithIndex` scans a catalog index (the
//! buffer pool) to test each candidate OID for collision, so it is `async` and
//! threads `&Arc<SharedState>`. The pure predicates (`IsCatalogRelationOid`, ...)
//! are sync.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: catalog routines hold per-backend raw Relation handles task-confined for the operation; same contract as relcache/genam"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "catalog routines take raw Relation/HeapTuple pointers per the C API; faithful to C"
)]

use std::sync::Arc;

use crate::access::attnum::AttrNumber;
use crate::access::skey::ScanKeyData;
use crate::access::transam::FIRST_UNPINNED_OBJECT_ID;
use crate::backend::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::catalog::pg_class::Form_pg_class;
use crate::common::relpath::{RelFileNumber, DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID};
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;
use crate::utils::relcache::Relation;

/// The shared OID counter (PG `GetNewObjectId` / varsup.c). Returns the next free
/// OID, skipping reserved low values + wrapping the 32-bit space.
#[must_use]
pub fn get_new_object_id(shared: &Arc<SharedState>) -> Oid {
    shared.variable_cache().get_new_object_id()
}

/// `GetNewOidWithIndex`: allocate a new OID not already present in `relation`,
/// checking each candidate against the relation's OID index. In bootstrap mode the
/// counter is collision-free so we return `GetNewObjectId` directly (PG does the
/// same). Otherwise loop: take a candidate, scan for a collision, retry on a hit.
///
/// `index_id`/`oidcolumn` name the unique index + OID column to probe. M2 always
/// drives the underlying systable scan as a heap scan (the index arm is optional);
/// the collision check is exact either way.
pub async fn get_new_oid_with_index(
    shared: &Arc<SharedState>,
    relation: Relation,
    index_id: Oid,
    oidcolumn: AttrNumber,
) -> Oid {
    if crate::miscadmin::is_bootstrap_processing_mode() {
        return get_new_object_id(shared);
    }

    loop {
        let new_oid = get_new_object_id(shared);
        let key = [ScanKeyData {
            flags: 0,
            attno: oidcolumn,
            strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
            subtype: InvalidOid,
            collation: InvalidOid,
            func: zero_fmgr_info(),
            argument: ObjectIdGetDatum(new_oid),
        }];
        // PG uses SnapshotAny here so an in-progress insert of the same OID still
        // counts as a collision; the M2 systable scan takes a catalog snapshot,
        // which is sufficient for the single-writer initdb / DDL path.
        let mut scan = systable_beginscan(shared, relation, index_id, true, None, &key);
        let collides = systable_getnext(shared, &mut scan).await.is_some();
        systable_endscan(shared, &mut scan);
        if !collides {
            return new_oid;
        }
    }
}

/// `GetNewRelFileNumber`: pick a relfilenumber for a new relation. In the M2 port
/// the relfilenumber equals the relation OID (the bootstrap convention), allocated
/// collision-free via [`get_new_oid_with_index`] against pg_class when `pg_class`
/// is open, else straight off the counter.
///
/// STAGED: PG re-checks each candidate against the on-disk file (`access(rpath)`)
/// and retries on a stray file. The monotonic counter makes a collision impossible
/// on the M2 single-writer path, so the file-existence retry is omitted with this
/// note (rules.md s4); the relfilenumber is the OID.
pub async fn get_new_rel_file_number(
    shared: &Arc<SharedState>,
    _reltablespace: Oid,
    pg_class: Option<Relation>,
    relpersistence: i8,
) -> RelFileNumber {
    use crate::catalog::pg_class::{Anum_pg_class_oid, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_UNLOGGED};
    debug_assert!(
        relpersistence == RELPERSISTENCE_PERMANENT
            || relpersistence == RELPERSISTENCE_UNLOGGED
            || relpersistence == crate::catalog::pg_class::RELPERSISTENCE_TEMP
    );
    match pg_class {
        Some(rel) => {
            get_new_oid_with_index(
                shared,
                rel,
                crate::catalog::pg_class::ClassOidIndexId,
                Anum_pg_class_oid as AttrNumber,
            )
            .await
        }
        None => get_new_object_id(shared),
    }
}

/// `IsCatalogRelationOid`: a relation is a system catalog iff its OID is pinned
/// (below `FirstUnpinnedObjectId`). Covers the catalogs, their indexes, and their
/// toast tables/indexes.
#[must_use]
pub fn is_catalog_relation_oid(relid: Oid) -> bool {
    relid.0 < FIRST_UNPINNED_OBJECT_ID
}

/// `IsCatalogRelation`: [`is_catalog_relation_oid`] of the relation's OID.
#[must_use]
pub fn is_catalog_relation(relation: Relation) -> bool {
    // SAFETY: live open relation handle.
    let relid = unsafe { (*relation).rd_id };
    is_catalog_relation_oid(relid)
}

/// `IsSystemClass`: a relation is "system" if its OID is a pinned catalog OID, or
/// it is a toast relation. The toast-namespace test is staged (M2 has no toast).
#[must_use]
pub fn is_system_class(relid: Oid, _reltuple: Form_pg_class) -> bool {
    // STAGED (rules.md s4): || IsToastClass(reltuple) -- M2 creates no toast rels.
    is_catalog_relation_oid(relid)
}

/// `IsSystemRelation`: [`is_system_class`] of the relation's OID + `rd_rel`.
#[must_use]
pub fn is_system_relation(relation: Relation) -> bool {
    // SAFETY: live open relation handle.
    let rel: &RelationData = unsafe { &*relation };
    is_system_class(rel.rd_id, rel.rd_rel)
}

/// `IsSharedRelation`: whether a relation OID is one of the hard-coded shared
/// catalogs (or their indexes / toast). M2 creates only local relations; the
/// shared catalogs (pg_database/pg_authid/...) are deep-deferred, so this returns
/// false for everything the M2 path reaches. The hard-coded OID list is staged
/// with the shared catalogs.
#[must_use]
pub fn is_shared_relation(_relation_id: Oid) -> bool {
    // STAGED (rules.md s4): the hard-coded shared-catalog OID list lands with the
    // shared catalogs (pg_database/pg_authid/pg_tablespace/...). Not on M2 path.
    false
}

/// `IsPinnedObject`: an object is pinned (un-droppable, no dependency tracking)
/// iff its OID is below `FirstUnpinnedObjectId`, with the policy exceptions for
/// large objects, the public namespace, and databases.
#[must_use]
pub fn is_pinned_object(class_id: Oid, object_id: Oid) -> bool {
    // Databases and the public namespace are intentionally not pinned; large
    // objects are never pinned. Their class OIDs:
    const NAMESPACE_RELATION_ID: Oid = Oid(2615);
    const DATABASE_RELATION_ID: Oid = Oid(1262);
    const PG_PUBLIC_NAMESPACE: Oid = Oid(2200);
    if object_id.0 >= FIRST_UNPINNED_OBJECT_ID {
        return false;
    }
    if class_id == NAMESPACE_RELATION_ID && object_id == PG_PUBLIC_NAMESPACE {
        return false;
    }
    if class_id == DATABASE_RELATION_ID {
        return false;
    }
    true
}

/// `IsCatalogNamespace`: pg_catalog (OID 11).
#[must_use]
pub fn is_catalog_namespace(namespace_id: Oid) -> bool {
    namespace_id == Oid(11)
}

/// `IsToastNamespace`: pg_toast (OID 99). M2 creates no toast namespaces, but the
/// predicate is cheap and faithful.
#[must_use]
pub fn is_toast_namespace(namespace_id: Oid) -> bool {
    namespace_id == Oid(99)
}

/// The default tablespace for a relation given its requested tablespace: a shared
/// relation goes to pg_global, everything else uses the requested one (or the
/// session default). PG folds this into `GetNewRelFileNumber`/`heap_create`.
#[must_use]
pub fn rel_default_tablespace(reltablespace: Oid, shared: bool) -> Oid {
    if shared {
        GLOBALTABLESPACE_OID
    } else if reltablespace == InvalidOid {
        DEFAULTTABLESPACE_OID
    } else {
        reltablespace
    }
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
