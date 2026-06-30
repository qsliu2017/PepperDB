//! Dependency-aware object deletion. Translated from the M10-reachable parts of
//! `src/backend/catalog/dependency.c` (disposition: grow).
//!
//! `perform_deletion` is the DROP engine: collect the objects that depend on the
//! target (`find_dependent_objects`), apply CASCADE vs RESTRICT (RESTRICT errors if
//! any dependent exists; CASCADE includes them in the deletion set), then delete
//! each object (`delete_one_object`, dispatching by `classId` to the catalog's own
//! delete routine).
//!
//! How dependents are found: PG scans pg_depend. On M10 the create paths do not yet
//! record pg_depend rows (rules.md s4: dependency recording is staged in
//! `heap_create_with_catalog`), so the reachable dependent -- a table's indexes --
//! is discovered through the per-task catalog-index registry
//! (`relation_get_index_list`), the same source the executor + planner read. This
//! is the dependency edge DROP TABLE needs now; the pg_depend-scan walk grows when
//! the create paths record dependencies (M11+).
//!
//! Async coloring (rules.md s5): deletion reaches the buffer pool (catalog row
//! deletes, storage unlink), so the walk is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::RelationRelationId;
use crate::nodes::parsenodes::DropBehavior;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

pub use crate::catalog::dependency::{DependencyType, PerformDeletion};

/// Panic for a deletion path / object class not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `performDeletion`: delete one object and everything that depends on it,
/// honoring CASCADE vs RESTRICT. Builds the full deletion set
/// (`find_dependent_objects`), then deletes each member (`delete_one_object`).
pub async fn perform_deletion(
    shared: &Arc<SharedState>,
    object: &ObjectAddress,
    behavior: DropBehavior,
) {
    let mut targets = vec![*object];
    find_dependent_objects(shared, object, behavior, &mut targets).await;

    // Delete dependents before the object they depend on (indexes before their
    // heap), matching PG's delete order (deepest dependent first).
    for target in targets.iter().rev() {
        Box::pin(delete_one_object(shared, target)).await;
    }
}

/// PG `performMultipleDeletions`: delete a batch of independent objects, each with
/// its dependency closure. M10 routes one object at a time (the DROP statement loop
/// calls this per name); the shared-deletion-set optimization is staged.
pub async fn perform_multiple_deletions(
    shared: &Arc<SharedState>,
    objects: &[ObjectAddress],
    behavior: DropBehavior,
) {
    for object in objects {
        Box::pin(perform_deletion(shared, object, behavior)).await;
    }
}

/// PG `findDependentObjects` (the M10-reachable walk). For a relation, the
/// dependent objects are its indexes (from the catalog-index registry, the
/// rd_indexlist stand-in). RESTRICT errors if a dependent exists and the target was
/// the originally requested object; CASCADE appends the dependents to `targets`.
///
/// The full pg_depend-scan recursion (auto/internal/normal edges, the object
/// stack, sub-object handling) grows when the create paths record pg_depend rows.
#[allow(
    clippy::unused_async,
    reason = "TEMPORARY-TODO: awaits the pg_depend index scan once dependency recording lands (M11+)"
)]
async fn find_dependent_objects(
    shared: &Arc<SharedState>,
    object: &ObjectAddress,
    behavior: DropBehavior,
    targets: &mut Vec<ObjectAddress>,
) {
    if object.classId != RelationRelationId {
        // Non-relation objects have no tracked dependents on M10.
        return;
    }

    // A relation's indexes depend on it (DEPENDENCY_AUTO in PG: an index is always
    // dropped with its table regardless of RESTRICT). Collect them from the
    // registry; each is itself a relation object.
    let indexes = crate::backend::catalog::indexing::relation_get_index_list(object.objectId);
    for idx in indexes {
        let idx_addr = ObjectAddress {
            classId: RelationRelationId,
            objectId: idx.index.rd_id,
            objectSubId: 0,
        };
        // Auto dependents are deleted with the owner even under RESTRICT.
        if !targets.contains(&idx_addr) {
            targets.push(idx_addr);
        }
    }

    // RESTRICT vs CASCADE only matters for non-auto dependents; on M10 the only
    // tracked dependents are auto (indexes), so behavior does not change the set.
    // The branch is kept faithful for when normal dependents (views, FKs) land.
    let _ = behavior;
}

/// PG `deleteOneObject`: delete a single catalog object, dispatching by `classId`
/// to the catalog's own delete routine, then remove its pg_depend rows. M10
/// reaches relation objects (tables + indexes); the other classes STAGE.
async fn delete_one_object(shared: &Arc<SharedState>, object: &ObjectAddress) {
    // deleteDependencyRecordsFor would scan + delete this object's pg_depend rows;
    // the create paths do not yet write them, so there is nothing to delete (the
    // scan grows with dependency recording).
    delete_dependency_records_for(object);

    match object.classId {
        RelationRelationId => {
            crate::backend::commands::tablecmds::heap_drop_with_catalog(shared, object.objectId).await;
        }
        crate::catalog::pg_namespace::NamespaceRelationId => {
            crate::backend::commands::schemacmds::remove_schema_by_id(shared, object.objectId).await;
        }
        other => not_yet_reachable(&format!("deleteOneObject: classId {other:?}")),
    }
}

/// PG `deleteDependencyRecordsFor`: delete every pg_depend row whose `(classid,
/// objid)` is this object. STAGED: the create paths do not record pg_depend rows on
/// M10, so this is a no-op until dependency recording lands (then it scans pg_depend
/// by the (classid, objid) index and `catalog_tuple_delete`s each row).
fn delete_dependency_records_for(_object: &ObjectAddress) -> i64 {
    0
}

/// PG `recordDependencyOn`: record a single pg_depend edge. STAGED on M10 (the
/// create paths do not record dependencies yet); kept as the entry the create paths
/// call into once dependency recording lands.
pub fn record_dependency_on(
    _depender: &ObjectAddress,
    _referenced: &ObjectAddress,
    _behavior: DependencyType,
) {
    // STAGED (rules.md s4): write a pg_depend row. M10's reachable dependent (a
    // table's indexes) is tracked through the catalog-index registry instead.
}

/// PG `AcquireDeletionLock`: take an AccessExclusiveLock on the object before
/// deleting it. The single-backend M10 path holds the command-level lock; the
/// per-object lock acquisition grows with the lock manager's object-lock support.
pub fn acquire_deletion_lock(_object: &ObjectAddress, _flags: i32) {}

/// The OID of the relation about to be dropped (helper for the storage-unlink
/// path); re-exported so the heap-drop routine and the dependency walk agree on the
/// class predicate.
#[must_use]
pub const fn is_relation_class(class_id: Oid) -> bool {
    class_id.0 == RelationRelationId.0
}
