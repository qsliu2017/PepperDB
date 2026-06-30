//! Generic DROP dispatch for non-relation objects. Translated from the
//! M10-reachable parts of `src/backend/commands/dropcmds.c` (disposition: full leaf
//! for the dispatch).
//!
//! `remove_objects` is the `DROP <objtype>` path for objects that do NOT go through
//! `RemoveRelations` (tables/indexes route there): resolve each named object via
//! `get_object_address`, then `performDeletion`. M10 reaches DROP of a TYPE / SCHEMA
//! (the object kinds `get_object_address` resolves now); the long tail (FUNCTION,
//! OPERATOR, CAST, ...) STAGES with its object-address support.
//!
//! Async coloring (rules.md s5): resolution + deletion reach the buffer pool, so the
//! dispatcher is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::catalog::dependency::PerformDeletion;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DropStmt;
use crate::shared_state::SharedState;

/// Panic for a DROP object kind not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `RemoveObjects`: the generic DROP dispatch for non-relation objects. Resolve
/// each named object to its `ObjectAddress` (`get_object_address`), collect them,
/// then `performMultipleDeletions` with the requested behavior. IF EXISTS on an
/// absent object emits a notice and skips it (PG `does not exist, skipping`).
pub async fn remove_objects(shared: &Arc<SharedState>, stmt: &DropStmt) {
    let mut addresses = Vec::with_capacity(stmt.objects.len());
    for obj in &stmt.objects {
        let Node::RangeVar(rv) = obj else {
            not_yet_reachable("RemoveObjects: object reference is not a name");
        };
        let addr = crate::backend::catalog::objectaddress::get_object_address(
            shared,
            stmt.removeType,
            rv,
            stmt.missing_ok,
        )
        .await;
        if addr.objectId == crate::postgres_ext::InvalidOid {
            // missing_ok resolved to "absent": emit the skip notice.
            let name = rv.relname.as_deref().unwrap_or("?");
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("object \"{name}\" does not exist, skipping"));
            });
            continue;
        }
        addresses.push(addr);
    }

    let _ = PerformDeletion::empty();
    crate::backend::catalog::dependency::perform_multiple_deletions(shared, &addresses, stmt.behavior)
        .await;
}
