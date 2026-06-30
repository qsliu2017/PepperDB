//! Privilege (GRANT/REVOKE) machinery. Translated from
//! `src/backend/catalog/aclchk.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `execute_grant_stmt` (resolve the
//! objects + grantees, compute the new ACL, update the object's pg_class.relacl /
//! pg_*-acl column) lands in PHASE B.

use std::sync::Arc;

use crate::nodes::parsenodes::GrantStmt;
use crate::shared_state::SharedState;

/// PG `ExecuteGrantStmt`: GRANT/REVOKE on objects. PHASE B.
pub async fn execute_grant_stmt(_shared: &Arc<SharedState>, _stmt: &GrantStmt) {
    unimplemented!("execute_grant_stmt: not yet translated (step 39 phase B)")
}
