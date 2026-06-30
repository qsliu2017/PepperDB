//! CREATE/DROP DATABASE commands. Translated from
//! `src/backend/commands/dbcommands.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `createdb` / `dropdb` (the
//! pg_database row + the per-database storage clone/unlink) land in PHASE B.

use std::sync::Arc;

use crate::nodes::parsenodes::{CreatedbStmt, DropdbStmt};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// PG `createdb`: CREATE DATABASE. PHASE B. Returns the new database OID.
pub async fn createdb(_shared: &Arc<SharedState>, _stmt: &CreatedbStmt) -> Oid {
    unimplemented!("createdb: not yet translated (step 39 phase B)")
}

/// PG `dropdb`: DROP DATABASE. PHASE B.
pub async fn dropdb(_shared: &Arc<SharedState>, _stmt: &DropdbStmt) {
    unimplemented!("dropdb: not yet translated (step 39 phase B)")
}
