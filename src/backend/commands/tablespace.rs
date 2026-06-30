//! CREATE TABLESPACE command. Translated from
//! `src/backend/commands/tablespace.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `create_tablespace` (the
//! pg_tablespace row + the symlink/directory creation) lands in PHASE B.

use std::sync::Arc;

use crate::nodes::parsenodes::CreateTableSpaceStmt;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// PG `CreateTableSpace`: CREATE TABLESPACE. PHASE B. Returns the new tablespace OID.
pub async fn create_tablespace(_shared: &Arc<SharedState>, _stmt: &CreateTableSpaceStmt) -> Oid {
    unimplemented!("create_tablespace: not yet translated (step 39 phase B)")
}
