//! CREATE COLLATION command. Translated from
//! `src/backend/commands/collationcmds.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `define_collation` (the
//! pg_collation row) lands in PHASE B.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::DefineStmt;
use crate::shared_state::SharedState;

/// PG `DefineCollation`: CREATE COLLATION. PHASE B.
pub async fn define_collation(_shared: &Arc<SharedState>, _stmt: &DefineStmt) -> ObjectAddress {
    unimplemented!("define_collation: not yet translated (step 39 phase B)")
}
