//! Commands for CREATE FUNCTION / PROCEDURE. Translated from
//! `src/backend/commands/functioncmds.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `create_function` (parameter
//! interpretation, language/body option handling, the pg_proc row via
//! `ProcedureCreate`) lands in PHASE B.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CreateFunctionStmt;
use crate::shared_state::SharedState;

/// PG `CreateFunction`: CREATE [OR REPLACE] FUNCTION/PROCEDURE. PHASE B.
pub async fn create_function(
    _shared: &Arc<SharedState>,
    _stmt: &CreateFunctionStmt,
) -> ObjectAddress {
    unimplemented!("create_function: not yet translated (step 39 phase B)")
}
