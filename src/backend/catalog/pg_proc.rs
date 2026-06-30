//! pg_proc catalog manipulation. Translated from the step-39-relevant parts of
//! `src/backend/catalog/pg_proc.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `procedure_create` (form + insert
//! the pg_proc row for CREATE FUNCTION, with the OR REPLACE update path) lands in
//! PHASE B. The header (`src/catalog/pg_proc.rs`) keeps the full C-named
//! `ProcedureCreate` signature; this body is the reachable async entry that
//! `functioncmds::create_function` will call.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// PG `ProcedureCreate` (reachable async form): insert the pg_proc row for a new
/// function/procedure. PHASE B. `proc_namespace`/`return_type`/`prosrc` are the
/// resolved CREATE FUNCTION inputs; the full parameter machinery is filled in B.
pub async fn procedure_create(
    _shared: &Arc<SharedState>,
    _procedure_name: &str,
    _proc_namespace: Oid,
    _return_type: Oid,
    _prosrc: &str,
) -> ObjectAddress {
    unimplemented!("procedure_create: not yet translated (step 39 phase B)")
}
