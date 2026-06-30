//! CREATE CONVERSION command. Translated from
//! `src/backend/commands/conversioncmds.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `create_conversion` (the
//! pg_conversion row) lands in PHASE B.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CreateConversionStmt;
use crate::shared_state::SharedState;

/// PG `CreateConversionCommand`: CREATE CONVERSION. PHASE B.
pub async fn create_conversion(
    _shared: &Arc<SharedState>,
    _stmt: &CreateConversionStmt,
) -> ObjectAddress {
    unimplemented!("create_conversion: not yet translated (step 39 phase B)")
}
