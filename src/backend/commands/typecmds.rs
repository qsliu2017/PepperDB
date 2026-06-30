//! Commands for CREATE TYPE / CREATE DOMAIN. Translated from
//! `src/backend/commands/typecmds.c` (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; the command bodies (composite /
//! enum / base-type DefineType, DefineDomain) land in PHASE B. Each entry point has
//! the real PG signature and fails loudly (rules.md s4) until then.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{CreateDomainStmt, DefineStmt};
use crate::shared_state::SharedState;

/// PG `DefineType`: CREATE TYPE (composite / enum / base type). PHASE B.
pub async fn define_type(_shared: &Arc<SharedState>, _stmt: &DefineStmt) -> ObjectAddress {
    unimplemented!("define_type: not yet translated (step 39 phase B)")
}

/// PG `DefineDomain`: CREATE DOMAIN. PHASE B.
pub async fn define_domain(_shared: &Arc<SharedState>, _stmt: &CreateDomainStmt) -> ObjectAddress {
    unimplemented!("define_domain: not yet translated (step 39 phase B)")
}

/// PG `DefineEnum`: the enum-label storage half of CREATE TYPE AS ENUM. PHASE B.
pub async fn define_enum(_shared: &Arc<SharedState>, _stmt: &DefineStmt) -> ObjectAddress {
    unimplemented!("define_enum: not yet translated (step 39 phase B)")
}
