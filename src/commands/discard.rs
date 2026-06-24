//! Translated from PostgreSQL src/include/commands/discard.h

use crate::nodes::parsenodes::DiscardStmt;

// DiscardStmt and DiscardMode (DISCARD_*) are defined in nodes::parsenodes.
pub fn DiscardCommand(stmt: &DiscardStmt, is_top_level: bool) {
    unimplemented!()
}
