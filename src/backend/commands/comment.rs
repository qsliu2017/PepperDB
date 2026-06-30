//! COMMENT ON command. Translated from `src/backend/commands/comment.c`
//! (disposition: grow).
//!
//! Step 39 PHASE A wires the dispatch + grammar; `comment_object` (resolve the
//! object address, store/delete the pg_description row) lands in PHASE B.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CommentStmt;
use crate::shared_state::SharedState;

/// PG `CommentObject`: COMMENT ON <object> IS 'text'. PHASE B.
pub async fn comment_object(_shared: &Arc<SharedState>, _stmt: &CommentStmt) -> ObjectAddress {
    unimplemented!("comment_object: not yet translated (step 39 phase B)")
}
