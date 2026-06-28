//! Pre-planning target-list manipulation. Translated from
//! backend/optimizer/prep/preptlist.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::prep` under the C names.
//!
//! Disposition: `grow`. `preprocess_targetlist` expands/orders the query's
//! target list into `root->processed_tlist`. For a SELECT (M1's only command)
//! there is no result relation and nothing to expand: it simply adopts the
//! query's target list. The INSERT column-fill, UPDATE colno extraction,
//! UPDATE/DELETE/MERGE row-identity junk columns, and FOR UPDATE rowmark junk
//! columns all route through a single grow guard (rules.md s4) and grow with the
//! DML milestones.

use crate::nodes::nodes::CmdType;
use crate::nodes::pathnodes::PlannerInfo;

/// Panic for a target-list path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `preprocess_targetlist`: prepare the parse tree target list for planning,
/// storing the result in `root->processed_tlist`.
pub fn preprocess_targetlist(root: &mut PlannerInfo) {
    let command_type = root.parse.commandType;
    let result_relation = root.parse.resultRelation;

    if result_relation != 0 {
        // INSERT/UPDATE/DELETE/MERGE open the target relation and expand/renumber
        // the tlist; not reachable for an M1 SELECT.
        not_yet_reachable("preprocess_targetlist: result relation");
    }
    crate::assert!(command_type == CmdType::SELECT);

    // INSERT targetlist expansion and UPDATE colno extraction are gated on the
    // command type above. For SELECT the tlist passes through unchanged.
    let tlist = root.parse.targetList.clone();

    if command_type == CmdType::UPDATE
        || command_type == CmdType::DELETE
        || command_type == CmdType::MERGE
    {
        not_yet_reachable("preprocess_targetlist: row-identity junk columns");
    }

    if !root.row_marks.is_empty() {
        // FOR UPDATE/SHARE adds junk TID/whole-row columns; no rowmarks in M1.
        not_yet_reachable("preprocess_targetlist: rowmark junk columns");
    }

    root.processed_tlist = tlist;
}
