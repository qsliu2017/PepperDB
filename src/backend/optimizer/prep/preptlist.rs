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
        // INSERT/UPDATE/DELETE/MERGE have a result relation. M2 supports INSERT,
        // whose tlist was already keyed to target attnos by transformInsertStmt
        // (rewriteTargetListIU normally fills missing columns with defaults and
        // re-orders by attno; M2 assumes VALUES supplies every column in order, so
        // the tlist passes through). UPDATE/DELETE/MERGE expansion grows later.
        if command_type != CmdType::INSERT {
            not_yet_reachable("preprocess_targetlist: non-INSERT result relation");
        }
    } else {
        crate::assert!(command_type == CmdType::SELECT);
    }

    // INSERT targetlist expansion (expand_insert_targetlist) and UPDATE colno
    // extraction grow later. For SELECT and the M2 INSERT the tlist passes through.
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
