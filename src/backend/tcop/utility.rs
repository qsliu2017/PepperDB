//! Process utility commands (anything that is not an optimizable statement).
//! Translated from the M2-reachable parts of `src/backend/tcop/utility.c`
//! (disposition: grow).
//!
//! `ProcessUtility` is the dispatcher for utility statements carried by a
//! `CMD_UTILITY` `PlannedStmt`. M2 fills the `T_CreateStmt` arm: run
//! `transformCreateStmt` then `DefineRelation`. All other statement tags are clean
//! grow guards (rules.md s4). `CreateCommandTag` returns the completion tag from
//! the raw parse node (`CREATE TABLE` for a `CreateStmt`).
//!
//! Async coloring (rules.md s5): `DefineRelation` -> `heap_create_with_catalog`
//! reaches the buffer pool + WAL, so `ProcessUtility`/`standard_ProcessUtility`
//! and the `ProcessUtilitySlow` create path are `async` and thread
//! `&Arc<SharedState>`. M2 drops the `ProcessUtility_hook` (plugin entry) and the
//! `params`/`query_env`/event-trigger plumbing until those subsystems land.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: ProcessUtility holds the per-backend &mut DestReceiver task-confined across the catalog-create await; the receiver is single-backend state (same contract as the portal path), never sent across tasks"
)]

use std::sync::Arc;

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::plannodes::PlannedStmt;
use crate::shared_state::SharedState;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::CommandTag;
use crate::tcop::dest::DestReceiver;
use crate::tcop::utility::ProcessUtilityContext;

/// Panic for a utility statement / sub-path not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `ProcessUtility`: the thin wrapper (the plugin `ProcessUtility_hook` is
/// dropped for M2). Delegates to `standard_ProcessUtility`.
pub async fn process_utility(
    shared: &Arc<SharedState>,
    pstmt: &PlannedStmt,
    query_string: &str,
    context: ProcessUtilityContext,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    crate::assert!(pstmt.command_type == CmdType::UTILITY);
    standard_process_utility(shared, pstmt, query_string, context, dest, qc).await;
}

/// PG `standard_ProcessUtility`: the utility dispatcher. M2 routes the
/// catalog-creating statements through the "slow" path; the fast in-utility.c arms
/// (transaction control, LISTEN/NOTIFY, ...) grow at their milestones.
pub async fn standard_process_utility(
    shared: &Arc<SharedState>,
    pstmt: &PlannedStmt,
    query_string: &str,
    context: ProcessUtilityContext,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    let parsetree = pstmt
        .utility_stmt
        .as_ref()
        .unwrap_or_else(|| unreachable!("a CMD_UTILITY PlannedStmt carries its utilityStmt"));

    match parsetree {
        Node::CreateStmt(_) => {
            process_utility_slow(shared, pstmt, parsetree, query_string, context, dest, qc).await;
        }
        other => not_yet_reachable(&format!("standard_ProcessUtility: {other:?}")),
    }
}

/// PG `ProcessUtilitySlow` (the `T_CreateStmt` arm): parse-analyze the raw
/// `CreateStmt` (`transformCreateStmt`) and create each resulting relation
/// (`DefineRelation`). M2's transform yields exactly the one `CreateStmt`; the
/// toast-table / event-trigger / LIKE-expansion / sub-statement recursion grow
/// with their features. A `CommandCounterIncrement` separates successive commands.
async fn process_utility_slow(
    shared: &Arc<SharedState>,
    _pstmt: &PlannedStmt,
    parsetree: &Node,
    query_string: &str,
    _context: ProcessUtilityContext,
    _dest: &mut dyn DestReceiver,
    _qc: Option<&mut QueryCompletion>,
) {
    let Node::CreateStmt(cstmt) = parsetree else {
        not_yet_reachable("ProcessUtilitySlow: non-CreateStmt");
    };
    let mut cstmt = (**cstmt).clone();

    // Run parse analysis (transformCreateStmt) ...
    let mut stmts = crate::backend::parser::parse_utilcmd::transformCreateStmt(&mut cstmt, query_string);

    // ... and do it. Pick off the elements one at a time (the list may grow with
    // LIKE expansion in later milestones).
    while !stmts.is_empty() {
        let stmt = stmts.remove(0);
        match stmt {
            Node::CreateStmt(cs) => {
                // Create the table itself.
                let _address = crate::backend::commands::tablecmds::DefineRelation(
                    shared,
                    &cs,
                    crate::catalog::pg_class::RELKIND_RELATION,
                    crate::postgres_ext::InvalidOid,
                    query_string,
                )
                .await;

                // Let a later milestone decide if a toast table is needed; PG does
                // a CommandCounterIncrement + NewRelationCreateToastTable here.
                crate::backend::access::transam::xact::CommandCounterIncrement();
            }
            Node::TableLikeClause(_) => not_yet_reachable("ProcessUtilitySlow: LIKE expansion"),
            other => not_yet_reachable(&format!("ProcessUtilitySlow: sub-statement {other:?}")),
        }

        // Need a CommandCounterIncrement between commands.
        if !stmts.is_empty() {
            crate::backend::access::transam::xact::CommandCounterIncrement();
        }
    }
}

/// PG `CreateCommandTag` (the M2-reachable arms): the completion tag for a raw
/// parse node. `CreateStmt` -> `CREATE TABLE`; other tags grow at their milestones.
pub fn create_command_tag(parsetree: &Node) -> CommandTag {
    match parsetree {
        Node::CreateStmt(_) => CommandTag::CreateTable,
        other => not_yet_reachable(&format!("CreateCommandTag: {other:?}")),
    }
}
