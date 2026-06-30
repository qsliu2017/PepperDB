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

    let is_top_level = context == ProcessUtilityContext::Toplevel;

    match parsetree {
        Node::CreateStmt(_) | Node::IndexStmt(_) => {
            process_utility_slow(shared, pstmt, parsetree, query_string, context, dest, qc).await;
        }
        Node::TransactionStmt(stmt) => {
            process_transaction_stmt(stmt, is_top_level, qc);
        }
        Node::VariableSetStmt(stmt) => {
            crate::backend::utils::misc::guc_funcs::ExecSetVariableStmt(stmt, is_top_level);
        }
        Node::VariableShowStmt(stmt) => {
            let name = stmt.name.as_deref().unwrap_or("");
            crate::backend::utils::misc::guc_funcs::GetPGVariable(name, dest);
        }
        // --- Prepared statements (prepare.rs) ---
        Node::PrepareStmt(stmt) => {
            let mut pstate = crate::backend::parser::parse_node::make_parsestate(None);
            pstate.p_sourcetext = Some(query_string.to_string());
            crate::backend::commands::prepare::prepare_query(
                shared,
                &mut pstate,
                stmt,
                pstmt.stmt_location,
                pstmt.stmt_len,
            )
            .await;
        }
        Node::ExecuteStmt(stmt) => {
            let mut pstate = crate::backend::parser::parse_node::make_parsestate(None);
            pstate.p_sourcetext = Some(query_string.to_string());
            let mut local_qc = QueryCompletion { command_tag: CommandTag::Unknown, nprocessed: 0 };
            crate::backend::commands::prepare::execute_query(
                shared,
                &mut pstate,
                stmt,
                dest.mydest(),
                &mut local_qc,
            )
            .await;
            if let Some(qc) = qc {
                qc.copy_from(&local_qc);
            }
        }
        Node::DeallocateStmt(stmt) => {
            crate::backend::commands::prepare::deallocate_query(stmt);
        }
        // --- Cursors / portals (portalcmds.rs) ---
        Node::DeclareCursorStmt(stmt) => {
            let mut pstate = crate::backend::parser::parse_node::make_parsestate(None);
            pstate.p_sourcetext = Some(query_string.to_string());
            crate::backend::commands::portalcmds::perform_cursor_open(
                shared, &mut pstate, stmt, is_top_level,
            )
            .await;
        }
        Node::FetchStmt(stmt) => {
            crate::backend::commands::portalcmds::perform_portal_fetch(stmt, dest, qc);
        }
        Node::ClosePortalStmt(stmt) => {
            crate::backend::commands::portalcmds::perform_portal_close(stmt.portalname.as_deref());
        }
        other => not_yet_reachable(&format!("standard_ProcessUtility: {other:?}")),
    }
}

/// PG `standard_ProcessUtility`'s `T_TransactionStmt` arm: dispatch each
/// `TransactionStmtKind` to the xact transaction-block layer (xact.rs). The
/// underlying block-state machine drives the per-statement commit/abort wrapping in
/// the main loop. COMMIT/PREPARE that the block layer turns into a rollback report
/// the `ROLLBACK` completion tag (PG's "report unsuccessful commit").
fn process_transaction_stmt(
    stmt: &crate::nodes::parsenodes::TransactionStmt,
    is_top_level: bool,
    qc: Option<&mut QueryCompletion>,
) {
    use crate::backend::access::transam::xact::{
        BeginTransactionBlock, DefineSavepoint, EndTransactionBlock, PrepareTransactionBlock,
        ReleaseSavepoint, RequireTransactionBlock, RollbackToSavepoint, UserAbortTransactionBlock,
    };
    use crate::nodes::parsenodes::TransactionStmtKind as Kind;
    use crate::nodes::nodes::Node as N;

    match stmt.kind {
        // START TRANSACTION (SQL99) is identical to BEGIN.
        Kind::BEGIN | Kind::START => {
            BeginTransactionBlock();
            // Apply any transaction_mode_list items as SET LOCAL on the GUCs.
            for item in &stmt.options {
                let N::DefElem(item) = item else { continue };
                let defname = item.defname.as_deref().unwrap_or("");
                if matches!(
                    defname,
                    "transaction_isolation" | "transaction_read_only" | "transaction_deferrable"
                ) {
                    let args: Vec<N> = item.arg.iter().cloned().collect();
                    crate::backend::utils::misc::guc_funcs::SetPGVariable(defname, &args, true);
                }
            }
        }
        Kind::COMMIT => {
            // A COMMIT the block layer turns into a rollback reports ROLLBACK.
            if !EndTransactionBlock(stmt.chain)
                && let Some(qc) = qc
            {
                qc.set(CommandTag::Rollback, 0);
            }
        }
        Kind::PREPARE => {
            // Two-phase commit is deferred; the block-state transition is faithful.
            if !PrepareTransactionBlock(stmt.gid.as_deref().unwrap_or(""))
                && let Some(qc) = qc
            {
                qc.set(CommandTag::Rollback, 0);
            }
        }
        Kind::COMMIT_PREPARED | Kind::ROLLBACK_PREPARED => {
            not_yet_reachable("ProcessUtility: two-phase COMMIT/ROLLBACK PREPARED");
        }
        Kind::ROLLBACK => {
            UserAbortTransactionBlock(stmt.chain);
        }
        Kind::SAVEPOINT => {
            RequireTransactionBlock(is_top_level, "SAVEPOINT");
            DefineSavepoint(stmt.savepoint_name.as_deref());
        }
        Kind::RELEASE => {
            RequireTransactionBlock(is_top_level, "RELEASE SAVEPOINT");
            ReleaseSavepoint(stmt.savepoint_name.as_deref().unwrap_or(""));
        }
        Kind::ROLLBACK_TO => {
            RequireTransactionBlock(is_top_level, "ROLLBACK TO SAVEPOINT");
            RollbackToSavepoint(stmt.savepoint_name.as_deref().unwrap_or(""));
            // CommitTransactionCommand re-defines the savepoint (SubRestart).
        }
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
    // CREATE INDEX: parse analysis (transformIndexStmt) is staged for the simple
    // single-column btree case; DefineIndex resolves the table + columns directly.
    if let Node::IndexStmt(istmt) = parsetree {
        crate::backend::commands::indexcmds::define_index(shared, istmt).await;
        crate::backend::access::transam::xact::CommandCounterIncrement();
        return;
    }

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
    use crate::nodes::parsenodes::{TransactionStmtKind as TxKind, VariableSetKind};
    match parsetree {
        // Plannable (optimizable) raw statements: their result tag. These are
        // reached via PREPARE / extended-protocol Parse, which tag the inner query.
        Node::SelectStmt(_) => CommandTag::Select,
        Node::InsertStmt(_) => CommandTag::Insert,
        Node::UpdateStmt(_) => CommandTag::Update,
        Node::DeleteStmt(_) => CommandTag::Delete,
        Node::MergeStmt(_) => CommandTag::Merge,
        Node::CreateStmt(_) => CommandTag::CreateTable,
        Node::IndexStmt(_) => CommandTag::CreateIndex,
        Node::TransactionStmt(stmt) => match stmt.kind {
            TxKind::BEGIN => CommandTag::Begin,
            TxKind::START => CommandTag::StartTransaction,
            TxKind::COMMIT => CommandTag::Commit,
            TxKind::ROLLBACK | TxKind::ROLLBACK_TO => CommandTag::Rollback,
            TxKind::SAVEPOINT => CommandTag::Savepoint,
            TxKind::RELEASE => CommandTag::Release,
            TxKind::PREPARE => CommandTag::PrepareTransaction,
            TxKind::COMMIT_PREPARED => CommandTag::CommitPrepared,
            TxKind::ROLLBACK_PREPARED => CommandTag::RollbackPrepared,
        },
        Node::VariableSetStmt(stmt) => match stmt.kind {
            VariableSetKind::RESET | VariableSetKind::RESET_ALL => CommandTag::Reset,
            _ => CommandTag::Set,
        },
        Node::VariableShowStmt(_) => CommandTag::Show,
        Node::PrepareStmt(_) => CommandTag::Prepare,
        Node::ExecuteStmt(_) => CommandTag::Execute,
        Node::DeallocateStmt(stmt) => {
            if stmt.isall {
                CommandTag::DeallocateAll
            } else {
                CommandTag::Deallocate
            }
        }
        Node::DeclareCursorStmt(_) => CommandTag::DeclareCursor,
        Node::FetchStmt(stmt) => {
            if stmt.ismove {
                CommandTag::Move
            } else {
                CommandTag::Fetch
            }
        }
        Node::ClosePortalStmt(stmt) => {
            if stmt.portalname.is_some() {
                CommandTag::CloseCursor
            } else {
                CommandTag::CloseCursorAll
            }
        }
        other => not_yet_reachable(&format!("CreateCommandTag: {other:?}")),
    }
}
