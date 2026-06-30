//! Utility commands affecting portals -- SQL cursor commands (DECLARE / FETCH /
//! MOVE / CLOSE). Translated from backend/commands/portalcmds.c (disposition:
//! full leaf, the M9-reachable subset).
//!
//! A SQL cursor is a named portal in the per-task portal table (pquery.rs). M9
//! cursors are MATERIALIZED: `PerformCursorOpen` analyzes + plans the SELECT and
//! runs it once into the portal's holdStore (a tuplestore); `PerformPortalFetch`
//! navigates that store (forward/backward/absolute/relative -- all directions,
//! since the store is randomly accessible). This is PG's holdStore path; it
//! sidesteps keeping a live executor (which borrows open relations) across FETCH
//! commands, which the borrow model cannot persist in a long-lived portal.
//!
//! STAGED: WITH HOLD (cursor surviving commit -- the store lives for the portal,
//! not past transaction end); SCROLL/NO_SCROLL planning subtleties (the store is
//! always scrollable here, so SCROLL is implicitly granted); BINARY cursors.

use std::sync::Arc;

use crate::nodes::parsenodes::{DeclareCursorStmt, FetchStmt};
use crate::parser::parse_node::ParseState;
use crate::shared_state::SharedState;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::CommandTag;
use crate::tcop::dest::DestReceiver;
use crate::utils::portal::PortalStatus;

/// PG `PerformCursorOpen`: execute DECLARE CURSOR. Analyze + rewrite + plan the
/// contained SELECT, create the named portal, and materialize the result set into
/// the portal's holdStore so FETCH/MOVE can navigate it.
///
/// Async: analysis opens relations and the executor reaches the buffer pool.
pub async fn perform_cursor_open(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    cstmt: &DeclareCursorStmt,
    is_top_level: bool,
) {
    use crate::backend::access::transam::xact::RequireTransactionBlock;
    use crate::utils::elog::ERROR;
    use crate::backend::parser::analyze::parse_analyze_fixedparams_async;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::backend::tcop::pquery::{create_named_portal, with_named_portal};
    use crate::nodes::nodes::CmdType;
    use crate::nodes::parsenodes::{CursorOptions, RawStmt};

    let name = cstmt.portalname.as_deref().unwrap_or("");
    if name.is_empty() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_CURSOR_NAME)
                .errmsg("invalid cursor name: must not be empty".to_string());
        });
    }

    // A non-holdable cursor requires a transaction block (else it has no visible
    // effect -- it would be dropped at the autocommit boundary).
    let options = CursorOptions::from_bits_truncate(cstmt.options);
    if !options.contains(CursorOptions::HOLD) {
        RequireTransactionBlock(is_top_level, "DECLARE CURSOR");
    }

    let source_text = pstate.p_sourcetext.clone().unwrap_or_default();
    let inner = cstmt
        .query
        .clone()
        .unwrap_or_else(|| unreachable!("DECLARE CURSOR carries its SELECT"));

    // Analyze + rewrite + plan the SELECT (the raw inner statement). M9's analyze
    // path handles a const or table SELECT; the cursor query is always a SELECT.
    let rawstmt = RawStmt { stmt: Some(inner), stmt_location: -1, stmt_len: 0 };
    let analyzed = parse_analyze_fixedparams_async(shared, &rawstmt, &source_text, &[], 0).await;
    if analyzed.commandType != CmdType::SELECT {
        crate::elog!(ERROR, "non-SELECT statement in DECLARE CURSOR");
    }
    let mut rewritten = query_rewrite(*analyzed);
    if rewritten.len() != 1 {
        crate::elog!(ERROR, "non-SELECT statement in DECLARE CURSOR");
    }
    let mut query = rewritten.remove(0);
    let plan = crate::backend::optimizer::plan::planner::standard_planner(
        &mut query,
        &source_text,
        cstmt.options,
        None,
    );

    // Materialize the result set into a tuplestore (random access enables the full
    // set of FETCH directions). The store is rewound to the start.
    let (store, tupdesc, _processed) =
        crate::backend::tcop::pquery::run_plan_into_store(shared, &plan, &source_text, None).await;

    // Create the named portal (a duplicate name is an error) and install the
    // materialized store + result descriptor.
    create_named_portal(name, false, false);
    with_named_portal(name, |portal| {
        portal.source_text.clone_from(&source_text);
        portal.command_tag = CommandTag::Select;
        portal.cursor_options = cstmt.options;
        portal.tup_desc = tupdesc;
        portal.hold_store = Some(store);
        portal.status = PortalStatus::Ready;
        portal.at_start = true;
        portal.at_end = false;
        portal.portal_pos = 0;
    })
    .unwrap_or_else(|| unreachable!("named portal just created"));
}

/// PG `PerformPortalFetch`: execute FETCH or MOVE. Navigate the named portal in
/// the requested direction, sending rows to `dest` (FETCH) or only counting them
/// (MOVE, which uses a None destination). Stores the FETCH/MOVE row count in `qc`.
pub fn perform_portal_fetch(
    stmt: &FetchStmt,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    use crate::backend::tcop::pquery::{portal_run_fetch, with_named_portal};

    let name = stmt.portalname.as_deref().unwrap_or("");
    if name.is_empty() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_CURSOR_NAME)
                .errmsg("invalid cursor name: must not be empty".to_string());
        });
    }

    // MOVE wants a None destination (count only); FETCH sends rows to `dest`.
    let nprocessed = with_named_portal(name, |portal| {
        portal.status = PortalStatus::Active;
        let to_dest: Option<&mut dyn DestReceiver> = if stmt.ismove { None } else { Some(dest) };
        let n = portal_run_fetch(portal, stmt.direction, stmt.howMany, to_dest);
        portal.status = PortalStatus::Ready;
        n
    });

    let Some(nprocessed) = nprocessed else {
        cursor_does_not_exist(name);
    };

    if let Some(qc) = qc {
        qc.set(
            if stmt.ismove { CommandTag::Move } else { CommandTag::Fetch },
            nprocessed,
        );
    }
}

/// PG `PerformPortalClose`: close a cursor (or all cursors with CLOSE ALL).
pub fn perform_portal_close(name: Option<&str>) {
    use crate::backend::tcop::pquery::{drop_all_named_portals, drop_named_portal, portal_exists};

    let Some(name) = name else {
        drop_all_named_portals();
        return;
    };
    if name.is_empty() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_CURSOR_NAME)
                .errmsg("invalid cursor name: must not be empty".to_string());
        });
    }
    if !portal_exists(name) {
        cursor_does_not_exist(name);
    }
    drop_named_portal(name);
}

#[cold]
fn cursor_does_not_exist(name: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("cursor \"{name}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}
