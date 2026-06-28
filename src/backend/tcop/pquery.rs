//! POSTGRES process query command code (portal execution). Translated from
//! backend/tcop/pquery.c (disposition: grow).
//!
//! The portal-strategy switch is correct-for-reachable: the
//! `PORTAL_ONE_SELECT` path (CreateQueryDesc + ExecutorStart/Run/End driving the
//! printtup destination) is COMPLETE; `PORTAL_ONE_RETURNING`/`ONE_MOD_WITH`/
//! `UTIL_SELECT`/`MULTI_QUERY` are clean grow guards (rules.md s4). The whole M1
//! SELECT path is synchronous (rules.md s5): no executor node reaches an I/O
//! leaf, so `PortalRun` does not `.await`; the receiver appends bytes to the send
//! buffer synchronously and the command loop flushes afterward.
//!
//! ## Portal ownership (M1)
//!
//! C threads a `Portal` (a `*mut PortalData`) through the global `PortalHashTable`
//! managed by portalmem.c (deferred). For M1 the unnamed portal is owned by the
//! caller as a `Box<PortalData>` and the entry points below take `&mut
//! PortalData`; the hashtable / pinning / subxact bookkeeping grows when
//! portalmem lands. `ActivePortal`/`PortalContext` globals are likewise deferred
//! (no internal-transaction-restarting utility runs on the M1 path).

use crate::access::sdir::ScanDirection;
use crate::executor::execdesc::QueryDesc;
use crate::executor::executor::{ExecutorEnd, ExecutorRun, ExecutorStart};
use crate::nodes::nodes::CmdType;
use crate::nodes::params::ParamListInfoData;
use crate::nodes::plannodes::PlannedStmt;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::CommandTag;
use crate::tcop::dest::DestReceiver;
use crate::utils::portal::{PortalData, PortalStatus, PortalStrategy};

use crate::backend::tcop::dest::NoneReceiver;

/// PG `CreateQueryDesc`: bundle everything the executor needs. Snapshot
/// registration is deferred (no MVCC visibility needed for a const SELECT); the
/// fields set by ExecutorStart (`tupDesc`/`estate`/`planstate`) start empty.
#[allow(deprecated)]
pub fn create_query_desc(
    plannedstmt: Box<PlannedStmt>,
    source_text: &str,
    dest: Box<dyn DestReceiver>,
) -> QueryDesc {
    QueryDesc {
        operation: plannedstmt.command_type,
        plannedstmt: Some(plannedstmt),
        sourceText: source_text.to_string(),
        snapshot: None,
        crosscheck_snapshot: None,
        dest: Some(dest),
        params: None,
        queryEnv: None,
        instrument_options: crate::executor::instrument::InstrumentOption::empty(),
        tupDesc: None,
        estate: None,
        planstate: None,
        already_executed: false,
        totaltime: None,
    }
}

/// PG `CreatePortal` (portalmem.c): create the unnamed portal. For M1 this
/// returns an owned, freshly-initialized `PortalData`; the hashtable insertion /
/// duplicate handling grows with portalmem.
pub fn create_portal(name: &str) -> Box<PortalData> {
    Box::new(empty_portal(name))
}

/// PG `PortalDefineQuery` (portalmem.c): attach the plan list + tags to a portal.
pub fn portal_define_query(
    portal: &mut PortalData,
    source_text: &str,
    command_tag: CommandTag,
    stmts: Vec<PlannedStmt>,
) {
    crate::assert!(portal.status == PortalStatus::New);
    portal.source_text = source_text.to_string();
    portal.command_tag = command_tag;
    portal.stmts = stmts;
    portal.status = PortalStatus::Defined;
}

/// PG `ChoosePortalStrategy`: select the execution strategy. M1 reaches only the
/// single-PlannedStmt SELECT case; the rest grow.
pub fn choose_portal_strategy(stmts: &[PlannedStmt]) -> PortalStrategy {
    if stmts.len() == 1 {
        let pstmt = &stmts[0];
        if pstmt.can_set_tag {
            if pstmt.command_type == CmdType::SELECT {
                if pstmt.has_modifying_cte {
                    return PortalStrategy::OneModWith;
                }
                return PortalStrategy::OneSelect;
            }
            if pstmt.command_type == CmdType::UTILITY {
                unimplemented!("ChoosePortalStrategy: utility statements deferred");
            }
        }
    }
    // ONE_RETURNING / MULTI_QUERY classification grows with INSERT/multi-stmt.
    unimplemented!("ChoosePortalStrategy: non-single-SELECT statement lists deferred")
}

/// PG `PortalSetResultFormat`: store the per-column wire format codes.
pub fn portal_set_result_format(portal: &mut PortalData, formats: &[i16]) {
    portal.formats = formats.to_vec();
}

/// PG `PortalStart`: fire up the portal per its strategy. The M1 ONE_SELECT arm
/// determines the strategy, builds a `QueryDesc` (initially `DestNone`, as in C),
/// and runs `ExecutorStart` to compute the result tuple descriptor.
pub fn portal_start(portal: &mut PortalData) {
    crate::assert!(portal.status == PortalStatus::Defined);

    portal.strategy = choose_portal_strategy(&portal.stmts);

    match portal.strategy {
        PortalStrategy::OneSelect => {
            // Snapshot push is deferred (const SELECT needs no MVCC snapshot).
            let pstmt = portal.stmts.first().cloned().unwrap_or_else(|| {
                unreachable!("PortalDefineQuery installed exactly one PlannedStmt")
            });
            let mut query_desc =
                create_query_desc(Box::new(pstmt), &portal.source_text, Box::new(NoneReceiver));

            ExecutorStart(&mut query_desc, 0);

            // Remember the tuple descriptor computed by ExecutorStart (an Arc
            // clone the portal co-owns alongside the QueryDesc).
            portal.tup_desc.clone_from(&query_desc.tupDesc);
            portal.query_desc = Some(Box::new(query_desc));

            portal.at_start = true;
            portal.at_end = false;
            portal.portal_pos = 0;
        }
        PortalStrategy::OneReturning
        | PortalStrategy::OneModWith
        | PortalStrategy::UtilSelect => {
            unimplemented!("PortalStart: holdStore strategies (RETURNING/MOD_WITH/UTIL) deferred")
        }
        PortalStrategy::MultiQuery => {
            unimplemented!("PortalStart: PORTAL_MULTI_QUERY deferred")
        }
    }

    portal.status = PortalStatus::Ready;
}

/// PG `PortalRun`: execute the portal to completion. Returns "all rows fetched".
/// M1 handles the ONE_SELECT strategy; the result tag + row count are stored in
/// `qc` (and the portal's own `qc`).
///
/// The destination receiver is MOVED into the portal's `QueryDesc` (as C assigns
/// `queryDesc->dest = dest`); it is dropped at `PortalDrop`/`ExecutorEnd` (C
/// `receiver->rDestroy`). Ownership is the Rust analog of C's separately-created
/// receiver passed by pointer.
pub fn portal_run(
    portal: &mut PortalData,
    count: i64,
    dest: Box<dyn DestReceiver>,
    qc: Option<&mut QueryCompletion>,
) -> bool {
    crate::assert!(portal.status == PortalStatus::Ready);

    portal.status = PortalStatus::Active;

    match portal.strategy {
        PortalStrategy::OneSelect => {
            let nprocessed = portal_run_select(portal, true, count, dest);

            // Store the completion tag + row count.
            portal.qc.set(portal.command_tag, nprocessed);
            if let Some(qc) = qc {
                qc.copy_from(&portal.qc);
                qc.nprocessed = nprocessed;
            }

            portal.status = PortalStatus::Ready;
            // Forward fetch: DONE iff we are now at end.
            portal.at_end
        }
        PortalStrategy::OneReturning
        | PortalStrategy::OneModWith
        | PortalStrategy::UtilSelect
        | PortalStrategy::MultiQuery => {
            unimplemented!("PortalRun: non-ONE_SELECT strategies deferred")
        }
    }
}

/// PG `PortalRunSelect`: drive the executor for a ONE_SELECT portal. Forward-only
/// for M1 (cursors / backward scan grow). Returns the number of rows processed.
fn portal_run_select(
    portal: &mut PortalData,
    forward: bool,
    mut count: i64,
    dest: Box<dyn DestReceiver>,
) -> u64 {
    if !forward {
        unimplemented!("PortalRunSelect: backward scan (scrollable cursor) deferred");
    }

    let query_desc = portal
        .query_desc
        .as_mut()
        .unwrap_or_else(|| unreachable!("PortalStart created the QueryDesc"));

    // Force the destination to the right thing on the fetch (C contract).
    query_desc.dest = Some(dest);

    let direction = if portal.at_end || count <= 0 {
        count = 0;
        ScanDirection::NoMovement
    } else {
        ScanDirection::Forward
    };
    // In the executor, zero count processes all rows (FETCH_ALL maps to 0).
    if count == crate::nodes::parsenodes::FETCH_ALL {
        count = 0;
    }

    // Snapshot push is deferred (const SELECT). Run the executor.
    ExecutorRun(query_desc, direction, count as u64);
    let nprocessed = query_desc.estate.as_ref().map_or(0, |e| e.processed);

    if direction != ScanDirection::NoMovement {
        if nprocessed > 0 {
            portal.at_start = false;
        }
        if count == 0 || nprocessed < count as u64 {
            portal.at_end = true;
        }
        portal.portal_pos += nprocessed;
    }

    nprocessed
}

/// PG `PortalDrop`: tear down a portal. M1 ends the executor (ExecutorEnd) if it
/// is still live (this also drops the moved-in receiver, C `rDestroy`); the
/// portal storage itself is freed by the owning `Box` drop.
pub fn portal_drop(portal: &mut PortalData) {
    if let Some(mut query_desc) = portal.query_desc.take() {
        // ExecutorEnd is only valid once the executor has finished; the M1 path
        // always runs ExecutorFinish via the caller before drop.
        ExecutorEnd(&mut query_desc);
    }
    portal.status = PortalStatus::Done;
}

/// Build a fresh, empty `PortalData` for the unnamed portal.
fn empty_portal(name: &str) -> PortalData {
    PortalData {
        name: name.to_string(),
        prep_stmt_name: None,
        portal_context: core::ptr::null_mut(),
        resowner: None,
        cleanup: None,
        create_subid: crate::c::InvalidSubTransactionId,
        active_subid: crate::c::InvalidSubTransactionId,
        create_level: 0,
        source_text: String::new(),
        command_tag: CommandTag::Unknown,
        qc: QueryCompletion { command_tag: CommandTag::Unknown, nprocessed: 0 },
        stmts: Vec::new(),
        cplan: None,
        portal_params: Box::new(empty_param_list()),
        query_env: None,
        strategy: PortalStrategy::OneSelect,
        cursor_options: 0,
        status: PortalStatus::New,
        portal_pinned: false,
        auto_held: false,
        query_desc: None,
        tup_desc: None,
        formats: Vec::new(),
        portal_snapshot: None,
        hold_store: None,
        hold_context: core::ptr::null_mut(),
        hold_snapshot: None,
        at_start: true,
        at_end: false,
        portal_pos: 0,
        creation_time: 0,
        visible: false,
    }
}

/// An empty parameter list (the M1 portal carries no params).
fn empty_param_list() -> ParamListInfoData {
    ParamListInfoData {
        param_fetch: None,
        param_compile: None,
        parser_setup: None,
        param_values_str: None,
        num_params: 0,
        params: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::access::tupdesc::TupleDesc;
    use crate::catalog::genbki::INT4OID;
    use crate::executor::executor::ExecutorFinish;
    use crate::executor::tuptable::{slot_getattr, TupleTableSlot};
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::{RawStmt, FETCH_ALL};
    use crate::parser::parser::RawParseMode;
    use crate::postgres::{Datum, DatumGetInt32};
    use crate::tcop::dest::CommandDest;

    /// Plan a const SELECT through the real front half of the pipeline.
    fn plan(sql: &str) -> PlannedStmt {
        let mut list = crate::backend::parser::parser::raw_parser(sql, RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs: RawStmt = *rs;
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, sql, &[], 0, None);
        let mut rewritten = crate::backend::rewrite::rewriteHandler::query_rewrite(*q);
        let mut parse = rewritten.remove(0);
        crate::backend::optimizer::plan::planner::standard_planner(&mut parse, sql, 0, None)
    }

    /// A collecting receiver (mirrors execMain's test sink).
    #[derive(Default)]
    struct Collected {
        rows: Vec<Vec<i32>>,
        startups: u32,
        shutdowns: u32,
    }
    struct CollectingDest {
        sink: Rc<RefCell<Collected>>,
    }
    impl DestReceiver for CollectingDest {
        fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
            let natts = i32::from(slot.nvalid);
            let row = (1..=natts)
                .map(|a| DatumGetInt32(slot_getattr(slot, a).unwrap_or(Datum(0))))
                .collect();
            self.sink.borrow_mut().rows.push(row);
            true
        }
        fn r_startup(&mut self, _op: CmdType, _ti: TupleDesc) {
            self.sink.borrow_mut().startups += 1;
        }
        fn r_shutdown(&mut self) {
            self.sink.borrow_mut().shutdowns += 1;
        }
        fn mydest(&self) -> CommandDest {
            CommandDest::DestNone
        }
    }

    #[test]
    fn portal_one_select_drives_executor() {
        let sink = Rc::new(RefCell::new(Collected::default()));
        let dest = Box::new(CollectingDest { sink: Rc::clone(&sink) });

        let mut portal = create_portal("");
        portal_define_query(&mut portal, "SELECT 1", CommandTag::Select, vec![plan("SELECT 1")]);
        portal_set_result_format(&mut portal, &[0]);
        portal_start(&mut portal);

        // ExecutorStart computed a one-int4 result descriptor.
        let td = portal.tup_desc.as_ref().expect("portal carries a tupdesc");
        assert_eq!(td.natts, 1);
        assert_eq!(td.attr(0).atttypid, INT4OID);

        let mut qc = QueryCompletion { command_tag: CommandTag::Unknown, nprocessed: 0 };
        let all_done = portal_run(&mut portal, FETCH_ALL, dest, Some(&mut qc));
        assert!(all_done, "single-row forward fetch reaches end");
        assert_eq!(qc.command_tag, CommandTag::Select);
        assert_eq!(qc.nprocessed, 1);

        // Finish + drop the executor (this also drops the moved-in receiver).
        if let Some(query_desc) = portal.query_desc.as_mut() {
            ExecutorFinish(query_desc);
        }
        portal_drop(&mut portal);

        let s = sink.borrow();
        assert_eq!(s.startups, 1);
        assert_eq!(s.shutdowns, 1);
        assert_eq!(s.rows, vec![vec![1]]);
    }

    #[test]
    fn portal_select_two_cols() {
        let sink = Rc::new(RefCell::new(Collected::default()));
        let dest = Box::new(CollectingDest { sink: Rc::clone(&sink) });
        let mut portal = create_portal("");
        portal_define_query(&mut portal, "SELECT 1, 2", CommandTag::Select, vec![plan("SELECT 1, 2")]);
        portal_set_result_format(&mut portal, &[]);
        portal_start(&mut portal);
        let mut qc = QueryCompletion { command_tag: CommandTag::Unknown, nprocessed: 0 };
        portal_run(&mut portal, FETCH_ALL, dest, Some(&mut qc));
        if let Some(query_desc) = portal.query_desc.as_mut() {
            ExecutorFinish(query_desc);
        }
        portal_drop(&mut portal);
        assert_eq!(sink.borrow().rows, vec![vec![1, 2]]);
    }
}
