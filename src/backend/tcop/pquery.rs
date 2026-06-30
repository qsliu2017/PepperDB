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


use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::executor::execdesc::QueryDesc;
use crate::executor::executor::{ExecutorEnd, ExecutorRun, ExecutorStart};
use crate::nodes::nodes::CmdType;
use crate::nodes::params::ParamListInfoData;
use crate::nodes::plannodes::PlannedStmt;
use crate::shared_state::SharedState;
use crate::tcop::cmdtag::QueryCompletion;

/// Drive a future that is known not to await any I/O leaf (the childless-const
/// executor path) to completion synchronously. The const `ExecutorRun` future
/// resolves on the first poll (it reaches no `.await` leaf), so a noop-waker poll
/// suffices; if a future that DOES suspend is ever routed here it panics rather
/// than busy-spin. The SharedState-driven async wire path (step 18B) runs under
/// the tokio runtime instead.
fn drive_const_executor<F: std::future::Future<Output = ()>>(fut: F) {
    use std::task::{Context, Poll};
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(()) => {}
        Poll::Pending => {
            unimplemented!("PortalRunSelect: const executor future suspended (needs the async wire path, 18B)")
        }
    }
}
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
) -> QueryDesc<'static> {
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
                // A utility statement runs through the multi-query path (PG: a
                // utility PlannedStmt has utilityStmt set and no plan tree).
                return PortalStrategy::MultiQuery;
            }
        }
    }
    // ONE_RETURNING classification grows with INSERT/RETURNING; a multi-statement
    // list (incl. utility) is MULTI_QUERY.
    PortalStrategy::MultiQuery
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
            let mut query_desc: QueryDesc<'static> =
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
            // PG: "Need do nothing now" -- no result descriptor; execution happens
            // in PortalRun -> PortalRunMulti.
            portal.tup_desc = None;
        }
    }

    portal.status = PortalStatus::Ready;
}

/// PG `PortalRunUtility`: run one utility `PlannedStmt` through `ProcessUtility`.
/// Async (`ProcessUtility` reaches the catalog/heap create). The portal snapshot
/// handling is staged for M2 (CREATE TABLE runs under the caller's active
/// snapshot).
pub async fn portal_run_utility(
    shared: &Arc<SharedState>,
    portal: &mut PortalData,
    pstmt: &PlannedStmt,
    is_top_level: bool,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    use crate::tcop::utility::ProcessUtilityContext;
    let context = if is_top_level {
        ProcessUtilityContext::Toplevel
    } else {
        ProcessUtilityContext::Query
    };
    crate::backend::tcop::utility::process_utility(
        shared,
        pstmt,
        &portal.source_text,
        context,
        dest,
        qc,
    )
    .await;
}

/// PG `PortalRunMulti` (the M2 utility subset): iterate the portal's PlannedStmts,
/// dispatching each utility statement through `PortalRunUtility`. The plannable-
/// query arm (ProcessQuery) grows with multi-statement DML; M2 reaches only
/// utility statements here. Async (`PortalRunUtility` is async).
pub async fn portal_run_multi(
    shared: &Arc<SharedState>,
    portal: &mut PortalData,
    is_top_level: bool,
    dest: &mut dyn DestReceiver,
    mut qc: Option<&mut QueryCompletion>,
) {
    let stmts = std::mem::take(&mut portal.stmts);
    for pstmt in &stmts {
        crate::miscadmin::check_for_interrupts();

        if pstmt.utility_stmt.is_none() {
            unimplemented!("PortalRunMulti: plannable-query statement deferred");
        }

        // A canSetTag utility carries the result tag; pass qc only for that one.
        let qc_for_stmt = if pstmt.can_set_tag { qc.as_deref_mut() } else { None };
        portal_run_utility(shared, portal, pstmt, is_top_level, dest, qc_for_stmt).await;

        // CCI after each utility command.
        crate::backend::access::transam::xact::CommandCounterIncrement();
    }
    portal.stmts = stmts;
}

/// PG `PortalRun` for the MULTI_QUERY strategy (async). Drives `PortalRunMulti`,
/// marks the portal done, and reports completion. The synchronous `portal_run`
/// above stays the ONE_SELECT path; this is the utility/multi path.
pub async fn portal_run_multi_query(
    shared: &Arc<SharedState>,
    portal: &mut PortalData,
    is_top_level: bool,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    crate::assert!(portal.status == PortalStatus::Ready);
    crate::assert!(portal.strategy == PortalStrategy::MultiQuery);
    portal.status = PortalStatus::Active;
    portal_run_multi(shared, portal, is_top_level, dest, qc).await;
    portal.status = PortalStatus::Done;
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

    // Snapshot push is deferred (const SELECT). Run the executor. The M1 wire
    // path drives only the childless-const plan, which reaches no I/O leaf, so
    // the (now async) ExecutorRun future is driven to completion synchronously
    // here with no SharedState (None). The SharedState-supplying wire path for
    // table scans/inserts is step 18B (rules.md s5).
    drive_const_executor(ExecutorRun(None, query_desc, direction, count as u64));
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
        // always runs ExecutorFinish via the caller before drop. No SharedState on
        // the const wire path (None); 18B supplies it for scan/insert teardown.
        ExecutorEnd(None, &mut query_desc);
    }
    portal.status = PortalStatus::Done;
}

// ---------------------------------------------------------------------------
// Named-portal registry (PG portalmem.c's PortalHashTable, per-backend).
//
// In C the PortalHashTable is a process-global dynahash keyed by portal name. As
// a tokio task per backend, it is per-task state (rules.md s10): a task-local
// RefCell, like the relcache. A `PortalData` is NOT Send/Sync (it holds plans
// and tuplestores), so it lives here, owned by the backend's session frame, and
// is borrowed during execution. The unnamed protocol portal also lives here
// (key ""); SQL cursors use their declared names.
// ---------------------------------------------------------------------------

use std::cell::RefCell;
use std::collections::HashMap;

tokio::task_local! {
    static PORTAL_TABLE: RefCell<HashMap<String, Box<PortalData>>>;
}

/// Establish the per-task named-portal registry and run `fut`. Wrapped into the
/// backend's connect-to-database scope stack (postgres.rs `init_postgres`).
pub async fn portal_scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    PORTAL_TABLE.scope(RefCell::new(HashMap::new()), fut).await
}

#[must_use]
pub fn portal_table_present() -> bool {
    PORTAL_TABLE.try_with(|_| ()).is_ok()
}

fn with_portal_table<R>(f: impl FnOnce(&mut HashMap<String, Box<PortalData>>) -> R) -> Option<R> {
    PORTAL_TABLE.try_with(|cell| f(&mut cell.borrow_mut())).ok()
}

/// PG `GetPortalByName`: look up a named portal. Returns whether one exists; the
/// portal itself is borrowed via `with_named_portal` (the owned `Box` stays in
/// the table). Unlike C this can't hand out a long-lived pointer (ownership stays
/// in the table), so callers operate on it under a closure.
#[must_use]
pub fn portal_exists(name: &str) -> bool {
    with_portal_table(|t| t.contains_key(name)).unwrap_or(false)
}

/// Run `f` with a borrow of the named portal, returning its result (or `None` if
/// no such portal). The table borrow is released before `f`'s result is returned.
pub fn with_named_portal<R>(name: &str, f: impl FnOnce(&mut PortalData) -> R) -> Option<R> {
    with_portal_table(|t| t.get_mut(name).map(|p| f(p))).flatten()
}

/// PG `CreatePortal` (portalmem.c): create a named portal and insert it into the
/// per-task table. `allow_dup`/`dup_silent` govern duplicate handling: a non-dup
/// create over an existing name raises "cursor already exists" (the cursor case).
pub fn create_named_portal(name: &str, allow_dup: bool, dup_silent: bool) {
    let existed = with_portal_table(|t| {
        if t.contains_key(name) {
            return true;
        }
        t.insert(name.to_string(), create_portal(name));
        false
    })
    .unwrap_or_else(|| unreachable!("create_named_portal outside a portal scope"));
    if existed {
        if !allow_dup {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_CURSOR)
                    .errmsg(format!("cursor \"{name}\" already exists"));
            });
        } else if !dup_silent {
            crate::ereport!(crate::utils::elog::WARNING, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("closing existing cursor \"{name}\""));
            });
        }
        // allow_dup: replace the existing portal (PG drops then recreates).
        with_portal_table(|t| t.insert(name.to_string(), create_portal(name)));
    }
}

/// PG `PortalDrop` (the per-task table variant): remove a named portal, tearing
/// down its executor if still live.
pub fn drop_named_portal(name: &str) {
    if let Some(mut portal) = with_portal_table(|t| t.remove(name)).flatten() {
        portal_drop(&mut portal);
    }
}

/// PG `PortalHashTableDeleteAll`: drop every portal (CLOSE ALL).
pub fn drop_all_named_portals() {
    let names: Vec<String> = with_portal_table(|t| t.keys().cloned().collect()).unwrap_or_default();
    for name in names {
        drop_named_portal(&name);
    }
}

// ---------------------------------------------------------------------------
// Materialized-result receiver + FETCH navigation (cursor support).
//
// A SQL cursor / EXECUTE-into-store is materialized: PortalStart-equivalent runs
// the SELECT once into a tuplestore (FillPortalStore), and FETCH/MOVE navigate
// the store (DoPortalRunFetch over the materialized rows). This is PG's
// holdStore path; it gives forward AND backward/absolute/relative scrolling for
// free since the store is randomly accessible. (A streaming ONE_SELECT executor
// kept live across FETCH commands needs a self-referential QueryDesc borrowing
// open relations, which the borrow model can't persist; materialization is the
// faithful, lifetime-clean choice for M9. STAGED: WITH HOLD survival across
// commit -- the store currently lives for the portal, not past transaction end.)
// ---------------------------------------------------------------------------

use crate::utils::tuplestore::Tuplestorestate;

/// A DestReceiver that stows received rows into a tuplestore (PG
/// `tstoreReceiver`). Holds a raw owning pointer-free design: the store is
/// threaded in by the caller and taken back out after the run.
pub struct TuplestoreReceiver {
    store: Box<Tuplestorestate>,
    tupdesc: Option<crate::access::tupdesc::TupleDesc>,
}

impl TuplestoreReceiver {
    #[must_use]
    pub fn new(store: Box<Tuplestorestate>) -> Self {
        Self { store, tupdesc: None }
    }
    /// Reclaim the (now filled) store + its tuple descriptor.
    #[must_use]
    pub fn into_parts(self) -> (Box<Tuplestorestate>, Option<crate::access::tupdesc::TupleDesc>) {
        (self.store, self.tupdesc)
    }
}

impl DestReceiver for TuplestoreReceiver {
    fn receive_slot(&mut self, slot: &mut crate::executor::tuptable::TupleTableSlot) -> bool {
        crate::backend::utils::sort::tuplestore::tuplestore_puttupleslot(&mut self.store, slot);
        true
    }
    fn r_startup(&mut self, _operation: CmdType, typeinfo: crate::access::tupdesc::TupleDesc) {
        crate::backend::utils::sort::tuplestore::tuplestore_set_tupdesc(&mut self.store, typeinfo.clone());
        self.tupdesc = Some(typeinfo);
    }
    fn r_shutdown(&mut self) {}
    fn mydest(&self) -> crate::tcop::dest::CommandDest {
        crate::tcop::dest::CommandDest::DestTuplestore
    }
}

/// A receiver that wraps a `TuplestoreReceiver` and, at shutdown, moves it into a
/// shared slot so the caller can reclaim the filled store (the shared executor
/// frame owns + drops its receiver, so this is how a materialized run hands the
/// store back).
struct CapturingReceiver {
    inner: Option<TuplestoreReceiver>,
    slot: Arc<std::sync::Mutex<Option<TuplestoreReceiver>>>,
}

impl DestReceiver for CapturingReceiver {
    fn receive_slot(&mut self, slot: &mut crate::executor::tuptable::TupleTableSlot) -> bool {
        self.inner.as_mut().is_none_or(|r| r.receive_slot(slot))
    }
    fn r_startup(&mut self, operation: CmdType, typeinfo: crate::access::tupdesc::TupleDesc) {
        if let Some(r) = self.inner.as_mut() {
            r.r_startup(operation, typeinfo);
        }
    }
    fn r_shutdown(&mut self) {
        if let Some(mut r) = self.inner.take() {
            r.r_shutdown();
            *self.slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(r);
        }
    }
    fn mydest(&self) -> crate::tcop::dest::CommandDest {
        crate::tcop::dest::CommandDest::DestTuplestore
    }
}

impl Drop for CapturingReceiver {
    fn drop(&mut self) {
        // A non-RETURNING DML produces no tuples, so ExecutorRun never calls
        // r_shutdown (send_tuples is false). Reclaim the (empty) store here so the
        // caller still gets it back instead of hitting the stash `unreachable!`.
        if let Some(r) = self.inner.take() {
            *self.slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(r);
        }
    }
}

/// Run a planned SELECT into a fresh tuplestore (with optional bound `$n` params)
/// and return the filled store, its result `TupleDesc`, and the executor's
/// `es_processed` count (rows sent for SELECT/RETURNING, rows modified for a
/// no-result DML). This is the materialization step behind SQL cursors
/// (portalcmds) and the extended-protocol Bind/Execute path. Async (the executor
/// reaches the buffer pool).
pub async fn run_plan_into_store(
    shared: &Arc<SharedState>,
    plan: &PlannedStmt,
    query_string: &str,
    bound_params: Option<&ParamListInfoData>,
) -> (Box<crate::utils::tuplestore::Tuplestorestate>, Option<crate::access::tupdesc::TupleDesc>, u64)
{
    let store = crate::backend::utils::sort::tuplestore::tuplestore_begin_heap(true, false, 1024);
    let slot: Arc<std::sync::Mutex<Option<TuplestoreReceiver>>> =
        Arc::new(std::sync::Mutex::new(None));
    let capture = Box::new(CapturingReceiver {
        inner: Some(TuplestoreReceiver::new(store)),
        slot: Arc::clone(&slot),
    });
    let processed = crate::backend::tcop::postgres::execute_plan_into(
        shared,
        plan,
        query_string,
        bound_params,
        capture,
        0,
    )
    .await;
    let recv = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .unwrap_or_else(|| unreachable!("CapturingReceiver stashed its inner store at shutdown"));
    let (mut store, tupdesc) = recv.into_parts();
    crate::backend::utils::sort::tuplestore::tuplestore_rescan(&mut store);
    (store, tupdesc, processed)
}

/// PG `DoPortalRunFetch` over a materialized portal store: navigate the portal's
/// holdStore in the requested direction, replaying the selected rows to `dest`.
/// Returns the number of rows actually sent to a real destination (FETCH) or
/// skipped past (MOVE). FORWARD/ALL is the full path; BACKWARD/ABSOLUTE/RELATIVE
/// are supported because the store is randomly accessible.
///
/// `to_dest`: a real receiver for FETCH, or `None` for MOVE (count rows only).
pub fn portal_run_fetch(
    portal: &mut PortalData,
    direction: crate::nodes::parsenodes::FetchDirection,
    how_many: i64,
    to_dest: Option<&mut dyn DestReceiver>,
) -> u64 {
    use crate::nodes::parsenodes::{FetchDirection, FETCH_ALL};

    crate::assert!(portal.hold_store.is_some());

    // Resolve (forward, count) from the FETCH direction + signed count, mirroring
    // DoPortalRunFetch. ABSOLUTE/RELATIVE reposition first, then fetch.
    let (forward, count): (bool, i64) = match direction {
        FetchDirection::FORWARD => {
            if how_many < 0 {
                (false, -how_many)
            } else {
                (true, how_many)
            }
        }
        FetchDirection::BACKWARD => {
            if how_many < 0 {
                (true, -how_many)
            } else {
                (false, how_many)
            }
        }
        FetchDirection::ABSOLUTE => {
            // Rewind to start, advance to the absolute position, then fetch one.
            fetch_rewind(portal);
            if how_many > 0 {
                if how_many > 1 {
                    skip_in_store(portal, true, how_many - 1);
                }
                return fetch_from_store(portal, true, 1, to_dest);
            } else if how_many < 0 {
                // Advance to end, back up abs(count)-1, return prior row.
                skip_in_store(portal, true, FETCH_ALL);
                if how_many < -1 {
                    skip_in_store(portal, false, -how_many - 1);
                }
                return fetch_from_store(portal, false, 1, to_dest);
            }
            // count == 0: rewind, return zero rows.
            return fetch_from_store(portal, true, 0, to_dest);
        }
        FetchDirection::RELATIVE => {
            if how_many > 0 {
                if how_many > 1 {
                    skip_in_store(portal, true, how_many - 1);
                }
                return fetch_from_store(portal, true, 1, to_dest);
            } else if how_many < 0 {
                if how_many < -1 {
                    skip_in_store(portal, false, -how_many - 1);
                }
                return fetch_from_store(portal, false, 1, to_dest);
            }
            // count == 0: same as FETCH FORWARD 0 -- re-fetch current row (handled
            // by the count == 0 block below).
            (true, 0)
        }
    };

    // Zero count means to re-fetch the current row, if any (per SQL), mirroring
    // DoPortalRunFetch's shared count == 0 path.
    if count == 0 {
        // Sitting on a row iff not before-first and not after-last.
        let on_row = !portal.at_start && !portal.at_end;
        if to_dest.is_none() {
            // MOVE 0 returns 0/1 based on whether FETCH 0 would return a row.
            return u64::from(on_row);
        }
        if on_row {
            // Back up one (no output) so the forward fetch re-reads the current row;
            // leaves the cursor position unchanged.
            skip_in_store(portal, false, 1);
            return fetch_from_store(portal, true, 1, to_dest);
        }
        // Not on a row: still start/shut the destination, fetching no row.
        return fetch_from_store(portal, true, 0, to_dest);
    }

    fetch_from_store(portal, forward, count, to_dest)
}

/// Rewind the portal's store read pointer to the start.
fn fetch_rewind(portal: &mut PortalData) {
    let store = portal
        .hold_store
        .as_mut()
        .unwrap_or_else(|| unreachable!("fetch_rewind on a portal with no store"));
    crate::backend::utils::sort::tuplestore::tuplestore_rescan(store);
    portal.at_start = true;
    portal.at_end = false;
    portal.portal_pos = 0;
}

/// Skip `n` rows in the store (no output), updating the cursor position. `n` may
/// be FETCH_ALL to drain to the end.
fn skip_in_store(portal: &mut PortalData, forward: bool, n: i64) {
    use crate::nodes::parsenodes::FETCH_ALL;
    let store = portal
        .hold_store
        .as_mut()
        .unwrap_or_else(|| unreachable!("skip_in_store on a portal with no store"));
    let mut skipped: i64 = 0;
    if n == FETCH_ALL {
        while crate::backend::utils::sort::tuplestore::tuplestore_advance(store, forward) {
            skipped += 1;
        }
    } else {
        for _ in 0..n {
            if !crate::backend::utils::sort::tuplestore::tuplestore_advance(store, forward) {
                break;
            }
            skipped += 1;
        }
    }
    advance_position(portal, forward, skipped as u64);
}

/// Fetch up to `count` rows from the store in `forward` direction, sending each to
/// `to_dest` (if a real destination). Returns the number of rows fetched.
fn fetch_from_store(
    portal: &mut PortalData,
    forward: bool,
    count: i64,
    mut to_dest: Option<&mut dyn DestReceiver>,
) -> u64 {
    use crate::nodes::parsenodes::FETCH_ALL;
    let tupdesc = portal.tup_desc.clone();

    // Start the destination (RowDescription is emitted by the caller for the wire;
    // r_startup here lets a printtup/tuplestore receiver initialize).
    if let (Some(dest), Some(td)) = (to_dest.as_deref_mut(), tupdesc.clone()) {
        dest.r_startup(CmdType::SELECT, td);
    }

    let mut slot = crate::backend::executor::execTuples::make_single_tuple_table_slot(
        tupdesc,
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let store = portal
        .hold_store
        .as_mut()
        .unwrap_or_else(|| unreachable!("fetch_from_store on a portal with no store"));

    let unlimited = count == FETCH_ALL;
    let mut fetched: u64 = 0;
    loop {
        if !unlimited && fetched >= count as u64 {
            break;
        }
        let got = crate::backend::utils::sort::tuplestore::tuplestore_gettupleslot(
            store, forward, false, &mut slot,
        );
        if !got {
            break;
        }
        if let Some(dest) = to_dest.as_deref_mut() {
            dest.receive_slot(&mut slot);
        }
        fetched += 1;
    }

    if let Some(dest) = to_dest {
        dest.r_shutdown();
    }

    advance_position(portal, forward, fetched);
    fetched
}

/// Update the portal's cursor position after moving `n` rows in `forward`.
fn advance_position(portal: &mut PortalData, forward: bool, n: u64) {
    if n > 0 {
        if forward {
            portal.at_start = false;
            portal.portal_pos += n;
        } else {
            portal.at_end = false;
            portal.portal_pos = portal.portal_pos.saturating_sub(n);
        }
    }
    // at_end / at_start are recomputed by the store's eof state on next fetch.
    let store = portal
        .hold_store
        .as_ref()
        .unwrap_or_else(|| unreachable!("advance_position on a portal with no store"));
    if forward && crate::backend::utils::sort::tuplestore::tuplestore_ateof(store) {
        portal.at_end = true;
    }
}

/// Build a fresh, empty `PortalData` for the unnamed portal.
fn empty_portal(name: &str) -> PortalData {
    PortalData {
        name: name.to_string(),
        prep_stmt_name: None,
        portal_context: (),
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
        hold_context: (),
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
    use std::sync::Mutex;

    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::catalog::genbki::INT4OID;
    use crate::executor::executor::ExecutorFinish;
    use crate::executor::tuptable::{slot_getattr, TupleTableSlot};
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::{FetchDirection, RawStmt, FETCH_ALL};
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
        sink: Arc<Mutex<Collected>>,
    }
    impl DestReceiver for CollectingDest {
        fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
            let natts = i32::from(slot.nvalid);
            let row = (1..=natts)
                .map(|a| DatumGetInt32(slot_getattr(slot, a).unwrap_or(Datum(0))))
                .collect();
            self.sink.lock().unwrap().rows.push(row);
            true
        }
        fn r_startup(&mut self, _op: CmdType, _ti: TupleDesc) {
            self.sink.lock().unwrap().startups += 1;
        }
        fn r_shutdown(&mut self) {
            self.sink.lock().unwrap().shutdowns += 1;
        }
        fn mydest(&self) -> CommandDest {
            CommandDest::DestNone
        }
    }

    #[test]
    fn portal_one_select_drives_executor() {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let dest = Box::new(CollectingDest { sink: Arc::clone(&sink) });

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

        let s = sink.lock().unwrap();
        assert_eq!(s.startups, 1);
        assert_eq!(s.shutdowns, 1);
        assert_eq!(s.rows, vec![vec![1]]);
    }

    #[test]
    fn portal_select_two_cols() {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let dest = Box::new(CollectingDest { sink: Arc::clone(&sink) });
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
        assert_eq!(sink.lock().unwrap().rows, vec![vec![1, 2]]);
    }

    /// Build a holdable portal materialized over a 5-row int4 store, positioned at
    /// the start (before-first), mirroring a freshly-opened SQL cursor.
    fn five_row_portal() -> Box<PortalData> {
        let mut td_data = TupleDescData::create_template(1);
        td_data.init_builtin_entry(1, "n", INT4OID, -1, 0);
        let td: TupleDesc = Arc::new(td_data);
        let mut store =
            crate::backend::utils::sort::tuplestore::tuplestore_begin_heap(true, false, 1024);
        for n in 1..=5i32 {
            crate::backend::utils::sort::tuplestore::tuplestore_putvalues(
                &mut store,
                &td,
                &[crate::postgres::Int32GetDatum(n)],
                &[false],
            );
        }
        crate::backend::utils::sort::tuplestore::tuplestore_rescan(&mut store);
        let mut portal = create_portal("c");
        portal.tup_desc = Some(td);
        portal.hold_store = Some(store);
        portal.status = PortalStatus::Ready;
        portal.at_start = true;
        portal.at_end = false;
        portal.portal_pos = 0;
        portal
    }

    fn fetch(portal: &mut PortalData, dir: FetchDirection, n: i64) -> Vec<i32> {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let mut dest = CollectingDest { sink: Arc::clone(&sink) };
        portal_run_fetch(portal, dir, n, Some(&mut dest));
        let rows = sink.lock().unwrap().rows.clone();
        rows.into_iter().map(|r| r[0]).collect()
    }

    /// FETCH 0 re-fetches the current row (PG `DoPortalRunFetch` count == 0), without
    /// moving the cursor: FETCH 2 -> row 2; FETCH 0 -> row 2 again; FETCH 1 -> row 3.
    #[test]
    fn fetch_zero_refetches_current_row() {
        let mut portal = five_row_portal();

        assert_eq!(fetch(&mut portal, FetchDirection::FORWARD, 2), vec![1, 2]);
        assert_eq!(portal.portal_pos, 2);

        // FETCH 0: re-fetch current row (row 2), position unchanged.
        assert_eq!(fetch(&mut portal, FetchDirection::FORWARD, 0), vec![2]);
        assert_eq!(portal.portal_pos, 2);

        // FORWARD/RELATIVE/BACKWARD 0 all re-fetch the current row too.
        assert_eq!(fetch(&mut portal, FetchDirection::RELATIVE, 0), vec![2]);
        assert_eq!(fetch(&mut portal, FetchDirection::BACKWARD, 0), vec![2]);
        assert_eq!(portal.portal_pos, 2);

        // Position is still 2, so the next forward fetch returns row 3.
        assert_eq!(fetch(&mut portal, FetchDirection::FORWARD, 1), vec![3]);
        assert_eq!(portal.portal_pos, 3);
    }

    /// MOVE 0 (no destination) returns 1 when on a row, 0 when before-first.
    #[test]
    fn move_zero_reports_on_row() {
        let mut portal = five_row_portal();
        // Before-first: not on a row -> 0.
        assert_eq!(portal_run_fetch(&mut portal, FetchDirection::FORWARD, 0, None), 0);
        // Advance onto row 1, then MOVE 0 -> 1, position unchanged.
        assert_eq!(fetch(&mut portal, FetchDirection::FORWARD, 1), vec![1]);
        assert_eq!(portal_run_fetch(&mut portal, FetchDirection::FORWARD, 0, None), 1);
        assert_eq!(portal.portal_pos, 1);
    }
}
