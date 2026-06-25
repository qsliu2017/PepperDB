//! Translated from PostgreSQL src/include/utils/portal.h
//!
//! A portal represents the execution state of a running or runnable query,
//! backing both SQL-level CURSORs and protocol-level portals.

use std::collections::HashMap;

use crate::c::SubTransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::executor::execdesc::QueryDesc;
use crate::access::tupdesc::TupleDesc;
use crate::nodes::params::ParamListInfo;
use crate::nodes::plannodes::PlannedStmt;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::CommandTag;
use crate::utils::plancache::CachedPlan;
use crate::utils::queryenvironment::QueryEnvironment;
use crate::utils::resowner::ResourceOwner;
use crate::utils::snapshot::SnapshotData;
use crate::utils::tuplestore::Tuplestorestate;
use crate::utils::palloc::MemoryContext;

/// Execution strategy for a portal. POOR (sequential ordinal) -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalStrategy {
    OneSelect,
    OneReturning,
    OneModWith,
    UtilSelect,
    MultiQuery,
}

/// Portal lifecycle state. POOR (sequential ordinal) -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalStatus {
    New,     // freshly created
    Defined, // PortalDefineQuery done
    Ready,   // PortalStart complete, can run it
    Active,  // portal is running (can't delete it)
    Done,    // portal is finished (don't re-run it)
    Failed,  // portal got error (can't re-run it)
}

/// The C `Portal` is a pointer to `PortalData`; per the <A>Data convention the
/// value type is `PortalData` and the handle is a reference.
pub type Portal = *mut PortalData; // TODO(ptr)

// Snapshot is `Option<&mut SnapshotData>`; portals own/register a snapshot, so
// store it as an owned optional here and keep a TODO for the borrow model.
type PortalSnapshot = Option<Box<SnapshotData>>; // TODO(ptr)

/// Execution state of a single source-SQL query. In-memory: idiomatic Rust.
pub struct PortalData {
    // Bookkeeping data
    pub name: String,                  // portal's name
    pub prep_stmt_name: Option<String>, // source prepared statement, if any
    pub portal_context: MemoryContext, // subsidiary memory for portal
    pub resowner: Option<ResourceOwner>, // resources owned by portal
    pub cleanup: Option<fn(Portal)>,   // cleanup hook (was fn pointer)

    // Which subtransaction(s) the portal was created/used in.
    pub create_subid: SubTransactionId, // the creating subxact
    pub active_subid: SubTransactionId, // the last subxact with activity
    pub create_level: i32,              // creating subxact's nesting level

    // The query or queries the portal will execute.
    pub source_text: String,            // text of query (never empty as of 8.4)
    pub command_tag: CommandTag,        // command tag for original query
    pub qc: QueryCompletion,            // completion data for executed query
    pub stmts: Vec<PlannedStmt>,        // list of PlannedStmts
    pub cplan: Option<Box<CachedPlan>>, // CachedPlan, if stmts are from one

    pub portal_params: ParamListInfo,            // params to pass to query
    pub query_env: Option<Box<QueryEnvironment>>, // environment for query

    // Features/options
    pub strategy: PortalStrategy, // see PortalStrategy
    pub cursor_options: i32,      // DECLARE CURSOR option bits

    // Status data
    pub status: PortalStatus, // see PortalStatus
    pub portal_pinned: bool,  // a pinned portal can't be dropped
    pub auto_held: bool,      // auto-converted from pinned to held

    // If Some, Executor is active; call ExecutorEnd eventually.
    pub query_desc: Option<Box<QueryDesc>>, // info needed for executor invocation

    // If portal returns tuples, this is their tupdesc.
    pub tup_desc: TupleDesc,    // descriptor for result tuples; TODO(ptr)
    pub formats: Vec<i16>,      // a format code for each column

    // Outermost ActiveSnapshot for execution of the portal's queries.
    pub portal_snapshot: PortalSnapshot, // active snapshot, or None

    // Where we store tuples for a held cursor / RETURNING / MOD_WITH / UTIL_SELECT.
    pub hold_store: Option<Box<Tuplestorestate>>, // store for holdable cursors
    pub hold_context: MemoryContext,              // memory containing holdStore

    // Snapshot under which tuples in the holdStore were read.
    pub hold_snapshot: PortalSnapshot, // registered snapshot, or None

    // Current cursor position. portalPos is zero before the first row.
    pub at_start: bool,
    pub at_end: bool,
    pub portal_pos: u64,

    // Presentation data, primarily for the pg_cursors system view.
    pub creation_time: TimestampTz, // time at which this portal was defined
    pub visible: bool,              // include this portal in pg_cursors?
}

// PortalIsValid(p) -> p is non-null. The C macro maps to an Option/null check at
// the handle's call site; no standalone item needed.

/// Portal lookup table (was `PortalHashTable`, a dynahash keyed by name).
pub type PortalHashTable = HashMap<String, Portal>;

// Prototypes for functions in utils/mmgr/portalmem.c

pub fn EnablePortalManager() {
    unimplemented!()
}

pub fn PreCommit_Portals(_is_prepare: bool) -> bool {
    unimplemented!()
}

pub fn AtAbort_Portals() {
    unimplemented!()
}

pub fn AtCleanup_Portals() {
    unimplemented!()
}

pub fn PortalErrorCleanup() {
    unimplemented!()
}

pub fn AtSubCommit_Portals(
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
    _parent_level: i32,
    _parent_xact_owner: ResourceOwner,
) {
    unimplemented!()
}

pub fn AtSubAbort_Portals(
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
    _my_xact_owner: ResourceOwner,
    _parent_xact_owner: ResourceOwner,
) {
    unimplemented!()
}

pub fn AtSubCleanup_Portals(_my_subid: SubTransactionId) {
    unimplemented!()
}

pub fn CreatePortal(_name: &str, _allow_dup: bool, _dup_silent: bool) -> Portal {
    unimplemented!()
}

pub fn CreateNewPortal() -> Portal {
    unimplemented!()
}

pub fn PinPortal(_portal: Portal) {
    unimplemented!()
}

pub fn UnpinPortal(_portal: Portal) {
    unimplemented!()
}

pub fn MarkPortalActive(_portal: Portal) {
    unimplemented!()
}

pub fn MarkPortalDone(_portal: Portal) {
    unimplemented!()
}

pub fn MarkPortalFailed(_portal: Portal) {
    unimplemented!()
}

pub fn PortalDrop(_portal: Portal, _is_top_commit: bool) {
    unimplemented!()
}

/// None if no portal of that name exists.
pub fn GetPortalByName(_name: &str) -> Option<Portal> {
    unimplemented!()
}

pub fn PortalDefineQuery(
    _portal: Portal,
    _prep_stmt_name: Option<&str>,
    _source_text: &str,
    _command_tag: CommandTag,
    _stmts: Vec<PlannedStmt>,
    _cplan: Option<Box<CachedPlan>>,
) {
    unimplemented!()
}

pub fn PortalGetPrimaryStmt(_portal: Portal) -> *mut PlannedStmt {
    unimplemented!() // TODO(ptr)
}

pub fn PortalCreateHoldStore(_portal: Portal) {
    unimplemented!()
}

pub fn PortalHashTableDeleteAll() {
    unimplemented!()
}

pub fn ThereAreNoReadyPortals() -> bool {
    unimplemented!()
}

pub fn HoldPinnedPortals() {
    unimplemented!()
}

pub fn ForgetPortalSnapshots() {
    unimplemented!()
}
