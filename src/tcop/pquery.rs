//! Translated from PostgreSQL src/include/tcop/pquery.h
//!
//! Prototypes for pquery.c (portal execution). `List *` of statements ->
//! `&[Box<Node>]`; the C `ActivePortal` global -> a `static mut` placeholder.

use crate::nodes::nodes::Node;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::FetchDirection;
use crate::nodes::plannodes::PlannedStmt;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::dest::DestReceiver;
use crate::utils::portal::{Portal, PortalStrategy};
use crate::utils::snapshot::Snapshot;

// The currently active Portal (C `ActivePortal` global). TODO(global): move to
// session/task state.
pub static mut ActivePortal: Portal = core::ptr::null_mut();

pub fn ChoosePortalStrategy(_stmts: &[Box<Node>]) -> PortalStrategy {
    unimplemented!()
}

pub fn FetchPortalTargetList(_portal: Portal) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn FetchStatementTargetList(_stmt: &Node) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn PortalStart(
    _portal: Portal,
    _params: ParamListInfo,
    _eflags: i32,
    _snapshot: Snapshot,
) {
    unimplemented!()
}

pub fn PortalSetResultFormat(_portal: Portal, _formats: &[i16]) {
    unimplemented!()
}

/// PortalRun: execute a portal. C returns "all rows fetched" as a bool.
pub fn PortalRun(
    _portal: Portal,
    _count: i64,
    _is_top_level: bool,
    _dest: &mut dyn DestReceiver,
    _altdest: &mut dyn DestReceiver,
    _qc: Option<&mut QueryCompletion>,
) -> bool {
    unimplemented!()
}

pub fn PortalRunFetch(
    _portal: Portal,
    _fdirection: FetchDirection,
    _count: i64,
    _dest: &mut dyn DestReceiver,
) -> u64 {
    unimplemented!()
}

pub fn PlannedStmtRequiresSnapshot(_pstmt: &PlannedStmt) -> bool {
    unimplemented!()
}

pub fn EnsurePortalSnapshotExists() {
    unimplemented!()
}
