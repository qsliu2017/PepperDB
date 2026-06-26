//! Translated from PostgreSQL src/include/commands/portalcmds.h

#![allow(
    clippy::boxed_local,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{DeclareCursorStmt, FetchStmt};
use crate::parser::parse_node::ParseState;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::dest::DestReceiver;
use crate::utils::portal::Portal;

pub fn PerformCursorOpen(
    _pstate: &mut ParseState,
    _cstmt: &mut DeclareCursorStmt,
    _params: ParamListInfo,
    _isTopLevel: bool,
) {
    unimplemented!()
}

pub fn PerformPortalFetch(
    _stmt: &mut FetchStmt,
    _dest: &mut dyn DestReceiver,
    _qc: Option<&mut QueryCompletion>,
) {
    unimplemented!()
}

pub fn PerformPortalClose(_name: &str) {
    unimplemented!()
}

pub fn PortalCleanup(_portal: Portal) {
    unimplemented!()
}

pub fn PersistHoldablePortal(_portal: Portal) {
    unimplemented!()
}
