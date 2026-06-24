//! Translated from PostgreSQL src/include/commands/matview.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::RefreshMatViewStmt;
use crate::postgres_ext::Oid;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::dest::DestReceiver;
use crate::utils::relcache::Relation;

pub fn SetMatViewPopulatedState(_relation: Relation, _newstate: bool) {
    unimplemented!()
}

pub fn ExecRefreshMatView(
    _stmt: &RefreshMatViewStmt,
    _queryString: &str,
    _qc: &mut QueryCompletion,
) -> ObjectAddress {
    unimplemented!()
}

pub fn RefreshMatViewByOid(
    _matviewOid: Oid,
    _is_create: bool,
    _skipData: bool,
    _concurrent: bool,
    _queryString: &str,
    _qc: &mut QueryCompletion,
) -> ObjectAddress {
    unimplemented!()
}

// TODO(ptr): the concrete receiver type is chosen at runtime; revisit ownership.
pub fn CreateTransientRelDestReceiver(_transientoid: Oid) -> Box<dyn DestReceiver> {
    unimplemented!()
}

pub fn MatViewIncrementalMaintenanceIsEnabled() -> bool {
    unimplemented!()
}
