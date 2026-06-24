//! Translated from PostgreSQL src/include/commands/cluster.h

use bitflags::bitflags;

use crate::c::{MultiXactId, TransactionId};
use crate::nodes::parsenodes::ClusterStmt;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::storage::lock::LOCKMODE;
use crate::utils::relcache::Relation;

bitflags! {
    /// flag bits for ClusterParams->options
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ClusterOptions: u32 {
        const VERBOSE = 0x01; // print progress info
        const RECHECK = 0x02; // recheck relation state
        const RECHECK_ISCLUSTERED = 0x04; // recheck relation state for indisclustered
    }
}

/// options for CLUSTER
pub struct ClusterParams {
    pub options: ClusterOptions, // bitmask of CLUOPT_*
}

pub fn cluster(_pstate: &mut ParseState, _stmt: &ClusterStmt, _is_top_level: bool) {
    unimplemented!()
}

pub fn cluster_rel(_old_heap: Relation, _index_oid: Oid, _params: &ClusterParams) {
    unimplemented!()
}

pub fn check_index_is_clusterable(_old_heap: Relation, _index_oid: Oid, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn mark_index_clustered(_rel: Relation, _index_oid: Oid, _is_internal: bool) {
    unimplemented!()
}

pub fn make_new_heap(
    _oid_old_heap: Oid,
    _new_tablespace: Oid,
    _new_access_method: Oid,
    _relpersistence: u8,
    _lockmode: LOCKMODE,
) -> Oid {
    unimplemented!()
}

pub fn finish_heap_swap(
    _oid_old_heap: Oid,
    _oid_new_heap: Oid,
    _is_system_catalog: bool,
    _swap_toast_by_content: bool,
    _check_constraints: bool,
    _is_internal: bool,
    _frozen_xid: TransactionId,
    _cutoff_multi: MultiXactId,
    _new_relpersistence: u8,
) {
    unimplemented!()
}
