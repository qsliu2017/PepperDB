//! Translated from PostgreSQL src/include/replication/logicalrelation.h

use crate::access::attmap::AttrMap;
use crate::replication::logicalproto::{LogicalRepRelation, LogicalRepRelId};
use crate::storage::lock::LOCKMODE;
use crate::postgres_ext::Oid;
use std::sync::Arc;
use crate::utils::rel::RelationData;

// XLogRecPtr (statelsn) -- c.h alias not yet imported in-tree; it is a u64 LSN.
type XLogRecPtr = u64;

/// Arc<RelationData> mapping entry for logical replication. In-memory cache entry.
pub struct LogicalRepRelMapEntry {
    pub remoterel: LogicalRepRelation, // key is remoterel.remoteid

    /// When false, revalidate all derived info at next logicalrep_rel_open.
    pub localrelvalid: bool,

    // Mapping to local relation.
    pub localreloid: Oid,          // local relation id
    pub localrel: Arc<RelationData>,        // relcache entry (NULL when closed); TODO(ptr)
    pub attrmap: Option<Box<AttrMap>>, // local->remote attribute map
    pub updatable: bool,           // can apply updates/deletes?
    pub localindexoid: Oid,        // index to use, or InvalidOid if none

    // Sync state.
    pub state: u8,            // was char
    pub statelsn: XLogRecPtr,
}

pub fn logicalrep_relmap_update(_remoterel: &LogicalRepRelation) {
    unimplemented!()
}

pub fn logicalrep_partmap_reset_relmap(_remoterel: &LogicalRepRelation) {
    unimplemented!()
}

pub fn logicalrep_rel_open(_remoteid: LogicalRepRelId, _lockmode: LOCKMODE)
    -> *mut LogicalRepRelMapEntry {
    unimplemented!() // TODO(ptr)
}

pub fn logicalrep_partition_open(_root: &mut LogicalRepRelMapEntry, _partrel: &RelationData,
                                 _map: &AttrMap) -> *mut LogicalRepRelMapEntry {
    unimplemented!() // TODO(ptr)
}

pub fn logicalrep_rel_close(_rel: &mut LogicalRepRelMapEntry, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn IsIndexUsableForReplicaIdentityFull(_idxrel: &RelationData, _attrmap: &AttrMap) -> bool {
    unimplemented!()
}

/// InvalidOid when none -> Option.
pub fn GetRelationIdentityOrPK(_rel: &RelationData) -> Option<Oid> {
    unimplemented!()
}
