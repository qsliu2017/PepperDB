//! Translated from PostgreSQL src/include/replication/logicalrelation.h

use crate::access::attmap::AttrMap;
use crate::replication::logicalproto::{LogicalRepRelation, LogicalRepRelId};
use crate::storage::lock::LOCKMODE;
use crate::utils::rel::Relation;
use crate::postgres_ext::Oid;

// XLogRecPtr (statelsn) -- c.h alias not yet imported in-tree; it is a u64 LSN.
type XLogRecPtr = u64;

/// Relation mapping entry for logical replication. In-memory cache entry.
pub struct LogicalRepRelMapEntry {
    pub remoterel: LogicalRepRelation, // key is remoterel.remoteid

    /// When false, revalidate all derived info at next logicalrep_rel_open.
    pub localrelvalid: bool,

    // Mapping to local relation.
    pub localreloid: Oid,          // local relation id
    pub localrel: Relation,        // relcache entry (NULL when closed); TODO(ptr)
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

pub fn logicalrep_partition_open(_root: &mut LogicalRepRelMapEntry, _partrel: Relation,
                                 _map: &AttrMap) -> *mut LogicalRepRelMapEntry {
    unimplemented!() // TODO(ptr)
}

pub fn logicalrep_rel_close(_rel: &mut LogicalRepRelMapEntry, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn IsIndexUsableForReplicaIdentityFull(_idxrel: Relation, _attrmap: &AttrMap) -> bool {
    unimplemented!()
}

/// InvalidOid when none -> Option.
pub fn GetRelationIdentityOrPK(_rel: Relation) -> Option<Oid> {
    unimplemented!()
}
