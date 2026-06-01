//! replication/logicalrelation.h - Relation definitions for logical replication relation mapping.

use std::ffi::c_char;

use crate::access::common::attmap::AttrMap;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LOCKMODE;
use crate::utils::rel::Relation;

// TODO: dedup when replication/logicalproto.h lands
pub type LogicalRepRelation = std::ffi::c_void;
// TODO: dedup when replication/logicalproto.h lands
pub type LogicalRepRelId = Oid;

#[repr(C)]
pub struct LogicalRepRelMapEntry {
    pub remoterel: LogicalRepRelation, // key is remoterel.remoteid

    // Validity flag -- when false, revalidate all derived info at next
    // logicalrep_rel_open.  (While the localrel is open, we assume our lock
    // on that rel ensures the info remains good.)
    pub localrelvalid: bool,

    // Mapping to local relation.
    pub localreloid: Oid,        // local relation id
    pub localrel: Relation,      // relcache entry (NULL when closed)
    pub attrmap: *mut AttrMap,   // map of local attributes to remote ones
    pub updatable: bool,         // Can apply updates/deletes?
    pub localindexoid: Oid,      // which index to use, or InvalidOid if none

    // Sync state.
    pub state: c_char,
    pub statelsn: XLogRecPtr,
}

pub unsafe fn logicalrep_relmap_update(remoterel: *mut LogicalRepRelation) {
    unimplemented!()
}

pub unsafe fn logicalrep_partmap_reset_relmap(remoterel: *mut LogicalRepRelation) {
    unimplemented!()
}

pub unsafe fn logicalrep_rel_open(
    remoteid: LogicalRepRelId,
    lockmode: LOCKMODE,
) -> *mut LogicalRepRelMapEntry {
    unimplemented!()
}

pub unsafe fn logicalrep_partition_open(
    root: *mut LogicalRepRelMapEntry,
    partrel: Relation,
    map: *mut AttrMap,
) -> *mut LogicalRepRelMapEntry {
    unimplemented!()
}

pub unsafe fn logicalrep_rel_close(rel: *mut LogicalRepRelMapEntry, lockmode: LOCKMODE) {
    unimplemented!()
}

pub unsafe fn IsIndexUsableForReplicaIdentityFull(idxrel: Relation, attrmap: *mut AttrMap) -> bool {
    unimplemented!()
}

pub unsafe fn GetRelationIdentityOrPK(rel: Relation) -> Oid {
    unimplemented!()
}
