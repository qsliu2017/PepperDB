//! Translated from PostgreSQL src/include/catalog/pg_subscription_rel.h

use crate::access::xlogdefs::XLogRecPtr;
use crate::postgres_ext::Oid;

pub const SubscriptionRelRelationId: Oid = Oid::new(6102);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_subscription_rel {
    pub srsubid: Oid, // BKI_LOOKUP(pg_subscription)
    pub srrelid: Oid, // BKI_LOOKUP(pg_class)
    pub srsubstate: i8, // char
    // CATALOG_VARLEN (not in fixed part) -- nullable field (BKI_FORCE_NULL):
    pub srsublsn: XLogRecPtr,
}

pub type Form_pg_subscription_rel = *mut FormData_pg_subscription_rel; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_subscription_rel_srrelid_srsubid_index, 6117, SubscriptionRelSrrelidSrsubidIndexId)
// MAKE_SYSCACHE(SUBSCRIPTIONRELMAP, pg_subscription_rel_srrelid_srsubid_index, 64)

// EXPOSE_TO_CLIENT_CODE -- substate constants

pub const SUBREL_STATE_INIT: i8 = b'i' as i8;
pub const SUBREL_STATE_DATASYNC: i8 = b'd' as i8;
pub const SUBREL_STATE_FINISHEDCOPY: i8 = b'f' as i8;
pub const SUBREL_STATE_SYNCDONE: i8 = b's' as i8;
pub const SUBREL_STATE_READY: i8 = b'r' as i8;

// Never stored in the catalog; used only for IPC.
pub const SUBREL_STATE_UNKNOWN: i8 = b'\0' as i8;
pub const SUBREL_STATE_SYNCWAIT: i8 = b'w' as i8;
pub const SUBREL_STATE_CATCHUP: i8 = b'c' as i8;

pub struct SubscriptionRelState {
    pub relid: Oid,
    pub lsn: XLogRecPtr,
    pub state: i8,
}

pub fn AddSubscriptionRelState(
    _subid: Oid,
    _relid: Oid,
    _state: i8,
    _sublsn: XLogRecPtr,
    _retain_lock: bool,
) {
    unimplemented!()
}

pub fn UpdateSubscriptionRelState(
    _subid: Oid,
    _relid: Oid,
    _state: i8,
    _sublsn: XLogRecPtr,
    _already_locked: bool,
) {
    unimplemented!()
}

// out-param XLogRecPtr *sublsn -> returned in the tuple.
pub fn GetSubscriptionRelState(_subid: Oid, _relid: Oid) -> (i8, XLogRecPtr) {
    unimplemented!()
}

pub fn RemoveSubscriptionRel(_subid: Oid, _relid: Oid) {
    unimplemented!()
}

pub fn HasSubscriptionRelations(_subid: Oid) -> bool {
    unimplemented!()
}

pub fn GetSubscriptionRelations(_subid: Oid, _not_ready: bool) -> Vec<SubscriptionRelState> {
    unimplemented!()
}
