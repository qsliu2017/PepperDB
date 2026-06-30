//! Translated from PostgreSQL src/include/catalog/pg_subscription.h

#![allow(
    clippy::boxed_local,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::{text, NameData};
use crate::postgres_ext::Oid;

// BKI_SHARED_RELATION BKI_ROWTYPE_OID(6101,SubscriptionRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const SubscriptionRelationId: Oid = Oid::new(6100);
pub const SubscriptionRelation_Rowtype_Id: Oid = Oid::new(6101);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_subscription {
    pub oid: Oid,
    pub subdbid: Oid, // BKI_LOOKUP(pg_database)
    pub subskiplsn: XLogRecPtr,
    pub subname: NameData,
    pub subowner: Oid, // BKI_LOOKUP(pg_authid)
    pub subenabled: bool,
    pub subbinary: bool,
    pub substream: i8,        // char; LOGICALREP_STREAM_xxx
    pub subtwophasestate: i8, // char
    pub subdisableonerr: bool,
    pub subpasswordrequired: bool,
    pub subrunasowner: bool,
    pub subfailover: bool,
    // CATALOG_VARLEN (not in fixed part) -- variable-length fields:
    pub subconninfo: text,           // BKI_FORCE_NOT_NULL
    pub subslotname: NameData,       // BKI_FORCE_NULL
    pub subsynccommit: text,         // BKI_FORCE_NOT_NULL
    pub subpublications: [text; 1],  // text[1] BKI_FORCE_NOT_NULL
    pub suborigin: text,             // BKI_DEFAULT(LOGICALREP_ORIGIN_ANY)
}

pub type Form_pg_subscription = *mut FormData_pg_subscription; // TODO(ptr)

// DECLARE_TOAST_WITH_MACRO(pg_subscription, 4183, 4184, PgSubscriptionToastTable, PgSubscriptionToastIndex)
// DECLARE_UNIQUE_INDEX_PKEY(pg_subscription_oid_index, 6114, SubscriptionObjectIndexId)
// DECLARE_UNIQUE_INDEX(pg_subscription_subname_index, 6115, SubscriptionNameIndexId)
// MAKE_SYSCACHE(SUBSCRIPTIONOID, pg_subscription_oid_index, 4)
// MAKE_SYSCACHE(SUBSCRIPTIONNAME, pg_subscription_subname_index, 4)

// In-memory representation (not on-disk): idiomatic Rust.
pub struct Subscription {
    pub oid: Oid,
    pub dbid: Oid,
    pub skiplsn: XLogRecPtr,
    pub name: String,
    pub owner: Oid,
    pub ownersuperuser: bool,
    pub enabled: bool,
    pub binary: bool,
    pub stream: i8,
    pub twophasestate: i8,
    pub disableonerr: bool,
    pub passwordrequired: bool,
    pub runasowner: bool,
    pub failover: bool,
    pub conninfo: String,
    pub slotname: Option<String>,
    pub synccommit: String,
    pub publications: Vec<String>,
    pub origin: String,
}

// EXPOSE_TO_CLIENT_CODE

// two_phase tri-state values.
pub const LOGICALREP_TWOPHASE_STATE_DISABLED: i8 = b'd' as i8;
pub const LOGICALREP_TWOPHASE_STATE_PENDING: i8 = b'p' as i8;
pub const LOGICALREP_TWOPHASE_STATE_ENABLED: i8 = b'e' as i8;

pub const LOGICALREP_ORIGIN_NONE: &str = "none";
pub const LOGICALREP_ORIGIN_ANY: &str = "any";

pub const LOGICALREP_STREAM_OFF: i8 = b'f' as i8;
pub const LOGICALREP_STREAM_ON: i8 = b't' as i8;
pub const LOGICALREP_STREAM_PARALLEL: i8 = b'p' as i8;

pub fn GetSubscription(_subid: Oid, _missing_ok: bool) -> Option<Box<Subscription>> {
    unimplemented!()
}

pub fn FreeSubscription(_sub: Box<Subscription>) {
    unimplemented!()
}

pub fn DisableSubscription(_subid: Oid) {
    unimplemented!()
}

pub fn CountDBSubscriptions(_dbid: Oid) -> i32 {
    unimplemented!()
}

// StringInfo dest -> &mut String (stringinfo.h is tombstoned).
pub fn GetPublicationsStr(_publications: &[String], _dest: &mut String, _quote_literal: bool) {
    unimplemented!()
}
