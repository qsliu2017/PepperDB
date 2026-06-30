//! Translated from PostgreSQL src/include/catalog/pg_event_trigger.h

use crate::c::{text, NameData};
use crate::postgres_ext::Oid;

pub const EventTriggerRelationId: Oid = Oid::new(3466);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_event_trigger {
    pub oid: Oid,
    pub evtname: NameData,
    pub evtevent: NameData,
    pub evtowner: Oid, // BKI_LOOKUP(pg_authid)
    pub evtfoid: Oid,  // BKI_LOOKUP(pg_proc)
    pub evtenabled: i8,
    // CATALOG_VARLEN (not in fixed part):
    pub evttags: [text; 1],
}

pub type Form_pg_event_trigger = *mut FormData_pg_event_trigger; // TODO(ptr)

// DECLARE_TOAST(pg_event_trigger, 4145, 4146)
// DECLARE_UNIQUE_INDEX(pg_event_trigger_evtname_index, 3467, EventTriggerNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_event_trigger_oid_index, 3468, EventTriggerOidIndexId)
// MAKE_SYSCACHE(EVENTTRIGGERNAME, pg_event_trigger_evtname_index, 8)
// MAKE_SYSCACHE(EVENTTRIGGEROID, pg_event_trigger_oid_index, 8)

