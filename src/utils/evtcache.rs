//! Translated from PostgreSQL src/include/utils/evtcache.h

use crate::nodes::bitmapset::Bitmapset;
use crate::postgres_ext::Oid;

pub enum EventTriggerEvent {
    DDLCommandStart,
    DDLCommandEnd,
    SQLDrop,
    TableRewrite,
    Login,
}

/// Cached event trigger entry. In-memory.
pub struct EventTriggerCacheItem {
    /// function to be called
    pub fnoid: Oid,
    /// as SESSION_REPLICATION_ROLE_*
    pub enabled: i8,
    /// command tags, or None if empty
    pub tagset: Option<Bitmapset>,
}

// List* -> Vec.
pub fn EventCacheLookup(_event: EventTriggerEvent) -> Vec<EventTriggerCacheItem> {
    unimplemented!()
}
