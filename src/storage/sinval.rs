//! Translated from PostgreSQL src/include/storage/sinval.h
//!
//! STUB. Shared cache-invalidation messaging. PG broadcasts these through a
//! shared-memory ring buffer (SI message queue) consumed by every backend.
//! Under the single-process async model that becomes a shared queue plus
//! per-task wakeups; the message *shapes* are translated here, the transport is
//! not yet implemented.

use crate::postgres_ext::Oid;
use crate::storage::relfilelocator::RelFileLocator;

/// Invalidate a specific tuple in a specific catcache.
///
/// The C union keys on a leading `int8 id`: 0+ is a catcache id, negative values
/// are the `SHAREDINVAL*_ID` type codes below. In Rust that discriminated union
/// becomes a tagged enum (`SharedInvalidationMessage`); the per-variant structs
/// keep their own fields (the `id` field disappears - the enum tag carries it,
/// except for catcache which keeps the cache id as data).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalCatcacheMsg {
    pub id: i8,            // cache ID (0+); kept as data, it identifies the cache
    pub db_id: Oid,        // database ID, or 0 if a shared relation
    pub hash_value: u32,   // hash value of key for this catcache
}

/// Type code: invalidate all catcache entries from a given system catalog.
pub const SHAREDINVALCATALOG_ID: i8 = -1;

/// Invalidate all catcache entries from a given system catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalCatalogMsg {
    pub db_id: Oid,  // database ID, or 0 if a shared catalog
    pub cat_id: Oid, // ID of catalog whose contents are invalid
}

/// Type code: invalidate a relcache entry (relId 0 = whole relcache).
pub const SHAREDINVALRELCACHE_ID: i8 = -2;

/// Invalidate a relcache entry for a specific logical relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalRelcacheMsg {
    pub db_id: Oid,  // database ID, or 0 if a shared relation
    pub rel_id: Oid, // relation ID, or 0 if whole relcache
}

/// Type code: invalidate an smgr cache entry for a physical relation.
pub const SHAREDINVALSMGR_ID: i8 = -3;

/// Invalidate an smgr cache entry for a specific physical relation.
///
/// C packs `backend_hi`/`backend_lo` to fit a backend procno into 16 bytes; in
/// memory that packing is irrelevant, so keep the procno as a single field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalSmgrMsg {
    pub backend: i32,             // backend procno, if temprel (-1 if not)
    pub rlocator: RelFileLocator, // spcOid, dbOid, relNumber
}

/// Type code: invalidate the mapped-relation mapping for a database.
pub const SHAREDINVALRELMAP_ID: i8 = -4;

/// Invalidate the mapped-relation mapping for a given database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalRelmapMsg {
    pub db_id: Oid, // database ID, or 0 for shared catalogs
}

/// Type code: invalidate any saved snapshot used to scan a relation.
pub const SHAREDINVALSNAPSHOT_ID: i8 = -5;

/// Invalidate any saved snapshot that might be used to scan a given relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalSnapshotMsg {
    pub db_id: Oid,  // database ID, or 0 if a shared relation
    pub rel_id: Oid, // relation ID
}

/// Type code: invalidate a RelationSyncCache entry for a relation.
pub const SHAREDINVALRELSYNC_ID: i8 = -6;

/// Invalidate a RelationSyncCache entry for a specific relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedInvalRelSyncMsg {
    pub db_id: Oid,  // database ID
    pub relid: Oid,  // relation ID, or 0 if whole RelationSyncCache
}

/// A single shared-invalidation message. The C `union` keyed on a leading
/// `int8 id` (function-mapping section 6.2: tagged union -> enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedInvalidationMessage {
    Catcache(SharedInvalCatcacheMsg),
    Catalog(SharedInvalCatalogMsg),
    Relcache(SharedInvalRelcacheMsg),
    Smgr(SharedInvalSmgrMsg),
    Relmap(SharedInvalRelmapMsg),
    Snapshot(SharedInvalSnapshotMsg),
    RelSync(SharedInvalRelSyncMsg),
}

/// Counter of messages processed; don't worry about overflow. Defined in the
/// backend module as an `AtomicU64`; read via the accessor below (no `static mut`).
#[deprecated(note = "use crate::backend::storage::ipc::sinval::shared_invalid_message_counter()")]
#[inline]
pub fn shared_invalid_message_counter() -> u64 {
    crate::backend::storage::ipc::sinval::shared_invalid_message_counter()
}

/// Whether a catchup interrupt is pending (was `volatile sig_atomic_t`). Now the
/// per-task ProcSignal slot `CatchupInterrupt` reason bit; read via the slot.
#[deprecated(note = "use the per-task ProcSignal slot CatchupInterrupt reason bit")]
#[inline]
pub fn catchup_interrupt_pending() -> bool {
    crate::backend::storage::ipc::procsignal::try_current().is_some_and(|slot| {
        slot.reason_is_set(crate::storage::procsignal::ProcSignalReason::CatchupInterrupt)
    })
}

// sinval.c function bodies live in the backend module; rewire the header stubs to
// `pub use` (non-type-centric global-state fns).
pub use crate::backend::storage::ipc::sinval::{
    HandleCatchupInterrupt as handle_catchup_interrupt,
    ProcessCatchupInterrupt as process_catchup_interrupt,
    ReceiveSharedInvalidMessages as receive_shared_invalid_messages,
    SendSharedInvalidMessages as send_shared_invalid_messages,
};

// These sinval.h-origin functions are defined in inval.c; rewire the header stubs
// to `pub use` the backend implementations (under the snake_case header names).
pub use crate::backend::utils::cache::inval::{
    inplace_get_invalidation_messages as inplaceGetInvalidationMessages,
    local_execute_invalidation_message as LocalExecuteInvalidationMessage,
    process_committed_invalidation_messages as ProcessCommittedInvalidationMessages,
    xact_get_committed_invalidation_messages as xactGetCommittedInvalidationMessages,
};
