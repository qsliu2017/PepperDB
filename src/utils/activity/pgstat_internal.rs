//! pgstat_internal.h - internal definitions for the cumulative statistics system.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! `src/include/utils/pgstat_internal.h`. These definitions are only needed by
//! files implementing statistics support (rather than ones reporting/querying
//! stats).
//!
//! NOTE: many of the types defined in this header (PgStat_HashKey,
//! PgStatShared_HashEntry, PgStatShared_Common, PgStat_EntryRef,
//! PgStat_KindInfo, PgStat_ShmemControl, PgStat_Snapshot, PgStat_LocalState,
//! PgStat_SubXactStatus, and the PgStatShared_* fixed/variable stat structs)
//! ALSO exist as a hand-picked SUBSET in `crate::utils::activity::pgstat`. This
//! file is the canonical full home; the duplicates there should be deduped by
//! the main agent. To avoid a conflicting redefinition while both exist, the
//! types here are written self-contained and stat payload types are imported
//! from `pgstat` where they already exist.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]

use std::ffi::{c_char, c_int, c_void};

use crate::c::{bits32, uint32, NameData, Size};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// pg_atomic_uint32 / pg_atomic_uint64
use crate::port::atomics::{pg_atomic_uint32, pg_atomic_uint64};

// dlist_node, dclist_head from lib/ilist
use crate::lib::ilist::{dclist_head, dlist_node};

// PgStat_Kind and custom-kind constants/predicates.
use crate::utils::pgstat_kind::{
    pgstat_is_kind_custom, PgStat_Kind, PGSTAT_KIND_BUILTIN_SIZE, PGSTAT_KIND_CUSTOM_MIN,
    PGSTAT_KIND_CUSTOM_SIZE,
};

// LWLock, TimestampTz, BACKEND_NUM_TYPES, SLRU_NUM_ELEMENTS and the concrete
// per-kind stat payload structs already live in the pgstat subset module.
//
// NOTE: `pgstat_get_kind_info`, `pgStatLocal`, and the PgStat_ShmemControl /
// PgStat_Snapshot / PgStat_LocalState types ALSO exist in the pgstat subset,
// but the subset versions are incomplete (e.g. they lack `custom_data`,
// `is_shutdown`, `hash_handle`). This header is the canonical FULL home, so
// those three names are (re)defined locally below and the subset versions
// should be deduped by the main agent. Only the leaf stat payload structs and
// shared scalar aliases are imported from the subset.
use crate::utils::activity::pgstat::{
    LWLock, PgStat_ArchiverStats, PgStat_BgWriterStats, PgStat_CheckpointerStats, PgStat_IO,
    PgStat_SLRUStats, PgStat_StatDBEntry, PgStat_StatFuncEntry, PgStat_StatReplSlotEntry,
    PgStat_StatSubEntry, PgStat_StatTabEntry, PgStat_WalStats, TimestampTz, BACKEND_NUM_TYPES,
    SLRU_NUM_ELEMENTS,
};

// ----------------------------------------------------------------------------
// Local stubs for types not yet ported anywhere.
// ----------------------------------------------------------------------------

// dsa.h / lib/dshash.h - dynamic shared memory + dshash. Not ported.
pub use crate::lib::dshash::dsa_pointer; // canonical (utils/dsa.h home: lib/dshash.rs)
pub type dsa_area = c_void; // TODO: dedup
pub type dshash_table = c_void; // TODO: dedup
pub use crate::lib::dshash::dshash_table_handle; // canonical (lib/dshash.h)

// pgstat.h types not present in the pgstat subset module.
// PgStat_Backend - per-backend statistics. TODO: dedup (canonical home: pgstat.h)
pub type PgStat_Backend = c_void;

// PgStat_TableXactStatus - per-(sub)xact tuple counts. TODO: dedup (pgstat.h)
pub type PgStat_TableXactStatus = c_void;

// PgStat_FetchConsistency - enum in pgstat.h. TODO: dedup.
pub type PgStat_FetchConsistency = c_int;

// MemoryContext - opaque memory context handle. TODO: dedup (utils/palloc.h).
pub type MemoryContext = *mut c_void;

// simplehash-generated snapshot hash table; opaque here. TODO: dedup.
pub type pgstat_snapshot_hash = c_void;

// ----------------------------------------------------------------------------
// Types related to shared memory storage of statistics.
// ----------------------------------------------------------------------------

/// struct for shared statistics hash entry key.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_HashKey {
    /// statistics entry kind
    pub kind: PgStat_Kind,
    /// database ID. InvalidOid for shared objects.
    pub dboid: Oid,
    /// object ID (table, function, etc.), or identifier.
    pub objid: u64,
}

/// Shared statistics hash entry. Doesn't itself contain any stats, but points
/// to them (with ->body). That allows the stats entries themselves to be of
/// variable size.
#[repr(C)]
pub struct PgStatShared_HashEntry {
    /// hash key
    pub key: PgStat_HashKey,

    /// If dropped is set, backends need to release their references so that the
    /// memory for the entry can be freed. No new references may be made once
    /// marked as dropped.
    pub dropped: bool,

    /// Refcount managing lifetime of the entry itself (as opposed to the dshash
    /// entry pointing to it).
    pub refcount: pg_atomic_uint32,

    /// Counter tracking the number of times the entry has been reused.
    pub generation: pg_atomic_uint32,

    /// Pointer to shared stats. The stats entry always starts with
    /// PgStatShared_Common, embedded in a larger struct containing the
    /// PgStat_Kind specific stats fields.
    pub body: dsa_pointer,
}

/// Common header struct for PgStatShared_*.
#[repr(C)]
pub struct PgStatShared_Common {
    /// just a validity cross-check
    pub magic: uint32,
    /// lock protecting stats contents (i.e. data following the header)
    pub lock: LWLock,
}

/// A backend local reference to a shared stats entry. As long as at least one
/// such reference exists, the shared stats entry will not be released.
///
/// If there are pending stats update to the shared stats, these are stored in
/// ->pending.
#[repr(C)]
pub struct PgStat_EntryRef {
    /// Pointer to the PgStatShared_HashEntry entry in the shared stats
    /// hashtable.
    pub shared_entry: *mut PgStatShared_HashEntry,

    /// Pointer to the stats data (i.e. PgStatShared_HashEntry->body), resolved
    /// as a local pointer, to avoid repeated dsa_get_address() calls.
    pub shared_stats: *mut PgStatShared_Common,

    /// Copy of PgStatShared_HashEntry->generation, keeping locally track of the
    /// shared stats entry "generation" retrieved (number of times reused).
    pub generation: uint32,

    /// Pending statistics data that will need to be flushed to shared memory
    /// stats eventually.
    pub pending: *mut c_void,
    /// membership in pgStatPending list
    pub pending_node: dlist_node,
}

/// Some stats changes are transactional. To maintain those, a stack of
/// PgStat_SubXactStatus entries is maintained, which contain data pertaining to
/// the current transaction and its active subtransactions.
#[repr(C)]
pub struct PgStat_SubXactStatus {
    /// subtransaction nest level
    pub nest_level: c_int,

    /// higher-level subxact if any
    pub prev: *mut PgStat_SubXactStatus,

    /// Statistics for transactionally dropped objects need to be
    /// transactionally dropped as well.
    pub pending_drops: dclist_head,

    /// Tuple insertion/deletion counts for an open transaction can't be
    /// propagated into PgStat_TableStatus counters until we know if it is going
    /// to commit or abort. head of list for this subxact
    pub first: *mut PgStat_TableXactStatus,
}

// ----------------------------------------------------------------------------
// Metadata for a specific kind of statistics.
// ----------------------------------------------------------------------------

// Callback type aliases for PgStat_KindInfo function pointers.
pub type PgStat_KindInfoInitBackendCb = Option<unsafe extern "C" fn()>;
pub type PgStat_KindInfoFlushPendingCb =
    Option<unsafe extern "C" fn(sr: *mut PgStat_EntryRef, nowait: bool) -> bool>;
pub type PgStat_KindInfoDeletePendingCb =
    Option<unsafe extern "C" fn(sr: *mut PgStat_EntryRef)>;
pub type PgStat_KindInfoResetTimestampCb =
    Option<unsafe extern "C" fn(header: *mut PgStatShared_Common, ts: TimestampTz)>;
pub type PgStat_KindInfoToSerializedNameCb = Option<
    unsafe extern "C" fn(
        key: *const PgStat_HashKey,
        header: *const PgStatShared_Common,
        name: *mut NameData,
    ),
>;
pub type PgStat_KindInfoFromSerializedNameCb =
    Option<unsafe extern "C" fn(name: *const NameData, key: *mut PgStat_HashKey) -> bool>;
pub type PgStat_KindInfoInitShmemCb = Option<unsafe extern "C" fn(stats: *mut c_void)>;
pub type PgStat_KindInfoFlushStaticCb = Option<unsafe extern "C" fn(nowait: bool) -> bool>;
pub type PgStat_KindInfoResetAllCb = Option<unsafe extern "C" fn(ts: TimestampTz)>;
pub type PgStat_KindInfoSnapshotCb = Option<unsafe extern "C" fn()>;

/// Metadata for a specific kind of statistics.
///
/// The C struct opens with three single-bit bitfields (fixed_amount,
/// accessed_across_databases, write_to_file) packed into one backing int.
/// They are represented here as a single `flags` backing field plus accessor
/// methods.
#[repr(C)]
pub struct PgStat_KindInfo {
    /// Backing storage for the three 1-bit C bitfields:
    ///   bit 0: fixed_amount
    ///   bit 1: accessed_across_databases
    ///   bit 2: write_to_file
    pub flags: u32,

    /// The size of an entry in the shared stats hash table (pointed to by
    /// PgStatShared_HashEntry->body). For fixed-numbered statistics, this is
    /// the size of an entry in PgStat_ShmemControl->custom_data.
    pub shared_size: uint32,

    /// The offset of the statistics struct in the cached statistics snapshot
    /// PgStat_Snapshot, for fixed-numbered statistics.
    pub snapshot_ctl_off: uint32,

    /// The offset of the statistics struct in the containing shared memory
    /// control structure PgStat_ShmemControl, for fixed-numbered statistics.
    pub shared_ctl_off: uint32,

    /// The offset of statistics inside the shared stats entry.
    pub shared_data_off: uint32,
    /// The size of statistics inside the shared stats entry.
    pub shared_data_len: uint32,

    /// The size of the pending data for this kind. 0 signals that an entry of
    /// this kind should never have a pending entry.
    pub pending_size: uint32,

    /// Perform custom actions when initializing a backend. Optional.
    pub init_backend_cb: PgStat_KindInfoInitBackendCb,

    /// For variable-numbered stats: flush pending stats.
    pub flush_pending_cb: PgStat_KindInfoFlushPendingCb,

    /// For variable-numbered stats: delete pending stats. Optional.
    pub delete_pending_cb: PgStat_KindInfoDeletePendingCb,

    /// For variable-numbered stats: reset the reset timestamp. Optional.
    pub reset_timestamp_cb: PgStat_KindInfoResetTimestampCb,

    /// For variable-numbered stats. Optional.
    pub to_serialized_name: PgStat_KindInfoToSerializedNameCb,
    pub from_serialized_name: PgStat_KindInfoFromSerializedNameCb,

    /// For fixed-numbered statistics: Initialize shared memory state.
    pub init_shmem_cb: PgStat_KindInfoInitShmemCb,

    /// For fixed-numbered or variable-numbered statistics: Flush pending stats
    /// entries, for stats kinds that do not use PgStat_EntryRef->pending.
    pub flush_static_cb: PgStat_KindInfoFlushStaticCb,

    /// For fixed-numbered statistics: Reset All.
    pub reset_all_cb: PgStat_KindInfoResetAllCb,

    /// For fixed-numbered statistics: Build snapshot for entry
    pub snapshot_cb: PgStat_KindInfoSnapshotCb,

    /// name of the kind of stats
    pub name: *const c_char,
}

impl PgStat_KindInfo {
    /// Do a fixed number of stats objects exist for this kind of stats.
    #[inline]
    pub fn fixed_amount(&self) -> bool {
        (self.flags & 0x1) != 0
    }
    #[inline]
    pub fn set_fixed_amount(&mut self, v: bool) {
        if v {
            self.flags |= 0x1;
        } else {
            self.flags &= !0x1;
        }
    }

    /// Can stats of this kind be accessed from another database?
    #[inline]
    pub fn accessed_across_databases(&self) -> bool {
        (self.flags & 0x2) != 0
    }
    #[inline]
    pub fn set_accessed_across_databases(&mut self, v: bool) {
        if v {
            self.flags |= 0x2;
        } else {
            self.flags &= !0x2;
        }
    }

    /// Should stats be written to the on-disk stats file?
    #[inline]
    pub fn write_to_file(&self) -> bool {
        (self.flags & 0x4) != 0
    }
    #[inline]
    pub fn set_write_to_file(&mut self, v: bool) {
        if v {
            self.flags |= 0x4;
        } else {
            self.flags &= !0x4;
        }
    }
}

// ----------------------------------------------------------------------------
// List of SLRU names that we keep stats for.
// ----------------------------------------------------------------------------

/// List of SLRU names that we keep stats for. The "other" entry is used for all
/// SLRUs without an explicit entry. Has to be last.
// A `const` (not `static`): an array of raw pointers is not `Sync` so it can't be
// a shared static, but the pointers are to 'static C-string literals, so a const
// lookup table is equivalent and Sync-free.
pub const slru_names: [*const c_char; 8] = [
    c"commit_timestamp".as_ptr(),
    c"multixact_member".as_ptr(),
    c"multixact_offset".as_ptr(),
    c"notify".as_ptr(),
    c"serializable".as_ptr(),
    c"subtransaction".as_ptr(),
    c"transaction".as_ptr(),
    c"other".as_ptr(), // has to be last
];

// SLRU_NUM_ELEMENTS == lengthof(slru_names) == 8; canonical value re-exported
// from the pgstat subset module (imported above). Kept here for completeness.

// ----------------------------------------------------------------------------
// Types and definitions for different kinds of fixed-amount stats.
// ----------------------------------------------------------------------------

#[repr(C)]
pub struct PgStatShared_Archiver {
    /// lock protects ->reset_offset as well as stats->stat_reset_timestamp
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_ArchiverStats,
    pub reset_offset: PgStat_ArchiverStats,
}

#[repr(C)]
pub struct PgStatShared_BgWriter {
    /// lock protects ->reset_offset as well as stats->stat_reset_timestamp
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_BgWriterStats,
    pub reset_offset: PgStat_BgWriterStats,
}

#[repr(C)]
pub struct PgStatShared_Checkpointer {
    /// lock protects ->reset_offset as well as stats->stat_reset_timestamp
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_CheckpointerStats,
    pub reset_offset: PgStat_CheckpointerStats,
}

/// Shared-memory ready PgStat_IO
#[repr(C)]
pub struct PgStatShared_IO {
    /// locks[i] protects stats.stats[i]. locks[0] also protects
    /// stats.stat_reset_timestamp.
    pub locks: [LWLock; BACKEND_NUM_TYPES],
    pub stats: PgStat_IO,
}

#[repr(C)]
pub struct PgStatShared_SLRU {
    /// lock protects ->stats
    pub lock: LWLock,
    pub stats: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
}

#[repr(C)]
pub struct PgStatShared_Wal {
    /// lock protects ->stats
    pub lock: LWLock,
    pub stats: PgStat_WalStats,
}

// ----------------------------------------------------------------------------
// Types and definitions for different kinds of variable-amount stats.
// ----------------------------------------------------------------------------

#[repr(C)]
pub struct PgStatShared_Database {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatDBEntry,
}

#[repr(C)]
pub struct PgStatShared_Relation {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatTabEntry,
}

#[repr(C)]
pub struct PgStatShared_Function {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatFuncEntry,
}

#[repr(C)]
pub struct PgStatShared_Subscription {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatSubEntry,
}

#[repr(C)]
pub struct PgStatShared_ReplSlot {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatReplSlotEntry,
}

#[repr(C)]
pub struct PgStatShared_Backend {
    pub header: PgStatShared_Common,
    pub stats: PgStat_Backend,
}

/// Central shared memory entry for the cumulative stats system.
#[repr(C)]
pub struct PgStat_ShmemControl {
    pub raw_dsa_area: *mut c_void,

    /// Stats for variable-numbered objects are kept in this shared hash table.
    pub hash_handle: dshash_table_handle,

    /// Has the stats system already been shut down? Just a debugging check.
    pub is_shutdown: bool,

    /// Whenever statistics for dropped objects could not be freed, the dropping
    /// backend increments this counter.
    pub gc_request_count: pg_atomic_uint64,

    /// Stats data for fixed-numbered objects.
    pub archiver: PgStatShared_Archiver,
    pub bgwriter: PgStatShared_BgWriter,
    pub checkpointer: PgStatShared_Checkpointer,
    pub io: PgStatShared_IO,
    pub slru: PgStatShared_SLRU,
    pub wal: PgStatShared_Wal,

    /// Custom stats data with fixed-numbered objects, indexed by (PgStat_Kind -
    /// PGSTAT_KIND_CUSTOM_MIN).
    pub custom_data: [*mut c_void; PGSTAT_KIND_CUSTOM_SIZE as usize],
}

/// Cached statistics snapshot
#[repr(C)]
pub struct PgStat_Snapshot {
    pub mode: PgStat_FetchConsistency,

    /// time at which snapshot was taken
    pub snapshot_timestamp: TimestampTz,

    pub fixed_valid: [bool; PGSTAT_KIND_BUILTIN_SIZE as usize],

    pub archiver: PgStat_ArchiverStats,

    pub bgwriter: PgStat_BgWriterStats,

    pub checkpointer: PgStat_CheckpointerStats,

    pub io: PgStat_IO,

    pub slru: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],

    pub wal: PgStat_WalStats,

    /// Data in snapshot for custom fixed-numbered statistics, indexed by
    /// (PgStat_Kind - PGSTAT_KIND_CUSTOM_MIN).
    pub custom_valid: [bool; PGSTAT_KIND_CUSTOM_SIZE as usize],
    pub custom_data: [*mut c_void; PGSTAT_KIND_CUSTOM_SIZE as usize],

    /// to free snapshot in bulk
    pub context: MemoryContext,
    pub stats: *mut pgstat_snapshot_hash,
}

/// Collection of backend-local stats state.
#[repr(C)]
pub struct PgStat_LocalState {
    pub shmem: *mut PgStat_ShmemControl,
    pub dsa: *mut dsa_area,
    pub shared_hash: *mut dshash_table,

    /// the current statistics snapshot
    pub snapshot: PgStat_Snapshot,
}

// ----------------------------------------------------------------------------
// Functions in pgstat.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_get_kind_info(_kind: PgStat_Kind) -> *const PgStat_KindInfo {
    unimplemented!()
}

pub unsafe fn pgstat_register_kind(_kind: PgStat_Kind, _kind_info: *const PgStat_KindInfo) {
    unimplemented!()
}

/// `pgstat_assert_is_up()`. Under USE_ASSERT_CHECKING this is an extern
/// function; otherwise a no-op macro. Translated as a no-op inline.
#[inline]
pub fn pgstat_assert_is_up() {}

pub unsafe fn pgstat_delete_pending_entry(_entry_ref: *mut PgStat_EntryRef) {
    unimplemented!()
}

pub unsafe fn pgstat_prep_pending_entry(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
    _created_entry: *mut bool,
) -> *mut PgStat_EntryRef {
    unimplemented!()
}

pub unsafe fn pgstat_fetch_pending_entry(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
) -> *mut PgStat_EntryRef {
    unimplemented!()
}

pub unsafe fn pgstat_fetch_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) -> *mut c_void {
    unimplemented!()
}

pub unsafe fn pgstat_snapshot_fixed(_kind: PgStat_Kind) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_archiver.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_archiver_init_shmem_cb(_stats: *mut c_void) {
    unimplemented!()
}
pub unsafe fn pgstat_archiver_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_archiver_snapshot_cb() {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_backend.c
// ----------------------------------------------------------------------------

/// Flush I/O statistics
pub const PGSTAT_BACKEND_FLUSH_IO: u32 = 1 << 0;
/// Flush WAL statistics
pub const PGSTAT_BACKEND_FLUSH_WAL: u32 = 1 << 1;
pub const PGSTAT_BACKEND_FLUSH_ALL: u32 = PGSTAT_BACKEND_FLUSH_IO | PGSTAT_BACKEND_FLUSH_WAL;

pub unsafe fn pgstat_flush_backend(_nowait: bool, _flags: bits32) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_backend_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_backend_reset_timestamp_cb(
    _header: *mut PgStatShared_Common,
    _ts: TimestampTz,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_bgwriter.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_bgwriter_init_shmem_cb(_stats: *mut c_void) {
    unimplemented!()
}
pub unsafe fn pgstat_bgwriter_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_bgwriter_snapshot_cb() {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_checkpointer.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_checkpointer_init_shmem_cb(_stats: *mut c_void) {
    unimplemented!()
}
pub unsafe fn pgstat_checkpointer_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_checkpointer_snapshot_cb() {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_database.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_report_disconnect(_dboid: Oid) {
    unimplemented!()
}
pub unsafe fn pgstat_update_dbstats(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn AtEOXact_PgStat_Database(_isCommit: bool, _parallel: bool) {
    unimplemented!()
}

pub unsafe fn pgstat_prep_database_pending(_dboid: Oid) -> *mut PgStat_StatDBEntry {
    unimplemented!()
}
pub unsafe fn pgstat_reset_database_timestamp(_dboid: Oid, _ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_database_flush_cb(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_database_reset_timestamp_cb(
    _header: *mut PgStatShared_Common,
    _ts: TimestampTz,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_function.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_function_flush_cb(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_io.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_flush_io(_nowait: bool) {
    unimplemented!()
}

pub unsafe fn pgstat_io_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_io_init_shmem_cb(_stats: *mut c_void) {
    unimplemented!()
}
pub unsafe fn pgstat_io_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_io_snapshot_cb() {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_relation.c
// ----------------------------------------------------------------------------

pub unsafe fn AtEOXact_PgStat_Relations(_xact_state: *mut PgStat_SubXactStatus, _isCommit: bool) {
    unimplemented!()
}
pub unsafe fn AtEOSubXact_PgStat_Relations(
    _xact_state: *mut PgStat_SubXactStatus,
    _isCommit: bool,
    _nestDepth: c_int,
) {
    unimplemented!()
}
pub unsafe fn AtPrepare_PgStat_Relations(_xact_state: *mut PgStat_SubXactStatus) {
    unimplemented!()
}
pub unsafe fn PostPrepare_PgStat_Relations(_xact_state: *mut PgStat_SubXactStatus) {
    unimplemented!()
}

pub unsafe fn pgstat_relation_flush_cb(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_relation_delete_pending_cb(_entry_ref: *mut PgStat_EntryRef) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_replslot.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_replslot_reset_timestamp_cb(
    _header: *mut PgStatShared_Common,
    _ts: TimestampTz,
) {
    unimplemented!()
}
pub unsafe fn pgstat_replslot_to_serialized_name_cb(
    _key: *const PgStat_HashKey,
    _header: *const PgStatShared_Common,
    _name: *mut NameData,
) {
    unimplemented!()
}
pub unsafe fn pgstat_replslot_from_serialized_name_cb(
    _name: *const NameData,
    _key: *mut PgStat_HashKey,
) -> bool {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_shmem.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_attach_shmem() {
    unimplemented!()
}
pub unsafe fn pgstat_detach_shmem() {
    unimplemented!()
}

pub unsafe fn pgstat_get_entry_ref(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
    _create: bool,
    _created_entry: *mut bool,
) -> *mut PgStat_EntryRef {
    unimplemented!()
}
pub unsafe fn pgstat_lock_entry(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_lock_entry_shared(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_unlock_entry(_entry_ref: *mut PgStat_EntryRef) {
    unimplemented!()
}
pub unsafe fn pgstat_drop_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_drop_all_entries() {
    unimplemented!()
}
pub unsafe fn pgstat_drop_matching_entries(
    _do_drop: Option<unsafe extern "C" fn(*mut PgStatShared_HashEntry, Datum) -> bool>,
    _match_data: Datum,
) {
    unimplemented!()
}
pub unsafe fn pgstat_get_entry_ref_locked(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
    _nowait: bool,
) -> *mut PgStat_EntryRef {
    unimplemented!()
}
pub unsafe fn pgstat_reset_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64, _ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_reset_entries_of_kind(_kind: PgStat_Kind, _ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_reset_matching_entries(
    _do_reset: Option<unsafe extern "C" fn(*mut PgStatShared_HashEntry, Datum) -> bool>,
    _match_data: Datum,
    _ts: TimestampTz,
) {
    unimplemented!()
}

pub unsafe fn pgstat_request_entry_refs_gc() {
    unimplemented!()
}
pub unsafe fn pgstat_init_entry(
    _kind: PgStat_Kind,
    _shhashent: *mut PgStatShared_HashEntry,
) -> *mut PgStatShared_Common {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_slru.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_slru_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_slru_init_shmem_cb(_stats: *mut c_void) {
    unimplemented!()
}
pub unsafe fn pgstat_slru_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_slru_snapshot_cb() {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_wal.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_wal_init_backend_cb() {
    unimplemented!()
}
pub unsafe fn pgstat_wal_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_wal_init_shmem_cb(_stats: *mut c_void) {
    unimplemented!()
}
pub unsafe fn pgstat_wal_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub unsafe fn pgstat_wal_snapshot_cb() {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_subscription.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_subscription_flush_cb(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub unsafe fn pgstat_subscription_reset_timestamp_cb(
    _header: *mut PgStatShared_Common,
    _ts: TimestampTz,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in pgstat_xact.c
// ----------------------------------------------------------------------------

pub unsafe fn pgstat_get_xact_stack_level(_nest_level: c_int) -> *mut PgStat_SubXactStatus {
    unimplemented!()
}
pub unsafe fn pgstat_drop_transactional(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) {
    unimplemented!()
}
pub unsafe fn pgstat_create_transactional(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Variables in pgstat.c
// ----------------------------------------------------------------------------

// extern PGDLLIMPORT bool pgstat_report_fixed;
// extern PGDLLIMPORT PgStat_LocalState pgStatLocal;
//
// These globals are owned by pgstat.c. They are declared here as process-local
// statics holding the canonical FULL types defined in this header. The pgstat
// subset also defines a `pgStatLocal`; the main agent should dedup so a single
// canonical definition remains.
pub static mut pgstat_report_fixed: bool = false;

/// Backend-local stats state. Canonical full version (the subset's `pgStatLocal`
/// is a reduced stand-in). TODO: dedup with crate::utils::activity::pgstat.
pub static mut pgStatLocal: PgStat_LocalState = PgStat_LocalState {
    shmem: std::ptr::null_mut(),
    dsa: std::ptr::null_mut(),
    shared_hash: std::ptr::null_mut(),
    snapshot: PgStat_Snapshot {
        mode: 0,
        snapshot_timestamp: 0,
        fixed_valid: [false; PGSTAT_KIND_BUILTIN_SIZE as usize],
        archiver: PgStat_ArchiverStats::zeroed(),
        bgwriter: PgStat_BgWriterStats::zeroed(),
        checkpointer: PgStat_CheckpointerStats::zeroed(),
        io: PgStat_IO::zeroed(),
        slru: [PgStat_SLRUStats::zeroed(); SLRU_NUM_ELEMENTS],
        wal: PgStat_WalStats::zeroed(),
        custom_valid: [false; PGSTAT_KIND_CUSTOM_SIZE as usize],
        custom_data: [std::ptr::null_mut(); PGSTAT_KIND_CUSTOM_SIZE as usize],
        context: std::ptr::null_mut(),
        stats: std::ptr::null_mut(),
    },
};

// ----------------------------------------------------------------------------
// Implementation of inline functions declared above.
// ----------------------------------------------------------------------------

/// Helpers for changecount manipulation. See comments around struct
/// PgBackendStatus for details.
///
/// NOTE: START_CRIT_SECTION / END_CRIT_SECTION and the write/read memory
/// barriers are not yet ported; they are elided here. The structure of the
/// changecount dance is preserved.
#[inline]
pub unsafe fn pgstat_begin_changecount_write(cc: *mut uint32) {
    debug_assert!((*cc & 1) == 0);

    // START_CRIT_SECTION();
    *cc = (*cc).wrapping_add(1);
    // pg_write_barrier();
}

#[inline]
pub unsafe fn pgstat_end_changecount_write(cc: *mut uint32) {
    debug_assert!((*cc & 1) == 1);

    // pg_write_barrier();

    *cc = (*cc).wrapping_add(1);

    // END_CRIT_SECTION();
}

#[inline]
pub unsafe fn pgstat_begin_changecount_read(cc: *mut uint32) -> uint32 {
    let before_cc = *cc;

    // CHECK_FOR_INTERRUPTS();

    // pg_read_barrier();

    before_cc
}

/// Returns true if the read succeeded, false if it needs to be repeated.
#[inline]
pub unsafe fn pgstat_end_changecount_read(cc: *mut uint32, before_cc: uint32) -> bool {
    // pg_read_barrier();

    let after_cc = *cc;

    /* was a write in progress when we started? */
    if before_cc & 1 != 0 {
        return false;
    }

    /* did writes start and complete while we read? */
    before_cc == after_cc
}

/// helper function for PgStat_KindInfo->snapshot_cb / reset_all_cb callbacks.
///
/// Copies out the specified memory area following change-count protocol.
#[inline]
pub unsafe fn pgstat_copy_changecounted_stats(
    dst: *mut c_void,
    src: *mut c_void,
    len: Size,
    cc: *mut uint32,
) {
    let mut cc_before: uint32;

    loop {
        cc_before = pgstat_begin_changecount_read(cc);

        std::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, len);

        if pgstat_end_changecount_read(cc, cc_before) {
            break;
        }
    }
}

/// helpers for dshash / simplehash hashtables
#[inline]
pub unsafe fn pgstat_cmp_hash_key(
    a: *const c_void,
    b: *const c_void,
    size: Size,
    arg: *mut c_void,
) -> c_int {
    debug_assert!(size == std::mem::size_of::<PgStat_HashKey>() && arg.is_null());
    let _ = size;
    let _ = arg;
    // memcmp(a, b, sizeof(PgStat_HashKey))
    let n = std::mem::size_of::<PgStat_HashKey>();
    let sa = std::slice::from_raw_parts(a as *const u8, n);
    let sb = std::slice::from_raw_parts(b as *const u8, n);
    match sa.cmp(sb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

#[inline]
pub unsafe fn pgstat_hash_hash_key(d: *const c_void, size: Size, arg: *mut c_void) -> uint32 {
    let key = d as *const c_char;
    debug_assert!(size == std::mem::size_of::<PgStat_HashKey>() && arg.is_null());
    let _ = arg;
    fasthash32(key, size, 0)
}

/// The length of the data portion of a shared memory stats entry (i.e. without
/// transient data such as refcounts, lwlocks, ...).
#[inline]
pub unsafe fn pgstat_get_entry_len(kind: PgStat_Kind) -> Size {
    (*pgstat_get_kind_info(kind)).shared_data_len as Size
}

/// Returns a pointer to the data portion of a shared memory stats entry.
#[inline]
pub unsafe fn pgstat_get_entry_data(
    kind: PgStat_Kind,
    entry: *mut PgStatShared_Common,
) -> *mut c_void {
    let off = (*pgstat_get_kind_info(kind)).shared_data_off as usize;

    debug_assert!(off != 0 && off < u32::MAX as usize);

    (entry as *mut c_char).add(off) as *mut c_void
}

/// Returns a pointer to the shared memory area of custom stats for
/// fixed-numbered statistics.
#[inline]
pub unsafe fn pgstat_get_custom_shmem_data(kind: PgStat_Kind) -> *mut c_void {
    let idx = (kind - PGSTAT_KIND_CUSTOM_MIN) as usize;

    debug_assert!(pgstat_is_kind_custom(kind));
    debug_assert!((*pgstat_get_kind_info(kind)).fixed_amount());

    (*pgStatLocal.shmem).custom_data[idx]
}

/// Returns a pointer to the portion of custom data for fixed-numbered
/// statistics in the current snapshot.
#[inline]
pub unsafe fn pgstat_get_custom_snapshot_data(kind: PgStat_Kind) -> *mut c_void {
    let idx = (kind - PGSTAT_KIND_CUSTOM_MIN) as usize;

    debug_assert!(pgstat_is_kind_custom(kind));
    debug_assert!((*pgstat_get_kind_info(kind)).fixed_amount());

    pgStatLocal.snapshot.custom_data[idx]
}

// ----------------------------------------------------------------------------
// External helpers referenced by inline functions.
// ----------------------------------------------------------------------------

// fasthash32 from common/hashfn_unstable.h. TODO: dedup if/when that header is
// ported. Declared here as a prototype-style stub.
unsafe fn fasthash32(_data: *const c_char, _len: Size, _seed: uint32) -> uint32 {
    unimplemented!()
}
