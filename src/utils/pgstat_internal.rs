//! Translated from PostgreSQL src/include/utils/pgstat_internal.h
//! Definitions for the cumulative statistics system, internal to stats support.
//!
//! Single-process model: PostgreSQL's shared-memory + dshash + dsa machinery
//! collapses to owned heap state behind ordinary locks. LWLocks are dropped (a
//! `std::sync::Mutex<()>` placeholder marks the protected region); dsa pointers
//! become owned/`Box`; the dshash table becomes a `HashMap`. pg_atomic_* become
//! `core::sync::atomic` types.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Mutex;

use bitflags::bitflags;

use crate::c::{bits32, NameData};
use crate::datatype::timestamp::TimestampTz;
use crate::nodes::memnodes::MemoryContext;
use crate::pgstat::{
    PgStat_ArchiverStats, PgStat_Backend, PgStat_BgWriterStats, PgStat_CheckpointerStats,
    PgStat_FetchConsistency, PgStat_IO, PgStat_SLRUStats, PgStat_StatDBEntry,
    PgStat_StatFuncEntry, PgStat_StatReplSlotEntry, PgStat_StatSubEntry, PgStat_StatTabEntry,
    PgStat_TableXactStatus, PgStat_WalStats,
};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::common::hashfn_unstable::fasthash32;
use crate::utils::pgstat_kind::{
    pgstat_is_kind_custom, PgStat_Kind, PGSTAT_KIND_BUILTIN_SIZE, PGSTAT_KIND_CUSTOM_MIN,
    PGSTAT_KIND_CUSTOM_SIZE,
};

/// Shared statistics hash entry key.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgStat_HashKey {
    /// Statistics entry kind.
    pub kind: PgStat_Kind,
    /// Database ID. InvalidOid for shared objects.
    pub dboid: Oid,
    /// Object ID (table, function, etc.), or identifier.
    pub objid: u64,
}

/// Shared statistics hash entry. Points to the stats body (variable-size).
/// Shared-memory refcount/generation kept as atomics; `body` becomes an owned
/// pointer instead of a dsa_pointer.
pub struct PgStatShared_HashEntry {
    /// Hash key.
    pub key: PgStat_HashKey,
    /// Backends must release references once set; no new references afterward.
    pub dropped: bool,
    /// Refcount managing lifetime of the entry itself.
    pub refcount: AtomicU32,
    /// Number of times the entry has been reused.
    pub generation: AtomicU32,
    /// Pointer to shared stats (was dsa_pointer). TODO(ptr): owned in Phase 2.
    pub body: *mut PgStatShared_Common,
}

/// Common header struct for PgStatShared_*.
pub struct PgStatShared_Common {
    /// Just a validity cross-check.
    pub magic: u32,
    /// Lock protecting stats contents. C: LWLock -> std Mutex placeholder.
    pub lock: Mutex<()>,
}

/// A backend-local reference to a shared stats entry.
pub struct PgStat_EntryRef {
    /// The shared hashtable entry. TODO(ptr).
    pub shared_entry: *mut PgStatShared_HashEntry,
    /// Resolved local pointer to the stats data (->body). TODO(ptr).
    pub shared_stats: *mut PgStatShared_Common,
    /// Copy of the shared entry "generation".
    pub generation: u32,
    /// Pending stats to be flushed to shared memory; kind-specific format.
    /// C: `void *` -> opaque owned box. TODO(ptr).
    pub pending: Option<Box<dyn core::any::Any>>,
    // C: dlist_node pending_node -> membership tracked by the owning list.
}

/// Stack of transactional stats status, one per (sub)transaction nest level.
pub struct PgStat_SubXactStatus {
    /// Subtransaction nest level.
    pub nest_level: i32,
    /// Higher-level subxact if any.
    pub prev: Option<Box<PgStat_SubXactStatus>>,
    /// Stats dropped in this (sub)transaction, executed on commit/abort.
    /// C: dclist_head pending_drops -> Vec.
    pub pending_drops: Vec<PgStat_HashKey>,
    /// Head of per-subxact tuple insertion/deletion counts list.
    pub first: Option<Box<PgStat_TableXactStatus>>,
}

bitflags! {
    /// Boolean property bits of PgStat_KindInfo (the C `:1` bitfields).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct KindInfoFlags: u8 {
        /// A fixed number of stats objects exist for this kind.
        const FIXED_AMOUNT = 1 << 0;
        /// Stats of this kind can be accessed from another database.
        const ACCESSED_ACROSS_DATABASES = 1 << 1;
        /// Stats should be written to the on-disk stats file.
        const WRITE_TO_FILE = 1 << 2;
    }
}

/// Metadata for a specific kind of statistics. A routine struct of callbacks plus
/// sizing/offset data; optional callbacks are `Option<fn>` (NULL-checked in C).
pub struct PgStat_KindInfo {
    /// The fixed_amount / accessed_across_databases / write_to_file bits.
    pub flags: KindInfoFlags,

    /// Size of an entry in the shared stats hash table (or custom_data slot).
    pub shared_size: u32,
    /// Offset of the stats struct in PgStat_Snapshot (fixed-numbered).
    pub snapshot_ctl_off: u32,
    /// Offset of the stats struct in PgStat_ShmemControl (fixed-numbered).
    pub shared_ctl_off: u32,
    /// Offset of statistics inside the shared stats entry (for [de]serialize).
    pub shared_data_off: u32,
    /// Length of statistics inside the shared stats entry.
    pub shared_data_len: u32,
    /// Size of the pending data for this kind. 0 = never has a pending entry.
    pub pending_size: u32,

    /// Custom actions when initializing a backend. Optional.
    pub init_backend_cb: Option<fn()>,
    /// For variable-numbered stats: flush pending stats. Required if pending used.
    pub flush_pending_cb: Option<fn(sr: &mut PgStat_EntryRef, nowait: bool) -> bool>,
    /// For variable-numbered stats: delete pending stats. Optional.
    pub delete_pending_cb: Option<fn(sr: &mut PgStat_EntryRef)>,
    /// For variable-numbered stats: reset the reset timestamp. Optional.
    pub reset_timestamp_cb: Option<fn(header: &mut PgStatShared_Common, ts: TimestampTz)>,
    /// For variable-numbered stats: serialize the entry's name. Optional.
    pub to_serialized_name:
        Option<fn(key: &PgStat_HashKey, header: &PgStatShared_Common, name: &mut NameData)>,
    /// For variable-numbered stats: parse a serialized name back to a key.
    pub from_serialized_name: Option<fn(name: &NameData, key: &mut PgStat_HashKey) -> bool>,
    /// For fixed-numbered stats: initialize shared memory state.
    pub init_shmem_cb: Option<fn(stats: &mut dyn core::any::Any)>,
    /// Flush pending stats for kinds not using PgStat_EntryRef->pending. Optional.
    pub flush_static_cb: Option<fn(nowait: bool) -> bool>,
    /// For fixed-numbered stats: reset all.
    pub reset_all_cb: Option<fn(ts: TimestampTz)>,
    /// For fixed-numbered stats: build snapshot for entry.
    pub snapshot_cb: Option<fn()>,

    /// Name of the kind of stats.
    pub name: &'static str,
}

/// SLRU names we keep stats for. The "other" entry must be last.
pub static slru_names: [&str; 8] = [
    "commit_timestamp",
    "multixact_member",
    "multixact_offset",
    "notify",
    "serializable",
    "subtransaction",
    "transaction",
    "other",
];

pub const SLRU_NUM_ELEMENTS: usize = slru_names.len();

// Fixed-amount stats shared structs. C: LWLock -> std Mutex placeholder.

pub struct PgStatShared_Archiver {
    pub lock: Mutex<()>,
    pub changecount: u32,
    pub stats: PgStat_ArchiverStats,
    pub reset_offset: PgStat_ArchiverStats,
}

pub struct PgStatShared_BgWriter {
    pub lock: Mutex<()>,
    pub changecount: u32,
    pub stats: PgStat_BgWriterStats,
    pub reset_offset: PgStat_BgWriterStats,
}

pub struct PgStatShared_Checkpointer {
    pub lock: Mutex<()>,
    pub changecount: u32,
    pub stats: PgStat_CheckpointerStats,
    pub reset_offset: PgStat_CheckpointerStats,
}

/// Shared-memory-ready PgStat_IO. locks[i] protects stats.stats[i].
pub struct PgStatShared_IO {
    pub locks: [Mutex<()>; crate::miscadmin::BACKEND_NUM_TYPES],
    pub stats: PgStat_IO,
}

pub struct PgStatShared_SLRU {
    pub lock: Mutex<()>,
    pub stats: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
}

pub struct PgStatShared_Wal {
    pub lock: Mutex<()>,
    pub stats: PgStat_WalStats,
}

// Variable-amount stats shared structs. Each starts with PgStatShared_Common.

pub struct PgStatShared_Database {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatDBEntry,
}

pub struct PgStatShared_Relation {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatTabEntry,
}

pub struct PgStatShared_Function {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatFuncEntry,
}

pub struct PgStatShared_Subscription {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatSubEntry,
}

pub struct PgStatShared_ReplSlot {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatReplSlotEntry,
}

pub struct PgStatShared_Backend {
    pub header: PgStatShared_Common,
    pub stats: PgStat_Backend,
}

/// Central shared memory entry for the cumulative stats system. Single-process:
/// dsa/dshash collapse to owned state; raw_dsa_area dropped.
pub struct PgStat_ShmemControl {
    /// Variable-numbered objects' stats (was a dshash table). TODO: HashMap.
    pub hash: HashMap<PgStat_HashKey, Box<PgStatShared_HashEntry>>,
    /// Has the stats system already been shut down? (debugging check).
    pub is_shutdown: bool,
    /// GC request counter for releasing dropped-object references.
    pub gc_request_count: AtomicU64,

    // Fixed-numbered objects.
    pub archiver: PgStatShared_Archiver,
    pub bgwriter: PgStatShared_BgWriter,
    pub checkpointer: PgStatShared_Checkpointer,
    pub io: PgStatShared_IO,
    pub slru: PgStatShared_SLRU,
    pub wal: PgStatShared_Wal,

    /// Custom fixed-numbered stats, indexed by (kind - PGSTAT_KIND_CUSTOM_MIN).
    pub custom_data: [Option<Box<dyn core::any::Any>>; PGSTAT_KIND_CUSTOM_SIZE as usize],
}

/// Cached statistics snapshot.
pub struct PgStat_Snapshot {
    pub mode: PgStat_FetchConsistency,
    /// Time at which snapshot was taken.
    pub snapshot_timestamp: TimestampTz,
    pub fixed_valid: [bool; PGSTAT_KIND_BUILTIN_SIZE as usize],
    pub archiver: PgStat_ArchiverStats,
    pub bgwriter: PgStat_BgWriterStats,
    pub checkpointer: PgStat_CheckpointerStats,
    pub io: PgStat_IO,
    pub slru: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
    pub wal: PgStat_WalStats,
    /// Custom fixed-numbered stats, indexed by (kind - PGSTAT_KIND_CUSTOM_MIN).
    pub custom_valid: [bool; PGSTAT_KIND_CUSTOM_SIZE as usize],
    pub custom_data: [Option<Box<dyn core::any::Any>>; PGSTAT_KIND_CUSTOM_SIZE as usize],
    /// To free snapshot in bulk.
    pub context: Option<MemoryContext>,
    /// Snapshot hash (was struct pgstat_snapshot_hash *).
    pub stats: HashMap<PgStat_HashKey, Box<dyn core::any::Any>>,
}

/// Collection of backend-local stats state. dsa/dshash collapse to owned state.
pub struct PgStat_LocalState {
    pub shmem: Option<Box<PgStat_ShmemControl>>,
    /// The current statistics snapshot.
    pub snapshot: PgStat_Snapshot,
}

// Functions in pgstat.c

/// Returns the kind info for a stat kind, or None if unregistered.
pub fn pgstat_get_kind_info(_kind: PgStat_Kind) -> Option<&'static PgStat_KindInfo> {
    unimplemented!()
}

pub fn pgstat_register_kind(_kind: PgStat_Kind, _kind_info: &'static PgStat_KindInfo) {
    unimplemented!()
}

pub fn pgstat_assert_is_up() {
    unimplemented!()
}

pub fn pgstat_delete_pending_entry(_entry_ref: &mut PgStat_EntryRef) {
    unimplemented!()
}

/// Returns (entry_ref, created_entry).
pub fn pgstat_prep_pending_entry(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
) -> (*mut PgStat_EntryRef, bool) {
    unimplemented!()
}

/// Returns the pending entry, or None if absent.
pub fn pgstat_fetch_pending_entry(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
) -> Option<*mut PgStat_EntryRef> {
    unimplemented!()
}

pub fn pgstat_fetch_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) -> *mut () {
    unimplemented!()
}

pub fn pgstat_snapshot_fixed(_kind: PgStat_Kind) {
    unimplemented!()
}

// Functions in pgstat_archiver.c
pub fn pgstat_archiver_init_shmem_cb(_stats: &mut dyn core::any::Any) {
    unimplemented!()
}
pub fn pgstat_archiver_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_archiver_snapshot_cb() {
    unimplemented!()
}

// Functions in pgstat_backend.c

bitflags! {
    /// Flags for pgstat_flush_backend(). Composite `ALL` per appendix B.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PgStatBackendFlush: bits32 {
        /// Flush I/O statistics.
        const IO  = 1 << 0;
        /// Flush WAL statistics.
        const WAL = 1 << 1;
        const ALL = Self::IO.bits() | Self::WAL.bits();
    }
}

pub fn pgstat_flush_backend(_nowait: bool, _flags: PgStatBackendFlush) -> bool {
    unimplemented!()
}
pub fn pgstat_backend_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_backend_reset_timestamp_cb(_header: &mut PgStatShared_Common, _ts: TimestampTz) {
    unimplemented!()
}

// Functions in pgstat_bgwriter.c
pub fn pgstat_bgwriter_init_shmem_cb(_stats: &mut dyn core::any::Any) {
    unimplemented!()
}
pub fn pgstat_bgwriter_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_bgwriter_snapshot_cb() {
    unimplemented!()
}

// Functions in pgstat_checkpointer.c
pub fn pgstat_checkpointer_init_shmem_cb(_stats: &mut dyn core::any::Any) {
    unimplemented!()
}
pub fn pgstat_checkpointer_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_checkpointer_snapshot_cb() {
    unimplemented!()
}

// Functions in pgstat_database.c
pub fn pgstat_report_disconnect(_dboid: Oid) {
    unimplemented!()
}
pub fn pgstat_update_dbstats(_ts: TimestampTz) {
    unimplemented!()
}
pub fn AtEOXact_PgStat_Database(_isCommit: bool, _parallel: bool) {
    unimplemented!()
}
pub fn pgstat_prep_database_pending(_dboid: Oid) -> *mut PgStat_StatDBEntry {
    unimplemented!()
}
pub fn pgstat_reset_database_timestamp(_dboid: Oid, _ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_database_flush_cb(_entry_ref: &mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_database_reset_timestamp_cb(_header: &mut PgStatShared_Common, _ts: TimestampTz) {
    unimplemented!()
}

// Functions in pgstat_function.c
pub fn pgstat_function_flush_cb(_entry_ref: &mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}

// Functions in pgstat_io.c
pub fn pgstat_flush_io(_nowait: bool) {
    unimplemented!()
}
pub fn pgstat_io_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_io_init_shmem_cb(_stats: &mut dyn core::any::Any) {
    unimplemented!()
}
pub fn pgstat_io_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_io_snapshot_cb() {
    unimplemented!()
}

// Functions in pgstat_relation.c
pub fn AtEOXact_PgStat_Relations(_xact_state: &mut PgStat_SubXactStatus, _isCommit: bool) {
    unimplemented!()
}
pub fn AtEOSubXact_PgStat_Relations(
    _xact_state: &mut PgStat_SubXactStatus,
    _isCommit: bool,
    _nestDepth: i32,
) {
    unimplemented!()
}
pub fn AtPrepare_PgStat_Relations(_xact_state: &mut PgStat_SubXactStatus) {
    unimplemented!()
}
pub fn PostPrepare_PgStat_Relations(_xact_state: &mut PgStat_SubXactStatus) {
    unimplemented!()
}
pub fn pgstat_relation_flush_cb(_entry_ref: &mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_relation_delete_pending_cb(_entry_ref: &mut PgStat_EntryRef) {
    unimplemented!()
}

// Functions in pgstat_replslot.c
pub fn pgstat_replslot_reset_timestamp_cb(_header: &mut PgStatShared_Common, _ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_replslot_to_serialized_name_cb(
    _key: &PgStat_HashKey,
    _header: &PgStatShared_Common,
    _name: &mut NameData,
) {
    unimplemented!()
}
pub fn pgstat_replslot_from_serialized_name_cb(
    _name: &NameData,
    _key: &mut PgStat_HashKey,
) -> bool {
    unimplemented!()
}

// Functions in pgstat_shmem.c
pub fn pgstat_attach_shmem() {
    unimplemented!()
}
pub fn pgstat_detach_shmem() {
    unimplemented!()
}

/// Returns (entry_ref, created_entry).
pub fn pgstat_get_entry_ref(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
    _create: bool,
) -> (*mut PgStat_EntryRef, bool) {
    unimplemented!()
}
pub fn pgstat_lock_entry(_entry_ref: &mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_lock_entry_shared(_entry_ref: &mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_unlock_entry(_entry_ref: &mut PgStat_EntryRef) {
    unimplemented!()
}
pub fn pgstat_drop_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) -> bool {
    unimplemented!()
}
pub fn pgstat_drop_all_entries() {
    unimplemented!()
}
/// `do_drop` is the predicate; the C `Datum match_data` opaque arg is folded into
/// the closure capture.
pub fn pgstat_drop_matching_entries(
    _do_drop: &mut dyn FnMut(&PgStatShared_HashEntry, Datum) -> bool,
    _match_data: Datum,
) {
    unimplemented!()
}
pub fn pgstat_get_entry_ref_locked(
    _kind: PgStat_Kind,
    _dboid: Oid,
    _objid: u64,
    _nowait: bool,
) -> *mut PgStat_EntryRef {
    unimplemented!()
}
pub fn pgstat_reset_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64, _ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_reset_entries_of_kind(_kind: PgStat_Kind, _ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_reset_matching_entries(
    _do_reset: &mut dyn FnMut(&PgStatShared_HashEntry, Datum) -> bool,
    _match_data: Datum,
    _ts: TimestampTz,
) {
    unimplemented!()
}
pub fn pgstat_request_entry_refs_gc() {
    unimplemented!()
}
pub fn pgstat_init_entry(
    _kind: PgStat_Kind,
    _shhashent: &mut PgStatShared_HashEntry,
) -> *mut PgStatShared_Common {
    unimplemented!()
}

// Functions in pgstat_slru.c
pub fn pgstat_slru_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_slru_init_shmem_cb(_stats: &mut dyn core::any::Any) {
    unimplemented!()
}
pub fn pgstat_slru_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_slru_snapshot_cb() {
    unimplemented!()
}

// Functions in pgstat_wal.c
pub fn pgstat_wal_init_backend_cb() {
    unimplemented!()
}
pub fn pgstat_wal_flush_cb(_nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_wal_init_shmem_cb(_stats: &mut dyn core::any::Any) {
    unimplemented!()
}
pub fn pgstat_wal_reset_all_cb(_ts: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_wal_snapshot_cb() {
    unimplemented!()
}

// Functions in pgstat_subscription.c
pub fn pgstat_subscription_flush_cb(_entry_ref: &mut PgStat_EntryRef, _nowait: bool) -> bool {
    unimplemented!()
}
pub fn pgstat_subscription_reset_timestamp_cb(
    _header: &mut PgStatShared_Common,
    _ts: TimestampTz,
) {
    unimplemented!()
}

// Functions in pgstat_xact.c
pub fn pgstat_get_xact_stack_level(_nest_level: i32) -> *mut PgStat_SubXactStatus {
    unimplemented!()
}
pub fn pgstat_drop_transactional(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) {
    unimplemented!()
}
pub fn pgstat_create_transactional(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) {
    unimplemented!()
}

// Variables in pgstat.c. Process-globals: see translation-rules "Global/session
// state"; kept as statics in the skeleton, to become session/task-local later.

/// Track if any pending fixed-numbered statistics should be flushed.
pub static mut pgstat_report_fixed: bool = false;

// pgStatLocal (PgStat_LocalState) is the backend-local stats state; it has no
// const initializer, so it is left to the runtime to construct (no static here).

// Inline functions (translated in full).

/// Begin a change-count write (odd value indicates write in progress).
pub fn pgstat_begin_changecount_write(cc: &mut u32) {
    debug_assert!(*cc & 1 == 0);
    // START_CRIT_SECTION + write barrier: critical-section/barrier deferred.
    *cc += 1;
}

/// End a change-count write.
pub fn pgstat_end_changecount_write(cc: &mut u32) {
    debug_assert!(*cc & 1 == 1);
    *cc += 1;
    // END_CRIT_SECTION + write barrier deferred.
}

/// Begin a change-count read, returning the count seen before the read.
pub fn pgstat_begin_changecount_read(cc: &u32) -> u32 {
    // CHECK_FOR_INTERRUPTS + read barrier deferred.
    *cc
}

/// Returns true if the read succeeded, false if it needs to be repeated.
pub fn pgstat_end_changecount_read(cc: &u32, before_cc: u32) -> bool {
    // read barrier deferred.
    let after_cc = *cc;
    if before_cc & 1 != 0 {
        return false;
    }
    before_cc == after_cc
}

/// Copy `src` to `dst` following the change-count protocol.
pub fn pgstat_copy_changecounted_stats(dst: &mut [u8], src: &[u8], cc: &u32) {
    loop {
        let cc_before = pgstat_begin_changecount_read(cc);
        dst.copy_from_slice(src);
        if pgstat_end_changecount_read(cc, cc_before) {
            break;
        }
    }
}

/// Compare two PgStat_HashKey values (dshash/simplehash comparator).
pub fn pgstat_cmp_hash_key(a: &PgStat_HashKey, b: &PgStat_HashKey) -> bool {
    a == b
}

/// Hash a PgStat_HashKey (dshash/simplehash hash function).
pub fn pgstat_hash_hash_key(d: &PgStat_HashKey) -> u32 {
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (d as *const PgStat_HashKey).cast::<u8>(),
            core::mem::size_of::<PgStat_HashKey>(),
        )
    };
    fasthash32(bytes, 0)
}

/// Length of the data portion of a shared memory stats entry.
pub fn pgstat_get_entry_len(kind: PgStat_Kind) -> usize {
    pgstat_get_kind_info(kind).unwrap().shared_data_len as usize
}

/// Pointer to the data portion of a shared memory stats entry.
pub fn pgstat_get_entry_data(kind: PgStat_Kind, entry: *mut PgStatShared_Common) -> *mut () {
    let off = pgstat_get_kind_info(kind).unwrap().shared_data_off as usize;
    debug_assert!(off != 0);
    unsafe { (entry as *mut u8).add(off) as *mut () }
}

/// Shared memory area of custom stats for fixed-numbered statistics.
/// C used the `pgStatLocal` global; here the state is threaded through.
pub fn pgstat_get_custom_shmem_data(
    local: &mut PgStat_LocalState,
    kind: PgStat_Kind,
) -> &mut Option<Box<dyn core::any::Any>> {
    let idx = (kind - PGSTAT_KIND_CUSTOM_MIN) as usize;
    debug_assert!(pgstat_is_kind_custom(kind));
    debug_assert!(pgstat_get_kind_info(kind).unwrap().flags.contains(KindInfoFlags::FIXED_AMOUNT));
    &mut local.shmem.as_mut().unwrap().custom_data[idx]
}

/// Custom data for fixed-numbered statistics in the current snapshot.
pub fn pgstat_get_custom_snapshot_data(
    local: &mut PgStat_LocalState,
    kind: PgStat_Kind,
) -> &mut Option<Box<dyn core::any::Any>> {
    let idx = (kind - PGSTAT_KIND_CUSTOM_MIN) as usize;
    debug_assert!(pgstat_is_kind_custom(kind));
    debug_assert!(pgstat_get_kind_info(kind).unwrap().flags.contains(KindInfoFlags::FIXED_AMOUNT));
    &mut local.snapshot.custom_data[idx]
}
