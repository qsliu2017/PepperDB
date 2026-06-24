//! Translated from PostgreSQL src/include/replication/slot.h
//! Replication slot management.
//!
//! `ReplicationSlotPersistentData` is the ON-DISK slot state file (the
//! `pg_replslot/<name>/state` payload), so it is `#[repr(C)]` with exact field
//! order/types and layout asserts. `ReplicationSlot` / `ReplicationSlotCtlData`
//! are shared-memory state: per LEVEL2-NOTES the single-process model drops the
//! spinlock / LWLock / ConditionVariable (`slock_t`, `LWLock`,
//! `ConditionVariable`) - replaced with owned/std types and short notes.

use crate::access::xlogdefs::{XLogRecPtr, XLogSegNo};
use crate::c::{NameData, TransactionId};
use crate::datatype::timestamp::TimestampTz;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::replication::walreceiver::WalReceiverConn;

/// Directory to store replication slot data in.
pub const PG_REPLSLOT_DIR: &str = "pg_replslot";

/// Behaviour of replication slots, upon release or crash. Sequential ordinal
/// enum (POOR for bitflags) - it is a one-of-N selector, not OR-able.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSlotPersistency {
    Persistent,
    Ephemeral,
    Temporary,
}

/// Reason a slot has been invalidated. The C enum members are powers of two and
/// also OR-ed together as `possible_causes` masks - kept as a plain enum here so
/// `invalidated` round-trips the exact discriminant; the explicit values match C.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSlotInvalidationCause {
    None = 0,
    /// required WAL has been removed
    WalRemoved = 1 << 0,
    /// required rows have been removed
    Horizon = 1 << 1,
    /// wal_level insufficient for slot
    WalLevel = 1 << 2,
    /// idle slot timeout has occurred
    IdleTimeout = 1 << 3,
}

/// Maximum number of invalidation causes.
pub const RS_INVAL_MAX_CAUSES: usize = 4;

/// ON-DISK data of a replication slot, preserved across restarts.
///
/// This is the slot state file layout; field order and types are exact. `synced`
/// is `char` on disk (not `bool`) - kept as `u8`. `persistency` and `invalidated`
/// are C enums (4-byte `int` storage) - represented as `i32` to fix the on-disk
/// width, with conversions to the Rust enums above.
#[repr(C)]
pub struct ReplicationSlotPersistentData {
    /// The slot's identifier.
    pub name: NameData,
    /// Database the slot is active on.
    pub database: Oid,
    /// The slot's behaviour when being dropped (C enum, 4-byte). See
    /// `ReplicationSlotPersistency`.
    pub persistency: i32,
    /// xmin horizon for data.
    pub xmin: TransactionId,
    /// xmin horizon for catalog tuples.
    pub catalog_xmin: TransactionId,
    /// Oldest LSN that might be required by this replication slot.
    pub restart_lsn: XLogRecPtr,
    /// RS_INVAL_NONE if valid, or the reason for invalidation (C enum, 4-byte).
    pub invalidated: i32,
    /// Oldest LSN that the client has acked receipt for.
    pub confirmed_flush: XLogRecPtr,
    /// LSN at which two_phase commit was enabled / consistent point found.
    pub two_phase_at: XLogRecPtr,
    /// Allow decoding of prepared transactions?
    pub two_phase: bool,
    /// Plugin name.
    pub plugin: NameData,
    /// Was this slot synchronized from the primary server? (`char` on disk.)
    pub synced: u8,
    /// Is this a failover slot (sync candidate for standbys)?
    pub failover: bool,
}

const _: () = assert!(core::mem::offset_of!(ReplicationSlotPersistentData, name) == 0);
const _: () = assert!(core::mem::offset_of!(ReplicationSlotPersistentData, database) == 64);

/// Shared-memory state of a single replication slot. In-memory: the C struct is
/// guarded by `slock_t mutex`, `LWLock io_in_progress_lock`, and
/// `ConditionVariable active_cv`; the single-process model drops them (see notes
/// on the affected fields). `data` embeds the on-disk persistent record.
pub struct ReplicationSlot {
    // slock_t mutex -> dropped (single-process; was on same cacheline as
    // effective_xmin). Use std::sync if a guard is later needed.
    /// Is this slot defined?
    pub in_use: bool,
    /// Who is streaming out changes for this slot? 0 in unused slots.
    pub active_pid: i32,
    /// Any outstanding modifications?
    pub just_dirtied: bool,
    pub dirty: bool,
    /// Latest xmin actually written to disk (logical) / `data.xmin` (physical).
    pub effective_xmin: TransactionId,
    pub effective_catalog_xmin: TransactionId,
    /// Data surviving shutdowns and crashes (on-disk record).
    pub data: ReplicationSlotPersistentData,
    // LWLock io_in_progress_lock -> dropped (single-process).
    // ConditionVariable active_cv (signaled when active_pid changes) ->
    // tokio::sync::Notify when wakeups are wired up; dropped here.

    // remaining fields are only used for logical slots
    pub candidate_catalog_xmin: TransactionId,
    pub candidate_xmin_lsn: XLogRecPtr,
    pub candidate_restart_valid: XLogRecPtr,
    pub candidate_restart_lsn: XLogRecPtr,
    /// Last confirmed_flush LSN flushed (shutdown-checkpoint decision).
    pub last_saved_confirmed_flush: XLogRecPtr,
    /// Time the slot became inactive (or sync last stopped, for synced slots).
    pub inactive_since: TimestampTz,
    /// Latest restart_lsn flushed to disk.
    pub last_saved_restart_lsn: XLogRecPtr,
}

/// C: `#define SlotIsPhysical(slot)`.
pub fn slot_is_physical(slot: &ReplicationSlot) -> bool {
    slot.data.database == InvalidOid
}

/// C: `#define SlotIsLogical(slot)`.
pub fn slot_is_logical(slot: &ReplicationSlot) -> bool {
    slot.data.database != InvalidOid
}

/// Shared-memory control area for all replication slots. The C FLEXIBLE_ARRAY of
/// `ReplicationSlot[1]` becomes an owned `Vec` (single-process; in-memory).
pub struct ReplicationSlotCtlData {
    pub replication_slots: Vec<ReplicationSlot>,
}

/// Set slot's inactive_since property unless previously invalidated. The C
/// `acquire_lock` toggles a SpinLockAcquire on `s->mutex`; the spinlock is
/// dropped under single-process, so the flag is gone (caller-side locking later).
pub fn replication_slot_set_inactive_since(s: &mut ReplicationSlot, ts: TimestampTz) {
    if s.data.invalidated == ReplicationSlotInvalidationCause::None as i32 {
        s.inactive_since = ts;
    }
}

// Pointers to shared memory (process globals -> session/global state later).
pub static mut ReplicationSlotCtl: Option<*mut ReplicationSlotCtlData> = None; // TODO(ptr): Arc-shared
pub static mut MyReplicationSlot: Option<*mut ReplicationSlot> = None; // TODO(ptr)

// GUCs.
pub static mut max_replication_slots: i32 = 0;
pub static mut synchronized_standby_slots: Option<String> = None;
pub static mut idle_replication_slot_timeout_secs: i32 = 0;

// shmem initialization functions
pub fn ReplicationSlotsShmemSize() -> usize {
    unimplemented!()
}
pub fn ReplicationSlotsShmemInit() {
    unimplemented!()
}

// management of individual slots
pub fn ReplicationSlotCreate(
    _name: &str,
    _db_specific: bool,
    _persistency: ReplicationSlotPersistency,
    _two_phase: bool,
    _failover: bool,
    _synced: bool,
) {
    unimplemented!()
}
pub fn ReplicationSlotPersist() {
    unimplemented!()
}
pub fn ReplicationSlotDrop(_name: &str, _nowait: bool) {
    unimplemented!()
}
pub fn ReplicationSlotDropAcquired() {
    unimplemented!()
}
/// C out-params `const bool *failover`/`*two_phase` are skippable -> `Option`.
pub fn ReplicationSlotAlter(_name: &str, _failover: Option<bool>, _two_phase: Option<bool>) {
    unimplemented!()
}

pub fn ReplicationSlotAcquire(_name: &str, _nowait: bool, _error_if_invalid: bool) {
    unimplemented!()
}
pub fn ReplicationSlotRelease() {
    unimplemented!()
}
pub fn ReplicationSlotCleanup(_synced_only: bool) {
    unimplemented!()
}
pub fn ReplicationSlotSave() {
    unimplemented!()
}
pub fn ReplicationSlotMarkDirty() {
    unimplemented!()
}

// misc stuff
pub fn ReplicationSlotInitialize() {
    unimplemented!()
}
pub fn ReplicationSlotValidateName(_name: &str, _elevel: i32) -> bool {
    unimplemented!()
}
/// C returns bool + fills `err_code`/`err_msg`/`err_hint` out-params -> on
/// failure return the (code, msg, hint) triple as the error variant.
pub fn ReplicationSlotValidateNameInternal(
    _name: &str,
) -> Result<(), (i32, String, String)> {
    unimplemented!()
}
pub fn ReplicationSlotReserveWal() {
    unimplemented!()
}
pub fn ReplicationSlotsComputeRequiredXmin(_already_locked: bool) {
    unimplemented!()
}
pub fn ReplicationSlotsComputeRequiredLSN() {
    unimplemented!()
}
pub fn ReplicationSlotsComputeLogicalRestartLSN() -> XLogRecPtr {
    unimplemented!()
}
/// C returns bool + `nslots`/`nactive` out-params -> `Option<(nslots, nactive)>`
/// (false -> None).
pub fn ReplicationSlotsCountDBSlots(_dboid: Oid) -> Option<(i32, i32)> {
    unimplemented!()
}
pub fn ReplicationSlotsDropDBSlots(_dboid: Oid) {
    unimplemented!()
}
pub fn InvalidateObsoleteReplicationSlots(
    _possible_causes: u32,
    _oldest_segno: XLogSegNo,
    _dboid: Oid,
    _snapshot_conflict_horizon: TransactionId,
) -> bool {
    unimplemented!()
}
/// C returns NULL when not found -> `Option`.
pub fn SearchNamedReplicationSlot(_name: &str, _need_lock: bool) -> Option<*mut ReplicationSlot> {
    unimplemented!()
}
pub fn ReplicationSlotIndex(_slot: &ReplicationSlot) -> i32 {
    unimplemented!()
}
/// C returns bool + fills `Name name` out-param -> `Option<NameData>`.
pub fn ReplicationSlotName(_index: i32) -> Option<NameData> {
    unimplemented!()
}
pub fn ReplicationSlotNameForTablesync(_suboid: Oid, _relid: Oid) -> String {
    unimplemented!()
}
pub fn ReplicationSlotDropAtPubNode(
    _wrconn: &mut WalReceiverConn,
    _slotname: &str,
    _missing_ok: bool,
) {
    unimplemented!()
}

pub fn StartupReplicationSlots() {
    unimplemented!()
}
pub fn CheckPointReplicationSlots(_is_shutdown: bool) {
    unimplemented!()
}

pub fn CheckSlotRequirements() {
    unimplemented!()
}
pub fn CheckSlotPermissions() {
    unimplemented!()
}
pub fn GetSlotInvalidationCause(_cause_name: &str) -> ReplicationSlotInvalidationCause {
    unimplemented!()
}
pub fn GetSlotInvalidationCauseName(_cause: ReplicationSlotInvalidationCause) -> &'static str {
    unimplemented!()
}

pub fn SlotExistsInSyncStandbySlots(_slot_name: &str) -> bool {
    unimplemented!()
}
pub fn StandbySlotsHaveCaughtup(_wait_for_lsn: XLogRecPtr, _elevel: i32) -> bool {
    unimplemented!()
}
pub fn WaitForStandbyConfirmation(_wait_for_lsn: XLogRecPtr) {
    unimplemented!()
}
