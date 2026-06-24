//! Translated from PostgreSQL src/include/replication/walsender_private.h
//!
//! Private definitions from walsender.c. In-memory (shmem under PG; collapses to
//! Arc-shared heap state under the single-process model). Spinlock/latch/CV are
//! tombstoned: `slock_t mutex` -> note (parking_lot::Mutex at the owning site);
//! `ConditionVariable` -> tokio::sync::Notify; the per-walsender FAM array
//! becomes a Vec.

use crate::access::xlogdefs::XLogRecPtr;
use crate::datatype::timestamp::{TimeOffset, TimestampTz};
use crate::nodes::replnodes::ReplicationKind;
use crate::replication::syncrep::NUM_SYNC_REP_WAIT_MODE;

/// `WalSndState` - state of a walsender. Sequential ordinal -> enum.
#[repr(i32)]
pub enum WalSndState {
    Startup = 0,
    Backup,
    Catchup,
    Streaming,
    Stopping,
}

/// `WalSnd` - per-walsender slot (shmem in PG). The `slock_t mutex` field is
/// dropped; under single-process the slot is owned/guarded at the registry
/// (e.g. `parking_lot::Mutex<Vec<WalSnd>>`).
pub struct WalSnd {
    /// This walsender's PID, or 0 if not active.
    pub pid: i32,
    pub state: WalSndState,
    /// WAL has been sent up to this point.
    pub sent_ptr: XLogRecPtr,
    /// Does currently-open file need to be reloaded?
    pub needreload: bool,

    /// Standby-side written / flushed / applied locations (may be invalid).
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,

    /// Measured lag times, or -1 for unknown/none.
    pub write_lag: TimeOffset,
    pub flush_lag: TimeOffset,
    pub apply_lag: TimeOffset,

    /// Priority order in synchronous_standby_names, or 0 if not listed.
    pub sync_standby_priority: i32,

    // C: slock_t mutex -> dropped; guard at the owning registry (parking_lot).
    /// Timestamp of the last message received from standby.
    pub reply_time: TimestampTz,
    pub kind: ReplicationKind,
}

/// Flags for `WalSndCtlData::sync_standbys_status` (`SYNC_STANDBY_*`).
/// Clean single-bit set (bitflags-port.md appendix A -> GOOD).
use bitflags::bitflags;
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SyncStandby: u8 {
        /// Synchronous standby data initialized from the GUC.
        const INIT    = 1 << 0;
        /// Synchronous standby data defined (GUC has data).
        const DEFINED = 1 << 1;
    }
}

/// `WalSndCtlData` - one per cluster (shmem in PG). The condition variables are
/// tombstoned to `tokio::sync::Notify`; the trailing FAM `walsnds[]` becomes a
/// Vec. `SyncRepQueue` (dlist) -> VecDeque of waiters.
pub struct WalSndCtlData {
    /// Synchronous replication queue, one per request type. Protected by
    /// SyncRepLock in PG; a per-mode wait queue here.
    pub sync_rep_queue: [std::collections::VecDeque<()>; NUM_SYNC_REP_WAIT_MODE], // TODO(ptr): SyncRepStandbyData waiter type
    /// Current head-of-queue location per request type.
    pub lsn: [XLogRecPtr; NUM_SYNC_REP_WAIT_MODE],
    /// Status of synchronous-standby data (see `SyncStandby`).
    pub sync_standbys_status: SyncStandby,

    // ConditionVariable -> tokio::sync::Notify (tombstoned); registry of
    // physical/logical walsenders to wake.
    pub wal_flush_cv: (),       // TODO(latch): tokio::sync::Notify
    pub wal_replay_cv: (),      // TODO(latch): tokio::sync::Notify
    pub wal_confirm_rcv_cv: (), // TODO(latch): tokio::sync::Notify

    /// Per-walsender slots (C: FLEXIBLE_ARRAY_MEMBER).
    pub walsnds: Vec<WalSnd>,
}

/// Set the current walsender's state (operates on MyWalSnd in PG).
pub fn wal_snd_set_state(_state: WalSndState) {
    unimplemented!()
}

// Replication grammar parser entry points (repl_gram.y / repl_scanner.l).
// yyscan_t -> opaque scanner handle (FFI); modeled as a raw pointer stub.
// Node** out-param -> &mut Option<Box<Node>> at call sites; kept minimal here.

/// Parse a replication command. Returns the yacc status code.
pub fn replication_yyparse() -> i32 {
    unimplemented!()
}

/// Initialize the replication scanner over `str`.
pub fn replication_scanner_init(_s: &str) {
    unimplemented!()
}

/// Finish / tear down the replication scanner.
pub fn replication_scanner_finish() {
    unimplemented!()
}

/// True if the scanned input is a replication command.
pub fn replication_scanner_is_replication_command() -> bool {
    unimplemented!()
}
