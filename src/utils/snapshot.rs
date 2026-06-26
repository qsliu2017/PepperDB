//! Translated from PostgreSQL src/include/utils/snapshot.h

use crate::access::transam::FullTransactionId;
use crate::c::{CommandId, TransactionId};

/// The different snapshot types (MVCC plus non-MVCC special semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotType {
    /// Visible iff valid for the given MVCC snapshot.
    Mvcc = 0,
    /// Visible iff valid "for itself".
    Self_,
    /// Any tuple is visible.
    Any,
    /// Visible iff valid as a TOAST row.
    Toast,
    /// Visible including effects of open transactions.
    Dirty,
    /// MVCC, but callable in timetravel context (logical decoding).
    HistoricMvcc,
    /// Visible iff might be visible to some transaction (else vacuumable).
    NonVacuumable,
}

/// State for the `GlobalVisTest*` family (procarray.c owns the real shape).
/// Two FullTransactionId boundaries computed while building a snapshot:
/// XIDs `>= definitely_needed` are still considered running by someone; XIDs
/// `< maybe_needed` are removable. The in-between range is resolved by
/// recomputing horizons (see `backend::storage::ipc::procarray`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalVisState {
    /// XIDs >= are considered running by some backend.
    pub definitely_needed: FullTransactionId,
    /// XIDs < are not considered to be running by any backend.
    pub maybe_needed: FullTransactionId,
}

/// In-memory snapshot. xip/subxip C arrays -> Vec; pairingheap link dropped
/// (registration tracked by the snapshot manager's own container).
#[derive(Clone)]
pub struct SnapshotData {
    pub snapshot_type: SnapshotType,

    pub xmin: TransactionId, // all XID < xmin are visible to me
    pub xmax: TransactionId, // all XID >= xmax are invisible to me

    pub xip: Vec<TransactionId>, // in-progress xacts (xcnt = xip.len())
    pub subxip: Vec<TransactionId>, // in-progress subxacts (subxcnt = subxip.len())
    pub suboverflowed: bool,     // has the subxip array overflowed?

    pub taken_during_recovery: bool,
    pub copied: bool, // false if it's a static snapshot

    pub curcid: CommandId, // in my xact, CID < curcid are visible

    pub speculative_token: u32, // extra return value for SatisfiesDirty

    pub vistest: Option<Box<GlobalVisState>>, // for NON_VACUUMABLE; TODO(ptr)

    pub active_count: u32, // refcount on ActiveSnapshot stack
    pub regd_count: u32,   // refcount on RegisteredSnapshots

    pub snap_xact_completion_count: u64,
}

/// C: `typedef struct SnapshotData *Snapshot;`. Shared ownership via `Arc` so a
/// snapshot can be handed to multiple holders (active stack, registered set,
/// callers) without aliasing `&mut`; nullable -> `Option`. Cloning is a refcount
/// bump; curcid mutation uses `Arc::make_mut` (copy-on-write) in snapmgr.
pub type Snapshot = Option<std::sync::Arc<SnapshotData>>;

/// `InvalidSnapshot` == NULL.
pub fn invalid_snapshot() -> Snapshot {
    None
}
