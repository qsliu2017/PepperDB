//! Translated from PostgreSQL src/include/utils/snapshot.h

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

// TODO(struct-forward): GlobalVisState lives in access/heapam (procarray.c).
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage's GlobalVisState in Phase 2")]
pub struct GlobalVisState;

/// In-memory snapshot. xip/subxip C arrays -> Vec; pairingheap link dropped
/// (registration tracked by the snapshot manager's own container).
pub struct SnapshotData {
    pub snapshot_type: SnapshotType,

    pub xmin: TransactionId, // all XID < xmin are visible to me
    pub xmax: TransactionId, // all XID >= xmax are invisible to me

    pub xip: Vec<TransactionId>,    // in-progress xacts (xcnt = xip.len())
    pub subxip: Vec<TransactionId>, // in-progress subxacts (subxcnt = subxip.len())
    pub suboverflowed: bool,        // has the subxip array overflowed?

    pub taken_during_recovery: bool,
    pub copied: bool, // false if it's a static snapshot

    pub curcid: CommandId, // in my xact, CID < curcid are visible

    pub speculative_token: u32, // extra return value for SatisfiesDirty

    #[allow(deprecated)]
    pub vistest: Option<Box<GlobalVisState>>, // for NON_VACUUMABLE; TODO(ptr)

    pub active_count: u32, // refcount on ActiveSnapshot stack
    pub regd_count: u32,   // refcount on RegisteredSnapshots

    pub snap_xact_completion_count: u64,
}

/// C: `typedef struct SnapshotData *Snapshot;` nullable -> Option<&SnapshotData>.
pub type Snapshot<'a> = Option<&'a mut SnapshotData>;

/// `InvalidSnapshot` == NULL.
pub const fn invalid_snapshot<'a>() -> Snapshot<'a> {
    None
}
