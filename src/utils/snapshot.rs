//! utils/snapshot.h - POSTGRES snapshot definition.

use std::ffi::c_int;
use std::ptr;

use crate::c::{uint32, uint64, int32, CommandId, TransactionId};
use crate::lib::pairingheap::pairingheap_node;

/*
 * The different snapshot types.  We use SnapshotData structures to represent
 * both "regular" (MVCC) snapshots and "special" snapshots that have non-MVCC
 * semantics.  The specific semantics of a snapshot are encoded by its type.
 *
 * (C enum SnapshotType -> type alias + consts, per project convention.)
 */
pub type SnapshotType = c_int;

/*
 * A tuple is visible iff the tuple is valid for the given MVCC snapshot.
 */
pub const SNAPSHOT_MVCC: SnapshotType = 0;
/*
 * A tuple is visible iff the tuple is valid "for itself".
 */
pub const SNAPSHOT_SELF: SnapshotType = 1;
/*
 * Any tuple is visible.
 */
pub const SNAPSHOT_ANY: SnapshotType = 2;
/*
 * A tuple is visible iff the tuple is valid as a TOAST row.
 */
pub const SNAPSHOT_TOAST: SnapshotType = 3;
/*
 * A tuple is visible iff the tuple is valid including effects of open
 * transactions.
 */
pub const SNAPSHOT_DIRTY: SnapshotType = 4;
/*
 * A tuple is visible iff it follows the rules of SNAPSHOT_MVCC, but
 * supports being called in timetravel context (for decoding catalog
 * contents in the context of logical decoding).
 */
pub const SNAPSHOT_HISTORIC_MVCC: SnapshotType = 5;
/*
 * A tuple is visible iff the tuple might be visible to some transaction;
 * false if it's surely dead to everyone, i.e., vacuumable.
 */
pub const SNAPSHOT_NON_VACUUMABLE: SnapshotType = 6;

pub type Snapshot = *mut SnapshotData;

// #define InvalidSnapshot ((Snapshot) NULL)
#[allow(non_upper_case_globals)]
pub const InvalidSnapshot: Snapshot = ptr::null_mut();

/*
 * GlobalVisState - opaque, defined in procarray.c / snapmgr; stub for now.
 */
// TODO: dedup when the canonical definition lands.
pub type GlobalVisState = std::ffi::c_void;

/*
 * Struct representing all kind of possible snapshots.
 */
#[repr(C)]
pub struct SnapshotData {
    pub snapshot_type: SnapshotType, /* type of snapshot */

    pub xmin: TransactionId, /* all XID < xmin are visible to me */
    pub xmax: TransactionId, /* all XID >= xmax are invisible to me */

    pub xip: *mut TransactionId,
    pub xcnt: uint32, /* # of xact ids in xip[] */

    pub subxip: *mut TransactionId,
    pub subxcnt: int32,     /* # of xact ids in subxip[] */
    pub suboverflowed: bool, /* has the subxip array overflowed? */

    pub takenDuringRecovery: bool, /* recovery-shaped snapshot? */
    pub copied: bool,              /* false if it's a static snapshot */

    pub curcid: CommandId, /* in my xact, CID < curcid are visible */

    /*
     * An extra return value for HeapTupleSatisfiesDirty, not used in MVCC
     * snapshots.
     */
    pub speculativeToken: uint32,

    /*
     * For SNAPSHOT_NON_VACUUMABLE (and hopefully more in the future) this is
     * used to determine whether row could be vacuumed.
     */
    pub vistest: *mut GlobalVisState,

    /*
     * Book-keeping information, used by the snapshot manager
     */
    pub active_count: uint32, /* refcount on ActiveSnapshot stack */
    pub regd_count: uint32,   /* refcount on RegisteredSnapshots */
    pub ph_node: pairingheap_node, /* link in the RegisteredSnapshots heap */

    /*
     * The transaction completion count at the time GetSnapshotData() built
     * this snapshot.
     */
    pub snapXactCompletionCount: uint64,
}
