//! Translated from PostgreSQL src/include/replication/snapbuild_internal.h
//!
//! Declarations for logical decoding utility functions for internal use.

#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]
#![allow(clippy::needless_pass_by_value, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::nodes::memnodes::MemoryContext;
use crate::port::pg_crc32c::pg_crc32c;
use crate::replication::reorderbuffer::ReorderBuffer;
use crate::replication::snapbuild::SnapBuildState;
use crate::utils::snapshot::SnapshotData;

/// Committed transactions that have modified the catalog (in-memory).
#[derive(Debug, Default)]
pub struct SnapBuildCommitted {
    /// number of committed transactions
    pub xcnt: usize,
    /// available space for committed transactions
    pub xcnt_space: usize,
    /// before CONSISTENT we record all commits, not just catalog-changing ones
    pub includes_all_transactions: bool,
    /// committed transactions that modified the catalog (not kept sorted)
    pub xip: Vec<TransactionId>,
}

/// Transactions/subtransactions that modified catalogs and were running at
/// serialization time (in-memory).
#[derive(Debug, Default)]
pub struct SnapBuildCatchange {
    /// number of transactions
    pub xcnt: usize,
    /// must be sorted in xidComparator order
    pub xip: Vec<TransactionId>,
}

/// Current state of the snapshot building machinery. Exposed to the public.
// Canonical body for the forward `SnapBuild` declared in snapbuild.rs.
// (No derive(Debug): holds ReorderBuffer/SnapshotData/MemoryContext, none Debug.)
pub struct SnapBuild {
    /// how far are we along building our first full snapshot
    pub state: SnapBuildState,

    /// private memory context used to allocate memory for this module
    pub context: MemoryContext,

    /// all transactions < than this have committed/aborted
    pub xmin: TransactionId,

    /// all transactions >= than this are uncommitted
    pub xmax: TransactionId,

    /// Don't replay commits from an LSN < this LSN. May be advanced (never retreats).
    pub start_decoding_at: XLogRecPtr,

    /// LSN at which two-phase decoding was enabled, or the consistent point at
    /// slot creation time.
    pub two_phase_at: XLogRecPtr,

    /// Don't start decoding WAL until xl_running_xacts shows no running xid
    /// smaller than this.
    pub initial_xmin_horizon: TransactionId,

    /// building a full snapshot or just a catalog one?
    pub building_full_snapshot: bool,

    /// using the builder for logical replication slot creation? (start point not
    /// determined yet, so we skip snapshot restores)
    pub in_slot_creation: bool,

    /// snapshot valid to see the catalog state seen at this moment
    pub snapshot: Option<Box<SnapshotData>>,

    /// LSN of the last location a snapshot has been serialized to
    pub last_serialized_snapshot: XLogRecPtr,

    /// reorderbuffer to update with usable snapshots et al.
    pub reorder: Option<Box<ReorderBuffer>>,

    /// xid at which the next initial-snapshot-building phase happens;
    /// InvalidTransactionId if not known or no next phase necessary.
    pub next_phase_at: TransactionId,

    /// transactions with possible catalog changes committed between xmin and xmax
    pub committed: SnapBuildCommitted,

    /// running catalog-modifying transactions captured at serialization time
    pub catchange: SnapBuildCatchange,
}

/* -----------------------------------
 * Snapshot serialization support
 * -----------------------------------
 */

pub const SNAPBUILD_MAGIC: u32 = 0x51A1E001;
pub const SNAPBUILD_VERSION: u32 = 6;

/// We store struct SnapBuild on disk as:
///   struct SnapBuildOnDisk;
///   TransactionId * committed.xcnt;  (not xcnt_space)
///   TransactionId * catchange.xcnt;
// On-disk header. Embeds the full SnapBuild as in C; the trailing variable-length
// TransactionId arrays live in the buffer after this header.
#[repr(C)]
pub struct SnapBuildOnDisk {
    /* first part of this struct needs to be version independent */
    /* data not covered by checksum */
    pub magic: u32,
    pub checksum: pg_crc32c,

    /* data covered by checksum */
    /// version, in case we want to support pg_upgrade
    pub version: u32,
    /// on-disk data size, excluding the constant-sized part
    pub length: u32,

    /* version dependent part */
    pub builder: SnapBuild,
    /* variable amount of TransactionIds follows */
}

/// `offsetof(SnapBuildOnDisk, builder)` -- constant-sized leading part.
pub const SNAP_BUILD_ON_DISK_CONSTANT_SIZE: usize = core::mem::offset_of!(SnapBuildOnDisk, builder);
/// `offsetof(SnapBuildOnDisk, version)` -- bytes before the checksummed region.
pub const SNAP_BUILD_ON_DISK_NOT_CHECKSUMMED_SIZE: usize =
    core::mem::offset_of!(SnapBuildOnDisk, version);

/// `missing_ok` selects "not found is ok" vs. error; success returns the LSN/state.
pub fn snap_build_restore_snapshot(
    ondisk: &mut SnapBuildOnDisk,
    lsn: XLogRecPtr,
    context: MemoryContext,
    missing_ok: bool,
) -> bool {
    unimplemented!()
}
