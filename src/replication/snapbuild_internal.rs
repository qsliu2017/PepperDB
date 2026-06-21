//! replication/snapbuild_internal.h - internal logical decoding snapshot build state.

use std::ffi::c_void;

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::{uint32, Size as size_t, TransactionId};
use crate::port::pg_crc32c::pg_crc32c;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::snapshot::Snapshot;

// TODO: dedup when replication/snapbuild.h lands.
// SnapBuildState is a C enum defined in snapbuild.h.
pub type SnapBuildState = std::ffi::c_int;

// TODO: dedup when replication/reorderbuffer.h lands.
pub type ReorderBuffer = c_void;

/*
 * Array of transactions which could have catalog changes that committed
 * between xmin and xmax. (anonymous struct in C: SnapBuild.committed)
 */
#[repr(C)]
pub struct SnapBuild_committed {
    /* number of committed transactions */
    pub xcnt: size_t,
    /* available space for committed transactions */
    pub xcnt_space: size_t,
    /*
     * Until we reach a CONSISTENT state, we record commits of all
     * transactions, not just the catalog changing ones. Record when that
     * changes so we know we cannot export a snapshot safely anymore.
     */
    pub includes_all_transactions: bool,
    /* Array of committed transactions that have modified the catalog. */
    pub xip: *mut TransactionId,
}

/*
 * Array of transactions and subtransactions that had modified catalogs
 * and were running when the snapshot was serialized.
 * (anonymous struct in C: SnapBuild.catchange)
 */
#[repr(C)]
pub struct SnapBuild_catchange {
    /* number of transactions */
    pub xcnt: size_t,
    /* This array must be sorted in xidComparator order */
    pub xip: *mut TransactionId,
}

/*
 * This struct contains the current state of the snapshot building
 * machinery. It is exposed to the public, so pay attention when changing its
 * contents.
 */
#[repr(C)]
pub struct SnapBuild {
    /* how far are we along building our first full snapshot */
    pub state: SnapBuildState,

    /* private memory context used to allocate memory for this module. */
    pub context: MemoryContext,

    /* all transactions < than this have committed/aborted */
    pub xmin: TransactionId,

    /* all transactions >= than this are uncommitted */
    pub xmax: TransactionId,

    /*
     * Don't replay commits from an LSN < this LSN. This can be set externally
     * but it will also be advanced (never retreat) from within snapbuild.c.
     */
    pub start_decoding_at: XLogRecPtr,

    /*
     * LSN at which two-phase decoding was enabled or LSN at which we found a
     * consistent point at the time of slot creation.
     */
    pub two_phase_at: XLogRecPtr,

    /*
     * Don't start decoding WAL until the "xl_running_xacts" information
     * indicates there are no running xids with an xid smaller than this.
     */
    pub initial_xmin_horizon: TransactionId,

    /* Indicates if we are building full snapshot or just catalog one. */
    pub building_full_snapshot: bool,

    /*
     * Indicates if we are using the snapshot builder for the creation of a
     * logical replication slot.
     */
    pub in_slot_creation: bool,

    /* Snapshot that's valid to see the catalog state seen at this moment. */
    pub snapshot: Snapshot,

    /* LSN of the last location we are sure a snapshot has been serialized to. */
    pub last_serialized_snapshot: XLogRecPtr,

    /* The reorderbuffer we need to update with usable snapshots et al. */
    pub reorder: *mut ReorderBuffer,

    /*
     * TransactionId at which the next phase of initial snapshot building will
     * happen.
     */
    pub next_phase_at: TransactionId,

    /*
     * Array of transactions which could have catalog changes that committed
     * between xmin and xmax.
     */
    pub committed: SnapBuild_committed,

    /*
     * Array of transactions and subtransactions that had modified catalogs
     * and were running when the snapshot was serialized.
     */
    pub catchange: SnapBuild_catchange,
}

/* -----------------------------------
 * Snapshot serialization support
 * -----------------------------------
 */

/*
 * We store current state of struct SnapBuild on disk in the following manner:
 *
 * struct SnapBuildOnDisk;
 * TransactionId * committed.xcnt; (*not xcnt_space*)
 * TransactionId * catchange.xcnt;
 */
#[repr(C)]
pub struct SnapBuildOnDisk {
    /* first part of this struct needs to be version independent */

    /* data not covered by checksum */
    pub magic: uint32,
    pub checksum: pg_crc32c,

    /* data covered by checksum */

    /* version, in case we want to support pg_upgrade */
    pub version: uint32,
    /* how large is the on disk data, excluding the constant sized part */
    pub length: uint32,

    /* version dependent part */
    pub builder: SnapBuild,
    /* variable amount of TransactionIds follows */
}

pub unsafe fn SnapBuildRestoreSnapshot(
    ondisk: *mut SnapBuildOnDisk,
    lsn: XLogRecPtr,
    context: MemoryContext,
    missing_ok: bool,
) -> bool { crate::replication::logical::snapbuild::SnapBuildRestoreSnapshot(ondisk as _, lsn as _, context, missing_ok) }
