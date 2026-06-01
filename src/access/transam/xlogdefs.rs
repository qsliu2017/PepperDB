//! access/xlogdefs.h - WAL manager record pointer and timeline number definitions.

use crate::c::{uint16, uint32, uint64};

// ===========================================================================
// XLogRecPtr - pointer to a location in the XLOG.
//
// These pointers are 64 bits wide, because we don't want them ever to overflow.
// ===========================================================================

/// Pointer to a location in the XLOG (byte position in the WAL stream).
pub type XLogRecPtr = uint64;

/// Zero is used to indicate an invalid pointer. Bootstrap skips the first
/// possible WAL segment, initializing the first WAL page at WAL segment size,
/// so no XLOG record can begin at zero.
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

/// `XLogRecPtrIsValid(r)` - true when `r` is not the invalid sentinel.
#[inline]
pub fn XLogRecPtrIsValid(r: XLogRecPtr) -> bool {
    r != InvalidXLogRecPtr
}

/// `XLogRecPtrIsInvalid(r)` - true when `r` is the invalid sentinel.
#[inline]
pub fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// First LSN to use for "fake" LSNs.
///
/// Values smaller than this can be used for special per-AM purposes.
pub const FirstNormalUnloggedLSN: XLogRecPtr = 1000;

/// Handy helper for printing XLogRecPtr in conventional format, e.g.,
///
/// ```text
/// printf("%X/%X", LSN_FORMAT_ARGS(lsn));
/// ```
///
/// In C this expands to two `uint32` arguments (high word, low word). Here we
/// return them as a tuple. The C macro's `AssertVariableIsOfTypeMacro` type
/// check is unnecessary in Rust because the signature enforces the type.
#[inline]
pub fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (uint32, uint32) {
    ((lsn >> 32) as uint32, lsn as uint32)
}

// ===========================================================================
// XLogSegNo - physical log file sequence number.
// ===========================================================================

/// XLogSegNo - physical log file sequence number.
pub type XLogSegNo = uint64;

// ===========================================================================
// TimeLineID (TLI) - identifies different database histories to prevent
// confusion after restoring a prior state of a database installation. TLI does
// not change in a normal stop/restart of the database (including
// crash-and-recover cases); but we must assign a new TLI after doing a recovery
// to a prior state, a/k/a point-in-time recovery. This makes the new WAL
// logfile sequence we generate distinguishable from the sequence that was
// generated in the previous incarnation.
// ===========================================================================

/// TimeLineID (TLI) - identifies different database histories.
pub type TimeLineID = uint32;

// ===========================================================================
// Replication origin id - this is located in this file to avoid having to
// include origin.h in a bunch of xlog related places.
// ===========================================================================

/// Replication origin id.
pub type RepOriginId = uint16;

// ===========================================================================
// Default WAL sync method.
//
// The C header uses platform feature macros (O_DSYNC / O_SYNC /
// PLATFORM_DEFAULT_WAL_SYNC_METHOD) to choose a default. On Linux and FreeBSD
// PLATFORM_DEFAULT_WAL_SYNC_METHOD is WAL_SYNC_METHOD_FDATASYNC; on platforms
// where O_DSYNC differs from O_SYNC the default is WAL_SYNC_METHOD_OPEN_DSYNC.
//
// The WalSyncMethod enum itself lives in access/xlog.h (translated separately).
// We re-declare the two discriminants we reference so this module does not
// depend on xlog.rs landing first.
// TODO: dedup WAL_SYNC_METHOD_* against access/transam/xlog.rs (xlog.h) when it
// lands; keep these as the canonical WalSyncMethod values (FSYNC=0, ...).
// ===========================================================================

/// `WAL_SYNC_METHOD_FDATASYNC` discriminant from access/xlog.h's WalSyncMethod
/// (FSYNC = 0, FDATASYNC = 1, OPEN = 2, FSYNC_WRITETHROUGH = 3, OPEN_DSYNC = 4).
pub const WAL_SYNC_METHOD_FDATASYNC: std::ffi::c_int = 1;

/// `WAL_SYNC_METHOD_OPEN_DSYNC` discriminant from access/xlog.h's WalSyncMethod.
pub const WAL_SYNC_METHOD_OPEN_DSYNC: std::ffi::c_int = 4;

/// Default sync method for WAL files.
///
/// On darwin/Linux/FreeBSD (PLATFORM_DEFAULT_WAL_SYNC_METHOD or the generic
/// O_DSYNC branch resolving to FDATASYNC) this is WAL_SYNC_METHOD_FDATASYNC.
#[cfg(not(target_os = "windows"))]
pub const DEFAULT_WAL_SYNC_METHOD: std::ffi::c_int = WAL_SYNC_METHOD_FDATASYNC;

/// On Windows, where we define our own O_DSYNC, the default is OPEN_DSYNC.
#[cfg(target_os = "windows")]
pub const DEFAULT_WAL_SYNC_METHOD: std::ffi::c_int = WAL_SYNC_METHOD_OPEN_DSYNC;
