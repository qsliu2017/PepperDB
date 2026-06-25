//! Translated from PostgreSQL src/include/access/xlogdefs.h

use crate::access::xlog::WalSyncMethod;

/// Pointer to a location in the XLOG (64 bits wide). Zero means invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct XLogRecPtr(pub u64);

pub const INVALID_XLOG_REC_PTR: XLogRecPtr = XLogRecPtr(0);

impl XLogRecPtr {
    pub const fn is_valid(self) -> bool {
        self.0 != INVALID_XLOG_REC_PTR.0
    }

    pub const fn is_invalid(self) -> bool {
        self.0 == INVALID_XLOG_REC_PTR.0
    }

    /// (high32, low32) for the conventional "%X/%X" format.
    pub const fn format_args(self) -> (u32, u32) {
        ((self.0 >> 32) as u32, self.0 as u32)
    }
}

/// First LSN to use for "fake" LSNs; smaller values are per-AM special uses.
pub const FIRST_NORMAL_UNLOGGED_LSN: XLogRecPtr = XLogRecPtr(1000);

/// Physical log file sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct XLogSegNo(pub u64);

/// Identifies different database histories (point-in-time recovery).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct TimeLineID(pub u32);

/// Replication origin id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct RepOriginId(pub u16);

/// On Linux/macOS O_DSYNC is available and distinct from O_SYNC.
pub const DEFAULT_WAL_SYNC_METHOD: WalSyncMethod = WalSyncMethod::OpenDsync;
