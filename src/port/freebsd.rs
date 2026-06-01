//! port/freebsd.h - FreeBSD platform tweaks.

use crate::access::transam::xlogdefs::WAL_SYNC_METHOD_FDATASYNC;

/// Set the default wal_sync_method to fdatasync.  xlogdefs.h's normal rules
/// would prefer open_datasync on FreeBSD 13+, but that is not a good choice on
/// many systems.
pub const PLATFORM_DEFAULT_WAL_SYNC_METHOD: std::ffi::c_int = WAL_SYNC_METHOD_FDATASYNC;
