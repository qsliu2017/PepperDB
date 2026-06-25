//! Translated from PostgreSQL src/include/storage/lockdefs.h

use crate::c::TransactionId;
use crate::postgres_ext::Oid;

/// Bit mask of held/requested lock types (bit 1<<mode per mode).
pub type LOCKMASK = i32;

/// Lock type, an integer (1..N). `NoLock` (0) is not a lock mode, but a flag
/// value meaning "don't get a lock".
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    NoLock = 0,
    AccessShareLock = 1,           // SELECT
    RowShareLock = 2,             // SELECT FOR UPDATE/FOR SHARE
    RowExclusiveLock = 3,         // INSERT, UPDATE, DELETE
    ShareUpdateExclusiveLock = 4, // VACUUM, ANALYZE, CIC
    ShareLock = 5,               // CREATE INDEX (non-concurrent)
    ShareRowExclusiveLock = 6,    // EXCLUSIVE but allows ROW SHARE
    ExclusiveLock = 7,           // blocks ROW SHARE/SELECT FOR UPDATE
    AccessExclusiveLock = 8,      // ALTER/DROP TABLE, VACUUM FULL
}

impl LockMode {
    /// Lock used for inplace updates (see README.tuplock).
    pub const INPLACE_UPDATE_TUPLE_LOCK: LockMode = LockMode::ExclusiveLock;
}

/// Highest standard lock mode.
pub const MAX_LOCK_MODE: i32 = 8;

/// WAL representation of an AccessExclusiveLock on a table (on-disk).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct xl_standby_lock {
    pub xid: TransactionId, // xid of holder of AccessExclusiveLock
    pub db_oid: Oid,        // DB containing table
    pub rel_oid: Oid,       // OID of table
}

const _: () = assert!(core::mem::size_of::<xl_standby_lock>() == 12);
const _: () = assert!(core::mem::offset_of!(xl_standby_lock, xid) == 0);
const _: () = assert!(core::mem::offset_of!(xl_standby_lock, db_oid) == 4);
const _: () = assert!(core::mem::offset_of!(xl_standby_lock, rel_oid) == 8);
