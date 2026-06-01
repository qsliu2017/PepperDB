//! storage/lockdefs.h - Frontend exposed parts of postgres' low level lock mechanism

use crate::c::TransactionId;
use crate::postgres_ext::Oid;
use std::ffi::c_int;

/*
 * LOCKMODE is an integer (1..N) indicating a lock type.  LOCKMASK is a bit
 * mask indicating a set of held or requested lock types (the bit 1<<mode
 * corresponds to a particular lock mode).
 */
pub type LOCKMASK = c_int;
pub type LOCKMODE = c_int;

/*
 * These are the valid values of type LOCKMODE for all the standard lock
 * methods (both DEFAULT and USER).
 */

/* NoLock is not a lock mode, but a flag value meaning "don't get a lock" */
pub const NoLock: LOCKMODE = 0;

pub const AccessShareLock: LOCKMODE = 1; /* SELECT */
pub const RowShareLock: LOCKMODE = 2; /* SELECT FOR UPDATE/FOR SHARE */
pub const RowExclusiveLock: LOCKMODE = 3; /* INSERT, UPDATE, DELETE */
pub const ShareUpdateExclusiveLock: LOCKMODE = 4; /* VACUUM (non-FULL), ANALYZE, CREATE
                                                   * INDEX CONCURRENTLY */
pub const ShareLock: LOCKMODE = 5; /* CREATE INDEX (WITHOUT CONCURRENTLY) */
pub const ShareRowExclusiveLock: LOCKMODE = 6; /* like EXCLUSIVE MODE, but allows ROW
                                               * SHARE */
pub const ExclusiveLock: LOCKMODE = 7; /* blocks ROW SHARE/SELECT...FOR UPDATE */
pub const AccessExclusiveLock: LOCKMODE = 8; /* ALTER TABLE, DROP TABLE, VACUUM FULL,
                                             * and unqualified LOCK TABLE */

pub const MaxLockMode: LOCKMODE = 8; /* highest standard lock mode */

/* See README.tuplock section "Locking to write inplace-updated tables" */
pub const InplaceUpdateTupleLock: LOCKMODE = ExclusiveLock;

/* WAL representation of an AccessExclusiveLock on a table */
#[repr(C)]
pub struct xl_standby_lock {
    pub xid: TransactionId, /* xid of holder of AccessExclusiveLock */
    pub dbOid: Oid,         /* DB containing table */
    pub relOid: Oid,        /* OID of table */
}
