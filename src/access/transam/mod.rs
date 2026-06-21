//! Translation of postgres/src/include/access/transam.h (type + macro layer only).
//!
//! Transaction-ID types and the inline comparison/conversion helpers.  The actual
//! transaction-state LOGIC (transam.c / varsup.c: TransactionIdPrecedes,
//! GetStableLatestTransactionId, the nextXid machinery, etc.) is NOT translated yet
//! - only the header's `static inline` functions and `#define`s live here, which are
//! pure value operations.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `TransactionId`/`CommandId`/`MultiXactId`/`SubTransactionId`/`MultiXactOffset` are
//! the base typedefs from c.h (crate::c).

use crate::c::{uint32, uint64, TransactionId};

/* ----------------
 *		Special transaction ID values
 * ---------------- */
pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

/* ----------------
 *		transaction ID manipulation macros
 * ---------------- */
#[inline]
#[no_mangle]
pub fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}
#[inline]
pub fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}
#[inline]
pub fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1 == id2
}

/* advance/retreat a (bare 32-bit) transaction ID, stepping over the special IDs */
#[inline]
pub fn TransactionIdAdvance(dest: &mut TransactionId) {
    *dest = dest.wrapping_add(1);
    while *dest < FirstNormalTransactionId {
        *dest = dest.wrapping_add(1);
    }
}
#[inline]
pub fn TransactionIdRetreat(dest: &mut TransactionId) {
    *dest = dest.wrapping_sub(1);
    while *dest < FirstNormalTransactionId {
        *dest = dest.wrapping_sub(1);
    }
}

/*
 * A 64 bit value that contains an epoch and a TransactionId.  Not all values
 * represent valid normal XIDs.
 */
#[derive(Clone, Copy)]
#[repr(C)]
pub struct FullTransactionId {
    pub value: uint64,
}

#[inline]
pub const fn FullTransactionIdFromEpochAndXid(epoch: uint32, xid: TransactionId) -> FullTransactionId {
    FullTransactionId {
        value: ((epoch as uint64) << 32) | (xid as uint64),
    }
}

#[inline]
pub const fn FullTransactionIdFromU64(value: uint64) -> FullTransactionId {
    FullTransactionId { value }
}

#[inline]
pub fn EpochFromFullTransactionId(x: FullTransactionId) -> uint32 {
    (x.value >> 32) as uint32
}
#[inline]
pub fn XidFromFullTransactionId(x: FullTransactionId) -> uint32 {
    x.value as uint32
}
#[inline]
pub fn U64FromFullTransactionId(x: FullTransactionId) -> uint64 {
    x.value
}
#[inline]
pub fn FullTransactionIdEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value == b.value
}
#[inline]
pub fn FullTransactionIdPrecedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}
#[inline]
pub fn FullTransactionIdPrecedesOrEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value <= b.value
}
#[inline]
pub fn FullTransactionIdFollows(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value > b.value
}
#[inline]
pub fn FullTransactionIdFollowsOrEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value >= b.value
}
#[inline]
pub fn FullTransactionIdIsValid(x: FullTransactionId) -> bool {
    TransactionIdIsValid(XidFromFullTransactionId(x))
}

pub const InvalidFullTransactionId: FullTransactionId =
    FullTransactionIdFromEpochAndXid(0, InvalidTransactionId);
pub const FirstNormalFullTransactionId: FullTransactionId =
    FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId);

/*
 * Advance/retreat a FullTransactionId variable, stepping over the IDs that would
 * appear to be special only when viewed as 32-bit XIDs.
 */
#[inline]
pub fn FullTransactionIdAdvance(dest: &mut FullTransactionId) {
    dest.value += 1;
    /* see transam.h: skip the special XIDs after wrap into a new epoch */
    if FullTransactionIdPrecedes(*dest, FirstNormalFullTransactionId) {
        return;
    }
    while XidFromFullTransactionId(*dest) < FirstNormalTransactionId {
        dest.value += 1;
    }
}
#[inline]
pub fn FullTransactionIdRetreat(dest: &mut FullTransactionId) {
    dest.value -= 1;
    if FullTransactionIdPrecedes(*dest, FirstNormalFullTransactionId) {
        return;
    }
    while XidFromFullTransactionId(*dest) < FirstNormalTransactionId {
        dest.value -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_xid_helpers() {
        let f = FullTransactionIdFromEpochAndXid(2, 100);
        assert_eq!(EpochFromFullTransactionId(f), 2);
        assert_eq!(XidFromFullTransactionId(f), 100);
        assert_eq!(U64FromFullTransactionId(f), (2u64 << 32) | 100);
        assert!(FullTransactionIdPrecedes(FullTransactionIdFromU64(5), FullTransactionIdFromU64(9)));
        assert!(FullTransactionIdFollows(FullTransactionIdFromU64(9), FullTransactionIdFromU64(5)));
        assert!(TransactionIdIsNormal(FirstNormalTransactionId));
        assert!(!TransactionIdIsNormal(FrozenTransactionId));
        assert!(!TransactionIdIsValid(InvalidTransactionId));
        assert_eq!(XidFromFullTransactionId(FirstNormalFullTransactionId), FirstNormalTransactionId);
    }
}

// Submodules under access/transam.
pub mod xloginsert;
pub mod xlog;
pub mod xlogprefetcher;
pub mod twophase;
pub mod multixact;
pub mod clog;
pub mod twophase_rmgr;
pub mod xlog_internal;
pub mod rmgr;
pub mod subtrans;
pub mod transam;
pub mod xlogbackup;
pub mod xlogrecord;
pub mod xlogdefs;
pub mod xlogreader;
pub mod xlogstats;
pub mod timeline;
pub mod generic_xlog;
pub mod varsup;
pub mod xlogarchive;
pub mod xlogutils;
pub mod commit_ts;
pub mod xact;
pub mod xlogrecovery;
pub mod slru;
pub mod parallel;
