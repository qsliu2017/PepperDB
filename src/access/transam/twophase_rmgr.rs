//! access/transam/twophase_rmgr.c - Two-phase-commit resource managers tables

use crate::prelude::*;
use crate::c::{uint16, uint32, TransactionId};

// typedef void (*TwoPhaseCallback) (TransactionId xid, uint16 info,
//                                   void *recdata, uint32 len);
pub type TwoPhaseCallback =
    unsafe fn(xid: TransactionId, info: uint16, recdata: *mut std::ffi::c_void, len: uint32);

// Built-in resource managers (from access/twophase_rmgr.h)
pub const TWOPHASE_RM_END_ID: u8 = 0;
pub const TWOPHASE_RM_LOCK_ID: u8 = 1;
pub const TWOPHASE_RM_PGSTAT_ID: u8 = 2;
pub const TWOPHASE_RM_MULTIXACT_ID: u8 = 3;
pub const TWOPHASE_RM_PREDICATELOCK_ID: u8 = 4;
pub const TWOPHASE_RM_MAX_ID: u8 = TWOPHASE_RM_PREDICATELOCK_ID;

// ---------------------------------------------------------------------------
// Callback implementations are not yet ported; stub them locally so the
// callback tables below can reference them.  TODO: port from their owning .c
// files (lock.c, multixact.c, predicate.c, pgstat_xact.c).
// ---------------------------------------------------------------------------

// TODO: storage/lmgr/lock.c
unsafe fn lock_twophase_recover(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: storage/lmgr/lock.c
unsafe fn lock_twophase_postcommit(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: storage/lmgr/lock.c
unsafe fn lock_twophase_postabort(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: storage/lmgr/lock.c
unsafe fn lock_twophase_standby_recover(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: access/transam/multixact.c
unsafe fn multixact_twophase_recover(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: access/transam/multixact.c
unsafe fn multixact_twophase_postcommit(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: access/transam/multixact.c
unsafe fn multixact_twophase_postabort(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: storage/lmgr/predicate.c
unsafe fn predicatelock_twophase_recover(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: utils/activity/pgstat_xact.c
unsafe fn pgstat_twophase_postcommit(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// TODO: utils/activity/pgstat_xact.c
unsafe fn pgstat_twophase_postabort(
    _xid: TransactionId,
    _info: uint16,
    _recdata: *mut std::ffi::c_void,
    _len: uint32,
) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Callback tables.  In C these are arrays indexed by TwoPhaseRmgrId with NULL
// entries for managers that do not implement a particular phase.  We model the
// NULLs as Option<TwoPhaseCallback>.
// ---------------------------------------------------------------------------

pub const twophase_recover_callbacks: [Option<TwoPhaseCallback>; (TWOPHASE_RM_MAX_ID + 1) as usize] = [
    None,                                  // END ID
    Some(lock_twophase_recover),           // Lock
    None,                                  // pgstat
    Some(multixact_twophase_recover),      // MultiXact
    Some(predicatelock_twophase_recover),  // PredicateLock
];

pub const twophase_postcommit_callbacks: [Option<TwoPhaseCallback>; (TWOPHASE_RM_MAX_ID + 1) as usize] = [
    None,                                  // END ID
    Some(lock_twophase_postcommit),        // Lock
    Some(pgstat_twophase_postcommit),      // pgstat
    Some(multixact_twophase_postcommit),   // MultiXact
    None,                                  // PredicateLock
];

pub const twophase_postabort_callbacks: [Option<TwoPhaseCallback>; (TWOPHASE_RM_MAX_ID + 1) as usize] = [
    None,                                  // END ID
    Some(lock_twophase_postabort),         // Lock
    Some(pgstat_twophase_postabort),       // pgstat
    Some(multixact_twophase_postabort),    // MultiXact
    None,                                  // PredicateLock
];

pub const twophase_standby_recover_callbacks: [Option<TwoPhaseCallback>; (TWOPHASE_RM_MAX_ID + 1) as usize] = [
    None,                                  // END ID
    Some(lock_twophase_standby_recover),   // Lock
    None,                                  // pgstat
    None,                                  // MultiXact
    None,                                  // PredicateLock
];
