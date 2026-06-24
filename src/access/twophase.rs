//! Translated from PostgreSQL src/include/access/twophase.h
//! Two-phase-commit related declarations.

use crate::access::xlogdefs::{RepOriginId, XLogRecPtr};
use crate::c::TransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::postgres_ext::Oid;
use crate::storage::lock::{VirtualTransactionId, PGPROC};

/// GlobalTransactionData is opaque (defined in twophase.c); other places only
/// hold the handle. C: `typedef struct GlobalTransactionData *GlobalTransaction`.
pub struct GlobalTransactionData {
    _private: (),
}
pub type GlobalTransaction = *mut GlobalTransactionData; // TODO(ptr)

// GUC variable (process-global in C).
pub static mut max_prepared_xacts: i32 = 0;

pub fn TwoPhaseShmemSize() -> usize {
    unimplemented!()
}
pub fn TwoPhaseShmemInit() {
    unimplemented!()
}

pub fn AtAbort_Twophase() {
    unimplemented!()
}
pub fn PostPrepare_Twophase() {
    unimplemented!()
}

/// C: returns the xid (or InvalidTransactionId) + a `have_more` out-param.
pub fn TwoPhaseGetXidByVirtualXID(_vxid: VirtualTransactionId) -> (TransactionId, bool) {
    unimplemented!()
}
pub fn TwoPhaseGetDummyProc(_xid: TransactionId, _lock_held: bool) -> *mut PGPROC {
    unimplemented!()
}
pub fn TwoPhaseGetDummyProcNumber(_xid: TransactionId, _lock_held: bool) -> i32 {
    unimplemented!()
}

pub fn MarkAsPreparing(
    _xid: TransactionId,
    _gid: &str,
    _prepared_at: TimestampTz,
    _owner: Oid,
    _databaseid: Oid,
) -> GlobalTransaction {
    unimplemented!()
}

pub fn StartPrepare(_gxact: GlobalTransaction) {
    unimplemented!()
}
pub fn EndPrepare(_gxact: GlobalTransaction) {
    unimplemented!()
}
pub fn StandbyTransactionIdIsPrepared(_xid: TransactionId) -> bool {
    unimplemented!()
}

/// C fills `*xids_p`/`*nxids_p` out-params and returns the oldest xid.
pub fn PrescanPreparedTransactions() -> (TransactionId, Vec<TransactionId>) {
    unimplemented!()
}
pub fn StandbyRecoverPreparedTransactions() {
    unimplemented!()
}
pub fn RecoverPreparedTransactions() {
    unimplemented!()
}

pub fn CheckPointTwoPhase(_redo_horizon: XLogRecPtr) {
    unimplemented!()
}

pub fn FinishPreparedTransaction(_gid: &str, _is_commit: bool) {
    unimplemented!()
}

pub fn PrepareRedoAdd(
    _buf: &mut [u8],
    _start_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
    _origin_id: RepOriginId,
) {
    unimplemented!()
}
pub fn PrepareRedoRemove(_xid: TransactionId, _give_warning: bool) {
    unimplemented!()
}
pub fn restoreTwoPhaseData() {
    unimplemented!()
}
pub fn LookupGXact(
    _gid: &str,
    _prepare_end_lsn: XLogRecPtr,
    _origin_prepare_timestamp: TimestampTz,
) -> bool {
    unimplemented!()
}

/// C writes the gid into a caller buffer of size `szgid`.
pub fn TwoPhaseTransactionGid(_subid: Oid, _xid: TransactionId, _gid_res: &mut String, _szgid: i32) {
    unimplemented!()
}
pub fn LookupGXactBySubid(_subid: Oid) -> bool {
    unimplemented!()
}
