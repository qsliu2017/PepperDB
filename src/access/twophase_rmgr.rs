//! Translated from PostgreSQL src/include/access/twophase_rmgr.h

// TODO(struct-forward): TransactionId lives in c.h; repoint to crate::c in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::c::TransactionId in Phase 2")]
pub type TransactionId = u32;

/// Two-phase callback. The C `void *recdata`/`len` pair becomes a byte slice.
#[allow(deprecated)]
pub type TwoPhaseCallback = fn(xid: TransactionId, info: u16, recdata: &[u8]);

/// Built-in two-phase-commit resource managers.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoPhaseRmgrId {
    End = 0,
    Lock = 1,
    Pgstat = 2,
    Multixact = 3,
    Predicatelock = 4,
}

impl TwoPhaseRmgrId {
    /// Highest assigned two-phase rmgr ID.
    pub const MAX_ID: TwoPhaseRmgrId = TwoPhaseRmgrId::Predicatelock;
}

#[allow(deprecated)]
pub static TWOPHASE_RECOVER_CALLBACKS: &[TwoPhaseCallback] = &[];
#[allow(deprecated)]
pub static TWOPHASE_POSTCOMMIT_CALLBACKS: &[TwoPhaseCallback] = &[];
#[allow(deprecated)]
pub static TWOPHASE_POSTABORT_CALLBACKS: &[TwoPhaseCallback] = &[];
#[allow(deprecated)]
pub static TWOPHASE_STANDBY_RECOVER_CALLBACKS: &[TwoPhaseCallback] = &[];

pub fn register_two_phase_record(_rmid: TwoPhaseRmgrId, _info: u16, _data: &[u8]) {
    unimplemented!()
}
