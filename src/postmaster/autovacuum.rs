//! Translated from PostgreSQL src/include/postmaster/autovacuum.h
//! Integrated autovacuum daemon.
//!
//! The implementation lives in `crate::backend::postmaster::autovacuum` (the .c
//! file maps there per the C-file-mapping invariant). This header rewires the
//! public C symbols to the snake-case backend functions.

use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;

/// Work that other processes can request from autovacuum. (C enum.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoVacuumWorkItemType {
    AVW_BRINSummarizeRange,
}

// Public entry points (PG C symbol -> backend snake fn).
pub use crate::backend::postmaster::autovacuum::auto_vac_launcher_main as AutoVacLauncherMain;
pub use crate::backend::postmaster::autovacuum::auto_vac_worker_failed as AutoVacWorkerFailed;
pub use crate::backend::postmaster::autovacuum::auto_vac_worker_main as AutoVacWorkerMain;
pub use crate::backend::postmaster::autovacuum::auto_vacuuming_active as AutoVacuumingActive;
pub use crate::backend::postmaster::autovacuum::autovac_init;

/// PG `AutoVacuumRequestWork(type, relationId, blkno)`. The backend fn takes the
/// caller's database OID explicitly (no thread-global `MyDatabaseId` yet); pass
/// `InvalidOid` until the per-session database id lands.
pub fn AutoVacuumRequestWork(
    type_: AutoVacuumWorkItemType,
    relation_id: Oid,
    blkno: BlockNumber,
) -> bool {
    crate::backend::postmaster::autovacuum::auto_vacuum_request_work(
        type_,
        crate::postgres_ext::InvalidOid,
        relation_id,
        blkno,
    )
}
