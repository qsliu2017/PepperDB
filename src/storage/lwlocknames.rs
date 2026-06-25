//! Built-in LWLock names (the C `storage/lwlocknames.h`, generated from the
//! `PG_LWLOCK(id, name)` list in `storage/lwlocklist.h`).
//!
//! PG's generator emits `#define <Name>Lock (&MainLWLockArray[id].lock)` plus the
//! `IndividualLWLockNames[]` array. In this port LWLocks are std/parking_lot locks
//! (there is no `MainLWLockArray`), so the meaningful generated artifacts are the
//! built-in lock IDs and their names (still used in wait-event reporting). The id
//! space has gaps where obsolete locks were removed; the `name` lookup is a match.

macro_rules! lwlocknames {
    ($(($id:literal, $name:literal)),* $(,)?) => {
        /// `(id, name)` for every built-in individual LWLock, in id order.
        pub const BUILTIN_LWLOCK_NAMES: &[(u32, &str)] = &[ $( ($id, $name) ),* ];

        /// Name of a built-in individual LWLock by id (`None` for unassigned ids).
        pub fn lwlock_builtin_name(id: u32) -> Option<&'static str> {
            match id {
                $( $id => Some($name), )*
                _ => None,
            }
        }
    };
}

lwlocknames! {
    (1, "ShmemIndex"),
    (2, "OidGen"),
    (3, "XidGen"),
    (4, "ProcArray"),
    (5, "SInvalRead"),
    (6, "SInvalWrite"),
    (7, "WALBufMapping"),
    (8, "WALWrite"),
    (9, "ControlFile"),
    (13, "MultiXactGen"),
    (16, "RelCacheInit"),
    (17, "CheckpointerComm"),
    (18, "TwoPhaseState"),
    (19, "TablespaceCreate"),
    (20, "BtreeVacuum"),
    (21, "AddinShmemInit"),
    (22, "Autovacuum"),
    (23, "AutovacuumSchedule"),
    (24, "SyncScan"),
    (25, "RelationMapping"),
    (27, "NotifyQueue"),
    (28, "SerializableXactHash"),
    (29, "SerializableFinishedList"),
    (30, "SerializablePredicateList"),
    (32, "SyncRep"),
    (33, "BackgroundWorker"),
    (34, "DynamicSharedMemoryControl"),
    (35, "AutoFile"),
    (36, "ReplicationSlotAllocation"),
    (37, "ReplicationSlotControl"),
    (39, "CommitTs"),
    (40, "ReplicationOrigin"),
    (41, "MultiXactTruncation"),
    (43, "LogicalRepWorker"),
    (44, "XactTruncation"),
    (46, "WrapLimitsVacuum"),
    (47, "NotifyQueueTail"),
    (48, "WaitEventCustom"),
    (49, "WALSummarizer"),
    (50, "DSMRegistry"),
    (51, "InjectionPoint"),
    (52, "SerialControl"),
    (53, "AioWorkerSubmissionQueue"),
}

/// One past the highest built-in individual LWLock id (C `NUM_INDIVIDUAL_LWLOCKS`).
pub const NUM_INDIVIDUAL_LWLOCKS: u32 = 54;
