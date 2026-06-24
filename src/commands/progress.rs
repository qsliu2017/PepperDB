//! Translated from PostgreSQL src/include/commands/progress.h
//
// Column indices into the per-command progress array. Each per-command group is
// a separate `#[repr(i32)]` enum; some groups share index 0 across commands,
// which is fine. Use `as i32`/`as usize` at the call site for indexing.

/// Progress parameters for (lazy) vacuum.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressVacuum {
    Phase = 0,
    TotalHeapBlks = 1,
    HeapBlksScanned = 2,
    HeapBlksVacuumed = 3,
    NumIndexVacuums = 4,
    MaxDeadTupleBytes = 5,
    DeadTupleBytes = 6,
    NumDeadItemIds = 7,
    IndexesTotal = 8,
    IndexesProcessed = 9,
    DelayTime = 10,
}

/// Phases of vacuum (as advertised via ProgressVacuum::Phase).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressVacuumPhase {
    ScanHeap = 1,
    VacuumIndex = 2,
    VacuumHeap = 3,
    IndexCleanup = 4,
    Truncate = 5,
    FinalCleanup = 6,
}

/// Progress parameters for analyze.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressAnalyze {
    Phase = 0,
    BlocksTotal = 1,
    BlocksDone = 2,
    ExtStatsTotal = 3,
    ExtStatsComputed = 4,
    ChildTablesTotal = 5,
    ChildTablesDone = 6,
    CurrentChildTableRelid = 7,
    DelayTime = 8,
}

/// Phases of analyze (as advertised via ProgressAnalyze::Phase).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressAnalyzePhase {
    AcquireSampleRows = 1,
    AcquireSampleRowsInh = 2,
    ComputeStats = 3,
    ComputeExtStats = 4,
    FinalizeAnalyze = 5,
}

/// Progress parameters for cluster.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCluster {
    Command = 0,
    Phase = 1,
    IndexRelid = 2,
    HeapTuplesScanned = 3,
    HeapTuplesWritten = 4,
    TotalHeapBlks = 5,
    HeapBlksScanned = 6,
    IndexRebuildCount = 7,
}

/// Phases of cluster (as advertised via ProgressCluster::Phase).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressClusterPhase {
    SeqScanHeap = 1,
    IndexScanHeap = 2,
    SortTuples = 3,
    WriteNewHeap = 4,
    SwapRelFiles = 5,
    RebuildIndex = 6,
    FinalCleanup = 7,
}

/// Commands of PROGRESS_CLUSTER.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressClusterCommand {
    Cluster = 1,
    VacuumFull = 2,
}

/// Progress parameters for CREATE INDEX.
// Indices 3, 4, 5 reserved for "waitfor" metrics; 15, 16 for "block number".
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCreateidx {
    Command = 0,
    IndexOid = 6,
    AccessMethodOid = 8,
    Phase = 9,
    Subphase = 10,
    TuplesTotal = 11,
    TuplesDone = 12,
    PartitionsTotal = 13,
    PartitionsDone = 14,
}

/// Phases of CREATE INDEX (as advertised via ProgressCreateidx::Phase).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCreateidxPhase {
    Wait1 = 1,
    Build = 2,
    Wait2 = 3,
    ValidateIdxscan = 4,
    ValidateSort = 5,
    ValidateTablescan = 6,
    Wait3 = 7,
    Wait4 = 8,
    Wait5 = 9,
}

/// Subphases of CREATE INDEX, for index_build.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCreateidxSubphase {
    Initialize = 1,
}

/// Commands of PROGRESS_CREATEIDX.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCreateidxCommand {
    Create = 1,
    CreateConcurrently = 2,
    Reindex = 3,
    ReindexConcurrently = 4,
}

/// Lock holder wait counts.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressWaitfor {
    Total = 3,
    Done = 4,
    CurrentPid = 5,
}

/// Block numbers in a generic relation scan.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressScan {
    BlocksTotal = 15,
    BlocksDone = 16,
}

/// Progress parameters for pg_basebackup.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressBasebackup {
    Phase = 0,
    BackupTotal = 1,
    BackupStreamed = 2,
    TblspcTotal = 3,
    TblspcStreamed = 4,
}

/// Phases of pg_basebackup (as advertised via ProgressBasebackup::Phase).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressBasebackupPhase {
    WaitCheckpoint = 1,
    EstimateBackupSize = 2,
    StreamBackup = 3,
    WaitWalArchive = 4,
    TransferWal = 5,
}

/// Progress parameters for PROGRESS_COPY.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCopy {
    BytesProcessed = 0,
    BytesTotal = 1,
    TuplesProcessed = 2,
    TuplesExcluded = 3,
    Command = 4,
    Type = 5,
    TuplesSkipped = 6,
}

/// Commands of COPY (as advertised via ProgressCopy::Command).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCopyCommand {
    From = 1,
    To = 2,
}

/// Types of COPY commands (as advertised via ProgressCopy::Type).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressCopyType {
    File = 1,
    Program = 2,
    Pipe = 3,
    Callback = 4,
}
