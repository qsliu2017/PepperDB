//! commands/progress.h - constants for the progress reporting facilities (backend_status.h).

use std::ffi::c_int;

/* Progress parameters for (lazy) vacuum */
pub const PROGRESS_VACUUM_PHASE: c_int = 0;
pub const PROGRESS_VACUUM_TOTAL_HEAP_BLKS: c_int = 1;
pub const PROGRESS_VACUUM_HEAP_BLKS_SCANNED: c_int = 2;
pub const PROGRESS_VACUUM_HEAP_BLKS_VACUUMED: c_int = 3;
pub const PROGRESS_VACUUM_NUM_INDEX_VACUUMS: c_int = 4;
pub const PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES: c_int = 5;
pub const PROGRESS_VACUUM_DEAD_TUPLE_BYTES: c_int = 6;
pub const PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS: c_int = 7;
pub const PROGRESS_VACUUM_INDEXES_TOTAL: c_int = 8;
pub const PROGRESS_VACUUM_INDEXES_PROCESSED: c_int = 9;
pub const PROGRESS_VACUUM_DELAY_TIME: c_int = 10;

/* Phases of vacuum (as advertised via PROGRESS_VACUUM_PHASE) */
pub const PROGRESS_VACUUM_PHASE_SCAN_HEAP: c_int = 1;
pub const PROGRESS_VACUUM_PHASE_VACUUM_INDEX: c_int = 2;
pub const PROGRESS_VACUUM_PHASE_VACUUM_HEAP: c_int = 3;
pub const PROGRESS_VACUUM_PHASE_INDEX_CLEANUP: c_int = 4;
pub const PROGRESS_VACUUM_PHASE_TRUNCATE: c_int = 5;
pub const PROGRESS_VACUUM_PHASE_FINAL_CLEANUP: c_int = 6;

/* Progress parameters for analyze */
pub const PROGRESS_ANALYZE_PHASE: c_int = 0;
pub const PROGRESS_ANALYZE_BLOCKS_TOTAL: c_int = 1;
pub const PROGRESS_ANALYZE_BLOCKS_DONE: c_int = 2;
pub const PROGRESS_ANALYZE_EXT_STATS_TOTAL: c_int = 3;
pub const PROGRESS_ANALYZE_EXT_STATS_COMPUTED: c_int = 4;
pub const PROGRESS_ANALYZE_CHILD_TABLES_TOTAL: c_int = 5;
pub const PROGRESS_ANALYZE_CHILD_TABLES_DONE: c_int = 6;
pub const PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID: c_int = 7;
pub const PROGRESS_ANALYZE_DELAY_TIME: c_int = 8;

/* Phases of analyze (as advertised via PROGRESS_ANALYZE_PHASE) */
pub const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS: c_int = 1;
pub const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH: c_int = 2;
pub const PROGRESS_ANALYZE_PHASE_COMPUTE_STATS: c_int = 3;
pub const PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS: c_int = 4;
pub const PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE: c_int = 5;

/* Progress parameters for cluster */
pub const PROGRESS_CLUSTER_COMMAND: c_int = 0;
pub const PROGRESS_CLUSTER_PHASE: c_int = 1;
pub const PROGRESS_CLUSTER_INDEX_RELID: c_int = 2;
pub const PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED: c_int = 3;
pub const PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN: c_int = 4;
pub const PROGRESS_CLUSTER_TOTAL_HEAP_BLKS: c_int = 5;
pub const PROGRESS_CLUSTER_HEAP_BLKS_SCANNED: c_int = 6;
pub const PROGRESS_CLUSTER_INDEX_REBUILD_COUNT: c_int = 7;

/* Phases of cluster (as advertised via PROGRESS_CLUSTER_PHASE) */
pub const PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP: c_int = 1;
pub const PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP: c_int = 2;
pub const PROGRESS_CLUSTER_PHASE_SORT_TUPLES: c_int = 3;
pub const PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP: c_int = 4;
pub const PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES: c_int = 5;
pub const PROGRESS_CLUSTER_PHASE_REBUILD_INDEX: c_int = 6;
pub const PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP: c_int = 7;

/* Commands of PROGRESS_CLUSTER */
pub const PROGRESS_CLUSTER_COMMAND_CLUSTER: c_int = 1;
pub const PROGRESS_CLUSTER_COMMAND_VACUUM_FULL: c_int = 2;

/* Progress parameters for CREATE INDEX */
/* 3, 4 and 5 reserved for "waitfor" metrics */
pub const PROGRESS_CREATEIDX_COMMAND: c_int = 0;
pub const PROGRESS_CREATEIDX_INDEX_OID: c_int = 6;
pub const PROGRESS_CREATEIDX_ACCESS_METHOD_OID: c_int = 8;
pub const PROGRESS_CREATEIDX_PHASE: c_int = 9; /* AM-agnostic phase # */
pub const PROGRESS_CREATEIDX_SUBPHASE: c_int = 10; /* phase # filled by AM */
pub const PROGRESS_CREATEIDX_TUPLES_TOTAL: c_int = 11;
pub const PROGRESS_CREATEIDX_TUPLES_DONE: c_int = 12;
pub const PROGRESS_CREATEIDX_PARTITIONS_TOTAL: c_int = 13;
pub const PROGRESS_CREATEIDX_PARTITIONS_DONE: c_int = 14;
/* 15 and 16 reserved for "block number" metrics */

/* Phases of CREATE INDEX (as advertised via PROGRESS_CREATEIDX_PHASE) */
pub const PROGRESS_CREATEIDX_PHASE_WAIT_1: c_int = 1;
pub const PROGRESS_CREATEIDX_PHASE_BUILD: c_int = 2;
pub const PROGRESS_CREATEIDX_PHASE_WAIT_2: c_int = 3;
pub const PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN: c_int = 4;
pub const PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT: c_int = 5;
pub const PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN: c_int = 6;
pub const PROGRESS_CREATEIDX_PHASE_WAIT_3: c_int = 7;
pub const PROGRESS_CREATEIDX_PHASE_WAIT_4: c_int = 8;
pub const PROGRESS_CREATEIDX_PHASE_WAIT_5: c_int = 9;

/*
 * Subphases of CREATE INDEX, for index_build.
 */
pub const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: c_int = 1;
/* Additional phases are defined by each AM */

/* Commands of PROGRESS_CREATEIDX */
pub const PROGRESS_CREATEIDX_COMMAND_CREATE: c_int = 1;
pub const PROGRESS_CREATEIDX_COMMAND_CREATE_CONCURRENTLY: c_int = 2;
pub const PROGRESS_CREATEIDX_COMMAND_REINDEX: c_int = 3;
pub const PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY: c_int = 4;

/* Lock holder wait counts */
pub const PROGRESS_WAITFOR_TOTAL: c_int = 3;
pub const PROGRESS_WAITFOR_DONE: c_int = 4;
pub const PROGRESS_WAITFOR_CURRENT_PID: c_int = 5;

/* Block numbers in a generic relation scan */
pub const PROGRESS_SCAN_BLOCKS_TOTAL: c_int = 15;
pub const PROGRESS_SCAN_BLOCKS_DONE: c_int = 16;

/* Progress parameters for pg_basebackup */
pub const PROGRESS_BASEBACKUP_PHASE: c_int = 0;
pub const PROGRESS_BASEBACKUP_BACKUP_TOTAL: c_int = 1;
pub const PROGRESS_BASEBACKUP_BACKUP_STREAMED: c_int = 2;
pub const PROGRESS_BASEBACKUP_TBLSPC_TOTAL: c_int = 3;
pub const PROGRESS_BASEBACKUP_TBLSPC_STREAMED: c_int = 4;

/* Phases of pg_basebackup (as advertised via PROGRESS_BASEBACKUP_PHASE) */
pub const PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT: c_int = 1;
pub const PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE: c_int = 2;
pub const PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP: c_int = 3;
pub const PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE: c_int = 4;
pub const PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL: c_int = 5;

/* Progress parameters for PROGRESS_COPY */
pub const PROGRESS_COPY_BYTES_PROCESSED: c_int = 0;
pub const PROGRESS_COPY_BYTES_TOTAL: c_int = 1;
pub const PROGRESS_COPY_TUPLES_PROCESSED: c_int = 2;
pub const PROGRESS_COPY_TUPLES_EXCLUDED: c_int = 3;
pub const PROGRESS_COPY_COMMAND: c_int = 4;
pub const PROGRESS_COPY_TYPE: c_int = 5;
pub const PROGRESS_COPY_TUPLES_SKIPPED: c_int = 6;

/* Commands of COPY (as advertised via PROGRESS_COPY_COMMAND) */
pub const PROGRESS_COPY_COMMAND_FROM: c_int = 1;
pub const PROGRESS_COPY_COMMAND_TO: c_int = 2;

/* Types of COPY commands (as advertised via PROGRESS_COPY_TYPE) */
pub const PROGRESS_COPY_TYPE_FILE: c_int = 1;
pub const PROGRESS_COPY_TYPE_PROGRAM: c_int = 2;
pub const PROGRESS_COPY_TYPE_PIPE: c_int = 3;
pub const PROGRESS_COPY_TYPE_CALLBACK: c_int = 4;
