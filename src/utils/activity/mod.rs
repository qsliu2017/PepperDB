//! Cumulative statistics subsystem (postgres/src/backend/utils/activity).
//!
//! So far: the foundational fixed-stats core (`pgstat`). The per-kind
//! reporters (archiver/bgwriter/checkpointer/wal/...) build on it.

pub mod pgstat_shmem;
pub mod backend_status;
pub mod backend_progress;
pub mod pgstat_backend;
pub mod pgstat_xact;
pub mod wait_event_funcs;
pub mod pgstat_internal;
pub mod pgstat;
pub mod pgstat_archiver;
pub mod pgstat_bgwriter;
pub mod pgstat_checkpointer;
pub mod pgstat_database;
pub mod pgstat_function;
pub mod pgstat_io;
pub mod pgstat_relation;
pub mod pgstat_replslot;
pub mod pgstat_slru;
pub mod pgstat_subscription;
pub mod pgstat_wal;
pub mod wait_event;
