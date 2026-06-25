//! Tombstone: src/include/storage/ipc.h
//!
//! Shared-memory segment + IPC setup replaced by Arc-shared heap state. No direct translation.
//!
//! The exit-callback machinery (`proc_exit`, `on_shmem_exit`,
//! `before_shmem_exit`, `on_proc_exit`) is replaced by RAII `Drop` for
//! per-resource cleanup plus supervisor-driven shutdown for ordering: a task
//! ends by returning/being cancelled, its owned state Drops, and the supervisor
//! sequences task teardown. No `proc_exit` symbol is reintroduced.
