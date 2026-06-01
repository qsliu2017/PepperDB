//! Postmaster and shared process-control support
//! (postgres/src/backend/postmaster).
//!
//! So far: the shared interrupt/signal-flag handlers (`interrupt`).

pub mod walsummarizer;
pub mod checkpointer;
pub mod bgworker;
pub mod auxprocess;
pub mod bgwriter;
pub mod bgworker_internals;
pub mod pmchild;
pub mod startup;
pub mod walwriter;
pub mod interrupt;

pub mod fork_process;
pub mod pgarch;
pub mod launch_backend;
pub mod postmaster;
pub mod autovacuum;
