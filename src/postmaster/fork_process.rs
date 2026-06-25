//! TOMBSTONE -- translated from PostgreSQL src/include/postmaster/fork_process.h
//!
//! `fork_process()` has no analogue under the single-process async model. The
//! postmaster no longer forks a child per backend; the supervisor task spawns a
//! tokio task instead (see `crate::backend::postmaster::postmaster::admit_and_spawn`,
//! which calls `tokio::spawn`). `fork_process.c` is deleted; nothing should call
//! this. Kept as a tombstone so the header path stays documented.

/// C: `pid_t fork_process(void)`. TOMBSTONED: use `tokio::spawn` via the
/// supervisor's backend dispatch instead.
#[deprecated(note = "single-process: no fork; the supervisor uses tokio::spawn")]
pub fn fork_process() -> i32 {
    unreachable!("fork_process is tombstoned under the single-process async model")
}
