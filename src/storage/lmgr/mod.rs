//! Lock manager (postgres/src/backend/storage/lmgr).
//!
//! So far: spinlock contention backoff (`s_lock`).

pub mod lock;
pub mod lwlock;
pub mod proc;
pub mod lmgr;
pub mod condition_variable;
pub mod s_lock;
pub mod deadlock;
pub mod predicate;
