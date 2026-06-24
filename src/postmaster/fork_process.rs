//! Translated from PostgreSQL src/include/postmaster/fork_process.h

/// C: `pid_t fork_process(void)`. Returns the child pid in the parent, 0 in the
/// child. Under the single-process async model this has no real analogue.
pub fn fork_process() -> i32 {
    unimplemented!()
}
