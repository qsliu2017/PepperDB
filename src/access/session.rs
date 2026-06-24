//! Translated from PostgreSQL src/include/access/session.h
//! Encapsulation of user session.

// Single-process model: the session-scoped DSM segment / DSA area and the
// shared dshash tables (shared_record_table, shared_typmod_table,
// shared_typmod_registry) only existed to share record-typmod state across
// parallel-worker processes via shared memory. With one process they collapse
// to ordinary owned heap state, so those fields are dropped here. dsm_handle /
// dsm_segment / dsa_area are not modeled.

/// A struct encapsulating some elements of a user's session.
pub struct Session {
    // shared parallel-query state dropped under single-process model; revisit
    // when typcache record registry is ported.
}

pub fn InitializeSession() {
    unimplemented!()
}

// GetSessionDsmHandle / AttachSession / DetachSession managed cross-process DSM
// attachment; not applicable single-process. Omitted.

// CurrentSession was a process-global (`Session *`, NULL for none). Under the
// async single-process model this becomes task-local / threaded session state
// rather than a global; see translation-rules "Global/session state".
