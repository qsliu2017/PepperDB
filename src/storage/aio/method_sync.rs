//! src/backend/storage/aio/method_sync.c
//!
//! AIO - perform "AIO" by executing it synchronously
//!
//! This method is mainly to check if AIO use causes regressions. Other IO
//! methods might also fall back to the synchronous method for functionality
//! they cannot provide.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/aio/method_sync.c

use crate::prelude::*;

use crate::c::{uint16, Size};

// Canonical IoMethodOps/PgAioHandle (aio_internal). The local stubs here had a
// DIFFERENT IoMethodOps layout, so aio.rs reading via the canonical layout landed
// on the wrong fields (init_backend read the `submit` slot = pgaio_sync_submit).
pub use crate::storage::aio_internal::{IoMethodOps, PgAioHandle};

pub const pgaio_sync_ops: IoMethodOps = IoMethodOps {
    needs_synchronous_execution: Some(pgaio_sync_needs_synchronous_execution),
    submit: Some(pgaio_sync_submit),
    ..IoMethodOps::DEFAULT
};

unsafe extern "C" fn pgaio_sync_needs_synchronous_execution(ioh: *mut PgAioHandle) -> bool {
    true
}

unsafe extern "C" fn pgaio_sync_submit(
    num_staged_ios: uint16,
    staged_ios: *mut *mut PgAioHandle,
) -> c_int {
    elog!(ERROR, "IO should have been executed synchronously");

    0
}
