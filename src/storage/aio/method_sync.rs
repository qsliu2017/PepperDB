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

pub type PgAioHandle = c_void;

#[repr(C)]
pub struct IoMethodOps {
    pub shmem_size: Option<unsafe extern "C" fn() -> Size>,
    pub shmem_init: Option<unsafe extern "C" fn(bool)>,
    pub needs_synchronous_execution: Option<unsafe extern "C" fn(*mut PgAioHandle) -> bool>,
    pub submit: Option<unsafe extern "C" fn(uint16, *mut *mut PgAioHandle) -> c_int>,
}

impl IoMethodOps {
    pub const DEFAULT: IoMethodOps = IoMethodOps {
        shmem_size: None,
        shmem_init: None,
        needs_synchronous_execution: None,
        submit: None,
    };
}

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
