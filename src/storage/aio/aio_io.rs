//! storage/aio/aio_io.c - AIO - Low Level IO Handling.
//!
//! Functions related to associating IO operations to IO Handles and IO-method
//! independent support functions for actually performing IO.

use crate::prelude::*;

use std::ffi::c_char;

use crate::miscadmin::{END_CRIT_SECTION, INTERRUPTS_CAN_BE_PROCESSED, START_CRIT_SECTION};
use crate::port::pg_iovec::{iovec, pg_preadv, pg_pwritev, PG_IOV_MAX};
use crate::storage::aio_internal::{
    pgaio_ctl, pgaio_io_process_completion, pgaio_io_stage, pgaio_my_backend, PgAioHandle, PgAioOp,
    PGAIO_HS_DEFINED, PGAIO_HS_HANDED_OUT,
};

// ---------------------------------------------------------------------------
// PgAioOp enum values (storage/aio.h). The PgAioOp typedef is an `int` alias in
// crate::storage::aio_internal; the discriminants are reprojected here.
// ---------------------------------------------------------------------------
pub const PGAIO_OP_INVALID: PgAioOp = 0;
pub const PGAIO_OP_READV: PgAioOp = 1;
pub const PGAIO_OP_WRITEV: PgAioOp = 2;
pub const PGAIO_OP_COUNT: PgAioOp = PGAIO_OP_WRITEV + 1;

// ---------------------------------------------------------------------------
// PgAioOpData union (storage/aio.h). The definition imported transitively from
// aio_internal is an opaque stub; the real layout is reprojected here so the
// op-specific fields can be accessed. The two arms share an identical layout.
// TODO: dedup once PgAioOpData has a real definition shared across modules.
// ---------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgAioOpDataRw {
    pub fd: c_int,
    pub iov_length: uint16,
    pub offset: uint64,
}

#[repr(C)]
pub union PgAioOpDataReal {
    pub read: PgAioOpDataRw,
    pub write: PgAioOpDataRw,
}

/// Reinterpret the (opaque-stub) `op_data` field of a handle as the real union.
#[inline]
unsafe fn op_data(ioh: *mut PgAioHandle) -> *mut PgAioOpDataReal {
    &mut (*ioh).op_data as *mut _ as *mut PgAioOpDataReal
}

// ---------------------------------------------------------------------------
// Wait events (utils/wait_event.h). Not yet ported; reprojected locally.
// TODO: dedup once the wait_event enum is generated/ported.
// ---------------------------------------------------------------------------
const WAIT_EVENT_DATA_FILE_READ: u32 = 0;
const WAIT_EVENT_DATA_FILE_WRITE: u32 = 0;

// --- locally-stubbed, not-yet-ported callees -------------------------------

// utils/wait_event.h - pgstat wait-event reporting.
// TODO: import from the pgstat wait-event module once ported.
#[inline]
unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {
    // no-op stub
}

#[inline]
unsafe fn pgstat_report_wait_end() {
    // no-op stub
}

// storage/aio.h - whether the handle has had a target associated with it.
// TODO: import from the ported aio target module once available.
unsafe fn pgaio_io_has_target(_ioh: *mut PgAioHandle) -> bool {
    unimplemented!()
}

// Per-thread C errno location (errno.h). macOS/BSD expose it as __error();
// glibc uses __errno_location(). Both return `*mut c_int`.
extern "C" {
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios", target_vendor = "apple"),
        link_name = "__error"
    )]
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")),
        link_name = "__errno_location"
    )]
    fn pg_errno_location() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *pg_errno_location()
}

/* --------------------------------------------------------------------------------
 * Public IO related functions operating on IO Handles
 * --------------------------------------------------------------------------------
 */

/*
 * Scatter/gather IO needs to associate an iovec with the Handle. To support
 * worker mode this data needs to be in shared memory.
 */
pub unsafe fn pgaio_io_get_iovec(ioh: *mut PgAioHandle, iov: *mut *mut iovec) -> c_int {
    Assert!((*ioh).state == PGAIO_HS_HANDED_OUT as uint8);

    *iov = (*pgaio_ctl).iovecs.add((*ioh).iovec_off as usize) as *mut iovec;

    PG_IOV_MAX
}

pub unsafe fn pgaio_io_get_op(ioh: *mut PgAioHandle) -> PgAioOp {
    (*ioh).op as PgAioOp
}

pub unsafe fn pgaio_io_get_op_data(ioh: *mut PgAioHandle) -> *mut PgAioOpDataReal {
    op_data(ioh)
}

/* --------------------------------------------------------------------------------
 * "Start" routines for individual IO operations
 *
 * These are called by the code actually initiating an IO, to associate the IO
 * specific data with an AIO handle.
 *
 * Each of the "start" routines first needs to call pgaio_io_before_start(),
 * then fill IO specific fields in the handle and then finally call
 * pgaio_io_stage().
 * --------------------------------------------------------------------------------
 */

pub unsafe fn pgaio_io_start_readv(ioh: *mut PgAioHandle, fd: c_int, iovcnt: c_int, offset: uint64) {
    pgaio_io_before_start(ioh);

    let od = op_data(ioh);
    (*od).read.fd = fd;
    (*od).read.offset = offset;
    (*od).read.iov_length = iovcnt as uint16;

    pgaio_io_stage(ioh, PGAIO_OP_READV);
}

pub unsafe fn pgaio_io_start_writev(
    ioh: *mut PgAioHandle,
    fd: c_int,
    iovcnt: c_int,
    offset: uint64,
) {
    pgaio_io_before_start(ioh);

    let od = op_data(ioh);
    (*od).write.fd = fd;
    (*od).write.offset = offset;
    (*od).write.iov_length = iovcnt as uint16;

    pgaio_io_stage(ioh, PGAIO_OP_WRITEV);
}

/* --------------------------------------------------------------------------------
 * Internal IO related functions operating on IO Handles
 * --------------------------------------------------------------------------------
 */

/*
 * Execute IO operation synchronously. This is implemented here, not in
 * method_sync.c, because other IO methods also might use it / fall back to
 * it.
 */
pub unsafe fn pgaio_io_perform_synchronously(ioh: *mut PgAioHandle) {
    let mut result: isize = 0;
    let iov = (*pgaio_ctl).iovecs.add((*ioh).iovec_off as usize) as *mut iovec;
    let od = op_data(ioh);

    START_CRIT_SECTION();

    /* Perform IO. */
    match (*ioh).op as PgAioOp {
        PGAIO_OP_READV => {
            pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
            result = pg_preadv(
                (*od).read.fd,
                iov,
                (*od).read.iov_length as c_int,
                (*od).read.offset as i64,
            );
            pgstat_report_wait_end();
        }
        PGAIO_OP_WRITEV => {
            pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
            result = pg_pwritev(
                (*od).write.fd,
                iov,
                (*od).write.iov_length as c_int,
                (*od).write.offset as i64,
            );
            pgstat_report_wait_end();
        }
        PGAIO_OP_INVALID => {
            elog!(ERROR, "trying to execute invalid IO operation");
        }
        _ => {}
    }

    (*ioh).result = if result < 0 {
        -errno() as int32
    } else {
        result as int32
    };

    pgaio_io_process_completion(ioh, (*ioh).result);

    END_CRIT_SECTION();
}

/*
 * Helper function to be called by IO operation preparation functions, before
 * any data in the handle is set.  Mostly to centralize assertions.
 */
unsafe fn pgaio_io_before_start(ioh: *mut PgAioHandle) {
    Assert!((*ioh).state == PGAIO_HS_HANDED_OUT as uint8);
    Assert!((*pgaio_my_backend).handed_out_io == ioh);
    Assert!(pgaio_io_has_target(ioh));
    Assert!((*ioh).op as PgAioOp == PGAIO_OP_INVALID);

    /*
     * Otherwise the FDs referenced by the IO could be closed due to interrupt
     * processing.
     */
    Assert!(!INTERRUPTS_CAN_BE_PROCESSED());
}

/*
 * Could be made part of the public interface, but it's not clear there's
 * really a use case for that.
 */
pub unsafe fn pgaio_io_get_op_name(ioh: *mut PgAioHandle) -> *const c_char {
    Assert!((*ioh).op as PgAioOp >= 0 && ((*ioh).op as PgAioOp) < PGAIO_OP_COUNT);

    match (*ioh).op as PgAioOp {
        PGAIO_OP_INVALID => c"invalid".as_ptr(),
        PGAIO_OP_READV => c"readv".as_ptr(),
        PGAIO_OP_WRITEV => c"writev".as_ptr(),
        _ => null(), /* silence compiler */
    }
}

/*
 * Used to determine if an IO needs to be waited upon before the file
 * descriptor can be closed.
 */
pub unsafe fn pgaio_io_uses_fd(ioh: *mut PgAioHandle, fd: c_int) -> bool {
    Assert!((*ioh).state >= PGAIO_HS_DEFINED as uint8);

    let od = op_data(ioh);
    match (*ioh).op as PgAioOp {
        PGAIO_OP_READV => (*od).read.fd == fd,
        PGAIO_OP_WRITEV => (*od).write.fd == fd,
        PGAIO_OP_INVALID => false,
        _ => false, /* silence compiler */
    }
}

/*
 * Return the iovec and its length. Currently only expected to be used by
 * debugging infrastructure
 */
pub unsafe fn pgaio_io_get_iovec_length(ioh: *mut PgAioHandle, iov: *mut *mut iovec) -> c_int {
    Assert!((*ioh).state >= PGAIO_HS_DEFINED as uint8);

    *iov = (*pgaio_ctl).iovecs.add((*ioh).iovec_off as usize) as *mut iovec;

    let od = op_data(ioh);
    match (*ioh).op as PgAioOp {
        PGAIO_OP_READV => (*od).read.iov_length as c_int,
        PGAIO_OP_WRITEV => (*od).write.iov_length as c_int,
        _ => {
            unreachable!();
        }
    }
}
