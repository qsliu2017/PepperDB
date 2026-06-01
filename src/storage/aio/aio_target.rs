//! storage/aio/aio_target.c - AIO functionality related to executing IO for different targets.

use crate::prelude::*;

use std::ffi::c_char;

use crate::Assert;
use crate::c::uint8;
use crate::storage::aio_internal::PgAioHandle;
use crate::storage::aio_types::PgAioTargetData;

// ---------------------------------------------------------------------------
// Types/constants owned by storage/aio.h, which has not yet been ported.
// Defined locally here so this implementation file is self-consistent.
// TODO: dedup -> import from crate::storage::aio once aio.h is translated.
// ---------------------------------------------------------------------------

/* PgAioOp - aio.h enum (only the bounds are needed here) */
pub type PgAioOp = c_int;
pub const PGAIO_OP_INVALID: PgAioOp = 0;
pub const PGAIO_OP_READV: PgAioOp = 1;
pub const PGAIO_OP_WRITEV: PgAioOp = 2;
pub const PGAIO_OP_COUNT: PgAioOp = PGAIO_OP_WRITEV + 1;

/*
 * On what is IO being performed?
 *
 * PgAioTargetID specific behaviour should be implemented in aio_target.c.
 */
pub type PgAioTargetID = c_int;
pub const PGAIO_TID_INVALID: PgAioTargetID = 0;
pub const PGAIO_TID_SMGR: PgAioTargetID = 1;
pub const PGAIO_TID_COUNT: PgAioTargetID = PGAIO_TID_SMGR + 1;

/*
 * Information the object that IO is executed on. Mostly callbacks that
 * operate on PgAioTargetData.
 *
 * typedef is in aio_types.h (where it is an opaque enum); this is the full
 * definition from aio.h.
 */
#[repr(C)]
pub struct PgAioTargetInfo {
    /*
     * To support executing using worker processes, the file descriptor for an
     * IO may need to be reopened in a different process.
     */
    pub reopen: Option<unsafe fn(ioh: *mut PgAioHandle)>,

    /* describe the target of the IO, used for log messages and views */
    pub describe_identity: Option<unsafe fn(sd: *const PgAioTargetData) -> *mut c_char>,

    /* name of the target, used in log messages / views */
    pub name: *const c_char,
}

// PgAioTargetInfo holds raw pointers / fn pointers; it must therefore live in
// `const`/static items only through these `const` definitions (Send/Sync via
// the wrapper below for the registry array).
struct TargetInfoRef(*const PgAioTargetInfo);
unsafe impl Sync for TargetInfoRef {}

/*
 * aio_smgr_target_info lives in storage/smgr/smgr.c, which has not been ported
 * yet. Provide a local placeholder so the registry below is complete.
 * TODO: dedup -> import the real one from crate::storage::smgr when ported.
 */
unsafe fn smgr_aio_reopen(_ioh: *mut PgAioHandle) {
    unimplemented!()
}
unsafe fn smgr_aio_describe_identity(_sd: *const PgAioTargetData) -> *mut c_char {
    unimplemented!()
}
const aio_smgr_target_info: PgAioTargetInfo = PgAioTargetInfo {
    reopen: Some(smgr_aio_reopen),
    describe_identity: Some(smgr_aio_describe_identity),
    name: b"smgr\0".as_ptr() as *const c_char,
};

/* The "invalid" target only carries a name. */
const pgaio_invalid_target_info: PgAioTargetInfo = PgAioTargetInfo {
    reopen: None,
    describe_identity: None,
    name: b"invalid\0".as_ptr() as *const c_char,
};

/*
 * Registry for entities that can be the target of AIO.
 *
 * C: static const PgAioTargetInfo *pgaio_target_info[] indexed by PgAioTargetID
 * via designated initializers.
 */
static PGAIO_TARGET_INFO: [TargetInfoRef; PGAIO_TID_COUNT as usize] = [
    /* [PGAIO_TID_INVALID] */ TargetInfoRef(&pgaio_invalid_target_info),
    /* [PGAIO_TID_SMGR]    */ TargetInfoRef(&aio_smgr_target_info),
];

#[inline]
unsafe fn target_info(target: uint8) -> *const PgAioTargetInfo {
    PGAIO_TARGET_INFO[target as usize].0
}

/* --------------------------------------------------------------------------------
 * Public target related functions operating on IO Handles
 * --------------------------------------------------------------------------------
 */

pub unsafe fn pgaio_io_has_target(ioh: *mut PgAioHandle) -> bool {
    (*ioh).target != PGAIO_TID_INVALID as uint8
}

/*
 * Return the name for the target associated with the IO. Mostly useful for
 * debugging/logging.
 */
pub unsafe fn pgaio_io_get_target_name(ioh: *mut PgAioHandle) -> *const c_char {
    /* explicitly allow INVALID here, function used by debug messages */
    Assert!(
        (*ioh).target as PgAioTargetID >= PGAIO_TID_INVALID
            && ((*ioh).target as PgAioTargetID) < PGAIO_TID_COUNT
    );

    (*target_info((*ioh).target)).name
}

/*
 * Assign a target to the IO.
 *
 * This has to be called exactly once before pgaio_io_start_*() is called.
 */
pub unsafe fn pgaio_io_set_target(ioh: *mut PgAioHandle, targetid: PgAioTargetID) {
    Assert!((*ioh).state == crate::storage::aio_internal::PGAIO_HS_HANDED_OUT as uint8);
    Assert!((*ioh).target == PGAIO_TID_INVALID as uint8);

    (*ioh).target = targetid as uint8;
}

pub unsafe fn pgaio_io_get_target_data(ioh: *mut PgAioHandle) -> *mut PgAioTargetData {
    &mut (*ioh).target_data
}

/*
 * Return a stringified description of the IO's target.
 *
 * The string is localized and allocated in the current memory context.
 */
pub unsafe fn pgaio_io_get_target_description(ioh: *mut PgAioHandle) -> *mut c_char {
    /* disallow INVALID, there wouldn't be a description */
    Assert!(
        (*ioh).target as PgAioTargetID > PGAIO_TID_INVALID
            && ((*ioh).target as PgAioTargetID) < PGAIO_TID_COUNT
    );

    let describe_identity = (*target_info((*ioh).target)).describe_identity.unwrap();
    describe_identity(&(*ioh).target_data)
}

/* --------------------------------------------------------------------------------
 * Internal target related functions operating on IO Handles
 * --------------------------------------------------------------------------------
 */

/*
 * Internal: Check if pgaio_io_reopen() is available for the IO.
 */
pub unsafe fn pgaio_io_can_reopen(ioh: *mut PgAioHandle) -> bool {
    Assert!(
        (*ioh).target as PgAioTargetID > PGAIO_TID_INVALID
            && ((*ioh).target as PgAioTargetID) < PGAIO_TID_COUNT
    );

    (*target_info((*ioh).target)).reopen.is_some()
}

/*
 * Internal: Before executing an IO outside of the context of the process the
 * IO has been staged in, the file descriptor has to be reopened - any FD
 * referenced in the IO itself, won't be valid in the separate process.
 */
pub unsafe fn pgaio_io_reopen(ioh: *mut PgAioHandle) {
    Assert!(
        (*ioh).target as PgAioTargetID > PGAIO_TID_INVALID
            && ((*ioh).target as PgAioTargetID) < PGAIO_TID_COUNT
    );
    Assert!(
        (*ioh).op as PgAioOp > PGAIO_OP_INVALID && ((*ioh).op as PgAioOp) < PGAIO_OP_COUNT
    );

    let reopen = (*target_info((*ioh).target)).reopen.unwrap();
    reopen(ioh);
}
