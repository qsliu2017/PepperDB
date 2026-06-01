//! storage/aio/aio_callback.c - AIO - callbacks that can be registered on IO Handles.

use crate::prelude::*;

use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::storage::aio_internal::{
    pgaio_ctl, pgaio_result_status_string, PgAioHandle, PGAIO_HANDLE_MAX_CALLBACKS,
};
use crate::storage::aio::aio_target::{PGAIO_OP_INVALID, PGAIO_TID_INVALID};
use crate::storage::aio::aio_target::{PGAIO_OP_COUNT, PGAIO_TID_COUNT};
use crate::storage::aio_types::{
    PgAioResult, PgAioTargetData, PGAIO_RS_OK, PGAIO_RS_UNKNOWN,
};

// ---------------------------------------------------------------------------
// PgAioHandleCallbackID (storage/aio.h). The typedef is a plain C enum; the
// project convention reprojects it as a `pub type` alias plus `pub const`
// variants. `id` is stored in PgAioResult as PGAIO_RESULT_ID_BITS bits.
// ---------------------------------------------------------------------------
pub type PgAioHandleCallbackID = c_int;
pub const PGAIO_HCB_INVALID: PgAioHandleCallbackID = 0;
pub const PGAIO_HCB_MD_READV: PgAioHandleCallbackID = 1;
pub const PGAIO_HCB_SHARED_BUFFER_READV: PgAioHandleCallbackID = 2;
pub const PGAIO_HCB_LOCAL_BUFFER_READV: PgAioHandleCallbackID = 3;
/* #define PGAIO_HCB_MAX PGAIO_HCB_LOCAL_BUFFER_READV */
pub const PGAIO_HCB_MAX: PgAioHandleCallbackID = PGAIO_HCB_LOCAL_BUFFER_READV;

// ---------------------------------------------------------------------------
// Callback function pointer typedefs (storage/aio.h).
// ---------------------------------------------------------------------------
pub type PgAioHandleCallbackStage = unsafe fn(ioh: *mut PgAioHandle, cb_flags: uint8);
pub type PgAioHandleCallbackComplete =
    unsafe fn(ioh: *mut PgAioHandle, prior_result: PgAioResult, cb_flags: uint8) -> PgAioResult;
pub type PgAioHandleCallbackReport =
    unsafe fn(result: PgAioResult, target_data: *const PgAioTargetData, elevel: c_int);

// ---------------------------------------------------------------------------
// struct PgAioHandleCallbacks (storage/aio.h). aio_types.rs declares this as
// an opaque forward enum; this is the full definition reprojected here so the
// dispatch table can be built. TODO: dedup once aio.h is fully ported.
// ---------------------------------------------------------------------------
pub struct PgAioHandleCallbacks {
    pub stage: Option<PgAioHandleCallbackStage>,
    pub complete_shared: Option<PgAioHandleCallbackComplete>,
    pub complete_local: Option<PgAioHandleCallbackComplete>,
    pub report: Option<PgAioHandleCallbackReport>,
}

/* just to have something to put into aio_handle_cbs */
static aio_invalid_cb: PgAioHandleCallbacks = PgAioHandleCallbacks {
    stage: None,
    complete_shared: None,
    complete_local: None,
    report: None,
};

// TODO: the following completion callback tables live in their own (not yet
// ported) .c files: md.c (aio_md_readv_cb) and bufmgr.c
// (aio_shared_buffer_readv_cb / aio_local_buffer_readv_cb). Provide local
// placeholder tables so aio_handle_cbs is well-formed. Once those files are
// ported these should be imported instead.
static aio_md_readv_cb: PgAioHandleCallbacks = PgAioHandleCallbacks {
    stage: None,
    complete_shared: None,
    complete_local: None,
    report: None,
};
static aio_shared_buffer_readv_cb: PgAioHandleCallbacks = PgAioHandleCallbacks {
    stage: None,
    complete_shared: None,
    complete_local: None,
    report: None,
};
static aio_local_buffer_readv_cb: PgAioHandleCallbacks = PgAioHandleCallbacks {
    stage: None,
    complete_shared: None,
    complete_local: None,
    report: None,
};

pub struct PgAioHandleCallbacksEntry {
    pub cb: &'static PgAioHandleCallbacks,
    pub name: &'static str,
}

/*
 * Callback definition for the callbacks that can be registered on an IO
 * handle.  See PgAioHandleCallbackID's definition for an explanation for why
 * callbacks are not identified by a pointer.
 *
 * CALLBACK_ENTRY(id, callback) => [id] = {.cb = &callback, .name = #callback}
 * The designated initializers are reprojected as a positional array indexed by
 * PgAioHandleCallbackID; the ordering below matches the enum discriminants.
 */
static aio_handle_cbs: [PgAioHandleCallbacksEntry; 4] = [
    /* [PGAIO_HCB_INVALID] */
    PgAioHandleCallbacksEntry {
        cb: &aio_invalid_cb,
        name: "aio_invalid_cb",
    },
    /* [PGAIO_HCB_MD_READV] */
    PgAioHandleCallbacksEntry {
        cb: &aio_md_readv_cb,
        name: "aio_md_readv_cb",
    },
    /* [PGAIO_HCB_SHARED_BUFFER_READV] */
    PgAioHandleCallbacksEntry {
        cb: &aio_shared_buffer_readv_cb,
        name: "aio_shared_buffer_readv_cb",
    },
    /* [PGAIO_HCB_LOCAL_BUFFER_READV] */
    PgAioHandleCallbacksEntry {
        cb: &aio_local_buffer_readv_cb,
        name: "aio_local_buffer_readv_cb",
    },
];

/* --------------------------------------------------------------------------------
 * Public callback related functions operating on IO Handles
 * --------------------------------------------------------------------------------
 */

/*
 * Register callback for the IO handle.
 *
 * Only a limited number (PGAIO_HANDLE_MAX_CALLBACKS) of callbacks can be
 * registered for each IO.
 *
 * Callbacks need to be registered before [indirectly] calling
 * pgaio_io_start_*(), as the IO may be executed immediately.
 *
 * A callback can be passed a small bit of data, e.g. to indicate whether to
 * zero a buffer if it is invalid.
 *
 * Note that callbacks are executed in critical sections.  See the C source
 * for the full rationale.
 */
pub unsafe fn pgaio_io_register_callbacks(
    ioh: *mut PgAioHandle,
    cb_id: PgAioHandleCallbackID,
    cb_data: uint8,
) {
    let _ce: *const PgAioHandleCallbacksEntry = &aio_handle_cbs[cb_id as usize];

    Assert!(cb_id <= PGAIO_HCB_MAX);
    if cb_id as usize >= aio_handle_cbs.len() {
        elog!(ERROR, "callback {} is out of range", cb_id);
    }
    if aio_handle_cbs[cb_id as usize].cb.complete_shared.is_none()
        && aio_handle_cbs[cb_id as usize].cb.complete_local.is_none()
    {
        elog!(ERROR, "callback {} does not have a completion callback", cb_id);
    }
    if (*ioh).num_callbacks as usize >= PGAIO_HANDLE_MAX_CALLBACKS {
        elog!(
            PANIC,
            "too many callbacks, the max is {}",
            PGAIO_HANDLE_MAX_CALLBACKS
        );
    }
    (*ioh).callbacks[(*ioh).num_callbacks as usize] = cb_id as uint8;
    (*ioh).callbacks_data[(*ioh).num_callbacks as usize] = cb_data;

    // pgaio_debug_io(DEBUG3, ioh, "adding cb #%d, id %d/%s",
    //                ioh->num_callbacks + 1, cb_id, ce->name);

    (*ioh).num_callbacks += 1;
}

/*
 * Associate an array of data with the Handle. This is e.g. useful to
 * transport knowledge about which buffers a multi-block IO affects to
 * completion callbacks.
 *
 * Right now this can be done only once for each IO, even though multiple
 * callbacks can be registered. There aren't any known usecases requiring more
 * and the required amount of shared memory does add up, so it doesn't seem
 * worth multiplying memory usage by PGAIO_HANDLE_MAX_CALLBACKS.
 */
pub unsafe fn pgaio_io_set_handle_data_64(ioh: *mut PgAioHandle, data: *mut uint64, len: uint8) {
    Assert!((*ioh).state == crate::storage::aio_internal::PGAIO_HS_HANDED_OUT as uint8);
    Assert!((*ioh).handle_data_len == 0);
    Assert!(len as usize <= PG_IOV_MAX);
    Assert!((len as c_int) <= io_max_combine_limit);

    let mut i: c_int = 0;
    while i < len as c_int {
        *(*pgaio_ctl)
            .handle_data
            .add((*ioh).iovec_off as usize + i as usize) = *data.add(i as usize);
        i += 1;
    }
    (*ioh).handle_data_len = len;
}

/*
 * Convenience version of pgaio_io_set_handle_data_64() that converts a 32bit
 * array to a 64bit array. Without it callers would end up needing to
 * open-code equivalent code.
 */
pub unsafe fn pgaio_io_set_handle_data_32(ioh: *mut PgAioHandle, data: *mut uint32, len: uint8) {
    Assert!((*ioh).state == crate::storage::aio_internal::PGAIO_HS_HANDED_OUT as uint8);
    Assert!((*ioh).handle_data_len == 0);
    Assert!(len as usize <= PG_IOV_MAX);
    Assert!((len as c_int) <= io_max_combine_limit);

    let mut i: c_int = 0;
    while i < len as c_int {
        *(*pgaio_ctl)
            .handle_data
            .add((*ioh).iovec_off as usize + i as usize) = *data.add(i as usize) as uint64;
        i += 1;
    }
    (*ioh).handle_data_len = len;
}

/*
 * Return data set with pgaio_io_set_handle_data_*().
 */
pub unsafe fn pgaio_io_get_handle_data(ioh: *mut PgAioHandle, len: *mut uint8) -> *mut uint64 {
    Assert!((*ioh).handle_data_len > 0);

    *len = (*ioh).handle_data_len;

    (*pgaio_ctl).handle_data.add((*ioh).iovec_off as usize)
}

/* --------------------------------------------------------------------------------
 * Public IO Result related functions
 * --------------------------------------------------------------------------------
 */

pub unsafe fn pgaio_result_report(
    result: PgAioResult,
    target_data: *const PgAioTargetData,
    elevel: c_int,
) {
    let cb_id: PgAioHandleCallbackID = result.id() as PgAioHandleCallbackID;
    let ce: *const PgAioHandleCallbacksEntry = &aio_handle_cbs[cb_id as usize];

    Assert!(result.status() != PGAIO_RS_UNKNOWN as uint32);
    Assert!(result.status() != PGAIO_RS_OK as uint32);

    if (*ce).cb.report.is_none() {
        elog!(
            ERROR,
            "callback {}/{} does not have report callback",
            result.id(),
            (*ce).name
        );
    }

    ((*ce).cb.report.unwrap())(result, target_data, elevel);
}

/* --------------------------------------------------------------------------------
 * Internal callback related functions operating on IO Handles
 * --------------------------------------------------------------------------------
 */

/*
 * Internal function which invokes ->stage for all the registered callbacks.
 */
pub unsafe fn pgaio_io_call_stage(ioh: *mut PgAioHandle) {
    Assert!(
        ((*ioh).target as c_int) > PGAIO_TID_INVALID && ((*ioh).target as c_int) < PGAIO_TID_COUNT
    );
    Assert!(((*ioh).op as c_int) > PGAIO_OP_INVALID && ((*ioh).op as c_int) < PGAIO_OP_COUNT);

    let mut i: c_int = (*ioh).num_callbacks as c_int;
    while i > 0 {
        let cb_id: PgAioHandleCallbackID = (*ioh).callbacks[(i - 1) as usize] as PgAioHandleCallbackID;
        let _cb_data: uint8 = (*ioh).callbacks_data[(i - 1) as usize];
        let ce: *const PgAioHandleCallbacksEntry = &aio_handle_cbs[cb_id as usize];

        if (*ce).cb.stage.is_none() {
            i -= 1;
            continue;
        }

        // pgaio_debug_io(DEBUG3, ioh, "calling cb #%d %d/%s->stage(%u)",
        //                i, cb_id, ce->name, cb_data);
        ((*ce).cb.stage.unwrap())(ioh, _cb_data);

        i -= 1;
    }
}

/*
 * Internal function which invokes ->complete_shared for all the registered
 * callbacks.
 */
pub unsafe fn pgaio_io_call_complete_shared(ioh: *mut PgAioHandle) {
    let mut result: PgAioResult = core::mem::zeroed();

    START_CRIT_SECTION();

    Assert!(
        ((*ioh).target as c_int) > PGAIO_TID_INVALID && ((*ioh).target as c_int) < PGAIO_TID_COUNT
    );
    Assert!(((*ioh).op as c_int) > PGAIO_OP_INVALID && ((*ioh).op as c_int) < PGAIO_OP_COUNT);

    result.set_status(PGAIO_RS_OK as uint32); /* low level IO is always considered OK */
    result.result = (*ioh).result;
    result.set_id(PGAIO_HCB_INVALID as uint32);
    result.set_error_data(0);

    /*
     * Call callbacks with the last registered (innermost) callback first.
     * Each callback can modify the result forwarded to the next callback.
     */
    let mut i: c_int = (*ioh).num_callbacks as c_int;
    while i > 0 {
        let cb_id: PgAioHandleCallbackID = (*ioh).callbacks[(i - 1) as usize] as PgAioHandleCallbackID;
        let cb_data: uint8 = (*ioh).callbacks_data[(i - 1) as usize];
        let ce: *const PgAioHandleCallbacksEntry = &aio_handle_cbs[cb_id as usize];

        if (*ce).cb.complete_shared.is_none() {
            i -= 1;
            continue;
        }

        // pgaio_debug_io(DEBUG4, ioh,
        //   "calling cb #%d, id %d/%s->complete_shared(%u) with distilled result: ...",
        //   ...);
        let _ = pgaio_result_status_string;
        result = ((*ce).cb.complete_shared.unwrap())(ioh, result, cb_data);

        /* the callback should never transition to unknown */
        Assert!(result.status() != PGAIO_RS_UNKNOWN as uint32);

        i -= 1;
    }

    (*ioh).distilled_result = result;

    // pgaio_debug_io(DEBUG3, ioh,
    //   "after shared completion: distilled result: (...), raw_result: %d", ...);

    END_CRIT_SECTION();
}

/*
 * Internal function which invokes ->complete_local for all the registered
 * callbacks.
 *
 * Returns ioh->distilled_result after, possibly, being modified by local
 * callbacks.
 *
 * XXX: It'd be nice to deduplicate with pgaio_io_call_complete_shared().
 */
pub unsafe fn pgaio_io_call_complete_local(ioh: *mut PgAioHandle) -> PgAioResult {
    let mut result: PgAioResult;

    START_CRIT_SECTION();

    Assert!(
        ((*ioh).target as c_int) > PGAIO_TID_INVALID && ((*ioh).target as c_int) < PGAIO_TID_COUNT
    );
    Assert!(((*ioh).op as c_int) > PGAIO_OP_INVALID && ((*ioh).op as c_int) < PGAIO_OP_COUNT);

    /* start with distilled result from shared callback */
    result = core::ptr::read(&(*ioh).distilled_result);
    Assert!(result.status() != PGAIO_RS_UNKNOWN as uint32);

    let mut i: c_int = (*ioh).num_callbacks as c_int;
    while i > 0 {
        let cb_id: PgAioHandleCallbackID = (*ioh).callbacks[(i - 1) as usize] as PgAioHandleCallbackID;
        let cb_data: uint8 = (*ioh).callbacks_data[(i - 1) as usize];
        let ce: *const PgAioHandleCallbacksEntry = &aio_handle_cbs[cb_id as usize];

        if (*ce).cb.complete_local.is_none() {
            i -= 1;
            continue;
        }

        // pgaio_debug_io(DEBUG4, ioh,
        //   "calling cb #%d, id %d/%s->complete_local(%u) with distilled result: ...", ...);
        result = ((*ce).cb.complete_local.unwrap())(ioh, result, cb_data);

        /* the callback should never transition to unknown */
        Assert!(result.status() != PGAIO_RS_UNKNOWN as uint32);

        i -= 1;
    }

    /*
     * Note that we don't save the result in ioh->distilled_result, the local
     * callback's result should not ever matter to other waiters. However, the
     * local backend does care, so we return the result as modified by local
     * callbacks, which then can be passed to ioh->report_return->result.
     */
    // pgaio_debug_io(DEBUG3, ioh,
    //   "after local completion: result: (...), raw_result: %d", ...);

    END_CRIT_SECTION();

    result
}

// `io_max_combine_limit` is a PGC_POSTMASTER GUC defined in aio_init.rs.
use crate::storage::aio::aio_init::io_max_combine_limit;

// PG_IOV_MAX = Min(IOV_MAX, 128); pulled from port/pg_iovec.rs (c_int const).
const PG_IOV_MAX: usize = crate::port::pg_iovec::PG_IOV_MAX as usize;
