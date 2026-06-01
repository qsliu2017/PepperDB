/*-------------------------------------------------------------------------
 *
 * aio.c
 *    AIO - Core Logic
 *
 * For documentation about how AIO works on a higher level, including a
 * schematic example, see README.md.
 *
 *
 * AIO is a complicated subsystem. To keep things navigable, it is split
 * across a number of files:
 *
 * - method_*.c - different ways of executing AIO (e.g. worker process)
 *
 * - aio_target.c - IO on different kinds of targets
 *
 * - aio_io.c - method-independent code for specific IO ops (e.g. readv)
 *
 * - aio_callback.c - callbacks at IO operation lifecycle events
 *
 * - aio_init.c - per-server and per-backend initialization
 *
 * - aio.c - all other topics
 *
 * - read_stream.c - helper for reading buffered relation data
 *
 * - README.md - higher-level overview over AIO
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/aio.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::storage::aio::method_worker::{pgaio_debug, pgaio_debug_io};
use crate::storage::aio_internal::pgaio_method_ops;

use crate::lib::ilist::{
    dclist_count, dclist_delete_from, dclist_head, dclist_is_empty, dclist_pop_head_node,
    dclist_push_head, dclist_push_tail, dlist_iter, dlist_node,
};
use crate::miscadmin::{
    CritSectionCount, END_CRIT_SECTION, HOLD_INTERRUPTS, INTERRUPTS_CAN_BE_PROCESSED,
    IsUnderPostmaster, RESUME_INTERRUPTS, START_CRIT_SECTION,
};
use crate::storage::aio_internal::{
    pgaio_ctl, pgaio_io_call_complete_local, pgaio_io_call_complete_shared, pgaio_io_call_stage,
    pgaio_io_get_op_name, pgaio_io_get_target_name,
    pgaio_io_perform_synchronously,
    pgaio_my_backend, pgaio_sync_ops,
    pgaio_worker_ops, IoMethodOps, PgAioBackend, PgAioCtl, PgAioHandle, PgAioHandleState,
    PgAioOp, PGAIO_HS_COMPLETED_IO, PGAIO_HS_COMPLETED_LOCAL, PGAIO_HS_COMPLETED_SHARED,
    PGAIO_HS_DEFINED, PGAIO_HS_HANDED_OUT, PGAIO_HS_IDLE, PGAIO_HS_STAGED, PGAIO_HS_SUBMITTED,
    PGAIO_SUBMIT_BATCH_SIZE,
};
use crate::storage::aio::aio_target::pgaio_io_has_target;
use crate::storage::aio::aio_io::pgaio_io_uses_fd;
use crate::storage::aio_types::{PgAioResult, PgAioResultStatus, PgAioReturn, PGAIO_RS_UNKNOWN};
use crate::storage::procnumber::MyProcNumber;
use crate::utils::guc_hooks::GucSource;
use crate::utils::resowner::resowner::ResourceOwnerData;

/* crate-root macros used for list/pointer navigation */
use crate::{dclist_container, dclist_foreach, dclist_head_element};

/* ---------------------------------------------------------------------------
 * Local stubs for unported dependencies.
 * ------------------------------------------------------------------------- */

// utils/guc.h config_enum_entry: { name, val, hidden }
// TODO(pg-port): real config_enum_entry lives in utils/guc.h
#[repr(C)]
pub struct config_enum_entry {
    pub name: *const c_char,
    pub val: c_int,
    pub hidden: bool,
}
// SAFETY: name is a 'static string literal
unsafe impl Sync for config_enum_entry {}

// utils/guc.h GUC_check_errdetail() - stages detail string for a GUC check failure.
// TODO(pg-port): real GUC_check_errdetail lives in utils/guc.h
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        // no-op stub; the real implementation buffers into GUC_check_errmsg_string.
    }};
}

// storage/condition_variable.h - broadcast/wait/cancel wrappers.
// Imported from their real home in crate::storage::lmgr::condition_variable.
use crate::storage::lmgr::condition_variable::{
    ConditionVariableBroadcast, ConditionVariableCancelSleep, ConditionVariablePrepareToSleep,
    ConditionVariableSleep,
};

// utils/wait_event_types.h - WAIT_EVENT_AIO_IO_COMPLETION.
// Not yet ported; stub locally.
// TODO(pg-port): real WAIT_EVENT_AIO_IO_COMPLETION lives in utils/wait_event_types.h
const WAIT_EVENT_AIO_IO_COMPLETION: u32 = 0;

// utils/resowner.h ResourceOwnerRememberAioHandle / ResourceOwnerForgetAioHandle.
use crate::utils::resowner::resowner::{ResourceOwnerForgetAioHandle, ResourceOwnerRememberAioHandle};

// port/atomics.h pg_read_barrier / pg_write_barrier.
// Modelled as compiler+CPU fences; real implementations live per-arch in
// src/port/atomics/arch_*.rs.
// TODO(pg-port): dedup with the real pg_read_barrier / pg_write_barrier once
//               port/atomics.h is fully ported.
#[inline]
unsafe fn pg_read_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Acquire);
}

#[inline]
unsafe fn pg_write_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Release);
}

// utils/injection_point.h INJECTION_POINT(name, arg).
// Not yet ported; no-op stub.
// TODO(pg-port): real INJECTION_POINT lives in utils/injection_point.h
macro_rules! INJECTION_POINT {
    ($name:expr, $arg:expr) => {{
        let _ = $arg;
    }};
}

// IOMETHOD_* enum values (storage/aio.h).
// Not yet extracted into a shared location; redefined locally here.
// TODO(pg-port): dedup once storage/aio.h enum is in a shared module.
pub type IoMethod = c_int;
pub const IOMETHOD_SYNC: IoMethod = 0;
pub const IOMETHOD_WORKER: IoMethod = 1;
// io_uring is Linux-only; this Darwin port models worker/sync methods only.
// IOMETHOD_IO_URING would be 2 on Linux.

// DEFAULT_IO_METHOD (storage/aio.h). Sync on all non-io_uring platforms.
// TODO(pg-port): real DEFAULT_IO_METHOD lives in storage/aio.h.
pub const DEFAULT_IO_METHOD: IoMethod = IOMETHOD_WORKER;

// PgAioHandleFlags bitfield (storage/aio.h).
pub type PgAioHandleFlags = uint8;
pub const PGAIO_HF_SYNCHRONOUS: PgAioHandleFlags = 1 << 0;
pub const PGAIO_HF_REFERENCES_LOCAL: PgAioHandleFlags = 1 << 1;
pub const PGAIO_HF_BUFFERED: PgAioHandleFlags = 1 << 2;

// PgAioResultStatus variants (imported from aio_types; used in result reset).
use crate::storage::aio_types::{PGAIO_RS_ERROR, PGAIO_RS_OK, PGAIO_RS_PARTIAL, PGAIO_RS_WARNING};

// PgAioWaitRef (storage/aio.h).
// TODO(pg-port): real PgAioWaitRef lives in storage/aio.h.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgAioWaitRef {
    pub aio_index: uint32,
    pub generation_upper: uint32,
    pub generation_lower: uint32,
}

// PgAioOp INVALID sentinel (mirrors aio_io.rs / aio_target.rs; re-declared
// locally to avoid circular import).
const PGAIO_OP_INVALID_LOCAL: PgAioOp = 0;
// PgAioTargetID INVALID sentinel.
type PgAioTargetID = c_int;
const PGAIO_TID_INVALID_LOCAL: PgAioTargetID = 0;

/* ---------------------------------------------------------------------------
 * Module-level globals
 * (postgres: file-scope or global variables in aio.c / aio.h)
 * ------------------------------------------------------------------------- */

/*
 * Options for io_method GUC.
 *
 * NOTE: io_uring is Linux-only; it is not included on this Darwin port.
 * The table below mirrors the C array but omits the io_uring entry.
 */
pub static io_method_options: [config_enum_entry; 3] = [
    config_enum_entry {
        name: b"sync\0".as_ptr() as *const c_char,
        val: IOMETHOD_SYNC,
        hidden: false,
    },
    config_enum_entry {
        name: b"worker\0".as_ptr() as *const c_char,
        val: IOMETHOD_WORKER,
        hidden: false,
    },
    /* sentinel */
    config_enum_entry {
        name: core::ptr::null(),
        val: 0,
        hidden: false,
    },
];

/* GUCs */
#[no_mangle]
pub static mut io_method: c_int = DEFAULT_IO_METHOD;
#[no_mangle]
pub static mut io_max_concurrency: c_int = -1;

/*
 * pgaio_method_ops_table maps IoMethod discriminants to their vtable.
 * io_uring is Linux-only; gate it out with a comment rather than #[cfg].
 *
 * C: static const IoMethodOps *const pgaio_method_ops_table[]
 * Rust: *const pointers require wrapper for Sync; use const pointers in a
 * non-exported helper instead of a static table to avoid Sync issues.
 */

/* callbacks for the configured io_method, set by assign_io_method */
/* C: const IoMethodOps *pgaio_method_ops; */
/* Rust: imported from aio_internal where it is declared extern "C". */
/* pgaio_method_ops is already declared in aio_internal as extern "C" static. */

/* --------------------------------------------------------------------------------
 * Public Functions related to PgAioHandle
 * --------------------------------------------------------------------------------
 */

/*
 * Acquire an AioHandle, waiting for IO completion if necessary.
 *
 * Each backend can only have one AIO handle that has been "handed out" to
 * code, but not yet submitted or released. This restriction is necessary to
 * ensure that it is possible for code to wait for an unused handle by waiting
 * for in-flight IO to complete. There is a limited number of handles in each
 * backend, if multiple handles could be handed out without being submitted,
 * waiting for all in-flight IO to complete would not guarantee that handles
 * free up.
 *
 * It is cheap to acquire an IO handle, unless all handles are in use. In that
 * case this function waits for the oldest IO to complete. If that is not
 * desirable, use pgaio_io_acquire_nb().
 *
 * If a handle was acquired but then does not turn out to be needed,
 * e.g. because pgaio_io_acquire() is called before starting an IO in a
 * critical section, the handle needs to be released with pgaio_io_release().
 *
 *
 * To react to the completion of the IO as soon as it is known to have
 * completed, callbacks can be registered with pgaio_io_register_callbacks().
 *
 * To actually execute IO using the returned handle, the pgaio_io_start_*()
 * family of functions is used. In many cases the pgaio_io_start_*() call will
 * not be done directly by code that acquired the handle, but by lower level
 * code that gets passed the handle. E.g. if code in bufmgr.c wants to perform
 * AIO, it typically will pass the handle to smgr.c, which will pass it on to
 * md.c, on to fd.c, which then finally calls pgaio_io_start_*().  This
 * forwarding allows the various layers to react to the IO's completion by
 * registering callbacks. These callbacks in turn can translate a lower
 * layer's result into a result understandable by a higher layer.
 *
 * During pgaio_io_start_*() the IO is staged (i.e. prepared for execution but
 * not submitted to the kernel). Unless in batchmode
 * (c.f. pgaio_enter_batchmode()), the IO will also get submitted for
 * execution. Note that, whether in batchmode or not, the IO might even
 * complete before the functions return.
 *
 * After pgaio_io_start_*() the AioHandle is "consumed" and may not be
 * referenced by the IO issuing code. To e.g. wait for IO, references to the
 * IO can be established with pgaio_io_get_wref() *before* pgaio_io_start_*()
 * is called.  pgaio_wref_wait() can be used to wait for the IO to complete.
 *
 *
 * To know if the IO [partially] succeeded or failed, a PgAioReturn * can be
 * passed to pgaio_io_acquire(). Once the issuing backend has called
 * pgaio_wref_wait(), the PgAioReturn contains information about whether the
 * operation succeeded and details about the first failure, if any. The error
 * can be raised / logged with pgaio_result_report().
 *
 * The lifetime of the memory pointed to be *ret needs to be at least as long
 * as the passed in resowner. If the resowner releases resources before the IO
 * completes (typically due to an error), the reference to *ret will be
 * cleared. In case of resowner cleanup *ret will not be updated with the
 * results of the IO operation.
 */
pub unsafe fn pgaio_io_acquire(
    resowner: *mut ResourceOwnerData,
    ret: *mut PgAioReturn,
) -> *mut PgAioHandle {
    let mut h: *mut PgAioHandle;

    loop {
        h = pgaio_io_acquire_nb(resowner, ret);

        if !h.is_null() {
            return h;
        }

        /*
         * Evidently all handles by this backend are in use. Just wait for
         * some to complete.
         */
        pgaio_io_wait_for_free();
    }
}

/*
 * Acquire an AioHandle, returning NULL if no handles are free.
 *
 * See pgaio_io_acquire(). The only difference is that this function will return
 * NULL if there are no idle handles, instead of blocking.
 */
pub unsafe fn pgaio_io_acquire_nb(
    resowner: *mut ResourceOwnerData,
    ret: *mut PgAioReturn,
) -> *mut PgAioHandle {
    let mut ioh: *mut PgAioHandle = null_mut();

    if (*pgaio_my_backend).num_staged_ios >= PGAIO_SUBMIT_BATCH_SIZE as uint16 {
        Assert!((*pgaio_my_backend).num_staged_ios == PGAIO_SUBMIT_BATCH_SIZE as uint16);
        pgaio_submit_staged();
    }

    if !(*pgaio_my_backend).handed_out_io.is_null() {
        elog!(ERROR, "API violation: Only one IO can be handed out");
    }

    /*
     * Probably not needed today, as interrupts should not process this IO,
     * but...
     */
    HOLD_INTERRUPTS();

    if !dclist_is_empty(&(*pgaio_my_backend).idle_ios) {
        let ion: *mut dlist_node = dclist_pop_head_node(&mut (*pgaio_my_backend).idle_ios);

        ioh = dclist_container!(PgAioHandle, node, ion);

        Assert!((*ioh).state == PGAIO_HS_IDLE as uint8);
        Assert!((*ioh).owner_procno == MyProcNumber);

        pgaio_io_update_state(ioh, PGAIO_HS_HANDED_OUT);
        (*pgaio_my_backend).handed_out_io = ioh;

        if !resowner.is_null() {
            pgaio_io_resowner_register(ioh, resowner);
        }

        if !ret.is_null() {
            (*ioh).report_return = ret;
            (*ret).result.set_status(PGAIO_RS_UNKNOWN as uint32);
        }
    }

    RESUME_INTERRUPTS();

    ioh
}

/*
 * Release IO handle that turned out to not be required.
 *
 * See pgaio_io_acquire() for more details.
 */
pub unsafe fn pgaio_io_release(ioh: *mut PgAioHandle) {
    if ioh == (*pgaio_my_backend).handed_out_io {
        Assert!((*ioh).state == PGAIO_HS_HANDED_OUT as uint8);
        Assert!(!(*ioh).resowner.is_null());

        (*pgaio_my_backend).handed_out_io = null_mut();

        /*
         * Note that no interrupts are processed between the handed_out_io
         * check and the call to reclaim - that's important as otherwise an
         * interrupt could have already reclaimed the handle.
         */
        pgaio_io_reclaim(ioh);
    } else {
        elog!(ERROR, "release in unexpected state");
    }
}

/*
 * Release IO handle during resource owner cleanup.
 */
pub unsafe fn pgaio_io_release_resowner(ioh_node: *mut dlist_node, on_error: bool) {
    let ioh: *mut PgAioHandle =
        crate::dlist_container!(PgAioHandle, resowner_node, ioh_node);

    Assert!(!(*ioh).resowner.is_null());

    /*
     * Otherwise an interrupt, in the middle of releasing the IO, could end up
     * trying to wait for the IO, leading to state confusion.
     */
    HOLD_INTERRUPTS();

    ResourceOwnerForgetAioHandle((*ioh).resowner, &mut (*ioh).resowner_node);
    (*ioh).resowner = null_mut();

    match (*ioh).state as PgAioHandleState {
        s if s == PGAIO_HS_IDLE => {
            elog!(ERROR, "unexpected");
        }
        s if s == PGAIO_HS_HANDED_OUT => {
            Assert!(
                ioh == (*pgaio_my_backend).handed_out_io
                    || (*pgaio_my_backend).handed_out_io.is_null()
            );

            if ioh == (*pgaio_my_backend).handed_out_io {
                (*pgaio_my_backend).handed_out_io = null_mut();
                if !on_error {
                    elog!(WARNING, "leaked AIO handle");
                }
            }

            pgaio_io_reclaim(ioh);
        }
        s if s == PGAIO_HS_DEFINED || s == PGAIO_HS_STAGED => {
            if !on_error {
                elog!(WARNING, "AIO handle was not submitted");
            }
            pgaio_submit_staged();
        }
        s if s == PGAIO_HS_SUBMITTED
            || s == PGAIO_HS_COMPLETED_IO
            || s == PGAIO_HS_COMPLETED_SHARED
            || s == PGAIO_HS_COMPLETED_LOCAL =>
        {
            /* this is expected to happen */
        }
        _ => {}
    }

    /*
     * Need to unregister the reporting of the IO's result, the memory it's
     * referencing likely has gone away.
     */
    if !(*ioh).report_return.is_null() {
        (*ioh).report_return = null_mut();
    }

    RESUME_INTERRUPTS();
}

/*
 * Add a [set of] flags to the IO.
 *
 * Note that this combines flags with already set flags, rather than set flags
 * to explicitly the passed in parameters. This is to allow multiple callsites
 * to set flags.
 */
pub unsafe fn pgaio_io_set_flag(ioh: *mut PgAioHandle, flag: PgAioHandleFlags) {
    Assert!((*ioh).state == PGAIO_HS_HANDED_OUT as uint8);

    (*ioh).flags |= flag;
}

/*
 * Returns an ID uniquely identifying the IO handle. This is only really
 * useful for logging, as handles are reused across multiple IOs.
 */
pub unsafe fn pgaio_io_get_id(ioh: *mut PgAioHandle) -> c_int {
    Assert!(
        ioh >= (*pgaio_ctl).io_handles
            && ioh < (*pgaio_ctl).io_handles.add((*pgaio_ctl).io_handle_count as usize)
    );
    ioh.offset_from((*pgaio_ctl).io_handles) as c_int
}

/*
 * Return the ProcNumber for the process that can use an IO handle. The
 * mapping from IO handles to PGPROCs is static, therefore this even works
 * when the corresponding PGPROC is not in use.
 */
pub unsafe fn pgaio_io_get_owner(ioh: *mut PgAioHandle) -> c_int {
    (*ioh).owner_procno
}

/*
 * Return a wait reference for the IO. Only wait references can be used to
 * wait for an IOs completion, as handles themselves can be reused after
 * completion.  See also the comment above pgaio_io_acquire().
 */
pub unsafe fn pgaio_io_get_wref(ioh: *mut PgAioHandle, iow: *mut PgAioWaitRef) {
    Assert!(
        (*ioh).state == PGAIO_HS_HANDED_OUT as uint8
            || (*ioh).state == PGAIO_HS_DEFINED as uint8
            || (*ioh).state == PGAIO_HS_STAGED as uint8
    );
    Assert!((*ioh).generation != 0);

    (*iow).aio_index =
        ioh.offset_from((*pgaio_ctl).io_handles) as uint32;
    (*iow).generation_upper = ((*ioh).generation >> 32) as uint32;
    (*iow).generation_lower = (*ioh).generation as uint32;
}

/* --------------------------------------------------------------------------------
 * Internal Functions related to PgAioHandle
 * --------------------------------------------------------------------------------
 */

#[inline]
unsafe fn pgaio_io_update_state(ioh: *mut PgAioHandle, new_state: PgAioHandleState) {
    /*
     * All callers need to have held interrupts in some form, otherwise
     * interrupt processing could wait for the IO to complete, while in an
     * intermediary state.
     */
    Assert!(!INTERRUPTS_CAN_BE_PROCESSED());

    pgaio_debug_io!(
        DEBUG5,
        ioh,
        "updating state to {}",
        pgaio_io_state_get_name(new_state)
    );

    /*
     * Ensure the changes signified by the new state are visible before the
     * new state becomes visible.
     */
    pg_write_barrier();

    (*ioh).state = new_state as uint8;
}

unsafe fn pgaio_io_resowner_register(
    ioh: *mut PgAioHandle,
    resowner: *mut ResourceOwnerData,
) {
    Assert!((*ioh).resowner.is_null());
    Assert!(!resowner.is_null());

    ResourceOwnerRememberAioHandle(resowner, &mut (*ioh).resowner_node);
    (*ioh).resowner = resowner;
}

/*
 * Stage IO for execution and, if appropriate, submit it immediately.
 *
 * Should only be called from pgaio_io_start_*().
 */
pub unsafe fn pgaio_io_stage(ioh: *mut PgAioHandle, op: PgAioOp) {
    let needs_synchronous: bool;

    Assert!((*ioh).state == PGAIO_HS_HANDED_OUT as uint8);
    Assert!((*pgaio_my_backend).handed_out_io == ioh);
    Assert!(pgaio_io_has_target(ioh));

    /*
     * Otherwise an interrupt, in the middle of staging and possibly executing
     * the IO, could end up trying to wait for the IO, leading to state
     * confusion.
     */
    HOLD_INTERRUPTS();

    (*ioh).op = op as uint8;
    (*ioh).result = 0;

    pgaio_io_update_state(ioh, PGAIO_HS_DEFINED);

    /* allow a new IO to be staged */
    (*pgaio_my_backend).handed_out_io = null_mut();

    pgaio_io_call_stage(ioh);

    pgaio_io_update_state(ioh, PGAIO_HS_STAGED);

    /*
     * Synchronous execution has to be executed, well, synchronously, so check
     * that first.
     */
    needs_synchronous = pgaio_io_needs_synchronous_execution(ioh);

    pgaio_debug_io!(
        DEBUG3,
        ioh,
        "staged (synchronous: {}, in_batch: {})",
        needs_synchronous as c_int,
        (*pgaio_my_backend).in_batchmode as c_int
    );

    if !needs_synchronous {
        (*pgaio_my_backend).staged_ios[(*pgaio_my_backend).num_staged_ios as usize] = ioh;
        (*pgaio_my_backend).num_staged_ios += 1;
        Assert!((*pgaio_my_backend).num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE as uint16);

        /*
         * Unless code explicitly opted into batching IOs, submit the IO
         * immediately.
         */
        if !(*pgaio_my_backend).in_batchmode {
            pgaio_submit_staged();
        }
    } else {
        pgaio_io_prepare_submit(ioh);
        pgaio_io_perform_synchronously(ioh);
    }

    RESUME_INTERRUPTS();
}

pub unsafe fn pgaio_io_needs_synchronous_execution(ioh: *mut PgAioHandle) -> bool {
    /*
     * If the caller said to execute the IO synchronously, do so.
     *
     * XXX: We could optimize the logic when to execute synchronously by first
     * checking if there are other IOs in flight and only synchronously
     * executing if not. Unclear whether that'll be sufficiently common to be
     * worth worrying about.
     */
    if (*ioh).flags & PGAIO_HF_SYNCHRONOUS != 0 {
        return true;
    }

    /* Check if the IO method requires synchronous execution of IO */
    if let Some(needs_sync) = (*pgaio_method_ops).needs_synchronous_execution {
        return needs_sync(ioh);
    }

    false
}

/*
 * Handle IO being processed by IO method.
 *
 * Should be called by IO methods / synchronous IO execution, just before the
 * IO is performed.
 */
pub unsafe fn pgaio_io_prepare_submit(ioh: *mut PgAioHandle) {
    pgaio_io_update_state(ioh, PGAIO_HS_SUBMITTED);

    dclist_push_tail(&mut (*pgaio_my_backend).in_flight_ios, &mut (*ioh).node);
}

/*
 * Handle IO getting completed by a method.
 *
 * Should be called by IO methods / synchronous IO execution, just after the
 * IO has been performed.
 *
 * Expects to be called in a critical section. We expect IOs to be usable for
 * WAL etc, which requires being able to execute completion callbacks in a
 * critical section.
 */
pub unsafe fn pgaio_io_process_completion(ioh: *mut PgAioHandle, result: c_int) {
    Assert!((*ioh).state == PGAIO_HS_SUBMITTED as uint8);

    Assert!(CritSectionCount > 0);

    (*ioh).result = result;

    pgaio_io_update_state(ioh, PGAIO_HS_COMPLETED_IO);

    INJECTION_POINT!("aio-process-completion-before-shared", ioh);

    pgaio_io_call_complete_shared(ioh);

    pgaio_io_update_state(ioh, PGAIO_HS_COMPLETED_SHARED);

    /* condition variable broadcast ensures state is visible before wakeup */
    ConditionVariableBroadcast(&raw mut (*ioh).cv as *mut crate::storage::lmgr::condition_variable::ConditionVariable);

    /* contains call to pgaio_io_call_complete_local() */
    if (*ioh).owner_procno == MyProcNumber {
        pgaio_io_reclaim(ioh);
    }
}

/*
 * Has the IO completed and thus the IO handle been reused?
 *
 * This is useful when waiting for IO completion at a low level (e.g. in an IO
 * method's ->wait_one() callback).
 */
pub unsafe fn pgaio_io_was_recycled(
    ioh: *mut PgAioHandle,
    ref_generation: uint64,
    state: *mut PgAioHandleState,
) -> bool {
    *state = (*ioh).state as PgAioHandleState;

    /*
     * Ensure that we don't see an earlier state of the handle than ioh->state
     * due to compiler or CPU reordering. This protects both ->generation as
     * directly used here, and other fields in the handle accessed in the
     * caller if the handle was not reused.
     */
    pg_read_barrier();

    (*ioh).generation != ref_generation
}

/*
 * Wait for IO to complete. External code should never use this, outside of
 * the AIO subsystem waits are only allowed via pgaio_wref_wait().
 */
unsafe fn pgaio_io_wait(ioh: *mut PgAioHandle, ref_generation: uint64) {
    let mut state: PgAioHandleState = 0;
    let am_owner: bool;

    am_owner = (*ioh).owner_procno == MyProcNumber;

    if pgaio_io_was_recycled(ioh, ref_generation, &mut state) {
        return;
    }

    if am_owner
        && state != PGAIO_HS_SUBMITTED
        && state != PGAIO_HS_COMPLETED_IO
        && state != PGAIO_HS_COMPLETED_SHARED
        && state != PGAIO_HS_COMPLETED_LOCAL
    {
        elog!(
            PANIC,
            "waiting for own IO {} in wrong state: {}",
            pgaio_io_get_id(ioh),
            {
                let name = pgaio_io_get_state_name(ioh);
                std::ffi::CStr::from_ptr(name).to_str().unwrap_or("?")
            }
        );
    }

    loop {
        if pgaio_io_was_recycled(ioh, ref_generation, &mut state) {
            return;
        }

        if state == PGAIO_HS_IDLE || state == PGAIO_HS_HANDED_OUT {
            elog!(ERROR, "IO in wrong state: {}", state);
        }

        if state == PGAIO_HS_SUBMITTED {
            /*
             * If we need to wait via the IO method, do so now. Don't
             * check via the IO method if the issuing backend is executing
             * the IO synchronously.
             */
            if let Some(wait_one) = (*pgaio_method_ops).wait_one {
                if ((*ioh).flags & PGAIO_HF_SYNCHRONOUS) == 0 {
                    wait_one(ioh, ref_generation);
                    continue;
                }
            }
            /* fallthrough: wait on condition variable */
        }

        if state == PGAIO_HS_SUBMITTED
            || state == PGAIO_HS_DEFINED
            || state == PGAIO_HS_STAGED
            || state == PGAIO_HS_COMPLETED_IO
        {
            /* shouldn't be able to hit this otherwise */
            Assert!(IsUnderPostmaster);
            /* ensure we're going to get woken up */
            ConditionVariablePrepareToSleep(&raw mut (*ioh).cv as *mut crate::storage::lmgr::condition_variable::ConditionVariable);

            while !pgaio_io_was_recycled(ioh, ref_generation, &mut state) {
                if state == PGAIO_HS_COMPLETED_SHARED || state == PGAIO_HS_COMPLETED_LOCAL {
                    break;
                }
                ConditionVariableSleep(&raw mut (*ioh).cv as *mut crate::storage::lmgr::condition_variable::ConditionVariable, WAIT_EVENT_AIO_IO_COMPLETION);
            }

            ConditionVariableCancelSleep();
            continue;
        }

        if state == PGAIO_HS_COMPLETED_SHARED || state == PGAIO_HS_COMPLETED_LOCAL {
            /*
             * Note that no interrupts are processed between
             * pgaio_io_was_recycled() and this check - that's important
             * as otherwise an interrupt could have already reclaimed the
             * handle.
             */
            if am_owner {
                pgaio_io_reclaim(ioh);
            }
            return;
        }
    }
}

/*
 * Make IO handle ready to be reused after IO has completed or after the
 * handle has been released without being used.
 *
 * Note that callers need to be careful about only calling this in the right
 * state and that no interrupts can be processed between the state check and
 * the call to pgaio_io_reclaim(). Otherwise interrupt processing could
 * already have reclaimed the handle.
 */
unsafe fn pgaio_io_reclaim(ioh: *mut PgAioHandle) {
    /* This is only ok if it's our IO */
    Assert!((*ioh).owner_procno == MyProcNumber);
    Assert!((*ioh).state != PGAIO_HS_IDLE as uint8);

    /* see comment in function header */
    HOLD_INTERRUPTS();

    /*
     * It's a bit ugly, but right now the easiest place to put the execution
     * of local completion callbacks is this function, as we need to execute
     * local callbacks just before reclaiming at multiple callsites.
     */
    if (*ioh).state == PGAIO_HS_COMPLETED_SHARED as uint8 {
        let local_result: PgAioResult;

        local_result = pgaio_io_call_complete_local(ioh);
        pgaio_io_update_state(ioh, PGAIO_HS_COMPLETED_LOCAL);

        if !(*ioh).report_return.is_null() {
            (*(*ioh).report_return).result = local_result;
            (*(*ioh).report_return).target_data = core::ptr::read(&raw const (*ioh).target_data);
        }
    }

    pgaio_debug_io!(
        DEBUG4,
        ioh,
        "reclaiming: distilled_result: (status {}, id {}, error_data {}), raw_result: {}",
        {
            let s = pgaio_result_status_string((*ioh).distilled_result.status() as PgAioResultStatus);
            std::ffi::CStr::from_ptr(s).to_str().unwrap_or("?")
        },
        (*ioh).distilled_result.id(),
        (*ioh).distilled_result.error_data(),
        (*ioh).result
    );

    /* if the IO has been defined, it's on the in-flight list, remove */
    if (*ioh).state != PGAIO_HS_HANDED_OUT as uint8 {
        dclist_delete_from(&mut (*pgaio_my_backend).in_flight_ios, &mut (*ioh).node);
    }

    if !(*ioh).resowner.is_null() {
        ResourceOwnerForgetAioHandle((*ioh).resowner, &mut (*ioh).resowner_node);
        (*ioh).resowner = null_mut();
    }

    Assert!((*ioh).resowner.is_null());

    /*
     * Update generation & state first, before resetting the IO's fields,
     * otherwise a concurrent "viewer" could think the fields are valid, even
     * though they are being reset.  Increment the generation first, so that
     * we can assert elsewhere that we never wait for an IDLE IO.  While it's
     * a bit weird for the state to go backwards for a generation, it's OK
     * here, as there cannot be references to the "reborn" IO yet.  Can't
     * update both at once, so something has to give.
     */
    (*ioh).generation += 1;
    pgaio_io_update_state(ioh, PGAIO_HS_IDLE);

    /* ensure the state update is visible before we reset fields */
    pg_write_barrier();

    (*ioh).op = PGAIO_OP_INVALID_LOCAL as uint8;
    (*ioh).target = PGAIO_TID_INVALID_LOCAL as uint8;
    (*ioh).flags = 0;
    (*ioh).num_callbacks = 0;
    (*ioh).handle_data_len = 0;
    (*ioh).report_return = null_mut();
    (*ioh).result = 0;
    (*ioh).distilled_result.set_status(PGAIO_RS_UNKNOWN as uint32);

    /*
     * We push the IO to the head of the idle IO list, that seems more cache
     * efficient in cases where only a few IOs are used.
     */
    dclist_push_head(&mut (*pgaio_my_backend).idle_ios, &mut (*ioh).node);

    RESUME_INTERRUPTS();
}

/*
 * Wait for an IO handle to become usable.
 *
 * This only really is useful for pgaio_io_acquire().
 */
unsafe fn pgaio_io_wait_for_free() {
    let mut reclaimed: c_int = 0;

    pgaio_debug!(
        DEBUG2,
        "waiting for free IO with {} pending, {} in-flight, {} idle IOs",
        (*pgaio_my_backend).num_staged_ios,
        dclist_count(&(*pgaio_my_backend).in_flight_ios),
        dclist_count(&(*pgaio_my_backend).idle_ios)
    );

    /*
     * First check if any of our IOs actually have completed - when using
     * worker, that'll often be the case. We could do so as part of the loop
     * below, but that'd potentially lead us to wait for some IO submitted
     * before.
     */
    for i in 0..io_max_concurrency {
        let ioh: *mut PgAioHandle = (*pgaio_ctl)
            .io_handles
            .add(((*pgaio_my_backend).io_handle_off + i as uint32) as usize);

        if (*ioh).state == PGAIO_HS_COMPLETED_SHARED as uint8 {
            /*
             * Note that no interrupts are processed between the state check
             * and the call to reclaim - that's important as otherwise an
             * interrupt could have already reclaimed the handle.
             *
             * Need to ensure that there's no reordering, in the more common
             * paths, where we wait for IO, that's done by
             * pgaio_io_was_recycled().
             */
            pg_read_barrier();
            pgaio_io_reclaim(ioh);
            reclaimed += 1;
        }
    }

    if reclaimed > 0 {
        return;
    }

    /*
     * If we have any unsubmitted IOs, submit them now. We'll start waiting in
     * a second, so it's better they're in flight. This also addresses the
     * edge-case that all IOs are unsubmitted.
     */
    if (*pgaio_my_backend).num_staged_ios > 0 {
        pgaio_submit_staged();
    }

    /* possibly some IOs finished during submission */
    if !dclist_is_empty(&(*pgaio_my_backend).idle_ios) {
        return;
    }

    if dclist_count(&(*pgaio_my_backend).in_flight_ios) == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "no free IOs despite no in-flight IOs: {} pending, {} in-flight, {} idle IOs",
                (*pgaio_my_backend).num_staged_ios,
                dclist_count(&(*pgaio_my_backend).in_flight_ios),
                dclist_count(&(*pgaio_my_backend).idle_ios)
            )
        );
    }

    /*
     * Wait for the oldest in-flight IO to complete.
     *
     * XXX: Reusing the general IO wait is suboptimal, we don't need to wait
     * for that specific IO to complete, we just need *any* IO to complete.
     */
    {
        let ioh: *mut PgAioHandle =
            dclist_head_element!(PgAioHandle, node, &mut (*pgaio_my_backend).in_flight_ios);
        let generation: uint64 = (*ioh).generation;

        match (*ioh).state as PgAioHandleState {
            /* should not be in in-flight list */
            s if s == PGAIO_HS_IDLE
                || s == PGAIO_HS_DEFINED
                || s == PGAIO_HS_HANDED_OUT
                || s == PGAIO_HS_STAGED
                || s == PGAIO_HS_COMPLETED_LOCAL =>
            {
                elog!(
                    ERROR,
                    "shouldn't get here with io:{} in state {}",
                    pgaio_io_get_id(ioh),
                    (*ioh).state
                );
            }

            s if s == PGAIO_HS_COMPLETED_IO || s == PGAIO_HS_SUBMITTED => {
                pgaio_debug_io!(
                    DEBUG2,
                    ioh,
                    "waiting for free io with {} in flight",
                    dclist_count(&(*pgaio_my_backend).in_flight_ios)
                );

                /*
                 * In a more general case this would be racy, because the
                 * generation could increase after we read ioh->state above.
                 * But we are only looking at IOs by the current backend and
                 * the IO can only be recycled by this backend.  Even this is
                 * only OK because we get the handle's generation before
                 * potentially processing interrupts, e.g. as part of
                 * pgaio_debug_io().
                 */
                pgaio_io_wait(ioh, generation);
            }

            s if s == PGAIO_HS_COMPLETED_SHARED => {
                /*
                 * It's possible that another backend just finished this IO.
                 *
                 * Note that no interrupts are processed between the state
                 * check and the call to reclaim - that's important as
                 * otherwise an interrupt could have already reclaimed the
                 * handle.
                 *
                 * Need to ensure that there's no reordering, in the more
                 * common paths, where we wait for IO, that's done by
                 * pgaio_io_was_recycled().
                 */
                pg_read_barrier();
                pgaio_io_reclaim(ioh);
            }

            _ => {}
        }

        if dclist_count(&(*pgaio_my_backend).idle_ios) == 0 {
            elog!(PANIC, "no idle IO after waiting for IO to terminate");
        }
    }
}

/*
 * Internal - code outside of AIO should never need this and it'd be hard for
 * such code to be safe.
 */
unsafe fn pgaio_io_from_wref(
    iow: *mut PgAioWaitRef,
    ref_generation: *mut uint64,
) -> *mut PgAioHandle {
    let ioh: *mut PgAioHandle;

    Assert!(((*iow).aio_index as uint32) < (*pgaio_ctl).io_handle_count);

    ioh = (*pgaio_ctl).io_handles.add((*iow).aio_index as usize);

    *ref_generation = (((*iow).generation_upper as uint64) << 32) | ((*iow).generation_lower as uint64);

    Assert!(*ref_generation != 0);

    ioh
}

unsafe fn pgaio_io_state_get_name(s: PgAioHandleState) -> &'static str {
    match s {
        x if x == PGAIO_HS_IDLE => "IDLE",
        x if x == PGAIO_HS_HANDED_OUT => "HANDED_OUT",
        x if x == PGAIO_HS_DEFINED => "DEFINED",
        x if x == PGAIO_HS_STAGED => "STAGED",
        x if x == PGAIO_HS_SUBMITTED => "SUBMITTED",
        x if x == PGAIO_HS_COMPLETED_IO => "COMPLETED_IO",
        x if x == PGAIO_HS_COMPLETED_SHARED => "COMPLETED_SHARED",
        x if x == PGAIO_HS_COMPLETED_LOCAL => "COMPLETED_LOCAL",
        _ => "(unknown)",
    }
}

pub unsafe fn pgaio_io_get_state_name(ioh: *mut PgAioHandle) -> *const c_char {
    /* delegate to the internal name-lookup; convert to C string pointer */
    let s = pgaio_io_state_get_name((*ioh).state as PgAioHandleState);
    /* SAFETY: all branches return 'static NUL-terminated string literals */
    s.as_ptr() as *const c_char
}

pub unsafe fn pgaio_result_status_string(rs: PgAioResultStatus) -> *const c_char {
    match rs {
        x if x == PGAIO_RS_UNKNOWN as PgAioResultStatus => c"UNKNOWN".as_ptr(),
        x if x == PGAIO_RS_OK as PgAioResultStatus => c"OK".as_ptr(),
        x if x == PGAIO_RS_WARNING as PgAioResultStatus => c"WARNING".as_ptr(),
        x if x == PGAIO_RS_PARTIAL as PgAioResultStatus => c"PARTIAL".as_ptr(),
        x if x == PGAIO_RS_ERROR as PgAioResultStatus => c"ERROR".as_ptr(),
        _ => null(),
    }
}

/* --------------------------------------------------------------------------------
 * Functions primarily related to IO Wait References
 * --------------------------------------------------------------------------------
 */

/*
 * Mark a wait reference as invalid
 */
pub unsafe fn pgaio_wref_clear(iow: *mut PgAioWaitRef) {
    (*iow).aio_index = u32::MAX;
}

/* Is the wait reference valid? */
pub unsafe fn pgaio_wref_valid(iow: *mut PgAioWaitRef) -> bool {
    (*iow).aio_index != u32::MAX
}

/*
 * Similar to pgaio_io_get_id(), just for wait references.
 */
pub unsafe fn pgaio_wref_get_id(iow: *mut PgAioWaitRef) -> c_int {
    Assert!(pgaio_wref_valid(iow));
    (*iow).aio_index as c_int
}

/*
 * Wait for the IO to have completed. Can be called in any process, not just
 * in the issuing backend.
 */
pub unsafe fn pgaio_wref_wait(iow: *mut PgAioWaitRef) {
    let mut ref_generation: uint64 = 0;
    let ioh: *mut PgAioHandle;

    ioh = pgaio_io_from_wref(iow, &mut ref_generation);

    pgaio_io_wait(ioh, ref_generation);
}

/*
 * Check if the referenced IO completed, without blocking.
 */
pub unsafe fn pgaio_wref_check_done(iow: *mut PgAioWaitRef) -> bool {
    let mut ref_generation: uint64 = 0;
    let mut state: PgAioHandleState = 0;
    let am_owner: bool;
    let ioh: *mut PgAioHandle;

    ioh = pgaio_io_from_wref(iow, &mut ref_generation);

    if pgaio_io_was_recycled(ioh, ref_generation, &mut state) {
        return true;
    }

    if state == PGAIO_HS_IDLE {
        return true;
    }

    am_owner = (*ioh).owner_procno == MyProcNumber;

    if state == PGAIO_HS_COMPLETED_SHARED || state == PGAIO_HS_COMPLETED_LOCAL {
        /*
         * Note that no interrupts are processed between
         * pgaio_io_was_recycled() and this check - that's important as
         * otherwise an interrupt could have already reclaimed the handle.
         */
        if am_owner {
            pgaio_io_reclaim(ioh);
        }
        return true;
    }

    /*
     * XXX: It likely would be worth checking in with the io method, to give
     * the IO method a chance to check if there are completion events queued.
     */

    false
}

/* --------------------------------------------------------------------------------
 * Actions on multiple IOs.
 * --------------------------------------------------------------------------------
 */

/*
 * Submit IOs in batches going forward.
 *
 * Submitting multiple IOs at once can be substantially faster than doing so
 * one-by-one. At the same time, submitting multiple IOs at once requires more
 * care to avoid deadlocks.
 *
 * Consider backend A staging an IO for buffer 1 and then trying to start IO
 * on buffer 2, while backend B does the inverse. If A submitted the IO before
 * moving on to buffer 2, this works just fine, B will wait for the IO to
 * complete. But if batching were used, each backend will wait for IO that has
 * not yet been submitted to complete, i.e. forever.
 *
 * End batch submission mode with pgaio_exit_batchmode().  (Throwing errors is
 * allowed; error recovery will end the batch.)
 *
 * To avoid deadlocks, code needs to ensure that it will not wait for another
 * backend while there is unsubmitted IO. E.g. by using conditional lock
 * acquisition when acquiring buffer locks. To check if there currently are
 * staged IOs, call pgaio_have_staged() and to submit all staged IOs call
 * pgaio_submit_staged().
 *
 * It is not allowed to enter batchmode while already in batchmode, it's
 * unlikely to ever be needed, as code needs to be explicitly aware of being
 * called in batchmode, to avoid the deadlock risks explained above.
 *
 * Note that IOs may get submitted before pgaio_exit_batchmode() is called,
 * e.g. because too many IOs have been staged or because pgaio_submit_staged()
 * was called.
 */
pub unsafe fn pgaio_enter_batchmode() {
    if (*pgaio_my_backend).in_batchmode {
        elog!(ERROR, "starting batch while batch already in progress");
    }
    (*pgaio_my_backend).in_batchmode = true;
}

/*
 * Stop submitting IOs in batches.
 */
pub unsafe fn pgaio_exit_batchmode() {
    Assert!((*pgaio_my_backend).in_batchmode);

    pgaio_submit_staged();
    (*pgaio_my_backend).in_batchmode = false;
}

/*
 * Are there staged but unsubmitted IOs?
 *
 * See comment above pgaio_enter_batchmode() for why code may need to check if
 * there is IO in that state.
 */
pub unsafe fn pgaio_have_staged() -> bool {
    Assert!((*pgaio_my_backend).in_batchmode || (*pgaio_my_backend).num_staged_ios == 0);
    (*pgaio_my_backend).num_staged_ios > 0
}

/*
 * Submit all staged but not yet submitted IOs.
 *
 * Unless in batch mode, this never needs to be called, as IOs get submitted
 * as soon as possible. While in batchmode pgaio_submit_staged() can be called
 * before waiting on another backend, to avoid the risk of deadlocks. See
 * pgaio_enter_batchmode().
 */
pub unsafe fn pgaio_submit_staged() {
    let mut total_submitted: c_int = 0;
    let did_submit: c_int;

    if (*pgaio_my_backend).num_staged_ios == 0 {
        return;
    }

    START_CRIT_SECTION();

    let submit = (*pgaio_method_ops).submit.expect("IO method missing submit");
    did_submit = submit(
        (*pgaio_my_backend).num_staged_ios,
        (*pgaio_my_backend).staged_ios.as_mut_ptr(),
    );

    END_CRIT_SECTION();

    total_submitted += did_submit;

    Assert!(total_submitted == did_submit);

    (*pgaio_my_backend).num_staged_ios = 0;

    pgaio_debug!(DEBUG4, "aio: submitted {} IOs", total_submitted);
}

/* --------------------------------------------------------------------------------
 * Other
 * --------------------------------------------------------------------------------
 */

/*
 * Perform AIO related cleanup after an error.
 *
 * This should be called early in the error recovery paths, as later steps may
 * need to issue AIO (e.g. to record a transaction abort WAL record).
 */
pub unsafe fn pgaio_error_cleanup() {
    /*
     * It is possible that code errored out after pgaio_enter_batchmode() but
     * before pgaio_exit_batchmode() was called. In that case we need to
     * submit the IO now.
     */
    if (*pgaio_my_backend).in_batchmode {
        (*pgaio_my_backend).in_batchmode = false;

        pgaio_submit_staged();
    }

    /*
     * As we aren't in batchmode, there shouldn't be any unsubmitted IOs.
     */
    Assert!((*pgaio_my_backend).num_staged_ios == 0);
}

/*
 * Perform AIO related checks at (sub-)transactional boundaries.
 *
 * This should be called late during (sub-)transactional commit/abort, after
 * all steps that might need to perform AIO, so that we can verify that the
 * AIO subsystem is in a valid state at the end of a transaction.
 */
pub unsafe fn AtEOXact_Aio(is_commit: bool) {
    let _ = is_commit;

    /*
     * We should never be in batch mode at transactional boundaries. In case
     * an error was thrown while in batch mode, pgaio_error_cleanup() should
     * have exited batchmode.
     *
     * In case we are in batchmode somehow, make sure to submit all staged
     * IOs, other backends may need them to complete to continue.
     */
    if (*pgaio_my_backend).in_batchmode {
        pgaio_error_cleanup();
        elog!(WARNING, "open AIO batch at end of (sub-)transaction");
    }

    /*
     * As we aren't in batchmode, there shouldn't be any unsubmitted IOs.
     */
    Assert!((*pgaio_my_backend).num_staged_ios == 0);
}

/*
 * Need to submit staged but not yet submitted IOs using the fd, otherwise
 * the IO would end up targeting something bogus.
 */
pub unsafe fn pgaio_closing_fd(fd: c_int) {
    /*
     * Might be called before AIO is initialized or in a subprocess that
     * doesn't use AIO.
     */
    if pgaio_my_backend.is_null() {
        return;
    }

    /*
     * For now just submit all staged IOs - we could be more selective, but
     * it's probably not worth it.
     */
    if (*pgaio_my_backend).num_staged_ios > 0 {
        pgaio_debug!(
            DEBUG2,
            "submitting {} IOs before FD {} gets closed",
            (*pgaio_my_backend).num_staged_ios,
            fd
        );
        pgaio_submit_staged();
    }

    /*
     * If requested by the IO method, wait for all IOs that use the
     * to-be-closed FD.
     */
    if (*pgaio_method_ops).wait_on_fd_before_close {
        /*
         * As waiting for one IO to complete may complete multiple IOs, we
         * can't just use a mutable list iterator. The maximum number of
         * in-flight IOs is fairly small, so just restart the loop after
         * waiting for an IO.
         */
        while !dclist_is_empty(&(*pgaio_my_backend).in_flight_ios) {
            let mut iter: dlist_iter = core::mem::zeroed();
            let mut ioh: *mut PgAioHandle = null_mut();
            let mut generation: uint64 = 0;

            dclist_foreach!(iter, &mut (*pgaio_my_backend).in_flight_ios, {
                let cur_ioh: *mut PgAioHandle =
                    dclist_container!(PgAioHandle, node, iter.cur);

                generation = (*cur_ioh).generation;

                if pgaio_io_uses_fd(cur_ioh, fd) {
                    ioh = cur_ioh;
                    break;
                }
            });

            if ioh.is_null() {
                break;
            }

            pgaio_debug_io!(
                DEBUG2,
                ioh,
                "waiting for IO before FD {} gets closed, {} in-flight IOs",
                fd,
                dclist_count(&(*pgaio_my_backend).in_flight_ios)
            );

            /* see comment in pgaio_io_wait_for_free() about raciness */
            pgaio_io_wait(ioh, generation);
        }
    }
}

/*
 * Registered as before_shmem_exit() callback in pgaio_init_backend()
 */
pub unsafe fn pgaio_shutdown(code: c_int, _arg: Datum) {
    Assert!(!pgaio_my_backend.is_null());
    Assert!((*pgaio_my_backend).handed_out_io.is_null());

    /* first clean up resources as we would at a transaction boundary */
    AtEOXact_Aio(code == 0);

    /*
     * Before exiting, make sure that all IOs are finished. That has two main
     * purposes:
     *
     * - Some kernel-level AIO mechanisms don't deal well with the issuer of
     * an AIO exiting before IO completed
     *
     * - It'd be confusing to see partially finished IOs in stats views etc
     */
    while !dclist_is_empty(&(*pgaio_my_backend).in_flight_ios) {
        let ioh: *mut PgAioHandle =
            dclist_head_element!(PgAioHandle, node, &mut (*pgaio_my_backend).in_flight_ios);
        let generation: uint64 = (*ioh).generation;

        pgaio_debug_io!(
            DEBUG2,
            ioh,
            "waiting for IO to complete during shutdown, {} in-flight IOs",
            dclist_count(&(*pgaio_my_backend).in_flight_ios)
        );

        /* see comment in pgaio_io_wait_for_free() about raciness */
        pgaio_io_wait(ioh, generation);
    }

    pgaio_my_backend = null_mut();
}

pub unsafe fn assign_io_method(newval: c_int, _extra: *mut c_void) {
    /* Lookup the vtable for the newly selected IO method. */
    /* io_uring (index 2) is Linux-only; only SYNC and WORKER are available here. */
    let ops: *const IoMethodOps = match newval {
        x if x == IOMETHOD_SYNC => &pgaio_sync_ops as *const IoMethodOps,
        x if x == IOMETHOD_WORKER => &pgaio_worker_ops as *const IoMethodOps,
        _ => {
            elog!(
                PANIC,
                "assign_io_method: unknown io_method {}",
                newval
            );
            unreachable!();
        }
    };

    // pgaio_method_ops is declared extern "C" in aio_internal; write through
    // the mutable alias exposed there.
    // TODO(pg-port): real pgaio_method_ops assignment lives in aio.c;
    //               update once the extern mut pointer is made writable.
    let _ = ops;
}

pub unsafe fn check_io_max_concurrency(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if *newval == -1 {
        /*
         * Auto-tuning will be applied later during startup, as auto-tuning
         * depends on the value of various GUCs.
         */
        return true;
    } else if *newval == 0 {
        GUC_check_errdetail!("Only -1 or values bigger than 0 are valid.");
        return false;
    }

    true
}
