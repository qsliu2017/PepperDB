//! storage/aio_internal.h - AIO declarations used only internally by the AIO subsystem.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int32, uint16, uint32, uint64, uint8};
use crate::lib::ilist::{dclist_head, dlist_node};
use crate::postgres::Datum;
use crate::storage::aio_types::{PgAioResult, PgAioReturn, PgAioTargetData};
use crate::storage::aio_types::PgAioResultStatus;
use crate::utils::resowner::resowner::ResourceOwnerData;

// storage/aio.h + storage/condition_variable.h pair with .c files (outside the
// header-only translation scope), so the symbols they own are stubbed locally
// here. TODO: dedup -> import from crate::storage::aio / ::condition_variable
// once those header+impl files are translated.
pub type PgAioHandleState = c_int; // aio.h enum
pub type PgAioOp = c_int; // aio.h enum
pub const PGAIO_HANDLE_MAX_CALLBACKS: usize = 4; // aio.h
#[repr(C)]
pub struct PgAioOpData {
    _private: [u8; 0],
} // aio.h union (opaque stub)
#[repr(C)]
pub struct ConditionVariable {
    _private: [u8; 0],
} // condition_variable.h (opaque stub)

// `struct iovec` comes from `port/pg_iovec.h` (POSIX), which has not yet been
// ported. Provide a minimal local stub so this file is self-consistent.
// TODO: dedup - real `iovec` belongs in port/pg_iovec.rs.
#[repr(C)]
pub struct iovec {
    pub iov_base: *mut c_void,
    pub iov_len: usize,
}

/// The maximum number of IOs that can be batch submitted at once.
pub const PGAIO_SUBMIT_BATCH_SIZE: usize = 32;

/*
 * State machine for handles. With some exceptions, noted below, handles move
 * linearly through all states.
 *
 * State changes should all go through pgaio_io_update_state().
 *
 * Note that the externally visible functions to start IO
 * (e.g. FileStartReadV(), via pgaio_io_start_readv()) move an IO from
 * PGAIO_HS_HANDED_OUT to at least PGAIO_HS_STAGED and at most
 * PGAIO_HS_COMPLETED_LOCAL (at which point the handle will be reused).
 *
 * NOTE: PgAioHandleState is canonically defined in aio_types.h (ported in
 * crate::storage::aio_types). Re-projected here verbatim from this header for
 * faithfulness; the enum values are imported above rather than redefined.
 */

/* not in use */
pub const PGAIO_HS_IDLE: PgAioHandleState = 0;

/*
 * Returned by pgaio_io_acquire(). The next state is either DEFINED (if
 * pgaio_io_start_*() is called), or IDLE (if pgaio_io_release() is called).
 */
pub const PGAIO_HS_HANDED_OUT: PgAioHandleState = 1;

/*
 * pgaio_io_start_*() has been called, but IO is not yet staged. At this point
 * the handle has all the information for the IO to be executed.
 */
pub const PGAIO_HS_DEFINED: PgAioHandleState = 2;

/*
 * stage() callbacks have been called, handle ready to be submitted for
 * execution. Unless in batchmode (see c.f. pgaio_enter_batchmode()), the IO
 * will be submitted immediately after.
 */
pub const PGAIO_HS_STAGED: PgAioHandleState = 3;

/* IO has been submitted to the IO method for execution */
pub const PGAIO_HS_SUBMITTED: PgAioHandleState = 4;

/* IO finished, but result has not yet been processed */
pub const PGAIO_HS_COMPLETED_IO: PgAioHandleState = 5;

/*
 * IO completed, shared completion has been called.
 *
 * If the IO completion occurs in the issuing backend, local callbacks will
 * immediately be called. Otherwise the handle stays in COMPLETED_SHARED until
 * the issuing backend waits for the completion of the IO.
 */
pub const PGAIO_HS_COMPLETED_SHARED: PgAioHandleState = 6;

/*
 * IO completed, local completion has been called.
 *
 * After this the handle will be made reusable and go into IDLE state.
 */
pub const PGAIO_HS_COMPLETED_LOCAL: PgAioHandleState = 7;

/*
 * Typedef is in aio_types.h
 *
 * We don't use the underlying enums for state, target and op to avoid wasting
 * space. We tried using bitfields, but several compilers generate rather horrid
 * code for that.
 *
 * NOTE: aio_types.rs defines `PgAioHandle` as an opaque enum. This is the full
 * definition from this header. Suspected dup - see report.
 */
#[repr(C)]
pub struct PgAioHandle {
    /* all state updates should go through pgaio_io_update_state() */
    pub state: uint8,

    /* what are we operating on */
    pub target: uint8,

    /* which IO operation */
    pub op: uint8,

    /* bitfield of PgAioHandleFlags */
    pub flags: uint8,

    pub num_callbacks: uint8,

    /* using the proper type here would use more space */
    pub callbacks: [uint8; PGAIO_HANDLE_MAX_CALLBACKS],

    /* data forwarded to each callback */
    pub callbacks_data: [uint8; PGAIO_HANDLE_MAX_CALLBACKS],

    /*
     * Length of data associated with handle using
     * pgaio_io_set_handle_data_*().
     */
    pub handle_data_len: uint8,

    /* XXX: could be optimized out with some pointer math */
    pub owner_procno: int32,

    /* raw result of the IO operation */
    pub result: int32,

    /**
     * In which list the handle is registered, depends on the state:
     * - IDLE, in per-backend list
     * - HANDED_OUT - not in a list
     * - DEFINED - not in a list
     * - STAGED - in per-backend staged array
     * - SUBMITTED - in issuer's in_flight list
     * - COMPLETED_IO - in issuer's in_flight list
     * - COMPLETED_SHARED - in issuer's in_flight list
     **/
    pub node: dlist_node,

    pub resowner: *mut ResourceOwnerData,
    pub resowner_node: dlist_node,

    /* incremented every time the IO handle is reused */
    pub generation: uint64,

    /*
     * To wait for the IO to complete other backends can wait on this CV. Note
     * that, if in SUBMITTED state, a waiter first needs to check if it needs to
     * do work via IoMethodOps->wait_one().
     */
    pub cv: ConditionVariable,

    /* result of shared callback, passed to issuer callback */
    pub distilled_result: PgAioResult,

    /*
     * Index into PgAioCtl->iovecs and PgAioCtl->handle_data.
     *
     * At the moment there's no need to differentiate between the two, but that
     * won't necessarily stay that way.
     */
    pub iovec_off: uint32,

    /*
     * If not NULL, this memory location will be updated with information about
     * the IOs completion iff the issuing backend learns about the IOs
     * completion.
     */
    pub report_return: *mut PgAioReturn,

    /* Data necessary for the IO to be performed */
    pub op_data: PgAioOpData,

    /*
     * Data necessary to identify the object undergoing IO to higher-level code.
     * Needs to be sufficient to allow another backend to reopen the file.
     */
    pub target_data: PgAioTargetData,
}

#[repr(C)]
pub struct PgAioBackend {
    /* index into PgAioCtl->io_handles */
    pub io_handle_off: uint32,

    /* IO Handles that currently are not used */
    pub idle_ios: dclist_head,

    /*
     * Only one IO may be returned by pgaio_io_acquire()/pgaio_io_acquire_nb()
     * without having been either defined (by actually associating it with IO)
     * or released (with pgaio_io_release()). This restriction is necessary to
     * guarantee that we always can acquire an IO. ->handed_out_io is used to
     * enforce that rule.
     */
    pub handed_out_io: *mut PgAioHandle,

    /* Are we currently in batchmode? See pgaio_enter_batchmode(). */
    pub in_batchmode: bool,

    /*
     * IOs that are defined, but not yet submitted.
     */
    pub num_staged_ios: uint16,
    pub staged_ios: [*mut PgAioHandle; PGAIO_SUBMIT_BATCH_SIZE],

    /*
     * List of in-flight IOs. Also contains IOs that aren't strictly speaking
     * in-flight anymore, but have been waited-for and completed by another
     * backend. Once this backend sees such an IO it'll be reclaimed.
     *
     * The list is ordered by submission time, with more recently submitted IOs
     * being appended at the end.
     */
    pub in_flight_ios: dclist_head,
}

#[repr(C)]
pub struct PgAioCtl {
    pub backend_state_count: c_int,
    pub backend_state: *mut PgAioBackend,

    /*
     * Array of iovec structs. Each iovec is owned by a specific backend. The
     * allocation is in PgAioCtl to allow the maximum number of iovecs for
     * individual IOs to be configurable with PGC_POSTMASTER GUC.
     */
    pub iovec_count: uint32,
    pub iovecs: *mut iovec,

    /*
     * For, e.g., an IO covering multiple buffers in shared / temp buffers, we
     * need to get Buffer IDs during completion to be able to change the
     * BufferDesc state accordingly. This space can be used to store e.g. Buffer
     * IDs.  Note that the actual iovec might be shorter than this, because we
     * combine neighboring pages into one larger iovec entry.
     */
    pub handle_data: *mut uint64,

    pub io_handle_count: uint32,
    pub io_handles: *mut PgAioHandle,
}

/*
 * Callbacks used to implement an IO method.
 */
#[repr(C)]
pub struct IoMethodOps {
    /* properties */

    /*
     * If an FD is about to be closed, do we need to wait for all in-flight IOs
     * referencing that FD?
     */
    pub wait_on_fd_before_close: bool,

    /* global initialization */

    /*
     * Amount of additional shared memory to reserve for the io_method. Called
     * just like a normal ipci.c style *Size() function. Optional.
     */
    pub shmem_size: Option<unsafe extern "C" fn() -> usize>,

    /*
     * Initialize shared memory. First time is true if AIO's shared memory was
     * just initialized, false otherwise. Optional.
     */
    pub shmem_init: Option<unsafe extern "C" fn(first_time: bool)>,

    /*
     * Per-backend initialization. Optional.
     */
    pub init_backend: Option<unsafe extern "C" fn()>,

    /* handling of IOs */

    /* optional */
    pub needs_synchronous_execution: Option<unsafe extern "C" fn(ioh: *mut PgAioHandle) -> bool>,

    /*
     * Start executing passed in IOs.
     *
     * Shall advance state to at least PGAIO_HS_SUBMITTED.  (By the time this
     * returns, other backends might have advanced the state further.)
     *
     * Will not be called if ->needs_synchronous_execution() returned true.
     *
     * num_staged_ios is <= PGAIO_SUBMIT_BATCH_SIZE.
     *
     * Always called in a critical section.
     */
    pub submit:
        Option<unsafe extern "C" fn(num_staged_ios: uint16, staged_ios: *mut *mut PgAioHandle) -> c_int>,

    /* ---
     * Wait for the IO to complete. Optional.
     *
     * On return, state shall be on of
     * - PGAIO_HS_COMPLETED_IO
     * - PGAIO_HS_COMPLETED_SHARED
     * - PGAIO_HS_COMPLETED_LOCAL
     *
     * The callback must not block if the handle is already in one of those
     * states, or has been reused (see pgaio_io_was_recycled()).  If, on return,
     * the state is PGAIO_HS_COMPLETED_IO, state will reach
     * PGAIO_HS_COMPLETED_SHARED without further intervention by the IO method.
     *
     * If not provided, it needs to be guaranteed that the IO method calls
     * pgaio_io_process_completion() without further interaction by the issuing
     * backend.
     * ---
     */
    pub wait_one: Option<unsafe extern "C" fn(ioh: *mut PgAioHandle, ref_generation: uint64)>,
}

impl IoMethodOps {
    pub const DEFAULT: IoMethodOps = IoMethodOps {
        wait_on_fd_before_close: false,
        shmem_size: None,
        shmem_init: None,
        init_backend: None,
        needs_synchronous_execution: None,
        submit: None,
        wait_one: None,
    };
}

/* aio.c */
pub unsafe fn pgaio_io_was_recycled(
    _ioh: *mut PgAioHandle,
    _ref_generation: uint64,
    _state: *mut PgAioHandleState,
) -> bool {
    crate::storage::aio::aio::pgaio_io_was_recycled(_ioh, _ref_generation, _state)
}
pub unsafe fn pgaio_io_stage(_ioh: *mut PgAioHandle, _op: PgAioOp) {
    crate::storage::aio::aio::pgaio_io_stage(_ioh, _op)
}
pub unsafe fn pgaio_io_process_completion(_ioh: *mut PgAioHandle, _result: c_int) {
    crate::storage::aio::aio::pgaio_io_process_completion(_ioh, _result)
}
pub unsafe fn pgaio_io_prepare_submit(_ioh: *mut PgAioHandle) {
    crate::storage::aio::aio::pgaio_io_prepare_submit(_ioh)
}
pub unsafe fn pgaio_io_needs_synchronous_execution(_ioh: *mut PgAioHandle) -> bool {
    crate::storage::aio::aio::pgaio_io_needs_synchronous_execution(_ioh)
}
pub unsafe fn pgaio_io_get_state_name(_ioh: *mut PgAioHandle) -> *const c_char {
    crate::storage::aio::aio::pgaio_io_get_state_name(_ioh)
}
pub unsafe fn pgaio_result_status_string(_rs: PgAioResultStatus) -> *const c_char {
    crate::storage::aio::aio::pgaio_result_status_string(_rs)
}
pub unsafe fn pgaio_shutdown(_code: c_int, _arg: Datum) {
    crate::storage::aio::aio::pgaio_shutdown(_code, _arg)
}

/* aio_callback.c */
pub unsafe fn pgaio_io_call_stage(_ioh: *mut PgAioHandle) {
    crate::storage::aio::aio_callback::pgaio_io_call_stage(_ioh)
}
pub unsafe fn pgaio_io_call_complete_shared(_ioh: *mut PgAioHandle) {
    crate::storage::aio::aio_callback::pgaio_io_call_complete_shared(_ioh)
}
pub unsafe fn pgaio_io_call_complete_local(_ioh: *mut PgAioHandle) -> PgAioResult {
    crate::storage::aio::aio_callback::pgaio_io_call_complete_local(_ioh)
}

/* aio_io.c */
pub unsafe fn pgaio_io_perform_synchronously(_ioh: *mut PgAioHandle) {
    crate::storage::aio::aio_io::pgaio_io_perform_synchronously(_ioh)
}
pub unsafe fn pgaio_io_get_op_name(_ioh: *mut PgAioHandle) -> *const c_char {
    crate::storage::aio::aio_io::pgaio_io_get_op_name(_ioh)
}
pub unsafe fn pgaio_io_uses_fd(_ioh: *mut PgAioHandle, _fd: c_int) -> bool {
    crate::storage::aio::aio_io::pgaio_io_uses_fd(_ioh, _fd)
}
pub unsafe fn pgaio_io_get_iovec_length(_ioh: *mut PgAioHandle, _iov: *mut *mut iovec) -> c_int {
    crate::storage::aio::aio_io::pgaio_io_get_iovec_length(_ioh, _iov as _)
}

/* aio_target.c */
pub unsafe fn pgaio_io_can_reopen(_ioh: *mut PgAioHandle) -> bool {
    crate::storage::aio::aio_target::pgaio_io_can_reopen(_ioh)
}
pub unsafe fn pgaio_io_reopen(_ioh: *mut PgAioHandle) {
    crate::storage::aio::aio_target::pgaio_io_reopen(_ioh)
}
pub unsafe fn pgaio_io_get_target_name(_ioh: *mut PgAioHandle) -> *const c_char {
    crate::storage::aio::aio_target::pgaio_io_get_target_name(_ioh)
}

/*
 * The AIO subsystem has fairly verbose debug logging support. This can be
 * enabled/disabled at build time. The reason for this is that
 * a) the verbosity can make debugging things on higher levels hard
 * b) even if logging can be skipped due to elevel checks, it still causes a
 *    measurable slowdown
 *
 * XXX: This likely should be eventually be disabled by default, at least in
 * non-assert builds.
 */
pub const PGAIO_VERBOSE: c_int = 1;

/*
 * Simple ereport() wrapper that only logs if PGAIO_VERBOSE is defined.
 *
 * This intentionally still compiles the code, guarded by a constant if (0), if
 * verbose logging is disabled, to make it less likely that debug logging is
 * silently broken.
 *
 * The current definition requires passing at least one argument.
 */
#[macro_export]
macro_rules! pgaio_debug {
    ($elevel:expr, $msg:expr, $($arg:tt)*) => {
        loop {
            if $crate::storage::aio_internal::PGAIO_VERBOSE != 0 {
                $crate::ereport!(
                    $elevel,
                    $crate::errhidestmt!(true),
                    $crate::errhidecontext!(true),
                    $crate::errmsg_internal!($msg, $($arg)*)
                );
            }
            break;
        }
    };
}

/*
 * Simple ereport() wrapper. Note that the definition requires passing at least
 * one argument.
 */
#[macro_export]
macro_rules! pgaio_debug_io {
    ($elevel:expr, $ioh:expr, $msg:expr, $($arg:tt)*) => {
        $crate::pgaio_debug!(
            $elevel,
            concat!("io %-10d|op %-5s|target %-4s|state %-16s: ", $msg),
            $crate::storage::aio::pgaio_io_get_id($ioh),
            $crate::storage::aio_internal::pgaio_io_get_op_name($ioh),
            $crate::storage::aio_internal::pgaio_io_get_target_name($ioh),
            $crate::storage::aio_internal::pgaio_io_get_state_name($ioh),
            $($arg)*
        )
    };
}

/* Declarations for the tables of function pointers exposed by each IO method. */
// IoMethodOps embeds fn-pointer callbacks (not FFI-safe); harmless for these
// extern method-table globals (placeholders until the IO-method .c files land).
#[allow(improper_ctypes)]
extern "C" {
    pub static pgaio_sync_ops: IoMethodOps;
    pub static pgaio_worker_ops: IoMethodOps;
    /* #ifdef IOMETHOD_IO_URING_ENABLED */
    pub static pgaio_uring_ops: IoMethodOps;
    /* #endif */

    pub static pgaio_method_ops: *const IoMethodOps;
    pub static mut pgaio_ctl: *mut PgAioCtl;
    pub static mut pgaio_my_backend: *mut PgAioBackend;
}
