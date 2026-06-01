//! storage/aio/aio_init.c - AIO subsystem initialization (shmem sizing/init, per-backend init).

use crate::prelude::*;

use crate::lib::ilist::{dclist_init, dclist_push_tail};
use crate::storage::aio_internal::{
    pgaio_ctl, pgaio_method_ops, pgaio_my_backend, pgaio_shutdown, ConditionVariable, IoMethodOps,
    PgAioBackend, PgAioCtl, PgAioHandle, PGAIO_SUBMIT_BATCH_SIZE,
};
use crate::storage::aio_types::PGAIO_RS_UNKNOWN;
use crate::miscadmin::{MaxBackends, MyBackendType, NBuffers, B_IO_WORKER};
use crate::storage::procnumber::MyProcNumber;

use std::ffi::c_char;

/* ---------------------------------------------------------------------------
 * Locally stubbed dependencies (not yet ported).
 * ------------------------------------------------------------------------- */

// storage/shmem.h - allocate (or attach to) a named chunk of shared memory.
// TODO: import from a real shmem.c port once it exists.
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _foundPtr: *mut bool) -> *mut c_void {
    unimplemented!()
}

// storage/shmem.h add_size(): overflow-checked addition of shared sizes.
// TODO: import from a real shmem.c port once it exists.
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}

// storage/shmem.h mul_size(): overflow-checked multiplication of shared sizes.
// TODO: import from a real shmem.c port once it exists.
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

// storage/condition_variable.h ConditionVariableInit().
// TODO: import from a real condition_variable.c port once it exists.
unsafe fn ConditionVariableInit(_cv: *mut ConditionVariable) {
    unimplemented!()
}

// utils/guc.h SetConfigOption().
// TODO: import from a real guc.c port once it exists.
unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: c_int,
) {
    unimplemented!()
}

// storage/ipc.h before_shmem_exit().
// TODO: import from a real ipc.c port once it exists.
unsafe fn before_shmem_exit(_function: pg_on_exit_callback, _arg: Datum) {
    unimplemented!()
}

// miscadmin.h MyProc (PGPROC*). Only ever compared against NULL here.
// TODO: import from a real proc.c port once it exists.
const MyProc: *mut c_void = null_mut();

/* GUC context / source enum values (utils/guc.h). */
const PGC_POSTMASTER: c_int = 0;
const PGC_S_DYNAMIC_DEFAULT: c_int = 0;
const PGC_S_OVERRIDE: c_int = 0;

/* miscadmin.h - number of auxiliary processes. */
const NUM_AUXILIARY_PROCS: c_int = 6;

/* storage/proc.h on-exit callback signature. */
type pg_on_exit_callback = unsafe fn(code: c_int, arg: Datum);

/* GUCs from storage/aio.c (not yet ported). */
// TODO: these are PGC_POSTMASTER GUCs defined in aio.c.
// io_max_concurrency: the real GUC global now lives in aio.rs (its C home is aio.c).
pub use crate::storage::aio::aio::io_max_concurrency;
#[no_mangle]
pub static mut io_max_combine_limit: c_int = 0;

/* ---------------------------------------------------------------------------
 * Shared memory sizing.
 * ------------------------------------------------------------------------- */

unsafe fn AioCtlShmemSize() -> Size {
    /* pgaio_ctl itself */
    core::mem::size_of::<PgAioCtl>()
}

unsafe fn AioProcs() -> uint32 {
    /*
     * While AIO workers don't need their own AIO context, we can't currently
     * guarantee nothing gets assigned to the a ProcNumber for an IO worker if
     * we just subtracted MAX_IO_WORKERS.
     */
    (MaxBackends + NUM_AUXILIARY_PROCS) as uint32
}

unsafe fn AioBackendShmemSize() -> Size {
    mul_size(AioProcs() as Size, core::mem::size_of::<PgAioBackend>())
}

unsafe fn AioHandleShmemSize() -> Size {
    /* verify AioChooseMaxConcurrency() did its thing */
    Assert!(io_max_concurrency > 0);

    /* io handles */
    mul_size(
        AioProcs() as Size,
        mul_size(io_max_concurrency as Size, core::mem::size_of::<PgAioHandle>()),
    )
}

unsafe fn AioHandleIOVShmemSize() -> Size {
    /* each IO handle can have up to io_max_combine_limit iovec objects */
    mul_size(
        core::mem::size_of::<crate::storage::aio_internal::iovec>(),
        mul_size(
            mul_size(io_max_combine_limit as Size, AioProcs() as Size),
            io_max_concurrency as Size,
        ),
    )
}

unsafe fn AioHandleDataShmemSize() -> Size {
    /* each buffer referenced by an iovec can have associated data */
    mul_size(
        core::mem::size_of::<uint64>(),
        mul_size(
            mul_size(io_max_combine_limit as Size, AioProcs() as Size),
            io_max_concurrency as Size,
        ),
    )
}

/*
 * Choose a suitable value for io_max_concurrency.
 *
 * It's unlikely that we could have more IOs in flight than buffers that we
 * would be allowed to pin.
 *
 * On the upper end, apply a cap too - just because shared_buffers is large,
 * it doesn't make sense have millions of buffers undergo IO concurrently.
 */
unsafe fn AioChooseMaxConcurrency() -> c_int {
    let max_backends: uint32;
    let mut max_proportional_pins: c_int;

    /* Similar logic to LimitAdditionalPins() */
    max_backends = (MaxBackends + NUM_AUXILIARY_PROCS) as uint32;
    max_proportional_pins = (NBuffers as uint32 / max_backends) as c_int;

    max_proportional_pins = max_proportional_pins.max(1);

    /* apply upper limit */
    max_proportional_pins.min(64)
}

pub unsafe fn AioShmemSize() -> Size {
    let mut sz: Size = 0;

    /*
     * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
     * However, if the DBA explicitly set io_max_concurrency = -1 in the
     * config file, then PGC_S_DYNAMIC_DEFAULT will fail to override that and
     * we must force the matter with PGC_S_OVERRIDE.
     */
    if io_max_concurrency == -1 {
        let buf = std::ffi::CString::new(format!("{}", AioChooseMaxConcurrency())).unwrap();
        let name = b"io_max_concurrency\0".as_ptr() as *const c_char;
        SetConfigOption(name, buf.as_ptr(), PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
        if io_max_concurrency == -1 {
            /* failed to apply it? */
            SetConfigOption(name, buf.as_ptr(), PGC_POSTMASTER, PGC_S_OVERRIDE);
        }
    }

    sz = add_size(sz, AioCtlShmemSize());
    sz = add_size(sz, AioBackendShmemSize());
    sz = add_size(sz, AioHandleShmemSize());
    sz = add_size(sz, AioHandleIOVShmemSize());
    sz = add_size(sz, AioHandleDataShmemSize());

    /* Reserve space for method specific resources. */
    if let Some(shmem_size) = (*pgaio_method_ops).shmem_size {
        sz = add_size(sz, shmem_size());
    }

    sz
}

pub unsafe fn AioShmemInit() {
    let mut found: bool = false;
    let mut io_handle_off: uint32 = 0;
    let mut iovec_off: uint32 = 0;
    let per_backend_iovecs: uint32 = (io_max_concurrency * io_max_combine_limit) as uint32;

    pgaio_ctl = ShmemInitStruct(
        b"AioCtl\0".as_ptr() as *const c_char,
        AioCtlShmemSize(),
        &mut found,
    ) as *mut PgAioCtl;

    if !found {
        std::ptr::write_bytes(pgaio_ctl as *mut u8, 0, AioCtlShmemSize());

        (*pgaio_ctl).io_handle_count = AioProcs() * io_max_concurrency as uint32;
        (*pgaio_ctl).iovec_count = AioProcs() * per_backend_iovecs;

        (*pgaio_ctl).backend_state = ShmemInitStruct(
            b"AioBackend\0".as_ptr() as *const c_char,
            AioBackendShmemSize(),
            &mut found,
        ) as *mut PgAioBackend;

        (*pgaio_ctl).io_handles = ShmemInitStruct(
            b"AioHandle\0".as_ptr() as *const c_char,
            AioHandleShmemSize(),
            &mut found,
        ) as *mut PgAioHandle;

        (*pgaio_ctl).iovecs = ShmemInitStruct(
            b"AioHandleIOV\0".as_ptr() as *const c_char,
            AioHandleIOVShmemSize(),
            &mut found,
        ) as *mut crate::storage::aio_internal::iovec;
        (*pgaio_ctl).handle_data = ShmemInitStruct(
            b"AioHandleData\0".as_ptr() as *const c_char,
            AioHandleDataShmemSize(),
            &mut found,
        ) as *mut uint64;

        for procno in 0..AioProcs() as c_int {
            let bs: *mut PgAioBackend = (*pgaio_ctl).backend_state.add(procno as usize);

            (*bs).io_handle_off = io_handle_off;
            io_handle_off += io_max_concurrency as uint32;

            dclist_init(&mut (*bs).idle_ios);
            std::ptr::write_bytes(
                (*bs).staged_ios.as_mut_ptr(),
                0,
                PGAIO_SUBMIT_BATCH_SIZE,
            );
            dclist_init(&mut (*bs).in_flight_ios);

            /* initialize per-backend IOs */
            for i in 0..io_max_concurrency {
                let ioh: *mut PgAioHandle = (*pgaio_ctl)
                    .io_handles
                    .add(((*bs).io_handle_off + i as uint32) as usize);

                (*ioh).generation = 1;
                (*ioh).owner_procno = procno as int32;
                (*ioh).iovec_off = iovec_off;
                (*ioh).handle_data_len = 0;
                (*ioh).report_return = null_mut();
                (*ioh).resowner = null_mut();
                (*ioh).num_callbacks = 0;
                (*ioh).distilled_result.set_status(PGAIO_RS_UNKNOWN as u32);
                (*ioh).flags = 0;

                ConditionVariableInit(&mut (*ioh).cv);

                dclist_push_tail(&mut (*bs).idle_ios, &mut (*ioh).node);
                iovec_off += io_max_combine_limit as uint32;
            }
        }
    }

    /* out: Initialize IO method specific resources. */
    if let Some(shmem_init) = (*pgaio_method_ops).shmem_init {
        shmem_init(!found);
    }
}

pub unsafe fn pgaio_init_backend() {
    /* shouldn't be initialized twice */
    Assert!(pgaio_my_backend.is_null());

    if MyBackendType == B_IO_WORKER {
        return;
    }

    if MyProc.is_null() || MyProcNumber >= AioProcs() as i32 {
        elog!(ERROR, "aio requires a normal PGPROC");
    }

    pgaio_my_backend = (*pgaio_ctl).backend_state.add(MyProcNumber as usize);

    if let Some(init_backend) = (*pgaio_method_ops).init_backend {
        init_backend();
    }

    before_shmem_exit(pgaio_shutdown_wrapper, 0);
}

/* before_shmem_exit() takes a `fn(c_int, Datum)`; adapt the ported
 * pgaio_shutdown (declared `unsafe fn`) to that signature. */
fn pgaio_shutdown_wrapper(code: c_int, arg: Datum) {
    unsafe { pgaio_shutdown(code, arg) }
}
