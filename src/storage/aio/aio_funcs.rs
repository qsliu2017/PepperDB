//! storage/aio/aio_funcs.c - AIO - SQL interface for AIO.

use crate::prelude::*;

use crate::access::common::tupdesc::TupleDesc;
use crate::nodes::execnodes::{ReturnSetInfo, Tuplestorestate};
use crate::storage::aio_internal::{
    iovec, pgaio_ctl, pgaio_io_get_op_name, pgaio_io_get_state_name, pgaio_io_get_target_name,
    pgaio_result_status_string, PgAioHandle, PgAioHandleState, PgAioOp, PGAIO_HS_COMPLETED_IO,
    PGAIO_HS_COMPLETED_LOCAL, PGAIO_HS_COMPLETED_SHARED, PGAIO_HS_HANDED_OUT, PGAIO_HS_IDLE,
};
use crate::storage::aio_types::PgAioResultStatus;
use crate::storage::procnumber::ProcNumber;
use crate::utils::builtins::CStringGetTextDatum;
use crate::utils::fmgr::FunctionCallInfo;

/*
 * struct iovec layout is identical to the local stub `iovec` (a base pointer +
 * length). PG_IOV_MAX = Min(IOV_MAX, 128). IOV_MAX is platform-dependent; 128
 * matches the upstream cap and the reserved space in PgAioCtl->iovecs.
 */
const PG_IOV_MAX: usize = 128;

/* Flag bits (storage/aio.h PgAioHandleFlags). */
const PGAIO_HF_REFERENCES_LOCAL: uint8 = 1 << 1;
const PGAIO_HF_SYNCHRONOUS: uint8 = 1 << 0;
const PGAIO_HF_BUFFERED: uint8 = 1 << 2;

/* PgAioOp values (storage/aio.h). */
const PGAIO_OP_INVALID: PgAioOp = 0;
const PGAIO_OP_READV: PgAioOp = 1;
const PGAIO_OP_WRITEV: PgAioOp = 2;

/*
 * Real layout of PgAioOpData (storage/aio.h), reprojected here because the
 * ported `crate::storage::aio_internal::PgAioOpData` is an opaque stub. The
 * `op_data` field of PgAioHandle is reinterpreted through this view to read the
 * offset / iov_length for the read and write operations.
 *
 * TODO: dedup once aio_internal::PgAioOpData carries the full union.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct PgAioOpDataReadWrite {
    fd: c_int,
    iov_length: uint16,
    offset: uint64,
}

/*
 * Byte length of an iovec.
 */
unsafe fn iov_byte_length(iov: *const iovec, cnt: c_int) -> Size {
    let mut len: Size = 0;

    for i in 0..cnt {
        len += (*iov.offset(i as isize)).iov_len;
    }

    len
}

const PG_GET_AIOS_COLS: usize = 15;

pub unsafe fn pg_get_aios(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    InitMaterializedSRF(fcinfo, 0);

    let mut i: uint64 = 0;
    while i < (*pgaio_ctl).io_handle_count as uint64 {
        let mut live_ioh: *mut PgAioHandle = (*pgaio_ctl).io_handles.offset(i as isize);
        let ioh_id: c_int = pgaio_io_get_id(live_ioh);
        let mut values: [Datum; PG_GET_AIOS_COLS] = [0; PG_GET_AIOS_COLS];
        let mut nulls: [bool; PG_GET_AIOS_COLS] = [false; PG_GET_AIOS_COLS];
        let mut owner: ProcNumber;
        let mut owner_proc: *mut PGPROC;
        let mut owner_pid: int32;
        let mut start_state: PgAioHandleState;
        let start_generation: uint64;
        let mut ioh_copy: PgAioHandle = core::mem::zeroed();
        let mut iov_copy: [iovec; PG_IOV_MAX] = core::mem::zeroed();

        /*
         * There is no lock that could prevent the state of the IO to advance
         * concurrently - and we don't want to introduce one, as that would
         * introduce atomics into a very common path. Instead we
         *
         * 1) Determine the state + generation of the IO.
         *
         * 2) Copy the IO to local memory.
         *
         * 3) Check if state or generation of the IO changed. If the state
         * changed, retry, if the generation changed don't display the IO.
         */

        /* 1) from above */
        start_generation = (*live_ioh).generation;

        /*
         * Retry at this point, so we can accept changing states, but not
         * changing generations.
         */
        'retry: loop {
            pg_read_barrier();
            start_state = (*live_ioh).state as PgAioHandleState;

            if start_state == PGAIO_HS_IDLE {
                break 'retry;
            }

            /* 2) from above */
            core::ptr::copy_nonoverlapping(
                live_ioh as *const PgAioHandle,
                &mut ioh_copy as *mut PgAioHandle,
                1,
            );

            /*
             * Safe to copy even if no iovec is used - we always reserve the
             * required space.
             */
            core::ptr::copy_nonoverlapping(
                (*pgaio_ctl).iovecs.offset(ioh_copy.iovec_off as isize) as *const iovec,
                iov_copy.as_mut_ptr(),
                PG_IOV_MAX,
            );

            /*
             * Copy information about owner before 3) below, if the process
             * exited it'd have to wait for the IO to finish first, which we
             * would detect in 3).
             */
            owner = ioh_copy.owner_procno;
            owner_proc = GetPGProcByNumber(owner);
            owner_pid = (*owner_proc).pid;

            /* 3) from above */
            pg_read_barrier();

            /*
             * The IO completed and a new one was started with the same ID.
             * Don't display it - it really started after this function was
             * called. There be a risk of a livelock if we just retried
             * endlessly, if IOs complete very quickly.
             */
            if (*live_ioh).generation != start_generation {
                break 'retry;
            }

            /*
             * The IO's state changed while we were "rendering" it. Just start
             * from scratch. There's no risk of a livelock here, as an IO has a
             * limited sets of states it can be in, and state changes go only in
             * a single direction.
             */
            if (*live_ioh).state as PgAioHandleState != start_state {
                continue 'retry;
            }

            /*
             * Now that we have copied the IO into local memory and checked that
             * it's still in the same state, we are not allowed to access "live"
             * memory anymore. To make it slightly easier to catch such cases,
             * set the "live" pointers to NULL.
             */
            live_ioh = null_mut();
            owner_proc = null_mut();

            /* column: owning pid */
            if owner_pid != 0 {
                values[0] = Int32GetDatum(owner_pid);
            } else {
                nulls[0] = false;
            }

            /* column: IO's id */
            values[1] = Int32GetDatum(ioh_id);

            /* column: IO's generation */
            values[2] = Int64GetDatum(start_generation as int64);

            /* column: IO's state */
            values[3] = CStringGetTextDatum(pgaio_io_get_state_name(&mut ioh_copy));

            /*
             * If the IO is in PGAIO_HS_HANDED_OUT state, none of the following
             * fields are valid yet (or are in the process of being set).
             * Therefore we don't want to display any other columns.
             */
            if start_state == PGAIO_HS_HANDED_OUT {
                for n in nulls.iter_mut().skip(4) {
                    *n = true;
                }
                break 'retry;
            }

            /* column: IO's operation */
            values[4] = CStringGetTextDatum(pgaio_io_get_op_name(&mut ioh_copy));

            /* columns: details about the IO's operation (offset, length) */
            let op_data: *const PgAioOpDataReadWrite =
                &ioh_copy.op_data as *const _ as *const PgAioOpDataReadWrite;
            match ioh_copy.op as PgAioOp {
                PGAIO_OP_INVALID => {
                    nulls[5] = true;
                    nulls[6] = true;
                }
                PGAIO_OP_READV => {
                    values[5] = Int64GetDatum((*op_data).offset as int64);
                    values[6] = Int64GetDatum(
                        iov_byte_length(iov_copy.as_ptr(), (*op_data).iov_length as c_int) as int64,
                    );
                }
                PGAIO_OP_WRITEV => {
                    values[5] = Int64GetDatum((*op_data).offset as int64);
                    values[6] = Int64GetDatum(
                        iov_byte_length(iov_copy.as_ptr(), (*op_data).iov_length as c_int) as int64,
                    );
                }
                _ => {}
            }

            /* column: IO's target */
            values[7] = CStringGetTextDatum(pgaio_io_get_target_name(&mut ioh_copy));

            /* column: length of IO's data array */
            values[8] = Int16GetDatum(ioh_copy.handle_data_len as int16);

            /* column: raw result (i.e. some form of syscall return value) */
            if start_state == PGAIO_HS_COMPLETED_IO
                || start_state == PGAIO_HS_COMPLETED_SHARED
                || start_state == PGAIO_HS_COMPLETED_LOCAL
            {
                values[9] = Int32GetDatum(ioh_copy.result);
            } else {
                nulls[9] = true;
            }

            /*
             * column: result in the higher level representation (unknown if not
             * finished)
             */
            values[10] = CStringGetTextDatum(pgaio_result_status_string(
                ioh_copy.distilled_result.status() as PgAioResultStatus,
            ));

            /* column: target description */
            values[11] = CStringGetTextDatum(pgaio_io_get_target_description(&mut ioh_copy));

            /* columns: one for each flag */
            values[12] = BoolGetDatum((ioh_copy.flags & PGAIO_HF_SYNCHRONOUS) != 0);
            values[13] = BoolGetDatum((ioh_copy.flags & PGAIO_HF_REFERENCES_LOCAL) != 0);
            values[14] = BoolGetDatum((ioh_copy.flags & PGAIO_HF_BUFFERED) != 0);

            break 'retry;
        }

        /*
         * `display:` label fall-through. An IDLE handle or one whose generation
         * changed skips emitting a row (mirrors `continue` in the C loop).
         */
        if start_state == PGAIO_HS_IDLE {
            i += 1;
            continue;
        }
        if !live_ioh.is_null() && (*live_ioh).generation != start_generation {
            i += 1;
            continue;
        }

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_ptr(),
            nulls.as_ptr(),
        );

        i += 1;
    }

    0 as Datum
}

/*
 * Local stubs for not-yet-ported callees. Imported from their real homes once
 * those .c files land.
 */

// PGPROC carries `pid`; the ported storage/proclist.rs PGPROC is an opaque
// stub without fields, so a minimal field-bearing stub is used here.
// TODO: dedup with storage/proc.rs once it lands.
#[repr(C)]
struct PGPROC {
    pid: int32,
}

// TODO: port GetPGProcByNumber (storage/proc.h) - &ProcGlobal->allProcs[n].
unsafe fn GetPGProcByNumber(_n: ProcNumber) -> *mut PGPROC {
    unimplemented!()
}

// TODO: port pgaio_io_get_id (src/backend/storage/aio/aio.c).
unsafe fn pgaio_io_get_id(_ioh: *mut PgAioHandle) -> c_int {
    unimplemented!()
}

// TODO: port pgaio_io_get_target_description (src/backend/storage/aio/aio_target.c).
unsafe fn pgaio_io_get_target_description(_ioh: *mut PgAioHandle) -> *const c_char {
    unimplemented!()
}

// TODO: port InitMaterializedSRF (src/backend/utils/fmgr/funcapi.c).
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!()
}

// TODO: port tuplestore_putvalues (src/backend/utils/sort/tuplestore.c).
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *const Datum,
    _isnull: *const bool,
) {
    unimplemented!()
}

// pg_read_barrier - storage/proc and atomics expose pg_read_barrier_impl per
// platform; the public pg_read_barrier macro is not yet ported. Acts as a
// compiler fence here.
// TODO: dedup with the real pg_read_barrier once port/atomics.h lands.
#[inline]
unsafe fn pg_read_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Acquire);
}
