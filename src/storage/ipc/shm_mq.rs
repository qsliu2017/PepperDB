/*-------------------------------------------------------------------------
 *
 * shm_mq.rs
 *   single-reader, single-writer shared memory message queue
 *
 * Both the sender and the receiver must have a PGPROC; their respective
 * process latches are used for synchronization.  Only the sender may send,
 * and only the receiver may receive.  This is intended to allow a user
 * backend to communicate with worker backends that it has registered.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/ipc/shm_mq.c -> src/storage/ipc/shm_mq.rs
 * Merged header: src/include/storage/shm_mq.h
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::c_void;

use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::pg_config::MAXIMUM_ALIGNOF;
use crate::port::pg_bitutils::pg_nextpower2_size_t;
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};

// ---------------------------------------------------------------------------
// Stubs for symbols whose home is not yet translated
// ---------------------------------------------------------------------------

/// PGPROC - the real definition lives in storage/proc.h (not yet ported).
/// We model only the `procLatch` field used by this module.
// TODO(pg-port): real PGPROC lives in src/include/storage/proc.h
#[repr(C)]
pub struct PGPROC {
    pub procLatch: Latch,
}

/// Latch - real definition is in storage/ipc/latch.h.
// TODO(pg-port): real Latch lives in src/include/storage/latch.h
#[repr(C)]
pub struct Latch {
    pub is_set: c_int,
    pub maybe_sleeping: c_int,
    pub is_shared: bool,
    pub owner_pid: c_int,
}

/// MyProc - the backend's own PGPROC pointer.
// TODO(pg-port): real MyProc lives in storage/proc.c (globals.c)
#[allow(non_upper_case_globals)]
extern "C" { pub static mut MyProc: *mut PGPROC; }
/// MyLatch - pointer to the backend's own process latch.
// TODO(pg-port): real MyLatch lives in storage/ipc/latch.c / globals.c
#[allow(non_upper_case_globals)]
static mut MyLatch: *mut Latch = core::ptr::null_mut();

/// SetLatch - wake a latch.
// TODO(pg-port): real SetLatch lives in storage/ipc/latch.c
#[inline]
unsafe fn SetLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::SetLatch(_latch as _)
}

/// ResetLatch - reset a latch after waking.
// TODO(pg-port): real ResetLatch lives in storage/ipc/latch.c
#[inline]
unsafe fn ResetLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::ResetLatch(_latch as _)
}

/// WaitLatch - block until a latch is set or a timeout occurs.
// TODO(pg-port): real WaitLatch lives in storage/ipc/latch.c
#[inline]
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout: i64,
    _wait_event_info: uint32,
) -> c_int {
    unimplemented!() // TODO(pg-port): real WaitLatch lives in storage/ipc/latch.c
}

// WaitLatch wakeEvent bitmasks (storage/latch.h)
const WL_LATCH_SET: c_int = 1 << 0;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// WaitEventIPC codes used for shm_mq waits (utils/wait_event.h)
// TODO(pg-port): real wait-event codes generated from wait_event_names.txt
const WAIT_EVENT_MESSAGE_QUEUE_SEND: uint32 = 0;
const WAIT_EVENT_MESSAGE_QUEUE_RECEIVE: uint32 = 0;
const WAIT_EVENT_MESSAGE_QUEUE_INTERNAL: uint32 = 0;

/// dsm_segment - opaque handle for a dynamic shared-memory segment.
// TODO(pg-port): real dsm_segment lives in storage/ipc/dsm.c
pub type dsm_segment = c_void;

/// Datum used as an argument to the on_dsm_detach callback.
// Datum is already in the prelude (from crate::postgres).

/// Register a callback to run when a DSM segment is detached.
// TODO(pg-port): real on_dsm_detach lives in storage/ipc/dsm.c
#[inline]
unsafe fn on_dsm_detach(
    _seg: *mut dsm_segment,
    _cb: unsafe fn(*mut dsm_segment, Datum),
    _arg: Datum,
) {
    unimplemented!() // TODO(pg-port): real on_dsm_detach lives in storage/ipc/dsm.c
}

/// Cancel a previously registered DSM detach callback.
// TODO(pg-port): real cancel_on_dsm_detach lives in storage/ipc/dsm.c
#[inline]
unsafe fn cancel_on_dsm_detach(
    _seg: *mut dsm_segment,
    _cb: unsafe fn(*mut dsm_segment, Datum),
    _arg: Datum,
) {
    unimplemented!() // TODO(pg-port): real cancel_on_dsm_detach lives in storage/ipc/dsm.c
}

/// BackgroundWorkerHandle - handle for a registered background worker.
// TODO(pg-port): real BackgroundWorkerHandle lives in postmaster/bgworker.h
pub type BackgroundWorkerHandle = c_void;

/// Status codes returned by GetBackgroundWorkerPid.
// TODO(pg-port): real BgwHandleStatus lives in postmaster/bgworker.h
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum BgwHandleStatus {
    BGWH_STARTED,
    BGWH_NOT_YET_STARTED,
    BGWH_STOPPED,
    BGWH_POSTMASTER_DIED,
}
use BgwHandleStatus::*;

/// Get the PID of a background worker (or the status if not running).
// TODO(pg-port): real GetBackgroundWorkerPid lives in postmaster/bgworker.c
#[inline]
pub unsafe fn GetBackgroundWorkerPid(
    _handle: *mut BackgroundWorkerHandle,
    _pidp: *mut pid_t,
) -> BgwHandleStatus {
    unimplemented!() // TODO(pg-port): real GetBackgroundWorkerPid lives in postmaster/bgworker.c
}

/// pid_t - process-ID type (miscadmin.h re-exports it, but pulled in locally
/// here to avoid an additional use statement).
use crate::miscadmin::pid_t;

// ERRCODE_PROGRAM_LIMIT_EXCEEDED from utils/errcodes.h
// TODO(pg-port): real value generated from errcodes.txt
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

// ---------------------------------------------------------------------------
// shm_mq.h public types (merged from the companion header)
// ---------------------------------------------------------------------------

/// Descriptor for a single write spanning one contiguous buffer region.
/// (`shm_mq_iovec` from shm_mq.h)
#[repr(C)]
pub struct shm_mq_iovec {
    pub data: *const c_char,
    pub len: Size,
}

/// Possible results of a send or receive operation.
/// (`shm_mq_result` from shm_mq.h)
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum shm_mq_result {
    /// Sent or received a message.
    SHM_MQ_SUCCESS,
    /// Not completed; retry later.
    SHM_MQ_WOULD_BLOCK,
    /// Other process has detached queue.
    SHM_MQ_DETACHED,
}
use shm_mq_result::*;

// ---------------------------------------------------------------------------
// Internal constants
// ---------------------------------------------------------------------------

const MQH_INITIAL_BUFSIZE: Size = 8192;

// ---------------------------------------------------------------------------
// shm_mq - the actual queue, stored in shared memory
//
// Synchronization notes (from the C source):
//
//   mq_receiver and mq_bytes_read can only be changed by the receiver; and
//   mq_sender and mq_bytes_written can only be changed by the sender.
//   mq_receiver and mq_sender are protected by mq_mutex, although they cannot
//   change once set, and thus may be read without a lock once this is known.
//
//   mq_bytes_read and mq_bytes_written are not protected by the mutex; they
//   are written atomically using 8-byte loads and stores.  Memory barriers
//   must be carefully used to synchronize reads and writes of these values
//   with reads and writes of the actual data in mq_ring.
//
//   mq_detached needs no locking.  It can be set by either sender or receiver,
//   but only ever from false to true, so redundant writes don't matter.
//
//   mq_ring_size and mq_ring_offset never change after initialization, and
//   can therefore be read without the lock.
//
//   mq_ring can be safely read and written without a lock.
// ---------------------------------------------------------------------------

/// The shared-memory queue structure.  (`struct shm_mq` from shm_mq.c)
#[repr(C)]
pub struct shm_mq {
    pub mq_mutex: slock_t,
    pub mq_receiver: *mut PGPROC,
    pub mq_sender: *mut PGPROC,
    pub mq_bytes_read: pg_atomic_uint64,
    pub mq_bytes_written: pg_atomic_uint64,
    pub mq_ring_size: Size,
    pub mq_detached: bool,
    pub mq_ring_offset: uint8,
    /// Ring buffer data (FLEXIBLE_ARRAY_MEMBER).
    pub mq_ring: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

// ---------------------------------------------------------------------------
// shm_mq_handle - backend-private handle for a queue
//
// Notes on the fields (from the C source):
//
//   mqh_queue     - pointer to the shared queue.
//   mqh_segment   - optional DSM segment; if set we register an on_dsm_detach
//                   callback.
//   mqh_handle    - optional background-worker handle; allows us to detect
//                   worker death before the worker attaches.
//   mqh_buffer    - reassembly buffer for wrapped / large messages.
//   mqh_buflen    - bytes allocated for mqh_buffer.
//   mqh_consume_pending - bytes consumed but not yet reported to shared mem.
//   mqh_send_pending    - bytes written but not yet reported to shared mem.
//   mqh_partial_bytes   - bytes of the current length-word or payload already
//                         sent/received.
//   mqh_expected_bytes  - expected total payload size (receive side only).
//   mqh_length_word_complete - whether the length word has been fully processed.
//   mqh_counterparty_attached - cached knowledge that the other side attached.
//   mqh_context   - memory context in effect when we attached.
// ---------------------------------------------------------------------------

/// Backend-private handle for a shared message queue.
/// (`struct shm_mq_handle` from shm_mq.c)
#[repr(C)]
pub struct shm_mq_handle {
    pub mqh_queue: *mut shm_mq,
    pub mqh_segment: *mut dsm_segment,
    pub mqh_handle: *mut BackgroundWorkerHandle,
    pub mqh_buffer: *mut c_char,
    pub mqh_buflen: Size,
    pub mqh_consume_pending: Size,
    pub mqh_send_pending: Size,
    pub mqh_partial_bytes: Size,
    pub mqh_expected_bytes: Size,
    pub mqh_length_word_complete: bool,
    pub mqh_counterparty_attached: bool,
    pub mqh_context: MemoryContext,
}

// ---------------------------------------------------------------------------
// Atomic helpers (re-export wrappers matching the C inline functions)
// ---------------------------------------------------------------------------

use crate::port::atomics::{
    pg_atomic_uint64,
    pg_atomic_init_u64_impl_native as pg_atomic_init_u64_native,
    pg_atomic_read_u64_impl_native as pg_atomic_read_u64_native,
};
use crate::port::atomics::generic::pg_memory_barrier_impl;
use core::sync::atomic::{compiler_fence, fence, Ordering};

/// Thin wrappers matching the C `pg_atomic_*_u64` call sites.
/// These take raw pointers (as the C code does).

#[inline]
unsafe fn pg_atomic_init_u64_raw(ptr: *mut pg_atomic_uint64, val: u64) {
    pg_atomic_init_u64_native(&*ptr, val);
}

#[inline]
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> u64 {
    pg_atomic_read_u64_native(&*ptr)
}

/// Write a u64 atomically - non-RMW; only the owner side calls this.
/// Uses a SeqCst store via the underlying AtomicU64.
#[inline]
unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    (*ptr).value.store(val, Ordering::SeqCst);
}

/// `pg_compiler_barrier()` - prevent the compiler from reordering reads/writes
/// across this point.
#[inline]
fn pg_compiler_barrier() {
    compiler_fence(Ordering::SeqCst);
}

/// `pg_memory_barrier()` - full hardware + compiler barrier.
#[inline]
fn pg_memory_barrier() {
    pg_memory_barrier_impl();
}

/// `pg_read_barrier()` - acquire barrier.
#[inline]
fn pg_read_barrier() {
    fence(Ordering::Acquire);
}

/// `pg_write_barrier()` - release barrier.
#[inline]
fn pg_write_barrier() {
    fence(Ordering::Release);
}

// ---------------------------------------------------------------------------
// Minimum queue size constant (shm_mq.h)
// ---------------------------------------------------------------------------

/// `shm_mq_minimum_size` - enough space for the header plus at least one
/// MAXALIGN chunk of data.
pub static shm_mq_minimum_size: Size = {
    // MAXALIGN(offsetof(shm_mq, mq_ring)) + MAXIMUM_ALIGNOF
    // We compute the offset of `mq_ring` at run time rather than const time
    // because MAXALIGN requires runtime evaluation in general; however
    // FLEXIBLE_ARRAY_MEMBER gives us offset_of! here.
    // The value is set lazily via the accessor below; the C extern is a const,
    // so we mirror it as a static initialised in a dedicated function.
    //
    // Simplification: compute with MAXALIGN of the known header size.
    // offsetof(shm_mq, mq_ring) = offset_of!(shm_mq, mq_ring)
    // In C: MAXALIGN(offsetof) is the same as the struct size because the
    // compiler already rounds up.  Use const fn MAXALIGN from crate::c.
    MAXALIGN(core::mem::offset_of!(shm_mq, mq_ring)) + MAXIMUM_ALIGNOF
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/*
 * Initialize a new shared message queue.
 */
pub unsafe fn shm_mq_create(address: *mut c_void, size: Size) -> *mut shm_mq {
    let mq: *mut shm_mq = address as *mut shm_mq;
    let data_offset: Size = MAXALIGN(core::mem::offset_of!(shm_mq, mq_ring));

    /* If the size isn't MAXALIGN'd, just discard the odd bytes. */
    let size = MAXALIGN_DOWN(size);

    /* Queue size must be large enough to hold some data. */
    Assert!(size > data_offset);

    /* Initialize queue header. */
    SpinLockInit(&mut (*mq).mq_mutex);
    (*mq).mq_receiver = core::ptr::null_mut();
    (*mq).mq_sender = core::ptr::null_mut();
    pg_atomic_init_u64_raw(&mut (*mq).mq_bytes_read, 0);
    pg_atomic_init_u64_raw(&mut (*mq).mq_bytes_written, 0);
    (*mq).mq_ring_size = size - data_offset;
    (*mq).mq_detached = false;
    (*mq).mq_ring_offset =
        (data_offset - core::mem::offset_of!(shm_mq, mq_ring)) as uint8;

    mq
}

/*
 * Set the identity of the process that will receive from a shared message
 * queue.
 */
pub unsafe fn shm_mq_set_receiver(mq: *mut shm_mq, proc_: *mut PGPROC) {
    let sender: *mut PGPROC;

    SpinLockAcquire(&mut (*mq).mq_mutex);
    Assert!((*mq).mq_receiver.is_null());
    (*mq).mq_receiver = proc_;
    sender = (*mq).mq_sender;
    SpinLockRelease(&mut (*mq).mq_mutex);

    if !sender.is_null() {
        SetLatch(&mut (*sender).procLatch);
    }
}

/*
 * Set the identity of the process that will send to a shared message queue.
 */
pub unsafe fn shm_mq_set_sender(mq: *mut shm_mq, proc_: *mut PGPROC) {
    let receiver: *mut PGPROC;

    SpinLockAcquire(&mut (*mq).mq_mutex);
    Assert!((*mq).mq_sender.is_null());
    (*mq).mq_sender = proc_;
    receiver = (*mq).mq_receiver;
    SpinLockRelease(&mut (*mq).mq_mutex);

    if !receiver.is_null() {
        SetLatch(&mut (*receiver).procLatch);
    }
}

/*
 * Get the configured receiver.
 */
pub unsafe fn shm_mq_get_receiver(mq: *mut shm_mq) -> *mut PGPROC {
    let receiver: *mut PGPROC;

    SpinLockAcquire(&mut (*mq).mq_mutex);
    receiver = (*mq).mq_receiver;
    SpinLockRelease(&mut (*mq).mq_mutex);

    receiver
}

/*
 * Get the configured sender.
 */
pub unsafe fn shm_mq_get_sender(mq: *mut shm_mq) -> *mut PGPROC {
    let sender: *mut PGPROC;

    SpinLockAcquire(&mut (*mq).mq_mutex);
    sender = (*mq).mq_sender;
    SpinLockRelease(&mut (*mq).mq_mutex);

    sender
}

/*
 * Attach to a shared message queue so we can send or receive messages.
 *
 * The memory context in effect at the time this function is called should
 * be one which will last for at least as long as the message queue itself.
 * We'll allocate the handle in that context, and future allocations that
 * are needed to buffer incoming data will happen in that context as well.
 *
 * If seg != NULL, the queue will be automatically detached when that dynamic
 * shared memory segment is detached.
 *
 * If handle != NULL, the queue can be read or written even before the
 * other process has attached.  We'll wait for it to do so if needed.
 *
 * shm_mq_detach() should be called when done.
 */
pub unsafe fn shm_mq_attach(
    mq: *mut shm_mq,
    seg: *mut dsm_segment,
    handle: *mut BackgroundWorkerHandle,
) -> *mut shm_mq_handle {
    let mqh = palloc(core::mem::size_of::<shm_mq_handle>()) as *mut shm_mq_handle;

    Assert!((*mq).mq_receiver == MyProc || (*mq).mq_sender == MyProc);
    (*mqh).mqh_queue = mq;
    (*mqh).mqh_segment = seg;
    (*mqh).mqh_handle = handle;
    (*mqh).mqh_buffer = core::ptr::null_mut();
    (*mqh).mqh_buflen = 0;
    (*mqh).mqh_consume_pending = 0;
    (*mqh).mqh_send_pending = 0;
    (*mqh).mqh_partial_bytes = 0;
    (*mqh).mqh_expected_bytes = 0;
    (*mqh).mqh_length_word_complete = false;
    (*mqh).mqh_counterparty_attached = false;
    (*mqh).mqh_context = CurrentMemoryContext;

    if !seg.is_null() {
        on_dsm_detach(seg, shm_mq_detach_callback, PointerGetDatum(mq as *mut c_void));
    }

    mqh
}

/*
 * Associate a BackgroundWorkerHandle with a shm_mq_handle just as if it had
 * been passed to shm_mq_attach.
 */
pub unsafe fn shm_mq_set_handle(
    mqh: *mut shm_mq_handle,
    handle: *mut BackgroundWorkerHandle,
) {
    Assert!((*mqh).mqh_handle.is_null());
    (*mqh).mqh_handle = handle;
}

/*
 * Write a message into a shared message queue.
 */
pub unsafe fn shm_mq_send(
    mqh: *mut shm_mq_handle,
    nbytes: Size,
    data: *const c_void,
    nowait: bool,
    force_flush: bool,
) -> shm_mq_result {
    let mut iov = shm_mq_iovec {
        data: data as *const c_char,
        len: nbytes,
    };

    shm_mq_sendv(mqh, &mut iov, 1, nowait, force_flush)
}

/*
 * Write a message into a shared message queue, gathered from multiple
 * addresses.
 *
 * When nowait = false, we'll wait on our process latch when the ring buffer
 * fills up, and then continue writing once the receiver has drained some data.
 * The process latch is reset after each wait.
 *
 * When nowait = true, we do not manipulate the state of the process latch;
 * instead, if the buffer becomes full, we return SHM_MQ_WOULD_BLOCK.  In
 * this case, the caller should call this function again, with the same
 * arguments, each time the process latch is set.  (Once begun, the sending
 * of a message cannot be aborted except by detaching from the queue; changing
 * the length or payload will corrupt the queue.)
 *
 * When force_flush = true, we immediately update the shm_mq's mq_bytes_written
 * and notify the receiver (if it is already attached).  Otherwise, we don't
 * update it until we have written an amount of data greater than 1/4th of the
 * ring size.
 */
pub unsafe fn shm_mq_sendv(
    mqh: *mut shm_mq_handle,
    iov: *mut shm_mq_iovec,
    iovcnt: c_int,
    nowait: bool,
    force_flush: bool,
) -> shm_mq_result {
    let mut res: shm_mq_result;
    let mq: *mut shm_mq = (*mqh).mqh_queue;
    let receiver: *mut PGPROC;
    let mut nbytes: Size = 0;
    let mut bytes_written: Size = 0;
    let mut which_iov: c_int = 0;
    let mut offset: Size;

    Assert!((*mq).mq_sender == MyProc);

    /* Compute total size of write. */
    for i in 0..iovcnt {
        nbytes += (*iov.offset(i as isize)).len;
    }

    /* Prevent writing messages overwhelming the receiver. */
    if nbytes > MaxAllocSize {
        // C also attaches errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED).
        ereport!(
            ERROR,
            errmsg!(
                "cannot send a message of size {} via shared memory queue",
                nbytes
            )
        );
    }

    /* Try to write, or finish writing, the length word into the buffer. */
    while !(*mqh).mqh_length_word_complete {
        Assert!((*mqh).mqh_partial_bytes < core::mem::size_of::<Size>());
        res = shm_mq_send_bytes(
            mqh,
            core::mem::size_of::<Size>() - (*mqh).mqh_partial_bytes,
            ((&raw const nbytes as *const c_char)
                .add((*mqh).mqh_partial_bytes)) as *const c_void,
            nowait,
            &mut bytes_written,
        );

        if res == SHM_MQ_DETACHED {
            /* Reset state in case caller tries to send another message. */
            (*mqh).mqh_partial_bytes = 0;
            (*mqh).mqh_length_word_complete = false;
            return res;
        }
        (*mqh).mqh_partial_bytes += bytes_written;

        if (*mqh).mqh_partial_bytes >= core::mem::size_of::<Size>() {
            Assert!((*mqh).mqh_partial_bytes == core::mem::size_of::<Size>());
            (*mqh).mqh_partial_bytes = 0;
            (*mqh).mqh_length_word_complete = true;
        }

        if res != SHM_MQ_SUCCESS {
            return res;
        }

        /* Length word can't be split unless bigger than required alignment. */
        Assert!((*mqh).mqh_length_word_complete || core::mem::size_of::<Size>() > MAXIMUM_ALIGNOF);
    }

    /* Write the actual data bytes into the buffer. */
    Assert!((*mqh).mqh_partial_bytes <= nbytes);
    offset = (*mqh).mqh_partial_bytes;
    'outer: loop {
        if offset >= (*iov.offset(which_iov as isize)).len {
            offset -= (*iov.offset(which_iov as isize)).len;
            which_iov += 1;
            if which_iov >= iovcnt {
                break 'outer;
            }
            continue;
        }

        /*
         * We want to avoid copying the data if at all possible, but every
         * chunk of bytes we write into the queue has to be MAXALIGN'd, except
         * the last.  Thus, if a chunk other than the last one ends on a
         * non-MAXALIGN'd boundary, we have to combine the tail end of its
         * data with data from one or more following chunks until we either
         * reach the last chunk or accumulate a number of bytes which is
         * MAXALIGN'd.
         */
        if which_iov + 1 < iovcnt
            && offset + MAXIMUM_ALIGNOF > (*iov.offset(which_iov as isize)).len
        {
            let mut tmpbuf = [0u8; MAXIMUM_ALIGNOF]; // MAXIMUM_ALIGNOF == 8
            let mut j: usize = 0;

            loop {
                if offset < (*iov.offset(which_iov as isize)).len {
                    tmpbuf[j] =
                        *(*iov.offset(which_iov as isize)).data.add(offset) as u8;
                    j += 1;
                    offset += 1;
                    if j == MAXIMUM_ALIGNOF {
                        break;
                    }
                } else {
                    offset -= (*iov.offset(which_iov as isize)).len;
                    which_iov += 1;
                    if which_iov >= iovcnt {
                        break;
                    }
                }
            }

            res = shm_mq_send_bytes(
                mqh,
                j,
                tmpbuf.as_ptr() as *const c_void,
                nowait,
                &mut bytes_written,
            );

            if res == SHM_MQ_DETACHED {
                /* Reset state in case caller tries to send another message. */
                (*mqh).mqh_partial_bytes = 0;
                (*mqh).mqh_length_word_complete = false;
                return res;
            }

            (*mqh).mqh_partial_bytes += bytes_written;
            if res != SHM_MQ_SUCCESS {
                return res;
            }
            continue;
        }

        /*
         * If this is the last chunk, we can write all the data, even if it
         * isn't a multiple of MAXIMUM_ALIGNOF.  Otherwise, we need to
         * MAXALIGN_DOWN the write size.
         */
        let mut chunksize: Size =
            (*iov.offset(which_iov as isize)).len - offset;
        if which_iov + 1 < iovcnt {
            chunksize = MAXALIGN_DOWN(chunksize);
        }
        res = shm_mq_send_bytes(
            mqh,
            chunksize,
            (*iov.offset(which_iov as isize)).data.add(offset) as *const c_void,
            nowait,
            &mut bytes_written,
        );

        if res == SHM_MQ_DETACHED {
            /* Reset state in case caller tries to send another message. */
            (*mqh).mqh_length_word_complete = false;
            (*mqh).mqh_partial_bytes = 0;
            return res;
        }

        (*mqh).mqh_partial_bytes += bytes_written;
        offset += bytes_written;
        if res != SHM_MQ_SUCCESS {
            return res;
        }

        if (*mqh).mqh_partial_bytes >= nbytes {
            break 'outer;
        }
    }

    /* Reset for next message. */
    (*mqh).mqh_partial_bytes = 0;
    (*mqh).mqh_length_word_complete = false;

    /* If queue has been detached, let caller know. */
    if (*mq).mq_detached {
        return SHM_MQ_DETACHED;
    }

    /*
     * If the counterparty is known to have attached, we can read mq_receiver
     * without acquiring the spinlock.  Otherwise, more caution is needed.
     */
    if (*mqh).mqh_counterparty_attached {
        receiver = (*mq).mq_receiver;
    } else {
        SpinLockAcquire(&mut (*mq).mq_mutex);
        receiver = (*mq).mq_receiver;
        SpinLockRelease(&mut (*mq).mq_mutex);
        if !receiver.is_null() {
            (*mqh).mqh_counterparty_attached = true;
        }
    }

    /*
     * If the caller has requested force flush or we have written more than
     * 1/4 of the ring size, mark it as written in shared memory and notify
     * the receiver.
     */
    if force_flush || (*mqh).mqh_send_pending > ((*mq).mq_ring_size >> 2) {
        shm_mq_inc_bytes_written(mq, (*mqh).mqh_send_pending);
        if !receiver.is_null() {
            SetLatch(&mut (*receiver).procLatch);
        }
        (*mqh).mqh_send_pending = 0;
    }

    SHM_MQ_SUCCESS
}

/*
 * Receive a message from a shared message queue.
 *
 * We set *nbytes to the message length and *data to point to the message
 * payload.  If the entire message exists in the queue as a single,
 * contiguous chunk, *data will point directly into shared memory; otherwise,
 * it will point to a temporary buffer.  This mostly avoids data copying in
 * the hoped-for case where messages are short compared to the buffer size,
 * while still allowing longer messages.  In either case, the return value
 * remains valid until the next receive operation is performed on the queue.
 *
 * When nowait = false, we'll wait on our process latch when the ring buffer
 * is empty and we have not yet received a full message.  The sender will
 * set our process latch after more data has been written, and we'll resume
 * processing.  Each call will therefore return a complete message
 * (unless the sender detaches the queue).
 *
 * When nowait = true, we do not manipulate the state of the process latch;
 * instead, whenever the buffer is empty and we need to read from it, we
 * return SHM_MQ_WOULD_BLOCK.
 */
pub unsafe fn shm_mq_receive(
    mqh: *mut shm_mq_handle,
    nbytesp: *mut Size,
    datap: *mut *mut c_void,
    nowait: bool,
) -> shm_mq_result {
    let mq: *mut shm_mq = (*mqh).mqh_queue;
    let mut res: shm_mq_result;
    let mut rb: Size = 0;
    let mut nbytes: Size;
    let mut rawdata: *mut c_void;

    Assert!((*mq).mq_receiver == MyProc);

    /* We can't receive data until the sender has attached. */
    if !(*mqh).mqh_counterparty_attached {
        if nowait {
            let counterparty_gone: bool;

            /*
             * We shouldn't return at this point at all unless the sender
             * hasn't attached yet.  However, the correct return value depends
             * on whether the sender is still attached.  If we first test
             * whether the sender has ever attached and then test whether the
             * sender has detached, there's a race condition: a sender that
             * attaches and detaches very quickly might fool us into thinking
             * the sender never attached at all.  So, test whether our
             * counterparty is definitively gone first, and only afterwards
             * check whether the sender ever attached in the first place.
             */
            counterparty_gone = shm_mq_counterparty_gone(mq, (*mqh).mqh_handle);
            if shm_mq_get_sender(mq).is_null() {
                if counterparty_gone {
                    return SHM_MQ_DETACHED;
                } else {
                    return SHM_MQ_WOULD_BLOCK;
                }
            }
        } else if !shm_mq_wait_internal(mq, &mut (*mq).mq_sender, (*mqh).mqh_handle)
            && shm_mq_get_sender(mq).is_null()
        {
            (*mq).mq_detached = true;
            return SHM_MQ_DETACHED;
        }
        (*mqh).mqh_counterparty_attached = true;
    }

    /*
     * If we've consumed an amount of data greater than 1/4th of the ring
     * size, mark it consumed in shared memory.  We try to avoid doing this
     * unnecessarily when only a small amount of data has been consumed,
     * because SetLatch() is fairly expensive and we don't want to do it too
     * often.
     */
    if (*mqh).mqh_consume_pending > (*mq).mq_ring_size / 4 {
        shm_mq_inc_bytes_read(mq, (*mqh).mqh_consume_pending);
        (*mqh).mqh_consume_pending = 0;
    }

    /* Try to read, or finish reading, the length word from the buffer. */
    while !(*mqh).mqh_length_word_complete {
        /* Try to receive the message length word. */
        Assert!((*mqh).mqh_partial_bytes < core::mem::size_of::<Size>());
        rawdata = core::ptr::null_mut();
        res = shm_mq_receive_bytes(
            mqh,
            core::mem::size_of::<Size>() - (*mqh).mqh_partial_bytes,
            nowait,
            &mut rb,
            &mut rawdata,
        );
        if res != SHM_MQ_SUCCESS {
            return res;
        }

        /*
         * Hopefully, we'll receive the entire message length word at once.
         * But if sizeof(Size) > MAXIMUM_ALIGNOF, then it might be split over
         * multiple reads.
         */
        if (*mqh).mqh_partial_bytes == 0 && rb >= core::mem::size_of::<Size>() {
            let needed: Size;

            nbytes = *(rawdata as *const Size);

            /* If we've already got the whole message, we're done. */
            needed = MAXALIGN(core::mem::size_of::<Size>()) + MAXALIGN(nbytes);
            if rb >= needed {
                (*mqh).mqh_consume_pending += needed;
                *nbytesp = nbytes;
                *datap = (rawdata as *mut c_char)
                    .add(MAXALIGN(core::mem::size_of::<Size>()))
                    as *mut c_void;
                return SHM_MQ_SUCCESS;
            }

            /*
             * We don't have the whole message, but we at least have the whole
             * length word.
             */
            (*mqh).mqh_expected_bytes = nbytes;
            (*mqh).mqh_length_word_complete = true;
            (*mqh).mqh_consume_pending += MAXALIGN(core::mem::size_of::<Size>());
            rb -= MAXALIGN(core::mem::size_of::<Size>());
        } else {
            let lengthbytes: Size;

            /* Can't be split unless bigger than required alignment. */
            Assert!(core::mem::size_of::<Size>() > MAXIMUM_ALIGNOF);

            /* Message word is split; need buffer to reassemble. */
            if (*mqh).mqh_buffer.is_null() {
                (*mqh).mqh_buffer = MemoryContextAlloc(
                    (*mqh).mqh_context,
                    MQH_INITIAL_BUFSIZE,
                ) as *mut c_char;
                (*mqh).mqh_buflen = MQH_INITIAL_BUFSIZE;
            }
            Assert!((*mqh).mqh_buflen >= core::mem::size_of::<Size>());

            /* Copy partial length word; remember to consume it. */
            lengthbytes = if (*mqh).mqh_partial_bytes + rb > core::mem::size_of::<Size>() {
                core::mem::size_of::<Size>() - (*mqh).mqh_partial_bytes
            } else {
                rb
            };
            core::ptr::copy_nonoverlapping(
                rawdata as *const u8,
                (*mqh).mqh_buffer.add((*mqh).mqh_partial_bytes) as *mut u8,
                lengthbytes,
            );
            (*mqh).mqh_partial_bytes += lengthbytes;
            (*mqh).mqh_consume_pending += MAXALIGN(lengthbytes);
            rb -= lengthbytes;

            /* If we now have the whole word, we're ready to read payload. */
            if (*mqh).mqh_partial_bytes >= core::mem::size_of::<Size>() {
                Assert!((*mqh).mqh_partial_bytes == core::mem::size_of::<Size>());
                (*mqh).mqh_expected_bytes =
                    *((*mqh).mqh_buffer as *const Size);
                (*mqh).mqh_length_word_complete = true;
                (*mqh).mqh_partial_bytes = 0;
            }
        }
    }
    nbytes = (*mqh).mqh_expected_bytes;

    /*
     * Should be disallowed on the sending side already, but better check and
     * error out on the receiver side as well rather than trying to read a
     * prohibitively large message.
     */
    if nbytes > MaxAllocSize {
        // C also attaches errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED).
        ereport!(
            ERROR,
            errmsg!("invalid message size {} in shared memory queue", nbytes)
        );
    }

    rawdata = core::ptr::null_mut();

    if (*mqh).mqh_partial_bytes == 0 {
        /*
         * Try to obtain the whole message in a single chunk.  If this works,
         * we need not copy the data and can return a pointer directly into
         * shared memory.
         */
        res = shm_mq_receive_bytes(mqh, nbytes, nowait, &mut rb, &mut rawdata);
        if res != SHM_MQ_SUCCESS {
            return res;
        }
        if rb >= nbytes {
            (*mqh).mqh_length_word_complete = false;
            (*mqh).mqh_consume_pending += MAXALIGN(nbytes);
            *nbytesp = nbytes;
            *datap = rawdata;
            return SHM_MQ_SUCCESS;
        }

        /*
         * The message has wrapped the buffer.  We'll need to copy it in order
         * to return it to the client in one chunk.  First, make sure we have
         * a large enough buffer available.
         */
        if (*mqh).mqh_buflen < nbytes {
            let mut newbuflen: Size;

            /*
             * Increase size to the next power of 2 that's >= nbytes, but
             * limit to MaxAllocSize.
             */
            newbuflen = pg_nextpower2_size_t(nbytes as uint64) as Size;
            newbuflen = if newbuflen < MaxAllocSize { newbuflen } else { MaxAllocSize };

            if !(*mqh).mqh_buffer.is_null() {
                pfree((*mqh).mqh_buffer as *mut c_void);
                (*mqh).mqh_buffer = core::ptr::null_mut();
                (*mqh).mqh_buflen = 0;
            }
            (*mqh).mqh_buffer =
                MemoryContextAlloc((*mqh).mqh_context, newbuflen) as *mut c_char;
            (*mqh).mqh_buflen = newbuflen;
        }
    }

    /* Loop until we've copied the entire message. */
    loop {
        let still_needed: Size;

        /* Copy as much as we can. */
        Assert!((*mqh).mqh_partial_bytes + rb <= nbytes);
        if rb > 0 {
            core::ptr::copy_nonoverlapping(
                rawdata as *const u8,
                (*mqh).mqh_buffer.add((*mqh).mqh_partial_bytes) as *mut u8,
                rb,
            );
            (*mqh).mqh_partial_bytes += rb;
        }

        /*
         * Update count of bytes that can be consumed, accounting for
         * alignment padding.  Note that this will never actually insert any
         * padding except at the end of a message, because the buffer size is
         * a multiple of MAXIMUM_ALIGNOF, and each read and write is as well.
         */
        Assert!((*mqh).mqh_partial_bytes == nbytes || rb == MAXALIGN(rb));
        (*mqh).mqh_consume_pending += MAXALIGN(rb);

        /* If we got all the data, exit the loop. */
        if (*mqh).mqh_partial_bytes >= nbytes {
            break;
        }

        /* Wait for some more data. */
        still_needed = nbytes - (*mqh).mqh_partial_bytes;
        res = shm_mq_receive_bytes(mqh, still_needed, nowait, &mut rb, &mut rawdata);
        if res != SHM_MQ_SUCCESS {
            return res;
        }
        if rb > still_needed {
            rb = still_needed;
        }
    }

    /* Return the complete message, and reset for next message. */
    *nbytesp = nbytes;
    *datap = (*mqh).mqh_buffer as *mut c_void;
    (*mqh).mqh_length_word_complete = false;
    (*mqh).mqh_partial_bytes = 0;
    SHM_MQ_SUCCESS
}

/*
 * Wait for the other process that's supposed to use this queue to attach
 * to it.
 *
 * The return value is SHM_MQ_DETACHED if the worker has already detached or
 * if it dies; it is SHM_MQ_SUCCESS if we detect that the worker has attached.
 * Note that we will only be able to detect that the worker has died before
 * attaching if a background worker handle was passed to shm_mq_attach().
 */
pub unsafe fn shm_mq_wait_for_attach(mqh: *mut shm_mq_handle) -> shm_mq_result {
    let mq: *mut shm_mq = (*mqh).mqh_queue;
    let victim: *mut *mut PGPROC;

    if shm_mq_get_receiver(mq) == MyProc {
        victim = &mut (*mq).mq_sender;
    } else {
        Assert!(shm_mq_get_sender(mq) == MyProc);
        victim = &mut (*mq).mq_receiver;
    }

    if shm_mq_wait_internal(mq, victim, (*mqh).mqh_handle) {
        SHM_MQ_SUCCESS
    } else {
        SHM_MQ_DETACHED
    }
}

/*
 * Detach from a shared message queue, and destroy the shm_mq_handle.
 */
pub unsafe fn shm_mq_detach(mqh: *mut shm_mq_handle) {
    /* Before detaching, notify the receiver about any already-written data. */
    if (*mqh).mqh_send_pending > 0 {
        shm_mq_inc_bytes_written((*mqh).mqh_queue, (*mqh).mqh_send_pending);
        (*mqh).mqh_send_pending = 0;
    }

    /* Notify counterparty that we're outta here. */
    shm_mq_detach_internal((*mqh).mqh_queue);

    /* Cancel on_dsm_detach callback, if any. */
    if !(*mqh).mqh_segment.is_null() {
        cancel_on_dsm_detach(
            (*mqh).mqh_segment,
            shm_mq_detach_callback,
            PointerGetDatum((*mqh).mqh_queue as *mut c_void),
        );
    }

    /* Release local memory associated with handle. */
    if !(*mqh).mqh_buffer.is_null() {
        pfree((*mqh).mqh_buffer as *mut c_void);
    }
    pfree(mqh as *mut c_void);
}

/*
 * Get the shm_mq from handle.
 */
pub unsafe fn shm_mq_get_queue(mqh: *mut shm_mq_handle) -> *mut shm_mq {
    (*mqh).mqh_queue
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/*
 * Notify counterparty that we're detaching from shared message queue.
 *
 * The purpose of this function is to make sure that the process
 * with which we're communicating doesn't block forever waiting for us to
 * fill or drain the queue once we've lost interest.
 *
 * This is separated out from shm_mq_detach() because if the on_dsm_detach
 * callback fires, we only want to do this much.
 */
unsafe fn shm_mq_detach_internal(mq: *mut shm_mq) {
    let victim: *mut PGPROC;

    SpinLockAcquire(&mut (*mq).mq_mutex);
    if (*mq).mq_sender == MyProc {
        victim = (*mq).mq_receiver;
    } else {
        Assert!((*mq).mq_receiver == MyProc);
        victim = (*mq).mq_sender;
    }
    (*mq).mq_detached = true;
    SpinLockRelease(&mut (*mq).mq_mutex);

    if !victim.is_null() {
        SetLatch(&mut (*victim).procLatch);
    }
}

/*
 * Write bytes into a shared message queue.
 */
unsafe fn shm_mq_send_bytes(
    mqh: *mut shm_mq_handle,
    nbytes: Size,
    data: *const c_void,
    nowait: bool,
    bytes_written: *mut Size,
) -> shm_mq_result {
    let mq: *mut shm_mq = (*mqh).mqh_queue;
    let mut sent: Size = 0;
    let mut used: u64;
    let ringsize: Size = (*mq).mq_ring_size;
    let mut available: Size;

    while sent < nbytes {
        let rb: u64;
        let wb: u64;

        /* Compute number of ring buffer bytes used and available. */
        rb = pg_atomic_read_u64(&mut (*mq).mq_bytes_read);
        wb = pg_atomic_read_u64(&mut (*mq).mq_bytes_written)
            + (*mqh).mqh_send_pending as u64;
        Assert!(wb >= rb);
        used = wb - rb;
        Assert!(used as Size <= ringsize);
        available = {
            let avail_from_ring = ringsize - used as Size;
            let remaining = nbytes - sent;
            if avail_from_ring < remaining { avail_from_ring } else { remaining }
        };

        /*
         * Bail out if the queue has been detached.  Note that we would be in
         * trouble if the compiler decided to cache the value of
         * mq->mq_detached in a register or on the stack across loop
         * iterations.  It probably shouldn't do that anyway since we'll
         * always return, call an external function that performs a system
         * call, or reach a memory barrier at some point later in the loop,
         * but just to be sure, insert a compiler barrier here.
         */
        pg_compiler_barrier();
        if (*mq).mq_detached {
            *bytes_written = sent;
            return SHM_MQ_DETACHED;
        }

        if available == 0 && !(*mqh).mqh_counterparty_attached {
            /*
             * The queue is full, so if the receiver isn't yet known to be
             * attached, we must wait for that to happen.
             */
            if nowait {
                if shm_mq_counterparty_gone(mq, (*mqh).mqh_handle) {
                    *bytes_written = sent;
                    return SHM_MQ_DETACHED;
                }
                if shm_mq_get_receiver(mq).is_null() {
                    *bytes_written = sent;
                    return SHM_MQ_WOULD_BLOCK;
                }
            } else if !shm_mq_wait_internal(
                mq,
                &mut (*mq).mq_receiver,
                (*mqh).mqh_handle,
            ) {
                (*mq).mq_detached = true;
                *bytes_written = sent;
                return SHM_MQ_DETACHED;
            }
            (*mqh).mqh_counterparty_attached = true;

            /*
             * The receiver may have read some data after attaching, so we
             * must not wait without rechecking the queue state.
             */
        } else if available == 0 {
            /* Update the pending send bytes in the shared memory. */
            shm_mq_inc_bytes_written(mq, (*mqh).mqh_send_pending);

            /*
             * Since mqh_counterparty_attached is known to be true at this
             * point, mq_receiver has been set, and it can't change once set.
             * Therefore, we can read it without acquiring the spinlock.
             */
            Assert!((*mqh).mqh_counterparty_attached);
            SetLatch(&mut (*(*mq).mq_receiver).procLatch);

            /*
             * We have just updated the mqh_send_pending bytes in the shared
             * memory so reset it.
             */
            (*mqh).mqh_send_pending = 0;

            /* Skip manipulation of our latch if nowait = true. */
            if nowait {
                *bytes_written = sent;
                return SHM_MQ_WOULD_BLOCK;
            }

            /*
             * Wait for our latch to be set.  It might already be set for some
             * unrelated reason, but that'll just result in one extra trip
             * through the loop.  It's worth it to avoid resetting the latch
             * at top of loop, because setting an already-set latch is much
             * cheaper than setting one that has been reset.
             */
            let _ = WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                WAIT_EVENT_MESSAGE_QUEUE_SEND,
            );

            /* Reset the latch so we don't spin. */
            ResetLatch(MyLatch);

            /* An interrupt may have occurred while we were waiting. */
            CHECK_FOR_INTERRUPTS();
        } else {
            let offset: Size;
            let sendnow: Size;

            offset = (wb % ringsize as u64) as Size;
            sendnow = {
                let rem = ringsize - offset;
                if available < rem { available } else { rem }
            };

            /*
             * Write as much data as we can via a single memcpy().  Make sure
             * these writes happen after the read of mq_bytes_read, above.
             * This barrier pairs with the one in shm_mq_inc_bytes_read.
             * (Since we're separating the read of mq_bytes_read from a
             * subsequent write to mq_ring, we need a full barrier here.)
             */
            pg_memory_barrier();
            core::ptr::copy_nonoverlapping(
                (data as *const u8).add(sent),
                ((*mq).mq_ring.as_mut_ptr() as *mut u8)
                    .add((*mq).mq_ring_offset as usize + offset),
                sendnow,
            );
            sent += sendnow;

            /*
             * Update count of bytes written, with alignment padding.  Note
             * that this will never actually insert any padding except at the
             * end of a run of bytes, because the buffer size is a multiple of
             * MAXIMUM_ALIGNOF, and each read is as well.
             */
            Assert!(sent == nbytes || sendnow == MAXALIGN(sendnow));

            /*
             * For efficiency, we don't update the bytes written in the shared
             * memory and also don't set the reader's latch here.
             */
            (*mqh).mqh_send_pending += MAXALIGN(sendnow);
        }
    }

    *bytes_written = sent;
    SHM_MQ_SUCCESS
}

/*
 * Wait until at least *nbytesp bytes are available to be read from the
 * shared message queue, or until the buffer wraps around.  If the queue is
 * detached, returns SHM_MQ_DETACHED.  If nowait is specified and a wait
 * would be required, returns SHM_MQ_WOULD_BLOCK.  Otherwise, *datap is set
 * to the location at which data bytes can be read, *nbytesp is set to the
 * number of bytes which can be read at that address, and the return value
 * is SHM_MQ_SUCCESS.
 */
unsafe fn shm_mq_receive_bytes(
    mqh: *mut shm_mq_handle,
    bytes_needed: Size,
    nowait: bool,
    nbytesp: *mut Size,
    datap: *mut *mut c_void,
) -> shm_mq_result {
    let mq: *mut shm_mq = (*mqh).mqh_queue;
    let ringsize: Size = (*mq).mq_ring_size;
    let mut used: u64;
    let mut written: u64;

    loop {
        let offset: Size;
        let read: u64;

        /* Get bytes written, so we can compute what's available to read. */
        written = pg_atomic_read_u64(&mut (*mq).mq_bytes_written);

        /*
         * Get bytes read.  Include bytes we could consume but have not yet
         * consumed.
         */
        read = pg_atomic_read_u64(&mut (*mq).mq_bytes_read)
            + (*mqh).mqh_consume_pending as u64;
        used = written - read;
        Assert!(used as Size <= ringsize);
        offset = (read % ringsize as u64) as Size;

        /* If we have enough data or buffer has wrapped, we're done. */
        if used as Size >= bytes_needed || offset + used as Size >= ringsize {
            *nbytesp = {
                let avail = used as Size;
                let rem = ringsize - offset;
                if avail < rem { avail } else { rem }
            };
            *datap = ((*mq).mq_ring.as_mut_ptr() as *mut u8)
                .add((*mq).mq_ring_offset as usize + offset) as *mut c_void;

            /*
             * Separate the read of mq_bytes_written, above, from caller's
             * attempt to read the data itself.  Pairs with the barrier in
             * shm_mq_inc_bytes_written.
             */
            pg_read_barrier();
            return SHM_MQ_SUCCESS;
        }

        /*
         * Fall out before waiting if the queue has been detached.
         *
         * Note that we don't check for this until *after* considering whether
         * the data already available is enough, since the receiver can finish
         * receiving a message stored in the buffer even after the sender has
         * detached.
         */
        if (*mq).mq_detached {
            /*
             * If the writer advanced mq_bytes_written and then set
             * mq_detached, we might not have read the final value of
             * mq_bytes_written above.  Insert a read barrier and then check
             * again if mq_bytes_written has advanced.
             */
            pg_read_barrier();
            if written != pg_atomic_read_u64(&mut (*mq).mq_bytes_written) {
                continue;
            }

            return SHM_MQ_DETACHED;
        }

        /*
         * We didn't get enough data to satisfy the request, so mark any data
         * previously-consumed as read to make more buffer space.
         */
        if (*mqh).mqh_consume_pending > 0 {
            shm_mq_inc_bytes_read(mq, (*mqh).mqh_consume_pending);
            (*mqh).mqh_consume_pending = 0;
        }

        /* Skip manipulation of our latch if nowait = true. */
        if nowait {
            return SHM_MQ_WOULD_BLOCK;
        }

        /*
         * Wait for our latch to be set.  It might already be set for some
         * unrelated reason, but that'll just result in one extra trip through
         * the loop.  It's worth it to avoid resetting the latch at top of
         * loop, because setting an already-set latch is much cheaper than
         * setting one that has been reset.
         */
        let _ = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_MESSAGE_QUEUE_RECEIVE,
        );

        /* Reset the latch so we don't spin. */
        ResetLatch(MyLatch);

        /* An interrupt may have occurred while we were waiting. */
        CHECK_FOR_INTERRUPTS();
    }
}

/*
 * Test whether a counterparty who may not even be alive yet is definitely gone.
 */
unsafe fn shm_mq_counterparty_gone(
    mq: *mut shm_mq,
    handle: *mut BackgroundWorkerHandle,
) -> bool {
    let mut pid: pid_t = 0;

    /* If the queue has been detached, counterparty is definitely gone. */
    if (*mq).mq_detached {
        return true;
    }

    /* If there's a handle, check worker status. */
    if !handle.is_null() {
        let status: BgwHandleStatus;

        /* Check for unexpected worker death. */
        status = GetBackgroundWorkerPid(handle, &mut pid);
        if status != BGWH_STARTED && status != BGWH_NOT_YET_STARTED {
            /* Mark it detached, just to make it official. */
            (*mq).mq_detached = true;
            return true;
        }
    }

    /* Counterparty is not definitively gone. */
    false
}

/*
 * This is used when a process is waiting for its counterpart to attach to the
 * queue.  We exit when the other process attaches as expected, or, if
 * handle != NULL, when the referenced background process or the postmaster
 * dies.  Note that if handle == NULL, and the process fails to attach, we'll
 * potentially get stuck here forever waiting for a process that may never
 * start.  We do check for interrupts, though.
 *
 * ptr is a pointer to the memory address that we're expecting to become
 * non-NULL when our counterpart attaches to the queue.
 */
unsafe fn shm_mq_wait_internal(
    mq: *mut shm_mq,
    ptr: *mut *mut PGPROC,
    handle: *mut BackgroundWorkerHandle,
) -> bool {
    let mut result: bool = false;

    loop {
        let status: BgwHandleStatus;
        let mut pid: pid_t = 0;

        /* Acquire the lock just long enough to check the pointer. */
        SpinLockAcquire(&mut (*mq).mq_mutex);
        result = !(*ptr).is_null();
        SpinLockRelease(&mut (*mq).mq_mutex);

        /* Fail if detached; else succeed if initialized. */
        if (*mq).mq_detached {
            result = false;
            break;
        }
        if result {
            break;
        }

        if !handle.is_null() {
            /* Check for unexpected worker death. */
            status = GetBackgroundWorkerPid(handle, &mut pid);
            if status != BGWH_STARTED && status != BGWH_NOT_YET_STARTED {
                result = false;
                break;
            }
        }

        /* Wait to be signaled. */
        let _ = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_MESSAGE_QUEUE_INTERNAL,
        );

        /* Reset the latch so we don't spin. */
        ResetLatch(MyLatch);

        /* An interrupt may have occurred while we were waiting. */
        CHECK_FOR_INTERRUPTS();
    }

    result
}

/*
 * Increment the number of bytes read.
 */
unsafe fn shm_mq_inc_bytes_read(mq: *mut shm_mq, n: Size) {
    let sender: *mut PGPROC;

    /*
     * Separate prior reads of mq_ring from the increment of mq_bytes_read
     * which follows.  This pairs with the full barrier in
     * shm_mq_send_bytes().  We only need a read barrier here because the
     * increment of mq_bytes_read is actually a read followed by a dependent
     * write.
     */
    pg_read_barrier();

    /*
     * There's no need to use pg_atomic_fetch_add_u64 here, because nobody
     * else can be changing this value.  This method should be cheaper.
     */
    pg_atomic_write_u64(
        &mut (*mq).mq_bytes_read,
        pg_atomic_read_u64(&mut (*mq).mq_bytes_read) + n as u64,
    );

    /*
     * We shouldn't have any bytes to read without a sender, so we can read
     * mq_sender here without a lock.  Once it's initialized, it can't change.
     */
    sender = (*mq).mq_sender;
    Assert!(!sender.is_null());
    SetLatch(&mut (*sender).procLatch);
}

/*
 * Increment the number of bytes written.
 */
unsafe fn shm_mq_inc_bytes_written(mq: *mut shm_mq, n: Size) {
    /*
     * Separate prior reads of mq_ring from the write of mq_bytes_written
     * which we're about to do.  Pairs with the read barrier found in
     * shm_mq_receive_bytes.
     */
    pg_write_barrier();

    /*
     * There's no need to use pg_atomic_fetch_add_u64 here, because nobody
     * else can be changing this value.  This method avoids taking the bus
     * lock unnecessarily.
     */
    pg_atomic_write_u64(
        &mut (*mq).mq_bytes_written,
        pg_atomic_read_u64(&mut (*mq).mq_bytes_written) + n as u64,
    );
}

/* Shim for on_dsm_detach callback. */
unsafe fn shm_mq_detach_callback(seg: *mut dsm_segment, arg: Datum) {
    // suppress unused parameter warning - seg is part of the callback signature
    let _ = seg;
    let mq: *mut shm_mq = DatumGetPointer(arg) as *mut shm_mq;

    shm_mq_detach_internal(mq);
}
