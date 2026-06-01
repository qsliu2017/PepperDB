//! tqueue.rs
//!   Use shm_mq to send & receive tuples between parallel backends
//!
//! A DestReceiver of type DestTupleQueue, which is a TQueueDestReceiver
//! under the hood, writes tuples from the executor to a shm_mq.
//!
//! A TupleQueueReader reads tuples from a shm_mq and returns the tuples.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Postgres source:
//!   src/backend/executor/tqueue.c
//!   src/include/executor/tqueue.h

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::ptr::null_mut;

use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::MinimalTuple;
use crate::c::Size;
use crate::executor::tuptable::TupleTableSlot;
use crate::tcop::dest::CommandDest::DestTupleQueue;
use crate::tcop::dest::DestReceiver;

/* ----------------------------------------------------------------
 * shm_mq interface (storage/shm_mq.h)
 *
 * These are defined as local stubs here; the real implementations live in
 * the storage layer.  Kept in sync (by-pointer / by-value) with the rest of
 * the tree (see src/libpq/pqmq.rs).
 * ----------------------------------------------------------------
 */

/* Opaque message-queue handle. */
pub type shm_mq_handle = c_void;

/* Possible results of a send or receive operation. */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub enum shm_mq_result {
    SHM_MQ_SUCCESS,     /* Sent or received a message. */
    SHM_MQ_WOULD_BLOCK, /* Not completed; retry later. */
    SHM_MQ_DETACHED,    /* Other process has detached queue. */
}
use shm_mq_result::*;

extern "C" {
    /*
     * These are deep external dependencies on the shared-memory message-queue
     * implementation (storage/ipc/shm_mq.c).  Declared here so the signatures
     * are visible to this translation unit.
     */
    fn shm_mq_send(
        mqh: *mut shm_mq_handle,
        nbytes: Size,
        data: *const c_void,
        nowait: bool,
        force_flush: bool,
    ) -> shm_mq_result;

    fn shm_mq_receive(
        mqh: *mut shm_mq_handle,
        nbytesp: *mut Size,
        datap: *mut *mut c_void,
        nowait: bool,
    ) -> shm_mq_result;

    fn shm_mq_detach(mqh: *mut shm_mq_handle);
}

/*
 * DestReceiver object's private contents
 *
 * queue is a pointer to data supplied by DestReceiver's caller.
 */
#[repr(C)]
pub struct TQueueDestReceiver {
    pub pub_: DestReceiver,        /* public fields */
    pub queue: *mut shm_mq_handle, /* shm_mq to send to */
}

/*
 * TupleQueueReader object's private contents
 *
 * queue is a pointer to data supplied by reader's caller.
 *
 * "typedef struct TupleQueueReader TupleQueueReader" is in tqueue.h
 */
#[repr(C)]
pub struct TupleQueueReader {
    pub queue: *mut shm_mq_handle, /* shm_mq to receive from */
}

/*
 * Receive a tuple from a query, and send it to the designated shm_mq.
 *
 * Returns true if successful, false if shm_mq has been detached.
 */
unsafe fn tqueueReceiveSlot(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let tqueue = self_ as *mut TQueueDestReceiver;
    let tuple: MinimalTuple;
    let result: shm_mq_result;
    let mut should_free: bool = false;

    /* Send the tuple itself. */
    tuple = crate::executor::execTuples::ExecFetchSlotMinimalTuple(slot, &mut should_free);
    result = shm_mq_send(
        (*tqueue).queue,
        (*tuple).t_len as Size,
        tuple as *const c_void,
        false,
        false,
    );

    if should_free {
        pfree(tuple as *mut c_void);
    }

    /* Check for failure. */
    if result == SHM_MQ_DETACHED {
        return false;
    } else if result != SHM_MQ_SUCCESS {
        ereport!(
            ERROR,
            "could not send tuple to shared-memory queue"
        );
    }

    true
}

/*
 * Prepare to receive tuples from executor.
 */
unsafe fn tqueueStartupReceiver(_self_: *mut DestReceiver, _operation: c_int, _typeinfo: TupleDesc) {
    /* do nothing */
}

/*
 * Clean up at end of an executor run
 */
unsafe fn tqueueShutdownReceiver(self_: *mut DestReceiver) {
    let tqueue = self_ as *mut TQueueDestReceiver;

    if !(*tqueue).queue.is_null() {
        shm_mq_detach((*tqueue).queue);
    }
    (*tqueue).queue = null_mut();
}

/*
 * Destroy receiver when done with it
 */
unsafe fn tqueueDestroyReceiver(self_: *mut DestReceiver) {
    let tqueue = self_ as *mut TQueueDestReceiver;

    /* We probably already detached from queue, but let's be sure */
    if !(*tqueue).queue.is_null() {
        shm_mq_detach((*tqueue).queue);
    }
    pfree(self_ as *mut c_void);
}

/*
 * Create a DestReceiver that writes tuples to a tuple queue.
 */
pub unsafe fn CreateTupleQueueDestReceiver(handle: *mut shm_mq_handle) -> *mut DestReceiver {
    let self_: *mut TQueueDestReceiver;

    self_ = palloc0(size_of::<TQueueDestReceiver>()) as *mut TQueueDestReceiver;

    (*self_).pub_.receiveSlot = Some(tqueueReceiveSlot);
    (*self_).pub_.rStartup = Some(tqueueStartupReceiver);
    (*self_).pub_.rShutdown = Some(tqueueShutdownReceiver);
    (*self_).pub_.rDestroy = Some(tqueueDestroyReceiver);
    (*self_).pub_.mydest = DestTupleQueue;
    (*self_).queue = handle;

    self_ as *mut DestReceiver
}

/*
 * Create a tuple queue reader.
 */
pub unsafe fn CreateTupleQueueReader(handle: *mut shm_mq_handle) -> *mut TupleQueueReader {
    let reader = palloc0(size_of::<TupleQueueReader>()) as *mut TupleQueueReader;

    (*reader).queue = handle;

    reader
}

/*
 * Destroy a tuple queue reader.
 *
 * Note: cleaning up the underlying shm_mq is the caller's responsibility.
 * We won't access it here, as it may be detached already.
 */
pub unsafe fn DestroyTupleQueueReader(reader: *mut TupleQueueReader) {
    pfree(reader as *mut c_void);
}

/*
 * Fetch a tuple from a tuple queue reader.
 *
 * The return value is NULL if there are no remaining tuples or if
 * nowait = true and no tuple is ready to return.  *done, if not NULL,
 * is set to true when there are no remaining tuples and otherwise to false.
 *
 * The returned tuple, if any, is either in shared memory or a private buffer
 * and should not be freed.  The pointer is invalid after the next call to
 * TupleQueueReaderNext().
 *
 * Even when shm_mq_receive() returns SHM_MQ_WOULD_BLOCK, this can still
 * accumulate bytes from a partially-read message, so it's useful to call
 * this with nowait = true even if nothing is returned.
 */
pub unsafe fn TupleQueueReaderNext(
    reader: *mut TupleQueueReader,
    nowait: bool,
    done: *mut bool,
) -> MinimalTuple {
    let tuple: MinimalTuple;
    let result: shm_mq_result;
    let mut nbytes: Size = 0;
    let mut data: *mut c_void = null_mut();

    if !done.is_null() {
        *done = false;
    }

    /* Attempt to read a message. */
    result = shm_mq_receive((*reader).queue, &mut nbytes, &mut data, nowait);

    /* If queue is detached, set *done and return NULL. */
    if result == SHM_MQ_DETACHED {
        if !done.is_null() {
            *done = true;
        }
        return null_mut();
    }

    /* In non-blocking mode, bail out if no message ready yet. */
    if result == SHM_MQ_WOULD_BLOCK {
        return null_mut();
    }
    Assert!(result == SHM_MQ_SUCCESS);

    /*
     * Return a pointer to the queue memory directly (which had better be
     * sufficiently aligned).
     */
    tuple = data as MinimalTuple;
    Assert!((*tuple).t_len as Size == nbytes);

    tuple
}
