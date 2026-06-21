//! src/backend/storage/aio/read_stream.c
//!
//! Mechanism for accessing buffered relation data with look-ahead
//!
//! Code that needs to access relation data typically pins blocks one at a
//! time, often in a predictable order that might be sequential or data-driven.
//! Calling the simple ReadBuffer() function for each block is inefficient,
//! because blocks that are not yet in the buffer pool require I/O operations
//! that are small and might stall waiting for storage.  This mechanism looks
//! into the future and calls StartReadBuffers() and WaitReadBuffers() to read
//! neighboring blocks together and ahead of time, with an adaptive look-ahead
//! distance.
//!
//! A user-provided callback generates a stream of block numbers that is used
//! to form reads of up to io_combine_limit, by attempting to merge them with a
//! pending read.  When that isn't possible, the existing pending read is sent
//! to StartReadBuffers() so that a new one can begin to form.
//!
//! The algorithm for controlling the look-ahead distance is based on recent
//! cache hit and miss history.  When no I/O is necessary, there is no benefit
//! in looking ahead more than one block.  This is the default initial
//! assumption, but when blocks needing I/O are streamed, the distance is
//! increased rapidly to try to benefit from I/O combining and concurrency.  It
//! is reduced gradually when cached blocks are streamed.
//!
//! The main data structure is a circular queue of buffers of size
//! max_pinned_buffers plus some extra space for technical reasons, ready to be
//! returned by read_stream_next_buffer().  Each buffer also has an optional
//! variable sized object that is passed from the callback to the consumer of
//! buffers.
//!
//! Parallel to the queue of buffers, there is a circular queue of in-progress
//! I/Os that have been started with StartReadBuffers(), and for which
//! WaitReadBuffers() must be called before returning the buffer.
//!
//! Portions Copyright (c) 2024-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/aio/read_stream.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, uint32, Size};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};

// ---------------------------------------------------------------------------
// Types and constants normally pulled in from headers.  Local stub aliases /
// constants are provided where the real definitions are not yet ported.
// ---------------------------------------------------------------------------

// from storage/bufmgr.h
pub type Buffer = c_int;
pub const InvalidBuffer: Buffer = 0;

pub type BufferAccessStrategy = *mut c_void;
pub type ForkNumber = c_int;

// from utils/rel.h
pub type Relation = *mut c_void;
// from storage/smgr.h
pub type SMgrRelation = *mut c_void;

// from postgres_ext.h / c.h
pub const PG_INT16_MAX: c_int = 0x7FFF;

// read_buffers flags (storage/bufmgr.h)
pub const READ_BUFFERS_ISSUE_ADVICE: c_int = 1 << 2;
pub const READ_BUFFERS_SYNCHRONOUSLY: c_int = 1 << 4;

// io_method values (storage/aio.h)
pub const IOMETHOD_SYNC: c_int = 0;

// io_direct flags (common/file_utils.h)
pub const IO_DIRECT_DATA: c_int = 1 << 0;

// MAXIMUM_ALIGNOF (pg_config.h)
pub const MAXIMUM_ALIGNOF: usize = 8;

// ---------------------------------------------------------------------------
// read_stream.h
// ---------------------------------------------------------------------------

/* Default tuning, reasonable for many users. */
pub const READ_STREAM_DEFAULT: c_int = 0x00;

/*
 * I/O streams that are performing maintenance work on behalf of potentially
 * many users, and thus should be governed by maintenance_io_concurrency
 * instead of effective_io_concurrency.  For example, VACUUM or CREATE INDEX.
 */
pub const READ_STREAM_MAINTENANCE: c_int = 0x01;

/*
 * We usually avoid issuing prefetch advice automatically when sequential
 * access is detected, but this flag explicitly disables it, for cases that
 * might not be correctly detected.  Explicit advice is known to perform worse
 * than letting the kernel (at least Linux) detect sequential access.
 */
pub const READ_STREAM_SEQUENTIAL: c_int = 0x02;

/*
 * We usually ramp up from smaller reads to larger ones, to support users who
 * don't know if it's worth reading lots of buffers yet.  This flag disables
 * that, declaring ahead of time that we'll be reading all available buffers.
 */
pub const READ_STREAM_FULL: c_int = 0x04;

/*
 * Opt-in to using AIO batchmode.
 */
pub const READ_STREAM_USE_BATCHING: c_int = 0x08;

/* for block_range_read_stream_cb */
#[repr(C)]
pub struct BlockRangeReadStreamPrivate {
    pub current_blocknum: BlockNumber,
    pub last_exclusive: BlockNumber,
}

/* Callback that returns the next block number to read. */
pub type ReadStreamBlockNumberCB = Option<
    unsafe extern "C" fn(
        stream: *mut ReadStream,
        callback_private_data: *mut c_void,
        per_buffer_data: *mut c_void,
    ) -> BlockNumber,
>;

// ---------------------------------------------------------------------------
// ReadBuffersOperation (storage/bufmgr.h) - partial stub mirroring the fields
// touched by this file.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct ReadBuffersOperation {
    pub rel: Relation,
    pub smgr: SMgrRelation,
    pub persistence: c_char,
    pub forknum: ForkNumber,
    pub strategy: BufferAccessStrategy,
    pub buffers: *mut Buffer,
    pub blocknum: BlockNumber,
    pub flags: int32,
    pub nblocks: int16,
    pub nblocks_done: int16,
    pub io_wref: crate::storage::aio::aio::PgAioWaitRef,
    pub io_return: crate::storage::aio_types::PgAioReturn,
}

#[repr(C)]
struct InProgressIO {
    buffer_index: int16,
    op: ReadBuffersOperation,
}

/*
 * State for managing a stream of reads.
 */
#[repr(C)]
pub struct ReadStream {
    max_ios: int16,
    io_combine_limit: int16,
    ios_in_progress: int16,
    queue_size: int16,
    max_pinned_buffers: int16,
    forwarded_buffers: int16,
    pinned_buffers: int16,
    distance: int16,
    initialized_buffers: int16,
    read_buffers_flags: c_int,
    sync_mode: bool,  /* using io_method=sync */
    batch_mode: bool, /* READ_STREAM_USE_BATCHING */
    advice_enabled: bool,
    temporary: bool,

    /*
     * One-block buffer to support 'ungetting' a block number, to resolve flow
     * control problems when I/Os are split.
     */
    buffered_blocknum: BlockNumber,

    /*
     * The callback that will tell us which block numbers to read, and an
     * opaque pointer that will be pass to it for its own purposes.
     */
    callback: ReadStreamBlockNumberCB,
    callback_private_data: *mut c_void,

    /* Next expected block, for detecting sequential access. */
    seq_blocknum: BlockNumber,
    seq_until_processed: BlockNumber,

    /* The read operation we are currently preparing. */
    pending_read_blocknum: BlockNumber,
    pending_read_nblocks: int16,

    /* Space for buffers and optional per-buffer private data. */
    per_buffer_data_size: Size,
    per_buffer_data: *mut c_void,

    /* Read operations that have been started but not waited for yet. */
    ios: *mut InProgressIO,
    oldest_io_index: int16,
    next_io_index: int16,

    fast_path: bool,

    /* Circular queue of buffers. */
    oldest_buffer_index: int16, /* Next pinned buffer to return */
    next_buffer_index: int16,   /* Index of next buffer to pin */
    buffers: [Buffer; 0],       /* FLEXIBLE_ARRAY_MEMBER */
}

// ---------------------------------------------------------------------------
// Helpers for accessing the flexible-array buffers[] member.
// ---------------------------------------------------------------------------
#[inline]
unsafe fn buffers_ptr(stream: *mut ReadStream) -> *mut Buffer {
    core::ptr::addr_of_mut!((*stream).buffers) as *mut Buffer
}

#[inline]
unsafe fn buf_get(stream: *mut ReadStream, index: c_int) -> Buffer {
    *buffers_ptr(stream).offset(index as isize)
}

#[inline]
unsafe fn buf_set(stream: *mut ReadStream, index: c_int, val: Buffer) {
    *buffers_ptr(stream).offset(index as isize) = val;
}

#[inline]
unsafe fn buf_at(stream: *mut ReadStream, index: c_int) -> *mut Buffer {
    buffers_ptr(stream).offset(index as isize)
}

/*
 * Return a pointer to the per-buffer data by index.
 */
#[inline]
unsafe fn get_per_buffer_data(stream: *mut ReadStream, buffer_index: int16) -> *mut c_void {
    ((*stream).per_buffer_data as *mut c_char)
        .offset(((*stream).per_buffer_data_size * buffer_index as Size) as isize)
        as *mut c_void
}

/*
 * General-use ReadStreamBlockNumberCB for block range scans.  Loops over the
 * blocks [current_blocknum, last_exclusive).
 */
#[no_mangle]
pub unsafe extern "C" fn block_range_read_stream_cb(
    _stream: *mut ReadStream,
    callback_private_data: *mut c_void,
    _per_buffer_data: *mut c_void,
) -> BlockNumber {
    let p = callback_private_data as *mut BlockRangeReadStreamPrivate;

    if (*p).current_blocknum < (*p).last_exclusive {
        let r = (*p).current_blocknum;
        (*p).current_blocknum += 1;
        return r;
    }

    InvalidBlockNumber
}

/*
 * Ask the callback which block it would like us to read next, with a one block
 * buffer in front to allow read_stream_unget_block() to work.
 */
#[inline]
unsafe fn read_stream_get_block(
    stream: *mut ReadStream,
    per_buffer_data: *mut c_void,
) -> BlockNumber {
    let blocknum;

    let buffered = (*stream).buffered_blocknum;
    if buffered != InvalidBlockNumber {
        (*stream).buffered_blocknum = InvalidBlockNumber;
        blocknum = buffered;
    } else {
        /*
         * Tell Valgrind that the per-buffer data is undefined.  That replaces
         * the "noaccess" state that was set when the consumer moved past this
         * entry last time around the queue, and should also catch callbacks
         * that fail to initialize data that the buffer consumer later
         * accesses.  On the first go around, it is undefined already.
         */
        VALGRIND_MAKE_MEM_UNDEFINED(per_buffer_data, (*stream).per_buffer_data_size);
        blocknum = ((*stream).callback.unwrap())(
            stream,
            (*stream).callback_private_data,
            per_buffer_data,
        );
    }

    blocknum
}

/*
 * In order to deal with buffer shortages and I/O limits after short reads, we
 * sometimes need to defer handling of a block we've already consumed from the
 * registered callback until later.
 */
#[inline]
unsafe fn read_stream_unget_block(stream: *mut ReadStream, blocknum: BlockNumber) {
    /* We shouldn't ever unget more than one block. */
    Assert!((*stream).buffered_blocknum == InvalidBlockNumber);
    Assert!(blocknum != InvalidBlockNumber);
    (*stream).buffered_blocknum = blocknum;
}

/*
 * Start as much of the current pending read as we can.  If we have to split it
 * because of the per-backend buffer limit, or the buffer manager decides to
 * split it, then the pending read is adjusted to hold the remaining portion.
 *
 * We can always start a read of at least size one if we have no progress yet.
 * Otherwise it's possible that we can't start a read at all because of a lack
 * of buffers, and then false is returned.  Buffer shortages also reduce the
 * distance to a level that prevents look-ahead until buffers are released.
 */
unsafe fn read_stream_start_pending_read(stream: *mut ReadStream) -> bool {
    let need_wait: bool;
    let requested_nblocks: c_int;
    let mut nblocks: c_int;
    let mut flags: c_int;
    let mut forwarded: c_int;
    let io_index: int16;
    let overflow: int16;
    let mut buffer_index: int16;
    let mut buffer_limit: c_int;

    /* This should only be called with a pending read. */
    Assert!((*stream).pending_read_nblocks > 0);
    Assert!((*stream).pending_read_nblocks <= (*stream).io_combine_limit);

    /* We had better not exceed the per-stream buffer limit with this read. */
    Assert!(
        (*stream).pinned_buffers + (*stream).pending_read_nblocks <= (*stream).max_pinned_buffers
    );

    // USE_ASSERT_CHECKING block omitted (assertions only)

    /* Do we need to issue read-ahead advice? */
    flags = (*stream).read_buffers_flags;
    if (*stream).advice_enabled {
        if (*stream).pending_read_blocknum == (*stream).seq_blocknum {
            /*
             * Sequential:  Issue advice until the preadv() calls have caught
             * up with the first advice issued for this sequential region, and
             * then stay of the way of the kernel's own read-ahead.
             */
            if (*stream).seq_until_processed != InvalidBlockNumber {
                flags |= READ_BUFFERS_ISSUE_ADVICE;
            }
        } else {
            /*
             * Random jump:  Note the starting location of a new potential
             * sequential region and start issuing advice.  Skip it this time
             * if the preadv() follows immediately, eg first block in stream.
             */
            (*stream).seq_until_processed = (*stream).pending_read_blocknum;
            if (*stream).pinned_buffers > 0 {
                flags |= READ_BUFFERS_ISSUE_ADVICE;
            }
        }
    }

    /*
     * How many more buffers is this backend allowed?
     *
     * Forwarded buffers are already pinned and map to the leading blocks of
     * the pending read (the remaining portion of an earlier short read that
     * we're about to continue).  They are not counted in pinned_buffers, but
     * they are counted as pins already held by this backend according to the
     * buffer manager, so they must be added to the limit it grants us.
     */
    if (*stream).temporary {
        buffer_limit = Min(GetAdditionalLocalPinLimit(), PG_INT16_MAX as uint32) as c_int;
    } else {
        buffer_limit = Min(GetAdditionalPinLimit(), PG_INT16_MAX as uint32) as c_int;
    }
    Assert!((*stream).forwarded_buffers <= (*stream).pending_read_nblocks);

    buffer_limit += (*stream).forwarded_buffers as c_int;
    buffer_limit = Min(buffer_limit, PG_INT16_MAX);

    if buffer_limit == 0 && (*stream).pinned_buffers == 0 {
        buffer_limit = 1; /* guarantee progress */
    }

    /* Does the per-backend limit affect this read? */
    nblocks = (*stream).pending_read_nblocks as c_int;
    if buffer_limit < nblocks {
        let new_distance: int16;

        /* Shrink distance: no more look-ahead until buffers are released. */
        new_distance = (*stream).pinned_buffers + buffer_limit as int16;
        if (*stream).distance > new_distance {
            (*stream).distance = new_distance;
        }

        /* Unless we have nothing to give the consumer, stop here. */
        if (*stream).pinned_buffers > 0 {
            return false;
        }

        /* A short read is required to make progress. */
        nblocks = buffer_limit;
    }

    /*
     * We say how many blocks we want to read, but it may be smaller on return
     * if the buffer manager decides to shorten the read.  Initialize buffers
     * to InvalidBuffer (= not a forwarded buffer) as input on first use only,
     * and keep the original nblocks number so we can check for forwarded
     * buffers as output, below.
     */
    buffer_index = (*stream).next_buffer_index;
    io_index = (*stream).next_io_index;
    while (*stream).initialized_buffers < buffer_index + nblocks as int16 {
        buf_set(stream, (*stream).initialized_buffers as c_int, InvalidBuffer);
        (*stream).initialized_buffers += 1;
    }
    requested_nblocks = nblocks;
    need_wait = StartReadBuffers(
        &mut (*(*stream).ios.offset(io_index as isize)).op,
        buf_at(stream, buffer_index as c_int),
        (*stream).pending_read_blocknum,
        &mut nblocks,
        flags,
    );
    (*stream).pinned_buffers += nblocks as int16;

    /* Remember whether we need to wait before returning this buffer. */
    if !need_wait {
        /* Look-ahead distance decays, no I/O necessary. */
        if (*stream).distance > 1 {
            (*stream).distance -= 1;
        }
    } else {
        /*
         * Remember to call WaitReadBuffers() before returning head buffer.
         * Look-ahead distance will be adjusted after waiting.
         */
        (*(*stream).ios.offset(io_index as isize)).buffer_index = buffer_index;
        (*stream).next_io_index += 1;
        if (*stream).next_io_index == (*stream).max_ios {
            (*stream).next_io_index = 0;
        }
        Assert!((*stream).ios_in_progress < (*stream).max_ios);
        (*stream).ios_in_progress += 1;
        (*stream).seq_blocknum = (*stream).pending_read_blocknum + nblocks as BlockNumber;
    }

    /*
     * How many pins were acquired but forwarded to the next call?  These need
     * to be passed to the next StartReadBuffers() call by leaving them
     * exactly where they are in the queue, or released if the stream ends
     * early.  We need the number for accounting purposes, since they are not
     * counted in stream->pinned_buffers but we already hold them.
     */
    forwarded = 0;
    while nblocks + forwarded < requested_nblocks
        && buf_get(stream, buffer_index as c_int + nblocks + forwarded) != InvalidBuffer
    {
        forwarded += 1;
    }
    (*stream).forwarded_buffers = forwarded as int16;

    /*
     * We gave a contiguous range of buffer space to StartReadBuffers(), but
     * we want it to wrap around at queue_size.  Copy overflowing buffers to
     * the front of the array where they'll be consumed, but also leave a copy
     * in the overflow zone which the I/O operation has a pointer to (it needs
     * a contiguous array).  Both copies will be cleared when the buffers are
     * handed to the consumer.
     */
    overflow = (buffer_index + nblocks as int16 + forwarded as int16) - (*stream).queue_size;
    if overflow > 0 {
        Assert!(overflow < (*stream).queue_size); /* can't overlap */
        std::ptr::copy_nonoverlapping(
            buf_at(stream, (*stream).queue_size as c_int),
            buf_at(stream, 0),
            overflow as usize,
        );
    }

    /* Compute location of start of next read, without using % operator. */
    buffer_index += nblocks as int16;
    if buffer_index >= (*stream).queue_size {
        buffer_index -= (*stream).queue_size;
    }
    Assert!(buffer_index >= 0 && buffer_index < (*stream).queue_size);
    (*stream).next_buffer_index = buffer_index;

    /* Adjust the pending read to cover the remaining portion, if any. */
    (*stream).pending_read_blocknum += nblocks as BlockNumber;
    (*stream).pending_read_nblocks -= nblocks as int16;

    true
}

unsafe fn read_stream_look_ahead(stream: *mut ReadStream) {
    /*
     * Allow amortizing the cost of submitting IO over multiple IOs. This
     * requires that we don't do any operations that could lead to a deadlock
     * with staged-but-unsubmitted IO. The callback needs to opt-in to being
     * careful.
     */
    if (*stream).batch_mode {
        pgaio_enter_batchmode();
    }

    while (*stream).ios_in_progress < (*stream).max_ios
        && (*stream).pinned_buffers + (*stream).pending_read_nblocks < (*stream).distance
    {
        let blocknum: BlockNumber;
        let mut buffer_index: int16;
        let per_buffer_data: *mut c_void;

        if (*stream).pending_read_nblocks == (*stream).io_combine_limit {
            read_stream_start_pending_read(stream);
            continue;
        }

        /*
         * See which block the callback wants next in the stream.  We need to
         * compute the index of the Nth block of the pending read including
         * wrap-around, but we don't want to use the expensive % operator.
         */
        buffer_index = (*stream).next_buffer_index + (*stream).pending_read_nblocks;
        if buffer_index >= (*stream).queue_size {
            buffer_index -= (*stream).queue_size;
        }
        Assert!(buffer_index >= 0 && buffer_index < (*stream).queue_size);
        per_buffer_data = get_per_buffer_data(stream, buffer_index);
        blocknum = read_stream_get_block(stream, per_buffer_data);
        if blocknum == InvalidBlockNumber {
            /* End of stream. */
            (*stream).distance = 0;
            break;
        }

        /* Can we merge it with the pending read? */
        if (*stream).pending_read_nblocks > 0
            && (*stream).pending_read_blocknum + (*stream).pending_read_nblocks as BlockNumber
                == blocknum
        {
            (*stream).pending_read_nblocks += 1;
            continue;
        }

        /* We have to start the pending read before we can build another. */
        while (*stream).pending_read_nblocks > 0 {
            if !read_stream_start_pending_read(stream)
                || (*stream).ios_in_progress == (*stream).max_ios
            {
                /* We've hit the buffer or I/O limit.  Rewind and stop here. */
                read_stream_unget_block(stream, blocknum);
                if (*stream).batch_mode {
                    pgaio_exit_batchmode();
                }
                return;
            }
        }

        /* This is the start of a new pending read. */
        (*stream).pending_read_blocknum = blocknum;
        (*stream).pending_read_nblocks = 1;
    }

    /*
     * We don't start the pending read just because we've hit the distance
     * limit, preferring to give it another chance to grow to full
     * io_combine_limit size once more buffers have been consumed.  However,
     * if we've already reached io_combine_limit, or we've reached the
     * distance limit and there isn't anything pinned yet, or the callback has
     * signaled end-of-stream, we start the read immediately.  Note that the
     * pending read can exceed the distance goal, if the latter was reduced
     * after hitting the per-backend buffer limit.
     */
    if (*stream).pending_read_nblocks > 0
        && ((*stream).pending_read_nblocks == (*stream).io_combine_limit
            || ((*stream).pending_read_nblocks >= (*stream).distance
                && (*stream).pinned_buffers == 0)
            || (*stream).distance == 0)
        && (*stream).ios_in_progress < (*stream).max_ios
    {
        read_stream_start_pending_read(stream);
    }

    /*
     * There should always be something pinned when we leave this function,
     * whether started by this call or not, unless we've hit the end of the
     * stream.  In the worst case we can always make progress one buffer at a
     * time.
     */
    Assert!((*stream).pinned_buffers > 0 || (*stream).distance == 0);

    if (*stream).batch_mode {
        pgaio_exit_batchmode();
    }
}

/*
 * Create a new read stream object that can be used to perform the equivalent
 * of a series of ReadBuffer() calls for one fork of one relation.
 * Internally, it generates larger vectored reads where possible by looking
 * ahead.  The callback should return block numbers or InvalidBlockNumber to
 * signal end-of-stream, and if per_buffer_data_size is non-zero, it may also
 * write extra data for each block into the space provided to it.  It will
 * also receive callback_private_data for its own purposes.
 */
unsafe fn read_stream_begin_impl(
    flags: c_int,
    strategy: BufferAccessStrategy,
    rel: Relation,
    smgr: SMgrRelation,
    persistence: c_char,
    forknum: ForkNumber,
    callback: ReadStreamBlockNumberCB,
    callback_private_data: *mut c_void,
    per_buffer_data_size: Size,
) -> *mut ReadStream {
    let stream: *mut ReadStream;
    let mut size: Size;
    let queue_size: int16;
    let queue_overflow: int16;
    let mut max_ios: c_int;
    let strategy_pin_limit: c_int;
    let mut max_pinned_buffers: uint32;
    let max_possible_buffer_limit: uint32;
    let tablespace_id: Oid;

    /*
     * Decide how many I/Os we will allow to run at the same time.  That
     * currently means advice to the kernel to tell it that we will soon read.
     * This number also affects how far we look ahead for opportunities to
     * start more I/Os.
     */
    tablespace_id = smgr_rlocator_spcOid(smgr);
    if !OidIsValid(MyDatabaseId)
        || (!rel.is_null() && IsCatalogRelation(rel))
        || IsCatalogRelationOid(smgr_rlocator_relNumber(smgr))
    {
        /*
         * Avoid circularity while trying to look up tablespace settings or
         * before spccache.c is ready.
         */
        max_ios = effective_io_concurrency;
    } else if (flags & READ_STREAM_MAINTENANCE) != 0 {
        max_ios = get_tablespace_maintenance_io_concurrency(tablespace_id);
    } else {
        max_ios = get_tablespace_io_concurrency(tablespace_id);
    }

    /* Cap to INT16_MAX to avoid overflowing below */
    max_ios = Min(max_ios, PG_INT16_MAX);

    /*
     * If starting a multi-block I/O near the end of the queue, we might
     * temporarily need extra space for overflowing buffers before they are
     * moved to regular circular position.  This is the maximum extra space we
     * could need.
     */
    queue_overflow = (io_combine_limit - 1) as int16;

    /*
     * Choose the maximum number of buffers we're prepared to pin.  We try to
     * pin fewer if we can, though.  We add one so that we can make progress
     * even if max_ios is set to 0 (see also further down).  For max_ios > 0,
     * this also allows an extra full I/O's worth of buffers: after an I/O
     * finishes we don't want to have to wait for its buffers to be consumed
     * before starting a new one.
     *
     * Be careful not to allow int16 to overflow.  That is possible with the
     * current GUC range limits, so this is an artificial limit of ~32k
     * buffers and we'd need to adjust the types to exceed that.  We also have
     * to allow for the spare entry and the overflow space.
     */
    max_pinned_buffers = (max_ios as uint32 + 1) * io_combine_limit as uint32;
    max_pinned_buffers = Min(
        max_pinned_buffers,
        (PG_INT16_MAX - queue_overflow as c_int - 1) as uint32,
    );

    /* Give the strategy a chance to limit the number of buffers we pin. */
    strategy_pin_limit = GetAccessStrategyPinLimit(strategy);
    max_pinned_buffers = Min(strategy_pin_limit as uint32, max_pinned_buffers);

    /*
     * Also limit our queue to the maximum number of pins we could ever be
     * allowed to acquire according to the buffer manager.  We may not really
     * be able to use them all due to other pins held by this backend, but
     * we'll check that later in read_stream_start_pending_read().
     */
    if SmgrIsTemp(smgr) {
        max_possible_buffer_limit = GetLocalPinLimit();
    } else {
        max_possible_buffer_limit = GetPinLimit();
    }
    max_pinned_buffers = Min(max_pinned_buffers, max_possible_buffer_limit);

    /*
     * The limit might be zero on a system configured with too few buffers for
     * the number of connections.  We need at least one to make progress.
     */
    max_pinned_buffers = Max(1, max_pinned_buffers);

    /*
     * We need one extra entry for buffers and per-buffer data, because users
     * of per-buffer data have access to the object until the next call to
     * read_stream_next_buffer(), so we need a gap between the head and tail
     * of the queue so that we don't clobber it.
     */
    queue_size = (max_pinned_buffers + 1) as int16;

    /*
     * Allocate the object, the buffers, the ios and per_buffer_data space in
     * one big chunk.  Though we have queue_size buffers, we want to be able
     * to assume that all the buffers for a single read are contiguous (i.e.
     * don't wrap around halfway through), so we allow temporary overflows of
     * up to the maximum possible overflow size.
     */
    size = core::mem::offset_of!(ReadStream, buffers);
    size += core::mem::size_of::<Buffer>() * (queue_size as Size + queue_overflow as Size);
    size += core::mem::size_of::<InProgressIO>() * Max(1, max_ios) as Size;
    size += per_buffer_data_size * queue_size as Size;
    size += MAXIMUM_ALIGNOF * 2;
    stream = palloc(size) as *mut ReadStream;
    std::ptr::write_bytes(
        stream as *mut u8,
        0,
        core::mem::offset_of!(ReadStream, buffers),
    );
    (*stream).ios = MAXALIGN(
        buf_at(stream, queue_size as c_int + queue_overflow as c_int) as usize,
    ) as *mut InProgressIO;
    if per_buffer_data_size > 0 {
        (*stream).per_buffer_data =
            MAXALIGN((*stream).ios.offset(Max(1, max_ios) as isize) as usize) as *mut c_void;
    }

    (*stream).sync_mode = io_method == IOMETHOD_SYNC;
    (*stream).batch_mode = (flags & READ_STREAM_USE_BATCHING) != 0;

    // USE_PREFETCH
    /*
     * Read-ahead advice simulating asynchronous I/O with synchronous calls.
     * Issue advice only if AIO is not used, direct I/O isn't enabled, the
     * caller hasn't promised sequential access (overriding our detection
     * heuristics), and max_ios hasn't been set to zero.
     */
    if (*stream).sync_mode
        && (io_direct_flags & IO_DIRECT_DATA) == 0
        && (flags & READ_STREAM_SEQUENTIAL) == 0
        && max_ios > 0
    {
        (*stream).advice_enabled = true;
    }

    /*
     * Setting max_ios to zero disables AIO and advice-based pseudo AIO, but
     * we still need to allocate space to combine and run one I/O.  Bump it up
     * to one, and remember to ask for synchronous I/O only.
     */
    if max_ios == 0 {
        max_ios = 1;
        (*stream).read_buffers_flags = READ_BUFFERS_SYNCHRONOUSLY;
    }

    /*
     * Capture stable values for these two GUC-derived numbers for the
     * lifetime of this stream, so we don't have to worry about the GUCs
     * changing underneath us beyond this point.
     */
    (*stream).max_ios = max_ios as int16;
    (*stream).io_combine_limit = io_combine_limit as int16;

    (*stream).per_buffer_data_size = per_buffer_data_size;
    (*stream).max_pinned_buffers = max_pinned_buffers as int16;
    (*stream).queue_size = queue_size;
    (*stream).callback = callback;
    (*stream).callback_private_data = callback_private_data;
    (*stream).buffered_blocknum = InvalidBlockNumber;
    (*stream).seq_blocknum = InvalidBlockNumber;
    (*stream).seq_until_processed = InvalidBlockNumber;
    (*stream).temporary = SmgrIsTemp(smgr);

    /*
     * Skip the initial ramp-up phase if the caller says we're going to be
     * reading the whole relation.  This way we start out assuming we'll be
     * doing full io_combine_limit sized reads.
     */
    if (flags & READ_STREAM_FULL) != 0 {
        (*stream).distance = Min(max_pinned_buffers as int16, (*stream).io_combine_limit);
    } else {
        (*stream).distance = 1;
    }

    /*
     * Since we always access the same relation, we can initialize parts of
     * the ReadBuffersOperation objects and leave them that way, to avoid
     * wasting CPU cycles writing to them for each read.
     */
    for i in 0..max_ios {
        let op = &mut (*(*stream).ios.offset(i as isize)).op;
        op.rel = rel;
        op.smgr = smgr;
        op.persistence = persistence;
        op.forknum = forknum;
        op.strategy = strategy;
    }

    stream
}

/*
 * Create a new read stream for reading a relation.
 * See read_stream_begin_impl() for the detailed explanation.
 */
#[no_mangle]
pub unsafe extern "C" fn read_stream_begin_relation(
    flags: c_int,
    strategy: BufferAccessStrategy,
    rel: Relation,
    forknum: ForkNumber,
    callback: ReadStreamBlockNumberCB,
    callback_private_data: *mut c_void,
    per_buffer_data_size: Size,
) -> *mut ReadStream {
    read_stream_begin_impl(
        flags,
        strategy,
        rel,
        RelationGetSmgr(rel),
        rel_relpersistence(rel),
        forknum,
        callback,
        callback_private_data,
        per_buffer_data_size,
    )
}

/*
 * Create a new read stream for reading a SMgr relation.
 * See read_stream_begin_impl() for the detailed explanation.
 */
#[no_mangle]
pub unsafe extern "C" fn read_stream_begin_smgr_relation(
    flags: c_int,
    strategy: BufferAccessStrategy,
    smgr: SMgrRelation,
    smgr_persistence: c_char,
    forknum: ForkNumber,
    callback: ReadStreamBlockNumberCB,
    callback_private_data: *mut c_void,
    per_buffer_data_size: Size,
) -> *mut ReadStream {
    read_stream_begin_impl(
        flags,
        strategy,
        std::ptr::null_mut(),
        smgr,
        smgr_persistence,
        forknum,
        callback,
        callback_private_data,
        per_buffer_data_size,
    )
}

/*
 * Pull one pinned buffer out of a stream.  Each call returns successive
 * blocks in the order specified by the callback.  If per_buffer_data_size was
 * set to a non-zero size, *per_buffer_data receives a pointer to the extra
 * per-buffer data that the callback had a chance to populate, which remains
 * valid until the next call to read_stream_next_buffer().  When the stream
 * runs out of data, InvalidBuffer is returned.  The caller may decide to end
 * the stream early at any time by calling read_stream_end().
 */
#[no_mangle]
pub unsafe extern "C" fn read_stream_next_buffer(
    stream: *mut ReadStream,
    per_buffer_data: *mut *mut c_void,
) -> Buffer {
    let buffer: Buffer;
    let oldest_buffer_index: int16;

    // READ_STREAM_DISABLE_FAST_PATH not defined
    /*
     * A fast path for all-cached scans.  This is the same as the usual
     * algorithm, but it is specialized for no I/O and no per-buffer data, so
     * we can skip the queue management code, stay in the same buffer slot and
     * use singular StartReadBuffer().
     */
    if likely((*stream).fast_path) {
        let next_blocknum: BlockNumber;

        /* Fast path assumptions. */
        Assert!((*stream).ios_in_progress == 0);
        Assert!((*stream).forwarded_buffers == 0);
        Assert!((*stream).pinned_buffers == 1);
        Assert!((*stream).distance == 1);
        Assert!((*stream).pending_read_nblocks == 0);
        Assert!((*stream).per_buffer_data_size == 0);
        Assert!((*stream).initialized_buffers > (*stream).oldest_buffer_index);

        /* We're going to return the buffer we pinned last time. */
        let obi = (*stream).oldest_buffer_index;
        Assert!((obi + 1) % (*stream).queue_size == (*stream).next_buffer_index);
        buffer = buf_get(stream, obi as c_int);
        Assert!(buffer != InvalidBuffer);

        /* Choose the next block to pin. */
        next_blocknum = read_stream_get_block(stream, std::ptr::null_mut());

        if likely(next_blocknum != InvalidBlockNumber) {
            let mut flags = (*stream).read_buffers_flags;

            if (*stream).advice_enabled {
                flags |= READ_BUFFERS_ISSUE_ADVICE;
            }

            /*
             * Pin a buffer for the next call.  Same buffer entry, and
             * arbitrary I/O entry (they're all free).  We don't have to
             * adjust pinned_buffers because we're transferring one to caller
             * but pinning one more.
             *
             * In the fast path we don't need to check the pin limit.  We're
             * always allowed at least one pin so that progress can be made,
             * and that's all we need here.  Although two pins are momentarily
             * held at the same time, the model used here is that the stream
             * holds only one, and the other now belongs to the caller.
             */
            if likely(!StartReadBuffer(
                &mut (*(*stream).ios.offset(0)).op,
                buf_at(stream, obi as c_int),
                next_blocknum,
                flags,
            )) {
                /* Fast return. */
                return buffer;
            }

            /* Next call must wait for I/O for the newly pinned buffer. */
            (*stream).oldest_io_index = 0;
            (*stream).next_io_index = if (*stream).max_ios > 1 { 1 } else { 0 };
            (*stream).ios_in_progress = 1;
            (*(*stream).ios.offset(0)).buffer_index = obi;
            (*stream).seq_blocknum = next_blocknum + 1;
        } else {
            /* No more blocks, end of stream. */
            (*stream).distance = 0;
            (*stream).oldest_buffer_index = (*stream).next_buffer_index;
            (*stream).pinned_buffers = 0;
            buf_set(stream, obi as c_int, InvalidBuffer);
        }

        (*stream).fast_path = false;
        return buffer;
    }

    if unlikely((*stream).pinned_buffers == 0) {
        Assert!((*stream).oldest_buffer_index == (*stream).next_buffer_index);

        /* End of stream reached?  */
        if (*stream).distance == 0 {
            return InvalidBuffer;
        }

        /*
         * The usual order of operations is that we look ahead at the bottom
         * of this function after potentially finishing an I/O and making
         * space for more, but if we're just starting up we'll need to crank
         * the handle to get started.
         */
        read_stream_look_ahead(stream);

        /* End of stream reached? */
        if (*stream).pinned_buffers == 0 {
            Assert!((*stream).distance == 0);
            return InvalidBuffer;
        }
    }

    /* Grab the oldest pinned buffer and associated per-buffer data. */
    Assert!((*stream).pinned_buffers > 0);
    oldest_buffer_index = (*stream).oldest_buffer_index;
    Assert!(oldest_buffer_index >= 0 && oldest_buffer_index < (*stream).queue_size);
    buffer = buf_get(stream, oldest_buffer_index as c_int);
    if !per_buffer_data.is_null() {
        *per_buffer_data = get_per_buffer_data(stream, oldest_buffer_index);
    }

    Assert!(BufferIsValid(buffer));

    /* Do we have to wait for an associated I/O first? */
    if (*stream).ios_in_progress > 0
        && (*(*stream).ios.offset((*stream).oldest_io_index as isize)).buffer_index
            == oldest_buffer_index
    {
        let io_index = (*stream).oldest_io_index;
        let mut distance: int32; /* wider temporary value, clamped below */

        /* Sanity check that we still agree on the buffers. */
        Assert!(
            (*(*stream).ios.offset(io_index as isize)).op.buffers
                == buf_at(stream, oldest_buffer_index as c_int)
        );

        WaitReadBuffers(&mut (*(*stream).ios.offset(io_index as isize)).op);

        Assert!((*stream).ios_in_progress > 0);
        (*stream).ios_in_progress -= 1;
        (*stream).oldest_io_index += 1;
        if (*stream).oldest_io_index == (*stream).max_ios {
            (*stream).oldest_io_index = 0;
        }

        /* Look-ahead distance ramps up rapidly after we do I/O. */
        distance = (*stream).distance as int32 * 2;
        distance = Min(distance, (*stream).max_pinned_buffers as int32);
        (*stream).distance = distance as int16;

        /*
         * If we've reached the first block of a sequential region we're
         * issuing advice for, cancel that until the next jump.  The kernel
         * will see the sequential preadv() pattern starting here.
         */
        if (*stream).advice_enabled
            && (*(*stream).ios.offset(io_index as isize)).op.blocknum
                == (*stream).seq_until_processed
        {
            (*stream).seq_until_processed = InvalidBlockNumber;
        }
    }

    /*
     * We must zap this queue entry, or else it would appear as a forwarded
     * buffer.  If it's potentially in the overflow zone (ie from a
     * multi-block I/O that wrapped around the queue), also zap the copy.
     */
    buf_set(stream, oldest_buffer_index as c_int, InvalidBuffer);
    if oldest_buffer_index < (*stream).io_combine_limit - 1 {
        buf_set(
            stream,
            (*stream).queue_size as c_int + oldest_buffer_index as c_int,
            InvalidBuffer,
        );
    }

    // CLOBBER_FREED_MEMORY / USE_VALGRIND
    /*
     * The caller will get access to the per-buffer data, until the next call.
     * We wipe the one before, which is never occupied because queue_size
     * allowed one extra element.  This will hopefully trip up client code
     * that is holding a dangling pointer to it.
     */
    if !(*stream).per_buffer_data.is_null() {
        let pbd = get_per_buffer_data(
            stream,
            if oldest_buffer_index == 0 {
                (*stream).queue_size - 1
            } else {
                oldest_buffer_index - 1
            },
        );

        // Under USE_VALGRIND: tell it the memory is noaccess.
        VALGRIND_MAKE_MEM_NOACCESS(pbd, (*stream).per_buffer_data_size);
    }

    /* Pin transferred to caller. */
    Assert!((*stream).pinned_buffers > 0);
    (*stream).pinned_buffers -= 1;

    /* Advance oldest buffer, with wrap-around. */
    (*stream).oldest_buffer_index += 1;
    if (*stream).oldest_buffer_index == (*stream).queue_size {
        (*stream).oldest_buffer_index = 0;
    }

    /* Prepare for the next call. */
    read_stream_look_ahead(stream);

    // READ_STREAM_DISABLE_FAST_PATH not defined
    /* See if we can take the fast path for all-cached scans next time. */
    if (*stream).ios_in_progress == 0
        && (*stream).forwarded_buffers == 0
        && (*stream).pinned_buffers == 1
        && (*stream).distance == 1
        && (*stream).pending_read_nblocks == 0
        && (*stream).per_buffer_data_size == 0
    {
        /*
         * The fast path spins on one buffer entry repeatedly instead of
         * rotating through the whole queue and clearing the entries behind
         * it.  If the buffer it starts with happened to be forwarded between
         * StartReadBuffers() calls and also wrapped around the circular queue
         * partway through, then a copy also exists in the overflow zone, and
         * it won't clear it out as the regular path would.  Do that now, so
         * it doesn't need code for that.
         */
        if (*stream).oldest_buffer_index < (*stream).io_combine_limit - 1 {
            buf_set(
                stream,
                (*stream).queue_size as c_int + (*stream).oldest_buffer_index as c_int,
                InvalidBuffer,
            );
        }

        (*stream).fast_path = true;
    }

    buffer
}

/*
 * Transitional support for code that would like to perform or skip reads
 * itself, without using the stream.  Returns, and consumes, the next block
 * number that would be read by the stream's look-ahead algorithm, or
 * InvalidBlockNumber if the end of the stream is reached.  Also reports the
 * strategy that would be used to read it.
 */
#[no_mangle]
pub unsafe extern "C" fn read_stream_next_block(
    stream: *mut ReadStream,
    strategy: *mut BufferAccessStrategy,
) -> BlockNumber {
    *strategy = (*(*stream).ios.offset(0)).op.strategy;
    read_stream_get_block(stream, std::ptr::null_mut())
}

/*
 * Reset a read stream by releasing any queued up buffers, allowing the stream
 * to be used again for different blocks.  This can be used to clear an
 * end-of-stream condition and start again, or to throw away blocks that were
 * speculatively read and read some different blocks instead.
 */
#[no_mangle]
pub unsafe extern "C" fn read_stream_reset(stream: *mut ReadStream) {
    let mut index: int16;
    let mut buffer: Buffer;

    /* Stop looking ahead. */
    (*stream).distance = 0;

    /* Forget buffered block number and fast path state. */
    (*stream).buffered_blocknum = InvalidBlockNumber;
    (*stream).fast_path = false;

    /* Unpin anything that wasn't consumed. */
    loop {
        buffer = read_stream_next_buffer(stream, std::ptr::null_mut());
        if buffer == InvalidBuffer {
            break;
        }
        ReleaseBuffer(buffer);
    }

    /* Unpin any unused forwarded buffers. */
    index = (*stream).next_buffer_index;
    while index < (*stream).initialized_buffers && {
        buffer = buf_get(stream, index as c_int);
        buffer != InvalidBuffer
    } {
        Assert!((*stream).forwarded_buffers > 0);
        (*stream).forwarded_buffers -= 1;
        ReleaseBuffer(buffer);

        buf_set(stream, index as c_int, InvalidBuffer);
        if index < (*stream).io_combine_limit - 1 {
            buf_set(
                stream,
                (*stream).queue_size as c_int + index as c_int,
                InvalidBuffer,
            );
        }

        index += 1;
        if index == (*stream).queue_size {
            index = 0;
        }
    }

    Assert!((*stream).forwarded_buffers == 0);
    Assert!((*stream).pinned_buffers == 0);
    Assert!((*stream).ios_in_progress == 0);

    /* Start off assuming data is cached. */
    (*stream).distance = 1;
}

/*
 * Release and free a read stream.
 */
#[no_mangle]
pub unsafe extern "C" fn read_stream_end(stream: *mut ReadStream) {
    read_stream_reset(stream);
    pfree(stream as *mut c_void);
}

// ---------------------------------------------------------------------------
// Inline-ish helpers from c.h that are simple enough to define locally.
// ---------------------------------------------------------------------------
// Min, Max, OidIsValid, likely, unlikely come from crate::c via the prelude.

#[inline]
fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// Assert! is the #[macro_export] macro from crate::c, brought in via the prelude.

#[inline]
fn BufferIsValid(bufnum: Buffer) -> bool {
    bufnum != InvalidBuffer
}

#[inline]
#[allow(unused_variables)]
unsafe fn VALGRIND_MAKE_MEM_UNDEFINED(addr: *mut c_void, len: Size) {
    /* no-op without USE_VALGRIND */
}

#[inline]
#[allow(unused_variables)]
unsafe fn VALGRIND_MAKE_MEM_NOACCESS(addr: *mut c_void, len: Size) {
    /* no-op without USE_VALGRIND */
}

// InvalidOid comes from crate::postgres_ext via the prelude.

// ---------------------------------------------------------------------------
// GUCs (globals defined elsewhere) - local extern-style stubs.
// ---------------------------------------------------------------------------
static effective_io_concurrency: c_int = 0; // TODO: storage/bufmgr.h
static io_combine_limit: c_int = 16; // TODO: storage/bufmgr.h
static io_method: c_int = IOMETHOD_SYNC; // TODO: storage/aio.h
static io_direct_flags: c_int = 0; // TODO: common/file_utils.h

// MyDatabaseId (miscadmin.h) - stub
static MyDatabaseId: Oid = 0; // TODO: miscadmin.h

// ---------------------------------------------------------------------------
// Stubs for functions not yet ported.
// ---------------------------------------------------------------------------
unsafe fn smgr_rlocator_spcOid(_smgr: SMgrRelation) -> Oid {
    (*(_smgr as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_rlocator.locator.spcOid
}

unsafe fn smgr_rlocator_relNumber(_smgr: SMgrRelation) -> Oid {
    (*(_smgr as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_rlocator.locator.relNumber
}

unsafe fn rel_relpersistence(_rel: Relation) -> c_char {
    (*(*(_rel as crate::utils::rel::Relation)).rd_rel).relpersistence
}

unsafe fn IsCatalogRelation(_rel: Relation) -> bool {
    crate::catalog::catalog::IsCatalogRelation(_rel as _)
}

unsafe fn IsCatalogRelationOid(_relid: Oid) -> bool {
    crate::catalog::catalog::IsCatalogRelationOid(_relid)
}

unsafe fn get_tablespace_maintenance_io_concurrency(_spcid: Oid) -> c_int {
    crate::utils::cache::spccache::get_tablespace_maintenance_io_concurrency(_spcid)
}

unsafe fn get_tablespace_io_concurrency(_spcid: Oid) -> c_int {
    crate::utils::cache::spccache::get_tablespace_io_concurrency(_spcid)
}

unsafe fn GetAccessStrategyPinLimit(_strategy: BufferAccessStrategy) -> c_int {
    crate::storage::buffer::freelist::GetAccessStrategyPinLimit(_strategy as _)
}

unsafe fn SmgrIsTemp(_smgr: SMgrRelation) -> bool {
    crate::storage::smgr::smgr::SmgrIsTemp(_smgr as _)
}

unsafe fn GetLocalPinLimit() -> uint32 {
    crate::storage::buffer::localbuf::GetLocalPinLimit()
}

unsafe fn GetPinLimit() -> uint32 {
    crate::storage::buffer::bufmgr::GetPinLimit()
}

unsafe fn GetAdditionalLocalPinLimit() -> uint32 {
    crate::storage::buffer::localbuf::GetAdditionalLocalPinLimit()
}

unsafe fn GetAdditionalPinLimit() -> uint32 {
    crate::storage::buffer::bufmgr::GetAdditionalPinLimit()
}

unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation {
    crate::storage::buffer::bufmgr::RelationGetSmgr(_rel as _) as _
}

unsafe fn StartReadBuffers(
    _operation: *mut ReadBuffersOperation,
    _buffers: *mut Buffer,
    _blocknum: BlockNumber,
    _nblocks: *mut c_int,
    _flags: c_int,
) -> bool {
    crate::storage::buffer::bufmgr::StartReadBuffers(_operation, _buffers, _blocknum, _nblocks, _flags)
}

unsafe fn StartReadBuffer(
    _operation: *mut ReadBuffersOperation,
    _buffer: *mut Buffer,
    _blocknum: BlockNumber,
    _flags: c_int,
) -> bool {
    crate::storage::buffer::bufmgr::StartReadBuffer(_operation, _buffer, _blocknum, _flags)
}

unsafe fn WaitReadBuffers(_operation: *mut ReadBuffersOperation) {
    crate::storage::buffer::bufmgr::WaitReadBuffers(_operation)
}

unsafe fn ReleaseBuffer(_buffer: Buffer) {
    crate::storage::buffer::bufmgr::ReleaseBuffer(_buffer)
}

unsafe fn pgaio_enter_batchmode() {
    crate::storage::aio::aio::pgaio_enter_batchmode()
}

unsafe fn pgaio_exit_batchmode() {
    crate::storage::aio::aio::pgaio_exit_batchmode()
}
