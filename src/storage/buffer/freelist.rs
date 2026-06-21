//! src/backend/storage/buffer/freelist.c
//!
//! routines for managing the buffer pool's replacement strategy.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// #include "pgstat.h"
// #include "port/atomics.h"
// #include "storage/buf_internals.h"
// #include "storage/bufmgr.h"
// #include "storage/proc.h"

// --- types pulled in from headers (stubbed where not yet ported) ---

type slock_t = c_int;

#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: u32, // volatile uint32
}

type Buffer = c_int;
type IOContext = c_int;
type BufferAccessStrategyType = c_int;

// values from storage/bufmgr.h
const BAS_NORMAL: BufferAccessStrategyType = 0;
const BAS_BULKREAD: BufferAccessStrategyType = 1;
const BAS_BULKWRITE: BufferAccessStrategyType = 2;
const BAS_VACUUM: BufferAccessStrategyType = 3;

// values from pgstat.h IOContext
const IOCONTEXT_NORMAL: IOContext = 0;
const IOCONTEXT_BULKREAD: IOContext = 1;
const IOCONTEXT_BULKWRITE: IOContext = 2;
const IOCONTEXT_VACUUM: IOContext = 3;

// from storage/buf_internals.h
const FREENEXT_NOT_IN_LIST: c_int = -2;
const InvalidBuffer: Buffer = 0;
const NUM_BUFFER_PARTITIONS: c_int = 128;

// BufferDesc (storage/buf_internals.h) -- use the canonical layout. A local
// stub here had buf_id/freeNext at the wrong offsets, so freelist writes to
// freeNext clobbered the real descriptor's tag.dbOid (offset 4).
use crate::storage::buf_internals::BufferDesc;

// BufferAccessStrategy is a pointer to BufferAccessStrategyData
type BufferAccessStrategy = *mut BufferAccessStrategyData;

macro_rules! INT_ACCESS_ONCE {
    ($var:expr) => {
        unsafe { ::std::ptr::read_volatile(&($var) as *const _ as *const ::std::ffi::c_int) }
    };
}

/*
 * The shared freelist control information.
 */
#[repr(C)]
pub struct BufferStrategyControl {
    /* Spinlock: protects the values below */
    pub buffer_strategy_lock: slock_t,

    /*
     * Clock sweep hand: index of next buffer to consider grabbing. Note that
     * this isn't a concrete buffer - we only ever increase the value. So, to
     * get an actual buffer, it needs to be used modulo NBuffers.
     */
    pub nextVictimBuffer: pg_atomic_uint32,

    pub firstFreeBuffer: c_int, /* Head of list of unused buffers */
    pub lastFreeBuffer: c_int,  /* Tail of list of unused buffers */

    /*
     * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
     * when the list is empty)
     */

    /*
     * Statistics.  These counters should be wide enough that they can't
     * overflow during a single bgwriter cycle.
     */
    pub completePasses: u32, /* Complete cycles of the clock sweep */
    pub numBufferAllocs: pg_atomic_uint32, /* Buffers allocated since last reset */

    /*
     * Bgworker process to be notified upon activity or -1 if none. See
     * StrategyNotifyBgWriter.
     */
    pub bgwprocno: c_int,
}

/* Pointers to shared state */
static mut StrategyControl: *mut BufferStrategyControl = std::ptr::null_mut();

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
#[repr(C)]
pub struct BufferAccessStrategyData {
    /* Overall strategy type */
    pub btype: BufferAccessStrategyType,
    /* Number of elements in buffers[] array */
    pub nbuffers: c_int,

    /*
     * Index of the "current" slot in the ring, ie, the one most recently
     * returned by GetBufferFromRing.
     */
    pub current: c_int,

    /*
     * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
     * have not yet selected a buffer for this ring slot.  For allocation
     * simplicity this is palloc'd together with the fixed fields of the
     * struct.
     */
    pub buffers: [Buffer; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
#[inline]
unsafe fn ClockSweepTick() -> u32 {
    let mut victim: u32;

    /*
     * Atomically move hand ahead one buffer - if there's several processes
     * doing this, this can lead to buffers being returned slightly out of
     * apparent order.
     */
    victim = pg_atomic_fetch_add_u32(&mut (*StrategyControl).nextVictimBuffer, 1);

    if victim >= NBuffers as u32 {
        let originalVictim: u32 = victim;

        /* always wrap what we look up in BufferDescriptors */
        victim = victim % NBuffers as u32;

        /*
         * If we're the one that just caused a wraparound, force
         * completePasses to be incremented while holding the spinlock. We
         * need the spinlock so StrategySyncStart() can return a consistent
         * value consisting of nextVictimBuffer and completePasses.
         */
        if victim == 0 {
            let mut expected: u32;
            let mut wrapped: u32;
            let mut success: bool = false;

            expected = originalVictim + 1;

            while !success {
                /*
                 * Acquire the spinlock while increasing completePasses. That
                 * allows other readers to read nextVictimBuffer and
                 * completePasses in a consistent manner which is required for
                 * StrategySyncStart().  In theory delaying the increment
                 * could lead to an overflow of nextVictimBuffers, but that's
                 * highly unlikely and wouldn't be particularly harmful.
                 */
                SpinLockAcquire(&mut (*StrategyControl).buffer_strategy_lock);

                wrapped = expected % NBuffers as u32;

                success = pg_atomic_compare_exchange_u32(
                    &mut (*StrategyControl).nextVictimBuffer,
                    &mut expected,
                    wrapped,
                );
                if success {
                    (*StrategyControl).completePasses += 1;
                }
                SpinLockRelease(&mut (*StrategyControl).buffer_strategy_lock);
            }
        }
    }
    victim
}

/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 */
#[no_mangle]
pub unsafe extern "C" fn have_free_buffer() -> bool {
    if (*StrategyControl).firstFreeBuffer >= 0 {
        true
    } else {
        false
    }
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
#[no_mangle]
pub unsafe extern "C" fn StrategyGetBuffer(
    strategy: BufferAccessStrategy,
    buf_state: *mut u32,
    from_ring: *mut bool,
) -> *mut BufferDesc {
    let mut buf: *mut BufferDesc;
    let bgwprocno: c_int;
    let mut trycounter: c_int;
    let mut local_buf_state: u32; /* to avoid repeated (de-)referencing */

    *from_ring = false;

    /*
     * If given a strategy object, see whether it can select a buffer. We
     * assume strategy objects don't need buffer_strategy_lock.
     */
    if !strategy.is_null() {
        buf = GetBufferFromRing(strategy, buf_state);
        if !buf.is_null() {
            *from_ring = true;
            return buf;
        }
    }

    /*
     * If asked, we need to waken the bgwriter. Since we don't want to rely on
     * a spinlock for this we force a read from shared memory once, and then
     * set the latch based on that value. We need to go through that length
     * because otherwise bgwprocno might be reset while/after we check because
     * the compiler might just reread from memory.
     *
     * This can possibly set the latch of the wrong process if the bgwriter
     * dies in the wrong moment. But since PGPROC->procLatch is never
     * deallocated the worst consequence of that is that we set the latch of
     * some arbitrary process.
     */
    bgwprocno = INT_ACCESS_ONCE!((*StrategyControl).bgwprocno);
    if bgwprocno != -1 {
        /* reset bgwprocno first, before setting the latch */
        (*StrategyControl).bgwprocno = -1;

        /*
         * Not acquiring ProcArrayLock here which is slightly icky. It's
         * actually fine because procLatch isn't ever freed, so we just can
         * potentially set the wrong process' (or no process') latch.
         */
        SetLatch(&mut (*(*ProcGlobal).allProcs.offset(bgwprocno as isize)).procLatch);
    }

    /*
     * We count buffer allocation requests so that the bgwriter can estimate
     * the rate of buffer consumption.  Note that buffers recycled by a
     * strategy object are intentionally not counted here.
     */
    pg_atomic_fetch_add_u32(&mut (*StrategyControl).numBufferAllocs, 1);

    /*
     * First check, without acquiring the lock, whether there's buffers in the
     * freelist. Since we otherwise don't require the spinlock in every
     * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
     * uselessly in most cases. That obviously leaves a race where a buffer is
     * put on the freelist but we don't see the store yet - but that's pretty
     * harmless, it'll just get used during the next buffer acquisition.
     *
     * If there's buffers on the freelist, acquire the spinlock to pop one
     * buffer of the freelist. Then check whether that buffer is usable and
     * repeat if not.
     *
     * Note that the freeNext fields are considered to be protected by the
     * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
     * manipulate them without holding the spinlock.
     */
    if (*StrategyControl).firstFreeBuffer >= 0 {
        loop {
            /* Acquire the spinlock to remove element from the freelist */
            SpinLockAcquire(&mut (*StrategyControl).buffer_strategy_lock);

            if (*StrategyControl).firstFreeBuffer < 0 {
                SpinLockRelease(&mut (*StrategyControl).buffer_strategy_lock);
                break;
            }

            buf = GetBufferDescriptor((*StrategyControl).firstFreeBuffer);
            Assert!((*buf).freeNext != FREENEXT_NOT_IN_LIST);

            /* Unconditionally remove buffer from freelist */
            (*StrategyControl).firstFreeBuffer = (*buf).freeNext;
            (*buf).freeNext = FREENEXT_NOT_IN_LIST;

            /*
             * Release the lock so someone else can access the freelist while
             * we check out this buffer.
             */
            SpinLockRelease(&mut (*StrategyControl).buffer_strategy_lock);

            /*
             * If the buffer is pinned or has a nonzero usage_count, we cannot
             * use it; discard it and retry.  (This can only happen if VACUUM
             * put a valid buffer in the freelist and then someone else used
             * it before we got to it.  It's probably impossible altogether as
             * of 8.3, but we'd better check anyway.)
             */
            local_buf_state = LockBufHdr(buf);
            if BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
                && BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0
            {
                if !strategy.is_null() {
                    AddBufferToRing(strategy, buf);
                }
                *buf_state = local_buf_state;
                return buf;
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }

    /* Nothing on the freelist, so run the "clock sweep" algorithm */
    trycounter = NBuffers;
    loop {
        buf = GetBufferDescriptor(ClockSweepTick() as c_int);

        /*
         * If the buffer is pinned or has a nonzero usage_count, we cannot use
         * it; decrement the usage_count (unless pinned) and keep scanning.
         */
        local_buf_state = LockBufHdr(buf);

        if BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 {
            if BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0 {
                local_buf_state -= BUF_USAGECOUNT_ONE;

                trycounter = NBuffers;
            } else {
                /* Found a usable buffer */
                if !strategy.is_null() {
                    AddBufferToRing(strategy, buf);
                }
                *buf_state = local_buf_state;
                return buf;
            }
        } else {
            trycounter -= 1;
            if trycounter == 0 {
                /*
                 * We've scanned all the buffers without making any state changes,
                 * so all the buffers are pinned (or were when we looked at them).
                 * We could hope that someone will free one eventually, but it's
                 * probably better to fail than to risk getting stuck in an
                 * infinite loop.
                 */
                UnlockBufHdr(buf, local_buf_state);
                elog!(ERROR, "no unpinned buffers available");
            }
        }
        UnlockBufHdr(buf, local_buf_state);
    }
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 */
#[no_mangle]
pub unsafe extern "C" fn StrategyFreeBuffer(buf: *mut BufferDesc) {
    SpinLockAcquire(&mut (*StrategyControl).buffer_strategy_lock);

    /*
     * It is possible that we are told to put something in the freelist that
     * is already in it; don't screw up the list if so.
     */
    if (*buf).freeNext == FREENEXT_NOT_IN_LIST {
        (*buf).freeNext = (*StrategyControl).firstFreeBuffer;
        if (*buf).freeNext < 0 {
            (*StrategyControl).lastFreeBuffer = (*buf).buf_id;
        }
        (*StrategyControl).firstFreeBuffer = (*buf).buf_id;
    }

    SpinLockRelease(&mut (*StrategyControl).buffer_strategy_lock);
}

/*
 * StrategySyncStart -- tell BgBufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BgBufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
#[no_mangle]
pub unsafe extern "C" fn StrategySyncStart(
    complete_passes: *mut u32,
    num_buf_alloc: *mut u32,
) -> c_int {
    let nextVictimBuffer: u32;
    let result: c_int;

    SpinLockAcquire(&mut (*StrategyControl).buffer_strategy_lock);
    nextVictimBuffer = pg_atomic_read_u32(&mut (*StrategyControl).nextVictimBuffer);
    result = (nextVictimBuffer % NBuffers as u32) as c_int;

    if !complete_passes.is_null() {
        *complete_passes = (*StrategyControl).completePasses;

        /*
         * Additionally add the number of wraparounds that happened before
         * completePasses could be incremented. C.f. ClockSweepTick().
         */
        *complete_passes += nextVictimBuffer / NBuffers as u32;
    }

    if !num_buf_alloc.is_null() {
        *num_buf_alloc = pg_atomic_exchange_u32(&mut (*StrategyControl).numBufferAllocs, 0);
    }
    SpinLockRelease(&mut (*StrategyControl).buffer_strategy_lock);
    result
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
#[no_mangle]
pub unsafe extern "C" fn StrategyNotifyBgWriter(bgwprocno: c_int) {
    /*
     * We acquire buffer_strategy_lock just to ensure that the store appears
     * atomic to StrategyGetBuffer.  The bgwriter should call this rather
     * infrequently, so there's no performance penalty from being safe.
     */
    SpinLockAcquire(&mut (*StrategyControl).buffer_strategy_lock);
    (*StrategyControl).bgwprocno = bgwprocno;
    SpinLockRelease(&mut (*StrategyControl).buffer_strategy_lock);
}

/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
#[no_mangle]
pub unsafe extern "C" fn StrategyShmemSize() -> Size {
    let mut size: Size = 0;

    /* size of lookup hash table ... see comment in StrategyInitialize */
    size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

    /* size of the shared replacement strategy control block */
    size = add_size(size, MAXALIGN(std::mem::size_of::<BufferStrategyControl>()));

    size
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
#[no_mangle]
pub unsafe extern "C" fn StrategyInitialize(init: bool) {
    let mut found: bool = false;

    /*
     * Initialize the shared buffer lookup hashtable.
     *
     * Since we can't tolerate running out of lookup table entries, we must be
     * sure to specify an adequate table size here.  The maximum steady-state
     * usage is of course NBuffers entries, but BufferAlloc() tries to insert
     * a new entry before deleting the old.  In principle this could be
     * happening in each partition concurrently, so we could need as many as
     * NBuffers + NUM_BUFFER_PARTITIONS entries.
     */
    InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

    /*
     * Get or create the shared strategy control block
     */
    StrategyControl = ShmemInitStruct(
        c"Buffer Strategy Status".as_ptr(),
        std::mem::size_of::<BufferStrategyControl>(),
        &mut found,
    ) as *mut BufferStrategyControl;

    if !found {
        /*
         * Only done once, usually in postmaster
         */
        Assert!(init);

        SpinLockInit(&mut (*StrategyControl).buffer_strategy_lock);

        /*
         * Grab the whole linked list of free buffers for our strategy. We
         * assume it was previously set up by BufferManagerShmemInit().
         */
        (*StrategyControl).firstFreeBuffer = 0;
        (*StrategyControl).lastFreeBuffer = NBuffers - 1;

        /* Initialize the clock sweep pointer */
        pg_atomic_init_u32(&mut (*StrategyControl).nextVictimBuffer, 0);

        /* Clear statistics */
        (*StrategyControl).completePasses = 0;
        pg_atomic_init_u32(&mut (*StrategyControl).numBufferAllocs, 0);

        /* No pending notification */
        (*StrategyControl).bgwprocno = -1;
    } else {
        Assert!(!init);
    }
}

/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */

/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
#[no_mangle]
pub unsafe extern "C" fn GetAccessStrategy(
    btype: BufferAccessStrategyType,
) -> BufferAccessStrategy {
    let mut ring_size_kb: c_int;

    /*
     * Select ring size to use.  See buffer/README for rationales.
     *
     * Note: if you change the ring size for BAS_BULKREAD, see also
     * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
     */
    match btype {
        BAS_NORMAL => {
            /* if someone asks for NORMAL, just give 'em a "default" object */
            return std::ptr::null_mut();
        }

        BAS_BULKREAD => {
            let mut ring_max_kb: c_int;

            /*
             * The ring always needs to be large enough to allow some
             * separation in time between providing a buffer to the user
             * of the strategy and that buffer being reused. Otherwise the
             * user's pin will prevent reuse of the buffer, even without
             * concurrent activity.
             *
             * We also need to ensure the ring always is large enough for
             * SYNC_SCAN_REPORT_INTERVAL, as noted above.
             *
             * Thus we start out a minimal size and increase the size
             * further if appropriate.
             */
            ring_size_kb = 256;

            /*
             * There's no point in a larger ring if we won't be allowed to
             * pin sufficiently many buffers.  But we never limit to less
             * than the minimal size above.
             */
            ring_max_kb = GetPinLimit() * (BLCKSZ / 1024) as c_int;
            ring_max_kb = Max(ring_size_kb, ring_max_kb);

            /*
             * We would like the ring to additionally have space for the
             * configured degree of IO concurrency. While being read in,
             * buffers can obviously not yet be reused.
             *
             * Each IO can be up to io_combine_limit blocks large, and we
             * want to start up to effective_io_concurrency IOs.
             *
             * Note that effective_io_concurrency may be 0, which disables
             * AIO.
             */
            ring_size_kb += (BLCKSZ / 1024) as c_int * io_combine_limit * effective_io_concurrency;

            if ring_size_kb > ring_max_kb {
                ring_size_kb = ring_max_kb;
            }
        }
        BAS_BULKWRITE => {
            ring_size_kb = 16 * 1024;
        }
        BAS_VACUUM => {
            ring_size_kb = 2048;
        }

        _ => {
            elog!(ERROR, "unrecognized buffer access strategy: {}", btype as c_int);
            return std::ptr::null_mut(); /* keep compiler quiet */
        }
    }

    GetAccessStrategyWithSize(btype, ring_size_kb)
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
#[no_mangle]
pub unsafe extern "C" fn GetAccessStrategyWithSize(
    btype: BufferAccessStrategyType,
    ring_size_kb: c_int,
) -> BufferAccessStrategy {
    let mut ring_buffers: c_int;
    let strategy: BufferAccessStrategy;

    Assert!(ring_size_kb >= 0);

    /* Figure out how many buffers ring_size_kb is */
    ring_buffers = ring_size_kb / (BLCKSZ / 1024) as c_int;

    /* 0 means unlimited, so no BufferAccessStrategy required */
    if ring_buffers == 0 {
        return std::ptr::null_mut();
    }

    /* Cap to 1/8th of shared_buffers */
    ring_buffers = Min(NBuffers / 8, ring_buffers);

    /* NBuffers should never be less than 16, so this shouldn't happen */
    Assert!(ring_buffers > 0);

    /* Allocate the object and initialize all elements to zeroes */
    strategy = palloc0(
        core::mem::offset_of!(BufferAccessStrategyData, buffers)
            + ring_buffers as usize * std::mem::size_of::<Buffer>(),
    ) as BufferAccessStrategy;

    /* Set fields that don't start out zero */
    (*strategy).btype = btype;
    (*strategy).nbuffers = ring_buffers;

    strategy
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.
 */
#[no_mangle]
pub unsafe extern "C" fn GetAccessStrategyBufferCount(strategy: BufferAccessStrategy) -> c_int {
    if strategy.is_null() {
        return 0;
    }

    (*strategy).nbuffers
}

/*
 * GetAccessStrategyPinLimit -- get cap of number of buffers that should be pinned
 *
 * When pinning extra buffers to look ahead, users of a ring-based strategy are
 * in danger of pinning too much of the ring at once while performing look-ahead.
 * For some strategies, that means "escaping" from the ring, and in others it
 * means forcing dirty data to disk very frequently with associated WAL
 * flushing.  Since external code has no insight into any of that, allow
 * individual strategy types to expose a clamp that should be applied when
 * deciding on a maximum number of buffers to pin at once.
 *
 * Callers should combine this number with other relevant limits and take the
 * minimum.
 */
#[no_mangle]
pub unsafe extern "C" fn GetAccessStrategyPinLimit(strategy: BufferAccessStrategy) -> c_int {
    if strategy.is_null() {
        return NBuffers;
    }

    match (*strategy).btype {
        BAS_BULKREAD => {
            /*
             * Since BAS_BULKREAD uses StrategyRejectBuffer(), dirty buffers
             * shouldn't be a problem and the caller is free to pin up to the
             * entire ring at once.
             */
            (*strategy).nbuffers
        }

        _ => {
            /*
             * Tell caller not to pin more than half the buffers in the ring.
             * This is a trade-off between look ahead distance and deferring
             * writeback and associated WAL traffic.
             */
            (*strategy).nbuffers / 2
        }
    }
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
#[no_mangle]
pub unsafe extern "C" fn FreeAccessStrategy(strategy: BufferAccessStrategy) {
    /* don't crash if called on a "default" strategy */
    if !strategy.is_null() {
        pfree(strategy as *mut c_void);
    }
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
unsafe fn GetBufferFromRing(
    strategy: BufferAccessStrategy,
    buf_state: *mut u32,
) -> *mut BufferDesc {
    let buf: *mut BufferDesc;
    let bufnum: Buffer;
    let local_buf_state: u32; /* to avoid repeated (de-)referencing */

    /* Advance to next ring slot */
    (*strategy).current += 1;
    if (*strategy).current >= (*strategy).nbuffers {
        (*strategy).current = 0;
    }

    /*
     * If the slot hasn't been filled yet, tell the caller to allocate a new
     * buffer with the normal allocation strategy.  He will then fill this
     * slot by calling AddBufferToRing with the new buffer.
     */
    bufnum = *(*strategy).buffers.as_ptr().offset((*strategy).current as isize);
    if bufnum == InvalidBuffer {
        return std::ptr::null_mut();
    }

    /*
     * If the buffer is pinned we cannot use it under any circumstances.
     *
     * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
     * since our own previous usage of the ring element would have left it
     * there, but it might've been decremented by clock sweep since then). A
     * higher usage_count indicates someone else has touched the buffer, so we
     * shouldn't re-use it.
     */
    buf = GetBufferDescriptor(bufnum - 1);
    local_buf_state = LockBufHdr(buf);
    if BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
        && BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1
    {
        *buf_state = local_buf_state;
        return buf;
    }
    UnlockBufHdr(buf, local_buf_state);

    /*
     * Tell caller to allocate a new buffer with the normal allocation
     * strategy.  He'll then replace this ring element via AddBufferToRing.
     */
    std::ptr::null_mut()
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
unsafe fn AddBufferToRing(strategy: BufferAccessStrategy, buf: *mut BufferDesc) {
    *(*strategy)
        .buffers
        .as_mut_ptr()
        .offset((*strategy).current as isize) = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
#[no_mangle]
pub unsafe extern "C" fn IOContextForStrategy(strategy: BufferAccessStrategy) -> IOContext {
    if strategy.is_null() {
        return IOCONTEXT_NORMAL;
    }

    match (*strategy).btype {
        BAS_NORMAL => {
            /*
             * Currently, GetAccessStrategy() returns NULL for
             * BufferAccessStrategyType BAS_NORMAL, so this case is
             * unreachable.
             */
            pg_unreachable();
            #[allow(unreachable_code)]
            IOCONTEXT_NORMAL
        }
        BAS_BULKREAD => IOCONTEXT_BULKREAD,
        BAS_BULKWRITE => IOCONTEXT_BULKWRITE,
        BAS_VACUUM => IOCONTEXT_VACUUM,
        _ => {
            elog!(ERROR, "unrecognized BufferAccessStrategyType: {}", (*strategy).btype);
            pg_unreachable();
            #[allow(unreachable_code)]
            0
        }
    }
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
#[no_mangle]
pub unsafe extern "C" fn StrategyRejectBuffer(
    strategy: BufferAccessStrategy,
    buf: *mut BufferDesc,
    from_ring: bool,
) -> bool {
    /* We only do this in bulkread mode */
    if (*strategy).btype != BAS_BULKREAD {
        return false;
    }

    /* Don't muck with behavior of normal buffer-replacement strategy */
    if !from_ring
        || *(*strategy).buffers.as_ptr().offset((*strategy).current as isize)
            != BufferDescriptorGetBuffer(buf)
    {
        return false;
    }

    /*
     * Remove the dirty buffer from the ring; necessary to prevent infinite
     * loop if all ring members are dirty.
     */
    *(*strategy)
        .buffers
        .as_mut_ptr()
        .offset((*strategy).current as isize) = InvalidBuffer;

    true
}

// ===========================================================================
// Local stubs for unported dependencies
// ===========================================================================

// Globals from miscadmin.h / bufmgr.h / globals
extern "C" {
    static NBuffers: c_int;
    static io_combine_limit: c_int;
    static effective_io_concurrency: c_int;
    static mut ProcGlobal: *mut PROC_HDR;
}

const BLCKSZ: usize = 8192;
const BUF_USAGECOUNT_ONE: u32 = 1 << 18; // storage/buf_internals.h

// PGPROC / PROC_HDR stubs (storage/proc.h)
#[repr(C)]
pub struct PGPROC {
    pub procLatch: Latch,
    // ... other fields omitted
}

#[repr(C)]
pub struct PROC_HDR {
    pub allProcs: *mut PGPROC,
    // ... other fields omitted
}

#[repr(C)]
pub struct Latch {
    // storage/latch.h
    _opaque: [u8; 0],
}

unsafe fn SetLatch(latch: *mut Latch) {
    crate::storage::ipc::latch::SetLatch(latch as *mut crate::storage::ipc::latch::Latch)
}

unsafe fn GetBufferDescriptor(id: c_int) -> *mut BufferDesc {
    crate::storage::buf_internals::GetBufferDescriptor(id as u32) as *mut BufferDesc
}

unsafe fn BufferDescriptorGetBuffer(bdesc: *mut BufferDesc) -> Buffer {
    crate::storage::buf_internals::BufferDescriptorGetBuffer(
        bdesc as *const crate::storage::buf_internals::BufferDesc,
    )
}

unsafe fn LockBufHdr(desc: *mut BufferDesc) -> u32 {
    crate::storage::buf_internals::LockBufHdr(
        desc as *mut crate::storage::buf_internals::BufferDesc,
    )
}

unsafe fn UnlockBufHdr(desc: *mut BufferDesc, buf_state: u32) {
    crate::storage::buf_internals::UnlockBufHdr(
        desc as *mut crate::storage::buf_internals::BufferDesc,
        buf_state,
    )
}

unsafe fn BUF_STATE_GET_REFCOUNT(state: u32) -> u32 {
    crate::storage::buf_internals::BUF_STATE_GET_REFCOUNT(state)
}

unsafe fn BUF_STATE_GET_USAGECOUNT(state: u32) -> u32 {
    crate::storage::buf_internals::BUF_STATE_GET_USAGECOUNT(state)
}

unsafe fn BufTableShmemSize(size: c_int) -> Size {
    crate::storage::buffer::buf_table::BufTableShmemSize(size)
}

unsafe fn InitBufTable(size: c_int) {
    crate::storage::buffer::buf_table::InitBufTable(size)
}

unsafe fn ShmemInitStruct(name: *const c_char, size: Size, found_ptr: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(name, size, found_ptr)
}

unsafe fn GetPinLimit() -> c_int {
    crate::storage::buffer::bufmgr::GetPinLimit() as c_int
}

unsafe fn pg_unreachable() {}

unsafe fn add_size(s1: Size, s2: Size) -> Size {
    crate::storage::ipc::shmem::add_size(s1, s2)
}

// spinlock primitives (storage/spin.h / s_lock.h)
unsafe fn SpinLockInit(lock: *mut slock_t) {
    crate::storage::spin::SpinLockInit(lock)
}

unsafe fn SpinLockAcquire(lock: *mut slock_t) {
    crate::storage::spin::SpinLockAcquire(lock)
}

unsafe fn SpinLockRelease(lock: *mut slock_t) {
    crate::storage::spin::SpinLockRelease(lock)
}

// atomics (port/atomics.h)
unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    crate::port::atomics::pg_atomic_init_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        val,
    )
}

unsafe fn pg_atomic_read_u32(ptr: *mut pg_atomic_uint32) -> u32 {
    crate::port::atomics::pg_atomic_read_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
    )
}

unsafe fn pg_atomic_fetch_add_u32(ptr: *mut pg_atomic_uint32, add: u32) -> u32 {
    crate::port::atomics::pg_atomic_fetch_add_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        add as i32,
    )
}

unsafe fn pg_atomic_exchange_u32(ptr: *mut pg_atomic_uint32, newval: u32) -> u32 {
    crate::port::atomics::generic::pg_atomic_exchange_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        newval,
    )
}

unsafe fn pg_atomic_compare_exchange_u32(
    ptr: *mut pg_atomic_uint32,
    expected: *mut u32,
    newval: u32,
) -> bool {
    crate::port::atomics::pg_atomic_compare_exchange_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        &mut *expected,
        newval,
    )
}

// Max/Min come from c.h (crate::c, re-exported by the prelude).
