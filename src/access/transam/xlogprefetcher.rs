/*-------------------------------------------------------------------------
 *
 * xlogprefetcher.c / xlogprefetcher.h
 *      Prefetching support for recovery.
 *
 * Portions Copyright (c) 2022-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/backend/access/transam/xlogprefetcher.c
 *
 * This module provides a drop-in replacement for an XLogReader that tries to
 * minimize I/O stalls by looking ahead in the WAL.  If blocks that will be
 * accessed in the near future are not already in the buffer pool, it initiates
 * I/Os that might complete before the caller eventually needs the data.  When
 * referenced blocks are found in the buffer pool already, the buffer is
 * recorded in the decoded record so that XLogReadBufferForRedo() can try to
 * avoid a second buffer mapping table lookup.
 *
 * Currently, only the main fork is considered for prefetching.  Currently,
 * prefetching is only effective on systems where PrefetchBuffer() does
 * something useful (mainly Linux).
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::GUC_check_errdetail;

// c_char, c_int, c_long, c_void come from crate::prelude::*

use crate::access::transam::xlogreader::{
    DecodedBkpBlock, DecodedXLogRecord, XLogReaderHasQueuedRecordOrError, XLogReaderState,
    XLogRecord, XLogRecPtr, InvalidXLogRecPtr, Buffer, InvalidBuffer, ForkNumber,
    RM_XLOG_ID, XLR_INFO_MASK,
};
#[allow(unused_imports)]
use crate::access::transam::xlogdefs::LSN_FORMAT_ARGS; // used by XLOGPREFETCHER_DEBUG_LEVEL elog branches
use crate::access::rmgrlist::{RM_SMGR_ID, RM_DBASE_ID};
use crate::access::rmgrdesc::smgrdesc::{
    xl_smgr_create, xl_smgr_truncate, XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE,
};
use crate::access::rmgrdesc::dbasedesc::{
    xl_dbase_create_file_copy_rec, XLOG_DBASE_CREATE_FILE_COPY,
};
use crate::access::rmgrdesc::xlogdesc::{XLOG_CHECKPOINT_SHUTDOWN, XLOG_END_OF_RECOVERY};
use crate::common::relpath::MAIN_FORKNUM;
use crate::lib::ilist::{dlist_head, dlist_node, dlist_init, dlist_push_head, dlist_delete,
    dlist_is_empty, dlist_tail_element_off};
use crate::nodes::execnodes::{ReturnSetInfo, Tuplestorestate};
// Int32GetDatum, Int64GetDatum come from crate::prelude::* (crate::postgres::*)
use crate::postgres_ext::InvalidOid;
#[allow(unused_imports)]
use crate::postgres_ext::Oid; // type alias used through RelFileLocator fields
use crate::port::atomics::pg_atomic_uint64;
use crate::storage::block::BlockNumber;
#[allow(unused_imports)]
use crate::storage::procnumber::ProcNumber; // type alias used through smgropen signature
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::smgr::{smgrexists, smgrnblocks, smgropen, SMgrRelation};
use crate::utils::fmgr::FunctionCallInfo;
#[allow(unused_imports)]
use crate::common::relpath::RelFileNumber; // needed for InvalidRelFileNumber type alias below

// ---------------------------------------------------------------------------
// xlogprefetcher.h public declarations merged here.
// ---------------------------------------------------------------------------

/* GUCs */
pub static mut recovery_prefetch: c_int = RECOVERY_PREFETCH_TRY;

/* Possible values for recovery_prefetch */
pub const RECOVERY_PREFETCH_OFF: c_int = 0;
pub const RECOVERY_PREFETCH_ON: c_int = 1;
pub const RECOVERY_PREFETCH_TRY: c_int = 2;

// ---------------------------------------------------------------------------
// Constants.
// ---------------------------------------------------------------------------

/*
 * Every time we process this much WAL, we'll update the values in
 * pg_stat_recovery_prefetch.
 */
const XLOGPREFETCHER_STATS_DISTANCE: XLogRecPtr = BLCKSZ as u64;

/*
 * To detect repeated access to the same block and skip useless extra system
 * calls, we remember a small window of recently prefetched blocks.
 */
const XLOGPREFETCHER_SEQ_WINDOW_SIZE: usize = 4;

/*
 * When maintenance_io_concurrency is not saturated, we're prepared to look
 * ahead up to N times that number of block references.
 */
const XLOGPREFETCHER_DISTANCE_MULTIPLIER: u32 = 4;

/* Define to log internal debugging messages. */
/* const XLOGPREFETCHER_DEBUG_LEVEL: c_int = LOG; */

static mut XLogPrefetchReconfigureCount: c_int = 0;

// ---------------------------------------------------------------------------
// LsnReadQueue
// ---------------------------------------------------------------------------

/*
 * Enum used to report whether an IO should be started.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LsnReadQueueNextStatus {
    LRQ_NEXT_NO_IO,
    LRQ_NEXT_IO,
    LRQ_NEXT_AGAIN,
}
use LsnReadQueueNextStatus::*;

/*
 * Type of callback that can decide which block to prefetch next.  For now
 * there is only one.
 */
pub type LsnReadQueueNextFun =
    unsafe fn(lrq_private: usize, lsn: *mut XLogRecPtr) -> LsnReadQueueNextStatus;

/*
 * A single slot in the circular LsnReadQueue.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct LsnReadQueueEntry {
    io: bool,
    lsn: XLogRecPtr,
}

/*
 * A simple circular queue of LSNs, used to control the number of
 * (potentially) inflight IOs.  This stands in for a later more general IO
 * control mechanism, which is why it has the apparently unnecessary
 * indirection through a function pointer.
 *
 * NOTE: In C, queue[] is a FLEXIBLE_ARRAY_MEMBER appended inline.  Here we
 * heap-allocate the Vec separately and keep a raw Vec storage pointer,
 * mirroring the C palloc layout at a safe abstraction boundary.
 */
#[repr(C)]
pub struct LsnReadQueue {
    next: LsnReadQueueNextFun,
    lrq_private: usize,
    max_inflight: u32,
    inflight: u32,
    completed: u32,
    head: u32,
    tail: u32,
    size: u32,
    /* queue[] slots live in a separately-palloc'd array of `size` entries. */
    queue: *mut LsnReadQueueEntry,
}

/*
 * Allocate a new LsnReadQueue.
 */
#[inline]
unsafe fn lrq_alloc(
    max_distance: u32,
    max_inflight: u32,
    lrq_private: usize,
    next: LsnReadQueueNextFun,
) -> *mut LsnReadQueue {
    Assert!(max_distance >= max_inflight);

    let size = max_distance + 1; /* full ring buffer has a gap */
    let lrq = palloc0(core::mem::size_of::<LsnReadQueue>()) as *mut LsnReadQueue;
    let queue = palloc0(core::mem::size_of::<LsnReadQueueEntry>() * size as usize)
        as *mut LsnReadQueueEntry;
    (*lrq).lrq_private = lrq_private;
    (*lrq).max_inflight = max_inflight;
    (*lrq).size = size;
    (*lrq).next = next;
    (*lrq).head = 0;
    (*lrq).tail = 0;
    (*lrq).inflight = 0;
    (*lrq).completed = 0;
    (*lrq).queue = queue;

    lrq
}

#[inline]
unsafe fn lrq_free(lrq: *mut LsnReadQueue) {
    if !lrq.is_null() {
        pfree((*lrq).queue as *mut c_void);
        pfree(lrq as *mut c_void);
    }
}

#[inline]
unsafe fn lrq_inflight(lrq: *const LsnReadQueue) -> u32 {
    (*lrq).inflight
}

#[inline]
unsafe fn lrq_completed(lrq: *const LsnReadQueue) -> u32 {
    (*lrq).completed
}

#[inline]
unsafe fn lrq_prefetch(lrq: *mut LsnReadQueue) {
    /* Try to start as many IOs as we can within our limits. */
    while (*lrq).inflight < (*lrq).max_inflight
        && (*lrq).inflight + (*lrq).completed < (*lrq).size - 1
    {
        Assert!((((*lrq).head + 1) % (*lrq).size) != (*lrq).tail);
        let slot = (*lrq).queue.add((*lrq).head as usize);
        match ((*lrq).next)((*lrq).lrq_private, &mut (*slot).lsn) {
            LRQ_NEXT_AGAIN => return,
            LRQ_NEXT_IO => {
                (*slot).io = true;
                (*lrq).inflight += 1;
            }
            LRQ_NEXT_NO_IO => {
                (*slot).io = false;
                (*lrq).completed += 1;
            }
        }
        (*lrq).head += 1;
        if (*lrq).head == (*lrq).size {
            (*lrq).head = 0;
        }
    }
}

#[inline]
unsafe fn lrq_complete_lsn(lrq: *mut LsnReadQueue, lsn: XLogRecPtr) {
    /*
     * We know that LSNs before 'lsn' have been replayed, so we can now assume
     * that any IOs that were started before then have finished.
     */
    while (*lrq).tail != (*lrq).head
        && (*(*lrq).queue.add((*lrq).tail as usize)).lsn < lsn
    {
        if (*(*lrq).queue.add((*lrq).tail as usize)).io {
            (*lrq).inflight -= 1;
        } else {
            (*lrq).completed -= 1;
        }
        (*lrq).tail += 1;
        if (*lrq).tail == (*lrq).size {
            (*lrq).tail = 0;
        }
    }
    if RecoveryPrefetchEnabled() {
        lrq_prefetch(lrq);
    }
}

// ---------------------------------------------------------------------------
// Struct definitions.
// ---------------------------------------------------------------------------

/*
 * A prefetcher.  This is a mechanism that wraps an XLogReader, prefetching
 * blocks that will be soon be referenced, to try to avoid IO stalls.
 */
#[repr(C)]
pub struct XLogPrefetcher {
    /* WAL reader and current reading state. */
    pub reader: *mut XLogReaderState,
    pub record: *mut DecodedXLogRecord,
    pub next_block_id: c_int,

    /* When to publish stats. */
    pub next_stats_shm_lsn: XLogRecPtr,

    /* Book-keeping to avoid accessing blocks that don't exist yet. */
    pub filter_table: *mut HTAB,
    pub filter_queue: dlist_head,

    /* Book-keeping to avoid repeat prefetches. */
    pub recent_rlocator: [RelFileLocator; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
    pub recent_block: [BlockNumber; XLOGPREFETCHER_SEQ_WINDOW_SIZE],
    pub recent_idx: c_int,

    /* Book-keeping to disable prefetching temporarily. */
    pub no_readahead_until: XLogRecPtr,

    /* IO depth manager. */
    pub streaming_read: *mut LsnReadQueue,

    pub begin_ptr: XLogRecPtr,

    pub reconfigure_count: c_int,
}

/*
 * A temporary filter used to track block ranges that haven't been created
 * yet, whole relations that haven't been created yet, and whole relations
 * that (we assume) have already been dropped, or will be created by bulk WAL
 * operators.
 */
#[repr(C)]
pub struct XLogPrefetcherFilter {
    pub rlocator: RelFileLocator,
    pub filter_until_replayed: XLogRecPtr,
    pub filter_from_block: BlockNumber,
    pub link: dlist_node,
}

/*
 * Counters exposed in shared memory for pg_stat_recovery_prefetch.
 */
#[repr(C)]
pub struct XLogPrefetchStats {
    pub reset_time: pg_atomic_uint64,   /* Time of last reset. */
    pub prefetch: pg_atomic_uint64,     /* Prefetches initiated. */
    pub hit: pg_atomic_uint64,          /* Blocks already in cache. */
    pub skip_init: pg_atomic_uint64,    /* Zero-inited blocks skipped. */
    pub skip_new: pg_atomic_uint64,     /* New/missing blocks filtered. */
    pub skip_fpw: pg_atomic_uint64,     /* FPWs skipped. */
    pub skip_rep: pg_atomic_uint64,     /* Repeat accesses skipped. */

    /* Dynamic values */
    pub wal_distance: c_int,            /* Number of WAL bytes ahead. */
    pub block_distance: c_int,          /* Number of block references ahead. */
    pub io_depth: c_int,                /* Number of I/Os in progress. */
}

static mut SharedStats: *mut XLogPrefetchStats = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// Public API (merged from xlogprefetcher.h)
// ---------------------------------------------------------------------------

pub unsafe fn XLogPrefetchReconfigure() {
    XLogPrefetchReconfigureCount += 1;
}

pub unsafe fn XLogPrefetchShmemSize() -> usize {
    core::mem::size_of::<XLogPrefetchStats>()
}

/*
 * Reset all counters to zero.
 */
pub unsafe fn XLogPrefetchResetStats() {
    pg_atomic_write_u64(&mut (*SharedStats).reset_time, GetCurrentTimestamp() as u64);
    pg_atomic_write_u64(&mut (*SharedStats).prefetch, 0);
    pg_atomic_write_u64(&mut (*SharedStats).hit, 0);
    pg_atomic_write_u64(&mut (*SharedStats).skip_init, 0);
    pg_atomic_write_u64(&mut (*SharedStats).skip_new, 0);
    pg_atomic_write_u64(&mut (*SharedStats).skip_fpw, 0);
    pg_atomic_write_u64(&mut (*SharedStats).skip_rep, 0);
}

pub unsafe fn XLogPrefetchShmemInit() {
    let mut found: bool = false;

    SharedStats = ShmemInitStruct(
        c"XLogPrefetchStats".as_ptr(),
        core::mem::size_of::<XLogPrefetchStats>(),
        &mut found,
    ) as *mut XLogPrefetchStats;

    if !found {
        pg_atomic_init_u64(&mut (*SharedStats).reset_time, GetCurrentTimestamp() as u64);
        pg_atomic_init_u64(&mut (*SharedStats).prefetch, 0);
        pg_atomic_init_u64(&mut (*SharedStats).hit, 0);
        pg_atomic_init_u64(&mut (*SharedStats).skip_init, 0);
        pg_atomic_init_u64(&mut (*SharedStats).skip_new, 0);
        pg_atomic_init_u64(&mut (*SharedStats).skip_fpw, 0);
        pg_atomic_init_u64(&mut (*SharedStats).skip_rep, 0);
    }
}

/*
 * Increment a counter in shared memory.  This is equivalent to *counter++ on a
 * plain uint64 without any memory barrier or locking, except on platforms
 * where readers can't read uint64 without possibly observing a torn value.
 */
#[inline]
unsafe fn XLogPrefetchIncrement(counter: *mut pg_atomic_uint64) {
    Assert!(AmStartupProcess() || !IsUnderPostmaster);
    pg_atomic_write_u64(counter, pg_atomic_read_u64(counter) + 1);
}

/*
 * Create a prefetcher that is ready to begin prefetching blocks referenced by
 * WAL records.
 */
pub unsafe fn XLogPrefetcherAllocate(reader: *mut XLogReaderState) -> *mut XLogPrefetcher {
    let prefetcher = palloc0(core::mem::size_of::<XLogPrefetcher>()) as *mut XLogPrefetcher;
    (*prefetcher).reader = reader;

    let mut ctl = HASHCTL {
        keysize: core::mem::size_of::<RelFileLocator>(),
        entrysize: core::mem::size_of::<XLogPrefetcherFilter>(),
    };
    (*prefetcher).filter_table = hash_create(
        c"XLogPrefetcherFilterTable".as_ptr(),
        1024,
        &mut ctl,
        HASH_ELEM | HASH_BLOBS,
    );
    dlist_init(&mut (*prefetcher).filter_queue);

    (*SharedStats).wal_distance = 0;
    (*SharedStats).block_distance = 0;
    (*SharedStats).io_depth = 0;

    /* First usage will cause streaming_read to be allocated. */
    (*prefetcher).reconfigure_count = XLogPrefetchReconfigureCount - 1;

    prefetcher
}

/*
 * Destroy a prefetcher and release all resources.
 */
pub unsafe fn XLogPrefetcherFree(prefetcher: *mut XLogPrefetcher) {
    lrq_free((*prefetcher).streaming_read);
    hash_destroy((*prefetcher).filter_table);
    pfree(prefetcher as *mut c_void);
}

/*
 * Provide access to the reader.
 */
pub unsafe fn XLogPrefetcherGetReader(prefetcher: *mut XLogPrefetcher) -> *mut XLogReaderState {
    (*prefetcher).reader
}

/*
 * Update the statistics visible in the pg_stat_recovery_prefetch view.
 */
pub unsafe fn XLogPrefetcherComputeStats(prefetcher: *mut XLogPrefetcher) {
    let io_depth: u32;
    let completed: u32;
    let wal_distance: i64;

    /* How far ahead of replay are we now? */
    if !(*(*prefetcher).reader).decode_queue_tail.is_null() {
        wal_distance = ((*(*(*prefetcher).reader).decode_queue_tail).lsn
            - (*(*(*prefetcher).reader).decode_queue_head).lsn) as i64;
    } else {
        wal_distance = 0;
    }

    /* How many IOs are currently in flight and completed? */
    io_depth = lrq_inflight((*prefetcher).streaming_read);
    completed = lrq_completed((*prefetcher).streaming_read);

    /* Update the instantaneous stats visible in pg_stat_recovery_prefetch. */
    (*SharedStats).io_depth = io_depth as c_int;
    (*SharedStats).block_distance = (io_depth + completed) as c_int;
    (*SharedStats).wal_distance = wal_distance as c_int;

    (*prefetcher).next_stats_shm_lsn =
        (*(*prefetcher).reader).ReadRecPtr + XLOGPREFETCHER_STATS_DISTANCE;
}

/*
 * A callback that examines the next block reference in the WAL, and possibly
 * starts an IO so that a later read will be fast.
 *
 * Returns LRQ_NEXT_AGAIN if no more WAL data is available yet.
 *
 * Returns LRQ_NEXT_IO if the next block reference is for a main fork block
 * that isn't in the buffer pool, and the kernel has been asked to start
 * reading it to make a future read system call faster. An LSN is written to
 * *lsn, and the I/O will be considered to have completed once that LSN is
 * replayed.
 *
 * Returns LRQ_NEXT_NO_IO if we examined the next block reference and found
 * that it was already in the buffer pool, or we decided for various reasons
 * not to prefetch.
 */
unsafe fn XLogPrefetcherNextBlock(
    pgsr_private: usize,
    lsn: *mut XLogRecPtr,
) -> LsnReadQueueNextStatus {
    let prefetcher = pgsr_private as *mut XLogPrefetcher;
    let reader = (*prefetcher).reader;
    let replaying_lsn: XLogRecPtr = (*reader).ReadRecPtr;

    /*
     * We keep track of the record and block we're up to between calls with
     * prefetcher->record and prefetcher->next_block_id.
     */
    loop {
        /* Try to read a new future record, if we don't already have one. */
        if (*prefetcher).record.is_null() {
            let nonblocking: bool;

            /*
             * If there are already records or an error queued up that could
             * be replayed, we don't want to block here.  Otherwise, it's OK
             * to block waiting for more data: presumably the caller has
             * nothing else to do.
             */
            nonblocking = XLogReaderHasQueuedRecordOrError(reader);

            /* Readahead is disabled until we replay past a certain point. */
            if nonblocking && replaying_lsn <= (*prefetcher).no_readahead_until {
                return LRQ_NEXT_AGAIN;
            }

            let record = XLogReadAhead((*prefetcher).reader, nonblocking);
            if record.is_null() {
                /*
                 * We can't read any more, due to an error or lack of data in
                 * nonblocking mode.  Don't try to read ahead again until
                 * we've replayed everything already decoded.
                 */
                if nonblocking && !(*reader).decode_queue_tail.is_null() {
                    (*prefetcher).no_readahead_until =
                        (*(*reader).decode_queue_tail).lsn;
                }

                return LRQ_NEXT_AGAIN;
            }

            /*
             * If prefetching is disabled, we don't need to analyze the record
             * or issue any prefetches.  We just need to cause one record to
             * be decoded.
             */
            if !RecoveryPrefetchEnabled() {
                *lsn = InvalidXLogRecPtr;
                return LRQ_NEXT_NO_IO;
            }

            /* We have a new record to process. */
            (*prefetcher).record = record;
            (*prefetcher).next_block_id = 0;
        }
        /* else: Continue to process from last call, or last loop. */

        let record = (*prefetcher).record;

        /*
         * Check for operations that require us to filter out block ranges, or
         * pause readahead completely.
         */
        if replaying_lsn < (*record).lsn {
            let rmid: u8 = (*record).header.xl_rmid;
            let record_type: u8 = (*record).header.xl_info & !XLR_INFO_MASK;

            if rmid == RM_XLOG_ID {
                if record_type == XLOG_CHECKPOINT_SHUTDOWN
                    || record_type == XLOG_END_OF_RECOVERY
                {
                    /*
                     * These records might change the TLI.  Avoid potential
                     * bugs if we were to allow "read TLI" and "replay TLI" to
                     * differ without more analysis.
                     */
                    (*prefetcher).no_readahead_until = (*record).lsn;

                    /* Fall through so we move past this record. */
                }
            } else if rmid == RM_DBASE_ID {
                /*
                 * When databases are created with the file-copy strategy,
                 * there are no WAL records to tell us about the creation of
                 * individual relations.
                 */
                if record_type == XLOG_DBASE_CREATE_FILE_COPY {
                    let xlrec = (*record).main_data as *const xl_dbase_create_file_copy_rec;
                    let rlocator = RelFileLocator {
                        spcOid: InvalidOid,
                        dbOid: (*xlrec).db_id,
                        relNumber: InvalidRelFileNumber,
                    };

                    /*
                     * Don't try to prefetch anything in this database until
                     * it has been created, or we might confuse the blocks of
                     * different generations, if a database OID or
                     * relfilenumber is reused.  It's also more efficient than
                     * discovering that relations don't exist on disk yet with
                     * ENOENT errors.
                     */
                    XLogPrefetcherAddFilter(prefetcher, rlocator, 0, (*record).lsn);
                }
            } else if rmid == RM_SMGR_ID {
                if record_type == XLOG_SMGR_CREATE {
                    let xlrec = (*record).main_data as *mut xl_smgr_create;

                    if (*xlrec).forkNum == MAIN_FORKNUM {
                        /*
                         * Don't prefetch anything for this whole relation
                         * until it has been created.  Otherwise we might
                         * confuse the blocks of different generations, if a
                         * relfilenumber is reused.  This also avoids the need
                         * to discover the problem via extra syscalls that
                         * report ENOENT.
                         */
                        XLogPrefetcherAddFilter(
                            prefetcher,
                            core::mem::transmute((*xlrec).rlocator),
                            0,
                            (*record).lsn,
                        );
                    }
                } else if record_type == XLOG_SMGR_TRUNCATE {
                    let xlrec = (*record).main_data as *mut xl_smgr_truncate;

                    /*
                     * Don't consider prefetching anything in the truncated
                     * range until the truncation has been performed.
                     */
                    XLogPrefetcherAddFilter(
                        prefetcher,
                        core::mem::transmute((*xlrec).rlocator),
                        (*xlrec).blkno,
                        (*record).lsn,
                    );
                }
            }
        }

        /* Scan the block references, starting where we left off last time. */
        while (*prefetcher).next_block_id <= (*record).max_block_id {
            let block_id = (*prefetcher).next_block_id as usize;
            (*prefetcher).next_block_id += 1;

            /* blocks[] is a flexible array; walk via raw pointer. */
            let base = (record as *mut u8)
                .add(core::mem::offset_of!(DecodedXLogRecord, blocks))
                as *mut DecodedBkpBlock;
            let block = base.add(block_id);
            let reln: SMgrRelation;
            let result: PrefetchBufferResult;

            if !(*block).in_use {
                continue;
            }

            Assert!(!BufferIsValid((*block).prefetch_buffer));

            /*
             * Record the LSN of this record.  When it's replayed,
             * LsnReadQueue will consider any IOs submitted for earlier LSNs
             * to be finished.
             */
            *lsn = (*record).lsn;

            /* We don't try to prefetch anything but the main fork for now. */
            if (*block).forknum != MAIN_FORKNUM {
                return LRQ_NEXT_NO_IO;
            }

            /*
             * If there is a full page image attached, we won't be reading the
             * page, so don't bother trying to prefetch.
             */
            if (*block).has_image {
                XLogPrefetchIncrement(&mut (*SharedStats).skip_fpw);
                return LRQ_NEXT_NO_IO;
            }

            /* There is no point in reading a page that will be zeroed. */
            if ((*block).flags & BKPBLOCK_WILL_INIT) != 0 {
                XLogPrefetchIncrement(&mut (*SharedStats).skip_init);
                return LRQ_NEXT_NO_IO;
            }

            /* Should we skip prefetching this block due to a filter? */
            if XLogPrefetcherIsFiltered(prefetcher, core::mem::transmute((*block).rlocator), (*block).blkno) {
                XLogPrefetchIncrement(&mut (*SharedStats).skip_new);
                return LRQ_NEXT_NO_IO;
            }

            /* There is no point in repeatedly prefetching the same block. */
            for i in 0..XLOGPREFETCHER_SEQ_WINDOW_SIZE {
                if (*block).blkno == (*prefetcher).recent_block[i]
                    && RelFileLocatorEqualsInline(
                        core::mem::transmute((*block).rlocator),
                        (*prefetcher).recent_rlocator[i],
                    )
                {
                    /*
                     * XXX If we also remembered where it was, we could set
                     * recent_buffer so that recovery could skip smgropen()
                     * and a buffer table lookup.
                     */
                    XLogPrefetchIncrement(&mut (*SharedStats).skip_rep);
                    return LRQ_NEXT_NO_IO;
                }
            }
            (*prefetcher).recent_rlocator[(*prefetcher).recent_idx as usize] =
                core::mem::transmute((*block).rlocator);
            (*prefetcher).recent_block[(*prefetcher).recent_idx as usize] = (*block).blkno;
            (*prefetcher).recent_idx =
                ((*prefetcher).recent_idx + 1) % XLOGPREFETCHER_SEQ_WINDOW_SIZE as c_int;

            /*
             * We could try to have a fast path for repeated references to the
             * same relation (with some scheme to handle invalidations
             * safely), but for now we'll call smgropen() every time.
             */
            reln = smgropen(core::mem::transmute((*block).rlocator), INVALID_PROC_NUMBER);

            /*
             * If the relation file doesn't exist on disk, for example because
             * we're replaying after a crash and the file will be created and
             * then unlinked by WAL that hasn't been replayed yet, suppress
             * further prefetching in the relation until this record is
             * replayed.
             */
            if !smgrexists(reln, MAIN_FORKNUM) {
                XLogPrefetcherAddFilter(prefetcher, core::mem::transmute((*block).rlocator), 0, (*record).lsn);
                XLogPrefetchIncrement(&mut (*SharedStats).skip_new);
                return LRQ_NEXT_NO_IO;
            }

            /*
             * If the relation isn't big enough to contain the referenced
             * block yet, suppress prefetching of this block and higher until
             * this record is replayed.
             */
            if (*block).blkno >= smgrnblocks(reln, (*block).forknum) {
                XLogPrefetcherAddFilter(
                    prefetcher,
                    core::mem::transmute((*block).rlocator),
                    (*block).blkno,
                    (*record).lsn,
                );
                XLogPrefetchIncrement(&mut (*SharedStats).skip_new);
                return LRQ_NEXT_NO_IO;
            }

            /* Try to initiate prefetching. */
            result = PrefetchSharedBuffer(reln, (*block).forknum, (*block).blkno);
            if BufferIsValid(result.recent_buffer) {
                /* Cache hit, nothing to do. */
                XLogPrefetchIncrement(&mut (*SharedStats).hit);
                (*block).prefetch_buffer = result.recent_buffer;
                return LRQ_NEXT_NO_IO;
            } else if result.initiated_io {
                /* Cache miss, I/O (presumably) started. */
                XLogPrefetchIncrement(&mut (*SharedStats).prefetch);
                (*block).prefetch_buffer = InvalidBuffer;
                return LRQ_NEXT_IO;
            } else if (io_direct_flags() & IO_DIRECT_DATA) == 0 {
                /*
                 * This shouldn't be possible, because we already determined
                 * that the relation exists on disk and is big enough.
                 * Something is wrong with the cache invalidation for
                 * smgrexists(), smgrnblocks(), or the file was unlinked or
                 * truncated beneath our feet?
                 */
                elog!(
                    ERROR,
                    "could not prefetch relation {}/{}/{} block {}",
                    (*reln).smgr_rlocator.locator.spcOid,
                    (*reln).smgr_rlocator.locator.dbOid,
                    (*reln).smgr_rlocator.locator.relNumber,
                    (*block).blkno
                );
            }
        }

        /*
         * Several callsites need to be able to read exactly one record
         * without any internal readahead.  Examples: xlog.c reading
         * checkpoint records with emode set to PANIC, which might otherwise
         * cause XLogPageRead() to panic on some future page, and xlog.c
         * determining where to start writing WAL next, which depends on the
         * contents of the reader's internal buffer after reading one record.
         * Therefore, don't even think about prefetching until the first
         * record after XLogPrefetcherBeginRead() has been consumed.
         */
        if !(*reader).decode_queue_tail.is_null()
            && (*(*reader).decode_queue_tail).lsn == (*prefetcher).begin_ptr
        {
            return LRQ_NEXT_AGAIN;
        }

        /* Advance to the next record. */
        (*prefetcher).record = core::ptr::null_mut();
    }
    /* pg_unreachable() */
    #[allow(unreachable_code)]
    {
        core::hint::unreachable_unchecked()
    }
}

/*
 * Expose statistics about recovery prefetching.
 */
pub unsafe fn pg_stat_get_recovery_prefetch(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_RECOVERY_PREFETCH_COLS: usize = 10;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut values: [Datum; PG_STAT_GET_RECOVERY_PREFETCH_COLS] =
        [0; PG_STAT_GET_RECOVERY_PREFETCH_COLS];
    let mut nulls: [bool; PG_STAT_GET_RECOVERY_PREFETCH_COLS] =
        [false; PG_STAT_GET_RECOVERY_PREFETCH_COLS];

    InitMaterializedSRF(fcinfo, 0);

    values[0] = TimestampTzGetDatum(pg_atomic_read_u64(&mut (*SharedStats).reset_time) as TimestampTz);
    values[1] = Int64GetDatum(pg_atomic_read_u64(&mut (*SharedStats).prefetch) as i64);
    values[2] = Int64GetDatum(pg_atomic_read_u64(&mut (*SharedStats).hit) as i64);
    values[3] = Int64GetDatum(pg_atomic_read_u64(&mut (*SharedStats).skip_init) as i64);
    values[4] = Int64GetDatum(pg_atomic_read_u64(&mut (*SharedStats).skip_new) as i64);
    values[5] = Int64GetDatum(pg_atomic_read_u64(&mut (*SharedStats).skip_fpw) as i64);
    values[6] = Int64GetDatum(pg_atomic_read_u64(&mut (*SharedStats).skip_rep) as i64);
    values[7] = Int32GetDatum((*SharedStats).wal_distance);
    values[8] = Int32GetDatum((*SharedStats).block_distance);
    values[9] = Int32GetDatum((*SharedStats).io_depth);
    tuplestore_putvalues(
        (*rsinfo).setResult,
        (*rsinfo).setDesc,
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    0 /* (Datum) 0 */
}

/*
 * Don't prefetch any blocks >= 'blockno' from a given 'rlocator', until 'lsn'
 * has been replayed.
 */
#[inline]
unsafe fn XLogPrefetcherAddFilter(
    prefetcher: *mut XLogPrefetcher,
    rlocator: RelFileLocator,
    blockno: BlockNumber,
    lsn: XLogRecPtr,
) {
    let mut found: bool = false;

    let filter = hash_search(
        (*prefetcher).filter_table,
        &rlocator as *const RelFileLocator as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut XLogPrefetcherFilter;

    if !found {
        /*
         * Don't allow any prefetching of this block or higher until replayed.
         */
        (*filter).filter_until_replayed = lsn;
        (*filter).filter_from_block = blockno;
        dlist_push_head(&mut (*prefetcher).filter_queue, &mut (*filter).link);
    } else {
        /*
         * We were already filtering this rlocator.  Extend the filter's
         * lifetime to cover this WAL record, but leave the lower of the block
         * numbers there because we don't want to have to track individual
         * blocks.
         */
        (*filter).filter_until_replayed = lsn;
        dlist_delete(&mut (*filter).link);
        dlist_push_head(&mut (*prefetcher).filter_queue, &mut (*filter).link);
        (*filter).filter_from_block = core::cmp::min((*filter).filter_from_block, blockno);
    }
}

/*
 * Have we replayed any records that caused us to begin filtering a block
 * range?  That means that relations should have been created, extended or
 * dropped as required, so we can stop filtering out accesses to a given
 * relfilenumber.
 */
#[inline]
unsafe fn XLogPrefetcherCompleteFilters(
    prefetcher: *mut XLogPrefetcher,
    replaying_lsn: XLogRecPtr,
) {
    while !dlist_is_empty(&(*prefetcher).filter_queue) {
        let filter = dlist_tail_element_off(
            &mut (*prefetcher).filter_queue,
            core::mem::offset_of!(XLogPrefetcherFilter, link),
        ) as *mut XLogPrefetcherFilter;

        if (*filter).filter_until_replayed >= replaying_lsn {
            break;
        }

        dlist_delete(&mut (*filter).link);
        hash_search(
            (*prefetcher).filter_table,
            filter as *const c_void,
            HASH_REMOVE,
            core::ptr::null_mut(),
        );
    }
}

/*
 * Check if a given block should be skipped due to a filter.
 */
#[inline]
unsafe fn XLogPrefetcherIsFiltered(
    prefetcher: *mut XLogPrefetcher,
    mut rlocator: RelFileLocator,
    blockno: BlockNumber,
) -> bool {
    /*
     * Test for empty queue first, because we expect it to be empty most of
     * the time and we can avoid the hash table lookup in that case.
     */
    if !dlist_is_empty(&(*prefetcher).filter_queue) {
        /* See if the block range is filtered. */
        let filter = hash_search(
            (*prefetcher).filter_table,
            &rlocator as *const RelFileLocator as *const c_void,
            HASH_FIND,
            core::ptr::null_mut(),
        ) as *mut XLogPrefetcherFilter;
        if !filter.is_null() && (*filter).filter_from_block <= blockno {
            return true;
        }

        /* See if the whole database is filtered. */
        rlocator.relNumber = InvalidRelFileNumber;
        rlocator.spcOid = InvalidOid;
        let filter = hash_search(
            (*prefetcher).filter_table,
            &rlocator as *const RelFileLocator as *const c_void,
            HASH_FIND,
            core::ptr::null_mut(),
        ) as *mut XLogPrefetcherFilter;
        if !filter.is_null() {
            return true;
        }
    }

    false
}

/*
 * A wrapper for XLogBeginRead() that also resets the prefetcher.
 */
pub unsafe fn XLogPrefetcherBeginRead(
    prefetcher: *mut XLogPrefetcher,
    recPtr: XLogRecPtr,
) {
    /* This will forget about any in-flight IO. */
    (*prefetcher).reconfigure_count -= 1;

    /* Book-keeping to avoid readahead on first read. */
    (*prefetcher).begin_ptr = recPtr;

    (*prefetcher).no_readahead_until = 0;

    /* This will forget about any queued up records in the decoder. */
    XLogBeginRead((*prefetcher).reader, recPtr);
}

/*
 * A wrapper for XLogReadRecord() that provides the same interface, but also
 * tries to initiate I/O for blocks referenced in future WAL records.
 */
pub unsafe fn XLogPrefetcherReadRecord(
    prefetcher: *mut XLogPrefetcher,
    errmsg: *mut *mut c_char,
) -> *mut XLogRecord {
    let record: *mut DecodedXLogRecord;
    let replayed_up_to: XLogRecPtr;

    /*
     * See if it's time to reset the prefetching machinery, because a relevant
     * GUC was changed.
     */
    if XLogPrefetchReconfigureCount != (*prefetcher).reconfigure_count {
        let max_distance: u32;
        let max_inflight: u32;

        if !(*prefetcher).streaming_read.is_null() {
            lrq_free((*prefetcher).streaming_read);
        }

        if RecoveryPrefetchEnabled() {
            Assert!(maintenance_io_concurrency() > 0);
            max_inflight = maintenance_io_concurrency() as u32;
            max_distance = max_inflight * XLOGPREFETCHER_DISTANCE_MULTIPLIER;
        } else {
            max_inflight = 1;
            max_distance = 1;
        }

        (*prefetcher).streaming_read = lrq_alloc(
            max_distance,
            max_inflight,
            prefetcher as usize,
            XLogPrefetcherNextBlock,
        );

        (*prefetcher).reconfigure_count = XLogPrefetchReconfigureCount;
    }

    /*
     * Release last returned record, if there is one, as it's now been
     * replayed.
     */
    replayed_up_to = XLogReleasePreviousRecord((*prefetcher).reader);

    /*
     * Can we drop any filters yet?  If we were waiting for a relation to be
     * created or extended, it is now OK to access blocks in the covered
     * range.
     */
    XLogPrefetcherCompleteFilters(prefetcher, replayed_up_to);

    /*
     * All IO initiated by earlier WAL is now completed.  This might trigger
     * further prefetching.
     */
    lrq_complete_lsn((*prefetcher).streaming_read, replayed_up_to);

    /*
     * If there's nothing queued yet, then start prefetching to cause at least
     * one record to be queued.
     */
    if !XLogReaderHasQueuedRecordOrError((*prefetcher).reader) {
        Assert!(lrq_inflight((*prefetcher).streaming_read) == 0);
        Assert!(lrq_completed((*prefetcher).streaming_read) == 0);
        lrq_prefetch((*prefetcher).streaming_read);
    }

    /* Read the next record. */
    record = XLogNextRecord((*prefetcher).reader, errmsg);
    if record.is_null() {
        return core::ptr::null_mut();
    }

    /*
     * The record we just got is the "current" one, for the benefit of the
     * XLogRecXXX() macros.
     */
    Assert!(record == (*(*prefetcher).reader).record);

    /*
     * If maintenance_io_concurrency is set very low, we might have started
     * prefetching some but not all of the blocks referenced in the record
     * we're about to return.  Forget about the rest of the blocks in this
     * record by dropping the prefetcher's reference to it.
     */
    if record == (*prefetcher).record {
        (*prefetcher).record = core::ptr::null_mut();
    }

    /*
     * See if it's time to compute some statistics, because enough WAL has
     * been processed.
     */
    if (*record).lsn >= (*prefetcher).next_stats_shm_lsn {
        XLogPrefetcherComputeStats(prefetcher);
    }

    Assert!(record == (*(*prefetcher).reader).record);

    &mut (*record).header as *mut XLogRecord
}

pub unsafe fn check_recovery_prefetch(
    new_value: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    /* Without USE_PREFETCH the ON setting cannot be honoured.
     * (Darwin lacks read-ahead advice, so this port models !USE_PREFETCH.) */
    {
        if *new_value == RECOVERY_PREFETCH_ON {
            GUC_check_errdetail!(
                "\"recovery_prefetch\" is not supported on platforms that lack \
                 support for issuing read-ahead advice."
            );
            return false;
        }
    }
    true
}

pub unsafe fn assign_recovery_prefetch(new_value: c_int, _extra: *mut c_void) {
    /* Reconfigure prefetching, because a setting it depends on changed. */
    recovery_prefetch = new_value;
    if AmStartupProcess() {
        XLogPrefetchReconfigure();
    }
}

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

/// Inline RelFileLocatorEquals (the canonical one takes references; here we
/// have copied values so we spell it out directly).
#[inline]
fn RelFileLocatorEqualsInline(a: RelFileLocator, b: RelFileLocator) -> bool {
    a.relNumber == b.relNumber && a.dbOid == b.dbOid && a.spcOid == b.spcOid
}

/// `#[cfg(feature = "use_prefetch")]` guarded check.  Mirrors the C macro
/// `RecoveryPrefetchEnabled()`.
#[inline]
unsafe fn RecoveryPrefetchEnabled() -> bool {
    /* (Darwin lacks read-ahead advice: !USE_PREFETCH, so prefetching is
     * never enabled; the USE_PREFETCH expression is
     * `recovery_prefetch != RECOVERY_PREFETCH_OFF && maintenance_io_concurrency() > 0`.) */
    false
}

// ---------------------------------------------------------------------------
// Stubs for symbols whose homes are not yet translated.
// ---------------------------------------------------------------------------

/// Invalid RelFileNumber sentinel (postgres_ext.h / common/relpath.h).
pub const InvalidRelFileNumber: RelFileNumber = InvalidOid;

/// pg_stat_recovery_prefetch column count.
const PG_STAT_GET_RECOVERY_PREFETCH_COLS: usize = 10;

// ---- Datum ------------------------------------------------------------------
// Datum comes from crate::prelude::* (crate::postgres::*)

// ---- TimestampTz ------------------------------------------------------------
type TimestampTz = i64; // utils/timestamp.h

// ---- GucSource --------------------------------------------------------------
pub type GucSource = c_int; // TODO(pg-port): real GucSource lives in utils/guc.h

// ---- pg_atomic helpers that forward to crate::port::atomics ----------------

#[inline]
unsafe fn pg_atomic_init_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    use crate::port::atomics::pg_atomic_init_u64_impl_native;
    pg_atomic_init_u64_impl_native(&*ptr, val);
}

#[inline]
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> u64 {
    use crate::port::atomics::pg_atomic_read_u64_impl_native;
    pg_atomic_read_u64_impl_native(&*ptr)
}

#[inline]
unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    use core::sync::atomic::Ordering;
    (*ptr).value.store(val, Ordering::Relaxed);
}

// ---- dynahash stubs ---------------------------------------------------------

type HTAB = c_void;

#[repr(C)]
struct HASHCTL {
    keysize: usize,
    entrysize: usize,
}

const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0010;
const HASH_ENTER: c_int = 1;
const HASH_FIND: c_int = 0;
const HASH_REMOVE: c_int = 2;

// TODO(pg-port): real hash_create lives in utils/hash/dynahash.c
unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO(pg-port): real hash_create lives in utils/hash/dynahash.c
}

// TODO(pg-port): real hash_search lives in utils/hash/dynahash.c
unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *const c_void,
    _action: c_int,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real hash_search lives in utils/hash/dynahash.c
}

// TODO(pg-port): real hash_destroy lives in utils/hash/dynahash.c
unsafe fn hash_destroy(_hashp: *mut HTAB) {
    unimplemented!() // TODO(pg-port): real hash_destroy lives in utils/hash/dynahash.c
}

// ---- palloc / pfree ---------------------------------------------------------

// palloc/pfree come from crate::prelude::* (crate::utils::palloc)

// ---- miscadmin stubs --------------------------------------------------------

use crate::miscadmin::AmStartupProcess;

// TODO(pg-port): real IsUnderPostmaster lives in miscadmin.h
static mut IsUnderPostmaster: bool = false;

// ---- GUC state stubs --------------------------------------------------------

// TODO(pg-port): real maintenance_io_concurrency lives in utils/guc_hooks.c / miscadmin.h
unsafe fn maintenance_io_concurrency() -> c_int {
    unimplemented!() // TODO(pg-port): real maintenance_io_concurrency lives in utils/guc_hooks.c
}

// TODO(pg-port): real io_direct_flags lives in storage/fd.c
unsafe fn io_direct_flags() -> c_int {
    unimplemented!() // TODO(pg-port): real io_direct_flags lives in storage/fd.c
}

pub const IO_DIRECT_DATA: c_int = 0x01; // TODO(pg-port): real IO_DIRECT_DATA lives in storage/fd.h

// ---- BKPBLOCK_WILL_INIT (from xlogreader.rs, also available here) -----------
// Already imported via crate::access::transam::xlogreader above; re-declare to
// avoid a glob import in this module.
const BKPBLOCK_WILL_INIT: u8 = 0x40; // xlogrecord.h

// ---- BLCKSZ -----------------------------------------------------------------
use crate::access::transam::xlogreader::BLCKSZ;

// ---- Buffer helpers ---------------------------------------------------------
// TODO(pg-port): real BufferIsValid lives in storage/buf.h
#[inline]
unsafe fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

// ---- shmem stub -------------------------------------------------------------
// TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(
    _name: *const c_char,
    _size: usize,
    _found: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
}

// ---- PrefetchSharedBuffer stub ----------------------------------------------

/// Result type mirroring C's PrefetchBufferResult (storage/bufmgr.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PrefetchBufferResult {
    pub recent_buffer: Buffer,
    pub initiated_io: bool,
}

// TODO(pg-port): real PrefetchSharedBuffer lives in storage/buffer/bufmgr.c
unsafe fn PrefetchSharedBuffer(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
) -> PrefetchBufferResult {
    unimplemented!() // TODO(pg-port): real PrefetchSharedBuffer lives in storage/buffer/bufmgr.c
}

// ---- XLogReader family stubs ------------------------------------------------
// These live in crate::access::transam::xlogreader but are declared `pub unsafe
// fn` there; import them directly.

use crate::access::transam::xlogreader::{
    XLogBeginRead, XLogNextRecord, XLogReadAhead, XLogReleasePreviousRecord,
};

// ---- TimestampTzGetDatum stub -----------------------------------------------
// TODO(pg-port): real TimestampTzGetDatum lives in utils/timestamp.h
unsafe fn TimestampTzGetDatum(_tz: TimestampTz) -> Datum {
    unimplemented!() // TODO(pg-port): real TimestampTzGetDatum lives in utils/timestamp.h
}

// ---- GetCurrentTimestamp stub -----------------------------------------------
// TODO(pg-port): real GetCurrentTimestamp lives in utils/timestamp.c
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    unimplemented!() // TODO(pg-port): real GetCurrentTimestamp lives in utils/timestamp.c
}

// ---- funcapi stubs ----------------------------------------------------------
// TODO(pg-port): real InitMaterializedSRF lives in utils/fmgr/funcapi.c
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO(pg-port): real InitMaterializedSRF lives in utils/fmgr/funcapi.c
}

// TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.c
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: crate::access::common::tupdesc::TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.c
}

// ---- GUC_check_errdetail macro stub ----------------------------------------
// TODO(pg-port): real GUC_check_errdetail lives in utils/guc.h (via elog.h)
#[macro_export]
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {
        // TODO(pg-port): real GUC_check_errdetail lives in utils/guc.h
    };
}

// (no further items)
