//! Translation of postgres/src/backend/executor/instrument.c
//! (+ MERGED structs/consts from postgres/src/include/executor/instrument.h)
//!
//! Functions for instrumentation of plan execution: run-time statistics
//! collection over instr_time.  The instr_time arithmetic comes from
//! crate::portability::instr_time.
//!
//! NOT-PORTED dependencies referenced here:
//!   - The pgstat globals `pgBufferUsage` / `pgWalUsage` are file-local statics
//!     here (the real PG ones are PGDLLIMPORT globals updated by the buffer/WAL
//!     managers, which are not ported yet).  Their struct arithmetic
//!     (BufferUsageAccumDiff / WalUsageAccumDiff / *Add) is FULLY REAL.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::portability::instr_time::*;
use core::ptr::write_bytes;

// ---------------------------------------------------------------------------
// Merged from executor/instrument.h
// ---------------------------------------------------------------------------

/// BufferUsage and WalUsage counters keep being incremented infinitely, i.e.,
/// must never be reset to zero, so that we can calculate how much the counters
/// are incremented in an arbitrary period.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct BufferUsage {
    pub shared_blks_hit: int64,             // # of shared buffer hits
    pub shared_blks_read: int64,            // # of shared disk blocks read
    pub shared_blks_dirtied: int64,         // # of shared blocks dirtied
    pub shared_blks_written: int64,         // # of shared disk blocks written
    pub local_blks_hit: int64,              // # of local buffer hits
    pub local_blks_read: int64,             // # of local disk blocks read
    pub local_blks_dirtied: int64,          // # of local blocks dirtied
    pub local_blks_written: int64,          // # of local disk blocks written
    pub temp_blks_read: int64,              // # of temp blocks read
    pub temp_blks_written: int64,           // # of temp blocks written
    pub shared_blk_read_time: instr_time,   // time spent reading shared blocks
    pub shared_blk_write_time: instr_time,  // time spent writing shared blocks
    pub local_blk_read_time: instr_time,    // time spent reading local blocks
    pub local_blk_write_time: instr_time,   // time spent writing local blocks
    pub temp_blk_read_time: instr_time,     // time spent reading temp blocks
    pub temp_blk_write_time: instr_time,    // time spent writing temp blocks
}

/// WalUsage tracks only WAL activity like WAL records generation that can be
/// measured per query and is displayed by EXPLAIN, pg_stat_statements, etc.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct WalUsage {
    pub wal_records: int64,         // # of WAL records produced
    pub wal_fpi: int64,             // # of WAL full page images produced
    pub wal_bytes: uint64,          // size of WAL records produced
    pub wal_buffers_full: int64,    // # of times the WAL buffers became full
}

/// Flag bits included in InstrAlloc's instrument_options bitmask
/// (typedef enum InstrumentOption).
pub const INSTRUMENT_TIMER: int32 = 1 << 0; // needs timer (and row counts)
pub const INSTRUMENT_BUFFERS: int32 = 1 << 1; // needs buffer usage
pub const INSTRUMENT_ROWS: int32 = 1 << 2; // needs row count
pub const INSTRUMENT_WAL: int32 = 1 << 3; // needs WAL usage
pub const INSTRUMENT_ALL: int32 = PG_INT32_MAX;

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Instrumentation {
    /* Parameters set at node creation: */
    pub need_timer: bool,    // true if we need timer data
    pub need_bufusage: bool, // true if we need buffer usage data
    pub need_walusage: bool, // true if we need WAL usage data
    pub async_mode: bool,    // true if node is in async mode
    /* Info about current plan cycle: */
    pub running: bool,                 // true if we've completed first tuple
    pub starttime: instr_time,         // start time of current iteration of node
    pub counter: instr_time,           // accumulated runtime for this node
    pub firsttuple: f64,               // time for first tuple of this cycle
    pub tuplecount: f64,               // # of tuples emitted so far this cycle
    pub bufusage_start: BufferUsage,   // buffer usage at start
    pub walusage_start: WalUsage,      // WAL usage at start
    /* Accumulated statistics across all completed cycles: */
    pub startup: f64,    // total startup time (in seconds)
    pub total: f64,      // total time (in seconds)
    pub ntuples: f64,    // total tuples produced
    pub ntuples2: f64,   // secondary node-specific tuple counter
    pub nloops: f64,     // # of run cycles for this node
    pub nfiltered1: f64, // # of tuples removed by scanqual or joinqual
    pub nfiltered2: f64, // # of tuples removed by "other" quals
    pub bufusage: BufferUsage, // total buffer usage
    pub walusage: WalUsage,    // total WAL usage
}

/// WorkerInstrumentation: num_workers followed by a flexible array of
/// Instrumentation structs.
#[repr(C)]
pub struct WorkerInstrumentation {
    pub num_workers: c_int, // # of structures that follow
    pub instrument: [Instrumentation; FLEXIBLE_ARRAY_MEMBER],
}

// ---------------------------------------------------------------------------
// File-local pgstat globals (STUB: real PG ones are PGDLLIMPORT globals updated
// by the buffer/WAL managers, not ported).  The diff/add arithmetic over them
// is fully real.
// TODO: replace with the real ported pgBufferUsage/pgWalUsage globals.
// ---------------------------------------------------------------------------

static mut pgBufferUsage: BufferUsage = BufferUsage {
    shared_blks_hit: 0,
    shared_blks_read: 0,
    shared_blks_dirtied: 0,
    shared_blks_written: 0,
    local_blks_hit: 0,
    local_blks_read: 0,
    local_blks_dirtied: 0,
    local_blks_written: 0,
    temp_blks_read: 0,
    temp_blks_written: 0,
    shared_blk_read_time: instr_time { ticks: 0 },
    shared_blk_write_time: instr_time { ticks: 0 },
    local_blk_read_time: instr_time { ticks: 0 },
    local_blk_write_time: instr_time { ticks: 0 },
    temp_blk_read_time: instr_time { ticks: 0 },
    temp_blk_write_time: instr_time { ticks: 0 },
};
static mut save_pgBufferUsage: BufferUsage = unsafe { core::mem::zeroed() };
pub static mut pgWalUsage: WalUsage = WalUsage {
    wal_records: 0,
    wal_fpi: 0,
    wal_bytes: 0,
    wal_buffers_full: 0,
};
static mut save_pgWalUsage: WalUsage = unsafe { core::mem::zeroed() };

// ---------------------------------------------------------------------------
// instrument.c
// ---------------------------------------------------------------------------

/// Allocate new instrumentation structure(s)
pub unsafe fn InstrAlloc(
    n: c_int,
    instrument_options: c_int,
    async_mode: bool,
) -> *mut Instrumentation {
    /* initialize all fields to zeroes, then modify as needed */
    let instr =
        palloc0(n as Size * core::mem::size_of::<Instrumentation>()) as *mut Instrumentation;
    if instrument_options & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER | INSTRUMENT_WAL) != 0 {
        let need_buffers = (instrument_options & INSTRUMENT_BUFFERS) != 0;
        let need_wal = (instrument_options & INSTRUMENT_WAL) != 0;
        let need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;

        for i in 0..n {
            let p = instr.offset(i as isize);
            (*p).need_bufusage = need_buffers;
            (*p).need_walusage = need_wal;
            (*p).need_timer = need_timer;
            (*p).async_mode = async_mode;
        }
    }

    instr
}

/// Initialize a pre-allocated instrumentation structure.
pub unsafe fn InstrInit(instr: *mut Instrumentation, instrument_options: c_int) {
    write_bytes(instr, 0, 1);
    (*instr).need_bufusage = (instrument_options & INSTRUMENT_BUFFERS) != 0;
    (*instr).need_walusage = (instrument_options & INSTRUMENT_WAL) != 0;
    (*instr).need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;
}

/// Entry to a plan node
pub unsafe fn InstrStartNode(instr: *mut Instrumentation) {
    if (*instr).need_timer && !INSTR_TIME_SET_CURRENT_LAZY(&mut (*instr).starttime) {
        elog!(ERROR, "InstrStartNode called twice in a row");
    }

    /* save buffer usage totals at node entry, if needed */
    if (*instr).need_bufusage {
        (*instr).bufusage_start = pgBufferUsage;
    }

    if (*instr).need_walusage {
        (*instr).walusage_start = pgWalUsage;
    }
}

/// Exit from a plan node
pub unsafe fn InstrStopNode(instr: *mut Instrumentation, nTuples: f64) {
    let save_tuplecount = (*instr).tuplecount;
    let mut endtime: instr_time = instr_time::default();

    /* count the returned tuples */
    (*instr).tuplecount += nTuples;

    /* let's update the time only if the timer was requested */
    if (*instr).need_timer {
        if INSTR_TIME_IS_ZERO((*instr).starttime) {
            elog!(ERROR, "InstrStopNode called without start");
        }

        INSTR_TIME_SET_CURRENT(&mut endtime);
        INSTR_TIME_ACCUM_DIFF(&mut (*instr).counter, endtime, (*instr).starttime);

        INSTR_TIME_SET_ZERO(&mut (*instr).starttime);
    }

    /* Add delta of buffer usage since entry to node's totals */
    if (*instr).need_bufusage {
        BufferUsageAccumDiff(
            &mut (*instr).bufusage,
            &pgBufferUsage,
            &(*instr).bufusage_start,
        );
    }

    if (*instr).need_walusage {
        WalUsageAccumDiff(
            &mut (*instr).walusage,
            &pgWalUsage,
            &(*instr).walusage_start,
        );
    }

    /* Is this the first tuple of this cycle? */
    if !(*instr).running {
        (*instr).running = true;
        (*instr).firsttuple = INSTR_TIME_GET_DOUBLE((*instr).counter);
    } else {
        /*
         * In async mode, if the plan node hadn't emitted any tuples before,
         * this might be the first tuple
         */
        if (*instr).async_mode && save_tuplecount < 1.0 {
            (*instr).firsttuple = INSTR_TIME_GET_DOUBLE((*instr).counter);
        }
    }
}

/// Update tuple count
pub unsafe fn InstrUpdateTupleCount(instr: *mut Instrumentation, nTuples: f64) {
    /* count the returned tuples */
    (*instr).tuplecount += nTuples;
}

/// Finish a run cycle for a plan node
pub unsafe fn InstrEndLoop(instr: *mut Instrumentation) {
    /* Skip if nothing has happened, or already shut down */
    if !(*instr).running {
        return;
    }

    if !INSTR_TIME_IS_ZERO((*instr).starttime) {
        elog!(ERROR, "InstrEndLoop called on running node");
    }

    /* Accumulate per-cycle statistics into totals */
    let totaltime = INSTR_TIME_GET_DOUBLE((*instr).counter);

    (*instr).startup += (*instr).firsttuple;
    (*instr).total += totaltime;
    (*instr).ntuples += (*instr).tuplecount;
    (*instr).nloops += 1.0;

    /* Reset for next cycle (if any) */
    (*instr).running = false;
    INSTR_TIME_SET_ZERO(&mut (*instr).starttime);
    INSTR_TIME_SET_ZERO(&mut (*instr).counter);
    (*instr).firsttuple = 0.0;
    (*instr).tuplecount = 0.0;
}

/// aggregate instrumentation information
pub unsafe fn InstrAggNode(dst: *mut Instrumentation, add: *mut Instrumentation) {
    if !(*dst).running && (*add).running {
        (*dst).running = true;
        (*dst).firsttuple = (*add).firsttuple;
    } else if (*dst).running && (*add).running && (*dst).firsttuple > (*add).firsttuple {
        (*dst).firsttuple = (*add).firsttuple;
    }

    INSTR_TIME_ADD(&mut (*dst).counter, (*add).counter);

    (*dst).tuplecount += (*add).tuplecount;
    (*dst).startup += (*add).startup;
    (*dst).total += (*add).total;
    (*dst).ntuples += (*add).ntuples;
    (*dst).ntuples2 += (*add).ntuples2;
    (*dst).nloops += (*add).nloops;
    (*dst).nfiltered1 += (*add).nfiltered1;
    (*dst).nfiltered2 += (*add).nfiltered2;

    /* Add delta of buffer usage since entry to node's totals */
    if (*dst).need_bufusage {
        BufferUsageAdd(&mut (*dst).bufusage, &(*add).bufusage);
    }

    if (*dst).need_walusage {
        WalUsageAdd(&mut (*dst).walusage, &(*add).walusage);
    }
}

/// note current values during parallel executor startup
pub unsafe fn InstrStartParallelQuery() {
    save_pgBufferUsage = pgBufferUsage;
    save_pgWalUsage = pgWalUsage;
}

/// report usage after parallel executor shutdown
pub unsafe fn InstrEndParallelQuery(bufusage: *mut BufferUsage, walusage: *mut WalUsage) {
    write_bytes(bufusage, 0, 1);
    BufferUsageAccumDiff(bufusage, &pgBufferUsage, &save_pgBufferUsage);
    write_bytes(walusage, 0, 1);
    WalUsageAccumDiff(walusage, &pgWalUsage, &save_pgWalUsage);
}

/// accumulate work done by workers in leader's stats
pub unsafe fn InstrAccumParallelQuery(bufusage: *mut BufferUsage, walusage: *mut WalUsage) {
    BufferUsageAdd(&raw mut pgBufferUsage, bufusage);
    WalUsageAdd(&raw mut pgWalUsage, walusage);
}

/// dst += add
unsafe fn BufferUsageAdd(dst: *mut BufferUsage, add: *const BufferUsage) {
    (*dst).shared_blks_hit += (*add).shared_blks_hit;
    (*dst).shared_blks_read += (*add).shared_blks_read;
    (*dst).shared_blks_dirtied += (*add).shared_blks_dirtied;
    (*dst).shared_blks_written += (*add).shared_blks_written;
    (*dst).local_blks_hit += (*add).local_blks_hit;
    (*dst).local_blks_read += (*add).local_blks_read;
    (*dst).local_blks_dirtied += (*add).local_blks_dirtied;
    (*dst).local_blks_written += (*add).local_blks_written;
    (*dst).temp_blks_read += (*add).temp_blks_read;
    (*dst).temp_blks_written += (*add).temp_blks_written;
    INSTR_TIME_ADD(&mut (*dst).shared_blk_read_time, (*add).shared_blk_read_time);
    INSTR_TIME_ADD(&mut (*dst).shared_blk_write_time, (*add).shared_blk_write_time);
    INSTR_TIME_ADD(&mut (*dst).local_blk_read_time, (*add).local_blk_read_time);
    INSTR_TIME_ADD(&mut (*dst).local_blk_write_time, (*add).local_blk_write_time);
    INSTR_TIME_ADD(&mut (*dst).temp_blk_read_time, (*add).temp_blk_read_time);
    INSTR_TIME_ADD(&mut (*dst).temp_blk_write_time, (*add).temp_blk_write_time);
}

/// dst += add - sub
pub unsafe fn BufferUsageAccumDiff(
    dst: *mut BufferUsage,
    add: *const BufferUsage,
    sub: *const BufferUsage,
) {
    (*dst).shared_blks_hit += (*add).shared_blks_hit - (*sub).shared_blks_hit;
    (*dst).shared_blks_read += (*add).shared_blks_read - (*sub).shared_blks_read;
    (*dst).shared_blks_dirtied += (*add).shared_blks_dirtied - (*sub).shared_blks_dirtied;
    (*dst).shared_blks_written += (*add).shared_blks_written - (*sub).shared_blks_written;
    (*dst).local_blks_hit += (*add).local_blks_hit - (*sub).local_blks_hit;
    (*dst).local_blks_read += (*add).local_blks_read - (*sub).local_blks_read;
    (*dst).local_blks_dirtied += (*add).local_blks_dirtied - (*sub).local_blks_dirtied;
    (*dst).local_blks_written += (*add).local_blks_written - (*sub).local_blks_written;
    (*dst).temp_blks_read += (*add).temp_blks_read - (*sub).temp_blks_read;
    (*dst).temp_blks_written += (*add).temp_blks_written - (*sub).temp_blks_written;
    INSTR_TIME_ACCUM_DIFF(
        &mut (*dst).shared_blk_read_time,
        (*add).shared_blk_read_time,
        (*sub).shared_blk_read_time,
    );
    INSTR_TIME_ACCUM_DIFF(
        &mut (*dst).shared_blk_write_time,
        (*add).shared_blk_write_time,
        (*sub).shared_blk_write_time,
    );
    INSTR_TIME_ACCUM_DIFF(
        &mut (*dst).local_blk_read_time,
        (*add).local_blk_read_time,
        (*sub).local_blk_read_time,
    );
    INSTR_TIME_ACCUM_DIFF(
        &mut (*dst).local_blk_write_time,
        (*add).local_blk_write_time,
        (*sub).local_blk_write_time,
    );
    INSTR_TIME_ACCUM_DIFF(
        &mut (*dst).temp_blk_read_time,
        (*add).temp_blk_read_time,
        (*sub).temp_blk_read_time,
    );
    INSTR_TIME_ACCUM_DIFF(
        &mut (*dst).temp_blk_write_time,
        (*add).temp_blk_write_time,
        (*sub).temp_blk_write_time,
    );
}

/// helper functions for WAL usage accumulation: dst += add
unsafe fn WalUsageAdd(dst: *mut WalUsage, add: *const WalUsage) {
    (*dst).wal_bytes += (*add).wal_bytes;
    (*dst).wal_records += (*add).wal_records;
    (*dst).wal_fpi += (*add).wal_fpi;
    (*dst).wal_buffers_full += (*add).wal_buffers_full;
}

/// dst += add - sub
pub unsafe fn WalUsageAccumDiff(dst: *mut WalUsage, add: *const WalUsage, sub: *const WalUsage) {
    (*dst).wal_bytes += (*add).wal_bytes - (*sub).wal_bytes;
    (*dst).wal_records += (*add).wal_records - (*sub).wal_records;
    (*dst).wal_fpi += (*add).wal_fpi - (*sub).wal_fpi;
    (*dst).wal_buffers_full += (*add).wal_buffers_full - (*sub).wal_buffers_full;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_usage_accum_diff_is_fieldwise() {
        unsafe {
            let mut dst = BufferUsage::default();
            let mut add = BufferUsage::default();
            let mut sub = BufferUsage::default();

            add.shared_blks_hit = 100;
            sub.shared_blks_hit = 40;
            add.shared_blks_read = 7;
            sub.shared_blks_read = 2;
            add.temp_blks_written = 9;
            sub.temp_blks_written = 1;
            add.shared_blk_read_time = instr_time { ticks: 500 };
            sub.shared_blk_read_time = instr_time { ticks: 200 };

            // dst starts non-zero to prove accumulation (+=).
            dst.shared_blks_hit = 5;

            BufferUsageAccumDiff(&mut dst, &add, &sub);

            assert_eq!(dst.shared_blks_hit, 5 + (100 - 40));
            assert_eq!(dst.shared_blks_read, 7 - 2);
            assert_eq!(dst.temp_blks_written, 9 - 1);
            assert_eq!(dst.shared_blk_read_time.ticks, 500 - 200);
            // untouched fields stay zero
            assert_eq!(dst.local_blks_hit, 0);
        }
    }

    #[test]
    fn wal_usage_accum_diff_is_fieldwise() {
        unsafe {
            let mut dst = WalUsage::default();
            let mut add = WalUsage::default();
            let mut sub = WalUsage::default();
            add.wal_records = 10;
            sub.wal_records = 3;
            add.wal_bytes = 1000;
            sub.wal_bytes = 250;
            WalUsageAccumDiff(&mut dst, &add, &sub);
            assert_eq!(dst.wal_records, 7);
            assert_eq!(dst.wal_bytes, 750);
            assert_eq!(dst.wal_fpi, 0);
        }
    }

    #[test]
    fn instr_init_sets_need_timer_from_flag() {
        unsafe {
            let mut instr = Instrumentation::default();
            // pre-dirty to confirm InstrInit zeroes first
            instr.tuplecount = 123.0;
            instr.running = true;

            InstrInit(&mut instr, INSTRUMENT_TIMER);
            assert!(instr.need_timer);
            assert!(!instr.need_bufusage);
            assert!(!instr.need_walusage);
            assert_eq!(instr.tuplecount, 0.0);
            assert!(!instr.running);

            InstrInit(&mut instr, INSTRUMENT_BUFFERS | INSTRUMENT_WAL);
            assert!(!instr.need_timer);
            assert!(instr.need_bufusage);
            assert!(instr.need_walusage);

            InstrInit(&mut instr, INSTRUMENT_ALL);
            assert!(instr.need_timer);
            assert!(instr.need_bufusage);
            assert!(instr.need_walusage);
        }
    }
}
