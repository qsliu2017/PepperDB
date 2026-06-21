//! Implementation of IO statistics (translation of
//! `src/backend/utils/activity/pgstat_io.c`).
//!
//! This file contains the implementation of IO statistics. It is kept separate
//! from `pgstat.rs` to enforce the line between the statistics access / storage
//! implementation and the details about individual types of statistics.
//!
//! Deviations from upstream PostgreSQL 18.3 (each noted again inline):
//!
//! * PER-BACKENDTYPE LWLOCK ARRAY -> SINGLE CHANGECOUNT. Upstream guards each
//!   per-`BackendType` row of the shared IO stats with its own LWLock taken from
//!   `PgStatShared_IO.locks[BACKEND_NUM_TYPES]`. The ported `PgStatShared_IO`
//!   (in `pgstat.rs`) instead uses the seqlock-style `changecount` protocol over
//!   the whole `PgStat_IO` block (like the archiver / WAL reporters). Flush and
//!   the snapshot are therefore wrapped in changecount writes/reads rather than
//!   per-row locks; `pgstat_io_init_shmem_cb` / `pgstat_io_reset_all_cb` /
//!   `pgstat_io_snapshot_cb` are adapted accordingly.
//!
//! * IO TIMING is STUBBED. `track_io_timing` is a process-local `static mut`
//!   defaulting to false, and the `instr_time` machinery is reduced to a local
//!   stub returning 0 microseconds. The per-backend / `pgBufferUsage` /
//!   `pg_stat_database` time bookkeeping in `pgstat_count_io_op_time()` is
//!   omitted (those subsystems are unported); only the `PgStat_IO` `times`
//!   accumulation path is kept.
//!
//! * MyBackendType is STUBBED to `B_BACKEND`. `pgstat_count_backend_io_op*`,
//!   `pgstat_report_fixed`, and `pgstat_assert_is_up` are no-ops (unported).

use crate::prelude::*;

use crate::utils::activity::pgstat::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write, pgstat_shmem, pgstat_snapshot_fixed, pgStatLocal,
    GetCurrentTimestamp, LWLock, PgStatShared_IO, PgStat_BktypeIO, PgStat_Counter, PgStat_IO,
    TimestampTz, BACKEND_NUM_TYPES, IOCONTEXT_NUM_TYPES, IOOBJECT_NUM_TYPES, IOOP_NUM_TYPES,
    PGSTAT_KIND_IO,
};

// We deliberately avoid the `libc` crate; pull in `memset` via a local extern.
extern "C" {
    fn memset(dest: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// IO dimension enums (pgstat.h) -- defined locally.
// ---------------------------------------------------------------------------

/// The kind of object an IO operation targets (pgstat.h: IOObject).
pub const IOOBJECT_RELATION: c_int = 0;
pub const IOOBJECT_TEMP_RELATION: c_int = 1;
pub const IOOBJECT_WAL: c_int = 2;
pub type IOObject = c_int;

/// The context in which an IO operation occurs (pgstat.h: IOContext).
pub const IOCONTEXT_BULKREAD: c_int = 0;
pub const IOCONTEXT_BULKWRITE: c_int = 1;
pub const IOCONTEXT_INIT: c_int = 2;
pub const IOCONTEXT_NORMAL: c_int = 3;
pub const IOCONTEXT_VACUUM: c_int = 4;
pub type IOContext = c_int;

/// The kind of IO operation (pgstat.h: IOOp).
pub const IOOP_EVICT: c_int = 0;
pub const IOOP_FSYNC: c_int = 1;
pub const IOOP_HIT: c_int = 2;
pub const IOOP_REUSE: c_int = 3;
pub const IOOP_WRITEBACK: c_int = 4;
pub const IOOP_EXTEND: c_int = 5;
pub const IOOP_READ: c_int = 6;
pub const IOOP_WRITE: c_int = 7;
pub type IOOp = c_int;

// ---------------------------------------------------------------------------
// BackendType (miscadmin.h) -- defined locally, 18 values.
// ---------------------------------------------------------------------------

pub const B_INVALID: c_int = 0;
pub const B_BACKEND: c_int = 1;
pub const B_DEAD_END_BACKEND: c_int = 2;
pub const B_AUTOVAC_LAUNCHER: c_int = 3;
pub const B_AUTOVAC_WORKER: c_int = 4;
pub const B_BG_WORKER: c_int = 5;
pub const B_WAL_SENDER: c_int = 6;
pub const B_SLOTSYNC_WORKER: c_int = 7;
pub const B_STANDALONE_BACKEND: c_int = 8;
pub const B_ARCHIVER: c_int = 9;
pub const B_BG_WRITER: c_int = 10;
pub const B_CHECKPOINTER: c_int = 11;
pub const B_IO_WORKER: c_int = 12;
pub const B_STARTUP: c_int = 13;
pub const B_WAL_RECEIVER: c_int = 14;
pub const B_WAL_SUMMARIZER: c_int = 15;
pub const B_WAL_WRITER: c_int = 16;
pub const B_LOGGER: c_int = 17;
pub type BackendType = c_int;

// ---------------------------------------------------------------------------
// instr_time STUB (executor/instrument.h unported)
// ---------------------------------------------------------------------------
//
// The real `instr_time` is a monotonic-clock reading; here it is reduced to a
// plain int64 of microseconds. With `track_io_timing` stubbed off, all timing
// paths are short-circuited and these helpers always yield zero.

/// STUB: stand-in for `instr_time` (microseconds since some epoch).
pub type instr_time = int64;

/// STUB: INSTR_TIME_SET_CURRENT. Real clock unported -> 0.
#[inline]
unsafe fn instr_time_current() -> instr_time {
    0
}

/// INSTR_TIME_IS_ZERO.
#[inline]
fn instr_time_is_zero(t: instr_time) -> bool {
    t == 0
}

/// INSTR_TIME_GET_MICROSEC.
#[inline]
fn instr_time_get_microsec(t: instr_time) -> uint64 {
    t as uint64
}

// ---------------------------------------------------------------------------
// GUC / backend-identity STUBS
// ---------------------------------------------------------------------------

/// STUB GUC: `track_io_timing` (real GUC lives in `utils/misc/guc_tables.c`).
/// Defaults to false, so the timing path in `pgstat_count_io_op_time` is inert.
static mut track_io_timing: bool = false;

/// STUB: upstream reads the global `MyBackendType`; here we assume an ordinary
/// client backend so the tracks-filters and flush target a valid row.
const MyBackendType: BackendType = B_BACKEND;

/// STUB no-op: upstream sets `pgstat_report_fixed = true` to mark fixed-amount
/// stats dirty for the next report cycle (unported).
#[inline]
unsafe fn pgstat_report_fixed_set() {}

/// STUB no-op: upstream `pgstat_assert_is_up()` checks the subsystem is running.
#[inline]
unsafe fn pgstat_assert_is_up() {}

/// STUB no-op: per-backend IO op counters (`pgstat_backend.c`, unported).
#[inline]
unsafe fn pgstat_count_backend_io_op(
    _io_object: IOObject,
    _io_context: IOContext,
    _io_op: IOOp,
    _cnt: uint32,
    _bytes: uint64,
) {
}

// ---------------------------------------------------------------------------
// Module-local pending state (pgstat_io.c statics)
// ---------------------------------------------------------------------------
//
// DEVIATION: upstream's `PgStat_PendingIO` has a `pending_times` array of
// `instr_time`; here the pending accumulator is a `PgStat_BktypeIO` and its
// `times` field doubles as the pending-times accumulator (already in
// microseconds, since the instr_time stub is microseconds).

static mut PendingIOStats: PgStat_BktypeIO = PgStat_BktypeIO::zeroed();
static mut have_iostats: bool = false;

// ---------------------------------------------------------------------------
// Local helpers (pgstat.h inline predicates)
// ---------------------------------------------------------------------------

/// pgstat_is_ioop_tracked_in_bytes(): only READ / WRITE / EXTEND carry bytes.
#[inline]
fn pgstat_is_ioop_tracked_in_bytes(io_op: IOOp) -> bool {
    io_op == IOOP_EXTEND || io_op == IOOP_READ || io_op == IOOP_WRITE
}

/// `pg_memory_is_all_zeros` for a `PgStat_BktypeIO`: a plain byte scan.
unsafe fn bktype_io_is_all_zeros(p: *const PgStat_BktypeIO) -> bool {
    let bytes = p as *const u8;
    let n = size_of::<PgStat_BktypeIO>();
    let mut i = 0usize;
    while i < n {
        if *bytes.add(i) != 0 {
            return false;
        }
        i += 1;
    }
    true
}

// ---------------------------------------------------------------------------
// pgstat_bktype_io_stats_valid (pgstat_io.c)
// ---------------------------------------------------------------------------

/// Check that stats have not been counted for any combination of `IOObject`,
/// `IOContext`, and `IOOp` which are not tracked for the passed-in
/// `BackendType`. If stats are tracked for this combination and IO times are
/// non-zero, counts should be non-zero.
pub unsafe fn pgstat_bktype_io_stats_valid(
    backend_io: *const PgStat_BktypeIO,
    bktype: BackendType,
) -> bool {
    for io_object in 0..IOOBJECT_NUM_TYPES {
        for io_context in 0..IOCONTEXT_NUM_TYPES {
            for io_op in 0..IOOP_NUM_TYPES {
                // we do track it
                if pgstat_tracks_io_op(
                    bktype,
                    io_object as IOObject,
                    io_context as IOContext,
                    io_op as IOOp,
                ) {
                    // ensure that if IO times are non-zero, counts are > 0
                    if (*backend_io).times[io_object][io_context][io_op] != 0
                        && (*backend_io).counts[io_object][io_context][io_op] <= 0
                    {
                        return false;
                    }
                    continue;
                }

                // we don't track it, and it is not 0
                if (*backend_io).counts[io_object][io_context][io_op] != 0 {
                    return false;
                }
            }
        }
    }

    true
}

// ---------------------------------------------------------------------------
// pgstat_count_io_op (pgstat_io.c)
// ---------------------------------------------------------------------------

pub unsafe fn pgstat_count_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: uint32,
    bytes: uint64,
) {
    Assert!((io_object as usize) < IOOBJECT_NUM_TYPES);
    Assert!((io_context as usize) < IOCONTEXT_NUM_TYPES);
    Assert!(pgstat_is_ioop_tracked_in_bytes(io_op) || bytes == 0);
    Assert!(pgstat_tracks_io_op(
        MyBackendType,
        io_object,
        io_context,
        io_op
    ));

    PendingIOStats.counts[io_object as usize][io_context as usize][io_op as usize] +=
        cnt as PgStat_Counter;
    PendingIOStats.bytes[io_object as usize][io_context as usize][io_op as usize] += bytes;

    // Add the per-backend counts
    pgstat_count_backend_io_op(io_object, io_context, io_op, cnt, bytes);

    have_iostats = true;
    pgstat_report_fixed_set();
}

// ---------------------------------------------------------------------------
// pgstat_prepare_io_time (pgstat_io.c)
// ---------------------------------------------------------------------------

/// Initialize the internal timing for an IO operation, depending on an IO
/// timing GUC.
pub unsafe fn pgstat_prepare_io_time(track_io_guc: bool) -> instr_time {
    let io_start: instr_time;

    if track_io_guc {
        io_start = instr_time_current();
    } else {
        // There is no need to set io_start when an IO timing GUC is disabled.
        // Initialize it to zero to let pgstat_count_io_op_time() know that
        // timings should be ignored.
        io_start = 0;
    }

    io_start
}

// ---------------------------------------------------------------------------
// pgstat_count_io_op_time (pgstat_io.c)
// ---------------------------------------------------------------------------

/// Like `pgstat_count_io_op()` except it also accumulates time.
///
/// DEVIATION: the `pgstat_count_buffer_*` (`pg_stat_database`) and
/// `pgBufferUsage` (EXPLAIN) bookkeeping and the per-backend time counters are
/// omitted (those subsystems are unported); only the `PgStat_IO` `times`
/// accumulation is kept. With `track_io_timing` stubbed false, `start_time` is
/// always zero and this whole block is skipped.
pub unsafe fn pgstat_count_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    start_time: instr_time,
    cnt: uint32,
    bytes: uint64,
) {
    if !instr_time_is_zero(start_time) {
        let mut io_time: instr_time = instr_time_current();
        io_time -= start_time;

        // Upstream here updates pgstat_database buffer read/write times and
        // pgBufferUsage shared/local block times -- omitted (unported).

        PendingIOStats.times[io_object as usize][io_context as usize][io_op as usize] +=
            instr_time_get_microsec(io_time) as PgStat_Counter;

        // Upstream also adds the per-backend time count here -- omitted.
    }

    pgstat_count_io_op(io_object, io_context, io_op, cnt, bytes);
}

// ---------------------------------------------------------------------------
// pgstat_fetch_stat_io (pgstat_io.c)
// ---------------------------------------------------------------------------

pub unsafe fn pgstat_fetch_stat_io() -> *mut PgStat_IO {
    pgstat_snapshot_fixed(PGSTAT_KIND_IO);

    &mut pgStatLocal.snapshot.io
}

// ---------------------------------------------------------------------------
// pgstat_flush_io / pgstat_io_flush_cb (pgstat_io.c)
// ---------------------------------------------------------------------------

/// Simpler wrapper of `pgstat_io_flush_cb()`.
pub unsafe fn pgstat_flush_io(nowait: bool) {
    let _ = pgstat_io_flush_cb(nowait);
}

/// Flush out locally pending IO statistics.
///
/// If no stats have been recorded, returns false.
///
/// DEVIATION: upstream takes the per-`BackendType` LWLock (conditionally when
/// `nowait`) around the accumulation. Here the shared block uses the
/// changecount protocol instead, so the accumulation is bracketed by
/// `pgstat_begin/end_changecount_write`; the `nowait` fast-path that returns
/// true on a failed conditional lock acquire is therefore unreachable (the
/// changecount write never blocks) and we always proceed.
pub unsafe fn pgstat_io_flush_cb(nowait: bool) -> bool {
    let _ = nowait;

    if !have_iostats {
        return false;
    }

    let ctl = pgstat_shmem();
    let bktype_shstats: *mut PgStat_BktypeIO =
        &mut (*ctl).io.stats.stats[MyBackendType as usize];

    pgstat_begin_changecount_write(&mut (*ctl).io.changecount);

    for io_object in 0..IOOBJECT_NUM_TYPES {
        for io_context in 0..IOCONTEXT_NUM_TYPES {
            for io_op in 0..IOOP_NUM_TYPES {
                (*bktype_shstats).counts[io_object][io_context][io_op] +=
                    PendingIOStats.counts[io_object][io_context][io_op];

                (*bktype_shstats).bytes[io_object][io_context][io_op] +=
                    PendingIOStats.bytes[io_object][io_context][io_op];

                let time = PendingIOStats.times[io_object][io_context][io_op];

                (*bktype_shstats).times[io_object][io_context][io_op] +=
                    instr_time_get_microsec(time as instr_time) as PgStat_Counter;
            }
        }
    }

    Assert!(pgstat_bktype_io_stats_valid(bktype_shstats, MyBackendType));

    pgstat_end_changecount_write(&mut (*ctl).io.changecount);

    memset(
        &mut PendingIOStats as *mut _ as *mut c_void,
        0,
        size_of::<PgStat_BktypeIO>(),
    );

    have_iostats = false;

    false
}

// ---------------------------------------------------------------------------
// Name lookups (pgstat_io.c)
// ---------------------------------------------------------------------------

pub fn pgstat_get_io_context_name(io_context: IOContext) -> *const c_char {
    let s: &[u8] = match io_context {
        IOCONTEXT_BULKREAD => b"bulkread\0",
        IOCONTEXT_BULKWRITE => b"bulkwrite\0",
        IOCONTEXT_INIT => b"init\0",
        IOCONTEXT_NORMAL => b"normal\0",
        IOCONTEXT_VACUUM => b"vacuum\0",
        // elog(ERROR, "unrecognized IOContext value: %d", io_context);
        _ => panic!("unrecognized IOContext value: {}", io_context),
    };
    s.as_ptr() as *const c_char
}

pub fn pgstat_get_io_object_name(io_object: IOObject) -> *const c_char {
    let s: &[u8] = match io_object {
        IOOBJECT_RELATION => b"relation\0",
        IOOBJECT_TEMP_RELATION => b"temp relation\0",
        IOOBJECT_WAL => b"wal\0",
        // elog(ERROR, "unrecognized IOObject value: %d", io_object);
        _ => panic!("unrecognized IOObject value: {}", io_object),
    };
    s.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// Kind callbacks (pgstat_io.c)
// ---------------------------------------------------------------------------

/// DEVIATION: upstream initializes a per-`BackendType` LWLock array. With the
/// single-changecount wrapper, there are no per-row locks to initialize; the
/// embedded `lock` field of `PgStatShared_IO` is zero-initialized in shmem and
/// left inert (the LWLock stub treats it as a no-op).
pub unsafe extern "C" fn pgstat_io_init_shmem_cb(stats: *mut c_void) {
    let stat_shmem = stats as *mut PgStatShared_IO;
    // No per-BackendType lock array in this port; nothing to initialize.
    let _ = stat_shmem;
}

/// Reset all shared IO stats and record the reset timestamp.
///
/// DEVIATION: bracketed by the changecount write rather than per-row locks.
pub unsafe fn pgstat_io_reset_all_cb(ts: TimestampTz) {
    let ctl = pgstat_shmem();

    pgstat_begin_changecount_write(&mut (*ctl).io.changecount);

    (*ctl).io.stats.stat_reset_timestamp = ts;

    for i in 0..BACKEND_NUM_TYPES {
        let bktype_shstats: *mut PgStat_BktypeIO = &mut (*ctl).io.stats.stats[i];
        memset(
            bktype_shstats as *mut c_void,
            0,
            size_of::<PgStat_BktypeIO>(),
        );
    }

    pgstat_end_changecount_write(&mut (*ctl).io.changecount);
}

/// Snapshot the shared IO stats into the process-local snapshot.
///
/// DEVIATION: upstream copies each per-row block under its LWLock. Here the
/// whole `PgStat_IO` block is copied under the changecount protocol via
/// `pgstat_copy_changecounted_stats`, which is exactly what
/// `pgstat_snapshot_fixed(PGSTAT_KIND_IO)` already does; this callback simply
/// delegates to it.
pub unsafe fn pgstat_io_snapshot_cb() {
    let ctl = pgstat_shmem();
    pgstat_copy_changecounted_stats(
        &mut pgStatLocal.snapshot.io as *mut _ as *mut c_void,
        &mut (*ctl).io.stats as *mut _ as *mut c_void,
        size_of::<PgStat_IO>(),
        &mut (*ctl).io.changecount,
    );
}

// ---------------------------------------------------------------------------
// Tracks filters (pgstat_io.c)
// ---------------------------------------------------------------------------

/// IO statistics are not collected for all `BackendType`s. Returns true if the
/// `BackendType` participates in the cumulative stats subsystem for IO.
pub fn pgstat_tracks_io_bktype(bktype: BackendType) -> bool {
    match bktype {
        B_INVALID | B_DEAD_END_BACKEND | B_ARCHIVER | B_LOGGER => false,

        B_AUTOVAC_LAUNCHER | B_AUTOVAC_WORKER | B_BACKEND | B_BG_WORKER | B_BG_WRITER
        | B_CHECKPOINTER | B_IO_WORKER | B_SLOTSYNC_WORKER | B_STANDALONE_BACKEND | B_STARTUP
        | B_WAL_RECEIVER | B_WAL_SENDER | B_WAL_SUMMARIZER | B_WAL_WRITER => true,

        _ => false,
    }
}

/// Check that the given `BackendType` is expected to do IO in the given
/// `IOContext` and on the given `IOObject`.
pub fn pgstat_tracks_io_object(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
) -> bool {
    // Some BackendTypes should never track IO statistics.
    if !pgstat_tracks_io_bktype(bktype) {
        return false;
    }

    // Currently, IO on IOOBJECT_WAL objects can only occur in the
    // IOCONTEXT_NORMAL and IOCONTEXT_INIT IOContexts.
    if io_object == IOOBJECT_WAL
        && (io_context != IOCONTEXT_NORMAL && io_context != IOCONTEXT_INIT)
    {
        return false;
    }

    // Currently, IO on temporary relations can only occur in the
    // IOCONTEXT_NORMAL IOContext.
    if io_context != IOCONTEXT_NORMAL && io_object == IOOBJECT_TEMP_RELATION {
        return false;
    }

    // In core Postgres, only regular backends and WAL Sender processes
    // executing queries will use local buffers and operate on temporary
    // relations. Parallel workers will not use local buffers; track IO on
    // IOOBJECT_TEMP_RELATION for B_BG_WORKER nonetheless.
    let no_temp_rel = bktype == B_AUTOVAC_LAUNCHER
        || bktype == B_BG_WRITER
        || bktype == B_CHECKPOINTER
        || bktype == B_AUTOVAC_WORKER
        || bktype == B_STANDALONE_BACKEND
        || bktype == B_STARTUP
        || bktype == B_WAL_SUMMARIZER
        || bktype == B_WAL_WRITER
        || bktype == B_WAL_RECEIVER;

    if no_temp_rel && io_context == IOCONTEXT_NORMAL && io_object == IOOBJECT_TEMP_RELATION {
        return false;
    }

    // Some BackendTypes only perform IO under IOOBJECT_WAL, hence exclude all
    // rows for all the other objects for these.
    if (bktype == B_WAL_SUMMARIZER || bktype == B_WAL_RECEIVER || bktype == B_WAL_WRITER)
        && io_object != IOOBJECT_WAL
    {
        return false;
    }

    // Some BackendTypes do not currently perform any IO in certain IOContexts.
    if (bktype == B_CHECKPOINTER || bktype == B_BG_WRITER)
        && (io_context == IOCONTEXT_BULKREAD
            || io_context == IOCONTEXT_BULKWRITE
            || io_context == IOCONTEXT_VACUUM)
    {
        return false;
    }

    if bktype == B_AUTOVAC_LAUNCHER && io_context == IOCONTEXT_VACUUM {
        return false;
    }

    if (bktype == B_AUTOVAC_WORKER || bktype == B_AUTOVAC_LAUNCHER)
        && io_context == IOCONTEXT_BULKWRITE
    {
        return false;
    }

    true
}

/// Check that the given `IOOp` is valid for the given `BackendType` in the
/// given `IOContext` and on the given `IOObject`.
pub fn pgstat_tracks_io_op(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
) -> bool {
    // if (io_context, io_object) will never collect stats, we're done
    if !pgstat_tracks_io_object(bktype, io_object, io_context) {
        return false;
    }

    // Some BackendTypes will not do certain IOOps.
    if bktype == B_BG_WRITER
        && (io_op == IOOP_READ || io_op == IOOP_EVICT || io_op == IOOP_HIT)
    {
        return false;
    }

    if bktype == B_CHECKPOINTER
        && ((io_object != IOOBJECT_WAL && io_op == IOOP_READ)
            || (io_op == IOOP_EVICT || io_op == IOOP_HIT))
    {
        return false;
    }

    if (bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype == B_CHECKPOINTER)
        && io_op == IOOP_EXTEND
    {
        return false;
    }

    // Some BackendTypes do not perform reads with IOOBJECT_WAL.
    if io_object == IOOBJECT_WAL
        && io_op == IOOP_READ
        && (bktype == B_WAL_RECEIVER
            || bktype == B_BG_WRITER
            || bktype == B_AUTOVAC_LAUNCHER
            || bktype == B_AUTOVAC_WORKER
            || bktype == B_WAL_WRITER)
    {
        return false;
    }

    // Temporary tables are not logged and thus do not require fsync'ing.
    // Writeback is not requested for temporary tables.
    if io_object == IOOBJECT_TEMP_RELATION
        && (io_op == IOOP_FSYNC || io_op == IOOP_WRITEBACK)
    {
        return false;
    }

    // Some IOOps are not valid in certain IOContexts and some IOOps are only
    // valid in certain contexts.
    if io_context == IOCONTEXT_BULKREAD && io_op == IOOP_EXTEND {
        return false;
    }

    let strategy_io_context = io_context == IOCONTEXT_BULKREAD
        || io_context == IOCONTEXT_BULKWRITE
        || io_context == IOCONTEXT_VACUUM;

    // IOOP_REUSE is only relevant when a BufferAccessStrategy is in use.
    if !strategy_io_context && io_op == IOOP_REUSE {
        return false;
    }

    // IOOBJECT_WAL IOObject will not do certain IOOps depending on IOContext.
    if io_object == IOOBJECT_WAL
        && io_context == IOCONTEXT_INIT
        && !(io_op == IOOP_WRITE || io_op == IOOP_FSYNC)
    {
        return false;
    }

    if io_object == IOOBJECT_WAL
        && io_context == IOCONTEXT_NORMAL
        && !(io_op == IOOP_WRITE || io_op == IOOP_READ || io_op == IOOP_FSYNC)
    {
        return false;
    }

    // IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
    // counted in the IOCONTEXT_NORMAL IOContext.
    if strategy_io_context && io_op == IOOP_FSYNC {
        return false;
    }

    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize tests that mutate the process-global PendingIOStats / shared
    // block / snapshot so they don't race under the test harness's threads.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    unsafe fn reset_globals() {
        memset(
            &mut PendingIOStats as *mut _ as *mut c_void,
            0,
            size_of::<PgStat_BktypeIO>(),
        );
        have_iostats = false;
        let ctl = pgstat_shmem();
        memset(
            &mut (*ctl).io as *mut _ as *mut c_void,
            0,
            size_of::<PgStatShared_IO>(),
        );
        memset(
            &mut pgStatLocal.snapshot.io as *mut _ as *mut c_void,
            0,
            size_of::<PgStat_IO>(),
        );
    }

    #[test]
    fn count_flush_fetch_roundtrip() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            reset_globals();

            // RELATION / NORMAL / READ is tracked for B_BACKEND.
            assert!(pgstat_tracks_io_op(
                B_BACKEND,
                IOOBJECT_RELATION,
                IOCONTEXT_NORMAL,
                IOOP_READ
            ));

            pgstat_count_io_op(IOOBJECT_RELATION, IOCONTEXT_NORMAL, IOOP_READ, 1, 8192);

            // Pending got bumped.
            assert_eq!(
                PendingIOStats.counts[IOOBJECT_RELATION as usize][IOCONTEXT_NORMAL as usize]
                    [IOOP_READ as usize],
                1
            );
            assert_eq!(
                PendingIOStats.bytes[IOOBJECT_RELATION as usize][IOCONTEXT_NORMAL as usize]
                    [IOOP_READ as usize],
                8192
            );
            assert!(have_iostats);

            pgstat_flush_io(false);

            // Pending cleared after flush.
            assert!(!have_iostats);
            assert_eq!(
                PendingIOStats.counts[IOOBJECT_RELATION as usize][IOCONTEXT_NORMAL as usize]
                    [IOOP_READ as usize],
                0
            );

            // Fetch the snapshot and verify the accumulated shared counters.
            let snap = pgstat_fetch_stat_io();
            assert_eq!(
                (*snap).stats[B_BACKEND as usize].counts[IOOBJECT_RELATION as usize]
                    [IOCONTEXT_NORMAL as usize][IOOP_READ as usize],
                1
            );
            assert_eq!(
                (*snap).stats[B_BACKEND as usize].bytes[IOOBJECT_RELATION as usize]
                    [IOCONTEXT_NORMAL as usize][IOOP_READ as usize],
                8192
            );
        }
    }

    #[test]
    fn flush_without_pending_is_noop() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            reset_globals();
            // No counts recorded -> have_iostats false -> flush returns false.
            assert!(!pgstat_io_flush_cb(false));
        }
    }

    #[test]
    fn tracks_filters_match_upstream_samples() {
        // A handful of representative cases from the upstream switches.
        assert!(!pgstat_tracks_io_bktype(B_ARCHIVER));
        assert!(pgstat_tracks_io_bktype(B_BACKEND));
        // BG writer never READs.
        assert!(!pgstat_tracks_io_op(
            B_BG_WRITER,
            IOOBJECT_RELATION,
            IOCONTEXT_NORMAL,
            IOOP_READ
        ));
        // REUSE only valid in a strategy context.
        assert!(!pgstat_tracks_io_op(
            B_BACKEND,
            IOOBJECT_RELATION,
            IOCONTEXT_NORMAL,
            IOOP_REUSE
        ));
        assert!(pgstat_tracks_io_op(
            B_BACKEND,
            IOOBJECT_RELATION,
            IOCONTEXT_VACUUM,
            IOOP_REUSE
        ));
    }

    #[test]
    fn all_zeros_scan() {
        unsafe {
            let z = PgStat_BktypeIO::zeroed();
            assert!(bktype_io_is_all_zeros(&z));
            let mut nz = PgStat_BktypeIO::zeroed();
            nz.counts[0][0][0] = 1;
            assert!(!bktype_io_is_all_zeros(&nz));
        }
    }
}
