//! utils/activity/pgstat_backend.c - Implementation of backend statistics.
//!
//! This statistics kind uses a proc number as object ID for the hash table of
//! pgstats.  Entries are created each time a process is spawned, and are dropped
//! when the process exits.  These are not written to the pgstats file on disk.
//! Pending statistics are managed without direct interactions with
//! PgStat_EntryRef->pending, relying on PendingBackendStats instead so as it is
//! possible to report data within critical sections.

use crate::prelude::*;
use crate::miscadmin::TimestampTz;

use crate::executor::instrument::{pgWalUsage, WalUsage, WalUsageAccumDiff};
use crate::miscadmin::{
    BackendType, MyBackendType, B_ARCHIVER, B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND,
    B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_DEAD_END_BACKEND, B_INVALID, B_IO_WORKER, B_LOGGER,
    B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND, B_STARTUP, B_WAL_RECEIVER, B_WAL_SENDER,
    B_WAL_SUMMARIZER, B_WAL_WRITER,
};
use crate::portability::instr_time::{instr_time, INSTR_TIME_ADD, INSTR_TIME_GET_MICROSEC};
use crate::storage::procnumber::{MyProcNumber, ProcNumber};
use crate::utils::activity::pgstat::{
    PgStat_BktypeIO, PgStat_Counter, PgStat_Kind, PgStat_WalCounters, IOCONTEXT_NUM_TYPES,
    IOOBJECT_NUM_TYPES, IOOP_NUM_TYPES,
};
use crate::utils::activity::pgstat_internal::{
    pgstat_get_entry_ref_locked, pgstat_report_fixed, pgstat_unlock_entry, PgStatShared_Common,
    PgStat_EntryRef,
};
use crate::utils::activity::pgstat_io::{
    pgstat_tracks_io_op, IOContext, IOObject, IOOp,
};

// ---------------------------------------------------------------------------
// Local const/type stubs for generated-catalog/header symbols not yet ported.
// ---------------------------------------------------------------------------

// pgstat.h: PGSTAT_KIND_BACKEND. The hash table uses a proc number as object ID.
const PGSTAT_KIND_BACKEND: PgStat_Kind = 13;

// pgstat_internal.h: flags controlling which backend statistics to flush.
const PGSTAT_BACKEND_FLUSH_IO: bits32 = 1 << 0;
const PGSTAT_BACKEND_FLUSH_WAL: bits32 = 1 << 1;
const PGSTAT_BACKEND_FLUSH_ALL: bits32 = PGSTAT_BACKEND_FLUSH_IO | PGSTAT_BACKEND_FLUSH_WAL;

// ---------------------------------------------------------------------------
// Real struct definitions (pgstat.h). The canonical PgStat_Backend in
// pgstat_internal.rs is presently a c_void placeholder; define the real layout
// here so backend stats can be flushed, and cast shared_stats accordingly.
// TODO: dedup once PgStat_Backend gains a real definition upstream.
// ---------------------------------------------------------------------------

// pgstat.h: PgStat_PendingIO. Same IO data as PGSTAT_KIND_IO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_PendingIO {
    pub bytes: [[[uint64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub pending_times: [[[instr_time; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}

impl PgStat_PendingIO {
    pub const fn zeroed() -> Self {
        PgStat_PendingIO {
            bytes: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            counts: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            pending_times: [[[instr_time { ticks: 0 }; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
        }
    }
}

// pgstat.h: PgStat_Backend. The shared, fixed-size per-backend statistics.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_Backend {
    pub stat_reset_timestamp: TimestampTz,
    pub io_stats: PgStat_BktypeIO,
    pub wal_counters: PgStat_WalCounters,
}

impl PgStat_Backend {
    pub const fn zeroed() -> Self {
        PgStat_Backend {
            stat_reset_timestamp: 0,
            io_stats: PgStat_BktypeIO::zeroed(),
            wal_counters: PgStat_WalCounters::zeroed(),
        }
    }
}

// pgstat.h: PgStat_BackendPending. Backend statistics waiting to be flushed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_BackendPending {
    // Backend statistics store the same amount of IO data as PGSTAT_KIND_IO.
    pub pending_io: PgStat_PendingIO,
}

impl PgStat_BackendPending {
    pub const fn zeroed() -> Self {
        PgStat_BackendPending {
            pending_io: PgStat_PendingIO::zeroed(),
        }
    }
}

// pgstat_internal.h: PgStatShared_Backend. Local mirror with the real stats
// payload (the canonical struct's `stats` field is presently a c_void).
#[repr(C)]
pub struct PgStatShared_Backend {
    pub header: PgStatShared_Common,
    pub stats: PgStat_Backend,
}

// ---------------------------------------------------------------------------
// File-local statics.
// ---------------------------------------------------------------------------

// Backend statistics counts waiting to be flushed out. These counters may be
// reported within critical sections so we use static memory in order to avoid
// memory allocation.
static mut PendingBackendStats: PgStat_BackendPending = PgStat_BackendPending::zeroed();
static mut backend_has_iostats: bool = false;

// WAL usage counters saved from pgWalUsage at the previous call to
// pgstat_flush_backend().  This is used to calculate how much WAL usage happens
// between pgstat_flush_backend() calls, by subtracting the previous counters
// from the current ones.
static mut prevBackendWalUsage: WalUsage = WalUsage {
    wal_records: 0,
    wal_fpi: 0,
    wal_bytes: 0,
    wal_buffers_full: 0,
};

// ---------------------------------------------------------------------------
// Local stubs for callees not yet ported.
// ---------------------------------------------------------------------------

// storage/proc.h: BackendPidGetProc / AuxiliaryPidGetProc. TODO: not ported.
unsafe fn BackendPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!()
}
unsafe fn AuxiliaryPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!()
}
// storage/proc.h: GetNumberFromPGProc. TODO: not ported.
unsafe fn GetNumberFromPGProc(_proc: *mut PGPROC) -> ProcNumber {
    unimplemented!()
}
// utils/backend_status.h: pgstat_get_beentry_by_proc_number. TODO: not ported.
unsafe fn pgstat_get_beentry_by_proc_number(_procNumber: ProcNumber) -> *mut PgBackendStatus {
    unimplemented!()
}
// pgstat.c: pgstat_fetch_entry. Fetch a snapshot copy of an entry's stats.
unsafe fn pgstat_fetch_entry(kind: PgStat_Kind, dboid: Oid, objoid: Oid) -> *mut c_void {
    crate::utils::activity::pgstat::pgstat_fetch_entry(kind, dboid, objoid)
}

// storage/proc.h: PGPROC. TODO: opaque placeholder.
#[repr(C)]
pub struct PGPROC {
    _private: [u8; 0],
}

// utils/backend_status.h: PgBackendStatus, minimal fields used here.
#[repr(C)]
pub struct PgBackendStatus {
    pub st_procpid: c_int,
    pub st_backendType: BackendType,
}

// ---------------------------------------------------------------------------
// Utility routines to report I/O stats for backends, kept here to avoid
// exposing PendingBackendStats to the outside world.
// ---------------------------------------------------------------------------

// STUB GUCs: track_io_timing / track_wal_io_timing live in guc_tables.c.
static mut track_io_timing: bool = false;
static mut track_wal_io_timing: bool = false;

pub unsafe fn pgstat_count_backend_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    io_time: instr_time,
) {
    Assert!(track_io_timing || track_wal_io_timing);

    if !pgstat_tracks_backend_bktype(MyBackendType) {
        return;
    }

    Assert!(pgstat_tracks_io_op(MyBackendType, io_object, io_context, io_op));

    INSTR_TIME_ADD(
        &mut PendingBackendStats.pending_io.pending_times[io_object as usize][io_context as usize]
            [io_op as usize],
        io_time,
    );

    backend_has_iostats = true;
    pgstat_report_fixed = true;
}

pub unsafe fn pgstat_count_backend_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: uint32,
    bytes: uint64,
) {
    if !pgstat_tracks_backend_bktype(MyBackendType) {
        return;
    }

    Assert!(pgstat_tracks_io_op(MyBackendType, io_object, io_context, io_op));

    PendingBackendStats.pending_io.counts[io_object as usize][io_context as usize]
        [io_op as usize] += cnt as PgStat_Counter;
    PendingBackendStats.pending_io.bytes[io_object as usize][io_context as usize]
        [io_op as usize] += bytes;

    backend_has_iostats = true;
    pgstat_report_fixed = true;
}

// Returns statistics of a backend by proc number.
pub unsafe fn pgstat_fetch_stat_backend(procNumber: ProcNumber) -> *mut PgStat_Backend {
    let backend_entry: *mut PgStat_Backend = pgstat_fetch_entry(
        PGSTAT_KIND_BACKEND,
        InvalidOid,
        procNumber as Oid,
    ) as *mut PgStat_Backend;

    backend_entry
}

// Returns statistics of a backend by pid.
//
// This routine includes sanity checks to ensure that the backend exists and is
// running.  "bktype" can be optionally defined to return the BackendType of the
// backend whose statistics are returned.
pub unsafe fn pgstat_fetch_stat_backend_by_pid(
    pid: c_int,
    bktype: *mut BackendType,
) -> *mut PgStat_Backend {
    let mut proc: *mut PGPROC;
    let beentry: *mut PgBackendStatus;
    let procNumber: ProcNumber;
    let backend_stats: *mut PgStat_Backend;

    proc = BackendPidGetProc(pid);
    if !bktype.is_null() {
        *bktype = B_INVALID;
    }

    // this could be an auxiliary process
    if proc.is_null() {
        proc = AuxiliaryPidGetProc(pid);
    }

    if proc.is_null() {
        return null_mut();
    }

    procNumber = GetNumberFromPGProc(proc);

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return null_mut();
    }

    // check if the backend type tracks statistics
    if !pgstat_tracks_backend_bktype((*beentry).st_backendType) {
        return null_mut();
    }

    // if PID does not match, leave
    if (*beentry).st_procpid != pid {
        return null_mut();
    }

    if !bktype.is_null() {
        *bktype = (*beentry).st_backendType;
    }

    // Retrieve the entry.  Note that "beentry" may be freed depending on the
    // value of stats_fetch_consistency, so do not access it from this point.
    backend_stats = pgstat_fetch_stat_backend(procNumber);
    if backend_stats.is_null() {
        if !bktype.is_null() {
            *bktype = B_INVALID;
        }
        return null_mut();
    }

    backend_stats
}

// Flush out locally pending backend IO statistics.  Locking is managed by the
// caller.
unsafe fn pgstat_flush_backend_entry_io(entry_ref: *mut PgStat_EntryRef) {
    let shbackendent: *mut PgStatShared_Backend;
    let bktype_shstats: *mut PgStat_BktypeIO;
    let pending_io: PgStat_PendingIO;

    // This function can be called even if nothing at all has happened for IO
    // statistics.  In this case, avoid unnecessarily modifying the stats entry.
    if !backend_has_iostats {
        return;
    }

    shbackendent = (*entry_ref).shared_stats as *mut PgStatShared_Backend;
    bktype_shstats = &mut (*shbackendent).stats.io_stats;
    pending_io = PendingBackendStats.pending_io;

    for io_object in 0..IOOBJECT_NUM_TYPES {
        for io_context in 0..IOCONTEXT_NUM_TYPES {
            for io_op in 0..IOOP_NUM_TYPES {
                let time: instr_time;

                (*bktype_shstats).counts[io_object][io_context][io_op] +=
                    pending_io.counts[io_object][io_context][io_op];
                (*bktype_shstats).bytes[io_object][io_context][io_op] +=
                    pending_io.bytes[io_object][io_context][io_op];
                time = pending_io.pending_times[io_object][io_context][io_op];

                (*bktype_shstats).times[io_object][io_context][io_op] +=
                    INSTR_TIME_GET_MICROSEC(time);
            }
        }
    }

    // Clear out the statistics buffer, so it can be re-used.
    PendingBackendStats.pending_io = PgStat_PendingIO::zeroed();

    backend_has_iostats = false;
}

// To determine whether WAL usage happened.
#[inline]
unsafe fn pgstat_backend_wal_have_pending() -> bool {
    pgWalUsage.wal_records != prevBackendWalUsage.wal_records
}

// Flush out locally pending backend WAL statistics.  Locking is managed by the
// caller.
unsafe fn pgstat_flush_backend_entry_wal(entry_ref: *mut PgStat_EntryRef) {
    let shbackendent: *mut PgStatShared_Backend;
    let bktype_shstats: *mut PgStat_WalCounters;
    let mut wal_usage_diff: WalUsage = WalUsage {
        wal_records: 0,
        wal_fpi: 0,
        wal_bytes: 0,
        wal_buffers_full: 0,
    };

    // This function can be called even if nothing at all has happened for WAL
    // statistics.  In this case, avoid unnecessarily modifying the stats entry.
    if !pgstat_backend_wal_have_pending() {
        return;
    }

    shbackendent = (*entry_ref).shared_stats as *mut PgStatShared_Backend;
    bktype_shstats = &mut (*shbackendent).stats.wal_counters;

    // Calculate how much WAL usage counters were increased by subtracting the
    // previous counters from the current ones.
    WalUsageAccumDiff(&mut wal_usage_diff, &pgWalUsage, &prevBackendWalUsage);

    // WALSTAT_ACC(fld, var_to_add): bktype_shstats->fld += var_to_add.fld
    (*bktype_shstats).wal_buffers_full += wal_usage_diff.wal_buffers_full;
    (*bktype_shstats).wal_records += wal_usage_diff.wal_records;
    (*bktype_shstats).wal_fpi += wal_usage_diff.wal_fpi;
    (*bktype_shstats).wal_bytes += wal_usage_diff.wal_bytes;

    // Save the current counters for the subsequent calculation of WAL usage.
    prevBackendWalUsage = pgWalUsage;
}

// Flush out locally pending backend statistics
//
// "flags" parameter controls which statistics to flush.  Returns true if some
// statistics could not be flushed due to lock contention.
pub unsafe fn pgstat_flush_backend(nowait: bool, flags: bits32) -> bool {
    let entry_ref: *mut PgStat_EntryRef;
    let mut has_pending_data: bool = false;

    if !pgstat_tracks_backend_bktype(MyBackendType) {
        return false;
    }

    // Some IO data pending?
    if (flags & PGSTAT_BACKEND_FLUSH_IO) != 0 && backend_has_iostats {
        has_pending_data = true;
    }

    // Some WAL data pending?
    if (flags & PGSTAT_BACKEND_FLUSH_WAL) != 0 && pgstat_backend_wal_have_pending() {
        has_pending_data = true;
    }

    if !has_pending_data {
        return false;
    }

    entry_ref = pgstat_get_entry_ref_locked(
        PGSTAT_KIND_BACKEND,
        InvalidOid,
        MyProcNumber as u64,
        nowait,
    );
    if entry_ref.is_null() {
        return true;
    }

    // Flush requested statistics
    if (flags & PGSTAT_BACKEND_FLUSH_IO) != 0 {
        pgstat_flush_backend_entry_io(entry_ref);
    }

    if (flags & PGSTAT_BACKEND_FLUSH_WAL) != 0 {
        pgstat_flush_backend_entry_wal(entry_ref);
    }

    pgstat_unlock_entry(entry_ref);

    false
}

// Callback to flush out locally pending backend statistics.
//
// If some stats could not be flushed due to lock contention, return true.
pub unsafe fn pgstat_backend_flush_cb(nowait: bool) -> bool {
    pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_ALL)
}

// Create backend statistics entry for proc number.
pub unsafe fn pgstat_create_backend(procnum: ProcNumber) {
    let entry_ref: *mut PgStat_EntryRef;
    let shstatent: *mut PgStatShared_Backend;

    entry_ref = pgstat_get_entry_ref_locked(
        PGSTAT_KIND_BACKEND,
        InvalidOid,
        procnum as u64,
        false,
    );
    shstatent = (*entry_ref).shared_stats as *mut PgStatShared_Backend;

    // NB: need to accept that there might be stats from an older backend, e.g.
    // if we previously used this proc number.
    (*shstatent).stats = PgStat_Backend::zeroed();
    pgstat_unlock_entry(entry_ref);

    PendingBackendStats = PgStat_BackendPending::zeroed();
    backend_has_iostats = false;

    // Initialize prevBackendWalUsage with pgWalUsage so that
    // pgstat_backend_flush_cb() can calculate how much pgWalUsage counters are
    // increased by subtracting prevBackendWalUsage from pgWalUsage.
    prevBackendWalUsage = pgWalUsage;
}

// Backend statistics are not collected for all BackendTypes.
//
// The following BackendTypes do not participate in the backend stats subsystem:
// - The same and for the same reasons as in pgstat_tracks_io_bktype().
// - B_BG_WRITER, B_CHECKPOINTER, B_STARTUP and B_AUTOVAC_LAUNCHER because their
//   I/O stats are already visible in pg_stat_io and there is only one of those.
//
// Function returns true if BackendType participates in the backend stats
// subsystem and false if it does not.
//
// When adding a new BackendType, also consider adding relevant restrictions to
// pgstat_tracks_io_object() and pgstat_tracks_io_op().
pub fn pgstat_tracks_backend_bktype(bktype: BackendType) -> bool {
    // List every type so that new backend types trigger a warning about needing
    // to adjust this switch.
    match bktype {
        B_INVALID | B_AUTOVAC_LAUNCHER | B_DEAD_END_BACKEND | B_ARCHIVER | B_LOGGER | B_BG_WRITER
        | B_CHECKPOINTER | B_IO_WORKER | B_STARTUP => false,

        B_AUTOVAC_WORKER | B_BACKEND | B_BG_WORKER | B_STANDALONE_BACKEND | B_SLOTSYNC_WORKER
        | B_WAL_RECEIVER | B_WAL_SENDER | B_WAL_SUMMARIZER | B_WAL_WRITER => true,

        _ => false,
    }
}

pub unsafe fn pgstat_backend_reset_timestamp_cb(header: *mut PgStatShared_Common, ts: TimestampTz) {
    (*(header as *mut PgStatShared_Backend)).stats.stat_reset_timestamp = ts;
}
