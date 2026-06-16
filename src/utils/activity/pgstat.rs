//! Foundational SUBSET of the cumulative statistics subsystem.
//!
//! This is a hand-picked translation of the parts of `pgstat.c`, `pgstat.h`,
//! and `pgstat_internal.h` that the *fixed-amount* statistics reporters
//! (archiver / bgwriter / checkpointer / wal) depend on. It is intentionally
//! incomplete: only the machinery those four reporters need to publish and
//! snapshot their counters is present here.
//!
//! Deviations from upstream PostgreSQL 18.3 (each noted again inline):
//!
//! * SHARED MEMORY -> PROCESS-LOCAL. Upstream allocates `PgStat_ShmemControl`
//!   in DSA/shared memory via `pgstat_attach_shmem()`. The shmem subsystem is
//!   not ported yet, so this file backs the shared control block with a single
//!   process-local `static mut PGSTAT_SHMEM`. `pgstat_attach_shmem()` simply
//!   points `pgStatLocal.shmem` at that static. This is correct for a single
//!   process; it does NOT provide cross-process visibility.
//!
//! * LWLOCK / BARRIERS / CRIT-SECTION are NO-OPS. `storage/lwlock.h`, the
//!   memory barriers behind the changecount protocol, and the critical-section
//!   guards are all stubbed. The changecount read/write dance is translated
//!   faithfully in terms of structure, but the barriers it relies on for
//!   cross-CPU ordering are absent.
//!
//! * GetCurrentTimestamp() is a STUB returning 0 (`utils/timestamp.c` unported).
//!
//! * SNAPSHOT reset_offset is SIMPLIFIED. `pgstat_snapshot_fixed()` here copies
//!   the live counters straight out of the shared block without subtracting the
//!   per-kind `reset_offset`. The reporters' own `*_snapshot_cb` callbacks (when
//!   ported) perform the full reset-offset-aware version.

use crate::prelude::*;

// Relation/RelationData (utils/rel.h) re-exported via nodes::execnodes. Used by
// PgStat_TableStatus.relation, mirroring the upstream `Relation` field.
use crate::nodes::execnodes::Relation;

// We deliberately avoid the `libc` crate; pull in `memcpy` via a local extern.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// Fundamental typedefs (pgstat.h)
// ---------------------------------------------------------------------------

/// The type used for all cumulative statistics counters.
pub type PgStat_Counter = int64;

/// Timestamp type (normally from `datatype/timestamp.h`). Mirrored here as the
/// underlying int64 microsecond representation.
pub type TimestampTz = int64;

/// Identifies a "kind" of statistics (a particular stats object family).
pub type PgStat_Kind = u32;

// Fixed-amount stats kinds (pgstat.h: PGSTAT_KIND_*). Only the four
// fixed-amount kinds handled by this subset are defined.
pub const PGSTAT_KIND_ARCHIVER: PgStat_Kind = 7;
pub const PGSTAT_KIND_BGWRITER: PgStat_Kind = 8;
pub const PGSTAT_KIND_CHECKPOINTER: PgStat_Kind = 9;
pub const PGSTAT_KIND_IO: PgStat_Kind = 10;
pub const PGSTAT_KIND_SLRU: PgStat_Kind = 11;
pub const PGSTAT_KIND_WAL: PgStat_Kind = 12;

// IO statistics dimensions (pgstat.h). The cumulative IO stats are a 3-D grid
// of [IOObject][IOContext][IOOp] per BackendType.
pub const IOOBJECT_NUM_TYPES: usize = 3;
pub const IOCONTEXT_NUM_TYPES: usize = 5;
pub const IOOP_NUM_TYPES: usize = 8;
pub const BACKEND_NUM_TYPES: usize = 18;

/// Number of SLRU statistics entries. Equals lengthof(slru_names) in upstream:
/// commit_timestamp / multixact_member / multixact_offset / notify /
/// serializable / subtransaction / transaction / other.
pub const SLRU_NUM_ELEMENTS: usize = 8;

/// Maximum length of an xlog file name (xlog_internal.h: MAXFNAMELEN - 1).
pub const MAX_XFN_CHARS: usize = 40;

// ---------------------------------------------------------------------------
// LWLock STUB (storage/lwlock.h unported)
// ---------------------------------------------------------------------------
//
// The real LWLock is a shared-memory lightweight lock. Here it is an inert
// placeholder so the shared-wrapper structs keep their field layout, and the
// acquire/release/initialize entry points are no-ops.

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LWLock {
    _stub: u32,
}

pub const LWTRANCHE_PGSTATS_DATA: c_int = 0;
pub const LW_EXCLUSIVE: c_int = 0;

#[inline]
pub unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {}

#[inline]
pub unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    true
}

#[inline]
pub unsafe fn LWLockRelease(_lock: *mut LWLock) {}

// ---------------------------------------------------------------------------
// Timestamp STUB (utils/timestamp.c unported)
// ---------------------------------------------------------------------------

/// STUB: real implementation lives in `utils/adt/timestamp.c` (unported).
pub unsafe fn GetCurrentTimestamp() -> TimestampTz {
    0
}

// ---------------------------------------------------------------------------
// Fixed-kind statistics structs (pgstat.h) -- EXACT field sets.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_ArchiverStats {
    pub archived_count: PgStat_Counter,
    pub last_archived_wal: [c_char; MAX_XFN_CHARS + 1],
    pub last_archived_timestamp: TimestampTz,
    pub failed_count: PgStat_Counter,
    pub last_failed_wal: [c_char; MAX_XFN_CHARS + 1],
    pub last_failed_timestamp: TimestampTz,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_ArchiverStats {
    pub const fn zeroed() -> Self {
        PgStat_ArchiverStats {
            archived_count: 0,
            last_archived_wal: [0; MAX_XFN_CHARS + 1],
            last_archived_timestamp: 0,
            failed_count: 0,
            last_failed_wal: [0; MAX_XFN_CHARS + 1],
            last_failed_timestamp: 0,
            stat_reset_timestamp: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_BgWriterStats {
    pub buf_written_clean: PgStat_Counter,
    pub maxwritten_clean: PgStat_Counter,
    pub buf_alloc: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_BgWriterStats {
    pub const fn zeroed() -> Self {
        PgStat_BgWriterStats {
            buf_written_clean: 0,
            maxwritten_clean: 0,
            buf_alloc: 0,
            stat_reset_timestamp: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_CheckpointerStats {
    pub num_timed: PgStat_Counter,
    pub num_requested: PgStat_Counter,
    pub num_performed: PgStat_Counter,
    pub restartpoints_timed: PgStat_Counter,
    pub restartpoints_requested: PgStat_Counter,
    pub restartpoints_performed: PgStat_Counter,
    pub write_time: PgStat_Counter,
    pub sync_time: PgStat_Counter,
    pub buffers_written: PgStat_Counter,
    pub slru_written: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_CheckpointerStats {
    pub const fn zeroed() -> Self {
        PgStat_CheckpointerStats {
            num_timed: 0,
            num_requested: 0,
            num_performed: 0,
            restartpoints_timed: 0,
            restartpoints_requested: 0,
            restartpoints_performed: 0,
            write_time: 0,
            sync_time: 0,
            buffers_written: 0,
            slru_written: 0,
            stat_reset_timestamp: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_WalCounters {
    pub wal_records: PgStat_Counter,
    pub wal_fpi: PgStat_Counter,
    pub wal_bytes: uint64,
    pub wal_buffers_full: PgStat_Counter,
}

impl PgStat_WalCounters {
    pub const fn zeroed() -> Self {
        PgStat_WalCounters {
            wal_records: 0,
            wal_fpi: 0,
            wal_bytes: 0,
            wal_buffers_full: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_WalStats {
    pub wal_counters: PgStat_WalCounters,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_WalStats {
    pub const fn zeroed() -> Self {
        PgStat_WalStats {
            wal_counters: PgStat_WalCounters::zeroed(),
            stat_reset_timestamp: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_SLRUStats {
    pub blocks_zeroed: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub blocks_read: PgStat_Counter,
    pub blocks_written: PgStat_Counter,
    pub blocks_exists: PgStat_Counter,
    pub flush: PgStat_Counter,
    pub truncate: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_SLRUStats {
    pub const fn zeroed() -> Self {
        PgStat_SLRUStats {
            blocks_zeroed: 0,
            blocks_hit: 0,
            blocks_read: 0,
            blocks_written: 0,
            blocks_exists: 0,
            flush: 0,
            truncate: 0,
            stat_reset_timestamp: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// IO statistics structs (pgstat.h)
// ---------------------------------------------------------------------------
//
// Per-BackendType IO counters. C indexes these as [IOOBJECT][IOCONTEXT][IOOP],
// so the Rust array nesting puts IOOBJECT_NUM_TYPES outermost and IOOP_NUM_TYPES
// innermost.

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_BktypeIO {
    pub bytes: [[[uint64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub times: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}

impl PgStat_BktypeIO {
    pub const fn zeroed() -> Self {
        PgStat_BktypeIO {
            bytes: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            counts: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            times: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_IO {
    pub stat_reset_timestamp: TimestampTz,
    pub stats: [PgStat_BktypeIO; BACKEND_NUM_TYPES],
}

impl PgStat_IO {
    pub const fn zeroed() -> Self {
        PgStat_IO {
            stat_reset_timestamp: 0,
            stats: [PgStat_BktypeIO::zeroed(); BACKEND_NUM_TYPES],
        }
    }
}

// ---------------------------------------------------------------------------
// Shared wrappers (pgstat_internal.h: PgStatShared_*)
// ---------------------------------------------------------------------------
//
// Each fixed-amount stat lives in shared memory behind an LWLock plus a
// `changecount` seqlock-style counter, with a `reset_offset` snapshot used to
// implement `pg_stat_reset_shared()` without zeroing the live counters.

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Archiver {
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_ArchiverStats,
    pub reset_offset: PgStat_ArchiverStats,
}

impl PgStatShared_Archiver {
    pub const fn zeroed() -> Self {
        PgStatShared_Archiver {
            lock: LWLock { _stub: 0 },
            changecount: 0,
            stats: PgStat_ArchiverStats::zeroed(),
            reset_offset: PgStat_ArchiverStats::zeroed(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_BgWriter {
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_BgWriterStats,
    pub reset_offset: PgStat_BgWriterStats,
}

impl PgStatShared_BgWriter {
    pub const fn zeroed() -> Self {
        PgStatShared_BgWriter {
            lock: LWLock { _stub: 0 },
            changecount: 0,
            stats: PgStat_BgWriterStats::zeroed(),
            reset_offset: PgStat_BgWriterStats::zeroed(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Checkpointer {
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_CheckpointerStats,
    pub reset_offset: PgStat_CheckpointerStats,
}

impl PgStatShared_Checkpointer {
    pub const fn zeroed() -> Self {
        PgStatShared_Checkpointer {
            lock: LWLock { _stub: 0 },
            changecount: 0,
            stats: PgStat_CheckpointerStats::zeroed(),
            reset_offset: PgStat_CheckpointerStats::zeroed(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Wal {
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_WalStats,
    pub reset_offset: PgStat_WalStats,
}

impl PgStatShared_Wal {
    pub const fn zeroed() -> Self {
        PgStatShared_Wal {
            lock: LWLock { _stub: 0 },
            changecount: 0,
            stats: PgStat_WalStats::zeroed(),
            reset_offset: PgStat_WalStats::zeroed(),
        }
    }
}

// IO uses the seqlock-style changecount protocol (like archiver), NOT the
// lock-only scheme SLRU uses. DEVIATION: upstream guards each per-BackendType
// row with its own LWLock in a `locks[BACKEND_NUM_TYPES]` array; this subset
// uses a single changecount guard over the whole `PgStat_IO` block.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_IO {
    pub lock: LWLock,
    pub changecount: uint32,
    pub stats: PgStat_IO,
}

impl PgStatShared_IO {
    pub const fn zeroed() -> Self {
        PgStatShared_IO {
            lock: LWLock { _stub: 0 },
            changecount: 0,
            stats: PgStat_IO::zeroed(),
        }
    }
}

// SLRU differs from the other fixed-amount wrappers: it has NO changecount and
// NO reset_offset. The snapshot is guarded by the LWLock directly rather than by
// the seqlock-style changecount protocol.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_SLRU {
    pub lock: LWLock,
    pub stats: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
}

impl PgStatShared_SLRU {
    pub const fn zeroed() -> Self {
        PgStatShared_SLRU {
            lock: LWLock { _stub: 0 },
            stats: [PgStat_SLRUStats::zeroed(); SLRU_NUM_ELEMENTS],
        }
    }
}

// ---------------------------------------------------------------------------
// Shared control block + local state (pgstat_internal.h)
// ---------------------------------------------------------------------------
//
// Upstream `PgStat_ShmemControl` carries the dshash table and every fixed-kind
// shared wrapper. This subset keeps only the four fixed wrappers needed here.

#[repr(C)]
pub struct PgStat_ShmemControl {
    pub archiver: PgStatShared_Archiver,
    pub bgwriter: PgStatShared_BgWriter,
    pub checkpointer: PgStatShared_Checkpointer,
    pub io: PgStatShared_IO,
    pub slru: PgStatShared_SLRU,
    pub wal: PgStatShared_Wal,
}

/// The local snapshot of fixed-amount statistics (pgstat_internal.h).
#[repr(C)]
pub struct PgStat_Snapshot {
    pub archiver: PgStat_ArchiverStats,
    pub bgwriter: PgStat_BgWriterStats,
    pub checkpointer: PgStat_CheckpointerStats,
    pub io: PgStat_IO,
    pub slru: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
    pub wal: PgStat_WalStats,
}

/// Process-local pgstat state (pgstat_internal.h: PgStat_LocalState).
#[repr(C)]
pub struct PgStat_LocalState {
    pub shmem: *mut PgStat_ShmemControl,
    pub snapshot: PgStat_Snapshot,
}

// ---------------------------------------------------------------------------
// PROCESS-LOCAL backing store (DEVIATION: stands in for shared memory)
// ---------------------------------------------------------------------------
//
// In upstream this block is carved out of DSA shared memory during startup.
// Until the shmem subsystem is ported we back it with a single const-initialized
// process-local static. Cross-process visibility is therefore NOT provided.

static mut PGSTAT_SHMEM: PgStat_ShmemControl = PgStat_ShmemControl {
    archiver: PgStatShared_Archiver::zeroed(),
    bgwriter: PgStatShared_BgWriter::zeroed(),
    checkpointer: PgStatShared_Checkpointer::zeroed(),
    io: PgStatShared_IO::zeroed(),
    slru: PgStatShared_SLRU::zeroed(),
    wal: PgStatShared_Wal::zeroed(),
};

/// Process-local pgstat state. Mirrors upstream's `pgStatLocal`.
pub static mut pgStatLocal: PgStat_LocalState = PgStat_LocalState {
    shmem: null_mut(),
    snapshot: PgStat_Snapshot {
        archiver: PgStat_ArchiverStats::zeroed(),
        bgwriter: PgStat_BgWriterStats::zeroed(),
        checkpointer: PgStat_CheckpointerStats::zeroed(),
        io: PgStat_IO::zeroed(),
        slru: [PgStat_SLRUStats::zeroed(); SLRU_NUM_ELEMENTS],
        wal: PgStat_WalStats::zeroed(),
    },
};

/// Wire `pgStatLocal.shmem` to the (process-local) shared control block.
///
/// DEVIATION: upstream attaches to the real DSM/shmem segment here.
pub unsafe fn pgstat_attach_shmem() {
    pgStatLocal.shmem = &raw mut PGSTAT_SHMEM;
}

/// Lazily ensure `pgStatLocal.shmem` is attached, returning it.
pub unsafe fn pgstat_shmem() -> *mut PgStat_ShmemControl {
    if pgStatLocal.shmem.is_null() {
        pgstat_attach_shmem();
    }
    pgStatLocal.shmem
}

// ---------------------------------------------------------------------------
// changecount protocol (pgstat_internal.h inline functions)
// ---------------------------------------------------------------------------
//
// A seqlock-style scheme: writers bump `changecount` to an odd value before
// mutating and back to even afterwards; readers retry until they see a stable
// even count that did not change across the copy. The memory barriers that make
// this safe across CPUs are NO-OPS in this subset.

/// Begin a changecounted write. The count becomes odd. (Assert: was even.)
pub unsafe fn pgstat_begin_changecount_write(cc: *mut uint32) {
    // Assert((*cc & 1) == 0);  -- barrier omitted (no-op here)
    *cc = (*cc).wrapping_add(1);
}

/// End a changecounted write. The count returns to even.
pub unsafe fn pgstat_end_changecount_write(cc: *mut uint32) {
    // barrier omitted (no-op here)
    *cc = (*cc).wrapping_add(1);
}

/// Begin a changecounted read; capture the current count.
unsafe fn pgstat_begin_changecount_read(cc: *mut uint32) -> uint32 {
    *cc
}

/// End a changecounted read. Returns true if the snapshot was consistent
/// (count even and unchanged across the read).
unsafe fn pgstat_end_changecount_read(cc: *mut uint32, before: uint32) -> bool {
    let after = *cc;
    if before & 1 != 0 {
        return false;
    }
    before == after
}

/// Copy `len` bytes from a changecounted source into `dst`, retrying until a
/// consistent snapshot is obtained.
pub unsafe fn pgstat_copy_changecounted_stats(
    dst: *mut c_void,
    src: *mut c_void,
    len: Size,
    cc: *mut uint32,
) {
    loop {
        let before = pgstat_begin_changecount_read(cc);
        memcpy(dst, src, len);
        if pgstat_end_changecount_read(cc, before) {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Fixed-kind snapshotting (pgstat.c: pgstat_snapshot_fixed, SIMPLIFIED)
// ---------------------------------------------------------------------------

/// Copy the live shared counters for `kind` into the process-local snapshot.
///
/// SIMPLIFICATION: upstream dispatches through a per-kind KindInfo callback
/// table honoring consistency modes and subtracting `reset_offset`. Here each
/// kind is inlined as a single changecounted memcpy of the live `stats` block;
/// the `reset_offset` subtraction is omitted (the reporters' own snapshot
/// callbacks apply the full version once ported).
pub unsafe fn pgstat_snapshot_fixed(kind: PgStat_Kind) {
    let ctl = pgstat_shmem();
    match kind {
        PGSTAT_KIND_ARCHIVER => pgstat_copy_changecounted_stats(
            &mut pgStatLocal.snapshot.archiver as *mut _ as *mut c_void,
            &mut (*ctl).archiver.stats as *mut _ as *mut c_void,
            size_of::<PgStat_ArchiverStats>(),
            &mut (*ctl).archiver.changecount,
        ),
        PGSTAT_KIND_BGWRITER => pgstat_copy_changecounted_stats(
            &mut pgStatLocal.snapshot.bgwriter as *mut _ as *mut c_void,
            &mut (*ctl).bgwriter.stats as *mut _ as *mut c_void,
            size_of::<PgStat_BgWriterStats>(),
            &mut (*ctl).bgwriter.changecount,
        ),
        PGSTAT_KIND_CHECKPOINTER => pgstat_copy_changecounted_stats(
            &mut pgStatLocal.snapshot.checkpointer as *mut _ as *mut c_void,
            &mut (*ctl).checkpointer.stats as *mut _ as *mut c_void,
            size_of::<PgStat_CheckpointerStats>(),
            &mut (*ctl).checkpointer.changecount,
        ),
        PGSTAT_KIND_IO => pgstat_copy_changecounted_stats(
            &mut pgStatLocal.snapshot.io as *mut _ as *mut c_void,
            &mut (*ctl).io.stats as *mut _ as *mut c_void,
            size_of::<PgStat_IO>(),
            &mut (*ctl).io.changecount,
        ),
        PGSTAT_KIND_WAL => pgstat_copy_changecounted_stats(
            &mut pgStatLocal.snapshot.wal as *mut _ as *mut c_void,
            &mut (*ctl).wal.stats as *mut _ as *mut c_void,
            size_of::<PgStat_WalStats>(),
            &mut (*ctl).wal.changecount,
        ),
        // SLRU is guarded by its LWLock directly (no changecount/reset_offset):
        // acquire the lock, memcpy the whole stats array into the local
        // snapshot, then release.
        PGSTAT_KIND_SLRU => {
            LWLockAcquire(&mut (*ctl).slru.lock, LW_EXCLUSIVE);
            memcpy(
                &mut pgStatLocal.snapshot.slru as *mut _ as *mut c_void,
                &mut (*ctl).slru.stats as *mut _ as *mut c_void,
                size_of::<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>(),
            );
            LWLockRelease(&mut (*ctl).slru.lock);
        }
        _ => {}
    }
}

// ===========================================================================
// VARIABLE-KIND entry-ref machinery (process-local stand-in for upstream
// dshash). DEVIATION: upstream stores variable-amount stats (database, table,
// function, replication slot, subscription, ...) in a dshash table living in
// DSA shared memory, with per-entry generations and a dlist of pending nodes.
// Until the shmem/dshash subsystem is ported we keep a flat process-local Vec
// of entries keyed by (kind, dboid, objoid). No cross-process visibility.
// ===========================================================================

/// Variable-amount stats kind for subscriptions (pgstat.h: PGSTAT_KIND_SUBSCRIPTION).
pub const PGSTAT_KIND_SUBSCRIPTION: PgStat_Kind = 5;

/// Variable-amount stats kind for functions (pgstat.h: PGSTAT_KIND_FUNCTION).
pub const PGSTAT_KIND_FUNCTION: PgStat_Kind = 3;

/// Variable-amount stats kind for replication slots (pgstat.h: PGSTAT_KIND_REPLSLOT).
pub const PGSTAT_KIND_REPLSLOT: PgStat_Kind = 4;

/// Number of logical-replication conflict types (pgstat.h: CONFLICT_NUM_TYPES).
pub const CONFLICT_NUM_TYPES: usize = 7;

/// Common header prefixing every shared variable-kind entry
/// (pgstat_internal.h: PgStatShared_Common). The real header also carries a
/// `dropped` flag and a `refcount`; this subset keeps only `magic` + `lock`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Common {
    pub magic: uint32,
    pub lock: LWLock,
}

impl PgStatShared_Common {
    pub const fn zeroed() -> Self {
        PgStatShared_Common {
            magic: 0,
            lock: LWLock { _stub: 0 },
        }
    }
}

/// Reference to one variable-kind stats entry (pgstat_internal.h: PgStat_EntryRef).
///
/// DEVIATION (SIMPLIFIED): upstream also carries `shared_entry`
/// (PgStatShared_HashEntry*), a `generation` counter, and a `pending_node`
/// dlist link. Those tie the entry into the shared dshash + per-process pending
/// list, neither of which is ported. We keep only the two payload pointers.
#[repr(C)]
pub struct PgStat_EntryRef {
    pub shared_stats: *mut c_void,
    pub pending: *mut c_void,
}

impl PgStat_EntryRef {
    const fn zeroed() -> Self {
        PgStat_EntryRef {
            shared_stats: null_mut(),
            pending: null_mut(),
        }
    }
}

// --- Subscription stat structs (pgstat.h) ---------------------------------

/// Shared, accumulated subscription statistics (pgstat.h: PgStat_StatSubEntry).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_StatSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_StatSubEntry {
    pub const fn zeroed() -> Self {
        PgStat_StatSubEntry {
            apply_error_count: 0,
            sync_error_count: 0,
            conflict_count: [0; CONFLICT_NUM_TYPES],
            stat_reset_timestamp: 0,
        }
    }
}

/// Per-backend pending subscription statistics (pgstat.h: PgStat_BackendSubEntry).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_BackendSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
}

impl PgStat_BackendSubEntry {
    pub const fn zeroed() -> Self {
        PgStat_BackendSubEntry {
            apply_error_count: 0,
            sync_error_count: 0,
            conflict_count: [0; CONFLICT_NUM_TYPES],
        }
    }
}

/// Shared wrapper for subscription stats (pgstat_internal.h: PgStatShared_Subscription).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Subscription {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatSubEntry,
}

impl PgStatShared_Subscription {
    pub const fn zeroed() -> Self {
        PgStatShared_Subscription {
            header: PgStatShared_Common::zeroed(),
            stats: PgStat_StatSubEntry::zeroed(),
        }
    }
}

// --- Function stat structs (pgstat.h) -------------------------------------

/// Shared, accumulated per-function statistics (pgstat.h: PgStat_StatFuncEntry).
/// All three counters are stored in microseconds for the time fields.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_StatFuncEntry {
    pub numcalls: PgStat_Counter,
    pub total_time: PgStat_Counter,
    pub self_time: PgStat_Counter,
}

impl PgStat_StatFuncEntry {
    pub const fn zeroed() -> Self {
        PgStat_StatFuncEntry {
            numcalls: 0,
            total_time: 0,
            self_time: 0,
        }
    }
}

/// Per-backend pending per-function statistics (pgstat.h: PgStat_FunctionCounts).
///
/// DEVIATION: upstream declares `total_time`/`self_time` as `instr_time`. Here we
/// store the accumulated time as plain `int64` microseconds; `pgstat_function.rs`
/// converts at the point the C does `INSTR_TIME_*`. The flush callback can then
/// add these directly into the shared (already-microseconds) counters.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_FunctionCounts {
    pub numcalls: PgStat_Counter,
    pub total_time: int64, /* instr_time microsecs */
    pub self_time: int64,
}

impl PgStat_FunctionCounts {
    pub const fn zeroed() -> Self {
        PgStat_FunctionCounts {
            numcalls: 0,
            total_time: 0,
            self_time: 0,
        }
    }
}

/// Shared wrapper for function stats (pgstat_internal.h: PgStatShared_Function).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Function {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatFuncEntry,
}

impl PgStatShared_Function {
    pub const fn zeroed() -> Self {
        PgStatShared_Function {
            header: PgStatShared_Common::zeroed(),
            stats: PgStat_StatFuncEntry::zeroed(),
        }
    }
}

// --- Replication slot stat structs (pgstat.h) -----------------------------

/// Shared, accumulated per-replication-slot statistics
/// (pgstat.h: PgStat_StatReplSlotEntry). Unlike most variable-kind stats these
/// are reported WHOLESALE into the shared entry (no per-backend pending
/// accumulator): each report copies the eight counters straight in.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_StatReplSlotEntry {
    pub spill_txns: PgStat_Counter,
    pub spill_count: PgStat_Counter,
    pub spill_bytes: PgStat_Counter,
    pub stream_txns: PgStat_Counter,
    pub stream_count: PgStat_Counter,
    pub stream_bytes: PgStat_Counter,
    pub total_txns: PgStat_Counter,
    pub total_bytes: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_StatReplSlotEntry {
    pub const fn zeroed() -> Self {
        PgStat_StatReplSlotEntry {
            spill_txns: 0,
            spill_count: 0,
            spill_bytes: 0,
            stream_txns: 0,
            stream_count: 0,
            stream_bytes: 0,
            total_txns: 0,
            total_bytes: 0,
            stat_reset_timestamp: 0,
        }
    }
}

/// Shared wrapper for replication slot stats (pgstat_internal.h:
/// PgStatShared_ReplSlot).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_ReplSlot {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatReplSlotEntry,
}

impl PgStatShared_ReplSlot {
    pub const fn zeroed() -> Self {
        PgStatShared_ReplSlot {
            header: PgStatShared_Common::zeroed(),
            stats: PgStat_StatReplSlotEntry::zeroed(),
        }
    }
}

// --- Database stat structs (pgstat.h) -------------------------------------

/// Variable-amount stats kind for databases (pgstat.h: PGSTAT_KIND_DATABASE).
pub const PGSTAT_KIND_DATABASE: PgStat_Kind = 1;

/// Variable-amount stats kind for relations (pgstat.h: PGSTAT_KIND_RELATION).
pub const PGSTAT_KIND_RELATION: PgStat_Kind = 2;

// --- Relation (table) stat structs (pgstat.h) -----------------------------

/// The actual per-table counts kept by a backend (pgstat.h: PgStat_TableCounts).
///
/// Field order is transcribed EXACTLY from PG 18.3. `truncdropped` is a `bool`
/// embedded between the tuple-action counters and the delta counters.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_TableCounts {
    pub numscans: PgStat_Counter,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,
    pub truncdropped: bool,
    pub delta_live_tuples: PgStat_Counter,
    pub delta_dead_tuples: PgStat_Counter,
    pub changed_tuples: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
}

impl PgStat_TableCounts {
    pub const fn zeroed() -> Self {
        PgStat_TableCounts {
            numscans: 0,
            tuples_returned: 0,
            tuples_fetched: 0,
            tuples_inserted: 0,
            tuples_updated: 0,
            tuples_deleted: 0,
            tuples_hot_updated: 0,
            tuples_newpage_updated: 0,
            truncdropped: false,
            delta_live_tuples: 0,
            delta_dead_tuples: 0,
            changed_tuples: 0,
            blocks_fetched: 0,
            blocks_hit: 0,
        }
    }
}

/// Per-table status within a backend (pgstat.h: PgStat_TableStatus).
///
/// DEVIATION (STUBBED): `trans` is upstream a `*mut PgStat_TableXactStatus`
/// pointing at the lowest subxact's transactional counts. The subxact-linked-
/// list machinery (xact.h nesting) is NOT ported, so `trans` is kept as a raw
/// `*mut c_void` that `pgstat_relation.rs` always leaves null; the transactional
/// counters it would track are written directly into `counts` instead.
#[repr(C)]
pub struct PgStat_TableStatus {
    pub id: Oid,
    pub shared: bool,
    pub trans: *mut c_void, /* PgStat_TableXactStatus*, STUBBED (always null) */
    pub counts: PgStat_TableCounts,
    pub relation: Relation,
}

impl PgStat_TableStatus {
    pub const fn zeroed() -> Self {
        PgStat_TableStatus {
            id: InvalidOid,
            shared: false,
            trans: null_mut(),
            counts: PgStat_TableCounts::zeroed(),
            relation: null_mut(),
        }
    }
}

/// Shared, accumulated per-table statistics (pgstat.h: PgStat_StatTabEntry).
///
/// 27 fields transcribed EXACTLY from PG 18.3 header order. The four `*_time`
/// fields named `last_*` and `total_*` paired with the timestamp slots are
/// `TimestampTz`; the `lastscan` field is `TimestampTz`; the `last_vacuum_time`
/// / `last_autovacuum_time` / `last_analyze_time` / `last_autoanalyze_time` are
/// `TimestampTz`. The remaining counter fields are `PgStat_Counter`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_StatTabEntry {
    pub numscans: PgStat_Counter,
    pub lastscan: TimestampTz,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,
    pub live_tuples: PgStat_Counter,
    pub dead_tuples: PgStat_Counter,
    pub mod_since_analyze: PgStat_Counter,
    pub ins_since_vacuum: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub last_vacuum_time: TimestampTz,
    pub vacuum_count: PgStat_Counter,
    pub last_autovacuum_time: TimestampTz,
    pub autovacuum_count: PgStat_Counter,
    pub last_analyze_time: TimestampTz,
    pub analyze_count: PgStat_Counter,
    pub last_autoanalyze_time: TimestampTz,
    pub autoanalyze_count: PgStat_Counter,
    pub total_vacuum_time: PgStat_Counter, /* times in milliseconds */
    pub total_autovacuum_time: PgStat_Counter,
    pub total_analyze_time: PgStat_Counter,
    pub total_autoanalyze_time: PgStat_Counter,
}

impl PgStat_StatTabEntry {
    pub const fn zeroed() -> Self {
        PgStat_StatTabEntry {
            numscans: 0,
            lastscan: 0,
            tuples_returned: 0,
            tuples_fetched: 0,
            tuples_inserted: 0,
            tuples_updated: 0,
            tuples_deleted: 0,
            tuples_hot_updated: 0,
            tuples_newpage_updated: 0,
            live_tuples: 0,
            dead_tuples: 0,
            mod_since_analyze: 0,
            ins_since_vacuum: 0,
            blocks_fetched: 0,
            blocks_hit: 0,
            last_vacuum_time: 0,
            vacuum_count: 0,
            last_autovacuum_time: 0,
            autovacuum_count: 0,
            last_analyze_time: 0,
            analyze_count: 0,
            last_autoanalyze_time: 0,
            autoanalyze_count: 0,
            total_vacuum_time: 0,
            total_autovacuum_time: 0,
            total_analyze_time: 0,
            total_autoanalyze_time: 0,
        }
    }
}

/// Shared wrapper for relation stats (pgstat_internal.h: PgStatShared_Relation).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Relation {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatTabEntry,
}

impl PgStatShared_Relation {
    pub const fn zeroed() -> Self {
        PgStatShared_Relation {
            header: PgStatShared_Common::zeroed(),
            stats: PgStat_StatTabEntry::zeroed(),
        }
    }
}

/// Shared, accumulated per-database statistics (pgstat.h: PgStat_StatDBEntry).
///
/// Flat struct of 33 fields transcribed EXACTLY from the PG 18.3 header order.
/// In PG 18.3 every counter field (including the *_time fields blk_read_time /
/// blk_write_time / session_time / active_time / idle_in_transaction_time) is a
/// `PgStat_Counter` (int64) in microseconds -- none are `double`. The three
/// timestamp fields (last_autovac_time, last_checksum_failure,
/// stat_reset_timestamp) are `TimestampTz` (int64).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_StatDBEntry {
    pub xact_commit: PgStat_Counter,
    pub xact_rollback: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub last_autovac_time: TimestampTz,
    pub conflict_tablespace: PgStat_Counter,
    pub conflict_lock: PgStat_Counter,
    pub conflict_snapshot: PgStat_Counter,
    pub conflict_logicalslot: PgStat_Counter,
    pub conflict_bufferpin: PgStat_Counter,
    pub conflict_startup_deadlock: PgStat_Counter,
    pub temp_files: PgStat_Counter,
    pub temp_bytes: PgStat_Counter,
    pub deadlocks: PgStat_Counter,
    pub checksum_failures: PgStat_Counter,
    pub last_checksum_failure: TimestampTz,
    pub blk_read_time: PgStat_Counter, /* times in microseconds */
    pub blk_write_time: PgStat_Counter,
    pub sessions: PgStat_Counter,
    pub session_time: PgStat_Counter,
    pub active_time: PgStat_Counter,
    pub idle_in_transaction_time: PgStat_Counter,
    pub sessions_abandoned: PgStat_Counter,
    pub sessions_fatal: PgStat_Counter,
    pub sessions_killed: PgStat_Counter,
    pub parallel_workers_to_launch: PgStat_Counter,
    pub parallel_workers_launched: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_StatDBEntry {
    pub const fn zeroed() -> Self {
        PgStat_StatDBEntry {
            xact_commit: 0,
            xact_rollback: 0,
            blocks_fetched: 0,
            blocks_hit: 0,
            tuples_returned: 0,
            tuples_fetched: 0,
            tuples_inserted: 0,
            tuples_updated: 0,
            tuples_deleted: 0,
            last_autovac_time: 0,
            conflict_tablespace: 0,
            conflict_lock: 0,
            conflict_snapshot: 0,
            conflict_logicalslot: 0,
            conflict_bufferpin: 0,
            conflict_startup_deadlock: 0,
            temp_files: 0,
            temp_bytes: 0,
            deadlocks: 0,
            checksum_failures: 0,
            last_checksum_failure: 0,
            blk_read_time: 0,
            blk_write_time: 0,
            sessions: 0,
            session_time: 0,
            active_time: 0,
            idle_in_transaction_time: 0,
            sessions_abandoned: 0,
            sessions_fatal: 0,
            sessions_killed: 0,
            parallel_workers_to_launch: 0,
            parallel_workers_launched: 0,
            stat_reset_timestamp: 0,
        }
    }
}

/// Shared wrapper for database stats (pgstat_internal.h: PgStatShared_Database).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStatShared_Database {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatDBEntry,
}

impl PgStatShared_Database {
    pub const fn zeroed() -> Self {
        PgStatShared_Database {
            header: PgStatShared_Common::zeroed(),
            stats: PgStat_StatDBEntry::zeroed(),
        }
    }
}

// --- Process-local entry table (replaces dshash) --------------------------

/// One variable-kind entry: its key, the raw shared blob, an optional pending
/// blob, and the `PgStat_EntryRef` whose pointers index into those blobs.
struct VarEntry {
    kind: PgStat_Kind,
    dboid: Oid,
    objoid: Oid,
    shared: Box<[u8]>,
    pending: Option<Box<[u8]>>,
    eref: PgStat_EntryRef,
}

/// The flat entry table. `Vec::new()` is not const, so we store an
/// `Option<Vec<..>>` initialized lazily by `var_entries()`.
static mut VAR_ENTRIES: Option<Vec<Box<VarEntry>>> = None;

/// Get-or-init the process-local entry table.
unsafe fn var_entries() -> &'static mut Vec<Box<VarEntry>> {
    let p = &raw mut VAR_ENTRIES;
    if (*p).is_none() {
        *p = Some(Vec::new());
    }
    (*p).as_mut().unwrap()
}

/// Per-kind blob-size registry. Returns (shared_size, pending_size) in bytes.
///
/// TODO: extend for the other variable kinds (database, table, function,
/// replslot, ...) as they are ported. Only subscription is wired today.
unsafe fn pgstat_kind_sizes(kind: PgStat_Kind) -> (usize, usize) {
    match kind {
        PGSTAT_KIND_SUBSCRIPTION => (
            size_of::<PgStatShared_Subscription>(),
            size_of::<PgStat_BackendSubEntry>(),
        ),
        PGSTAT_KIND_FUNCTION => (
            size_of::<PgStatShared_Function>(),
            size_of::<PgStat_FunctionCounts>(),
        ),
        PGSTAT_KIND_DATABASE => (
            size_of::<PgStatShared_Database>(),
            size_of::<PgStat_StatDBEntry>(),
        ),
        PGSTAT_KIND_RELATION => (
            size_of::<PgStatShared_Relation>(),
            size_of::<PgStat_TableStatus>(),
        ),
        // Replication slots report wholesale into the shared entry; there is no
        // per-backend pending accumulator, so pending_size is 0.
        PGSTAT_KIND_REPLSLOT => (size_of::<PgStatShared_ReplSlot>(), 0),
        _ => (0, 0),
    }
}

/// Find or (optionally) create the entry-ref for (kind, dboid, objoid).
///
/// On a hit, `*found` is set true and a pointer to the existing entry's
/// `PgStat_EntryRef` is returned. On a miss with `create`, a new zeroed entry
/// is pushed (shared blob zeroed, no pending blob yet), its eref's
/// `shared_stats` is pointed at the shared blob, `*found` is set false, and the
/// new eref is returned. On a miss without `create`, returns null.
pub unsafe fn pgstat_get_entry_ref(
    kind: PgStat_Kind,
    dboid: Oid,
    objoid: Oid,
    create: bool,
    found: *mut bool,
) -> *mut PgStat_EntryRef {
    let entries = var_entries();
    for e in entries.iter_mut() {
        if e.kind == kind && e.dboid == dboid && e.objoid == objoid {
            if !found.is_null() {
                *found = true;
            }
            return &mut e.eref as *mut PgStat_EntryRef;
        }
    }

    if !create {
        if !found.is_null() {
            *found = false;
        }
        return null_mut();
    }

    let (shared_size, _pending_size) = pgstat_kind_sizes(kind);
    let mut entry = Box::new(VarEntry {
        kind,
        dboid,
        objoid,
        shared: vec![0u8; shared_size].into_boxed_slice(),
        pending: None,
        eref: PgStat_EntryRef::zeroed(),
    });
    entry.eref.shared_stats = entry.shared.as_mut_ptr() as *mut c_void;
    entry.eref.pending = null_mut();
    entries.push(entry);

    if !found.is_null() {
        *found = false;
    }
    let last = entries.last_mut().unwrap();
    &mut last.eref as *mut PgStat_EntryRef
}

/// Get (creating if needed) the entry-ref and ensure a zeroed pending blob
/// exists, pointing `eref.pending` at it (pgstat.c: pgstat_prep_pending_entry).
pub unsafe fn pgstat_prep_pending_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objoid: Oid,
    created: *mut bool,
) -> *mut PgStat_EntryRef {
    // get_entry_ref creates the entry; reuse its `found` out-param for `created`
    // (created == !found).
    let mut found = false;
    let eref = pgstat_get_entry_ref(kind, dboid, objoid, true, &mut found);
    if !created.is_null() {
        *created = !found;
    }

    // Find the owning entry again to (lazily) allocate its pending blob. We
    // match on the eref pointer rather than the key to stay O(1)-ish robust.
    let entries = var_entries();
    let (_shared_size, pending_size) = pgstat_kind_sizes(kind);
    for e in entries.iter_mut() {
        if &mut e.eref as *mut PgStat_EntryRef == eref {
            if e.pending.is_none() {
                e.pending = Some(vec![0u8; pending_size].into_boxed_slice());
                e.eref.pending = e.pending.as_mut().unwrap().as_mut_ptr() as *mut c_void;
            }
            break;
        }
    }
    eref
}

/// Fetch the shared stats blob for (kind, dboid, objoid), or null.
///
/// SIMPLIFICATION: upstream copies the shared entry into a process-local
/// snapshot (honoring the snapshot consistency mode) and returns the snapshot
/// copy. Here we return the live `shared_stats` pointer directly -- no
/// snapshot copy is taken.
pub unsafe fn pgstat_fetch_entry(kind: PgStat_Kind, dboid: Oid, objoid: Oid) -> *mut c_void {
    let eref = pgstat_get_entry_ref(kind, dboid, objoid, false, null_mut());
    if eref.is_null() {
        return null_mut();
    }
    (*eref).shared_stats
}

/// Acquire the entry's lock (pgstat.c: pgstat_lock_entry). STUB: locking is a
/// no-op in this process-local subset, so always succeeds.
pub unsafe fn pgstat_lock_entry(_entry_ref: *mut PgStat_EntryRef, _nowait: bool) -> bool {
    true
}

/// Release the entry's lock (pgstat.c: pgstat_unlock_entry). STUB no-op.
pub unsafe fn pgstat_unlock_entry(_entry_ref: *mut PgStat_EntryRef) {}

/// Mark a stats entry as created within the current (sub)transaction so it is
/// dropped on rollback (pgstat.c: pgstat_create_transactional).
///
/// TODO: no transactional drop machinery is ported yet; no-op.
pub unsafe fn pgstat_create_transactional(_kind: PgStat_Kind, _dboid: Oid, _objoid: Oid) {}

/// Mark a stats entry to be dropped on commit (pgstat.c: pgstat_drop_transactional).
/// TODO: no transactional drop machinery is ported yet; no-op.
pub unsafe fn pgstat_drop_transactional(_kind: PgStat_Kind, _dboid: Oid, _objoid: Oid) {}

/// Reset a variable-kind entry's shared stats to zero (pgstat.c:
/// pgstat_reset_entry, SIMPLIFIED). The `ts` reset-timestamp is accepted for
/// signature fidelity but, since the per-kind reset_timestamp_cb is invoked by
/// callers, here we simply zero the whole shared blob.
pub unsafe fn pgstat_reset_entry(kind: PgStat_Kind, dboid: Oid, objoid: Oid, _ts: TimestampTz) {
    let eref = pgstat_get_entry_ref(kind, dboid, objoid, false, null_mut());
    if eref.is_null() {
        return;
    }
    let (shared_size, _) = pgstat_kind_sizes(kind);
    let p = (*eref).shared_stats as *mut u8;
    if !p.is_null() && shared_size > 0 {
        core::ptr::write_bytes(p, 0, shared_size);
    }
}

// ===========================================================================
// Top-level pgstat.c functions (1:1 translation).
//
// DEVIATION: the canonical infrastructure these functions lean on -- the
// `PgStat_KindInfo` table, the dshash `shared_hash`, the `pgStatPending` dlist,
// the snapshot simplehash, and the on-disk stats file I/O -- is hosted in the
// canonical `pgstat_internal` / `pgstat_shmem` modules against the FULL types,
// whose signatures are incompatible with this subset's own `pgStatLocal` /
// `pgstat_get_entry_ref` (objid is `Oid` here, `u64` there). To keep this file
// self-consistent and compiling until the duplicate types are deduped, the
// genuinely-unported infrastructure deps are provided as LOCAL `TODO(pg-port)`
// stubs below, and the function bodies are otherwise translated statement for
// statement from upstream PostgreSQL 18.3 `pgstat.c`.
// ===========================================================================

use crate::utils::pgstat_kind::{
    pgstat_is_kind_builtin, pgstat_is_kind_custom, PGSTAT_KIND_BUILTIN_MAX, PGSTAT_KIND_BUILTIN_MIN,
    PGSTAT_KIND_CUSTOM_MAX, PGSTAT_KIND_CUSTOM_MIN, PGSTAT_KIND_INVALID, PGSTAT_KIND_MAX,
    PGSTAT_KIND_MIN,
};

use crate::miscadmin::MyDatabaseId;
use crate::port::pgstrcasecmp::pg_strcasecmp;

/// Variable-amount stats kind for per-backend statistics (pgstat.h:
/// PGSTAT_KIND_BACKEND).
pub const PGSTAT_KIND_BACKEND: PgStat_Kind = 6;

// ---------------------------------------------------------------------------
// Timer definitions (pgstat.c). In milliseconds.
// ---------------------------------------------------------------------------

/// minimum interval non-forced stats flushes.
const PGSTAT_MIN_INTERVAL: c_long = 1000;
/// how long until to block flushing pending stats updates
const PGSTAT_MAX_INTERVAL: c_long = 60000;
/// when to call pgstat_report_stat() again, even when idle
const PGSTAT_IDLE_INTERVAL: c_long = 10000;

// ---------------------------------------------------------------------------
// GUC parameters (pgstat.c)
// ---------------------------------------------------------------------------

pub const PGSTAT_FETCH_CONSISTENCY_NONE: c_int = 0;
pub const PGSTAT_FETCH_CONSISTENCY_CACHE: c_int = 1;
pub const PGSTAT_FETCH_CONSISTENCY_SNAPSHOT: c_int = 2;

#[no_mangle]
pub static mut pgstat_track_counts: bool = false;
#[no_mangle]
pub static mut pgstat_fetch_consistency: c_int = PGSTAT_FETCH_CONSISTENCY_CACHE;

/// Track pending reports for fixed-numbered stats, used by pgstat_report_stat().
#[no_mangle]
pub static mut pgstat_report_fixed: bool = false;

// ---------------------------------------------------------------------------
// Local data (pgstat.c)
// ---------------------------------------------------------------------------

/// Force the next stats flush to happen regardless of PGSTAT_MIN_INTERVAL.
static mut pgStatForceNextFlush: bool = false;

/// Force-clear existing snapshot before next use when stats_fetch_consistency
/// is changed.
static mut force_stats_snapshot_clear: bool = false;

// For assertions that check pgstat is not used before init / after shutdown.
#[cfg(debug_assertions)]
static mut pgstat_is_initialized: bool = false;
#[cfg(debug_assertions)]
static mut pgstat_is_shutdown: bool = false;

// ---------------------------------------------------------------------------
// TODO(pg-port) stubs for canonical infrastructure not present in this subset.
// (Hosted for real in pgstat_internal.rs / pgstat_shmem.rs against the full,
// currently-incompatible types.)
// ---------------------------------------------------------------------------

/// TODO(pg-port): the per-kind KindInfo table lives in `pgstat_internal`; the
/// subset has no compatible table, so this returns null.
unsafe fn pgstat_get_kind_info(_kind: PgStat_Kind) -> *const c_void {
    null()
}

/// TODO(pg-port): real home `pgstat_internal::pgstat_drop_all_entries`
/// (incompatible types). No-op in the subset.
unsafe fn pgstat_drop_all_entries() {}

/// TODO(pg-port): real home `pgstat_database.rs` (incompatible signature).
unsafe fn pgstat_reset_database_timestamp(_dboid: Oid, _ts: TimestampTz) {}

/// TODO(pg-port): real home `pgstat_shmem.rs::pgstat_reset_entries_of_kind`.
unsafe fn pgstat_reset_entries_of_kind(_kind: PgStat_Kind, _ts: TimestampTz) {}

/// TODO(pg-port): real home `pgstat_shmem.rs::pgstat_reset_matching_entries`.
unsafe fn pgstat_reset_matching_entries(
    _match_fn: unsafe fn(*mut c_void, Datum) -> bool,
    _match_data: Datum,
    _ts: TimestampTz,
) {
}

/// TODO(pg-port): real home `backend_status.rs`.
unsafe fn pgstat_clear_backend_activity_snapshot() {}

/// TODO(pg-port): real home `pgstat_database.rs`.
unsafe fn pgstat_report_disconnect(_dboid: Oid) {}

/// TODO(pg-port): real home `pgstat_database.rs`.
unsafe fn pgstat_update_dbstats(_now: TimestampTz) {}

/// TODO(pg-port): real home `pgstat_shmem.rs::pgstat_drop_entry`.
unsafe fn pgstat_drop_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: Oid) -> bool {
    true
}

/// TODO(pg-port): real home `pgstat_shmem.rs::pgstat_request_entry_refs_gc`.
unsafe fn pgstat_request_entry_refs_gc() {}

/// TODO(pg-port): real home `pgstat_shmem.rs::pgstat_detach_shmem`.
unsafe fn pgstat_detach_shmem() {}

/// TODO(pg-port): transaction state predicate, `access/xact.c` (unported here).
unsafe fn IsTransactionOrTransactionBlock() -> bool {
    false
}

/// TODO(pg-port): `utils/timestamp.c` (unported); upstream uses the xact stop
/// timestamp as an approximation of "now".
unsafe fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    GetCurrentTimestamp()
}

/// TODO(pg-port): `utils/timestamp.c` TimestampDifferenceExceeds.
unsafe fn TimestampDifferenceExceeds(
    _start: TimestampTz,
    _stop: TimestampTz,
    _msec: c_long,
) -> bool {
    false
}

// ---------------------------------------------------------------------------
// Functions managing the state of the stats system for all backends.
// ---------------------------------------------------------------------------

/// Read on-disk stats into memory at server start.
///
/// Should only be called by the startup process or in single user mode.
pub unsafe fn pgstat_restore_stats() {
    pgstat_read_statsfile();
}

/// Remove the stats file.  This is currently used only if WAL recovery is
/// needed after a crash.
///
/// Should only be called by the startup process or in single user mode.
pub unsafe fn pgstat_discard_stats() {
    /* NB: this needs to be done even in single user mode */

    // TODO(pg-port): unlink(PGSTAT_STAT_PERMANENT_FILENAME) -- on-disk stats
    // file path/IO not ported in this subset.
    let ret: c_int = pgstat_unlink_permanent();
    if ret != 0 {
        // C: distinguishes ENOENT (DEBUG2) from other errors (LOG). The file
        // path/errno plumbing is not ported; emit the DEBUG2 variant.
        elog!(DEBUG2, "didn't need to unlink permanent stats file - didn't exist");
        /* C also: ereport(LOG, errcode_for_file_access(),
         * errmsg("could not unlink permanent statistics file \"%s\": %m", ...)) */
    } else {
        ereport!(DEBUG2, errmsg!("unlinked permanent statistics file"));
        /* C also: errcode_for_file_access() */
    }

    /*
     * Reset stats contents. This will set reset timestamps of fixed-numbered
     * stats to the current time (no variable stats exist).
     */
    pgstat_reset_after_failure();
}

/// pgstat_before_server_shutdown() needs to be called by exactly one process
/// during regular server shutdowns. Otherwise all stats will be lost.
pub unsafe fn pgstat_before_server_shutdown(code: c_int, _arg: Datum) {
    Assert!(!pgStatLocal.shmem.is_null());
    // Assert(!pgStatLocal.shmem->is_shutdown); -- is_shutdown not in subset ctl.

    /*
     * Stats should only be reported after pgstat_initialize() and before
     * pgstat_shutdown(). This is a convenient point to catch most violations
     * of this rule.
     */
    // Assert(pgstat_is_initialized && !pgstat_is_shutdown);

    /* flush out our own pending changes before writing out */
    pgstat_report_stat(true);

    /*
     * Only write out file during normal shutdown. Don't even signal that we've
     * shutdown during irregular shutdowns, because the shutdown sequence isn't
     * coordinated to ensure this backend shuts down last.
     */
    if code == 0 {
        // pgStatLocal.shmem->is_shutdown = true; -- field not in subset ctl.
        pgstat_write_statsfile();
    }
}

// ---------------------------------------------------------------------------
// Backend initialization / shutdown functions
// ---------------------------------------------------------------------------

/// Shut down a single backend's statistics reporting at process exit.
///
/// Flush out any remaining statistics counts.  Without this, operations
/// triggered during backend exit (such as temp table deletions) won't be
/// counted.
unsafe fn pgstat_shutdown_hook(_code: c_int, _arg: Datum) {
    // Assert(!pgstat_is_shutdown);
    // Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

    /*
     * If we got as far as discovering our own database ID, we can flush out
     * what we did so far.  Otherwise, we'd be reporting an invalid database
     * ID, so forget it.
     */
    if OidIsValid(MyDatabaseId) {
        pgstat_report_disconnect(MyDatabaseId);
    }

    pgstat_report_stat(true);

    /* there shouldn't be any pending changes left */
    // Assert(dlist_is_empty(&pgStatPending));
    // dlist_init(&pgStatPending);

    /* drop the backend stats entry */
    if !pgstat_drop_entry(PGSTAT_KIND_BACKEND, InvalidOid, MyProcNumber as Oid) {
        pgstat_request_entry_refs_gc();
    }

    pgstat_detach_shmem();

    #[cfg(debug_assertions)]
    {
        pgstat_is_shutdown = true;
    }
}

/// Initialize pgstats state, and set up our on-proc-exit hook. Called from
/// BaseInit().
///
/// NOTE: MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
pub unsafe fn pgstat_initialize() {
    // Assert(!pgstat_is_initialized);

    pgstat_attach_shmem();

    pgstat_init_snapshot_fixed();

    /* Backend initialization callbacks */
    // TODO(pg-port): the per-kind init_backend_cb dispatch needs the KindInfo
    // table; the subset's pgstat_get_kind_info() returns null, so this loop is
    // a faithful no-op body. Structure preserved:
    let mut kind: PgStat_Kind = PGSTAT_KIND_MIN;
    while kind <= PGSTAT_KIND_MAX {
        let kind_info = pgstat_get_kind_info(kind);
        if kind_info.is_null() {
            kind += 1;
            continue;
        }
        /* kind_info->init_backend_cb() -- dispatched once KindInfo is ported */
        kind += 1;
    }

    /* Set up a process-exit hook to clean up */
    before_shmem_exit(pgstat_shutdown_hook, 0);

    #[cfg(debug_assertions)]
    {
        pgstat_is_initialized = true;
    }
}

// ---------------------------------------------------------------------------
// Public functions used by backends follow
// ---------------------------------------------------------------------------

/// Flush pending statistics updates to shared memory.  See upstream pgstat.c
/// for the detailed force / interval / nowait contract.
pub unsafe fn pgstat_report_stat(mut force: bool) -> c_long {
    static mut pending_since: TimestampTz = 0;
    static mut last_flush: TimestampTz = 0;
    let partial_flush;
    let now: TimestampTz;
    let nowait;

    pgstat_assert_is_up();
    Assert!(!IsTransactionOrTransactionBlock());

    /* "absorb" the forced flush even if there's nothing to flush */
    if pgStatForceNextFlush {
        force = true;
        pgStatForceNextFlush = false;
    }

    /* Don't expend a clock check if nothing to do */
    if pgstat_pending_is_empty() && !pgstat_report_fixed {
        return 0;
    }

    /*
     * There should never be stats to report once stats are shut down.
     */
    // Assert(!pgStatLocal.shmem->is_shutdown);

    if force {
        now = GetCurrentTimestamp();
    } else {
        now = GetCurrentTransactionStopTimestamp();

        if pending_since > 0
            && TimestampDifferenceExceeds(pending_since, now, PGSTAT_MAX_INTERVAL)
        {
            /* don't keep pending updates longer than PGSTAT_MAX_INTERVAL */
            force = true;
        } else if last_flush > 0
            && !TimestampDifferenceExceeds(last_flush, now, PGSTAT_MIN_INTERVAL)
        {
            /* don't flush too frequently */
            if pending_since == 0 {
                pending_since = now;
            }

            return PGSTAT_IDLE_INTERVAL;
        }
    }

    pgstat_update_dbstats(now);

    /* don't wait for lock acquisition when !force */
    nowait = !force;

    let mut partial = false;

    /* flush of variable-numbered stats tracked in pending entries list */
    partial |= pgstat_flush_pending_entries(nowait);

    /* flush of other stats kinds */
    if pgstat_report_fixed {
        let mut kind: PgStat_Kind = PGSTAT_KIND_MIN;
        while kind <= PGSTAT_KIND_MAX {
            let kind_info = pgstat_get_kind_info(kind);

            if kind_info.is_null() {
                kind += 1;
                continue;
            }
            /* if (!kind_info->flush_static_cb) continue;
             * partial |= kind_info->flush_static_cb(nowait); -- needs KindInfo */
            kind += 1;
        }
    }
    partial_flush = partial;

    last_flush = now;

    /*
     * If some of the pending stats could not be flushed due to lock
     * contention, let the caller know when to retry.
     */
    if partial_flush {
        /* force should have prevented us from getting here */
        Assert!(!force);

        /* remember since when stats have been pending */
        if pending_since == 0 {
            pending_since = now;
        }

        return PGSTAT_IDLE_INTERVAL;
    }

    pending_since = 0;
    pgstat_report_fixed = false;

    0
}

/// Force locally pending stats to be flushed during the next
/// pgstat_report_stat() call. This is useful for writing tests.
pub unsafe fn pgstat_force_next_flush() {
    pgStatForceNextFlush = true;
}

/// Only for use by pgstat_reset_counters()
unsafe fn match_db_entries(entry: *mut c_void, _match_data: Datum) -> bool {
    let entry = entry as *mut crate::utils::activity::pgstat_internal::PgStatShared_HashEntry;
    (*entry).key.dboid == DatumGetObjectId(MyDatabaseId as Datum)
}

/// Reset counters for our database.
///
/// Permission checking for this function is managed through the normal GRANT
/// system.
pub unsafe fn pgstat_reset_counters() {
    let ts: TimestampTz = GetCurrentTimestamp();

    pgstat_reset_matching_entries(match_db_entries, ObjectIdGetDatum(MyDatabaseId), ts);
}

/// Reset a single variable-numbered entry.
///
/// If the stats kind is within a database, also reset the database's
/// stat_reset_timestamp.
pub unsafe fn pgstat_reset(kind: PgStat_Kind, dboid: Oid, objid: Oid) {
    let _kind_info = pgstat_get_kind_info(kind);
    let ts: TimestampTz = GetCurrentTimestamp();

    /* not needed atm, and doesn't make sense with the current signature */
    // Assert(!pgstat_get_kind_info(kind)->fixed_amount);

    /* reset the "single counter" */
    pgstat_reset_entry(kind, dboid, objid, ts);

    // C: if (!kind_info->accessed_across_databases)
    //        pgstat_reset_database_timestamp(dboid, ts);
    // TODO(pg-port): accessed_across_databases lives in KindInfo (unported);
    // forward the reset unconditionally to preserve the timestamp behavior.
    pgstat_reset_database_timestamp(dboid, ts);
}

/// Reset stats for all entries of a kind.
pub unsafe fn pgstat_reset_of_kind(kind: PgStat_Kind) {
    let _kind_info = pgstat_get_kind_info(kind);
    let ts: TimestampTz = GetCurrentTimestamp();

    // C: if (kind_info->fixed_amount) kind_info->reset_all_cb(ts);
    //    else pgstat_reset_entries_of_kind(kind, ts);
    // TODO(pg-port): fixed_amount / reset_all_cb come from KindInfo (unported);
    // route variable-numbered kinds through the entry resetter.
    pgstat_reset_entries_of_kind(kind, ts);
}

// ---------------------------------------------------------------------------
// Fetching of stats
// ---------------------------------------------------------------------------

/// Discard any data collected in the current transaction.  Any subsequent
/// request will cause new snapshots to be read.
pub unsafe fn pgstat_clear_snapshot() {
    pgstat_assert_is_up();

    // C resets snapshot.fixed_valid / custom_valid / stats / mode and frees the
    // snapshot memory context. Those snapshot-machinery fields are not present
    // in this subset's PgStat_Snapshot, so only the forwarded reset + flag
    // clear are translated here.
    // TODO(pg-port): full snapshot-context teardown once PgStat_Snapshot carries
    // the stats/context/mode fields.

    /*
     * Historically the backend_status.c facilities lived in this file, and were
     * reset with the same function. For now keep it that way, and forward the
     * reset request.
     */
    pgstat_clear_backend_activity_snapshot();

    /* Reset this flag, as it may be possible that a cleanup was forced. */
    force_stats_snapshot_clear = false;
}

/// If a stats snapshot has been taken, return the timestamp at which that was
/// done, and set *have_snapshot accordingly.
pub unsafe fn pgstat_get_stat_snapshot_timestamp(have_snapshot: *mut bool) -> TimestampTz {
    if force_stats_snapshot_clear {
        pgstat_clear_snapshot();
    }

    // C: if (pgStatLocal.snapshot.mode == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT) {
    //        *have_snapshot = true; return pgStatLocal.snapshot.snapshot_timestamp; }
    // TODO(pg-port): snapshot.mode / snapshot_timestamp not in subset; no full
    // snapshot is ever built here.
    *have_snapshot = false;

    0
}

/// Whether an entry of `kind` for (dboid, objid) exists.
pub unsafe fn pgstat_have_entry(kind: PgStat_Kind, dboid: Oid, objid: Oid) -> bool {
    /* fixed-numbered stats always exist */
    // C: if (pgstat_get_kind_info(kind)->fixed_amount) return true;
    // TODO(pg-port): fixed_amount lives in KindInfo (unported); fixed kinds are
    // recognized via the kind-id range instead.
    if pgstat_is_fixed_kind(kind) {
        return true;
    }

    !pgstat_get_entry_ref(kind, dboid, objid, false, null_mut()).is_null()
}

/// Initialize fixed-numbered statistics data in snapshots, only for custom
/// stats kinds.
unsafe fn pgstat_init_snapshot_fixed() {
    // C iterates PGSTAT_KIND_CUSTOM_MIN..=MAX and allocates snapshot.custom_data
    // for fixed custom kinds. The subset has no custom_data slot in its snapshot,
    // and no custom kinds are registered, so this body is a faithful no-op.
    let mut kind: PgStat_Kind = PGSTAT_KIND_CUSTOM_MIN;
    while kind <= PGSTAT_KIND_CUSTOM_MAX {
        let kind_info = pgstat_get_kind_info(kind);
        if kind_info.is_null() {
            kind += 1;
            continue;
        }
        /* pgStatLocal.snapshot.custom_data[...] = MemoryContextAlloc(...) */
        kind += 1;
    }
}

/// Prepare the snapshot simplehash for caching mode.
unsafe fn pgstat_prep_snapshot() {
    if force_stats_snapshot_clear {
        pgstat_clear_snapshot();
    }

    // C: if (consistency == NONE || snapshot.stats != NULL) return;
    //    create snapshot.context + snapshot.stats simplehash.
    // TODO(pg-port): snapshot.stats simplehash + context not present in subset.
    if pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE {
        // (no snapshot.stats field to check)
    }
}

/// Build a full snapshot of all stats (snapshot consistency mode).
unsafe fn pgstat_build_snapshot() {
    /* should only be called when we need a snapshot */
    Assert!(pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT);

    // C walks the dshash of variable stats and all fixed kinds, copying each into
    // snapshot.stats / the per-kind snapshot slots, then sets snapshot.mode.
    // TODO(pg-port): dshash seq + snapshot.stats simplehash + snapshot.mode not
    // present in this subset. Build the fixed-kind snapshots that DO exist:
    pgstat_prep_snapshot();

    let mut kind: PgStat_Kind = PGSTAT_KIND_MIN;
    while kind <= PGSTAT_KIND_MAX {
        if pgstat_is_fixed_kind(kind) {
            pgstat_build_snapshot_fixed(kind);
        }
        kind += 1;
    }
}

/// Build a snapshot for a single fixed-numbered kind.
unsafe fn pgstat_build_snapshot_fixed(kind: PgStat_Kind) {
    use crate::utils::activity::pgstat_internal as pgi;
    let kind_info = pgi::pgstat_get_kind_info(kind);
    let idx: usize;
    let valid: *mut bool;

    /* Position in fixed_valid or custom_valid */
    if pgstat_is_kind_builtin(kind) {
        idx = kind as usize;
        valid = pgi::pgStatLocal.snapshot.fixed_valid.as_mut_ptr();
    } else {
        idx = (kind as i32 - PGSTAT_KIND_CUSTOM_MIN as i32) as usize;
        valid = pgi::pgStatLocal.snapshot.custom_valid.as_mut_ptr();
    }

    Assert!((*kind_info).fixed_amount());
    Assert!((*kind_info).snapshot_cb.is_some());

    if pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE {
        /* rebuild every time */
        *valid.add(idx) = false;
    } else if *valid.add(idx) {
        /* in snapshot mode we shouldn't get called again */
        Assert!(pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_CACHE);
        return;
    }

    Assert!(!*valid.add(idx));

    ((*kind_info).snapshot_cb.unwrap())();

    Assert!(!*valid.add(idx));
    *valid.add(idx) = true;
}


// ---------------------------------------------------------------------------
// Backend-local pending stats infrastructure
// ---------------------------------------------------------------------------

/// Return an existing stats entry, or NULL.
///
/// This should only be used as a helper function for pgstatfuncs.c.
pub unsafe fn pgstat_fetch_pending_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: Oid,
) -> *mut PgStat_EntryRef {
    let entry_ref = pgstat_get_entry_ref(kind, dboid, objid, false, null_mut());

    if entry_ref.is_null() || (*entry_ref).pending.is_null() {
        return null_mut();
    }

    entry_ref
}

/// Drop the pending entry referenced by `entry_ref`.
pub unsafe fn pgstat_delete_pending_entry(entry_ref: *mut PgStat_EntryRef) {
    // C derives kind from entry_ref->shared_entry->key.kind, then calls the
    // kind's delete_pending_cb before pfree-ing the pending blob and unlinking
    // the dlist node.
    // TODO(pg-port): shared_entry / delete_pending_cb / pending_node dlist not in
    // subset. Drop the pending blob via the owning VarEntry.
    let pending_data = (*entry_ref).pending;
    Assert!(!pending_data.is_null());

    pgstat_free_pending_blob(entry_ref);
    (*entry_ref).pending = null_mut();
}

/// Flush out pending variable-numbered stats.
/* Backend-local list of pending stats entries (pgstat.c). */
static mut pgStatPending: crate::lib::ilist::dlist_head = crate::lib::ilist::dlist_head {
    head: crate::lib::ilist::dlist_node { prev: core::ptr::null_mut(), next: core::ptr::null_mut() },
};

unsafe fn pgstat_flush_pending_entries(nowait: bool) -> bool {
    use crate::lib::ilist::{dlist_is_empty, dlist_head_node, dlist_has_next, dlist_next_node, dlist_node};
    use crate::utils::activity::pgstat_internal as pgi;
    type ER = pgi::PgStat_EntryRef;
    let mut have_pending = false;
    let mut cur: *mut dlist_node = core::ptr::null_mut();

    if !dlist_is_empty(&raw const pgStatPending) {
        cur = dlist_head_node(&raw mut pgStatPending);
    }

    while !cur.is_null() {
        let entry_ref = crate::dlist_container!(ER, pending_node, cur);
        let key = (*(*entry_ref).shared_entry).key;
        let kind = key.kind;
        let kind_info = pgi::pgstat_get_kind_info(kind);
        let did_flush: bool;
        let next: *mut dlist_node;

        Assert!(!(*kind_info).fixed_amount());
        Assert!((*kind_info).flush_pending_cb.is_some());

        /* flush the stats, if possible */
        did_flush = ((*kind_info).flush_pending_cb.unwrap())(entry_ref, nowait);

        Assert!(did_flush || nowait);

        /* determine next entry, before deleting the pending entry */
        if dlist_has_next(&raw const pgStatPending, cur) {
            next = dlist_next_node(&raw mut pgStatPending, cur);
        } else {
            next = core::ptr::null_mut();
        }

        /* if successfully flushed, remove entry */
        if did_flush {
            pgi::pgstat_delete_pending_entry(entry_ref);
        } else {
            have_pending = true;
        }

        cur = next;
    }

    Assert!(dlist_is_empty(&raw const pgStatPending) == !have_pending);

    have_pending
}

// ---------------------------------------------------------------------------
// Helper / infrastructure functions
// ---------------------------------------------------------------------------

/// Map a stats-kind name string to its PgStat_Kind.
pub unsafe fn pgstat_get_kind_from_str(kind_str: *mut c_char) -> PgStat_Kind {
    let mut kind: PgStat_Kind = PGSTAT_KIND_BUILTIN_MIN;
    while kind <= PGSTAT_KIND_BUILTIN_MAX {
        let name = pgstat_kind_builtin_name(kind);
        if !name.is_null() && pg_strcasecmp(kind_str, name) == 0 {
            return kind;
        }
        kind += 1;
    }

    /* Check the custom set of cumulative stats */
    // TODO(pg-port): pgstat_kind_custom_infos table not in subset.

    ereport!(
        ERROR,
        errmsg!(
            "invalid statistics kind: \"{}\"",
            std::ffi::CStr::from_ptr(kind_str).to_string_lossy()
        )
    );
    /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    #[allow(unreachable_code)]
    PGSTAT_KIND_INVALID /* avoid compiler warnings */
}

#[inline]
unsafe fn pgstat_is_kind_valid(kind: PgStat_Kind) -> bool {
    pgstat_is_kind_builtin(kind) || pgstat_is_kind_custom(kind)
}

/// Register a new (custom) stats kind.
///
/// TODO(pg-port): the custom-kind registry (pgstat_kind_custom_infos) and the
/// KindInfo type are hosted canonically in `pgstat_internal`; the subset has no
/// compatible table, so registration is not performed. The validation order
/// from upstream is preserved as documentation.
pub unsafe fn pgstat_register_kind(kind: PgStat_Kind, _kind_info: *const c_void) {
    /* C validates name non-empty, kind in custom range, in shared_preload, and
     * non-duplicate, then records pgstat_kind_custom_infos[idx] = kind_info. */
    if !pgstat_is_kind_custom(kind) {
        ereport!(
            ERROR,
            errmsg!("custom cumulative statistics ID {} is out of range", kind)
        );
        /* C also: errhint("Provide a custom cumulative statistics ID between %u and %u.", ...) */
    }
}

/// Stats should only be reported after pgstat_initialize() and before
/// pgstat_shutdown().
pub fn pgstat_assert_is_up() {
    // Assert(pgstat_is_initialized && !pgstat_is_shutdown);
}

// ---------------------------------------------------------------------------
// reading and writing of on-disk stats file
// ---------------------------------------------------------------------------

// C stdio primitives used by the chunk helpers and the record-walk drivers.
extern "C" {
    fn fwrite(ptr: *const c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn fputc(c: c_int, stream: *mut c_void) -> c_int;
    fn fgetc(stream: *mut c_void) -> c_int;
    fn fseek(stream: *mut c_void, offset: c_long, whence: c_int) -> c_int;
    fn ferror(stream: *mut c_void) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    #[cfg_attr(target_os = "macos", link_name = "__error")]
    #[cfg_attr(target_os = "linux", link_name = "__errno_location")]
    fn __error() -> *mut c_int;
}

unsafe fn errno() -> c_int {
    *__error()
}

const ENOENT: c_int = 2;
const SEEK_CUR: c_int = 1;
const EOF: c_int = -1;

/// Render a NUL-terminated C string for `errmsg!`/`elog!` placeholders.
unsafe fn display_cstr(p: *const c_char) -> String {
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}

// pgStatLocal.shared_hash is `*mut c_void` in the divergent pgstat_internal
// alias (dshash_table = c_void there); the canonical dshash entry points want a
// `*mut dshash::dshash_table`. Cast through this alias at the call boundary.
use crate::lib::dshash::dshash_table as DsHashTable;

/// pg_atomic_read_u32 (port/atomics.h). Read of the refcount field.
#[inline]
unsafe fn pg_atomic_read_u32(ptr: *mut crate::port::atomics::pg_atomic_uint32) -> uint32 {
    crate::port::atomics::pg_atomic_read_u32_impl(&*ptr)
}

/// TODO(pg-port): utils/dsa.c not wired into the module tree yet (mirrors the
/// local stub in lib/dshash.rs). Resolves a dsa_pointer within the stats DSA.
unsafe fn dsa_get_address(
    _area: *mut crate::utils::activity::pgstat_internal::dsa_area,
    _dp: crate::lib::dshash::dsa_pointer,
) -> *mut c_void {
    unimplemented!() // TODO: utils/dsa.c
}

/// pgstat_internal.h: PGSTAT_FILE_FORMAT_ID.
const PGSTAT_FILE_FORMAT_ID: int32 = 0x01A5BCB7;

// Identifiers in stats file (pgstat.c).
const PGSTAT_FILE_ENTRY_END: c_int = b'E' as c_int; /* end of file */
const PGSTAT_FILE_ENTRY_FIXED: c_int = b'F' as c_int; /* fixed-numbered stats entry */
const PGSTAT_FILE_ENTRY_NAME: c_int = b'N' as c_int; /* stats entry identified by name */
const PGSTAT_FILE_ENTRY_HASH: c_int = b'S' as c_int; /* stats entry identified by PgStat_HashKey */

/// pgstat.h: PGSTAT_STAT_PERMANENT_FILENAME / PGSTAT_STAT_PERMANENT_TMPFILE.
const PGSTAT_STAT_PERMANENT_FILENAME: &core::ffi::CStr = c"pg_stat/pgstat.stat";
const PGSTAT_STAT_PERMANENT_TMPFILE: &core::ffi::CStr = c"pg_stat/pgstat.tmp";

/// helper for pgstat_write_statsfile() (pgstat.c: write_chunk).
#[allow(dead_code)]
unsafe fn write_chunk(fpout: *mut c_void, ptr: *mut c_void, len: Size) {
    let rc: c_int;

    rc = fwrite(ptr, len, 1, fpout) as c_int;

    /* we'll check for errors with ferror once at the end */
    let _ = rc;
}

/// helper for pgstat_read_statsfile() (pgstat.c: read_chunk).
#[allow(dead_code)]
unsafe fn read_chunk(fpin: *mut c_void, ptr: *mut c_void, len: Size) -> bool {
    fread(ptr, 1, len, fpin) == len
}

/// This function is called in the last process that is accessing the shared
/// stats so locking is not required.
unsafe fn pgstat_write_statsfile() {
    use crate::lib::dshash::{dshash_seq_init, dshash_seq_next, dshash_seq_status, dshash_seq_term};
    use crate::miscadmin::{
        IsUnderPostmaster, MyBackendType, B_CHECKPOINTER, CHECK_FOR_INTERRUPTS,
    };
    use crate::storage::file::fd::{durable_rename, AllocateFile, FreeFile};
    use crate::utils::activity::pgstat_internal as pgi;

    let fpout: *mut c_void;
    let mut format_id: int32;
    let tmpfile: *const c_char = PGSTAT_STAT_PERMANENT_TMPFILE.as_ptr();
    let statfile: *const c_char = PGSTAT_STAT_PERMANENT_FILENAME.as_ptr();
    let mut hstat: dshash_seq_status = core::mem::zeroed();
    let mut ps: *mut pgi::PgStatShared_HashEntry;

    pgstat_assert_is_up();

    /* should be called only by the checkpointer or single user mode */
    Assert!(!IsUnderPostmaster || MyBackendType == B_CHECKPOINTER);

    /* we're shutting down, so it's ok to just override this */
    pgstat_fetch_consistency = PGSTAT_FETCH_CONSISTENCY_NONE;

    elog!(DEBUG2, "writing stats file \"{}\"", display_cstr(statfile));

    /*
     * Open the statistics temp file to write out the current values.
     */
    fpout = AllocateFile(tmpfile, c"w".as_ptr());
    if fpout.is_null() {
        ereport!(
            LOG,
            errmsg!(
                "could not open temporary statistics file \"{}\": %m",
                display_cstr(tmpfile)
            )
        );
        /* C also: errcode_for_file_access() */
        return;
    }

    /*
     * Write the file header --- currently just a format ID.
     */
    format_id = PGSTAT_FILE_FORMAT_ID;
    write_chunk(
        fpout,
        &raw mut format_id as *mut c_void,
        core::mem::size_of::<int32>(),
    );

    /* Write various stats structs for fixed number of objects */
    let mut kind: PgStat_Kind = PGSTAT_KIND_MIN;
    while kind <= PGSTAT_KIND_MAX {
        let ptr: *mut c_char;
        let info = pgi::pgstat_get_kind_info(kind);

        if info.is_null() || !(*info).fixed_amount() {
            kind += 1;
            continue;
        }

        if pgstat_is_kind_builtin(kind) {
            Assert!((*info).snapshot_ctl_off != 0);
        }

        /* skip if no need to write to file */
        if !(*info).write_to_file() {
            kind += 1;
            continue;
        }

        pgstat_build_snapshot_fixed(kind);
        if pgstat_is_kind_builtin(kind) {
            ptr = (&raw mut pgi::pgStatLocal.snapshot as *mut c_char)
                .add((*info).snapshot_ctl_off as usize);
        } else {
            ptr = pgi::pgStatLocal.snapshot.custom_data[(kind - PGSTAT_KIND_CUSTOM_MIN) as usize]
                as *mut c_char;
        }

        fputc(PGSTAT_FILE_ENTRY_FIXED, fpout);
        write_chunk(
            fpout,
            &raw mut kind as *mut c_void,
            core::mem::size_of::<PgStat_Kind>(),
        );
        write_chunk(fpout, ptr as *mut c_void, (*info).shared_data_len as Size);

        kind += 1;
    }

    /*
     * Walk through the stats entries
     */
    dshash_seq_init(
        &raw mut hstat,
        pgi::pgStatLocal.shared_hash as *mut DsHashTable,
        false,
    );
    loop {
        ps = dshash_seq_next(&raw mut hstat) as *mut pgi::PgStatShared_HashEntry;
        if ps.is_null() {
            break;
        }

        let shstats: *mut pgi::PgStatShared_Common;
        let kind_info: *const pgi::PgStat_KindInfo;

        CHECK_FOR_INTERRUPTS();

        /*
         * We should not see any "dropped" entries when writing the stats file,
         * as all backends and auxiliary processes should have cleaned up their
         * references before they terminated.
         *
         * However, since we are already shutting down, it is not worth crashing
         * the server over any potential cleanup issues, so we simply skip such
         * entries if encountered.
         */
        Assert!(!(*ps).dropped);
        if (*ps).dropped {
            continue;
        }

        /*
         * This discards data related to custom stats kinds that are unknown to
         * this process.
         */
        if !pgstat_is_kind_valid((*ps).key.kind) {
            elog!(
                WARNING,
                "found unknown stats entry {}/{}/{}",
                (*ps).key.kind,
                (*ps).key.dboid,
                (*ps).key.objid
            );
            continue;
        }

        shstats =
            dsa_get_address(pgi::pgStatLocal.dsa, (*ps).body) as *mut pgi::PgStatShared_Common;

        kind_info = pgi::pgstat_get_kind_info((*ps).key.kind);

        /* if not dropped the valid-entry refcount should exist */
        Assert!(pg_atomic_read_u32(&raw mut (*ps).refcount) > 0);

        /* skip if no need to write to file */
        if !(*kind_info).write_to_file() {
            continue;
        }

        if (*kind_info).to_serialized_name.is_none() {
            /* normal stats entry, identified by PgStat_HashKey */
            fputc(PGSTAT_FILE_ENTRY_HASH, fpout);
            write_chunk(
                fpout,
                &raw mut (*ps).key as *mut c_void,
                core::mem::size_of::<pgi::PgStat_HashKey>(),
            );
        } else {
            /* stats entry identified by name on disk (e.g. slots) */
            let mut name: NameData = core::mem::zeroed();

            ((*kind_info).to_serialized_name.unwrap())(
                &raw const (*ps).key,
                shstats,
                &raw mut name,
            );

            fputc(PGSTAT_FILE_ENTRY_NAME, fpout);
            write_chunk(
                fpout,
                &raw mut (*ps).key.kind as *mut c_void,
                core::mem::size_of::<PgStat_Kind>(),
            );
            write_chunk(
                fpout,
                &raw mut name as *mut c_void,
                core::mem::size_of::<NameData>(),
            );
        }

        /* Write except the header part of the entry */
        write_chunk(
            fpout,
            pgi::pgstat_get_entry_data((*ps).key.kind, shstats),
            pgi::pgstat_get_entry_len((*ps).key.kind),
        );
    }
    dshash_seq_term(&raw mut hstat);

    /*
     * No more output to be done. Close the temp file and replace the old
     * pgstat.stat with it.  The ferror() check replaces testing for error after
     * each individual fputc or fwrite (in write_chunk()) above.
     */
    fputc(PGSTAT_FILE_ENTRY_END, fpout);

    if ferror(fpout) != 0 {
        ereport!(
            LOG,
            errmsg!(
                "could not write temporary statistics file \"{}\": %m",
                display_cstr(tmpfile)
            )
        );
        /* C also: errcode_for_file_access() */
        FreeFile(fpout);
        unlink(tmpfile);
    } else if FreeFile(fpout) < 0 {
        ereport!(
            LOG,
            errmsg!(
                "could not close temporary statistics file \"{}\": %m",
                display_cstr(tmpfile)
            )
        );
        /* C also: errcode_for_file_access() */
        unlink(tmpfile);
    } else if durable_rename(tmpfile, statfile, LOG) < 0 {
        /* durable_rename already emitted log message */
        unlink(tmpfile);
    }
}

/// Reads in existing statistics file into memory.
///
/// This function is called in the only process that is accessing the shared
/// stats so locking is not required.
unsafe fn pgstat_read_statsfile() {
    use crate::lib::dshash::{dshash_find_or_insert, dshash_release_lock};
    use crate::miscadmin::{IsPostmasterEnvironment, IsUnderPostmaster, CHECK_FOR_INTERRUPTS};
    use crate::storage::file::fd::{AllocateFile, FreeFile};
    use crate::utils::activity::pgstat_internal as pgi;

    let fpin: *mut c_void;
    let mut format_id: int32 = 0;
    let mut found: bool = false;
    let statfile: *const c_char = PGSTAT_STAT_PERMANENT_FILENAME.as_ptr();
    let shmem: *mut pgi::PgStat_ShmemControl = pgi::pgStatLocal.shmem;

    /* shouldn't be called from postmaster */
    Assert!(IsUnderPostmaster || !IsPostmasterEnvironment);

    elog!(DEBUG2, "reading stats file \"{}\"", display_cstr(statfile));

    /*
     * Try to open the stats file. If it doesn't exist, the backends simply
     * returns zero for anything and statistics simply starts from scratch with
     * empty counters.
     *
     * ENOENT is a possibility if stats collection was previously disabled or has
     * not yet written the stats file for the first time.  Any other failure
     * condition is suspicious.
     */
    fpin = AllocateFile(statfile, c"r".as_ptr());
    if fpin.is_null() {
        if errno() != ENOENT {
            ereport!(
                LOG,
                errmsg!(
                    "could not open statistics file \"{}\": %m",
                    display_cstr(statfile)
                )
            );
            /* C also: errcode_for_file_access() */
        }
        pgstat_reset_after_failure();
        return;
    }

    /*
     * Verify it's of the expected format.
     */
    if !read_chunk(
        fpin,
        &raw mut format_id as *mut c_void,
        core::mem::size_of::<int32>(),
    ) {
        elog!(WARNING, "could not read format ID");
        return pgstat_read_statsfile_error(fpin, statfile);
    }

    if format_id != PGSTAT_FILE_FORMAT_ID {
        elog!(
            WARNING,
            "found incorrect format ID {} (expected {})",
            format_id,
            PGSTAT_FILE_FORMAT_ID
        );
        return pgstat_read_statsfile_error(fpin, statfile);
    }

    /*
     * We found an existing statistics file. Read it and put all the stats data
     * into place.
     */
    loop {
        let t = fgetc(fpin);

        match t {
            PGSTAT_FILE_ENTRY_FIXED => {
                let mut kind: PgStat_Kind = 0;
                let info: *const pgi::PgStat_KindInfo;
                let ptr: *mut c_char;

                /* entry for fixed-numbered stats */
                if !read_chunk(
                    fpin,
                    &raw mut kind as *mut c_void,
                    core::mem::size_of::<PgStat_Kind>(),
                ) {
                    elog!(
                        WARNING,
                        "could not read stats kind for entry of type {}",
                        t as u8 as char
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }

                if !pgstat_is_kind_valid(kind) {
                    elog!(
                        WARNING,
                        "invalid stats kind {} for entry of type {}",
                        kind,
                        t as u8 as char
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }

                info = pgi::pgstat_get_kind_info(kind);
                if info.is_null() {
                    elog!(
                        WARNING,
                        "could not find information of kind {} for entry of type {}",
                        kind,
                        t as u8 as char
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }

                if !(*info).fixed_amount() {
                    elog!(
                        WARNING,
                        "invalid fixed_amount in stats kind {} for entry of type {}",
                        kind,
                        t as u8 as char
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }

                /* Load back stats into shared memory */
                if pgstat_is_kind_builtin(kind) {
                    ptr = (shmem as *mut c_char)
                        .add((*info).shared_ctl_off as usize + (*info).shared_data_off as usize);
                } else {
                    let idx = (kind - PGSTAT_KIND_CUSTOM_MIN) as usize;

                    ptr = ((*shmem).custom_data[idx] as *mut c_char)
                        .add((*info).shared_data_off as usize);
                }

                if !read_chunk(fpin, ptr as *mut c_void, (*info).shared_data_len as Size) {
                    elog!(
                        WARNING,
                        "could not read data of stats kind {} for entry of type {} with size {}",
                        kind,
                        t as u8 as char,
                        (*info).shared_data_len
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }
            }
            PGSTAT_FILE_ENTRY_HASH | PGSTAT_FILE_ENTRY_NAME => {
                let mut key: pgi::PgStat_HashKey = core::mem::zeroed();
                let p: *mut pgi::PgStatShared_HashEntry;
                let header: *mut pgi::PgStatShared_Common;

                CHECK_FOR_INTERRUPTS();

                if t == PGSTAT_FILE_ENTRY_HASH {
                    /* normal stats entry, identified by PgStat_HashKey */
                    if !read_chunk(
                        fpin,
                        &raw mut key as *mut c_void,
                        core::mem::size_of::<pgi::PgStat_HashKey>(),
                    ) {
                        elog!(
                            WARNING,
                            "could not read key for entry of type {}",
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }

                    if !pgstat_is_kind_valid(key.kind) {
                        elog!(
                            WARNING,
                            "invalid stats kind for entry {}/{}/{} of type {}",
                            key.kind,
                            key.dboid,
                            key.objid,
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }

                    if pgi::pgstat_get_kind_info(key.kind).is_null() {
                        elog!(
                            WARNING,
                            "could not find information of kind for entry {}/{}/{} of type {}",
                            key.kind,
                            key.dboid,
                            key.objid,
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }
                } else {
                    /* stats entry identified by name on disk (e.g. slots) */
                    let kind_info: *const pgi::PgStat_KindInfo;
                    let mut kind: PgStat_Kind = 0;
                    let mut name: NameData = core::mem::zeroed();

                    if !read_chunk(
                        fpin,
                        &raw mut kind as *mut c_void,
                        core::mem::size_of::<PgStat_Kind>(),
                    ) {
                        elog!(
                            WARNING,
                            "could not read stats kind for entry of type {}",
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }
                    if !read_chunk(
                        fpin,
                        &raw mut name as *mut c_void,
                        core::mem::size_of::<NameData>(),
                    ) {
                        elog!(
                            WARNING,
                            "could not read name of stats kind {} for entry of type {}",
                            kind,
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }
                    if !pgstat_is_kind_valid(kind) {
                        elog!(
                            WARNING,
                            "invalid stats kind {} for entry of type {}",
                            kind,
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }

                    kind_info = pgi::pgstat_get_kind_info(kind);
                    if kind_info.is_null() {
                        elog!(
                            WARNING,
                            "could not find information of kind {} for entry of type {}",
                            kind,
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }

                    if (*kind_info).from_serialized_name.is_none() {
                        elog!(
                            WARNING,
                            "invalid from_serialized_name in stats kind {} for entry of type {}",
                            kind,
                            t as u8 as char
                        );
                        return pgstat_read_statsfile_error(fpin, statfile);
                    }

                    if !((*kind_info).from_serialized_name.unwrap())(&raw const name, &raw mut key) {
                        /* skip over data for entry we don't care about */
                        if fseek(
                            fpin,
                            pgi::pgstat_get_entry_len(kind) as c_long,
                            SEEK_CUR,
                        ) != 0
                        {
                            elog!(
                                WARNING,
                                "could not seek \"{}\" of stats kind {} for entry of type {}",
                                display_cstr(NameStr(&name)),
                                kind,
                                t as u8 as char
                            );
                            return pgstat_read_statsfile_error(fpin, statfile);
                        }

                        continue;
                    }

                    Assert!(key.kind == kind);
                }

                /*
                 * This intentionally doesn't use pgstat_get_entry_ref() -
                 * putting all stats into checkpointer's pgStatEntryRefHash would
                 * be wasted effort and memory.
                 */
                p = dshash_find_or_insert(
                    pgi::pgStatLocal.shared_hash as *mut DsHashTable,
                    &raw const key as *const c_void,
                    &raw mut found,
                ) as *mut pgi::PgStatShared_HashEntry;

                /* don't allow duplicate entries */
                if found {
                    dshash_release_lock(
                        pgi::pgStatLocal.shared_hash as *mut DsHashTable,
                        p as *mut c_void,
                    );
                    elog!(
                        WARNING,
                        "found duplicate stats entry {}/{}/{} of type {}",
                        key.kind,
                        key.dboid,
                        key.objid,
                        t as u8 as char
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }

                header = pgi::pgstat_init_entry(key.kind, p);
                dshash_release_lock(
                    pgi::pgStatLocal.shared_hash as *mut DsHashTable,
                    p as *mut c_void,
                );
                if header.is_null() {
                    /*
                     * It would be tempting to switch this ERROR to a WARNING,
                     * but it would mean that all the statistics are discarded
                     * when the environment fails on OOM.
                     */
                    elog!(
                        ERROR,
                        "could not allocate entry {}/{}/{} of type {}",
                        key.kind,
                        key.dboid,
                        key.objid,
                        t as u8 as char
                    );
                }

                if !read_chunk(
                    fpin,
                    pgi::pgstat_get_entry_data(key.kind, header),
                    pgi::pgstat_get_entry_len(key.kind),
                ) {
                    elog!(
                        WARNING,
                        "could not read data for entry {}/{}/{} of type {}",
                        key.kind,
                        key.dboid,
                        key.objid,
                        t as u8 as char
                    );
                    return pgstat_read_statsfile_error(fpin, statfile);
                }
            }
            PGSTAT_FILE_ENTRY_END => {
                /*
                 * check that PGSTAT_FILE_ENTRY_END actually signals end of file
                 */
                if fgetc(fpin) != EOF {
                    elog!(WARNING, "could not read end-of-file");
                    return pgstat_read_statsfile_error(fpin, statfile);
                }

                break;
            }
            _ => {
                elog!(
                    WARNING,
                    "could not read entry of type {}",
                    t as u8 as char
                );
                return pgstat_read_statsfile_error(fpin, statfile);
            }
        }
    }

    /* done: */
    FreeFile(fpin);

    elog!(
        DEBUG2,
        "removing permanent stats file \"{}\"",
        display_cstr(statfile)
    );
    unlink(statfile);
}

/// The `error:`/`done:` tail of pgstat_read_statsfile(). C uses goto; the loop's
/// many `goto error` sites are translated as `return` calls to this helper.
unsafe fn pgstat_read_statsfile_error(fpin: *mut c_void, statfile: *const c_char) {
    use crate::storage::file::fd::FreeFile;

    ereport!(
        LOG,
        errmsg!("corrupted statistics file \"{}\"", display_cstr(statfile))
    );

    pgstat_reset_after_failure();

    /* done: */
    FreeFile(fpin);

    elog!(
        DEBUG2,
        "removing permanent stats file \"{}\"",
        display_cstr(statfile)
    );
    unlink(statfile);
}

/// Reset / drop stats after a crash or after restoring stats from disk failed.
unsafe fn pgstat_reset_after_failure() {
    let ts: TimestampTz = GetCurrentTimestamp();

    /* reset fixed-numbered stats */
    let mut kind: PgStat_Kind = PGSTAT_KIND_MIN;
    while kind <= PGSTAT_KIND_MAX {
        let kind_info = pgstat_get_kind_info(kind);
        if kind_info.is_null() || !pgstat_is_fixed_kind(kind) {
            kind += 1;
            continue;
        }
        /* kind_info->reset_all_cb(ts) -- dispatched once KindInfo is ported */
        kind += 1;
    }
    let _ = ts;

    /* and drop variable-numbered ones */
    pgstat_drop_all_entries();
}

/// GUC assign_hook for stats_fetch_consistency.
pub unsafe fn assign_stats_fetch_consistency(newval: c_int, _extra: *mut c_void) {
    /*
     * Changing this value in a transaction may cause snapshot state
     * inconsistencies, so force a clear of the current snapshot on the next
     * snapshot build attempt.
     */
    if pgstat_fetch_consistency != newval {
        force_stats_snapshot_clear = true;
    }
}

// ---------------------------------------------------------------------------
// Small local helpers backing the TODO(pg-port) translations above.
// ---------------------------------------------------------------------------

/// Whether `kind` is one of the fixed-numbered built-in kinds (archiver,
/// bgwriter, checkpointer, io, slru, wal). Stand-in for KindInfo.fixed_amount.
#[inline]
unsafe fn pgstat_is_fixed_kind(kind: PgStat_Kind) -> bool {
    matches!(
        kind,
        PGSTAT_KIND_ARCHIVER
            | PGSTAT_KIND_BGWRITER
            | PGSTAT_KIND_CHECKPOINTER
            | PGSTAT_KIND_IO
            | PGSTAT_KIND_SLRU
            | PGSTAT_KIND_WAL
    )
}

/// The built-in kind name (KindInfo.name stand-in), as a NUL-terminated C
/// string, or null for non-builtin kinds.
unsafe fn pgstat_kind_builtin_name(kind: PgStat_Kind) -> *const c_char {
    let s: &[u8] = match kind {
        PGSTAT_KIND_DATABASE => b"database\0",
        PGSTAT_KIND_RELATION => b"relation\0",
        PGSTAT_KIND_FUNCTION => b"function\0",
        PGSTAT_KIND_REPLSLOT => b"replslot\0",
        PGSTAT_KIND_SUBSCRIPTION => b"subscription\0",
        PGSTAT_KIND_BACKEND => b"backend\0",
        PGSTAT_KIND_ARCHIVER => b"archiver\0",
        PGSTAT_KIND_BGWRITER => b"bgwriter\0",
        PGSTAT_KIND_CHECKPOINTER => b"checkpointer\0",
        PGSTAT_KIND_IO => b"io\0",
        PGSTAT_KIND_SLRU => b"slru\0",
        PGSTAT_KIND_WAL => b"wal\0",
        _ => return null(),
    };
    s.as_ptr() as *const c_char
}

/// Whether the process-local pending list is empty. Stand-in for
/// dlist_is_empty(&pgStatPending); the subset has no pending dlist.
#[inline]
unsafe fn pgstat_pending_is_empty() -> bool {
    let p = &raw const VAR_ENTRIES;
    match (*p).as_ref() {
        None => true,
        Some(v) => !v.iter().any(|e| e.pending.is_some()),
    }
}

/// Free the pending blob owned by the VarEntry whose eref matches `entry_ref`.
unsafe fn pgstat_free_pending_blob(entry_ref: *mut PgStat_EntryRef) {
    let entries = var_entries();
    for e in entries.iter_mut() {
        if &mut e.eref as *mut PgStat_EntryRef == entry_ref {
            e.pending = None;
            break;
        }
    }
}

/// TODO(pg-port): unlink the permanent stats file. The on-disk path and the
/// unlink syscall plumbing are not ported; report "did not exist" (non-zero).
unsafe fn pgstat_unlink_permanent() -> c_int {
    -1
}

/// TODO(pg-port): proc-exit hook registration (`storage/ipc.h`). The before-
/// shmem-exit callback list is not ported here; registration is a no-op.
unsafe fn before_shmem_exit(_function: unsafe fn(c_int, Datum), _arg: Datum) {}

/// TODO(pg-port): this backend's proc number (`storage/proc.h` MyProcNumber).
const MyProcNumber: c_int = 0;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changecount_write_roundtrip() {
        // begin_write/end_write must leave the count even and advanced by 2.
        let mut cc: uint32 = 0;
        unsafe {
            pgstat_begin_changecount_write(&mut cc);
            assert_eq!(cc & 1, 1, "count is odd between begin and end");
            pgstat_end_changecount_write(&mut cc);
        }
        assert_eq!(cc, 2);
        assert_eq!(cc & 1, 0, "count even after a complete write");
    }

    #[test]
    fn copy_changecounted_copies_bytes() {
        let mut src = PgStat_CheckpointerStats::zeroed();
        src.num_timed = 11;
        src.num_requested = 22;
        src.buffers_written = 333;
        let mut cc: uint32 = 0; // even -> consistent on first try
        let mut dst = PgStat_CheckpointerStats::zeroed();
        unsafe {
            pgstat_copy_changecounted_stats(
                &mut dst as *mut _ as *mut c_void,
                &mut src as *mut _ as *mut c_void,
                size_of::<PgStat_CheckpointerStats>(),
                &mut cc,
            );
        }
        assert_eq!(dst.num_timed, 11);
        assert_eq!(dst.num_requested, 22);
        assert_eq!(dst.buffers_written, 333);
    }

    #[test]
    fn attach_shmem_wires_non_null() {
        unsafe {
            pgStatLocal.shmem = null_mut();
            pgstat_attach_shmem();
            assert!(!pgStatLocal.shmem.is_null(), "shmem pointer attached");
            // A snapshot through it must not crash and yields zeroed defaults.
            pgstat_snapshot_fixed(PGSTAT_KIND_WAL);
            assert_eq!(pgStatLocal.snapshot.wal.wal_counters.wal_records, 0);
        }
    }
}
