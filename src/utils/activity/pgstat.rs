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
