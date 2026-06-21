//! Implementation of relation statistics.
//!
//! This file contains the implementation of function relation. It is kept
//! separate from `pgstat.rs` to enforce the line between the statistics access /
//! storage implementation and the details about individual types of statistics.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3 `pgstat_relation.c`.
//!
//! IDENTIFICATION
//!   src/backend/utils/activity/pgstat_relation.c

use crate::prelude::*;

use crate::nodes::execnodes::Relation;
use crate::utils::rel::RelationGetRelid;

use crate::access::transam::twophase_rmgr::TWOPHASE_RM_PGSTAT_ID;
use crate::access::transam::xact::TopTransactionContext;
use crate::catalog::pg_class::RELKIND_PARTITIONED_TABLE;
use crate::miscadmin::AmAutoVacuumWorkerProcess;
use crate::utils::palloc::{palloc, pfree, MemoryContextAllocZero};

use crate::utils::activity::pgstat::{
    pgstat_create_transactional, pgstat_drop_transactional, pgstat_fetch_entry,
    pgstat_get_entry_ref, pgstat_lock_entry, pgstat_prep_pending_entry, pgstat_unlock_entry,
    PgStatShared_Relation, PgStat_Counter, PgStat_EntryRef, PgStat_Kind, PgStat_StatDBEntry,
    PgStat_StatTabEntry, PgStat_TableCounts, PgStat_TableStatus, PGSTAT_KIND_RELATION,
};
use crate::utils::activity::pgstat_internal::PgStat_SubXactStatus;

pub type TimestampTz = crate::c::int64;

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/// Per-(sub)xact transactional tuple counts for one relation (pgstat.h:
/// PgStat_TableXactStatus). The `trans` field of `PgStat_TableStatus` is kept
/// as an opaque `*mut c_void` upstream-shared, so we cast it to/from this.
#[repr(C)]
pub struct PgStat_TableXactStatus {
    /// tuples inserted in (sub)xact
    pub tuples_inserted: PgStat_Counter,
    /// tuples updated in (sub)xact
    pub tuples_updated: PgStat_Counter,
    /// tuples deleted in (sub)xact
    pub tuples_deleted: PgStat_Counter,
    /// relation truncated/dropped in this (sub)xact
    pub truncdropped: bool,
    /* tuples i/u/d prior to truncate/drop */
    pub inserted_pre_truncdrop: PgStat_Counter,
    pub updated_pre_truncdrop: PgStat_Counter,
    pub deleted_pre_truncdrop: PgStat_Counter,
    /// subtransaction nest level
    pub nest_level: c_int,
    /* links to other structs for same relation: */
    /// next higher subxact if any
    pub upper: *mut PgStat_TableXactStatus,
    /// per-table status
    pub parent: *mut PgStat_TableStatus,
    /* structs of same subxact level are linked here: */
    /// next of same subxact
    pub next: *mut PgStat_TableXactStatus,
}

/// Record that's written to 2PC state file when pgstat state is persisted.
#[repr(C)]
struct TwoPhasePgStatRecord {
    tuples_inserted: PgStat_Counter, /* tuples inserted in xact */
    tuples_updated: PgStat_Counter,  /* tuples updated in xact */
    tuples_deleted: PgStat_Counter,  /* tuples deleted in xact */
    /* tuples i/u/d prior to truncate/drop */
    inserted_pre_truncdrop: PgStat_Counter,
    updated_pre_truncdrop: PgStat_Counter,
    deleted_pre_truncdrop: PgStat_Counter,
    id: Oid,            /* table's OID */
    shared: bool,       /* is it a shared catalog? */
    truncdropped: bool, /* was the relation truncated/dropped? */
}

/// Read the typed `trans` head off a `PgStat_TableStatus` (which stores it as an
/// opaque `*mut c_void`).
#[inline]
unsafe fn tabstat_trans(pgstat_info: *mut PgStat_TableStatus) -> *mut PgStat_TableXactStatus {
    (*pgstat_info).trans as *mut PgStat_TableXactStatus
}

/// True iff every one of `len` bytes at `p` is zero (utils/memutils.h:
/// pg_memory_is_all_zeros). Local copy; the helper is defined per-unit upstream
/// is an inline header function, and there is no shared crate-level export yet.
unsafe fn pg_memory_is_all_zeros(p: *const c_void, len: usize) -> bool {
    let bytes = core::slice::from_raw_parts(p as *const u8, len);
    for &b in bytes {
        if b != 0 {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Stubs for dependencies that are not yet ported.
// ---------------------------------------------------------------------------

/// STUB: real value comes from `miscadmin.h` (set during backend startup).
pub static mut MyDatabaseId: Oid = InvalidOid;

/// STUB: real GUC from `utils/guc.h`. Defaults to true (counting enabled).
pub static mut pgstat_track_counts: bool = true;

/// STUB: real value from `catalog/catalog.c`. Without the shared-catalog
/// metadata ported, every relation is treated as non-shared.
unsafe fn IsSharedRelation(_relid: Oid) -> bool {
    false
}

/// STUB: assert the cumulative-stats subsystem is up (pgstat_internal.h). No-op.
#[inline]
unsafe fn pgstat_assert_is_up() {}

/// STUB: `access/xact.c` GetCurrentTransactionNestLevel(). The xact nesting
/// machinery is unported; always report the top level.
#[inline]
unsafe fn GetCurrentTransactionNestLevel() -> c_int {
    0
}

const PGSTAT_BACKEND_FLUSH_IO: bits32 = 1 << 0;

/// TODO(pg-port): real `pgstat_get_entry_ref_locked` lives in
/// utils/activity/pgstat.c. The process-local entry subset in `pgstat.rs` does
/// not export a locked variant, so we get-or-create the eref and rely on the
/// no-op `pgstat_lock_entry`/`pgstat_unlock_entry` for serialization.
unsafe fn pgstat_get_entry_ref_locked(
    kind: PgStat_Kind,
    dboid: Oid,
    objoid: Oid,
    nowait: bool,
) -> *mut PgStat_EntryRef {
    let eref = pgstat_get_entry_ref(kind, dboid, objoid, true, null_mut());
    let _ = pgstat_lock_entry(eref, nowait);
    eref
}

/// TODO(pg-port): real `pgstat_fetch_pending_entry` lives in pgstat.c. Returns
/// the eref iff a pending blob already exists for (kind, dboid, objoid).
unsafe fn pgstat_fetch_pending_entry(kind: PgStat_Kind, dboid: Oid, objoid: Oid) -> *mut PgStat_EntryRef {
    let eref = pgstat_get_entry_ref(kind, dboid, objoid, false, null_mut());
    if eref.is_null() || (*eref).pending.is_null() {
        return null_mut();
    }
    eref
}

/// TODO(pg-port): real `pgstat_get_xact_stack_level` lives in pgstat_xact.c.
/// The xact subxact stack is unported here; return null.
unsafe fn pgstat_get_xact_stack_level(_nest_level: c_int) -> *mut PgStat_SubXactStatus { crate::utils::activity::pgstat_xact::pgstat_get_xact_stack_level(_nest_level) }

/// TODO(pg-port): real `pgstat_prep_database_pending` lives in pgstat_database.c
/// and returns the `pgstat.rs` `PgStat_StatDBEntry`. Database pending wiring is
/// not connected in this subset; return null.
unsafe fn pgstat_prep_database_pending(_dboid: Oid) -> *mut PgStat_StatDBEntry { crate::utils::activity::pgstat_database::pgstat_prep_database_pending(_dboid) }

/// TODO(pg-port): real `pgstat_flush_io` lives in pgstat_io.c.
unsafe fn pgstat_flush_io(_nowait: bool) {}

/// TODO(pg-port): real `pgstat_flush_backend` lives in pgstat_backend.c.
unsafe fn pgstat_flush_backend(_nowait: bool, _flags: bits32) -> bool { crate::utils::activity::pgstat_backend::pgstat_flush_backend(_nowait, _flags) }

/// TODO(pg-port): real `GetCurrentTimestamp` lives in utils/adt/timestamp.c.
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    0
}

/// TODO(pg-port): real `TimestampDifferenceMilliseconds` lives in
/// utils/adt/timestamp.c.
unsafe fn TimestampDifferenceMilliseconds(_start: TimestampTz, _stop: TimestampTz) -> PgStat_Counter {
    0
}

/// TODO(pg-port): real `GetCurrentTransactionStopTimestamp` lives in
/// access/transam/xact.c. The xact machinery is unported; return 0.
unsafe fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    0
}

/// TODO(pg-port): real `RegisterTwoPhaseRecord` lives in
/// access/transam/twophase.c. 2PC record persistence is unported; no-op.
unsafe fn RegisterTwoPhaseRecord(_rmid: u8, _info: u16, _recdata: *const c_void, _len: u32) {}

// ---------------------------------------------------------------------------
// pgstat_should_count_relation (pgstat.h macro)
// ---------------------------------------------------------------------------
//
// Upstream macro:
//   (likely(rel->pgstat_info != NULL) ? true :
//    (rel->pgstat_enabled ? pgstat_assoc_relation(rel), true : false))
// i.e. if pending stats already exist, count; else if enabled, associate them
// now (lazily) and count; else don't.

#[inline]
unsafe fn pgstat_should_count_relation(rel: Relation) -> bool {
    if !(*rel).pgstat_info.is_null() {
        true
    } else if (*rel).pgstat_enabled {
        pgstat_assoc_relation(rel);
        true
    } else {
        false
    }
}

/// Cast the relcache `pgstat_info` opaque pointer to the typed pending entry.
#[inline]
unsafe fn rel_pgstat_info(rel: Relation) -> *mut PgStat_TableStatus {
    (*rel).pgstat_info as *mut PgStat_TableStatus
}

/// TODO(pg-port): real `RELKIND_HAS_STORAGE` macro lives in catalog/pg_class.h;
/// no single canonical export exists in src/ yet. Mirrors its definition.
unsafe fn RELKIND_HAS_STORAGE(relkind: c_char) -> bool {
    relkind == b'r' as c_char  /* RELKIND_RELATION */
        || relkind == b't' as c_char /* RELKIND_TOASTVALUE */
        || relkind == b'm' as c_char /* RELKIND_MATVIEW */
        || relkind == b'S' as c_char /* RELKIND_SEQUENCE */
        || relkind == b'i' as c_char /* RELKIND_INDEX */
}

// ---------------------------------------------------------------------------
// Copy stats between relations (REAL)
// ---------------------------------------------------------------------------

/// Copy stats between relations. This is used for things like REINDEX
/// CONCURRENTLY (pgstat_relation.c: pgstat_copy_relation_stats).
pub unsafe fn pgstat_copy_relation_stats(dst: Relation, src: Relation) {
    let srcstats: *mut PgStat_StatTabEntry;
    let dstshstats: *mut PgStatShared_Relation;
    let dst_ref: *mut PgStat_EntryRef;

    srcstats =
        pgstat_fetch_stat_tabentry_ext((*(*src).rd_rel).relisshared, RelationGetRelid(src));
    if srcstats.is_null() {
        return;
    }

    dst_ref = pgstat_get_entry_ref_locked(
        PGSTAT_KIND_RELATION,
        if (*(*dst).rd_rel).relisshared {
            InvalidOid
        } else {
            MyDatabaseId
        },
        RelationGetRelid(dst),
        false,
    );

    dstshstats = (*dst_ref).shared_stats as *mut PgStatShared_Relation;
    (*dstshstats).stats = *srcstats;

    pgstat_unlock_entry(dst_ref);
}

// ---------------------------------------------------------------------------
// Relation init / association / unlink (REAL)
// ---------------------------------------------------------------------------

/// Initialize a relcache entry to count access statistics. Called whenever a
/// relation is opened (pgstat_relation.c: pgstat_init_relation).
pub unsafe fn pgstat_init_relation(rel: Relation) {
    let relkind: c_char = (*(*rel).rd_rel).relkind;

    /*
     * We only count stats for relations with storage and partitioned tables
     */
    if !RELKIND_HAS_STORAGE(relkind) && relkind != RELKIND_PARTITIONED_TABLE {
        (*rel).pgstat_enabled = false;
        (*rel).pgstat_info = null_mut();
        return;
    }

    if !pgstat_track_counts {
        if !(*rel).pgstat_info.is_null() {
            pgstat_unlink_relation(rel);
        }

        /* We're not counting at all */
        (*rel).pgstat_enabled = false;
        (*rel).pgstat_info = null_mut();
        return;
    }

    (*rel).pgstat_enabled = true;
}

/// Prepare for statistics for this relation to be collected (pgstat_relation.c:
/// pgstat_assoc_relation). Ensures a pending stats reference exists before
/// stats can be generated, then mutually links the relcache entry and the
/// pending entry.
pub unsafe fn pgstat_assoc_relation(rel: Relation) {
    Assert!((*rel).pgstat_enabled);
    Assert!((*rel).pgstat_info.is_null());

    /* Else find or make the PgStat_TableStatus entry, and update link */
    let pending =
        pgstat_prep_relation_pending(RelationGetRelid(rel), (*(*rel).rd_rel).relisshared);
    (*rel).pgstat_info = pending as *mut c_void;

    /* don't allow link a stats to multiple relcache entries */
    Assert!((*pending).relation.is_null());

    /* mark this relation as the owner */
    (*pending).relation = rel;
}

/// Break the mutual link between a relcache entry and pending stats entry
/// (pgstat_relation.c: pgstat_unlink_relation). Must be called whenever one end
/// of the link is removed.
pub unsafe fn pgstat_unlink_relation(rel: Relation) {
    /* remove the link to stats info if any */
    if (*rel).pgstat_info.is_null() {
        return;
    }

    let pending = rel_pgstat_info(rel);
    /* link sanity check */
    Assert!((*pending).relation == rel);
    (*pending).relation = null_mut();
    (*rel).pgstat_info = null_mut();
}

/// Ensure that stats are dropped if transaction aborts (pgstat_relation.c:
/// pgstat_create_relation).
pub unsafe fn pgstat_create_relation(rel: Relation) {
    pgstat_create_transactional(
        PGSTAT_KIND_RELATION,
        if (*(*rel).rd_rel).relisshared {
            InvalidOid
        } else {
            MyDatabaseId
        },
        RelationGetRelid(rel),
    );
}

/// Ensure that stats are dropped if transaction commits (pgstat_relation.c:
/// pgstat_drop_relation).
pub unsafe fn pgstat_drop_relation(rel: Relation) {
    let nest_level: c_int = GetCurrentTransactionNestLevel();
    let pgstat_info: *mut PgStat_TableStatus;

    pgstat_drop_transactional(
        PGSTAT_KIND_RELATION,
        if (*(*rel).rd_rel).relisshared {
            InvalidOid
        } else {
            MyDatabaseId
        },
        RelationGetRelid(rel),
    );

    if !pgstat_should_count_relation(rel) {
        return;
    }

    /*
     * Transactionally set counters to 0. That ensures that accesses to
     * pg_stat_xact_all_tables inside the transaction show 0.
     */
    pgstat_info = rel_pgstat_info(rel);
    if !tabstat_trans(pgstat_info).is_null()
        && (*tabstat_trans(pgstat_info)).nest_level == nest_level
    {
        save_truncdrop_counters(tabstat_trans(pgstat_info), true);
        (*tabstat_trans(pgstat_info)).tuples_inserted = 0;
        (*tabstat_trans(pgstat_info)).tuples_updated = 0;
        (*tabstat_trans(pgstat_info)).tuples_deleted = 0;
    }
}

// ---------------------------------------------------------------------------
// Vacuum / analyze reporting (REAL)
// ---------------------------------------------------------------------------

/// Report that the table was just vacuumed and flush IO statistics
/// (pgstat_relation.c: pgstat_report_vacuum).
pub unsafe fn pgstat_report_vacuum(
    tableoid: Oid,
    shared: bool,
    livetuples: PgStat_Counter,
    deadtuples: PgStat_Counter,
    starttime: TimestampTz,
) {
    let entry_ref: *mut PgStat_EntryRef;
    let shtabentry: *mut PgStatShared_Relation;
    let tabentry: *mut PgStat_StatTabEntry;
    let dboid: Oid = if shared { InvalidOid } else { MyDatabaseId };
    let ts: TimestampTz;
    let elapsedtime: PgStat_Counter;

    if !pgstat_track_counts {
        return;
    }

    /* Store the data in the table's hash table entry. */
    ts = GetCurrentTimestamp();
    elapsedtime = TimestampDifferenceMilliseconds(starttime, ts);

    /* block acquiring lock for the same reason as pgstat_report_autovac() */
    entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, dboid, tableoid, false);

    shtabentry = (*entry_ref).shared_stats as *mut PgStatShared_Relation;
    tabentry = &mut (*shtabentry).stats;

    (*tabentry).live_tuples = livetuples;
    (*tabentry).dead_tuples = deadtuples;

    /*
     * It is quite possible that a non-aggressive VACUUM ended up skipping
     * various pages, however, we'll zero the insert counter here regardless.
     * It's currently used only to track when we need to perform an "insert"
     * autovacuum, which are mainly intended to freeze newly inserted tuples.
     * Zeroing this may just mean we'll not try to vacuum the table again until
     * enough tuples have been inserted to trigger another insert autovacuum.
     * An anti-wraparound autovacuum will catch any persistent stragglers.
     */
    (*tabentry).ins_since_vacuum = 0;

    if AmAutoVacuumWorkerProcess() {
        (*tabentry).last_autovacuum_time = ts;
        (*tabentry).autovacuum_count += 1;
        (*tabentry).total_autovacuum_time += elapsedtime;
    } else {
        (*tabentry).last_vacuum_time = ts;
        (*tabentry).vacuum_count += 1;
        (*tabentry).total_vacuum_time += elapsedtime;
    }

    pgstat_unlock_entry(entry_ref);

    /*
     * Flush IO statistics now. pgstat_report_stat() will flush IO stats,
     * however this will not be called until after an entire autovacuum cycle is
     * done -- which will likely vacuum many relations -- or until the VACUUM
     * command has processed all tables and committed.
     */
    pgstat_flush_io(false);
    let _ = pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO);
}

/// Report that the table was just analyzed and flush IO statistics
/// (pgstat_relation.c: pgstat_report_analyze).
///
/// Caller must provide new live- and dead-tuples estimates, as well as a flag
/// indicating whether to reset the mod_since_analyze counter.
pub unsafe fn pgstat_report_analyze(
    rel: Relation,
    mut livetuples: PgStat_Counter,
    mut deadtuples: PgStat_Counter,
    resetcounter: bool,
    starttime: TimestampTz,
) {
    let entry_ref: *mut PgStat_EntryRef;
    let shtabentry: *mut PgStatShared_Relation;
    let tabentry: *mut PgStat_StatTabEntry;
    let dboid: Oid = if (*(*rel).rd_rel).relisshared {
        InvalidOid
    } else {
        MyDatabaseId
    };
    let ts: TimestampTz;
    let elapsedtime: PgStat_Counter;

    if !pgstat_track_counts {
        return;
    }

    /*
     * Unlike VACUUM, ANALYZE might be running inside a transaction that has
     * already inserted and/or deleted rows in the target table. ANALYZE will
     * have counted such rows as live or dead respectively. Because we will
     * report our counts of such rows at transaction end, we should subtract off
     * these counts from the update we're making now, else they'll be
     * double-counted after commit.  (This approach also ensures that the shared
     * stats entry ends up with the right numbers if we abort instead of
     * committing.)
     *
     * Waste no time on partitioned tables, though.
     */
    if pgstat_should_count_relation(rel) && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE {
        let mut trans: *mut PgStat_TableXactStatus;

        trans = tabstat_trans(rel_pgstat_info(rel));
        while !trans.is_null() {
            livetuples -= (*trans).tuples_inserted - (*trans).tuples_deleted;
            deadtuples -= (*trans).tuples_updated + (*trans).tuples_deleted;
            trans = (*trans).upper;
        }
        /* count stuff inserted by already-aborted subxacts, too */
        deadtuples -= (*rel_pgstat_info(rel)).counts.delta_dead_tuples;
        /* Since ANALYZE's counts are estimates, we could have underflowed */
        livetuples = Max(livetuples, 0);
        deadtuples = Max(deadtuples, 0);
    }

    /* Store the data in the table's hash table entry. */
    ts = GetCurrentTimestamp();
    elapsedtime = TimestampDifferenceMilliseconds(starttime, ts);

    /* block acquiring lock for the same reason as pgstat_report_autovac() */
    entry_ref =
        pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, dboid, RelationGetRelid(rel), false);
    /* can't get dropped while accessed */
    Assert!(!entry_ref.is_null() && !(*entry_ref).shared_stats.is_null());

    shtabentry = (*entry_ref).shared_stats as *mut PgStatShared_Relation;
    tabentry = &mut (*shtabentry).stats;

    (*tabentry).live_tuples = livetuples;
    (*tabentry).dead_tuples = deadtuples;

    /*
     * If commanded, reset mod_since_analyze to zero.  This forgets any changes
     * that were committed while the ANALYZE was in progress, but we have no good
     * way to estimate how many of those there were.
     */
    if resetcounter {
        (*tabentry).mod_since_analyze = 0;
    }

    if AmAutoVacuumWorkerProcess() {
        (*tabentry).last_autoanalyze_time = ts;
        (*tabentry).autoanalyze_count += 1;
        (*tabentry).total_autoanalyze_time += elapsedtime;
    } else {
        (*tabentry).last_analyze_time = ts;
        (*tabentry).analyze_count += 1;
        (*tabentry).total_analyze_time += elapsedtime;
    }

    pgstat_unlock_entry(entry_ref);

    /* see pgstat_report_vacuum() */
    pgstat_flush_io(false);
    let _ = pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO);
}

// ---------------------------------------------------------------------------
// Nontransactional direct count functions (pgstat.h macros)
// ---------------------------------------------------------------------------
//
// These mirror the inline pgstat.h macros (not pgstat_relation.c). Each bumps a
// field of the pending `PgStat_TableCounts` directly.

/// pgstat.h: pgstat_count_heap_scan -- one sequential scan started.
pub unsafe fn pgstat_count_heap_scan(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.numscans += 1;
    }
}

/// pgstat.h: pgstat_count_heap_getnext -- one tuple returned by heap_getnext.
pub unsafe fn pgstat_count_heap_getnext(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.tuples_returned += 1;
    }
}

/// pgstat.h: pgstat_count_heap_fetch -- one tuple fetched by heap_fetch.
pub unsafe fn pgstat_count_heap_fetch(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.tuples_fetched += 1;
    }
}

/// pgstat.h: pgstat_count_index_scan -- one index scan started.
pub unsafe fn pgstat_count_index_scan(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.numscans += 1;
    }
}

/// pgstat.h: pgstat_count_index_tuples -- n index entries returned.
pub unsafe fn pgstat_count_index_tuples(rel: Relation, n: PgStat_Counter) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.tuples_returned += n;
    }
}

/// pgstat.h: pgstat_count_buffer_read -- one buffer read (fetched).
pub unsafe fn pgstat_count_buffer_read(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.blocks_fetched += 1;
    }
}

/// pgstat.h: pgstat_count_buffer_hit -- one buffer hit.
pub unsafe fn pgstat_count_buffer_hit(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.blocks_hit += 1;
    }
}

// ---------------------------------------------------------------------------
// Transactional count functions (REAL)
// ---------------------------------------------------------------------------

/// count a tuple insertion of n tuples (pgstat_relation.c:
/// pgstat_count_heap_insert).
pub unsafe fn pgstat_count_heap_insert(rel: Relation, n: PgStat_Counter) {
    if pgstat_should_count_relation(rel) {
        let pgstat_info: *mut PgStat_TableStatus = rel_pgstat_info(rel);

        ensure_tabstat_xact_level(pgstat_info);
        (*tabstat_trans(pgstat_info)).tuples_inserted += n;
    }
}

/// count a tuple update (pgstat_relation.c: pgstat_count_heap_update).
pub unsafe fn pgstat_count_heap_update(rel: Relation, hot: bool, newpage: bool) {
    Assert!(!(hot && newpage));

    if pgstat_should_count_relation(rel) {
        let pgstat_info: *mut PgStat_TableStatus = rel_pgstat_info(rel);

        ensure_tabstat_xact_level(pgstat_info);
        (*tabstat_trans(pgstat_info)).tuples_updated += 1;

        /*
         * tuples_hot_updated and tuples_newpage_updated counters are
         * nontransactional, so just advance them
         */
        if hot {
            (*pgstat_info).counts.tuples_hot_updated += 1;
        } else if newpage {
            (*pgstat_info).counts.tuples_newpage_updated += 1;
        }
    }
}

/// count a tuple deletion (pgstat_relation.c: pgstat_count_heap_delete).
pub unsafe fn pgstat_count_heap_delete(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        let pgstat_info: *mut PgStat_TableStatus = rel_pgstat_info(rel);

        ensure_tabstat_xact_level(pgstat_info);
        (*tabstat_trans(pgstat_info)).tuples_deleted += 1;
    }
}

/// update tuple counters due to truncate (pgstat_relation.c:
/// pgstat_count_truncate).
pub unsafe fn pgstat_count_truncate(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        let pgstat_info: *mut PgStat_TableStatus = rel_pgstat_info(rel);

        ensure_tabstat_xact_level(pgstat_info);
        save_truncdrop_counters(tabstat_trans(pgstat_info), false);
        (*tabstat_trans(pgstat_info)).tuples_inserted = 0;
        (*tabstat_trans(pgstat_info)).tuples_updated = 0;
        (*tabstat_trans(pgstat_info)).tuples_deleted = 0;
    }
}

/// update dead-tuples count (pgstat_relation.c: pgstat_update_heap_dead_tuples).
///
/// The semantics of this are that we are reporting the nontransactional recovery
/// of "delta" dead tuples; so delta_dead_tuples decreases rather than
/// increasing, and the change goes straight into the per-table counter, not into
/// transactional state.
pub unsafe fn pgstat_update_heap_dead_tuples(rel: Relation, delta: c_int) {
    if pgstat_should_count_relation(rel) {
        let pgstat_info: *mut PgStat_TableStatus = rel_pgstat_info(rel);

        (*pgstat_info).counts.delta_dead_tuples -= delta as PgStat_Counter;
    }
}

// ---------------------------------------------------------------------------
// Fetch helpers (REAL)
// ---------------------------------------------------------------------------

/// Support function for the SQL-callable pgstat* functions
/// (pgstat_relation.c: pgstat_fetch_stat_tabentry). Returns the collected
/// statistics for one table or null.
pub unsafe fn pgstat_fetch_stat_tabentry(relid: Oid) -> *mut PgStat_StatTabEntry {
    pgstat_fetch_stat_tabentry_ext(IsSharedRelation(relid), relid)
}

/// More efficient version of pgstat_fetch_stat_tabentry(), specifying whether
/// the table is a shared relation (pgstat_relation.c:
/// pgstat_fetch_stat_tabentry_ext).
///
/// HEADER-OFFSET RULE: `pgstat_fetch_entry` returns the `PgStatShared_Relation`
/// blob; the `PgStat_StatTabEntry` lives in its `.stats` member.
pub unsafe fn pgstat_fetch_stat_tabentry_ext(shared: bool, reloid: Oid) -> *mut PgStat_StatTabEntry {
    let dboid = if shared { InvalidOid } else { MyDatabaseId };

    let sh = pgstat_fetch_entry(PGSTAT_KIND_RELATION, dboid, reloid) as *mut PgStatShared_Relation;
    if sh.is_null() {
        null_mut()
    } else {
        &mut (*sh).stats as *mut PgStat_StatTabEntry
    }
}

/// find any existing PgStat_TableStatus entry for rel (pgstat_relation.c:
/// find_tabstat_entry).
///
/// Find any existing PgStat_TableStatus entry for rel_id in the current
/// database. If not found, try finding from shared tables.
///
/// If an entry is found, copy it and increment the copy's counters with their
/// subtransaction counterparts, then return the copy.  The caller may need to
/// pfree() the copy.
///
/// If no entry found, return NULL, don't create a new one.
pub unsafe fn find_tabstat_entry(rel_id: Oid) -> *mut PgStat_TableStatus {
    let mut entry_ref: *mut PgStat_EntryRef;
    let mut trans: *mut PgStat_TableXactStatus;
    let tabentry: *mut PgStat_TableStatus;
    let tablestatus: *mut PgStat_TableStatus;

    entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, MyDatabaseId, rel_id);
    if entry_ref.is_null() {
        entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, InvalidOid, rel_id);
        if entry_ref.is_null() {
            return null_mut();
        }
    }

    tabentry = (*entry_ref).pending as *mut PgStat_TableStatus;
    tablestatus = palloc(size_of::<PgStat_TableStatus>()) as *mut PgStat_TableStatus;
    *tablestatus = core::ptr::read(tabentry);

    /*
     * Reset tablestatus->trans in the copy of PgStat_TableStatus as it may
     * point to a shared memory area.  Its data is saved below, so removing it
     * does not matter.
     */
    (*tablestatus).trans = null_mut();

    /*
     * Live subtransaction counts are not included yet.  This is not a hot code
     * path so reconcile tuples_inserted, tuples_updated and tuples_deleted even
     * if the caller may not be interested in this data.
     */
    trans = tabstat_trans(tabentry);
    while !trans.is_null() {
        (*tablestatus).counts.tuples_inserted += (*trans).tuples_inserted;
        (*tablestatus).counts.tuples_updated += (*trans).tuples_updated;
        (*tablestatus).counts.tuples_deleted += (*trans).tuples_deleted;
        trans = (*trans).upper;
    }

    tablestatus
}

// ---------------------------------------------------------------------------
// Flush callback (REAL)
// ---------------------------------------------------------------------------

/// Flush out pending stats for the entry (pgstat_relation.c:
/// pgstat_relation_flush_cb).
///
/// If nowait is true and the lock could not be immediately acquired, returns
/// false without flushing the entry.  Otherwise returns true.
///
/// Some of the stats are copied to the corresponding pending database stats
/// entry when successfully flushing.
///
/// DEVIATION: upstream reads `dboid = entry_ref->shared_entry->key.dboid`. The
/// `pgstat.rs` `PgStat_EntryRef` subset does not carry `shared_entry`; we derive
/// the dboid from `lstats->shared` (semantically equivalent under this port's
/// non-shared assumption). `pgstat_prep_database_pending` is a local stub that
/// returns null until database pending wiring lands, so the database
/// propagation is guarded.
pub unsafe fn pgstat_relation_flush_cb(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    let dboid: Oid;
    let lstats: *mut PgStat_TableStatus; /* pending stats entry  */
    let shtabstats: *mut PgStatShared_Relation;
    let tabentry: *mut PgStat_StatTabEntry; /* table entry of shared stats */
    let dbentry: *mut PgStat_StatDBEntry; /* pending database entry */

    let _ = pgstat_assert_is_up();

    lstats = (*entry_ref).pending as *mut PgStat_TableStatus;
    shtabstats = (*entry_ref).shared_stats as *mut PgStatShared_Relation;
    dboid = if (*lstats).shared {
        InvalidOid
    } else {
        MyDatabaseId
    };

    /*
     * Ignore entries that didn't accumulate any actual counts, such as indexes
     * that were opened by the planner but not used.
     */
    if pg_memory_is_all_zeros(
        &(*lstats).counts as *const PgStat_TableCounts as *const c_void,
        size_of::<PgStat_TableCounts>(),
    ) {
        return true;
    }

    if !pgstat_lock_entry(entry_ref, nowait) {
        return false;
    }

    /* add the values to the shared entry. */
    tabentry = &mut (*shtabstats).stats;

    (*tabentry).numscans += (*lstats).counts.numscans;
    if (*lstats).counts.numscans != 0 {
        let t: TimestampTz = GetCurrentTransactionStopTimestamp();

        if t > (*tabentry).lastscan {
            (*tabentry).lastscan = t;
        }
    }
    (*tabentry).tuples_returned += (*lstats).counts.tuples_returned;
    (*tabentry).tuples_fetched += (*lstats).counts.tuples_fetched;
    (*tabentry).tuples_inserted += (*lstats).counts.tuples_inserted;
    (*tabentry).tuples_updated += (*lstats).counts.tuples_updated;
    (*tabentry).tuples_deleted += (*lstats).counts.tuples_deleted;
    (*tabentry).tuples_hot_updated += (*lstats).counts.tuples_hot_updated;
    (*tabentry).tuples_newpage_updated += (*lstats).counts.tuples_newpage_updated;

    /*
     * If table was truncated/dropped, first reset the live/dead counters.
     */
    if (*lstats).counts.truncdropped {
        (*tabentry).live_tuples = 0;
        (*tabentry).dead_tuples = 0;
        (*tabentry).ins_since_vacuum = 0;
    }

    (*tabentry).live_tuples += (*lstats).counts.delta_live_tuples;
    (*tabentry).dead_tuples += (*lstats).counts.delta_dead_tuples;
    (*tabentry).mod_since_analyze += (*lstats).counts.changed_tuples;

    /*
     * Using tuples_inserted to update ins_since_vacuum does mean that we'll
     * track aborted inserts too.  This isn't ideal, but otherwise probably not
     * worth adding an extra field for.  It may just amount to autovacuums
     * triggering for inserts more often than they maybe should, which is
     * probably not going to be common enough to be too concerned about here.
     */
    (*tabentry).ins_since_vacuum += (*lstats).counts.tuples_inserted;

    (*tabentry).blocks_fetched += (*lstats).counts.blocks_fetched;
    (*tabentry).blocks_hit += (*lstats).counts.blocks_hit;

    /* Clamp live_tuples in case of negative delta_live_tuples */
    (*tabentry).live_tuples = Max((*tabentry).live_tuples, 0);
    /* Likewise for dead_tuples */
    (*tabentry).dead_tuples = Max((*tabentry).dead_tuples, 0);

    pgstat_unlock_entry(entry_ref);

    /* The entry was successfully flushed, add the same to database stats */
    dbentry = pgstat_prep_database_pending(dboid);
    if !dbentry.is_null() {
        (*dbentry).tuples_returned += (*lstats).counts.tuples_returned;
        (*dbentry).tuples_fetched += (*lstats).counts.tuples_fetched;
        (*dbentry).tuples_inserted += (*lstats).counts.tuples_inserted;
        (*dbentry).tuples_updated += (*lstats).counts.tuples_updated;
        (*dbentry).tuples_deleted += (*lstats).counts.tuples_deleted;
        (*dbentry).blocks_fetched += (*lstats).counts.blocks_fetched;
        (*dbentry).blocks_hit += (*lstats).counts.blocks_hit;
    }

    true
}

/// Pending-entry delete callback (pgstat_relation.c:
/// pgstat_relation_delete_pending_cb). Unlinks the owning relcache entry, if any.
pub unsafe fn pgstat_relation_delete_pending_cb(entry_ref: *mut PgStat_EntryRef) {
    let pending = (*entry_ref).pending as *mut PgStat_TableStatus;

    if !(*pending).relation.is_null() {
        pgstat_unlink_relation((*pending).relation);
    }
}

// ---------------------------------------------------------------------------
// Pending-entry prep (REAL)
// ---------------------------------------------------------------------------

/// Find or create a PgStat_TableStatus entry for rel, initialized if not present
/// (pgstat_relation.c: pgstat_prep_relation_pending).
unsafe fn pgstat_prep_relation_pending(rel_id: Oid, isshared: bool) -> *mut PgStat_TableStatus {
    let dboid = if isshared { InvalidOid } else { MyDatabaseId };

    let entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_RELATION, dboid, rel_id, null_mut());
    let pending = (*entry_ref).pending as *mut PgStat_TableStatus;
    (*pending).id = rel_id;
    (*pending).shared = isshared;

    pending
}

// ---------------------------------------------------------------------------
// Transaction hooks (REAL)
// ---------------------------------------------------------------------------

/// Read the typed `first` head off a `PgStat_SubXactStatus` (whose field is
/// typed against the `c_void`-aliased `PgStat_TableXactStatus` in
/// pgstat_internal).
#[inline]
unsafe fn xact_first(xact_state: *mut PgStat_SubXactStatus) -> *mut PgStat_TableXactStatus {
    (*xact_state).first as *mut PgStat_TableXactStatus
}

/// Perform relation stats specific end-of-transaction work. Helper for
/// AtEOXact_PgStat (pgstat_relation.c: AtEOXact_PgStat_Relations).
///
/// Transfer transactional insert/update counts into the base tabstat entries.
/// We don't bother to free any of the transactional state, since it's all in
/// TopTransactionContext and will go away anyway.
pub unsafe fn AtEOXact_PgStat_Relations(xact_state: *mut PgStat_SubXactStatus, is_commit: bool) {
    let mut trans: *mut PgStat_TableXactStatus;

    trans = xact_first(xact_state);
    while !trans.is_null() {
        let tabstat: *mut PgStat_TableStatus;

        Assert!((*trans).nest_level == 1);
        Assert!((*trans).upper.is_null());
        tabstat = (*trans).parent;
        Assert!(tabstat_trans(tabstat) == trans);
        /* restore pre-truncate/drop stats (if any) in case of aborted xact */
        if !is_commit {
            restore_truncdrop_counters(trans);
        }
        /* count attempted actions regardless of commit/abort */
        (*tabstat).counts.tuples_inserted += (*trans).tuples_inserted;
        (*tabstat).counts.tuples_updated += (*trans).tuples_updated;
        (*tabstat).counts.tuples_deleted += (*trans).tuples_deleted;
        if is_commit {
            (*tabstat).counts.truncdropped = (*trans).truncdropped;
            if (*trans).truncdropped {
                /* forget live/dead stats seen by backend thus far */
                (*tabstat).counts.delta_live_tuples = 0;
                (*tabstat).counts.delta_dead_tuples = 0;
            }
            /* insert adds a live tuple, delete removes one */
            (*tabstat).counts.delta_live_tuples +=
                (*trans).tuples_inserted - (*trans).tuples_deleted;
            /* update and delete each create a dead tuple */
            (*tabstat).counts.delta_dead_tuples +=
                (*trans).tuples_updated + (*trans).tuples_deleted;
            /* insert, update, delete each count as one change event */
            (*tabstat).counts.changed_tuples +=
                (*trans).tuples_inserted + (*trans).tuples_updated + (*trans).tuples_deleted;
        } else {
            /* inserted tuples are dead, deleted tuples are unaffected */
            (*tabstat).counts.delta_dead_tuples +=
                (*trans).tuples_inserted + (*trans).tuples_updated;
            /* an aborted xact generates no changed_tuple events */
        }
        (*tabstat).trans = null_mut();

        trans = (*trans).next;
    }
}

/// Perform relation stats specific end-of-sub-transaction work. Helper for
/// AtEOSubXact_PgStat (pgstat_relation.c: AtEOSubXact_PgStat_Relations).
///
/// Transfer transactional insert/update counts into the next higher
/// subtransaction state.
pub unsafe fn AtEOSubXact_PgStat_Relations(
    xact_state: *mut PgStat_SubXactStatus,
    is_commit: bool,
    nest_depth: c_int,
) {
    let mut trans: *mut PgStat_TableXactStatus;
    let mut next_trans: *mut PgStat_TableXactStatus;

    trans = xact_first(xact_state);
    while !trans.is_null() {
        let tabstat: *mut PgStat_TableStatus;

        next_trans = (*trans).next;
        Assert!((*trans).nest_level == nest_depth);
        tabstat = (*trans).parent;
        Assert!(tabstat_trans(tabstat) == trans);

        if is_commit {
            if !(*trans).upper.is_null() && (*(*trans).upper).nest_level == nest_depth - 1 {
                if (*trans).truncdropped {
                    /* propagate the truncate/drop status one level up */
                    save_truncdrop_counters((*trans).upper, false);
                    /* replace upper xact stats with ours */
                    (*(*trans).upper).tuples_inserted = (*trans).tuples_inserted;
                    (*(*trans).upper).tuples_updated = (*trans).tuples_updated;
                    (*(*trans).upper).tuples_deleted = (*trans).tuples_deleted;
                } else {
                    (*(*trans).upper).tuples_inserted += (*trans).tuples_inserted;
                    (*(*trans).upper).tuples_updated += (*trans).tuples_updated;
                    (*(*trans).upper).tuples_deleted += (*trans).tuples_deleted;
                }
                (*tabstat).trans = (*trans).upper as *mut c_void;
                pfree(trans as *mut c_void);
            } else {
                /*
                 * When there isn't an immediate parent state, we can just reuse
                 * the record instead of going through a palloc/pfree pushup
                 * (this works since it's all in TopTransactionContext anyway).
                 * We have to re-link it into the parent level, though, and that
                 * might mean pushing a new entry into the pgStatXactStack.
                 */
                let upper_xact_state: *mut PgStat_SubXactStatus;

                upper_xact_state = pgstat_get_xact_stack_level(nest_depth - 1);
                (*trans).next = (*upper_xact_state).first as *mut PgStat_TableXactStatus;
                (*upper_xact_state).first = trans as *mut crate::utils::activity::pgstat_internal::PgStat_TableXactStatus;
                (*trans).nest_level = nest_depth - 1;
            }
        } else {
            /*
             * On abort, update top-level tabstat counts, then forget the
             * subtransaction
             */

            /* first restore values obliterated by truncate/drop */
            restore_truncdrop_counters(trans);
            /* count attempted actions regardless of commit/abort */
            (*tabstat).counts.tuples_inserted += (*trans).tuples_inserted;
            (*tabstat).counts.tuples_updated += (*trans).tuples_updated;
            (*tabstat).counts.tuples_deleted += (*trans).tuples_deleted;
            /* inserted tuples are dead, deleted tuples are unaffected */
            (*tabstat).counts.delta_dead_tuples +=
                (*trans).tuples_inserted + (*trans).tuples_updated;
            (*tabstat).trans = (*trans).upper as *mut c_void;
            pfree(trans as *mut c_void);
        }

        trans = next_trans;
    }
}

/// Generate 2PC records for all the pending transaction-dependent relation
/// stats (pgstat_relation.c: AtPrepare_PgStat_Relations).
pub unsafe fn AtPrepare_PgStat_Relations(xact_state: *mut PgStat_SubXactStatus) {
    let mut trans: *mut PgStat_TableXactStatus;

    trans = xact_first(xact_state);
    while !trans.is_null() {
        let tabstat: *mut PgStat_TableStatus;
        let mut record: TwoPhasePgStatRecord = core::mem::zeroed();

        Assert!((*trans).nest_level == 1);
        Assert!((*trans).upper.is_null());
        tabstat = (*trans).parent;
        Assert!(tabstat_trans(tabstat) == trans);

        record.tuples_inserted = (*trans).tuples_inserted;
        record.tuples_updated = (*trans).tuples_updated;
        record.tuples_deleted = (*trans).tuples_deleted;
        record.inserted_pre_truncdrop = (*trans).inserted_pre_truncdrop;
        record.updated_pre_truncdrop = (*trans).updated_pre_truncdrop;
        record.deleted_pre_truncdrop = (*trans).deleted_pre_truncdrop;
        record.id = (*tabstat).id;
        record.shared = (*tabstat).shared;
        record.truncdropped = (*trans).truncdropped;

        RegisterTwoPhaseRecord(
            TWOPHASE_RM_PGSTAT_ID,
            0,
            &record as *const TwoPhasePgStatRecord as *const c_void,
            size_of::<TwoPhasePgStatRecord>() as u32,
        );

        trans = (*trans).next;
    }
}

/// All we need do here is unlink the transaction stats state from the
/// nontransactional state (pgstat_relation.c: PostPrepare_PgStat_Relations).
/// The nontransactional action counts will be reported to the stats system
/// immediately, while the effects on live and dead tuple counts are preserved in
/// the 2PC state file.
///
/// Note: AtEOXact_PgStat_Relations is not called during PREPARE.
pub unsafe fn PostPrepare_PgStat_Relations(xact_state: *mut PgStat_SubXactStatus) {
    let mut trans: *mut PgStat_TableXactStatus;

    trans = xact_first(xact_state);
    while !trans.is_null() {
        let tabstat: *mut PgStat_TableStatus;

        tabstat = (*trans).parent;
        (*tabstat).trans = null_mut();

        trans = (*trans).next;
    }
}

// ---------------------------------------------------------------------------
// 2PC processing routines (REAL)
// ---------------------------------------------------------------------------

/// 2PC processing routine for COMMIT PREPARED case (pgstat_relation.c:
/// pgstat_twophase_postcommit). Load the saved counts into our local pgstats
/// state.
pub unsafe fn pgstat_twophase_postcommit(
    _xid: TransactionId,
    _info: u16,
    recdata: *mut c_void,
    _len: u32,
) {
    let rec: *mut TwoPhasePgStatRecord = recdata as *mut TwoPhasePgStatRecord;
    let pgstat_info: *mut PgStat_TableStatus;

    /* Find or create a tabstat entry for the rel */
    pgstat_info = pgstat_prep_relation_pending((*rec).id, (*rec).shared);

    /* Same math as in AtEOXact_PgStat, commit case */
    (*pgstat_info).counts.tuples_inserted += (*rec).tuples_inserted;
    (*pgstat_info).counts.tuples_updated += (*rec).tuples_updated;
    (*pgstat_info).counts.tuples_deleted += (*rec).tuples_deleted;
    (*pgstat_info).counts.truncdropped = (*rec).truncdropped;
    if (*rec).truncdropped {
        /* forget live/dead stats seen by backend thus far */
        (*pgstat_info).counts.delta_live_tuples = 0;
        (*pgstat_info).counts.delta_dead_tuples = 0;
    }
    (*pgstat_info).counts.delta_live_tuples += (*rec).tuples_inserted - (*rec).tuples_deleted;
    (*pgstat_info).counts.delta_dead_tuples += (*rec).tuples_updated + (*rec).tuples_deleted;
    (*pgstat_info).counts.changed_tuples +=
        (*rec).tuples_inserted + (*rec).tuples_updated + (*rec).tuples_deleted;
}

/// 2PC processing routine for ROLLBACK PREPARED case (pgstat_relation.c:
/// pgstat_twophase_postabort). Load the saved counts into our local pgstats
/// state, but treat them as aborted.
pub unsafe fn pgstat_twophase_postabort(
    _xid: TransactionId,
    _info: u16,
    recdata: *mut c_void,
    _len: u32,
) {
    let rec: *mut TwoPhasePgStatRecord = recdata as *mut TwoPhasePgStatRecord;
    let pgstat_info: *mut PgStat_TableStatus;

    /* Find or create a tabstat entry for the rel */
    pgstat_info = pgstat_prep_relation_pending((*rec).id, (*rec).shared);

    /* Same math as in AtEOXact_PgStat, abort case */
    if (*rec).truncdropped {
        (*rec).tuples_inserted = (*rec).inserted_pre_truncdrop;
        (*rec).tuples_updated = (*rec).updated_pre_truncdrop;
        (*rec).tuples_deleted = (*rec).deleted_pre_truncdrop;
    }
    (*pgstat_info).counts.tuples_inserted += (*rec).tuples_inserted;
    (*pgstat_info).counts.tuples_updated += (*rec).tuples_updated;
    (*pgstat_info).counts.tuples_deleted += (*rec).tuples_deleted;
    (*pgstat_info).counts.delta_dead_tuples += (*rec).tuples_inserted + (*rec).tuples_updated;
}

// ---------------------------------------------------------------------------
// Static helpers (REAL)
// ---------------------------------------------------------------------------

/// add a new (sub)transaction state record (pgstat_relation.c:
/// add_tabstat_xact_level).
unsafe fn add_tabstat_xact_level(pgstat_info: *mut PgStat_TableStatus, nest_level: c_int) {
    let xact_state: *mut PgStat_SubXactStatus;
    let trans: *mut PgStat_TableXactStatus;

    /*
     * If this is the first rel to be modified at the current nest level, we
     * first have to push a transaction stack entry.
     */
    xact_state = pgstat_get_xact_stack_level(nest_level);

    /* Now make a per-table stack entry */
    trans = MemoryContextAllocZero(
        TopTransactionContext as crate::utils::palloc::MemoryContext,
        size_of::<PgStat_TableXactStatus>(),
    ) as *mut PgStat_TableXactStatus;
    (*trans).nest_level = nest_level;
    (*trans).upper = tabstat_trans(pgstat_info);
    (*trans).parent = pgstat_info;
    (*trans).next = (*xact_state).first as *mut PgStat_TableXactStatus;
    (*xact_state).first = trans as *mut crate::utils::activity::pgstat_internal::PgStat_TableXactStatus;
    (*pgstat_info).trans = trans as *mut c_void;
}

/// Add a new (sub)transaction record if needed (pgstat_relation.c:
/// ensure_tabstat_xact_level).
unsafe fn ensure_tabstat_xact_level(pgstat_info: *mut PgStat_TableStatus) {
    let nest_level: c_int = GetCurrentTransactionNestLevel();

    if tabstat_trans(pgstat_info).is_null()
        || (*tabstat_trans(pgstat_info)).nest_level != nest_level
    {
        add_tabstat_xact_level(pgstat_info, nest_level);
    }
}

/// Whenever a table is truncated/dropped, we save its i/u/d counters so that
/// they can be cleared, and if the (sub)xact that executed the truncate/drop
/// later aborts, the counters can be restored to the saved (pre-truncate/drop)
/// values (pgstat_relation.c: save_truncdrop_counters).
///
/// Note that for truncate we do this on the first truncate in any particular
/// subxact level only.
unsafe fn save_truncdrop_counters(trans: *mut PgStat_TableXactStatus, is_drop: bool) {
    if !(*trans).truncdropped || is_drop {
        (*trans).inserted_pre_truncdrop = (*trans).tuples_inserted;
        (*trans).updated_pre_truncdrop = (*trans).tuples_updated;
        (*trans).deleted_pre_truncdrop = (*trans).tuples_deleted;
        (*trans).truncdropped = true;
    }
}

/// restore counters when a truncate aborts (pgstat_relation.c:
/// restore_truncdrop_counters).
unsafe fn restore_truncdrop_counters(trans: *mut PgStat_TableXactStatus) {
    if (*trans).truncdropped {
        (*trans).tuples_inserted = (*trans).inserted_pre_truncdrop;
        (*trans).tuples_updated = (*trans).updated_pre_truncdrop;
        (*trans).tuples_deleted = (*trans).deleted_pre_truncdrop;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::activity::pgstat::pgstat_prep_pending_entry;

    // The process-local entry table in pgstat.rs is shared mutable global
    // state, so serialize tests that touch it.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn flush_accumulates_pending_into_shared() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let db: Oid = MyDatabaseId;
            let reloid: Oid = 7;

            // Prepare a pending+shared entry and bump pending counts directly.
            let eref = pgstat_prep_pending_entry(PGSTAT_KIND_RELATION, db, reloid, null_mut());
            assert!(!eref.is_null());

            let ts = (*eref).pending as *mut PgStat_TableStatus;
            (*ts).id = reloid;
            (*ts).counts.numscans += 4;
            (*ts).counts.tuples_inserted += 2;

            // Flush pending -> shared.
            assert!(pgstat_relation_flush_cb(eref, false));

            // Fetch the shared entry and assert the accumulated values.
            let shared = pgstat_fetch_stat_tabentry(reloid);
            assert!(!shared.is_null());
            assert_eq!((*shared).numscans, 4);
            assert_eq!((*shared).tuples_inserted, 2);

            // Pending counts must have been cleared by the flush.
            assert_eq!((*ts).counts.numscans, 0);
            assert_eq!((*ts).counts.tuples_inserted, 0);
        }
    }
}
