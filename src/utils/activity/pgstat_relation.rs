//! Implementation of relation (table/index) statistics.
//!
//! Faithful translation of the *reporter* half of `pgstat_relation.c`. It is
//! kept separate from `pgstat.rs` to enforce the line between the statistics
//! access/storage implementation and the details of individual statistics types.
//!
//! Deviations from upstream PostgreSQL 18.3 (each noted again inline):
//!
//! * TRANSACTION-ROLLBACK MACHINERY STUBBED. Upstream tracks per-(sub)xact
//!   insert/update/delete/truncate counts in a `PgStat_TableXactStatus`
//!   linked-list hung off `PgStat_TableStatus.trans`, propagating them up to the
//!   base `counts` at (sub)commit/abort via AtEOXact/AtEOSubXact/AtPrepare/
//!   PostPrepare. The xact.h nesting subsystem is NOT ported, so `trans` is
//!   always null here and the transactional count functions
//!   (pgstat_count_heap_insert/update/delete, pgstat_count_truncate,
//!   pgstat_update_heap_dead_tuples) write DIRECTLY into `counts`. This is a
//!   SIMPLIFICATION: it loses rollback-correctness (aborted-xact tuple deltas
//!   are not unwound, truncate/drop pre-counts are not saved/restored) and the
//!   live/dead-tuple delta bookkeeping that AtEOXact would derive is collapsed
//!   into the straightforward "insert -> +1 live, delete -> +1 dead, update ->
//!   +1 dead" accounting done inline. The transaction hooks themselves
//!   (AtEOXact_PgStat_Relations / AtEOSubXact_PgStat_Relations /
//!   AtPrepare_PgStat_Relations / PostPrepare_PgStat_Relations,
//!   pgstat_twophase_*, pgstat_drop_relation, add_tabstat_xact_level,
//!   ensure_tabstat_xact_level, save/restore_truncdrop_counters) are reduced to
//!   no-ops / minimal stubs with TODOs.
//!
//! * SHARED-MEMORY ENTRY ACCESS via the process-local entry table from
//!   `pgstat.rs`: `pgstat_get_entry_ref_locked`/`pgstat_fetch_pending_entry`/
//!   `pgstat_prep_database_pending` and the `entry_ref->shared_entry->key`
//!   accessor upstream uses do not exist here. We reach the dboid via the
//!   reporter's own argument and reach the shared blob through
//!   `pgstat_fetch_entry`/the eref's `shared_stats`/`pending` pointers directly.
//!
//! * `pgstat_fetch_stat_tabentry` applies the HEADER-OFFSET RULE: the shared
//!   blob is a `PgStatShared_Relation`, so the `PgStat_StatTabEntry` lives at
//!   `&(*sh).stats`, not at the blob base.
//!
//! * MyDatabaseId / pgstat_track_counts / GetCurrentTransactionNestLevel /
//!   GetCurrentTransactionStopTimestamp / IsSharedRelation / pgstat_assert_is_up
//!   are stubs (miscadmin.h / xact.h / catalog.h unported).
//!
//! * pgstat_report_vacuum/analyze, pgstat_copy_relation_stats and the 2PC
//!   routines are out of this port's reporter scope and omitted.
//!
//! IDENTIFICATION
//!   src/backend/utils/activity/pgstat_relation.c

use crate::prelude::*;

use crate::nodes::execnodes::Relation;
use crate::utils::rel::RelationGetRelid;

use crate::utils::activity::pgstat::{
    pgstat_create_transactional, pgstat_drop_transactional, pgstat_fetch_entry,
    pgstat_get_entry_ref, pgstat_lock_entry, pgstat_prep_pending_entry, pgstat_unlock_entry,
    PgStatShared_Relation, PgStat_Counter, PgStat_EntryRef, PgStat_StatTabEntry,
    PgStat_TableCounts, PgStat_TableStatus, PGSTAT_KIND_RELATION,
};

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
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

// ---------------------------------------------------------------------------
// Relation init / association / unlink (REAL)
// ---------------------------------------------------------------------------

/// Initialize a relcache entry to count access statistics. Called whenever a
/// relation is opened (pgstat_relation.c: pgstat_init_relation).
///
/// DEVIATION: upstream gates on `RELKIND_HAS_STORAGE(relkind) || relkind ==
/// RELKIND_PARTITIONED_TABLE` using `rel->rd_rel->relkind`. The relkind macros
/// are part of catalog/pg_class.h; rather than depend on them here, we keep the
/// `pgstat_track_counts` gate (the load-bearing one for the reporter half) and
/// the unlink-on-disabled behavior, and otherwise enable counting. The relkind
/// filter is a TODO.
pub unsafe fn pgstat_init_relation(rel: Relation) {
    // TODO: filter on relkind (RELKIND_HAS_STORAGE || PARTITIONED_TABLE) once
    // the pg_class relkind macros are wired up.

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
    // DEVIATION: upstream reads `rel->rd_rel->relisshared`; with shared-catalog
    // metadata unported we treat every relation as non-shared.
    let pending = pgstat_prep_relation_pending(RelationGetRelid(rel), false);
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
///
/// DEVIATION: dboid is MyDatabaseId (shared-catalog detection unported).
pub unsafe fn pgstat_create_relation(rel: Relation) {
    pgstat_create_transactional(PGSTAT_KIND_RELATION, MyDatabaseId, RelationGetRelid(rel));
}

/// Ensure that stats are dropped if transaction commits (pgstat_relation.c:
/// pgstat_drop_relation).
///
/// STUB: the `pgstat_info->trans` / nest-level branch that transactionally
/// zeroes the i/u/d counters is omitted (xact machinery unported). We retain
/// the `pgstat_drop_transactional` call so the entry is still scheduled for
/// drop. TODO: restore the trans-aware zeroing once `trans` is populated.
pub unsafe fn pgstat_drop_relation(rel: Relation) {
    let _nest_level = GetCurrentTransactionNestLevel();

    pgstat_drop_transactional(PGSTAT_KIND_RELATION, MyDatabaseId, RelationGetRelid(rel));

    if !pgstat_should_count_relation(rel) {
        return;
    }

    // TODO (xact machinery unported): upstream here, when
    // pgstat_info->trans->nest_level == nest_level, calls
    // save_truncdrop_counters(trans, true) and zeroes the trans i/u/d counts.
    // `trans` is always null in this port, so there is nothing to do.
}

// ---------------------------------------------------------------------------
// Nontransactional direct count functions (REAL)
// ---------------------------------------------------------------------------
//
// These mirror the inline pgstat.h macros. Each bumps a field of the pending
// `PgStat_TableCounts` directly. Translated as functions (Rust has no macro-
// with-statement idiom matching the C do/while(0) form well here).

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
// Transactional count functions (STUBBED -> write directly to counts)
// ---------------------------------------------------------------------------
//
// SIMPLIFICATION: upstream routes these through ensure_tabstat_xact_level() and
// accumulates into pgstat_info->trans (the per-subxact record) so they can be
// unwound on rollback. With the xact machinery unported, `trans` is always
// null; we therefore write straight into the base `counts`, deriving the
// live/dead-tuple deltas inline as AtEOXact's commit path would. This loses
// rollback-correctness (see file header).

/// count a tuple insertion of n tuples (pgstat_relation.c:
/// pgstat_count_heap_insert). STUB: writes counts directly; also advances
/// delta_live_tuples and changed_tuples as the commit path would.
pub unsafe fn pgstat_count_heap_insert(rel: Relation, n: PgStat_Counter) {
    if pgstat_should_count_relation(rel) {
        let counts = &mut (*rel_pgstat_info(rel)).counts;
        counts.tuples_inserted += n;
        // commit-path derivation (AtEOXact_PgStat_Relations):
        counts.delta_live_tuples += n;
        counts.changed_tuples += n;
    }
}

/// count a tuple update (pgstat_relation.c: pgstat_count_heap_update). STUB:
/// writes counts directly. tuples_hot_updated / tuples_newpage_updated are
/// nontransactional upstream too and were always bumped directly.
pub unsafe fn pgstat_count_heap_update(rel: Relation, hot: bool, newpage: bool) {
    Assert!(!(hot && newpage));

    if pgstat_should_count_relation(rel) {
        let counts = &mut (*rel_pgstat_info(rel)).counts;
        counts.tuples_updated += 1;

        /*
         * tuples_hot_updated and tuples_newpage_updated counters are
         * nontransactional, so just advance them
         */
        if hot {
            counts.tuples_hot_updated += 1;
        } else if newpage {
            counts.tuples_newpage_updated += 1;
        }

        // commit-path derivation: update creates one dead tuple, one change.
        counts.delta_dead_tuples += 1;
        counts.changed_tuples += 1;
    }
}

/// count a tuple deletion (pgstat_relation.c: pgstat_count_heap_delete). STUB:
/// writes counts directly; delete removes a live and creates a dead tuple.
pub unsafe fn pgstat_count_heap_delete(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        let counts = &mut (*rel_pgstat_info(rel)).counts;
        counts.tuples_deleted += 1;
        // commit-path derivation:
        counts.delta_live_tuples -= 1;
        counts.delta_dead_tuples += 1;
        counts.changed_tuples += 1;
    }
}

/// update tuple counters due to truncate (pgstat_relation.c:
/// pgstat_count_truncate). STUB: sets the truncdropped flag and zeroes the
/// tuple-action counters directly (no save_truncdrop_counters / restore-on-abort
/// path, which lives in the unported xact machinery).
pub unsafe fn pgstat_count_truncate(rel: Relation) {
    if pgstat_should_count_relation(rel) {
        let counts = &mut (*rel_pgstat_info(rel)).counts;
        counts.truncdropped = true;
        counts.tuples_inserted = 0;
        counts.tuples_updated = 0;
        counts.tuples_deleted = 0;
    }
}

/// update dead-tuples count (pgstat_relation.c: pgstat_update_heap_dead_tuples).
/// This was already nontransactional upstream: it reports the nontransactional
/// recovery of `delta` dead tuples straight into the per-table counter.
pub unsafe fn pgstat_update_heap_dead_tuples(rel: Relation, delta: c_int) {
    if pgstat_should_count_relation(rel) {
        (*rel_pgstat_info(rel)).counts.delta_dead_tuples -= delta as PgStat_Counter;
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
/// find_tabstat_entry). Tries the current database first, then shared tables.
/// Returns the pending entry pointer, or null if none. If no entry is found,
/// does not create one.
///
/// DEVIATION: upstream copies the entry into a freshly palloc'd
/// PgStat_TableStatus, then folds the live subtransaction counts
/// (pgstat_info->trans chain) into the copy before returning. With `trans`
/// always null and no per-call palloc copy needed for the reporter half, we
/// return the live pending pointer directly; the subxact reconciliation loop is
/// a no-op (nothing to add).
pub unsafe fn find_tabstat_entry(rel_id: Oid) -> *mut PgStat_TableStatus {
    let mut eref = pgstat_get_entry_ref(PGSTAT_KIND_RELATION, MyDatabaseId, rel_id, false, null_mut());
    if eref.is_null() {
        eref = pgstat_get_entry_ref(PGSTAT_KIND_RELATION, InvalidOid, rel_id, false, null_mut());
        if eref.is_null() {
            return null_mut();
        }
    }

    // TODO: upstream reconciles tablestatus->counts.tuples_{inserted,updated,
    // deleted} with the live `trans` chain here; `trans` is unported (null).
    (*eref).pending as *mut PgStat_TableStatus
}

// ---------------------------------------------------------------------------
// Flush callback (REAL)
// ---------------------------------------------------------------------------

/// Flush out pending stats for the entry (pgstat_relation.c:
/// pgstat_relation_flush_cb). Accumulates every pending `PgStat_TableCounts`
/// field into the shared `PgStat_StatTabEntry`, then clears the pending counts.
///
/// Returns true on success (or when there is nothing to flush). If `nowait` is
/// true and the lock could not be acquired, returns false without flushing.
///
/// DEVIATIONS:
/// * upstream reads `dboid = entry_ref->shared_entry->key.dboid` and, after the
///   shared update, copies the same counts into the pending database entry via
///   `pgstat_prep_database_pending(dboid)`. Neither the `shared_entry`/key
///   accessor nor a wired-up database pending entry exist in this subset, so
///   the database-stats propagation is omitted (TODO).
/// * `GetCurrentTransactionStopTimestamp()` (lastscan update) is part of the
///   unported xact machinery; the `lastscan` bump is omitted (TODO).
/// * after a successful flush, upstream relies on a separate reset of the
///   pending counts by the caller path; we memset `lstats->counts` to zero here
///   so repeated flushes do not double-count (matching the net effect).
pub unsafe fn pgstat_relation_flush_cb(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    pgstat_assert_is_up();

    let lstats = (*entry_ref).pending as *mut PgStat_TableStatus;
    let shtabstats = (*entry_ref).shared_stats as *mut PgStatShared_Relation;

    /*
     * Ignore entries that didn't accumulate any actual counts, such as indexes
     * that were opened by the planner but not used. We test the whole
     * PgStat_TableCounts for all-zeros.
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
    let tabentry: *mut PgStat_StatTabEntry = &mut (*shtabstats).stats;
    let counts = &(*lstats).counts;

    (*tabentry).numscans += counts.numscans;
    // DEVIATION: upstream bumps tabentry->lastscan via
    // GetCurrentTransactionStopTimestamp() when numscans>0; xact ts unported.
    (*tabentry).tuples_returned += counts.tuples_returned;
    (*tabentry).tuples_fetched += counts.tuples_fetched;
    (*tabentry).tuples_inserted += counts.tuples_inserted;
    (*tabentry).tuples_updated += counts.tuples_updated;
    (*tabentry).tuples_deleted += counts.tuples_deleted;
    (*tabentry).tuples_hot_updated += counts.tuples_hot_updated;
    (*tabentry).tuples_newpage_updated += counts.tuples_newpage_updated;

    /*
     * If table was truncated/dropped, first reset the live/dead counters.
     */
    if counts.truncdropped {
        (*tabentry).live_tuples = 0;
        (*tabentry).dead_tuples = 0;
        (*tabentry).ins_since_vacuum = 0;
    }

    (*tabentry).live_tuples += counts.delta_live_tuples;
    (*tabentry).dead_tuples += counts.delta_dead_tuples;
    (*tabentry).mod_since_analyze += counts.changed_tuples;

    /*
     * Using tuples_inserted to update ins_since_vacuum does mean that we'll
     * track aborted inserts too. (See upstream note.)
     */
    (*tabentry).ins_since_vacuum += counts.tuples_inserted;

    (*tabentry).blocks_fetched += counts.blocks_fetched;
    (*tabentry).blocks_hit += counts.blocks_hit;

    /* Clamp live_tuples in case of negative delta_live_tuples */
    (*tabentry).live_tuples = Max((*tabentry).live_tuples, 0);
    /* Likewise for dead_tuples */
    (*tabentry).dead_tuples = Max((*tabentry).dead_tuples, 0);

    pgstat_unlock_entry(entry_ref);

    // DEVIATION: upstream also propagates these counts into the pending
    // per-database entry via pgstat_prep_database_pending(dboid). Database
    // pending wiring is out of this port's scope (TODO).

    /*
     * Clear the pending counts now they have been folded into the shared entry,
     * so a subsequent flush does not re-add them.
     */
    memset(
        &mut (*lstats).counts as *mut PgStat_TableCounts as *mut c_void,
        0,
        size_of::<PgStat_TableCounts>(),
    );

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
// Transaction hooks (STUBBED no-ops)
// ---------------------------------------------------------------------------
//
// The subxact-linked-list (PgStat_TableXactStatus) propagation machinery is not
// ported. These are the entry points the (unported) xact.c would call; they are
// reduced to no-ops. The transactional count functions above already fold their
// effects directly into the base counts, so the reporter half stays correct in
// the no-rollback case.

/// STUB (xact machinery unported): upstream transfers the top-level subxact
/// insert/update/delete counts into the base tabstat counts and derives the
/// live/dead deltas. Here that derivation already happens inline at count time,
/// so this is a no-op. TODO: real subxact propagation.
pub unsafe fn AtEOXact_PgStat_Relations(_xact_state: *mut c_void, _is_commit: bool) {}

/// STUB (xact machinery unported): propagates a subtransaction's counts up to
/// its parent on (sub)commit, or unwinds them on abort. No-op. TODO.
pub unsafe fn AtEOSubXact_PgStat_Relations(
    _xact_state: *mut c_void,
    _is_commit: bool,
    _nest_depth: c_int,
) {
}

/// STUB (2PC + xact machinery unported): emits TwoPhasePgStatRecord 2PC records
/// for pending transaction-dependent relation stats. No-op. TODO.
pub unsafe fn AtPrepare_PgStat_Relations(_xact_state: *mut c_void) {}

/// STUB (2PC + xact machinery unported): unlinks the transaction stats state
/// from the nontransactional state at PREPARE. No-op. TODO.
pub unsafe fn PostPrepare_PgStat_Relations(_xact_state: *mut c_void) {}

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
