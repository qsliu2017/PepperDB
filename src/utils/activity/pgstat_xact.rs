//! pgstat_xact.c - Transactional integration for the cumulative statistics system.

use crate::prelude::*;

use crate::{dclist_container, dclist_foreach, dclist_foreach_modify};

use crate::access::rmgrdesc::xactdesc::xl_xact_stats_item;
use crate::lib::ilist::{
    dclist_count, dclist_init, dclist_push_tail, dclist_delete_from, dlist_node, dlist_iter,
    dlist_mutable_iter,
};
use crate::utils::pgstat_kind::PgStat_Kind;
use crate::utils::mmgr::mcxt::{MemoryContextAlloc, TopTransactionContext};
use crate::utils::activity::pgstat_internal::{
    PgStat_SubXactStatus, pgstat_drop_entry, pgstat_request_entry_refs_gc, pgstat_get_entry_ref,
};
use crate::utils::activity::pgstat_database::AtEOXact_PgStat_Database;
use crate::utils::activity::pgstat_relation::{
    AtEOXact_PgStat_Relations, AtEOSubXact_PgStat_Relations, AtPrepare_PgStat_Relations,
    PostPrepare_PgStat_Relations,
};

#[repr(C)]
pub struct PgStat_PendingDroppedStatsItem {
    pub item: xl_xact_stats_item,
    pub is_create: bool,
    pub node: dlist_node,
}

static mut pgStatXactStack: *mut PgStat_SubXactStatus = core::ptr::null_mut();

// ----------------------------------------------------------------------------
// Local stubs for not-yet-ported called functions.
// ----------------------------------------------------------------------------

// TODO: pgstat_clear_snapshot not yet ported.
unsafe fn pgstat_clear_snapshot() {}

// TODO: pgstat_reset (pgstat.c public entry) not yet ported.
unsafe fn pgstat_reset(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) {}

// TODO: GetCurrentTransactionNestLevel (access/transam/xact.c) not yet ported.
unsafe fn GetCurrentTransactionNestLevel() -> c_int {
    1
}

/*
 * Called from access/transam/xact.c at top-level transaction commit/abort.
 */
pub unsafe fn AtEOXact_PgStat(isCommit: bool, parallel: bool) {
    let xact_state: *mut PgStat_SubXactStatus;

    AtEOXact_PgStat_Database(isCommit, parallel);

    /* handle transactional stats information */
    xact_state = pgStatXactStack;
    if xact_state != core::ptr::null_mut() {
        Assert!((*xact_state).nest_level == 1);
        Assert!((*xact_state).prev == core::ptr::null_mut());

        AtEOXact_PgStat_Relations(xact_state as *mut _, isCommit);
        AtEOXact_PgStat_DroppedStats(xact_state, isCommit);
    }
    pgStatXactStack = core::ptr::null_mut();

    /* Make sure any stats snapshot is thrown away */
    pgstat_clear_snapshot();
}

/*
 * When committing, drop stats for objects dropped in the transaction. When
 * aborting, drop stats for objects created in the transaction.
 */
unsafe fn AtEOXact_PgStat_DroppedStats(xact_state: *mut PgStat_SubXactStatus, isCommit: bool) {
    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    let mut not_freed_count: c_int = 0;

    if dclist_count(&(*xact_state).pending_drops) == 0 {
        return;
    }

    dclist_foreach_modify!(iter, &mut (*xact_state).pending_drops, {
        let pending = dclist_container!(PgStat_PendingDroppedStatsItem, node, iter.cur);
        let it = &mut (*pending).item;
        let objid: u64 = ((it.objid_hi as u64) << 32) | it.objid_lo as u64;

        if isCommit && !(*pending).is_create {
            /*
             * Transaction that dropped an object committed. Drop the stats
             * too.
             */
            if !pgstat_drop_entry(it.kind as PgStat_Kind, it.dboid, objid) {
                not_freed_count += 1;
            }
        } else if !isCommit && (*pending).is_create {
            /*
             * Transaction that created an object aborted. Drop the stats
             * associated with the object.
             */
            if !pgstat_drop_entry(it.kind as PgStat_Kind, it.dboid, objid) {
                not_freed_count += 1;
            }
        }

        dclist_delete_from(&mut (*xact_state).pending_drops, &mut (*pending).node);
        pfree(pending as *mut c_void);
    });

    if not_freed_count > 0 {
        pgstat_request_entry_refs_gc();
    }
}

/*
 * Called from access/transam/xact.c at subtransaction commit/abort.
 */
pub unsafe fn AtEOSubXact_PgStat(isCommit: bool, nestDepth: c_int) {
    let xact_state: *mut PgStat_SubXactStatus;

    /* merge the sub-transaction's transactional stats into the parent */
    xact_state = pgStatXactStack;
    if xact_state != core::ptr::null_mut() && (*xact_state).nest_level >= nestDepth {
        /* delink xact_state from stack immediately to simplify reuse case */
        pgStatXactStack = (*xact_state).prev;

        AtEOSubXact_PgStat_Relations(xact_state as *mut _, isCommit, nestDepth);
        AtEOSubXact_PgStat_DroppedStats(xact_state, isCommit, nestDepth);

        pfree(xact_state as *mut c_void);
    }
}

/*
 * Like AtEOXact_PgStat_DroppedStats(), but for subtransactions.
 */
unsafe fn AtEOSubXact_PgStat_DroppedStats(
    xact_state: *mut PgStat_SubXactStatus,
    isCommit: bool,
    nestDepth: c_int,
) {
    let parent_xact_state: *mut PgStat_SubXactStatus;
    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    let mut not_freed_count: c_int = 0;

    if dclist_count(&(*xact_state).pending_drops) == 0 {
        return;
    }

    parent_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);

    dclist_foreach_modify!(iter, &mut (*xact_state).pending_drops, {
        let pending = dclist_container!(PgStat_PendingDroppedStatsItem, node, iter.cur);
        let it = &mut (*pending).item;
        let objid: u64 = ((it.objid_hi as u64) << 32) | it.objid_lo as u64;

        dclist_delete_from(&mut (*xact_state).pending_drops, &mut (*pending).node);

        if !isCommit && (*pending).is_create {
            /*
             * Subtransaction creating a new stats object aborted. Drop the
             * stats object.
             */
            if !pgstat_drop_entry(it.kind as PgStat_Kind, it.dboid, objid) {
                not_freed_count += 1;
            }
            pfree(pending as *mut c_void);
        } else if isCommit {
            /*
             * Subtransaction dropping a stats object committed. Can't yet
             * remove the stats object, the surrounding transaction might
             * still abort. Pass it on to the parent.
             */
            dclist_push_tail(&mut (*parent_xact_state).pending_drops, &mut (*pending).node);
        } else {
            pfree(pending as *mut c_void);
        }
    });

    Assert!(dclist_count(&(*xact_state).pending_drops) == 0);
    if not_freed_count > 0 {
        pgstat_request_entry_refs_gc();
    }
}

/*
 * Save the transactional stats state at 2PC transaction prepare.
 */
pub unsafe fn AtPrepare_PgStat() {
    let xact_state: *mut PgStat_SubXactStatus;

    xact_state = pgStatXactStack;
    if xact_state != core::ptr::null_mut() {
        Assert!((*xact_state).nest_level == 1);
        Assert!((*xact_state).prev == core::ptr::null_mut());

        AtPrepare_PgStat_Relations(xact_state as *mut _);
    }
}

/*
 * Clean up after successful PREPARE.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
pub unsafe fn PostPrepare_PgStat() {
    let xact_state: *mut PgStat_SubXactStatus;

    /*
     * We don't bother to free any of the transactional state, since it's all
     * in TopTransactionContext and will go away anyway.
     */
    xact_state = pgStatXactStack;
    if xact_state != core::ptr::null_mut() {
        Assert!((*xact_state).nest_level == 1);
        Assert!((*xact_state).prev == core::ptr::null_mut());

        PostPrepare_PgStat_Relations(xact_state as *mut _);
    }
    pgStatXactStack = core::ptr::null_mut();

    /* Make sure any stats snapshot is thrown away */
    pgstat_clear_snapshot();
}

/*
 * Ensure (sub)transaction stack entry for the given nest_level exists, adding
 * it if needed.
 */
pub unsafe fn pgstat_get_xact_stack_level(nest_level: c_int) -> *mut PgStat_SubXactStatus {
    let mut xact_state: *mut PgStat_SubXactStatus;

    xact_state = pgStatXactStack;
    if xact_state == core::ptr::null_mut() || (*xact_state).nest_level != nest_level {
        xact_state = MemoryContextAlloc(
            TopTransactionContext,
            core::mem::size_of::<PgStat_SubXactStatus>(),
        ) as *mut PgStat_SubXactStatus;
        dclist_init(&mut (*xact_state).pending_drops);
        (*xact_state).nest_level = nest_level;
        (*xact_state).prev = pgStatXactStack;
        (*xact_state).first = core::ptr::null_mut();
        pgStatXactStack = xact_state;
    }
    xact_state
}

/*
 * Get stat items that need to be dropped at commit / abort.
 *
 * When committing, stats for objects that have been dropped in the
 * transaction are returned. When aborting, stats for newly created objects are
 * returned.
 *
 * Used by COMMIT / ABORT and 2PC PREPARE processing when building their
 * respective WAL records, to ensure stats are dropped in case of a crash / on
 * standbys.
 *
 * The list of items is allocated in CurrentMemoryContext and must be freed by
 * the caller (directly or via memory context reset).
 */
pub unsafe fn pgstat_get_transactional_drops(
    isCommit: bool,
    items: *mut *mut xl_xact_stats_item,
) -> c_int {
    let xact_state: *mut PgStat_SubXactStatus = pgStatXactStack;
    let mut nitems: c_int = 0;
    let mut iter: dlist_iter = core::mem::zeroed();

    if xact_state == core::ptr::null_mut() {
        return 0;
    }

    /*
     * We expect to be called for subtransaction abort (which logs a WAL
     * record), but not for subtransaction commit (which doesn't).
     */
    Assert!(!isCommit || (*xact_state).nest_level == 1);
    Assert!(!isCommit || (*xact_state).prev == core::ptr::null_mut());

    *items = palloc(
        dclist_count(&(*xact_state).pending_drops) as usize
            * core::mem::size_of::<xl_xact_stats_item>(),
    ) as *mut xl_xact_stats_item;

    dclist_foreach!(iter, &mut (*xact_state).pending_drops, {
        let pending = dclist_container!(PgStat_PendingDroppedStatsItem, node, iter.cur);

        if isCommit && (*pending).is_create {
            continue;
        }
        if !isCommit && !(*pending).is_create {
            continue;
        }

        Assert!((nitems as u32) < dclist_count(&(*xact_state).pending_drops));
        *(*items).offset(nitems as isize) = (*pending).item;
        nitems += 1;
    });

    nitems
}

/*
 * Execute scheduled drops post-commit. Called from xact_redo_commit() /
 * xact_redo_abort() during recovery, and from FinishPreparedTransaction()
 * during normal 2PC COMMIT/ABORT PREPARED processing.
 */
pub unsafe fn pgstat_execute_transactional_drops(
    ndrops: c_int,
    items: *mut xl_xact_stats_item,
    _is_redo: bool,
) {
    let mut not_freed_count: c_int = 0;

    if ndrops == 0 {
        return;
    }

    for i in 0..ndrops {
        let it = &mut *items.offset(i as isize);
        let objid: u64 = ((it.objid_hi as u64) << 32) | it.objid_lo as u64;

        if !pgstat_drop_entry(it.kind as PgStat_Kind, it.dboid, objid) {
            not_freed_count += 1;
        }
    }

    if not_freed_count > 0 {
        pgstat_request_entry_refs_gc();
    }
}

unsafe fn create_drop_transactional_internal(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    is_create: bool,
) {
    let nest_level: c_int = GetCurrentTransactionNestLevel();
    let xact_state: *mut PgStat_SubXactStatus;
    let drop = MemoryContextAlloc(
        TopTransactionContext,
        core::mem::size_of::<PgStat_PendingDroppedStatsItem>(),
    ) as *mut PgStat_PendingDroppedStatsItem;

    xact_state = pgstat_get_xact_stack_level(nest_level);

    (*drop).is_create = is_create;
    (*drop).item.kind = kind as c_int;
    (*drop).item.dboid = dboid;
    (*drop).item.objid_lo = objid as uint32;
    (*drop).item.objid_hi = (objid >> 32) as uint32;

    dclist_push_tail(&mut (*xact_state).pending_drops, &mut (*drop).node);
}

/*
 * Create a stats entry for a newly created database object in a transactional
 * manner.
 *
 * I.e. if the current (sub-)transaction aborts, the stats entry will also be
 * dropped.
 */
pub unsafe fn pgstat_create_transactional(kind: PgStat_Kind, dboid: Oid, objid: u64) {
    if !pgstat_get_entry_ref(kind, dboid, objid, false, core::ptr::null_mut()).is_null() {
        ereport!(
            WARNING,
            "resetting existing statistics for kind, db, oid"
        );

        pgstat_reset(kind, dboid, objid);
    }

    create_drop_transactional_internal(kind, dboid, objid, /* create */ true);
}

/*
 * Drop a stats entry for a just dropped database object in a transactional
 * manner.
 *
 * I.e. if the current (sub-)transaction aborts, the stats entry will stay
 * alive.
 */
pub unsafe fn pgstat_drop_transactional(kind: PgStat_Kind, dboid: Oid, objid: u64) {
    create_drop_transactional_internal(kind, dboid, objid, /* create */ false);
}
