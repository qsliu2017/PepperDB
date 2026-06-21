//! portalmem.rs
//!   backend portal memory management
//!
//! Translated 1:1 from postgres/src/backend/utils/mmgr/portalmem.c
//!
//! Portals are objects representing the execution state of a query.
//! This module provides memory management services for portals, but it
//! doesn't actually run the executor for them.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/mmgr/portalmem.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::tcop::cmdtag::CommandTag::CMDTAG_UNKNOWN;
use crate::{foreach, current_cell};
use crate::pg_config_manual::NAMEDATALEN;
use crate::utils::fmgr::FunctionCallInfo;


use crate::{
    // access/xact.h
    // commands/portalcmds.h
    // funcapi.h
    // miscadmin.h
    // storage/ipc.h
    // utils/builtins.h
    // utils/memutils.h
    // utils/snapmgr.h
    // utils/timestamp.h
    AllocSetContextCreate,
};

// access/xact.h: subxact / nesting-level / commit-ts helpers, and
// CurTransactionResourceOwner.
use crate::utils::resowner::resowner::{
    ResourceOwner, ResourceOwnerCreate, ResourceOwnerDelete,
    ResourceOwnerNewParent, ResourceOwnerRelease, RESOURCE_RELEASE_AFTER_LOCKS,
    RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
};
use crate::access::transam::xact::CurTransactionResourceOwner;

// commands/portalcmds.h
use crate::commands::portalcmds::{PersistHoldablePortal, PortalCleanup};

// nodes/parsenodes.h: DECLARE CURSOR option bits.
use crate::nodes::parsenodes::{
    CURSOR_OPT_BINARY, CURSOR_OPT_HOLD, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
};

// nodes/pg_list.h
use crate::nodes::pg_list::{lfirst, List, NIL};

// nodes/plannodes.h
use crate::nodes::plannodes::PlannedStmt;

// storage/ipc.h
use crate::storage::ipc::ipc::shmem_exit_inprogress;

// tcop/cmdtag.h
use crate::tcop::cmdtag::{CommandTag};

// utils/hash/dynahash.h (dynahash.c public interface)
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_search, hash_seq_term, HASHCTL,
    HASH_ELEM, HASH_ENTER, HASH_FIND, HASH_REMOVE, HASH_SEQ_STATUS, HASH_STRINGS, HTAB,
};

// utils/mmgr/mcxt.c: real MemoryContext dispatch (children deletion).
use crate::utils::mmgr::mcxt::MemoryContextDeleteChildren;

// utils/palloc.h / utils/memutils.h come in via the prelude:
//   MemoryContext, MemoryContextAllocZero, MemoryContextSetIdentifier,
//   MemoryContextSwitchTo, MemoryContextDelete, pfree, TopMemoryContext,
//   ALLOCSET_DEFAULT_SIZES, ALLOCSET_SMALL_SIZES, errcode, ERROR, WARNING.

// utils/portal.h
use crate::utils::portal::{
    Portal, PortalIsValid, PORTAL_ACTIVE, PORTAL_DEFINED, PORTAL_FAILED, PORTAL_MULTI_QUERY,
    PORTAL_NEW, PORTAL_ONE_SELECT, PORTAL_READY,
};

// ---------------------------------------------------------------------------
// External symbols not yet ported: minimal local stubs.
// ---------------------------------------------------------------------------

// access/xact.h: subtransaction / nesting bookkeeping (xact.c not yet ported).
// TODO(pg-port): real GetCurrentSubTransactionId lives in access/transam/xact.rs
unsafe fn GetCurrentSubTransactionId() -> SubTransactionId { crate::access::transam::xact::GetCurrentSubTransactionId() }
// TODO(pg-port): real GetCurrentTransactionNestLevel lives in access/transam/xact.rs
unsafe fn GetCurrentTransactionNestLevel() -> c_int { crate::access::transam::xact::GetCurrentTransactionNestLevel() }

// utils/timestamp.h: statement-start timestamp (timestamp.c stub upstream).
// TODO(pg-port): real GetCurrentStatementStartTimestamp lives in access/transam/xact.rs
unsafe fn GetCurrentStatementStartTimestamp() -> TimestampTz { crate::utils::adt::timestamp::GetCurrentStatementStartTimestamp() }

// utils/plancache.h: cached-plan refcounting (plancache.c not yet ported).
// TODO(pg-port): real ReleaseCachedPlan lives in utils/cache/plancache.rs
unsafe fn ReleaseCachedPlan(plan: *mut CachedPlan, owner: ResourceOwner) { crate::utils::cache::plancache::ReleaseCachedPlan(plan as _, owner) }

// utils/snapmgr.h: active-snapshot / registered-snapshot management
// (snapmgr.c not yet ported).
// TODO(pg-port): real UnregisterSnapshotFromOwner lives in utils/time/snapmgr.rs
unsafe fn UnregisterSnapshotFromOwner(snapshot: Snapshot, owner: ResourceOwner) { crate::utils::time::snapmgr::UnregisterSnapshotFromOwner(snapshot, owner) }
// TODO(pg-port): real ActiveSnapshotSet lives in utils/time/snapmgr.rs
unsafe fn ActiveSnapshotSet() -> bool { crate::utils::time::snapmgr::ActiveSnapshotSet() }
// TODO(pg-port): real PopActiveSnapshot lives in utils/time/snapmgr.rs
unsafe fn PopActiveSnapshot() { crate::utils::time::snapmgr::PopActiveSnapshot() }

// utils/tuplestore.h: tuplestore for holdable cursors (tuplestore.c not yet ported).
// TODO(pg-port): real tuplestore_begin_heap lives in utils/sort/tuplestore.rs
unsafe fn tuplestore_begin_heap(
    randomAccess: bool,
    interXact: bool,
    maxKBytes: c_int,
) -> *mut Tuplestorestate {
    crate::utils::sort::tuplestore::tuplestore_begin_heap(randomAccess, interXact, maxKBytes) as _
}
// TODO(pg-port): real tuplestore_end lives in utils/sort/tuplestore.rs
unsafe fn tuplestore_end(state: *mut Tuplestorestate) {
    crate::utils::sort::tuplestore::tuplestore_end(state as _)
}
// TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.rs
unsafe fn tuplestore_putvalues(
    state: *mut Tuplestorestate,
    tdesc: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) {
    crate::utils::sort::tuplestore::tuplestore_putvalues(state as _, tdesc as _, values as _, isnull as _)
}

// miscadmin.h / utils/guc.h: work_mem GUC. The prelude pulls in palloc etc.;
// work_mem lives in miscadmin.rs as an extern static.
use crate::miscadmin::work_mem;

// utils/builtins.h / postgres.h Datum constructors. CStringGetTextDatum and
// BoolGetDatum come from the prelude / builtins; TimestampTzGetDatum is local to
// timestamp.c.
use crate::utils::builtins::CStringGetTextDatum;
// TODO(pg-port): real TimestampTzGetDatum lives in utils/adt/timestamp.rs
unsafe fn TimestampTzGetDatum(x: TimestampTz) -> Datum {
    unimplemented!("TimestampTzGetDatum: crate::utils::adt::timestamp")
}

// funcapi.h: set-returning-function support (funcapi.c not yet ported).
// TODO(pg-port): real ReturnSetInfo lives in nodes/execnodes.rs
#[repr(C)]
pub struct ReturnSetInfo {
    pub setResult: *mut Tuplestorestate,
    pub setDesc: TupleDesc,
    // ... remaining fields elided until execnodes.h lands.
}
// TODO(pg-port): real InitMaterializedSRF lives in utils/fmgr/funcapi.rs
unsafe fn InitMaterializedSRF(fcinfo: crate::utils::fmgr::FunctionCallInfo, flags: bits32) { unimplemented!() }

// Re-exported portal.h types used in this file.
use crate::utils::portal::{CachedPlan, TimestampTz, Tuplestorestate};
// access/common/tupdesc.h
use crate::access::common::tupdesc::TupleDesc;
// utils/snapshot.h
use crate::utils::snapshot::Snapshot;

/*
 * Estimate of the maximum number of open portals a user would have,
 * used in initially sizing the PortalHashTable in EnablePortalManager().
 * Since the hash table can expand, there's no need to make this overly
 * generous, and keeping it small avoids unnecessary overhead in the
 * hash_seq_search() calls executed during transaction end.
 */
const PORTALS_PER_USER: c_long = 16;


/* ----------------
 *		Global state
 * ----------------
 */

const MAX_PORTALNAME_LEN: usize = NAMEDATALEN;

#[repr(C)]
pub struct PortalHashEnt {
    pub portalname: [c_char; MAX_PORTALNAME_LEN],
    pub portal: Portal,
}

static mut PortalHashTable: *mut HTAB = null_mut();

/*
 * PortalHashTableLookup(NAME, PORTAL)
 *
 * C macro expanded inline at each call site.
 */
unsafe fn PortalHashTableLookup(name: *const c_char) -> Portal {
    let hentry = hash_search(PortalHashTable, name as *const c_void, HASH_FIND, null_mut())
        as *mut PortalHashEnt;
    if !hentry.is_null() {
        (*hentry).portal
    } else {
        null_mut()
    }
}

/*
 * PortalHashTableInsert(PORTAL, NAME)
 */
unsafe fn PortalHashTableInsert(portal: Portal, name: *const c_char) {
    let mut found: bool = false;

    let hentry = hash_search(
        PortalHashTable,
        name as *const c_void,
        HASH_ENTER,
        &raw mut found,
    ) as *mut PortalHashEnt;
    if found {
        elog!(ERROR, "duplicate portal name");
    }
    (*hentry).portal = portal;
    /* To avoid duplicate storage, make PORTAL->name point to htab entry */
    (*portal).name = (*hentry).portalname.as_ptr();
}

/*
 * PortalHashTableDelete(PORTAL)
 */
unsafe fn PortalHashTableDelete(portal: Portal) {
    let hentry = hash_search(
        PortalHashTable,
        (*portal).name as *const c_void,
        HASH_REMOVE,
        null_mut(),
    ) as *mut PortalHashEnt;
    if hentry.is_null() {
        elog!(WARNING, "trying to delete portal name that does not exist");
    }
}

static mut TopPortalContext: MemoryContext = null_mut();


/* ----------------------------------------------------------------
 *				   public portal interface functions
 * ----------------------------------------------------------------
 */

/*
 * EnablePortalManager
 *		Enables the portal management module at backend startup.
 */
pub unsafe fn EnablePortalManager() {
    let mut ctl: HASHCTL = core::mem::zeroed();

    Assert!(TopPortalContext.is_null());

    TopPortalContext = AllocSetContextCreate!(
        TopMemoryContext,
        c"TopPortalContext".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    ctl.keysize = MAX_PORTALNAME_LEN;
    ctl.entrysize = core::mem::size_of::<PortalHashEnt>();

    /*
     * use PORTALS_PER_USER as a guess of how many hash table entries to
     * create, initially
     */
    PortalHashTable = hash_create(
        c"Portal hash".as_ptr(),
        PORTALS_PER_USER,
        &raw mut ctl,
        HASH_ELEM | HASH_STRINGS,
    );
}

/*
 * GetPortalByName
 *		Returns a portal given a portal name, or NULL if name not found.
 */
pub unsafe fn GetPortalByName(name: *const c_char) -> Portal {
    let portal: Portal;

    if PointerIsValid(name) {
        portal = PortalHashTableLookup(name);
    } else {
        portal = null_mut();
    }

    portal
}

/*
 * PortalGetPrimaryStmt
 *		Get the "primary" stmt within a portal, ie, the one marked canSetTag.
 *
 * Returns NULL if no such stmt.  If multiple PlannedStmt structs within the
 * portal are marked canSetTag, returns the first one.  Neither of these
 * cases should occur in present usages of this function.
 */
pub unsafe fn PortalGetPrimaryStmt(portal: Portal) -> *mut PlannedStmt {
    foreach!(lc, (*portal).stmts, {
        let stmt = lfirst(current_cell!(lc)) as *mut PlannedStmt;

        if (*stmt).canSetTag {
            return stmt;
        }
    });
    null_mut()
}

/*
 * CreatePortal
 *		Returns a new portal given a name.
 *
 * allowDup: if true, automatically drop any pre-existing portal of the
 * same name (if false, an error is raised).
 *
 * dupSilent: if true, don't even emit a WARNING.
 */
pub unsafe fn CreatePortal(name: *const c_char, allowDup: bool, dupSilent: bool) -> Portal {
    let mut portal: Portal;

    Assert!(PointerIsValid(name));

    portal = GetPortalByName(name);
    if PortalIsValid(portal) {
        if !allowDup {
            ereport!(
                ERROR,
                errmsg!("cursor \"{}\" already exists", cstr_to_str(name))
            );
        }
        if !dupSilent {
            ereport!(
                WARNING,
                errmsg!("closing existing cursor \"{}\"", cstr_to_str(name))
            );
        }
        PortalDrop(portal, false);
    }

    /* make new portal structure */
    portal = MemoryContextAllocZero(TopPortalContext, core::mem::size_of::<crate::utils::portal::PortalData>())
        as Portal;

    /* initialize portal context; typically it won't store much */
    (*portal).portalContext = (AllocSetContextCreate!(
        TopPortalContext,
        c"PortalContext".as_ptr(),
        ALLOCSET_SMALL_SIZES
    )) as crate::utils::mmgr::memnodes::MemoryContext;

    /* create a resource owner for the portal */
    (*portal).resowner = ResourceOwnerCreate(CurTransactionResourceOwner as _, c"Portal".as_ptr());

    /* initialize portal fields that don't start off zero */
    (*portal).status = PORTAL_NEW;
    (*portal).cleanup = Some(core::mem::transmute::<_, unsafe extern "C" fn(Portal)>(PortalCleanup as *const ()));
    (*portal).createSubid = GetCurrentSubTransactionId();
    (*portal).activeSubid = (*portal).createSubid;
    (*portal).createLevel = GetCurrentTransactionNestLevel();
    (*portal).strategy = PORTAL_MULTI_QUERY;
    (*portal).cursorOptions = CURSOR_OPT_NO_SCROLL;
    (*portal).atStart = true;
    (*portal).atEnd = true; /* disallow fetches until query is set */
    (*portal).visible = true;
    (*portal).creation_time = GetCurrentStatementStartTimestamp();

    /* put portal in table (sets portal->name) */
    PortalHashTableInsert(portal, name);

    /* for named portals reuse portal->name copy */
    MemoryContextSetIdentifier(
        (*portal).portalContext as crate::utils::palloc::MemoryContext,
        if *(*portal).name != 0 {
            (*portal).name
        } else {
            c"<unnamed>".as_ptr()
        },
    );

    portal
}

/*
 * CreateNewPortal
 *		Create a new portal, assigning it a random nonconflicting name.
 */
pub unsafe fn CreateNewPortal() -> Portal {
    static mut unnamed_portal_count: c_uint = 0;

    let mut portalname: [c_char; MAX_PORTALNAME_LEN] = [0; MAX_PORTALNAME_LEN];

    /* Select a nonconflicting name */
    loop {
        unnamed_portal_count += 1;
        let s = std::ffi::CString::new(format!("<unnamed portal {}>", unnamed_portal_count))
            .unwrap();
        let bytes = s.as_bytes_with_nul();
        for (i, b) in bytes.iter().enumerate() {
            portalname[i] = *b as c_char;
        }
        if GetPortalByName(portalname.as_ptr()).is_null() {
            break;
        }
    }

    CreatePortal(portalname.as_ptr(), false, false)
}

/*
 * PortalDefineQuery
 *		A simple subroutine to establish a portal's query.
 *
 * Notes: as of PG 8.4, caller MUST supply a sourceText string; it is not
 * allowed anymore to pass NULL.  (If you really don't have source text,
 * you can pass a constant string, perhaps "(query not available)".)
 *
 * commandTag shall be NULL if and only if the original query string
 * (before rewriting) was an empty string.  Also, the passed commandTag must
 * be a pointer to a constant string, since it is not copied.
 *
 * If cplan is provided, then it is a cached plan containing the stmts, and
 * the caller must have done GetCachedPlan(), causing a refcount increment.
 * The refcount will be released when the portal is destroyed.
 *
 * If cplan is NULL, then it is the caller's responsibility to ensure that
 * the passed plan trees have adequate lifetime.  Typically this is done by
 * copying them into the portal's context.
 *
 * The caller is also responsible for ensuring that the passed prepStmtName
 * (if not NULL) and sourceText have adequate lifetime.
 *
 * NB: this function mustn't do much beyond storing the passed values; in
 * particular don't do anything that risks elog(ERROR).  If that were to
 * happen here before storing the cplan reference, we'd leak the plancache
 * refcount that the caller is trying to hand off to us.
 */
pub unsafe fn PortalDefineQuery(
    portal: Portal,
    prepStmtName: *const c_char,
    sourceText: *const c_char,
    commandTag: CommandTag,
    stmts: *mut List,
    cplan: *mut CachedPlan,
) {
    Assert!(PortalIsValid(portal));
    Assert!((*portal).status == PORTAL_NEW);

    Assert!(!sourceText.is_null());
    Assert!(commandTag != CMDTAG_UNKNOWN || stmts == NIL);

    (*portal).prepStmtName = prepStmtName;
    (*portal).sourceText = sourceText;
    (*portal).qc.commandTag = commandTag;
    (*portal).qc.nprocessed = 0;
    (*portal).commandTag = commandTag;
    (*portal).stmts = stmts;
    (*portal).cplan = cplan;
    (*portal).status = PORTAL_DEFINED;
}

/*
 * PortalReleaseCachedPlan
 *		Release a portal's reference to its cached plan, if any.
 */
unsafe fn PortalReleaseCachedPlan(portal: Portal) {
    if !(*portal).cplan.is_null() {
        ReleaseCachedPlan((*portal).cplan, null_mut());
        (*portal).cplan = null_mut();

        /*
         * We must also clear portal->stmts which is now a dangling reference
         * to the cached plan's plan list.  This protects any code that might
         * try to examine the Portal later.
         */
        (*portal).stmts = NIL;
    }
}

/*
 * PortalCreateHoldStore
 *		Create the tuplestore for a portal.
 */
pub unsafe fn PortalCreateHoldStore(portal: Portal) {
    let oldcxt: MemoryContext;

    Assert!((*portal).holdContext.is_null());
    Assert!((*portal).holdStore.is_null());
    Assert!((*portal).holdSnapshot.is_null());

    /*
     * Create the memory context that is used for storage of the tuple set.
     * Note this is NOT a child of the portal's portalContext.
     */
    (*portal).holdContext = (AllocSetContextCreate!(
        TopPortalContext,
        c"PortalHoldContext".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    )) as crate::utils::mmgr::memnodes::MemoryContext;

    /*
     * Create the tuple store, selecting cross-transaction temp files, and
     * enabling random access only if cursor requires scrolling.
     *
     * XXX: Should maintenance_work_mem be used for the portal size?
     */
    oldcxt = MemoryContextSwitchTo((*portal).holdContext as crate::utils::palloc::MemoryContext);

    (*portal).holdStore = tuplestore_begin_heap(
        ((*portal).cursorOptions & CURSOR_OPT_SCROLL) != 0,
        true,
        work_mem,
    );

    MemoryContextSwitchTo(oldcxt);
}

/*
 * PinPortal
 *		Protect a portal from dropping.
 *
 * A pinned portal is still unpinned and dropped at transaction or
 * subtransaction abort.
 */
#[no_mangle]
pub unsafe fn PinPortal(portal: Portal) {
    if (*portal).portalPinned {
        elog!(ERROR, "portal already pinned");
    }

    (*portal).portalPinned = true;
}

#[no_mangle]
pub unsafe fn UnpinPortal(portal: Portal) {
    if !(*portal).portalPinned {
        elog!(ERROR, "portal not pinned");
    }

    (*portal).portalPinned = false;
}

/*
 * MarkPortalActive
 *		Transition a portal from READY to ACTIVE state.
 *
 * NOTE: never set portal->status = PORTAL_ACTIVE directly; call this instead.
 */
pub unsafe fn MarkPortalActive(portal: Portal) {
    /* For safety, this is a runtime test not just an Assert */
    if (*portal).status != PORTAL_READY {
        ereport!(
            ERROR,
            errmsg!(
                "portal \"{}\" cannot be run",
                cstr_to_str((*portal).name)
            )
        );
    }
    /* Perform the state transition */
    (*portal).status = PORTAL_ACTIVE;
    (*portal).activeSubid = GetCurrentSubTransactionId();
}

/*
 * MarkPortalDone
 *		Transition a portal from ACTIVE to DONE state.
 *
 * NOTE: never set portal->status = PORTAL_DONE directly; call this instead.
 */
pub unsafe fn MarkPortalDone(portal: Portal) {
    /* Perform the state transition */
    Assert!((*portal).status == PORTAL_ACTIVE);
    (*portal).status = crate::utils::portal::PORTAL_DONE;

    /*
     * Allow portalcmds.c to clean up the state it knows about.  We might as
     * well do that now, since the portal can't be executed any more.
     *
     * In some cases involving execution of a ROLLBACK command in an already
     * aborted transaction, this is necessary, or we'd reach AtCleanup_Portals
     * with the cleanup hook still unexecuted.
     */
    if let Some(cleanup) = (*portal).cleanup {
        cleanup(portal);
        (*portal).cleanup = None;
    }
}

/*
 * MarkPortalFailed
 *		Transition a portal into FAILED state.
 *
 * NOTE: never set portal->status = PORTAL_FAILED directly; call this instead.
 */
pub unsafe fn MarkPortalFailed(portal: Portal) {
    /* Perform the state transition */
    Assert!((*portal).status != crate::utils::portal::PORTAL_DONE);
    (*portal).status = PORTAL_FAILED;

    /*
     * Allow portalcmds.c to clean up the state it knows about.  We might as
     * well do that now, since the portal can't be executed any more.
     *
     * In some cases involving cleanup of an already aborted transaction, this
     * is necessary, or we'd reach AtCleanup_Portals with the cleanup hook
     * still unexecuted.
     */
    if let Some(cleanup) = (*portal).cleanup {
        cleanup(portal);
        (*portal).cleanup = None;
    }
}

/*
 * PortalDrop
 *		Destroy the portal.
 */
pub unsafe fn PortalDrop(portal: Portal, isTopCommit: bool) {
    Assert!(PortalIsValid(portal));

    /*
     * Don't allow dropping a pinned portal, it's still needed by whoever
     * pinned it.
     */
    if (*portal).portalPinned {
        ereport!(
            ERROR,
            errmsg!(
                "cannot drop pinned portal \"{}\"",
                cstr_to_str((*portal).name)
            )
        );
    }

    /*
     * Not sure if the PORTAL_ACTIVE case can validly happen or not...
     */
    if (*portal).status == PORTAL_ACTIVE {
        ereport!(
            ERROR,
            errmsg!(
                "cannot drop active portal \"{}\"",
                cstr_to_str((*portal).name)
            )
        );
    }

    /*
     * Allow portalcmds.c to clean up the state it knows about, in particular
     * shutting down the executor if still active.  This step potentially runs
     * user-defined code so failure has to be expected.  It's the cleanup
     * hook's responsibility to not try to do that more than once, in the case
     * that failure occurs and then we come back to drop the portal again
     * during transaction abort.
     *
     * Note: in most paths of control, this will have been done already in
     * MarkPortalDone or MarkPortalFailed.  We're just making sure.
     */
    if let Some(cleanup) = (*portal).cleanup {
        cleanup(portal);
        (*portal).cleanup = None;
    }

    /* There shouldn't be an active snapshot anymore, except after error */
    Assert!((*portal).portalSnapshot.is_null() || !isTopCommit);

    /*
     * Remove portal from hash table.  Because we do this here, we will not
     * come back to try to remove the portal again if there's any error in the
     * subsequent steps.  Better to leak a little memory than to get into an
     * infinite error-recovery loop.
     */
    PortalHashTableDelete(portal);

    /* drop cached plan reference, if any */
    PortalReleaseCachedPlan(portal);

    /*
     * If portal has a snapshot protecting its data, release that.  This needs
     * a little care since the registration will be attached to the portal's
     * resowner; if the portal failed, we will already have released the
     * resowner (and the snapshot) during transaction abort.
     */
    if !(*portal).holdSnapshot.is_null() {
        if !(*portal).resowner.is_null() {
            UnregisterSnapshotFromOwner((*portal).holdSnapshot, (*portal).resowner);
        }
        (*portal).holdSnapshot = null_mut();
    }

    /*
     * Release any resources still attached to the portal.  There are several
     * cases being covered here:
     *
     * Top transaction commit (indicated by isTopCommit): normally we should
     * do nothing here and let the regular end-of-transaction resource
     * releasing mechanism handle these resources too.  However, if we have a
     * FAILED portal (eg, a cursor that got an error), we'd better clean up
     * its resources to avoid resource-leakage warning messages.
     *
     * Sub transaction commit: never comes here at all, since we don't kill
     * any portals in AtSubCommit_Portals().
     *
     * Main or sub transaction abort: we will do nothing here because
     * portal->resowner was already set NULL; the resources were already
     * cleaned up in transaction abort.
     *
     * Ordinary portal drop: must release resources.  However, if the portal
     * is not FAILED then we do not release its locks.  The locks become the
     * responsibility of the transaction's ResourceOwner (since it is the
     * parent of the portal's owner) and will be released when the transaction
     * eventually ends.
     */
    if !(*portal).resowner.is_null() && (!isTopCommit || (*portal).status == PORTAL_FAILED) {
        let isCommit: bool = (*portal).status != PORTAL_FAILED;

        ResourceOwnerRelease(
            (*portal).resowner,
            RESOURCE_RELEASE_BEFORE_LOCKS,
            isCommit,
            false,
        );
        ResourceOwnerRelease(
            (*portal).resowner,
            RESOURCE_RELEASE_LOCKS,
            isCommit,
            false,
        );
        ResourceOwnerRelease(
            (*portal).resowner,
            RESOURCE_RELEASE_AFTER_LOCKS,
            isCommit,
            false,
        );
        ResourceOwnerDelete((*portal).resowner);
    }
    (*portal).resowner = null_mut();

    /*
     * Delete tuplestore if present.  We should do this even under error
     * conditions; since the tuplestore would have been using cross-
     * transaction storage, its temp files need to be explicitly deleted.
     */
    if !(*portal).holdStore.is_null() {
        let oldcontext: MemoryContext;

        oldcontext = MemoryContextSwitchTo((*portal).holdContext as crate::utils::palloc::MemoryContext);
        tuplestore_end((*portal).holdStore);
        MemoryContextSwitchTo(oldcontext);
        (*portal).holdStore = null_mut();
    }

    /* delete tuplestore storage, if any */
    if !(*portal).holdContext.is_null() {
        MemoryContextDelete((*portal).holdContext as crate::utils::palloc::MemoryContext);
    }

    /* release subsidiary storage */
    MemoryContextDelete((*portal).portalContext as crate::utils::palloc::MemoryContext);

    /* release portal struct (it's in TopPortalContext) */
    pfree(portal as *mut c_void);
}

/*
 * Delete all declared cursors.
 *
 * Used by commands: CLOSE ALL, DISCARD ALL
 */
pub unsafe fn PortalHashTableDeleteAll() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    if PortalHashTable.is_null() {
        return;
    }

    hash_seq_init(&raw mut status, PortalHashTable);
    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        /* Can't close the active portal (the one running the command) */
        if (*portal).status == PORTAL_ACTIVE {
            continue;
        }

        PortalDrop(portal, false);

        /* Restart the iteration in case that led to other drops */
        hash_seq_term(&raw mut status);
        hash_seq_init(&raw mut status, PortalHashTable);
    }
}

/*
 * "Hold" a portal.  Prepare it for access by later transactions.
 */
unsafe fn HoldPortal(portal: Portal) {
    /*
     * Note that PersistHoldablePortal() must release all resources used by
     * the portal that are local to the creating transaction.
     */
    PortalCreateHoldStore(portal);
    PersistHoldablePortal(portal as *mut core::ffi::c_void);

    /* drop cached plan reference, if any */
    PortalReleaseCachedPlan(portal);

    /*
     * Any resources belonging to the portal will be released in the upcoming
     * transaction-wide cleanup; the portal will no longer have its own
     * resources.
     */
    (*portal).resowner = null_mut();

    /*
     * Having successfully exported the holdable cursor, mark it as not
     * belonging to this transaction.
     */
    (*portal).createSubid = InvalidSubTransactionId;
    (*portal).activeSubid = InvalidSubTransactionId;
    (*portal).createLevel = 0;
}

/*
 * Pre-commit processing for portals.
 *
 * Holdable cursors created in this transaction need to be converted to
 * materialized form, since we are going to close down the executor and
 * release locks.  Non-holdable portals created in this transaction are
 * simply removed.  Portals remaining from prior transactions should be
 * left untouched.
 *
 * Returns true if any portals changed state (possibly causing user-defined
 * code to be run), false if not.
 */
pub unsafe fn PreCommit_Portals(isPrepare: bool) -> bool {
    let mut result: bool = false;
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        /*
         * There should be no pinned portals anymore. Complain if someone
         * leaked one. Auto-held portals are allowed; we assume that whoever
         * pinned them is managing them.
         */
        if (*portal).portalPinned && !(*portal).autoHeld {
            elog!(ERROR, "cannot commit while a portal is pinned");
        }

        /*
         * Do not touch active portals --- this can only happen in the case of
         * a multi-transaction utility command, such as VACUUM, or a commit in
         * a procedure.
         *
         * Note however that any resource owner attached to such a portal is
         * still going to go away, so don't leave a dangling pointer.  Also
         * unregister any snapshots held by the portal, mainly to avoid
         * snapshot leak warnings from ResourceOwnerRelease().
         */
        if (*portal).status == PORTAL_ACTIVE {
            if !(*portal).holdSnapshot.is_null() {
                if !(*portal).resowner.is_null() {
                    UnregisterSnapshotFromOwner((*portal).holdSnapshot, (*portal).resowner);
                }
                (*portal).holdSnapshot = null_mut();
            }
            (*portal).resowner = null_mut();
            /* Clear portalSnapshot too, for cleanliness */
            (*portal).portalSnapshot = null_mut();
            continue;
        }

        /* Is it a holdable portal created in the current xact? */
        if ((*portal).cursorOptions & CURSOR_OPT_HOLD) != 0
            && (*portal).createSubid != InvalidSubTransactionId
            && (*portal).status == PORTAL_READY
        {
            /*
             * We are exiting the transaction that created a holdable cursor.
             * Instead of dropping the portal, prepare it for access by later
             * transactions.
             *
             * However, if this is PREPARE TRANSACTION rather than COMMIT,
             * refuse PREPARE, because the semantics seem pretty unclear.
             */
            if isPrepare {
                ereport!(
                    ERROR,
                    errmsg!("cannot PREPARE a transaction that has created a cursor WITH HOLD")
                );
            }

            HoldPortal(portal);

            /* Report we changed state */
            result = true;
        } else if (*portal).createSubid == InvalidSubTransactionId {
            /*
             * Do nothing to cursors held over from a previous transaction
             * (including ones we just froze in a previous cycle of this loop)
             */
            continue;
        } else {
            /* Zap all non-holdable portals */
            PortalDrop(portal, true);

            /* Report we changed state */
            result = true;
        }

        /*
         * After either freezing or dropping a portal, we have to restart the
         * iteration, because we could have invoked user-defined code that
         * caused a drop of the next portal in the hash chain.
         */
        hash_seq_term(&raw mut status);
        hash_seq_init(&raw mut status, PortalHashTable);
    }

    result
}

/*
 * Abort processing for portals.
 *
 * At this point we run the cleanup hook if present, but we can't release the
 * portal's memory until the cleanup call.
 */
pub unsafe fn AtAbort_Portals() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        /*
         * Errors propagate via siglongjmp, which bypasses PortalRun's Rust
         * catch_unwind (where PG marks the active portal failed). So mark any
         * still-active portal failed here, not only during shmem_exit, else the
         * next command's CreatePortal hits "cannot drop active portal".
         */
        if (*portal).status == PORTAL_ACTIVE {
            MarkPortalFailed(portal);
        }

        /*
         * Do nothing else to cursors held over from a previous transaction.
         */
        if (*portal).createSubid == InvalidSubTransactionId {
            continue;
        }

        /*
         * Do nothing to auto-held cursors.  This is similar to the case of a
         * cursor from a previous transaction, but it could also be that the
         * cursor was auto-held in this transaction, so it wants to live on.
         */
        if (*portal).autoHeld {
            continue;
        }

        /*
         * If it was created in the current transaction, we can't do normal
         * shutdown on a READY portal either; it might refer to objects
         * created in the failed transaction.  See comments in
         * AtSubAbort_Portals.
         */
        if (*portal).status == PORTAL_READY {
            MarkPortalFailed(portal);
        }

        /*
         * Allow portalcmds.c to clean up the state it knows about, if we
         * haven't already.
         */
        if let Some(cleanup) = (*portal).cleanup {
            cleanup(portal);
            (*portal).cleanup = None;
        }

        /* drop cached plan reference, if any */
        PortalReleaseCachedPlan(portal);

        /*
         * Any resources belonging to the portal will be released in the
         * upcoming transaction-wide cleanup; they will be gone before we run
         * PortalDrop.
         */
        (*portal).resowner = null_mut();

        /*
         * Although we can't delete the portal data structure proper, we can
         * release any memory in subsidiary contexts, such as executor state.
         * The cleanup hook was the last thing that might have needed data
         * there.  But leave active portals alone.
         */
        if (*portal).status != PORTAL_ACTIVE {
            MemoryContextDeleteChildren((*portal).portalContext);
        }
    }
}

/*
 * Post-abort cleanup for portals.
 *
 * Delete all portals not held over from prior transactions.  */
pub unsafe fn AtCleanup_Portals() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        /*
         * Do not touch active portals --- this can only happen in the case of
         * a multi-transaction command.
         */
        if (*portal).status == PORTAL_ACTIVE {
            continue;
        }

        /*
         * Do nothing to cursors held over from a previous transaction or
         * auto-held ones.
         */
        if (*portal).createSubid == InvalidSubTransactionId || (*portal).autoHeld {
            Assert!((*portal).status != PORTAL_ACTIVE);
            Assert!((*portal).resowner.is_null());
            continue;
        }

        /*
         * If a portal is still pinned, forcibly unpin it. PortalDrop will not
         * let us drop the portal otherwise. Whoever pinned the portal was
         * interrupted by the abort too and won't try to use it anymore.
         */
        if (*portal).portalPinned {
            (*portal).portalPinned = false;
        }

        /*
         * We had better not call any user-defined code during cleanup, so if
         * the cleanup hook hasn't been run yet, too bad; we'll just skip it.
         */
        if PointerIsValid_opt(&(*portal).cleanup) {
            elog!(
                WARNING,
                "skipping cleanup for portal \"{}\"",
                cstr_to_str((*portal).name)
            );
            (*portal).cleanup = None;
        }

        /* Zap it. */
        PortalDrop(portal, false);
    }
}

/*
 * Portal-related cleanup when we return to the main loop on error.
 *
 * This is different from the cleanup at transaction abort.  Auto-held portals
 * are cleaned up on error but not on transaction abort.
 */
pub unsafe fn PortalErrorCleanup() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        if (*portal).autoHeld {
            (*portal).portalPinned = false;
            PortalDrop(portal, false);
        }
    }
}

/*
 * Pre-subcommit processing for portals.
 *
 * Reassign portals created or used in the current subtransaction to the
 * parent subtransaction.
 */
pub unsafe fn AtSubCommit_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    parentLevel: c_int,
    parentXactOwner: ResourceOwner,
) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        if (*portal).createSubid == mySubid {
            (*portal).createSubid = parentSubid;
            (*portal).createLevel = parentLevel;
            if !(*portal).resowner.is_null() {
                ResourceOwnerNewParent((*portal).resowner, parentXactOwner);
            }
        }
        if (*portal).activeSubid == mySubid {
            (*portal).activeSubid = parentSubid;
        }
    }
}

/*
 * Subtransaction abort handling for portals.
 *
 * Deactivate portals created or used during the failed subtransaction.
 * Note that per AtSubCommit_Portals, this will catch portals created/used
 * in descendants of the subtransaction too.
 *
 * We don't destroy any portals here; that's done in AtSubCleanup_Portals.
 */
pub unsafe fn AtSubAbort_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    myXactOwner: ResourceOwner,
    parentXactOwner: ResourceOwner,
) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        /* Was it created in this subtransaction? */
        if (*portal).createSubid != mySubid {
            /* No, but maybe it was used in this subtransaction? */
            if (*portal).activeSubid == mySubid {
                /* Maintain activeSubid until the portal is removed */
                (*portal).activeSubid = parentSubid;

                /*
                 * A MarkPortalActive() caller ran an upper-level portal in
                 * this subtransaction and left the portal ACTIVE.  This can't
                 * happen, but force the portal into FAILED state for the same
                 * reasons discussed below.
                 *
                 * We assume we can get away without forcing upper-level READY
                 * portals to fail, even if they were run and then suspended.
                 * In theory a suspended upper-level portal could have
                 * acquired some references to objects that are about to be
                 * destroyed, but there should be sufficient defenses against
                 * such cases: the portal's original query cannot contain such
                 * references, and any references within, say, cached plans of
                 * PL/pgSQL functions are not from active queries and should
                 * be protected by revalidation logic.
                 */
                if (*portal).status == PORTAL_ACTIVE {
                    MarkPortalFailed(portal);
                }

                /*
                 * Also, if we failed it during the current subtransaction
                 * (either just above, or earlier), reattach its resource
                 * owner to the current subtransaction's resource owner, so
                 * that any resources it still holds will be released while
                 * cleaning up this subtransaction.  This prevents some corner
                 * cases wherein we might get Asserts or worse while cleaning
                 * up objects created during the current subtransaction
                 * (because they're still referenced within this portal).
                 */
                if (*portal).status == PORTAL_FAILED && !(*portal).resowner.is_null() {
                    ResourceOwnerNewParent((*portal).resowner, myXactOwner);
                    (*portal).resowner = null_mut();
                }
            }
            /* Done if it wasn't created in this subtransaction */
            continue;
        }

        /*
         * Force any live portals of my own subtransaction into FAILED state.
         * We have to do this because they might refer to objects created or
         * changed in the failed subtransaction, leading to crashes within
         * ExecutorEnd when portalcmds.c tries to close down the portal.
         * Currently, every MarkPortalActive() caller ensures it updates the
         * portal status again before relinquishing control, so ACTIVE can't
         * happen here.  If it does happen, dispose the portal like existing
         * MarkPortalActive() callers would.
         */
        if (*portal).status == PORTAL_READY || (*portal).status == PORTAL_ACTIVE {
            MarkPortalFailed(portal);
        }

        /*
         * Allow portalcmds.c to clean up the state it knows about, if we
         * haven't already.
         */
        if let Some(cleanup) = (*portal).cleanup {
            cleanup(portal);
            (*portal).cleanup = None;
        }

        /* drop cached plan reference, if any */
        PortalReleaseCachedPlan(portal);

        /*
         * Any resources belonging to the portal will be released in the
         * upcoming transaction-wide cleanup; they will be gone before we run
         * PortalDrop.
         */
        (*portal).resowner = null_mut();

        /*
         * Although we can't delete the portal data structure proper, we can
         * release any memory in subsidiary contexts, such as executor state.
         * The cleanup hook was the last thing that might have needed data
         * there.
         */
        MemoryContextDeleteChildren((*portal).portalContext);
    }
}

/*
 * Post-subabort cleanup for portals.
 *
 * Drop all portals created in the failed subtransaction (but note that
 * we will not drop any that were reassigned to the parent above).
 */
pub unsafe fn AtSubCleanup_Portals(mySubid: SubTransactionId) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        if (*portal).createSubid != mySubid {
            continue;
        }

        /*
         * If a portal is still pinned, forcibly unpin it. PortalDrop will not
         * let us drop the portal otherwise. Whoever pinned the portal was
         * interrupted by the abort too and won't try to use it anymore.
         */
        if (*portal).portalPinned {
            (*portal).portalPinned = false;
        }

        /*
         * We had better not call any user-defined code during cleanup, so if
         * the cleanup hook hasn't been run yet, too bad; we'll just skip it.
         */
        if PointerIsValid_opt(&(*portal).cleanup) {
            elog!(
                WARNING,
                "skipping cleanup for portal \"{}\"",
                cstr_to_str((*portal).name)
            );
            (*portal).cleanup = None;
        }

        /* Zap it. */
        PortalDrop(portal, false);
    }
}

/* Find all available cursors */
pub unsafe fn pg_cursor(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut hash_seq: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    /*
     * We put all the tuples into a tuplestore in one scan of the hashtable.
     * This avoids any issue of the hashtable possibly changing between calls.
     */
    InitMaterializedSRF(fcinfo, 0);

    hash_seq_init(&raw mut hash_seq, PortalHashTable);
    loop {
        hentry = hash_seq_search(&raw mut hash_seq) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;
        let mut values: [Datum; 6] = [0; 6];
        let mut nulls: [bool; 6] = [false; 6];

        /* report only "visible" entries */
        if !(*portal).visible {
            continue;
        }
        /* also ignore it if PortalDefineQuery hasn't been called yet */
        if (*portal).sourceText.is_null() {
            continue;
        }

        values[0] = CStringGetTextDatum((*portal).name);
        values[1] = CStringGetTextDatum((*portal).sourceText);
        values[2] = BoolGetDatum(((*portal).cursorOptions & CURSOR_OPT_HOLD) != 0);
        values[3] = BoolGetDatum(((*portal).cursorOptions & CURSOR_OPT_BINARY) != 0);
        values[4] = BoolGetDatum(((*portal).cursorOptions & CURSOR_OPT_SCROLL) != 0);
        values[5] = TimestampTzGetDatum((*portal).creation_time);

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    0 as Datum
}

pub unsafe fn ThereAreNoReadyPortals() -> bool {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        if (*portal).status == PORTAL_READY {
            return false;
        }
    }

    true
}

/*
 * Hold all pinned portals.
 *
 * When initiating a COMMIT or ROLLBACK inside a procedure, this must be
 * called to protect internally-generated cursors from being dropped during
 * the transaction shutdown.  Currently, SPI calls this automatically; PLs
 * that initiate COMMIT or ROLLBACK some other way are on the hook to do it
 * themselves.  (Note that we couldn't do this in, say, AtAbort_Portals
 * because we need to run user-defined code while persisting a portal.
 * It's too late to do that once transaction abort has started.)
 *
 * We protect such portals by converting them to held cursors.  We mark them
 * as "auto-held" so that exception exit knows to clean them up.  (In normal,
 * non-exception code paths, the PL needs to clean such portals itself, since
 * transaction end won't do it anymore; but that should be normal practice
 * anyway.)
 */
pub unsafe fn HoldPinnedPortals() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;

    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        if (*portal).portalPinned && !(*portal).autoHeld {
            /*
             * Doing transaction control, especially abort, inside a cursor
             * loop that is not read-only, for example using UPDATE ...
             * RETURNING, has weird semantics issues.  Also, this
             * implementation wouldn't work, because such portals cannot be
             * held.  (The core grammar enforces that only SELECT statements
             * can drive a cursor, but for example PL/pgSQL does not restrict
             * it.)
             */
            if (*portal).strategy != PORTAL_ONE_SELECT {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot perform transaction commands inside a cursor loop that is not read-only"
                    )
                );
            }

            /* Verify it's in a suitable state to be held */
            if (*portal).status != PORTAL_READY {
                elog!(ERROR, "pinned portal is not ready to be auto-held");
            }

            HoldPortal(portal);
            (*portal).autoHeld = true;
        }
    }
}

/*
 * Drop the outer active snapshots for all portals, so that no snapshots
 * remain active.
 *
 * Like HoldPinnedPortals, this must be called when initiating a COMMIT or
 * ROLLBACK inside a procedure.  This has to be separate from that since it
 * should not be run until we're done with steps that are likely to fail.
 *
 * It's tempting to fold this into PreCommit_Portals, but to do so, we'd
 * need to clean up snapshot management in VACUUM and perhaps other places.
 */
pub unsafe fn ForgetPortalSnapshots() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut PortalHashEnt;
    let mut numPortalSnaps: c_int = 0;
    let mut numActiveSnaps: c_int = 0;

    /* First, scan PortalHashTable and clear portalSnapshot fields */
    hash_seq_init(&raw mut status, PortalHashTable);

    loop {
        hentry = hash_seq_search(&raw mut status) as *mut PortalHashEnt;
        if hentry.is_null() {
            break;
        }
        let portal: Portal = (*hentry).portal;

        if !(*portal).portalSnapshot.is_null() {
            (*portal).portalSnapshot = null_mut();
            numPortalSnaps += 1;
        }
        /* portal->holdSnapshot will be cleaned up in PreCommit_Portals */
    }

    /*
     * Now, pop all the active snapshots, which should be just those that were
     * portal snapshots.  Ideally we'd drive this directly off the portal
     * scan, but there's no good way to visit the portals in the correct
     * order.  So just cross-check after the fact.
     */
    while ActiveSnapshotSet() {
        PopActiveSnapshot();
        numActiveSnaps += 1;
    }

    if numPortalSnaps != numActiveSnaps {
        elog!(
            ERROR,
            "portal snapshots ({}) did not account for all active snapshots ({})",
            numPortalSnaps,
            numActiveSnaps
        );
    }
}

// ---------------------------------------------------------------------------
// Small local helpers for the translation.
// ---------------------------------------------------------------------------

/// Render a NUL-terminated C string for `{}` formatting inside elog!/ereport!.
#[inline]
unsafe fn cstr_to_str<'a>(s: *const c_char) -> std::borrow::Cow<'a, str> {
    if s.is_null() {
        std::borrow::Cow::Borrowed("(null)")
    } else {
        std::ffi::CStr::from_ptr(s).to_string_lossy()
    }
}

/// C `PointerIsValid(portal->cleanup)` over a function-pointer field that the
/// Rust translation models as `Option<fn>`.
#[inline]
unsafe fn PointerIsValid_opt(
    cleanup: &Option<unsafe extern "C" fn(portal: Portal)>,
) -> bool {
    cleanup.is_some()
}
