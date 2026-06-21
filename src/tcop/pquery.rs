/*-------------------------------------------------------------------------
 *
 * pquery.c
 *	  POSTGRES process query command code
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/tcop/pquery.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{IsA, foreach, current_cell, linitial_node, lfirst_node, Assert};

use core::ffi::{c_char, c_int, c_long};

use crate::c::{int16, uint64};

// access/sdir.h
use crate::access::sdir::{
    ScanDirection, BackwardScanDirection, ForwardScanDirection, NoMovementScanDirection,
    ScanDirectionIsForward, ScanDirectionIsNoMovement,
};
// access/xact.h
use crate::access::transam::xact::{
    CommandCounterIncrement, CurrentMemoryContext, TopTransactionContext,
    TopTransactionResourceOwner,
};

// commands/prepare.h
use crate::commands::prepare::{
    FetchPreparedStatement, FetchPreparedStatementTargetList, PreparedStatement,
};

// miscadmin.h
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

// executor/execdesc.h
use crate::executor::execdesc::QueryDesc;
// executor/executor.h
use crate::executor::executor::{
    ExecCleanTypeFromTL, ExecutorEnd, ExecutorFinish, ExecutorRewind, ExecutorRun, ExecutorStart,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_REWIND,
};
// executor/execTuples.h (executor/tuptable.h)
use crate::executor::execTuples::{
    ExecDropSingleTupleTableSlot, MakeSingleTupleTableSlot, TTSOpsMinimalTuple,
};
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
// executor/tstoreReceiver.h
use crate::executor::tstoreReceiver::SetTuplestoreDestReceiverParams;

// nodes/execnodes.h (EState, via QueryDesc)
// nodes/nodes.h
use crate::nodes::nodes::{nodeTag, CmdType, Node, NodeTag};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::nodes::NodeTag::*;
// nodes/params.h
use crate::nodes::params::ParamListInfo;
// nodes/parsenodes.h
use crate::nodes::parsenodes::{
    ExecuteStmt, FetchDirection, FetchStmt, Query, FETCH_ALL,
};
use crate::nodes::parsenodes::FetchDirection::*;
// nodes/pg_list.h
use crate::nodes::pg_list::{linitial, lfirst, list_length, lnext, List, NIL};
// nodes/plannodes.h
use crate::nodes::plannodes::PlannedStmt;

// tcop/cmdtag.h
use crate::tcop::cmdtag::{
    CommandTag, CopyQueryCompletion, InitializeQueryCompletion, QueryCompletion,
    SetQueryCompletion,
};
use crate::tcop::cmdtag::CommandTag::*;
// tcop/dest.h
use crate::tcop::dest::{
    CreateDestReceiver, DestReceiver, None_Receiver,
};
use crate::tcop::dest::CommandDest::{DestNone, DestRemoteExecute, DestTuplestore};
// tcop/tcopprot.h (executor stats hooks)
use crate::tcop::tcopprot::{ResetUsage, ShowUsage};
// tcop/postgres.c GUC
use crate::tcop::postgres::log_executor_stats;
// tcop/utility.h
use crate::tcop::utility::{
    ProcessUtility, UtilityReturnsTuples, UtilityTupleDescriptor, PROCESS_UTILITY_QUERY,
    PROCESS_UTILITY_TOPLEVEL,
};

// utils/memutils.h (mmgr)
use crate::utils::mmgr::mcxt::{
    MemoryContextAlloc, MemoryContextDeleteChildren, MemoryContextSwitchTo, PortalContext,
};
use crate::utils::mmgr::memnodes::MemoryContext;
// utils/portal.h
use crate::utils::portal::{
    MarkPortalActive, MarkPortalDone, MarkPortalFailed, Portal, PortalCreateHoldStore,
    PortalGetPrimaryStmt, PortalIsValid, GetPortalByName,
    PORTAL_DEFINED, PORTAL_MULTI_QUERY, PORTAL_ONE_MOD_WITH, PORTAL_ONE_RETURNING,
    PORTAL_ONE_SELECT, PORTAL_READY, PORTAL_UTIL_SELECT,
};
// utils/resowner.h
use crate::utils::resowner::resowner::{CurrentResourceOwner, ResourceOwner};
// utils/snapmgr.h
use crate::utils::time::snapmgr::{
    ActiveSnapshotSet, GetActiveSnapshot, GetTransactionSnapshot, PopActiveSnapshot,
    PushActiveSnapshot, PushActiveSnapshotWithLevel, PushCopiedSnapshot, RegisterSnapshot,
    UnregisterSnapshot, UpdateActiveSnapshotCommandId,
};
// utils/snapshot.h
use crate::utils::snapshot::{InvalidSnapshot, Snapshot};
// utils/sort/tuplestore.h (module not yet wired; local stubs below)

// ---------------------------------------------------------------------------
// DECLARE CURSOR option bits (utils/portal.h)
// ---------------------------------------------------------------------------
// TODO(pg-port): dedup when utils/portal.h fully lands these constants.
const CURSOR_OPT_SCROLL: c_int = 0x0002;
const CURSOR_OPT_NO_SCROLL: c_int = 0x0004;

// ---------------------------------------------------------------------------
// pg_trace.h tracepoints (no-ops in this port).
// ---------------------------------------------------------------------------
#[inline]
fn TRACE_POSTGRESQL_QUERY_EXECUTE_START() {}
#[inline]
fn TRACE_POSTGRESQL_QUERY_EXECUTE_DONE() {}

// utils/sort/tuplestore.h stubs (module not yet wired); cursor/held-portal paths only.
unsafe fn tuplestore_gettupleslot(
    state: *mut c_void,
    forward: bool,
    copy: bool,
    slot: *mut TupleTableSlot,
) -> bool { crate::utils::sort::tuplestore::tuplestore_gettupleslot(state as _, forward, copy, slot as _) }
unsafe fn tuplestore_rescan(state: *mut c_void) { crate::utils::sort::tuplestore::tuplestore_rescan(state as _) }

/*
 * ActivePortal is the currently executing Portal (the most closely nested,
 * if there are several).
 */
#[no_mangle]
pub static mut ActivePortal: Portal = core::ptr::null_mut();

/*
 * CreateQueryDesc
 */
pub unsafe fn CreateQueryDesc(
    plannedstmt: *mut PlannedStmt,
    sourceText: *const c_char,
    snapshot: Snapshot,
    crosscheck_snapshot: Snapshot,
    dest: *mut DestReceiver,
    params: ParamListInfo,
    queryEnv: *mut crate::utils::misc::queryenvironment::QueryEnvironment,
    instrument_options: c_int,
) -> *mut QueryDesc {
    let qd = palloc(core::mem::size_of::<QueryDesc>()) as *mut QueryDesc;

    (*qd).operation = (*plannedstmt).commandType; /* operation */
    (*qd).plannedstmt = plannedstmt; /* plan */
    (*qd).sourceText = sourceText; /* query text */
    (*qd).snapshot = RegisterSnapshot(snapshot) as _; /* snapshot */
    /* RI check snapshot */
    (*qd).crosscheck_snapshot = RegisterSnapshot(crosscheck_snapshot) as _;
    (*qd).dest = dest; /* output dest */
    (*qd).params = params; /* parameter values passed into query */
    (*qd).queryEnv = queryEnv;
    (*qd).instrument_options = instrument_options; /* instrumentation wanted? */

    /* null these fields until set by ExecutorStart */
    (*qd).tupDesc = core::ptr::null_mut();
    (*qd).estate = core::ptr::null_mut();
    (*qd).planstate = core::ptr::null_mut();
    (*qd).totaltime = core::ptr::null_mut();

    /* not yet executed */
    (*qd).already_executed = false;

    qd
}

/*
 * FreeQueryDesc
 */
pub unsafe fn FreeQueryDesc(qdesc: *mut QueryDesc) {
    /* Can't be a live query */
    Assert!((*qdesc).estate.is_null());

    /* forget our snapshots */
    UnregisterSnapshot((*qdesc).snapshot as _);
    UnregisterSnapshot((*qdesc).crosscheck_snapshot as _);

    /* Only the QueryDesc itself need be freed */
    pfree(qdesc as *mut c_void);
}

/*
 * ProcessQuery
 *		Execute a single plannable query within a PORTAL_MULTI_QUERY,
 *		PORTAL_ONE_RETURNING, or PORTAL_ONE_MOD_WITH portal
 *
 *	plan: the plan tree for the query
 *	sourceText: the source text of the query
 *	params: any parameters needed
 *	dest: where to send results
 *	qc: where to store the command completion status data.
 *
 * qc may be NULL if caller doesn't want a status string.
 *
 * Must be called in a memory context that will be reset or deleted on
 * error; otherwise the executor's memory usage will be leaked.
 */
unsafe fn ProcessQuery(
    plan: *mut PlannedStmt,
    sourceText: *const c_char,
    params: ParamListInfo,
    queryEnv: *mut crate::utils::misc::queryenvironment::QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let queryDesc: *mut QueryDesc;

    /*
     * Create the QueryDesc object
     */
    queryDesc = CreateQueryDesc(
        plan,
        sourceText,
        GetActiveSnapshot(),
        InvalidSnapshot,
        dest,
        params,
        queryEnv,
        0,
    );

    /*
     * Call ExecutorStart to prepare the plan for execution
     */
    ExecutorStart(queryDesc, 0);

    /*
     * Run the plan to completion.
     */
    ExecutorRun(queryDesc, ForwardScanDirection, 0);

    /*
     * Build command completion status data, if caller wants one.
     */
    if !qc.is_null() {
        match (*queryDesc).operation {
            CMD_SELECT => {
                SetQueryCompletion(&mut *qc, CMDTAG_SELECT, (*(*queryDesc).estate).es_processed);
            }
            CMD_INSERT => {
                SetQueryCompletion(&mut *qc, CMDTAG_INSERT, (*(*queryDesc).estate).es_processed);
            }
            CMD_UPDATE => {
                SetQueryCompletion(&mut *qc, CMDTAG_UPDATE, (*(*queryDesc).estate).es_processed);
            }
            CMD_DELETE => {
                SetQueryCompletion(&mut *qc, CMDTAG_DELETE, (*(*queryDesc).estate).es_processed);
            }
            CMD_MERGE => {
                SetQueryCompletion(&mut *qc, CMDTAG_MERGE, (*(*queryDesc).estate).es_processed);
            }
            _ => {
                SetQueryCompletion(&mut *qc, CMDTAG_UNKNOWN, (*(*queryDesc).estate).es_processed);
            }
        }
    }

    /*
     * Now, we close down all the scans and free allocated resources.
     */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);
}

/*
 * ChoosePortalStrategy
 *		Select portal execution strategy given the intended statement list.
 *
 * The list elements can be Querys or PlannedStmts.
 * That's more general than portals need, but plancache.c uses this too.
 *
 * See the comments in portal.h.
 */
pub unsafe fn ChoosePortalStrategy(stmts: *mut List) -> crate::utils::portal::PortalStrategy {
    let mut nSetTag: c_int;

    /*
     * PORTAL_ONE_SELECT and PORTAL_UTIL_SELECT need only consider the
     * single-statement case, since there are no rewrite rules that can add
     * auxiliary queries to a SELECT or a utility command. PORTAL_ONE_MOD_WITH
     * likewise allows only one top-level statement.
     */
    if list_length(stmts) == 1 {
        let stmt = linitial(stmts) as *mut Node;

        if IsA!(stmt, T_Query) {
            let query = stmt as *mut Query;

            if (*query).canSetTag {
                if (*query).commandType == CMD_SELECT {
                    if (*query).hasModifyingCTE {
                        return PORTAL_ONE_MOD_WITH;
                    } else {
                        return PORTAL_ONE_SELECT;
                    }
                }
                if (*query).commandType == CMD_UTILITY {
                    if UtilityReturnsTuples((*query).utilityStmt) {
                        return PORTAL_UTIL_SELECT;
                    }
                    /* it can't be ONE_RETURNING, so give up */
                    return PORTAL_MULTI_QUERY;
                }
            }
        } else if IsA!(stmt, T_PlannedStmt) {
            let pstmt = stmt as *mut PlannedStmt;

            if (*pstmt).canSetTag {
                if (*pstmt).commandType == CMD_SELECT {
                    if (*pstmt).hasModifyingCTE {
                        return PORTAL_ONE_MOD_WITH;
                    } else {
                        return PORTAL_ONE_SELECT;
                    }
                }
                if (*pstmt).commandType == CMD_UTILITY {
                    if UtilityReturnsTuples((*pstmt).utilityStmt) {
                        return PORTAL_UTIL_SELECT;
                    }
                    /* it can't be ONE_RETURNING, so give up */
                    return PORTAL_MULTI_QUERY;
                }
            }
        } else {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(stmt) as c_int);
        }
    }

    /*
     * PORTAL_ONE_RETURNING has to allow auxiliary queries added by rewrite.
     * Choose PORTAL_ONE_RETURNING if there is exactly one canSetTag query and
     * it has a RETURNING list.
     */
    nSetTag = 0;
    foreach!(lc, stmts, {
        let stmt = lfirst(current_cell!(lc)) as *mut Node;

        if IsA!(stmt, T_Query) {
            let query = stmt as *mut Query;

            if (*query).canSetTag {
                nSetTag += 1;
                if nSetTag > 1 {
                    return PORTAL_MULTI_QUERY; /* no need to look further */
                }
                if (*query).commandType == CMD_UTILITY || (*query).returningList == NIL {
                    return PORTAL_MULTI_QUERY; /* no need to look further */
                }
            }
        } else if IsA!(stmt, T_PlannedStmt) {
            let pstmt = stmt as *mut PlannedStmt;

            if (*pstmt).canSetTag {
                nSetTag += 1;
                if nSetTag > 1 {
                    return PORTAL_MULTI_QUERY; /* no need to look further */
                }
                if (*pstmt).commandType == CMD_UTILITY || !(*pstmt).hasReturning {
                    return PORTAL_MULTI_QUERY; /* no need to look further */
                }
            }
        } else {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(stmt) as c_int);
        }
    });
    if nSetTag == 1 {
        return PORTAL_ONE_RETURNING;
    }

    /* Else, it's the general case... */
    PORTAL_MULTI_QUERY
}

/*
 * FetchPortalTargetList
 *		Given a portal that returns tuples, extract the query targetlist.
 *		Returns NIL if the portal doesn't have a determinable targetlist.
 *
 * Note: do not modify the result.
 */
pub unsafe fn FetchPortalTargetList(portal: Portal) -> *mut List {
    /* no point in looking if we determined it doesn't return tuples */
    if (*portal).strategy == PORTAL_MULTI_QUERY {
        return NIL;
    }
    /* get the primary statement and find out what it returns */
    FetchStatementTargetList(PortalGetPrimaryStmt(portal) as *mut Node)
}

/*
 * FetchStatementTargetList
 *		Given a statement that returns tuples, extract the query targetlist.
 *		Returns NIL if the statement doesn't have a determinable targetlist.
 *
 * This can be applied to a Query or a PlannedStmt.
 * That's more general than portals need, but plancache.c uses this too.
 *
 * Note: do not modify the result.
 *
 * XXX be careful to keep this in sync with UtilityReturnsTuples.
 */
pub unsafe fn FetchStatementTargetList(mut stmt: *mut Node) -> *mut List {
    if stmt.is_null() {
        return NIL;
    }
    if IsA!(stmt, T_Query) {
        let query = stmt as *mut Query;

        if (*query).commandType == CMD_UTILITY {
            /* transfer attention to utility statement */
            stmt = (*query).utilityStmt;
        } else {
            if (*query).commandType == CMD_SELECT {
                return (*query).targetList;
            }
            if !(*query).returningList.is_null() {
                return (*query).returningList;
            }
            return NIL;
        }
    }
    if IsA!(stmt, T_PlannedStmt) {
        let pstmt = stmt as *mut PlannedStmt;

        if (*pstmt).commandType == CMD_UTILITY {
            /* transfer attention to utility statement */
            stmt = (*pstmt).utilityStmt;
        } else {
            if (*pstmt).commandType == CMD_SELECT {
                return (*(*pstmt).planTree).targetlist;
            }
            if (*pstmt).hasReturning {
                return (*(*pstmt).planTree).targetlist;
            }
            return NIL;
        }
    }
    if IsA!(stmt, T_FetchStmt) {
        let fstmt = stmt as *mut FetchStmt;
        let subportal: Portal;

        Assert!(!(*fstmt).ismove);
        subportal = GetPortalByName((*fstmt).portalname);
        Assert!(PortalIsValid(subportal));
        return FetchPortalTargetList(subportal);
    }
    if IsA!(stmt, T_ExecuteStmt) {
        let estmt = stmt as *mut ExecuteStmt;
        let entry: *mut PreparedStatement;

        entry = FetchPreparedStatement((*estmt).name, true);
        return FetchPreparedStatementTargetList(entry);
    }
    NIL
}

/*
 * PortalStart
 *		Prepare a portal for execution.
 *
 * Caller must already have created the portal, done PortalDefineQuery(),
 * and adjusted portal options if needed.
 *
 * If parameters are needed by the query, they must be passed in "params"
 * (caller is responsible for giving them appropriate lifetime).
 *
 * The caller can also provide an initial set of "eflags" to be passed to
 * ExecutorStart (but note these can be modified internally, and they are
 * currently only honored for PORTAL_ONE_SELECT portals).  Most callers
 * should simply pass zero.
 *
 * The caller can optionally pass a snapshot to be used; pass InvalidSnapshot
 * for the normal behavior of setting a new snapshot.  This parameter is
 * presently ignored for non-PORTAL_ONE_SELECT portals (it's only intended
 * to be used for cursors).
 *
 * On return, portal is ready to accept PortalRun() calls, and the result
 * tupdesc (if any) is known.
 */
pub unsafe fn PortalStart(
    portal: Portal,
    params: ParamListInfo,
    eflags: c_int,
    snapshot: Snapshot,
) {
    let saveActivePortal: Portal;
    let saveResourceOwner: ResourceOwner;
    let savePortalContext: MemoryContext;
    let mut oldContext: MemoryContext = core::ptr::null_mut();

    Assert!(PortalIsValid(portal));
    Assert!((*portal).status == PORTAL_DEFINED);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;

    // PG_TRY();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let queryDesc: *mut QueryDesc;
        let myeflags: c_int;

        ActivePortal = portal;
        if !(*portal).resowner.is_null() {
            CurrentResourceOwner = (*portal).resowner;
        }
        PortalContext = (*portal).portalContext;

        oldContext = MemoryContextSwitchTo(PortalContext);

        /* Must remember portal param list, if any */
        (*portal).portalParams = params;

        /*
         * Determine the portal execution strategy
         */
        (*portal).strategy = ChoosePortalStrategy((*portal).stmts);

        /*
         * Fire her up according to the strategy
         */
        match (*portal).strategy {
            PORTAL_ONE_SELECT => {
                /* Must set snapshot before starting executor. */
                if !snapshot.is_null() {
                    PushActiveSnapshot(snapshot);
                } else {
                    PushActiveSnapshot(GetTransactionSnapshot());
                }

                /*
                 * We could remember the snapshot in portal->portalSnapshot,
                 * but presently there seems no need to, as this code path
                 * cannot be used for non-atomic execution.  Hence there can't
                 * be any commit/abort that might destroy the snapshot.  Since
                 * we don't do that, there's also no need to force a
                 * non-default nesting level for the snapshot.
                 */

                /*
                 * Create QueryDesc in portal's context; for the moment, set
                 * the destination to DestNone.
                 */
                let queryDesc_l = CreateQueryDesc(
                    linitial_node!(PlannedStmt, T_PlannedStmt, (*portal).stmts),
                    (*portal).sourceText,
                    GetActiveSnapshot(),
                    InvalidSnapshot,
                    None_Receiver(),
                    params,
                    (*portal).queryEnv,
                    0,
                );
                queryDesc = queryDesc_l;

                /*
                 * If it's a scrollable cursor, executor needs to support
                 * REWIND and backwards scan, as well as whatever the caller
                 * might've asked for.
                 */
                if (*portal).cursorOptions & CURSOR_OPT_SCROLL != 0 {
                    myeflags = eflags | EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD;
                } else {
                    myeflags = eflags;
                }

                /*
                 * Call ExecutorStart to prepare the plan for execution
                 */
                ExecutorStart(queryDesc, myeflags);

                /*
                 * This tells PortalCleanup to shut down the executor
                 */
                (*portal).queryDesc = queryDesc;

                /*
                 * Remember tuple descriptor (computed by ExecutorStart)
                 */
                (*portal).tupDesc = (*queryDesc).tupDesc;

                /*
                 * Reset cursor position data to "start of query"
                 */
                (*portal).atStart = true;
                (*portal).atEnd = false; /* allow fetches */
                (*portal).portalPos = 0;

                PopActiveSnapshot();
            }

            PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH => {
                /*
                 * We don't start the executor until we are told to run the
                 * portal.  We do need to set up the result tupdesc.
                 */
                {
                    let pstmt: *mut PlannedStmt;

                    pstmt = PortalGetPrimaryStmt(portal);
                    (*portal).tupDesc = ExecCleanTypeFromTL((*(*pstmt).planTree).targetlist);
                }

                /*
                 * Reset cursor position data to "start of query"
                 */
                (*portal).atStart = true;
                (*portal).atEnd = false; /* allow fetches */
                (*portal).portalPos = 0;
            }

            PORTAL_UTIL_SELECT => {
                /*
                 * We don't set snapshot here, because PortalRunUtility will
                 * take care of it if needed.
                 */
                {
                    let pstmt: *mut PlannedStmt = PortalGetPrimaryStmt(portal);

                    Assert!((*pstmt).commandType == CMD_UTILITY);
                    (*portal).tupDesc = UtilityTupleDescriptor((*pstmt).utilityStmt) as _;
                }

                /*
                 * Reset cursor position data to "start of query"
                 */
                (*portal).atStart = true;
                (*portal).atEnd = false; /* allow fetches */
                (*portal).portalPos = 0;
            }

            PORTAL_MULTI_QUERY => {
                /* Need do nothing now */
                (*portal).tupDesc = core::ptr::null_mut();
            }

            _ => {}
        }
    }));

    // PG_CATCH();
    if let Err(e) = result {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        // PG_RE_THROW();
        std::panic::resume_unwind(e);
    }
    // PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    (*portal).status = PORTAL_READY;
}

/*
 * PortalSetResultFormat
 *		Select the format codes for a portal's output.
 *
 * This must be run after PortalStart for a portal that will be read by
 * a DestRemote or DestRemoteExecute destination.  It is not presently needed
 * for other destination types.
 *
 * formats[] is the client format request, as per Bind message conventions.
 */
pub unsafe fn PortalSetResultFormat(portal: Portal, nFormats: c_int, formats: *mut int16) {
    let natts: c_int;
    let mut i: c_int;

    /* Do nothing if portal won't return tuples */
    if (*portal).tupDesc.is_null() {
        return;
    }
    natts = (*(*portal).tupDesc).natts;
    (*portal).formats = MemoryContextAlloc(
        (*portal).portalContext,
        natts as usize * core::mem::size_of::<int16>(),
    ) as *mut int16;
    if nFormats > 1 {
        /* format specified for each column */
        if nFormats != natts {
            ereport!(
                ERROR,
                errmsg!(
                    "bind message has {} result formats but query has {} columns",
                    nFormats,
                    natts
                )
            );
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        }
        core::ptr::copy_nonoverlapping(
            formats,
            (*portal).formats,
            natts as usize,
        );
    } else if nFormats > 0 {
        /* single format specified, use for all columns */
        let format1: int16 = *formats.add(0);

        i = 0;
        while i < natts {
            *(*portal).formats.add(i as usize) = format1;
            i += 1;
        }
    } else {
        /* use default format for all columns */
        i = 0;
        while i < natts {
            *(*portal).formats.add(i as usize) = 0;
            i += 1;
        }
    }
}

/*
 * PortalRun
 *		Run a portal's query or queries.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  Note that count is ignored in multi-query
 * situations, where we always run the portal to completion.
 *
 * isTopLevel: true if query is being executed at backend "top level"
 * (that is, directly from a client command message)
 *
 * dest: where to send output of primary (canSetTag) query
 *
 * altdest: where to send output of non-primary queries
 *
 * qc: where to store command completion status data.
 *		May be NULL if caller doesn't want status data.
 *
 * Returns true if the portal's execution is complete, false if it was
 * suspended due to exhaustion of the count parameter.
 */
pub unsafe fn PortalRun(
    portal: Portal,
    count: c_long,
    isTopLevel: bool,
    dest: *mut DestReceiver,
    altdest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) -> bool {
    let mut result: bool = false;
    let saveTopTransactionResourceOwner: ResourceOwner;
    let saveTopTransactionContext: MemoryContext;
    let saveActivePortal: Portal;
    let saveResourceOwner: ResourceOwner;
    let savePortalContext: MemoryContext;
    let saveMemoryContext: MemoryContext;

    Assert!(PortalIsValid(portal));

    TRACE_POSTGRESQL_QUERY_EXECUTE_START();

    /* Initialize empty completion data */
    if !qc.is_null() {
        InitializeQueryCompletion(&mut *qc);
    }

    if log_executor_stats && (*portal).strategy != PORTAL_MULTI_QUERY {
        elog!(DEBUG3, "PortalRun");
        /* PORTAL_MULTI_QUERY logs its own stats per query */
        ResetUsage();
    }

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    /*
     * Set up global portal context pointers.
     *
     * We have to play a special game here to support utility commands like
     * VACUUM and CLUSTER, which internally start and commit transactions.
     * When we are called to execute such a command, CurrentResourceOwner will
     * be pointing to the TopTransactionResourceOwner --- which will be
     * destroyed and replaced in the course of the internal commit and
     * restart.  So we need to be prepared to restore it as pointing to the
     * exit-time TopTransactionResourceOwner.  (Ain't that ugly?  This idea of
     * internally starting whole new transactions is not good.)
     * CurrentMemoryContext has a similar problem, but the other pointers we
     * save here will be NULL or pointing to longer-lived objects.
     */
    saveTopTransactionResourceOwner = TopTransactionResourceOwner as _;
    saveTopTransactionContext = TopTransactionContext as _;
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;
    saveMemoryContext = CurrentMemoryContext as _;

    // PG_TRY();
    let try_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let nprocessed: uint64;

        ActivePortal = portal;
        if !(*portal).resowner.is_null() {
            CurrentResourceOwner = (*portal).resowner;
        }
        PortalContext = (*portal).portalContext;

        MemoryContextSwitchTo(PortalContext);

        match (*portal).strategy {
            PORTAL_ONE_SELECT | PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH
            | PORTAL_UTIL_SELECT => {
                /*
                 * If we have not yet run the command, do so, storing its
                 * results in the portal's tuplestore.  But we don't do that
                 * for the PORTAL_ONE_SELECT case.
                 */
                if (*portal).strategy != PORTAL_ONE_SELECT && (*portal).holdStore.is_null() {
                    FillPortalStore(portal, isTopLevel);
                }

                /*
                 * Now fetch desired portion of results.
                 */
                nprocessed = PortalRunSelect(portal, true, count, dest);

                /*
                 * If the portal result contains a command tag and the caller
                 * gave us a pointer to store it, copy it and update the
                 * rowcount.
                 */
                if !qc.is_null() && (*portal).qc.commandTag != CMDTAG_UNKNOWN {
                    CopyQueryCompletion(&mut *qc, &(*portal).qc);
                    (*qc).nprocessed = nprocessed;
                }

                /* Mark portal not active */
                (*portal).status = PORTAL_READY;

                /*
                 * Since it's a forward fetch, say DONE iff atEnd is now true.
                 */
                result = (*portal).atEnd;
            }

            PORTAL_MULTI_QUERY => {
                PortalRunMulti(portal, isTopLevel, false, dest, altdest, qc);

                /* Prevent portal's commands from being re-executed */
                MarkPortalDone(portal);

                /* Always complete at end of RunMulti */
                result = true;
            }

            _ => {
                elog!(
                    ERROR,
                    "unrecognized portal strategy: {}",
                    (*portal).strategy as c_int
                );
                result = false; /* keep compiler quiet */
            }
        }
    }));

    // PG_CATCH();
    if let Err(e) = try_result {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        if saveMemoryContext == saveTopTransactionContext {
            MemoryContextSwitchTo(TopTransactionContext as _);
        } else {
            MemoryContextSwitchTo(saveMemoryContext);
        }
        ActivePortal = saveActivePortal;
        if saveResourceOwner == saveTopTransactionResourceOwner {
            CurrentResourceOwner = TopTransactionResourceOwner as _;
        } else {
            CurrentResourceOwner = saveResourceOwner;
        }
        PortalContext = savePortalContext;

        // PG_RE_THROW();
        std::panic::resume_unwind(e);
    }
    // PG_END_TRY();

    if saveMemoryContext == saveTopTransactionContext {
        MemoryContextSwitchTo(TopTransactionContext as _);
    } else {
        MemoryContextSwitchTo(saveMemoryContext);
    }
    ActivePortal = saveActivePortal;
    if saveResourceOwner == saveTopTransactionResourceOwner {
        CurrentResourceOwner = TopTransactionResourceOwner as _;
    } else {
        CurrentResourceOwner = saveResourceOwner;
    }
    PortalContext = savePortalContext;

    if log_executor_stats && (*portal).strategy != PORTAL_MULTI_QUERY {
        ShowUsage(c"EXECUTOR STATISTICS".as_ptr());
    }

    TRACE_POSTGRESQL_QUERY_EXECUTE_DONE();

    result
}

/*
 * PortalRunSelect
 *		Execute a portal's query in PORTAL_ONE_SELECT mode, and also
 *		when fetching from a completed holdStore in PORTAL_ONE_RETURNING,
 *		PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT cases.
 *
 * This handles simple N-rows-forward-or-backward cases.  For more complex
 * nonsequential access to a portal, see PortalRunFetch.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  (cf FetchStmt.howMany)
 *
 * Caller must already have validated the Portal and done appropriate
 * setup (cf. PortalRun).
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
unsafe fn PortalRunSelect(
    portal: Portal,
    forward: bool,
    mut count: c_long,
    dest: *mut DestReceiver,
) -> uint64 {
    let queryDesc: *mut QueryDesc;
    let mut direction: ScanDirection;
    let mut nprocessed: uint64;

    /*
     * NB: queryDesc will be NULL if we are fetching from a held cursor or a
     * completed utility query; can't use it in that path.
     */
    queryDesc = (*portal).queryDesc;

    /* Caller messed up if we have neither a ready query nor held data. */
    Assert!(!queryDesc.is_null() || !(*portal).holdStore.is_null());

    /*
     * Force the queryDesc destination to the right thing.  This supports
     * MOVE, for example, which will pass in dest = DestNone.  This is okay to
     * change as long as we do it on every fetch.  (The Executor must not
     * assume that dest never changes.)
     */
    if !queryDesc.is_null() {
        (*queryDesc).dest = dest;
    }

    /*
     * Determine which direction to go in, and check to see if we're already
     * at the end of the available tuples in that direction.  If so, set the
     * direction to NoMovement to avoid trying to fetch any tuples.  (This
     * check exists because not all plan node types are robust about being
     * called again if they've already returned NULL once.)  Then call the
     * executor (we must not skip this, because the destination needs to see a
     * setup and shutdown even if no tuples are available).  Finally, update
     * the portal position state depending on the number of tuples that were
     * retrieved.
     */
    if forward {
        if (*portal).atEnd || count <= 0 {
            direction = NoMovementScanDirection;
            count = 0; /* don't pass negative count to executor */
        } else {
            direction = ForwardScanDirection;
        }

        /* In the executor, zero count processes all rows */
        if count == FETCH_ALL {
            count = 0;
        }

        if !(*portal).holdStore.is_null() {
            nprocessed = RunFromStore(portal, direction, count as uint64, dest);
        } else {
            PushActiveSnapshot((*queryDesc).snapshot as _);
            ExecutorRun(queryDesc, direction, count as uint64);
            nprocessed = (*(*queryDesc).estate).es_processed;
            PopActiveSnapshot();
        }

        if !ScanDirectionIsNoMovement(direction) {
            if nprocessed > 0 {
                (*portal).atStart = false; /* OK to go backward now */
            }
            if count == 0 || nprocessed < count as uint64 {
                (*portal).atEnd = true; /* we retrieved 'em all */
            }
            (*portal).portalPos += nprocessed;
        }
    } else {
        if (*portal).cursorOptions & CURSOR_OPT_NO_SCROLL != 0 {
            ereport!(
                ERROR,
                errmsg!("cursor can only scan forward")
            );
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            // C also: errhint("Declare it with SCROLL option to enable backward scan.")
        }

        if (*portal).atStart || count <= 0 {
            direction = NoMovementScanDirection;
            count = 0; /* don't pass negative count to executor */
        } else {
            direction = BackwardScanDirection;
        }

        /* In the executor, zero count processes all rows */
        if count == FETCH_ALL {
            count = 0;
        }

        if !(*portal).holdStore.is_null() {
            nprocessed = RunFromStore(portal, direction, count as uint64, dest);
        } else {
            PushActiveSnapshot((*queryDesc).snapshot as _);
            ExecutorRun(queryDesc, direction, count as uint64);
            nprocessed = (*(*queryDesc).estate).es_processed;
            PopActiveSnapshot();
        }

        if !ScanDirectionIsNoMovement(direction) {
            if nprocessed > 0 && (*portal).atEnd {
                (*portal).atEnd = false; /* OK to go forward now */
                (*portal).portalPos += 1; /* adjust for endpoint case */
            }
            if count == 0 || nprocessed < count as uint64 {
                (*portal).atStart = true; /* we retrieved 'em all */
                (*portal).portalPos = 0;
            } else {
                (*portal).portalPos -= nprocessed;
            }
        }
    }

    nprocessed
}

/*
 * FillPortalStore
 *		Run the query and load result tuples into the portal's tuple store.
 *
 * This is used for PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and
 * PORTAL_UTIL_SELECT cases only.
 */
unsafe fn FillPortalStore(portal: Portal, isTopLevel: bool) {
    let treceiver: *mut DestReceiver;
    let mut qc: QueryCompletion = core::mem::zeroed();

    InitializeQueryCompletion(&mut qc);
    PortalCreateHoldStore(portal);
    treceiver = CreateDestReceiver(DestTuplestore);
    SetTuplestoreDestReceiverParams(
        treceiver,
        (*portal).holdStore,
        (*portal).holdContext,
        false,
        core::ptr::null_mut(),
        core::ptr::null(),
    );

    match (*portal).strategy {
        PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH => {
            /*
             * Run the portal to completion just as for the default
             * PORTAL_MULTI_QUERY case, but send the primary query's output to
             * the tuplestore.  Auxiliary query outputs are discarded. Set the
             * portal's holdSnapshot to the snapshot used (or a copy of it).
             */
            PortalRunMulti(portal, isTopLevel, true, treceiver, None_Receiver(), &mut qc);
        }

        PORTAL_UTIL_SELECT => {
            PortalRunUtility(
                portal,
                linitial_node!(PlannedStmt, T_PlannedStmt, (*portal).stmts),
                isTopLevel,
                true,
                treceiver,
                &mut qc,
            );
        }

        _ => {
            elog!(
                ERROR,
                "unsupported portal strategy: {}",
                (*portal).strategy as c_int
            );
        }
    }

    /* Override portal completion data with actual command results */
    if qc.commandTag != CMDTAG_UNKNOWN {
        CopyQueryCompletion(&mut (*portal).qc, &qc);
    }

    ((*treceiver).rDestroy.unwrap())(treceiver);
}

/*
 * RunFromStore
 *		Fetch tuples from the portal's tuple store.
 *
 * Calling conventions are similar to ExecutorRun, except that we
 * do not depend on having a queryDesc or estate.  Therefore we return the
 * number of tuples processed as the result, not in estate->es_processed.
 *
 * One difference from ExecutorRun is that the destination receiver functions
 * are run in the caller's memory context (since we have no estate).  Watch
 * out for memory leaks.
 */
unsafe fn RunFromStore(
    portal: Portal,
    direction: ScanDirection,
    count: uint64,
    dest: *mut DestReceiver,
) -> uint64 {
    let mut current_tuple_count: uint64 = 0;
    let slot: *mut TupleTableSlot;

    slot = MakeSingleTupleTableSlot((*portal).tupDesc, &TTSOpsMinimalTuple);

    ((*dest).rStartup.unwrap())(dest, CMD_SELECT as c_int, (*portal).tupDesc);

    if ScanDirectionIsNoMovement(direction) {
        /* do nothing except start/stop the destination */
    } else {
        let forward: bool = ScanDirectionIsForward(direction);

        loop {
            let oldcontext: MemoryContext;
            let ok: bool;

            oldcontext = MemoryContextSwitchTo((*portal).holdContext);

            ok = tuplestore_gettupleslot((*portal).holdStore, forward, false, slot);

            MemoryContextSwitchTo(oldcontext);

            if !ok {
                break;
            }

            /*
             * If we are not able to send the tuple, we assume the destination
             * has closed and no more tuples can be sent. If that's the case,
             * end the loop.
             */
            if !((*dest).receiveSlot.unwrap())(slot, dest) {
                break;
            }

            ExecClearTuple(slot);

            /*
             * check our tuple count.. if we've processed the proper number
             * then quit, else loop again and process more tuples. Zero count
             * means no limit.
             */
            current_tuple_count += 1;
            if count != 0 && count == current_tuple_count {
                break;
            }
        }
    }

    ((*dest).rShutdown.unwrap())(dest);

    ExecDropSingleTupleTableSlot(slot);

    current_tuple_count
}

/*
 * PortalRunUtility
 *		Execute a utility statement inside a portal.
 */
unsafe fn PortalRunUtility(
    portal: Portal,
    pstmt: *mut PlannedStmt,
    isTopLevel: bool,
    setHoldSnapshot: bool,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    /*
     * Set snapshot if utility stmt needs one.
     */
    if PlannedStmtRequiresSnapshot(pstmt) {
        let mut snapshot: Snapshot = GetTransactionSnapshot();

        /* If told to, register the snapshot we're using and save in portal */
        if setHoldSnapshot {
            snapshot = RegisterSnapshot(snapshot);
            (*portal).holdSnapshot = snapshot;
        }

        /*
         * In any case, make the snapshot active and remember it in portal.
         * Because the portal now references the snapshot, we must tell
         * snapmgr.c that the snapshot belongs to the portal's transaction
         * level, else we risk portalSnapshot becoming a dangling pointer.
         */
        PushActiveSnapshotWithLevel(snapshot, (*portal).createLevel);
        /* PushActiveSnapshotWithLevel might have copied the snapshot */
        (*portal).portalSnapshot = GetActiveSnapshot();
    } else {
        (*portal).portalSnapshot = core::ptr::null_mut();
    }

    ProcessUtility(
        pstmt,
        (*portal).sourceText,
        !(*portal).cplan.is_null(), /* protect tree if in plancache */
        if isTopLevel {
            PROCESS_UTILITY_TOPLEVEL
        } else {
            PROCESS_UTILITY_QUERY
        },
        (*portal).portalParams,
        (*portal).queryEnv,
        dest,
        qc,
    );

    /* Some utility statements may change context on us */
    MemoryContextSwitchTo((*portal).portalContext);

    /*
     * Some utility commands (e.g., VACUUM) pop the ActiveSnapshot stack from
     * under us, so don't complain if it's now empty.  Otherwise, our snapshot
     * should be the top one; pop it.  Note that this could be a different
     * snapshot from the one we made above; see EnsurePortalSnapshotExists.
     */
    if !(*portal).portalSnapshot.is_null() && ActiveSnapshotSet() {
        Assert!((*portal).portalSnapshot == GetActiveSnapshot());
        PopActiveSnapshot();
    }
    (*portal).portalSnapshot = core::ptr::null_mut();
}

/*
 * PortalRunMulti
 *		Execute a portal's queries in the general case (multi queries
 *		or non-SELECT-like queries)
 */
unsafe fn PortalRunMulti(
    portal: Portal,
    isTopLevel: bool,
    setHoldSnapshot: bool,
    mut dest: *mut DestReceiver,
    mut altdest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let mut active_snapshot_set: bool = false;

    /*
     * If the destination is DestRemoteExecute, change to DestNone.  The
     * reason is that the client won't be expecting any tuples, and indeed has
     * no way to know what they are, since there is no provision for Describe
     * to send a RowDescription message when this portal execution strategy is
     * in effect.  This presently will only affect SELECT commands added to
     * non-SELECT queries by rewrite rules: such commands will be executed,
     * but the results will be discarded unless you use "simple Query"
     * protocol.
     */
    if (*dest).mydest == DestRemoteExecute {
        dest = None_Receiver();
    }
    if (*altdest).mydest == DestRemoteExecute {
        altdest = None_Receiver();
    }

    /*
     * Loop to handle the individual queries generated from a single parsetree
     * by analysis and rewrite.
     */
    foreach!(stmtlist_item, (*portal).stmts, {
        let pstmt: *mut PlannedStmt =
            lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(stmtlist_item));

        /*
         * If we got a cancel signal in prior command, quit
         */
        CHECK_FOR_INTERRUPTS();

        if (*pstmt).utilityStmt.is_null() {
            /*
             * process a plannable query.
             */
            TRACE_POSTGRESQL_QUERY_EXECUTE_START();

            if log_executor_stats {
                ResetUsage();
            }

            /*
             * Must always have a snapshot for plannable queries.  First time
             * through, take a new snapshot; for subsequent queries in the
             * same portal, just update the snapshot's copy of the command
             * counter.
             */
            if !active_snapshot_set {
                let mut snapshot: Snapshot = GetTransactionSnapshot();

                /* If told to, register the snapshot and save in portal */
                if setHoldSnapshot {
                    snapshot = RegisterSnapshot(snapshot);
                    (*portal).holdSnapshot = snapshot;
                }

                /*
                 * We can't have the holdSnapshot also be the active one,
                 * because UpdateActiveSnapshotCommandId would complain.  So
                 * force an extra snapshot copy.  Plain PushActiveSnapshot
                 * would have copied the transaction snapshot anyway, so this
                 * only adds a copy step when setHoldSnapshot is true.  (It's
                 * okay for the command ID of the active snapshot to diverge
                 * from what holdSnapshot has.)
                 */
                PushCopiedSnapshot(snapshot);

                /*
                 * As for PORTAL_ONE_SELECT portals, it does not seem
                 * necessary to maintain portal->portalSnapshot here.
                 */

                active_snapshot_set = true;
            } else {
                UpdateActiveSnapshotCommandId();
            }

            if (*pstmt).canSetTag {
                /* statement can set tag string */
                ProcessQuery(
                    pstmt,
                    (*portal).sourceText,
                    (*portal).portalParams,
                    (*portal).queryEnv,
                    dest,
                    qc,
                );
            } else {
                /* stmt added by rewrite cannot set tag */
                ProcessQuery(
                    pstmt,
                    (*portal).sourceText,
                    (*portal).portalParams,
                    (*portal).queryEnv,
                    altdest,
                    core::ptr::null_mut(),
                );
            }

            if log_executor_stats {
                ShowUsage(c"EXECUTOR STATISTICS".as_ptr());
            }

            TRACE_POSTGRESQL_QUERY_EXECUTE_DONE();
        } else {
            /*
             * process utility functions (create, destroy, etc..)
             *
             * We must not set a snapshot here for utility commands (if one is
             * needed, PortalRunUtility will do it).  If a utility command is
             * alone in a portal then everything's fine.  The only case where
             * a utility command can be part of a longer list is that rules
             * are allowed to include NotifyStmt.  NotifyStmt doesn't care
             * whether it has a snapshot or not, so we just leave the current
             * snapshot alone if we have one.
             */
            if (*pstmt).canSetTag {
                Assert!(!active_snapshot_set);
                /* statement can set tag string */
                PortalRunUtility(portal, pstmt, isTopLevel, false, dest, qc);
            } else {
                Assert!(IsA!((*pstmt).utilityStmt, T_NotifyStmt));
                /* stmt added by rewrite cannot set tag */
                PortalRunUtility(portal, pstmt, isTopLevel, false, altdest, core::ptr::null_mut());
            }
        }

        /*
         * Clear subsidiary contexts to recover temporary memory.
         */
        Assert!((*portal).portalContext == CurrentMemoryContext as _);

        MemoryContextDeleteChildren((*portal).portalContext);

        /*
         * Avoid crashing if portal->stmts has been reset.  This can only
         * occur if a CALL or DO utility statement executed an internal
         * COMMIT/ROLLBACK (cf PortalReleaseCachedPlan).  The CALL or DO must
         * have been the only statement in the portal, so there's nothing left
         * for us to do; but we don't want to dereference a now-dangling list
         * pointer.
         */
        if (*portal).stmts == NIL {
            break;
        }

        /*
         * Increment command counter between queries, but not after the last
         * one.
         */
        if !lnext((*portal).stmts, current_cell!(stmtlist_item)).is_null() {
            CommandCounterIncrement();
        }
    });

    /* Pop the snapshot if we pushed one. */
    if active_snapshot_set {
        PopActiveSnapshot();
    }

    /*
     * If a command tag was requested and we did not fill in a run-time-
     * determined tag above, copy the parse-time tag from the Portal.  (There
     * might not be any tag there either, in edge cases such as empty prepared
     * statements.  That's OK.)
     */
    if !qc.is_null()
        && (*qc).commandTag == CMDTAG_UNKNOWN
        && (*portal).qc.commandTag != CMDTAG_UNKNOWN
    {
        CopyQueryCompletion(&mut *qc, &(*portal).qc);
    }
}

/*
 * PortalRunFetch
 *		Variant form of PortalRun that supports SQL FETCH directions.
 *
 * Note: we presently assume that no callers of this want isTopLevel = true.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  (cf FetchStmt.howMany)
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
pub unsafe fn PortalRunFetch(
    portal: Portal,
    fdirection: FetchDirection,
    count: c_long,
    dest: *mut DestReceiver,
) -> uint64 {
    let mut result: uint64 = 0;
    let saveActivePortal: Portal;
    let saveResourceOwner: ResourceOwner;
    let savePortalContext: MemoryContext;
    let mut oldContext: MemoryContext = core::ptr::null_mut();

    Assert!(PortalIsValid(portal));

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;

    // PG_TRY();
    let try_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ActivePortal = portal;
        if !(*portal).resowner.is_null() {
            CurrentResourceOwner = (*portal).resowner;
        }
        PortalContext = (*portal).portalContext;

        oldContext = MemoryContextSwitchTo(PortalContext);

        match (*portal).strategy {
            PORTAL_ONE_SELECT => {
                result = DoPortalRunFetch(portal, fdirection, count, dest);
            }

            PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH | PORTAL_UTIL_SELECT => {
                /*
                 * If we have not yet run the command, do so, storing its
                 * results in the portal's tuplestore.
                 */
                if (*portal).holdStore.is_null() {
                    FillPortalStore(portal, false /* isTopLevel */);
                }

                /*
                 * Now fetch desired portion of results.
                 */
                result = DoPortalRunFetch(portal, fdirection, count, dest);
            }

            _ => {
                elog!(ERROR, "unsupported portal strategy");
                result = 0; /* keep compiler quiet */
            }
        }
    }));

    // PG_CATCH();
    if let Err(e) = try_result {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        // PG_RE_THROW();
        std::panic::resume_unwind(e);
    }
    // PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    /* Mark portal not active */
    (*portal).status = PORTAL_READY;

    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    result
}

/*
 * DoPortalRunFetch
 *		Guts of PortalRunFetch --- the portal context is already set up
 *
 * Here, count < 0 typically reverses the direction.  Also, count == FETCH_ALL
 * is interpreted as "all rows".  (cf FetchStmt.howMany)
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
unsafe fn DoPortalRunFetch(
    portal: Portal,
    mut fdirection: FetchDirection,
    mut count: c_long,
    dest: *mut DestReceiver,
) -> uint64 {
    let mut forward: bool;

    Assert!(
        (*portal).strategy == PORTAL_ONE_SELECT
            || (*portal).strategy == PORTAL_ONE_RETURNING
            || (*portal).strategy == PORTAL_ONE_MOD_WITH
            || (*portal).strategy == PORTAL_UTIL_SELECT
    );

    /*
     * Note: we disallow backwards fetch (including re-fetch of current row)
     * for NO SCROLL cursors, but we interpret that very loosely: you can use
     * any of the FetchDirection options, so long as the end result is to move
     * forwards by at least one row.  Currently it's sufficient to check for
     * NO SCROLL in DoPortalRewind() and in the forward == false path in
     * PortalRunSelect(); but someday we might prefer to account for that
     * restriction explicitly here.
     */
    match fdirection {
        FETCH_FORWARD => {
            if count < 0 {
                fdirection = FETCH_BACKWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_BACKWARD */
        }
        FETCH_BACKWARD => {
            if count < 0 {
                fdirection = FETCH_FORWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_FORWARD */
        }
        FETCH_ABSOLUTE => {
            if count > 0 {
                /*
                 * Definition: Rewind to start, advance count-1 rows, return
                 * next row (if any).
                 *
                 * In practice, if the goal is less than halfway back to the
                 * start, it's better to scan from where we are.
                 *
                 * Also, if current portalPos is outside the range of "long",
                 * do it the hard way to avoid possible overflow of the count
                 * argument to PortalRunSelect.  We must exclude exactly
                 * LONG_MAX, as well, lest the count look like FETCH_ALL.
                 *
                 * In any case, we arrange to fetch the target row going
                 * forwards.
                 */
                if (count - 1) as uint64 <= (*portal).portalPos / 2
                    || (*portal).portalPos >= c_long::MAX as uint64
                {
                    DoPortalRewind(portal);
                    if count > 1 {
                        PortalRunSelect(portal, true, count - 1, None_Receiver());
                    }
                } else {
                    let mut pos: c_long = (*portal).portalPos as c_long;

                    if (*portal).atEnd {
                        pos += 1; /* need one extra fetch if off end */
                    }
                    if count <= pos {
                        PortalRunSelect(portal, false, pos - count + 1, None_Receiver());
                    } else if count > pos + 1 {
                        PortalRunSelect(portal, true, count - pos - 1, None_Receiver());
                    }
                }
                return PortalRunSelect(portal, true, 1, dest);
            } else if count < 0 {
                /*
                 * Definition: Advance to end, back up abs(count)-1 rows,
                 * return prior row (if any).  We could optimize this if we
                 * knew in advance where the end was, but typically we won't.
                 * (Is it worth considering case where count > half of size of
                 * query?  We could rewind once we know the size ...)
                 */
                PortalRunSelect(portal, true, FETCH_ALL, None_Receiver());
                if count < -1 {
                    PortalRunSelect(portal, false, -count - 1, None_Receiver());
                }
                return PortalRunSelect(portal, false, 1, dest);
            } else {
                /* count == 0 */
                /* Rewind to start, return zero rows */
                DoPortalRewind(portal);
                return PortalRunSelect(portal, true, 0, dest);
            }
        }
        FETCH_RELATIVE => {
            if count > 0 {
                /*
                 * Definition: advance count-1 rows, return next row (if any).
                 */
                if count > 1 {
                    PortalRunSelect(portal, true, count - 1, None_Receiver());
                }
                return PortalRunSelect(portal, true, 1, dest);
            } else if count < 0 {
                /*
                 * Definition: back up abs(count)-1 rows, return prior row (if
                 * any).
                 */
                if count < -1 {
                    PortalRunSelect(portal, false, -count - 1, None_Receiver());
                }
                return PortalRunSelect(portal, false, 1, dest);
            } else {
                /* count == 0 */
                /* Same as FETCH FORWARD 0, so fall out of switch */
                fdirection = FETCH_FORWARD;
            }
        }
        _ => {
            elog!(ERROR, "bogus direction");
        }
    }

    /*
     * Get here with fdirection == FETCH_FORWARD or FETCH_BACKWARD, and count
     * >= 0.
     */
    forward = fdirection == FETCH_FORWARD;

    /*
     * Zero count means to re-fetch the current row, if any (per SQL)
     */
    if count == 0 {
        let on_row: bool;

        /* Are we sitting on a row? */
        on_row = !(*portal).atStart && !(*portal).atEnd;

        if (*dest).mydest == DestNone {
            /* MOVE 0 returns 0/1 based on if FETCH 0 would return a row */
            return if on_row { 1 } else { 0 };
        } else {
            /*
             * If we are sitting on a row, back up one so we can re-fetch it.
             * If we are not sitting on a row, we still have to start up and
             * shut down the executor so that the destination is initialized
             * and shut down correctly; so keep going.  To PortalRunSelect,
             * count == 0 means we will retrieve no row.
             */
            if on_row {
                PortalRunSelect(portal, false, 1, None_Receiver());
                /* Set up to fetch one row forward */
                count = 1;
                forward = true;
            }
        }
    }

    /*
     * Optimize MOVE BACKWARD ALL into a Rewind.
     */
    if !forward && count == FETCH_ALL && (*dest).mydest == DestNone {
        let mut result: uint64 = (*portal).portalPos;

        if result > 0 && !(*portal).atEnd {
            result -= 1;
        }
        DoPortalRewind(portal);
        return result;
    }

    PortalRunSelect(portal, forward, count, dest)
}

/*
 * DoPortalRewind - rewind a Portal to starting point
 */
unsafe fn DoPortalRewind(portal: Portal) {
    let queryDesc: *mut QueryDesc;

    /*
     * No work is needed if we've not advanced nor attempted to advance the
     * cursor (and we don't want to throw a NO SCROLL error in this case).
     */
    if (*portal).atStart && !(*portal).atEnd {
        return;
    }

    /* Otherwise, cursor must allow scrolling */
    if (*portal).cursorOptions & CURSOR_OPT_NO_SCROLL != 0 {
        ereport!(
            ERROR,
            errmsg!("cursor can only scan forward")
        );
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        // C also: errhint("Declare it with SCROLL option to enable backward scan.")
    }

    /* Rewind holdStore, if we have one */
    if !(*portal).holdStore.is_null() {
        let oldcontext: MemoryContext;

        oldcontext = MemoryContextSwitchTo((*portal).holdContext);
        tuplestore_rescan((*portal).holdStore);
        MemoryContextSwitchTo(oldcontext);
    }

    /* Rewind executor, if active */
    queryDesc = (*portal).queryDesc;
    if !queryDesc.is_null() {
        PushActiveSnapshot((*queryDesc).snapshot as _);
        ExecutorRewind(queryDesc);
        PopActiveSnapshot();
    }

    (*portal).atStart = true;
    (*portal).atEnd = false;
    (*portal).portalPos = 0;
}

/*
 * PlannedStmtRequiresSnapshot - what it says on the tin
 */
pub unsafe fn PlannedStmtRequiresSnapshot(pstmt: *mut PlannedStmt) -> bool {
    let utilityStmt: *mut Node = (*pstmt).utilityStmt;

    /* If it's not a utility statement, it definitely needs a snapshot */
    if utilityStmt.is_null() {
        return true;
    }

    /*
     * Most utility statements need a snapshot, and the default presumption
     * about new ones should be that they do too.  Hence, enumerate those that
     * do not need one.
     *
     * Transaction control, LOCK, and SET must *not* set a snapshot, since
     * they need to be executable at the start of a transaction-snapshot-mode
     * transaction without freezing a snapshot.  By extension we allow SHOW
     * not to set a snapshot.  The other stmts listed are just efficiency
     * hacks.  Beware of listing anything that can modify the database --- if,
     * say, it has to update an index with expressions that invoke
     * user-defined functions, then it had better have a snapshot.
     */
    if IsA!(utilityStmt, T_TransactionStmt)
        || IsA!(utilityStmt, T_LockStmt)
        || IsA!(utilityStmt, T_VariableSetStmt)
        || IsA!(utilityStmt, T_VariableShowStmt)
        || IsA!(utilityStmt, T_ConstraintsSetStmt)
        /* efficiency hacks from here down */
        || IsA!(utilityStmt, T_FetchStmt)
        || IsA!(utilityStmt, T_ListenStmt)
        || IsA!(utilityStmt, T_NotifyStmt)
        || IsA!(utilityStmt, T_UnlistenStmt)
        || IsA!(utilityStmt, T_CheckPointStmt)
    {
        return false;
    }

    true
}

/*
 * EnsurePortalSnapshotExists - recreate Portal-level snapshot, if needed
 *
 * Generally, we will have an active snapshot whenever we are executing
 * inside a Portal, unless the Portal's query is one of the utility
 * statements exempted from that rule (see PlannedStmtRequiresSnapshot).
 * However, procedures and DO blocks can commit or abort the transaction,
 * and thereby destroy all snapshots.  This function can be called to
 * re-establish the Portal-level snapshot when none exists.
 */
#[no_mangle]
pub unsafe fn EnsurePortalSnapshotExists() {
    let portal: Portal;

    /*
     * Nothing to do if a snapshot is set.  (We take it on faith that the
     * outermost active snapshot belongs to some Portal; or if there is no
     * Portal, it's somebody else's responsibility to manage things.)
     */
    if ActiveSnapshotSet() {
        return;
    }

    /* Otherwise, we'd better have an active Portal */
    portal = ActivePortal;
    if unlikely(portal.is_null()) {
        elog!(ERROR, "cannot execute SQL without an outer snapshot or portal");
    }
    Assert!((*portal).portalSnapshot.is_null());

    /*
     * Create a new snapshot, make it active, and remember it in portal.
     * Because the portal now references the snapshot, we must tell snapmgr.c
     * that the snapshot belongs to the portal's transaction level, else we
     * risk portalSnapshot becoming a dangling pointer.
     */
    PushActiveSnapshotWithLevel(GetTransactionSnapshot(), (*portal).createLevel);
    /* PushActiveSnapshotWithLevel might have copied the snapshot */
    (*portal).portalSnapshot = GetActiveSnapshot();
}
