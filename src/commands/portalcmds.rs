//! src/backend/commands/portalcmds.c
//!
//! portalcmds.c
//!   Utility commands affecting portals (that is, SQL cursor commands)
//!
//! Note: see also tcop/pquery.c, which implements portal operations for
//! the FE/BE protocol.  This module uses pquery.c for some operations.
//! And both modules depend on utils/mmgr/portalmem.c, which controls
//! storage management for portals (but doesn't run any queries in them).
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!   src/backend/commands/portalcmds.c

use crate::prelude::*;

// ---------------------------------------------------------------------------
// Local type/constant stubs for dependencies not yet ported.
// ---------------------------------------------------------------------------

type ParseState = c_void;
type DeclareCursorStmt = c_void;
type ParamListInfo = *mut c_void;
type Query = c_void;
type JumbleState = c_void;
type List = c_void;
type PlannedStmt = c_void;
type Portal = *mut c_void;
use crate::utils::palloc::MemoryContext;
type FetchStmt = c_void;
type DestReceiver = c_void;
type QueryCompletion = c_void;
type QueryDesc = c_void;
type ResourceOwner = *mut c_void;
type ScanDirection = c_int;
type CommandType = c_int;

// ---------------------------------------------------------------------------
// Stubs for unported helper functions and globals.
// ---------------------------------------------------------------------------

unsafe fn castNode_Query(_node: *mut c_void) -> *mut Query {
    unimplemented!() // TODO: nodes/nodes.h
}
unsafe fn linitial_node_Query(_l: *mut List) -> *mut Query {
    unimplemented!() // TODO: nodes/pg_list.h
}

unsafe fn IsQueryIdEnabled() -> bool {
    unimplemented!() // TODO: nodes/queryjumble.h
}
unsafe fn JumbleQuery(_query: *mut Query) -> *mut JumbleState {
    unimplemented!() // TODO: nodes/queryjumble.c
}
unsafe fn RequireTransactionBlock(_isTopLevel: bool, _stmtType: *const c_char) {
    unimplemented!() // TODO: access/xact.c
}
unsafe fn InSecurityRestrictedOperation() -> bool {
    unimplemented!() // TODO: utils/misc/guc.c
}
unsafe fn QueryRewrite(_query: *mut Query) -> *mut List {
    unimplemented!() // TODO: rewrite/rewriteHandler.c
}
unsafe fn list_length(_l: *mut List) -> c_int {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn pg_plan_query(
    _query: *mut Query,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    unimplemented!() // TODO: tcop/postgres.c
}
unsafe fn CreatePortal(_name: *const c_char, _allowDup: bool, _dupSilent: bool) -> Portal {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn copyObjectImpl(_from: *const c_void) -> *mut c_void {
    unimplemented!() // TODO: nodes/copyfuncs.c
}
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn PortalDefineQuery(
    _portal: Portal,
    _prepStmtName: *const c_char,
    _sourceText: *const c_char,
    _commandTag: c_int,
    _stmts: *mut List,
    _cplan: *mut c_void,
) {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn list_make1(_datum: *mut c_void) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn copyParamList(_from: ParamListInfo) -> ParamListInfo {
    unimplemented!() // TODO: nodes/params.c
}
unsafe fn ExecSupportsBackwardScan(_node: *mut c_void) -> bool {
    unimplemented!() // TODO: executor/execAmi.c
}
unsafe fn PortalStart(
    _portal: Portal,
    _params: ParamListInfo,
    _eflags: c_int,
    _snapshot: *mut c_void,
) {
    unimplemented!() // TODO: tcop/pquery.c
}
unsafe fn GetActiveSnapshot() -> *mut c_void {
    unimplemented!() // TODO: utils/time/snapmgr.c
}

unsafe fn GetPortalByName(_name: *const c_char) -> Portal {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn PortalIsValid(_p: Portal) -> bool {
    unimplemented!() // TODO: utils/portal.h
}
unsafe fn PortalRunFetch(
    _portal: Portal,
    _fdirection: c_int,
    _count: c_long,
    _dest: *mut DestReceiver,
) -> u64 {
    unimplemented!() // TODO: tcop/pquery.c
}
unsafe fn SetQueryCompletion(_qc: *mut QueryCompletion, _commandTag: c_int, _nprocessed: u64) {
    unimplemented!() // TODO: tcop/cmdtag.h
}

unsafe fn PortalHashTableDeleteAll() {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn PortalDrop(_portal: Portal, _isError: bool) {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}

unsafe fn ExecutorFinish(_queryDesc: *mut QueryDesc) {
    unimplemented!() // TODO: executor/execMain.c
}
unsafe fn ExecutorEnd(_queryDesc: *mut QueryDesc) {
    unimplemented!() // TODO: executor/execMain.c
}
unsafe fn FreeQueryDesc(_queryDesc: *mut QueryDesc) {
    unimplemented!() // TODO: tcop/pquery.c
}

unsafe fn CreateTupleDescCopy(_tupdesc: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn MarkPortalActive(_portal: Portal) {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn MarkPortalFailed(_portal: Portal) {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn PushActiveSnapshot(_snap: *mut c_void) {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn PopActiveSnapshot() {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn ExecutorRewind(_queryDesc: *mut QueryDesc) {
    unimplemented!() // TODO: executor/execMain.c
}
unsafe fn ExecutorRun(_queryDesc: *mut QueryDesc, _direction: ScanDirection, _count: u64) {
    unimplemented!() // TODO: executor/execMain.c
}
unsafe fn CreateDestReceiver(_dest: c_int) -> *mut DestReceiver {
    unimplemented!() // TODO: tcop/dest.c
}
unsafe fn SetTuplestoreDestReceiverParams(
    _self_: *mut DestReceiver,
    _tStore: *mut c_void,
    _tContext: MemoryContext,
    _detoast: bool,
    _target_tupdesc: *mut c_void,
    _map: *const c_char,
) {
    unimplemented!() // TODO: executor/tstoreReceiver.c
}
unsafe fn tuplestore_skiptuples(_state: *mut c_void, _ntuples: i64, _forward: bool) -> bool {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}
unsafe fn tuplestore_rescan(_state: *mut c_void) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}
unsafe fn MemoryContextDeleteChildren(_context: MemoryContext) {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}

// Globals (declared elsewhere in PostgreSQL).
static mut None_Receiver: *mut DestReceiver = std::ptr::null_mut();
static mut post_parse_analyze_hook: Option<unsafe fn(*mut ParseState, *mut Query, *mut JumbleState)> =
    None;
static mut ActivePortal: Portal = std::ptr::null_mut();
static mut CurrentResourceOwner: ResourceOwner = std::ptr::null_mut();
static mut PortalContext: MemoryContext = std::ptr::null_mut();

// Constants.
const ERRCODE_INVALID_CURSOR_NAME: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_UNDEFINED_CURSOR: c_int = 0;

const CURSOR_OPT_HOLD: c_int = 0x0010;
const CURSOR_OPT_SCROLL: c_int = 0x0002;
const CURSOR_OPT_NO_SCROLL: c_int = 0x0004;

const CMD_SELECT: CommandType = 1;

const CMDTAG_SELECT: c_int = 0;
const CMDTAG_MOVE: c_int = 0;
const CMDTAG_FETCH: c_int = 0;

const NIL: *mut List = std::ptr::null_mut();
const PORTAL_ONE_SELECT: c_int = 0;
const PORTAL_FAILED: c_int = 0;
const PORTAL_READY: c_int = 0;
const InvalidSubTransactionId: u32 = 0;

const ForwardScanDirection: ScanDirection = 1;
const NoMovementScanDirection: ScanDirection = 0;

const DestTuplestore: c_int = 0;
const DestNone: c_int = 0;

// Accessors emulating the C Portal/Query/Stmt struct field references.
// These are stubbed because the underlying struct layouts are not yet ported.
// They centralize the TODO so call sites read like the C original.

unsafe fn copyObject<T>(obj: *mut T) -> *mut T {
    copyObjectImpl(obj as *const c_void) as *mut T
}

// ---------------------------------------------------------------------------
// PerformCursorOpen
//   Execute SQL DECLARE CURSOR command.
// ---------------------------------------------------------------------------
pub unsafe fn PerformCursorOpen(
    pstate: *mut ParseState,
    cstmt: *mut DeclareCursorStmt,
    mut params: ParamListInfo,
    isTopLevel: bool,
) {
    let mut query: *mut Query = castNode_Query((*cstmt_query(cstmt)) as *mut c_void);
    let mut jstate: *mut JumbleState = std::ptr::null_mut();
    let rewritten: *mut List;
    let mut plan: *mut PlannedStmt;
    let portal: Portal;
    let oldContext: MemoryContext;
    let queryString: *mut c_char;

    // Disallow empty-string cursor name (conflicts with protocol-level
    // unnamed portal).
    if cstmt_portalname(cstmt).is_null() || *cstmt_portalname(cstmt) == b'\0' as c_char {
        ereport!(ERROR, "invalid cursor name: must not be empty");
        unreachable!();
    }

    // If this is a non-holdable cursor, we require that this statement has
    // been executed inside a transaction block (or else, it would have no
    // user-visible effect).
    if (cstmt_options(cstmt) & CURSOR_OPT_HOLD) == 0 {
        RequireTransactionBlock(isTopLevel, c"DECLARE CURSOR".as_ptr());
    } else if InSecurityRestrictedOperation() {
        ereport!(
            ERROR,
            "cannot create a cursor WITH HOLD within security-restricted operation"
        );
        unreachable!();
    }

    // Query contained by DeclareCursor needs to be jumbled if requested
    if IsQueryIdEnabled() {
        jstate = JumbleQuery(query);
    }

    if let Some(hook) = post_parse_analyze_hook {
        hook(pstate, query, jstate);
    }

    // Parse analysis was done already, but we still have to run the rule
    // rewriter.  We do not do AcquireRewriteLocks: we assume the query either
    // came straight from the parser, or suitable locks were acquired by
    // plancache.c.
    rewritten = QueryRewrite(query);

    // SELECT should never rewrite to more or less than one query
    if list_length(rewritten) != 1 {
        elog!(ERROR, "non-SELECT statement in DECLARE CURSOR");
        unreachable!();
    }

    query = linitial_node_Query(rewritten);

    if query_commandType(query) != CMD_SELECT {
        elog!(ERROR, "non-SELECT statement in DECLARE CURSOR");
        unreachable!();
    }

    // Plan the query, applying the specified options
    plan = pg_plan_query(
        query,
        pstate_p_sourcetext(pstate),
        cstmt_options(cstmt),
        params,
    );

    // Create a portal and copy the plan and query string into its memory.
    portal = CreatePortal(cstmt_portalname(cstmt), false, false);

    oldContext = MemoryContextSwitchTo(portal_portalContext(portal) as MemoryContext);

    plan = copyObject(plan);

    queryString = pstrdup(pstate_p_sourcetext(pstate));

    PortalDefineQuery(
        portal,
        std::ptr::null(),
        queryString,
        CMDTAG_SELECT, // cursor's query is always a SELECT
        list_make1(plan as *mut c_void),
        std::ptr::null_mut(),
    );

    //----------
    // Also copy the outer portal's parameter list into the inner portal's
    // memory context.  We want to pass down the parameter values in case we
    // had a command like
    //      DECLARE c CURSOR FOR SELECT ... WHERE foo = $1
    // This will have been parsed using the outer parameter set and the
    // parameter value needs to be preserved for use when the cursor is
    // executed.
    //----------
    params = copyParamList(params);

    MemoryContextSwitchTo(oldContext);

    // Set up options for portal.
    //
    // If the user didn't specify a SCROLL type, allow or disallow scrolling
    // based on whether it would require any additional runtime overhead to do
    // so.  Also, we disallow scrolling for FOR UPDATE cursors.
    set_portal_cursorOptions(portal, cstmt_options(cstmt));
    if (portal_cursorOptions(portal) & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)) == 0 {
        if plan_rowMarks(plan) == NIL && ExecSupportsBackwardScan(plan_planTree(plan)) {
            set_portal_cursorOptions(portal, portal_cursorOptions(portal) | CURSOR_OPT_SCROLL);
        } else {
            set_portal_cursorOptions(portal, portal_cursorOptions(portal) | CURSOR_OPT_NO_SCROLL);
        }
    }

    // Start execution, inserting parameters if any.
    PortalStart(portal, params, 0, GetActiveSnapshot());

    Assert!(portal_strategy(portal) == PORTAL_ONE_SELECT);

    // We're done; the query won't actually be run until PerformPortalFetch is
    // called.
}

// ---------------------------------------------------------------------------
// PerformPortalFetch
//   Execute SQL FETCH or MOVE command.
//
//  stmt: parsetree node for command
//  dest: where to send results
//  qc: where to store a command completion status data.
//
// qc may be NULL if caller doesn't want status data.
// ---------------------------------------------------------------------------
pub unsafe fn PerformPortalFetch(
    stmt: *mut FetchStmt,
    mut dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let portal: Portal;
    let nprocessed: u64;

    // Disallow empty-string cursor name (conflicts with protocol-level
    // unnamed portal).
    if stmt_portalname(stmt).is_null() || *stmt_portalname(stmt) == b'\0' as c_char {
        ereport!(ERROR, "invalid cursor name: must not be empty");
        unreachable!();
    }

    // get the portal from the portal name
    portal = GetPortalByName(stmt_portalname(stmt));
    if !PortalIsValid(portal) {
        elog!(
            ERROR,
            "cursor \"{}\" does not exist",
            cstr_to_display(stmt_portalname(stmt))
        );
        return; // keep compiler happy
    }

    // Adjust dest if needed.  MOVE wants destination DestNone
    if fetchstmt_ismove(stmt) {
        dest = None_Receiver;
    }

    // Do it
    nprocessed = PortalRunFetch(
        portal,
        fetchstmt_direction(stmt),
        fetchstmt_howMany(stmt),
        dest,
    );

    // Return command status if wanted
    if !qc.is_null() {
        SetQueryCompletion(
            qc,
            if fetchstmt_ismove(stmt) {
                CMDTAG_MOVE
            } else {
                CMDTAG_FETCH
            },
            nprocessed,
        );
    }
}

// ---------------------------------------------------------------------------
// PerformPortalClose
//   Close a cursor.
// ---------------------------------------------------------------------------
pub unsafe fn PerformPortalClose(name: *const c_char) {
    let portal: Portal;

    // NULL means CLOSE ALL
    if name.is_null() {
        PortalHashTableDeleteAll();
        return;
    }

    // Disallow empty-string cursor name (conflicts with protocol-level
    // unnamed portal).
    if *name == b'\0' as c_char {
        ereport!(ERROR, "invalid cursor name: must not be empty");
        unreachable!();
    }

    // get the portal from the portal name
    portal = GetPortalByName(name);
    if !PortalIsValid(portal) {
        elog!(
            ERROR,
            "cursor \"{}\" does not exist",
            cstr_to_display(name)
        );
        return; // keep compiler happy
    }

    // Note: PortalCleanup is called as a side-effect, if not already done.
    PortalDrop(portal, false);
}

// ---------------------------------------------------------------------------
// PortalCleanup
//
// Clean up a portal when it's dropped.  This is the standard cleanup hook
// for portals.
//
// Note: if portal->status is PORTAL_FAILED, we are probably being called
// during error abort, and must be careful to avoid doing anything that
// is likely to fail again.
// ---------------------------------------------------------------------------
pub unsafe fn PortalCleanup(portal: Portal) {
    let queryDesc: *mut QueryDesc;

    // sanity checks
    Assert!(PortalIsValid(portal));
    // Assert(portal->cleanup == PortalCleanup);

    // Shut down executor, if still running.  We skip this during error abort,
    // since other mechanisms will take care of releasing executor resources,
    // and we can't be sure that ExecutorEnd itself wouldn't fail.
    queryDesc = portal_queryDesc(portal);
    if !queryDesc.is_null() {
        // Reset the queryDesc before anything else.  This prevents us from
        // trying to shut down the executor twice, in case of an error below.
        // The transaction abort mechanisms will take care of resource cleanup
        // in such a case.
        set_portal_queryDesc(portal, std::ptr::null_mut());

        if portal_status(portal) != PORTAL_FAILED {
            let saveResourceOwner: ResourceOwner;

            // We must make the portal's resource owner current
            saveResourceOwner = CurrentResourceOwner;
            if !portal_resowner(portal).is_null() {
                CurrentResourceOwner = portal_resowner(portal);
            }

            ExecutorFinish(queryDesc);
            ExecutorEnd(queryDesc);
            FreeQueryDesc(queryDesc);

            CurrentResourceOwner = saveResourceOwner;
        }
    }
}

// ---------------------------------------------------------------------------
// PersistHoldablePortal
//
// Prepare the specified Portal for access outside of the current
// transaction. When this function returns, all future accesses to the
// portal must be done via the Tuplestore (not by invoking the
// executor).
// ---------------------------------------------------------------------------
pub unsafe fn PersistHoldablePortal(portal: Portal) {
    let mut queryDesc: *mut QueryDesc = portal_queryDesc(portal);
    let saveActivePortal: Portal;
    let saveResourceOwner: ResourceOwner;
    let savePortalContext: MemoryContext;
    let oldcxt: MemoryContext;

    // If we're preserving a holdable portal, we had better be inside the
    // transaction that originally created it.
    Assert!(portal_createSubid(portal) != InvalidSubTransactionId);
    Assert!(!queryDesc.is_null());

    // Caller must have created the tuplestore already ... but not a snapshot.
    Assert!(!portal_holdContext(portal).is_null());
    Assert!(!portal_holdStore(portal).is_null());
    Assert!(portal_holdSnapshot(portal).is_null());

    // Before closing down the executor, we must copy the tupdesc into
    // long-term memory, since it was created in executor memory.
    oldcxt = MemoryContextSwitchTo(portal_holdContext(portal) as MemoryContext);

    set_portal_tupDesc(portal, CreateTupleDescCopy(portal_tupDesc(portal)));

    MemoryContextSwitchTo(oldcxt);

    // Check for improper portal use, and mark portal active.
    MarkPortalActive(portal);

    // Set up global portal context pointers.
    saveActivePortal = ActivePortal;
    saveResourceOwner = CurrentResourceOwner;
    savePortalContext = PortalContext;

    // PG_TRY()
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut direction: ScanDirection = ForwardScanDirection;

        ActivePortal = portal;
        if !portal_resowner(portal).is_null() {
            CurrentResourceOwner = portal_resowner(portal);
        }
        PortalContext = portal_portalContext(portal) as MemoryContext;

        MemoryContextSwitchTo(PortalContext);

        PushActiveSnapshot(querydesc_snapshot(queryDesc));

        // If the portal is marked scrollable, we need to store the entire
        // result set in the tuplestore, so that subsequent backward FETCHs
        // can be processed.  Otherwise, store only the not-yet-fetched rows.
        // (The latter is not only more efficient, but avoids semantic
        // problems if the query's output isn't stable.)
        //
        // In the no-scroll case, tuple indexes in the tuplestore will not
        // match the cursor's nominal position (portalPos).  Currently this
        // causes no difficulty because we only navigate in the tuplestore by
        // relative position, except for the tuplestore_skiptuples call below
        // and the tuplestore_rescan call in DoPortalRewind, both of which are
        // disabled for no-scroll cursors.  But someday we might need to track
        // the offset between the holdStore and the cursor's nominal position
        // explicitly.
        if (portal_cursorOptions(portal) & CURSOR_OPT_SCROLL) != 0 {
            ExecutorRewind(queryDesc);
        } else {
            // If we already reached end-of-query, set the direction to
            // NoMovement to avoid trying to fetch any tuples.  (This check
            // exists because not all plan node types are robust about being
            // called again if they've already returned NULL once.)  We'll
            // still set up an empty tuplestore, though, to keep this from
            // being a special case later.
            if portal_atEnd(portal) {
                direction = NoMovementScanDirection;
            }
        }

        // Change the destination to output to the tuplestore.  Note we tell
        // the tuplestore receiver to detoast all data passed through it; this
        // makes it safe to not keep a snapshot associated with the data.
        set_querydesc_dest(queryDesc, CreateDestReceiver(DestTuplestore));
        SetTuplestoreDestReceiverParams(
            querydesc_dest(queryDesc),
            portal_holdStore(portal),
            portal_holdContext(portal) as MemoryContext,
            true,
            std::ptr::null_mut(),
            std::ptr::null(),
        );

        // Fetch the result set into the tuplestore
        ExecutorRun(queryDesc, direction, 0);

        rDestroy(querydesc_dest(queryDesc));
        set_querydesc_dest(queryDesc, std::ptr::null_mut());

        // Now shut down the inner executor.
        set_portal_queryDesc(portal, std::ptr::null_mut()); // prevent double shutdown
        ExecutorFinish(queryDesc);
        ExecutorEnd(queryDesc);
        FreeQueryDesc(queryDesc);
        queryDesc = std::ptr::null_mut();

        // Set the position in the result set.
        MemoryContextSwitchTo(portal_holdContext(portal) as MemoryContext);

        if portal_atEnd(portal) {
            // Just force the tuplestore forward to its end.  The size of the
            // skip request here is arbitrary.
            while tuplestore_skiptuples(portal_holdStore(portal), 1000000, true) {
                // continue
            }
        } else {
            tuplestore_rescan(portal_holdStore(portal));

            // In the no-scroll case, the start of the tuplestore is exactly
            // where we want to be, so no repositioning is wanted.
            if (portal_cursorOptions(portal) & CURSOR_OPT_SCROLL) != 0 {
                if !tuplestore_skiptuples(
                    portal_holdStore(portal),
                    portal_portalPos(portal) as i64,
                    true,
                ) {
                    elog!(ERROR, "unexpected end of tuple stream");
                }
            }
        }
    }));

    // PG_CATCH()
    if result.is_err() {
        // Uncaught error while executing portal: mark it dead
        MarkPortalFailed(portal);

        // Restore global vars and propagate error
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;

        // PG_RE_THROW();
        std::panic::resume_unwind(result.err().unwrap());
    }
    // PG_END_TRY()

    MemoryContextSwitchTo(oldcxt);

    // Mark portal not active
    set_portal_status(portal, PORTAL_READY);

    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    PopActiveSnapshot();

    // We can now release any subsidiary memory of the portal's context; we'll
    // never use it again.  The executor already dropped its context, but this
    // will clean up anything that glommed onto the portal's context via
    // PortalContext.
    MemoryContextDeleteChildren(portal_portalContext(portal) as MemoryContext);
}

// ---------------------------------------------------------------------------
// Field-accessor stubs for struct members of opaque/unported types.
// These centralize the unported struct layout dependencies.
// ---------------------------------------------------------------------------

unsafe fn cstr_to_display(_s: *const c_char) -> &'static str {
    "" // TODO: render C string for elog
}

unsafe fn cstmt_query(_cstmt: *mut DeclareCursorStmt) -> *mut *mut c_void {
    unimplemented!() // TODO: nodes/parsenodes.h DeclareCursorStmt.query
}
unsafe fn cstmt_portalname(_cstmt: *mut DeclareCursorStmt) -> *const c_char {
    unimplemented!() // TODO: nodes/parsenodes.h DeclareCursorStmt.portalname
}
unsafe fn cstmt_options(_cstmt: *mut DeclareCursorStmt) -> c_int {
    unimplemented!() // TODO: nodes/parsenodes.h DeclareCursorStmt.options
}

unsafe fn query_commandType(_query: *mut Query) -> CommandType {
    unimplemented!() // TODO: nodes/parsenodes.h Query.commandType
}

unsafe fn pstate_p_sourcetext(_pstate: *mut ParseState) -> *const c_char {
    unimplemented!() // TODO: parser/parse_node.h ParseState.p_sourcetext
}

unsafe fn plan_rowMarks(_plan: *mut PlannedStmt) -> *mut List {
    unimplemented!() // TODO: nodes/plannodes.h PlannedStmt.rowMarks
}
unsafe fn plan_planTree(_plan: *mut PlannedStmt) -> *mut c_void {
    unimplemented!() // TODO: nodes/plannodes.h PlannedStmt.planTree
}

unsafe fn portal_portalContext(_portal: Portal) -> *mut c_void {
    unimplemented!() // TODO: utils/portal.h PortalData.portalContext
}
unsafe fn portal_cursorOptions(_portal: Portal) -> c_int {
    unimplemented!() // TODO: utils/portal.h PortalData.cursorOptions
}
unsafe fn set_portal_cursorOptions(_portal: Portal, _v: c_int) {
    unimplemented!() // TODO: utils/portal.h PortalData.cursorOptions
}
unsafe fn portal_strategy(_portal: Portal) -> c_int {
    unimplemented!() // TODO: utils/portal.h PortalData.strategy
}
unsafe fn portal_queryDesc(_portal: Portal) -> *mut QueryDesc {
    unimplemented!() // TODO: utils/portal.h PortalData.queryDesc
}
unsafe fn set_portal_queryDesc(_portal: Portal, _v: *mut QueryDesc) {
    unimplemented!() // TODO: utils/portal.h PortalData.queryDesc
}
unsafe fn portal_status(_portal: Portal) -> c_int {
    unimplemented!() // TODO: utils/portal.h PortalData.status
}
unsafe fn set_portal_status(_portal: Portal, _v: c_int) {
    unimplemented!() // TODO: utils/portal.h PortalData.status
}
unsafe fn portal_resowner(_portal: Portal) -> ResourceOwner {
    unimplemented!() // TODO: utils/portal.h PortalData.resowner
}
unsafe fn portal_createSubid(_portal: Portal) -> u32 {
    unimplemented!() // TODO: utils/portal.h PortalData.createSubid
}
unsafe fn portal_holdContext(_portal: Portal) -> *mut c_void {
    unimplemented!() // TODO: utils/portal.h PortalData.holdContext
}
unsafe fn portal_holdStore(_portal: Portal) -> *mut c_void {
    unimplemented!() // TODO: utils/portal.h PortalData.holdStore
}
unsafe fn portal_holdSnapshot(_portal: Portal) -> *mut c_void {
    unimplemented!() // TODO: utils/portal.h PortalData.holdSnapshot
}
unsafe fn portal_tupDesc(_portal: Portal) -> *mut c_void {
    unimplemented!() // TODO: utils/portal.h PortalData.tupDesc
}
unsafe fn set_portal_tupDesc(_portal: Portal, _v: *mut c_void) {
    unimplemented!() // TODO: utils/portal.h PortalData.tupDesc
}
unsafe fn portal_atEnd(_portal: Portal) -> bool {
    unimplemented!() // TODO: utils/portal.h PortalData.atEnd
}
unsafe fn portal_portalPos(_portal: Portal) -> u64 {
    unimplemented!() // TODO: utils/portal.h PortalData.portalPos
}

unsafe fn querydesc_snapshot(_qd: *mut QueryDesc) -> *mut c_void {
    unimplemented!() // TODO: executor/execdesc.h QueryDesc.snapshot
}
unsafe fn querydesc_dest(_qd: *mut QueryDesc) -> *mut DestReceiver {
    unimplemented!() // TODO: executor/execdesc.h QueryDesc.dest
}
unsafe fn set_querydesc_dest(_qd: *mut QueryDesc, _v: *mut DestReceiver) {
    unimplemented!() // TODO: executor/execdesc.h QueryDesc.dest
}
unsafe fn rDestroy(_dest: *mut DestReceiver) {
    unimplemented!() // TODO: tcop/dest.h DestReceiver.rDestroy
}

unsafe fn stmt_portalname(_stmt: *mut FetchStmt) -> *const c_char {
    unimplemented!() // TODO: nodes/parsenodes.h FetchStmt.portalname
}
unsafe fn fetchstmt_ismove(_stmt: *mut FetchStmt) -> bool {
    unimplemented!() // TODO: nodes/parsenodes.h FetchStmt.ismove
}
unsafe fn fetchstmt_direction(_stmt: *mut FetchStmt) -> c_int {
    unimplemented!() // TODO: nodes/parsenodes.h FetchStmt.direction
}
unsafe fn fetchstmt_howMany(_stmt: *mut FetchStmt) -> c_long {
    unimplemented!() // TODO: nodes/parsenodes.h FetchStmt.howMany
}
