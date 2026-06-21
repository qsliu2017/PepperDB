//! portal.h - POSTGRES portal definitions.
//!
//! A portal is an abstraction which represents the execution state of a
//! running or runnable query. Portals support both SQL-level CURSORs and
//! protocol-level portals.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, uint64, SubTransactionId};
use crate::nodes::params::ParamListInfo;
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::PlannedStmt;
use crate::tcop::cmdtag::{CommandTag, QueryCompletion};
use crate::utils::misc::queryenvironment::QueryEnvironment;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::resowner::resowner::ResourceOwner;
use crate::utils::snapshot::Snapshot;

// TupleDesc from access/common/tupdesc.h
use crate::access::common::tupdesc::TupleDesc;

// QueryDesc from executor/execdesc.h
use crate::executor::execdesc::QueryDesc;

// TimestampTz from datatype/timestamp.h (not yet ported as a standalone module).
// TODO: dedup when datatype/timestamp.h lands.
pub type TimestampTz = crate::c::int64;

// CachedPlan from utils/plancache.h (not yet ported).
// TODO: dedup when utils/plancache.h lands.
pub type CachedPlan = c_void;

// Tuplestorestate from utils/tuplestore.h (opaque, not yet ported).
// TODO: dedup when utils/tuplestore.h lands.
pub type Tuplestorestate = c_void;

/*
 * We have several execution strategies for Portals, depending on what
 * query or queries are to be executed.
 *
 * Project convention: C enum -> `pub type X = c_int;` + `pub const` variants.
 */
pub type PortalStrategy = c_int;
pub const PORTAL_ONE_SELECT: PortalStrategy = 0;
pub const PORTAL_ONE_RETURNING: PortalStrategy = 1;
pub const PORTAL_ONE_MOD_WITH: PortalStrategy = 2;
pub const PORTAL_UTIL_SELECT: PortalStrategy = 3;
pub const PORTAL_MULTI_QUERY: PortalStrategy = 4;

/*
 * A portal is always in one of these states. It is possible to transit from
 * ACTIVE back to READY if the query is not run to completion; otherwise we
 * never back up in status.
 */
pub type PortalStatus = c_int;
pub const PORTAL_NEW: PortalStatus = 0; /* freshly created */
pub const PORTAL_DEFINED: PortalStatus = 1; /* PortalDefineQuery done */
pub const PORTAL_READY: PortalStatus = 2; /* PortalStart complete, can run it */
pub const PORTAL_ACTIVE: PortalStatus = 3; /* portal is running (can't delete it) */
pub const PORTAL_DONE: PortalStatus = 4; /* portal is finished (don't re-run it) */
pub const PORTAL_FAILED: PortalStatus = 5; /* portal got error (can't re-run it) */

pub type Portal = *mut PortalData;

#[repr(C)]
pub struct PortalData {
    /* Bookkeeping data */
    pub name: *const c_char,        /* portal's name */
    pub prepStmtName: *const c_char, /* source prepared statement (NULL if none) */
    pub portalContext: MemoryContext, /* subsidiary memory for portal */
    pub resowner: ResourceOwner,    /* resources owned by portal */
    pub cleanup: Option<unsafe extern "C" fn(portal: Portal)>, /* cleanup hook */

    /*
     * State data for remembering which subtransaction(s) the portal was
     * created or used in. If the portal is held over from a previous
     * transaction, both subxids are InvalidSubTransactionId. Otherwise,
     * createSubid is the creating subxact and activeSubid is the last subxact
     * in which we ran the portal.
     */
    pub createSubid: SubTransactionId, /* the creating subxact */
    pub activeSubid: SubTransactionId, /* the last subxact with activity */
    pub createLevel: c_int,            /* creating subxact's nesting level */

    /* The query or queries the portal will execute */
    pub sourceText: *const c_char, /* text of query (as of 8.4, never NULL) */
    pub commandTag: CommandTag,    /* command tag for original query */
    pub qc: QueryCompletion,       /* command completion data for executed query */
    pub stmts: *mut List,          /* list of PlannedStmts */
    pub cplan: *mut CachedPlan,    /* CachedPlan, if stmts are from one */

    pub portalParams: ParamListInfo, /* params to pass to query */
    pub queryEnv: *mut QueryEnvironment, /* environment for query */

    /* Features/options */
    pub strategy: PortalStrategy, /* see above */
    pub cursorOptions: c_int,     /* DECLARE CURSOR option bits */

    /* Status data */
    pub status: PortalStatus, /* see above */
    pub portalPinned: bool,   /* a pinned portal can't be dropped */
    pub autoHeld: bool,       /* was automatically converted from pinned to
                               * held (see HoldPinnedPortals()) */

    /* If not NULL, Executor is active; call ExecutorEnd eventually: */
    pub queryDesc: *mut QueryDesc, /* info needed for executor invocation */

    /* If portal returns tuples, this is their tupdesc: */
    pub tupDesc: TupleDesc, /* descriptor for result tuples */
    /* and these are the format codes to use for the columns: */
    pub formats: *mut int16, /* a format code for each column */

    /*
     * Outermost ActiveSnapshot for execution of the portal's queries. For all
     * but a few utility commands, we require such a snapshot to exist. This
     * ensures that TOAST references in query results can be detoasted, and
     * helps to reduce thrashing of the process's exposed xmin.
     */
    pub portalSnapshot: Snapshot, /* active snapshot, or NULL if none */

    /*
     * Where we store tuples for a held cursor or a PORTAL_ONE_RETURNING,
     * PORTAL_ONE_MOD_WITH, or PORTAL_UTIL_SELECT query. (A cursor held past the
     * end of its transaction no longer has any active executor state.)
     */
    pub holdStore: *mut Tuplestorestate, /* store for holdable cursors */
    pub holdContext: MemoryContext,      /* memory containing holdStore */

    /*
     * Snapshot under which tuples in the holdStore were read. We must keep a
     * reference to this snapshot if there is any possibility that the tuples
     * contain TOAST references, because releasing the snapshot could allow
     * recently-dead rows to be vacuumed away, along with any toast data
     * belonging to them. In the case of a held cursor, we avoid needing to keep
     * such a snapshot by forcibly detoasting the data.
     */
    pub holdSnapshot: Snapshot, /* registered snapshot, or NULL if none */

    /*
     * atStart, atEnd and portalPos indicate the current cursor position.
     * portalPos is zero before the first row, N after fetching N'th row of
     * query. After we run off the end, portalPos = # of rows in query, and
     * atEnd is true. Note that atStart implies portalPos == 0, but not the
     * reverse: we might have backed up only as far as the first row, not to the
     * start. Also note that various code inspects atStart and atEnd, but only
     * the portal movement routines should touch portalPos.
     */
    pub atStart: bool,
    pub atEnd: bool,
    pub portalPos: uint64,

    /* Presentation data, primarily used by the pg_cursors system view */
    pub creation_time: TimestampTz, /* time at which this portal was defined */
    pub visible: bool,              /* include this portal in pg_cursors? */
}

/*
 * PortalIsValid
 *		True iff portal is valid.
 */
#[inline]
pub fn PortalIsValid(p: Portal) -> bool {
    crate::c::PointerIsValid(p)
}

/* Prototypes for functions in utils/mmgr/portalmem.c */
pub use crate::utils::mmgr::portalmem::{
    AtAbort_Portals, AtCleanup_Portals, AtSubAbort_Portals, AtSubCleanup_Portals,
    AtSubCommit_Portals, CreateNewPortal, CreatePortal, EnablePortalManager, ForgetPortalSnapshots,
    GetPortalByName, HoldPinnedPortals, MarkPortalActive, MarkPortalDone, MarkPortalFailed,
    PinPortal, PortalCreateHoldStore, PortalDefineQuery, PortalDrop, PortalErrorCleanup,
    PortalGetPrimaryStmt, PortalHashTableDeleteAll, PreCommit_Portals, ThereAreNoReadyPortals,
    UnpinPortal,
};
