//! src/backend/commands/matview.c
//!
//! matview.c
//!   materialized view support
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use std::ffi::{c_char, c_int};

// Crate-root #[macro_export] macros used in this unit.
use crate::{foreach, current_cell, linitial_node};

// Function-like helpers / constants imported from their real modules.
use crate::miscadmin::{
    CHECK_FOR_INTERRUPTS, SECURITY_RESTRICTED_OPERATION, SECURITY_LOCAL_USERID_CHANGE,
};
use crate::catalog::pg_class::{RELKIND_MATVIEW, RELPERSISTENCE_TEMP};
use crate::catalog::catalog_oids::RelationRelationId;
use crate::storage::lockdefs::{
    NoLock, AccessShareLock, RowExclusiveLock, ExclusiveLock, AccessExclusiveLock,
};
use crate::storage::block::InvalidBlockNumber;
use crate::nodes::nodes::NodeTag::T_Query;
use crate::nodes::nodes::CmdType::CMD_SELECT;
use crate::nodes::pg_list::NIL;
use crate::utils::snapshot::InvalidSnapshot;
use crate::access::sdir::ForwardScanDirection;
use crate::access::cmptype::COMPARE_EQ;
use crate::access::table::tableam::{TABLE_INSERT_SKIP_FSM, TABLE_INSERT_FROZEN};
use crate::nodes::parsenodes::CURSOR_OPT_PARALLEL_OK;
use crate::tcop::dest::CommandDest::DestTransientRel;
use crate::tcop::cmdtaglist::{CMDTAG_SELECT, CMDTAG_REFRESH_MATERIALIZED_VIEW};

// ---------------------------------------------------------------------------
// Stub type aliases for dependencies not yet ported. These mirror the C
// pointer-based opaque types used throughout matview.c.
// ---------------------------------------------------------------------------

type DestReceiver = crate::tcop::dest::DestReceiver;
type TupleTableSlot = crate::executor::tuptable::TupleTableSlot;
type TupleDesc = crate::access::common::tupdesc::TupleDesc;
type Relation = crate::utils::rel::Relation;
type HeapTuple = crate::access::htup_details::HeapTuple;
type CommandId = crate::c::CommandId;
type BulkInsertState = *mut crate::access::table::tableam::BulkInsertStateData;
type Query = crate::nodes::parsenodes::Query;
type PlannedStmt = crate::nodes::plannodes::PlannedStmt;
type QueryDesc = crate::executor::execdesc::QueryDesc;
type RewriteRule = crate::rewrite::prs2lock::RewriteRule;
type RuleLock = crate::rewrite::prs2lock::RuleLock;
type List = crate::nodes::pg_list::List;
type ListCell = crate::nodes::pg_list::ListCell;
type RefreshMatViewStmt = crate::nodes::parsenodes::RefreshMatViewStmt;
type QueryCompletion = crate::tcop::cmdtag::QueryCompletion;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type LOCKMODE = crate::storage::lockdefs::LOCKMODE;
type StringInfoData = crate::lib::stringinfo::StringInfoData;
type Form_pg_class = *mut crate::catalog::pg_class::FormData_pg_class;
// The crate's catalog FormData_pg_index exposes only the fixed part; matview.c
// also touches the variable-length `indkey` (an int2vector that begins right
// after the fixed fields). Mirror the genam.c port and use a local layout-stub
// that exposes the fields this unit reads.
type Form_pg_index = *mut FormData_pg_index_matview;

#[repr(C)]
pub struct FormData_pg_index_matview {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    /* variable-length region begins here */
    pub indkey: int2vector_stub,
}

#[repr(C)]
pub struct int2vector_stub {
    pub vl_len_: i32,
    pub ndim: c_int,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: c_int,
    pub lbound1: c_int,
    pub values: [i16; crate::c::FLEXIBLE_ARRAY_MEMBER],
}
type Form_pg_opclass = *mut crate::catalog::pg_opclass::FormData_pg_opclass;
type Form_pg_attribute = *mut crate::catalog::pg_attribute::FormData_pg_attribute;
type oidvector = crate::c::oidvector;

// catalog/namespace.h: RangeVarGetRelidCallback function-pointer type (the
// shared catalog/namespace module is not yet ported, so mirror sibling units).
type RangeVarGetRelidCallback =
    unsafe extern "C" fn(rv: *const crate::nodes::primnodes::RangeVar, relid: Oid, oldrelid: Oid, arg: *mut std::ffi::c_void);

// Local macros: faithful PG_TRY/PG_RE_THROW/NameStr are not yet provided at the
// crate root, so stub them here (matching other translated units).
macro_rules! PG_TRY {
    ($try_block:block, $catch_block:block) => {{
        // TODO: src/include/utils/elog.h - faithful PG_TRY/PG_CATCH/PG_END_TRY
        $try_block
    }};
}
use PG_TRY;

macro_rules! PG_RE_THROW {
    () => {
        unimplemented!() // TODO: utils/elog.h -- PG_RE_THROW
    };
}
use PG_RE_THROW;

macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *const c_char
    };
}
use NameStr;

/*
 * struct DR_transientrel: DestReceiver for a transient relation.
 */
#[repr(C)]
pub struct DR_transientrel {
    pub pub_: DestReceiver,        /* publicly-known function pointers */
    pub transientoid: Oid,         /* OID of new heap into which to store */
    /* These fields are filled by transientrel_startup: */
    pub transientrel: Relation,    /* relation to write to */
    pub output_cid: CommandId,     /* cmin to insert in output tuples */
    pub ti_options: c_int,         /* table_tuple_insert performance options */
    pub bistate: BulkInsertState,  /* bulk insert state */
}

static mut matview_maintenance_depth: c_int = 0;

/*
 * SetMatViewPopulatedState
 *		Mark a materialized view as populated, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
pub unsafe fn SetMatViewPopulatedState(relation: Relation, newstate: bool) {
    let pgrel: Relation;
    let tuple: HeapTuple;

    Assert!((*(*relation).rd_rel).relkind == RELKIND_MATVIEW);

    /*
     * Update relation's pg_class entry.  Crucial side-effect: other backends
     * (and this one too!) are sent SI message to make them rebuild relcache
     * entries.
     */
    pgrel = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID,
                                ObjectIdGetDatum(RelationGetRelid(relation)));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}",
              RelationGetRelid(relation));
    }

    (*(GETSTRUCT(tuple) as Form_pg_class)).relispopulated = newstate;

    CatalogTupleUpdate(pgrel, &mut (*tuple).t_self, tuple);

    heap_freetuple(tuple);
    table_close(pgrel, RowExclusiveLock);

    /*
     * Advance command counter to make the updated pg_class row locally
     * visible.
     */
    CommandCounterIncrement();
}

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field shows whether the clause was used.
 */
pub unsafe fn ExecRefreshMatView(stmt: *mut RefreshMatViewStmt, queryString: *const c_char,
                                 qc: *mut QueryCompletion) -> ObjectAddress {
    let matviewOid: Oid;
    let lockmode: LOCKMODE;

    /* Determine strength of lock needed. */
    lockmode = if (*stmt).concurrent { ExclusiveLock } else { AccessExclusiveLock };

    /*
     * Get a lock until end of transaction.
     */
    matviewOid = RangeVarGetRelidExtended((*stmt).relation,
                                          lockmode, 0,
                                          Some(RangeVarCallbackMaintainsTable),
                                          std::ptr::null_mut());

    return RefreshMatViewByOid(matviewOid, false, (*stmt).skipData,
                               (*stmt).concurrent, queryString, qc);
}

/*
 * RefreshMatViewByOid -- refresh materialized view by OID
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenumbers of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If skipData is true, this is effectively like a TRUNCATE; otherwise it is
 * like a TRUNCATE followed by an INSERT using the SELECT statement associated
 * with the materialized view.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The matview's "populated" state is changed based on whether the contents
 * reflect the result set of the materialized view's query.
 *
 * This is also used to populate the materialized view created by CREATE
 * MATERIALIZED VIEW command.
 */
pub unsafe fn RefreshMatViewByOid(matviewOid: Oid, is_create: bool, skipData: bool,
                                  concurrent: bool, queryString: *const c_char,
                                  qc: *mut QueryCompletion) -> ObjectAddress {
    let matviewRel: Relation;
    let rule: *mut RewriteRule;
    let actions: *mut List;
    let dataQuery: *mut Query;
    let tableSpace: Oid;
    let relowner: Oid;
    let OIDNewHeap: Oid;
    let mut processed: u64 = 0;
    let relpersistence: c_char;
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    let save_nestlevel: c_int;
    let mut address: ObjectAddress = std::mem::zeroed();

    matviewRel = table_open(matviewOid, NoLock);
    relowner = (*(*matviewRel).rd_rel).relowner;

    /*
     * Switch to the owner's userid, so that any functions are run as that
     * user.  Also lock down security-restricted operations and arrange to
     * make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext(relowner,
                           save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    /* Make sure it is a materialized view. */
    if (*(*matviewRel).rd_rel).relkind != RELKIND_MATVIEW {
        ereport!(ERROR,
                 "is not a materialized view");
    }

    /* Check that CONCURRENTLY is not specified if not populated. */
    if concurrent && !RelationIsPopulated(matviewRel) {
        ereport!(ERROR,
                 "CONCURRENTLY cannot be used when the materialized view is not populated");
    }

    /* Check that conflicting options have not been specified. */
    if concurrent && skipData {
        elog!(ERROR,
              "{} and {} options cannot be used together",
              "CONCURRENTLY", "WITH NO DATA");
    }

    /*
     * Check that everything is correct for a refresh. Problems at this point
     * are internal errors, so elog is sufficient.
     */
    if (*(*matviewRel).rd_rel).relhasrules == false ||
        (*((*matviewRel).rd_rules as *mut RuleLock)).numLocks < 1 {
        elog!(ERROR,
              "materialized view \"{}\" is missing rewrite information",
              CStr_to_str(RelationGetRelationName(matviewRel)));
    }

    if (*((*matviewRel).rd_rules as *mut RuleLock)).numLocks > 1 {
        elog!(ERROR,
              "materialized view \"{}\" has too many rules",
              CStr_to_str(RelationGetRelationName(matviewRel)));
    }

    rule = *(*((*matviewRel).rd_rules as *mut RuleLock)).rules;
    if (*rule).event != CMD_SELECT || !((*rule).isInstead) {
        elog!(ERROR,
              "the rule for materialized view \"{}\" is not a SELECT INSTEAD OF rule",
              CStr_to_str(RelationGetRelationName(matviewRel)));
    }

    actions = (*rule).actions;
    if list_length(actions) != 1 {
        elog!(ERROR,
              "the rule for materialized view \"{}\" is not a single action",
              CStr_to_str(RelationGetRelationName(matviewRel)));
    }

    /*
     * Check that there is a unique index with no WHERE clause on one or more
     * columns of the materialized view if CONCURRENTLY is specified.
     */
    if concurrent {
        let indexoidlist: *mut List = RelationGetIndexList(matviewRel);
        let mut hasUniqueIndex: bool = false;

        Assert!(!is_create);

        foreach!(indexoidscan, indexoidlist, {
            let indexoid: Oid = lfirst_oid(current_cell!(indexoidscan));
            let indexRel: Relation;

            indexRel = index_open(indexoid, AccessShareLock);
            hasUniqueIndex = is_usable_unique_index(indexRel);
            index_close(indexRel, AccessShareLock);
            if hasUniqueIndex {
                break;
            }
        });

        list_free(indexoidlist);

        if !hasUniqueIndex {
            ereport!(ERROR,
                     "cannot refresh materialized view concurrently");
        }
    }

    /*
     * The stored query was rewritten at the time of the MV definition, but
     * has not been scribbled on by the planner.
     */
    dataQuery = linitial_node!(Query, T_Query, actions);

    /*
     * Check for active uses of the relation in the current transaction, such
     * as open scans.
     *
     * NB: We count on this to protect us against problems with refreshing the
     * data using TABLE_INSERT_FROZEN.
     */
    CheckTableNotInUse(matviewRel,
                       if is_create { c"CREATE MATERIALIZED VIEW".as_ptr() }
                       else { c"REFRESH MATERIALIZED VIEW".as_ptr() });

    /*
     * Tentatively mark the matview as populated or not (this will roll back
     * if we fail later).
     */
    SetMatViewPopulatedState(matviewRel, !skipData);

    /* Concurrent refresh builds new data in temp tablespace, and does diff. */
    if concurrent {
        tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
        relpersistence = RELPERSISTENCE_TEMP;
    } else {
        tableSpace = (*(*matviewRel).rd_rel).reltablespace;
        relpersistence = (*(*matviewRel).rd_rel).relpersistence;
    }

    /*
     * Create the transient table that will receive the regenerated data. Lock
     * it against access by any other process until commit (by which time it
     * will be gone).
     */
    OIDNewHeap = make_new_heap(matviewOid, tableSpace,
                               (*(*matviewRel).rd_rel).relam,
                               relpersistence, ExclusiveLock);
    Assert!(CheckRelationOidLockedByMe(OIDNewHeap, AccessExclusiveLock, false));

    /* Generate the data, if wanted. */
    if !skipData {
        let dest: *mut DestReceiver;

        dest = CreateTransientRelDestReceiver(OIDNewHeap);
        processed = refresh_matview_datafill(dest, dataQuery, queryString,
                                             is_create);
    }

    /* Make the matview match the newly generated data. */
    if concurrent {
        let old_depth: c_int = matview_maintenance_depth;

        PG_TRY!({
            refresh_by_match_merge(matviewOid, OIDNewHeap, relowner,
                                   save_sec_context);
        }, {
            matview_maintenance_depth = old_depth;
            PG_RE_THROW!();
        });
        Assert!(matview_maintenance_depth == old_depth);
    } else {
        refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

        /*
         * Inform cumulative stats system about our activity: basically, we
         * truncated the matview and inserted some new data.  (The concurrent
         * code path above doesn't need to worry about this because the
         * inserts and deletes it issues get counted by lower-level code.)
         */
        pgstat_count_truncate(matviewRel);
        if !skipData {
            pgstat_count_heap_insert(matviewRel, processed);
        }
    }

    table_close(matviewRel, NoLock);

    /* Roll back any GUC changes */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    ObjectAddressSet(&mut address, RelationRelationId, matviewOid);

    /*
     * Save the rowcount so that pg_stat_statements can track the total number
     * of rows processed by REFRESH MATERIALIZED VIEW command. Note that we
     * still don't display the rowcount in the command completion tag output,
     * i.e., the display_rowcount flag of CMDTAG_REFRESH_MATERIALIZED_VIEW
     * command tag is left false in cmdtaglist.h. Otherwise, the change of
     * completion tag output might break applications using it.
     *
     * When called from CREATE MATERIALIZED VIEW command, the rowcount is
     * displayed with the command tag CMDTAG_SELECT.
     */
    if !qc.is_null() {
        SetQueryCompletion(qc,
                           if is_create { CMDTAG_SELECT } else { CMDTAG_REFRESH_MATERIALIZED_VIEW },
                           processed);
    }

    return address;
}

/*
 * refresh_matview_datafill
 *
 * Execute the given query, sending result rows to "dest" (which will
 * insert them into the target matview).
 *
 * Returns number of rows inserted.
 */
unsafe fn refresh_matview_datafill(dest: *mut DestReceiver, mut query: *mut Query,
                                   queryString: *const c_char, is_create: bool) -> u64 {
    let rewritten: *mut List;
    let plan: *mut PlannedStmt;
    let queryDesc: *mut QueryDesc;
    let copied_query: *mut Query;
    let processed: u64;

    /* Lock and rewrite, using a copy to preserve the original query. */
    copied_query = copyObject(query as *mut _) as *mut Query;
    AcquireRewriteLocks(copied_query, true, false);
    rewritten = QueryRewrite(copied_query);

    /* SELECT should never rewrite to more or less than one SELECT query */
    if list_length(rewritten) != 1 {
        elog!(ERROR, "unexpected rewrite result for {}",
              if is_create { "CREATE MATERIALIZED VIEW " } else { "REFRESH MATERIALIZED VIEW" });
    }
    query = linitial(rewritten) as *mut Query;

    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();

    /* Plan the query which will generate data for the refresh. */
    plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, std::ptr::null_mut());

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.  (This could only matter if
     * the planner executed an allegedly-stable function that changed the
     * database contents, but let's do it anyway to be safe.)
     */
    PushCopiedSnapshot(GetActiveSnapshot());
    UpdateActiveSnapshotCommandId();

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    queryDesc = CreateQueryDesc(plan, queryString,
                                GetActiveSnapshot(), InvalidSnapshot,
                                dest, std::ptr::null_mut(), std::ptr::null_mut(), 0);

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, 0);

    /* run the plan */
    ExecutorRun(queryDesc, ForwardScanDirection, 0);

    processed = (*(*queryDesc).estate).es_processed;

    /* and clean up */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);

    PopActiveSnapshot();

    return processed;
}

pub unsafe fn CreateTransientRelDestReceiver(transientoid: Oid) -> *mut DestReceiver {
    let self_: *mut DR_transientrel =
        palloc0(std::mem::size_of::<DR_transientrel>()) as *mut DR_transientrel;

    (*self_).pub_.receiveSlot = Some(transientrel_receive);
    (*self_).pub_.rStartup = Some(transientrel_startup);
    (*self_).pub_.rShutdown = Some(transientrel_shutdown);
    (*self_).pub_.rDestroy = Some(transientrel_destroy);
    (*self_).pub_.mydest = DestTransientRel;
    (*self_).transientoid = transientoid;

    return self_ as *mut DestReceiver;
}

/*
 * transientrel_startup --- executor startup
 */
unsafe fn transientrel_startup(self_: *mut DestReceiver, _operation: c_int, _typeinfo: TupleDesc) {
    let myState: *mut DR_transientrel = self_ as *mut DR_transientrel;
    let transientrel: Relation;

    transientrel = table_open((*myState).transientoid, NoLock);

    /*
     * Fill private fields of myState for use by later routines
     */
    (*myState).transientrel = transientrel;
    (*myState).output_cid = GetCurrentCommandId(true);
    (*myState).ti_options = (TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN) as c_int;
    (*myState).bistate = GetBulkInsertState();

    /*
     * Valid smgr_targblock implies something already wrote to the relation.
     * This may be harmless, but this function hasn't planned for it.
     */
    Assert!(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
unsafe fn transientrel_receive(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState: *mut DR_transientrel = self_ as *mut DR_transientrel;

    /*
     * Note that the input slot might not be of the type of the target
     * relation. That's supported by table_tuple_insert(), but slightly less
     * efficient than inserting with the right slot - but the alternative
     * would be to copy into a slot of the right type, which would not be
     * cheap either. This also doesn't allow accessing per-AM data (say a
     * tuple's xmin), but since we don't do that here...
     */

    table_tuple_insert((*myState).transientrel,
                       slot,
                       (*myState).output_cid,
                       (*myState).ti_options,
                       (*myState).bistate);

    /* We know this is a newly created relation, so there are no indexes */

    return true;
}

/*
 * transientrel_shutdown --- executor end
 */
unsafe fn transientrel_shutdown(self_: *mut DestReceiver) {
    let myState: *mut DR_transientrel = self_ as *mut DR_transientrel;

    FreeBulkInsertState((*myState).bistate);

    table_finish_bulk_insert((*myState).transientrel, (*myState).ti_options);

    /* close transientrel, but keep lock until commit */
    table_close((*myState).transientrel, NoLock);
    (*myState).transientrel = std::ptr::null_mut();
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
unsafe fn transientrel_destroy(self_: *mut DestReceiver) {
    pfree(self_ as *mut _);
}


/*
 * Given a qualified temporary table name, append an underscore followed by
 * the given integer, to make a new table name based on the old one.
 * The result is a palloc'd string.
 *
 * As coded, this would fail to make a valid SQL name if the given name were,
 * say, "FOO"."BAR".  Currently, the table name portion of the input will
 * never be double-quoted because it's of the form "pg_temp_NNN", cf
 * make_new_heap().  But we might have to work harder someday.
 */
unsafe fn make_temptable_name_n(tempname: *mut c_char, n: c_int) -> *mut c_char {
    let mut namebuf: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut namebuf);
    appendStringInfoString(&mut namebuf, tempname);
    appendStringInfo(&mut namebuf, c"_%d".as_ptr(), n);
    return namebuf.data;
}

/*
 * refresh_by_match_merge
 *
 * Refresh a materialized view with transactional semantics, while allowing
 * concurrent reads.
 *
 * This is called after a new version of the data has been created in a
 * temporary table.  It performs a full outer join against the old version of
 * the data, producing "diff" results.  This join cannot work if there are any
 * duplicated rows in either the old or new versions, in the sense that every
 * column would compare as equal between the two rows.  It does work correctly
 * in the face of rows which have at least one NULL value, with all non-NULL
 * columns equal.  The behavior of NULLs on equality tests and on UNIQUE
 * indexes turns out to be quite convenient here; the tests we need to make
 * are consistent with default behavior.  If there is at least one UNIQUE
 * index on the materialized view, we have exactly the guarantee we need.
 *
 * The temporary table used to hold the diff results contains just the TID of
 * the old record (if matched) and the ROW from the new table as a single
 * column of complex record type (if matched).
 *
 * Once we have the diff table, we perform set-based DELETE and INSERT
 * operations against the materialized view, and discard both temporary
 * tables.
 *
 * Everything from the generation of the new data to applying the differences
 * takes place under cover of an ExclusiveLock, since it seems as though we
 * would want to prohibit not only concurrent REFRESH operations, but also
 * incremental maintenance.  It also doesn't seem reasonable or safe to allow
 * SELECT FOR UPDATE or SELECT FOR SHARE on rows being updated or deleted by
 * this command.
 */
unsafe fn refresh_by_match_merge(matviewOid: Oid, tempOid: Oid, relowner: Oid,
                                 save_sec_context: c_int) {
    let mut querybuf: StringInfoData = std::mem::zeroed();
    let matviewRel: Relation;
    let tempRel: Relation;
    let matviewname: *mut c_char;
    let tempname: *mut c_char;
    let diffname: *mut c_char;
    let tupdesc: TupleDesc;
    let mut foundUniqueIndex: bool;
    let indexoidlist: *mut List;
    let relnatts: i16;
    let opUsedForQual: *mut Oid;

    initStringInfo(&mut querybuf);
    matviewRel = table_open(matviewOid, NoLock);
    matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
                                             RelationGetRelationName(matviewRel));
    tempRel = table_open(tempOid, NoLock);
    tempname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel)),
                                          RelationGetRelationName(tempRel));
    diffname = make_temptable_name_n(tempname, 2);

    relnatts = RelationGetNumberOfAttributes(matviewRel) as i16;

    /* Open SPI context. */
    SPI_connect();

    /* Analyze the temp table with the new contents. */
    appendStringInfo(&mut querybuf, c"ANALYZE %s".as_ptr(), tempname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    /*
     * We need to ensure that there are not duplicate rows without NULLs in
     * the new data set before we can count on the "diff" results.  Check for
     * that in a way that allows showing the first duplicated row found.  Even
     * after we pass this test, a unique index on the materialized view may
     * find a duplicate key problem.
     *
     * Note: here and below, we use "tablename.*::tablerowtype" as a hack to
     * keep ".*" from being expanded into multiple columns in a SELECT list.
     * Compare ruleutils.c's get_variable().
     */
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf,
                     c"SELECT newdata.*::%s FROM %s newdata WHERE newdata.* IS NOT NULL AND EXISTS (SELECT 1 FROM %s newdata2 WHERE newdata2.* IS NOT NULL AND newdata2.* OPERATOR(pg_catalog.*=) newdata.* AND newdata2.ctid OPERATOR(pg_catalog.<>) newdata.ctid)".as_ptr(),
                     tempname, tempname, tempname);
    if SPI_execute(querybuf.data, false, 1) != SPI_OK_SELECT {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }
    if SPI_processed > 0 {
        /*
         * Note that this ereport() is returning data to the user.  Generally,
         * we would want to make sure that the user has been granted access to
         * this data.  However, REFRESH MAT VIEW is only able to be run by the
         * owner of the mat view (or a superuser) and therefore there is no
         * need to check for access to data in the mat view.
         */
        ereport!(ERROR,
                 "new data for materialized view contains duplicate rows without any null columns");
    }

    /*
     * Create the temporary "diff" table.
     *
     * Temporarily switch out of the SECURITY_RESTRICTED_OPERATION context,
     * because you cannot create temp tables in SRO context.  For extra
     * paranoia, add the composite type column only after switching back to
     * SRO context.
     */
    SetUserIdAndSecContext(relowner,
                           save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf,
                     c"CREATE TEMP TABLE %s (tid pg_catalog.tid)".as_ptr(),
                     diffname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }
    SetUserIdAndSecContext(relowner,
                           save_sec_context | SECURITY_RESTRICTED_OPERATION);
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf,
                     c"ALTER TABLE %s ADD COLUMN newdata %s".as_ptr(),
                     diffname, tempname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    /* Start building the query for populating the diff table. */
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf,
                     c"INSERT INTO %s SELECT mv.ctid AS tid, newdata.*::%s AS newdata FROM %s mv FULL JOIN %s newdata ON (".as_ptr(),
                     diffname, tempname, matviewname, tempname);

    /*
     * Get the list of index OIDs for the table from the relcache, and look up
     * each one in the pg_index syscache.  We will test for equality on all
     * columns present in all unique indexes which only reference columns and
     * include all rows.
     */
    tupdesc = (*matviewRel).rd_att;
    opUsedForQual = palloc0(std::mem::size_of::<Oid>() * relnatts as usize) as *mut Oid;
    foundUniqueIndex = false;

    indexoidlist = RelationGetIndexList(matviewRel);

    foreach!(indexoidscan, indexoidlist, {
        let indexoid: Oid = lfirst_oid(current_cell!(indexoidscan));
        let indexRel: Relation;

        indexRel = index_open(indexoid, RowExclusiveLock);
        if is_usable_unique_index(indexRel) {
            let indexStruct: Form_pg_index = (*indexRel).rd_index as Form_pg_index;
            let indnkeyatts: c_int = (*indexStruct).indnkeyatts as c_int;
            let indclass: *mut oidvector;
            let indclassDatum: Datum;
            let mut i: c_int;

            /* Must get indclass the hard way. */
            indclassDatum = SysCacheGetAttrNotNull(INDEXRELID,
                                                   (*indexRel).rd_indextuple as HeapTuple,
                                                   Anum_pg_index_indclass);
            indclass = DatumGetPointer(indclassDatum) as *mut oidvector;

            /* Add quals for all columns from this index. */
            i = 0;
            while i < indnkeyatts {
                let attnum: c_int = (*indexStruct).indkey.values[i as usize] as c_int;
                let opclass: Oid = *(*indclass).values.as_ptr().add(i as usize);
                let attr: Form_pg_attribute = TupleDescAttr(tupdesc, (attnum - 1) as usize);
                let attrtype: Oid = (*attr).atttypid;
                let cla_ht: HeapTuple;
                let cla_tup: Form_pg_opclass;
                let opfamily: Oid;
                let opcintype: Oid;
                let op: Oid;
                let leftop: *const c_char;
                let rightop: *const c_char;

                /*
                 * Identify the equality operator associated with this index
                 * column.  First we need to look up the column's opclass.
                 */
                cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
                if !HeapTupleIsValid(cla_ht) {
                    elog!(ERROR, "cache lookup failed for opclass {}", opclass);
                }
                cla_tup = GETSTRUCT(cla_ht) as Form_pg_opclass;
                opfamily = (*cla_tup).opcfamily;
                opcintype = (*cla_tup).opcintype;
                ReleaseSysCache(cla_ht);

                op = get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ);
                if !OidIsValid(op) {
                    elog!(ERROR, "missing equality operator for ({},{}) in opfamily {}",
                          opcintype, opcintype, opfamily);
                }

                /*
                 * If we find the same column with the same equality semantics
                 * in more than one index, we only need to emit the equality
                 * clause once.
                 *
                 * Since we only remember the last equality operator, this
                 * code could be fooled into emitting duplicate clauses given
                 * multiple indexes with several different opclasses ... but
                 * that's so unlikely it doesn't seem worth spending extra
                 * code to avoid.
                 */
                if *opUsedForQual.add((attnum - 1) as usize) == op {
                    i += 1;
                    continue;
                }
                *opUsedForQual.add((attnum - 1) as usize) = op;

                /*
                 * Actually add the qual, ANDed with any others.
                 */
                if foundUniqueIndex {
                    appendStringInfoString(&mut querybuf, c" AND ".as_ptr());
                }

                leftop = quote_qualified_identifier(c"newdata".as_ptr(),
                                                    NameStr!((*attr).attname));
                rightop = quote_qualified_identifier(c"mv".as_ptr(),
                                                     NameStr!((*attr).attname));

                generate_operator_clause(&mut querybuf,
                                         leftop, attrtype,
                                         op,
                                         rightop, attrtype);

                foundUniqueIndex = true;

                i += 1;
            }
        }

        /* Keep the locks, since we're about to run DML which needs them. */
        index_close(indexRel, NoLock);
    });

    list_free(indexoidlist);

    /*
     * There must be at least one usable unique index on the matview.
     *
     * ExecRefreshMatView() checks that after taking the exclusive lock on the
     * matview. So at least one unique index is guaranteed to exist here
     * because the lock is still being held.  (One known exception is if a
     * function called as part of refreshing the matview drops the index.
     * That's a pretty silly thing to do.)
     */
    if !foundUniqueIndex {
        ereport!(ERROR,
                 "could not find suitable unique index on materialized view");
    }

    appendStringInfoString(&mut querybuf,
                           c" AND newdata.* OPERATOR(pg_catalog.*=) mv.*) WHERE newdata.* IS NULL OR mv.* IS NULL ORDER BY tid".as_ptr());

    /* Populate the temporary "diff" table. */
    if SPI_exec(querybuf.data, 0) != SPI_OK_INSERT {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    /*
     * We have no further use for data from the "full-data" temp table, but we
     * must keep it around because its type is referenced from the diff table.
     */

    /* Analyze the diff table. */
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf, c"ANALYZE %s".as_ptr(), diffname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    OpenMatViewIncrementalMaintenance();

    /* Deletes must come before inserts; do them first. */
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf,
                     c"DELETE FROM %s mv WHERE ctid OPERATOR(pg_catalog.=) ANY (SELECT diff.tid FROM %s diff WHERE diff.tid IS NOT NULL AND diff.newdata IS NULL)".as_ptr(),
                     matviewname, diffname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_DELETE {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    /* Inserts go last. */
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf,
                     c"INSERT INTO %s SELECT (diff.newdata).* FROM %s diff WHERE tid IS NULL".as_ptr(),
                     matviewname, diffname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_INSERT {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    /* We're done maintaining the materialized view. */
    CloseMatViewIncrementalMaintenance();
    table_close(tempRel, NoLock);
    table_close(matviewRel, NoLock);

    /* Clean up temp tables. */
    resetStringInfo(&mut querybuf);
    appendStringInfo(&mut querybuf, c"DROP TABLE %s, %s".as_ptr(), diffname, tempname);
    if SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY {
        elog!(ERROR, "SPI_exec failed: {}", CStr_to_str(querybuf.data));
    }

    /* Close SPI context. */
    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }
}

/*
 * Swap the physical files of the target and transient tables, then rebuild
 * the target's indexes and throw away the transient table.  Security context
 * swapping is handled by the called function, so it is not needed here.
 */
unsafe fn refresh_by_heap_swap(matviewOid: Oid, OIDNewHeap: Oid, relpersistence: c_char) {
    finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
                     RecentXmin, ReadNextMultiXactId(), relpersistence);
}

/*
 * Check whether specified index is usable for match merge.
 */
unsafe fn is_usable_unique_index(indexRel: Relation) -> bool {
    let indexStruct: Form_pg_index = (*indexRel).rd_index as Form_pg_index;

    /*
     * Must be unique, valid, immediate, non-partial, and be defined over
     * plain user columns (not expressions).
     */
    if (*indexStruct).indisunique &&
        (*indexStruct).indimmediate &&
        (*indexStruct).indisvalid &&
        RelationGetIndexPredicate(indexRel) == NIL &&
        (*indexStruct).indnatts > 0 {
        /*
         * The point of groveling through the index columns individually is to
         * reject both index expressions and system columns.  Currently,
         * matviews couldn't have OID columns so there's no way to create an
         * index on a system column; but maybe someday that wouldn't be true,
         * so let's be safe.
         */
        let numatts: c_int = (*indexStruct).indnatts as c_int;
        let mut i: c_int;

        i = 0;
        while i < numatts {
            let attnum: c_int = (*indexStruct).indkey.values[i as usize] as c_int;

            if attnum <= 0 {
                return false;
            }
            i += 1;
        }
        return true;
    }
    return false;
}


/*
 * This should be used to test whether the backend is in a context where it is
 * OK to allow DML statements to modify materialized views.  We only want to
 * allow that for internal code driven by the materialized view definition,
 * not for arbitrary user-supplied code.
 *
 * While the function names reflect the fact that their main intended use is
 * incremental maintenance of materialized views (in response to changes to
 * the data in referenced relations), they are initially used to allow REFRESH
 * without blocking concurrent reads.
 */
pub unsafe fn MatViewIncrementalMaintenanceIsEnabled() -> bool {
    return matview_maintenance_depth > 0;
}

unsafe fn OpenMatViewIncrementalMaintenance() {
    matview_maintenance_depth += 1;
}

unsafe fn CloseMatViewIncrementalMaintenance() {
    matview_maintenance_depth -= 1;
    Assert!(matview_maintenance_depth >= 0);
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers.
// ---------------------------------------------------------------------------

unsafe fn CStr_to_str(_s: *const c_char) -> &'static str { unimplemented!() /* TODO: matview.c */ }
unsafe fn table_open(_relid: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!() /* TODO: access/table */ }
unsafe fn table_close(_rel: Relation, _lockmode: LOCKMODE) { unimplemented!() /* TODO: access/table */ }
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!() /* TODO: utils/cache/syscache */ }
unsafe fn HeapTupleIsValid(_t: HeapTuple) -> bool { unimplemented!() /* TODO: access/htup */ }
unsafe fn GETSTRUCT(_t: HeapTuple) -> *mut std::ffi::c_void { unimplemented!() /* TODO: access/htup_details */ }
unsafe fn RelationGetRelid(_rel: Relation) -> Oid { unimplemented!() /* TODO: utils/rel */ }
unsafe fn CatalogTupleUpdate(_rel: Relation, _otid: *mut crate::storage::itemptr::ItemPointerData, _tup: HeapTuple) { unimplemented!() /* TODO: catalog/indexing */ }
unsafe fn heap_freetuple(_t: HeapTuple) { unimplemented!() /* TODO: access/common/heaptuple */ }
unsafe fn CommandCounterIncrement() { unimplemented!() /* TODO: access/transam/xact */ }
unsafe fn RangeVarGetRelidExtended(_relation: *mut crate::nodes::primnodes::RangeVar, _lockmode: LOCKMODE, _flags: u32, _callback: Option<RangeVarGetRelidCallback>, _callback_arg: *mut std::ffi::c_void) -> Oid { unimplemented!() /* TODO: catalog/namespace */ }
unsafe extern "C" fn RangeVarCallbackMaintainsTable(_relation: *const crate::nodes::primnodes::RangeVar, _relId: Oid, _oldRelId: Oid, _arg: *mut std::ffi::c_void) { unimplemented!() /* TODO: commands/tablecmds */ }
unsafe fn GetUserIdAndSecContext(_userid: *mut Oid, _sec_context: *mut c_int) { unimplemented!() /* TODO: utils/init/miscinit */ }
unsafe fn SetUserIdAndSecContext(_userid: Oid, _sec_context: c_int) { unimplemented!() /* TODO: utils/init/miscinit */ }
unsafe fn NewGUCNestLevel() -> c_int { unimplemented!() /* TODO: utils/misc/guc */ }
unsafe fn RestrictSearchPath() { unimplemented!() /* TODO: catalog/namespace */ }
unsafe fn RelationIsPopulated(_rel: Relation) -> bool { unimplemented!() /* TODO: utils/rel */ }
unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char { unimplemented!() /* TODO: utils/rel */ }
unsafe fn list_length(_l: *const List) -> c_int { unimplemented!() /* TODO: nodes/pg_list */ }
unsafe fn RelationGetIndexList(_rel: Relation) -> *mut List { unimplemented!() /* TODO: utils/cache/relcache */ }
unsafe fn lfirst_oid(_cell: *mut ListCell) -> Oid { unimplemented!() /* TODO: nodes/pg_list */ }
unsafe fn index_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!() /* TODO: access/index */ }
unsafe fn index_close(_relation: Relation, _lockmode: LOCKMODE) { unimplemented!() /* TODO: access/index */ }
unsafe fn list_free(_l: *mut List) { unimplemented!() /* TODO: nodes/list */ }
unsafe fn quote_qualified_identifier(_qualifier: *const c_char, _ident: *const c_char) -> *mut c_char { unimplemented!() /* TODO: utils/adt/ruleutils */ }
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char { unimplemented!() /* TODO: utils/cache/lsyscache */ }
unsafe fn RelationGetNamespace(_rel: Relation) -> Oid { unimplemented!() /* TODO: utils/rel */ }
unsafe fn linitial(_l: *mut List) -> *mut std::ffi::c_void { unimplemented!() /* TODO: nodes/pg_list */ }
unsafe fn CheckTableNotInUse(_rel: Relation, _stmt: *const c_char) { unimplemented!() /* TODO: commands/tablecmds */ }
unsafe fn GetDefaultTablespace(_relpersistence: c_char, _partitioned: bool) -> Oid { unimplemented!() /* TODO: commands/tablespace */ }
unsafe fn make_new_heap(_OIDOldHeap: Oid, _NewTableSpace: Oid, _NewAccessMethod: Oid, _relpersistence: c_char, _lockmode: LOCKMODE) -> Oid { unimplemented!() /* TODO: commands/cluster */ }
unsafe fn CheckRelationOidLockedByMe(_relid: Oid, _lockmode: LOCKMODE, _orstronger: bool) -> bool { unimplemented!() /* TODO: storage/lmgr/lmgr */ }
unsafe fn pgstat_count_truncate(_rel: Relation) { unimplemented!() /* TODO: pgstat */ }
unsafe fn pgstat_count_heap_insert(_rel: Relation, _n: u64) { unimplemented!() /* TODO: pgstat */ }
unsafe fn AtEOXact_GUC(_isCommit: bool, _nestLevel: c_int) { unimplemented!() /* TODO: utils/misc/guc */ }
unsafe fn ObjectAddressSet(_addr: *mut ObjectAddress, _classId: Oid, _objectId: Oid) { unimplemented!() /* TODO: catalog/objectaddress */ }
unsafe fn SetQueryCompletion(_qc: *mut QueryCompletion, _commandTag: c_int, _nprocessed: u64) { unimplemented!() /* TODO: tcop/cmdtag */ }
unsafe fn copyObject(_from: *const std::ffi::c_void) -> *mut std::ffi::c_void { unimplemented!() /* TODO: nodes/copyfuncs */ }
unsafe fn AcquireRewriteLocks(_parsetree: *mut Query, _forExecute: bool, _forUpdatePushedDown: bool) { unimplemented!() /* TODO: rewrite/rewriteHandler */ }
unsafe fn QueryRewrite(_parsetree: *mut Query) -> *mut List { unimplemented!() /* TODO: rewrite/rewriteHandler */ }
unsafe fn pg_plan_query(_querytree: *mut Query, _query_string: *const c_char, _cursorOptions: c_int, _boundParams: *mut crate::nodes::params::ParamListInfoData) -> *mut PlannedStmt { unimplemented!() /* TODO: tcop/postgres */ }
unsafe fn PushCopiedSnapshot(_snapshot: crate::utils::snapshot::Snapshot) { unimplemented!() /* TODO: utils/time/snapmgr */ }
unsafe fn GetActiveSnapshot() -> crate::utils::snapshot::Snapshot { unimplemented!() /* TODO: utils/time/snapmgr */ }
unsafe fn UpdateActiveSnapshotCommandId() { unimplemented!() /* TODO: utils/time/snapmgr */ }
unsafe fn CreateQueryDesc(_plannedstmt: *mut PlannedStmt, _sourceText: *const c_char, _snapshot: crate::utils::snapshot::Snapshot, _crosscheck_snapshot: crate::utils::snapshot::Snapshot, _dest: *mut DestReceiver, _params: *mut crate::nodes::params::ParamListInfoData, _queryEnv: *mut crate::utils::misc::queryenvironment::QueryEnvironment, _instrument_options: c_int) -> *mut QueryDesc { unimplemented!() /* TODO: executor/execUtils */ }
unsafe fn ExecutorStart(_queryDesc: *mut QueryDesc, _eflags: c_int) -> bool { unimplemented!() /* TODO: executor/execMain */ }
unsafe fn ExecutorRun(_queryDesc: *mut QueryDesc, _direction: c_int, _count: u64) { unimplemented!() /* TODO: executor/execMain */ }
unsafe fn ExecutorFinish(_queryDesc: *mut QueryDesc) { unimplemented!() /* TODO: executor/execMain */ }
unsafe fn ExecutorEnd(_queryDesc: *mut QueryDesc) { unimplemented!() /* TODO: executor/execMain */ }
unsafe fn FreeQueryDesc(_qdesc: *mut QueryDesc) { unimplemented!() /* TODO: executor/execUtils */ }
unsafe fn PopActiveSnapshot() { unimplemented!() /* TODO: utils/time/snapmgr */ }
unsafe fn GetCurrentCommandId(_used: bool) -> CommandId { unimplemented!() /* TODO: access/transam/xact */ }
unsafe fn GetBulkInsertState() -> BulkInsertState { unimplemented!() /* TODO: access/heap/heapam */ }
unsafe fn RelationGetTargetBlock(_rel: Relation) -> crate::storage::block::BlockNumber { unimplemented!() /* TODO: utils/rel */ }
unsafe fn table_tuple_insert(_rel: Relation, _slot: *mut TupleTableSlot, _cid: CommandId, _options: c_int, _bistate: BulkInsertState) { unimplemented!() /* TODO: access/table/tableam */ }
unsafe fn FreeBulkInsertState(_bistate: BulkInsertState) { unimplemented!() /* TODO: access/heap/heapam */ }
unsafe fn table_finish_bulk_insert(_rel: Relation, _options: c_int) { unimplemented!() /* TODO: access/table/tableam */ }
unsafe fn initStringInfo(_str: *mut StringInfoData) { unimplemented!() /* TODO: lib/stringinfo */ }
unsafe fn resetStringInfo(_str: *mut StringInfoData) { unimplemented!() /* TODO: lib/stringinfo */ }
unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) { unimplemented!() /* TODO: lib/stringinfo */ }
unsafe extern "C" {
    // C printf-style varargs append (lib/stringinfo). Declared as a C variadic
    // because the translated calls use C format strings + varargs.
    fn appendStringInfo(str: *mut StringInfoData, fmt: *const c_char, ...);
}
unsafe fn RelationGetNumberOfAttributes(_rel: Relation) -> c_int { unimplemented!() /* TODO: utils/rel */ }
unsafe fn SPI_connect() -> c_int { unimplemented!() /* TODO: executor/spi */ }
unsafe fn SPI_exec(_src: *const c_char, _tcount: u64) -> c_int { unimplemented!() /* TODO: executor/spi */ }
unsafe fn SPI_execute(_src: *const c_char, _read_only: bool, _tcount: u64) -> c_int { unimplemented!() /* TODO: executor/spi */ }
unsafe fn SPI_getvalue(_tuple: HeapTuple, _tupdesc: TupleDesc, _fnumber: c_int) -> *mut c_char { unimplemented!() /* TODO: executor/spi */ }
unsafe fn SPI_finish() -> c_int { unimplemented!() /* TODO: executor/spi */ }
unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: crate::access::attnum::AttrNumber) -> Datum { unimplemented!() /* TODO: utils/cache/syscache */ }
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: usize) -> Form_pg_attribute { unimplemented!() /* TODO: access/common/tupdesc */ }
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!() /* TODO: utils/cache/syscache */ }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) { unimplemented!() /* TODO: utils/cache/syscache */ }
unsafe fn get_opfamily_member_for_cmptype(_opfamily: Oid, _lefttype: Oid, _righttype: Oid, _cmptype: c_int) -> Oid { unimplemented!() /* TODO: utils/cache/lsyscache */ }
unsafe fn OidIsValid(_oid: Oid) -> bool { unimplemented!() /* TODO: c.h */ }
unsafe fn generate_operator_clause(_buf: *mut StringInfoData, _leftop: *const c_char, _leftoptype: Oid, _opoid: Oid, _rightop: *const c_char, _rightoptype: Oid) { unimplemented!() /* TODO: utils/adt/ruleutils */ }
unsafe fn finish_heap_swap(_OIDOldHeap: Oid, _OIDNewHeap: Oid, _is_system_catalog: bool, _swap_toast_by_content: bool, _check_constraints: bool, _is_internal: bool, _frozenXid: crate::c::TransactionId, _cutoffMulti: crate::c::MultiXactId, _newrelpersistence: c_char) { unimplemented!() /* TODO: commands/cluster */ }
unsafe fn RelationGetIndexPredicate(_relation: Relation) -> *mut List { unimplemented!() /* TODO: utils/cache/relcache */ }
unsafe fn DatumGetPointer(_d: Datum) -> *mut std::ffi::c_void { unimplemented!() /* TODO: postgres.h */ }
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum { unimplemented!() /* TODO: postgres.h */ }

// ---------------------------------------------------------------------------
// Local constant stubs for unported dependencies.
// ---------------------------------------------------------------------------

const RELOID: c_int = 57; // TODO: utils/cache/syscache (SysCacheIdentifier)
const CLAOID: c_int = 14; // TODO: utils/cache/syscache (SysCacheIdentifier)
const INDEXRELID: c_int = 34; // TODO: utils/cache/syscache (SysCacheIdentifier)
const Anum_pg_index_indclass: crate::access::attnum::AttrNumber = 0; // TODO: catalog/pg_index_d.h

const SPI_OK_UTILITY: c_int = 4; // TODO: executor/spi.h
const SPI_OK_SELECT: c_int = 5; // TODO: executor/spi.h
const SPI_OK_INSERT: c_int = 6; // TODO: executor/spi.h
const SPI_OK_DELETE: c_int = 7; // TODO: executor/spi.h
const SPI_OK_FINISH: c_int = 1; // TODO: executor/spi.h

static mut SPI_processed: u64 = 0; // TODO: executor/spi.c (global rowcount)
static mut RecentXmin: crate::c::TransactionId = 0; // TODO: utils/snapmgr.c

unsafe fn ReadNextMultiXactId() -> crate::c::MultiXactId { unimplemented!() /* TODO: access/multixact */ }
