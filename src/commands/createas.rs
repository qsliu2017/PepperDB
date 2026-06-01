//! src/backend/commands/createas.c
//!
//! Execution of CREATE TABLE ... AS, a/k/a SELECT INTO.
//!   Since CREATE MATERIALIZED VIEW shares syntax and most behaviors,
//!   we implement that here, too.
//!
//! We implement this by diverting the query's normal output to a
//! specialized DestReceiver type.
//!
//! Formerly, CTAS was implemented as a variant of SELECT, which led
//! to assorted legacy behaviors that we still try to preserve, notably that
//! we must return a tuples-processed count in the QueryCompletion.  (We no
//! longer do that for CTAS ... WITH NO DATA, however.)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/createas.c

use crate::prelude::*;
// wave27 createas imports
use crate::{foreach, current_cell, IsA, castNode, makeNode, linitial_node, strVal};
use crate::nodes::nodes::CmdType::{CMD_SELECT, CMD_UTILITY};
use crate::catalog::pg_class::{RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_TOASTVALUE};
use crate::storage::lockdefs::{NoLock, AccessExclusiveLock};
use crate::storage::block::InvalidBlockNumber;
use crate::access::sdir::ForwardScanDirection;
use crate::executor::executor::EXEC_FLAG_WITH_NO_DATA;
use crate::tcop::dest::CommandDest::DestIntoRel;
use crate::access::table::tableam::TABLE_INSERT_SKIP_FSM;
use crate::utils::snapshot::InvalidSnapshot;
use crate::utils::misc::rls::RLS_ENABLED;
use crate::tcop::cmdtaglist::CMDTAG_SELECT;
use crate::nodes::parsenodes::CURSOR_OPT_PARALLEL_OK;

// NameStr (c.h): pointer to a NameData's bytes.
macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *mut c_char
    };
}
use NameStr;

// ObjectAddressSet (catalog/objectaddress.h): init an ObjectAddress in place.
macro_rules! ObjectAddressSet {
    ($addr:expr, $class:expr, $obj:expr) => {{
        $addr.classId = $class;
        $addr.objectId = $obj;
        $addr.objectSubId = 0;
    }};
}
use ObjectAddressSet;
use crate::catalog::catalog_oids::RelationRelationId;

// InvalidObjectAddress (objectaddress.h): the all-invalid address sentinel.
const InvalidObjectAddress: ObjectAddress = ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 };

// post_parse_analyze_hook (analyze.h): GUC-installed hook; none ported yet.
static mut post_parse_analyze_hook: Option<unsafe fn(*mut c_void, *mut c_void, *mut c_void)> = None;


use std::ffi::{c_char, c_int};

use crate::nodes::pg_list::*;

// ============================================================================
// Stub type aliases / opaque types for as-yet-unported modules.
// ============================================================================

type DestReceiver = crate::tcop::dest::DestReceiver;
type IntoClause = crate::nodes::primnodes::IntoClause;
type Relation = crate::utils::rel::Relation;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type CommandId = crate::c::CommandId;
type BulkInsertState = *mut std::ffi::c_void; // TODO: access/heapam.h BulkInsertStateData
type TupleDesc = crate::access::common::tupdesc::TupleDesc;
type TupleTableSlot = crate::executor::tuptable::TupleTableSlot;
type ParseState = crate::parser::parse_node::ParseState;
type CreateTableAsStmt = crate::nodes::parsenodes::CreateTableAsStmt;
type ParamListInfo = crate::nodes::params::ParamListInfo;
type QueryEnvironment = std::ffi::c_void; // TODO: utils/queryenvironment.h
type QueryCompletion = crate::tcop::cmdtag::QueryCompletion;
type Query = crate::nodes::parsenodes::Query;
type JumbleState = crate::nodes::queryjumble::JumbleState;
type CreateStmt = crate::nodes::parsenodes::CreateStmt;
type ColumnDef = crate::nodes::parsenodes::ColumnDef;
type TargetEntry = crate::nodes::primnodes::TargetEntry;
type Node = crate::nodes::nodes::Node;
type ExecuteStmt = crate::nodes::parsenodes::ExecuteStmt;
type PlannedStmt = crate::nodes::plannodes::PlannedStmt;
type QueryDesc = crate::executor::execdesc::QueryDesc;
type Form_pg_attribute = *mut crate::catalog::pg_attribute::FormData_pg_attribute;

/*
 * DR_intorel --- private DestReceiver state for CTAS.
 */
#[repr(C)]
pub struct DR_intorel {
    pub pub_: DestReceiver,       /* publicly-known function pointers */
    pub into: *mut IntoClause,    /* target relation specification */
    /* These fields are filled by intorel_startup: */
    pub rel: Relation,            /* relation to write to */
    pub reladdr: ObjectAddress,   /* address of rel, for ExecCreateTableAs */
    pub output_cid: CommandId,    /* cmin to insert in output tuples */
    pub ti_options: c_int,        /* table_tuple_insert performance options */
    pub bistate: BulkInsertState, /* bulk insert state */
}

/*
 * create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
unsafe fn create_ctas_internal(attrList: *mut List, into: *mut IntoClause) -> ObjectAddress {
    let create: *mut CreateStmt = makeNode!(CreateStmt, T_CreateStmt);
    let is_matview: bool;
    let relkind: c_char;
    let toast_options: Datum;
    let validnsps: [*const c_char; 2] = HEAP_RELOPT_NAMESPACES();
    let intoRelationAddr: ObjectAddress;

    /* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
    is_matview = !(*into).viewQuery.is_null();
    relkind = if is_matview {
        RELKIND_MATVIEW as c_char
    } else {
        RELKIND_RELATION as c_char
    };

    /*
     * Create the target relation by faking up a CREATE TABLE parsetree and
     * passing it to DefineRelation.
     */
    (*create).relation = (*into).rel;
    (*create).tableElts = attrList;
    (*create).inhRelations = std::ptr::null_mut(); /* NIL */
    (*create).ofTypename = std::ptr::null_mut();
    (*create).constraints = std::ptr::null_mut(); /* NIL */
    (*create).options = (*into).options;
    (*create).oncommit = (*into).onCommit;
    (*create).tablespacename = (*into).tableSpaceName;
    (*create).if_not_exists = false;
    (*create).accessMethod = (*into).accessMethod;

    /*
     * Create the relation.  (This will error out if there's an existing view,
     * so we don't need more code to complain if "replace" is false.)
     */
    intoRelationAddr = DefineRelation(
        create,
        relkind,
        InvalidOid,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );

    /*
     * If necessary, create a TOAST table for the target table.  Note that
     * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
     * that the TOAST table will be visible for insertion.
     */
    CommandCounterIncrement();

    /* parse and validate reloptions for the toast table */
    toast_options = transformRelOptions(
        0 as Datum,
        (*create).options,
        c"toast".as_ptr(),
        validnsps.as_ptr(),
        true,
        false,
    );

    let _ = heap_reloptions(RELKIND_TOASTVALUE as c_char, toast_options, true);

    NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

    /* Create the "view" part of a materialized view. */
    if is_matview {
        /* StoreViewQuery scribbles on tree, so make a copy */
        let query: *mut Query = copyObject((*into).viewQuery as *mut _) as *mut Query;

        StoreViewQuery(intoRelationAddr.objectId, query, false);
        CommandCounterIncrement();
    }

    intoRelationAddr
}

/*
 * create_ctas_nodata
 *
 * Create CTAS or materialized view when WITH NO DATA is used, starting from
 * the targetlist of the SELECT or view definition.
 */
unsafe fn create_ctas_nodata(tlist: *mut List, into: *mut IntoClause) -> ObjectAddress {
    let mut attrList: *mut List;
    let mut lc: *mut ListCell;

    /*
     * Build list of ColumnDefs from non-junk elements of the tlist.  If a
     * column name list was specified in CREATE TABLE AS, override the column
     * names in the query.  (Too few column names are OK, too many are not.)
     */
    attrList = std::ptr::null_mut(); /* NIL */
    lc = list_head((*into).colNames);
    foreach!(t, tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(t)) as *mut TargetEntry;

        if !(*tle).resjunk {
            let col: *mut ColumnDef;
            let colname: *mut c_char;

            if !lc.is_null() {
                colname = strVal!(lfirst(lc));
                lc = lnext((*into).colNames, lc);
            } else {
                colname = (*tle).resname;
            }

            col = makeColumnDef(
                colname,
                exprType((*tle).expr as *mut Node),
                exprTypmod((*tle).expr as *mut Node),
                exprCollation((*tle).expr as *mut Node),
            );

            /*
             * It's possible that the column is of a collatable type but the
             * collation could not be resolved, so double-check.  (We must
             * check this here because DefineRelation would adopt the type's
             * default collation rather than complaining.)
             */
            if !OidIsValid((*col).collOid) && type_is_collatable((*(*col).typeName).typeOid) {
                ereport!(
                    ERROR,
                    "no collation was derived for column with collatable type"
                );
            }

            attrList = lappend(attrList, col as *mut _);
        }
    });

    if !lc.is_null() {
        ereport!(ERROR, "too many column names were specified");
    }

    /* Create the relation definition using the ColumnDef list */
    create_ctas_internal(attrList, into)
}

/*
 * ExecCreateTableAs -- execute a CREATE TABLE AS command
 */
pub unsafe fn ExecCreateTableAs(
    pstate: *mut ParseState,
    stmt: *mut CreateTableAsStmt,
    params: ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    qc: *mut QueryCompletion,
) -> ObjectAddress {
    let mut query: *mut Query = castNode!(Query, T_Query, (*stmt).query);
    let into: *mut IntoClause = (*stmt).into;
    let mut jstate: *mut JumbleState = std::ptr::null_mut();
    let is_matview: bool = !(*into).viewQuery.is_null();
    let mut do_refresh: bool = false;
    let dest: *mut DestReceiver;
    let address: ObjectAddress;

    /* Check if the relation exists or not */
    if CreateTableAsRelExists(stmt) {
        return InvalidObjectAddress;
    }

    /*
     * Create the tuple receiver object and insert info it will need
     */
    dest = CreateIntoRelDestReceiver(into);

    /* Query contained by CTAS needs to be jumbled if requested */
    if IsQueryIdEnabled() {
        jstate = JumbleQuery(query);
    }

    if post_parse_analyze_hook.is_some() {
        (post_parse_analyze_hook.unwrap())(pstate as *mut c_void, query as *mut c_void, jstate as *mut c_void);
    }

    /*
     * The contained Query could be a SELECT, or an EXECUTE utility command.
     * If the latter, we just pass it off to ExecuteQuery.
     */
    if (*query).commandType == CMD_UTILITY && IsA!((*query).utilityStmt, T_ExecuteStmt) {
        let estmt: *mut ExecuteStmt = castNode!(ExecuteStmt, T_ExecuteStmt, (*query).utilityStmt);

        Assert!(!is_matview); /* excluded by syntax */
        ExecuteQuery(pstate, estmt, into, params, dest, qc);

        /* get object address that intorel_startup saved for us */
        address = (*(dest as *mut DR_intorel)).reladdr;

        return address;
    }
    Assert!((*query).commandType == CMD_SELECT);

    /*
     * For materialized views, always skip data during table creation, and use
     * REFRESH instead (see below).
     */
    if is_matview {
        do_refresh = !(*into).skipData;
        (*into).skipData = true;
    }

    if (*into).skipData {
        /*
         * If WITH NO DATA was specified, do not go through the rewriter,
         * planner and executor.  Just define the relation using a code path
         * similar to CREATE VIEW.  This avoids dump/restore problems stemming
         * from running the planner before all dependencies are set up.
         */
        address = create_ctas_nodata((*query).targetList, into);

        /*
         * For materialized views, reuse the REFRESH logic, which locks down
         * security-restricted operations and restricts the search_path.  This
         * reduces the chance that a subsequent refresh will fail.
         */
        if do_refresh {
            RefreshMatViewByOid(
                address.objectId,
                true,
                false,
                false,
                (*pstate).p_sourcetext,
                qc,
            );
        }
    } else {
        let rewritten: *mut List;
        let plan: *mut PlannedStmt;
        let queryDesc: *mut QueryDesc;

        Assert!(!is_matview);

        /*
         * Parse analysis was done already, but we still have to run the rule
         * rewriter.  We do not do AcquireRewriteLocks: we assume the query
         * either came straight from the parser, or suitable locks were
         * acquired by plancache.c.
         */
        rewritten = QueryRewrite(query);

        /* SELECT should never rewrite to more or less than one SELECT query */
        if list_length(rewritten) != 1 {
            elog!(ERROR, "unexpected rewrite result for CREATE TABLE AS SELECT");
        }
        query = linitial_node!(Query, T_Query, rewritten);
        Assert!((*query).commandType == CMD_SELECT);

        /* plan the query */
        plan = pg_plan_query(
            query,
            (*pstate).p_sourcetext,
            CURSOR_OPT_PARALLEL_OK as c_int,
            params,
        );

        /*
         * Use a snapshot with an updated command ID to ensure this query sees
         * results of any previously executed queries.  (This could only
         * matter if the planner executed an allegedly-stable function that
         * changed the database contents, but let's do it anyway to be
         * parallel to the EXPLAIN code path.)
         */
        PushCopiedSnapshot(GetActiveSnapshot());
        UpdateActiveSnapshotCommandId();

        /* Create a QueryDesc, redirecting output to our tuple receiver */
        queryDesc = CreateQueryDesc(
            plan,
            (*pstate).p_sourcetext,
            GetActiveSnapshot(),
            InvalidSnapshot,
            dest,
            params,
            queryEnv,
            0,
        );

        /* call ExecutorStart to prepare the plan for execution */
        ExecutorStart(queryDesc, GetIntoRelEFlags(into));

        /* run the plan to completion */
        ExecutorRun(queryDesc, ForwardScanDirection, 0);

        /* save the rowcount if we're given a qc to fill */
        if !qc.is_null() {
            SetQueryCompletion(qc, CMDTAG_SELECT, (*(*queryDesc).estate).es_processed);
        }

        /* get object address that intorel_startup saved for us */
        address = (*(dest as *mut DR_intorel)).reladdr;

        /* and clean up */
        ExecutorFinish(queryDesc);
        ExecutorEnd(queryDesc);

        FreeQueryDesc(queryDesc);

        PopActiveSnapshot();
    }

    address
}

/*
 * GetIntoRelEFlags --- compute executor flags needed for CREATE TABLE AS
 *
 * This is exported because EXPLAIN and PREPARE need it too.  (Note: those
 * callers still need to deal explicitly with the skipData flag; since they
 * use different methods for suppressing execution, it doesn't seem worth
 * trying to encapsulate that part.)
 */
pub unsafe fn GetIntoRelEFlags(intoClause: *mut IntoClause) -> c_int {
    let mut flags: c_int = 0;

    if (*intoClause).skipData {
        flags |= EXEC_FLAG_WITH_NO_DATA as c_int;
    }

    flags
}

/*
 * CreateTableAsRelExists --- check existence of relation for CreateTableAsStmt
 *
 * Utility wrapper checking if the relation pending for creation in this
 * CreateTableAsStmt query already exists or not.  Returns true if the
 * relation exists, otherwise false.
 */
pub unsafe fn CreateTableAsRelExists(ctas: *mut CreateTableAsStmt) -> bool {
    let nspid: Oid;
    let oldrelid: Oid;
    let mut address: ObjectAddress = std::mem::zeroed();
    let into: *mut IntoClause = (*ctas).into;

    nspid = RangeVarGetCreationNamespace((*into).rel);

    oldrelid = get_relname_relid((*(*into).rel).relname, nspid);
    if OidIsValid(oldrelid) {
        if !(*ctas).if_not_exists {
            ereport!(ERROR, "relation already exists");
        }

        /*
         * The relation exists and IF NOT EXISTS has been specified.
         *
         * If we are in an extension script, insist that the pre-existing
         * object be a member of the extension, to avoid security risks.
         */
        ObjectAddressSet!(address, RelationRelationId, oldrelid);
        checkMembershipInCurrentExtension(&mut address);

        /* OK to skip */
        ereport!(NOTICE, "relation already exists, skipping");
        return true;
    }

    /* Relation does not exist, it can be created */
    false
}

/*
 * CreateIntoRelDestReceiver -- create a suitable DestReceiver object
 *
 * intoClause will be NULL if called from CreateDestReceiver(), in which
 * case it has to be provided later.  However, it is convenient to allow
 * self->into to be filled in immediately for other callers.
 */
pub unsafe fn CreateIntoRelDestReceiver(intoClause: *mut IntoClause) -> *mut DestReceiver {
    let self_: *mut DR_intorel =
        palloc0(std::mem::size_of::<DR_intorel>()) as *mut DR_intorel;

    (*self_).pub_.receiveSlot = Some(intorel_receive);
    (*self_).pub_.rStartup = Some(intorel_startup);
    (*self_).pub_.rShutdown = Some(intorel_shutdown);
    (*self_).pub_.rDestroy = Some(intorel_destroy);
    (*self_).pub_.mydest = DestIntoRel;
    (*self_).into = intoClause;
    /* other private fields will be set during intorel_startup */

    self_ as *mut DestReceiver
}

/*
 * intorel_startup --- executor startup
 */
unsafe fn intorel_startup(self_: *mut DestReceiver, _operation: c_int, typeinfo: TupleDesc) {
    let myState: *mut DR_intorel = self_ as *mut DR_intorel;
    let into: *mut IntoClause = (*myState).into;
    let is_matview: bool;
    let mut attrList: *mut List;
    let intoRelationAddr: ObjectAddress;
    let intoRelationDesc: Relation;
    let mut lc: *mut ListCell;
    let mut attnum: c_int;

    Assert!(!into.is_null()); /* else somebody forgot to set it */

    /* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
    is_matview = !(*into).viewQuery.is_null();

    /*
     * Build column definitions using "pre-cooked" type and collation info. If
     * a column name list was specified in CREATE TABLE AS, override the
     * column names derived from the query.  (Too few column names are OK, too
     * many are not.)
     */
    attrList = std::ptr::null_mut(); /* NIL */
    lc = list_head((*into).colNames);
    attnum = 0;
    while attnum < (*typeinfo).natts {
        let attribute: Form_pg_attribute = TupleDescAttr(typeinfo, attnum);
        let col: *mut ColumnDef;
        let colname: *mut c_char;

        if !lc.is_null() {
            colname = strVal!(lfirst(lc));
            lc = lnext((*into).colNames, lc);
        } else {
            colname = NameStr!((*attribute).attname);
        }

        col = makeColumnDef(
            colname,
            (*attribute).atttypid,
            (*attribute).atttypmod,
            (*attribute).attcollation,
        );

        /*
         * It's possible that the column is of a collatable type but the
         * collation could not be resolved, so double-check.  (We must check
         * this here because DefineRelation would adopt the type's default
         * collation rather than complaining.)
         */
        if !OidIsValid((*col).collOid) && type_is_collatable((*(*col).typeName).typeOid) {
            ereport!(
                ERROR,
                "no collation was derived for column with collatable type"
            );
        }

        attrList = lappend(attrList, col as *mut _);

        attnum += 1;
    }

    if !lc.is_null() {
        ereport!(ERROR, "too many column names were specified");
    }

    /*
     * Actually create the target table
     */
    intoRelationAddr = create_ctas_internal(attrList, into);

    /*
     * Finally we can open the target table
     */
    intoRelationDesc = table_open(intoRelationAddr.objectId, AccessExclusiveLock as c_int);

    /*
     * Make sure the constructed table does not have RLS enabled.
     *
     * check_enable_rls() will ereport(ERROR) itself if the user has requested
     * something invalid, and otherwise will return RLS_ENABLED if RLS should
     * be enabled here.  We don't actually support that currently, so throw
     * our own ereport(ERROR) if that happens.
     */
    if check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED as c_int {
        ereport!(ERROR, "policies not yet implemented for this command");
    }

    /*
     * Tentatively mark the target as populated, if it's a matview and we're
     * going to fill it; otherwise, no change needed.
     */
    if is_matview && !(*into).skipData {
        SetMatViewPopulatedState(intoRelationDesc, true);
    }

    /*
     * Fill private fields of myState for use by later routines
     */
    (*myState).rel = intoRelationDesc;
    (*myState).reladdr = intoRelationAddr;
    (*myState).output_cid = GetCurrentCommandId(true);
    (*myState).ti_options = TABLE_INSERT_SKIP_FSM as c_int;

    /*
     * If WITH NO DATA is specified, there is no need to set up the state for
     * bulk inserts as there are no tuples to insert.
     */
    if !(*into).skipData {
        (*myState).bistate = GetBulkInsertState();
    } else {
        (*myState).bistate = std::ptr::null_mut();
    }

    /*
     * Valid smgr_targblock implies something already wrote to the relation.
     * This may be harmless, but this function hasn't planned for it.
     */
    Assert!(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber);
}

/*
 * intorel_receive --- receive one tuple
 */
unsafe fn intorel_receive(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState: *mut DR_intorel = self_ as *mut DR_intorel;

    /* Nothing to insert if WITH NO DATA is specified. */
    if !(*(*myState).into).skipData {
        /*
         * Note that the input slot might not be of the type of the target
         * relation. That's supported by table_tuple_insert(), but slightly
         * less efficient than inserting with the right slot - but the
         * alternative would be to copy into a slot of the right type, which
         * would not be cheap either. This also doesn't allow accessing per-AM
         * data (say a tuple's xmin), but since we don't do that here...
         */
        table_tuple_insert(
            (*myState).rel,
            slot,
            (*myState).output_cid,
            (*myState).ti_options,
            (*myState).bistate,
        );
    }

    /* We know this is a newly created relation, so there are no indexes */

    true
}

/*
 * intorel_shutdown --- executor end
 */
unsafe fn intorel_shutdown(self_: *mut DestReceiver) {
    let myState: *mut DR_intorel = self_ as *mut DR_intorel;
    let into: *mut IntoClause = (*myState).into;

    if !(*into).skipData {
        FreeBulkInsertState((*myState).bistate);
        table_finish_bulk_insert((*myState).rel, (*myState).ti_options);
    }

    /* close rel, but keep lock until commit */
    table_close((*myState).rel, NoLock as c_int);
    (*myState).rel = std::ptr::null_mut();
}

/*
 * intorel_destroy --- release DestReceiver object
 */
unsafe fn intorel_destroy(self_: *mut DestReceiver) {
    pfree(self_ as *mut _);
}

// ============================================================================
// Local stubs for helper functions / macros from not-yet-ported modules.
// ============================================================================

#[allow(non_snake_case)]
unsafe fn HEAP_RELOPT_NAMESPACES() -> [*const c_char; 2] {
    // #define HEAP_RELOPT_NAMESPACES { "toast", NULL }
    [c"toast".as_ptr(), std::ptr::null()]
}

unsafe fn DefineRelation(
    _stmt: *mut CreateStmt,
    _relkind: c_char,
    _ownerId: Oid,
    _typaddress: *mut ObjectAddress,
    _queryString: *const c_char,
) -> ObjectAddress {
    unimplemented!() // TODO: commands/tablecmds.c
}

unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO: access/transam/xact.c
}

unsafe fn transformRelOptions(
    _oldOptions: Datum,
    _defList: *mut List,
    _namspace: *const c_char,
    _validnsps: *const *const c_char,
    _acceptOidsOff: bool,
    _isReset: bool,
) -> Datum {
    unimplemented!() // TODO: access/common/reloptions.c
}

unsafe fn heap_reloptions(_relkind: c_char, _reloptions: Datum, _validate: bool) -> *mut crate::c::bytea {
    unimplemented!() // TODO: access/common/reloptions.c
}

unsafe fn NewRelationCreateToastTable(_relOid: Oid, _reloptions: Datum) {
    unimplemented!() // TODO: catalog/toasting.c
}

unsafe fn StoreViewQuery(_viewOid: Oid, _viewParse: *mut Query, _replace: bool) {
    unimplemented!() // TODO: commands/view.c
}

unsafe fn makeColumnDef(
    _colname: *const c_char,
    _typeOid: Oid,
    _typmod: i32,
    _collOid: Oid,
) -> *mut ColumnDef {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn exprType(_expr: *const Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn exprTypmod(_expr: *const Node) -> i32 {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn exprCollation(_expr: *const Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn type_is_collatable(_typid: Oid) -> bool {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn IsQueryIdEnabled() -> bool {
    unimplemented!() // TODO: nodes/queryjumble.c
}

unsafe fn JumbleQuery(_query: *mut Query) -> *mut JumbleState {
    unimplemented!() // TODO: nodes/queryjumble.c
}

unsafe fn ExecuteQuery(
    _pstate: *mut ParseState,
    _stmt: *mut ExecuteStmt,
    _intoClause: *mut IntoClause,
    _params: ParamListInfo,
    _dest: *mut DestReceiver,
    _qc: *mut QueryCompletion,
) {
    unimplemented!() // TODO: commands/prepare.c
}

unsafe fn RefreshMatViewByOid(
    _matviewOid: Oid,
    _is_create: bool,
    _skipData: bool,
    _concurrent: bool,
    _queryString: *const c_char,
    _qc: *mut QueryCompletion,
) -> ObjectAddress {
    unimplemented!() // TODO: commands/matview.c
}

unsafe fn QueryRewrite(_parsetree: *mut Query) -> *mut List {
    unimplemented!() // TODO: rewrite/rewriteHandler.c
}

unsafe fn pg_plan_query(
    _querytree: *mut Query,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    unimplemented!() // TODO: tcop/postgres.c
}

unsafe fn PushCopiedSnapshot(_snapshot: crate::utils::snapshot::Snapshot) {
    unimplemented!() // TODO: utils/time/snapmgr.c
}

unsafe fn UpdateActiveSnapshotCommandId() {
    unimplemented!() // TODO: utils/time/snapmgr.c
}

unsafe fn GetActiveSnapshot() -> crate::utils::snapshot::Snapshot {
    unimplemented!() // TODO: utils/time/snapmgr.c
}

unsafe fn CreateQueryDesc(
    _plannedstmt: *mut PlannedStmt,
    _sourceText: *const c_char,
    _snapshot: crate::utils::snapshot::Snapshot,
    _crosscheck_snapshot: crate::utils::snapshot::Snapshot,
    _dest: *mut DestReceiver,
    _params: ParamListInfo,
    _queryEnv: *mut QueryEnvironment,
    _instrument_options: c_int,
) -> *mut QueryDesc {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn ExecutorStart(_queryDesc: *mut QueryDesc, _eflags: c_int) -> bool {
    unimplemented!() // TODO: executor/execMain.c
}

unsafe fn ExecutorRun(
    _queryDesc: *mut QueryDesc,
    _direction: crate::access::sdir::ScanDirection,
    _count: u64,
) {
    unimplemented!() // TODO: executor/execMain.c
}

unsafe fn SetQueryCompletion(_qc: *mut QueryCompletion, _commandTag: c_int, _nprocessed: u64) {
    unimplemented!() // TODO: tcop/cmdtag.c
}

unsafe fn ExecutorFinish(_queryDesc: *mut QueryDesc) {
    unimplemented!() // TODO: executor/execMain.c
}

unsafe fn ExecutorEnd(_queryDesc: *mut QueryDesc) {
    unimplemented!() // TODO: executor/execMain.c
}

unsafe fn FreeQueryDesc(_qdesc: *mut QueryDesc) {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn PopActiveSnapshot() {
    unimplemented!() // TODO: utils/time/snapmgr.c
}

unsafe fn RangeVarGetCreationNamespace(_newRelation: *mut crate::nodes::primnodes::RangeVar) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}

unsafe fn get_relname_relid(_relname: *const c_char, _relnamespace: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn checkMembershipInCurrentExtension(_object: *const ObjectAddress) {
    unimplemented!() // TODO: catalog/pg_depend.c
}

unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}

unsafe fn check_enable_rls(_relid: Oid, _checkAsUser: Oid, _noError: bool) -> c_int {
    unimplemented!() // TODO: utils/misc/rls.c
}

unsafe fn SetMatViewPopulatedState(_relation: Relation, _newstate: bool) {
    unimplemented!() // TODO: commands/matview.c
}

unsafe fn GetCurrentCommandId(_used: bool) -> CommandId {
    unimplemented!() // TODO: access/transam/xact.c
}

unsafe fn GetBulkInsertState() -> BulkInsertState {
    unimplemented!() // TODO: access/heap/heapam.c
}

unsafe fn RelationGetTargetBlock(_relation: Relation) -> crate::storage::block::BlockNumber {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn table_tuple_insert(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: c_int,
    _bistate: BulkInsertState,
) {
    unimplemented!() // TODO: access/table/tableam.h
}

unsafe fn FreeBulkInsertState(_bistate: BulkInsertState) {
    unimplemented!() // TODO: access/heap/heapam.c
}

unsafe fn table_finish_bulk_insert(_rel: Relation, _options: c_int) {
    unimplemented!() // TODO: access/table/tableam.h
}

unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/table/table.c
}

unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!() // TODO: access/common/tupdesc.h
}

unsafe fn copyObject(_from: *const std::ffi::c_void) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: nodes/copyfuncs.c
}
