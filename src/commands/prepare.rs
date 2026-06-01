//! src/backend/commands/prepare.c
//!
//! prepare.c
//!   Prepareable SQL statements via PREPARE, EXECUTE and DEALLOCATE
//!
//! This module also implements storage of prepared statements that are
//! accessed via the extended FE/BE query protocol.
//!
//! Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/commands/prepare.c
//!
//! ----- (merged from src/include/commands/prepare.h) -----
//!
//! prepare.h
//!   PREPARE, EXECUTE and DEALLOCATE commands, and prepared-stmt storage
//!
//! Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! src/include/commands/prepare.h

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::miscadmin::TimestampTz;
use crate::nodes::pg_list::{lfirst, list_length, lnext, List, ListCell};
use crate::nodes::nodes::Node;
use crate::pg_config_manual::NAMEDATALEN;

// #[macro_export] macros live at the crate root.
use crate::{current_cell, foreach, linitial_node, lfirst_node, makeNode, IsA};

/*
 * The data structure representing a prepared statement.  This is now just
 * a thin veneer over a plancache entry --- the main addition is that of
 * a name.
 *
 * Note: all subsidiary storage lives in the referenced plancache entry.
 */
#[repr(C)]
pub struct PreparedStatement {
    /* dynahash.c requires key to be first field */
    pub stmt_name: [c_char; NAMEDATALEN as usize],
    pub plansource: *mut CachedPlanSource, /* the actual cached plan */
    pub from_sql: bool,                    /* prepared via SQL, not FE/BE protocol? */
    pub prepare_time: TimestampTz,         /* the time when the stmt was prepared */
}

/* --- stub types for unported dependencies --- */
/*
 * These are minimal local stubs for types whose defining modules (plancache,
 * parsenodes-as-needed, etc.) are not yet ported.  Only the fields actually
 * touched by this file are declared.  Opaque types that are only ever handled
 * by pointer keep the zero-variant `enum` form.
 */
pub enum HTAB {}

#[repr(C)]
pub struct CachedPlanSource {
    pub query_string: *const c_char,
    pub commandTag: *mut CommandTag,
    pub param_types: *mut Oid,
    pub num_params: c_int,
    pub fixed_result: bool,
    pub resultDesc: *mut TupleDesc,
    pub num_generic_plans: int64,
    pub num_custom_plans: int64,
}

#[repr(C)]
pub struct CachedPlan {
    pub stmt_list: *mut List,
}

#[repr(C)]
pub struct ParseState {
    pub p_sourcetext: *const c_char,
    pub p_queryEnv: *mut c_void,
}

#[repr(C)]
pub struct PrepareStmt {
    pub name: *mut c_char,
    pub query: *mut Node,
    pub argtypes: *mut List,
}

#[repr(C)]
pub struct ExecuteStmt {
    pub name: *mut c_char,
    pub params: *mut List,
}

#[repr(C)]
pub struct DeallocateStmt {
    pub name: *mut c_char,
}

#[repr(C)]
pub struct IntoClause {
    pub skipData: bool,
}

#[repr(C)]
pub struct ParamListInfo {
    pub params: *mut ParamExternData,
}

pub enum DestReceiver {}
pub enum QueryCompletion {}

#[repr(C)]
pub struct EState {
    pub es_param_list_info: *mut ParamListInfo,
}

#[repr(C)]
pub struct Portal {
    pub visible: bool,
    pub portalContext: MemoryContext,
}

#[repr(C)]
pub struct ExplainState {
    pub memory: bool,
    pub buffers: bool,
}

#[repr(C)]
pub struct RawStmt {
    pub stmt: *mut Node,
    pub stmt_location: c_int,
    pub stmt_len: c_int,
}

pub enum TypeName {}
pub enum ArrayType {}

#[repr(C)]
pub struct TupleDesc {
    pub natts: c_int,
}

/*
 * The hash table in which prepared queries are stored. This is
 * per-backend: query plans are not shared between backends.
 * The keys for this hash table are the arguments to PREPARE and EXECUTE
 * (statement names); the entries are PreparedStatement structs.
 */
static mut prepared_queries: *mut HTAB = std::ptr::null_mut();

/*
 * Implements the 'PREPARE' utility statement.
 */
pub unsafe fn PrepareQuery(
    pstate: *mut ParseState,
    stmt: *mut PrepareStmt,
    stmt_location: c_int,
    stmt_len: c_int,
) {
    let rawstmt: *mut RawStmt;
    let plansource: *mut CachedPlanSource;
    let mut argtypes: *mut Oid = std::ptr::null_mut();
    let mut nargs: c_int;
    let query_list: *mut List;

    /*
     * Disallow empty-string statement name (conflicts with protocol-level
     * unnamed statement).
     */
    if (*stmt).name.is_null() || *(*stmt).name == b'\0' as c_char {
        ereport!(
            ERROR,
            "invalid statement name: must not be empty"
        );
    }

    /*
     * Need to wrap the contained statement in a RawStmt node to pass it to
     * parse analysis.
     */
    rawstmt = makeNode!(RawStmt, T_RawStmt);
    (*rawstmt).stmt = (*stmt).query;
    (*rawstmt).stmt_location = stmt_location;
    (*rawstmt).stmt_len = stmt_len;

    /*
     * Create the CachedPlanSource before we do parse analysis, since it needs
     * to see the unmodified raw parse tree.
     */
    plansource = CreateCachedPlan(
        rawstmt,
        (*pstate).p_sourcetext,
        CreateCommandTag((*stmt).query),
    );

    /* Transform list of TypeNames to array of type OIDs */
    nargs = list_length((*stmt).argtypes);

    if nargs != 0 {
        let mut i: c_int;

        argtypes = palloc_array::<Oid>(nargs as usize);
        i = 0;

        foreach!(l, (*stmt).argtypes, {
            let tn = lfirst(current_cell!(l)) as *mut TypeName;
            let toid: Oid = typenameTypeId(pstate, tn);

            *argtypes.add(i as usize) = toid;
            i += 1;
        });
    }

    /*
     * Analyze the statement using these parameter types (any parameters
     * passed in from above us will not be visible to it), allowing
     * information about unknown parameters to be deduced from context.
     * Rewrite the query. The result could be 0, 1, or many queries.
     */
    query_list = pg_analyze_and_rewrite_varparams(
        rawstmt,
        (*pstate).p_sourcetext,
        &mut argtypes,
        &mut nargs,
        std::ptr::null_mut(),
    );

    /* Finish filling in the CachedPlanSource */
    CompleteCachedPlan(
        plansource,
        query_list,
        std::ptr::null_mut(),
        argtypes,
        nargs,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        CURSOR_OPT_PARALLEL_OK, /* allow parallel mode */
        true,                   /* fixed result */
    );

    /*
     * Save the results.
     */
    StorePreparedStatement((*stmt).name, plansource, true);
}

/*
 * ExecuteQuery --- implement the 'EXECUTE' utility statement.
 *
 * This code also supports CREATE TABLE ... AS EXECUTE.  That case is
 * indicated by passing a non-null intoClause.  The DestReceiver is already
 * set up correctly for CREATE TABLE AS, but we still have to make a few
 * other adjustments here.
 */
pub unsafe fn ExecuteQuery(
    pstate: *mut ParseState,
    stmt: *mut ExecuteStmt,
    intoClause: *mut IntoClause,
    params: *mut ParamListInfo,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let entry: *mut PreparedStatement;
    let cplan: *mut CachedPlan;
    let plan_list: *mut List;
    let mut paramLI: *mut ParamListInfo = std::ptr::null_mut();
    let mut estate: *mut EState = std::ptr::null_mut();
    let portal: *mut Portal;
    let query_string: *mut c_char;
    let eflags: c_int;
    let count: std::os::raw::c_long;

    /* Look it up in the hash table */
    entry = FetchPreparedStatement((*stmt).name, true);

    /* Shouldn't find a non-fixed-result cached plan */
    if !(*(*entry).plansource).fixed_result {
        elog!(
            ERROR,
            "EXECUTE does not support variable-result cached plans"
        );
    }

    /* Evaluate parameters, if any */
    if (*(*entry).plansource).num_params > 0 {
        /*
         * Need an EState to evaluate parameters; must not delete it till end
         * of query, in case parameters are pass-by-reference.  Note that the
         * passed-in "params" could possibly be referenced in the parameter
         * expressions.
         */
        estate = CreateExecutorState();
        (*estate).es_param_list_info = params;
        paramLI = EvaluateParams(pstate, entry, (*stmt).params, estate);
    }

    /* Create a new portal to run the query in */
    portal = CreateNewPortal();
    /* Don't display the portal in pg_cursors, it is for internal use only */
    (*portal).visible = false;

    /* Copy the plan's saved query string into the portal's memory */
    query_string = MemoryContextStrdup(
        (*portal).portalContext,
        (*(*entry).plansource).query_string,
    );

    /* Replan if needed, and increment plan refcount for portal */
    cplan = GetCachedPlan(
        (*entry).plansource,
        paramLI,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    plan_list = (*cplan).stmt_list;

    /*
     * DO NOT add any logic that could possibly throw an error between
     * GetCachedPlan and PortalDefineQuery, or you'll leak the plan refcount.
     */
    PortalDefineQuery(
        portal,
        std::ptr::null(),
        query_string,
        (*(*entry).plansource).commandTag,
        plan_list,
        cplan,
    );

    /*
     * For CREATE TABLE ... AS EXECUTE, we must verify that the prepared
     * statement is one that produces tuples.  Currently we insist that it be
     * a plain old SELECT.  In future we might consider supporting other
     * things such as INSERT ... RETURNING, but there are a couple of issues
     * to be settled first, notably how WITH NO DATA should be handled in such
     * a case (do we really want to suppress execution?) and how to pass down
     * the OID-determining eflags (PortalStart won't handle them in such a
     * case, and for that matter it's not clear the executor will either).
     *
     * For CREATE TABLE ... AS EXECUTE, we also have to ensure that the proper
     * eflags and fetch count are passed to PortalStart/PortalRun.
     */
    if !intoClause.is_null() {
        let pstmt: *mut PlannedStmt;

        if list_length(plan_list) != 1 {
            ereport!(
                ERROR,
                "prepared statement is not a SELECT"
            );
        }
        pstmt = linitial_node!(PlannedStmt, T_PlannedStmt, plan_list);
        if (*pstmt).commandType != CMD_SELECT {
            ereport!(
                ERROR,
                "prepared statement is not a SELECT"
            );
        }

        /* Set appropriate eflags */
        eflags = GetIntoRelEFlags(intoClause);

        /* And tell PortalRun whether to run to completion or not */
        if (*intoClause).skipData {
            count = 0;
        } else {
            count = FETCH_ALL;
        }
    } else {
        /* Plain old EXECUTE */
        eflags = 0;
        count = FETCH_ALL;
    }

    /*
     * Run the portal as appropriate.
     */
    PortalStart(portal, paramLI, eflags, GetActiveSnapshot());

    PortalRun(portal, count, false, dest, dest, qc);

    PortalDrop(portal, false);

    if !estate.is_null() {
        FreeExecutorState(estate);
    }

    /* No need to pfree other memory, MemoryContext will be reset */
}

/*
 * EvaluateParams: evaluate a list of parameters.
 *
 * pstate: parse state
 * pstmt: statement we are getting parameters for.
 * params: list of given parameter expressions (raw parser output!)
 * estate: executor state to use.
 *
 * Returns a filled-in ParamListInfo -- this can later be passed to
 * CreateQueryDesc(), which allows the executor to make use of the parameters
 * during query execution.
 */
unsafe fn EvaluateParams(
    pstate: *mut ParseState,
    pstmt: *mut PreparedStatement,
    mut params: *mut List,
    estate: *mut EState,
) -> *mut ParamListInfo {
    let param_types: *mut Oid = (*(*pstmt).plansource).param_types;
    let num_params: c_int = (*(*pstmt).plansource).num_params;
    let nparams: c_int = list_length(params);
    let paramLI: *mut ParamListInfo;
    let exprstates: *mut List;
    let mut i: c_int;

    if nparams != num_params {
        ereport!(
            ERROR,
            "wrong number of parameters for prepared statement"
        );
    }

    /* Quick exit if no parameters */
    if num_params == 0 {
        return std::ptr::null_mut();
    }

    /*
     * We have to run parse analysis for the expressions.  Since the parser is
     * not cool about scribbling on its input, copy first.
     */
    params = copyObject(params as *mut c_void) as *mut List;

    i = 0;
    foreach!(l, params, {
        let mut expr = lfirst(current_cell!(l)) as *mut Node;
        let expected_type_id: Oid = *param_types.add(i as usize);
        let given_type_id: Oid;

        expr = transformExpr(pstate, expr, EXPR_KIND_EXECUTE_PARAMETER);

        given_type_id = exprType(expr);

        expr = coerce_to_target_type(
            pstate,
            expr,
            given_type_id,
            expected_type_id,
            -1,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );

        if expr.is_null() {
            ereport!(
                ERROR,
                "parameter of type cannot be coerced to the expected type"
            );
        }

        /* Take care of collations in the finished expression. */
        assign_expr_collations(pstate, expr);

        *(lfirst_ptr(current_cell!(l))) = expr as *mut c_void;
        i += 1;
    });

    /* Prepare the expressions for execution */
    exprstates = ExecPrepareExprList(params, estate);

    paramLI = makeParamList(num_params);

    i = 0;
    foreach!(l, exprstates, {
        let n = lfirst(current_cell!(l)) as *mut ExprState;
        let prm: *mut ParamExternData = &mut *(*paramLI).params.add(i as usize);

        (*prm).ptype = *param_types.add(i as usize);
        (*prm).pflags = PARAM_FLAG_CONST;
        (*prm).value = ExecEvalExprSwitchContext(
            n,
            GetPerTupleExprContext(estate),
            &mut (*prm).isnull,
        );

        i += 1;
    });

    paramLI
}

/*
 * Initialize query hash table upon first use.
 */
unsafe fn InitQueryHashTable() {
    let mut hash_ctl: HASHCTL = std::mem::zeroed();

    hash_ctl.keysize = NAMEDATALEN as Size;
    hash_ctl.entrysize = std::mem::size_of::<PreparedStatement>() as Size;

    prepared_queries = hash_create(
        c"Prepared Queries".as_ptr(),
        32,
        &mut hash_ctl,
        HASH_ELEM | HASH_STRINGS,
    );
}

/*
 * Store all the data pertaining to a query in the hash table using
 * the specified key.  The passed CachedPlanSource should be "unsaved"
 * in case we get an error here; we'll save it once we've created the hash
 * table entry.
 */
pub unsafe fn StorePreparedStatement(
    stmt_name: *const c_char,
    plansource: *mut CachedPlanSource,
    from_sql: bool,
) {
    let entry: *mut PreparedStatement;
    let cur_ts: TimestampTz = GetCurrentStatementStartTimestamp();
    let mut found: bool = false;

    /* Initialize the hash table, if necessary */
    if prepared_queries.is_null() {
        InitQueryHashTable();
    }

    /* Add entry to hash table */
    entry = hash_search(
        prepared_queries,
        stmt_name as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut PreparedStatement;

    /* Shouldn't get a duplicate entry */
    if found {
        ereport!(
            ERROR,
            "prepared statement already exists"
        );
    }

    /* Fill in the hash table entry */
    (*entry).plansource = plansource;
    (*entry).from_sql = from_sql;
    (*entry).prepare_time = cur_ts;

    /* Now it's safe to move the CachedPlanSource to permanent memory */
    SaveCachedPlan(plansource);
}

/*
 * Lookup an existing query in the hash table. If the query does not
 * actually exist, throw ereport(ERROR) or return NULL per second parameter.
 *
 * Note: this does not force the referenced plancache entry to be valid,
 * since not all callers care.
 */
pub unsafe fn FetchPreparedStatement(
    stmt_name: *const c_char,
    throwError: bool,
) -> *mut PreparedStatement {
    let entry: *mut PreparedStatement;

    /*
     * If the hash table hasn't been initialized, it can't be storing
     * anything, therefore it couldn't possibly store our plan.
     */
    if !prepared_queries.is_null() {
        entry = hash_search(
            prepared_queries,
            stmt_name as *const c_void,
            HASH_FIND,
            std::ptr::null_mut(),
        ) as *mut PreparedStatement;
    } else {
        entry = std::ptr::null_mut();
    }

    if entry.is_null() && throwError {
        ereport!(
            ERROR,
            "prepared statement does not exist"
        );
    }

    entry
}

/*
 * Given a prepared statement, determine the result tupledesc it will
 * produce.  Returns NULL if the execution will not return tuples.
 *
 * Note: the result is created or copied into current memory context.
 */
pub unsafe fn FetchPreparedStatementResultDesc(
    stmt: *mut PreparedStatement,
) -> *mut TupleDesc {
    /*
     * Since we don't allow prepared statements' result tupdescs to change,
     * there's no need to worry about revalidating the cached plan here.
     */
    Assert!((*(*stmt).plansource).fixed_result);
    if !(*(*stmt).plansource).resultDesc.is_null() {
        CreateTupleDescCopy((*(*stmt).plansource).resultDesc)
    } else {
        std::ptr::null_mut()
    }
}

/*
 * Given a prepared statement that returns tuples, extract the query
 * targetlist.  Returns NIL if the statement doesn't have a determinable
 * targetlist.
 *
 * Note: this is pretty ugly, but since it's only used in corner cases like
 * Describe Statement on an EXECUTE command, we don't worry too much about
 * efficiency.
 */
pub unsafe fn FetchPreparedStatementTargetList(
    stmt: *mut PreparedStatement,
) -> *mut List {
    let tlist: *mut List;

    /* Get the plan's primary targetlist */
    tlist = CachedPlanGetTargetList((*stmt).plansource, std::ptr::null_mut());

    /* Copy into caller's context in case plan gets invalidated */
    copyObject(tlist as *mut c_void) as *mut List
}

/*
 * Implements the 'DEALLOCATE' utility statement: deletes the
 * specified plan from storage.
 */
pub unsafe fn DeallocateQuery(stmt: *mut DeallocateStmt) {
    if !(*stmt).name.is_null() {
        DropPreparedStatement((*stmt).name, true);
    } else {
        DropAllPreparedStatements();
    }
}

/*
 * Internal version of DEALLOCATE
 *
 * If showError is false, dropping a nonexistent statement is a no-op.
 */
pub unsafe fn DropPreparedStatement(stmt_name: *const c_char, showError: bool) {
    let entry: *mut PreparedStatement;

    /* Find the query's hash table entry; raise error if wanted */
    entry = FetchPreparedStatement(stmt_name, showError);

    if !entry.is_null() {
        /* Release the plancache entry */
        DropCachedPlan((*entry).plansource);

        /* Now we can remove the hash table entry */
        hash_search(
            prepared_queries,
            (*entry).stmt_name.as_ptr() as *const c_void,
            HASH_REMOVE,
            std::ptr::null_mut(),
        );
    }
}

/*
 * Drop all cached statements.
 */
pub unsafe fn DropAllPreparedStatements() {
    let mut seq: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut entry: *mut PreparedStatement;

    /* nothing cached */
    if prepared_queries.is_null() {
        return;
    }

    /* walk over cache */
    hash_seq_init(&mut seq, prepared_queries);
    loop {
        entry = hash_seq_search(&mut seq) as *mut PreparedStatement;
        if entry.is_null() {
            break;
        }
        /* Release the plancache entry */
        DropCachedPlan((*entry).plansource);

        /* Now we can remove the hash table entry */
        hash_search(
            prepared_queries,
            (*entry).stmt_name.as_ptr() as *const c_void,
            HASH_REMOVE,
            std::ptr::null_mut(),
        );
    }
}

/*
 * Implements the 'EXPLAIN EXECUTE' utility statement.
 *
 * "into" is NULL unless we are doing EXPLAIN CREATE TABLE AS EXECUTE,
 * in which case executing the query should result in creating that table.
 *
 * Note: the passed-in pstate's queryString is that of the EXPLAIN EXECUTE,
 * not the original PREPARE; we get the latter string from the plancache.
 */
pub unsafe fn ExplainExecuteQuery(
    execstmt: *mut ExecuteStmt,
    into: *mut IntoClause,
    es: *mut ExplainState,
    pstate: *mut ParseState,
    params: *mut ParamListInfo,
) {
    let entry: *mut PreparedStatement;
    let query_string: *const c_char;
    let cplan: *mut CachedPlan;
    let plan_list: *mut List;
    let mut paramLI: *mut ParamListInfo = std::ptr::null_mut();
    let mut estate: *mut EState = std::ptr::null_mut();
    let mut planstart: instr_time = std::mem::zeroed();
    let mut planduration: instr_time = std::mem::zeroed();
    let mut bufusage_start: BufferUsage = std::mem::zeroed();
    let mut bufusage: BufferUsage = std::mem::zeroed();
    let mut mem_counters: MemoryContextCounters = std::mem::zeroed();
    let mut planner_ctx: MemoryContext = std::ptr::null_mut();
    let mut saved_ctx: MemoryContext = std::ptr::null_mut();

    if (*es).memory {
        /* See ExplainOneQuery about this */
        Assert!(IsA!(CurrentMemoryContext, T_AllocSetContext));
        planner_ctx = AllocSetContextCreate(
            CurrentMemoryContext,
            c"explain analyze planner context".as_ptr(),
            ALLOCSET_DEFAULT_SIZES,
        );
        saved_ctx = MemoryContextSwitchTo(planner_ctx);
    }

    if (*es).buffers {
        bufusage_start = pgBufferUsage;
    }
    INSTR_TIME_SET_CURRENT(&mut planstart);

    /* Look it up in the hash table */
    entry = FetchPreparedStatement((*execstmt).name, true);

    /* Shouldn't find a non-fixed-result cached plan */
    if !(*(*entry).plansource).fixed_result {
        elog!(
            ERROR,
            "EXPLAIN EXECUTE does not support variable-result cached plans"
        );
    }

    query_string = (*(*entry).plansource).query_string;

    /* Evaluate parameters, if any */
    if (*(*entry).plansource).num_params != 0 {
        let pstate_params: *mut ParseState;

        pstate_params = make_parsestate(std::ptr::null_mut());
        (*pstate_params).p_sourcetext = (*pstate).p_sourcetext;

        /*
         * Need an EState to evaluate parameters; must not delete it till end
         * of query, in case parameters are pass-by-reference.  Note that the
         * passed-in "params" could possibly be referenced in the parameter
         * expressions.
         */
        estate = CreateExecutorState();
        (*estate).es_param_list_info = params;

        paramLI = EvaluateParams(pstate_params, entry, (*execstmt).params, estate);
    }

    /* Replan if needed, and acquire a transient refcount */
    cplan = GetCachedPlan(
        (*entry).plansource,
        paramLI,
        CurrentResourceOwner,
        (*pstate).p_queryEnv,
    );

    INSTR_TIME_SET_CURRENT(&mut planduration);
    INSTR_TIME_SUBTRACT(&mut planduration, planstart);

    if (*es).memory {
        MemoryContextSwitchTo(saved_ctx);
        MemoryContextMemConsumed(planner_ctx, &mut mem_counters);
    }

    /* calc differences of buffer counters. */
    if (*es).buffers {
        std::ptr::write_bytes(&mut bufusage as *mut BufferUsage, 0, 1);
        BufferUsageAccumDiff(&mut bufusage, &mut pgBufferUsage, &mut bufusage_start);
    }

    plan_list = (*cplan).stmt_list;

    /* Explain each query */
    foreach!(p, plan_list, {
        let pstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(p));

        if (*pstmt).commandType != CMD_UTILITY {
            ExplainOnePlan(
                pstmt,
                into,
                es,
                query_string,
                paramLI,
                (*pstate).p_queryEnv,
                &mut planduration,
                if (*es).buffers {
                    &mut bufusage
                } else {
                    std::ptr::null_mut()
                },
                if (*es).memory {
                    &mut mem_counters
                } else {
                    std::ptr::null_mut()
                },
            );
        } else {
            ExplainOneUtility((*pstmt).utilityStmt, into, es, pstate, paramLI);
        }

        /* No need for CommandCounterIncrement, as ExplainOnePlan did it */

        /* Separate plans with an appropriate separator */
        if !lnext(plan_list, current_cell!(p)).is_null() {
            ExplainSeparatePlans(es);
        }
    });

    if !estate.is_null() {
        FreeExecutorState(estate);
    }

    ReleaseCachedPlan(cplan, CurrentResourceOwner);
}

/*
 * This set returning function reads all the prepared statements and
 * returns a set of (name, statement, prepare_time, param_types, from_sql,
 * generic_plans, custom_plans).
 */
#[no_mangle]
pub unsafe extern "C" fn pg_prepared_statement(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    /*
     * We put all the tuples into a tuplestore in one scan of the hashtable.
     * This avoids any issue of the hashtable possibly changing between calls.
     */
    InitMaterializedSRF(fcinfo, 0);

    /* hash table might be uninitialized */
    if !prepared_queries.is_null() {
        let mut hash_seq: HASH_SEQ_STATUS = std::mem::zeroed();
        let mut prep_stmt: *mut PreparedStatement;

        hash_seq_init(&mut hash_seq, prepared_queries);
        loop {
            prep_stmt = hash_seq_search(&mut hash_seq) as *mut PreparedStatement;
            if prep_stmt.is_null() {
                break;
            }

            let result_desc: *mut TupleDesc;
            let mut values: [Datum; 8] = [0; 8];
            let mut nulls: [bool; 8] = [false; 8];

            result_desc = (*(*prep_stmt).plansource).resultDesc;

            values[0] = CStringGetTextDatum((*prep_stmt).stmt_name.as_ptr());
            values[1] = CStringGetTextDatum((*(*prep_stmt).plansource).query_string);
            values[2] = TimestampTzGetDatum((*prep_stmt).prepare_time);
            values[3] = build_regtype_array(
                (*(*prep_stmt).plansource).param_types,
                (*(*prep_stmt).plansource).num_params,
            );
            if !result_desc.is_null() {
                let result_types: *mut Oid;

                result_types = palloc_array::<Oid>((*result_desc).natts as usize);
                let mut i: c_int = 0;
                while i < (*result_desc).natts {
                    *result_types.add(i as usize) =
                        (*TupleDescAttr(result_desc, i)).atttypid;
                    i += 1;
                }
                values[4] = build_regtype_array(result_types, (*result_desc).natts);
            } else {
                /* no result descriptor (for example, DML statement) */
                nulls[4] = true;
            }
            values[5] = BoolGetDatum((*prep_stmt).from_sql);
            values[6] =
                Int64GetDatumFast((*(*prep_stmt).plansource).num_generic_plans);
            values[7] =
                Int64GetDatumFast((*(*prep_stmt).plansource).num_custom_plans);

            tuplestore_putvalues(
                (*rsinfo).setResult,
                (*rsinfo).setDesc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
        }
    }

    0 as Datum
}

/*
 * This utility function takes a C array of Oids, and returns a Datum
 * pointing to a one-dimensional Postgres array of regtypes. An empty
 * array is returned as a zero-element array, not NULL.
 */
unsafe fn build_regtype_array(param_types: *mut Oid, num_params: c_int) -> Datum {
    let tmp_ary: *mut Datum;
    let result: *mut ArrayType;
    let mut i: c_int;

    tmp_ary = palloc_array::<Datum>(num_params as usize);

    i = 0;
    while i < num_params {
        *tmp_ary.add(i as usize) = ObjectIdGetDatum(*param_types.add(i as usize));
        i += 1;
    }

    result = construct_array_builtin(tmp_ary, num_params, REGTYPEOID);
    PointerGetDatum(result as *mut c_void)
}

/* ------------------------------------------------------------------------
 * Local stubs for unported dependencies
 * ------------------------------------------------------------------------ */

#[repr(C)]
pub struct PlannedStmt {
    pub commandType: c_int,
    pub utilityStmt: *mut Node,
}

pub enum ExprState {}

#[repr(C)]
pub struct ParamExternData {
    pub value: Datum,
    pub isnull: bool,
    pub pflags: u16,
    pub ptype: Oid,
}

#[repr(C)]
pub struct ReturnSetInfo {
    pub setResult: *mut Tuplestorestate,
    pub setDesc: *mut TupleDesc,
}

#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub resultinfo: *mut c_void,
}
pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;

#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
}
// These are value types that get zero-initialized (`std::mem::zeroed`), so they
// must be inhabited (empty structs) rather than zero-variant enums.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HASH_SEQ_STATUS;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct instr_time;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BufferUsage;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemoryContextCounters;
// MemoryContext / MemoryContextData come from the prelude (crate::utils::palloc).
pub enum CommandTag {}
pub enum Snapshot {}
pub enum ResourceOwnerData {}

/* constants used above (stubbed) */
const CURSOR_OPT_PARALLEL_OK: c_int = 0; // TODO: nodes/parsenodes.h
const CMD_SELECT: c_int = 0; // TODO: nodes/nodes.h
const CMD_UTILITY: c_int = 0; // TODO: nodes/nodes.h
const FETCH_ALL: std::os::raw::c_long = 0; // TODO: tcop/pquery.h
const EXPR_KIND_EXECUTE_PARAMETER: c_int = 0; // TODO: parser/parse_node.h
const COERCION_ASSIGNMENT: c_int = 0; // TODO: nodes/primnodes.h
const COERCE_IMPLICIT_CAST: c_int = 0; // TODO: nodes/primnodes.h
const PARAM_FLAG_CONST: u16 = 0; // TODO: nodes/params.h
const HASH_ELEM: c_int = 0; // TODO: utils/hsearch.h
const HASH_STRINGS: c_int = 0; // TODO: utils/hsearch.h
const HASH_ENTER: c_int = 0; // TODO: utils/hsearch.h
const HASH_FIND: c_int = 0; // TODO: utils/hsearch.h
const HASH_REMOVE: c_int = 0; // TODO: utils/hsearch.h
const ALLOCSET_DEFAULT_SIZES: () = (); // TODO: utils/memutils.h (macro-expands to 4 args; placeholder)
const REGTYPEOID: Oid = 0; // TODO: catalog/pg_type_d.h

#[allow(non_upper_case_globals)]
static mut pgBufferUsage: BufferUsage = unsafe { std::mem::transmute([0u8; 0]) }; // TODO: executor/instrument.h

#[allow(non_upper_case_globals)]
static mut CurrentResourceOwner: *mut ResourceOwnerData = std::ptr::null_mut(); // TODO: utils/resowner.h

unsafe fn lfirst_ptr(_cell: *mut ListCell) -> *mut *mut c_void {
    unimplemented!() // TODO: nodes/pg_list.h (lvalue lfirst)
}

// palloc_array(type, count) in C expands to (type *) palloc(count * sizeof(type)).
unsafe fn palloc_array<T>(_count: usize) -> *mut T {
    unimplemented!() // TODO: utils/palloc.h (palloc_array macro)
}

unsafe fn CreateCachedPlan(
    _rawstmt: *mut RawStmt,
    _query_string: *const c_char,
    _commandTag: *mut CommandTag,
) -> *mut CachedPlanSource {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn CreateCommandTag(_parsetree: *mut Node) -> *mut CommandTag {
    unimplemented!() // TODO: tcop/cmdtag.c
}
unsafe fn typenameTypeId(_pstate: *mut ParseState, _tn: *mut TypeName) -> Oid {
    unimplemented!() // TODO: parser/parse_type.c
}
unsafe fn pg_analyze_and_rewrite_varparams(
    _rawstmt: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *mut *mut Oid,
    _numParams: *mut c_int,
    _queryEnv: *mut c_void,
) -> *mut List {
    unimplemented!() // TODO: tcop/postgres.c
}
unsafe fn CompleteCachedPlan(
    _plansource: *mut CachedPlanSource,
    _querytree_list: *mut List,
    _querytree_context: MemoryContext,
    _param_types: *mut Oid,
    _num_params: c_int,
    _parserSetup: *mut c_void,
    _parserSetupArg: *mut c_void,
    _cursor_options: c_int,
    _fixed_result: bool,
) {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn CreateExecutorState() -> *mut EState {
    unimplemented!() // TODO: executor/execUtils.c
}
unsafe fn CreateNewPortal() -> *mut Portal {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn MemoryContextStrdup(_ctx: MemoryContext, _s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn GetCachedPlan(
    _plansource: *mut CachedPlanSource,
    _boundParams: *mut ParamListInfo,
    _owner: *mut ResourceOwnerData,
    _queryEnv: *mut c_void,
) -> *mut CachedPlan {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn PortalDefineQuery(
    _portal: *mut Portal,
    _prepStmtName: *const c_char,
    _sourceText: *mut c_char,
    _commandTag: *mut CommandTag,
    _stmts: *mut List,
    _cplan: *mut CachedPlan,
) {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn GetIntoRelEFlags(_intoClause: *mut IntoClause) -> c_int {
    unimplemented!() // TODO: commands/createas.c
}
unsafe fn PortalStart(
    _portal: *mut Portal,
    _params: *mut ParamListInfo,
    _eflags: c_int,
    _snapshot: *mut Snapshot,
) {
    unimplemented!() // TODO: tcop/pquery.c
}
unsafe fn GetActiveSnapshot() -> *mut Snapshot {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn PortalRun(
    _portal: *mut Portal,
    _count: std::os::raw::c_long,
    _isTopLevel: bool,
    _dest: *mut DestReceiver,
    _altdest: *mut DestReceiver,
    _qc: *mut QueryCompletion,
) -> bool {
    unimplemented!() // TODO: tcop/pquery.c
}
unsafe fn PortalDrop(_portal: *mut Portal, _isError: bool) {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}
unsafe fn FreeExecutorState(_estate: *mut EState) {
    unimplemented!() // TODO: executor/execUtils.c
}
unsafe fn copyObject(_from: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: nodes/copyfuncs.c
}
unsafe fn transformExpr(
    _pstate: *mut ParseState,
    _expr: *mut Node,
    _exprKind: c_int,
) -> *mut Node {
    unimplemented!() // TODO: parser/parse_expr.c
}
unsafe fn exprType(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn coerce_to_target_type(
    _pstate: *mut ParseState,
    _expr: *mut Node,
    _exprtype: Oid,
    _targettype: Oid,
    _targettypmod: c_int,
    _ccontext: c_int,
    _cformat: c_int,
    _location: c_int,
) -> *mut Node {
    unimplemented!() // TODO: parser/parse_coerce.c
}
unsafe fn assign_expr_collations(_pstate: *mut ParseState, _expr: *mut Node) {
    unimplemented!() // TODO: parser/parse_collate.c
}
unsafe fn ExecPrepareExprList(_nodes: *mut List, _estate: *mut EState) -> *mut List {
    unimplemented!() // TODO: executor/execExpr.c
}
unsafe fn makeParamList(_numParams: c_int) -> *mut ParamListInfo {
    unimplemented!() // TODO: nodes/params.c
}
unsafe fn ExecEvalExprSwitchContext(
    _state: *mut ExprState,
    _econtext: *mut c_void,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: executor/execExprInterp.c
}
unsafe fn GetPerTupleExprContext(_estate: *mut EState) -> *mut c_void {
    unimplemented!() // TODO: executor/executor.h
}
unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: std::os::raw::c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *const c_void,
    _action: c_int,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    unimplemented!() // TODO: access/transam/xact.c
}
unsafe fn SaveCachedPlan(_plansource: *mut CachedPlanSource) {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn DropCachedPlan(_plansource: *mut CachedPlanSource) {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn CreateTupleDescCopy(_tupdesc: *mut TupleDesc) -> *mut TupleDesc {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn CachedPlanGetTargetList(
    _plansource: *mut CachedPlanSource,
    _queryEnv: *mut c_void,
) -> *mut List {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn make_parsestate(_parentParseState: *mut ParseState) -> *mut ParseState {
    unimplemented!() // TODO: parser/parse_node.c
}
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _sizes: (),
) -> MemoryContext {
    unimplemented!() // TODO: utils/mmgr/aset.c
}
unsafe fn MemoryContextMemConsumed(
    _context: MemoryContext,
    _consumed: *mut MemoryContextCounters,
) {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn INSTR_TIME_SET_CURRENT(_t: *mut instr_time) {
    unimplemented!() // TODO: portability/instr_time.h
}
unsafe fn INSTR_TIME_SUBTRACT(_x: *mut instr_time, _y: instr_time) {
    unimplemented!() // TODO: portability/instr_time.h
}
unsafe fn BufferUsageAccumDiff(
    _dst: *mut BufferUsage,
    _add: *mut BufferUsage,
    _sub: *mut BufferUsage,
) {
    unimplemented!() // TODO: executor/instrument.c
}
unsafe fn ExplainOnePlan(
    _plannedstmt: *mut PlannedStmt,
    _into: *mut IntoClause,
    _es: *mut ExplainState,
    _queryString: *const c_char,
    _params: *mut ParamListInfo,
    _queryEnv: *mut c_void,
    _planduration: *mut instr_time,
    _bufusage: *mut BufferUsage,
    _mem_counters: *mut MemoryContextCounters,
) {
    unimplemented!() // TODO: commands/explain.c
}
unsafe fn ExplainOneUtility(
    _utilityStmt: *mut Node,
    _into: *mut IntoClause,
    _es: *mut ExplainState,
    _pstate: *mut ParseState,
    _params: *mut ParamListInfo,
) {
    unimplemented!() // TODO: commands/explain.c
}
unsafe fn ExplainSeparatePlans(_es: *mut ExplainState) {
    unimplemented!() // TODO: commands/explain.c
}
unsafe fn ReleaseCachedPlan(_plan: *mut CachedPlan, _owner: *mut ResourceOwnerData) {
    unimplemented!() // TODO: utils/plancache.c
}
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/builtins.h
}
unsafe fn TimestampTzGetDatum(_ts: TimestampTz) -> Datum {
    unimplemented!() // TODO: utils/timestamp.h
}
unsafe fn TupleDescAttr(_tupdesc: *mut TupleDesc, _i: c_int) -> *mut FormData_pg_attribute {
    unimplemented!() // TODO: access/tupdesc.h
}
unsafe fn BoolGetDatum(_b: bool) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int64GetDatumFast(_n: int64) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: *mut TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn construct_array_builtin(
    _elems: *mut Datum,
    _nelems: c_int,
    _elmtype: Oid,
) -> *mut ArrayType {
    unimplemented!() // TODO: utils/adt/arrayfuncs.c
}
unsafe fn PointerGetDatum(_p: *mut c_void) -> Datum {
    unimplemented!() // TODO: postgres.h
}

#[repr(C)]
pub struct FormData_pg_attribute {
    pub atttypid: Oid,
}
pub enum Tuplestorestate {}
pub enum AllocSetContext {}
