//! Prepareable SQL statements via PREPARE / EXECUTE / DEALLOCATE, plus the
//! per-backend prepared-statement store the extended FE/BE protocol shares.
//! Translated from backend/commands/prepare.c (disposition: full leaf, the
//! M9-reachable subset).
//!
//! PREPARE wraps the inner statement in a `RawStmt`, creates a `CachedPlanSource`
//! over the unmodified raw tree, runs `parse_analyze_varparams` with the declared
//! argument types (so unknown $n types are deduced), completes the source, and
//! stores it in the per-backend prepared-statement table keyed by name. EXECUTE
//! looks the entry up, evaluates the EXECUTE argument expressions to a
//! `ParamListInfo` (EvaluateParams), gets the cached plan, and runs it into the
//! destination. DEALLOCATE drops the entry.
//!
//! Ownership (rules.md s10): the prepared-statement table is per-task state (a
//! `tokio::task_local!` + `RefCell`, like the relcache and the named-portal
//! table), since a `CachedPlanSource` holds plans and is not `Send`. The C
//! `prepared_queries` dynahash becomes that table; `StorePreparedStatement` /
//! `FetchPreparedStatement` / `DropPreparedStatement` operate on it under a
//! borrow closure rather than handing out a long-lived pointer.
//!
//! STAGED: EXPLAIN EXECUTE, CREATE TABLE AS EXECUTE (the `intoClause` path),
//! `pg_prepared_statement` SRF, and the generic-vs-custom cost tuning (plancache).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::prepare::PreparedStatement;
use crate::nodes::nodes::Node;
use crate::nodes::params::{makeParamList, ParamFlags, ParamListInfo};
use crate::nodes::parsenodes::{DeallocateStmt, ExecuteStmt, PrepareStmt, RawStmt};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::CommandTag;
use crate::tcop::dest::DestReceiver;
use crate::utils::plancache::CachedPlanSource;

// ---------------------------------------------------------------------------
// The per-backend prepared-statement table (PG `prepared_queries` dynahash).
// ---------------------------------------------------------------------------

tokio::task_local! {
    static PREPARED_QUERIES: RefCell<HashMap<String, PreparedStatement>>;
}

/// Establish the per-task prepared-statement table and run `fut`. Wrapped into
/// the backend's connect-to-database scope stack (postgres.rs `init_postgres`).
pub async fn prepared_scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    PREPARED_QUERIES.scope(RefCell::new(HashMap::new()), fut).await
}

fn with_table<R>(f: impl FnOnce(&mut HashMap<String, PreparedStatement>) -> R) -> Option<R> {
    PREPARED_QUERIES.try_with(|cell| f(&mut cell.borrow_mut())).ok()
}

// ---------------------------------------------------------------------------
// PREPARE
// ---------------------------------------------------------------------------

/// PG `PrepareQuery`: implement the PREPARE utility statement. Parse-analyzes the
/// contained statement with the declared parameter types (deducing unknowns from
/// context), completes a `CachedPlanSource`, and stores it under `stmt->name`.
///
/// Async because parse-analysis of the inner statement can open relations (e.g.
/// `PREPARE p AS SELECT * FROM t`). The const `SELECT $1 + 1` path opens none.
pub async fn prepare_query(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &PrepareStmt,
    stmt_location: i32,
    stmt_len: i32,
) {
    use crate::backend::parser::analyze::parse_analyze_varparams_async;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::nodes::nodes::CmdType;

    // Disallow empty-string statement name (conflicts with the protocol-level
    // unnamed statement).
    let name = stmt.name.as_deref().unwrap_or("");
    if name.is_empty() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_PSTATEMENT_DEFINITION)
                .errmsg("invalid statement name: must not be empty".to_string());
        });
    }

    let inner = stmt
        .query
        .clone()
        .unwrap_or_else(|| unreachable!("PREPARE carries its contained statement"));
    let source_text = pstate.p_sourcetext.clone().unwrap_or_default();

    // Wrap the contained statement in a RawStmt for parse analysis.
    let rawstmt = RawStmt {
        stmt: Some(inner.clone()),
        stmt_location,
        stmt_len,
    };

    // Create the CachedPlanSource over the unmodified raw parse tree.
    let command_tag = crate::backend::tcop::utility::create_command_tag(&inner);
    let mut plansource =
        crate::backend::utils::cache::plancache::CreateCachedPlan(rawstmt.clone(), &source_text, command_tag);

    // Transform the declared TypeName list to an array of type OIDs (async: the
    // type-name -> OID lookup warms + reads the TYPENAMENSP syscache).
    let mut argtypes: Vec<Oid> = Vec::with_capacity(stmt.argtypes.len());
    for tn in &stmt.argtypes {
        argtypes.push(typename_to_oid(shared, tn).await);
    }

    // Analyze with these parameter types (deducing unknowns from context), then
    // rewrite. M9 reaches a single plannable query (no rules/views).
    let analyzed = parse_analyze_varparams_async(shared, &rawstmt, &source_text, &mut argtypes).await;
    let query_list: Vec<crate::nodes::parsenodes::Query> = if matches!(
        analyzed.commandType,
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
    ) {
        vec![*analyzed]
    } else {
        query_rewrite(*analyzed)
    };

    // Complete the CachedPlanSource: install the query list + resolved param spec.
    let num_params = i32::try_from(argtypes.len()).unwrap_or(0);
    crate::backend::utils::cache::plancache::CompleteCachedPlan(
        &mut plansource,
        query_list,
        &argtypes,
        num_params,
        None,
        0, // cursor_options: parallel mode is staged; plan with no special options
        true, // fixed result
    );

    store_prepared_statement(name, plansource, true);
}

/// Resolve a declared argument `TypeName` node to its type OID (PG
/// `typenameTypeId`). The PREPARE arg list carries `Node::TypeName` entries.
/// Async: the type-name -> OID lookup warms + reads the TYPENAMENSP syscache
/// (the same path CREATE TABLE column types use).
async fn typename_to_oid(shared: &Arc<SharedState>, tn: &Node) -> Oid {
    use crate::backend::catalog::namespace::{
        lookup_explicit_namespace, typename_get_typid, typename_nsp_get_typid,
    };
    let Node::TypeName(type_name) = tn else {
        unimplemented!("PREPARE argument type is not a TypeName: {tn:?}");
    };
    if type_name.names.is_empty() {
        if type_name.typeOid == crate::postgres_ext::InvalidOid {
            unimplemented!("PREPARE: OID-less internal TypeName");
        }
        return type_name.typeOid;
    }
    let names: Vec<&str> = type_name.names.iter().map(|s| s.sval.as_str()).collect();
    let resolved = match names.as_slice() {
        [typname] => typename_get_typid(shared, typname).await,
        [schemaname, typname] => match lookup_explicit_namespace(schemaname, false) {
            Some(nsp) => typename_nsp_get_typid(shared, typname, nsp).await,
            None => None,
        },
        _ => unimplemented!("PREPARE: 3+-part type name"),
    };
    resolved.unwrap_or_else(|| {
        let printed = names.join(".");
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("type \"{printed}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    })
}

// ---------------------------------------------------------------------------
// EXECUTE
// ---------------------------------------------------------------------------

/// PG `ExecuteQuery`: implement the EXECUTE utility statement. Looks up the
/// prepared statement, evaluates the EXECUTE argument expressions into a
/// `ParamListInfo`, gets the cached plan, and runs it into `dest`.
///
/// Async: running the plan reaches the executor / buffer pool. The CREATE TABLE
/// AS EXECUTE (`intoClause`) path is STAGED.
pub async fn execute_query(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &ExecuteStmt,
    dest_kind: crate::tcop::dest::CommandDest,
    qc: &mut QueryCompletion,
) {
    use crate::utils::elog::ERROR;
    let name = stmt.name.as_deref().unwrap_or("");

    // Evaluate the EXECUTE params (if the prepared statement declares any) into a
    // ParamListInfo, and capture the query string + command tag from the entry.
    // The lookup + EvaluateParams happen under a table borrow; the plan list is
    // cloned out so the borrow is released before the async run.
    let (query_string, command_tag, stmt_list, param_li) = {
        let resolved = with_table(|t| {
            let entry = t.get_mut(name);
            let Some(entry) = entry else {
                return Err(());
            };
            if !entry.plansource.fixed_result {
                crate::elog!(ERROR, "EXECUTE does not support variable-result cached plans");
            }
            let num_params = entry.plansource.num_params;
            let param_types = entry.plansource.param_types.clone();
            let param_li = if num_params > 0 {
                Some(evaluate_params(pstate, name, &param_types, &stmt.params))
            } else {
                None
            };
            // GetCachedPlan: replan if needed (custom plan when params are bound).
            let cplan = crate::backend::utils::cache::plancache::GetCachedPlan(
                &mut entry.plansource,
                param_li.as_deref(),
                None,
            );
            Ok((
                entry.plansource.query_string.clone(),
                entry.plansource.commandTag,
                cplan.stmt_list.clone(),
                param_li,
            ))
        })
        .unwrap_or_else(|| unreachable!("execute_query outside a prepared-statement scope"));
        match resolved {
            Ok(v) => v,
            Err(()) => prepared_statement_does_not_exist(name),
        }
    };

    // Run the (single) plan into the destination. M9 reaches a single plannable
    // statement; a multi-statement plan list grows with rules.
    let mut total: u64 = 0;
    let mut tag = command_tag;
    for plan in &stmt_list {
        if plan.command_type == crate::nodes::nodes::CmdType::UTILITY {
            unimplemented!("EXECUTE of a prepared utility statement deferred");
        }
        // Build a fresh receiver of the same kind as the command's destination (PG
        // hands the EXECUTE its own portal + dest). DestRemote printtup writes to
        // the per-task send buffer, so a fresh receiver behaves identically to the
        // utility dispatcher's. Set text format for every column (simple wire mode).
        let mut receiver = crate::backend::tcop::dest::create_dest_receiver(dest_kind);
        if dest_kind == crate::tcop::dest::CommandDest::DestRemote {
            crate::backend::access::common::printtup::set_remote_dest_receiver_params(
                receiver.as_mut(),
                &[],
            );
        }
        total += crate::backend::tcop::postgres::execute_plan_into(
            shared,
            plan,
            &query_string,
            param_li.as_deref(),
            receiver,
            0,
        )
        .await;
        tag = command_tag_for_plan(plan, command_tag);
    }

    qc.command_tag = tag;
    qc.nprocessed = total;
}

/// The completion tag for a just-run EXECUTE plan (SELECT keeps the source tag;
/// DML reports its own).
fn command_tag_for_plan(
    plan: &crate::nodes::plannodes::PlannedStmt,
    fallback: CommandTag,
) -> CommandTag {
    use crate::nodes::nodes::CmdType;
    match plan.command_type {
        CmdType::SELECT => CommandTag::Select,
        CmdType::INSERT => CommandTag::Insert,
        CmdType::UPDATE => CommandTag::Update,
        CmdType::DELETE => CommandTag::Delete,
        CmdType::MERGE => CommandTag::Merge,
        _ => fallback,
    }
}

// ---------------------------------------------------------------------------
// EvaluateParams
// ---------------------------------------------------------------------------

/// PG `EvaluateParams`: evaluate the EXECUTE argument expressions into a
/// `ParamListInfo`. Each raw expression is transformed (transformExpr), coerced
/// to the prepared statement's declared parameter type, prepared (ExecInitExpr),
/// and evaluated to a constant Datum.
#[allow(
    clippy::unnecessary_box_returns,
    reason = "ParamListInfo is the PG-canonical handle type (Box<ParamListInfoData>); the executor + plancache thread it by this type"
)]
fn evaluate_params(
    pstate: &mut ParseState,
    stmt_name: &str,
    param_types: &[Oid],
    params: &[Node],
) -> ParamListInfo {
    use crate::backend::executor::execExpr::exec_init_expr;
    use crate::backend::executor::execExprInterp::exec_interp_expr;
    use crate::backend::nodes::nodeFuncs::exprType;
    use crate::backend::parser::parse_coerce::coerce_to_target_type;
    use crate::backend::parser::parse_collate::assign_expr_collations;
    use crate::backend::parser::parse_expr::transformExpr;
    use crate::nodes::primnodes::{CoercionContext, CoercionForm};
    use crate::parser::parse_node::ParseExprKind;

    let num_params = param_types.len();
    if params.len() != num_params {
        let nparams = params.len();
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "wrong number of parameters for prepared statement \"{stmt_name}\""
                ))
                .errdetail(format!("Expected {num_params} parameters but got {nparams}."));
        });
    }

    let mut param_li = makeParamList(i32::try_from(num_params).unwrap_or(0));

    // Build a throwaway ExprContext to evaluate the (constant) param expressions.
    let mut econtext = crate::nodes::execnodes::ExprContext::default();

    for (i, raw) in params.iter().enumerate() {
        let expected = param_types[i];
        let expr = transformExpr(pstate, Some(raw.clone()), ParseExprKind::ExecuteParameter);
        let given = expr.as_ref().map_or(crate::postgres_ext::InvalidOid, exprType);
        let coerced = coerce_to_target_type(
            pstate,
            expr,
            given,
            expected,
            -1,
            CoercionContext::ASSIGNMENT,
            CoercionForm::IMPLICIT_CAST,
            -1,
        );
        let Some(mut coerced) = coerced else {
            param_cannot_be_coerced(i + 1);
        };
        assign_expr_collations(pstate, &mut coerced);

        // Prepare + evaluate the expression to a constant Datum (Expr == Node here).
        let mut state = exec_init_expr(Some(&coerced), None)
            .unwrap_or_else(|| unreachable!("EXECUTE param expr has an ExprState"));
        let mut isnull = true;
        let value = exec_interp_expr(&mut state, &mut econtext, &mut isnull);

        let prm = &mut param_li.params[i];
        prm.ptype = expected;
        prm.pflags = ParamFlags::CONST;
        prm.value = value;
        prm.isnull = isnull;
    }

    param_li
}

// ---------------------------------------------------------------------------
// DEALLOCATE + the prepared-statement table operations
// ---------------------------------------------------------------------------

/// PG `DeallocateQuery`: drop the named prepared statement, or all of them.
pub fn deallocate_query(stmt: &DeallocateStmt) {
    match stmt.name.as_deref() {
        Some(name) => drop_prepared_statement(name, true),
        None => drop_all_prepared_statements(),
    }
}

/// PG `StorePreparedStatement`: insert a prepared statement into the per-backend
/// table; a duplicate name is an error.
pub fn store_prepared_statement(stmt_name: &str, plansource: Box<CachedPlanSource>, from_sql: bool) {
    let existed = with_table(|t| {
        if t.contains_key(stmt_name) {
            return true;
        }
        t.insert(
            stmt_name.to_string(),
            PreparedStatement {
                stmt_name: stmt_name.to_string(),
                plansource,
                from_sql,
                prepare_time: 0,
            },
        );
        false
    })
    .unwrap_or_else(|| unreachable!("store_prepared_statement outside a prepared-statement scope"));
    if existed {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_PSTATEMENT)
                .errmsg(format!("prepared statement \"{stmt_name}\" already exists"));
        });
    }
}

/// PG `FetchPreparedStatement`: does a prepared statement of this name exist?
/// (The owned entry stays in the table; callers operate under a borrow.)
#[must_use]
pub fn prepared_statement_exists(stmt_name: &str) -> bool {
    with_table(|t| t.contains_key(stmt_name)).unwrap_or(false)
}

/// Store (or replace) a prepared statement (the extended-protocol Parse path,
/// where re-Parsing a name replaces the prior unsaved source; an unnamed "" name
/// is always replaced). Unlike `store_prepared_statement` this does not error on a
/// duplicate -- Parse semantics.
pub fn store_or_replace_prepared_statement(
    stmt_name: &str,
    plansource: Box<CachedPlanSource>,
    from_sql: bool,
) {
    with_table(|t| {
        t.insert(
            stmt_name.to_string(),
            PreparedStatement {
                stmt_name: stmt_name.to_string(),
                plansource,
                from_sql,
                prepare_time: 0,
            },
        );
    });
}

/// Run `f` with a borrow of the named prepared statement's `CachedPlanSource`,
/// or raise "does not exist" if absent. Used by the extended-protocol Bind /
/// Describe arms (GetCachedPlan / result tupdesc) and by SPI.
pub fn with_plansource<R>(stmt_name: &str, f: impl FnOnce(&mut CachedPlanSource) -> R) -> R {
    let result = with_table(|t| t.get_mut(stmt_name).map(|e| f(&mut e.plansource)))
        .unwrap_or_else(|| unreachable!("with_plansource outside a prepared-statement scope"));
    result.unwrap_or_else(|| prepared_statement_does_not_exist(stmt_name))
}

/// The declared parameter type OIDs + the result `TupleDesc` for a named prepared
/// statement (the Describe Statement reply: ParameterDescription + RowDescription
/// or NoData). `None` tupdesc when the statement returns no rows.
#[must_use]
pub fn statement_describe(
    stmt_name: &str,
) -> (Vec<Oid>, Option<crate::access::tupdesc::TupleDesc>) {
    with_plansource(stmt_name, |src| {
        let tupdesc = crate::backend::utils::cache::plancache::plan_cache_compute_result_desc(src);
        (src.param_types.clone(), tupdesc)
    })
}

/// PG `DropPreparedStatement`: remove a prepared statement. If `show_error` and
/// the statement does not exist, raise the standard error.
pub fn drop_prepared_statement(stmt_name: &str, show_error: bool) {
    let removed = with_table(|t| t.remove(stmt_name)).flatten();
    if removed.is_none() && show_error {
        prepared_statement_does_not_exist(stmt_name);
    }
    // DropCachedPlan is the Box drop of the removed entry's plansource.
}

/// PG `DropAllPreparedStatements`.
pub fn drop_all_prepared_statements() {
    with_table(HashMap::clear);
}

#[cold]
fn prepared_statement_does_not_exist(stmt_name: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_PSTATEMENT)
            .errmsg(format!("prepared statement \"{stmt_name}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cold]
fn param_cannot_be_coerced(paramno: usize) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("parameter ${paramno} cannot be coerced to the expected type"));
    });
    unreachable!("ereport(ERROR) diverges");
}
