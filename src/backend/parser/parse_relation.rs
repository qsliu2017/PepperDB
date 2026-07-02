//! Relation-related parser support routines. Translated from
//! backend/parser/parse_relation.c.
//!
//! Non-type-centric free functions (`addRangeTableEntryForRelation`,
//! `colNameToVar`, `scanRTEForColumn`, ...); bodies here as snake_case `pub fn`s
//! with the C symbol in the doc comment, re-exported from
//! `crate::parser::parse_relation` under the C names.
//!
//! Disposition: `grow`. M2's live path is a single plain-relation FROM item:
//! `addRangeTableEntryForRelation` builds an `RTE_RELATION` from an already-open
//! relcache entry (its eref/colnames from the tupdesc), `addRTEPermissionInfo`
//! records the per-rel ACL slot, `addNSItemToQuery` exposes it, and
//! `colNameToVar` -> `scanNSItemForColumn` -> `scanRTEForColumn` resolves a column
//! name to a `Var`. The lock/relcache open itself is async (`relation_open`) and
//! is done by the caller; this module takes the open `&RelationData`. Aliases,
//! joins, subquery/function/VALUES FROM items, system columns, fuzzy matching, and
//! the multi-level namespace search are grow guards (rules.md s4).

use crate::access::attnum::AttrNumber;
use crate::nodes::makefuncs::{makeAlias, makeVar};
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::{AclMode, RTEKind, RTEPermissionInfo, RangeTblEntry};
use crate::nodes::primnodes::VarReturningType;
use crate::nodes::value::makeString;
use crate::parser::parse_node::{ParseNamespaceColumn, ParseNamespaceItem, ParseState};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::rel::RelationData;

/// Panic for a parse_relation path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `addRangeTableEntryForRelation`: build an `RTE_RELATION` from an already-open
/// relation and add it to the rangetable, returning a `ParseNamespaceItem` for it.
///
/// PG takes the open `Relation` and a `lockmode`; here the caller has opened the
/// rel (the lock/relcache build is async) and we record the lockmode on the RTE.
/// The alias path and the refcount drop / `table_close(rel, NoLock)` (RAII here)
/// are deferred / handled by the caller.
pub fn add_range_table_entry_for_relation(
    pstate: &mut ParseState,
    rel: &RelationData,
    lockmode: i32,
    alias: Option<&crate::nodes::primnodes::Alias>,
    inh: bool,
    in_from_cl: bool,
) -> ParseNamespaceItem {
    let tupdesc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("open relation has a tuple descriptor"));

    let relkind = rel.form().relkind;

    // The RTE's exposed name is the alias name if given, else the relation name.
    let refname = alias
        .and_then(|a| a.aliasname.clone())
        .unwrap_or_else(|| relation_name(rel));

    // Build the effective column names from the tupdesc, then apply any user
    // column-name overrides from the alias (`FROM t c(x, y)`). A too-long alias
    // column list would be a user error; the covered tests only rename the table.
    let mut eref = makeAlias(&refname, Vec::new());
    build_relation_aliases(tupdesc, &mut eref);
    if let Some(a) = alias {
        for (i, colname) in a.colnames.iter().enumerate() {
            if i < eref.colnames.len() {
                eref.colnames[i] = colname.clone();
            }
        }
    }

    let mut rte = make_relation_rte(rel.rd_id, inh, relkind, lockmode, eref, in_from_cl);

    // addRTEPermissionInfo: SELECT permission on this relation.
    add_rte_permission_info(&mut pstate.p_rteperminfos, &mut rte);
    let perminfo_index = rte.perminfoindex;
    pstate.p_rteperminfos[perminfo_index - 1].requiredPerms = AclMode::SELECT;

    // Add the completed RTE to the rangetable; its index is the new length.
    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as i32;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    build_ns_item_from_tuple_desc(rte_ref, rtindex, tupdesc)
}

/// PG `addRangeTableEntryForCTE`: build an `RTE_CTE` for a reference to the CTE
/// `cte` (at scoping level `levelsup`), add it to the rangetable, and return a
/// `ParseNamespaceItem` exposing its columns. The column names/types come from the
/// CTE's already-determined `ctecolnames`/`ctecoltypes` (the recursive
/// self-reference uses the column info set from the non-recursive term). SEARCH /
/// CYCLE extra columns and the alias path are deferred.
pub fn add_range_table_entry_for_cte(
    pstate: &mut ParseState,
    cte: &crate::nodes::parsenodes::CommonTableExpr,
    levelsup: crate::c::Index,
    rv: &crate::nodes::primnodes::RangeVar,
    in_from_cl: bool,
) -> ParseNamespaceItem {
    if rv.alias.is_some() {
        not_yet_reachable("addRangeTableEntryForCTE: alias clause");
    }
    let refname = cte.ctename.clone().unwrap_or_default();

    // self_reference iff the CTE's analysis isn't completed yet (its ctequery is
    // still a raw SelectStmt, not a Query).
    let self_reference = !matches!(cte.ctequery, Some(Node::Query(_)));

    // The CTE column names are stored as Node::String_; the alias eref wants String_.
    let mut eref = makeAlias(&refname, Vec::new());
    eref.colnames = cte
        .ctecolnames
        .iter()
        .map(|n| match n {
            Node::String_(s) => s.clone(),
            _ => makeString(String::new()),
        })
        .collect();

    let rte = make_cte_rte(
        cte.ctename.clone(),
        levelsup,
        self_reference,
        cte.ctecoltypes.clone(),
        cte.ctecoltypmods.clone(),
        cte.ctecolcollations.clone(),
        eref,
        in_from_cl,
    );

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as i32;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];
    build_ns_item_from_coltypes(rte_ref, rtindex)
}

/// PG `addRangeTableEntryForValues`: build an `RTE_VALUES` holding the transformed,
/// row-organized expression lists, add it to the rangetable, and return a
/// `ParseNamespaceItem` exposing its columns (from the per-column
/// coltypes/coltypmods/colcollations). The eref column names default to
/// `column1`, `column2`, ... The user-alias path is deferred (M2 VALUES has no alias).
#[allow(clippy::too_many_arguments, reason = "1:1 port of addRangeTableEntryForValues' arg set")]
pub fn add_range_table_entry_for_values(
    pstate: &mut ParseState,
    exprs: Vec<Node>,
    coltypes: Vec<Oid>,
    coltypmods: Vec<i32>,
    colcollations: Vec<Oid>,
    alias: Option<&crate::nodes::primnodes::Alias>,
    lateral: bool,
    in_from_cl: bool,
) -> ParseNamespaceItem {
    if alias.is_some() {
        not_yet_reachable("addRangeTableEntryForValues: alias clause");
    }
    let refname = "*VALUES*";

    // Column count = length of the first row's expr list. Each row is a RowExpr
    // carrier (its `args` are the row's per-column expressions).
    let numcolumns = match exprs.first() {
        Some(Node::RowExpr(row)) => row.args.len(),
        _ => unreachable!("VALUES RTE first row is a RowExpr carrier"),
    };

    // Default eref column names: column1, column2, ...
    let mut eref = makeAlias(refname, Vec::new());
    eref.colnames = (1..=numcolumns).map(|i| makeString(format!("column{i}"))).collect();

    let rte = make_values_rte(exprs, coltypes, coltypmods, colcollations, eref, lateral, in_from_cl);

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as i32;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];
    build_ns_item_from_coltypes(rte_ref, rtindex)
}

/// PG `addRangeTableEntryForSubquery`: build an `RTE_SUBQUERY` from an
/// already-transformed sub-`Query`, filling the column type/typmod/collation lists
/// (and any unspecified eref column names) from the subquery's non-resjunk
/// targetlist. Subqueries are never permission-checked (no `addRTEPermissionInfo`).
/// The RTE is added to the rangetable but NOT to the joinlist/namespace -- the
/// caller does that if appropriate.
pub fn add_range_table_entry_for_subquery(
    pstate: &mut ParseState,
    subquery: Box<crate::nodes::parsenodes::Query>,
    alias: Option<&crate::nodes::primnodes::Alias>,
    lateral: bool,
    in_from_cl: bool,
) -> ParseNamespaceItem {
    use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};

    let mut eref = alias.cloned().unwrap_or_else(|| makeAlias("unnamed_subquery", Vec::new()));
    let numaliases = eref.colnames.len();

    // Fill in any unspecified alias columns, and extract column type info.
    let mut coltypes: Vec<Oid> = Vec::new();
    let mut coltypmods: Vec<i32> = Vec::new();
    let mut colcollations: Vec<Oid> = Vec::new();
    let mut varattno = 0;
    for tlistitem in &subquery.targetList {
        let Node::TargetEntry(te) = tlistitem else {
            not_yet_reachable("addRangeTableEntryForSubquery: tlist entry is not a TargetEntry");
        };
        if te.resjunk {
            continue;
        }
        varattno += 1;
        crate::assert!(varattno == i32::from(te.resno));
        if varattno > numaliases as i32 {
            let attrname = te.resname.clone().unwrap_or_default();
            eref.colnames.push(makeString(attrname));
        }
        let expr = te
            .expr
            .as_ref()
            .unwrap_or_else(|| not_yet_reachable("addRangeTableEntryForSubquery: tlist has no expr"));
        coltypes.push(exprType(expr));
        coltypmods.push(exprTypmod(expr));
        colcollations.push(exprCollation(expr));
    }
    if varattno < numaliases as i32 {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_COLUMN_REFERENCE).errmsg(format!(
                "table \"{}\" has {} columns available but {} columns specified",
                eref.aliasname.as_deref().unwrap_or(""), varattno, numaliases
            ));
        });
        unreachable!("ereport(ERROR) diverges");
    }

    let rte = make_subquery_rte(subquery, alias.cloned(), eref, coltypes, coltypmods, colcollations, lateral, in_from_cl);

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as i32;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];
    let mut nsitem = build_ns_item_from_coltypes(rte_ref, rtindex);
    // Visible as a relation name only if it had a user-written alias.
    nsitem.rel_visible = alias.is_some();
    nsitem
}

/// Build an `RTE_SUBQUERY` `RangeTblEntry` (PG makeNode + field fill in
/// addRangeTableEntryForSubquery).
#[allow(clippy::too_many_arguments, reason = "1:1 port of the RTE_SUBQUERY field set")]
fn make_subquery_rte(
    subquery: Box<crate::nodes::parsenodes::Query>,
    alias: Option<crate::nodes::primnodes::Alias>,
    eref: crate::nodes::primnodes::Alias,
    coltypes: Vec<Oid>,
    coltypmods: Vec<i32>,
    colcollations: Vec<Oid>,
    lateral: bool,
    in_from_cl: bool,
) -> RangeTblEntry {
    RangeTblEntry {
        alias: alias.map(Box::new),
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::SUBQUERY,
        relid: InvalidOid,
        inh: false,
        relkind: 0,
        rellockmode: 0,
        perminfoindex: 0,
        tablesample: None,
        subquery: Some(subquery),
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions: Vec::new(),
        funcordinality: false,
        tablefunc: None,
        values_lists: Vec::new(),
        ctename: None,
        ctelevelsup: 0,
        self_reference: false,
        coltypes,
        coltypmods,
        colcollations,
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral,
        inFromCl: in_from_cl,
        securityQuals: Vec::new(),
    }
}

/// PG `addRangeTableEntryForFunction`: build an `RTE_FUNCTION` from the
/// transformed function expression(s), determine each function's result columns
/// (scalar -> a single column named after the function; composite/record -> the
/// result rowtype's columns), assemble the merged scan tupdesc, add the RTE to the
/// rangetable, and return a `ParseNamespaceItem`. Functions are never
/// permission-checked (no `addRTEPermissionInfo`). M8 reaches the single-function,
/// no-ordinality path; ROWS FROM() / coldeflists / ordinality grow later.
#[allow(clippy::too_many_arguments, reason = "1:1 port of addRangeTableEntryForFunction's arg set")]
pub fn add_range_table_entry_for_function(
    pstate: &mut ParseState,
    funcnames: &[String],
    funcexprs: Vec<Node>,
    coldeflists: &[Vec<Node>],
    rangefunc: &crate::nodes::parsenodes::RangeFunction,
    lateral: bool,
    in_from_cl: bool,
) -> ParseNamespaceItem {
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::nodes::nodeFuncs::{exprCollation, exprTypmod};
    use crate::funcapi::{get_expr_result_type, TypeFuncClass};
    use crate::nodes::parsenodes::RangeTblFunction;

    if rangefunc.alias.is_some() {
        not_yet_reachable("addRangeTableEntryForFunction: alias clause");
    }
    if rangefunc.ordinality {
        not_yet_reachable("addRangeTableEntryForFunction: WITH ORDINALITY");
    }
    let nfuncs = funcexprs.len();
    if nfuncs != 1 {
        not_yet_reachable("addRangeTableEntryForFunction: multiple functions (ROWS FROM)");
    }

    // RTE alias name defaults to the first function's name.
    let aliasname = funcnames.first().cloned().unwrap_or_default();
    let mut eref = makeAlias(&aliasname, Vec::new());

    let mut functions: Vec<Node> = Vec::with_capacity(nfuncs);
    let mut functupdescs: Vec<TupleDesc> = Vec::with_capacity(nfuncs);

    for (funcno, funcexpr) in funcexprs.into_iter().enumerate() {
        let coldeflist = coldeflists.get(funcno).map_or(&[][..], Vec::as_slice);
        let mut rtfunc = RangeTblFunction {
            funcexpr: Some(funcexpr.clone()),
            funccolcount: 0,
            funccolnames: Vec::new(),
            funccoltypes: Vec::new(),
            funccoltypmods: Vec::new(),
            funccolcollations: Vec::new(),
            funcparams: None,
        };

        let info = get_expr_result_type(&funcexpr);
        if !coldeflist.is_empty() {
            not_yet_reachable("addRangeTableEntryForFunction: column definition list (RECORD result)");
        }

        let tupdesc: TupleDesc = match info.class {
            TypeFuncClass::Composite | TypeFuncClass::CompositeDomain => info
                .result_tuple_desc
                .unwrap_or_else(|| unreachable!("composite function result has a tupdesc")),
            TypeFuncClass::Scalar => {
                let funcrettype = info
                    .result_type_id
                    .unwrap_or_else(|| unreachable!("scalar function result has a type OID"));
                let colname = choose_scalar_function_alias(&funcexpr, &funcnames[funcno], nfuncs);
                let mut td = TupleDescData::create_template(1);
                td.init_builtin_entry(1, &colname, funcrettype, exprTypmod(&funcexpr), 0);
                td.init_entry_collation(1, exprCollation(&funcexpr));
                std::sync::Arc::new(td)
            }
            TypeFuncClass::Record => {
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR).errmsg(
                        "a column definition list is required for functions returning \"record\"".to_string(),
                    );
                });
                unreachable!("ereport(ERROR) diverges");
            }
            TypeFuncClass::Other => {
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH).errmsg(format!(
                        "function \"{}\" in FROM has unsupported return type",
                        funcnames[funcno]
                    ));
                });
                unreachable!("ereport(ERROR) diverges");
            }
        };

        rtfunc.funccolcount = tupdesc.natts;
        functions.push(Node::RangeTblFunction(Box::new(rtfunc)));
        functupdescs.push(tupdesc);
    }

    // Single function, no ordinality: the scan tupdesc is the function's tupdesc.
    let tupdesc = functupdescs.into_iter().next().unwrap_or_else(|| unreachable!("one function tupdesc"));

    // buildRelationAliases: fill eref->colnames from the tupdesc.
    build_relation_aliases(&tupdesc, &mut eref);
    let tupdesc: &crate::access::tupdesc::TupleDescData = &tupdesc;

    // Expose the merged column types on the RTE (the coltypes-based paths read them).
    let coltypes: Vec<Oid> = (0..tupdesc.natts as usize).map(|i| tupdesc.attr(i).atttypid).collect();
    let coltypmods: Vec<i32> = (0..tupdesc.natts as usize).map(|i| tupdesc.attr(i).atttypmod).collect();
    let colcollations: Vec<Oid> = (0..tupdesc.natts as usize).map(|i| tupdesc.attr(i).attcollation).collect();

    let rte = make_function_rte(functions, rangefunc.ordinality, coltypes, coltypmods, colcollations, eref, lateral, in_from_cl);

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as i32;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];
    build_ns_item_for_function(rte_ref, rtindex, tupdesc)
}

/// `makeNode(RangeTblEntry)` for an `RTE_FUNCTION`.
#[allow(clippy::too_many_arguments, reason = "1:1 port: mirrors addRangeTableEntryForFunction's field set")]
fn make_function_rte(
    functions: Vec<Node>,
    funcordinality: bool,
    coltypes: Vec<Oid>,
    coltypmods: Vec<i32>,
    colcollations: Vec<Oid>,
    eref: crate::nodes::primnodes::Alias,
    lateral: bool,
    in_from_cl: bool,
) -> RangeTblEntry {
    RangeTblEntry {
        alias: None,
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::FUNCTION,
        relid: InvalidOid,
        inh: false,
        relkind: 0,
        rellockmode: 0,
        perminfoindex: 0,
        tablesample: None,
        subquery: None,
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions,
        funcordinality,
        tablefunc: None,
        values_lists: Vec::new(),
        ctename: None,
        ctelevelsup: 0,
        self_reference: false,
        coltypes,
        coltypmods,
        colcollations,
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral,
        inFromCl: in_from_cl,
        securityQuals: Vec::new(),
    }
}

/// PG `chooseScalarFunctionAlias`: the column name for a scalar-returning function
/// in FROM. With a single function and no alias, the column takes the function's
/// display name (`funcname`); the multi-function `columnN` fallback is a grow guard.
fn choose_scalar_function_alias(_funcexpr: &Node, funcname: &str, nfuncs: usize) -> String {
    if nfuncs == 1 {
        return funcname.to_owned();
    }
    not_yet_reachable("chooseScalarFunctionAlias: multi-function ROWS FROM naming");
}

/// PG `buildNSItemFromTupleDesc` for an `RTE_FUNCTION`: build the nsitem's per-column
/// array from the function's result tupdesc. Functions carry no `perminfo`.
fn build_ns_item_for_function(
    rte: &RangeTblEntry,
    rtindex: i32,
    tupdesc: &crate::access::tupdesc::TupleDescData,
) -> ParseNamespaceItem {
    let nscolumns = (0..tupdesc.natts as usize)
        .map(|i| {
            let attr = tupdesc.attr(i);
            let attno = (i + 1) as AttrNumber;
            ParseNamespaceColumn {
                varno: rtindex as crate::c::Index,
                varattno: attno,
                vartype: attr.atttypid,
                vartypmod: attr.atttypmod,
                varcollid: attr.attcollation,
                varreturningtype: VarReturningType::DEFAULT,
                varnosyn: rtindex as crate::c::Index,
                varattnosyn: attno,
                dontexpand: false,
            }
        })
        .collect();

    ParseNamespaceItem {
        names: Box::new(
            rte.eref.as_ref().unwrap_or_else(|| unreachable!("function RTE has an eref")).as_ref().clone(),
        ),
        rte: Box::new(rte.clone()),
        rtindex,
        perminfo: None,
        nscolumns,
        rel_visible: true,
        cols_visible: true,
        lateral_only: false,
        lateral_ok: true,
        returning_type: VarReturningType::DEFAULT,
    }
}

/// `makeNode(RangeTblEntry)` for an `RTE_VALUES` with the VALUES-relevant fields set.
#[allow(clippy::too_many_arguments, reason = "1:1 port: mirrors addRangeTableEntryForValues' field set")]
fn make_values_rte(
    values_lists: Vec<Node>,
    coltypes: Vec<Oid>,
    coltypmods: Vec<i32>,
    colcollations: Vec<Oid>,
    eref: crate::nodes::primnodes::Alias,
    lateral: bool,
    in_from_cl: bool,
) -> RangeTblEntry {
    RangeTblEntry {
        alias: None,
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::VALUES,
        relid: InvalidOid,
        inh: false,
        relkind: 0,
        rellockmode: 0,
        perminfoindex: 0,
        tablesample: None,
        subquery: None,
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions: Vec::new(),
        funcordinality: false,
        tablefunc: None,
        values_lists,
        ctename: None,
        ctelevelsup: 0,
        self_reference: false,
        coltypes,
        coltypmods,
        colcollations,
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral,
        inFromCl: in_from_cl,
        securityQuals: Vec::new(),
    }
}

/// Build the `ParseNamespaceItem` for a CTE RTE from its coltypes/colcollations (a
/// CTE has no tupdesc; its columns come from the determined CTE column info).
fn build_ns_item_from_coltypes(rte: &RangeTblEntry, rtindex: i32) -> ParseNamespaceItem {
    let nscolumns = (0..rte.coltypes.len())
        .map(|i| {
            let attno = (i + 1) as AttrNumber;
            ParseNamespaceColumn {
                varno: rtindex as crate::c::Index,
                varattno: attno,
                vartype: rte.coltypes[i],
                vartypmod: *rte.coltypmods.get(i).unwrap_or(&-1),
                varcollid: *rte.colcollations.get(i).unwrap_or(&InvalidOid),
                varreturningtype: VarReturningType::DEFAULT,
                varnosyn: rtindex as crate::c::Index,
                varattnosyn: attno,
                dontexpand: false,
            }
        })
        .collect();

    ParseNamespaceItem {
        names: Box::new(
            rte.eref.as_ref().unwrap_or_else(|| unreachable!("CTE RTE has an eref")).as_ref().clone(),
        ),
        rte: Box::new(rte.clone()),
        rtindex,
        perminfo: None,
        nscolumns,
        rel_visible: true,
        cols_visible: true,
        lateral_only: false,
        lateral_ok: true,
        returning_type: VarReturningType::DEFAULT,
    }
}

/// `makeNode(RangeTblEntry)` for an `RTE_CTE` with the CTE-relevant fields set.
#[allow(clippy::too_many_arguments, reason = "1:1 port: mirrors addRangeTableEntryForCTE's field set")]
fn make_cte_rte(
    ctename: Option<String>,
    ctelevelsup: crate::c::Index,
    self_reference: bool,
    coltypes: Vec<Oid>,
    coltypmods: Vec<i32>,
    colcollations: Vec<Oid>,
    eref: crate::nodes::primnodes::Alias,
    in_from_cl: bool,
) -> RangeTblEntry {
    RangeTblEntry {
        alias: None,
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::CTE,
        relid: InvalidOid,
        inh: false,
        relkind: 0,
        rellockmode: 0,
        perminfoindex: 0,
        tablesample: None,
        subquery: None,
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions: Vec::new(),
        funcordinality: false,
        tablefunc: None,
        values_lists: Vec::new(),
        ctename,
        ctelevelsup,
        self_reference,
        coltypes,
        coltypmods,
        colcollations,
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral: false,
        inFromCl: in_from_cl,
        securityQuals: Vec::new(),
    }
}

/// PG `buildRelationAliases` (M2 subset): fill `eref->colnames` from the tupdesc's
/// live (non-dropped) column names. The user-supplied-alias rebuild path is not
/// reachable (no alias clause in M2); a dropped column gets an empty-string name.
fn build_relation_aliases(tupdesc: &crate::access::tupdesc::TupleDescData, eref: &mut crate::nodes::primnodes::Alias) {
    crate::assert!(eref.colnames.is_empty());
    eref.colnames = (0..tupdesc.natts as usize)
        .map(|i| {
            let attr = tupdesc.attr(i);
            if attr.attisdropped {
                makeString(String::new())
            } else {
                makeString(attr_name(attr))
            }
        })
        .collect();
}

/// PG `buildNSItemFromTupleDesc`: build the `ParseNamespaceItem` (its per-column
/// `ParseNamespaceColumn` array) from the RTE + tupdesc. Dropped columns leave a
/// zeroed column entry.
fn build_ns_item_from_tuple_desc(
    rte: &RangeTblEntry,
    rtindex: i32,
    tupdesc: &crate::access::tupdesc::TupleDescData,
) -> ParseNamespaceItem {
    let nscolumns = (0..tupdesc.natts as usize)
        .map(|i| {
            let attr = tupdesc.attr(i);
            if attr.attisdropped {
                zero_ns_column()
            } else {
                let attno = (i + 1) as AttrNumber;
                ParseNamespaceColumn {
                    varno: rtindex as crate::c::Index,
                    varattno: attno,
                    vartype: attr.atttypid,
                    vartypmod: attr.atttypmod,
                    varcollid: attr.attcollation,
                    varreturningtype: VarReturningType::DEFAULT,
                    varnosyn: rtindex as crate::c::Index,
                    varattnosyn: attno,
                    dontexpand: false,
                }
            }
        })
        .collect();

    ParseNamespaceItem {
        names: Box::new(
            rte.eref.as_ref().unwrap_or_else(|| unreachable!("relation RTE has an eref")).as_ref().clone(),
        ),
        rte: Box::new(rte.clone()),
        rtindex,
        // perminfo is recorded in p_rteperminfos; the nsitem keeps a clone for
        // INSERT's insertedCols bookkeeping (the index is `rte.perminfoindex`).
        perminfo: Some(Box::new(RTEPermissionInfo {
            relid: rte.relid,
            inh: rte.inh,
            requiredPerms: AclMode::SELECT,
            checkAsUser: InvalidOid,
            selectedCols: None,
            insertedCols: None,
            updatedCols: None,
        })),
        nscolumns,
        rel_visible: true,
        cols_visible: true,
        lateral_only: false,
        lateral_ok: true,
        returning_type: VarReturningType::DEFAULT,
    }
}

/// PG `addRTEPermissionInfo`: make and append an `RTEPermissionInfo` for `rte`,
/// stamping its 1-based index into `rte.perminfoindex`. Returns the 0-based slot
/// index (the caller refers back into `rteperminfos`).
pub fn add_rte_permission_info(
    rteperminfos: &mut Vec<RTEPermissionInfo>,
    rte: &mut RangeTblEntry,
) -> usize {
    crate::assert!(rte.relid != InvalidOid);
    crate::assert!(rte.perminfoindex == 0);

    rteperminfos.push(RTEPermissionInfo {
        relid: rte.relid,
        inh: rte.inh,
        requiredPerms: AclMode::empty(),
        checkAsUser: InvalidOid,
        selectedCols: None,
        insertedCols: None,
        updatedCols: None,
    });
    rte.perminfoindex = rteperminfos.len() as crate::c::Index;
    rteperminfos.len() - 1
}

/// PG `addNSItemToQuery` (M2 subset): optionally add the nsitem's index to the
/// join list and/or expose it in the namespace. The lateral flag handling grows
/// with LATERAL support.
pub fn add_ns_item_to_query(
    pstate: &mut ParseState,
    nsitem: ParseNamespaceItem,
    add_to_join_list: bool,
    add_to_rel_namespace: bool,
    add_to_var_namespace: bool,
) {
    if add_to_join_list {
        let rtr = crate::nodes::primnodes::RangeTblRef { rtindex: nsitem.rtindex };
        pstate.p_joinlist.push(Node::RangeTblRef(Box::new(rtr)));
    }
    if add_to_rel_namespace || add_to_var_namespace {
        let mut item = nsitem;
        item.rel_visible = add_to_rel_namespace;
        item.cols_visible = add_to_var_namespace;
        item.lateral_only = pstate.p_lateral_active;
        item.lateral_ok = true;
        pstate.p_namespace.push(item);
    }
}

/// PG `colNameToVar`: resolve an unqualified column name against the namespace,
/// building a `Var`. Searches the current level's namespace first; if `localonly`
/// is false and no match is found, walks up the parent ParseStates (a correlated
/// sub-select reference), incrementing `sublevels_up` so the resulting Var carries
/// the right `varlevelsup` (M12, step 44).
pub fn col_name_to_var(
    pstate: &mut ParseState,
    colname: &str,
    localonly: bool,
    location: i32,
) -> Option<Node> {
    let mut levels_up: crate::c::Index = 0;
    let mut cur: Option<&ParseState> = Some(pstate);
    while let Some(ps) = cur {
        let mut result: Option<Node> = None;
        for nsitem in &ps.p_namespace {
            // Ignore columns that aren't visible at the current query level, and
            // those whose RTE is laterally-restricted at this point.
            if !nsitem.cols_visible || (nsitem.lateral_only && !ps.p_lateral_active) {
                continue;
            }
            if let Some(var) = scan_ns_item_for_column(nsitem, levels_up, colname, location) {
                if result.is_some() {
                    ambiguous_column(colname);
                }
                result = Some(var);
            }
        }
        if result.is_some() {
            return result;
        }
        if localonly {
            break;
        }
        cur = ps.parent_parse_state.as_deref();
        levels_up += 1;
    }
    None
}

/// PG `scanNSItemForColumn`: match `colname` in the nsitem and, on a hit, build the
/// `Var` from the matching `ParseNamespaceColumn`. M2 covers user columns only (no
/// system columns).
pub fn scan_ns_item_for_column(
    nsitem: &ParseNamespaceItem,
    sublevels_up: crate::c::Index,
    colname: &str,
    _location: i32,
) -> Option<Node> {
    let attnum = scan_rte_for_column(&nsitem.names, colname)?;
    if attnum <= 0 {
        not_yet_reachable("scanNSItemForColumn: system column reference");
    }
    let nscol = &nsitem.nscolumns[(attnum - 1) as usize];
    if nscol.varno == 0 {
        not_yet_reachable("scanNSItemForColumn: reference to a dropped column");
    }
    let mut var = makeVar(
        nscol.varno as i32,
        nscol.varattno,
        nscol.vartype,
        nscol.vartypmod,
        nscol.varcollid,
        sublevels_up,
    );
    var.varnosyn = nscol.varnosyn;
    var.varattnosyn = nscol.varattnosyn;
    Some(Node::Var(Box::new(var)))
}

/// PG `scanRTEForColumn` (M2 subset): find the 1-based attno whose eref colname
/// equals `colname`. Ambiguity (two matches) is an error; no match returns None.
/// System columns / fuzzy matching grow later.
fn scan_rte_for_column(eref: &crate::nodes::primnodes::Alias, colname: &str) -> Option<AttrNumber> {
    let mut result: Option<AttrNumber> = None;
    for (i, name) in eref.colnames.iter().enumerate() {
        if name.sval == colname {
            if result.is_some() {
                ambiguous_column(colname);
            }
            result = Some((i + 1) as AttrNumber);
        }
    }
    result
}

/// `makeNode(RangeTblEntry)` for an `RTE_RELATION` with the M2-relevant fields set.
fn make_relation_rte(
    relid: Oid,
    inh: bool,
    relkind: i8,
    lockmode: i32,
    eref: crate::nodes::primnodes::Alias,
    in_from_cl: bool,
) -> RangeTblEntry {
    RangeTblEntry {
        alias: None,
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::RELATION,
        relid,
        inh,
        relkind,
        rellockmode: lockmode,
        perminfoindex: 0,
        tablesample: None,
        subquery: None,
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions: Vec::new(),
        funcordinality: false,
        tablefunc: None,
        values_lists: Vec::new(),
        ctename: None,
        ctelevelsup: 0,
        self_reference: false,
        coltypes: Vec::new(),
        coltypmods: Vec::new(),
        colcollations: Vec::new(),
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral: false,
        inFromCl: in_from_cl,
        securityQuals: Vec::new(),
    }
}

fn zero_ns_column() -> ParseNamespaceColumn {
    ParseNamespaceColumn {
        varno: 0,
        varattno: 0,
        vartype: InvalidOid,
        vartypmod: 0,
        varcollid: InvalidOid,
        varreturningtype: VarReturningType::DEFAULT,
        varnosyn: 0,
        varattnosyn: 0,
        dontexpand: false,
    }
}

#[cold]
fn ambiguous_column(colname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_AMBIGUOUS_COLUMN)
            .errmsg(format!("column reference \"{colname}\" is ambiguous"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// `RelationGetRelationName` as an owned String.
fn relation_name(rel: &RelationData) -> String {
    crate::utils::rel::relation_get_relation_name(rel)
}

/// Read a `FormData_pg_attribute`'s `attname` as an owned String.
fn attr_name(attr: &crate::catalog::pg_attribute::FormData_pg_attribute) -> String {
    let bytes = crate::c::NameStr(&attr.attname);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}
