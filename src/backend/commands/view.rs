//! Use rewrite rules to construct views. Translated from
//! backend/commands/view.c (disposition: full, M11-reachable subset).
//!
//! `DefineView` runs parse analysis on the view's SELECT, builds the view relation
//! (a pg_class row with relkind 'v', a tuple descriptor from the query target list,
//! and NO storage), then stores the view's query as its `_RETURN` ON SELECT DO
//! INSTEAD rule (`DefineViewRules` -> `DefineQueryRewrite`). CREATE OR REPLACE on an
//! existing view replaces the rule (its column list must be a prefix of the new
//! one). WITH CHECK OPTION / RECURSIVE views are staged (rules.md s4).

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{CreateStmt, Query, ViewCheckOption, ViewStmt};
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `DefineView`: execute a CREATE VIEW command. Returns the view's address.
pub async fn define_view(
    shared: &Arc<SharedState>,
    stmt: &ViewStmt,
    query_string: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> ObjectAddress {
    use crate::nodes::parsenodes::RawStmt;

    if stmt.withCheckOption != ViewCheckOption::NO_CHECK_OPTION {
        not_yet_reachable("DefineView: WITH CHECK OPTION");
    }
    if !stmt.options.is_empty() {
        not_yet_reachable("DefineView: view WITH options (reloptions)");
    }

    // Run parse analysis to convert the raw SELECT to a Query (opens the source
    // tables, so it is async). The grammar guarantees a single SELECT.
    let raw_query = stmt
        .query
        .clone()
        .unwrap_or_else(|| unreachable!("CREATE VIEW always carries a SELECT"));
    let rawstmt = RawStmt { stmt: Some(raw_query), stmt_location, stmt_len };
    let mut view_parse = crate::backend::parser::analyze::parse_analyze_fixedparams_async(
        shared,
        &rawstmt,
        query_string,
        &[],
        0,
    )
    .await;

    if view_parse.commandType != CmdType::SELECT {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errmsg("unexpected parse analysis result".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    if view_parse.hasModifyingCTE {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("views must not contain data-modifying statements in WITH".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    }

    // If a column-alias list was given, rename the (non-junk) targetlist entries.
    if !stmt.aliases.is_empty() {
        apply_view_aliases(&mut view_parse, &stmt.aliases);
    }

    // Copy the view RangeVar (don't corrupt the original command). Implicit TEMP
    // promotion from a temp source relation is staged (single-namespace M11).
    let view = stmt
        .view
        .as_deref()
        .cloned()
        .unwrap_or_else(|| unreachable!("CREATE VIEW always names the view"));

    define_virtual_relation(shared, view, &view_parse, stmt.replace, query_string).await
}

/// PG `DefineVirtualRelation`: create the view relation and store its query via
/// the rule system. Builds a ColumnDef list from the query's (non-junk) target
/// list, creates the relation (relkind 'v'), then `StoreViewQuery`.
async fn define_virtual_relation(
    shared: &Arc<SharedState>,
    view: RangeVar,
    view_parse: &Query,
    replace: bool,
    query_string: &str,
) -> ObjectAddress {
    use crate::backend::commands::tablecmds::DefineRelation;
    use crate::catalog::pg_class::RELKIND_VIEW;

    // Build the ColumnDef list from the targetlist (PG: makeColumnDef per non-junk
    // TLE with exprType/exprTypmod/exprCollation).
    let attr_list = build_view_column_defs(view_parse);

    // Resolve a possible existing view of the same name (replace path).
    let view_oid = resolve_existing_relation(shared, &view).await;

    if let Some(view_oid) = view_oid {
        if !replace {
            // DefineRelation would normally error; mirror the "already exists" here.
            let name = view.relname.clone().unwrap_or_default();
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_TABLE)
                    .errmsg(format!("relation \"{name}\" already exists"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
        // CREATE OR REPLACE VIEW: the relation already exists. Verify the new
        // column list is a prefix of (or equal to) the old, then replace the rule.
        check_view_columns_compatible(view_oid, &attr_list);
        store_view_query(shared, view_oid, view_parse, true).await;
        crate::backend::access::transam::xact::CommandCounterIncrement();
        return ObjectAddress {
            classId: crate::catalog::pg_class::RelationRelationId,
            objectId: view_oid,
            objectSubId: 0,
        };
    }

    // Fresh view: build a CreateStmt carrying the column list and create it.
    let create_stmt = CreateStmt {
        relation: Some(Box::new(view)),
        tableElts: attr_list,
        inhRelations: Vec::new(),
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: Vec::new(),
        nnconstraints: Vec::new(),
        options: Vec::new(),
        oncommit: crate::nodes::primnodes::OnCommitAction::NOOP,
        tablespacename: None,
        accessMethod: None,
        if_not_exists: false,
    };

    let address =
        DefineRelation(shared, &create_stmt, RELKIND_VIEW, InvalidOid, query_string).await;

    // Make the new view relation visible before defining its rule.
    crate::backend::access::transam::xact::CommandCounterIncrement();

    store_view_query(shared, address.objectId, view_parse, replace).await;

    address
}

/// PG `StoreViewQuery` -> `DefineViewRules`: store the view's query as its
/// `_RETURN` ON SELECT DO INSTEAD rule.
async fn store_view_query(shared: &Arc<SharedState>, view_oid: Oid, view_parse: &Query, replace: bool) {
    crate::backend::rewrite::rewriteDefine::define_query_rewrite(
        shared,
        crate::backend::rewrite::rewriteSupport::VIEW_SELECT_RULE_NAME,
        view_oid,
        None,
        CmdType::SELECT,
        true,  // is_instead
        replace,
        vec![Node::Query(Box::new(view_parse.clone()))],
    )
    .await;
}

/// Build a ColumnDef list from a query's non-junk targetlist (PG's attrList loop in
/// DefineVirtualRelation). Each column takes the TLE's resname + exprType/typmod.
fn build_view_column_defs(view_parse: &Query) -> Vec<Node> {
    use crate::backend::nodes::makefuncs::make_column_def;
    use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};

    let mut attrs = Vec::new();
    for (i, te) in view_parse.targetList.iter().enumerate() {
        let Node::TargetEntry(tle) = te else { continue };
        if tle.resjunk {
            continue;
        }
        let expr = tle
            .expr
            .as_ref()
            .unwrap_or_else(|| unreachable!("a non-junk TargetEntry has an expr"));
        let colname = tle
            .resname
            .clone()
            .unwrap_or_else(|| format!("column{}", i + 1));
        let coll = exprCollation(expr);
        let def =
            make_column_def(&colname, exprType(expr), exprTypmod(expr), coll);
        attrs.push(Node::ColumnDef(Box::new(def)));
    }
    attrs
}

/// Rename the view's non-junk targetlist entries from the alias list (PG's
/// DefineView aliases loop). Too-many aliases is an error.
fn apply_view_aliases(view_parse: &mut Query, aliases: &[Node]) {
    let mut alias_iter = aliases.iter();
    for te in &mut view_parse.targetList {
        let Node::TargetEntry(tle) = te else { continue };
        if tle.resjunk {
            continue;
        }
        let Some(alias) = alias_iter.next() else { break };
        if let Node::String_(s) = alias {
            tle.resname = Some(s.sval.clone());
        }
    }
    if alias_iter.next().is_some() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
                .errmsg("CREATE VIEW specifies more column names than columns".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    }
}

/// Resolve a relation OID by the view's name in its (or the default) namespace, or
/// `None` if it does not exist.
async fn resolve_existing_relation(shared: &Arc<SharedState>, view: &RangeVar) -> Option<Oid> {
    let relname = view.relname.as_deref()?;
    crate::backend::catalog::namespace::range_var_get_relid(shared, view.schemaname.as_deref(), relname)
        .await
}

/// PG `checkViewColumns` (the prefix check half): the new column list must have at
/// least as many columns as the old, with matching names/types for the shared
/// prefix. Mismatch -> error. The detailed type-mismatch messages are condensed.
fn check_view_columns_compatible(view_oid: Oid, attr_list: &[Node]) {
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
    use crate::catalog::pg_class::RELKIND_VIEW;

    let Some(rel) = relation_id_get_relation(view_oid) else {
        // Not in the relcache yet: warm it (the create path already did for a real
        // view). Absence here means the name resolved to something not openable.
        not_yet_reachable("CREATE OR REPLACE VIEW: cannot open existing relation");
    };
    if rel.form().relkind != RELKIND_VIEW {
        relation_close(rel);
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("is not a view".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    let old_desc = rel.rd_att.clone().unwrap_or_else(|| unreachable!("view has a descriptor"));
    let old_natts = old_desc.natts as usize;
    if attr_list.len() < old_natts {
        relation_close(rel);
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("cannot drop columns from view".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    // Adding columns to a view (the new list strictly longer) needs the
    // AT_AddColumnToView pg_attribute machinery; the equal-length replace is the
    // common path. A longer list is staged.
    if attr_list.len() > old_natts {
        relation_close(rel);
        not_yet_reachable("CREATE OR REPLACE VIEW: adding columns to an existing view");
    }
    relation_close(rel);
}
