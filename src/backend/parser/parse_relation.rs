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
    if alias.is_some() {
        not_yet_reachable("addRangeTableEntryForRelation: alias clause");
    }

    let refname = relation_name(rel);
    let tupdesc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("open relation has a tuple descriptor"));

    let relkind = rel.form().relkind;

    // Build the effective column names from the tupdesc (no user alias for M2).
    let mut eref = makeAlias(&refname, Vec::new());
    build_relation_aliases(tupdesc, &mut eref);

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
/// building a `Var`. M2 searches only the current level's namespace (no parent
/// levels; `localonly` is effectively always true here).
pub fn col_name_to_var(
    pstate: &mut ParseState,
    colname: &str,
    _localonly: bool,
    location: i32,
) -> Option<Node> {
    let mut result: Option<Node> = None;
    // Snapshot the visible nsitems (borrow ends before building the Var).
    for idx in 0..pstate.p_namespace.len() {
        if !pstate.p_namespace[idx].cols_visible {
            continue;
        }
        let found = scan_ns_item_for_column(&pstate.p_namespace[idx], 0, colname, location);
        if let Some(var) = found {
            if result.is_some() {
                ambiguous_column(colname);
            }
            result = Some(var);
        }
    }
    result
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
