/*-------------------------------------------------------------------------
 *
 * parse_relation.rs
 *    parser support routines dealing with relations
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/parser/parse_relation.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use core::mem::size_of;

use crate::{castNode, current_cell, foreach, lfirst_node, makeNode, strVal, IsA};

// ---------------------------------------------------------------------------
// Standard library imports
// ---------------------------------------------------------------------------
use crate::access::attnum::{AttrNumber, InvalidAttrNumber, MaxAttrNumber};
use crate::access::htup_details::{
    HeapTuple, HeapTupleIsValid, MaxHeapAttributeNumber, MaxTupleAttributeNumber, GETSTRUCT,
};
use crate::access::sysattr::{FirstLowInvalidHeapAttributeNumber, TableOidAttributeNumber};
use crate::access::common::tupdesc::{
    CreateTemplateTupleDesc, TupleDescAttr, TupleDescCopyEntry, TupleDescInitEntry,
    TupleDescInitEntryCollation, TupleDesc,
};

use crate::catalog::heap::{SystemAttributeByName, SystemAttributeDefinition};
use crate::catalog::namespace::LookupNamespaceNoError;
use crate::utils::cache::lsyscache::get_relname_relid;
use crate::catalog::pg_attribute::{Form_pg_attribute, FormData_pg_attribute};
use crate::catalog::pg_class::RELKIND_COMPOSITE_TYPE;

use crate::nodes::bitmapset::{bms_add_member, bms_union, Bitmapset};
use crate::nodes::makefuncs::{
    makeAlias, makeNullConst, makeTargetEntry, makeVar,
};
use crate::nodes::nodeFuncs::{
    exprCollation, exprLocation, exprType, exprTypmod,
    expression_tree_walker, query_tree_walker,
};
use crate::nodes::nodes::{nodeTag, CmdType, JoinType, Node, NodeTag};
use crate::nodes::parsenodes::{
    ColumnDef, CommonTableExpr, LockingClause, Query,
    RangeFunction, RangeTblEntry, RangeTblFunction,
    RTEKind, RTEPermissionInfo, RowMarkClause,
    RTEKind::*,
};
use crate::nodes::pg_list::{
    lappend, lappend_int, lappend_oid, list_concat, list_copy, list_copy_tail,
    list_head, list_length, list_nth, list_nth_cell, list_nth_oid, list_truncate,
    linitial, llast, lnext, NIL, List, ListCell,
};
/* Types from primnodes (some re-exported through parsenodes as private imports) */
use crate::nodes::primnodes::{
    Alias, Expr, JoinExpr, RangeTblRef, RangeVar, TargetEntry,
    TableFunc, Var, VarReturningType,
    VarReturningType::*,
};
use crate::nodes::value::{makeString, String as PgString};
// copyObjectImpl: copyfuncs module not yet enabled; stub below
#[allow(dead_code)]
unsafe fn copyObjectImpl(from: *const core::ffi::c_void) -> *mut core::ffi::c_void {
    unimplemented!("copyObjectImpl not yet translated")
}

use crate::parser::parse_enr::{get_visible_ENR, name_matches_visible_ENR};
use crate::utils::misc::queryenvironment::EphemeralNamedRelationMetadata;
use crate::utils::misc::queryenvironment::EphemeralNameRelationType::ENR_NAMED_TUPLESTORE;
use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition, setup_parser_errposition_callback,
    Index, ParseCallbackState, ParseExprKind, ParseExprKind::*, ParseNamespaceColumn,
    ParseNamespaceItem, ParseState, Relation,
};

use crate::storage::lockdefs::{AccessShareLock, NoLock, RowExclusiveLock, RowShareLock, LOCKMODE};

use crate::utils::cache::lsyscache::get_attname;
use crate::postgres::{ObjectIdGetDatum, Int16GetDatum};
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache2};
use crate::utils::rel::{
    RelationGetNumberOfAttributes, RelationGetRelationName, RelationGetRelid,
    RelationData,
};

use crate::access::table::table::{table_close, table_open, table_openrv_extended};
use crate::access::common::relation::relation_open;
// relation_close is also in access::common::relation
use crate::access::common::relation::relation_close;

// ---------------------------------------------------------------------------
// Stubs for unported siblings.
// ---------------------------------------------------------------------------

// TODO(pg-port): utils/adt/varlena.c - Levenshtein distance used for fuzzy col matching.
unsafe fn varstr_levenshtein_less_equal(
    source: *const c_char,
    slen: usize,
    target: *const c_char,
    tlen: usize,
    ins_c: c_int,
    del_c: c_int,
    sub_c: c_int,
    max_d: c_int,
    trusted: bool,
) -> c_int {
    let _ = (source, slen, target, tlen, ins_c, del_c, sub_c, max_d, trusted);
    max_d
}

// TODO(pg-port): storage/lmgr/lmgr.c
unsafe fn isLockedRefname_lmgr(_pstate: *mut ParseState, _refname: *const c_char) -> bool {
    false
}

// TODO(pg-port): storage/lmgr/lmgr.c - CheckRelationLockedByMe
unsafe fn CheckRelationLockedByMe(_rel: Relation, _lockmode: LOCKMODE, _orstronger: bool) -> bool {
    true
}

// TODO(pg-port): catalog/namespace.c - RangeVarGetRelid
unsafe fn RangeVarGetRelid(relation: *const RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> Oid {
    InvalidOid
}

// TODO(pg-port): funcapi.c - get_expr_result_type
pub type TypeFuncClass = c_int;
pub const TYPEFUNC_SCALAR: TypeFuncClass = 1;
pub const TYPEFUNC_COMPOSITE: TypeFuncClass = 2;
pub const TYPEFUNC_COMPOSITE_DOMAIN: TypeFuncClass = 3;
pub const TYPEFUNC_RECORD: TypeFuncClass = 4;

unsafe fn get_expr_result_type(
    expr: *mut Node,
    funcrettype: *mut Oid,
    tupdesc: *mut TupleDesc,
) -> TypeFuncClass {
    let _ = (expr, funcrettype, tupdesc);
    TYPEFUNC_SCALAR
}

unsafe fn get_expr_result_tupdesc(expr: *mut Node, noerror: bool) -> TupleDesc {
    let _ = (expr, noerror);
    core::ptr::null_mut()
}

// TODO(pg-port): utils/cache/lsyscache.c - get_func_result_name
unsafe fn get_func_result_name(_funcid: Oid) -> *mut c_char {
    core::ptr::null_mut()
}

// TODO(pg-port): catalog/heap.c - CheckAttributeNamesTypes / CHKATYPE_ANYRECORD
const CHKATYPE_ANYRECORD: c_int = 1;
unsafe fn CheckAttributeNamesTypes(_tupdesc: TupleDesc, _relkind: c_char, _flags: c_int) {}

// TODO(pg-port): parser/parse_type.c - typenameTypeIdAndMod
unsafe fn typenameTypeIdAndMod(
    pstate: *mut ParseState,
    typename: *mut c_void,
    typeId_p: *mut Oid,
    typmod_p: *mut int32,
) {
    let _ = (pstate, typename, typeId_p, typmod_p);
}

// TODO(pg-port): parser/parse_type.c - GetColumnDefCollation
unsafe fn GetColumnDefCollation(
    pstate: *mut ParseState,
    coldef: *mut ColumnDef,
    type_id: Oid,
) -> Oid {
    let _ = (pstate, coldef, type_id);
    InvalidOid
}

// TODO(pg-port): nodes/nodeFuncs.c - query_tree_walker flags
const QTW_IGNORE_JOINALIASES: c_int = 0x0001;

// TODO(pg-port): catalog/pg_class.h - RELPERSISTENCE_TEMP
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

// TODO(pg-port): catalog/pg_type_d.h
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const RECORDOID: Oid = 2249;
const RECORDARRAYOID: Oid = 2287;

// TODO(pg-port): catalog/namespace.c - LookupNamespaceNoError
// (real impl in crate::catalog::namespace if ported, else this local stub)

// TODO(pg-port): utils/syscache - ATTNUM cache id
const ATTNUM: c_int = 0; // TODO: catalog/syscache_ids.h

// TODO(pg-port): utils/acl.h - ACL_SELECT
type AclMode = uint64;
const ACL_SELECT: AclMode = 0x0002;

/* TFT_XMLTABLE / TFT_JSON_TABLE are real enum variants in primnodes */
use crate::nodes::primnodes::TableFuncType::{TFT_XMLTABLE, TFT_JSON_TABLE};

// TODO(pg-port): access/table/table.c ENR metadata helper
unsafe fn ENRMetadataGetTupDesc(enrmd: EphemeralNamedRelationMetadata) -> TupleDesc {
    let _ = enrmd;
    core::ptr::null_mut()
}

// SearchSysCacheExists2 - stub (catalog lookup)
unsafe fn SearchSysCacheExists2(cacheId: c_int, key1: Datum, key2: Datum) -> bool {
    !crate::utils::cache::syscache::SearchSysCache2(cacheId, key1, key2).is_null()
}

// copyObject - uses the real copyObjectImpl under the hood
unsafe fn copyObject<T>(node: *mut T) -> *mut T {
    copyObjectImpl(node as *const c_void) as *mut T
}

// rt_fetch - thin wrapper over list_nth
unsafe fn rt_fetch(rangetable_index: c_int, rangetable: *mut List) -> *mut RangeTblEntry {
    list_nth(rangetable, rangetable_index - 1) as *mut RangeTblEntry
}

// list_nth_node - typed list_nth
unsafe fn list_nth_node_joinexpr(list: *mut List, n: c_int) -> *mut JoinExpr {
    list_nth(list, n) as *mut JoinExpr
}
unsafe fn list_nth_node_rteperminfo(list: *mut List, n: c_int) -> *mut RTEPermissionInfo {
    list_nth(list, n) as *mut RTEPermissionInfo
}

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    fn OidIsValid(oid: Oid) -> bool;
}

// Re-exported from elsewhere in the crate for convenience.
use crate::utils::mmgr::mcxt::{palloc, palloc0, pstrdup};

/*
 * Support for fuzzily matching columns.
 *
 * This is for building diagnostic messages, where multiple or non-exact
 * matching attributes are of interest.
 *
 * "distance" is the current best fuzzy-match distance if rfirst isn't NULL,
 * otherwise it is the maximum acceptable distance plus 1.
 *
 * rfirst/first record the closest non-exact match so far, and distance
 * is its distance from the target name.  If we have found a second non-exact
 * match of exactly the same distance, rsecond/second record that.  (If
 * we find three of the same distance, we conclude that "distance" is not
 * a tight enough bound for a useful hint and clear rfirst/rsecond again.
 * Only if we later find something closer will we re-populate rfirst.)
 *
 * rexact1/exact1 record the location of the first exactly-matching column,
 * if any.  If we find multiple exact matches then rexact2/exact2 record
 * another one (we don't especially care which).  Currently, these get
 * populated independently of the fuzzy-match fields.
 */
#[repr(C)]
struct FuzzyAttrMatchState {
    distance: c_int,                  /* Current or limit distance */
    rfirst:   *mut RangeTblEntry,     /* RTE of closest non-exact match, or NULL */
    first:    AttrNumber,             /* Col index in rfirst */
    rsecond:  *mut RangeTblEntry,     /* RTE of another non-exact match w/same dist */
    second:   AttrNumber,             /* Col index in rsecond */
    rexact1:  *mut RangeTblEntry,     /* RTE of first exact match, or NULL */
    exact1:   AttrNumber,             /* Col index in rexact1 */
    rexact2:  *mut RangeTblEntry,     /* RTE of second exact match, or NULL */
    exact2:   AttrNumber,             /* Col index in rexact2 */
}

const MAX_FUZZY_DISTANCE: c_int = 3;

/*
 * refnameNamespaceItem
 *    Given a possibly-qualified refname, look to see if it matches any visible
 *    namespace item.  If so, return a pointer to the nsitem; else return NULL.
 *
 *    Optionally get nsitem's nesting depth (0 = current) into *sublevels_up.
 *    If sublevels_up is NULL, only consider items at the current nesting
 *    level.
 *
 * An unqualified refname (schemaname == NULL) can match any item with matching
 * alias, or matching unqualified relname in the case of alias-less relation
 * items.  It is possible that such a refname matches multiple items in the
 * nearest nesting level that has a match; if so, we report an error via
 * ereport().
 *
 * A qualified refname (schemaname != NULL) can only match a relation item
 * that (a) has no alias and (b) is for the same relation identified by
 * schemaname.refname.  In this case we convert schemaname.refname to a
 * relation OID and search by relid, rather than by alias name.  This is
 * peculiar, but it's what SQL says to do.  While processing a query's
 * RETURNING list, there may be additional namespace items for OLD and NEW,
 * with the same relation OID as the target namespace item.  These are
 * ignored in the search, since they don't match by schemaname.refname.
 */
pub unsafe fn refnameNamespaceItem(
    pstate: *mut ParseState,
    schemaname: *const c_char,
    refname: *const c_char,
    location: c_int,
    sublevels_up: *mut c_int,
) -> *mut ParseNamespaceItem {
    let mut relId: Oid = InvalidOid;
    let mut pstate = pstate;

    if !sublevels_up.is_null() {
        *sublevels_up = 0;
    }

    if !schemaname.is_null() {
        let namespaceId: Oid;

        /*
         * We can use LookupNamespaceNoError() here because we are only
         * interested in finding existing RTEs.  Checking USAGE permission on
         * the schema is unnecessary since it would have already been checked
         * when the RTE was made.  Furthermore, we want to report "RTE not
         * found", not "no permissions for schema", if the name happens to
         * match a schema name the user hasn't got access to.
         */
        namespaceId = LookupNamespaceNoError(schemaname);
        if !OidIsValid(namespaceId) {
            return core::ptr::null_mut();
        }
        relId = get_relname_relid(refname, namespaceId);
        if !OidIsValid(relId) {
            return core::ptr::null_mut();
        }
    }

    while !pstate.is_null() {
        let result: *mut ParseNamespaceItem;

        if OidIsValid(relId) {
            result = scanNameSpaceForRelid(pstate, relId, location);
        } else {
            result = scanNameSpaceForRefname(pstate, refname, location);
        }

        if !result.is_null() {
            return result;
        }

        if !sublevels_up.is_null() {
            *sublevels_up += 1;
        } else {
            break;
        }

        pstate = (*pstate).parentParseState;
    }
    core::ptr::null_mut()
}

/*
 * Search the query's table namespace for an item matching the
 * given unqualified refname.  Return the nsitem if a unique match, or NULL
 * if no match.  Raise error if multiple matches.
 *
 * Note: it might seem that we shouldn't have to worry about the possibility
 * of multiple matches; after all, the SQL standard disallows duplicate table
 * aliases within a given SELECT level.  Historically, however, Postgres has
 * been laxer than that.  For example, we allow
 *        SELECT ... FROM tab1 x CROSS JOIN (tab2 x CROSS JOIN tab3 y) z
 * on the grounds that the aliased join (z) hides the aliases within it,
 * therefore there is no conflict between the two RTEs named "x".  However,
 * if tab3 is a LATERAL subquery, then from within the subquery both "x"es
 * are visible.  Rather than rejecting queries that used to work, we allow
 * this situation, and complain only if there's actually an ambiguous
 * reference to "x".
 */
unsafe fn scanNameSpaceForRefname(
    pstate: *mut ParseState,
    refname: *const c_char,
    location: c_int,
) -> *mut ParseNamespaceItem {
    let mut result: *mut ParseNamespaceItem = core::ptr::null_mut();
    let mut l: *mut ListCell;

    foreach!(l, (*pstate).p_namespace, {
        let nsitem = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut ParseNamespaceItem;

        /* Ignore columns-only items */
        if !(*nsitem).p_rel_visible {
            continue;
        }
        /* If not inside LATERAL, ignore lateral-only items */
        if (*nsitem).p_lateral_only && !(*pstate).p_lateral_active {
            continue;
        }

        let aliasname = (*((*nsitem).p_names as *mut Alias)).aliasname;
        if strcmp(aliasname, refname) == 0 {
            if !result.is_null() {
                ereport!(ERROR, errmsg!("table reference \"{}\" is ambiguous", cstr_to_str(refname))) /* C also: errcode, parser_errposition */;
            }
            check_lateral_ref_ok(pstate, nsitem, location);
            result = nsitem;
        }
    });
    result
}

/*
 * Search the query's table namespace for a relation item matching the
 * given relation OID.  Return the nsitem if a unique match, or NULL
 * if no match.  Raise error if multiple matches.
 *
 * See the comments for refnameNamespaceItem to understand why this
 * acts the way it does.
 */
unsafe fn scanNameSpaceForRelid(
    pstate: *mut ParseState,
    relid: Oid,
    location: c_int,
) -> *mut ParseNamespaceItem {
    let mut result: *mut ParseNamespaceItem = core::ptr::null_mut();
    let mut l: *mut ListCell;

    foreach!(l, (*pstate).p_namespace, {
        let nsitem = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut ParseNamespaceItem;
        let rte = (*nsitem).p_rte as *mut RangeTblEntry;

        /* Ignore columns-only items */
        if !(*nsitem).p_rel_visible {
            continue;
        }
        /* If not inside LATERAL, ignore lateral-only items */
        if (*nsitem).p_lateral_only && !(*pstate).p_lateral_active {
            continue;
        }
        /* Ignore OLD/NEW namespace items that can appear in RETURNING */
        if (*nsitem).p_returning_type != VAR_RETURNING_DEFAULT as c_int {
            continue;
        }

        /* yes, the test for alias == NULL should be there... */
        if (*rte).rtekind == RTE_RELATION
            && (*rte).relid == relid
            && ((*rte).alias as *const c_void).is_null()
        {
            if !result.is_null() {
                ereport!(ERROR, errmsg!("table reference {} is ambiguous", relid)) /* C also: errcode, parser_errposition */;
            }
            check_lateral_ref_ok(pstate, nsitem, location);
            result = nsitem;
        }
    });
    result
}

/*
 * Search the query's CTE namespace for a CTE matching the given unqualified
 * refname.  Return the CTE (and its levelsup count) if a match, or NULL
 * if no match.  We need not worry about multiple matches, since parse_cte.c
 * rejects WITH lists containing duplicate CTE names.
 */
pub unsafe fn scanNameSpaceForCTE(
    pstate: *mut ParseState,
    refname: *const c_char,
    ctelevelsup: *mut Index,
) -> *mut CommonTableExpr {
    let mut levelsup: Index = 0;
    let mut pstate = pstate;

    while !pstate.is_null() {
        let mut lc: *mut ListCell;

        foreach!(lc, (*pstate).p_ctenamespace, {
            let cte = lfirst_node!(CommonTableExpr, T_CommonTableExpr, current_cell!(lc));

            if strcmp((*cte).ctename, refname) == 0 {
                *ctelevelsup = levelsup;
                return cte;
            }
        });

        pstate = (*pstate).parentParseState;
        levelsup += 1;
    }
    core::ptr::null_mut()
}

/*
 * Search for a possible "future CTE", that is one that is not yet in scope
 * according to the WITH scoping rules.  This has nothing to do with valid
 * SQL semantics, but it's important for error reporting purposes.
 */
unsafe fn isFutureCTE(pstate: *mut ParseState, refname: *const c_char) -> bool {
    let mut pstate = pstate;
    while !pstate.is_null() {
        let mut lc: *mut ListCell;

        foreach!(lc, (*pstate).p_future_ctes, {
            let cte = lfirst_node!(CommonTableExpr, T_CommonTableExpr, current_cell!(lc));

            if strcmp((*cte).ctename, refname) == 0 {
                return true;
            }
        });

        pstate = (*pstate).parentParseState;
    }
    false
}

/*
 * Search the query's ephemeral named relation namespace for a relation
 * matching the given unqualified refname.
 */
pub unsafe fn scanNameSpaceForENR(pstate: *mut ParseState, refname: *const c_char) -> bool {
    name_matches_visible_ENR(pstate, refname)
}

/*
 * searchRangeTableForRel
 *    See if any RangeTblEntry could possibly match the RangeVar.
 *    If so, return a pointer to the RangeTblEntry; else return NULL.
 *
 * This is different from refnameNamespaceItem in that it considers every
 * entry in the ParseState's rangetable(s), not only those that are currently
 * visible in the p_namespace list(s).  This behavior is invalid per the SQL
 * spec, and it may give ambiguous results (there might be multiple equally
 * valid matches, but only one will be returned).  This must be used ONLY
 * as a heuristic in giving suitable error messages.  See errorMissingRTE.
 *
 * Notice that we consider both matches on actual relation (or CTE) name
 * and matches on alias.
 */
unsafe fn searchRangeTableForRel(
    pstate: *mut ParseState,
    relation: *mut RangeVar,
) -> *mut RangeTblEntry {
    let refname = (*relation).relname;
    let mut relId: Oid = InvalidOid;
    let mut cte: *mut CommonTableExpr = core::ptr::null_mut();
    let mut isenr: bool = false;
    let mut ctelevelsup: Index = 0;
    let mut levelsup: Index = 0;
    let mut pstate = pstate;

    /*
     * If it's an unqualified name, check for possible CTE matches. A CTE
     * hides any real relation matches.  If no CTE, look for a matching
     * relation.
     *
     * NB: It's not critical that RangeVarGetRelid return the correct answer
     * here in the face of concurrent DDL.  If it doesn't, the worst case
     * scenario is a less-clear error message.  Also, the tables involved in
     * the query are already locked, which reduces the number of cases in
     * which surprising behavior can occur.  So we do the name lookup
     * unlocked.
     */
    if ((*relation).schemaname as *const c_void).is_null() {
        cte = scanNameSpaceForCTE(pstate, refname, &mut ctelevelsup);
        if cte.is_null() {
            isenr = scanNameSpaceForENR(pstate, refname);
        }
    }

    if cte.is_null() && !isenr {
        relId = RangeVarGetRelid(relation, NoLock, true);
    }

    /* Now look for RTEs matching either the relation/CTE/ENR or the alias */
    while !pstate.is_null() {
        let mut l: *mut ListCell;

        foreach!(l, (*pstate).p_rtable, {
            let rte = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));

            if (*rte).rtekind == RTE_RELATION
                && OidIsValid(relId)
                && (*rte).relid == relId
            {
                return rte;
            }
            if (*rte).rtekind == RTE_CTE
                && !cte.is_null()
                && (*rte).ctelevelsup + levelsup == ctelevelsup
                && strcmp((*rte).ctename, refname) == 0
            {
                return rte;
            }
            if (*rte).rtekind == RTE_NAMEDTUPLESTORE
                && isenr
                && strcmp((*rte).enrname, refname) == 0
            {
                return rte;
            }
            if strcmp((*(*rte).eref).aliasname, refname) == 0 {
                return rte;
            }
        });

        pstate = (*pstate).parentParseState;
        levelsup += 1;
    }
    core::ptr::null_mut()
}

/*
 * Check for relation-name conflicts between two namespace lists.
 * Raise an error if any is found.
 *
 * Note: we assume that each given argument does not contain conflicts
 * itself; we just want to know if the two can be merged together.
 *
 * Per SQL, two alias-less plain relation RTEs do not conflict even if
 * they have the same eref->aliasname (ie, same relation name), if they
 * are for different relation OIDs (implying they are in different schemas).
 *
 * We ignore the lateral-only flags in the namespace items: the lists must
 * not conflict, even when all items are considered visible.  However,
 * columns-only items should be ignored.
 */
pub unsafe fn checkNameSpaceConflicts(
    pstate: *mut ParseState,
    namespace1: *mut List,
    namespace2: *mut List,
) {
    let mut l1: *mut ListCell;

    foreach!(l1, namespace1, {
        let nsitem1 = crate::nodes::pg_list::lfirst(current_cell!(l1)) as *mut ParseNamespaceItem;
        let rte1 = (*nsitem1).p_rte as *mut RangeTblEntry;
        let aliasname1 = (*((*nsitem1).p_names as *mut Alias)).aliasname;
        let mut l2: *mut ListCell;

        if !(*nsitem1).p_rel_visible {
            continue;
        }

        foreach!(l2, namespace2, {
            let nsitem2 = crate::nodes::pg_list::lfirst(current_cell!(l2)) as *mut ParseNamespaceItem;
            let rte2 = (*nsitem2).p_rte as *mut RangeTblEntry;
            let aliasname2 = (*((*nsitem2).p_names as *mut Alias)).aliasname;

            if !(*nsitem2).p_rel_visible {
                continue;
            }
            if strcmp(aliasname2, aliasname1) != 0 {
                continue; /* definitely no conflict */
            }
            if (*rte1).rtekind == RTE_RELATION
                && ((*rte1).alias as *const c_void).is_null()
                && (*rte2).rtekind == RTE_RELATION
                && ((*rte2).alias as *const c_void).is_null()
                && (*rte1).relid != (*rte2).relid
            {
                continue; /* no conflict per SQL rule */
            }
            ereport!(ERROR, errmsg!(
                    "table name \"{}\" specified more than once",
                    cstr_to_str(aliasname1)
                )) /* C also: errcode */;
        });
    });
}

/*
 * Complain if a namespace item is currently disallowed as a LATERAL reference.
 * This enforces both SQL:2008's rather odd idea of what to do with a LATERAL
 * reference to the wrong side of an outer join, and our own prohibition on
 * referencing the target table of an UPDATE or DELETE as a lateral reference
 * in a FROM/USING clause.
 *
 * Note: the pstate should be the same query level the nsitem was found in.
 *
 * Convenience subroutine to avoid multiple copies of a rather ugly ereport.
 */
unsafe fn check_lateral_ref_ok(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    location: c_int,
) {
    if (*nsitem).p_lateral_only && !(*nsitem).p_lateral_ok {
        /* SQL:2008 demands this be an error, not an invisible item */
        let rte = (*nsitem).p_rte as *mut RangeTblEntry;
        let refname = (*((*nsitem).p_names as *mut Alias)).aliasname;

        ereport!(ERROR, errmsg!(
                "invalid reference to FROM-clause entry for table \"{}\"",
                cstr_to_str(refname)
            )) /* C also: errcode, if, parser_errposition */;
    }
}

/*
 * Given an RT index and nesting depth, find the corresponding
 * ParseNamespaceItem (there must be one).
 */
pub unsafe fn GetNSItemByRangeTablePosn(
    pstate: *mut ParseState,
    varno: c_int,
    sublevels_up: c_int,
) -> *mut ParseNamespaceItem {
    let mut pstate = pstate;
    let mut sublevels_up = sublevels_up;

    while sublevels_up > 0 {
        sublevels_up -= 1;
        pstate = (*pstate).parentParseState;
        Assert!(!pstate.is_null());
    }

    let mut lc: *mut ListCell;
    foreach!(lc, (*pstate).p_namespace, {
        let nsitem = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut ParseNamespaceItem;

        if (*nsitem).p_rtindex == varno {
            return nsitem;
        }
    });
    elog!(ERROR, "nsitem not found (internal error)");
    core::ptr::null_mut() /* keep compiler quiet */
}

/*
 * Given an RT index and nesting depth, find the corresponding RTE.
 * (Note that the RTE need not be in the query's namespace.)
 */
pub unsafe fn GetRTEByRangeTablePosn(
    pstate: *mut ParseState,
    varno: c_int,
    sublevels_up: c_int,
) -> *mut RangeTblEntry {
    let mut pstate = pstate;
    let mut sublevels_up = sublevels_up;

    while sublevels_up > 0 {
        sublevels_up -= 1;
        pstate = (*pstate).parentParseState;
        Assert!(!pstate.is_null());
    }
    Assert!(varno > 0 && varno <= list_length((*pstate).p_rtable));
    rt_fetch(varno, (*pstate).p_rtable)
}

/*
 * Fetch the CTE for a CTE-reference RTE.
 *
 * rtelevelsup is the number of query levels above the given pstate that the
 * RTE came from.
 */
pub unsafe fn GetCTEForRTE(
    pstate: *mut ParseState,
    rte: *mut RangeTblEntry,
    rtelevelsup: c_int,
) -> *mut CommonTableExpr {
    let mut levelsup: Index;
    let mut lc: *mut ListCell;
    let mut pstate = pstate;

    Assert!((*rte).rtekind == RTE_CTE);
    levelsup = (*rte).ctelevelsup + rtelevelsup as Index;
    while levelsup > 0 {
        levelsup -= 1;
        pstate = (*pstate).parentParseState;
        if pstate.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "bad levelsup for CTE \"{}\"", cstr_to_str((*rte).ctename));
        }
    }
    foreach!(lc, (*pstate).p_ctenamespace, {
        let cte = lfirst_node!(CommonTableExpr, T_CommonTableExpr, current_cell!(lc));

        if strcmp((*cte).ctename, (*rte).ctename) == 0 {
            return cte;
        }
    });
    /* shouldn't happen */
    elog!(ERROR, "could not find CTE \"{}\"", cstr_to_str((*rte).ctename));
    core::ptr::null_mut() /* keep compiler quiet */
}

// helper: C string -> &str for formatting (best-effort; only in error paths)
unsafe fn cstr_to_str(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "<null>";
    }
    core::ffi::CStr::from_ptr(s)
        .to_str()
        .unwrap_or("<invalid utf8>")
}

/*
 * updateFuzzyAttrMatchState
 *    Using Levenshtein distance, consider if column is best fuzzy match.
 */
unsafe fn updateFuzzyAttrMatchState(
    fuzzy_rte_penalty: c_int,
    fuzzystate: *mut FuzzyAttrMatchState,
    rte: *mut RangeTblEntry,
    actual: *const c_char,
    match_: *const c_char,
    attnum: c_int,
) {
    let columndistance: c_int;
    let matchlen: usize;

    /* Bail before computing the Levenshtein distance if there's no hope. */
    if fuzzy_rte_penalty > (*fuzzystate).distance {
        return;
    }

    /*
     * Outright reject dropped columns, which can appear here with apparent
     * empty actual names, per remarks within scanRTEForColumn().
     */
    if *actual == b'\0' as c_char {
        return;
    }

    /* Use Levenshtein to compute match distance. */
    matchlen = strlen(match_);
    let raw_dist = varstr_levenshtein_less_equal(
        actual,
        strlen(actual),
        match_,
        matchlen,
        1,
        1,
        1,
        (*fuzzystate).distance + 1 - fuzzy_rte_penalty,
        true,
    );

    /*
     * If more than half the characters are different, don't treat it as a
     * match, to avoid making ridiculous suggestions.
     */
    if raw_dist > matchlen as c_int / 2 {
        return;
    }

    /*
     * From this point on, we can ignore the distinction between the RTE-name
     * distance and the column-name distance.
     */
    let columndistance = raw_dist + fuzzy_rte_penalty;

    /*
     * If the new distance is less than or equal to that of the best match
     * found so far, update fuzzystate.
     */
    if columndistance < (*fuzzystate).distance {
        /* Store new lowest observed distance as first/only match */
        (*fuzzystate).distance = columndistance;
        (*fuzzystate).rfirst = rte;
        (*fuzzystate).first = attnum as AttrNumber;
        (*fuzzystate).rsecond = core::ptr::null_mut();
    } else if columndistance == (*fuzzystate).distance {
        /* If we already have a match of this distance, update state */
        if !(*fuzzystate).rsecond.is_null() {
            /*
             * Too many matches at same distance.  Clearly, this value of
             * distance is too low a bar, so drop these entries while keeping
             * the current distance value, so that only smaller distances will
             * be considered interesting.  Only if we find something of lower
             * distance will we re-populate rfirst (via the stanza above).
             */
            (*fuzzystate).rfirst = core::ptr::null_mut();
            (*fuzzystate).rsecond = core::ptr::null_mut();
        } else if !(*fuzzystate).rfirst.is_null() {
            /* Record as provisional second match */
            (*fuzzystate).rsecond = rte;
            (*fuzzystate).second = attnum as AttrNumber;
        } else {
            /*
             * Do nothing.  When rfirst is NULL, distance is more than what we
             * want to consider acceptable, so we should ignore this match.
             */
        }
    }
}

/*
 * scanNSItemForColumn
 *    Search the column names of a single namespace item for the given name.
 *    If found, return an appropriate Var node, else return NULL.
 *    If the name proves ambiguous within this nsitem, raise error.
 *
 * Side effect: if we find a match, mark the corresponding RTE as requiring
 * read access for the column.
 */
pub unsafe fn scanNSItemForColumn(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    sublevels_up: c_int,
    colname: *const c_char,
    location: c_int,
) -> *mut Node {
    let rte = (*nsitem).p_rte as *mut RangeTblEntry;
    let attnum: c_int;
    let var: *mut Var;

    /*
     * Scan the nsitem's column names (or aliases) for a match.  Complain if
     * multiple matches.
     */
    let attnum = scanRTEForColumn(
        pstate,
        rte,
        (*nsitem).p_names as *mut Alias,
        colname,
        location,
        0,
        core::ptr::null_mut(),
    );

    if attnum == InvalidAttrNumber as c_int {
        return core::ptr::null_mut(); /* Return NULL if no match */
    }

    /* In constraint check, no system column is allowed except tableOid */
    if (*pstate).p_expr_kind == EXPR_KIND_CHECK_CONSTRAINT
        && attnum < InvalidAttrNumber as c_int
        && attnum != TableOidAttributeNumber as c_int
    {
        ereport!(ERROR, errmsg!(
                "system column \"{}\" reference in check constraint is invalid",
                cstr_to_str(colname)
            )) /* C also: errcode, parser_errposition */;
    }

    /*
     * In generated column, no system column is allowed except tableOid.
     * (Required for stored generated, but we also do it for virtual generated
     * for now for consistency.)
     */
    if (*pstate).p_expr_kind == EXPR_KIND_GENERATED_COLUMN
        && attnum < InvalidAttrNumber as c_int
        && attnum != TableOidAttributeNumber as c_int
    {
        ereport!(ERROR, errmsg!(
                "cannot use system column \"{}\" in column generation expression",
                cstr_to_str(colname)
            )) /* C also: errcode, parser_errposition */;
    }

    /*
     * In a MERGE WHEN condition, no system column is allowed except tableOid
     */
    if (*pstate).p_expr_kind == EXPR_KIND_MERGE_WHEN
        && attnum < InvalidAttrNumber as c_int
        && attnum != TableOidAttributeNumber as c_int
    {
        ereport!(ERROR, errmsg!(
                "cannot use system column \"{}\" in MERGE WHEN condition",
                cstr_to_str(colname)
            )) /* C also: errcode, parser_errposition */;
    }

    /* Found a valid match, so build a Var */
    if attnum > InvalidAttrNumber as c_int {
        /* Get attribute data from the ParseNamespaceColumn array */
        let nscol = &*(*nsitem).p_nscolumns.offset((attnum - 1) as isize);

        /* Complain if dropped column.  See notes in scanRTEForColumn. */
        if nscol.p_varno == 0 {
            ereport!(ERROR, errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    cstr_to_str(colname),
                    cstr_to_str((*((*nsitem).p_names as *mut Alias)).aliasname)
                )) /* C also: errcode */;
        }

        let var = makeVar(
            nscol.p_varno as c_int,
            nscol.p_varattno,
            nscol.p_vartype,
            nscol.p_vartypmod,
            nscol.p_varcollid,
            sublevels_up as u32,
        );
        /* makeVar doesn't offer parameters for these, so set them by hand: */
        (*var).varnosyn = nscol.p_varnosyn;
        (*var).varattnosyn = nscol.p_varattnosyn;

        (*var).location = location;

        /* Mark Var for RETURNING OLD/NEW, as necessary */
        (*var).varreturningtype =
            core::mem::transmute::<c_int, VarReturningType>((*nsitem).p_returning_type);

        /* Mark Var if it's nulled by any outer joins */
        markNullableIfNeeded(pstate, var);

        /* Require read access to the column */
        markVarForSelectPriv(pstate, var);

        return var as *mut Node;
    } else {
        /* System column, so use predetermined type data */
        let sysatt = SystemAttributeDefinition(attnum as AttrNumber);
        let var = makeVar(
            (*nsitem).p_rtindex,
            attnum as AttrNumber,
            (*sysatt).atttypid,
            (*sysatt).atttypmod,
            (*sysatt).attcollation,
            sublevels_up as u32,
        );

        (*var).location = location;

        /* Mark Var for RETURNING OLD/NEW, as necessary */
        (*var).varreturningtype =
            core::mem::transmute::<c_int, VarReturningType>((*nsitem).p_returning_type);

        /* Mark Var if it's nulled by any outer joins */
        markNullableIfNeeded(pstate, var);

        /* Require read access to the column */
        markVarForSelectPriv(pstate, var);

        return var as *mut Node;
    }
}

/*
 * scanRTEForColumn
 *    Search the column names of a single RTE for the given name.
 *    If found, return the attnum (possibly negative, for a system column);
 *    else return InvalidAttrNumber.
 *    If the name proves ambiguous within this RTE, raise error.
 *
 * Actually, we only search the names listed in "eref".  This can be either
 * rte->eref, in which case we are indeed searching all the column names,
 * or for a join it can be rte->join_using_alias, in which case we are only
 * considering the common column names (which are the first N columns of the
 * join, so everything works).
 *
 * pstate and location are passed only for error-reporting purposes.
 *
 * Side effect: if fuzzystate is non-NULL, check non-system columns
 * for an approximate match and update fuzzystate accordingly.
 *
 * Note: this is factored out of scanNSItemForColumn because error message
 * creation may want to check RTEs that are not in the namespace.  To support
 * that usage, minimize the number of validity checks performed here.  It's
 * okay to complain about ambiguous-name cases, though, since if we are
 * working to complain about an invalid name, we've already eliminated that.
 */
unsafe fn scanRTEForColumn(
    pstate: *mut ParseState,
    rte: *mut RangeTblEntry,
    eref: *mut Alias,
    colname: *const c_char,
    location: c_int,
    fuzzy_rte_penalty: c_int,
    fuzzystate: *mut FuzzyAttrMatchState,
) -> c_int {
    let mut result: c_int = InvalidAttrNumber as c_int;
    let mut attnum: c_int = 0;
    let mut c: *mut ListCell;

    /*
     * Scan the user column names (or aliases) for a match. Complain if
     * multiple matches.
     *
     * Note: eref->colnames may include entries for dropped columns, but those
     * will be empty strings that cannot match any legal SQL identifier, so we
     * don't bother to test for that case here.
     *
     * Should this somehow go wrong and we try to access a dropped column,
     * we'll still catch it by virtue of the check in scanNSItemForColumn().
     * Callers interested in finding match with shortest distance need to
     * defend against this directly, though.
     */
    foreach!(c, (*eref).colnames, {
        let attcolname = strVal!(lfirst_node!(PgString, T_String, current_cell!(c)));
        attnum += 1;
        if strcmp(attcolname, colname) == 0 {
            if result != 0 {
                ereport!(ERROR, errmsg!(
                        "column reference \"{}\" is ambiguous",
                        cstr_to_str(colname)
                    )) /* C also: errcode, parser_errposition */;
            }
            result = attnum;
        }

        /* Update fuzzy match state, if provided. */
        if !fuzzystate.is_null() {
            updateFuzzyAttrMatchState(
                fuzzy_rte_penalty,
                fuzzystate,
                rte,
                attcolname,
                colname,
                attnum,
            );
        }
    });

    /*
     * If we have a unique match, return it.  Note that this allows a user
     * alias to override a system column name (such as OID) without error.
     */
    if result != 0 {
        return result;
    }

    /*
     * If the RTE represents a real relation, consider system column names.
     * Composites are only used for pseudo-relations like ON CONFLICT's
     * excluded.
     */
    if (*rte).rtekind == RTE_RELATION && (*rte).relkind != RELKIND_COMPOSITE_TYPE {
        /* quick check to see if name could be a system column */
        let attnum = specialAttNum(colname);
        if attnum != InvalidAttrNumber as c_int {
            /* now check to see if column actually is defined */
            if SearchSysCacheExists2(
                ATTNUM,
                ObjectIdGetDatum((*rte).relid),
                Int16GetDatum(attnum as i16),
            ) {
                return attnum;
            }
        }
    }

    result
}

/*
 * colNameToVar
 *    Search for an unqualified column name.
 *    If found, return the appropriate Var node (or expression).
 *    If not found, return NULL.  If the name proves ambiguous, raise error.
 *    If localonly is true, only names in the innermost query are considered.
 */
pub unsafe fn colNameToVar(
    pstate: *mut ParseState,
    colname: *const c_char,
    localonly: bool,
    location: c_int,
) -> *mut Node {
    let mut result: *mut Node = core::ptr::null_mut();
    let mut sublevels_up: c_int = 0;
    let orig_pstate = pstate;
    let mut pstate = pstate;

    while !pstate.is_null() {
        let mut l: *mut ListCell;

        foreach!(l, (*pstate).p_namespace, {
            let nsitem = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut ParseNamespaceItem;
            let newresult: *mut Node;

            /* Ignore table-only items */
            if !(*nsitem).p_cols_visible {
                continue;
            }
            /* If not inside LATERAL, ignore lateral-only items */
            if (*nsitem).p_lateral_only && !(*pstate).p_lateral_active {
                continue;
            }

            /* use orig_pstate here for consistency with other callers */
            let newresult = scanNSItemForColumn(orig_pstate, nsitem, sublevels_up, colname, location);

            if !newresult.is_null() {
                if !result.is_null() {
                    ereport!(ERROR, errmsg!(
                            "column reference \"{}\" is ambiguous",
                            cstr_to_str(colname)
                        )) /* C also: errcode, parser_errposition */;
                }
                check_lateral_ref_ok(pstate, nsitem, location);
                result = newresult;
            }
        });

        if !result.is_null() || localonly {
            break; /* found, or don't want to look at parent */
        }

        pstate = (*pstate).parentParseState;
        sublevels_up += 1;
    }

    result
}

/*
 * searchRangeTableForCol
 *    See if any RangeTblEntry could possibly provide the given column name (or
 *    find the best match available).  Returns state with relevant details.
 *
 * This is different from colNameToVar in that it considers every entry in
 * the ParseState's rangetable(s), not only those that are currently visible
 * in the p_namespace list(s).  This behavior is invalid per the SQL spec,
 * and it may give ambiguous results (since there might be multiple equally
 * valid matches).  This must be used ONLY as a heuristic in giving suitable
 * error messages.  See errorMissingColumn.
 *
 * This function is also different in that it will consider approximate
 * matches -- if the user entered an alias/column pair that is only slightly
 * different from a valid pair, we may be able to infer what they meant to
 * type and provide a reasonable hint.  We return a FuzzyAttrMatchState
 * struct providing information about both exact and approximate matches.
 */
unsafe fn searchRangeTableForCol(
    pstate: *mut ParseState,
    alias: *const c_char,
    colname: *const c_char,
    location: c_int,
) -> *mut FuzzyAttrMatchState {
    let orig_pstate = pstate;
    let fuzzystate = palloc(size_of::<FuzzyAttrMatchState>()) as *mut FuzzyAttrMatchState;
    let mut pstate = pstate;

    (*fuzzystate).distance = MAX_FUZZY_DISTANCE + 1;
    (*fuzzystate).rfirst = core::ptr::null_mut();
    (*fuzzystate).rsecond = core::ptr::null_mut();
    (*fuzzystate).rexact1 = core::ptr::null_mut();
    (*fuzzystate).rexact2 = core::ptr::null_mut();

    while !pstate.is_null() {
        let mut l: *mut ListCell;

        foreach!(l, (*pstate).p_rtable, {
            let rte = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));
            let mut fuzzy_rte_penalty: c_int = 0;

            /*
             * Typically, it is not useful to look for matches within join
             * RTEs; they effectively duplicate other RTEs for our purposes,
             * and if a match is chosen from a join RTE, an unhelpful alias is
             * displayed in the final diagnostic message.
             */
            if (*rte).rtekind == RTE_JOIN {
                continue;
            }

            /*
             * If the user didn't specify an alias, then matches against one
             * RTE are as good as another.  But if the user did specify an
             * alias, then we want at least a fuzzy - and preferably an exact
             * - match for the range table entry.
             */
            if !alias.is_null() {
                fuzzy_rte_penalty = varstr_levenshtein_less_equal(
                    alias,
                    strlen(alias),
                    (*(*rte).eref).aliasname,
                    strlen((*(*rte).eref).aliasname),
                    1,
                    1,
                    1,
                    MAX_FUZZY_DISTANCE + 1,
                    true,
                );
            }

            /*
             * Scan for a matching column, and update fuzzystate.  Non-exact
             * matches are dealt with inside scanRTEForColumn, but exact
             * matches are handled here.  (There won't be more than one exact
             * match in the same RTE, else we'd have thrown error earlier.)
             */
            let attnum = scanRTEForColumn(
                orig_pstate,
                rte,
                (*rte).eref,
                colname,
                location,
                fuzzy_rte_penalty,
                fuzzystate,
            );
            if attnum != InvalidAttrNumber as c_int && fuzzy_rte_penalty == 0 {
                if (*fuzzystate).rexact1.is_null() {
                    (*fuzzystate).rexact1 = rte;
                    (*fuzzystate).exact1 = attnum as AttrNumber;
                } else {
                    /* Needn't worry about overwriting previous rexact2 */
                    (*fuzzystate).rexact2 = rte;
                    (*fuzzystate).exact2 = attnum as AttrNumber;
                }
            }
        });

        pstate = (*pstate).parentParseState;
    }

    fuzzystate
}

/*
 * markNullableIfNeeded
 *        If the RTE referenced by the Var is nullable by outer join(s)
 *        at this point in the query, set var->varnullingrels to show that.
 */
pub unsafe fn markNullableIfNeeded(pstate: *mut ParseState, var: *mut Var) {
    let rtindex = (*var).varno as c_int;
    let relids: *mut Bitmapset;
    let mut pstate = pstate;

    /* Find the appropriate pstate */
    for _lv in 0..(*var).varlevelsup {
        pstate = (*pstate).parentParseState;
    }

    /* Find currently-relevant join relids for the Var's rel */
    if rtindex > 0 && rtindex <= list_length((*pstate).p_nullingrels) {
        relids =
            list_nth((*pstate).p_nullingrels, rtindex - 1) as *mut Bitmapset;
    } else {
        relids = core::ptr::null_mut();
    }

    /*
     * Merge with any already-declared nulling rels.  (Typically there won't
     * be any, but let's get it right if there are.)
     */
    if !relids.is_null() {
        (*var).varnullingrels = bms_union((*var).varnullingrels, relids);
    }
}

/*
 * markRTEForSelectPriv
 *       Mark the specified column of the RTE with index rtindex
 *       as requiring SELECT privilege
 *
 * col == InvalidAttrNumber means a "whole row" reference
 */
unsafe fn markRTEForSelectPriv(pstate: *mut ParseState, rtindex: c_int, col: AttrNumber) {
    let rte = rt_fetch(rtindex, (*pstate).p_rtable);

    if (*rte).rtekind == RTE_RELATION {
        let perminfo: *mut RTEPermissionInfo;

        /* Make sure the rel as a whole is marked for SELECT access */
        let perminfo = getRTEPermissionInfo((*pstate).p_rteperminfos, rte);
        (*perminfo).requiredPerms |= ACL_SELECT;
        /* Must offset the attnum to fit in a bitmapset */
        (*perminfo).selectedCols = bms_add_member(
            (*perminfo).selectedCols,
            (col - FirstLowInvalidHeapAttributeNumber) as c_int,
        );
    } else if (*rte).rtekind == RTE_JOIN {
        if col == InvalidAttrNumber {
            /*
             * A whole-row reference to a join has to be treated as whole-row
             * references to the two inputs.
             */
            let j: *mut JoinExpr;

            if rtindex > 0 && rtindex <= list_length((*pstate).p_joinexprs) {
                let j = list_nth_node_joinexpr((*pstate).p_joinexprs, rtindex - 1);
            } else {
                let j = core::ptr::null_mut::<JoinExpr>();
            }

            let j = if rtindex > 0 && rtindex <= list_length((*pstate).p_joinexprs) {
                list_nth_node_joinexpr((*pstate).p_joinexprs, rtindex - 1)
            } else {
                core::ptr::null_mut::<JoinExpr>()
            };

            if j.is_null() {
                elog!(ERROR, "could not find JoinExpr for whole-row reference");
            }

            /* Note: we can't see FromExpr here */
            if IsA!((*j).larg, T_RangeTblRef) {
                let varno = (*((*j).larg as *mut RangeTblRef)).rtindex;
                markRTEForSelectPriv(pstate, varno, InvalidAttrNumber);
            } else if IsA!((*j).larg, T_JoinExpr) {
                let varno = (*((*j).larg as *mut JoinExpr)).rtindex;
                markRTEForSelectPriv(pstate, varno, InvalidAttrNumber);
            } else {
                elog!(
                    ERROR,
                    "unrecognized node type: {}",
                    nodeTag((*j).larg as *mut Node) as c_int
                );
            }
            if IsA!((*j).rarg, T_RangeTblRef) {
                let varno = (*((*j).rarg as *mut RangeTblRef)).rtindex;
                markRTEForSelectPriv(pstate, varno, InvalidAttrNumber);
            } else if IsA!((*j).rarg, T_JoinExpr) {
                let varno = (*((*j).rarg as *mut JoinExpr)).rtindex;
                markRTEForSelectPriv(pstate, varno, InvalidAttrNumber);
            } else {
                elog!(
                    ERROR,
                    "unrecognized node type: {}",
                    nodeTag((*j).rarg as *mut Node) as c_int
                );
            }
        } else {
            /*
             * Join alias Vars for ordinary columns must refer to merged JOIN
             * USING columns.  We don't need to do anything here, because the
             * join input columns will also be referenced in the join's qual
             * clause, and will get marked for select privilege there.
             */
        }
    }
    /* other RTE types don't require privilege marking */
}

/*
 * markVarForSelectPriv
 *       Mark the RTE referenced by the Var as requiring SELECT privilege
 *       for the Var's column (the Var could be a whole-row Var, too)
 */
pub unsafe fn markVarForSelectPriv(pstate: *mut ParseState, var: *mut Var) {
    let mut pstate = pstate;

    Assert!(IsA!(var, T_Var));
    /* Find the appropriate pstate if it's an uplevel Var */
    for _lv in 0..(*var).varlevelsup {
        pstate = (*pstate).parentParseState;
    }
    markRTEForSelectPriv(pstate, (*var).varno as c_int, (*var).varattno);
}

/*
 * buildRelationAliases
 *        Construct the eref column name list for a relation RTE.
 *        This code is also used for function RTEs.
 *
 * tupdesc: the physical column information
 * alias: the user-supplied alias, or NULL if none
 * eref: the eref Alias to store column names in
 *
 * eref->colnames is filled in.  Also, alias->colnames is rebuilt to insert
 * empty strings for any dropped columns, so that it will be one-to-one with
 * physical column numbers.
 *
 * It is an error for there to be more aliases present than required.
 */
unsafe fn buildRelationAliases(tupdesc: TupleDesc, alias: *mut Alias, eref: *mut Alias) {
    let maxattrs = (*tupdesc).natts;
    let aliaslist: *mut List;
    let mut aliaslc: *mut ListCell;
    let numaliases: c_int;
    let mut varattno: c_int;
    let mut numdropped: c_int = 0;

    Assert!((*eref).colnames.is_null() || list_length((*eref).colnames) == 0);

    if !alias.is_null() {
        aliaslist = (*alias).colnames;
        aliaslc = list_head(aliaslist);
        numaliases = list_length(aliaslist);
        /* We'll rebuild the alias colname list */
        (*alias).colnames = NIL;
    } else {
        aliaslist = NIL;
        aliaslc = core::ptr::null_mut();
        numaliases = 0;
    }

    for varattno in 0..maxattrs {
        let attr = TupleDescAttr(tupdesc, varattno);
        let attrname: *mut PgString;

        if (*attr).attisdropped {
            /* Always insert an empty string for a dropped column */
            attrname = makeString(pstrdup(b"\0" as *const u8 as *const c_char));
            if !aliaslc.is_null() {
                (*alias).colnames = lappend((*alias).colnames, attrname as *mut c_void);
            }
            numdropped += 1;
        } else if !aliaslc.is_null() {
            /* Use the next user-supplied alias */
            attrname = lfirst_node!(PgString, T_String, aliaslc);
            aliaslc = lnext(aliaslist, aliaslc);
            (*alias).colnames = lappend((*alias).colnames, attrname as *mut c_void);
            (*eref).colnames = lappend((*eref).colnames, attrname as *mut c_void);
            continue;
        } else {
            attrname = makeString(pstrdup(crate::c::NameStr(&(*attr).attname)));
            (*eref).colnames = lappend((*eref).colnames, attrname as *mut c_void);
            continue;
        }

        (*eref).colnames = lappend((*eref).colnames, attrname as *mut c_void);
    }

    /* Too many user-supplied aliases? */
    if !aliaslc.is_null() {
        ereport!(ERROR, errmsg!(
                "table \"{}\" has {} columns available but {} columns specified",
                cstr_to_str((*eref).aliasname),
                maxattrs - numdropped,
                numaliases
            )) /* C also: errcode */;
    }
}

/*
 * chooseScalarFunctionAlias
 *        Select the column alias for a function in a function RTE,
 *        when the function returns a scalar type (not composite or RECORD).
 *
 * funcexpr: transformed expression tree for the function call
 * funcname: function name (as determined by FigureColname)
 * alias: the user-supplied alias for the RTE, or NULL if none
 * nfuncs: the number of functions appearing in the function RTE
 *
 * Note that the name we choose might be overridden later, if the user-given
 * alias includes column alias names.  That's of no concern here.
 */
unsafe fn chooseScalarFunctionAlias(
    funcexpr: *mut Node,
    funcname: *mut c_char,
    alias: *mut Alias,
    nfuncs: c_int,
) -> *mut c_char {
    let pname: *mut c_char;

    /*
     * If the expression is a simple function call, and the function has a
     * single OUT parameter that is named, use the parameter's name.
     */
    if !funcexpr.is_null() && IsA!(funcexpr, T_FuncExpr) {
        let pname = get_func_result_name((*(funcexpr as *mut crate::nodes::primnodes::FuncExpr)).funcid);
        if !pname.is_null() {
            return pname;
        }
    }

    /*
     * If there's just one function in the RTE, and the user gave an RTE alias
     * name, use that name.  (This makes FROM func() AS foo use "foo" as the
     * column name as well as the table alias.)
     */
    if nfuncs == 1 && !alias.is_null() {
        return (*alias).aliasname;
    }

    /*
     * Otherwise use the function name.
     */
    funcname
}

/*
 * buildNSItemFromTupleDesc
 *        Build a ParseNamespaceItem, given a tupdesc describing the columns.
 *
 * rte: the new RangeTblEntry for the rel
 * rtindex: its index in the rangetable list
 * perminfo: permission list entry for the rel
 * tupdesc: the physical column information
 */
unsafe fn buildNSItemFromTupleDesc(
    rte: *mut RangeTblEntry,
    rtindex: c_int,
    perminfo: *mut RTEPermissionInfo,
    tupdesc: TupleDesc,
) -> *mut ParseNamespaceItem {
    let nsitem: *mut ParseNamespaceItem;
    let nscolumns: *mut ParseNamespaceColumn;
    let maxattrs = (*tupdesc).natts;

    /* colnames must have the same number of entries as the nsitem */
    Assert!(maxattrs == list_length((*(*rte).eref).colnames));

    /* extract per-column data from the tupdesc */
    let nscolumns = palloc0(maxattrs as usize * size_of::<ParseNamespaceColumn>())
        as *mut ParseNamespaceColumn;

    for varattno in 0..maxattrs {
        let attr = TupleDescAttr(tupdesc, varattno);

        /* For a dropped column, just leave the entry as zeroes */
        if (*attr).attisdropped {
            continue;
        }

        let col = &mut *nscolumns.offset(varattno as isize);
        col.p_varno = rtindex as Index;
        col.p_varattno = (varattno + 1) as AttrNumber;
        col.p_vartype = (*attr).atttypid;
        col.p_vartypmod = (*attr).atttypmod;
        col.p_varcollid = (*attr).attcollation;
        col.p_varnosyn = rtindex as Index;
        col.p_varattnosyn = (varattno + 1) as AttrNumber;
    }

    /* ... and build the nsitem */
    let nsitem = palloc(size_of::<ParseNamespaceItem>()) as *mut ParseNamespaceItem;
    (*nsitem).p_names = (*rte).eref as *mut c_void;
    (*nsitem).p_rte = rte as *mut c_void;
    (*nsitem).p_rtindex = rtindex;
    (*nsitem).p_perminfo = perminfo as *mut c_void;
    (*nsitem).p_nscolumns = nscolumns;
    /* set default visibility flags; might get changed later */
    (*nsitem).p_rel_visible = true;
    (*nsitem).p_cols_visible = true;
    (*nsitem).p_lateral_only = false;
    (*nsitem).p_lateral_ok = true;
    (*nsitem).p_returning_type = VAR_RETURNING_DEFAULT as c_int;

    nsitem
}

/*
 * buildNSItemFromLists
 *        Build a ParseNamespaceItem, given column type information in lists.
 *
 * rte: the new RangeTblEntry for the rel
 * rtindex: its index in the rangetable list
 * coltypes: per-column datatype OIDs
 * coltypmods: per-column type modifiers
 * colcollation: per-column collation OIDs
 */
unsafe fn buildNSItemFromLists(
    rte: *mut RangeTblEntry,
    rtindex: c_int,
    coltypes: *mut List,
    coltypmods: *mut List,
    colcollations: *mut List,
) -> *mut ParseNamespaceItem {
    let nsitem: *mut ParseNamespaceItem;
    let nscolumns: *mut ParseNamespaceColumn;
    let maxattrs = list_length(coltypes);

    /* colnames must have the same number of entries as the nsitem */
    Assert!(maxattrs == list_length((*(*rte).eref).colnames));
    Assert!(maxattrs == list_length(coltypmods));
    Assert!(maxattrs == list_length(colcollations));

    /* extract per-column data from the lists */
    let nscolumns = palloc0(maxattrs as usize * size_of::<ParseNamespaceColumn>())
        as *mut ParseNamespaceColumn;

    let mut varattno: c_int = 0;
    let mut lct = list_head(coltypes);
    let mut lcm = list_head(coltypmods);
    let mut lcc = list_head(colcollations);
    while !lct.is_null() {
        let coltype = crate::nodes::pg_list::lfirst_oid(lct);
        let coltypmod = crate::nodes::pg_list::lfirst_int(lcm);
        let colcoll = crate::nodes::pg_list::lfirst_oid(lcc);

        let col = &mut *nscolumns.offset(varattno as isize);
        col.p_varno = rtindex as Index;
        col.p_varattno = (varattno + 1) as AttrNumber;
        col.p_vartype = coltype;
        col.p_vartypmod = coltypmod;
        col.p_varcollid = colcoll;
        col.p_varnosyn = rtindex as Index;
        col.p_varattnosyn = (varattno + 1) as AttrNumber;
        varattno += 1;

        lct = lnext(coltypes, lct);
        lcm = lnext(coltypmods, lcm);
        lcc = lnext(colcollations, lcc);
    }

    /* ... and build the nsitem */
    let nsitem = palloc(size_of::<ParseNamespaceItem>()) as *mut ParseNamespaceItem;
    (*nsitem).p_names = (*rte).eref as *mut c_void;
    (*nsitem).p_rte = rte as *mut c_void;
    (*nsitem).p_rtindex = rtindex;
    (*nsitem).p_perminfo = core::ptr::null_mut();
    (*nsitem).p_nscolumns = nscolumns;
    /* set default visibility flags; might get changed later */
    (*nsitem).p_rel_visible = true;
    (*nsitem).p_cols_visible = true;
    (*nsitem).p_lateral_only = false;
    (*nsitem).p_lateral_ok = true;
    (*nsitem).p_returning_type = VAR_RETURNING_DEFAULT as c_int;

    nsitem
}

/*
 * Open a table during parse analysis
 *
 * This is essentially just the same as table_openrv(), except that it caters
 * to some parser-specific error reporting needs, notably that it arranges
 * to include the RangeVar's parse location in any resulting error.
 *
 * Note: properly, lockmode should be declared LOCKMODE not int, but that
 * would require importing storage/lock.h into parse_relation.h.  Since
 * LOCKMODE is typedef'd as int anyway, that seems like overkill.
 */
pub unsafe fn parserOpenTable(
    pstate: *mut ParseState,
    relation: *const RangeVar,
    lockmode: c_int,
) -> Relation {
    let rel: Relation;
    let mut pcbstate: ParseCallbackState = core::mem::zeroed();

    setup_parser_errposition_callback(&mut pcbstate, pstate, (*relation).location);
    let rel = table_openrv_extended(relation, lockmode, true);
    if rel.is_null() {
        if !((*relation).schemaname as *const c_void).is_null() {
            ereport!(ERROR, errmsg!(
                    "relation \"{}.{}\" does not exist",
                    cstr_to_str((*relation).schemaname),
                    cstr_to_str((*relation).relname)
                )) /* C also: errcode */;
        } else {
            /*
             * An unqualified name might have been meant as a reference to
             * some not-yet-in-scope CTE.  The bare "does not exist" message
             * has proven remarkably unhelpful for figuring out such problems,
             * so we take pains to offer a specific hint.
             */
            if isFutureCTE(pstate, (*relation).relname) {
                ereport!(ERROR, errmsg!(
                        "relation \"{}\" does not exist",
                        cstr_to_str((*relation).relname)
                    )) /* C also: errcode, errdetail, errhint */;
            } else {
                ereport!(ERROR, errmsg!(
                        "relation \"{}\" does not exist",
                        cstr_to_str((*relation).relname)
                    )) /* C also: errcode */;
            }
        }
    }
    cancel_parser_errposition_callback(&mut pcbstate);
    rel as *mut c_void
}

/*
 * Add an entry for a relation to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * We do not link the ParseNamespaceItem into the pstate here; it's the
 * caller's job to do that in the appropriate way.
 *
 * Note: formerly this checked for refname conflicts, but that's wrong.
 * Caller is responsible for checking for conflicts in the appropriate scope.
 */
pub unsafe fn addRangeTableEntry(
    pstate: *mut ParseState,
    relation: *mut RangeVar,
    alias: *mut Alias,
    inh: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let perminfo: *mut RTEPermissionInfo;
    let refname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        (*relation).relname
    };
    let lockmode: LOCKMODE;
    let rel: Relation;
    let nsitem: *mut ParseNamespaceItem;

    Assert!(!pstate.is_null());

    (*rte).rtekind = RTE_RELATION;
    (*rte).alias = alias;

    /*
     * Identify the type of lock we'll need on this relation.  It's not the
     * query's target table (that case is handled elsewhere), so we need
     * either RowShareLock if it's locked by FOR UPDATE/SHARE, or plain
     * AccessShareLock otherwise.
     */
    let lockmode = if isLockedRefname(pstate, refname) {
        RowShareLock
    } else {
        AccessShareLock
    };

    /*
     * Get the rel's OID.  This access also ensures that we have an up-to-date
     * relcache entry for the rel.  Since this is typically the first access
     * to a rel in a statement, we must open the rel with the proper lockmode.
     */
    let rel = parserOpenTable(pstate, relation, lockmode);
    (*rte).relid = RelationGetRelid(rel as *mut RelationData);
    (*rte).inh = inh;
    (*rte).relkind = (*(*(rel as *mut RelationData)).rd_rel).relkind;
    (*rte).rellockmode = lockmode;

    /*
     * Build the list of effective column names using user-supplied aliases
     * and/or actual column names.
     */
    (*rte).eref = makeAlias(refname, NIL);
    buildRelationAliases((*(rel as *mut RelationData)).rd_att, alias, (*rte).eref);

    /*
     * Set flags and initialize access permissions.
     *
     * The initial default on access checks is always check-for-READ-access,
     * which is the right thing for all except target tables.
     */
    (*rte).lateral = false;
    (*rte).inFromCl = inFromCl;

    let perminfo = addRTEPermissionInfo(&mut (*pstate).p_rteperminfos, rte);
    (*perminfo).requiredPerms = ACL_SELECT;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    let nsitem = buildNSItemFromTupleDesc(
        rte,
        list_length((*pstate).p_rtable),
        perminfo,
        (*(rel as *mut RelationData)).rd_att,
    );

    /*
     * Drop the rel refcount, but keep the access lock till end of transaction
     * so that the table can't be deleted or have its schema modified
     * underneath us.
     */
    table_close(rel as *mut RelationData, NoLock);

    nsitem
}

/*
 * Add an entry for a relation to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is just like addRangeTableEntry() except that it makes an RTE
 * given an already-open relation instead of a RangeVar reference.
 *
 * lockmode is the lock type required for query execution; it must be one
 * of AccessShareLock, RowShareLock, or RowExclusiveLock depending on the
 * RTE's role within the query.  The caller must hold that lock mode
 * or a stronger one.
 *
 * Note: properly, lockmode should be declared LOCKMODE not int, but that
 * would require importing storage/lock.h into parse_relation.h.  Since
 * LOCKMODE is typedef'd as int anyway, that seems like overkill.
 */
pub unsafe fn addRangeTableEntryForRelation(
    pstate: *mut ParseState,
    rel: Relation,
    lockmode: c_int,
    alias: *mut Alias,
    inh: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let perminfo: *mut RTEPermissionInfo;
    let refname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        RelationGetRelationName(rel as *mut RelationData)
    };

    Assert!(!pstate.is_null());

    Assert!(
        lockmode == AccessShareLock
            || lockmode == RowShareLock
            || lockmode == RowExclusiveLock
    );
    Assert!(CheckRelationLockedByMe(rel, lockmode, true));

    (*rte).rtekind = RTE_RELATION;
    (*rte).alias = alias;
    (*rte).relid = RelationGetRelid(rel as *mut RelationData);
    (*rte).inh = inh;
    (*rte).relkind = (*(*(rel as *mut RelationData)).rd_rel).relkind;
    (*rte).rellockmode = lockmode;

    /*
     * Build the list of effective column names using user-supplied aliases
     * and/or actual column names.
     */
    (*rte).eref = makeAlias(refname, NIL);
    buildRelationAliases((*(rel as *mut RelationData)).rd_att, alias, (*rte).eref);

    /*
     * Set flags and initialize access permissions.
     *
     * The initial default on access checks is always check-for-READ-access,
     * which is the right thing for all except target tables.
     */
    (*rte).lateral = false;
    (*rte).inFromCl = inFromCl;

    let perminfo = addRTEPermissionInfo(&mut (*pstate).p_rteperminfos, rte);
    (*perminfo).requiredPerms = ACL_SELECT;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    buildNSItemFromTupleDesc(
        rte,
        list_length((*pstate).p_rtable),
        perminfo,
        (*(rel as *mut RelationData)).rd_att,
    )
}

/*
 * Add an entry for a subquery to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is much like addRangeTableEntry() except that it makes a subquery RTE.
 *
 * If the subquery does not have an alias, the auto-generated relation name in
 * the returned ParseNamespaceItem will be marked as not visible, and so only
 * unqualified references to the subquery columns will be allowed, and the
 * relation name will not conflict with others in the pstate's namespace list.
 */
pub unsafe fn addRangeTableEntryForSubquery(
    pstate: *mut ParseState,
    subquery: *mut Query,
    alias: *mut Alias,
    lateral: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let eref: *mut Alias;
    let numaliases: c_int;
    let mut coltypes: *mut List = NIL;
    let mut coltypmods: *mut List = NIL;
    let mut colcollations: *mut List = NIL;
    let mut varattno: c_int;
    let mut tlistitem: *mut ListCell;
    let nsitem: *mut ParseNamespaceItem;

    Assert!(!pstate.is_null());

    (*rte).rtekind = RTE_SUBQUERY;
    (*rte).subquery = subquery;
    (*rte).alias = alias;

    let eref: *mut Alias = if !alias.is_null() {
        copyObject(alias)
    } else {
        makeAlias(b"unnamed_subquery\0" as *const u8 as *const c_char, NIL)
    };
    let numaliases = list_length((*eref).colnames);

    /* fill in any unspecified alias columns, and extract column type info */
    varattno = 0;
    foreach!(tlistitem, (*subquery).targetList, {
        let te = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(tlistitem));

        if (*te).resjunk {
            continue;
        }
        varattno += 1;
        Assert!(varattno == (*te).resno as c_int);
        if varattno > numaliases {
            let attrname = pstrdup((*te).resname);
            (*eref).colnames = lappend((*eref).colnames, makeString(attrname) as *mut c_void);
        }
        coltypes = lappend_oid(coltypes, exprType((*te).expr as *mut Node));
        coltypmods = lappend_int(coltypmods, exprTypmod((*te).expr as *mut Node));
        colcollations = lappend_oid(colcollations, exprCollation((*te).expr as *mut Node));
    });
    if varattno < numaliases {
        ereport!(ERROR, errmsg!(
                "table \"{}\" has {} columns available but {} columns specified",
                cstr_to_str((*eref).aliasname),
                varattno,
                numaliases
            )) /* C also: errcode */;
    }

    (*rte).eref = eref;

    /*
     * Set flags.
     *
     * Subqueries are never checked for access rights, so no need to perform
     * addRTEPermissionInfo().
     */
    (*rte).lateral = lateral;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    let nsitem = buildNSItemFromLists(
        rte,
        list_length((*pstate).p_rtable),
        coltypes,
        coltypmods,
        colcollations,
    );

    /*
     * Mark it visible as a relation name only if it had a user-written alias.
     */
    (*nsitem).p_rel_visible = !alias.is_null();

    nsitem
}

/*
 * Add an entry for a function (or functions) to the pstate's range table
 * (p_rtable).  Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is much like addRangeTableEntry() except that it makes a function RTE.
 */
pub unsafe fn addRangeTableEntryForFunction(
    pstate: *mut ParseState,
    funcnames: *mut List,
    funcexprs: *mut List,
    coldeflists: *mut List,
    rangefunc: *mut RangeFunction,
    lateral: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let alias: *mut Alias = (*rangefunc).alias;
    let eref: *mut Alias;
    let aliasname: *mut c_char;
    let nfuncs = list_length(funcexprs);
    let functupdescs: *mut TupleDesc;
    let mut tupdesc: TupleDesc;
    let mut i: c_int;
    let mut j: c_int;
    let mut funcno: c_int;
    let mut natts: c_int;
    let mut totalatts: c_int;

    Assert!(!pstate.is_null());

    (*rte).rtekind = RTE_FUNCTION;
    (*rte).relid = InvalidOid;
    (*rte).subquery = core::ptr::null_mut();
    (*rte).functions = NIL; /* we'll fill this list below */
    (*rte).funcordinality = (*rangefunc).ordinality;
    (*rte).alias = alias;

    /*
     * Choose the RTE alias name.  We default to using the first function's
     * name even when there's more than one; which is maybe arguable but beats
     * using something constant like "table".
     */
    let aliasname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        linitial(funcnames) as *mut c_char
    };

    let eref = makeAlias(aliasname, NIL);
    (*rte).eref = eref;

    /* Process each function ... */
    let functupdescs = palloc(nfuncs as usize * size_of::<TupleDesc>()) as *mut TupleDesc;

    totalatts = 0;
    funcno = 0;

    let mut lc1 = list_head(funcexprs);
    let mut lc2 = list_head(funcnames);
    let mut lc3 = list_head(coldeflists);
    while !lc1.is_null() {
        let funcexpr = crate::nodes::pg_list::lfirst(lc1) as *mut Node;
        let funcname = crate::nodes::pg_list::lfirst(lc2) as *mut c_char;
        let coldeflist = crate::nodes::pg_list::lfirst(lc3) as *mut List;
        let rtfunc = makeNode!(RangeTblFunction, T_RangeTblFunction) as *mut RangeTblFunction;
        let mut functypclass: TypeFuncClass;
        let mut funcrettype: Oid = InvalidOid;

        /* Initialize RangeTblFunction node */
        (*rtfunc).funcexpr = funcexpr;
        (*rtfunc).funccolnames = NIL;
        (*rtfunc).funccoltypes = NIL;
        (*rtfunc).funccoltypmods = NIL;
        (*rtfunc).funccolcollations = NIL;
        (*rtfunc).funcparams = core::ptr::null_mut(); /* not set until planning */

        /*
         * Now determine if the function returns a simple or composite type.
         */
        tupdesc = core::ptr::null_mut();
        let functypclass = get_expr_result_type(funcexpr, &mut funcrettype, &mut tupdesc);

        /*
         * A coldeflist is required if the function returns RECORD and hasn't
         * got a predetermined record type, and is prohibited otherwise.  This
         * can be a bit confusing, so we expend some effort on delivering a
         * relevant error message.
         */
        if !coldeflist.is_null() && list_length(coldeflist) > 0 {
            match functypclass {
                x if x == TYPEFUNC_RECORD => {
                    /* ok */
                }
                x if x == TYPEFUNC_COMPOSITE || x == TYPEFUNC_COMPOSITE_DOMAIN => {
                    /*
                     * If the function's raw result type is RECORD, we must
                     * have resolved it using its OUT parameters.  Otherwise,
                     * it must have a named composite type.
                     */
                    if exprType(funcexpr) == RECORDOID {
                        ereport!(ERROR, errmsg!("a column definition list is redundant for a function with OUT parameters")) /* C also: errcode, parser_errposition */;
                    } else {
                        ereport!(ERROR, errmsg!("a column definition list is redundant for a function returning a named composite type")) /* C also: errcode, parser_errposition */;
                    }
                }
                _ => {
                    ereport!(ERROR, errmsg!(
                            "a column definition list is only allowed for functions returning \"record\""
                        )) /* C also: errcode, parser_errposition */;
                }
            }
        } else {
            if functypclass == TYPEFUNC_RECORD {
                ereport!(ERROR, errmsg!("a column definition list is required for functions returning \"record\"")) /* C also: errcode, parser_errposition */;
            }
        }

        if functypclass == TYPEFUNC_COMPOSITE || functypclass == TYPEFUNC_COMPOSITE_DOMAIN {
            /* Composite data type, e.g. a table's row type */
            Assert!(!tupdesc.is_null());
        } else if functypclass == TYPEFUNC_SCALAR {
            /* Base data type, i.e. scalar */
            tupdesc = CreateTemplateTupleDesc(1);
            TupleDescInitEntry(
                tupdesc,
                1 as AttrNumber,
                chooseScalarFunctionAlias(funcexpr, funcname, alias, nfuncs),
                funcrettype,
                exprTypmod(funcexpr),
                0,
            );
            TupleDescInitEntryCollation(tupdesc, 1 as AttrNumber, exprCollation(funcexpr));
        } else if functypclass == TYPEFUNC_RECORD {
            let mut col_lc: *mut ListCell;

            /*
             * Use the column definition list to construct a tupdesc and fill
             * in the RangeTblFunction's lists.  Limit number of columns to
             * MaxHeapAttributeNumber, because CheckAttributeNamesTypes will.
             */
            if list_length(coldeflist) > MaxHeapAttributeNumber {
                ereport!(ERROR, errmsg!(
                        "column definition lists can have at most {} entries",
                        MaxHeapAttributeNumber
                    )) /* C also: errcode, parser_errposition */;
            }
            tupdesc = CreateTemplateTupleDesc(list_length(coldeflist));
            i = 1;
            foreach!(col_lc, coldeflist, {
                let n = lfirst_node!(ColumnDef, T_ColumnDef, current_cell!(col_lc));
                let attrname: *mut c_char = (*n).colname;
                let mut attrtype: Oid = InvalidOid;
                let mut attrtypmod: int32 = 0;
                let mut attrcollation: Oid = InvalidOid;

                if (*(*n).typeName).setof {
                    ereport!(ERROR, errmsg!(
                            "column \"{}\" cannot be declared SETOF",
                            cstr_to_str(attrname)
                        )) /* C also: errcode, parser_errposition */;
                }
                typenameTypeIdAndMod(pstate, (*n).typeName as *mut c_void, &mut attrtype, &mut attrtypmod);
                attrcollation = GetColumnDefCollation(pstate, n, attrtype);
                TupleDescInitEntry(tupdesc, i as AttrNumber, attrname, attrtype, attrtypmod, 0);
                TupleDescInitEntryCollation(tupdesc, i as AttrNumber, attrcollation);
                (*rtfunc).funccolnames =
                    lappend((*rtfunc).funccolnames, makeString(pstrdup(attrname)) as *mut c_void);
                (*rtfunc).funccoltypes = lappend_oid((*rtfunc).funccoltypes, attrtype);
                (*rtfunc).funccoltypmods = lappend_int((*rtfunc).funccoltypmods, attrtypmod);
                (*rtfunc).funccolcollations = lappend_oid((*rtfunc).funccolcollations, attrcollation);

                i += 1;
            });

            /*
             * Ensure that the coldeflist defines a legal set of names (no
             * duplicates, but we needn't worry about system column names) and
             * datatypes.  Although we mostly can't allow pseudo-types, it
             * seems safe to allow RECORD and RECORD[], since values within
             * those type classes are self-identifying at runtime, and the
             * coldeflist doesn't represent anything that will be visible to
             * other sessions.
             */
            CheckAttributeNamesTypes(tupdesc, RELKIND_COMPOSITE_TYPE, CHKATYPE_ANYRECORD);
        } else {
            ereport!(ERROR, errmsg!(
                    "function \"{}\" in FROM has unsupported return type {}",
                    cstr_to_str(funcname),
                    cstr_to_str(format_type_be_stub(funcrettype))
                )) /* C also: errcode, parser_errposition */;
        }

        /* Finish off the RangeTblFunction and add it to the RTE's list */
        (*rtfunc).funccolcount = (*tupdesc).natts;
        (*rte).functions = lappend((*rte).functions, rtfunc as *mut c_void);

        /* Save the tupdesc for use below */
        *functupdescs.offset(funcno as isize) = tupdesc;
        totalatts += (*tupdesc).natts;
        funcno += 1;

        lc1 = lnext(funcexprs, lc1);
        lc2 = lnext(funcnames, lc2);
        lc3 = lnext(coldeflists, lc3);
    }

    /*
     * If there's more than one function, or we want an ordinality column, we
     * have to produce a merged tupdesc.
     */
    if nfuncs > 1 || (*rangefunc).ordinality {
        if (*rangefunc).ordinality {
            totalatts += 1;
        }

        /* Disallow more columns than will fit in a tuple */
        if totalatts > MaxTupleAttributeNumber {
            ereport!(ERROR, errmsg!(
                    "functions in FROM can return at most {} columns",
                    MaxTupleAttributeNumber
                )) /* C also: errcode, parser_errposition */;
        }

        /* Merge the tuple descs of each function into a composite one */
        tupdesc = CreateTemplateTupleDesc(totalatts);
        natts = 0;
        for ii in 0..nfuncs {
            let src = *functupdescs.offset(ii as isize);
            for jj in 1..=(*src).natts {
                natts += 1;
                TupleDescCopyEntry(tupdesc, natts as AttrNumber, src, jj as AttrNumber);
            }
        }

        /* Add the ordinality column if needed */
        if (*rangefunc).ordinality {
            natts += 1;
            TupleDescInitEntry(
                tupdesc,
                natts as AttrNumber,
                b"ordinality\0" as *const u8 as *const c_char,
                INT8OID,
                -1,
                0,
            );
            /* no need to set collation */
        }

        Assert!(natts == totalatts);
    } else {
        /* We can just use the single function's tupdesc as-is */
        tupdesc = *functupdescs.offset(0);
    }

    /* Use the tupdesc while assigning column aliases for the RTE */
    buildRelationAliases(tupdesc, alias, eref);

    /*
     * Set flags and access permissions.
     *
     * Functions are never checked for access rights (at least, not by
     * ExecCheckPermissions()), so no need to perform addRTEPermissionInfo().
     */
    (*rte).lateral = lateral;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    buildNSItemFromTupleDesc(
        rte,
        list_length((*pstate).p_rtable),
        core::ptr::null_mut(),
        tupdesc,
    )
}

// TODO(pg-port): utils/adt/format_type.c - format_type_be
unsafe fn format_type_be_stub(_type_oid: Oid) -> *const c_char {
    b"<type>\0" as *const u8 as *const c_char
}

// Stub: isLockedRefname - the real version is exposed in this file as a public fn below,
// but internally we call the real logic through pstate->p_locking_clause.
unsafe fn isLockedRefname(pstate: *mut ParseState, refname: *mut c_char) -> bool {
    isLockedRefname_pub(pstate, refname as *const c_char)
}

/*
 * Add an entry for a table function to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is much like addRangeTableEntry() except that it makes a tablefunc RTE.
 */
pub unsafe fn addRangeTableEntryForTableFunc(
    pstate: *mut ParseState,
    tf: *mut TableFunc,
    alias: *mut Alias,
    lateral: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let refname: *mut c_char;
    let eref: *mut Alias;
    let numaliases: c_int;

    Assert!(!pstate.is_null());

    /* Disallow more columns than will fit in a tuple */
    if list_length((*tf).colnames) > MaxTupleAttributeNumber {
        ereport!(ERROR, errmsg!(
                "functions in FROM can return at most {} columns",
                MaxTupleAttributeNumber
            )) /* C also: errcode, parser_errposition */;
    }
    Assert!(list_length((*tf).coltypes) == list_length((*tf).colnames));
    Assert!(list_length((*tf).coltypmods) == list_length((*tf).colnames));
    Assert!(list_length((*tf).colcollations) == list_length((*tf).colnames));

    (*rte).rtekind = RTE_TABLEFUNC;
    (*rte).relid = InvalidOid;
    (*rte).subquery = core::ptr::null_mut();
    (*rte).tablefunc = tf;
    (*rte).coltypes = (*tf).coltypes;
    (*rte).coltypmods = (*tf).coltypmods;
    (*rte).colcollations = (*tf).colcollations;
    (*rte).alias = alias;

    let refname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        pstrdup(if (*tf).functype == TFT_XMLTABLE {
            b"xmltable\0" as *const u8 as *const c_char
        } else {
            b"json_table\0" as *const u8 as *const c_char
        })
    };
    let eref: *mut Alias = if !alias.is_null() {
        copyObject(alias)
    } else {
        makeAlias(refname, NIL)
    };
    let numaliases = list_length((*eref).colnames);

    /* fill in any unspecified alias columns */
    if numaliases < list_length((*tf).colnames) {
        (*eref).colnames = list_concat(
            (*eref).colnames,
            list_copy_tail((*tf).colnames, numaliases),
        );
    }

    if numaliases > list_length((*tf).colnames) {
        ereport!(ERROR, errmsg!(
                "{} function has {} columns available but {} columns specified",
                if (*tf).functype == TFT_XMLTABLE { "XMLTABLE" } else { "JSON_TABLE" },
                list_length((*tf).colnames),
                numaliases
            )) /* C also: errcode */;
    }

    (*rte).eref = eref;

    /*
     * Set flags and access permissions.
     *
     * Tablefuncs are never checked for access rights (at least, not by
     * ExecCheckPermissions()), so no need to perform addRTEPermissionInfo().
     */
    (*rte).lateral = lateral;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    buildNSItemFromLists(
        rte,
        list_length((*pstate).p_rtable),
        (*rte).coltypes,
        (*rte).coltypmods,
        (*rte).colcollations,
    )
}

/*
 * Add an entry for a VALUES list to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is much like addRangeTableEntry() except that it makes a values RTE.
 */
pub unsafe fn addRangeTableEntryForValues(
    pstate: *mut ParseState,
    exprs: *mut List,
    coltypes: *mut List,
    coltypmods: *mut List,
    colcollations: *mut List,
    alias: *mut Alias,
    lateral: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let refname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        pstrdup(b"*VALUES*\0" as *const u8 as *const c_char)
    };
    let eref: *mut Alias;
    let mut numaliases: c_int;
    let numcolumns: c_int;

    Assert!(!pstate.is_null());

    (*rte).rtekind = RTE_VALUES;
    (*rte).relid = InvalidOid;
    (*rte).subquery = core::ptr::null_mut();
    (*rte).values_lists = exprs;
    (*rte).coltypes = coltypes;
    (*rte).coltypmods = coltypmods;
    (*rte).colcollations = colcollations;
    (*rte).alias = alias;

    let eref: *mut Alias = if !alias.is_null() {
        copyObject(alias)
    } else {
        makeAlias(refname, NIL)
    };

    /* fill in any unspecified alias columns */
    let numcolumns = list_length(linitial(exprs) as *const List);
    numaliases = list_length((*eref).colnames);
    while numaliases < numcolumns {
        let mut attrname: [c_char; 64] = [0; 64];
        numaliases += 1;
        snprintf(
            attrname.as_mut_ptr(),
            64,
            b"column{}\0" as *const u8 as *const c_char,
            numaliases,
        );
        (*eref).colnames = lappend(
            (*eref).colnames,
            makeString(pstrdup(attrname.as_ptr())) as *mut c_void,
        );
    }
    if numcolumns < numaliases {
        ereport!(ERROR, errmsg!(
                "VALUES lists \"{}\" have {} columns available but {} columns specified",
                cstr_to_str(refname),
                numcolumns,
                numaliases
            )) /* C also: errcode */;
    }

    (*rte).eref = eref;

    /*
     * Set flags and access permissions.
     *
     * Subqueries are never checked for access rights, so no need to perform
     * addRTEPermissionInfo().
     */
    (*rte).lateral = lateral;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    buildNSItemFromLists(
        rte,
        list_length((*pstate).p_rtable),
        (*rte).coltypes,
        (*rte).coltypmods,
        (*rte).colcollations,
    )
}

/*
 * Add an entry for a join to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is much like addRangeTableEntry() except that it makes a join RTE.
 * Also, it's more convenient for the caller to construct the
 * ParseNamespaceColumn array, so we pass that in.
 */
pub unsafe fn addRangeTableEntryForJoin(
    pstate: *mut ParseState,
    colnames: *mut List,
    nscolumns: *mut ParseNamespaceColumn,
    jointype: JoinType,
    nummergedcols: c_int,
    aliasvars: *mut List,
    leftcols: *mut List,
    rightcols: *mut List,
    join_using_alias: *mut Alias,
    alias: *mut Alias,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let eref: *mut Alias;
    let mut numaliases: c_int;
    let nsitem: *mut ParseNamespaceItem;

    Assert!(!pstate.is_null());

    /*
     * Fail if join has too many columns --- we must be able to reference any
     * of the columns with an AttrNumber.
     */
    if list_length(aliasvars) > MaxAttrNumber as c_int {
        ereport!(ERROR, errmsg!("joins can have at most {} columns", MaxAttrNumber)) /* C also: errcode */;
    }

    (*rte).rtekind = RTE_JOIN;
    (*rte).relid = InvalidOid;
    (*rte).subquery = core::ptr::null_mut();
    (*rte).jointype = jointype;
    (*rte).joinmergedcols = nummergedcols;
    (*rte).joinaliasvars = aliasvars;
    (*rte).joinleftcols = leftcols;
    (*rte).joinrightcols = rightcols;
    (*rte).join_using_alias = join_using_alias;
    (*rte).alias = alias;

    let eref: *mut Alias = if !alias.is_null() {
        copyObject(alias)
    } else {
        makeAlias(b"unnamed_join\0" as *const u8 as *const c_char, NIL)
    };
    let numaliases = list_length((*eref).colnames);

    /* fill in any unspecified alias columns */
    if numaliases < list_length(colnames) {
        (*eref).colnames =
            list_concat((*eref).colnames, list_copy_tail(colnames, numaliases));
    }

    if numaliases > list_length(colnames) {
        ereport!(ERROR, errmsg!(
                "join expression \"{}\" has {} columns available but {} columns specified",
                cstr_to_str((*eref).aliasname),
                list_length(colnames),
                numaliases
            )) /* C also: errcode */;
    }

    (*rte).eref = eref;

    /*
     * Set flags and access permissions.
     *
     * Joins are never checked for access rights, so no need to perform
     * addRTEPermissionInfo().
     */
    (*rte).lateral = false;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    let nsitem = palloc(size_of::<ParseNamespaceItem>()) as *mut ParseNamespaceItem;
    (*nsitem).p_names = (*rte).eref as *mut c_void;
    (*nsitem).p_rte = rte as *mut c_void;
    (*nsitem).p_perminfo = core::ptr::null_mut();
    (*nsitem).p_rtindex = list_length((*pstate).p_rtable);
    (*nsitem).p_nscolumns = nscolumns;
    /* set default visibility flags; might get changed later */
    (*nsitem).p_rel_visible = true;
    (*nsitem).p_cols_visible = true;
    (*nsitem).p_lateral_only = false;
    (*nsitem).p_lateral_ok = true;
    (*nsitem).p_returning_type = VAR_RETURNING_DEFAULT as c_int;

    nsitem
}

/*
 * Add an entry for a CTE reference to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * This is much like addRangeTableEntry() except that it makes a CTE RTE.
 */
pub unsafe fn addRangeTableEntryForCTE(
    pstate: *mut ParseState,
    cte: *mut CommonTableExpr,
    levelsup: Index,
    rv: *mut RangeVar,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let alias: *mut Alias = (*rv).alias;
    let refname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        (*cte).ctename
    };
    let eref: *mut Alias;
    let mut numaliases: c_int;
    let mut varattno: c_int;
    let mut lc: *mut ListCell;
    let mut n_dontexpand_columns: c_int = 0;
    let psi: *mut ParseNamespaceItem;

    Assert!(!pstate.is_null());

    (*rte).rtekind = RTE_CTE;
    (*rte).ctename = (*cte).ctename;
    (*rte).ctelevelsup = levelsup;

    /* Self-reference if and only if CTE's parse analysis isn't completed */
    (*rte).self_reference = !IsA!((*cte).ctequery, T_Query);
    Assert!((*cte).cterecursive || !(*rte).self_reference);
    /* Bump the CTE's refcount if this isn't a self-reference */
    if !(*rte).self_reference {
        (*cte).cterefcount += 1;
    }

    /*
     * We throw error if the CTE is INSERT/UPDATE/DELETE/MERGE without
     * RETURNING.  This won't get checked in case of a self-reference, but
     * that's OK because data-modifying CTEs aren't allowed to be recursive
     * anyhow.
     */
    if IsA!((*cte).ctequery, T_Query) {
        let ctequery = (*cte).ctequery as *mut Query;

        if (*ctequery).commandType != CmdType::CMD_SELECT
            && list_length((*ctequery).returningList) == 0
        {
            ereport!(ERROR, errmsg!(
                    "WITH query \"{}\" does not have a RETURNING clause",
                    cstr_to_str((*cte).ctename)
                )) /* C also: errcode, parser_errposition */;
        }
    }

    (*rte).coltypes = list_copy((*cte).ctecoltypes);
    (*rte).coltypmods = list_copy((*cte).ctecoltypmods);
    (*rte).colcollations = list_copy((*cte).ctecolcollations);

    (*rte).alias = alias;
    let eref: *mut Alias = if !alias.is_null() {
        copyObject(alias)
    } else {
        makeAlias(refname, NIL)
    };
    let numaliases = list_length((*eref).colnames);

    /* fill in any unspecified alias columns */
    varattno = 0;
    foreach!(lc, (*cte).ctecolnames, {
        varattno += 1;
        if varattno > numaliases {
            (*eref).colnames = lappend((*eref).colnames, crate::nodes::pg_list::lfirst(current_cell!(lc)));
        }
    });
    if varattno < numaliases {
        ereport!(ERROR, errmsg!(
                "table \"{}\" has {} columns available but {} columns specified",
                cstr_to_str(refname),
                varattno,
                numaliases
            )) /* C also: errcode */;
    }

    (*rte).eref = eref;

    if !(*cte).search_clause.is_null() {
        (*(*rte).eref).colnames = lappend(
            (*(*rte).eref).colnames,
            makeString((*(*cte).search_clause).search_seq_column) as *mut c_void,
        );
        if (*(*cte).search_clause).search_breadth_first {
            (*rte).coltypes = lappend_oid((*rte).coltypes, RECORDOID);
        } else {
            (*rte).coltypes = lappend_oid((*rte).coltypes, RECORDARRAYOID);
        }
        (*rte).coltypmods = lappend_int((*rte).coltypmods, -1);
        (*rte).colcollations = lappend_oid((*rte).colcollations, InvalidOid);

        n_dontexpand_columns += 1;
    }

    if !(*cte).cycle_clause.is_null() {
        (*(*rte).eref).colnames = lappend(
            (*(*rte).eref).colnames,
            makeString((*(*cte).cycle_clause).cycle_mark_column) as *mut c_void,
        );
        (*rte).coltypes = lappend_oid((*rte).coltypes, (*(*cte).cycle_clause).cycle_mark_type);
        (*rte).coltypmods =
            lappend_int((*rte).coltypmods, (*(*cte).cycle_clause).cycle_mark_typmod);
        (*rte).colcollations =
            lappend_oid((*rte).colcollations, (*(*cte).cycle_clause).cycle_mark_collation);

        (*(*rte).eref).colnames = lappend(
            (*(*rte).eref).colnames,
            makeString((*(*cte).cycle_clause).cycle_path_column) as *mut c_void,
        );
        (*rte).coltypes = lappend_oid((*rte).coltypes, RECORDARRAYOID);
        (*rte).coltypmods = lappend_int((*rte).coltypmods, -1);
        (*rte).colcollations = lappend_oid((*rte).colcollations, InvalidOid);

        n_dontexpand_columns += 2;
    }

    /*
     * Set flags and access permissions.
     *
     * Subqueries are never checked for access rights, so no need to perform
     * addRTEPermissionInfo().
     */
    (*rte).lateral = false;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    let psi = buildNSItemFromLists(
        rte,
        list_length((*pstate).p_rtable),
        (*rte).coltypes,
        (*rte).coltypmods,
        (*rte).colcollations,
    );

    /*
     * The columns added by search and cycle clauses are not included in star
     * expansion in queries contained in the CTE.
     */
    if (*rte).ctelevelsup > 0 {
        let ncols = list_length((*((*psi).p_names as *mut Alias)).colnames);
        for ii in 0..n_dontexpand_columns {
            (*(*psi).p_nscolumns.offset((ncols - 1 - ii) as isize)).p_dontexpand = true;
        }
    }

    psi
}

/*
 * Add an entry for an ephemeral named relation reference to the pstate's
 * range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 *
 * It is expected that the RangeVar, which up until now is only known to be an
 * ephemeral named relation, will (in conjunction with the QueryEnvironment in
 * the ParseState), create a RangeTblEntry for a specific *kind* of ephemeral
 * named relation, based on enrtype.
 *
 * This is much like addRangeTableEntry() except that it makes an RTE for an
 * ephemeral named relation.
 */
pub unsafe fn addRangeTableEntryForENR(
    pstate: *mut ParseState,
    rv: *mut RangeVar,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let alias: *mut Alias = (*rv).alias;
    let refname: *mut c_char = if !alias.is_null() {
        (*alias).aliasname
    } else {
        (*rv).relname
    };
    let enrmd: EphemeralNamedRelationMetadata;
    let tupdesc: TupleDesc;
    let mut attno: c_int;

    Assert!(!pstate.is_null());
    let enrmd = get_visible_ENR(pstate, (*rv).relname);
    Assert!(!enrmd.is_null());

    match (*enrmd).enrtype {
        x if x == ENR_NAMED_TUPLESTORE => {
            (*rte).rtekind = RTE_NAMEDTUPLESTORE;
        }
        _other => {
            elog!(ERROR, "unexpected enrtype");
            return core::ptr::null_mut(); /* for fussy compilers */
        }
    }

    /*
     * Record dependency on a relation.  This allows plans to be invalidated
     * if they access transition tables linked to a table that is altered.
     */
    (*rte).relid = (*enrmd).reliddesc;

    /*
     * Build the list of effective column names using user-supplied aliases
     * and/or actual column names.
     */
    let tupdesc = ENRMetadataGetTupDesc(enrmd);
    (*rte).eref = makeAlias(refname, NIL);
    buildRelationAliases(tupdesc, alias, (*rte).eref);

    /* Record additional data for ENR, including column type info */
    (*rte).enrname = (*enrmd).name;
    (*rte).enrtuples = (*enrmd).enrtuples;
    (*rte).coltypes = NIL;
    (*rte).coltypmods = NIL;
    (*rte).colcollations = NIL;
    attno = 1;
    while attno <= (*tupdesc).natts {
        let att = TupleDescAttr(tupdesc, attno - 1);

        if (*att).attisdropped {
            /* Record zeroes for a dropped column */
            (*rte).coltypes = lappend_oid((*rte).coltypes, InvalidOid);
            (*rte).coltypmods = lappend_int((*rte).coltypmods, 0);
            (*rte).colcollations = lappend_oid((*rte).colcollations, InvalidOid);
        } else {
            /* Let's just make sure we can tell this isn't dropped */
            if (*att).atttypid == InvalidOid {
                elog!(
                    ERROR,
                    "atttypid is invalid for non-dropped column in \"{}\"",
                    cstr_to_str((*rv).relname)
                );
            }
            (*rte).coltypes = lappend_oid((*rte).coltypes, (*att).atttypid);
            (*rte).coltypmods = lappend_int((*rte).coltypmods, (*att).atttypmod);
            (*rte).colcollations = lappend_oid((*rte).colcollations, (*att).attcollation);
        }
        attno += 1;
    }

    /*
     * Set flags and access permissions.
     *
     * ENRs are never checked for access rights, so no need to perform
     * addRTEPermissionInfo().
     */
    (*rte).lateral = false;
    (*rte).inFromCl = inFromCl;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    buildNSItemFromTupleDesc(
        rte,
        list_length((*pstate).p_rtable),
        core::ptr::null_mut(),
        tupdesc,
    )
}

/*
 * Add an entry for grouping step to the pstate's range table (p_rtable).
 * Then, construct and return a ParseNamespaceItem for the new RTE.
 */
pub unsafe fn addRangeTableEntryForGroup(
    pstate: *mut ParseState,
    groupClauses: *mut List,
) -> *mut ParseNamespaceItem {
    let rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    let eref: *mut Alias;
    let mut groupexprs: *mut List = NIL;
    let mut coltypes: *mut List = NIL;
    let mut coltypmods: *mut List = NIL;
    let mut colcollations: *mut List = NIL;
    let mut lc: *mut ListCell;
    let nsitem: *mut ParseNamespaceItem;

    Assert!(!pstate.is_null());

    (*rte).rtekind = RTE_GROUP;
    (*rte).alias = core::ptr::null_mut();

    let eref = makeAlias(b"*GROUP*\0" as *const u8 as *const c_char, NIL);

    /* fill in any unspecified alias columns, and extract column type info */
    foreach!(lc, groupClauses, {
        let te = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(lc));
        let colname: *mut c_char = if !(*te).resname.is_null() {
            pstrdup((*te).resname)
        } else {
            b"?column?\0" as *const u8 as *mut u8 as *mut c_char
        };

        (*eref).colnames = lappend((*eref).colnames, makeString(colname) as *mut c_void);

        groupexprs = lappend(groupexprs, copyObject((*te).expr as *mut Expr) as *mut c_void);

        coltypes = lappend_oid(coltypes, exprType((*te).expr as *mut Node));
        coltypmods = lappend_int(coltypmods, exprTypmod((*te).expr as *mut Node));
        colcollations = lappend_oid(colcollations, exprCollation((*te).expr as *mut Node));
    });

    (*rte).eref = eref;
    (*rte).groupexprs = groupexprs;

    /*
     * Set flags.
     *
     * The grouping step is never checked for access rights, so no need to
     * perform addRTEPermissionInfo().
     */
    (*rte).lateral = false;
    (*rte).inFromCl = false;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.  But we don't add it to the join list --- caller must do that if
     * appropriate.
     */
    (*pstate).p_rtable = lappend((*pstate).p_rtable, rte as *mut c_void);

    /*
     * Build a ParseNamespaceItem, but don't add it to the pstate's namespace
     * list --- caller must do that if appropriate.
     */
    let nsitem = buildNSItemFromLists(
        rte,
        list_length((*pstate).p_rtable),
        coltypes,
        coltypmods,
        colcollations,
    );

    nsitem
}


/*
 * Has the specified refname been selected FOR UPDATE/FOR SHARE?
 *
 * This is used when we have not yet done transformLockingClause, but need
 * to know the correct lock to take during initial opening of relations.
 *
 * Note that refname may be NULL (for a subquery without an alias), in which
 * case the relation can't be locked by name, but it might still be locked if
 * a locking clause requests that all tables be locked.
 *
 * Note: we pay no attention to whether it's FOR UPDATE vs FOR SHARE,
 * since the table-level lock is the same either way.
 */
pub unsafe fn isLockedRefname_pub(pstate: *mut ParseState, refname: *const c_char) -> bool {
    let mut l: *mut ListCell;

    /*
     * If we are in a subquery specified as locked FOR UPDATE/SHARE from
     * parent level, then act as though there's a generic FOR UPDATE here.
     */
    if (*pstate).p_locked_from_parent {
        return true;
    }

    foreach!(l, (*pstate).p_locking_clause, {
        let lc = lfirst_node!(LockingClause, T_LockingClause, current_cell!(l));

        if list_length((*lc).lockedRels) == 0 {
            /* all tables used in query */
            return true;
        } else if !refname.is_null() {
            /* just the named tables */
            let mut l2: *mut ListCell;

            foreach!(l2, (*lc).lockedRels, {
                let thisrel = lfirst_node!(RangeVar, T_RangeVar, current_cell!(l2));

                if strcmp(refname, (*thisrel).relname) == 0 {
                    return true;
                }
            });
        }
    });
    false
}

/*
 * Add the given nsitem/RTE as a top-level entry in the pstate's join list
 * and/or namespace list.  (We assume caller has checked for any
 * namespace conflicts.)  The nsitem is always marked as unconditionally
 * visible, that is, not LATERAL-only.
 */
pub unsafe fn addNSItemToQuery(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    addToJoinList: bool,
    addToRelNameSpace: bool,
    addToVarNameSpace: bool,
) {
    if addToJoinList {
        let rtr = makeNode!(RangeTblRef, T_RangeTblRef) as *mut RangeTblRef;

        (*rtr).rtindex = (*nsitem).p_rtindex;
        (*pstate).p_joinlist = lappend((*pstate).p_joinlist, rtr as *mut c_void);
    }
    if addToRelNameSpace || addToVarNameSpace {
        /* Set the new nsitem's visibility flags correctly */
        (*nsitem).p_rel_visible = addToRelNameSpace;
        (*nsitem).p_cols_visible = addToVarNameSpace;
        (*nsitem).p_lateral_only = false;
        (*nsitem).p_lateral_ok = true;
        (*pstate).p_namespace = lappend((*pstate).p_namespace, nsitem as *mut c_void);
    }
}

/*
 * expandRTE -- expand the columns of a rangetable entry
 *
 * This creates lists of an RTE's column names (aliases if provided, else
 * real names) and Vars for each column.  Only user columns are considered.
 * If include_dropped is false then dropped columns are omitted from the
 * results.  If include_dropped is true then empty strings and NULL constants
 * (not Vars!) are returned for dropped columns.
 *
 * rtindex, sublevels_up, returning_type, and location are the varno,
 * varlevelsup, varreturningtype, and location values to use in the created
 * Vars.  Ordinarily rtindex should match the actual position of the RTE in
 * its rangetable.
 *
 * The output lists go into *colnames and *colvars.
 * If only one of the two kinds of output list is needed, pass NULL for the
 * output pointer for the unwanted one.
 */
pub unsafe fn expandRTE(
    rte: *mut RangeTblEntry,
    rtindex: c_int,
    sublevels_up: c_int,
    returning_type: VarReturningType,
    location: c_int,
    include_dropped: bool,
    colnames: *mut *mut List,
    colvars: *mut *mut List,
) {
    let mut varattno: c_int;

    if !colnames.is_null() {
        *colnames = NIL;
    }
    if !colvars.is_null() {
        *colvars = NIL;
    }

    #[allow(unreachable_patterns)]
    match (*rte).rtekind {
        RTE_RELATION => {
            /* Ordinary relation RTE */
            expandRelation(
                (*rte).relid,
                (*rte).eref,
                rtindex,
                sublevels_up,
                returning_type,
                location,
                include_dropped,
                colnames,
                colvars,
            );
        }
        RTE_SUBQUERY => {
            /* Subquery RTE */
            let mut aliasp_item = list_head((*(*rte).eref).colnames);
            let mut tlistitem: *mut ListCell;

            varattno = 0;
            foreach!(tlistitem, (*(*rte).subquery).targetList, {
                let te = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(tlistitem));

                if (*te).resjunk {
                    continue;
                }
                varattno += 1;
                Assert!(varattno == (*te).resno as c_int);

                /*
                 * Formerly it was possible for the subquery tlist to have
                 * more non-junk entries than the colnames list does (if
                 * this RTE has been expanded from a view that has more
                 * columns than it did when the current query was parsed).
                 * Now that ApplyRetrieveRule cleans up such cases, we
                 * shouldn't see that anymore, but let's just check.
                 */
                if aliasp_item.is_null() {
                    elog!(
                        ERROR,
                        "too few column names for subquery {}",
                        cstr_to_str((*(*rte).eref).aliasname)
                    );
                }

                if !colnames.is_null() {
                    let label = strVal!(lfirst_node!(PgString, T_String, aliasp_item));
                    *colnames = lappend(*colnames, makeString(pstrdup(label)) as *mut c_void);
                }

                if !colvars.is_null() {
                    let varnode = makeVar(
                        rtindex,
                        varattno as AttrNumber,
                        exprType((*te).expr as *mut Node),
                        exprTypmod((*te).expr as *mut Node),
                        exprCollation((*te).expr as *mut Node),
                        sublevels_up as u32,
                    );
                    (*varnode).varreturningtype = returning_type;
                    (*varnode).location = location;

                    *colvars = lappend(*colvars, varnode as *mut c_void);
                }

                aliasp_item = lnext((*(*rte).eref).colnames, aliasp_item);
            });
        }
        RTE_FUNCTION => {
            /* Function RTE */
            let mut atts_done: c_int = 0;
            let mut lc: *mut ListCell;

            foreach!(lc, (*rte).functions, {
                let rtfunc = lfirst_node!(RangeTblFunction, T_RangeTblFunction, current_cell!(lc));
                let functypclass: TypeFuncClass;
                let mut funcrettype: Oid = InvalidOid;
                let mut tupdesc: TupleDesc = core::ptr::null_mut();

                /* If it has a coldeflist, it returns RECORD */
                if !(*rtfunc).funccolnames.is_null()
                    && list_length((*rtfunc).funccolnames) > 0
                {
                    functypclass = TYPEFUNC_RECORD;
                } else {
                    functypclass = get_expr_result_type(
                        (*rtfunc).funcexpr as *mut Node,
                        &mut funcrettype,
                        &mut tupdesc,
                    );
                }

                if functypclass == TYPEFUNC_COMPOSITE
                    || functypclass == TYPEFUNC_COMPOSITE_DOMAIN
                {
                    /* Composite data type, e.g. a table's row type */
                    Assert!(!tupdesc.is_null());
                    expandTupleDesc(
                        tupdesc,
                        (*rte).eref,
                        (*rtfunc).funccolcount,
                        atts_done,
                        rtindex,
                        sublevels_up,
                        returning_type,
                        location,
                        include_dropped,
                        colnames,
                        colvars,
                    );
                } else if functypclass == TYPEFUNC_SCALAR {
                    /* Base data type, i.e. scalar */
                    if !colnames.is_null() {
                        *colnames = lappend(
                            *colnames,
                            list_nth((*(*rte).eref).colnames, atts_done),
                        );
                    }

                    if !colvars.is_null() {
                        let varnode = makeVar(
                            rtindex,
                            (atts_done + 1) as AttrNumber,
                            funcrettype,
                            exprTypmod((*rtfunc).funcexpr as *mut Node),
                            exprCollation((*rtfunc).funcexpr as *mut Node),
                            sublevels_up as u32,
                        );
                        (*varnode).varreturningtype = returning_type;
                        (*varnode).location = location;

                        *colvars = lappend(*colvars, varnode as *mut c_void);
                    }
                } else if functypclass == TYPEFUNC_RECORD {
                    if !colnames.is_null() {
                        let mut namelist = list_copy_tail((*(*rte).eref).colnames, atts_done);
                        namelist = list_truncate(namelist, (*rtfunc).funccolcount);
                        *colnames = list_concat(*colnames, namelist);
                    }

                    if !colvars.is_null() {
                        let mut l1 = list_head((*rtfunc).funccoltypes);
                        let mut l2 = list_head((*rtfunc).funccoltypmods);
                        let mut l3 = list_head((*rtfunc).funccolcollations);
                        let mut attnum = atts_done;

                        while !l1.is_null() {
                            let attrtype = crate::nodes::pg_list::lfirst_oid(l1);
                            let attrtypmod = crate::nodes::pg_list::lfirst_int(l2);
                            let attrcollation = crate::nodes::pg_list::lfirst_oid(l3);

                            attnum += 1;
                            let varnode = makeVar(
                                rtindex,
                                attnum as AttrNumber,
                                attrtype,
                                attrtypmod,
                                attrcollation,
                                sublevels_up as u32,
                            );
                            (*varnode).varreturningtype = returning_type;
                            (*varnode).location = location;
                            *colvars = lappend(*colvars, varnode as *mut c_void);

                            l1 = lnext((*rtfunc).funccoltypes, l1);
                            l2 = lnext((*rtfunc).funccoltypmods, l2);
                            l3 = lnext((*rtfunc).funccolcollations, l3);
                        }
                    }
                } else {
                    /* addRangeTableEntryForFunction should've caught this */
                    elog!(ERROR, "function in FROM has unsupported return type");
                }
                atts_done += (*rtfunc).funccolcount;
            });

            /* Append the ordinality column if any */
            if (*rte).funcordinality {
                if !colnames.is_null() {
                    *colnames = lappend(*colnames, llast((*(*rte).eref).colnames));
                }

                if !colvars.is_null() {
                    let varnode = makeVar(
                        rtindex,
                        (atts_done + 1) as AttrNumber,
                        INT8OID,
                        -1,
                        InvalidOid,
                        sublevels_up as u32,
                    );
                    (*varnode).varreturningtype = returning_type;
                    *colvars = lappend(*colvars, varnode as *mut c_void);
                }
            }
        }
        RTE_JOIN => {
            /* Join RTE */
            let mut colname: *mut ListCell;
            let mut aliasvar: *mut ListCell;

            Assert!(
                list_length((*(*rte).eref).colnames)
                    == list_length((*rte).joinaliasvars)
            );

            varattno = 0;
            colname = list_head((*(*rte).eref).colnames);
            aliasvar = list_head((*rte).joinaliasvars);
            while !colname.is_null() {
                let avar = crate::nodes::pg_list::lfirst(aliasvar) as *mut Node;

                varattno += 1;

                /*
                 * During ordinary parsing, there will never be any
                 * deleted columns in the join.  While this function is
                 * also used by the rewriter and planner, they do not
                 * currently call it on any JOIN RTEs.  Therefore, this
                 * next bit is dead code, but it seems prudent to handle
                 * the case correctly anyway.
                 */
                if avar.is_null() {
                    if include_dropped {
                        if !colnames.is_null() {
                            *colnames =
                                lappend(*colnames, makeString(pstrdup(b"\0" as *const u8 as *const c_char)) as *mut c_void);
                        }
                        if !colvars.is_null() {
                            /*
                             * Can't use join's column type here (it might
                             * be dropped!); but it doesn't really matter
                             * what type the Const claims to be.
                             */
                            *colvars =
                                lappend(*colvars, makeNullConst(INT4OID, -1, InvalidOid) as *mut c_void);
                        }
                    }
                    colname = lnext((*(*rte).eref).colnames, colname);
                    aliasvar = lnext((*rte).joinaliasvars, aliasvar);
                    continue;
                }

                if !colnames.is_null() {
                    let label = strVal!(lfirst_node!(PgString, T_String, colname));

                    *colnames = lappend(*colnames, makeString(pstrdup(label)) as *mut c_void);
                }

                if !colvars.is_null() {
                    let varnode: *mut Var;

                    /*
                     * If the joinaliasvars entry is a simple Var, just
                     * copy it (with adjustment of varlevelsup and
                     * location); otherwise it is a JOIN USING column and
                     * we must generate a join alias Var.  This matches
                     * the results that expansion of "join.*" by
                     * expandNSItemVars would have produced, if we had
                     * access to the ParseNamespaceItem for the join.
                     */
                    let varnode: *mut Var = if IsA!(avar, T_Var) {
                        let v = copyObject(avar as *mut Var);
                        (*v).varlevelsup = sublevels_up as crate::parser::parse_node::Index;
                        v
                    } else {
                        makeVar(
                            rtindex,
                            varattno as AttrNumber,
                            exprType(avar),
                            exprTypmod(avar),
                            exprCollation(avar),
                            sublevels_up as u32,
                        )
                    };
                    (*varnode).varreturningtype = returning_type;
                    (*varnode).location = location;

                    *colvars = lappend(*colvars, varnode as *mut c_void);
                }

                colname = lnext((*(*rte).eref).colnames, colname);
                aliasvar = lnext((*rte).joinaliasvars, aliasvar);
            }
        }
        RTE_TABLEFUNC | RTE_VALUES | RTE_CTE | RTE_NAMEDTUPLESTORE => {
            /* Tablefunc, Values, CTE, or ENR RTE */
            let mut aliasp_item = list_head((*(*rte).eref).colnames);
            let mut lct = list_head((*rte).coltypes);
            let mut lcm = list_head((*rte).coltypmods);
            let mut lcc = list_head((*rte).colcollations);

            varattno = 0;
            while !lct.is_null() {
                let coltype = crate::nodes::pg_list::lfirst_oid(lct);
                let coltypmod = crate::nodes::pg_list::lfirst_int(lcm);
                let colcoll = crate::nodes::pg_list::lfirst_oid(lcc);

                varattno += 1;

                if !colnames.is_null() {
                    /* Assume there is one alias per output column */
                    if OidIsValid(coltype) {
                        let label = strVal!(lfirst_node!(PgString, T_String, aliasp_item));

                        *colnames = lappend(*colnames, makeString(pstrdup(label)) as *mut c_void);
                    } else if include_dropped {
                        *colnames = lappend(
                            *colnames,
                            makeString(pstrdup(b"\0" as *const u8 as *const c_char)) as *mut c_void,
                        );
                    }

                    aliasp_item = lnext((*(*rte).eref).colnames, aliasp_item);
                }

                if !colvars.is_null() {
                    if OidIsValid(coltype) {
                        let varnode = makeVar(
                            rtindex,
                            varattno as AttrNumber,
                            coltype,
                            coltypmod,
                            colcoll,
                            sublevels_up as u32,
                        );
                        (*varnode).varreturningtype = returning_type;
                        (*varnode).location = location;

                        *colvars = lappend(*colvars, varnode as *mut c_void);
                    } else if include_dropped {
                        /*
                         * It doesn't really matter what type the Const
                         * claims to be.
                         */
                        *colvars = lappend(
                            *colvars,
                            makeNullConst(INT4OID, -1, InvalidOid) as *mut c_void,
                        );
                    }
                }

                lct = lnext((*rte).coltypes, lct);
                lcm = lnext((*rte).coltypmods, lcm);
                lcc = lnext((*rte).colcollations, lcc);
            }
        }
        RTE_RESULT | RTE_GROUP => {
            /* These expose no columns, so nothing to do */
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized RTE kind: {}",
                (*rte).rtekind as c_int
            );
        }
    }
}

/*
 * expandRelation -- expandRTE subroutine
 */
unsafe fn expandRelation(
    relid: Oid,
    eref: *mut Alias,
    rtindex: c_int,
    sublevels_up: c_int,
    returning_type: VarReturningType,
    location: c_int,
    include_dropped: bool,
    colnames: *mut *mut List,
    colvars: *mut *mut List,
) {
    let rel: Relation;

    /* Get the tupledesc and turn it over to expandTupleDesc */
    let rel = relation_open(relid, AccessShareLock);
    expandTupleDesc(
        (*(rel as *mut RelationData)).rd_att,
        eref,
        (*(*(rel as *mut RelationData)).rd_att).natts,
        0,
        rtindex,
        sublevels_up,
        returning_type,
        location,
        include_dropped,
        colnames,
        colvars,
    );
    relation_close(rel, AccessShareLock);
}

/*
 * expandTupleDesc -- expandRTE subroutine
 *
 * Generate names and/or Vars for the first "count" attributes of the tupdesc,
 * and append them to colnames/colvars.  "offset" is added to the varattno
 * that each Var would otherwise have, and we also skip the first "offset"
 * entries in eref->colnames.  (These provisions allow use of this code for
 * an individual composite-returning function in an RTE_FUNCTION RTE.)
 */
unsafe fn expandTupleDesc(
    tupdesc: TupleDesc,
    eref: *mut Alias,
    count: c_int,
    offset: c_int,
    rtindex: c_int,
    sublevels_up: c_int,
    returning_type: VarReturningType,
    location: c_int,
    include_dropped: bool,
    colnames: *mut *mut List,
    colvars: *mut *mut List,
) {
    let mut aliascell: *mut ListCell;

    aliascell = if offset < list_length((*eref).colnames) {
        list_nth_cell((*eref).colnames, offset)
    } else {
        core::ptr::null_mut()
    };

    Assert!(count <= (*tupdesc).natts);
    for varattno in 0..count {
        let attr = TupleDescAttr(tupdesc, varattno);

        if (*attr).attisdropped {
            if include_dropped {
                if !colnames.is_null() {
                    *colnames = lappend(
                        *colnames,
                        makeString(pstrdup(b"\0" as *const u8 as *const c_char)) as *mut c_void,
                    );
                }
                if !colvars.is_null() {
                    /*
                     * can't use atttypid here, but it doesn't really matter
                     * what type the Const claims to be.
                     */
                    *colvars = lappend(
                        *colvars,
                        makeNullConst(INT4OID, -1, InvalidOid) as *mut c_void,
                    );
                }
            }
            if !aliascell.is_null() {
                aliascell = lnext((*eref).colnames, aliascell);
            }
            continue;
        }

        if !colnames.is_null() {
            let label: *const c_char;

            if !aliascell.is_null() {
                label = strVal!(lfirst_node!(PgString, T_String, aliascell));
                aliascell = lnext((*eref).colnames, aliascell);
            } else {
                /* If we run out of aliases, use the underlying name */
                label = crate::c::NameStr(&(*attr).attname);
            }
            *colnames = lappend(*colnames, makeString(pstrdup(label)) as *mut c_void);
        }

        if !colvars.is_null() {
            let varnode = makeVar(
                rtindex,
                (varattno + offset + 1) as AttrNumber,
                (*attr).atttypid,
                (*attr).atttypmod,
                (*attr).attcollation,
                sublevels_up as u32,
            );
            (*varnode).varreturningtype = returning_type;
            (*varnode).location = location;

            *colvars = lappend(*colvars, varnode as *mut c_void);
        }
    }
}

/*
 * expandNSItemVars
 *    Produce a list of Vars, and optionally a list of column names,
 *    for the non-dropped columns of the nsitem.
 *
 * The emitted Vars are marked with the given sublevels_up and location.
 *
 * If colnames isn't NULL, a list of String items for the columns is stored
 * there; note that it's just a subset of the RTE's eref list, and hence
 * the list elements mustn't be modified.
 */
pub unsafe fn expandNSItemVars(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    sublevels_up: c_int,
    location: c_int,
    colnames: *mut *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut colindex: c_int = 0;
    let mut lc: *mut ListCell;

    if !colnames.is_null() {
        *colnames = NIL;
    }

    foreach!(lc, (*((*nsitem).p_names as *mut Alias)).colnames, {
        let colnameval = lfirst_node!(PgString, T_String, current_cell!(lc));
        let colname = strVal!(colnameval);
        let nscol = &*(*nsitem).p_nscolumns.offset(colindex as isize);

        if nscol.p_dontexpand {
            /* skip */
        } else if *colname != b'\0' as c_char {
            let var: *mut Var;

            Assert!(nscol.p_varno > 0);
            let var = makeVar(
                nscol.p_varno as c_int,
                nscol.p_varattno,
                nscol.p_vartype,
                nscol.p_vartypmod,
                nscol.p_varcollid,
                sublevels_up as u32,
            );
            /* makeVar doesn't offer parameters for these, so set by hand: */
            (*var).varreturningtype =
                core::mem::transmute::<c_int, VarReturningType>(nscol.p_varreturningtype);
            (*var).varnosyn = nscol.p_varnosyn;
            (*var).varattnosyn = nscol.p_varattnosyn;
            (*var).location = location;

            /* ... and update varnullingrels */
            markNullableIfNeeded(pstate, var);

            result = lappend(result, var as *mut c_void);
            if !colnames.is_null() {
                *colnames = lappend(*colnames, colnameval as *mut c_void);
            }
        } else {
            /* dropped column, ignore */
            Assert!(nscol.p_varno == 0);
        }
        colindex += 1;
    });
    result
}

/*
 * expandNSItemAttrs -
 *    Workhorse for "*" expansion: produce a list of targetentries
 *    for the attributes of the nsitem
 *
 * pstate->p_next_resno determines the resnos assigned to the TLEs.
 * The referenced columns are marked as requiring SELECT access, if
 * caller requests that.
 */
pub unsafe fn expandNSItemAttrs(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    sublevels_up: c_int,
    require_col_privs: bool,
    location: c_int,
) -> *mut List {
    let rte = (*nsitem).p_rte as *mut RangeTblEntry;
    let perminfo = (*nsitem).p_perminfo as *mut RTEPermissionInfo;
    let mut names: *mut List = NIL;
    let mut vars: *mut List;
    let mut name: *mut ListCell;
    let mut var: *mut ListCell;
    let mut te_list: *mut List = NIL;

    let vars = expandNSItemVars(pstate, nsitem, sublevels_up, location, &mut names);

    /*
     * Require read access to the table.  This is normally redundant with the
     * markVarForSelectPriv calls below, but not if the table has zero
     * columns.  We need not do anything if the nsitem is for a join: its
     * component tables will have been marked ACL_SELECT when they were added
     * to the rangetable.  (This step changes things only for the target
     * relation of UPDATE/DELETE, which cannot be under a join.)
     */
    if (*rte).rtekind == RTE_RELATION {
        Assert!(!perminfo.is_null());
        (*perminfo).requiredPerms |= ACL_SELECT;
    }

    name = list_head(names);
    var = list_head(vars);
    while !name.is_null() {
        let label = strVal!(lfirst_node!(PgString, T_String, name));
        let varnode = crate::nodes::pg_list::lfirst(var) as *mut Var;
        let te: *mut TargetEntry;

        let te = makeTargetEntry(
            varnode as *mut Expr,
            (*pstate).p_next_resno as AttrNumber,
            label,
            false,
        );
        (*pstate).p_next_resno += 1;
        te_list = lappend(te_list, te as *mut c_void);

        if require_col_privs {
            /* Require read access to each column */
            markVarForSelectPriv(pstate, varnode);
        }

        name = lnext(names, name);
        var = lnext(vars, var);
    }

    Assert!(name.is_null() && var.is_null()); /* lists not the same length? */

    te_list
}

/*
 * get_rte_attribute_name
 *        Get an attribute name from a RangeTblEntry
 *
 * This is unlike get_attname() because we use aliases if available.
 * In particular, it will work on an RTE for a subselect or join, whereas
 * get_attname() only works on real relations.
 *
 * "*" is returned if the given attnum is InvalidAttrNumber --- this case
 * occurs when a Var represents a whole tuple of a relation.
 *
 * It is caller's responsibility to not call this on a dropped attribute.
 * (You will get some answer for such cases, but it might not be sensible.)
 */
pub unsafe fn get_rte_attribute_name(
    rte: *mut RangeTblEntry,
    attnum: AttrNumber,
) -> *mut c_char {
    if attnum == InvalidAttrNumber {
        return b"*\0" as *const u8 as *mut c_char;
    }

    /*
     * If there is a user-written column alias, use it.
     */
    if !((*rte).alias as *const c_void).is_null()
        && attnum > 0
        && (attnum as c_int) <= list_length((*(*rte).alias).colnames)
    {
        return strVal!(lfirst_node!(
            PgString, T_String,
            list_nth_cell((*(*rte).alias).colnames, (attnum - 1) as c_int)
        ));
    }

    /*
     * If the RTE is a relation, go to the system catalogs not the
     * eref->colnames list.  This is a little slower but it will give the
     * right answer if the column has been renamed since the eref list was
     * built (which can easily happen for rules).
     */
    if (*rte).rtekind == RTE_RELATION {
        return get_attname((*rte).relid, attnum, false);
    }

    /*
     * Otherwise use the column name from eref.  There should always be one.
     */
    if attnum > 0 && (attnum as c_int) <= list_length((*(*rte).eref).colnames) {
        return strVal!(lfirst_node!(
            PgString, T_String,
            list_nth_cell((*(*rte).eref).colnames, (attnum - 1) as c_int)
        ));
    }

    /* else caller gave us a bogus attnum */
    elog!(
        ERROR,
        "invalid attnum {} for rangetable entry {}",
        attnum,
        cstr_to_str((*(*rte).eref).aliasname)
    );
    core::ptr::null_mut() /* keep compiler quiet */
}

/*
 * get_rte_attribute_is_dropped
 *        Check whether attempted attribute ref is to a dropped column
 */
pub unsafe fn get_rte_attribute_is_dropped(
    rte: *mut RangeTblEntry,
    attnum: AttrNumber,
) -> bool {
    let result: bool;

    #[allow(unreachable_patterns)]
    match (*rte).rtekind {
        RTE_RELATION => {
            /*
             * Plain relation RTE --- get the attribute's catalog entry
             */
            let tp: HeapTuple;
            let att_tup: Form_pg_attribute;

            let tp = SearchSysCache2(
                ATTNUM,
                ObjectIdGetDatum((*rte).relid),
                Int16GetDatum(attnum),
            );
            if !HeapTupleIsValid(tp) {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "cache lookup failed for attribute {} of relation {}",
                    attnum,
                    (*rte).relid
                );
            }
            let att_tup = GETSTRUCT(tp) as Form_pg_attribute;
            let result = (*att_tup).attisdropped;
            ReleaseSysCache(tp);
            return result;
        }
        RTE_SUBQUERY | RTE_TABLEFUNC | RTE_VALUES | RTE_CTE | RTE_GROUP => {
            /*
             * Subselect, Table Functions, Values, CTE, GROUP RTEs never have
             * dropped columns
             */
            return false;
        }
        RTE_NAMEDTUPLESTORE => {
            /* Check dropped-ness by testing for valid coltype */
            if (attnum as c_int) <= 0 || (attnum as c_int) > list_length((*rte).coltypes) {
                elog!(ERROR, "invalid varattno {}", attnum);
            }
            return !OidIsValid(list_nth_oid((*rte).coltypes, (attnum - 1) as c_int));
        }
        RTE_JOIN => {
            /*
             * A join RTE would not have dropped columns when constructed,
             * but one in a stored rule might contain columns that were
             * dropped from the underlying tables, if said columns are
             * nowhere explicitly referenced in the rule.  This will be
             * signaled to us by a null pointer in the joinaliasvars list.
             */
            let aliasvar: *mut Var;

            if (attnum as c_int) <= 0
                || (attnum as c_int) > list_length((*rte).joinaliasvars)
            {
                elog!(ERROR, "invalid varattno {}", attnum);
            }
            let aliasvar = list_nth((*rte).joinaliasvars, (attnum - 1) as c_int) as *mut Var;

            return aliasvar.is_null();
        }
        RTE_FUNCTION => {
            /* Function RTE */
            let mut lc: *mut ListCell;
            let mut atts_done: c_int = 0;

            /*
             * Dropped attributes are only possible with functions that
             * return named composite types.  In such a case we have to
             * look up the result type to see if it currently has this
             * column dropped.  So first, loop over the funcs until we
             * find the one that covers the requested column.
             */
            foreach!(lc, (*rte).functions, {
                let rtfunc = lfirst_node!(RangeTblFunction, T_RangeTblFunction, current_cell!(lc));

                if (attnum as c_int) > atts_done
                    && (attnum as c_int) <= atts_done + (*rtfunc).funccolcount
                {
                    let tupdesc: TupleDesc;

                    /* If it has a coldeflist, it returns RECORD */
                    if !(*rtfunc).funccolnames.is_null()
                        && list_length((*rtfunc).funccolnames) > 0
                    {
                        return false; /* can't have any dropped columns */
                    }

                    let tupdesc = get_expr_result_tupdesc((*rtfunc).funcexpr as *mut Node, true);
                    if !tupdesc.is_null() {
                        /* Composite data type, e.g. a table's row type */
                        let att_tup: Form_pg_attribute;

                        Assert!(!tupdesc.is_null());
                        Assert!((attnum as c_int) - atts_done <= (*tupdesc).natts);
                        let att_tup = TupleDescAttr(tupdesc, (attnum as c_int) - atts_done - 1);
                        return (*att_tup).attisdropped;
                    }
                    /* Otherwise, it can't have any dropped columns */
                    return false;
                }
                atts_done += (*rtfunc).funccolcount;
            });

            /* If we get here, must be looking for the ordinality column */
            if (*rte).funcordinality && (attnum as c_int) == atts_done + 1 {
                return false;
            }

            /* this probably can't happen ... */
            ereport!(ERROR, errmsg!(
                    "column {} of relation \"{}\" does not exist",
                    attnum,
                    cstr_to_str((*(*rte).eref).aliasname)
                )) /* C also: errcode */;
            return false; /* keep compiler quiet */
        }
        RTE_RESULT => {
            /* this probably can't happen ... */
            ereport!(ERROR, errmsg!(
                    "column {} of relation \"{}\" does not exist",
                    attnum,
                    cstr_to_str((*(*rte).eref).aliasname)
                )) /* C also: errcode */;
            return false; /* keep compiler quiet */
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized RTE kind: {}",
                (*rte).rtekind as c_int
            );
            return false; /* keep compiler quiet */
        }
    }
}

/*
 * Given a targetlist and a resno, return the matching TargetEntry
 *
 * Returns NULL if resno is not present in list.
 *
 * Note: we need to search, rather than just indexing with list_nth(),
 * because not all tlists are sorted by resno.
 */
pub unsafe fn get_tle_by_resno(tlist: *mut List, resno: AttrNumber) -> *mut TargetEntry {
    let mut l: *mut ListCell;

    foreach!(l, tlist, {
        let tle = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(l));

        if (*tle).resno == resno {
            return tle;
        }
    });
    core::ptr::null_mut()
}

/*
 * Given a Query and rangetable index, return relation's RowMarkClause if any
 *
 * Returns NULL if relation is not selected FOR UPDATE/SHARE
 */
pub unsafe fn get_parse_rowmark(qry: *mut Query, rtindex: Index) -> *mut RowMarkClause {
    let mut l: *mut ListCell;

    foreach!(l, (*qry).rowMarks, {
        let rc = lfirst_node!(RowMarkClause, T_RowMarkClause, current_cell!(l));

        if (*rc).rti == rtindex {
            return rc;
        }
    });
    core::ptr::null_mut()
}

/*
 *    given relation and att name, return attnum of variable
 *
 *    Returns InvalidAttrNumber if the attr doesn't exist (or is dropped).
 *
 *    This should only be used if the relation is already
 *    table_open()'ed.  Use the cache version get_attnum()
 *    for access to non-opened relations.
 */
pub unsafe fn attnameAttNum(rd: Relation, attname: *const c_char, sysColOK: bool) -> c_int {
    let rddata = rd as *mut RelationData;

    for i in 0..RelationGetNumberOfAttributes(rd as *mut RelationData) {
        let att = TupleDescAttr((*rddata).rd_att, i);

        if crate::utils::builtins::namestrcmp(&mut (*att).attname, attname) == 0
            && !(*att).attisdropped
        {
            return i + 1;
        }
    }

    if sysColOK {
        let i = specialAttNum(attname);
        if i != InvalidAttrNumber as c_int {
            return i;
        }
    }

    /* on failure */
    InvalidAttrNumber as c_int
}

/* specialAttNum()
 *
 * Check attribute name to see if it is "special", e.g. "xmin".
 * - thomas 2000-02-07
 *
 * Note: this only discovers whether the name could be a system attribute.
 * Caller needs to ensure that it really is an attribute of the rel.
 */
unsafe fn specialAttNum(attname: *const c_char) -> c_int {
    let sysatt = SystemAttributeByName(attname);
    if !sysatt.is_null() {
        return (*sysatt).attnum as c_int;
    }
    InvalidAttrNumber as c_int
}


/*
 * given attribute id, return name of that attribute
 *
 *    This should only be used if the relation is already
 *    table_open()'ed.  Use the cache version get_atttype()
 *    for access to non-opened relations.
 */
pub unsafe fn attnumAttName(rd: Relation, attid: c_int) -> *const crate::c::NameData {
    let rddata = rd as *mut RelationData;

    if attid <= 0 {
        let sysatt = SystemAttributeDefinition(attid as AttrNumber);
        return &(*sysatt).attname;
    }
    if attid > (*(*rddata).rd_att).natts {
        elog!(ERROR, "invalid attribute number {}", attid);
    }
    &(*TupleDescAttr((*rddata).rd_att, attid - 1)).attname
}

/*
 * given attribute id, return type of that attribute
 *
 *    This should only be used if the relation is already
 *    table_open()'ed.  Use the cache version get_atttype()
 *    for access to non-opened relations.
 */
pub unsafe fn attnumTypeId(rd: Relation, attid: c_int) -> Oid {
    let rddata = rd as *mut RelationData;

    if attid <= 0 {
        let sysatt = SystemAttributeDefinition(attid as AttrNumber);
        return (*sysatt).atttypid;
    }
    if attid > (*(*rddata).rd_att).natts {
        elog!(ERROR, "invalid attribute number {}", attid);
    }
    (*TupleDescAttr((*rddata).rd_att, attid - 1)).atttypid
}

/*
 * given attribute id, return collation of that attribute
 *
 *    This should only be used if the relation is already table_open()'ed.
 */
pub unsafe fn attnumCollationId(rd: Relation, attid: c_int) -> Oid {
    let rddata = rd as *mut RelationData;

    if attid <= 0 {
        /* All system attributes are of noncollatable types. */
        return InvalidOid;
    }
    if attid > (*(*rddata).rd_att).natts {
        elog!(ERROR, "invalid attribute number {}", attid);
    }
    (*TupleDescAttr((*rddata).rd_att, attid - 1)).attcollation
}

/*
 * Generate a suitable error about a missing RTE.
 *
 * Since this is a very common type of error, we work rather hard to
 * produce a helpful message.
 */
pub unsafe fn errorMissingRTE(pstate: *mut ParseState, relation: *mut RangeVar) {
    let rte: *mut RangeTblEntry;
    let mut badAlias: *const c_char = core::ptr::null();

    /*
     * Check to see if there are any potential matches in the query's
     * rangetable.  (Note: cases involving a bad schema name in the RangeVar
     * will throw error immediately here.  That seems OK.)
     */
    let rte = searchRangeTableForRel(pstate, relation);

    /*
     * If we found a match that has an alias and the alias is visible in the
     * namespace, then the problem is probably use of the relation's real name
     * instead of its alias, ie "SELECT foo.* FROM foo f". This mistake is
     * common enough to justify a specific hint.
     *
     * If we found a match that doesn't meet those criteria, assume the
     * problem is illegal use of a relation outside its scope, as in the
     * MySQL-ism "SELECT ... FROM a, b LEFT JOIN c ON (a.x = c.y)".
     */
    if !rte.is_null()
        && !((*rte).alias as *const c_void).is_null()
        && strcmp((*(*rte).eref).aliasname, (*relation).relname) != 0
    {
        let nsitem: *mut ParseNamespaceItem;
        let mut sublevels_up: c_int = 0;

        let nsitem = refnameNamespaceItem(
            pstate,
            core::ptr::null(),
            (*(*rte).eref).aliasname,
            (*relation).location,
            &mut sublevels_up,
        );
        if !nsitem.is_null() && (*nsitem).p_rte as *mut RangeTblEntry == rte {
            badAlias = (*(*rte).eref).aliasname;
        }
    }

    /* If it looks like the user forgot to use an alias, hint about that */
    if !badAlias.is_null() {
        ereport!(ERROR, errmsg!(
                "invalid reference to FROM-clause entry for table \"{}\"",
                cstr_to_str((*relation).relname)
            )) /* C also: errcode, errhint, parser_errposition */;
    /* Hint about case where we found an (inaccessible) exact match */
    } else if !rte.is_null() {
        ereport!(ERROR, errmsg!(
                "invalid reference to FROM-clause entry for table \"{}\"",
                cstr_to_str((*relation).relname)
            )) /* C also: errcode, errdetail, if rte_visible_if_lateral, parser_errposition */;
    /* Else, we have nothing to offer but the bald statement of error */
    } else {
        ereport!(ERROR, errmsg!(
                "missing FROM-clause entry for table \"{}\"",
                cstr_to_str((*relation).relname)
            )) /* C also: errcode, parser_errposition */;
    }
}

/*
 * Generate a suitable error about a missing column.
 *
 * Since this is a very common type of error, we work rather hard to
 * produce a helpful message.
 */
pub unsafe fn errorMissingColumn(
    pstate: *mut ParseState,
    relname: *const c_char,
    colname: *const c_char,
    location: c_int,
) {
    let state: *mut FuzzyAttrMatchState;

    /*
     * Search the entire rtable looking for possible matches.  If we find one,
     * emit a hint about it.
     */
    let state = searchRangeTableForCol(pstate, relname, colname, location);

    /*
     * If there are exact match(es), they must be inaccessible for some
     * reason.
     */
    if !(*state).rexact1.is_null() {
        /*
         * We don't try too hard when there's multiple inaccessible exact
         * matches, but at least be sure that we don't misleadingly suggest
         * that there's only one.
         */
        if !(*state).rexact2.is_null() {
            ereport!(
                ERROR,
                if !relname.is_null() {
                    errmsg!("column {}.{} does not exist", cstr_to_str(relname), cstr_to_str(colname))
                } else {
                    errmsg!("column \"{}\" does not exist", cstr_to_str(colname))
                }
                /* C also: errcode, parser_errposition */
            );
        }
        /* Single exact match, so try to determine why it's inaccessible. */
        ereport!(
            ERROR,
            if !relname.is_null() {
                errmsg!("column {}.{} does not exist", cstr_to_str(relname), cstr_to_str(colname))
            } else {
                errmsg!("column \"{}\" does not exist", cstr_to_str(colname))
            }
            /* C also: errcode, parser_errposition */
        );
    }

    if (*state).rsecond.is_null() {
        /* If we found no match at all, we have little to report */
        if (*state).rfirst.is_null() {
            ereport!(
                ERROR,
                if !relname.is_null() {
                    errmsg!("column {}.{} does not exist", cstr_to_str(relname), cstr_to_str(colname))
                } else {
                    errmsg!("column \"{}\" does not exist", cstr_to_str(colname))
                }
                /* C also: errcode, parser_errposition */
            );
        }
        /* Handle case where we have a single alternative spelling to offer */
        ereport!(
            ERROR,
            if !relname.is_null() {
                errmsg!("column {}.{} does not exist", cstr_to_str(relname), cstr_to_str(colname))
            } else {
                errmsg!("column \"{}\" does not exist", cstr_to_str(colname))
            }
            /* C also: errcode, parser_errposition */
        );
    } else {
        /* Handle case where there are two equally useful column hints */
        ereport!(
            ERROR,
            if !relname.is_null() {
                errmsg!("column {}.{} does not exist", cstr_to_str(relname), cstr_to_str(colname))
            } else {
                errmsg!("column \"{}\" does not exist", cstr_to_str(colname))
            }
            /* C also: errcode, parser_errposition */
        );
    }
}

/*
 * Find ParseNamespaceItem for RTE, if it's visible at all.
 * We assume an RTE couldn't appear more than once in the namespace lists.
 */
unsafe fn findNSItemForRTE(
    pstate: *mut ParseState,
    rte: *mut RangeTblEntry,
) -> *mut ParseNamespaceItem {
    let mut pstate = pstate;
    while !pstate.is_null() {
        let mut l: *mut ListCell;

        foreach!(l, (*pstate).p_namespace, {
            let nsitem = crate::nodes::pg_list::lfirst(current_cell!(l)) as *mut ParseNamespaceItem;

            if (*nsitem).p_rte as *mut RangeTblEntry == rte {
                return nsitem;
            }
        });
        pstate = (*pstate).parentParseState;
    }
    core::ptr::null_mut()
}

/*
 * Would this RTE be visible, if only the user had written LATERAL?
 *
 * This is a helper for deciding whether to issue a HINT about LATERAL.
 * As such, it doesn't need to be 100% accurate; the HINT could be useful
 * even if it's not quite right.  Hence, we don't delve into fine points
 * about whether a found nsitem has the appropriate one of p_rel_visible or
 * p_cols_visible set.
 */
unsafe fn rte_visible_if_lateral(pstate: *mut ParseState, rte: *mut RangeTblEntry) -> bool {
    let nsitem: *mut ParseNamespaceItem;

    /* If LATERAL *is* active, we're clearly barking up the wrong tree */
    if (*pstate).p_lateral_active {
        return false;
    }
    let nsitem = findNSItemForRTE(pstate, rte);
    if !nsitem.is_null() {
        /* Found it, report whether it's LATERAL-only */
        return (*nsitem).p_lateral_only && (*nsitem).p_lateral_ok;
    }
    false
}

/*
 * Would columns in this RTE be visible if qualified?
 */
unsafe fn rte_visible_if_qualified(pstate: *mut ParseState, rte: *mut RangeTblEntry) -> bool {
    let nsitem = findNSItemForRTE(pstate, rte);

    if !nsitem.is_null() {
        /* Found it, report whether it's relation-only */
        return (*nsitem).p_rel_visible && !(*nsitem).p_cols_visible;
    }
    false
}


/*
 * Examine a fully-parsed query, and return true iff any relation underlying
 * the query is a temporary relation (table, view, or materialized view).
 */
pub unsafe fn isQueryUsingTempRelation(query: *mut Query) -> bool {
    isQueryUsingTempRelation_walker(query as *mut Node, core::ptr::null_mut())
}

unsafe fn isQueryUsingTempRelation_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    if IsA!(node, T_Query) {
        let query = node as *mut Query;
        let mut rtable: *mut ListCell;

        foreach!(rtable, (*query).rtable, {
            let rte = crate::nodes::pg_list::lfirst(current_cell!(rtable)) as *mut RangeTblEntry;

            if (*rte).rtekind == RTE_RELATION {
                let rel = table_open((*rte).relid, AccessShareLock);
                let relpersistence = (*(*(rel as *mut RelationData)).rd_rel).relpersistence;

                table_close(rel as *mut RelationData, AccessShareLock);
                if relpersistence == RELPERSISTENCE_TEMP {
                    return true;
                }
            }
        });

        return query_tree_walker(
            query,
            Some(isQueryUsingTempRelation_walker),
            context,
            QTW_IGNORE_JOINALIASES,
        );
    }

    expression_tree_walker(node, Some(isQueryUsingTempRelation_walker), context)
}

/*
 * addRTEPermissionInfo
 *        Creates RTEPermissionInfo for a given RTE and adds it into the
 *        provided list.
 *
 * Returns the RTEPermissionInfo and sets rte->perminfoindex.
 */
pub unsafe fn addRTEPermissionInfo(
    rteperminfos: *mut *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    let perminfo: *mut RTEPermissionInfo;

    Assert!(OidIsValid((*rte).relid));
    Assert!((*rte).perminfoindex == 0);

    /* Nope, so make one and add to the list. */
    let perminfo = makeNode!(RTEPermissionInfo, T_RTEPermissionInfo) as *mut RTEPermissionInfo;
    (*perminfo).relid = (*rte).relid;
    (*perminfo).inh = (*rte).inh;
    /* Other information is set by fetching the node as and where needed. */

    *rteperminfos = lappend(*rteperminfos, perminfo as *mut c_void);

    /* Note its index (1-based!) */
    (*rte).perminfoindex = list_length(*rteperminfos) as u32;

    perminfo
}

/*
 * getRTEPermissionInfo
 *        Find RTEPermissionInfo for a given relation in the provided list.
 *
 * This is a simple list_nth() operation, though it's good to have the
 * function for the various sanity checks.
 */
pub unsafe fn getRTEPermissionInfo(
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    let perminfo: *mut RTEPermissionInfo;

    if (*rte).perminfoindex == 0
        || ((*rte).perminfoindex as c_int) > list_length(rteperminfos)
    {
        elog!(
            ERROR,
            "invalid perminfoindex {} in RTE with relid {}",
            (*rte).perminfoindex,
            (*rte).relid
        );
    }
    let perminfo = list_nth_node_rteperminfo(rteperminfos, (*rte).perminfoindex as c_int - 1);
    if (*perminfo).relid != (*rte).relid {
        elog!(
            ERROR,
            "permission info at index {} (with relid={}) does not match provided RTE (with relid={})",
            (*rte).perminfoindex,
            (*perminfo).relid,
            (*rte).relid
        );
    }

    perminfo
}
