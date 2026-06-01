//! src/backend/rewrite/rewriteDefine.c
//!   routines for defining a rewrite rule
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::{foreach, current_cell, castNode, IsA, lfirst_node, linitial_node};

// List helpers (lfirst/list_length are fns; NIL is a const) from pg_list.
use crate::nodes::pg_list::{lfirst, list_length, NIL, List};

// NodeTag variants used by IsA!/castNode!/lfirst_node!/linitial_node! call sites.
use crate::nodes::nodes::Node;

// ---------------------------------------------------------------------------
// rewriteDefine.h
// ---------------------------------------------------------------------------

pub const RULE_FIRES_ON_ORIGIN: c_char = b'O' as c_char;
pub const RULE_FIRES_ALWAYS: c_char = b'A' as c_char;
pub const RULE_FIRES_ON_REPLICA: c_char = b'R' as c_char;
pub const RULE_DISABLED: c_char = b'D' as c_char;

// ---------------------------------------------------------------------------
// Stub types (faithful pointers; concrete defs live in their own modules)
// ---------------------------------------------------------------------------

type Relation = *mut crate::utils::rel::RelationData;
type HeapTuple = *mut crate::access::htup_details::HeapTupleData;
type TupleDesc = *mut crate::access::common::tupdesc::TupleDescData;
type Query = crate::nodes::parsenodes::Query;
type RuleStmt = crate::nodes::parsenodes::RuleStmt;
type RangeVar = crate::nodes::primnodes::RangeVar;
type TargetEntry = crate::nodes::primnodes::TargetEntry;
type RangeTblEntry = crate::nodes::parsenodes::RangeTblEntry;
type RTEPermissionInfo = crate::nodes::parsenodes::RTEPermissionInfo;
type CommonTableExpr = crate::nodes::parsenodes::CommonTableExpr;
use crate::rewrite::prs2lock::{RewriteRule, RuleLock};
type CmdType = crate::nodes::nodes::CmdType;
type Form_pg_rewrite = *mut crate::catalog::pg_rewrite::FormData_pg_rewrite;
type Form_pg_attribute = *mut crate::catalog::pg_attribute::FormData_pg_attribute;
type Form_pg_class = *mut crate::catalog::pg_class::FormData_pg_class;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type NameData = crate::c::NameData;
use crate::access::attnum::AttrNumber;

// ---------------------------------------------------------------------------
// Local stubs for unported helpers
// ---------------------------------------------------------------------------

unsafe fn nodeToString(obj: *const Node) -> *mut c_char {
    unimplemented!() // TODO: nodes/outfuncs.c
}
unsafe fn namestrcpy(name: *mut NameData, s: *const c_char) -> c_int {
    unimplemented!() // TODO: common/string.c
}
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn table_close(relation: Relation, lockmode: c_int) {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn relation_open(relationId: Oid, lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/common/relation.c
}
unsafe fn relation_close(relation: Relation, lockmode: c_int) {
    unimplemented!() // TODO: access/common/relation.c
}
unsafe fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCacheCopy2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn heap_form_tuple(
    tupleDescriptor: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn heap_freetuple(htup: HeapTuple) {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *mut ItemPointerData, tup: HeapTuple) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: AttrNumber) -> Oid {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn get_rel_name(relid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_rel_relkind(relid: Oid) -> c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn deleteDependencyRecordsFor(classId: Oid, objectId: Oid, skipExtensionDeps: bool) -> c_long {
    unimplemented!() // TODO: catalog/pg_depend.c
}
unsafe fn recordDependencyOn(
    depender: *const ObjectAddress,
    referenced: *const ObjectAddress,
    behavior: c_int,
) {
    unimplemented!() // TODO: catalog/pg_depend.c
}
unsafe fn recordDependencyOnExpr(
    depender: *const ObjectAddress,
    expr: *const Node,
    rtable: *mut List,
    behavior: c_int,
) {
    unimplemented!() // TODO: rewrite/rewriteHandler.c (catalog/dependency.c)
}
unsafe fn getInsertSelectQuery(parsetree: *mut Query, subquery_ptr: *mut *mut *mut Query) -> *mut Query {
    unimplemented!() // TODO: rewrite/rewriteManip.c
}
unsafe fn transformRuleStmt(
    stmt: *mut RuleStmt,
    queryString: *const c_char,
    actions: *mut *mut List,
    whereClause: *mut *mut Node,
) {
    unimplemented!() // TODO: parser/parse_utilcmd.c
}
unsafe fn RangeVarGetRelid(relation: *const RangeVar, lockmode: c_int, missing_ok: bool) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn RangeVarGetRelidExtended(
    relation: *const RangeVar,
    lockmode: c_int,
    flags: u32,
    callback: RangeVarGetRelidCallback,
    callback_arg: *mut c_void,
) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
type RangeVarGetRelidCallback =
    Option<unsafe extern "C" fn(*const RangeVar, Oid, Oid, *mut c_void)>;
unsafe fn IsSystemRelation(relation: Relation) -> bool {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn IsSystemClass(relid: Oid, reltuple: Form_pg_class) -> bool {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn aclcheck_error(aclerr: c_int, objtype: c_int, objectname: *const c_char) {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn get_relkind_objtype(relkind: c_char) -> c_int {
    unimplemented!() // TODO: catalog/objectaddress.c
}
unsafe fn GetUserId() -> Oid {
    unimplemented!() // TODO: utils/init/miscinit.c
}
unsafe fn errdetail_relkind_not_supported(relkind: c_char) -> c_int {
    unimplemented!() // TODO: catalog/pg_class.c
}
unsafe fn SetRelationRuleStatus(relationId: Oid, relHasRules: bool) {
    unimplemented!() // TODO: rewrite/rewriteSupport.c
}
unsafe fn IsDefinedRewriteRule(owningRel: Oid, ruleName: *const c_char) -> bool {
    unimplemented!() // TODO: rewrite/rewriteSupport.c
}
unsafe fn CacheInvalidateRelcache(relation: Relation) {
    unimplemented!() // TODO: utils/cache/inval.c
}
unsafe fn exprType(expr: *const Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn exprTypmod(expr: *const Node) -> i32 {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
) -> bool {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn query_tree_walker(
    query: *mut Query,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/format_type.c
}
unsafe fn format_type_with_typemod(type_oid: Oid, typemod: i32) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/format_type.c
}
unsafe fn pstrdup(string: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}

// Datum conversion helpers not in prelude
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/builtins.h (cstring_to_text)
}
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    PointerGetDatum(name as *const c_void)
}
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    (*relation).rd_att as *mut _
}
unsafe fn RelationGetRelid(relation: Relation) -> Oid {
    (*relation).rd_id
}
unsafe fn RelationGetRelationName(relation: Relation) -> *mut c_char {
    NameStr(&mut (*(*relation).rd_rel).relname)
}
unsafe fn NameStr(name: *mut NameData) -> *mut c_char {
    (*name).data.as_mut_ptr()
}
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc, i) as *mut _
}
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO: access/htup_details.h
}
unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) {
    unimplemented!() // TODO: catalog/objectaccess.h
}
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {
    unimplemented!() // TODO: catalog/objectaccess.h
}
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

type ItemPointerData = crate::storage::itemptr::ItemPointerData;

// Lock modes
use crate::storage::lockdefs::{AccessExclusiveLock, RowExclusiveLock, NoLock};

// Catalog OIDs (from catalog_oids.rs, generated from pg_rewrite.h / pg_class.h)
use crate::catalog::catalog_oids::{RewriteRelationId, RelationRelationId};

// GUC: allow modification of system tables (miscadmin.h)
use crate::miscadmin::allowSystemTableMods;

// NAMEDATALEN (pg_config_manual.h)
use crate::pg_config_manual::NAMEDATALEN;

// Attribute numbers / column count (from pg_rewrite.h; not yet generated)
const Natts_pg_rewrite: c_int = 9;
const Anum_pg_rewrite_oid: c_int = 1;
const Anum_pg_rewrite_rulename: c_int = 2;
const Anum_pg_rewrite_ev_class: c_int = 3;
const Anum_pg_rewrite_ev_type: c_int = 4;
const Anum_pg_rewrite_ev_enabled: c_int = 5;
const Anum_pg_rewrite_is_instead: c_int = 6;
const Anum_pg_rewrite_ev_qual: c_int = 7;
const Anum_pg_rewrite_ev_action: c_int = 8;
const RewriteOidIndexId: Oid = 2692;

// relkind chars
use crate::catalog::pg_class::{
    RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_VIEW, RELKIND_PARTITIONED_TABLE,
};

// CmdType variants
use crate::nodes::nodes::CmdType::{CMD_SELECT};

// PRS2 varnos
use crate::nodes::primnodes::{PRS2_OLD_VARNO, PRS2_NEW_VARNO};

// dependency behaviors (catalog/dependency.h: DependencyType chars)
const DEPENDENCY_NORMAL: c_int = b'n' as c_int;
const DEPENDENCY_AUTO: c_int = b'a' as c_int;
const DEPENDENCY_INTERNAL: c_int = b'i' as c_int;

// aclcheck error codes / objtype (utils/acl.h: AclResult)
const ACLCHECK_NOT_OWNER: c_int = 2;

// syscache ids (utils/syscache.h placeholders)
const RULERELNAME: c_int = 0;
const RELOID: c_int = 1;

// rtekind
use crate::nodes::parsenodes::RTEKind::RTE_SUBQUERY;

// query_tree_walker flag
const QTW_IGNORE_RC_SUBQUERIES: c_int = 0x02;

// ViewSelectRuleName
const ViewSelectRuleName: *const c_char = c"_RETURN".as_ptr();

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
}

// ---------------------------------------------------------------------------
// InsertRule -
//   takes the arguments and inserts them as a row into the system
//   relation "pg_rewrite"
// ---------------------------------------------------------------------------
unsafe fn InsertRule(
    rulname: *const c_char,
    evtype: c_int,
    eventrel_oid: Oid,
    evinstead: bool,
    event_qual: *mut Node,
    action: *mut List,
    replace: bool,
) -> Oid {
    let evqual: *mut c_char = nodeToString(event_qual);
    let actiontree: *mut c_char = nodeToString(action as *mut Node);
    let mut values: [Datum; Natts_pg_rewrite as usize] = [0; Natts_pg_rewrite as usize];
    let mut nulls: [bool; Natts_pg_rewrite as usize] = [false; Natts_pg_rewrite as usize];
    let mut rname: NameData = core::mem::zeroed();
    let pg_rewrite_desc: Relation;
    let tup: HeapTuple;
    let oldtup: HeapTuple;
    let rewriteObjectId: Oid;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let mut is_update: bool = false;

    /*
     * Set up *nulls and *values arrays
     */
    namestrcpy(&mut rname, rulname);
    values[(Anum_pg_rewrite_rulename - 1) as usize] = NameGetDatum(&rname);
    values[(Anum_pg_rewrite_ev_class - 1) as usize] = ObjectIdGetDatum(eventrel_oid);
    values[(Anum_pg_rewrite_ev_type - 1) as usize] = CharGetDatum((evtype + '0' as c_int) as c_char);
    values[(Anum_pg_rewrite_ev_enabled - 1) as usize] = CharGetDatum(RULE_FIRES_ON_ORIGIN);
    values[(Anum_pg_rewrite_is_instead - 1) as usize] = BoolGetDatum(evinstead);
    values[(Anum_pg_rewrite_ev_qual - 1) as usize] = CStringGetTextDatum(evqual);
    values[(Anum_pg_rewrite_ev_action - 1) as usize] = CStringGetTextDatum(actiontree);

    /*
     * Ready to store new pg_rewrite tuple
     */
    pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);

    /*
     * Check to see if we are replacing an existing tuple
     */
    oldtup = SearchSysCache2(
        RULERELNAME,
        ObjectIdGetDatum(eventrel_oid),
        PointerGetDatum(rulname as *const c_void),
    );

    if HeapTupleIsValid(oldtup) {
        let mut replaces: [bool; Natts_pg_rewrite as usize] = [false; Natts_pg_rewrite as usize];

        if !replace {
            elog!(
                ERROR,
                "rule \"{}\" for relation \"{}\" already exists",
                cstr(rulname),
                cstr(get_rel_name(eventrel_oid))
            );
        }

        /*
         * When replacing, we don't need to replace every attribute
         */
        replaces[(Anum_pg_rewrite_ev_type - 1) as usize] = true;
        replaces[(Anum_pg_rewrite_is_instead - 1) as usize] = true;
        replaces[(Anum_pg_rewrite_ev_qual - 1) as usize] = true;
        replaces[(Anum_pg_rewrite_ev_action - 1) as usize] = true;

        tup = heap_modify_tuple(
            oldtup,
            RelationGetDescr(pg_rewrite_desc),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
        );

        CatalogTupleUpdate(pg_rewrite_desc, &mut (*tup).t_self, tup);

        ReleaseSysCache(oldtup);

        rewriteObjectId = (*(GETSTRUCT(tup) as Form_pg_rewrite)).oid;
        is_update = true;
    } else {
        rewriteObjectId = GetNewOidWithIndex(
            pg_rewrite_desc,
            RewriteOidIndexId,
            Anum_pg_rewrite_oid as AttrNumber,
        );
        values[(Anum_pg_rewrite_oid - 1) as usize] = ObjectIdGetDatum(rewriteObjectId);

        tup = heap_form_tuple((*pg_rewrite_desc).rd_att as *mut _, values.as_mut_ptr(), nulls.as_mut_ptr());

        CatalogTupleInsert(pg_rewrite_desc, tup);
    }

    heap_freetuple(tup);

    /* If replacing, get rid of old dependencies and make new ones */
    if is_update {
        deleteDependencyRecordsFor(RewriteRelationId, rewriteObjectId, false);
    }

    /*
     * Install dependency on rule's relation to ensure it will go away on
     * relation deletion.  If the rule is ON SELECT, make the dependency
     * implicit --- this prevents deleting a view's SELECT rule.  Other kinds
     * of rules can be AUTO.
     */
    myself.classId = RewriteRelationId;
    myself.objectId = rewriteObjectId;
    myself.objectSubId = 0;

    referenced.classId = RelationRelationId;
    referenced.objectId = eventrel_oid;
    referenced.objectSubId = 0;

    recordDependencyOn(
        &myself,
        &referenced,
        if evtype == CMD_SELECT as c_int {
            DEPENDENCY_INTERNAL
        } else {
            DEPENDENCY_AUTO
        },
    );

    /*
     * Also install dependencies on objects referenced in action and qual.
     */
    recordDependencyOnExpr(&myself, action as *mut Node, NIL, DEPENDENCY_NORMAL);

    if !event_qual.is_null() {
        /* Find query containing OLD/NEW rtable entries */
        let mut qry: *mut Query = linitial_node!(Query, T_Query, action);

        qry = getInsertSelectQuery(qry, std::ptr::null_mut());
        recordDependencyOnExpr(&myself, event_qual, (*qry).rtable, DEPENDENCY_NORMAL);
    }

    /* Post creation hook for new rule */
    InvokeObjectPostCreateHook(RewriteRelationId, rewriteObjectId, 0);

    table_close(pg_rewrite_desc, RowExclusiveLock);

    rewriteObjectId
}

// ---------------------------------------------------------------------------
// DefineRule
//   Execute a CREATE RULE command.
// ---------------------------------------------------------------------------
pub unsafe fn DefineRule(stmt: *mut RuleStmt, queryString: *const c_char) -> ObjectAddress {
    let mut actions: *mut List = std::ptr::null_mut();
    let mut whereClause: *mut Node = std::ptr::null_mut();
    let relId: Oid;

    /* Parse analysis. */
    transformRuleStmt(stmt, queryString, &mut actions, &mut whereClause);

    /*
     * Find and lock the relation.  Lock level should match
     * DefineQueryRewrite.
     */
    relId = RangeVarGetRelid((*stmt).relation, AccessExclusiveLock, false);

    /* ... and execute */
    DefineQueryRewrite(
        (*stmt).rulename,
        relId,
        whereClause,
        (*stmt).event,
        (*stmt).instead,
        (*stmt).replace,
        actions,
    )
}

// ---------------------------------------------------------------------------
// DefineQueryRewrite
//   Create a rule
//
// This is essentially the same as DefineRule() except that the rule's
// action and qual have already been passed through parse analysis.
// ---------------------------------------------------------------------------
pub unsafe fn DefineQueryRewrite(
    mut rulename: *const c_char,
    event_relid: Oid,
    event_qual: *mut Node,
    event_type: CmdType,
    is_instead: bool,
    replace: bool,
    action: *mut List,
) -> ObjectAddress {
    let event_relation: Relation;
    let mut query: *mut Query;
    let mut ruleId: Oid = InvalidOid;
    let mut address: ObjectAddress = core::mem::zeroed();

    /*
     * If we are installing an ON SELECT rule, we had better grab
     * AccessExclusiveLock to ensure no SELECTs are currently running on the
     * event relation. For other types of rules, it would be sufficient to
     * grab ShareRowExclusiveLock to lock out insert/update/delete actions and
     * to ensure that we lock out current CREATE RULE statements; but because
     * of race conditions in access to catalog entries, we can't do that yet.
     *
     * Note that this lock level should match the one used in DefineRule.
     */
    event_relation = table_open(event_relid, AccessExclusiveLock);

    /*
     * Verify relation is of a type that rules can sensibly be applied to.
     * Internal callers can target materialized views, but transformRuleStmt()
     * blocks them for users.  Don't mention them in the error message.
     */
    if (*(*event_relation).rd_rel).relkind != RELKIND_RELATION
        && (*(*event_relation).rd_rel).relkind != RELKIND_MATVIEW
        && (*(*event_relation).rd_rel).relkind != RELKIND_VIEW
        && (*(*event_relation).rd_rel).relkind != RELKIND_PARTITIONED_TABLE
    {
        errdetail_relkind_not_supported((*(*event_relation).rd_rel).relkind);
        elog!(
            ERROR,
            "relation \"{}\" cannot have rules",
            cstr(RelationGetRelationName(event_relation))
        );
    }

    if !allowSystemTableMods && IsSystemRelation(event_relation) {
        elog!(
            ERROR,
            "permission denied: \"{}\" is a system catalog",
            cstr(RelationGetRelationName(event_relation))
        );
    }

    /*
     * Check user has permission to apply rules to this relation.
     */
    if !object_ownercheck(RelationRelationId, event_relid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype((*(*event_relation).rd_rel).relkind),
            RelationGetRelationName(event_relation),
        );
    }

    /*
     * No rule actions that modify OLD or NEW
     */
    foreach!(l, action, {
        query = lfirst_node!(Query, T_Query, current_cell!(l));
        if (*query).resultRelation == 0 {
            continue;
        }
        /* Don't be fooled by INSERT/SELECT */
        if query != getInsertSelectQuery(query, std::ptr::null_mut()) {
            continue;
        }
        if (*query).resultRelation == PRS2_OLD_VARNO {
            elog!(ERROR, "rule actions on OLD are not implemented");
        }
        if (*query).resultRelation == PRS2_NEW_VARNO {
            elog!(ERROR, "rule actions on NEW are not implemented");
        }
    });

    if event_type == CMD_SELECT {
        /*
         * Rules ON SELECT are restricted to view definitions
         *
         * So this had better be a view, ...
         */
        if (*(*event_relation).rd_rel).relkind != RELKIND_VIEW
            && (*(*event_relation).rd_rel).relkind != RELKIND_MATVIEW
        {
            errdetail_relkind_not_supported((*(*event_relation).rd_rel).relkind);
            elog!(
                ERROR,
                "relation \"{}\" cannot have ON SELECT rules",
                cstr(RelationGetRelationName(event_relation))
            );
        }

        /*
         * ... there cannot be INSTEAD NOTHING, ...
         */
        if action == NIL {
            elog!(ERROR, "INSTEAD NOTHING rules on SELECT are not implemented");
        }

        /*
         * ... there cannot be multiple actions, ...
         */
        if list_length(action) > 1 {
            elog!(ERROR, "multiple actions for rules on SELECT are not implemented");
        }

        /*
         * ... the one action must be a SELECT, ...
         */
        query = linitial_node!(Query, T_Query, action);
        if !is_instead || (*query).commandType != CMD_SELECT {
            elog!(ERROR, "rules on SELECT must have action INSTEAD SELECT");
        }

        /*
         * ... it cannot contain data-modifying WITH ...
         */
        if (*query).hasModifyingCTE {
            elog!(
                ERROR,
                "rules on SELECT must not contain data-modifying statements in WITH"
            );
        }

        /*
         * ... there can be no rule qual, ...
         */
        if !event_qual.is_null() {
            elog!(ERROR, "event qualifications are not implemented for rules on SELECT");
        }

        /*
         * ... the targetlist of the SELECT action must exactly match the
         * event relation, ...
         */
        checkRuleResultList(
            (*query).targetList,
            RelationGetDescr(event_relation),
            true,
            (*(*event_relation).rd_rel).relkind != RELKIND_MATVIEW,
        );

        /*
         * ... there must not be another ON SELECT rule already ...
         */
        if !replace && !(*event_relation).rd_rules.is_null() {
            let mut i: c_int = 0;
            let rd_rules = (*event_relation).rd_rules as *mut RuleLock;

            while i < (*rd_rules).numLocks {
                let rule: *mut RewriteRule;

                rule = *(*rd_rules).rules.add(i as usize);
                if (*rule).event == CMD_SELECT {
                    elog!(
                        ERROR,
                        "\"{}\" is already a view",
                        cstr(RelationGetRelationName(event_relation))
                    );
                }
                i += 1;
            }
        }

        /*
         * ... and finally the rule must be named _RETURN.
         */
        if strcmp(rulename, ViewSelectRuleName) != 0 {
            /*
             * In versions before 7.3, the expected name was _RETviewname. For
             * backwards compatibility with old pg_dump output, accept that
             * and silently change it to _RETURN.  Since this is just a quick
             * backwards-compatibility hack, limit the number of characters
             * checked to a few less than NAMEDATALEN; this saves having to
             * worry about where a multibyte character might have gotten
             * truncated.
             */
            if strncmp(rulename, c"_RET".as_ptr(), 4) != 0
                || strncmp(
                    rulename.add(4),
                    RelationGetRelationName(event_relation),
                    (NAMEDATALEN - 4 - 4) as usize,
                ) != 0
            {
                elog!(
                    ERROR,
                    "view rule for \"{}\" must be named \"{}\"",
                    cstr(RelationGetRelationName(event_relation)),
                    cstr(ViewSelectRuleName)
                );
            }
            rulename = pstrdup(ViewSelectRuleName);
        }
    } else {
        /*
         * For non-SELECT rules, a RETURNING list can appear in at most one of
         * the actions ... and there can't be any RETURNING list at all in a
         * conditional or non-INSTEAD rule.  (Actually, there can be at most
         * one RETURNING list across all rules on the same event, but it seems
         * best to enforce that at rule expansion time.)  If there is a
         * RETURNING list, it must match the event relation.
         */
        let mut haveReturning: bool = false;

        foreach!(l, action, {
            query = lfirst_node!(Query, T_Query, current_cell!(l));

            if (*query).returningList.is_null() {
                continue;
            }
            if haveReturning {
                elog!(ERROR, "cannot have multiple RETURNING lists in a rule");
            }
            haveReturning = true;
            if !event_qual.is_null() {
                elog!(ERROR, "RETURNING lists are not supported in conditional rules");
            }
            if !is_instead {
                elog!(ERROR, "RETURNING lists are not supported in non-INSTEAD rules");
            }
            checkRuleResultList(
                (*query).returningList,
                RelationGetDescr(event_relation),
                false,
                false,
            );
        });

        /*
         * And finally, if it's not an ON SELECT rule then it must *not* be
         * named _RETURN.  This prevents accidentally or maliciously replacing
         * a view's ON SELECT rule with some other kind of rule.
         */
        if strcmp(rulename, ViewSelectRuleName) == 0 {
            elog!(
                ERROR,
                "non-view rule for \"{}\" must not be named \"{}\"",
                cstr(RelationGetRelationName(event_relation)),
                cstr(ViewSelectRuleName)
            );
        }
    }

    /*
     * This rule is allowed - prepare to install it.
     */

    /* discard rule if it's null action and not INSTEAD; it's a no-op */
    if action != NIL || is_instead {
        ruleId = InsertRule(
            rulename,
            event_type as c_int,
            event_relid,
            is_instead,
            event_qual,
            action,
            replace,
        );

        /*
         * Set pg_class 'relhasrules' field true for event relation.
         *
         * Important side effect: an SI notice is broadcast to force all
         * backends (including me!) to update relcache entries with the new
         * rule.
         */
        SetRelationRuleStatus(event_relid, true);
    }

    ObjectAddressSet(&mut address, RewriteRelationId, ruleId);

    /* Close rel, but keep lock till commit... */
    table_close(event_relation, NoLock);

    address
}

// ---------------------------------------------------------------------------
// checkRuleResultList
//   Verify that targetList produces output compatible with a tupledesc
//
// The targetList might be either a SELECT targetlist, or a RETURNING list;
// isSelect tells which.  This is used for choosing error messages.
//
// A SELECT targetlist may optionally require that column names match.
// ---------------------------------------------------------------------------
unsafe fn checkRuleResultList(
    targetList: *mut List,
    resultDesc: TupleDesc,
    isSelect: bool,
    requireColumnNameMatch: bool,
) {
    let mut i: c_int;

    /* Only a SELECT may require a column name match. */
    assert!(isSelect || !requireColumnNameMatch);

    i = 0;
    foreach!(tllist, targetList, {
        let tle: *mut TargetEntry = lfirst(current_cell!(tllist)) as *mut TargetEntry;
        let tletypid: Oid;
        let tletypmod: i32;
        let attr: Form_pg_attribute;
        let attname: *mut c_char;

        /* resjunk entries may be ignored */
        if (*tle).resjunk {
            continue;
        }
        i += 1;
        if i > (*resultDesc).natts {
            if isSelect {
                elog!(ERROR, "SELECT rule's target list has too many entries");
            } else {
                elog!(ERROR, "RETURNING list has too many entries");
            }
        }

        attr = TupleDescAttr(resultDesc, i - 1);
        attname = NameStr(&mut (*attr).attname);

        /*
         * Disallow dropped columns in the relation.  This is not really
         * expected to happen when creating an ON SELECT rule.  It'd be
         * possible if someone tried to convert a relation with dropped
         * columns to a view, but the only case we care about supporting
         * table-to-view conversion for is pg_dump, and pg_dump won't do that.
         *
         * Unfortunately, the situation is also possible when adding a rule
         * with RETURNING to a regular table, and rejecting that case is
         * altogether more annoying.  In principle we could support it by
         * modifying the targetlist to include dummy NULL columns
         * corresponding to the dropped columns in the tupdesc.  However,
         * places like ruleutils.c would have to be fixed to not process such
         * entries, and that would take an uncertain and possibly rather large
         * amount of work.  (Note we could not dodge that by marking the dummy
         * columns resjunk, since it's precisely the non-resjunk tlist columns
         * that are expected to correspond to table columns.)
         */
        if (*attr).attisdropped {
            if isSelect {
                elog!(ERROR, "cannot convert relation containing dropped columns to view");
            } else {
                elog!(
                    ERROR,
                    "cannot create a RETURNING list for a relation containing dropped columns"
                );
            }
        }

        /* Check name match if required; no need for two error texts here */
        if requireColumnNameMatch && strcmp((*tle).resname, attname) != 0 {
            elog!(
                ERROR,
                "SELECT rule's target entry {} has different column name from column \"{}\"",
                i,
                cstr(attname)
            );
        }

        /* Check type match. */
        tletypid = exprType((*tle).expr as *mut Node);
        if (*attr).atttypid != tletypid {
            if isSelect {
                elog!(
                    ERROR,
                    "SELECT rule's target entry {} has different type from column \"{}\"",
                    i,
                    cstr(attname)
                );
            } else {
                elog!(
                    ERROR,
                    "RETURNING list's entry {} has different type from column \"{}\"",
                    i,
                    cstr(attname)
                );
            }
        }

        /*
         * Allow typmods to be different only if one of them is -1, ie,
         * "unspecified".  This is necessary for cases like "numeric", where
         * the table will have a filled-in default length but the select
         * rule's expression will probably have typmod = -1.
         */
        tletypmod = exprTypmod((*tle).expr as *mut Node);
        if (*attr).atttypmod != tletypmod && (*attr).atttypmod != -1 && tletypmod != -1 {
            if isSelect {
                elog!(
                    ERROR,
                    "SELECT rule's target entry {} has different size from column \"{}\"",
                    i,
                    cstr(attname)
                );
            } else {
                elog!(
                    ERROR,
                    "RETURNING list's entry {} has different size from column \"{}\"",
                    i,
                    cstr(attname)
                );
            }
        }
    });

    if i != (*resultDesc).natts {
        if isSelect {
            elog!(ERROR, "SELECT rule's target list has too few entries");
        } else {
            elog!(ERROR, "RETURNING list has too few entries");
        }
    }
}

// ---------------------------------------------------------------------------
// setRuleCheckAsUser
//   Recursively scan a query or expression tree and set the checkAsUser
//   field to the given userid in all RTEPermissionInfos of the query.
// ---------------------------------------------------------------------------
pub unsafe fn setRuleCheckAsUser(node: *mut Node, userid: Oid) {
    let mut userid = userid;
    setRuleCheckAsUser_walker(node, &mut userid as *mut Oid as *mut c_void);
}

unsafe fn setRuleCheckAsUser_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Query) {
        setRuleCheckAsUser_Query(node as *mut Query, *(context as *mut Oid));
        return false;
    }
    expression_tree_walker(node, setRuleCheckAsUser_walker, context)
}

unsafe fn setRuleCheckAsUser_Query(qry: *mut Query, userid: Oid) {
    /* Set in all RTEPermissionInfos for this query. */
    foreach!(l, (*qry).rteperminfos, {
        let perminfo: *mut RTEPermissionInfo =
            lfirst_node!(RTEPermissionInfo, T_RTEPermissionInfo, current_cell!(l));

        (*perminfo).checkAsUser = userid;
    });

    /* Now recurse to any subquery RTEs */
    foreach!(l, (*qry).rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(l)) as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_SUBQUERY {
            setRuleCheckAsUser_Query((*rte).subquery, userid);
        }
    });

    /* Recurse into subquery-in-WITH */
    foreach!(l, (*qry).cteList, {
        let cte: *mut CommonTableExpr = lfirst(current_cell!(l)) as *mut CommonTableExpr;

        setRuleCheckAsUser_Query(castNode!(Query, T_Query, (*cte).ctequery), userid);
    });

    /* If there are sublinks, search for them and process their RTEs */
    if (*qry).hasSubLinks {
        let mut userid = userid;
        query_tree_walker(
            qry,
            setRuleCheckAsUser_walker,
            &mut userid as *mut Oid as *mut c_void,
            QTW_IGNORE_RC_SUBQUERIES,
        );
    }
}

// ---------------------------------------------------------------------------
// Change the firing semantics of an existing rule.
// ---------------------------------------------------------------------------
pub unsafe fn EnableDisableRule(rel: Relation, rulename: *const c_char, fires_when: c_char) {
    let pg_rewrite_desc: Relation;
    let owningRel: Oid = RelationGetRelid(rel);
    let eventRelationOid: Oid;
    let ruletup: HeapTuple;
    let ruleform: Form_pg_rewrite;
    let mut changed: bool = false;

    /*
     * Find the rule tuple to change.
     */
    pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);
    ruletup = SearchSysCacheCopy2(
        RULERELNAME,
        ObjectIdGetDatum(owningRel),
        PointerGetDatum(rulename as *const c_void),
    );
    if !HeapTupleIsValid(ruletup) {
        elog!(
            ERROR,
            "rule \"{}\" for relation \"{}\" does not exist",
            cstr(rulename),
            cstr(get_rel_name(owningRel))
        );
    }

    ruleform = GETSTRUCT(ruletup) as Form_pg_rewrite;

    /*
     * Verify that the user has appropriate permissions.
     */
    eventRelationOid = (*ruleform).ev_class;
    assert!(eventRelationOid == owningRel);
    if !object_ownercheck(RelationRelationId, eventRelationOid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(eventRelationOid)),
            get_rel_name(eventRelationOid),
        );
    }

    /*
     * Change ev_enabled if it is different from the desired new state.
     */
    if DatumGetChar((*ruleform).ev_enabled as Datum) != fires_when {
        (*ruleform).ev_enabled = CharGetDatum(fires_when) as c_char;
        CatalogTupleUpdate(pg_rewrite_desc, &mut (*ruletup).t_self, ruletup);

        changed = true;
    }

    InvokeObjectPostAlterHook(RewriteRelationId, (*ruleform).oid, 0);

    heap_freetuple(ruletup);
    table_close(pg_rewrite_desc, RowExclusiveLock);

    /*
     * If we changed anything, broadcast a SI inval message to force each
     * backend (including our own!) to rebuild relation's relcache entry.
     * Otherwise they will fail to apply the change promptly.
     */
    if changed {
        CacheInvalidateRelcache(rel);
    }
}

// ---------------------------------------------------------------------------
// Perform permissions and integrity checks before acquiring a relation lock.
// ---------------------------------------------------------------------------
unsafe extern "C" fn RangeVarCallbackForRenameRule(
    rv: *const RangeVar,
    relid: Oid,
    _oldrelid: Oid,
    _arg: *mut c_void,
) {
    let tuple: HeapTuple;
    let form: Form_pg_class;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped */
    }
    form = GETSTRUCT(tuple) as Form_pg_class;

    /* only tables and views can have rules */
    if (*form).relkind != RELKIND_RELATION
        && (*form).relkind != RELKIND_VIEW
        && (*form).relkind != RELKIND_PARTITIONED_TABLE
    {
        errdetail_relkind_not_supported((*form).relkind);
        elog!(ERROR, "relation \"{}\" cannot have rules", cstr((*rv).relname));
    }

    if !allowSystemTableMods && IsSystemClass(relid, form) {
        elog!(
            ERROR,
            "permission denied: \"{}\" is a system catalog",
            cstr((*rv).relname)
        );
    }

    /* you must own the table to rename one of its rules */
    if !object_ownercheck(RelationRelationId, relid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(relid)),
            (*rv).relname,
        );
    }

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// Rename an existing rewrite rule.
// ---------------------------------------------------------------------------
pub unsafe fn RenameRewriteRule(
    relation: *mut RangeVar,
    oldName: *const c_char,
    newName: *const c_char,
) -> ObjectAddress {
    let relid: Oid;
    let targetrel: Relation;
    let pg_rewrite_desc: Relation;
    let ruletup: HeapTuple;
    let ruleform: Form_pg_rewrite;
    let ruleOid: Oid;
    let mut address: ObjectAddress = core::mem::zeroed();

    /*
     * Look up name, check permissions, and acquire lock (which we will NOT
     * release until end of transaction).
     */
    relid = RangeVarGetRelidExtended(
        relation,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForRenameRule),
        std::ptr::null_mut(),
    );

    /* Have lock already, so just need to build relcache entry. */
    targetrel = relation_open(relid, NoLock);

    /* Prepare to modify pg_rewrite */
    pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);

    /* Fetch the rule's entry (it had better exist) */
    ruletup = SearchSysCacheCopy2(
        RULERELNAME,
        ObjectIdGetDatum(relid),
        PointerGetDatum(oldName as *const c_void),
    );
    if !HeapTupleIsValid(ruletup) {
        elog!(
            ERROR,
            "rule \"{}\" for relation \"{}\" does not exist",
            cstr(oldName),
            cstr(RelationGetRelationName(targetrel))
        );
    }
    ruleform = GETSTRUCT(ruletup) as Form_pg_rewrite;
    ruleOid = (*ruleform).oid;

    /* rule with the new name should not already exist */
    if IsDefinedRewriteRule(relid, newName) {
        elog!(
            ERROR,
            "rule \"{}\" for relation \"{}\" already exists",
            cstr(newName),
            cstr(RelationGetRelationName(targetrel))
        );
    }

    /*
     * We disallow renaming ON SELECT rules, because they should always be
     * named "_RETURN".
     */
    if (*ruleform).ev_type == (CMD_SELECT as c_int + '0' as c_int) as c_char {
        elog!(ERROR, "renaming an ON SELECT rule is not allowed");
    }

    /* OK, do the update */
    namestrcpy(&mut (*ruleform).rulename, newName);

    CatalogTupleUpdate(pg_rewrite_desc, &mut (*ruletup).t_self, ruletup);

    InvokeObjectPostAlterHook(RewriteRelationId, ruleOid, 0);

    heap_freetuple(ruletup);
    table_close(pg_rewrite_desc, RowExclusiveLock);

    /*
     * Invalidate relation's relcache entry so that other backends (and this
     * one too!) are sent SI message to make them rebuild relcache entries.
     * (Ideally this should happen automatically...)
     */
    CacheInvalidateRelcache(targetrel);

    ObjectAddressSet(&mut address, RewriteRelationId, ruleOid);

    /*
     * Close rel, but keep exclusive lock!
     */
    relation_close(targetrel, NoLock);

    address
}

// helper to render a *const c_char as a Rust string for elog! formatting
unsafe fn cstr(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "(null)";
    }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("(invalid)")
}
