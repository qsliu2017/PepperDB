//! Translation of postgres/src/backend/optimizer/util/appendinfo.c
//!
//! Routines for mapping between append parent(s) and children
//! (inheritance / partition planning).
//!
//! #include mapping:
//!   "postgres.h"               -> crate::prelude::*
//!   "access/htup_details.h"    -> (only via make_inh_translation_list, STUBbed)
//!   "access/table.h"           -> table_open/table_close (STUB, used in
//!                                  distribute_row_identity_vars edge case)
//!   "foreign/fdwapi.h"         -> FdwRoutine/GetFdwRoutineForRelation (STUB,
//!                                  add_row_identity_columns foreign branch)
//!   "nodes/makefuncs.h"        -> crate::nodes::makefuncs
//!                                  (makeVar/makeNullConst/makeTargetEntry)
//!   "nodes/nodeFuncs.h"        -> crate::nodes::nodeFuncs
//!                                  (expression_tree_mutator/exprType/exprTypmod)
//!   "optimizer/appendinfo.h"   -> public fn signatures
//!   "optimizer/pathnode.h"     -> find_base_rel/find_base_rel_ignore_join (STUB)
//!   "optimizer/planmain.h"     -> build_base_rel_tlists (STUB)
//!   "parser/parsetree.h"       -> rt_fetch (inlined: list_nth over parse->rtable)
//!   "utils/lsyscache.h"        -> get_rel_name/get_typavgwidth (STUB)
//!   "utils/rel.h"              -> Relation accessors (STUB)
//!   "utils/syscache.h"         -> syscache lookups (STUB)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translation notes (deviations from the C source):
//!
//! * The REAL parts are the attribute-translation mutators built on
//!   expression_tree_mutator: adjust_appendrel_attrs (+ its mutator, with
//!   Var / PlaceHolderVar / CurrentOfExpr / RowExpr / RestrictInfo rewriting),
//!   adjust_appendrel_attrs_multilevel, adjust_child_relids,
//!   adjust_child_relids_multilevel, adjust_inherited_attnums (+ multilevel),
//!   get_translated_update_targetlist, find_appinfos_by_relids,
//!   distribute_row_identity_vars (the ROWID_VAR scan part).  These run over
//!   bms_* and the AppendRelInfo fields and are ported faithfully.
//!
//! * STUBbed (need relcache/syscache/table_open/FDW): make_append_rel_info,
//!   make_inh_translation_list, add_row_identity_var (uses equal()/get_typavgwidth),
//!   add_row_identity_columns, plus the table_open edge case inside
//!   distribute_row_identity_vars.  Signatures kept, bodies unimplemented!() + TODO.
//!
//! * copyObject: the generated copyfuncs.c is not yet ported.  As elsewhere in
//!   the optimizer (placeholder.rs/paramassign.rs), we use a shallow byte copy of
//!   the struct; for the node types touched here (Var/CurrentOfExpr/Const-ish via
//!   list elements) this preserves the tag and scalar/pointer fields.  The list
//!   copies (copyObject of translated_vars / colnames) likewise share the inner
//!   cells, matching how these copies are immediately mutated by the caller.
//!   TODO(pg-port): replace with the real recursive copyObject once copyfuncs.c
//!   lands.
//!
//! * rt_fetch(rti, rtable) is the C macro `((RangeTblEntry *) list_nth(rtable,
//!   rti-1))`; inlined here over crate::nodes::pg_list::list_nth.
//!
//! * The mutator's `context` is the C adjust_appendrel_attrs_context, passed
//!   through expression_tree_mutator as a *mut c_void and read back via cast.

use crate::prelude::*;
use core::ffi::{c_int, c_void};

use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_copy, bms_del_member, bms_is_member, bms_next_member,
    bms_num_members, bms_overlap,
};
use crate::access::attnum::AttrNumber;
use crate::nodes::bitmapset::bms_make_singleton;
use crate::nodes::makefuncs::{makeNullConst, makeTargetEntry, makeVar};
use crate::nodes::nodeFuncs::{exprType, exprTypmod, expression_tree_mutator};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::parsenodes::{Query, RangeTblEntry};
use crate::nodes::pathnodes::{
    AppendRelInfo, PlaceHolderVar, PlannerInfo, RelOptInfo, Relids, RowIdentityVarInfo,
};
use crate::nodes::pg_list::{lappend, lappend_int, lfirst, lfirst_int, list_length, list_nth, List};
use crate::nodes::primnodes::{
    ConvertRowtypeExpr, CurrentOfExpr, RowExpr, Var, ROWID_VAR, VAR_RETURNING_DEFAULT,
};
use crate::{foreach, current_cell, makeNode, IsA, NodeSetTag};

use crate::nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST;
use crate::nodes::nodes::CmdType::CMD_UPDATE;

// ----------------------------------------------------------------------------
// Local helpers (macros / inline functions reproduced from C headers).
// ----------------------------------------------------------------------------

/// copyObject() for a single node pointer.  Shallow byte copy of the struct
/// (the generated copyfuncs.c is not yet ported; see module note).  Returns NULL
/// for a NULL input, matching C copyObject's handling.
///
/// TODO(pg-port): replace with the real recursive copyObject once copyfuncs.c
/// is translated.
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    if node.is_null() {
        return core::ptr::null_mut();
    }
    let p = palloc(core::mem::size_of::<T>()) as *mut T;
    core::ptr::copy_nonoverlapping(node, p, 1);
    p
}

/// `rt_fetch(rangetable_index, rangetable)` (parser/parsetree.h):
/// `((RangeTblEntry *) list_nth(rtable, rti - 1))`.  RT indexes are 1-based.
#[inline]
unsafe fn rt_fetch(rti: Index, rtable: *mut List) -> *mut RangeTblEntry {
    list_nth(rtable, rti as c_int - 1) as *mut RangeTblEntry
}

/// The C `adjust_appendrel_attrs_context` struct: planner root plus the array of
/// AppendRelInfos describing the substitutions to perform.
struct adjust_appendrel_attrs_context {
    root: *mut PlannerInfo,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
}

// ----------------------------------------------------------------------------
// STUBs: relcache / syscache / FDW dependent helpers.
// ----------------------------------------------------------------------------

/// Relation handle (utils/rel.h).
type Relation = crate::utils::rel::Relation;
type TupleDesc = crate::access::common::tupdesc::TupleDesc;
type Form_pg_attribute = crate::catalog::pg_attribute::Form_pg_attribute;

use crate::catalog::pg_type_d::TIDOID;
use crate::nodes::makefuncs::RECORDOID;
use crate::access::sysattr::SelfItemPointerAttributeNumber;
use crate::foreign::fdwapi::{FdwRoutine, GetFdwRoutineForRelation};
use crate::utils::reltrigger::TriggerDesc;

const InvalidAttrNumber: AttrNumber = 0;

/// `RELKIND_*` (catalog/pg_class.h): relation-kind discriminators.
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_MATVIEW: c_char = b'm' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;

/// `RelationGetRelid(relation)` (utils/rel.h): `relation->rd_id`.
unsafe fn RelationGetRelid(relation: Relation) -> Oid {
    (*relation).rd_id
}

/// `RelationGetDescr(relation)` (utils/rel.h): `relation->rd_att`.
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    (*relation).rd_att
}

/// `RelationGetRelationName(relation)` (utils/rel.h):
/// `NameStr(relation->rd_rel->relname)`.
unsafe fn RelationGetRelationName(relation: Relation) -> *const c_char {
    NameStr(&raw const (*(*relation).rd_rel).relname)
}

/// `TupleDescAttr(tupdesc, i)` (access/tupdesc.h): the i'th attribute.
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc, i)
}

/// `NameStr(name)` (c.h): pointer to the embedded C string of a NameData.
unsafe fn NameStr(name: *const crate::c::NameData) -> *const c_char {
    (*name).data.as_ptr()
}

/// `SearchSysCacheAttName(relid, attname)` (utils/syscache.h): not yet ported.
unsafe fn SearchSysCacheAttName(_relid: Oid, _attname: *const c_char) -> *mut c_void {
    // TODO(pg-port): syscache.c SearchSysCacheAttName not yet ported.
    unimplemented!("SearchSysCacheAttName: syscache not yet ported")
}

/// `HeapTupleIsValid(tuple)` (access/htup.h): non-NULL test.
unsafe fn HeapTupleIsValid(tuple: *mut c_void) -> bool {
    !tuple.is_null()
}

/// `GETSTRUCT(tuple)` (access/htup_details.h): the Form pointer from a HeapTuple;
/// not yet ported.
unsafe fn GETSTRUCT(_tuple: *mut c_void) -> Form_pg_attribute {
    // TODO(pg-port): access/htup_details.h GETSTRUCT not yet ported.
    unimplemented!("GETSTRUCT: htup_details not yet ported")
}

/// `ReleaseSysCache(tuple)` (utils/syscache.h): not yet ported.
unsafe fn ReleaseSysCache(_tuple: *mut c_void) {
    // TODO(pg-port): syscache.c ReleaseSysCache not yet ported.
}

/// `equal(a, b)` (nodes/equalfuncs.c): structural node equality.
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    crate::nodes::equalfuncs::equal(a, b)
}

/// `get_typavgwidth(typid, typmod)` (utils/lsyscache.h).
unsafe fn get_typavgwidth(typid: Oid, typmod: int32) -> int32 {
    crate::utils::cache::lsyscache::get_typavgwidth(typid, typmod)
}

/// `table_open(relationId, lockmode)` (access/table.h): not yet ported.  STUB.
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    // TODO(pg-port): access/table.c table_open not yet ported.
    unimplemented!("table_open: access/table.c not yet ported")
}

/// `table_close(relation, lockmode)` (access/table.h): not yet ported.  STUB.
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    // TODO(pg-port): access/table.c table_close not yet ported.
}

/// `NoLock` (storage/lock.h): the no-lock lock mode.
const NoLock: c_int = 0;

extern "C" {
    /// libc `strcmp` (string.h).
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

/// `build_base_rel_tlists(root, final_tlist)` (optimizer/planmain.h).
unsafe fn build_base_rel_tlists(root: *mut PlannerInfo, final_tlist: *mut List) {
    crate::optimizer::plan::initsplan::build_base_rel_tlists(root, final_tlist)
}

/// `get_rel_name(relid)` (utils/lsyscache.h): not yet ported.  Used only in
/// error-message paths.  STUB.
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    // TODO(pg-port): lsyscache.c get_rel_name (syscache lookup) not yet ported.
    core::ptr::null_mut()
}

/// `find_base_rel(root, relid)` (optimizer/pathnode.h): not yet ported.  STUB.
unsafe fn find_base_rel(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo {
    // TODO(pg-port): relnode.c find_base_rel not yet ported.
    unimplemented!("find_base_rel: relnode.c not yet ported")
}

/// `find_base_rel_ignore_join(root, relid)` (optimizer/pathnode.h): not yet
/// ported.  STUB.
unsafe fn find_base_rel_ignore_join(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo {
    // TODO(pg-port): relnode.c find_base_rel_ignore_join not yet ported.
    unimplemented!("find_base_rel_ignore_join: relnode.c not yet ported")
}

// ----------------------------------------------------------------------------
// make_append_rel_info / make_inh_translation_list (STUB: relcache + syscache).
// ----------------------------------------------------------------------------

/*
 * make_append_rel_info
 *	  Build an AppendRelInfo for the parent-child pair
 */
pub unsafe fn make_append_rel_info(
    parentrel: Relation,
    childrel: Relation,
    parentRTindex: Index,
    childRTindex: Index,
) -> *mut AppendRelInfo {
    let appinfo: *mut AppendRelInfo = makeNode!(AppendRelInfo, T_AppendRelInfo);

    (*appinfo).parent_relid = parentRTindex;
    (*appinfo).child_relid = childRTindex;
    (*appinfo).parent_reltype = (*(*parentrel).rd_rel).reltype;
    (*appinfo).child_reltype = (*(*childrel).rd_rel).reltype;
    make_inh_translation_list(parentrel, childrel, childRTindex, appinfo);
    (*appinfo).parent_reloid = RelationGetRelid(parentrel);

    appinfo
}

/*
 * make_inh_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  an inheritance child, as well as a reverse-translation array.
 */
unsafe fn make_inh_translation_list(
    oldrelation: Relation,
    newrelation: Relation,
    newvarno: Index,
    appinfo: *mut AppendRelInfo,
) {
    let mut vars: *mut List = core::ptr::null_mut();
    let pcolnos: *mut AttrNumber;
    let old_tupdesc: TupleDesc = RelationGetDescr(oldrelation);
    let new_tupdesc: TupleDesc = RelationGetDescr(newrelation);
    let new_relid: Oid = RelationGetRelid(newrelation);
    let oldnatts: c_int = (*old_tupdesc).natts;
    let newnatts: c_int = (*new_tupdesc).natts;
    let mut new_attno: c_int = 0;

    /* Initialize reverse-translation array with all entries zero */
    (*appinfo).num_child_cols = newnatts;
    pcolnos = palloc0(newnatts as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    (*appinfo).parent_colnos = pcolnos;

    let mut old_attno: c_int = 0;
    while old_attno < oldnatts {
        let mut att: Form_pg_attribute;
        let attname: *const c_char;
        let atttypid: Oid;
        let atttypmod: int32;
        let attcollation: Oid;

        att = TupleDescAttr(old_tupdesc, old_attno);
        if (*att).attisdropped {
            /* Just put NULL into this list entry */
            vars = lappend(vars, core::ptr::null_mut());
            old_attno += 1;
            continue;
        }
        attname = NameStr(&raw const (*att).attname);
        atttypid = (*att).atttypid;
        atttypmod = (*att).atttypmod;
        attcollation = (*att).attcollation;

        /*
         * When we are generating the "translation list" for the parent table
         * of an inheritance set, no need to search for matches.
         */
        if oldrelation == newrelation {
            vars = lappend(
                vars,
                makeVar(
                    newvarno as c_int,
                    (old_attno + 1) as AttrNumber,
                    atttypid,
                    atttypmod,
                    attcollation,
                    0,
                ) as *mut c_void,
            );
            *pcolnos.add(old_attno as usize) = (old_attno + 1) as AttrNumber;
            old_attno += 1;
            continue;
        }

        /*
         * Otherwise we have to search for the matching column by name.
         * There's no guarantee it'll have the same column position, because
         * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
         * However, in simple cases, the relative order of columns is mostly
         * the same in both relations, so try the column of newrelation that
         * follows immediately after the one that we just found, and if that
         * fails, let syscache handle it.
         */
        if new_attno >= newnatts
            || {
                att = TupleDescAttr(new_tupdesc, new_attno);
                (*att).attisdropped
            }
            || strcmp(attname, NameStr(&raw const (*att).attname)) != 0
        {
            let newtup: *mut c_void;

            newtup = SearchSysCacheAttName(new_relid, attname);
            if !HeapTupleIsValid(newtup) {
                elog!(
                    ERROR,
                    "could not find inherited attribute \"{}\" of relation \"{}\"",
                    std::ffi::CStr::from_ptr(attname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(newrelation))
                        .to_string_lossy()
                );
            }
            new_attno = (*(GETSTRUCT(newtup))).attnum as c_int - 1;
            Assert!(new_attno >= 0 && new_attno < newnatts);
            ReleaseSysCache(newtup);

            att = TupleDescAttr(new_tupdesc, new_attno);
        }

        /* Found it, check type and collation match */
        if atttypid != (*att).atttypid || atttypmod != (*att).atttypmod {
            ereport!(
                ERROR,
                errmsg!(
                    "attribute \"{}\" of relation \"{}\" does not match parent's type",
                    std::ffi::CStr::from_ptr(attname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(newrelation))
                        .to_string_lossy()
                )
                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION) */
            );
        }
        if attcollation != (*att).attcollation {
            ereport!(
                ERROR,
                errmsg!(
                    "attribute \"{}\" of relation \"{}\" does not match parent's collation",
                    std::ffi::CStr::from_ptr(attname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(newrelation))
                        .to_string_lossy()
                )
                /* C also: errcode(ERRCODE_INVALID_COLUMN_DEFINITION) */
            );
        }

        vars = lappend(
            vars,
            makeVar(
                newvarno as c_int,
                (new_attno + 1) as AttrNumber,
                atttypid,
                atttypmod,
                attcollation,
                0,
            ) as *mut c_void,
        );
        *pcolnos.add(new_attno as usize) = (old_attno + 1) as AttrNumber;
        new_attno += 1;
        old_attno += 1;
    }

    (*appinfo).translated_vars = vars;
}

// ----------------------------------------------------------------------------
// adjust_appendrel_attrs and its mutator (REAL).
// ----------------------------------------------------------------------------

/*
 * adjust_appendrel_attrs
 *	  Copy the specified query or expression and translate Vars referring to a
 *	  parent rel to refer to the corresponding child rel instead.  We also
 *	  update rtindexes appearing outside Vars, such as resultRelation and
 *	  jointree relids.
 *
 * Note: this is only applied after conversion of sublinks to subplans,
 * so we don't need to cope with recursion into sub-queries.
 */
pub unsafe fn adjust_appendrel_attrs(
    root: *mut PlannerInfo,
    node: *mut Node,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
) -> *mut Node {
    let mut context = adjust_appendrel_attrs_context {
        root,
        nappinfos,
        appinfos,
    };

    /* If there's nothing to adjust, don't call this function. */
    Assert!(nappinfos >= 1 && !appinfos.is_null());

    /* Should never be translating a Query tree. */
    Assert!(node.is_null() || !IsA!(node, T_Query));

    adjust_appendrel_attrs_mutator(node, &mut context as *mut _ as *mut c_void)
}

unsafe fn adjust_appendrel_attrs_mutator(
    node: *mut Node,
    context_ptr: *mut c_void,
) -> *mut Node {
    let context = context_ptr as *mut adjust_appendrel_attrs_context;
    let appinfos = (*context).appinfos;
    let nappinfos = (*context).nappinfos;

    if node.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(node, T_Var) {
        let var: *mut Var = copyObject(node as *const Var);
        let mut appinfo: *mut AppendRelInfo = core::ptr::null_mut();

        if (*var).varlevelsup != 0 {
            return var as *mut Node; /* no changes needed */
        }

        /*
         * You might think we need to adjust var->varnullingrels, but that
         * shouldn't need any changes.  It will contain outer-join relids,
         * while the transformation we are making affects only baserels.
         * Below, we just merge var->varnullingrels into the translated Var.
         */
        let mut cnt = 0;
        while cnt < nappinfos {
            if (*var).varno == (**appinfos.add(cnt as usize)).parent_relid as c_int {
                appinfo = *appinfos.add(cnt as usize);
                break;
            }
            cnt += 1;
        }

        if !appinfo.is_null() {
            (*var).varno = (*appinfo).child_relid as c_int;
            /* it's now a generated Var, so drop any syntactic labeling */
            (*var).varnosyn = 0;
            (*var).varattnosyn = 0;
            if (*var).varattno > 0 {
                if (*var).varattno as c_int > list_length((*appinfo).translated_vars) {
                    elog!(
                        ERROR,
                        "attribute {} of relation (OID {}) does not exist",
                        (*var).varattno as c_int,
                        (*appinfo).parent_reloid
                    );
                }
                let newnode: *mut Node = copyObject(list_nth(
                    (*appinfo).translated_vars,
                    (*var).varattno as c_int - 1,
                ) as *const Node);
                if newnode.is_null() {
                    elog!(
                        ERROR,
                        "attribute {} of relation (OID {}) does not exist",
                        (*var).varattno as c_int,
                        (*appinfo).parent_reloid
                    );
                }
                if IsA!(newnode, T_Var) {
                    let newvar = newnode as *mut Var;

                    (*newvar).varreturningtype = (*var).varreturningtype;
                    (*newvar).varnullingrels =
                        bms_add_members((*newvar).varnullingrels, (*var).varnullingrels);
                } else {
                    if (*var).varreturningtype != VAR_RETURNING_DEFAULT {
                        elog!(ERROR, "failed to apply returningtype to a non-Var");
                    }
                    if !(*var).varnullingrels.is_null() {
                        elog!(ERROR, "failed to apply nullingrels to a non-Var");
                    }
                }
                return newnode;
            } else if (*var).varattno == 0 {
                /*
                 * Whole-row Var: if we are dealing with named rowtypes, we
                 * can use a whole-row Var for the child table plus a coercion
                 * step to convert the tuple layout to the parent's rowtype.
                 * Otherwise we have to generate a RowExpr.
                 */
                if OidIsValid((*appinfo).child_reltype) {
                    Assert!((*var).vartype == (*appinfo).parent_reltype);
                    if (*appinfo).parent_reltype != (*appinfo).child_reltype {
                        let r: *mut ConvertRowtypeExpr =
                            makeNode!(ConvertRowtypeExpr, T_ConvertRowtypeExpr);

                        (*r).arg = var as *mut crate::nodes::primnodes::Expr;
                        (*r).resulttype = (*appinfo).parent_reltype;
                        (*r).convertformat = COERCE_IMPLICIT_CAST;
                        (*r).location = -1;
                        /* Make sure the Var node has the right type ID, too */
                        (*var).vartype = (*appinfo).child_reltype;
                        return r as *mut Node;
                    }
                } else {
                    /*
                     * Build a RowExpr containing the translated variables.
                     *
                     * In practice var->vartype will always be RECORDOID here,
                     * so we need to come up with some suitable column names.
                     * We use the parent RTE's column names.
                     */
                    let rte: *mut RangeTblEntry = rt_fetch(
                        (*appinfo).parent_relid,
                        (*(*(*context).root).parse).rtable,
                    );
                    let fields: *mut List = copyObject((*appinfo).translated_vars);
                    let rowexpr: *mut RowExpr = makeNode!(RowExpr, T_RowExpr);
                    (*rowexpr).args = fields;
                    (*rowexpr).row_typeid = (*var).vartype;
                    (*rowexpr).row_format = COERCE_IMPLICIT_CAST;
                    (*rowexpr).colnames = copyObject((*(*rte).eref).colnames);
                    (*rowexpr).location = -1;

                    if (*var).varreturningtype != VAR_RETURNING_DEFAULT {
                        elog!(ERROR, "failed to apply returningtype to a non-Var");
                    }
                    if !(*var).varnullingrels.is_null() {
                        elog!(ERROR, "failed to apply nullingrels to a non-Var");
                    }

                    return rowexpr as *mut Node;
                }
            }
            /* system attributes don't need any other translation */
        } else if (*var).varno == ROWID_VAR {
            /*
             * If it's a ROWID_VAR placeholder, see if we've reached a leaf
             * target rel, for which we can translate the Var to a specific
             * instantiation.  We should never be asked to translate to a set
             * of relids containing more than one leaf target rel, so the
             * answer will be unique.  If we're still considering non-leaf
             * inheritance levels, return the ROWID_VAR Var as-is.
             */
            let leaf_result_relids: Relids = (*(*context).root).leaf_result_relids;
            let mut leaf_relid: Index = 0;

            let mut cnt = 0;
            while cnt < nappinfos {
                if bms_is_member(
                    (**appinfos.add(cnt as usize)).child_relid as c_int,
                    leaf_result_relids,
                ) {
                    if leaf_relid != 0 {
                        elog!(ERROR, "cannot translate to multiple leaf relids");
                    }
                    leaf_relid = (**appinfos.add(cnt as usize)).child_relid;
                }
                cnt += 1;
            }

            if leaf_relid != 0 {
                let ridinfo: *mut RowIdentityVarInfo = list_nth(
                    (*(*context).root).row_identity_vars,
                    (*var).varattno as c_int - 1,
                ) as *mut RowIdentityVarInfo;

                if bms_is_member(leaf_relid as c_int, (*ridinfo).rowidrels) {
                    /* Substitute the Var given in the RowIdentityVarInfo */
                    let var2: *mut Var = copyObject((*ridinfo).rowidvar as *const Var);
                    /* ... but use the correct relid */
                    (*var2).varno = leaf_relid as c_int;
                    /* identity vars shouldn't have nulling rels */
                    Assert!((*var2).varnullingrels.is_null());
                    /* varnosyn in the RowIdentityVarInfo is probably wrong */
                    (*var2).varnosyn = 0;
                    (*var2).varattnosyn = 0;
                    return var2 as *mut Node;
                } else {
                    /*
                     * This leaf rel can't return the desired value, so
                     * substitute a NULL of the correct type.
                     */
                    return makeNullConst((*var).vartype, (*var).vartypmod, (*var).varcollid)
                        as *mut Node;
                }
            }
        }
        return var as *mut Node;
    }
    if IsA!(node, T_CurrentOfExpr) {
        let cexpr: *mut CurrentOfExpr = copyObject(node as *const CurrentOfExpr);

        let mut cnt = 0;
        while cnt < nappinfos {
            let appinfo = *appinfos.add(cnt as usize);

            if (*cexpr).cvarno == (*appinfo).parent_relid {
                (*cexpr).cvarno = (*appinfo).child_relid;
                break;
            }
            cnt += 1;
        }
        return cexpr as *mut Node;
    }
    if IsA!(node, T_PlaceHolderVar) {
        /* Copy the PlaceHolderVar node with correct mutation of subnodes */
        let phv: *mut PlaceHolderVar = expression_tree_mutator(
            node,
            Some(adjust_appendrel_attrs_mutator),
            context_ptr,
        ) as *mut PlaceHolderVar;
        /* now fix PlaceHolderVar's relid sets */
        if (*phv).phlevelsup == 0 {
            (*phv).phrels = adjust_child_relids((*phv).phrels, nappinfos, appinfos);
            /* as above, we needn't touch phnullingrels */
        }
        return phv as *mut Node;
    }
    /* Shouldn't need to handle planner auxiliary nodes here */
    Assert!(!IsA!(node, T_SpecialJoinInfo));
    Assert!(!IsA!(node, T_AppendRelInfo));
    Assert!(!IsA!(node, T_PlaceHolderInfo));
    Assert!(!IsA!(node, T_MinMaxAggInfo));

    /*
     * We have to process RestrictInfo nodes specially.  (Note: although
     * set_append_rel_pathlist will hide RestrictInfos in the parent's
     * baserestrictinfo list from us, it doesn't hide those in joininfo.)
     */
    if IsA!(node, T_RestrictInfo) {
        use crate::nodes::pathnodes::RestrictInfo;
        let oldinfo = node as *mut RestrictInfo;
        let newinfo: *mut RestrictInfo = makeNode!(RestrictInfo, T_RestrictInfo);

        /* Copy all flat-copiable fields, notably including rinfo_serial */
        core::ptr::copy_nonoverlapping(oldinfo, newinfo, 1);

        /* Recursively fix the clause itself */
        (*newinfo).clause = adjust_appendrel_attrs_mutator(
            (*oldinfo).clause as *mut Node,
            context_ptr,
        ) as *mut crate::nodes::primnodes::Expr;

        /* and the modified version, if an OR clause */
        (*newinfo).orclause = adjust_appendrel_attrs_mutator(
            (*oldinfo).orclause as *mut Node,
            context_ptr,
        ) as *mut crate::nodes::primnodes::Expr;

        /* adjust relid sets too */
        (*newinfo).clause_relids =
            adjust_child_relids((*oldinfo).clause_relids, (*context).nappinfos, (*context).appinfos);
        (*newinfo).required_relids = adjust_child_relids(
            (*oldinfo).required_relids,
            (*context).nappinfos,
            (*context).appinfos,
        );
        (*newinfo).outer_relids =
            adjust_child_relids((*oldinfo).outer_relids, (*context).nappinfos, (*context).appinfos);
        (*newinfo).left_relids =
            adjust_child_relids((*oldinfo).left_relids, (*context).nappinfos, (*context).appinfos);
        (*newinfo).right_relids =
            adjust_child_relids((*oldinfo).right_relids, (*context).nappinfos, (*context).appinfos);

        /*
         * Reset cached derivative fields, since these might need to have
         * different values when considering the child relation.  Note we
         * don't reset left_ec/right_ec: each child variable is implicitly
         * equivalent to its parent, so still a member of the same EC if any.
         */
        (*newinfo).eval_cost.startup = -1.0;
        (*newinfo).norm_selec = -1.0;
        (*newinfo).outer_selec = -1.0;
        (*newinfo).left_em = core::ptr::null_mut();
        (*newinfo).right_em = core::ptr::null_mut();
        (*newinfo).scansel_cache = core::ptr::null_mut();
        (*newinfo).left_bucketsize = -1.0;
        (*newinfo).right_bucketsize = -1.0;
        (*newinfo).left_mcvfreq = -1.0;
        (*newinfo).right_mcvfreq = -1.0;

        return newinfo as *mut Node;
    }

    /*
     * NOTE: we do not need to recurse into sublinks, because they should
     * already have been converted to subplans before we see them.
     */
    Assert!(!IsA!(node, T_SubLink));
    Assert!(!IsA!(node, T_Query));
    /* We should never see these Query substructures, either. */
    Assert!(!IsA!(node, T_RangeTblRef));
    Assert!(!IsA!(node, T_JoinExpr));

    expression_tree_mutator(node, Some(adjust_appendrel_attrs_mutator), context_ptr)
}

/*
 * adjust_appendrel_attrs_multilevel
 *	  Apply Var translations from an appendrel parent down to a child.
 *
 * Replace Vars in the "node" expression that reference "parentrel" with
 * the appropriate Vars for "childrel".  childrel can be more than one
 * inheritance level removed from parentrel.
 */
pub unsafe fn adjust_appendrel_attrs_multilevel(
    root: *mut PlannerInfo,
    mut node: *mut Node,
    childrel: *mut RelOptInfo,
    parentrel: *mut RelOptInfo,
) -> *mut Node {
    /* Recurse if immediate parent is not the top parent. */
    if (*childrel).parent != parentrel {
        if !(*childrel).parent.is_null() {
            node =
                adjust_appendrel_attrs_multilevel(root, node, (*childrel).parent, parentrel);
        } else {
            elog!(ERROR, "childrel is not a child of parentrel");
        }
    }

    /* Now translate for this child. */
    let mut nappinfos: c_int = 0;
    let appinfos = find_appinfos_by_relids(root, (*childrel).relids, &mut nappinfos);

    node = adjust_appendrel_attrs(root, node, nappinfos, appinfos);

    pfree(appinfos as *mut c_void);

    node
}

/*
 * Substitute child relids for parent relids in a Relid set.  The array of
 * appinfos specifies the substitutions to be performed.
 */
pub unsafe fn adjust_child_relids(
    relids: Relids,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
) -> Relids {
    let mut result: Relids = core::ptr::null_mut();

    let mut cnt = 0;
    while cnt < nappinfos {
        let appinfo = *appinfos.add(cnt as usize);

        /* Remove parent, add child */
        if bms_is_member((*appinfo).parent_relid as c_int, relids) {
            /* Make a copy if we are changing the set. */
            if result.is_null() {
                result = bms_copy(relids);
            }

            result = bms_del_member(result, (*appinfo).parent_relid as c_int);
            result = bms_add_member(result, (*appinfo).child_relid as c_int);
        }
        cnt += 1;
    }

    /* If we made any changes, return the modified copy. */
    if !result.is_null() {
        return result;
    }

    /* Otherwise, return the original set without modification. */
    relids
}

/*
 * Substitute child's relids for parent's relids in a Relid set.
 * The childrel can be multiple inheritance levels below the parent.
 */
pub unsafe fn adjust_child_relids_multilevel(
    root: *mut PlannerInfo,
    mut relids: Relids,
    childrel: *mut RelOptInfo,
    parentrel: *mut RelOptInfo,
) -> Relids {
    /*
     * If the given relids set doesn't contain any of the parent relids, it
     * will remain unchanged.
     */
    if !bms_overlap(relids, (*parentrel).relids) {
        return relids;
    }

    /* Recurse if immediate parent is not the top parent. */
    if (*childrel).parent != parentrel {
        if !(*childrel).parent.is_null() {
            relids =
                adjust_child_relids_multilevel(root, relids, (*childrel).parent, parentrel);
        } else {
            elog!(ERROR, "childrel is not a child of parentrel");
        }
    }

    /* Now translate for this child. */
    let mut nappinfos: c_int = 0;
    let appinfos = find_appinfos_by_relids(root, (*childrel).relids, &mut nappinfos);

    relids = adjust_child_relids(relids, nappinfos, appinfos);

    pfree(appinfos as *mut c_void);

    relids
}

/*
 * adjust_inherited_attnums
 *	  Translate an integer list of attribute numbers from parent to child.
 */
pub unsafe fn adjust_inherited_attnums(
    attnums: *mut List,
    context: *mut AppendRelInfo,
) -> *mut List {
    let mut result: *mut List = core::ptr::null_mut();

    /* This should only happen for an inheritance case, not UNION ALL */
    Assert!(OidIsValid((*context).parent_reloid));

    /* Look up each attribute in the AppendRelInfo's translated_vars list */
    foreach!(lc, attnums, {
        let parentattno = lfirst_int(current_cell!(lc));

        /* Look up the translation of this column: it must be a Var */
        if parentattno <= 0 || parentattno > list_length((*context).translated_vars) {
            elog!(
                ERROR,
                "attribute {} of relation (OID {}) does not exist",
                parentattno,
                (*context).parent_reloid
            );
        }
        let childvar =
            list_nth((*context).translated_vars, parentattno - 1) as *mut Var;
        if childvar.is_null() || !IsA!(childvar, T_Var) {
            elog!(
                ERROR,
                "attribute {} of relation (OID {}) does not exist",
                parentattno,
                (*context).parent_reloid
            );
        }

        result = lappend_int(result, (*childvar).varattno as c_int);
    });
    result
}

/*
 * adjust_inherited_attnums_multilevel
 *	  As above, but traverse multiple inheritance levels as needed.
 */
pub unsafe fn adjust_inherited_attnums_multilevel(
    root: *mut PlannerInfo,
    mut attnums: *mut List,
    child_relid: Index,
    top_parent_relid: Index,
) -> *mut List {
    let appinfo = *(*root).append_rel_array.add(child_relid as usize);

    if appinfo.is_null() {
        elog!(
            ERROR,
            "child rel {} not found in append_rel_array",
            child_relid as c_int
        );
    }

    /* Recurse if immediate parent is not the top parent. */
    if (*appinfo).parent_relid != top_parent_relid {
        attnums = adjust_inherited_attnums_multilevel(
            root,
            attnums,
            (*appinfo).parent_relid,
            top_parent_relid,
        );
    }

    /* Now translate for this child */
    adjust_inherited_attnums(attnums, appinfo)
}

/*
 * get_translated_update_targetlist
 *	  Get the processed_tlist of an UPDATE query, translated as needed to
 *	  match a child target relation.
 *
 * Optionally also return the list of target column numbers translated
 * to this target relation.  (The resnos in processed_tlist MUST NOT be
 * relied on for this purpose.)
 */
pub unsafe fn get_translated_update_targetlist(
    root: *mut PlannerInfo,
    relid: Index,
    processed_tlist: *mut *mut List,
    update_colnos: *mut *mut List,
) {
    /* This is pretty meaningless for commands other than UPDATE. */
    Assert!((*(*root).parse).commandType == CMD_UPDATE);
    if relid == (*(*root).parse).resultRelation as Index {
        /*
         * Non-inheritance case, so it's easy.  The caller might be expecting
         * a tree it can scribble on, though, so copy.
         */
        *processed_tlist = copyObject((*root).processed_tlist);
        if !update_colnos.is_null() {
            *update_colnos = copyObject((*root).update_colnos);
        }
    } else {
        Assert!(bms_is_member(relid as c_int, (*root).all_result_relids));
        *processed_tlist = adjust_appendrel_attrs_multilevel(
            root,
            (*root).processed_tlist as *mut Node,
            find_base_rel(root, relid as c_int),
            find_base_rel(root, (*(*root).parse).resultRelation),
        ) as *mut List;
        if !update_colnos.is_null() {
            *update_colnos = adjust_inherited_attnums_multilevel(
                root,
                (*root).update_colnos,
                relid,
                (*(*root).parse).resultRelation as Index,
            );
        }
    }
}

/*
 * find_appinfos_by_relids
 * 		Find AppendRelInfo structures for base relations listed in relids.
 *
 * The relids argument is typically a join relation's relids, which can
 * include outer-join RT indexes in addition to baserels.  We silently
 * ignore the outer joins.
 *
 * The AppendRelInfos are returned in an array, which can be pfree'd by the
 * caller. *nappinfos is set to the number of entries in the array.
 */
pub unsafe fn find_appinfos_by_relids(
    root: *mut PlannerInfo,
    relids: Relids,
    nappinfos: *mut c_int,
) -> *mut *mut AppendRelInfo {
    let mut cnt: c_int = 0;

    /* Allocate an array that's certainly big enough */
    let appinfos = palloc(
        core::mem::size_of::<*mut AppendRelInfo>() * bms_num_members(relids) as usize,
    ) as *mut *mut AppendRelInfo;

    let mut i: c_int = -1;
    loop {
        i = bms_next_member(relids, i);
        if i < 0 {
            break;
        }
        let appinfo = *(*root).append_rel_array.add(i as usize);

        if appinfo.is_null() {
            /* Probably i is an OJ index, but let's check */
            if find_base_rel_ignore_join(root, i).is_null() {
                continue;
            }
            /* It's a base rel, but we lack an append_rel_array entry */
            elog!(ERROR, "child rel {} not found in append_rel_array", i);
        }

        *appinfos.add(cnt as usize) = appinfo;
        cnt += 1;
    }
    *nappinfos = cnt;
    appinfos
}

/*****************************************************************************
 *
 *		ROW-IDENTITY VARIABLE MANAGEMENT
 *
 * This code lacks a good home, perhaps.  We choose to keep it here because
 * adjust_appendrel_attrs_mutator() is its principal co-conspirator.  That
 * function does most of what is needed to expand ROWID_VAR Vars into the
 * right things.
 *
 *****************************************************************************/

/*
 * add_row_identity_var
 *	  Register a row-identity column to be used in UPDATE/DELETE/MERGE.
 *
 * STUB: the non-inheritance fast path and the matching path both depend on
 * makeTargetEntry + pstrdup (available) but the RowIdentityVarInfo dedup uses
 * equal() (equalfuncs.c is a STUB that panics) and get_typavgwidth
 * (lsyscache.c, not ported).  Keep the signature; body unimplemented!() + TODO.
 */
pub unsafe fn add_row_identity_var(
    root: *mut PlannerInfo,
    orig_var: *mut Var,
    rtindex: Index,
    rowid_name: *const c_char,
) {
    use crate::nodes::primnodes::{Expr, TargetEntry};

    let tle: *mut TargetEntry;
    let rowid_var: *mut Var;
    let mut ridinfo: *mut RowIdentityVarInfo;

    /* For now, the argument must be just a Var of the given rtindex */
    Assert!(IsA!(orig_var, T_Var));
    Assert!((*orig_var).varno == rtindex as c_int);
    Assert!((*orig_var).varlevelsup == 0);
    Assert!((*orig_var).varnullingrels.is_null());

    /*
     * If we're doing non-inherited UPDATE/DELETE/MERGE, there's little need
     * for ROWID_VAR shenanigans.  Just shove the presented Var into the
     * processed_tlist, and we're done.
     */
    if rtindex as c_int == (*(*root).parse).resultRelation {
        tle = makeTargetEntry(
            orig_var as *mut Expr,
            (list_length((*root).processed_tlist) + 1) as AttrNumber,
            pstrdup(rowid_name),
            true,
        );
        (*root).processed_tlist = lappend((*root).processed_tlist, tle as *mut c_void);
        return;
    }

    /*
     * Otherwise, rtindex should reference a leaf target relation that's being
     * added to the query during expand_inherited_rtentry().
     */
    Assert!(bms_is_member(rtindex as c_int, (*root).leaf_result_relids));
    Assert!(!(*(*root).append_rel_array.add(rtindex as usize)).is_null());

    /*
     * We have to find a matching RowIdentityVarInfo, or make one if there is
     * none.  To allow using equal() to match the vars, change the varno to
     * ROWID_VAR, leaving all else alone.
     */
    rowid_var = copyObject(orig_var as *const Var);
    /* This could eventually become ChangeVarNodes() */
    (*rowid_var).varno = ROWID_VAR;

    /* Look for an existing row-id column of the same name */
    foreach!(lc, (*root).row_identity_vars, {
        ridinfo = lfirst(current_cell!(lc)) as *mut RowIdentityVarInfo;
        if strcmp(rowid_name, (*ridinfo).rowidname) != 0 {
            continue;
        }
        if equal(rowid_var as *const c_void, (*ridinfo).rowidvar as *const c_void) {
            /* Found a match; we need only record that rtindex needs it too */
            (*ridinfo).rowidrels = bms_add_member((*ridinfo).rowidrels, rtindex as c_int);
            return;
        } else {
            /* Ooops, can't handle this */
            elog!(
                ERROR,
                "conflicting uses of row-identity name \"{}\"",
                std::ffi::CStr::from_ptr(rowid_name).to_string_lossy()
            );
        }
    });

    /* No request yet, so add a new RowIdentityVarInfo */
    ridinfo = makeNode!(RowIdentityVarInfo, T_RowIdentityVarInfo);
    (*ridinfo).rowidvar = copyObject(rowid_var as *const Var);
    /* for the moment, estimate width using just the datatype info */
    (*ridinfo).rowidwidth = get_typavgwidth(
        exprType(rowid_var as *mut Node),
        exprTypmod(rowid_var as *mut Node),
    );
    (*ridinfo).rowidname = pstrdup(rowid_name);
    (*ridinfo).rowidrels = bms_make_singleton(rtindex as c_int);

    (*root).row_identity_vars = lappend((*root).row_identity_vars, ridinfo as *mut c_void);

    /* Change rowid_var into a reference to this row_identity_vars entry */
    (*rowid_var).varattno = list_length((*root).row_identity_vars) as AttrNumber;

    /* Push the ROWID_VAR reference variable into processed_tlist */
    tle = makeTargetEntry(
        rowid_var as *mut Expr,
        (list_length((*root).processed_tlist) + 1) as AttrNumber,
        pstrdup(rowid_name),
        true,
    );
    (*root).processed_tlist = lappend((*root).processed_tlist, tle as *mut c_void);
}

/*
 * add_row_identity_columns
 *
 * This function adds the row identity columns needed by the core code.
 * FDWs might call add_row_identity_var() for themselves to add nonstandard
 * columns.  (Duplicate requests are fine.)
 *
 * STUB: reads target_relation->rd_rel->relkind (relcache) and dispatches into
 * the FDW routine (fdwapi.h) for foreign tables; neither is ported.
 */
pub unsafe fn add_row_identity_columns(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    use crate::nodes::nodes::CmdType::{CMD_DELETE, CMD_MERGE};

    let commandType = (*(*root).parse).commandType;
    let relkind: c_char = (*(*target_relation).rd_rel).relkind;
    let var: *mut Var;

    Assert!(
        commandType == CMD_UPDATE || commandType == CMD_DELETE || commandType == CMD_MERGE
    );

    if relkind == RELKIND_RELATION
        || relkind == RELKIND_MATVIEW
        || relkind == RELKIND_PARTITIONED_TABLE
    {
        /*
         * Emit CTID so that executor can find the row to merge, update or
         * delete.
         */
        var = makeVar(
            rtindex as c_int,
            SelfItemPointerAttributeNumber,
            TIDOID,
            -1,
            InvalidOid,
            0,
        );
        add_row_identity_var(root, var, rtindex, c"ctid".as_ptr());
    } else if relkind == RELKIND_FOREIGN_TABLE {
        /*
         * Let the foreign table's FDW add whatever junk TLEs it wants.
         */
        let fdwroutine: *mut FdwRoutine;

        fdwroutine = GetFdwRoutineForRelation(target_relation as *mut c_void, false);

        if (*fdwroutine).AddForeignUpdateTargets.is_some() {
            ((*fdwroutine).AddForeignUpdateTargets.unwrap())(
                root as *mut c_void,
                rtindex,
                target_rte as *mut c_void,
                target_relation as *mut c_void,
            );
        }

        /*
         * For UPDATE, we need to make the FDW fetch unchanged columns by
         * asking it to fetch a whole-row Var.  That's because the top-level
         * targetlist only contains entries for changed columns, but
         * ExecUpdate will need to build the complete new tuple.  (Actually,
         * we only really need this in UPDATEs that are not pushed to the
         * remote side, but it's hard to tell if that will be the case at the
         * point when this function is called.)
         *
         * We will also need the whole row if there are any row triggers, so
         * that the executor will have the "old" row to pass to the trigger.
         * Alas, this misses system columns.
         */
        if commandType == CMD_UPDATE
            || (!(*target_relation).trigdesc.is_null()
                && ((*((*target_relation).trigdesc as *mut TriggerDesc)).trig_delete_after_row
                    || (*((*target_relation).trigdesc as *mut TriggerDesc))
                        .trig_delete_before_row))
        {
            var = makeVar(rtindex as c_int, InvalidAttrNumber, RECORDOID, -1, InvalidOid, 0);
            add_row_identity_var(root, var, rtindex, c"wholerow".as_ptr());
        }
    }
}

/*
 * distribute_row_identity_vars
 *
 * After we have finished identifying all the row identity columns
 * needed by an inherited UPDATE/DELETE/MERGE query, make sure that
 * these columns will be generated by all the target relations.
 *
 * The ROWID_VAR scan over processed_tlist is ported REAL.  The constraint-
 * exclusion edge case (root->row_identity_vars == NIL) re-opens the target
 * relation via table_open + add_row_identity_columns + build_base_rel_tlists,
 * which need relcache/planmain not yet ported; that branch is STUBbed.
 */
pub unsafe fn distribute_row_identity_vars(root: *mut PlannerInfo) {
    use crate::nodes::nodes::CmdType::{CMD_DELETE, CMD_MERGE};
    use crate::nodes::primnodes::TargetEntry;

    let parse: *mut Query = (*root).parse;
    let result_relation = (*parse).resultRelation;

    /*
     * There's nothing to do if this isn't an inherited UPDATE/DELETE/MERGE.
     */
    if (*parse).commandType != CMD_UPDATE
        && (*parse).commandType != CMD_DELETE
        && (*parse).commandType != CMD_MERGE
    {
        Assert!((*root).row_identity_vars.is_null());
        return;
    }
    let target_rte: *mut RangeTblEntry = rt_fetch(result_relation as Index, (*parse).rtable);
    if !(*target_rte).inh {
        Assert!((*root).row_identity_vars.is_null());
        return;
    }

    /*
     * Ordinarily, we expect that leaf result relation(s) will have added some
     * ROWID_VAR Vars to the query.  However, it's possible that constraint
     * exclusion suppressed every leaf relation.  Handle this edge case by
     * re-opening the top result relation and adding the row identity columns
     * it would have used, then re-running build_base_rel_tlists.
     */
    if (*root).row_identity_vars.is_null() {
        let target_relation: Relation;

        target_relation = table_open((*target_rte).relid, NoLock);
        add_row_identity_columns(root, result_relation as Index, target_rte, target_relation);
        table_close(target_relation, NoLock);
        build_base_rel_tlists(root, (*root).processed_tlist);
        /* There are no ROWID_VAR Vars in this case, so we're done. */
        return;
    }

    /*
     * Dig through the processed_tlist to find the ROWID_VAR reference Vars,
     * and forcibly copy them into the reltarget list of the topmost target
     * relation.  That's sufficient because they'll be copied to the
     * individual leaf target rels (with appropriate translation) later,
     * during appendrel expansion --- see set_append_rel_size().
     */
    let target_rel: *mut RelOptInfo = find_base_rel(root, result_relation);

    foreach!(lc, (*root).processed_tlist, {
        let tle = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut TargetEntry;
        let var = (*tle).expr as *mut Var;

        if !var.is_null() && IsA!(var, T_Var) && (*var).varno == ROWID_VAR {
            (*(*target_rel).reltarget).exprs = crate::nodes::pg_list::lappend(
                (*(*target_rel).reltarget).exprs,
                copyObject(var) as *mut c_void,
            );
            /* reltarget cost and width will be computed later */
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::{bms_add_member as bms_add, bms_is_member as bms_mem};

    /// Build a zero-initialized AppendRelInfo with the given parent/child relids
    /// (and a valid node tag) for relid-remapping tests.
    unsafe fn make_appinfo(parent: Index, child: Index) -> *mut AppendRelInfo {
        let a = palloc0(core::mem::size_of::<AppendRelInfo>()) as *mut AppendRelInfo;
        NodeSetTag!(a, NodeTag::T_AppendRelInfo);
        (*a).parent_relid = parent;
        (*a).child_relid = child;
        a
    }

    // adjust_child_relids remaps a Relids set from parent to child relid.
    #[test]
    fn test_adjust_child_relids_remaps_parent_to_child() {
        unsafe {
            // Parent relid 2 -> child relid 5; parent relid 3 -> child relid 7.
            let a1 = make_appinfo(2, 5);
            let a2 = make_appinfo(3, 7);
            let mut arr: [*mut AppendRelInfo; 2] = [a1, a2];

            // Build relids {2, 4} : 2 is a parent (remapped), 4 is untouched.
            let mut relids: Relids = core::ptr::null_mut();
            relids = bms_add(relids, 2);
            relids = bms_add(relids, 4);

            let result = adjust_child_relids(relids, 2, arr.as_mut_ptr());

            // 2 -> 5, 4 stays, 3/7 not present.
            assert!(bms_mem(5, result), "child relid 5 should be present");
            assert!(bms_mem(4, result), "untouched relid 4 should remain");
            assert!(!bms_mem(2, result), "parent relid 2 should be removed");
            assert!(!bms_mem(7, result), "child relid 7 should not be present");

            // The original set must be unchanged (adjust_child_relids copies).
            assert!(bms_mem(2, relids), "original relids must still contain 2");
        }
    }

    // adjust_child_relids returns the original set unmodified when no parent
    // relid is present.
    #[test]
    fn test_adjust_child_relids_no_change_returns_original() {
        unsafe {
            let a1 = make_appinfo(2, 5);
            let mut arr: [*mut AppendRelInfo; 1] = [a1];

            let mut relids: Relids = core::ptr::null_mut();
            relids = bms_add(relids, 9);
            relids = bms_add(relids, 10);

            let result = adjust_child_relids(relids, 1, arr.as_mut_ptr());

            // Same pointer back (no copy made).
            assert_eq!(result, relids, "unchanged set should be returned as-is");
        }
    }

    // find_appinfos_by_relids returns the matching AppendRelInfo for a child
    // relid via root->append_rel_array.
    #[test]
    fn test_find_appinfos_by_relids() {
        unsafe {
            // Build a PlannerInfo with an append_rel_array large enough to index
            // relids 0..=5.  Entry for child relid 5 -> a1, others NULL.
            let n = 6usize;
            let arr = palloc0(core::mem::size_of::<*mut AppendRelInfo>() * n)
                as *mut *mut AppendRelInfo;
            let a1 = make_appinfo(2, 5);
            *arr.add(5) = a1;

            let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
            (*root).append_rel_array = arr;

            // relids = {5} ; expect one appinfo (a1) returned.
            let mut relids: Relids = core::ptr::null_mut();
            relids = bms_add(relids, 5);

            let mut nappinfos: c_int = 0;
            let got = find_appinfos_by_relids(root, relids, &mut nappinfos);

            assert_eq!(nappinfos, 1, "exactly one appinfo expected");
            assert_eq!(*got.add(0), a1, "appinfo for child relid 5 expected");
            assert_eq!((**got.add(0)).child_relid, 5);
        }
    }
}
