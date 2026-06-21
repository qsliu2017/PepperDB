/*-------------------------------------------------------------------------
 *
 * equivclass.rs
 *    Routines for managing EquivalenceClasses
 *
 * See src/backend/optimizer/README for discussion of EquivalenceClasses.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/optimizer/path/equivclass.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use core::ffi::c_void;
use core::mem::size_of;

use crate::{current_cell, foreach, foreach_node, foreach_delete_current, IsA, makeNode, Assert, list_make1, foreach_current_index};

use crate::access::cmptype::COMPARE_EQ;
use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_add_range, bms_copy, bms_difference,
    bms_get_singleton_member, bms_int_members, bms_intersect, bms_is_empty, bms_is_member,
    bms_is_subset, bms_join, bms_make_singleton, bms_membership, bms_next_member, bms_overlap,
    bms_union, Bitmapset, BMS_MULTIPLE,
};
use crate::nodes::equalfuncs::equal;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pathnodes::{
    AppendRelInfo, EquivalenceClass, EquivalenceMember, EquivalenceMemberIterator,
    ForeignKeyOptInfo, IndexClause, JoinDomain, OuterJoinClauseInfo, PathKey, PathTarget,
    PlannerInfo, RelOptInfo, Relids, RestrictInfo, SpecialJoinInfo,
    IS_JOIN_REL, IS_OTHER_REL, IS_SIMPLE_REL, EC_MUST_BE_REDUNDANT,
};
use crate::nodes::pg_list::{
    lappend, lappend_oid, lfirst, lfirst_oid, linitial, list_concat, list_copy, list_delete_nth_cell,
    list_free, list_head, list_length, list_member, list_member_oid, list_nth,
    lnext, lsecond, List, ListCell, NIL,
};
use crate::nodes::primnodes::{
    CoalesceExpr, Const, Expr, NullTest, NullTestType, OpExpr, RelabelType,
    TargetEntry, Var,
};
use crate::postgres_ext::Oid;
use crate::utils::palloc::{palloc0, pfree};

// ===========================================================================
// #include-mapped stubs -- local re-export aliases and TODO stubs for
// functions whose source files are not yet ported.
// ===========================================================================

// ---------------------------------------------------------------------------
// access/stratnum.h  -- already in crate::access::cmptype as COMPARE_EQ etc.
// catalog/pg_type.h  -- OIDs used below come from prelude::RECORDOID etc.
// ---------------------------------------------------------------------------

// RECORDOID from catalog/pg_type.h
const RECORDOID: Oid = 2249;

// IsPolymorphicType -- catalog/pg_type.h macro
#[inline]
unsafe fn IsPolymorphicType(typid: Oid) -> bool {
    // ANYELEMENTOID=2283, ANYARRAYOID=2277, ANYNONARRAYOID=2776,
    // ANYENUMOID=3500, ANYRANGEOID=3831, ANYMULTIRANGEOID=4537,
    // ANYCOMPATIBLEOID=5077, ANYCOMPATIBLEARRAYOID=5078,
    // ANYCOMPATIBLENONARRAYOID=5079, ANYCOMPATIBLERANGEOID=5080,
    // ANYCOMPATIBLEMULTIRANGEOID=5082
    matches!(
        typid,
        2283 | 2277 | 2776 | 3500 | 3831 | 4537 | 5077 | 5078 | 5079 | 5080 | 5082
    )
}

// InvalidOid
const INVALID_OID: Oid = 0;

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != INVALID_OID
}

// ---------------------------------------------------------------------------
// common/hashfn.h
// ---------------------------------------------------------------------------

// TODO(pg-port): hash_bytes lives in src/common/hashfn.rs (not yet wired here)
extern "C" {
    fn hash_bytes(k: *const u8, keylen: i32) -> u32;
}

// ---------------------------------------------------------------------------
// nodes/makefuncs.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): makeNode / real constructors from src/nodes/makefuncs.rs
extern "C" {
    fn make_restrictinfo(
        root: *mut PlannerInfo,
        clause: *mut Expr,
        is_pushed_down: bool,
        has_clone: bool,
        is_clone: bool,
        pseudoconstant: bool,
        security_level: u32,
        required_relids: Relids,
        incompatible_relids: Relids,
        outer_relids: Relids,
    ) -> *mut RestrictInfo;

    fn makeBoolConst(value: bool, isnull: bool) -> *mut Node;
}

// ---------------------------------------------------------------------------
// nodes/nodeFuncs.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): nodeFuncs stubs -- real impls in src/nodes/nodeFuncs.rs

extern "C" {
    fn exprType(expr: *const Node) -> Oid;
    fn exprTypmod(expr: *const Node) -> i32;
    fn exprCollation(expr: *const Node) -> Oid;
    fn applyRelabelType(
        val: *mut Node,
        req_type: Oid,
        req_typmod: i32,
        req_collation: Oid,
        coerce_type: i32,
        location: i32,
        overwrite_ok: bool,
    ) -> *mut Node;
    fn is_opclause(clause: *const c_void) -> bool;
    fn get_leftop(clause: *const Expr) -> *mut Node;
    fn get_rightop(clause: *const Expr) -> *mut Node;
    fn expression_returns_set(expr: *const Node) -> bool;
}

// COERCE_IMPLICIT_CAST value from nodes/primnodes.h
const COERCE_IMPLICIT_CAST: i32 = 2;

// ---------------------------------------------------------------------------
// optimizer/appendinfo.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): appendinfo stubs -- src/optimizer/util/appendinfo.rs

extern "C" {
    fn adjust_appendrel_attrs(
        root: *mut PlannerInfo,
        node: *mut Node,
        nappinfos: i32,
        appinfos: *mut *mut AppendRelInfo,
    ) -> *mut Node;
    fn adjust_appendrel_attrs_multilevel(
        root: *mut PlannerInfo,
        node: *mut Node,
        child_rel: *mut RelOptInfo,
        top_parent: *mut RelOptInfo,
    ) -> *mut Node;
    fn find_childrel_parents(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> Relids;
}

// ---------------------------------------------------------------------------
// optimizer/clauses.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): clauses stubs -- src/optimizer/util/clauses.rs

extern "C" {
    fn contain_volatile_functions(node: *const Node) -> bool;
    fn contain_agg_clause(node: *const Node) -> bool;
    fn contain_window_function(node: *const Node) -> bool;
    fn is_parallel_safe(root: *mut PlannerInfo, node: *const Node) -> bool;
}

// ---------------------------------------------------------------------------
// optimizer/optimizer.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): optimizer.h stubs -- src/optimizer/optimizer.rs

extern "C" {
    fn canonicalize_ec_expression_impl(
        expr: *mut Expr,
        req_type: Oid,
        req_collation: Oid,
    ) -> *mut Expr; // NOTE: this file defines the real one; extern needed only for cross-module
    fn pull_varnos(root: *mut PlannerInfo, node: *const Node) -> Relids;
    fn pull_var_clause(node: *const Node, flags: i32) -> *mut List;
    fn add_vars_to_targetlist(root: *mut PlannerInfo, vars: *mut List, joinrelids: Relids);
    fn add_vars_to_attr_needed(root: *mut PlannerInfo, vars: *mut List, joinrelids: Relids);
    fn set_opfuncid(expr: *mut OpExpr);
    fn func_strict(funcid: Oid) -> bool;
    fn op_input_types(opno: Oid, lefttype: *mut Oid, righttype: *mut Oid);
}

// pull_var_clause flags (nodes/nodeFuncs.h)
const PVC_INCLUDE_AGGREGATES: i32 = 0x0001;
const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_INCLUDE_WINDOWFUNCS: i32 = 0x0004;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;
const PVC_RECURSE_PLACEHOLDERS: i32 = 0x0020;
const PVC_INCLUDE_CONVERTROWTYPES: i32 = 0x0040;

// ---------------------------------------------------------------------------
// optimizer/pathnode.h / paths.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): pathnode / paths stubs

extern "C" {
    fn build_implied_join_equality(
        root: *mut PlannerInfo,
        opno: Oid,
        collation: Oid,
        leftop: *mut Expr,
        rightop: *mut Expr,
        qualscope: Relids,
        security_level: u32,
    ) -> *mut RestrictInfo;
    fn add_outer_joins_to_relids(
        root: *mut PlannerInfo,
        nominal_join_relids: Relids,
        sjinfo: *mut SpecialJoinInfo,
        pushed_down_joins: *mut *mut List,
    ) -> Relids;
}

// ---------------------------------------------------------------------------
// optimizer/planmain.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): planmain stubs -- src/optimizer/plan/planmain.rs

extern "C" {
    fn process_implied_equality(
        root: *mut PlannerInfo,
        opno: Oid,
        collation: Oid,
        item1: *mut Expr,
        item2: *mut Expr,
        qualscope: Relids,
        security_level: u32,
        both_const: bool,
    ) -> *mut RestrictInfo;
    fn distribute_restrictinfo_to_rels(root: *mut PlannerInfo, rinfo: *mut RestrictInfo);
}

// ---------------------------------------------------------------------------
// optimizer/restrictinfo.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): restrictinfo stubs -- src/optimizer/util/restrictinfo.rs

// (make_restrictinfo already declared above)

// ---------------------------------------------------------------------------
// rewrite/rewriteManip.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): rewriteManip stubs -- src/rewrite/rewriteManip.rs

extern "C" {
    fn remove_nulling_relids(node: *mut Node, removable_relids: Relids, except_relids: Relids) -> *mut Node;
}

// ---------------------------------------------------------------------------
// utils/lsyscache.h stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): lsyscache stubs -- utils/cache/lsyscache.c not yet ported

extern "C" {
    fn get_opfamily_member_for_cmptype(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        cmptype: crate::access::cmptype::CompareType,
    ) -> Oid;
    fn get_mergejoin_opfamilies(opno: Oid) -> *mut List;
    fn get_func_leakproof(funcid: Oid) -> bool;
    fn get_opcode(opno: Oid) -> Oid;
    fn op_hashjoinable(opno: Oid, lefttype: Oid) -> bool;
    fn copyObject(node: *const c_void) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// utils/palloc.h helpers
// ---------------------------------------------------------------------------

extern "C" {
    fn palloc0_array_impl(size: usize, n: usize) -> *mut c_void;
    fn repalloc0_array_impl(ptr: *mut c_void, size: usize, old_n: usize, new_n: usize) -> *mut c_void;
    fn MemoryContextSwitchTo(context: *mut c_void) -> *mut c_void;
}

// palloc0_array!(T, n) -- allocate n zero-initialized T-sized elements
macro_rules! palloc0_array {
    ($ty:ty, $n:expr) => {
        palloc0_array_impl(size_of::<$ty>(), $n as usize) as *mut $ty
    };
}

// repalloc0_array!(ptr, T, old_n, new_n)
macro_rules! repalloc0_array {
    ($ptr:expr, $ty:ty, $old_n:expr, $new_n:expr) => {
        repalloc0_array_impl(
            $ptr as *mut c_void,
            size_of::<$ty>(),
            $old_n as usize,
            $new_n as usize,
        ) as *mut $ty
    };
}

// ---------------------------------------------------------------------------
// ec_derives_hash (simplehash-generated in C; stub here)
// ---------------------------------------------------------------------------

// The C source generates a private derives_hash table via lib/simplehash.h.
// We expose an opaque handle; the real hash operations are done through
// the extern "C" functions declared below.
// TODO(pg-port): replace with a native Rust hash table once simplehash.h is
// fully ported.
use crate::nodes::pathnodes::derives_hash;

/// Key type for ec_derives_hash, mirroring C struct ECDerivesKey.
#[repr(C)]
struct ECDerivesKey {
    em1: *mut EquivalenceMember,
    em2: *mut EquivalenceMember,
    parent_ec: *mut EquivalenceClass,
}

/// Entry type for ec_derives_hash, mirroring C struct ECDerivesEntry.
#[repr(C)]
struct ECDerivesEntry {
    status: u32,
    key: ECDerivesKey,
    rinfo: *mut RestrictInfo,
}

/// Threshold for switching from list to hash table (matches C define).
const EC_DERIVES_HASH_THRESHOLD: i32 = 32;

// TODO(pg-port): these simplehash functions are generated by simplehash.h in C.
extern "C" {
    fn derives_create(ctx: *mut c_void, nelements: i32, private_data: *mut c_void) -> *mut derives_hash;
    fn derives_destroy(tb: *mut derives_hash);
    fn derives_insert(tb: *mut derives_hash, key: ECDerivesKey, found: *mut bool) -> *mut ECDerivesEntry;
    fn derives_lookup(tb: *mut derives_hash, key: ECDerivesKey) -> *mut ECDerivesEntry;
}

// ---------------------------------------------------------------------------
// ec_matches callback type (optimizer/paths.h)
// ---------------------------------------------------------------------------

/// Callback type passed to generate_implied_equalities_for_column.
/// Mirrors C typedef `bool (*ec_matches_callback_type)(...)`.
pub type ec_matches_callback_type = unsafe fn(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
    arg: *mut c_void,
) -> bool;

// ===========================================================================
// Forward declarations (static / private functions)
// ===========================================================================

// (All functions below; Rust does not need forward declarations.)

// ===========================================================================
// Part 1 ends here -- see continuation below for functions
// ===========================================================================

/*
 * process_equivalence
 *    The given clause has a mergejoinable operator and is not an outer-join
 *    qualification, so its two sides can be considered equal
 *    anywhere they are both computable; moreover that equality can be
 *    extended transitively.  Record this knowledge in the EquivalenceClass
 *    data structure, if applicable.  Returns true if successful, false if not
 *    (in which case caller should treat the clause as ordinary, not an
 *    equivalence).
 *
 * In some cases, although we cannot convert a clause into EquivalenceClass
 * knowledge, we can still modify it to a more useful form than the original.
 * Then, *p_restrictinfo will be replaced by a new RestrictInfo, which is what
 * the caller should use for further processing.
 *
 * jdomain is the join domain within which the given clause was found.
 * This limits the applicability of deductions from the EquivalenceClass,
 * as described in optimizer/README.
 *
 * We reject proposed equivalence clauses if they contain leaky functions
 * and have security_level above zero.  The EC evaluation rules require us to
 * apply certain tests at certain joining levels, and we can't tolerate
 * delaying any test on security_level grounds.  By rejecting candidate clauses
 * that might require security delays, we ensure it's safe to apply an EC
 * clause as soon as it's supposed to be applied.
 *
 * On success return, we have also initialized the clause's left_ec/right_ec
 * fields to point to the EquivalenceClass representing it.  This saves lookup
 * effort later.
 *
 * Note: constructing merged EquivalenceClasses is a standard UNION-FIND
 * problem, for which there exist better data structures than simple lists.
 * If this code ever proves to be a bottleneck then it could be sped up ---
 * but for now, simple is beautiful.
 *
 * Note: this is only called during planner startup, not during GEQO
 * exploration, so we need not worry about whether we're in the right
 * memory context.
 */
pub unsafe fn process_equivalence(
    root: *mut PlannerInfo,
    p_restrictinfo: *mut *mut RestrictInfo,
    jdomain: *mut JoinDomain,
) -> bool {
    let restrictinfo: *mut RestrictInfo = *p_restrictinfo;
    let clause: *mut Expr = (*restrictinfo).clause;
    let mut opno: Oid = 0;
    let mut collation: Oid = 0;
    let mut item1_type: Oid = 0;
    let mut item2_type: Oid = 0;
    let mut item1: *mut Expr;
    let mut item2: *mut Expr;
    let item1_relids: Relids;
    let item2_relids: Relids;
    let opfamilies: *mut List;
    let mut ec1: *mut EquivalenceClass;
    let mut ec2: *mut EquivalenceClass;
    let mut em1: *mut EquivalenceMember;
    let mut em2: *mut EquivalenceMember;
    let mut ec2_idx: i32;

    /* Should not already be marked as having generated an eclass */
    Assert!((*restrictinfo).left_ec.is_null());
    Assert!((*restrictinfo).right_ec.is_null());

    /* Reject if it is potentially postponable by security considerations */
    if (*restrictinfo).security_level > 0 && !(*restrictinfo).leakproof {
        return false;
    }

    /* Extract info from given clause */
    Assert!(is_opclause(clause as *const c_void));
    opno = (*(clause as *mut OpExpr)).opno;
    collation = (*(clause as *mut OpExpr)).inputcollid;
    item1 = get_leftop(clause) as *mut Expr;
    item2 = get_rightop(clause) as *mut Expr;
    item1_relids = (*restrictinfo).left_relids;
    item2_relids = (*restrictinfo).right_relids;

    /*
     * Ensure both input expressions expose the desired collation (their types
     * should be OK already); see comments for canonicalize_ec_expression.
     */
    item1 = canonicalize_ec_expression(item1, exprType(item1 as *const Node), collation);
    item2 = canonicalize_ec_expression(item2, exprType(item2 as *const Node), collation);

    /*
     * Clauses of the form X=X cannot be translated into EquivalenceClasses.
     * We'd either end up with a single-entry EC, losing the knowledge that
     * the clause was present at all, or else make an EC with duplicate
     * entries, causing other issues.
     */
    if equal(item1 as *const c_void, item2 as *const c_void) {
        /*
         * If the operator is strict, then the clause can be treated as just
         * "X IS NOT NULL".  (Since we know we are considering a top-level
         * qual, we can ignore the difference between FALSE and NULL results.)
         * It's worth making the conversion because we'll typically get a much
         * better selectivity estimate than we would for X=X.
         *
         * If the operator is not strict, we can't be sure what it will do
         * with NULLs, so don't attempt to optimize it.
         */
        set_opfuncid(clause as *mut OpExpr);
        if func_strict((*(clause as *mut OpExpr)).opfuncid) {
            let ntest: *mut NullTest = makeNode!(NullTest, T_NullTest);
            (*ntest).arg = item1;
            (*ntest).nulltesttype = NullTestType::IS_NOT_NULL;
            (*ntest).argisrow = false; /* correct even if composite arg */
            (*ntest).location = -1;

            *p_restrictinfo = make_restrictinfo(
                root,
                ntest as *mut Expr,
                (*restrictinfo).is_pushed_down,
                (*restrictinfo).has_clone,
                (*restrictinfo).is_clone,
                (*restrictinfo).pseudoconstant,
                (*restrictinfo).security_level,
                core::ptr::null_mut(),
                (*restrictinfo).incompatible_relids,
                (*restrictinfo).outer_relids,
            );
        }
        return false;
    }

    /*
     * We use the declared input types of the operator, not exprType() of the
     * inputs, as the nominal datatypes for opfamily lookup.  This presumes
     * that btree operators are always registered with amoplefttype and
     * amoprighttype equal to their declared input types.  We will need this
     * info anyway to build EquivalenceMember nodes, and by extracting it now
     * we can use type comparisons to short-circuit some equal() tests.
     */
    op_input_types(opno, &mut item1_type, &mut item2_type);

    opfamilies = (*restrictinfo).mergeopfamilies;

    /*
     * Sweep through the existing EquivalenceClasses looking for matches to
     * item1 and item2.  These are the possible outcomes:
     *
     * 1. We find both in the same EC.  The equivalence is already known, so
     * there's nothing to do.
     *
     * 2. We find both in different ECs.  Merge the two ECs together.
     *
     * 3. We find just one.  Add the other to its EC.
     *
     * 4. We find neither.  Make a new, two-entry EC.
     *
     * Note: since all ECs are built through this process or the similar
     * search in get_eclass_for_sort_expr(), it's impossible that we'd match
     * an item in more than one existing nonvolatile EC.  So it's okay to stop
     * at the first match.
     */
    ec1 = core::ptr::null_mut();
    ec2 = core::ptr::null_mut();
    em1 = core::ptr::null_mut();
    em2 = core::ptr::null_mut();
    ec2_idx = -1;
    foreach!(lc1, (*root).eq_classes, {
        let cur_ec: *mut EquivalenceClass = lfirst(current_cell!(lc1)) as *mut EquivalenceClass;

        /* Never match to a volatile EC */
        if (*cur_ec).ec_has_volatile {
            continue;
        }

        /*
         * The collation has to match; check this first since it's cheaper
         * than the opfamily comparison.
         */
        if collation != (*cur_ec).ec_collation {
            continue;
        }

        /*
         * A "match" requires matching sets of btree opfamilies.  Use of
         * equal() for this test has implications discussed in the comments
         * for get_mergejoin_opfamilies().
         */
        if !equal(opfamilies as *const c_void, (*cur_ec).ec_opfamilies as *const c_void) {
            continue;
        }

        /* We don't expect any children yet */
        Assert!((*cur_ec).ec_childmembers.is_null());

        foreach!(lc2, (*cur_ec).ec_members, {
            let cur_em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*cur_em).em_is_child);

            /*
             * Match constants only within the same JoinDomain (see
             * optimizer/README).
             */
            if (*cur_em).em_is_const && (*cur_em).em_jdomain != jdomain {
                continue;
            }

            if ec1.is_null()
                && item1_type == (*cur_em).em_datatype
                && equal(item1 as *const c_void, (*cur_em).em_expr as *const c_void)
            {
                ec1 = cur_ec;
                em1 = cur_em;
                if !ec2.is_null() {
                    break;
                }
            }

            if ec2.is_null()
                && item2_type == (*cur_em).em_datatype
                && equal(item2 as *const c_void, (*cur_em).em_expr as *const c_void)
            {
                ec2 = cur_ec;
                ec2_idx = foreach_current_index!(lc1) as i32;
                em2 = cur_em;
                if !ec1.is_null() {
                    break;
                }
            }
        });

        if !ec1.is_null() && !ec2.is_null() {
            break;
        }
    });

    /* Sweep finished, what did we find? */

    if !ec1.is_null() && !ec2.is_null() {
        /* If case 1, nothing to do, except add to sources */
        if ec1 == ec2 {
            (*ec1).ec_sources = lappend((*ec1).ec_sources, restrictinfo as *mut c_void);
            (*ec1).ec_min_security = (*ec1).ec_min_security.min((*restrictinfo).security_level);
            (*ec1).ec_max_security = (*ec1).ec_max_security.max((*restrictinfo).security_level);
            /* mark the RI as associated with this eclass */
            (*restrictinfo).left_ec = ec1;
            (*restrictinfo).right_ec = ec1;
            /* mark the RI as usable with this pair of EMs */
            (*restrictinfo).left_em = em1;
            (*restrictinfo).right_em = em2;
            return true;
        }

        /*
         * Case 2: need to merge ec1 and ec2.  This should never happen after
         * the ECs have reached canonical state; otherwise, pathkeys could be
         * rendered non-canonical by the merge, and relation eclass indexes
         * would get broken by removal of an eq_classes list entry.
         */
        if (*root).ec_merging_done {
            ereport!(
                crate::utils::elog::ERROR,
                errmsg!("too late to merge equivalence classes")
            );
        }

        /*
         * We add ec2's items to ec1, then set ec2's ec_merged link to point
         * to ec1 and remove ec2 from the eq_classes list.  We cannot simply
         * delete ec2 because that could leave dangling pointers in existing
         * PathKeys.  We leave it behind with a link so that the merged EC can
         * be found.
         */
        (*ec1).ec_members = list_concat((*ec1).ec_members, (*ec2).ec_members);
        (*ec1).ec_sources = list_concat((*ec1).ec_sources, (*ec2).ec_sources);

        /*
         * Appends ec2's derived clauses to ec1->ec_derives_list and adds them
         * to ec1->ec_derives_hash if present.
         */
        ec_add_derived_clauses(ec1, (*ec2).ec_derives_list);
        (*ec1).ec_relids = bms_join((*ec1).ec_relids, (*ec2).ec_relids);
        (*ec1).ec_has_const |= (*ec2).ec_has_const;
        /* can't need to set has_volatile */
        (*ec1).ec_min_security = (*ec1).ec_min_security.min((*ec2).ec_min_security);
        (*ec1).ec_max_security = (*ec1).ec_max_security.max((*ec2).ec_max_security);
        (*ec2).ec_merged = ec1;
        (*root).eq_classes = list_delete_nth_cell((*root).eq_classes, ec2_idx);
        /* just to avoid debugging confusion w/ dangling pointers: */
        (*ec2).ec_members = NIL;
        (*ec2).ec_sources = NIL;
        ec_clear_derived_clauses(ec2);
        (*ec2).ec_relids = core::ptr::null_mut();
        (*ec1).ec_sources = lappend((*ec1).ec_sources, restrictinfo as *mut c_void);
        (*ec1).ec_min_security = (*ec1).ec_min_security.min((*restrictinfo).security_level);
        (*ec1).ec_max_security = (*ec1).ec_max_security.max((*restrictinfo).security_level);
        /* mark the RI as associated with this eclass */
        (*restrictinfo).left_ec = ec1;
        (*restrictinfo).right_ec = ec1;
        /* mark the RI as usable with this pair of EMs */
        (*restrictinfo).left_em = em1;
        (*restrictinfo).right_em = em2;
    } else if !ec1.is_null() {
        /* Case 3: add item2 to ec1 */
        em2 = add_eq_member(ec1, item2, item2_relids, jdomain, item2_type);
        (*ec1).ec_sources = lappend((*ec1).ec_sources, restrictinfo as *mut c_void);
        (*ec1).ec_min_security = (*ec1).ec_min_security.min((*restrictinfo).security_level);
        (*ec1).ec_max_security = (*ec1).ec_max_security.max((*restrictinfo).security_level);
        /* mark the RI as associated with this eclass */
        (*restrictinfo).left_ec = ec1;
        (*restrictinfo).right_ec = ec1;
        /* mark the RI as usable with this pair of EMs */
        (*restrictinfo).left_em = em1;
        (*restrictinfo).right_em = em2;
    } else if !ec2.is_null() {
        /* Case 3: add item1 to ec2 */
        em1 = add_eq_member(ec2, item1, item1_relids, jdomain, item1_type);
        (*ec2).ec_sources = lappend((*ec2).ec_sources, restrictinfo as *mut c_void);
        (*ec2).ec_min_security = (*ec2).ec_min_security.min((*restrictinfo).security_level);
        (*ec2).ec_max_security = (*ec2).ec_max_security.max((*restrictinfo).security_level);
        /* mark the RI as associated with this eclass */
        (*restrictinfo).left_ec = ec2;
        (*restrictinfo).right_ec = ec2;
        /* mark the RI as usable with this pair of EMs */
        (*restrictinfo).left_em = em1;
        (*restrictinfo).right_em = em2;
    } else {
        /* Case 4: make a new, two-entry EC */
        let ec: *mut EquivalenceClass = makeNode!(EquivalenceClass, T_EquivalenceClass);
        (*ec).ec_opfamilies = opfamilies;
        (*ec).ec_collation = collation;
        (*ec).ec_childmembers_size = 0;
        (*ec).ec_members = NIL;
        (*ec).ec_childmembers = core::ptr::null_mut();
        (*ec).ec_sources = list_make1!(restrictinfo as *mut c_void);
        (*ec).ec_derives_list = NIL;
        (*ec).ec_derives_hash = core::ptr::null_mut();
        (*ec).ec_relids = core::ptr::null_mut();
        (*ec).ec_has_const = false;
        (*ec).ec_has_volatile = false;
        (*ec).ec_broken = false;
        (*ec).ec_sortref = 0;
        (*ec).ec_min_security = (*restrictinfo).security_level;
        (*ec).ec_max_security = (*restrictinfo).security_level;
        (*ec).ec_merged = core::ptr::null_mut();
        em1 = add_eq_member(ec, item1, item1_relids, jdomain, item1_type);
        em2 = add_eq_member(ec, item2, item2_relids, jdomain, item2_type);

        (*root).eq_classes = lappend((*root).eq_classes, ec as *mut c_void);

        /* mark the RI as associated with this eclass */
        (*restrictinfo).left_ec = ec;
        (*restrictinfo).right_ec = ec;
        /* mark the RI as usable with this pair of EMs */
        (*restrictinfo).left_em = em1;
        (*restrictinfo).right_em = em2;
    }

    true
}

// ===========================================================================
// Part 2
// ===========================================================================

/*
 * canonicalize_ec_expression
 *
 * This function ensures that the expression exposes the expected type and
 * collation, so that it will be equal() to other equivalence-class expressions
 * that it ought to be equal() to.
 *
 * The rule for datatypes is that the exposed type should match what it would
 * be for an input to an operator of the EC's opfamilies; which is usually
 * the declared input type of the operator, but in the case of polymorphic
 * operators no relabeling is wanted (compare the behavior of parse_coerce.c).
 * Expressions coming in from quals will generally have the right type
 * already, but expressions coming from indexkeys may not (because they are
 * represented without any explicit relabel in pg_index), and the same problem
 * occurs for sort expressions (because the parser is likewise cavalier about
 * putting relabels on them).  Such cases will be binary-compatible with the
 * real operators, so adding a RelabelType is sufficient.
 *
 * Also, the expression's exposed collation must match the EC's collation.
 * This is important because in comparisons like "foo < bar COLLATE baz",
 * only one of the expressions has the correct exposed collation as we receive
 * it from the parser.  Forcing both of them to have it ensures that all
 * variant spellings of such a construct behave the same.  Again, we can
 * stick on a RelabelType to force the right exposed collation.  (It might
 * work to not label the collation at all in EC members, but this is risky
 * since some parts of the system expect exprCollation() to deliver the
 * right answer for a sort key.)
 */
pub unsafe fn canonicalize_ec_expression(
    mut expr: *mut Expr,
    mut req_type: Oid,
    req_collation: Oid,
) -> *mut Expr {
    let expr_type: Oid = exprType(expr as *const Node);

    /*
     * For a polymorphic-input-type opclass, just keep the same exposed type.
     * RECORD opclasses work like polymorphic-type ones for this purpose.
     */
    if IsPolymorphicType(req_type) || req_type == RECORDOID {
        req_type = expr_type;
    }

    /*
     * No work if the expression exposes the right type/collation already.
     */
    if expr_type != req_type || exprCollation(expr as *const Node) != req_collation {
        /*
         * If we have to change the type of the expression, set typmod to -1,
         * since the new type may not have the same typmod interpretation.
         * When we only have to change collation, preserve the exposed typmod.
         */
        let req_typmod: i32;
        if expr_type != req_type {
            req_typmod = -1;
        } else {
            req_typmod = exprTypmod(expr as *const Node);
        }

        /*
         * Use applyRelabelType so that we preserve const-flatness.  This is
         * important since eval_const_expressions has already been applied.
         */
        expr = applyRelabelType(
            expr as *mut Node,
            req_type,
            req_typmod,
            req_collation,
            COERCE_IMPLICIT_CAST,
            -1,
            false,
        ) as *mut Expr;
    }

    expr
}

/*
 * make_eq_member
 *    Build a new EquivalenceMember without adding it to an EC.  If 'parent'
 *    is NULL, the result will be a parent member, otherwise a child member.
 */
unsafe fn make_eq_member(
    ec: *mut EquivalenceClass,
    expr: *mut Expr,
    relids: Relids,
    jdomain: *mut JoinDomain,
    parent: *mut EquivalenceMember,
    datatype: Oid,
) -> *mut EquivalenceMember {
    let em: *mut EquivalenceMember = makeNode!(EquivalenceMember, T_EquivalenceMember);

    (*em).em_expr = expr;
    (*em).em_relids = relids;
    (*em).em_is_const = false;
    (*em).em_is_child = !parent.is_null();
    (*em).em_datatype = datatype;
    (*em).em_jdomain = jdomain;
    (*em).em_parent = parent;

    if bms_is_empty(relids) {
        /*
         * No Vars, assume it's a pseudoconstant.  This is correct for entries
         * generated from process_equivalence(), because a WHERE clause can't
         * contain aggregates or SRFs, and non-volatility was checked before
         * process_equivalence() ever got called.  But
         * get_eclass_for_sort_expr() has to work harder.  We put the tests
         * there not here to save cycles in the equivalence case.
         */
        Assert!(parent.is_null());
        (*em).em_is_const = true;
        (*ec).ec_has_const = true;
        /* it can't affect ec_relids */
    }

    em
}

/*
 * add_eq_member - build a new non-child EquivalenceMember and add it to 'ec'.
 */
unsafe fn add_eq_member(
    ec: *mut EquivalenceClass,
    expr: *mut Expr,
    relids: Relids,
    jdomain: *mut JoinDomain,
    datatype: Oid,
) -> *mut EquivalenceMember {
    let em: *mut EquivalenceMember =
        make_eq_member(ec, expr, relids, jdomain, core::ptr::null_mut(), datatype);

    /* add to the members list */
    (*ec).ec_members = lappend((*ec).ec_members, em as *mut c_void);

    /* record the relids for parent members */
    (*ec).ec_relids = bms_add_members((*ec).ec_relids, relids);

    em
}

/*
 * add_child_eq_member
 *    Create an em_is_child=true EquivalenceMember and add it to 'ec'.
 *
 * 'root' is the PlannerInfo that 'ec' belongs to.
 * 'ec' is the EquivalenceClass to add the child member to.
 * 'ec_index' the index of 'ec' within root->eq_classes, or -1 if maintaining
 * the RelOptInfo.eclass_indexes isn't needed.
 * 'expr' is the em_expr for the new member.
 * 'relids' is the 'em_relids' for the new member.
 * 'jdomain' is the 'em_jdomain' for the new member.
 * 'parent_em' is the parent member of the child to create.
 * 'datatype' is the em_datatype of the new member.
 * 'child_relid' defines which element of ec_childmembers to add this member
 * to.  This is generally a RELOPT_OTHER_MEMBER_REL, but for set operations
 * can be a RELOPT_BASEREL representing the set-op children.
 */
unsafe fn add_child_eq_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    ec_index: i32,
    expr: *mut Expr,
    relids: Relids,
    jdomain: *mut JoinDomain,
    parent_em: *mut EquivalenceMember,
    datatype: Oid,
    child_relid: u32,
) -> *mut EquivalenceMember {
    Assert!(!parent_em.is_null());

    /*
     * Allocate the array to store child members; an array of Lists indexed by
     * relid, or expand the existing one, if necessary.
     */
    if (*ec).ec_childmembers_size < (*root).simple_rel_array_size {
        if (*ec).ec_childmembers.is_null() {
            (*ec).ec_childmembers = palloc0_array!(*mut List, (*root).simple_rel_array_size);
        } else {
            (*ec).ec_childmembers = repalloc0_array!(
                (*ec).ec_childmembers,
                *mut List,
                (*ec).ec_childmembers_size,
                (*root).simple_rel_array_size
            );
        }
        (*ec).ec_childmembers_size = (*root).simple_rel_array_size;
    }

    let em: *mut EquivalenceMember = make_eq_member(ec, expr, relids, jdomain, parent_em, datatype);

    /* add member to the ec_childmembers List for the given child_relid */
    let slot: *mut *mut List = (*ec).ec_childmembers.add(child_relid as usize);
    *slot = lappend(*slot, em as *mut c_void);

    /* Record this EC index for the child rel */
    if ec_index >= 0 {
        let child_rel: *mut RelOptInfo = *(*root).simple_rel_array.add(child_relid as usize);
        (*child_rel).eclass_indexes = bms_add_member((*child_rel).eclass_indexes, ec_index);
    }

    em
}

/*
 * get_eclass_for_sort_expr
 *    Given an expression and opfamily/collation info, find an existing
 *    equivalence class it is a member of; if none, optionally build a new
 *    single-member EquivalenceClass for it.
 *
 * sortref is the SortGroupRef of the originating SortGroupClause, if any,
 * or zero if not.  (It should never be zero if the expression is volatile!)
 *
 * If rel is not NULL, it identifies a specific relation we're considering
 * a path for, and indicates that child EC members for that relation can be
 * considered.  Otherwise child members are ignored.  (Note: since child EC
 * members aren't guaranteed unique, a non-NULL value means that there could
 * be more than one EC that matches the expression; if so it's order-dependent
 * which one you get.  This is annoying but it only happens in corner cases,
 * so for now we live with just reporting the first match.  See also
 * generate_implied_equalities_for_column and match_pathkeys_to_index.)
 *
 * If create_it is true, we'll build a new EquivalenceClass when there is no
 * match.  If create_it is false, we just return NULL when no match.
 *
 * This can be used safely both before and after EquivalenceClass merging;
 * since it never causes merging it does not invalidate any existing ECs
 * or PathKeys.  However, ECs added after path generation has begun are
 * of limited usefulness, so usually it's best to create them beforehand.
 *
 * Note: opfamilies must be chosen consistently with the way
 * process_equivalence() would do; that is, generated from a mergejoinable
 * equality operator.  Else we might fail to detect valid equivalences,
 * generating poor (but not incorrect) plans.
 */
pub unsafe fn get_eclass_for_sort_expr(
    root: *mut PlannerInfo,
    mut expr: *mut Expr,
    opfamilies: *mut List,
    opcintype: Oid,
    collation: Oid,
    sortref: u32,
    rel: Relids,
    create_it: bool,
) -> *mut EquivalenceClass {
    let jdomain: *mut JoinDomain;
    let expr_relids: Relids;
    let newec: *mut EquivalenceClass;
    let newem: *mut EquivalenceMember;
    let oldcontext: *mut c_void;

    /*
     * Ensure the expression exposes the correct type and collation.
     */
    expr = canonicalize_ec_expression(expr, opcintype, collation);

    /*
     * Since SortGroupClause nodes are top-level expressions (GROUP BY, ORDER
     * BY, etc), they can be presumed to belong to the top JoinDomain.
     */
    jdomain = linitial((*root).join_domains) as *mut JoinDomain;

    /*
     * Scan through the existing EquivalenceClasses for a match
     */
    foreach!(lc1, (*root).eq_classes, {
        let cur_ec: *mut EquivalenceClass = lfirst(current_cell!(lc1)) as *mut EquivalenceClass;
        let mut it: EquivalenceMemberIterator = core::mem::zeroed();
        let mut cur_em: *mut EquivalenceMember;

        /*
         * Never match to a volatile EC, except when we are looking at another
         * reference to the same volatile SortGroupClause.
         */
        if (*cur_ec).ec_has_volatile && (sortref == 0 || sortref != (*cur_ec).ec_sortref) {
            continue;
        }

        if collation != (*cur_ec).ec_collation {
            continue;
        }
        if !equal(opfamilies as *const c_void, (*cur_ec).ec_opfamilies as *const c_void) {
            continue;
        }

        setup_eclass_member_iterator(&mut it, cur_ec, rel);
        loop {
            cur_em = eclass_member_iterator_next(&mut it);
            if cur_em.is_null() {
                break;
            }

            /*
             * Ignore child members unless they match the request.
             */
            if (*cur_em).em_is_child && !bms_equal((*cur_em).em_relids, rel) {
                continue;
            }

            /*
             * Match constants only within the same JoinDomain (see
             * optimizer/README).
             */
            if (*cur_em).em_is_const && (*cur_em).em_jdomain != jdomain {
                continue;
            }

            if opcintype == (*cur_em).em_datatype
                && equal(expr as *const c_void, (*cur_em).em_expr as *const c_void)
            {
                return cur_ec; /* Match! */
            }
        }
    });

    /* No match; does caller want a NULL result? */
    if !create_it {
        return core::ptr::null_mut();
    }

    /*
     * OK, build a new single-member EC
     *
     * Here, we must be sure that we construct the EC in the right context.
     */
    oldcontext = MemoryContextSwitchTo((*root).planner_cxt as *mut c_void);

    newec = makeNode!(EquivalenceClass, T_EquivalenceClass);
    (*newec).ec_opfamilies = list_copy(opfamilies);
    (*newec).ec_collation = collation;
    (*newec).ec_childmembers_size = 0;
    (*newec).ec_members = NIL;
    (*newec).ec_childmembers = core::ptr::null_mut();
    (*newec).ec_sources = NIL;
    (*newec).ec_derives_list = NIL;
    (*newec).ec_derives_hash = core::ptr::null_mut();
    (*newec).ec_relids = core::ptr::null_mut();
    (*newec).ec_has_const = false;
    (*newec).ec_has_volatile = contain_volatile_functions(expr as *const Node);
    (*newec).ec_broken = false;
    (*newec).ec_sortref = sortref;
    (*newec).ec_min_security = u32::MAX;
    (*newec).ec_max_security = 0;
    (*newec).ec_merged = core::ptr::null_mut();

    if (*newec).ec_has_volatile && sortref == 0 {
        /* should not happen */
        ereport!(
            crate::utils::elog::ERROR,
            errmsg!("volatile EquivalenceClass has no sortref")
        );
    }

    /*
     * Get the precise set of relids appearing in the expression.
     */
    expr_relids = pull_varnos(root, expr as *const Node);

    newem = add_eq_member(
        newec,
        copyObject(expr as *const c_void) as *mut Expr,
        expr_relids,
        jdomain,
        opcintype,
    );

    /*
     * add_eq_member doesn't check for volatile functions, set-returning
     * functions, aggregates, or window functions, but such could appear in
     * sort expressions; so we have to check whether its const-marking was
     * correct.
     */
    if (*newec).ec_has_const {
        if (*newec).ec_has_volatile
            || expression_returns_set(expr as *const Node)
            || contain_agg_clause(expr as *const Node)
            || contain_window_function(expr as *const Node)
        {
            (*newec).ec_has_const = false;
            (*newem).em_is_const = false;
        }
    }

    (*root).eq_classes = lappend((*root).eq_classes, newec as *mut c_void);

    /*
     * If EC merging is already complete, we have to mop up by adding the new
     * EC to the eclass_indexes of the relation(s) mentioned in it.
     */
    if (*root).ec_merging_done {
        let ec_index: i32 = list_length((*root).eq_classes) - 1;
        let mut i: i32 = -1;

        loop {
            i = bms_next_member((*newec).ec_relids, i);
            if i <= 0 {
                break;
            }

            let rel_ptr: *mut RelOptInfo = *(*root).simple_rel_array.add(i as usize);

            /* ignore the RTE_GROUP RTE */
            if i == (*root).group_rtindex as i32 {
                continue;
            }

            if rel_ptr.is_null() {
                /* must be an outer join */
                Assert!(bms_is_member(i, (*root).outer_join_rels));
                continue;
            }

            Assert!((*rel_ptr).reloptkind == crate::nodes::pathnodes::RelOptKind::RELOPT_BASEREL);

            (*rel_ptr).eclass_indexes = bms_add_member((*rel_ptr).eclass_indexes, ec_index);
        }
    }

    MemoryContextSwitchTo(oldcontext);

    newec
}

// bms_equal -- wrapper (missing from bitmapset imports in this file)
#[inline]
unsafe fn bms_equal(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_equal(a, b)
}

/*
 * find_ec_member_matching_expr
 *    Locate an EquivalenceClass member matching the given expr, if any;
 *    return NULL if no match.
 *
 * "Matching" is defined as "equal after stripping RelabelTypes".
 * This is used for identifying sort expressions, and we need to allow
 * binary-compatible relabeling for some cases involving binary-compatible
 * sort operators.
 *
 * Child EC members are ignored unless they belong to given 'relids'.
 */
pub unsafe fn find_ec_member_matching_expr(
    ec: *mut EquivalenceClass,
    mut expr: *mut Expr,
    relids: Relids,
) -> *mut EquivalenceMember {
    let mut it: EquivalenceMemberIterator = core::mem::zeroed();
    let mut em: *mut EquivalenceMember;

    /* We ignore binary-compatible relabeling on both ends */
    while !expr.is_null() && IsA!(expr, T_RelabelType) {
        expr = (*(expr as *mut RelabelType)).arg;
    }

    setup_eclass_member_iterator(&mut it, ec, relids);
    loop {
        em = eclass_member_iterator_next(&mut it);
        if em.is_null() {
            break;
        }

        let mut emexpr: *mut Expr;

        /*
         * We shouldn't be trying to sort by an equivalence class that
         * contains a constant, so no need to consider such cases any further.
         */
        if (*em).em_is_const {
            continue;
        }

        /*
         * Ignore child members unless they belong to the requested rel.
         */
        if (*em).em_is_child && !bms_is_subset((*em).em_relids, relids) {
            continue;
        }

        /*
         * Match if same expression (after stripping relabel).
         */
        emexpr = (*em).em_expr;
        while !emexpr.is_null() && IsA!(emexpr, T_RelabelType) {
            emexpr = (*(emexpr as *mut RelabelType)).arg;
        }

        if equal(emexpr as *const c_void, expr as *const c_void) {
            return em;
        }
    }

    core::ptr::null_mut()
}

/*
 * find_computable_ec_member
 *    Locate an EquivalenceClass member that can be computed from the
 *    expressions appearing in "exprs"; return NULL if no match.
 *
 * "exprs" can be either a list of bare expression trees, or a list of
 * TargetEntry nodes.  Typically it will contain Vars and possibly Aggrefs
 * and WindowFuncs; however, when considering an appendrel member the list
 * could contain arbitrary expressions.  We consider an EC member to be
 * computable if all the Vars, PlaceHolderVars, Aggrefs, and WindowFuncs
 * it needs are present in "exprs".
 *
 * There is some subtlety in that definition: for example, if an EC member is
 * Var_A + 1 while what is in "exprs" is Var_A + 2, it's still computable.
 * This works because in the final plan tree, the EC member's expression will
 * be computed as part of the same plan node targetlist that is currently
 * represented by "exprs".  So if we have Var_A available for the existing
 * tlist member, it must be OK to use it in the EC expression too.
 *
 * Unlike find_ec_member_matching_expr, there's no special provision here
 * for binary-compatible relabeling.  This is intentional: if we have to
 * compute an expression in this way, setrefs.c is going to insist on exact
 * matches of Vars to the source tlist.
 *
 * Child EC members are ignored unless they belong to given 'relids'.
 * Also, non-parallel-safe expressions are ignored if 'require_parallel_safe'.
 *
 * Note: some callers pass root == NULL for notational reasons.  This is OK
 * when require_parallel_safe is false.
 */
pub unsafe fn find_computable_ec_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    exprs: *mut List,
    relids: Relids,
    require_parallel_safe: bool,
) -> *mut EquivalenceMember {
    let mut it: EquivalenceMemberIterator = core::mem::zeroed();
    let mut em: *mut EquivalenceMember;

    /*
     * Pull out the Vars and quasi-Vars present in "exprs".  In the typical
     * non-appendrel case, this is just another representation of the same
     * list.  However, it does remove the distinction between the case of a
     * list of plain expressions and a list of TargetEntrys.
     */
    let exprvars: *mut List = pull_var_clause(
        exprs as *const Node,
        PVC_INCLUDE_AGGREGATES
            | PVC_INCLUDE_WINDOWFUNCS
            | PVC_INCLUDE_PLACEHOLDERS
            | PVC_INCLUDE_CONVERTROWTYPES,
    );

    setup_eclass_member_iterator(&mut it, ec, relids);
    loop {
        em = eclass_member_iterator_next(&mut it);
        if em.is_null() {
            break;
        }

        /*
         * We shouldn't be trying to sort by an equivalence class that
         * contains a constant, so no need to consider such cases any further.
         */
        if (*em).em_is_const {
            continue;
        }

        /*
         * Ignore child members unless they belong to the requested rel.
         */
        if (*em).em_is_child && !bms_is_subset((*em).em_relids, relids) {
            continue;
        }

        /*
         * Match if all Vars and quasi-Vars are present in "exprs".
         */
        let emvars: *mut List = pull_var_clause(
            (*em).em_expr as *const Node,
            PVC_INCLUDE_AGGREGATES | PVC_INCLUDE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        let mut found_all = true;
        foreach!(lc2, emvars, {
            if !list_member(exprvars, lfirst(current_cell!(lc2))) {
                found_all = false;
                break;
            }
        });
        list_free(emvars);
        if !found_all {
            continue; /* we hit a non-available Var */
        }

        /*
         * If requested, reject expressions that are not parallel-safe.  We
         * check this last because it's a rather expensive test.
         */
        if require_parallel_safe && !is_parallel_safe(root, (*em).em_expr as *const Node) {
            continue;
        }

        return em; /* found usable expression */
    }

    core::ptr::null_mut()
}

/*
 * relation_can_be_sorted_early
 *    Can this relation be sorted on this EC before the final output step?
 *
 * To succeed, we must find an EC member that prepare_sort_from_pathkeys knows
 * how to sort on, given the rel's reltarget as input.  There are also a few
 * additional constraints based on the fact that the desired sort will be done
 * "early", within the scan/join part of the plan.  Also, non-parallel-safe
 * expressions are ignored if 'require_parallel_safe'.
 *
 * At some point we might want to return the identified EquivalenceMember,
 * but for now, callers only want to know if there is one.
 */
#[allow(unreachable_code)]
pub unsafe fn relation_can_be_sorted_early(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ec: *mut EquivalenceClass,
    require_parallel_safe: bool,
) -> bool {
    let target: *mut PathTarget = (*rel).reltarget;
    let mut em: *mut EquivalenceMember;

    /*
     * Reject volatile ECs immediately; such sorts must always be postponed.
     */
    if (*ec).ec_has_volatile {
        return false;
    }

    /*
     * Try to find an EM directly matching some reltarget member.
     */
    foreach!(lc, (*target).exprs, {
        let targetexpr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;

        em = find_ec_member_matching_expr(ec, targetexpr, (*rel).relids);
        if em.is_null() {
            continue;
        }

        /*
         * Reject expressions involving set-returning functions, as those
         * can't be computed early either.  (Note: this test and the following
         * one are effectively checking properties of targetexpr, so there's
         * no point in asking whether some other EC member would be better.)
         */
        if expression_returns_set((*em).em_expr as *const Node) {
            continue;
        }

        /*
         * If requested, reject expressions that are not parallel-safe.  We
         * check this last because it's a rather expensive test.
         */
        if require_parallel_safe && !is_parallel_safe(root, (*em).em_expr as *const Node) {
            continue;
        }

        return true;
    });

    /*
     * Try to find an expression computable from the reltarget.
     */
    em = find_computable_ec_member(
        root,
        ec,
        (*target).exprs,
        (*rel).relids,
        require_parallel_safe,
    );
    if em.is_null() {
        return false;
    }

    /*
     * Reject expressions involving set-returning functions, as those can't be
     * computed early either.  (There's no point in looking for another EC
     * member in this case; since SRFs can't appear in WHERE, they cannot
     * belong to multi-member ECs.)
     */
    if expression_returns_set((*em).em_expr as *const Node) {
        return false;
    }

    true
}

/*
 * generate_base_implied_equalities
 *    Generate any restriction clauses that we can deduce from equivalence
 *    classes.
 *
 * When an EC contains pseudoconstants, our strategy is to generate
 * "member = const1" clauses where const1 is the first constant member, for
 * every other member (including other constants).  If we are able to do this
 * then we don't need any "var = var" comparisons because we've successfully
 * constrained all the vars at their points of creation.  If we fail to
 * generate any of these clauses due to lack of cross-type operators, we fall
 * back to the "ec_broken" strategy described below.  (XXX if there are
 * multiple constants of different types, it's possible that we might succeed
 * in forming all the required clauses if we started from a different const
 * member; but this seems a sufficiently hokey corner case to not be worth
 * spending lots of cycles on.)
 *
 * For ECs that contain no pseudoconstants, we generate derived clauses
 * "member1 = member2" for each pair of members belonging to the same base
 * relation (actually, if there are more than two for the same base relation,
 * we only need enough clauses to link each to each other).  This provides
 * the base case for the recursion: each row emitted by a base relation scan
 * will constrain all computable members of the EC to be equal.  As each
 * join path is formed, we'll add additional derived clauses on-the-fly
 * to maintain this invariant (see generate_join_implied_equalities).
 *
 * If the opfamilies used by the EC do not provide complete sets of cross-type
 * equality operators, it is possible that we will fail to generate a clause
 * that must be generated to maintain the invariant.  (An example: given
 * "WHERE a.x = b.y AND b.y = a.z", the scheme breaks down if we cannot
 * generate "a.x = a.z" as a restriction clause for A.)  In this case we mark
 * the EC "ec_broken" and fall back to regurgitating its original source
 * RestrictInfos at appropriate times.  We do not try to retract any derived
 * clauses already generated from the broken EC, so the resulting plan could
 * be poor due to bad selectivity estimates caused by redundant clauses.  But
 * the correct solution to that is to fix the opfamilies ...
 *
 * Equality clauses derived by this function are passed off to
 * process_implied_equality (in plan/initsplan.c) to be inserted into the
 * restrictinfo datastructures.  Note that this must be called after initial
 * scanning of the quals and before Path construction begins.
 *
 * We make no attempt to avoid generating duplicate RestrictInfos here: we
 * don't search existing source or derived clauses in the EC for matches.  It
 * doesn't really seem worth the trouble to do so.
 */
pub unsafe fn generate_base_implied_equalities(root: *mut PlannerInfo) {
    let mut ec_index: i32;

    /*
     * At this point, we're done absorbing knowledge of equivalences in the
     * query, so no further EC merging should happen, and ECs remaining in the
     * eq_classes list can be considered canonical.  (But note that it's still
     * possible for new single-member ECs to be added through
     * get_eclass_for_sort_expr().)
     */
    (*root).ec_merging_done = true;

    ec_index = 0;
    foreach!(lc, (*root).eq_classes, {
        let ec: *mut EquivalenceClass = lfirst(current_cell!(lc)) as *mut EquivalenceClass;
        let mut can_generate_joinclause = false;
        let mut i: i32;

        Assert!((*ec).ec_merged.is_null()); /* else shouldn't be in list */
        Assert!(!(*ec).ec_broken); /* not yet anyway... */

        /*
         * Generate implied equalities that are restriction clauses.
         * Single-member ECs won't generate any deductions, either here or at
         * the join level.
         */
        if list_length((*ec).ec_members) > 1 {
            if (*ec).ec_has_const {
                generate_base_implied_equalities_const(root, ec);
            } else {
                generate_base_implied_equalities_no_const(root, ec);
            }

            /* Recover if we failed to generate required derived clauses */
            if (*ec).ec_broken {
                generate_base_implied_equalities_broken(root, ec);
            }

            /* Detect whether this EC might generate join clauses */
            can_generate_joinclause = bms_membership((*ec).ec_relids) == BMS_MULTIPLE;
        }

        /*
         * Mark the base rels cited in each eclass (which should all exist by
         * now) with the eq_classes indexes of all eclasses mentioning them.
         * This will let us avoid searching in subsequent lookups.  While
         * we're at it, we can mark base rels that have pending eclass joins;
         * this is a cheap version of has_relevant_eclass_joinclause().
         */
        i = -1;
        loop {
            i = bms_next_member((*ec).ec_relids, i);
            if i <= 0 {
                break;
            }

            let rel: *mut RelOptInfo = *(*root).simple_rel_array.add(i as usize);

            /* ignore the RTE_GROUP RTE */
            if i == (*root).group_rtindex as i32 {
                continue;
            }

            if rel.is_null() {
                /* must be an outer join */
                Assert!(bms_is_member(i, (*root).outer_join_rels));
                continue;
            }

            Assert!((*rel).reloptkind == crate::nodes::pathnodes::RelOptKind::RELOPT_BASEREL);

            (*rel).eclass_indexes = bms_add_member((*rel).eclass_indexes, ec_index);

            if can_generate_joinclause {
                (*rel).has_eclass_joins = true;
            }
        }

        ec_index += 1;
    });
}

/*
 * generate_base_implied_equalities when EC contains pseudoconstant(s)
 */
unsafe fn generate_base_implied_equalities_const(root: *mut PlannerInfo, ec: *mut EquivalenceClass) {
    let mut const_em: *mut EquivalenceMember = core::ptr::null_mut();

    /*
     * In the trivial case where we just had one "var = const" clause, push
     * the original clause back into the main planner machinery.  There is
     * nothing to be gained by doing it differently, and we save the effort to
     * re-build and re-analyze an equality clause that will be exactly
     * equivalent to the old one.
     */
    if list_length((*ec).ec_members) == 2 && list_length((*ec).ec_sources) == 1 {
        let restrictinfo: *mut RestrictInfo =
            linitial((*ec).ec_sources) as *mut RestrictInfo;
        distribute_restrictinfo_to_rels(root, restrictinfo);
        return;
    }

    /* We don't expect any children yet */
    Assert!((*ec).ec_childmembers.is_null());

    /*
     * Find the constant member to use.  We prefer an actual constant to
     * pseudo-constants (such as Params), because the constraint exclusion
     * machinery might be able to exclude relations on the basis of generated
     * "var = const" equalities, but "var = param" won't work for that.
     */
    foreach!(lc, (*ec).ec_members, {
        let cur_em: *mut EquivalenceMember = lfirst(current_cell!(lc)) as *mut EquivalenceMember;
        if (*cur_em).em_is_const {
            const_em = cur_em;
            if IsA!((*cur_em).em_expr, T_Const) {
                break;
            }
        }
    });
    Assert!(!const_em.is_null());

    /* Generate a derived equality against each other member */
    foreach!(lc, (*ec).ec_members, {
        let cur_em: *mut EquivalenceMember = lfirst(current_cell!(lc)) as *mut EquivalenceMember;

        /* Child members should not exist in ec_members */
        Assert!(!(*cur_em).em_is_child);
        if cur_em == const_em {
            continue;
        }
        let eq_op: Oid = select_equality_operator(ec, (*cur_em).em_datatype, (*const_em).em_datatype);
        if !OidIsValid(eq_op) {
            /* failed... */
            (*ec).ec_broken = true;
            break;
        }

        /*
         * We use the constant's em_jdomain as qualscope, so that if the
         * generated clause is variable-free (i.e, both EMs are consts) it
         * will be enforced at the join domain level.
         */
        let rinfo: *mut RestrictInfo = process_implied_equality(
            root,
            eq_op,
            (*ec).ec_collation,
            (*cur_em).em_expr,
            (*const_em).em_expr,
            (*(*const_em).em_jdomain).jd_relids,
            (*ec).ec_min_security,
            (*cur_em).em_is_const,
        );

        /*
         * If the clause didn't degenerate to a constant, fill in the correct
         * markings for a mergejoinable clause, and save it as a derived
         * clause. (We will not re-use such clauses directly, but selectivity
         * estimation may consult those later.  Note that this use of derived
         * clauses does not overlap with its use for join clauses, since we
         * never generate join clauses from an ec_has_const eclass.)
         */
        if !rinfo.is_null() && !(*rinfo).mergeopfamilies.is_null() {
            /* it's not redundant, so don't set parent_ec */
            (*rinfo).left_ec = ec;
            (*rinfo).right_ec = ec;
            (*rinfo).left_em = cur_em;
            (*rinfo).right_em = const_em;
            ec_add_derived_clause(ec, rinfo);
        }
    });
}

/*
 * generate_base_implied_equalities when EC contains no pseudoconstants
 */
unsafe fn generate_base_implied_equalities_no_const(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
) {
    /*
     * We scan the EC members once and track the last-seen member for each
     * base relation.  When we see another member of the same base relation,
     * we generate "prev_em = cur_em".  This results in the minimum number of
     * derived clauses, but it's possible that it will fail when a different
     * ordering would succeed.  XXX FIXME: use a UNION-FIND algorithm similar
     * to the way we build merged ECs.  (Use a list-of-lists for each rel.)
     */
    let prev_ems: *mut *mut EquivalenceMember = palloc0(
        (*root).simple_rel_array_size as usize * size_of::<*mut EquivalenceMember>(),
    ) as *mut *mut EquivalenceMember;

    /* We don't expect any children yet */
    Assert!((*ec).ec_childmembers.is_null());

    foreach!(lc, (*ec).ec_members, {
        let cur_em: *mut EquivalenceMember = lfirst(current_cell!(lc)) as *mut EquivalenceMember;
        let mut relid: i32 = 0;

        /* Child members should not exist in ec_members */
        Assert!(!(*cur_em).em_is_child);

        if !bms_get_singleton_member((*cur_em).em_relids, &mut relid) {
            continue;
        }
        Assert!(relid < (*root).simple_rel_array_size as i32);

        let prev_em_slot: *mut *mut EquivalenceMember = prev_ems.add(relid as usize);
        if !(*prev_em_slot).is_null() {
            let prev_em: *mut EquivalenceMember = *prev_em_slot;

            let eq_op: Oid = select_equality_operator(ec, (*prev_em).em_datatype, (*cur_em).em_datatype);
            if !OidIsValid(eq_op) {
                /* failed... */
                (*ec).ec_broken = true;
                break;
            }

            /*
             * The expressions aren't constants, so the passed qualscope will
             * never be used to place the generated clause.  We just need to
             * be sure it covers both expressions, which em_relids should do.
             */
            let rinfo: *mut RestrictInfo = process_implied_equality(
                root,
                eq_op,
                (*ec).ec_collation,
                (*prev_em).em_expr,
                (*cur_em).em_expr,
                (*cur_em).em_relids,
                (*ec).ec_min_security,
                false,
            );

            /*
             * If the clause didn't degenerate to a constant, fill in the
             * correct markings for a mergejoinable clause.  We don't record
             * it as a derived clause, since we don't currently need to
             * re-find such clauses, and don't want to clutter the
             * derived-clause set with non-join clauses.
             */
            if !rinfo.is_null() && !(*rinfo).mergeopfamilies.is_null() {
                /* it's not redundant, so don't set parent_ec */
                (*rinfo).left_ec = ec;
                (*rinfo).right_ec = ec;
                (*rinfo).left_em = prev_em;
                (*rinfo).right_em = cur_em;
            }
        }
        *prev_em_slot = cur_em;
    });

    pfree(prev_ems as *mut c_void);

    /*
     * We also have to make sure that all the Vars used in the member clauses
     * will be available at any join node we might try to reference them at.
     * For the moment we force all the Vars to be available at all join nodes
     * for this eclass.  Perhaps this could be improved by doing some
     * pre-analysis of which members we prefer to join, but it's no worse than
     * what happened in the pre-8.3 code.  (Note: rebuild_eclass_attr_needed
     * needs to match this code.)
     */
    foreach!(lc, (*ec).ec_members, {
        let cur_em: *mut EquivalenceMember = lfirst(current_cell!(lc)) as *mut EquivalenceMember;
        let vars: *mut List = pull_var_clause(
            (*cur_em).em_expr as *const Node,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        add_vars_to_targetlist(root, vars, (*ec).ec_relids);
        list_free(vars);
    });
}

/*
 * generate_base_implied_equalities cleanup after failure
 *
 * What we must do here is push any zero- or one-relation source RestrictInfos
 * of the EC back into the main restrictinfo datastructures.  Multi-relation
 * clauses will be regurgitated later by generate_join_implied_equalities().
 * (We do it this way to maintain continuity with the case that ec_broken
 * becomes set only after we've gone up a join level or two.)  However, for
 * an EC that contains constants, we can adopt a simpler strategy and just
 * throw back all the source RestrictInfos immediately; that works because
 * we know that such an EC can't become broken later.  (This rule justifies
 * ignoring ec_has_const ECs in generate_join_implied_equalities, even when
 * they are broken.)
 */
unsafe fn generate_base_implied_equalities_broken(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
) {
    foreach!(lc, (*ec).ec_sources, {
        let restrictinfo: *mut RestrictInfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;

        if (*ec).ec_has_const
            || bms_membership((*restrictinfo).required_relids) != BMS_MULTIPLE
        {
            distribute_restrictinfo_to_rels(root, restrictinfo);
        }
    });
}

// ===========================================================================
// Part 3
// ===========================================================================

/*
 * generate_join_implied_equalities
 *    Generate any join clauses that we can deduce from equivalence classes.
 *
 * At a join node, we must enforce restriction clauses sufficient to ensure
 * that all equivalence-class members computable at that node are equal.
 * Since the set of clauses to enforce can vary depending on which subset
 * relations are the inputs, we have to compute this afresh for each join
 * relation pair.  Hence a fresh List of RestrictInfo nodes is built and
 * passed back on each call.
 *
 * In addition to its use at join nodes, this can be applied to generate
 * eclass-based join clauses for use in a parameterized scan of a base rel.
 * The reason for the asymmetry of specifying the inner rel as a RelOptInfo
 * and the outer rel by Relids is that this usage occurs before we have
 * built any join RelOptInfos.
 *
 * An annoying special case for parameterized scans is that the inner rel can
 * be an appendrel child (an "other rel").  In this case we must generate
 * appropriate clauses using child EC members.  add_child_rel_equivalences
 * must already have been done for the child rel.
 *
 * The results are sufficient for use in merge, hash, and plain nestloop join
 * methods.  We do not worry here about selecting clauses that are optimal
 * for use in a parameterized indexscan.  indxpath.c makes its own selections
 * of clauses to use, and if the ones we pick here are redundant with those,
 * the extras will be eliminated at createplan time, using the parent_ec
 * markers that we provide (see is_redundant_derived_clause()).
 *
 * Because the same join clauses are likely to be needed multiple times as
 * we consider different join paths, we avoid generating multiple copies:
 * whenever we select a particular pair of EquivalenceMembers to join,
 * we check to see if the pair matches any original clause (in ec_sources)
 * or previously-built derived clause.  This saves memory and allows
 * re-use of information cached in RestrictInfos.  We also avoid generating
 * commutative duplicates, i.e. if the algorithm selects "a.x = b.y" but
 * we already have "b.y = a.x", we return the existing clause.
 *
 * If we are considering an outer join, sjinfo is the associated OJ info,
 * otherwise it can be NULL.
 *
 * join_relids should always equal bms_union(outer_relids, inner_rel->relids)
 * plus whatever add_outer_joins_to_relids() would add.  We could simplify
 * this function's API by computing it internally, but most callers have the
 * value at hand anyway.
 */
pub unsafe fn generate_join_implied_equalities(
    root: *mut PlannerInfo,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
) -> *mut List {
    let mut result: *mut List = NIL;
    let inner_relids: Relids = (*inner_rel).relids;
    let nominal_inner_relids: Relids;
    let nominal_join_relids: Relids;
    let matching_ecs: *mut Bitmapset;
    let mut i: i32;

    /* If inner rel is a child, extra setup work is needed */
    if IS_OTHER_REL(inner_rel) {
        Assert!(!bms_is_empty((*inner_rel).top_parent_relids));

        /* Fetch relid set for the topmost parent rel */
        nominal_inner_relids = (*inner_rel).top_parent_relids;
        /* ECs will be marked with the parent's relid, not the child's */
        let mut nom_jr = bms_union(outer_relids, nominal_inner_relids);
        nom_jr = add_outer_joins_to_relids(root, nom_jr, sjinfo, core::ptr::null_mut());
        nominal_join_relids = nom_jr;
    } else {
        nominal_inner_relids = inner_relids;
        nominal_join_relids = join_relids;
    }

    /*
     * Examine all potentially-relevant eclasses.
     *
     * If we are considering an outer join, we must include "join" clauses
     * that mention either input rel plus the outer join's relid; these
     * represent post-join filter clauses that have to be applied at this
     * join.  We don't have infrastructure that would let us identify such
     * eclasses cheaply, so just fall back to considering all eclasses
     * mentioning anything in nominal_join_relids.
     *
     * At inner joins, we can be smarter: only consider eclasses mentioning
     * both input rels.
     */
    if !sjinfo.is_null() && (*sjinfo).ojrelid != 0 {
        matching_ecs = get_eclass_indexes_for_relids(root, nominal_join_relids);
    } else {
        matching_ecs = get_common_eclass_indexes(root, nominal_inner_relids, outer_relids);
    }

    i = -1;
    loop {
        i = bms_next_member(matching_ecs, i);
        if i < 0 {
            break;
        }

        let ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i as i32) as *mut EquivalenceClass;
        let mut sublist: *mut List = NIL;

        /* ECs containing consts do not need any further enforcement */
        if (*ec).ec_has_const {
            continue;
        }

        /* Single-member ECs won't generate any deductions */
        if list_length((*ec).ec_members) <= 1 {
            continue;
        }

        /* Sanity check that this eclass overlaps the join */
        Assert!(bms_overlap((*ec).ec_relids, nominal_join_relids));

        if !(*ec).ec_broken {
            sublist = generate_join_implied_equalities_normal(
                root,
                ec,
                join_relids,
                outer_relids,
                inner_relids,
            );
        }

        /* Recover if we failed to generate required derived clauses */
        if (*ec).ec_broken {
            sublist = generate_join_implied_equalities_broken(
                root,
                ec,
                nominal_join_relids,
                outer_relids,
                nominal_inner_relids,
                inner_rel,
            );
        }

        result = list_concat(result, sublist);
    }

    result
}

/*
 * generate_join_implied_equalities_for_ecs
 *    As above, but consider only the listed ECs.
 *
 * For the sole current caller, we can assume sjinfo == NULL, that is we are
 * not interested in outer-join filter clauses.  This might need to change
 * in future.
 */
pub unsafe fn generate_join_implied_equalities_for_ecs(
    root: *mut PlannerInfo,
    eclasses: *mut List,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: *mut RelOptInfo,
) -> *mut List {
    let mut result: *mut List = NIL;
    let inner_relids: Relids = (*inner_rel).relids;
    let nominal_inner_relids: Relids;
    let nominal_join_relids: Relids;

    /* If inner rel is a child, extra setup work is needed */
    if IS_OTHER_REL(inner_rel) {
        Assert!(!bms_is_empty((*inner_rel).top_parent_relids));

        /* Fetch relid set for the topmost parent rel */
        nominal_inner_relids = (*inner_rel).top_parent_relids;
        /* ECs will be marked with the parent's relid, not the child's */
        nominal_join_relids = bms_union(outer_relids, nominal_inner_relids);
    } else {
        nominal_inner_relids = inner_relids;
        nominal_join_relids = join_relids;
    }

    foreach!(lc, eclasses, {
        let ec: *mut EquivalenceClass = lfirst(current_cell!(lc)) as *mut EquivalenceClass;
        let mut sublist: *mut List = NIL;

        /* ECs containing consts do not need any further enforcement */
        if (*ec).ec_has_const {
            continue;
        }

        /* Single-member ECs won't generate any deductions */
        if list_length((*ec).ec_members) <= 1 {
            continue;
        }

        /* We can quickly ignore any that don't overlap the join, too */
        if !bms_overlap((*ec).ec_relids, nominal_join_relids) {
            continue;
        }

        if !(*ec).ec_broken {
            sublist = generate_join_implied_equalities_normal(
                root,
                ec,
                join_relids,
                outer_relids,
                inner_relids,
            );
        }

        /* Recover if we failed to generate required derived clauses */
        if (*ec).ec_broken {
            sublist = generate_join_implied_equalities_broken(
                root,
                ec,
                nominal_join_relids,
                outer_relids,
                nominal_inner_relids,
                inner_rel,
            );
        }

        result = list_concat(result, sublist);
    });

    result
}

/*
 * generate_join_implied_equalities for a still-valid EC
 */
unsafe fn generate_join_implied_equalities_normal(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    join_relids: Relids,
    outer_relids: Relids,
    inner_relids: Relids,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut new_members: *mut List = NIL;
    let mut outer_members: *mut List = NIL;
    let mut inner_members: *mut List = NIL;
    let mut it: EquivalenceMemberIterator = core::mem::zeroed();
    let mut cur_em: *mut EquivalenceMember;

    /*
     * First, scan the EC to identify member values that are computable at the
     * outer rel, at the inner rel, or at this relation but not in either
     * input rel.  The outer-rel members should already be enforced equal,
     * likewise for the inner-rel members.  We'll need to create clauses to
     * enforce that any newly computable members are all equal to each other
     * as well as to at least one input member, plus enforce at least one
     * outer-rel member equal to at least one inner-rel member.
     */
    setup_eclass_member_iterator(&mut it, ec, join_relids);
    loop {
        cur_em = eclass_member_iterator_next(&mut it);
        if cur_em.is_null() {
            break;
        }

        /*
         * We don't need to check explicitly for child EC members.  This test
         * against join_relids will cause them to be ignored except when
         * considering a child inner rel, which is what we want.
         */
        if !bms_is_subset((*cur_em).em_relids, join_relids) {
            continue; /* not computable yet, or wrong child */
        }

        if bms_is_subset((*cur_em).em_relids, outer_relids) {
            outer_members = lappend(outer_members, cur_em as *mut c_void);
        } else if bms_is_subset((*cur_em).em_relids, inner_relids) {
            inner_members = lappend(inner_members, cur_em as *mut c_void);
        } else {
            new_members = lappend(new_members, cur_em as *mut c_void);
        }
    }

    /*
     * First, select the joinclause if needed.  We can equate any one outer
     * member to any one inner member, but we have to find a datatype
     * combination for which an opfamily member operator exists.  If we have
     * choices, we prefer simple Var members (possibly with RelabelType) since
     * these are (a) cheapest to compute at runtime and (b) most likely to
     * have useful statistics. Also, prefer operators that are also
     * hashjoinable.
     */
    if !outer_members.is_null() && !inner_members.is_null() {
        let mut best_outer_em: *mut EquivalenceMember = core::ptr::null_mut();
        let mut best_inner_em: *mut EquivalenceMember = core::ptr::null_mut();
        let mut best_eq_op: Oid = INVALID_OID;
        let mut best_score: i32 = -1;
        let rinfo: *mut RestrictInfo;

        'outer: {
            foreach!(lc1, outer_members, {
                let outer_em: *mut EquivalenceMember =
                    lfirst(current_cell!(lc1)) as *mut EquivalenceMember;

                foreach!(lc2, inner_members, {
                    let inner_em: *mut EquivalenceMember =
                        lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

                    let eq_op: Oid = select_equality_operator(
                        ec,
                        (*outer_em).em_datatype,
                        (*inner_em).em_datatype,
                    );
                    if !OidIsValid(eq_op) {
                        continue;
                    }
                    let mut score: i32 = 0;
                    if IsA!((*outer_em).em_expr, T_Var)
                        || (IsA!((*outer_em).em_expr, T_RelabelType)
                            && IsA!((*((*outer_em).em_expr as *mut RelabelType)).arg, T_Var))
                    {
                        score += 1;
                    }
                    if IsA!((*inner_em).em_expr, T_Var)
                        || (IsA!((*inner_em).em_expr, T_RelabelType)
                            && IsA!((*((*inner_em).em_expr as *mut RelabelType)).arg, T_Var))
                    {
                        score += 1;
                    }
                    if op_hashjoinable(eq_op, exprType((*outer_em).em_expr as *const Node)) {
                        score += 1;
                    }
                    if score > best_score {
                        best_outer_em = outer_em;
                        best_inner_em = inner_em;
                        best_eq_op = eq_op;
                        best_score = score;
                        if best_score == 3 {
                            break; /* no need to look further */
                        }
                    }
                });
                if best_score == 3 {
                    break 'outer; /* no need to look further */
                }
            });
        }

        if best_score < 0 {
            /* failed... */
            (*ec).ec_broken = true;
            return NIL;
        }

        /*
         * Create clause, setting parent_ec to mark it as redundant with other
         * joinclauses
         */
        rinfo = create_join_clause(root, ec, best_eq_op, best_outer_em, best_inner_em, ec);

        result = lappend(result, rinfo as *mut c_void);
    }

    /*
     * Now deal with building restrictions for any expressions that involve
     * Vars from both sides of the join.  We have to equate all of these to
     * each other as well as to at least one old member (if any).
     *
     * XXX as in generate_base_implied_equalities_no_const, we could be a lot
     * smarter here to avoid unnecessary failures in cross-type situations.
     * For now, use the same left-to-right method used there.
     */
    if !new_members.is_null() {
        let old_members: *mut List = list_concat(outer_members, inner_members);
        let mut prev_em: *mut EquivalenceMember = core::ptr::null_mut();
        let mut rinfo: *mut RestrictInfo;

        /* For now, arbitrarily take the first old_member as the one to use */
        if !old_members.is_null() {
            new_members = lappend(new_members, linitial(old_members));
        }

        foreach!(lc1, new_members, {
            cur_em = lfirst(current_cell!(lc1)) as *mut EquivalenceMember;

            if !prev_em.is_null() {
                let eq_op: Oid = select_equality_operator(
                    ec,
                    (*prev_em).em_datatype,
                    (*cur_em).em_datatype,
                );
                if !OidIsValid(eq_op) {
                    /* failed... */
                    (*ec).ec_broken = true;
                    return NIL;
                }
                /* do NOT set parent_ec, this qual is not redundant! */
                rinfo = create_join_clause(root, ec, eq_op, prev_em, cur_em, core::ptr::null_mut());

                result = lappend(result, rinfo as *mut c_void);
            }
            prev_em = cur_em;
        });
    }

    result
}

/*
 * generate_join_implied_equalities cleanup after failure
 *
 * Return any original RestrictInfos that are enforceable at this join.
 *
 * In the case of a child inner relation, we have to translate the
 * original RestrictInfos from parent to child Vars.
 */
unsafe fn generate_join_implied_equalities_broken(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    nominal_join_relids: Relids,
    outer_relids: Relids,
    nominal_inner_relids: Relids,
    inner_rel: *mut RelOptInfo,
) -> *mut List {
    let mut result: *mut List = NIL;

    foreach!(lc, (*ec).ec_sources, {
        let restrictinfo: *mut RestrictInfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;
        let clause_relids: Relids = (*restrictinfo).required_relids;

        if bms_is_subset(clause_relids, nominal_join_relids)
            && !bms_is_subset(clause_relids, outer_relids)
            && !bms_is_subset(clause_relids, nominal_inner_relids)
        {
            result = lappend(result, restrictinfo as *mut c_void);
        }
    });

    /*
     * If we have to translate, just brute-force apply adjust_appendrel_attrs
     * to all the RestrictInfos at once.  This will result in returning
     * RestrictInfos that are not included in EC's derived clauses, but there
     * shouldn't be any duplication, and it's a sufficiently narrow corner
     * case that we shouldn't sweat too much over it anyway.
     *
     * Since inner_rel might be an indirect descendant of the baserel
     * mentioned in the ec_sources clauses, we have to be prepared to apply
     * multiple levels of Var translation.
     */
    if IS_OTHER_REL(inner_rel) && !result.is_null() {
        result = adjust_appendrel_attrs_multilevel(
            root,
            result as *mut Node,
            inner_rel,
            (*inner_rel).top_parent,
        ) as *mut List;
    }

    result
}

/*
 * select_equality_operator
 *    Select a suitable equality operator for comparing two EC members
 *
 * Returns InvalidOid if no operator can be found for this datatype combination
 */
unsafe fn select_equality_operator(ec: *mut EquivalenceClass, lefttype: Oid, righttype: Oid) -> Oid {
    foreach!(lc, (*ec).ec_opfamilies, {
        let opfamily: Oid = lfirst_oid(current_cell!(lc));

        let opno: Oid =
            get_opfamily_member_for_cmptype(opfamily, lefttype, righttype, COMPARE_EQ);
        if !OidIsValid(opno) {
            continue;
        }
        /* If no barrier quals in query, don't worry about leaky operators */
        if (*ec).ec_max_security == 0 {
            return opno;
        }
        /* Otherwise, insist that selected operators be leakproof */
        if get_func_leakproof(get_opcode(opno)) {
            return opno;
        }
    });

    INVALID_OID
}

/*
 * create_join_clause
 *    Find or make a RestrictInfo comparing the two given EC members
 *    with the given operator (or, possibly, its commutator, because
 *    the ordering of the operands in the result is not guaranteed).
 *
 * parent_ec is either equal to ec (if the clause is a potentially-redundant
 * join clause) or NULL (if not).  We have to treat this as part of the
 * match requirements --- it's possible that a clause comparing the same two
 * EMs is a join clause in one join path and a restriction clause in another.
 */
unsafe fn create_join_clause(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    opno: Oid,
    leftem: *mut EquivalenceMember,
    rightem: *mut EquivalenceMember,
    parent_ec: *mut EquivalenceClass,
) -> *mut RestrictInfo {
    let rinfo: *mut RestrictInfo;
    let mut parent_rinfo: *mut RestrictInfo = core::ptr::null_mut();
    let oldcontext: *mut c_void;

    let existing = ec_search_clause_for_ems(root, ec, leftem, rightem, parent_ec);
    if !existing.is_null() {
        return existing;
    }

    /*
     * Not there, so build it, in planner context so we can re-use it. (Not
     * important in normal planning, but definitely so in GEQO.)
     */
    oldcontext = MemoryContextSwitchTo((*root).planner_cxt as *mut c_void);

    /*
     * If either EM is a child, recursively create the corresponding
     * parent-to-parent clause, so that we can duplicate its rinfo_serial.
     */
    if (*leftem).em_is_child || (*rightem).em_is_child {
        let leftp: *mut EquivalenceMember = if !(*leftem).em_parent.is_null() {
            (*leftem).em_parent
        } else {
            leftem
        };
        let rightp: *mut EquivalenceMember = if !(*rightem).em_parent.is_null() {
            (*rightem).em_parent
        } else {
            rightem
        };

        parent_rinfo = create_join_clause(root, ec, opno, leftp, rightp, parent_ec);
    }

    let rinfo_new: *mut RestrictInfo = build_implied_join_equality(
        root,
        opno,
        (*ec).ec_collation,
        (*leftem).em_expr,
        (*rightem).em_expr,
        bms_union((*leftem).em_relids, (*rightem).em_relids),
        (*ec).ec_min_security,
    );

    /*
     * If either EM is a child, force the clause's clause_relids to include
     * the relid(s) of the child rel.  In normal cases it would already, but
     * not if we are considering appendrel child relations with pseudoconstant
     * translated variables (i.e., UNION ALL sub-selects with constant output
     * items).  We must do this so that join_clause_is_movable_into() will
     * think that the clause should be evaluated at the correct place.
     */
    if (*leftem).em_is_child {
        (*rinfo_new).clause_relids =
            bms_add_members((*rinfo_new).clause_relids, (*leftem).em_relids);
    }
    if (*rightem).em_is_child {
        (*rinfo_new).clause_relids =
            bms_add_members((*rinfo_new).clause_relids, (*rightem).em_relids);
    }

    /* If it's a child clause, copy the parent's rinfo_serial */
    if !parent_rinfo.is_null() {
        (*rinfo_new).rinfo_serial = (*parent_rinfo).rinfo_serial;
    }

    /* Mark the clause as redundant, or not */
    (*rinfo_new).parent_ec = parent_ec;

    /*
     * We know the correct values for left_ec/right_ec, ie this particular EC,
     * so we can just set them directly instead of forcing another lookup.
     */
    (*rinfo_new).left_ec = ec;
    (*rinfo_new).right_ec = ec;

    /* Mark it as usable with these EMs */
    (*rinfo_new).left_em = leftem;
    (*rinfo_new).right_em = rightem;
    /* and save it for possible re-use */
    ec_add_derived_clause(ec, rinfo_new);

    MemoryContextSwitchTo(oldcontext);

    rinfo_new
}

// Use rinfo binding to avoid re-bind after fn call
#[allow(unused_variables)]
fn _suppress_unused_rinfo() {}

// ===========================================================================
// Part 4
// ===========================================================================

/*
 * reconsider_outer_join_clauses
 *    Re-examine any outer-join clauses that were set aside by
 *    distribute_qual_to_rels(), and see if we can derive any
 *    EquivalenceClasses from them.  Then, if they were not made
 *    redundant, push them out into the regular join-clause lists.
 *
 * When we have mergejoinable clauses A = B that are outer-join clauses,
 * we can't blindly combine them with other clauses A = C to deduce B = C,
 * since in fact the "equality" A = B won't necessarily hold above the
 * outer join (one of the variables might be NULL instead).  Nonetheless
 * there are cases where we can add qual clauses using transitivity.
 *
 * One case that we look for here is an outer-join clause OUTERVAR = INNERVAR
 * for which there is also an equivalence clause OUTERVAR = CONSTANT.
 * It is safe and useful to push a clause INNERVAR = CONSTANT into the
 * evaluation of the inner (nullable) relation, because any inner rows not
 * meeting this condition will not contribute to the outer-join result anyway.
 * (Any outer rows they could join to will be eliminated by the pushed-down
 * equivalence clause.)
 *
 * Note that the above rule does not work for full outer joins; nor is it
 * very interesting to consider cases where the generated equivalence clause
 * would involve relations outside the outer join, since such clauses couldn't
 * be pushed into the inner side's scan anyway.  So the restriction to
 * outervar = pseudoconstant is not really giving up anything.
 *
 * For full-join cases, we can only do something useful if it's a FULL JOIN
 * USING and a merged column has an equivalence MERGEDVAR = CONSTANT.
 * By the time it gets here, the merged column will look like
 *    COALESCE(LEFTVAR, RIGHTVAR)
 * and we will have a full-join clause LEFTVAR = RIGHTVAR that we can match
 * the COALESCE expression to. In this situation we can push LEFTVAR = CONSTANT
 * and RIGHTVAR = CONSTANT into the input relations, since any rows not
 * meeting these conditions cannot contribute to the join result.
 *
 * Again, there isn't any traction to be gained by trying to deal with
 * clauses comparing a mergedvar to a non-pseudoconstant.  So we can make
 * use of the EquivalenceClasses to search for matching variables that were
 * equivalenced to constants.  The interesting outer-join clauses were
 * accumulated for us by distribute_qual_to_rels.
 *
 * When we find one of these cases, we implement the changes we want by
 * generating a new equivalence clause INNERVAR = CONSTANT (or LEFTVAR, etc)
 * and pushing it into the EquivalenceClass structures.  This is because we
 * may already know that INNERVAR is equivalenced to some other var(s), and
 * we'd like the constant to propagate to them too.  Note that it would be
 * unsafe to merge any existing EC for INNERVAR with the OUTERVAR's EC ---
 * that could result in propagating constant restrictions from
 * INNERVAR to OUTERVAR, which would be very wrong.
 *
 * It's possible that the INNERVAR is also an OUTERVAR for some other
 * outer-join clause, in which case the process can be repeated.  So we repeat
 * looping over the lists of clauses until no further deductions can be made.
 * Whenever we do make a deduction, we remove the generating clause from the
 * lists, since we don't want to make the same deduction twice.
 *
 * If we don't find any match for a set-aside outer join clause, we must
 * throw it back into the regular joinclause processing by passing it to
 * distribute_restrictinfo_to_rels().  If we do generate a derived clause,
 * however, the outer-join clause is redundant.  We must still put some
 * clause into the regular processing, because otherwise the join will be
 * seen as a clauseless join and avoided during join order searching.
 * We handle this by generating a constant-TRUE clause that is marked with
 * the same required_relids etc as the removed outer-join clause, thus
 * making it a join clause between the correct relations.
 */
pub unsafe fn reconsider_outer_join_clauses(root: *mut PlannerInfo) {
    let mut found: bool;

    /* Outer loop repeats until we find no more deductions */
    loop {
        found = false;

        /* Process the LEFT JOIN clauses */
        foreach!(cell, (*root).left_join_clauses, {
            let ojcinfo: *mut OuterJoinClauseInfo =
                lfirst(current_cell!(cell)) as *mut OuterJoinClauseInfo;

            if reconsider_outer_join_clause(root, ojcinfo, true) {
                let mut rinfo: *mut RestrictInfo = (*ojcinfo).rinfo;

                found = true;
                /* remove it from the list */
                (*root).left_join_clauses =
                    foreach_delete_current!((*root).left_join_clauses, cell);
                /* throw back a dummy replacement clause (see notes above) */
                rinfo = make_restrictinfo(
                    root,
                    makeBoolConst(true, false) as *mut Expr,
                    (*rinfo).is_pushed_down,
                    (*rinfo).has_clone,
                    (*rinfo).is_clone,
                    false, /* pseudoconstant */
                    0,     /* security_level */
                    (*rinfo).required_relids,
                    (*rinfo).incompatible_relids,
                    (*rinfo).outer_relids,
                );
                distribute_restrictinfo_to_rels(root, rinfo);
            }
        });

        /* Process the RIGHT JOIN clauses */
        foreach!(cell, (*root).right_join_clauses, {
            let ojcinfo: *mut OuterJoinClauseInfo =
                lfirst(current_cell!(cell)) as *mut OuterJoinClauseInfo;

            if reconsider_outer_join_clause(root, ojcinfo, false) {
                let mut rinfo: *mut RestrictInfo = (*ojcinfo).rinfo;

                found = true;
                /* remove it from the list */
                (*root).right_join_clauses =
                    foreach_delete_current!((*root).right_join_clauses, cell);
                /* throw back a dummy replacement clause (see notes above) */
                rinfo = make_restrictinfo(
                    root,
                    makeBoolConst(true, false) as *mut Expr,
                    (*rinfo).is_pushed_down,
                    (*rinfo).has_clone,
                    (*rinfo).is_clone,
                    false, /* pseudoconstant */
                    0,     /* security_level */
                    (*rinfo).required_relids,
                    (*rinfo).incompatible_relids,
                    (*rinfo).outer_relids,
                );
                distribute_restrictinfo_to_rels(root, rinfo);
            }
        });

        /* Process the FULL JOIN clauses */
        foreach!(cell, (*root).full_join_clauses, {
            let ojcinfo: *mut OuterJoinClauseInfo =
                lfirst(current_cell!(cell)) as *mut OuterJoinClauseInfo;

            if reconsider_full_join_clause(root, ojcinfo) {
                let mut rinfo: *mut RestrictInfo = (*ojcinfo).rinfo;

                found = true;
                /* remove it from the list */
                (*root).full_join_clauses =
                    foreach_delete_current!((*root).full_join_clauses, cell);
                /* throw back a dummy replacement clause (see notes above) */
                rinfo = make_restrictinfo(
                    root,
                    makeBoolConst(true, false) as *mut Expr,
                    (*rinfo).is_pushed_down,
                    (*rinfo).has_clone,
                    (*rinfo).is_clone,
                    false, /* pseudoconstant */
                    0,     /* security_level */
                    (*rinfo).required_relids,
                    (*rinfo).incompatible_relids,
                    (*rinfo).outer_relids,
                );
                distribute_restrictinfo_to_rels(root, rinfo);
            }
        });

        if !found {
            break;
        }
    }

    /* Now, any remaining clauses have to be thrown back */
    foreach!(cell, (*root).left_join_clauses, {
        let ojcinfo: *mut OuterJoinClauseInfo =
            lfirst(current_cell!(cell)) as *mut OuterJoinClauseInfo;
        distribute_restrictinfo_to_rels(root, (*ojcinfo).rinfo);
    });
    foreach!(cell, (*root).right_join_clauses, {
        let ojcinfo: *mut OuterJoinClauseInfo =
            lfirst(current_cell!(cell)) as *mut OuterJoinClauseInfo;
        distribute_restrictinfo_to_rels(root, (*ojcinfo).rinfo);
    });
    foreach!(cell, (*root).full_join_clauses, {
        let ojcinfo: *mut OuterJoinClauseInfo =
            lfirst(current_cell!(cell)) as *mut OuterJoinClauseInfo;
        distribute_restrictinfo_to_rels(root, (*ojcinfo).rinfo);
    });
}

/*
 * reconsider_outer_join_clauses for a single LEFT/RIGHT JOIN clause
 *
 * Returns true if we were able to propagate a constant through the clause.
 */
#[allow(unreachable_code)]
unsafe fn reconsider_outer_join_clause(
    root: *mut PlannerInfo,
    ojcinfo: *mut OuterJoinClauseInfo,
    outer_on_left: bool,
) -> bool {
    let rinfo: *mut RestrictInfo = (*ojcinfo).rinfo;
    let sjinfo: *mut SpecialJoinInfo = (*ojcinfo).sjinfo;
    let outervar: *mut Expr;
    let innervar: *mut Expr;
    let mut opno: Oid = 0;
    let mut collation: Oid = 0;
    let mut left_type: Oid = 0;
    let mut right_type: Oid = 0;
    let inner_datatype: Oid;
    let inner_relids: Relids;

    Assert!(is_opclause((*rinfo).clause as *const c_void));
    opno = (*((*rinfo).clause as *mut OpExpr)).opno;
    collation = (*((*rinfo).clause as *mut OpExpr)).inputcollid;

    /* Extract needed info from the clause */
    op_input_types(opno, &mut left_type, &mut right_type);
    if outer_on_left {
        outervar = get_leftop((*rinfo).clause) as *mut Expr;
        innervar = get_rightop((*rinfo).clause) as *mut Expr;
        inner_datatype = right_type;
        inner_relids = (*rinfo).right_relids;
    } else {
        outervar = get_rightop((*rinfo).clause) as *mut Expr;
        innervar = get_leftop((*rinfo).clause) as *mut Expr;
        inner_datatype = left_type;
        inner_relids = (*rinfo).left_relids;
    }

    /* Scan EquivalenceClasses for a match to outervar */
    foreach!(lc1, (*root).eq_classes, {
        let cur_ec: *mut EquivalenceClass = lfirst(current_cell!(lc1)) as *mut EquivalenceClass;
        let mut cur_match: bool;

        /* We don't expect any children yet */
        Assert!((*cur_ec).ec_childmembers.is_null());

        /* Ignore EC unless it contains pseudoconstants */
        if !(*cur_ec).ec_has_const {
            continue;
        }
        /* Never match to a volatile EC */
        if (*cur_ec).ec_has_volatile {
            continue;
        }
        /* It has to match the outer-join clause as to semantics, too */
        if collation != (*cur_ec).ec_collation {
            continue;
        }
        if !equal(
            (*rinfo).mergeopfamilies as *const c_void,
            (*cur_ec).ec_opfamilies as *const c_void,
        ) {
            continue;
        }
        /* Does it contain a match to outervar? */
        cur_match = false;
        foreach!(lc2, (*cur_ec).ec_members, {
            let cur_em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*cur_em).em_is_child);
            if equal(outervar as *const c_void, (*cur_em).em_expr as *const c_void) {
                cur_match = true;
                break;
            }
        });
        if !cur_match {
            continue; /* no match, so ignore this EC */
        }

        /*
         * Yes it does!  Try to generate a clause INNERVAR = CONSTANT for each
         * CONSTANT in the EC.  Note that we must succeed with at least one
         * constant before we can decide to throw away the outer-join clause.
         */
        cur_match = false;
        foreach!(lc2, (*cur_ec).ec_members, {
            let cur_em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            if !(*cur_em).em_is_const {
                continue; /* ignore non-const members */
            }
            let eq_op: Oid =
                select_equality_operator(cur_ec, inner_datatype, (*cur_em).em_datatype);
            if !OidIsValid(eq_op) {
                continue; /* can't generate equality */
            }
            let newrinfo: *mut RestrictInfo = build_implied_join_equality(
                root,
                eq_op,
                (*cur_ec).ec_collation,
                innervar,
                (*cur_em).em_expr,
                bms_copy(inner_relids),
                (*cur_ec).ec_min_security,
            );
            /* This equality holds within the OJ's child JoinDomain */
            let jdomain: *mut JoinDomain =
                find_join_domain(root, (*sjinfo).syn_righthand);
            if process_equivalence(root, &mut (newrinfo as *mut RestrictInfo), jdomain) {
                cur_match = true;
            }
        });

        /*
         * If we were able to equate INNERVAR to any constant, report success.
         * Otherwise, fall out of the search loop, since we know the OUTERVAR
         * appears in at most one EC.
         */
        if cur_match {
            return true;
        } else {
            break;
        }
    });

    false /* failed to make any deduction */
}

/*
 * reconsider_outer_join_clauses for a single FULL JOIN clause
 *
 * Returns true if we were able to propagate a constant through the clause.
 */
#[allow(unreachable_code)]
unsafe fn reconsider_full_join_clause(
    root: *mut PlannerInfo,
    ojcinfo: *mut OuterJoinClauseInfo,
) -> bool {
    let rinfo: *mut RestrictInfo = (*ojcinfo).rinfo;
    let sjinfo: *mut SpecialJoinInfo = (*ojcinfo).sjinfo;
    let fjrelids: Relids = bms_make_singleton((*sjinfo).ojrelid as i32);
    let leftvar: *mut Expr;
    let rightvar: *mut Expr;
    let mut opno: Oid = 0;
    let mut collation: Oid = 0;
    let mut left_type: Oid = 0;
    let mut right_type: Oid = 0;
    let left_relids: Relids;
    let right_relids: Relids;

    /* Extract needed info from the clause */
    Assert!(is_opclause((*rinfo).clause as *const c_void));
    opno = (*((*rinfo).clause as *mut OpExpr)).opno;
    collation = (*((*rinfo).clause as *mut OpExpr)).inputcollid;
    op_input_types(opno, &mut left_type, &mut right_type);
    leftvar = get_leftop((*rinfo).clause) as *mut Expr;
    rightvar = get_rightop((*rinfo).clause) as *mut Expr;
    left_relids = (*rinfo).left_relids;
    right_relids = (*rinfo).right_relids;

    foreach!(lc1, (*root).eq_classes, {
        let cur_ec: *mut EquivalenceClass = lfirst(current_cell!(lc1)) as *mut EquivalenceClass;
        let mut coal_em: *mut EquivalenceMember = core::ptr::null_mut();
        let mut cur_match: bool;
        let mut matchleft: bool;
        let mut matchright: bool;
        let mut coal_idx: i32 = -1;

        /* We don't expect any children yet */
        Assert!((*cur_ec).ec_childmembers.is_null());

        /* Ignore EC unless it contains pseudoconstants */
        if !(*cur_ec).ec_has_const {
            continue;
        }
        /* Never match to a volatile EC */
        if (*cur_ec).ec_has_volatile {
            continue;
        }
        /* It has to match the outer-join clause as to semantics, too */
        if collation != (*cur_ec).ec_collation {
            continue;
        }
        if !equal(
            (*rinfo).mergeopfamilies as *const c_void,
            (*cur_ec).ec_opfamilies as *const c_void,
        ) {
            continue;
        }

        /*
         * Does it contain a COALESCE(leftvar, rightvar) construct?
         *
         * We can assume the COALESCE() inputs are in the same order as the
         * join clause, since both were automatically generated in the cases
         * we care about.
         *
         * XXX currently this may fail to match in cross-type cases because
         * the COALESCE will contain typecast operations while the join clause
         * may not (if there is a cross-type mergejoin operator available for
         * the two column types). Is it OK to strip implicit coercions from
         * the COALESCE arguments?
         */
        cur_match = false;
        foreach!(lc2, (*cur_ec).ec_members, {
            coal_em = lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*coal_em).em_is_child);
            if IsA!((*coal_em).em_expr, T_CoalesceExpr) {
                let cexpr: *mut CoalesceExpr = (*coal_em).em_expr as *mut CoalesceExpr;

                if list_length((*cexpr).args) != 2 {
                    continue;
                }
                let cfirst_raw: *mut Node = linitial((*cexpr).args) as *mut Node;
                let csecond_raw: *mut Node = lsecond((*cexpr).args) as *mut Node;

                /*
                 * The COALESCE arguments will be marked as possibly nulled by
                 * the full join, while we wish to generate clauses that apply
                 * to the join's inputs.  So we must strip the join from the
                 * nullingrels fields of cfirst/csecond before comparing them
                 * to leftvar/rightvar.
                 */
                let cfirst: *mut Node =
                    remove_nulling_relids(cfirst_raw, fjrelids, core::ptr::null_mut());
                let csecond: *mut Node =
                    remove_nulling_relids(csecond_raw, fjrelids, core::ptr::null_mut());

                if equal(leftvar as *const c_void, cfirst as *const c_void)
                    && equal(rightvar as *const c_void, csecond as *const c_void)
                {
                    coal_idx = foreach_current_index!(lc2) as i32;
                    cur_match = true;
                    break;
                }
            }
        });
        if !cur_match {
            continue; /* no match, so ignore this EC */
        }

        /*
         * Yes it does!  Try to generate clauses LEFTVAR = CONSTANT and
         * RIGHTVAR = CONSTANT for each CONSTANT in the EC.  Note that we must
         * succeed with at least one constant for each var before we can
         * decide to throw away the outer-join clause.
         */
        matchleft = false;
        matchright = false;
        foreach!(lc2, (*cur_ec).ec_members, {
            let cur_em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            if !(*cur_em).em_is_const {
                continue; /* ignore non-const members */
            }
            let mut eq_op: Oid =
                select_equality_operator(cur_ec, left_type, (*cur_em).em_datatype);
            if OidIsValid(eq_op) {
                let newrinfo: *mut RestrictInfo = build_implied_join_equality(
                    root,
                    eq_op,
                    (*cur_ec).ec_collation,
                    leftvar,
                    (*cur_em).em_expr,
                    bms_copy(left_relids),
                    (*cur_ec).ec_min_security,
                );
                /* This equality holds within the lefthand child JoinDomain */
                let jdomain: *mut JoinDomain =
                    find_join_domain(root, (*sjinfo).syn_lefthand);
                if process_equivalence(root, &mut (newrinfo as *mut RestrictInfo), jdomain) {
                    matchleft = true;
                }
            }
            eq_op = select_equality_operator(cur_ec, right_type, (*cur_em).em_datatype);
            if OidIsValid(eq_op) {
                let newrinfo: *mut RestrictInfo = build_implied_join_equality(
                    root,
                    eq_op,
                    (*cur_ec).ec_collation,
                    rightvar,
                    (*cur_em).em_expr,
                    bms_copy(right_relids),
                    (*cur_ec).ec_min_security,
                );
                /* This equality holds within the righthand child JoinDomain */
                let jdomain: *mut JoinDomain =
                    find_join_domain(root, (*sjinfo).syn_righthand);
                if process_equivalence(root, &mut (newrinfo as *mut RestrictInfo), jdomain) {
                    matchright = true;
                }
            }
        });

        /*
         * If we were able to equate both vars to constants, we're done, and
         * we can throw away the full-join clause as redundant.  Moreover, we
         * can remove the COALESCE entry from the EC, since the added
         * restrictions ensure it will always have the expected value. (We
         * don't bother trying to update ec_relids or ec_sources.)
         */
        if matchleft && matchright {
            (*cur_ec).ec_members =
                list_delete_nth_cell((*cur_ec).ec_members, coal_idx);
            return true;
        }

        /*
         * Otherwise, fall out of the search loop, since we know the COALESCE
         * appears in at most one EC.
         */
        break;
    });

    false /* failed to make any deduction */
}

/*
 * rebuild_eclass_attr_needed
 *    Put back attr_needed bits for Vars/PHVs needed for join eclasses.
 *
 * This is used to rebuild attr_needed/ph_needed sets after removal of a
 * useless outer join.  It should match what
 * generate_base_implied_equalities_no_const did, except that we call
 * add_vars_to_attr_needed not add_vars_to_targetlist.
 */
pub unsafe fn rebuild_eclass_attr_needed(root: *mut PlannerInfo) {
    foreach!(lc, (*root).eq_classes, {
        let ec: *mut EquivalenceClass = lfirst(current_cell!(lc)) as *mut EquivalenceClass;

        /*
         * We don't expect any EC child members to exist at this point. Ensure
         * that's the case, otherwise, we might be getting asked to do
         * something this function hasn't been coded for.
         */
        Assert!((*ec).ec_childmembers.is_null());

        /* Need do anything only for a multi-member, no-const EC. */
        if list_length((*ec).ec_members) > 1 && !(*ec).ec_has_const {
            foreach!(lc2, (*ec).ec_members, {
                let cur_em: *mut EquivalenceMember =
                    lfirst(current_cell!(lc2)) as *mut EquivalenceMember;
                let vars: *mut List = pull_var_clause(
                    (*cur_em).em_expr as *const Node,
                    PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
                );
                add_vars_to_attr_needed(root, vars, (*ec).ec_relids);
                list_free(vars);
            });
        }
    });
}

/*
 * find_join_domain
 *    Find the highest JoinDomain enclosed within the given relid set.
 *
 * (We could avoid this search at the cost of complicating APIs elsewhere,
 * which doesn't seem worth it.)
 */
unsafe fn find_join_domain(root: *mut PlannerInfo, relids: Relids) -> *mut JoinDomain {
    foreach!(lc, (*root).join_domains, {
        let jdomain: *mut JoinDomain = lfirst(current_cell!(lc)) as *mut JoinDomain;

        if bms_is_subset((*jdomain).jd_relids, relids) {
            return jdomain;
        }
    });
    ereport!(
        crate::utils::elog::ERROR,
        errmsg!("failed to find appropriate JoinDomain")
    );
    core::ptr::null_mut() /* keep compiler quiet */
}

// ===========================================================================
// Part 5
// ===========================================================================

/*
 * exprs_known_equal
 *    Detect whether two expressions are known equal due to equivalence
 *    relationships.
 *
 * If opfamily is given, the expressions must be known equal per the semantics
 * of that opfamily (note it has to be a btree opfamily, since those are the
 * only opfamilies equivclass.c deals with).  If opfamily is InvalidOid, we'll
 * return true if they're equal according to any opfamily, which is fuzzy but
 * OK for estimation purposes.
 *
 * Note: does not bother to check for "equal(item1, item2)"; caller must
 * check that case if it's possible to pass identical items.
 */
pub unsafe fn exprs_known_equal(
    root: *mut PlannerInfo,
    item1: *const Node,
    item2: *const Node,
    opfamily: Oid,
) -> bool {
    foreach!(lc1, (*root).eq_classes, {
        let ec: *mut EquivalenceClass = lfirst(current_cell!(lc1)) as *mut EquivalenceClass;
        let mut item1member = false;
        let mut item2member = false;

        /* Never match to a volatile EC */
        if (*ec).ec_has_volatile {
            continue;
        }

        /*
         * It's okay to consider ec_broken ECs here.  Brokenness just means we
         * couldn't derive all the implied clauses we'd have liked to; it does
         * not invalidate our knowledge that the members are equal.
         */

        /* Ignore if this EC doesn't use specified opfamily */
        if OidIsValid(opfamily) && !list_member_oid((*ec).ec_opfamilies, opfamily) {
            continue;
        }

        /* Ignore children here */
        foreach!(lc2, (*ec).ec_members, {
            let em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*em).em_is_child);
            if equal(item1 as *const c_void, (*em).em_expr as *const c_void) {
                item1member = true;
            } else if equal(item2 as *const c_void, (*em).em_expr as *const c_void) {
                item2member = true;
            }
            /* Exit as soon as equality is proven */
            if item1member && item2member {
                return true;
            }
        });
    });

    false
}

/*
 * match_eclasses_to_foreign_key_col
 *    See whether a foreign key column match is proven by any eclass.
 *
 * If the referenced and referencing Vars of the fkey's colno'th column are
 * known equal due to any eclass, return that eclass; otherwise return NULL.
 * (In principle there might be more than one matching eclass if multiple
 * collations are involved, but since collation doesn't matter for equality,
 * we ignore that fine point here.)  This is much like exprs_known_equal,
 * except for the format of the input.
 *
 * On success, we also set fkinfo->eclass[colno] to the matching eclass,
 * and set fkinfo->fk_eclass_member[colno] to the eclass member for the
 * referencing Var.
 */
pub unsafe fn match_eclasses_to_foreign_key_col(
    root: *mut PlannerInfo,
    fkinfo: *mut ForeignKeyOptInfo,
    colno: i32,
) -> *mut EquivalenceClass {
    let var1varno: u32 = (*fkinfo).con_relid;
    let var1attno: i16 = (*fkinfo).conkey[colno as usize];
    let var2varno: u32 = (*fkinfo).ref_relid;
    let var2attno: i16 = (*fkinfo).confkey[colno as usize];
    let eqop: Oid = (*fkinfo).conpfeqop[colno as usize];
    let rel1: *mut RelOptInfo = *(*root).simple_rel_array.add(var1varno as usize);
    let rel2: *mut RelOptInfo = *(*root).simple_rel_array.add(var2varno as usize);
    let mut opfamilies: *mut List = NIL; /* compute only if needed */
    let matching_ecs: *mut Bitmapset;
    let mut i: i32;

    /* Consider only eclasses mentioning both relations */
    Assert!((*root).ec_merging_done);
    Assert!(IS_SIMPLE_REL(rel1));
    Assert!(IS_SIMPLE_REL(rel2));
    matching_ecs = bms_intersect((*rel1).eclass_indexes, (*rel2).eclass_indexes);

    i = -1;
    loop {
        i = bms_next_member(matching_ecs, i);
        if i < 0 {
            break;
        }

        let ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i) as *mut EquivalenceClass;
        let mut item1_em: *mut EquivalenceMember = core::ptr::null_mut();
        let mut item2_em: *mut EquivalenceMember = core::ptr::null_mut();

        /* Never match to a volatile EC */
        if (*ec).ec_has_volatile {
            continue;
        }

        /*
         * It's okay to consider "broken" ECs here, see exprs_known_equal.
         * Ignore children here.
         */
        foreach!(lc2, (*ec).ec_members, {
            let em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*em).em_is_child);

            /* EM must be a Var, possibly with RelabelType */
            let mut var: *mut Var = (*em).em_expr as *mut Var;
            while !var.is_null() && IsA!(var, T_RelabelType) {
                var = (*(var as *mut RelabelType)).arg as *mut Var;
            }
            if var.is_null() || !IsA!(var, T_Var) {
                continue;
            }

            /* Match? */
            if (*var).varno == var1varno as i32 && (*var).varattno == var1attno {
                item1_em = em;
            } else if (*var).varno == var2varno as i32 && (*var).varattno == var2attno {
                item2_em = em;
            }

            /* Have we found both PK and FK column in this EC? */
            if !item1_em.is_null() && !item2_em.is_null() {
                /*
                 * Succeed if eqop matches EC's opfamilies.  We could test
                 * this before scanning the members, but it's probably cheaper
                 * to test for member matches first.
                 */
                if opfamilies.is_null() {
                    /* compute if we didn't already */
                    opfamilies = get_mergejoin_opfamilies(eqop);
                }
                if equal(opfamilies as *const c_void, (*ec).ec_opfamilies as *const c_void) {
                    (*fkinfo).eclass[colno as usize] = ec;
                    (*fkinfo).fk_eclass_member[colno as usize] = item2_em;
                    return ec;
                }
                /* Otherwise, done with this EC, move on to the next */
                break;
            }
        });
    }

    core::ptr::null_mut()
}

/*
 * find_derived_clause_for_ec_member
 *    Search for a previously-derived clause mentioning the given EM.
 *
 * The eclass should be an ec_has_const EC, of which the EM is a non-const
 * member.  This should ensure there is just one derived clause mentioning
 * the EM (and equating it to a constant).
 * Returns NULL if no such clause can be found.
 */
pub unsafe fn find_derived_clause_for_ec_member(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
) -> *mut RestrictInfo {
    Assert!((*ec).ec_has_const);
    Assert!(!(*em).em_is_const);

    ec_search_derived_clause_for_ems(root, ec, em, core::ptr::null_mut(), core::ptr::null_mut())
}

/*
 * add_child_rel_equivalences
 *    Search for EC members that reference the root parent of child_rel, and
 *    add transformed members referencing the child_rel.
 *
 * Note that this function won't be called at all unless we have at least some
 * reason to believe that the EC members it generates will be useful.
 *
 * parent_rel and child_rel could be derived from appinfo, but since the
 * caller has already computed them, we might as well just pass them in.
 *
 * The passed-in AppendRelInfo is not used when the parent_rel is not a
 * top-level baserel, since it shows the mapping from the parent_rel but
 * we need to translate EC expressions that refer to the top-level parent.
 * Using it is faster than using adjust_appendrel_attrs_multilevel(), though,
 * so we prefer it when we can.
 */
pub unsafe fn add_child_rel_equivalences(
    root: *mut PlannerInfo,
    mut appinfo: *mut AppendRelInfo,
    parent_rel: *mut RelOptInfo,
    child_rel: *mut RelOptInfo,
) {
    let top_parent_relids: Relids = (*child_rel).top_parent_relids;
    let child_relids: Relids = (*child_rel).relids;
    let mut i: i32;

    /*
     * EC merging should be complete already, so we can use the parent rel's
     * eclass_indexes to avoid searching all of root->eq_classes.
     */
    Assert!((*root).ec_merging_done);
    Assert!(IS_SIMPLE_REL(parent_rel));

    i = -1;
    loop {
        i = bms_next_member((*parent_rel).eclass_indexes, i);
        if i < 0 {
            break;
        }

        let cur_ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i) as *mut EquivalenceClass;

        /*
         * If this EC contains a volatile expression, then generating child
         * EMs would be downright dangerous, so skip it.  We rely on a
         * volatile EC having only one EM.
         */
        if (*cur_ec).ec_has_volatile {
            continue;
        }

        /* Sanity check eclass_indexes only contain ECs for parent_rel */
        Assert!(bms_is_subset(top_parent_relids, (*cur_ec).ec_relids));

        foreach_node!(EquivalenceMember, T_EquivalenceMember, cur_em, (*cur_ec).ec_members, {
            if (*cur_em).em_is_const {
                continue; /* ignore consts here */
            }

            /* Child members should not exist in ec_members */
            Assert!(!(*cur_em).em_is_child);

            /*
             * Consider only members that reference and can be computed at
             * child's topmost parent rel.  In particular we want to exclude
             * parent-rel Vars that have nonempty varnullingrels.  Translating
             * those might fail, if the transformed expression wouldn't be a
             * simple Var; and in any case it wouldn't produce a member that
             * has any use in creating plans for the child rel.
             */
            if bms_is_subset((*cur_em).em_relids, top_parent_relids)
                && !bms_is_empty((*cur_em).em_relids)
            {
                /* OK, generate transformed child version */
                let child_expr: *mut Expr;
                let new_relids: Relids;

                if (*parent_rel).reloptkind == crate::nodes::pathnodes::RelOptKind::RELOPT_BASEREL {
                    /* Simple single-level transformation */
                    child_expr = adjust_appendrel_attrs(
                        root,
                        (*cur_em).em_expr as *mut Node,
                        1,
                        &mut appinfo,
                    ) as *mut Expr;
                } else {
                    /* Must do multi-level transformation */
                    child_expr = adjust_appendrel_attrs_multilevel(
                        root,
                        (*cur_em).em_expr as *mut Node,
                        child_rel,
                        (*child_rel).top_parent,
                    ) as *mut Expr;
                }

                /*
                 * Transform em_relids to match.  Note we do *not* do
                 * pull_varnos(child_expr) here, as for example the
                 * transformation might have substituted a constant, but we
                 * don't want the child member to be marked as constant.
                 */
                new_relids = bms_difference((*cur_em).em_relids, top_parent_relids);
                let new_relids = bms_add_members(new_relids, child_relids);

                add_child_eq_member(
                    root,
                    cur_ec,
                    i,
                    child_expr,
                    new_relids,
                    (*cur_em).em_jdomain,
                    cur_em,
                    (*cur_em).em_datatype,
                    (*child_rel).relid,
                );
            }
        });
    }
}

/*
 * add_child_join_rel_equivalences
 *    Like add_child_rel_equivalences(), but for joinrels
 *
 * Here we find the ECs relevant to the top parent joinrel and add transformed
 * member expressions that refer to this child joinrel.
 *
 * Note that this function won't be called at all unless we have at least some
 * reason to believe that the EC members it generates will be useful.
 */
pub unsafe fn add_child_join_rel_equivalences(
    root: *mut PlannerInfo,
    nappinfos: i32,
    appinfos: *mut *mut AppendRelInfo,
    parent_joinrel: *mut RelOptInfo,
    child_joinrel: *mut RelOptInfo,
) {
    let top_parent_relids: Relids = (*child_joinrel).top_parent_relids;
    let child_relids: Relids = (*child_joinrel).relids;
    let matching_ecs: *mut Bitmapset;
    let oldcontext: *mut c_void;
    let mut i: i32;

    Assert!(IS_JOIN_REL(child_joinrel) && IS_JOIN_REL(parent_joinrel));

    /* We need consider only ECs that mention the parent joinrel */
    matching_ecs = get_eclass_indexes_for_relids(root, top_parent_relids);

    /*
     * If we're being called during GEQO join planning, we still have to
     * create any new EC members in the main planner context, to avoid having
     * a corrupt EC data structure after the GEQO context is reset.  This is
     * problematic since we'll leak memory across repeated GEQO cycles.  For
     * now, though, bloat is better than crash.  If it becomes a real issue
     * we'll have to do something to avoid generating duplicate EC members.
     */
    oldcontext = MemoryContextSwitchTo((*root).planner_cxt as *mut c_void);

    i = -1;
    loop {
        i = bms_next_member(matching_ecs, i);
        if i < 0 {
            break;
        }

        let cur_ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i) as *mut EquivalenceClass;

        /*
         * If this EC contains a volatile expression, then generating child
         * EMs would be downright dangerous, so skip it.  We rely on a
         * volatile EC having only one EM.
         */
        if (*cur_ec).ec_has_volatile {
            continue;
        }

        /* Sanity check on get_eclass_indexes_for_relids result */
        Assert!(bms_overlap(top_parent_relids, (*cur_ec).ec_relids));

        foreach_node!(EquivalenceMember, T_EquivalenceMember, cur_em, (*cur_ec).ec_members, {
            if (*cur_em).em_is_const {
                continue; /* ignore consts here */
            }

            /* Child members should not exist in ec_members */
            Assert!(!(*cur_em).em_is_child);

            /*
             * We may ignore expressions that reference a single baserel,
             * because add_child_rel_equivalences should have handled them.
             */
            if bms_membership((*cur_em).em_relids) != BMS_MULTIPLE {
                continue;
            }

            /* Does this member reference child's topmost parent rel? */
            if bms_overlap((*cur_em).em_relids, top_parent_relids) {
                /* Yes, generate transformed child version */
                let child_expr: *mut Expr;
                let new_relids: Relids;

                if (*parent_joinrel).reloptkind
                    == crate::nodes::pathnodes::RelOptKind::RELOPT_JOINREL
                {
                    /* Simple single-level transformation */
                    child_expr = adjust_appendrel_attrs(
                        root,
                        (*cur_em).em_expr as *mut Node,
                        nappinfos,
                        appinfos,
                    ) as *mut Expr;
                } else {
                    /* Must do multi-level transformation */
                    Assert!(
                        (*parent_joinrel).reloptkind
                            == crate::nodes::pathnodes::RelOptKind::RELOPT_OTHER_JOINREL
                    );
                    child_expr = adjust_appendrel_attrs_multilevel(
                        root,
                        (*cur_em).em_expr as *mut Node,
                        child_joinrel,
                        (*child_joinrel).top_parent,
                    ) as *mut Expr;
                }

                /*
                 * Transform em_relids to match.  Note we do *not* do
                 * pull_varnos(child_expr) here, as for example the
                 * transformation might have substituted a constant, but we
                 * don't want the child member to be marked as constant.
                 */
                new_relids = bms_difference((*cur_em).em_relids, top_parent_relids);
                let new_relids = bms_add_members(new_relids, child_relids);

                /*
                 * Add new child member to the EquivalenceClass.  Because this
                 * is a RELOPT_OTHER_JOINREL which has multiple component
                 * relids, there is no ideal place to store these members in
                 * the class.  Ordinarily, child members are stored in the
                 * ec_childmembers[] array element corresponding to their
                 * relid, however, here we have multiple component relids, so
                 * there's no single ec_childmembers[] array element to store
                 * this member.  So that we still correctly find this member
                 * in loops iterating over an EquivalenceMemberIterator, we
                 * opt to store the member in the ec_childmembers array in
                 * only the first component relid slot of the array.  This
                 * allows the member to be found, providing callers of
                 * setup_eclass_member_iterator() specify all the component
                 * relids for the RELOPT_OTHER_JOINREL, which they do.  If we
                 * opted to store the member in each ec_childmembers[] element
                 * for all the component relids, then that would just result
                 * in eclass_member_iterator_next() finding the member
                 * multiple times, which is a waste of effort.
                 */
                add_child_eq_member(
                    root,
                    cur_ec,
                    -1,
                    child_expr,
                    new_relids,
                    (*cur_em).em_jdomain,
                    cur_em,
                    (*cur_em).em_datatype,
                    bms_next_member((*child_joinrel).relids, -1) as u32,
                );
            }
        });
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * add_setop_child_rel_equivalences
 *    Add equivalence members for each non-resjunk target in 'child_tlist'
 *    to the EquivalenceClass in the corresponding setop_pathkey's pk_eclass.
 *
 * 'root' is the PlannerInfo belonging to the top-level set operation.
 * 'child_rel' is the RelOptInfo of the child relation we're adding
 * EquivalenceMembers for.
 * 'child_tlist' is the target list for the setop child relation.  The target
 * list expressions are what we add as EquivalenceMembers.
 * 'setop_pathkeys' is a list of PathKeys which must contain an entry for each
 * non-resjunk target in 'child_tlist'.
 */
pub unsafe fn add_setop_child_rel_equivalences(
    root: *mut PlannerInfo,
    child_rel: *mut RelOptInfo,
    child_tlist: *mut List,
    setop_pathkeys: *mut List,
) {
    let mut lc2: *mut crate::nodes::pg_list::ListCell = list_head(setop_pathkeys);

    foreach!(lc, child_tlist, {
        let tle: *mut TargetEntry =
            lfirst(current_cell!(lc)) as *mut TargetEntry;
        let parent_em: *mut EquivalenceMember;
        let pk: *mut PathKey;

        if (*tle).resjunk {
            continue;
        }

        if lc2.is_null() {
            ereport!(
                crate::utils::elog::ERROR,
                errmsg!("too few pathkeys for set operation")
            );
        }

        pk = lfirst(lc2) as *mut PathKey;
        parent_em = linitial((*(*pk).pk_eclass).ec_members) as *mut EquivalenceMember;

        /*
         * We can safely pass the parent member as the first member in the
         * ec_members list as this is added first in generate_union_paths,
         * likewise, the JoinDomain can be that of the initial member of the
         * Pathkey's EquivalenceClass.  We pass -1 for ec_index since we
         * maintain the eclass_indexes for the child_rel after the loop.
         */
        add_child_eq_member(
            root,
            (*pk).pk_eclass,
            -1,
            (*tle).expr,
            (*child_rel).relids,
            (*parent_em).em_jdomain,
            parent_em,
            exprType((*tle).expr as *const Node),
            (*child_rel).relid,
        );

        lc2 = lnext(setop_pathkeys, lc2);
    });

    /*
     * transformSetOperationStmt() ensures that the targetlist never contains
     * any resjunk columns, so all eclasses that exist in 'root' must have
     * received a new member in the loop above.  Add them to the child_rel's
     * eclass_indexes.
     */
    (*child_rel).eclass_indexes = bms_add_range(
        (*child_rel).eclass_indexes,
        0,
        list_length((*root).eq_classes) - 1,
    );
}

/*
 * setup_eclass_member_iterator
 *    Setup an EquivalenceMemberIterator 'it' to iterate over all parent
 *    EquivalenceMembers and child members belonging to the given 'ec'.
 *
 * This iterator returns:
 *  - All parent members stored directly in ec_members for 'ec', and;
 *  - Any child member added to the given ec by add_child_eq_member() where
 *    the child_relid specified in the add_child_eq_member() call is a member
 *    of the 'child_relids' parameter.
 *
 * Note:
 * The given 'child_relids' must remain allocated and not be changed for the
 * lifetime of the iterator.
 *
 * Parameters:
 *  'it' is a pointer to the iterator to set up.  Normally stack allocated.
 *  'ec' is the EquivalenceClass from which to iterate members for.
 *  'child_relids' is the relids to return child members for.
 */
pub unsafe fn setup_eclass_member_iterator(
    it: *mut EquivalenceMemberIterator,
    ec: *mut EquivalenceClass,
    child_relids: Relids,
) {
    (*it).ec = ec;
    /* no need to set this if the class has no child members array set */
    (*it).child_relids = if !(*ec).ec_childmembers.is_null() {
        child_relids
    } else {
        core::ptr::null_mut()
    };
    (*it).current_relid = -1;
    (*it).current_list = (*ec).ec_members;
    (*it).current_cell = list_head((*it).current_list);
}

/*
 * eclass_member_iterator_next
 *    Get the next EquivalenceMember from the EquivalenceMemberIterator 'it',
 *    as setup by setup_eclass_member_iterator().  NULL is returned if there
 *    are no members left, after which callers must not call
 *    eclass_member_iterator_next() again for the given iterator.
 */
pub unsafe fn eclass_member_iterator_next(
    it: *mut EquivalenceMemberIterator,
) -> *mut EquivalenceMember {
    'outer: while !(*it).current_list.is_null() {
        while !(*it).current_cell.is_null() {
            // nextcell:
            let em: *mut EquivalenceMember = crate::nodes::pg_list::lfirst((*it).current_cell)
                as *mut EquivalenceMember;
            (*it).current_cell = lnext((*it).current_list, (*it).current_cell);
            return em;
        }

        /* Search for the next list to return members from */
        loop {
            (*it).current_relid =
                bms_next_member((*it).child_relids, (*it).current_relid);
            if (*it).current_relid <= 0 {
                return core::ptr::null_mut();
            }

            /*
             * Be paranoid in case we're given relids above what we've sized
             * the ec_childmembers array to.
             */
            if (*it).current_relid >= (*(*it).ec).ec_childmembers_size as i32 {
                return core::ptr::null_mut();
            }

            (*it).current_list =
                *(*(*it).ec).ec_childmembers.add((*it).current_relid as usize);

            /* If there are members in this list, use it. */
            if !(*it).current_list.is_null() {
                /* point current_cell to the head of this list */
                (*it).current_cell = list_head((*it).current_list);
                // goto nextcell -- handled by outer while loop continuing
                continue 'outer;
            }
        }
    }

    core::ptr::null_mut()
}

/*
 * generate_implied_equalities_for_column
 *    Create EC-derived joinclauses usable with a specific column.
 *
 * This is used by indxpath.c to extract potentially indexable joinclauses
 * from ECs, and can be used by foreign data wrappers for similar purposes.
 * We assume that only expressions in Vars of a single table are of interest,
 * but the caller provides a callback function to identify exactly which
 * such expressions it would like to know about.
 *
 * We assume that any given table/index column could appear in only one EC.
 * (This should be true in all but the most pathological cases, and if it
 * isn't, we stop on the first match anyway.)  Therefore, what we return
 * is a redundant list of clauses equating the table/index column to each of
 * the other-relation values it is known to be equal to.  Any one of
 * these clauses can be used to create a parameterized path, and there
 * is no value in using more than one.  (But it *is* worthwhile to create
 * a separate parameterized path for each one, since that leads to different
 * join orders.)
 *
 * The caller can pass a Relids set of rels we aren't interested in joining
 * to, so as to save the work of creating useless clauses.
 */
pub unsafe fn generate_implied_equalities_for_column(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    callback: ec_matches_callback_type,
    callback_arg: *mut c_void,
    prohibited_rels: Relids,
) -> *mut List {
    let mut result: *mut List = NIL;
    let is_child_rel: bool =
        (*rel).reloptkind == crate::nodes::pathnodes::RelOptKind::RELOPT_OTHER_MEMBER_REL;
    let parent_relids: Relids;
    let mut i: i32;

    /* Should be OK to rely on eclass_indexes */
    Assert!((*root).ec_merging_done);

    /* Indexes are available only on base or "other" member relations. */
    Assert!(IS_SIMPLE_REL(rel));

    /* If it's a child rel, we'll need to know what its parent(s) are */
    if is_child_rel {
        parent_relids = find_childrel_parents(root, rel);
    } else {
        parent_relids = core::ptr::null_mut(); /* not used, but keep compiler quiet */
    }

    i = -1;
    loop {
        i = bms_next_member((*rel).eclass_indexes, i);
        if i < 0 {
            break;
        }

        let cur_ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i) as *mut EquivalenceClass;
        let mut it: EquivalenceMemberIterator = core::mem::zeroed();
        let mut cur_em: *mut EquivalenceMember;

        /* Sanity check eclass_indexes only contain ECs for rel */
        Assert!(is_child_rel || bms_is_subset((*rel).relids, (*cur_ec).ec_relids));

        /*
         * Won't generate joinclauses if const or single-member (the latter
         * test covers the volatile case too)
         */
        if (*cur_ec).ec_has_const || list_length((*cur_ec).ec_members) <= 1 {
            continue;
        }

        /*
         * Scan members, looking for a match to the target column.  Note that
         * child EC members are considered, but only when they belong to the
         * target relation.  (Unlike regular members, the same expression
         * could be a child member of more than one EC.  Therefore, it's
         * potentially order-dependent which EC a child relation's target
         * column gets matched to.  This is annoying but it only happens in
         * corner cases, so for now we live with just reporting the first
         * match.  See also get_eclass_for_sort_expr.)
         */
        setup_eclass_member_iterator(&mut it, cur_ec, (*rel).relids);
        loop {
            cur_em = eclass_member_iterator_next(&mut it);
            if cur_em.is_null() {
                break;
            }
            if bms_equal((*cur_em).em_relids, (*rel).relids)
                && callback(root, rel, cur_ec, cur_em, callback_arg)
            {
                break;
            }
        }

        if cur_em.is_null() {
            continue;
        }

        /*
         * Found our match.  Scan the other EC members and attempt to generate
         * joinclauses.  Ignore children here.
         */
        foreach!(lc2, (*cur_ec).ec_members, {
            let other_em: *mut EquivalenceMember =
                lfirst(current_cell!(lc2)) as *mut EquivalenceMember;

            /* Child members should not exist in ec_members */
            Assert!(!(*other_em).em_is_child);

            /* Make sure it'll be a join to a different rel */
            if other_em == cur_em || bms_overlap((*other_em).em_relids, (*rel).relids) {
                continue;
            }

            /* Forget it if caller doesn't want joins to this rel */
            if bms_overlap((*other_em).em_relids, prohibited_rels) {
                continue;
            }

            /*
             * Also, if this is a child rel, avoid generating a useless join
             * to its parent rel(s).
             */
            if is_child_rel && bms_overlap(parent_relids, (*other_em).em_relids) {
                continue;
            }

            let eq_op: Oid = select_equality_operator(
                cur_ec,
                (*cur_em).em_datatype,
                (*other_em).em_datatype,
            );
            if !OidIsValid(eq_op) {
                continue;
            }

            /* set parent_ec to mark as redundant with other joinclauses */
            let rinfo: *mut RestrictInfo =
                create_join_clause(root, cur_ec, eq_op, cur_em, other_em, cur_ec);

            result = lappend(result, rinfo as *mut c_void);
        });

        /*
         * If somehow we failed to create any join clauses, we might as well
         * keep scanning the ECs for another match.  But if we did make any,
         * we're done, because we don't want to return non-redundant clauses.
         */
        if !result.is_null() {
            break;
        }
    }

    result
}

/*
 * have_relevant_eclass_joinclause
 *    Detect whether there is an EquivalenceClass that could produce
 *    a joinclause involving the two given relations.
 *
 * This is essentially a very cut-down version of
 * generate_join_implied_equalities().  Note it's OK to occasionally say "yes"
 * incorrectly.  Hence we don't bother with details like whether the lack of a
 * cross-type operator might prevent the clause from actually being generated.
 * False negatives are not always fatal either: they will discourage, but not
 * completely prevent, investigation of particular join pathways.
 */
pub unsafe fn have_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool {
    let matching_ecs: *mut Bitmapset;
    let mut i: i32;

    /*
     * Examine only eclasses mentioning both rel1 and rel2.
     *
     * Note that we do not consider the possibility of an eclass generating
     * "join" clauses that mention just one of the rels plus an outer join
     * that could be formed from them.  Although such clauses must be
     * correctly enforced when we form the outer join, they don't seem like
     * sufficient reason to prioritize this join over other ones.  The join
     * ordering rules will force the join to be made when necessary.
     */
    matching_ecs = get_common_eclass_indexes(root, (*rel1).relids, (*rel2).relids);

    i = -1;
    loop {
        i = bms_next_member(matching_ecs, i);
        if i < 0 {
            break;
        }

        let ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i) as *mut EquivalenceClass;

        /*
         * Sanity check that get_common_eclass_indexes gave only ECs
         * containing both rels.
         */
        Assert!(bms_overlap((*rel1).relids, (*ec).ec_relids));
        Assert!(bms_overlap((*rel2).relids, (*ec).ec_relids));

        /*
         * Won't generate joinclauses if single-member (this test covers the
         * volatile case too)
         */
        if list_length((*ec).ec_members) <= 1 {
            continue;
        }

        /*
         * We do not need to examine the individual members of the EC, because
         * all that we care about is whether each rel overlaps the relids of
         * at least one member, and get_common_eclass_indexes() and the single
         * member check above are sufficient to prove that.  (As with
         * have_relevant_joinclause(), it is not necessary that the EC be able
         * to form a joinclause relating exactly the two given rels, only that
         * it be able to form a joinclause mentioning both, and this will
         * surely be true if both of them overlap ec_relids.)
         *
         * Note we don't test ec_broken; if we did, we'd need a separate code
         * path to look through ec_sources.  Checking the membership anyway is
         * OK as a possibly-overoptimistic heuristic.
         *
         * We don't test ec_has_const either, even though a const eclass won't
         * generate real join clauses.  This is because if we had "WHERE a.x =
         * b.y and a.x = 42", it is worth considering a join between a and b,
         * since the join result is likely to be small even though it'll end
         * up being an unqualified nestloop.
         */

        return true;
    }

    false
}

/*
 * has_relevant_eclass_joinclause
 *    Detect whether there is an EquivalenceClass that could produce
 *    a joinclause involving the given relation and anything else.
 *
 * This is the same as have_relevant_eclass_joinclause with the other rel
 * implicitly defined as "everything else in the query".
 */
pub unsafe fn has_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
) -> bool {
    let matched_ecs: *mut Bitmapset;
    let mut i: i32;

    /* Examine only eclasses mentioning rel1 */
    matched_ecs = get_eclass_indexes_for_relids(root, (*rel1).relids);

    i = -1;
    loop {
        i = bms_next_member(matched_ecs, i);
        if i < 0 {
            break;
        }

        let ec: *mut EquivalenceClass =
            list_nth((*root).eq_classes, i) as *mut EquivalenceClass;

        /*
         * Won't generate joinclauses if single-member (this test covers the
         * volatile case too)
         */
        if list_length((*ec).ec_members) <= 1 {
            continue;
        }

        /*
         * Per the comment in have_relevant_eclass_joinclause, it's sufficient
         * to find an EC that mentions both this rel and some other rel.
         */
        if !bms_is_subset((*ec).ec_relids, (*rel1).relids) {
            return true;
        }
    }

    false
}

/*
 * eclass_useful_for_merging
 *    Detect whether the EC could produce any mergejoinable join clauses
 *    against the specified relation.
 *
 * This is just a heuristic test and doesn't have to be exact; it's better
 * to say "yes" incorrectly than "no".  Hence we don't bother with details
 * like whether the lack of a cross-type operator might prevent the clause
 * from actually being generated.
 */
pub unsafe fn eclass_useful_for_merging(
    root: *mut PlannerInfo,
    eclass: *mut EquivalenceClass,
    rel: *mut RelOptInfo,
) -> bool {
    let relids: Relids;

    Assert!((*eclass).ec_merged.is_null());

    /*
     * Won't generate joinclauses if const or single-member (the latter test
     * covers the volatile case too)
     */
    if (*eclass).ec_has_const || list_length((*eclass).ec_members) <= 1 {
        return false;
    }

    /*
     * Note we don't test ec_broken; if we did, we'd need a separate code path
     * to look through ec_sources.  Checking the members anyway is OK as a
     * possibly-overoptimistic heuristic.
     */

    /* If specified rel is a child, we must consider the topmost parent rel */
    if IS_OTHER_REL(rel) {
        Assert!(!bms_is_empty((*rel).top_parent_relids));
        relids = (*rel).top_parent_relids;
    } else {
        relids = (*rel).relids;
    }

    /* If rel already includes all members of eclass, no point in searching */
    if bms_is_subset((*eclass).ec_relids, relids) {
        return false;
    }

    /*
     * To join, we need a member not in the given rel.  Ignore children here.
     */
    foreach!(lc, (*eclass).ec_members, {
        let cur_em: *mut EquivalenceMember =
            lfirst(current_cell!(lc)) as *mut EquivalenceMember;

        /* Child members should not exist in ec_members */
        Assert!(!(*cur_em).em_is_child);

        if !bms_overlap((*cur_em).em_relids, relids) {
            return true;
        }
    });

    false
}

/*
 * is_redundant_derived_clause
 *    Test whether rinfo is derived from same EC as any clause in clauselist;
 *    if so, it can be presumed to represent a condition that's redundant
 *    with that member of the list.
 */
pub unsafe fn is_redundant_derived_clause(
    rinfo: *mut RestrictInfo,
    clauselist: *mut List,
) -> bool {
    let parent_ec: *mut EquivalenceClass = (*rinfo).parent_ec;

    /* Fail if it's not a potentially-redundant clause from some EC */
    if parent_ec.is_null() {
        return false;
    }

    foreach!(lc, clauselist, {
        let otherrinfo: *mut RestrictInfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;

        if (*otherrinfo).parent_ec == parent_ec {
            return true;
        }
    });

    false
}

/*
 * is_redundant_with_indexclauses
 *    Test whether rinfo is redundant with any clause in the IndexClause
 *    list.  Here, for convenience, we test both simple identity and
 *    whether it is derived from the same EC as any member of the list.
 */
pub unsafe fn is_redundant_with_indexclauses(
    rinfo: *mut RestrictInfo,
    indexclauses: *mut List,
) -> bool {
    let parent_ec: *mut EquivalenceClass = (*rinfo).parent_ec;

    foreach!(lc, indexclauses, {
        let iclause: *mut IndexClause = lfirst(current_cell!(lc)) as *mut IndexClause;
        let otherrinfo: *mut RestrictInfo = (*iclause).rinfo;

        /* If indexclause is lossy, it won't enforce the condition exactly */
        if (*iclause).lossy {
            continue;
        }

        /* Match if it's same clause (pointer equality should be enough) */
        if rinfo == otherrinfo {
            return true;
        }
        /* Match if derived from same EC */
        if !parent_ec.is_null() && (*otherrinfo).parent_ec == parent_ec {
            return true;
        }

        /*
         * No need to look at the derived clauses in iclause->indexquals; they
         * couldn't match if the parent clause didn't.
         */
    });

    false
}

/*
 * get_eclass_indexes_for_relids
 *    Build and return a Bitmapset containing the indexes into root's
 *    eq_classes list for all eclasses that mention any of these relids
 */
unsafe fn get_eclass_indexes_for_relids(root: *mut PlannerInfo, relids: Relids) -> *mut Bitmapset {
    let mut ec_indexes: *mut Bitmapset = core::ptr::null_mut();
    let mut i: i32 = -1;

    /* Should be OK to rely on eclass_indexes */
    Assert!((*root).ec_merging_done);

    loop {
        i = bms_next_member(relids, i);
        if i <= 0 {
            break;
        }

        let rel: *mut RelOptInfo = *(*root).simple_rel_array.add(i as usize);

        /* ignore the RTE_GROUP RTE */
        if i == (*root).group_rtindex as i32 {
            continue;
        }

        if rel.is_null() {
            /* must be an outer join */
            Assert!(bms_is_member(i, (*root).outer_join_rels));
            continue;
        }

        ec_indexes = bms_add_members(ec_indexes, (*rel).eclass_indexes);
    }

    ec_indexes
}

/*
 * get_common_eclass_indexes
 *    Build and return a Bitmapset containing the indexes into root's
 *    eq_classes list for all eclasses that mention rels in both
 *    relids1 and relids2.
 */
unsafe fn get_common_eclass_indexes(
    root: *mut PlannerInfo,
    relids1: Relids,
    relids2: Relids,
) -> *mut Bitmapset {
    let rel1ecs: *mut Bitmapset;
    let rel2ecs: *mut Bitmapset;
    let mut relid: i32 = 0;

    rel1ecs = get_eclass_indexes_for_relids(root, relids1);

    /*
     * We can get away with just using the relation's eclass_indexes directly
     * when relids2 is a singleton set.
     */
    if bms_get_singleton_member(relids2, &mut relid) {
        rel2ecs = (*(*(*root).simple_rel_array.add(relid as usize))).eclass_indexes;
    } else {
        rel2ecs = get_eclass_indexes_for_relids(root, relids2);
    }

    /* Calculate and return the common EC indexes, recycling the left input. */
    bms_int_members(rel1ecs, rel2ecs)
}

/*
 * ec_build_derives_hash
 *    Construct the auxiliary hash table for derived clause lookups.
 */
unsafe fn ec_build_derives_hash(root: *mut PlannerInfo, ec: *mut EquivalenceClass) {
    Assert!((*ec).ec_derives_hash.is_null());

    /*
     * Create the hash table.
     *
     * We pass list_length(ec->ec_derives_list) as the initial size.
     * Simplehash will divide this by the fillfactor (typically 0.9) and round
     * up to the next power of two, so this will usually give us at least 64
     * buckets around the threshold. That avoids immediate resizing without
     * hardcoding a specific size.
     */
    (*ec).ec_derives_hash = derives_create(
        (*root).planner_cxt as *mut c_void,
        list_length((*ec).ec_derives_list),
        core::ptr::null_mut(),
    );

    foreach_node!(RestrictInfo, T_RestrictInfo, rinfo, (*ec).ec_derives_list, {
        ec_add_clause_to_derives_hash(ec, rinfo);
    });
}

/*
 * ec_add_derived_clause
 *    Add a clause to the set of derived clauses for the given
 *    EquivalenceClass. Always appends to ec_derives_list; also adds
 *    to ec_derives_hash if it exists.
 *
 * Also asserts expected invariants of derived clauses.
 */
unsafe fn ec_add_derived_clause(ec: *mut EquivalenceClass, clause: *mut RestrictInfo) {
    /*
     * Constant, if present, is always placed on the RHS; see
     * generate_base_implied_equalities_const(). LHS is never a constant.
     */
    Assert!(!(*(*clause).left_em).em_is_const);

    /*
     * Clauses containing a constant are never considered redundant, so
     * parent_ec is not set.
     */
    Assert!((*clause).parent_ec.is_null() || !(*(*clause).right_em).em_is_const);

    (*ec).ec_derives_list = lappend((*ec).ec_derives_list, clause as *mut c_void);
    if !(*ec).ec_derives_hash.is_null() {
        ec_add_clause_to_derives_hash(ec, clause);
    }
}

/*
 * ec_add_derived_clauses
 *    Add a list of clauses to the set of clauses derived from the given
 *    EquivalenceClass; adding to the list and hash table if needed.
 *
 * This function is similar to ec_add_derived_clause() but optimized for adding
 * multiple clauses at a time to the ec_derives_list.  The assertions from
 * ec_add_derived_clause() are not repeated here, as the input clauses are
 * assumed to have already been validated.
 */
unsafe fn ec_add_derived_clauses(ec: *mut EquivalenceClass, clauses: *mut List) {
    (*ec).ec_derives_list = list_concat((*ec).ec_derives_list, clauses);
    if !(*ec).ec_derives_hash.is_null() {
        foreach_node!(RestrictInfo, T_RestrictInfo, rinfo, clauses, {
            ec_add_clause_to_derives_hash(ec, rinfo);
        });
    }
}

/*
 * fill_ec_derives_key
 *    Compute a canonical key for ec_derives_hash lookup or insertion.
 *
 * Derived clauses are looked up using a pair of EquivalenceMembers and a
 * parent EquivalenceClass. To avoid storing or searching for both EM orderings,
 * we canonicalize the key:
 *
 * - For clauses involving two non-constant EMs, em1 is set to the EM with lower
 *   memory address and em2 is set to the other one.
 * - For clauses involving a constant EM, the caller must pass the non-constant
 *   EM as leftem and NULL as rightem; we then set em1 = NULL and em2 = leftem.
 */
#[inline]
unsafe fn fill_ec_derives_key(
    key: *mut ECDerivesKey,
    leftem: *mut EquivalenceMember,
    rightem: *mut EquivalenceMember,
    parent_ec: *mut EquivalenceClass,
) {
    Assert!(!leftem.is_null()); /* Always required for lookup or insertion */

    if rightem.is_null() {
        (*key).em1 = core::ptr::null_mut();
        (*key).em2 = leftem;
    } else if (leftem as usize) < (rightem as usize) {
        (*key).em1 = leftem;
        (*key).em2 = rightem;
    } else {
        (*key).em1 = rightem;
        (*key).em2 = leftem;
    }
    (*key).parent_ec = parent_ec;
}

/*
 * ec_add_clause_to_derives_hash
 *    Add a derived clause to ec_derives_hash in the given EquivalenceClass.
 *
 * Each clause is associated with a canonicalized key. For constant-containing
 * clauses, only the non-constant EM is used for lookup; see comments in
 * fill_ec_derives_key().
 */
unsafe fn ec_add_clause_to_derives_hash(ec: *mut EquivalenceClass, rinfo: *mut RestrictInfo) {
    let mut key: ECDerivesKey = core::mem::zeroed();
    let mut found: bool = false;

    /*
     * Constants are always placed on the RHS; see
     * generate_base_implied_equalities_const().
     */
    Assert!(!(*(*rinfo).left_em).em_is_const);

    /*
     * Clauses containing a constant are never considered redundant, so
     * parent_ec is not set.
     */
    Assert!((*rinfo).parent_ec.is_null() || !(*(*rinfo).right_em).em_is_const);

    /*
     * See fill_ec_derives_key() for details: we use a canonicalized key to
     * avoid storing both EM orderings. For constant EMs, only the
     * non-constant EM is included in the key.
     */
    fill_ec_derives_key(
        &mut key,
        (*rinfo).left_em,
        if (*(*rinfo).right_em).em_is_const {
            core::ptr::null_mut()
        } else {
            (*rinfo).right_em
        },
        (*rinfo).parent_ec,
    );
    let entry: *mut ECDerivesEntry = derives_insert((*ec).ec_derives_hash, key, &mut found);
    Assert!(!found);
    (*entry).rinfo = rinfo;
}

/*
 * ec_clear_derived_clauses
 *      Reset ec_derives_list and ec_derives_hash.
 *
 * We destroy the hash table explicitly, since it may consume significant
 * space. The list holds the same set of entries and can become equally large
 * when thousands of partitions are involved, so we free it as well -- even
 * though we do not typically free lists.
 */
pub unsafe fn ec_clear_derived_clauses(ec: *mut EquivalenceClass) {
    list_free((*ec).ec_derives_list);
    (*ec).ec_derives_list = NIL;

    if !(*ec).ec_derives_hash.is_null() {
        derives_destroy((*ec).ec_derives_hash);
        (*ec).ec_derives_hash = core::ptr::null_mut();
    }
}

/*
 * ec_search_clause_for_ems
 *    Search for an existing RestrictInfo that equates the given pair
 *    of EquivalenceMembers, either from ec_sources or ec_derives.
 *
 * Returns a clause with matching operands in either given order or commuted
 * order. We used to require matching operator OIDs, but dropped that since any
 * semantically different operator here would indicate a broken operator family.
 *
 * Returns NULL if no matching clause is found.
 */
unsafe fn ec_search_clause_for_ems(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    leftem: *mut EquivalenceMember,
    rightem: *mut EquivalenceMember,
    parent_ec: *mut EquivalenceClass,
) -> *mut RestrictInfo {
    /* Check original source clauses */
    foreach_node!(RestrictInfo, T_RestrictInfo, rinfo, (*ec).ec_sources, {
        if (*rinfo).left_em == leftem
            && (*rinfo).right_em == rightem
            && (*rinfo).parent_ec == parent_ec
        {
            return rinfo;
        }
        if (*rinfo).left_em == rightem
            && (*rinfo).right_em == leftem
            && (*rinfo).parent_ec == parent_ec
        {
            return rinfo;
        }
    });

    /* Not found in ec_sources; search derived clauses */
    ec_search_derived_clause_for_ems(root, ec, leftem, rightem, parent_ec)
}

/*
 * ec_search_derived_clause_for_ems
 *    Search for an existing derived clause between two EquivalenceMembers.
 *
 * If the number of derived clauses exceeds a threshold, switch to hash table
 * lookup; otherwise, scan ec_derives_list linearly.
 *
 * Clauses involving constants are looked up by passing the non-constant EM
 * as leftem and setting rightem to NULL. In that case, we expect to find a
 * clause with a constant on the RHS.
 *
 * While searching the list, we compare each given EM with both sides of each
 * clause. But for hash table lookups, we construct a canonicalized key and
 * perform a single lookup.
 */
unsafe fn ec_search_derived_clause_for_ems(
    root: *mut PlannerInfo,
    ec: *mut EquivalenceClass,
    leftem: *mut EquivalenceMember,
    rightem: *mut EquivalenceMember,
    parent_ec: *mut EquivalenceClass,
) -> *mut RestrictInfo {
    /* Switch to using hash lookup when list grows "too long". */
    if (*ec).ec_derives_hash.is_null()
        && list_length((*ec).ec_derives_list) >= EC_DERIVES_HASH_THRESHOLD
    {
        ec_build_derives_hash(root, ec);
    }

    /* Perform hash table lookup if available */
    if !(*ec).ec_derives_hash.is_null() {
        let mut key: ECDerivesKey = core::mem::zeroed();
        fill_ec_derives_key(&mut key, leftem, rightem, parent_ec);
        let entry: *mut ECDerivesEntry = derives_lookup((*ec).ec_derives_hash, key);
        if !entry.is_null() {
            let rinfo: *mut RestrictInfo = (*entry).rinfo;
            Assert!(!rinfo.is_null());
            Assert!(rightem.is_null() || (*(*rinfo).right_em).em_is_const);
            return rinfo;
        }
    } else {
        /* Fallback to linear search over ec_derives_list */
        foreach_node!(RestrictInfo, T_RestrictInfo, rinfo, (*ec).ec_derives_list, {
            /* Handle special case: lookup by non-const EM alone */
            if rightem.is_null() && (*rinfo).left_em == leftem {
                Assert!((*(*rinfo).right_em).em_is_const);
                return rinfo;
            }
            if (*rinfo).left_em == leftem
                && (*rinfo).right_em == rightem
                && (*rinfo).parent_ec == parent_ec
            {
                return rinfo;
            }
            if (*rinfo).left_em == rightem
                && (*rinfo).right_em == leftem
                && (*rinfo).parent_ec == parent_ec
            {
                return rinfo;
            }
        });
    }

    core::ptr::null_mut()
}
