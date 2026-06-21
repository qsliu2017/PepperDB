//! joininfo list manipulation routines.
//!
//! Source: postgres/src/backend/optimizer/util/joininfo.c
//!
//! #include mapping:
//!   "postgres.h"                       -> crate::prelude::*
//!   "nodes/makefuncs.h"                -> crate::nodes::makefuncs (makeBoolConst)
//!   "optimizer/joininfo.h"             -> this module (declares the fns defined here)
//!   "optimizer/pathnode.h"             -> STUB (make_restrictinfo)
//!   "optimizer/paths.h"                -> STUB (have/has_relevant_eclass_joinclause -
//!                                         actually DEFINED in equivclass.c, only called here)
//!   "optimizer/planmain.h"             -> not needed
//!   "optimizer/restrictinfo.h"         -> STUB (restriction_is_always_true/false)
//!
//! RelOptInfo / RestrictInfo / EquivalenceClass / PlannerInfo / Relids come from
//! crate::nodes::pathnodes.  Bitmapset ops from crate::nodes::bitmapset, list ops
//! from crate::nodes::pg_list.
//!
//! STUBs (defined in OTHER C files; opaque here):
//!   - restriction_is_always_true / restriction_is_always_false (restrictinfo.c)
//!   - make_restrictinfo (relnode.c / restrictinfo.c)
//!   - find_base_rel_ignore_join (relnode.c)
//!   - have_relevant_eclass_joinclause / has_relevant_eclass_joinclause (equivclass.c).
//!     These two are DECLARED in optimizer/paths.h and merely *called* from this file
//!     (by have_relevant_joinclause); their real bodies live in equivclass.c.

use crate::prelude::*;

use crate::nodes::bitmapset::{bms_next_member, bms_overlap};
use crate::nodes::makefuncs::makeBoolConst;
use crate::nodes::pathnodes::{
    EquivalenceClass, PlannerInfo, RelOptInfo, Relids, RestrictInfo,
};
use crate::nodes::pg_list::{
    lappend, lfirst, list_delete_ptr, list_length, list_member_ptr, List, ListCell,
};
use crate::nodes::primnodes::Expr;
use crate::{current_cell, foreach};

// ---------------------------------------------------------------------------
// STUBs for dependencies defined in other backend files.
// ---------------------------------------------------------------------------

/// STUB: restrictinfo.c.  TODO port restriction_is_always_true.
unsafe fn restriction_is_always_true(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) -> bool {
    crate::optimizer::plan::initsplan::restriction_is_always_true(root, restrictinfo)
}

/// STUB: restrictinfo.c.  TODO port restriction_is_always_false.
unsafe fn restriction_is_always_false(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) -> bool {
    crate::optimizer::plan::initsplan::restriction_is_always_false(root, restrictinfo)
}

/// STUB: relnode.c / restrictinfo.c.  TODO port make_restrictinfo.
unsafe fn make_restrictinfo(
    root: *mut PlannerInfo,
    clause: *mut Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: c_int,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> *mut RestrictInfo {
    crate::optimizer::util::restrictinfo::make_restrictinfo(
        root,
        clause,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level as _,
        required_relids,
        incompatible_relids,
        outer_relids,
    )
}

/// STUB: relnode.c.  TODO port find_base_rel_ignore_join.
/// Returns NULL for non-baserels (e.g. join relids).
unsafe fn find_base_rel_ignore_join(
    _root: *mut PlannerInfo,
    _relid: c_int,
) -> *mut RelOptInfo {
    crate::optimizer::util::relnode::find_base_rel_ignore_join(_root as _, _relid as _) as _
}

/// STUB: equivclass.c (declared in optimizer/paths.h).  TODO port.
/// Detect whether there is an EquivalenceClass that could produce a join clause
/// involving the two given relations.
pub unsafe fn have_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool {
    crate::optimizer::path::equivclass::have_relevant_eclass_joinclause(root, rel1, rel2)
}

/// Same as have_relevant_eclass_joinclause but tests rel1 against every other rel.
pub unsafe fn has_relevant_eclass_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
) -> bool {
    crate::optimizer::path::equivclass::has_relevant_eclass_joinclause(root, rel1)
}

// ---------------------------------------------------------------------------
// have_relevant_joinclause
// ---------------------------------------------------------------------------

/// have_relevant_joinclause
///     Detect whether there is a joinclause that involves
///     the two given relations.
///
/// Note: the joinclause does not have to be evaluable with only these two
/// relations.  This is intentional.  For example consider
///     SELECT * FROM a, b, c WHERE a.x = (b.y + c.z)
/// If a is much larger than the other tables, it may be worthwhile to
/// cross-join b and c and then use an inner indexscan on a.x.  Therefore
/// we should consider this joinclause as reason to join b to c, even though
/// it can't be applied at that join step.
pub unsafe fn have_relevant_joinclause(
    root: *mut PlannerInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
) -> bool {
    let mut result = false;
    let joininfo: *mut List;
    let other_relids: Relids;

    // We could scan either relation's joininfo list; may as well use the
    // shorter one.
    if list_length((*rel1).joininfo) <= list_length((*rel2).joininfo) {
        joininfo = (*rel1).joininfo;
        other_relids = (*rel2).relids;
    } else {
        joininfo = (*rel2).joininfo;
        other_relids = (*rel1).relids;
    }

    foreach!(l, joininfo, {
        let rinfo = lfirst(current_cell!(l)) as *mut RestrictInfo;

        if bms_overlap(other_relids, (*rinfo).required_relids) {
            result = true;
            break;
        }
    });

    // We also need to check the EquivalenceClass data structure, which might
    // contain relationships not emitted into the joininfo lists.
    if !result && (*rel1).has_eclass_joins && (*rel2).has_eclass_joins {
        result = have_relevant_eclass_joinclause(root, rel1, rel2);
    }

    result
}

// ---------------------------------------------------------------------------
// add_join_clause_to_rels
// ---------------------------------------------------------------------------

/// add_join_clause_to_rels
///     Add 'restrictinfo' to the joininfo list of each relation it requires.
///
/// Note that the same copy of the restrictinfo node is linked to by all the
/// lists it is in.  This allows us to exploit caching of information about
/// the restriction clause (but we must be careful that the information does
/// not depend on context).
///
/// 'restrictinfo' describes the join clause
/// 'join_relids' is the set of relations participating in the join clause
///               (some of these could be outer joins)
pub unsafe fn add_join_clause_to_rels(
    root: *mut PlannerInfo,
    mut restrictinfo: *mut RestrictInfo,
    join_relids: Relids,
) {
    let mut cur_relid: c_int;

    // Don't add the clause if it is always true
    if restriction_is_always_true(root, restrictinfo) {
        return;
    }

    // Substitute the origin qual with constant-FALSE if it is provably always
    // false.
    //
    // Note that we need to keep the same rinfo_serial, since it is in
    // practice the same condition.  We also need to reset the
    // last_rinfo_serial counter, which is essential to ensure that the
    // RestrictInfos for the "same" qual condition get identical serial
    // numbers (see deconstruct_distribute_oj_quals).
    if restriction_is_always_false(root, restrictinfo) {
        let save_rinfo_serial = (*restrictinfo).rinfo_serial;
        let save_last_rinfo_serial = (*root).last_rinfo_serial;

        restrictinfo = make_restrictinfo(
            root,
            makeBoolConst(false, false) as *mut Expr,
            (*restrictinfo).is_pushed_down,
            (*restrictinfo).has_clone,
            (*restrictinfo).is_clone,
            (*restrictinfo).pseudoconstant,
            0, /* security_level */
            (*restrictinfo).required_relids,
            (*restrictinfo).incompatible_relids,
            (*restrictinfo).outer_relids,
        );
        (*restrictinfo).rinfo_serial = save_rinfo_serial;
        (*root).last_rinfo_serial = save_last_rinfo_serial;
    }

    cur_relid = -1;
    loop {
        cur_relid = bms_next_member(join_relids, cur_relid);
        if cur_relid < 0 {
            break;
        }
        let rel = find_base_rel_ignore_join(root, cur_relid);

        // We only need to add the clause to baserels
        if rel.is_null() {
            continue;
        }
        (*rel).joininfo = lappend((*rel).joininfo, restrictinfo as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// remove_join_clause_from_rels
// ---------------------------------------------------------------------------

/// remove_join_clause_from_rels
///     Delete 'restrictinfo' from all the joininfo lists it is in
///
/// This reverses the effect of add_join_clause_to_rels.  It's used when we
/// discover that a relation need not be joined at all.
///
/// 'restrictinfo' describes the join clause
/// 'join_relids' is the set of relations participating in the join clause
///               (some of these could be outer joins)
pub unsafe fn remove_join_clause_from_rels(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
    join_relids: Relids,
) {
    let mut cur_relid: c_int;

    cur_relid = -1;
    loop {
        cur_relid = bms_next_member(join_relids, cur_relid);
        if cur_relid < 0 {
            break;
        }
        let rel = find_base_rel_ignore_join(root, cur_relid);

        // We would only have added the clause to baserels
        if rel.is_null() {
            continue;
        }

        // Remove the restrictinfo from the list.  Pointer comparison is
        // sufficient.
        Assert!(list_member_ptr((*rel).joininfo, restrictinfo as *const c_void));
        (*rel).joininfo =
            list_delete_ptr((*rel).joininfo, restrictinfo as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::bms_add_member;
    use crate::nodes::nodes::NodeTag;

    // Build a zeroed RelOptInfo with the given relids set (as a single-member
    // bitmapset) and an empty joininfo list.
    unsafe fn make_rel(relid: c_int) -> *mut RelOptInfo {
        let rel = palloc0(core::mem::size_of::<RelOptInfo>()) as *mut RelOptInfo;
        (*rel).relids = bms_add_member(null_mut(), relid);
        (*rel).joininfo = null_mut();
        (*rel).has_eclass_joins = false;
        rel
    }

    // Build a zeroed RestrictInfo whose required_relids is the union of the two
    // given single relids.
    unsafe fn make_rinfo(r1: c_int, r2: c_int) -> *mut RestrictInfo {
        let rinfo = palloc0(core::mem::size_of::<RestrictInfo>()) as *mut RestrictInfo;
        (*rinfo).r#type = NodeTag::T_RestrictInfo;
        let mut req: Relids = null_mut();
        req = bms_add_member(req, r1);
        req = bms_add_member(req, r2);
        (*rinfo).required_relids = req;
        rinfo
    }

    #[test]
    fn test_have_relevant_joinclause_finds_added_clause() {
        unsafe {
            let rel1 = make_rel(1);
            let rel2 = make_rel(2);

            // A join clause referencing both rels 1 and 2.
            let rinfo = make_rinfo(1, 2);

            // Manually attach the clause to both rels' joininfo lists (mirrors
            // what add_join_clause_to_rels would do, but without needing the
            // stubbed find_base_rel_ignore_join).
            (*rel1).joininfo = lappend((*rel1).joininfo, rinfo as *mut c_void);
            (*rel2).joininfo = lappend((*rel2).joininfo, rinfo as *mut c_void);

            // root is unused on this path (no eclass check since
            // has_eclass_joins is false), so a null PlannerInfo is fine.
            let root: *mut PlannerInfo = null_mut();

            assert!(have_relevant_joinclause(root, rel1, rel2));
        }
    }

    #[test]
    fn test_have_relevant_joinclause_none_when_unrelated() {
        unsafe {
            let rel1 = make_rel(1);
            let rel2 = make_rel(2);
            let rel3 = make_rel(3);

            // Clause references rels 1 and 3, so it should NOT make 1 and 2
            // relevant to each other.
            let rinfo = make_rinfo(1, 3);
            (*rel1).joininfo = lappend((*rel1).joininfo, rinfo as *mut c_void);

            let root: *mut PlannerInfo = null_mut();

            // rel1.joininfo (len 1) vs rel2.joininfo (len 0): scans the shorter
            // (rel2's empty list) against rel1.relids -> no overlap -> false.
            assert!(!have_relevant_joinclause(root, rel1, rel2));
        }
    }
}
